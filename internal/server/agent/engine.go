// agent/engine.go
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/googleapis/mcp-toolbox-sdk-go/core"
	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/llms/googleai"
)
// ChatEvent is streamed to the UI via SSE.
type ChatEvent struct {
	Type      string      `json:"type"`                // user | assistant | tool_call | tool_resp | agent_error | done
	Content   interface{} `json:"content,omitempty"`   // text or raw JSON
	ToolName  string      `json:"toolName,omitempty"`  // for tool_* events
	Arguments interface{} `json:"arguments,omitempty"` // for tool_call
}

// Engine can be reused safely by many goroutines.
type Engine struct {
	llm            llms.LLM
	langchainTools []llms.Tool                 // tools passed to the LLM
	toolsMap       map[string]*core.ToolboxTool // lookup by both hyphen and snake names
	validNames     []string                    // cached list for error messages
	sysPrompt      string
	maxToolRuns    int
}

// New builds a single Engine instance that you can share.
func New(ctx context.Context, genaiKey, toolboxURL, toolsetID string) (*Engine, error) {
	llm, err := googleai.New(ctx,
		googleai.WithAPIKey(genaiKey),
		googleai.WithDefaultModel("gemini-2.5-pro"))
	if err != nil {
		return nil, fmt.Errorf("googleai: %w", err)
	}

	tb, err := core.NewToolboxClient(toolboxURL)
	if err != nil {
		return nil, fmt.Errorf("toolbox client: %w", err)
	}
	tools, err := tb.LoadToolset(toolsetID, ctx)
	if err != nil {
		return nil, fmt.Errorf("load toolset: %w", err)
	}

	toolsMap := make(map[string]*core.ToolboxTool, len(tools)*2)
	var langTools []llms.Tool
	var valid []string

	for _, t := range tools {
		orig := t.Name()           
		alias := toSnake(orig)    

		toolsMap[orig] = t
		valid = append(valid, orig)

		if alias != orig {
			toolsMap[alias] = t
			valid = append(valid, alias)
		}

		langTools = append(langTools, makeLangTool(t, alias))
	}

	fullPrompt := fmt.Sprintf("%s\n\nValid tools:\n- %s",
		basePrompt, strings.Join(valid, "\n- "))

	return &Engine{
		llm:            llm,
		langchainTools: langTools,
		toolsMap:       toolsMap,
		validNames:     valid,
		sysPrompt:      fullPrompt,
		maxToolRuns:    5,
	}, nil
}

func (e *Engine) Run(ctx context.Context, userMsg string, sink chan<- ChatEvent) {
	defer close(sink)

	// seed history
	history := []llms.MessageContent{
		llms.TextParts(llms.ChatMessageTypeSystem, e.sysPrompt),
		llms.TextParts(llms.ChatMessageTypeHuman, userMsg),
	}
	sink <- ChatEvent{Type: "user", Content: userMsg}

	toolRuns := 0

	for {
		// ask the model
		resp, err := e.llm.GenerateContent(ctx, history, llms.WithTools(e.langchainTools))
		if err != nil {
			sink <- ChatEvent{Type: "agent_error", Content: err.Error()}
			return
		}
		choice := resp.Choices[0]

		// stream assistant thought
		sink <- ChatEvent{Type: "assistant", Content: choice.Content}

		// if no tool calls, we're done
		if len(choice.ToolCalls) == 0 {
			sink <- ChatEvent{Type: "done"}
			return
		}

		// handle every tool call synchronously
		retry := false
		for _, tc := range choice.ToolCalls {
			if toolRuns >= e.maxToolRuns {
				sink <- ChatEvent{Type: "agent_error",
					Content: fmt.Sprintf("aborted: exceeded max tool runs (%d)", e.maxToolRuns)}
				sink <- ChatEvent{Type: "done"}
				return
			}
			toolRuns++

			tool, ok := e.toolsMap[tc.FunctionCall.Name]
			if !ok {
				// hallucinated tool kept happening add correction, retry loop
				msg := fmt.Sprintf("Tool %q does not exist. Valid tools: %s",
					tc.FunctionCall.Name, strings.Join(e.validNames, ", "))
				sink <- ChatEvent{Type: "agent_error", Content: msg}
				history = append(history,
					llms.TextParts(llms.ChatMessageTypeSystem, msg))
				retry = true
				break // leave inner loop, go back to LLM
			}

			// parse arguments
			var args map[string]any
			if err := json.Unmarshal([]byte(tc.FunctionCall.Arguments), &args); err != nil {
				sink <- ChatEvent{Type: "agent_error",
					Content: fmt.Sprintf("arg unmarshal: %v", err)}
				sink <- ChatEvent{Type: "done"}
				return
			}

			// announce call
			sink <- ChatEvent{Type: "tool_call", ToolName: tc.FunctionCall.Name, Arguments: args}

			// invoke tool
			result, err := tool.Invoke(ctx, args)
			if err != nil {
				sink <- ChatEvent{Type: "agent_error",
					Content: fmt.Sprintf("tool error: %v", err)}
				sink <- ChatEvent{Type: "done"}
				return
			}
			if result == "" || result == nil {
				result = "Operation completed successfully."
			}

			// stream response
			sink <- ChatEvent{Type: "tool_resp", ToolName: tc.FunctionCall.Name, Content: result}

			// add to memory
			history = append(history,
				llms.MessageContent{
					Role: llms.ChatMessageTypeTool,
					Parts: []llms.ContentPart{
						llms.ToolCallResponse{
							Name:    tc.FunctionCall.Name,
							Content: fmt.Sprintf("%v", result),
						},
					},
				})
		}

		if retry {
			continue // model will be asked again with correction in history
		}

		// append assistant message (already streamed)
		history = append(history,
			llms.TextParts(llms.ChatMessageTypeAI, choice.Content))
	}
}

// makeLangTool converts a Toolbox tool into a LangChain function tool.
func makeLangTool(t *core.ToolboxTool, exposedName string) llms.Tool {
	schemaBytes, _ := t.InputSchema()
	var paramsSchema map[string]any
	_ = json.Unmarshal(schemaBytes, &paramsSchema)

	return llms.Tool{
		Type: "function",
		Function: &llms.FunctionDefinition{
			Name:        exposedName,
			Description: t.Description(),
			Parameters:  paramsSchema,
		},
	}
}

// toSnake replaces hyphens with underscores.
func toSnake(s string) string {
	return strings.ReplaceAll(s, "-", "_")
}

const basePrompt = `
You are a helpful hotel assistant that uses tools to handle hotel searching, booking, updating, and cancellations.

Rules:
1. When the user searches for a hotel (by name, location, or price tier), call the appropriate tool.
2. Always return the hotel name, id, location, and price tier in search results.
3. When the user asks to book, update, or cancel a hotel, extract the hotel ID and use it in the tool call.
4. You may chain multiple tools in sequence, passing outputs as inputs.
5. Do NOT ask the user for confirmation; just act.
6. Call ONLY tools from list of valid tools; every other name is invalid.
`
