import { gemini, GEMINI_MODEL } from "@llamaindex/google";
import { agent } from "@llamaindex/workflow";
import { createMemory, staticBlock, tool } from "llamaindex";
import { ToolboxClient } from "@toolbox-sdk/core";

const TOOLBOX_URL = "http://127.0.0.1:5000"; // Update if needed
const GOOGLE_API_KEY = process.env.GOOGLE_API_KEY || 'your-api-key'; // Replace it with your API key

const prompt = `

You're a helpful hotel assistant. You handle hotel searching, booking and cancellations.
When the user searches for a hotel, mention its name, id, location and price tier.
Always mention hotel ids while performing any searches â€” this is very important for operations.
For any bookings or cancellations, please provide the appropriate confirmation.
Update check-in or check-out dates if mentioned by the user.
Don't ask for confirmations from the user.

`;

const queries = [
  "Find hotels in Basel with Basel in its name.",
  "Can you book the Hilton Basel for me?",
  "Oh wait, this is too expensive. Please cancel it and book the Hyatt Regency instead.",
  "My check in dates would be from April 10, 2024 to April 19, 2024.",
];

export async function main() {
  // Connect to MCP Toolbox
  const client = new ToolboxClient(TOOLBOX_URL);
  const toolboxTools = await client.loadToolset("my-toolset");
  const tools = toolboxTools.map((toolboxTool) => {
    return tool({
      name: toolboxTool.getName(),
      description: toolboxTool.getDescription(),
      parameters: toolboxTool.getParamSchema(),
      execute: toolboxTool,
    });
  });

  // Initialize LLM
  const llm = gemini({
    model: GEMINI_MODEL.GEMINI_2_0_FLASH,
    apiKey: GOOGLE_API_KEY,
  });

  const memory = createMemory({
    memoryBlocks: [
      staticBlock({
        content: prompt,
      }),
    ],
  });

  // Create the Agent
  const myAgent = agent({
    tools: tools,
    llm,
    memory,
    systemPrompt: prompt,
  });

  for (const query of queries) {
    const result = await myAgent.run(query);
    const output = result.data.result;

    console.log(`\nUser: ${query}`);
    if (typeof output === "string") {
      console.log(output.trim());
    } else if (typeof output === "object" && "text" in output) {
      console.log(output.text.trim());
    } else {
      console.log(JSON.stringify(output));
    }
  }
  //You may observe some extra logs during execution due to the run method provided by Llama.
  console.log("Agent run finished.");
}

main();