package server

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"sync"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"

 	"github.com/googleapis/genai-toolbox/internal/server/agent"
)

//go:embed all:static
var staticContent embed.FS

type session struct {
	events chan agent.ChatEvent
}

var (
	sessions = struct {
		sync.RWMutex
		m map[string]*session
	}{m: make(map[string]*session)}
)

func webRouter() (chi.Router, error) {
	r := chi.NewRouter()
	r.Use(middleware.StripSlashes)

	// HTML entry points
	r.Get("/", func(w http.ResponseWriter, r *http.Request) { serveHTML(w, r, "static/index.html") })
	r.Get("/tools", func(w http.ResponseWriter, r *http.Request) { serveHTML(w, r, "static/tools.html") })
	r.Get("/toolsets", func(w http.ResponseWriter, r *http.Request) { serveHTML(w, r, "static/toolsets.html") })
	r.Get("/agent", func(w http.ResponseWriter, r *http.Request) { serveHTML(w, r, "static/agent.html") })

	// Chat endpoints -------------------------------------------------
	r.Post("/chat", startChatHandler)                   // POST  /ui/chat
	r.Get("/chat/{id}/events", streamChatHandler)       //  GET /ui/chat/{id}/events

	// static assets
	staticFS, _ := fs.Sub(staticContent, "static")
	r.Handle("/*", http.StripPrefix("/ui", http.FileServer(http.FS(staticFS))))

	return r, nil
}

type startReq struct {
	Message string `json:"message"`
}
type startResp struct {
	ID string `json:"id"`
}

func startChatHandler(w http.ResponseWriter, r *http.Request) {
	var req startReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Message == "" {
		http.Error(w, "invalid body: need {\"message\":\"...\"}", http.StatusBadRequest)
		return
	}

	eng, err := getEngine(r.Context())
	if err != nil {
		http.Error(w, "engine init: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// create session
	id := uuid.NewString()
	s := &session{events: make(chan agent.ChatEvent, 32)}

	sessions.Lock()
	sessions.m[id] = s
	sessions.Unlock()

	// go eng.Run(r.Context(), req.Message, s.events)
	go eng.Run(context.Background(), req.Message, s.events)

	_ = json.NewEncoder(w).Encode(startResp{ID: id})
}

func streamChatHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	sessions.RLock()
	s, ok := sessions.m[id]
	sessions.RUnlock()
	if !ok {
		http.NotFound(w, r)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "stream unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, open := <-s.events:
			if !open {
				return // chat finished
			}
			b, _ := json.Marshal(ev)
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, b)
			flusher.Flush()
		}
	}
}


var (
	engineOnce sync.Once
	globalEng  *agent.Engine
	engineErr  error
)

func getEngine(ctx context.Context) (*agent.Engine, error) {
	engineOnce.Do(func() {
		genaiKey   := os.Getenv("GOOGLE_API_KEY")
		toolboxURL := "http://localhost:5000"
		toolsetID  := "my-toolset-5"

		globalEng, engineErr = agent.New(ctx, genaiKey, toolboxURL, toolsetID)
	})
	return globalEng, engineErr
}

func serveHTML(w http.ResponseWriter, r *http.Request, filepath string) {
	file, err := staticContent.Open(filepath)
	if err != nil {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}
	defer file.Close()

	fileBytes, err := io.ReadAll(file)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error reading file: %v", err), http.StatusInternalServerError)
		return
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return
	}
	http.ServeContent(w, r, fileInfo.Name(), fileInfo.ModTime(), bytes.NewReader(fileBytes))
}
