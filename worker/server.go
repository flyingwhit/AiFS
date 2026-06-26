package worker

import (
	"encoding/json"
	"io"
	"net/http"

	"infra/internal/logx"
	"infra/models"
)

// Server exposes data-plane HTTP APIs for inference.
type Server struct {
	pool *Pool
}

// NewServer wraps an inference pool.
func NewServer(pool *Pool) *Server {
	return &Server{pool: pool}
}

// ServeHTTP routes data-plane endpoints.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost && r.URL.Path == "/v1/chat/completions" {
		s.handleChat(w, r)
		return
	}
	http.NotFound(w, r)
}

func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	logx.Info("Worker", "HTTP recv POST /v1/chat/completions | remote=%s", r.RemoteAddr)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var req models.ChatRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	prompt := promptFromRequest(req)
	if prompt == "" {
		http.Error(w, "prompt required", http.StatusBadRequest)
		return
	}

	resp, err := s.pool.Submit(r.Context(), prompt)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
	logx.Info("Worker", "HTTP respond OK | reply_len=%d worker_id=%s gpu_id=%d", len(resp.Reply), resp.WorkerID, resp.GPUID)
}

func promptFromRequest(req models.ChatRequest) string {
	if req.Prompt != "" {
		return req.Prompt
	}
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == "user" && req.Messages[i].Content != "" {
			return req.Messages[i].Content
		}
	}
	return ""
}

// StartHTTPServer listens on addr for inference APIs.
func StartHTTPServer(s *Server, addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/v1/chat/completions", s)
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		logx.Info("Worker", "HTTP listen | addr=%s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logx.Info("Worker", "HTTP listen failed | addr=%s err=%v", addr, err)
		}
	}()
	return srv
}
