package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"infra/internal/logx"
	"infra/models"
)

// clientJob is layer-1 work: accept user HTTP and wait for final inference result.
type clientJob struct {
	prompt   string
	resultCh chan jobResult
}

type jobResult struct {
	resp models.ChatResponse
	err  error
}

// dispatchJob is layer-2 work: forward to a chosen worker node.
type dispatchJob struct {
	prompt   string
	worker   models.WorkerInfo
	resultCh chan jobResult
}

// Gateway is the data-plane entry with double producer-consumer pipelines.
type Gateway struct {
	controllerURL string
	httpClient    *http.Client

	connJobs     chan *clientJob
	dispatchJobs chan *dispatchJob
	wg           sync.WaitGroup
}

// New creates a gateway and starts worker goroutines for both layers.
func New(controllerURL string, connPoolSize, dispatchPoolSize int) *Gateway {
	if connPoolSize < 1 {
		connPoolSize = 32
	}
	if dispatchPoolSize < 1 {
		dispatchPoolSize = 32
	}
	g := &Gateway{
		controllerURL: controllerURL,
		httpClient:    &http.Client{Timeout: 30 * time.Second},
		connJobs:      make(chan *clientJob, 256),
		dispatchJobs:  make(chan *dispatchJob, 1024),
	}
	for i := 0; i < connPoolSize; i++ {
		g.wg.Add(1)
		go g.connWorker(i)
	}
	for i := 0; i < dispatchPoolSize; i++ {
		g.wg.Add(1)
		go g.dispatchWorker(i)
	}
	logx.Info("Gateway", "started | conn_pool=%d dispatch_pool=%d controller=%s", connPoolSize, dispatchPoolSize, controllerURL)
	return g
}

// HandleChat is the HTTP entry (producer for layer-1).
func (g *Gateway) HandleChat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	logx.Info("Gateway", "recv user request | remote=%s path=%s", r.RemoteAddr, r.URL.Path)

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

	job := &clientJob{
		prompt:   prompt,
		resultCh: make(chan jobResult, 1),
	}
	select {
	case g.connJobs <- job:
		logx.Info("Gateway", "enqueued conn job | prompt_len=%d conn_queue=%d", len(prompt), len(g.connJobs))
	case <-time.After(5 * time.Second):
		http.Error(w, "gateway overloaded", http.StatusServiceUnavailable)
		return
	}

	res := <-job.resultCh
	if res.err != nil {
		http.Error(w, res.err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(res.resp)
	logx.Info("Gateway", "respond user OK | reply_len=%d", len(res.resp.Reply))
}

// connWorker is layer-1 consumer: parse, schedule worker, wait for layer-2.
func (g *Gateway) connWorker(id int) {
	defer g.wg.Done()
	for job := range g.connJobs {
		logx.Info("Gateway", "conn worker %d handling | prompt_len=%d", id, len(job.prompt))

		winfo, err := g.pickWorker()
		if err != nil {
			job.resultCh <- jobResult{err: err}
			continue
		}
		logx.Info("Gateway", "scheduled worker | worker_id=%s addr=%s:%d status=%s", winfo.ID, winfo.IP, winfo.Port, winfo.Status)

		djob := &dispatchJob{
			prompt:   job.prompt,
			worker:   winfo,
			resultCh: job.resultCh,
		}
		select {
		case g.dispatchJobs <- djob:
			logx.Info("Gateway", "enqueued dispatch job | worker_id=%s dispatch_queue=%d", winfo.ID, len(g.dispatchJobs))
		case <-time.After(5 * time.Second):
			job.resultCh <- jobResult{err: fmt.Errorf("dispatch queue full")}
		}
	}
}

// dispatchWorker is layer-2 consumer: HTTP forward to worker inference API.
func (g *Gateway) dispatchWorker(id int) {
	defer g.wg.Done()
	for job := range g.dispatchJobs {
		logx.Info("Gateway", "dispatch worker %d forward | worker_id=%s", id, job.worker.ID)
		resp, err := g.forwardToWorker(job.worker, job.prompt)
		if err != nil {
			logx.Info("Gateway", "dispatch failed | worker_id=%s err=%v", job.worker.ID, err)
			job.resultCh <- jobResult{err: err}
			continue
		}
		logx.Info("Gateway", "dispatch success | worker_id=%s gpu_id=%d", job.worker.ID, resp.GPUID)
		job.resultCh <- jobResult{resp: resp}
	}
}

func (g *Gateway) pickWorker() (models.WorkerInfo, error) {
	url := g.controllerURL + "/workers/best"
	resp, err := g.httpClient.Get(url)
	if err != nil {
		return models.WorkerInfo{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return models.WorkerInfo{}, fmt.Errorf("controller: %s", resp.Status)
	}
	var winfo models.WorkerInfo
	if err := json.NewDecoder(resp.Body).Decode(&winfo); err != nil {
		return models.WorkerInfo{}, err
	}
	return winfo, nil
}

func (g *Gateway) forwardToWorker(w models.WorkerInfo, prompt string) (models.ChatResponse, error) {
	url := fmt.Sprintf("http://%s:%d/v1/chat/completions", w.IP, w.Port)
	reqBody, _ := json.Marshal(models.ChatRequest{Prompt: prompt})
	resp, err := g.httpClient.Post(url, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		return models.ChatResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return models.ChatResponse{}, fmt.Errorf("worker %s: %s (%s)", w.ID, resp.Status, string(raw))
	}
	var out models.ChatResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return models.ChatResponse{}, err
	}
	return out, nil
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

// ServeHTTP implements http.Handler for the gateway mux.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/v1/chat/completions" {
		g.HandleChat(w, r)
		return
	}
	http.NotFound(w, r)
}

// StartHTTPServer runs the gateway HTTP server.
func StartHTTPServer(g *Gateway, addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/v1/chat/completions", g)
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		logx.Info("Gateway", "HTTP listen | addr=%s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logx.Info("Gateway", "HTTP listen failed | addr=%s err=%v", addr, err)
		}
	}()
	return srv
}
