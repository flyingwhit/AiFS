package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"infra/internal/logx"
	"infra/models"
	"infra/worker"
)

func main() {
	controllerURL := flag.String("controller", "http://127.0.0.1:9000", "controller base URL")
	ip := flag.String("ip", "127.0.0.1", "worker IP advertised to control plane")
	port := flag.Int("port", 9100, "worker HTTP listen port")
	status := flag.String("status", "idle", "worker status: idle, busy, offline")
	gpus := flag.Int("gpus", 1, "number of local task consumer goroutines")
	interval := flag.Duration("interval", 2*time.Second, "heartbeat interval; 0 disables")
	backendType := flag.String("backend", worker.BackendMock, "inference backend: mock or vllm")
	vllmURL := flag.String("vllm-url", "http://127.0.0.1:8000", "vLLM OpenAI-compatible base URL")
	model := flag.String("model", "Qwen/Qwen3-0.6B", "model name sent to vLLM")
	maxTokens := flag.Int("max-tokens", 256, "max tokens for vLLM responses")
	backendWait := flag.Duration("backend-wait", 5*time.Minute, "max time to wait for backend readiness")
	flag.Parse()

	info := models.WorkerInfo{
		IP:     *ip,
		Port:   *port,
		Status: models.WorkerStatus(*status),
		GPUs:   make([]models.GPUInfo, *gpus),
	}
	for i := 0; i < *gpus; i++ {
		info.GPUs[i] = models.GPUInfo{
			ID:          i,
			Model:       "mock-gpu",
			MemoryMB:    24576,
			Utilization: 0.1,
		}
	}
	info.EnsureID()

	backend, err := buildBackend(*backendType, *vllmURL, *model, *maxTokens, info.ID)
	if err != nil {
		log.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), *backendWait)
	if err := worker.WaitForBackend(waitCtx, backend, 2*time.Second); err != nil {
		cancel()
		log.Fatalf("backend not ready: %v", err)
	}
	cancel()
	logx.Info("Worker", "backend ready | type=%s model=%s", *backendType, *model)

	if err := register(*controllerURL, info); err != nil {
		log.Fatalf("register: %v", err)
	}
	logx.Info("Worker", "registered to controller | id=%s", info.ID)

	pool := worker.NewPool(info.ID, *gpus, backend)
	srv := worker.StartHTTPServer(worker.NewServer(pool), fmt.Sprintf(":%d", *port))

	if *interval > 0 {
		go heartbeatLoop(*controllerURL, info, *interval)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	_ = srv.Close()
	logx.Info("Worker", "stopped | id=%s", info.ID)
}

func buildBackend(kind string, vllmURL string, model string, maxTokens int, workerID string) (worker.Backend, error) {
	switch kind {
	case worker.BackendMock:
		return &worker.MockBackend{WorkerID: workerID}, nil
	case worker.BackendVLLM:
		return worker.NewVLLMBackend(vllmURL, model, maxTokens, workerID), nil
	default:
		return nil, fmt.Errorf("unknown backend %q", kind)
	}
}

func heartbeatLoop(base string, info models.WorkerInfo, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		if err := heartbeat(base, info); err != nil {
			logx.Info("Worker", "heartbeat error | id=%s err=%v", info.ID, err)
		}
	}
}

func register(base string, info models.WorkerInfo) error {
	return postJSON(base+"/workers/register", info)
}

func heartbeat(base string, info models.WorkerInfo) error {
	return postJSON(base+"/workers/heartbeat", info)
}

func postJSON(url string, body interface{}) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("http %s", resp.Status)
	}
	return nil
}
