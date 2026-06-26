package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"infra/internal/logx"
	"infra/models"
)

const (
	BackendMock = "mock"
	BackendVLLM = "vllm"
)

// Backend is the execution engine used by GPU consumer goroutines.
type Backend interface {
	Health(ctx context.Context) error
	Generate(ctx context.Context, prompt string) (models.ChatResponse, error)
}

// MockBackend preserves the original Phase 3 behavior for local development.
type MockBackend struct {
	WorkerID string
	Delay    time.Duration
}

func (b *MockBackend) Health(ctx context.Context) error {
	return nil
}

func (b *MockBackend) Generate(ctx context.Context, prompt string) (models.ChatResponse, error) {
	delay := b.Delay
	if delay <= 0 {
		delay = 500 * time.Millisecond
	}
	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return models.ChatResponse{}, ctx.Err()
	}
	return models.ChatResponse{
		Reply:    fmt.Sprintf("[mock] reply to: %s", prompt),
		WorkerID: b.WorkerID,
	}, nil
}

// VLLMBackend calls vLLM's OpenAI-compatible API.
type VLLMBackend struct {
	BaseURL   string
	Model     string
	MaxTokens int
	WorkerID  string
	Client    *http.Client
}

func NewVLLMBackend(baseURL, model string, maxTokens int, workerID string) *VLLMBackend {
	return &VLLMBackend{
		BaseURL:   strings.TrimRight(baseURL, "/"),
		Model:     model,
		MaxTokens: maxTokens,
		WorkerID:  workerID,
		Client: &http.Client{
			Timeout: 2 * time.Minute,
		},
	}
}

func (b *VLLMBackend) Health(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.BaseURL+"/v1/models", nil)
	if err != nil {
		return err
	}
	resp, err := b.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("vllm health: %s %s", resp.Status, strings.TrimSpace(string(raw)))
	}
	return nil
}

func (b *VLLMBackend) Generate(ctx context.Context, prompt string) (models.ChatResponse, error) {
	reqBody := vllmChatRequest{
		Model: b.Model,
		Messages: []models.ChatMessage{
			{Role: "user", Content: prompt},
		},
		MaxTokens: b.MaxTokens,
	}
	if reqBody.MaxTokens <= 0 {
		reqBody.MaxTokens = 256
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return models.ChatResponse{}, err
	}

	url := b.BaseURL + "/v1/chat/completions"
	logx.Info("Worker", "vLLM request | model=%s url=%s prompt_len=%d", b.Model, url, len(prompt))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return models.ChatResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := b.Client.Do(req)
	if err != nil {
		return models.ChatResponse{}, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return models.ChatResponse{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return models.ChatResponse{}, fmt.Errorf("vllm: %s %s", resp.Status, strings.TrimSpace(string(raw)))
	}

	var out vllmChatResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return models.ChatResponse{}, err
	}
	if len(out.Choices) == 0 {
		return models.ChatResponse{}, fmt.Errorf("vllm response has no choices")
	}

	reply := out.Choices[0].Message.Content
	logx.Info("Worker", "vLLM response | model=%s reply_len=%d", out.Model, len(reply))
	return models.ChatResponse{
		Reply:    reply,
		WorkerID: b.WorkerID,
	}, nil
}

type vllmChatRequest struct {
	Model     string               `json:"model"`
	Messages  []models.ChatMessage `json:"messages"`
	MaxTokens int                  `json:"max_tokens,omitempty"`
}

type vllmChatResponse struct {
	Model   string `json:"model"`
	Choices []struct {
		Message models.ChatMessage `json:"message"`
	} `json:"choices"`
}

// WaitForBackend blocks until the selected backend is ready or the timeout expires.
func WaitForBackend(ctx context.Context, backend Backend, interval time.Duration) error {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if err := backend.Health(ctx); err == nil {
			return nil
		} else {
			logx.Info("Worker", "backend not ready | err=%v", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
