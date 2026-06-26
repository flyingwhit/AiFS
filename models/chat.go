package models

// ChatMessage is the OpenAI-compatible message unit used by vLLM.
type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ChatRequest is the AIFS chat completion input.
//
// Prompt keeps the original Phase 3 simple API working. Messages/Model/MaxTokens
// let the same struct carry OpenAI-style requests when the caller already has
// them.
type ChatRequest struct {
	Prompt    string        `json:"prompt,omitempty"`
	Model     string        `json:"model,omitempty"`
	Messages  []ChatMessage `json:"messages,omitempty"`
	MaxTokens int           `json:"max_tokens,omitempty"`
}

// ChatResponse is an OpenAI-style chat completion output.
type ChatResponse struct {
	Reply    string `json:"reply"`
	WorkerID string `json:"worker_id,omitempty"`
	GPUID    int    `json:"gpu_id,omitempty"`
}
