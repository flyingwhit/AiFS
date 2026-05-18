package models

import "fmt"

// WorkerStatus describes whether a worker can accept inference jobs.
type WorkerStatus string

const (
	WorkerBusy    WorkerStatus = "busy"
	WorkerIdle    WorkerStatus = "idle"
	WorkerOffline WorkerStatus = "offline"
)

// GPUInfo describes one GPU on a worker node.
type GPUInfo struct {
	ID          int     `json:"id"`
	Model       string  `json:"model,omitempty"`
	MemoryMB    int     `json:"memory_mb,omitempty"`
	Utilization float64 `json:"utilization,omitempty"`
}

// WorkerInfo is reported by an AI worker to the control plane.
type WorkerInfo struct {
	ID     string       `json:"id,omitempty"`
	IP     string       `json:"ip"`
	Port   int          `json:"port"`
	GPUs   []GPUInfo    `json:"gpus,omitempty"`
	Status WorkerStatus `json:"status"`
}

// WorkerKey returns the KV key used to store this worker.
func WorkerKey(w WorkerInfo) string {
	if w.ID != "" {
		return "worker/" + w.ID
	}
	return fmt.Sprintf("worker/%s:%d", w.IP, w.Port)
}

// EnsureID fills WorkerInfo.ID from IP:port when empty.
func (w *WorkerInfo) EnsureID() {
	if w.ID == "" {
		w.ID = fmt.Sprintf("%s:%d", w.IP, w.Port)
	}
}
