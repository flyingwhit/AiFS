package worker

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"infra/internal/logx"
	"infra/models"
)

const defaultQueueCap = 1024

// Task is one inference job in the local GPU queue.
type Task struct {
	ID     string
	Prompt string
	Ctx    context.Context
	Done   chan taskResult
}

type taskResult struct {
	Response models.ChatResponse
	Err      error
}

// Pool is a bounded channel queue + fixed GPU consumer goroutines.
type Pool struct {
	queue    chan *Task
	gpuCount int
	workerID string
	backend  Backend
	seq      atomic.Uint64
}

// NewPool creates a pool with queue capacity 1024 and gpuCount consumers.
func NewPool(workerID string, gpuCount int, backend Backend) *Pool {
	if gpuCount < 1 {
		gpuCount = 1
	}
	if backend == nil {
		backend = &MockBackend{WorkerID: workerID}
	}
	p := &Pool{
		queue:    make(chan *Task, defaultQueueCap),
		gpuCount: gpuCount,
		workerID: workerID,
		backend:  backend,
	}
	for i := 0; i < gpuCount; i++ {
		go p.gpuLoop(i)
	}
	logx.Info("Worker", "inference pool started | worker=%s gpus=%d queue_cap=%d", workerID, gpuCount, defaultQueueCap)
	return p
}

// Submit enqueues a task and blocks until a GPU consumer completes it.
func (p *Pool) Submit(ctx context.Context, prompt string) (models.ChatResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	id := fmt.Sprintf("%s-%d", p.workerID, p.seq.Add(1))
	task := &Task{
		ID:     id,
		Prompt: prompt,
		Ctx:    ctx,
		Done:   make(chan taskResult, 1),
	}

	logx.Info("Worker", "task enqueue | task_id=%s prompt_len=%d queue_len=%d", id, len(prompt), len(p.queue))

	select {
	case p.queue <- task:
		logx.Info("Worker", "task queued | task_id=%s", id)
	case <-time.After(5 * time.Second):
		return models.ChatResponse{}, fmt.Errorf("queue full, enqueue timeout")
	case <-ctx.Done():
		return models.ChatResponse{}, ctx.Err()
	}

	select {
	case result := <-task.Done:
		return result.Response, result.Err
	case <-ctx.Done():
		return models.ChatResponse{}, ctx.Err()
	}
}

func (p *Pool) gpuLoop(gpuID int) {
	for task := range p.queue {
		logx.Info("Worker", "GPU compute start | task_id=%s gpu_id=%d", task.ID, gpuID)
		resp, err := p.backend.Generate(task.Ctx, task.Prompt)
		if resp.WorkerID == "" {
			resp.WorkerID = p.workerID
		}
		resp.GPUID = gpuID
		if err != nil {
			logx.Info("Worker", "GPU compute failed | task_id=%s gpu_id=%d err=%v", task.ID, gpuID, err)
		} else {
			logx.Info("Worker", "GPU compute done | task_id=%s gpu_id=%d reply_len=%d", task.ID, gpuID, len(resp.Reply))
		}
		task.Done <- taskResult{Response: resp, Err: err}
	}
}
