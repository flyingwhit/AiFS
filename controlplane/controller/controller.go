package controller

import (
	"encoding/json"
	"fmt"
	"infra/controlplane/kvclient"
	"infra/models"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

const workersIndexKey = "workers/index"

// Controller receives worker registrations and persists metadata in the Raft KV cluster.
type Controller struct {
	kv       *kvclient.Client
	mu       sync.Mutex
	seen     map[string]struct{}
	lastseen map[string]time.Time
}

// New creates a controller backed by the given KV server HTTP endpoints.
func New(kvAddrs []string) *Controller {
	c := &Controller{
		kv:       kvclient.New(kvAddrs),
		seen:     make(map[string]struct{}),
		lastseen: make(map[string]time.Time),
	}
	go c.startHeartbeatChecker(6 * time.Second)

	return c
}

func (c *Controller) startHeartbeatChecker(timeout time.Duration) {
	ticker := time.NewTicker(timeout)
	for range ticker.C {
		hasChange := false
		c.mu.Lock()
		for id, lasttime := range c.lastseen {
			if time.Since(lasttime) > timeout {
				log.Printf("[Controller] worker %s timeout\n", id)
				hasChange = true
				delete(c.seen, id)
				delete(c.lastseen, id)
			}

		}
		if hasChange == true {
			_ = c.saveIndexLocked()
		}
		c.mu.Unlock()
	}
}

// RegisterWorker stores worker metadata as a JSON KV pair.
func (c *Controller) RegisterWorker(info models.WorkerInfo) error {
	info.EnsureID()
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	key := models.WorkerKey(info)
	if err := c.kv.Put(key, string(data)); err != nil {
		return err
	}
	log.Printf("[Controller] Node [%s] (IP: %s) registered with %d GPU(s)",
		info.ID, info.IP, len(info.GPUs))

	for _, gpu := range info.GPUs {
		log.Printf("  ├── [GPU #%d] Model: %s | Memory: %d MB | Utilization: %.2f%%",
			gpu.ID,
			gpu.Model,
			gpu.MemoryMB,
			gpu.Utilization*100,
		)
	}
	c.mu.Lock()
	c.seen[info.ID] = struct{}{}
	c.lastseen[info.ID] = time.Now()
	err = c.saveIndexLocked()
	c.mu.Unlock()
	return err
}

// Heartbeat updates an existing worker record (status, GPUs, etc.).
func (c *Controller) Heartbeat(info models.WorkerInfo) error {
	c.mu.Lock()
	c.seen[info.ID] = struct{}{}
	c.lastseen[info.ID] = time.Now()
	err := c.saveIndexLocked()
	c.mu.Unlock()
	log.Printf("[Controller] receive heartbeat from node %s\n", info.ID)
	return err
}

// GetWorker loads one worker record from KV.
func (c *Controller) GetWorker(id string) (models.WorkerInfo, error) {
	raw, err := c.kv.Get("worker/" + id)
	if err != nil {
		return models.WorkerInfo{}, err
	}
	if raw == "" {
		return models.WorkerInfo{}, fmt.Errorf("worker %q not found", id)
	}
	var info models.WorkerInfo
	if err := json.Unmarshal([]byte(raw), &info); err != nil {
		return models.WorkerInfo{}, err
	}
	return info, nil
}

// PickBestWorker returns the alive worker with lowest GPU utilization (idle preferred).
func (c *Controller) PickBestWorker() (models.WorkerInfo, error) {
	workers, err := c.ListWorkers()
	if err != nil {
		return models.WorkerInfo{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if best, ok := pickLowestLoadWorker(workers, c.seen, true); ok {
		return best, nil
	}

	// Controller memory is intentionally volatile. If the process restarted
	// or the heartbeat checker briefly removed an entry, fall back to the
	// strongly-consistent KV worker list instead of making Gateway fail while
	// /workers still shows routable nodes.
	if best, ok := pickLowestLoadWorker(workers, nil, false); ok {
		return best, nil
	}
	return models.WorkerInfo{}, fmt.Errorf("no available worker")
}

func pickLowestLoadWorker(workers []models.WorkerInfo, seen map[string]struct{}, requireSeen bool) (models.WorkerInfo, bool) {
	var best models.WorkerInfo
	bestScore := 0.0
	found := false

	for _, w := range workers {
		if requireSeen {
			if _, ok := seen[w.ID]; !ok {
				continue
			}
		}
		if w.Status == models.WorkerOffline {
			continue
		}
		score := workerLoadScore(w)
		if !found || score < bestScore {
			best = w
			bestScore = score
			found = true
		}
	}
	return best, found
}

func workerLoadScore(w models.WorkerInfo) float64 {
	if w.Status == models.WorkerBusy {
		return 1000
	}
	maxUtil := 0.0
	for _, g := range w.GPUs {
		if g.Utilization > maxUtil {
			maxUtil = g.Utilization
		}
	}
	return maxUtil
}

// ListWorkers returns all registered workers.
func (c *Controller) ListWorkers() ([]models.WorkerInfo, error) {
	ids, err := c.loadIndex()
	if err != nil {
		return nil, err
	}
	out := make([]models.WorkerInfo, 0, len(ids))
	for _, id := range ids {
		info, err := c.GetWorker(id)
		if err != nil {
			continue
		}
		out = append(out, info)
	}
	return out, nil
}

func (c *Controller) addToIndex(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.seen[id]; ok {
		return c.saveIndexLocked()
	}
	c.seen[id] = struct{}{}
	return c.saveIndexLocked()
}

func (c *Controller) loadIndex() ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.seen) > 0 {
		return c.idsLocked(), nil
	}

	raw, err := c.kv.Get(workersIndexKey)
	if err != nil {
		return nil, err
	}
	if raw == "" {
		return nil, nil
	}
	var ids []string
	if err := json.Unmarshal([]byte(raw), &ids); err != nil {
		return nil, err
	}
	for _, id := range ids {
		c.seen[id] = struct{}{}
	}
	return ids, nil
}

func (c *Controller) saveIndexLocked() error {
	data, err := json.Marshal(c.idsLocked())
	if err != nil {
		return err
	}
	return c.kv.Put(workersIndexKey, string(data))
}

func (c *Controller) idsLocked() []string {
	ids := make([]string, 0, len(c.seen))
	for id := range c.seen {
		ids = append(ids, id)
	}
	return ids
}

// ServeHTTP exposes worker control APIs on the controller.
func (c *Controller) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/workers/register":
		c.handleRegister(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/workers/heartbeat":
		c.handleHeartbeat(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/workers":
		c.handleList(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/workers/best":
		c.handleBest(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (c *Controller) handleRegister(w http.ResponseWriter, r *http.Request) {
	info, err := decodeWorker(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := c.RegisterWorker(info); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "registered", "id": info.ID})
}

func (c *Controller) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	// get workerinfo
	info, err := decodeWorker(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := c.Heartbeat(info); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "id": info.ID})
}

func (c *Controller) handleBest(w http.ResponseWriter, r *http.Request) {
	winfo, err := c.PickBestWorker()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, winfo)
}

func (c *Controller) handleList(w http.ResponseWriter, r *http.Request) {
	workers, err := c.ListWorkers()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"workers": workers})
}

func decodeWorker(r *http.Request) (models.WorkerInfo, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return models.WorkerInfo{}, err
	}
	defer r.Body.Close()
	var info models.WorkerInfo
	if err := json.Unmarshal(body, &info); err != nil {
		return models.WorkerInfo{}, err
	}
	if info.IP == "" || info.Port == 0 {
		return models.WorkerInfo{}, fmt.Errorf("ip and port are required")
	}
	if info.Status == "" {
		info.Status = models.WorkerIdle
	}
	info.EnsureID()
	return info, nil
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// StartHTTPServer listens on addr (e.g. ":9000").
func StartHTTPServer(c *Controller, addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/workers/register", c)
	mux.Handle("/workers/heartbeat", c)
	mux.Handle("/workers/best", c)
	mux.Handle("/workers", c)
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[Controller] HTTP listen failed on %s: %v", addr, err)
		}
	}()
	return srv
}

// ParseKVAddrs splits a comma-separated list of KV HTTP base URLs.
func ParseKVAddrs(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
