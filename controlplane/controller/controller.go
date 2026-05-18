package controller

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"infra/controlplane/kvclient"
	"infra/models"
)

const workersIndexKey = "workers/index"

// Controller receives worker registrations and persists metadata in the Raft KV cluster.
type Controller struct {
	kv   *kvclient.Client
	mu   sync.Mutex
	seen map[string]struct{}
}

// New creates a controller backed by the given KV server HTTP endpoints.
func New(kvAddrs []string) *Controller {
	return &Controller{
		kv:   kvclient.New(kvAddrs),
		seen: make(map[string]struct{}),
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
	return c.addToIndex(info.ID)
}

// Heartbeat updates an existing worker record (status, GPUs, etc.).
func (c *Controller) Heartbeat(info models.WorkerInfo) error {
	return c.RegisterWorker(info)
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
	mux.Handle("/workers", c)
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		_ = srv.ListenAndServe()
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
