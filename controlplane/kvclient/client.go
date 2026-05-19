package kvclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"infra/kvraft"
)

// Client talks to the Raft KV cluster over HTTP POST /kv.
type Client struct {
	addrs []string
	curr  int
	mu    sync.Mutex
	http  *http.Client // http keep alive ()
}

// New creates a client with a round-robin list of KV server base URLs.
func New(addrs []string) *Client {
	normalized := make([]string, 0, len(addrs))
	for _, a := range addrs {
		a = strings.TrimRight(strings.TrimSpace(a), "/")
		if a == "" {
			continue
		}
		normalized = append(normalized, a)
	}
	return &Client{
		addrs: normalized,
		http: &http.Client{
			Timeout: 2 * time.Second,
		},
	}
}

func (c *Client) nextAddr() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.addrs) == 0 {
		return ""
	}
	addr := c.addrs[c.curr]
	c.curr = (c.curr + 1) % len(c.addrs)
	return addr
}

func (c *Client) doOnce(addr string, req kvraft.HTTPRequest) (kvraft.HTTPResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return kvraft.HTTPResponse{}, err
	}

	url := addr + "/kv"
	// prepare httpReq without sending
	httpReq, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return kvraft.HTTPResponse{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return kvraft.HTTPResponse{}, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return kvraft.HTTPResponse{}, err
	}

	var kvResp kvraft.HTTPResponse
	if err := json.Unmarshal(raw, &kvResp); err != nil {
		return kvraft.HTTPResponse{}, fmt.Errorf("decode response: %w", err)
	}
	return kvResp, nil
}

func (c *Client) execute(req kvraft.HTTPRequest) (kvraft.HTTPResponse, error) {
	if len(c.addrs) == 0 {
		return kvraft.HTTPResponse{}, fmt.Errorf("no kv server addresses configured")
	}

	// round-robin
	tries := len(c.addrs)
	for i := 0; i < tries; i++ {
		addr := c.nextAddr()
		kvResp, err := c.doOnce(addr, req)
		if err != nil {
			continue
		}
		if kvResp.Err == string(kvraft.ErrWrongLeader) || kvResp.Err == string(kvraft.ErrTimeOut) {
			continue
		}
		return kvResp, nil
	}
	return kvraft.HTTPResponse{}, fmt.Errorf("no available kv leader")
}

// Put stores key/value in the Raft cluster.
func (c *Client) Put(key, value string) error {
	resp, err := c.execute(kvraft.HTTPRequest{Op: "put", Key: key, Value: value})
	if err != nil {
		return err
	}
	if resp.Err != string(kvraft.OK) {
		return fmt.Errorf("kv put: %s", resp.Err)
	}
	return nil
}

// Get reads key from the Raft cluster.
func (c *Client) Get(key string) (string, error) {
	resp, err := c.execute(kvraft.HTTPRequest{Op: "get", Key: key})
	if err != nil {
		return "", err
	}
	switch resp.Err {
	case string(kvraft.OK):
		return resp.Value, nil
	case string(kvraft.ErrNoKey):
		return "", nil
	default:
		return "", fmt.Errorf("kv get: %s", resp.Err)
	}
}

// Delete removes key from the Raft cluster.
func (c *Client) Delete(key string) error {
	resp, err := c.execute(kvraft.HTTPRequest{Op: "delete", Key: key})
	if err != nil {
		return err
	}
	if resp.Err != string(kvraft.OK) {
		return fmt.Errorf("kv delete: %s", resp.Err)
	}
	return nil
}
