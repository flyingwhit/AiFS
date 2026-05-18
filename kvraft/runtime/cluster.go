package runtime

import (
	"fmt"
	"net/http"

	"infra/kvraft"
	"infra/labrpc"
	"infra/raft"
)

// Cluster runs a multi-node Raft KV cluster with HTTP fronts.
type Cluster struct {
	net       *labrpc.Network
	n         int
	kv        []*kvraft.KVServer
	http      []*http.Server
	saved     []*raft.Persister
	names     [][]string
	httpAddrs []string
}

// NewCluster starts n KV servers connected via labrpc and listening on httpAddrs.
func NewCluster(n int, httpAddrs []string, maxRaftState int) (*Cluster, error) {
	if len(httpAddrs) != n {
		return nil, fmt.Errorf("need %d http addresses, got %d", n, len(httpAddrs))
	}

	c := &Cluster{
		net:       labrpc.MakeNetwork(),
		n:         n,
		kv:        make([]*kvraft.KVServer, n),
		http:      make([]*http.Server, n),
		saved:     make([]*raft.Persister, n),
		names:     make([][]string, n),
		httpAddrs: httpAddrs,
	}

	for i := 0; i < n; i++ {
		c.names[i] = make([]string, n)
		for j := 0; j < n; j++ {
			c.names[i][j] = fmt.Sprintf("raft-%d-to-%d", i, j)
		}
	}

	for i := 0; i < n; i++ {
		if err := c.startServer(i, maxRaftState); err != nil {
			c.Shutdown()
			return nil, err
		}
	}

	c.connectAll()
	return c, nil
}

func (c *Cluster) startServer(i int, maxRaftState int) error {
	ends := make([]*labrpc.ClientEnd, c.n)
	for j := 0; j < c.n; j++ {
		ends[j] = c.net.MakeEnd(c.names[i][j])
		c.net.Connect(c.names[i][j], j)
	}

	c.saved[i] = raft.MakePersister()
	c.kv[i] = kvraft.StartKVServer(ends, i, c.saved[i], maxRaftState)

	kvsvc := labrpc.MakeService(c.kv[i])
	rfsvc := labrpc.MakeService(c.kv[i].Rf())
	srv := labrpc.MakeServer()
	srv.AddService(kvsvc)
	srv.AddService(rfsvc)
	c.net.AddServer(i, srv)

	c.http[i] = kvraft.StartHTTPServer(c.kv[i], c.httpAddrs[i])
	return nil
}

func (c *Cluster) connectAll() {
	for i := 0; i < c.n; i++ {
		for j := 0; j < c.n; j++ {
			c.net.Enable(c.names[i][j], true)
			c.net.Enable(c.names[j][i], true)
		}
	}
}

// Shutdown stops HTTP servers and Raft peers.
func (c *Cluster) Shutdown() {
	for i := 0; i < c.n; i++ {
		if c.http[i] != nil {
			_ = c.http[i].Close()
			c.http[i] = nil
		}
		if c.kv[i] != nil {
			c.kv[i].Kill()
			c.kv[i] = nil
		}
	}
	if c.net != nil {
		c.net.Cleanup()
	}
}

// KV returns server i.
func (c *Cluster) KV(i int) *kvraft.KVServer {
	return c.kv[i]
}
