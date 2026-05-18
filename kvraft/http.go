package kvraft

import (
	"encoding/json"
	"io"
	"net/http"
	"sync/atomic"
)

const httpClientID int64 = 0xA1F5

// HTTPRequest is the JSON body for POST /kv.
type HTTPRequest struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// HTTPResponse is the JSON body returned by POST /kv.
type HTTPResponse struct {
	OK    bool   `json:"ok"`
	Value string `json:"value,omitempty"`
	Err   string `json:"err"`
}

func (kv *KVServer) nextHTTPSeq() int {
	return int(atomic.AddInt64(&kv.httpSeq, 1))
}

func (kv *KVServer) executeHTTP(op, key, value string) HTTPResponse {
	seq := kv.nextHTTPSeq()
	switch op {
	case "get":
		args := GetArgs{Key: key, SeqId: seq, ClientId: httpClientID}
		reply := GetReply{}
		kv.Get(&args, &reply)
		return httpResponseFromErr(reply.Err, reply.Value)
	case "put":
		args := PutAppendArgs{Key: key, Value: value, Op: "Put", SeqId: seq, ClientId: httpClientID}
		reply := PutAppendReply{}
		kv.PutAppend(&args, &reply)
		return httpResponseFromErr(reply.Err, "")
	case "delete":
		args := DeleteArgs{Key: key, SeqId: seq, ClientId: httpClientID}
		reply := DeleteReply{}
		kv.Delete(&args, &reply)
		return httpResponseFromErr(reply.Err, "")
	default:
		return HTTPResponse{OK: false, Err: "ErrBadRequest"}
	}
}

func httpResponseFromErr(err Err, value string) HTTPResponse {
	resp := HTTPResponse{
		OK:    err == OK,
		Value: value,
		Err:   string(err),
	}
	return resp
}

// ServeHTTP handles POST /kv for control-plane metadata operations.
func (kv *KVServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var req HTTPRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if req.Key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	resp := kv.executeHTTP(req.Op, req.Key, req.Value)
	w.Header().Set("Content-Type", "application/json")
	if resp.Err == string(ErrWrongLeader) {
		w.WriteHeader(http.StatusServiceUnavailable)
	} else if !resp.OK && resp.Err == string(ErrBadRequest) {
		w.WriteHeader(http.StatusBadRequest)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	_ = json.NewEncoder(w).Encode(resp)
}

// StartHTTPServer exposes the KV HTTP API on addr (e.g. ":8001").
func StartHTTPServer(kv *KVServer, addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/kv", kv)
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		_ = srv.ListenAndServe()
	}()
	return srv
}
