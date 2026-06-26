package kvraft

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sync/atomic"
)

const httpClientID int64 = 0xA1F5

// HTTPRequest is the JSON body for POST /kv.
type HTTPRequest struct {
	Op       string `json:"op"`
	Key      string `json:"key"`
	Value    string `json:"value,omitempty"`
	ClientID int64  `json:"client_id,omitempty"`
	SeqID    int    `json:"seq_id,omitempty"`
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
	clientID := httpClientID
	return kv.executeHTTPWithClient(op, key, value, clientID, seq)
}

func (kv *KVServer) executeHTTPWithClient(op, key, value string, clientID int64, seq int) HTTPResponse {
	switch op {
	case "get":
		args := GetArgs{Key: key, SeqId: seq, ClientId: clientID}
		reply := GetReply{}
		kv.Get(&args, &reply)
		return httpResponseFromErr(reply.Err, reply.Value)
	case "put":
		args := PutAppendArgs{Key: key, Value: value, Op: "Put", SeqId: seq, ClientId: clientID}
		reply := PutAppendReply{}
		kv.PutAppend(&args, &reply)
		return httpResponseFromErr(reply.Err, "")
	case "delete":
		args := DeleteArgs{Key: key, SeqId: seq, ClientId: clientID}
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

	clientID := req.ClientID
	seqID := req.SeqID
	if clientID == 0 || seqID == 0 {
		clientID = httpClientID
		seqID = kv.nextHTTPSeq()
	}

	resp := kv.executeHTTPWithClient(req.Op, req.Key, req.Value, clientID, seqID)
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
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[KVServer] HTTP listen failed on %s: %v", addr, err)
		}
	}()
	return srv
}
