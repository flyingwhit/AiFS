package controller_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"infra/controlplane/controller"
	"infra/kvraft/runtime"
	"infra/models"
)

func startTestStack(t *testing.T) (*runtime.Cluster, *controller.Controller, func()) {
	t.Helper()
	addrs := []string{":18001", ":18002", ":18003"}
	cluster, err := runtime.NewCluster(3, addrs, 1000)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1500 * time.Millisecond)

	ctl := controller.New([]string{
		"http://127.0.0.1:18001",
		"http://127.0.0.1:18002",
		"http://127.0.0.1:18003",
	})
	cleanup := func() {
		cluster.Shutdown()
	}
	return cluster, ctl, cleanup
}

func TestWorkerRegisterAndList(t *testing.T) {
	_, ctl, cleanup := startTestStack(t)
	defer cleanup()

	info := models.WorkerInfo{
		IP:     "10.0.0.5",
		Port:   9100,
		Status: models.WorkerIdle,
		GPUs:   []models.GPUInfo{{ID: 0, Model: "A100", MemoryMB: 80000}},
	}
	if err := ctl.RegisterWorker(info); err != nil {
		t.Fatal(err)
	}

	workers, err := ctl.ListWorkers()
	if err != nil {
		t.Fatal(err)
	}
	if len(workers) != 1 {
		t.Fatalf("expected 1 worker, got %d", len(workers))
	}
	if workers[0].IP != info.IP || workers[0].Port != info.Port {
		t.Fatalf("unexpected worker: %+v", workers[0])
	}
}

func TestWorkerHTTPRegister(t *testing.T) {
	_, ctl, cleanup := startTestStack(t)
	defer cleanup()

	srv := httptest.NewServer(http.HandlerFunc(ctl.ServeHTTP))
	defer srv.Close()

	body, _ := json.Marshal(models.WorkerInfo{
		IP:     "127.0.0.1",
		Port:   9200,
		Status: models.WorkerBusy,
	})
	resp, err := http.Post(srv.URL+"/workers/register", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("register status %d", resp.StatusCode)
	}

	resp, err = http.Get(srv.URL + "/workers")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list status %d", resp.StatusCode)
	}

	var out struct {
		Workers []models.WorkerInfo `json:"workers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.Workers) != 1 {
		t.Fatalf("expected 1 worker via HTTP, got %d", len(out.Workers))
	}
	if out.Workers[0].Status != models.WorkerBusy {
		t.Fatalf("status = %s", out.Workers[0].Status)
	}
}
