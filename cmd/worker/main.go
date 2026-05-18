package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"infra/models"
)

func main() {
	// flag : parsing cmdline arguments(argument default value manual)
	controllerURL := flag.String("controller", "http://127.0.0.1:9000", "controller base URL")
	ip := flag.String("ip", "127.0.0.1", "worker IP advertised to control plane")
	port := flag.Int("port", 9100, "worker port")
	status := flag.String("status", "idle", "worker status: idle, busy, offline")
	interval := flag.Duration("interval", 0, "if >0, send heartbeat periodically") // heartbeat split
	flag.Parse()

	info := models.WorkerInfo{
		IP:     *ip,
		Port:   *port,
		Status: models.WorkerStatus(*status),
		GPUs: []models.GPUInfo{
			{ID: 0, Model: "mock-gpu", MemoryMB: 24576, Utilization: 0.1},
		},
	}
	info.EnsureID()

	if err := register(*controllerURL, info); err != nil {
		log.Fatalf("register: %v", err)
	}
	log.Printf("registered worker %s (%s)", info.ID, info.Status)

	if *interval <= 0 {
		return
	}

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()
	for range ticker.C {
		if err := heartbeat(*controllerURL, info); err != nil {
			log.Printf("heartbeat error: %v", err)
			continue
		}
		log.Printf("heartbeat ok (%s)", info.Status)
	}
}

func register(base string, info models.WorkerInfo) error {
	return postJSON(base+"/workers/register", info)
}

func heartbeat(base string, info models.WorkerInfo) error {
	return postJSON(base+"/workers/heartbeat", info)
}

func postJSON(url string, body interface{}) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("http %s", resp.Status)
	}
	return nil
}
