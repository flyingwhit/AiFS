package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"infra/controlplane/controller"
)

func main() {
	addr := flag.String("addr", ":9000", "controller HTTP listen address")
	kvAddrs := flag.String("kv", "http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003", "comma-separated kv server URLs")
	flag.Parse()

	ctl := controller.New(controller.ParseKVAddrs(*kvAddrs))
	srv := controller.StartHTTPServer(ctl, *addr)

	log.Printf("controller listening on http://127.0.0.1%s", *addr)
	log.Printf("  POST /workers/register")
	log.Printf("  POST /workers/heartbeat")
	log.Printf("  GET  /workers")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	log.Println("controller stopped")
}
