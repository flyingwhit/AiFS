package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"infra/gateway"
	"infra/internal/logx"
)

func main() {
	addr := flag.String("addr", ":8080", "gateway HTTP listen address")
	controllerURL := flag.String("controller", "http://127.0.0.1:9000", "controller base URL")
	connPool := flag.Int("conn-pool", 32, "layer-1 connection worker pool size")
	dispatchPool := flag.Int("dispatch-pool", 32, "layer-2 dispatch worker pool size")
	flag.Parse()

	gw := gateway.New(*controllerURL, *connPool, *dispatchPool)
	srv := gateway.StartHTTPServer(gw, *addr)

	logx.Info("Gateway", "ready | addr=%s", *addr)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	logx.Info("Gateway", "stopped")
}
