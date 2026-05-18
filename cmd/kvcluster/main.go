package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"infra/kvraft/runtime"
)

func main() {
	n := flag.Int("n", 3, "number of raft kv nodes")
	basePort := flag.Int("port", 8001, "first HTTP port")
	maxState := flag.Int("maxraftstate", 1000, "snapshot threshold; -1 disables")
	flag.Parse()

	addrs := make([]string, *n)
	for i := 0; i < *n; i++ {
		addrs[i] = fmt.Sprintf(":%d", *basePort+i)
	}

	cluster, err := runtime.NewCluster(*n, addrs, *maxState)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("kv cluster started (%d nodes)", *n)
	for i, a := range addrs {
		log.Printf("  node %d: http://127.0.0.1%s/kv", i, a)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	cluster.Shutdown()
	log.Println("kv cluster stopped")
}
