#!/usr/bin/env bash
# Phase 3 smoke test: kvcluster + controller + worker + gateway chat completion
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

cleanup() {
  [[ -n "${GW_PID:-}" ]] && kill "$GW_PID" 2>/dev/null || true
  [[ -n "${WK_PID:-}" ]] && kill "$WK_PID" 2>/dev/null || true
  [[ -n "${CTL_PID:-}" ]] && kill "$CTL_PID" 2>/dev/null || true
  [[ -n "${KV_PID:-}" ]] && kill "$KV_PID" 2>/dev/null || true
}
trap cleanup EXIT

go build -o /tmp/aifs-kvcluster ./cmd/kvcluster
go build -o /tmp/aifs-controller ./cmd/controller
go build -o /tmp/aifs-worker ./cmd/worker
go build -o /tmp/aifs-gateway ./cmd/gateway

/tmp/aifs-kvcluster -port 8001 >/tmp/aifs-kv.log 2>&1 &
KV_PID=$!
sleep 2

/tmp/aifs-controller -addr :9000 \
  -kv http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003 >/tmp/aifs-ctl.log 2>&1 &
CTL_PID=$!
sleep 1

/tmp/aifs-worker -port 9100 -gpus 2 -interval 2s >/tmp/aifs-wk.log 2>&1 &
WK_PID=$!
sleep 2

/tmp/aifs-gateway -addr :8080 -controller http://127.0.0.1:9000 >/tmp/aifs-gw.log 2>&1 &
GW_PID=$!
sleep 2

echo "==> chat completion via gateway"
RESP=$(curl -sf -X POST http://127.0.0.1:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"hello AIFS"}')
echo "$RESP" | grep -q 'mock-gpu' || { echo "unexpected: $RESP"; exit 1; }
echo "$RESP"
echo "PASS phase3"
