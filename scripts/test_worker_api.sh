#!/usr/bin/env bash
# End-to-end smoke test: kv cluster -> controller -> worker register -> list workers
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

KV_LOG=/tmp/aifs-kvcluster.log
CTL_LOG=/tmp/aifs-controller.log

cleanup() {
  if [[ -n "${CTL_PID:-}" ]]; then kill "$CTL_PID" 2>/dev/null || true; fi
  if [[ -n "${KV_PID:-}" ]]; then kill "$KV_PID" 2>/dev/null || true; fi
}
trap cleanup EXIT

echo "==> building binaries"
go build -o /tmp/aifs-kvcluster ./cmd/kvcluster
go build -o /tmp/aifs-controller ./cmd/controller
go build -o /tmp/aifs-worker ./cmd/worker

echo "==> starting kv cluster (ports 8001-8003)"
/tmp/aifs-kvcluster -port 8001 >"$KV_LOG" 2>&1 &
KV_PID=$!
sleep 2

echo "==> starting controller (port 9000)"
/tmp/aifs-controller -addr :9000 \
  -kv http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003 >"$CTL_LOG" 2>&1 &
CTL_PID=$!
sleep 1

echo "==> worker registers via controller"
/tmp/aifs-worker -controller http://127.0.0.1:9000 -ip 127.0.0.1 -port 9100 -status idle

echo "==> listing workers"
RESP=$(curl -sf http://127.0.0.1:9000/workers)
echo "$RESP" | grep -q '127.0.0.1' || { echo "worker not found in response: $RESP"; exit 1; }
echo "$RESP" | grep -q '"status":"idle"' || { echo "unexpected status in: $RESP"; exit 1; }

echo "==> direct kv put/get via leader rotation"
PUT_BODY='{"op":"put","key":"test/ping","value":"pong"}'
curl -sf -X POST http://127.0.0.1:8001/kv -H 'Content-Type: application/json' -d "$PUT_BODY" >/dev/null \
  || curl -sf -X POST http://127.0.0.1:8002/kv -H 'Content-Type: application/json' -d "$PUT_BODY" >/dev/null \
  || curl -sf -X POST http://127.0.0.1:8003/kv -H 'Content-Type: application/json' -d "$PUT_BODY" >/dev/null

GET_BODY='{"op":"get","key":"test/ping"}'
VAL=$(curl -sf -X POST http://127.0.0.1:8001/kv -H 'Content-Type: application/json' -d "$GET_BODY" \
  || curl -sf -X POST http://127.0.0.1:8002/kv -H 'Content-Type: application/json' -d "$GET_BODY" \
  || curl -sf -X POST http://127.0.0.1:8003/kv -H 'Content-Type: application/json' -d "$GET_BODY")
echo "$VAL" | grep -q '"value":"pong"' || { echo "kv get failed: $VAL"; exit 1; }

echo "PASS: worker API and kv HTTP path OK"
