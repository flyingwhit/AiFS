#!/usr/bin/env bash
# Phase 4 smoke test: AIFS compose stack + vLLM backend.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

MODEL="${MODEL:-Qwen/Qwen3-0.6B}"

wait_for() {
  local name="$1"
  local url="$2"
  local timeout="${3:-300}"
  echo "==> waiting for ${name}: ${url}"
  for i in $(seq 1 "$timeout"); do
    if curl -sf "$url" >/dev/null; then
      echo "==> ${name} ready"
      return 0
    fi
    sleep 1
  done
  echo "${name} not ready after ${timeout}s"
  return 1
}

echo "==> starting compose stack"
docker compose up -d --build

wait_for "vLLM" "http://127.0.0.1:8000/v1/models" 900
wait_for "controller workers" "http://127.0.0.1:9000/workers/best" 180

echo "==> direct Worker -> vLLM"
WORKER_RESP=$(curl -sf -X POST http://127.0.0.1:9100/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"用一句话介绍 AIFS"}')
echo "$WORKER_RESP"
echo "$WORKER_RESP" | grep -q '"reply"' || { echo "worker response missing reply"; exit 1; }

echo "==> Gateway -> Controller -> Worker -> vLLM"
GATEWAY_RESP=$(curl -sf -X POST http://127.0.0.1:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"用一句话介绍 AIFS"}')
echo "$GATEWAY_RESP"
echo "$GATEWAY_RESP" | grep -q '"reply"' || { echo "gateway response missing reply"; exit 1; }

echo "==> OpenAI-style messages through Gateway"
OPENAI_STYLE_RESP=$(curl -sf -X POST http://127.0.0.1:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d "{
    \"model\":\"${MODEL}\",
    \"messages\":[{\"role\":\"user\",\"content\":\"Say hello to AIFS in one short sentence.\"}],
    \"max_tokens\":64
  }")
echo "$OPENAI_STYLE_RESP"
echo "$OPENAI_STYLE_RESP" | grep -q '"reply"' || { echo "OpenAI-style response missing reply"; exit 1; }

echo "PASS: Phase 4 vLLM stack OK"
