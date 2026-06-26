#!/usr/bin/env bash
# Verify vLLM independently before wiring it into AIFS.
set -euo pipefail

MODEL="${MODEL:-Qwen/Qwen3-0.6B}"
PORT="${PORT:-8000}"
MAX_TOKENS="${MAX_TOKENS:-64}"

mkdir -p "${HOME}/.cache/huggingface"

echo "==> checking host GPU"
nvidia-smi

echo "==> checking Docker GPU runtime"
docker run --rm --gpus all nvidia/cuda:12.4.1-base-ubuntu22.04 nvidia-smi

echo "==> starting vLLM (${MODEL}) on port ${PORT}"
docker run --rm --gpus all \
  -p "${PORT}:8000" \
  --ipc=host \
  -v "${HOME}/.cache/huggingface:/root/.cache/huggingface" \
  -e "HF_TOKEN=${HF_TOKEN:-}" \
  --name aifs-vllm-verify \
  vllm/vllm-openai:latest \
  --model "${MODEL}" \
  --host 0.0.0.0 \
  --port 8000 &

VLLM_PID=$!
cleanup() {
  docker rm -f aifs-vllm-verify >/dev/null 2>&1 || true
  kill "${VLLM_PID}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "==> waiting for /v1/models"
for i in $(seq 1 300); do
  if curl -sf "http://127.0.0.1:${PORT}/v1/models" >/dev/null; then
    break
  fi
  sleep 2
  if [[ "$i" == "300" ]]; then
    echo "vLLM did not become ready in time"
    exit 1
  fi
done

echo "==> sending chat completion"
curl -sf -X POST "http://127.0.0.1:${PORT}/v1/chat/completions" \
  -H 'Content-Type: application/json' \
  -d "{
    \"model\":\"${MODEL}\",
    \"messages\":[{\"role\":\"user\",\"content\":\"hello from AIFS\"}],
    \"max_tokens\":${MAX_TOKENS}
  }"
echo
echo "PASS: vLLM is ready"
