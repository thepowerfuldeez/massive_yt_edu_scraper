#!/bin/bash
# Launch vLLM server with Qwen3-ASR-1.7B, DP=8 (one replica per GPU)
# NOTE: For server mode only. Default pipeline uses offline mode (launch_transcribers.sh)
#
# Usage:
#   ./launch_vllm.sh          # DP=8 (default, all 8 GPUs)
#   ./launch_vllm.sh 4        # DP=4
#   ./launch_vllm.sh 1        # Single GPU

cd "$(dirname "$0")"
DP=${1:-8}
PORT=8000
MODEL="Qwen/Qwen3-ASR-1.7B"
# ~380 tokens/min * 60 min max = 22800 tokens
MAX_TOKENS=22800
PYTHON=./venv/bin/python3

echo "============================================================"
echo "Launching vLLM: $MODEL"
echo "  Data parallel: $DP"
echo "  Max model len: $MAX_TOKENS"
echo "  Port: $PORT"
echo "============================================================"

exec $PYTHON -m vllm.entrypoints.openai.api_server \
    --model "$MODEL" \
    --data-parallel-size "$DP" \
    --gpu-memory-utilization 0.85 \
    --max-model-len "$MAX_TOKENS" \
    --host 0.0.0.0 \
    --port "$PORT" \
    --disable-log-requests
