#!/bin/bash
# Launch 8 Qwen3-ASR transcription workers (one per GPU, offline mode)
# Each worker loads its own vLLM instance on a single GPU
#
# Usage: ./launch_transcribers.sh [batch_size]

cd "$(dirname "$0")"
BATCH=${1:-8}
PYTHON=./venv/bin/python3
MAX_TOKENS=22800

echo "============================================================"
echo "Launching 8 Qwen3-ASR workers (offline, 1 per GPU)"
echo "  Batch size: $BATCH"
echo "  Max tokens: $MAX_TOKENS"
echo "============================================================"

for gpu in $(seq 0 7); do
    echo "Starting GPU $gpu..."
    CUDA_VISIBLE_DEVICES=$gpu nohup $PYTHON src/transcribe_qwen.py \
        --batch-size "$BATCH" \
        --max-tokens "$MAX_TOKENS" \
        > /tmp/transcriber_gpu${gpu}.log 2>&1 &
    echo "  PID: $! -> /tmp/transcriber_gpu${gpu}.log"
    # Stagger starts to avoid thundering herd on model download
    sleep 5
done

echo ""
echo "All workers launched. Monitor with:"
echo "  tail -f /tmp/transcriber_gpu*.log"
echo "  grep 'batch' /tmp/transcriber_gpu*.log"
