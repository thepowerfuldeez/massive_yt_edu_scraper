#!/bin/bash
# Launch faster-whisper workers on all 8 GPUs (legacy — use run.sh for Qwen3-ASR)
cd "$(dirname "$0")"
PYTHON=./venv/bin/python3

# CUDA 12 compat libs for CTranslate2 (faster-whisper) on CUDA 13 driver
export LD_LIBRARY_PATH=/usr/local/lib/python3.10/dist-packages/nvidia/cublas/lib:/usr/local/lib/python3.10/dist-packages/nvidia/cudnn/lib:${LD_LIBRARY_PATH:-}

NUM_GPUS=8

pkill -f "worker.py [0-7]" 2>/dev/null || true
sleep 2
rm -f /tmp/gpu_*_ready

for gpu in $(seq 0 $((NUM_GPUS - 1))); do
    echo "[launch] Starting GPU $gpu..."
    CUDA_VISIBLE_DEVICES=$gpu nohup $PYTHON -u src/worker.py $gpu > /tmp/gpu_${gpu}.log 2>&1 &

    while [ ! -f /tmp/gpu_${gpu}_0_ready ]; do
        sleep 2
    done
    echo "[launch] GPU $gpu ready!"
done

echo "[launch] All $NUM_GPUS GPUs running!"
