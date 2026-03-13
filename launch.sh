#!/bin/bash
# Launch faster-whisper workers on all 8 GPUs sequentially
cd /persistent/poolside/oss/massive_yt_edu_scraper
source venv/bin/activate

# CUDA 12 compat libs for CTranslate2 (faster-whisper) on CUDA 13 driver
export LD_LIBRARY_PATH=/usr/local/lib/python3.10/dist-packages/nvidia/cublas/lib:/usr/local/lib/python3.10/dist-packages/nvidia/cudnn/lib:${LD_LIBRARY_PATH:-}

NUM_GPUS=8

# Kill any old workers
pkill -f "worker.py [0-7]" 2>/dev/null || true
pkill -f "gpu_worker" 2>/dev/null || true
sleep 2

# Clean old ready signals
rm -f /tmp/gpu_*_ready

for gpu in $(seq 0 $((NUM_GPUS - 1))); do
    echo "[launch] Starting GPU $gpu..."
    CUDA_VISIBLE_DEVICES=$gpu nohup python3 -u src/worker.py $gpu > /tmp/gpu_${gpu}.log 2>&1 &

    # Wait for model to load before starting next (avoid OOM)
    while [ ! -f /tmp/gpu_${gpu}_ready ]; do
        sleep 2
    done
    echo "[launch] GPU $gpu ready!"
done

echo "[launch] All $NUM_GPUS GPUs running!"
