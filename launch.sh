#!/bin/bash
# Launch faster-whisper workers on all 4 GPUs sequentially
cd ~/million-hour-transcription
source ~/vllm-env/bin/activate

# Kill any old workers
pkill -f "worker.py [0-3]" 2>/dev/null || true
pkill -f "gpu_worker" 2>/dev/null || true
sleep 2

# Clean old ready signals
rm -f /tmp/gpu_*_ready

for gpu in 0 1 2 3; do
    echo "[launch] Starting GPU $gpu..."
    CUDA_VISIBLE_DEVICES=$gpu nohup python3 -u src/worker.py $gpu > /tmp/gpu_${gpu}.log 2>&1 &

    # Wait for model to load before starting next (avoid OOM)
    while [ ! -f /tmp/gpu_${gpu}_ready ]; do
        sleep 2
    done
    echo "[launch] GPU $gpu ready!"
done

echo "[launch] All 4 GPUs running!"
