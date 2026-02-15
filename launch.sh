#!/bin/bash
# Launch v2 workers on all 4 GPUs sequentially
cd ~/academic_transcriptions
source ~/vllm-env/bin/activate

# Clean old ready signals
rm -f /tmp/gpu_*_ready

for gpu in 0 1 2 3; do
    echo "[launch] Starting GPU $gpu..."
    CUDA_VISIBLE_DEVICES=$gpu nohup python3 -u gpu_worker_v2.py $gpu > /tmp/gpu_${gpu}.log 2>&1 &
    
    # Wait for model to load before starting next (avoid OOM)
    while [ ! -f /tmp/gpu_${gpu}_ready ]; do
        sleep 2
    done
    echo "[launch] GPU $gpu ready!"
done

echo "[launch] All 4 GPUs running!"
