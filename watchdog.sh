#!/bin/bash
# Watchdog: check all 4 GPU workers are alive, restart any that died.
# Also resets stale 'processing' entries and reports stats.
cd ~/million-hour-transcription
source ~/vllm-env/bin/activate

for gpu in 0 1 2 3; do
    if ! pgrep -f "worker.py $gpu" > /dev/null 2>&1; then
        echo "[watchdog] GPU $gpu DEAD — restarting..."
        CUDA_VISIBLE_DEVICES=$gpu nohup python3 -u src/worker.py $gpu > /tmp/gpu_${gpu}.log 2>&1 &
        sleep 10  # let model load
    fi
done

# Reset stale processing (claimed >30min ago, probably from dead workers)
python3 -c "
import sqlite3
db = sqlite3.connect('$HOME/academic_transcriptions/massive_production.db', timeout=30)
n = db.execute(\"UPDATE videos SET status='pending' WHERE status='processing' AND processing_started_at < datetime('now', '-30 minutes')\").rowcount
if n: print(f'[watchdog] Reset {n} stale processing entries')
db.commit()

# Quick stats
for r in db.execute('SELECT status, COUNT(*) FROM videos GROUP BY status ORDER BY COUNT(*) DESC'):
    print(f'  {r[0]}: {r[1]:,}')
"

# Check GPU utilization
echo "[watchdog] GPU status:"
nvidia-smi --query-gpu=index,utilization.gpu,memory.used --format=csv,noheader

# Recent throughput from logs
echo "[watchdog] Latest per-GPU:"
for gpu in 0 1 2 3; do
    last=$(grep "\[GPU $gpu\] #" /tmp/gpu_${gpu}.log 2>/dev/null | tail -1)
    if [ -n "$last" ]; then
        echo "  $last"
    else
        echo "  GPU $gpu: no transcriptions yet"
    fi
done
