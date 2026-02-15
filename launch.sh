#!/bin/bash
# Launch pipelined GPU workers with proper CUDA isolation.
# Models load sequentially to avoid RAM OOM, then run in parallel.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Reset stale states
python3 -c "
import sqlite3, os
db = os.path.join('$SCRIPT_DIR', 'data.db')
c = sqlite3.connect(db)
c.execute('PRAGMA journal_mode=WAL')
n = c.execute(\"UPDATE videos SET status='pending' WHERE status IN ('processing','transcribing','downloading') RETURNING video_id\").fetchall()
c.commit()
stats = dict(c.execute('SELECT status,count(*) FROM videos GROUP BY status').fetchall())
print(f'Reset {len(n)} stale. Queue: {stats}')
"

pkill -f 'src/worker.py' 2>/dev/null || true
sleep 1

GPUS="${1:-0 1 2 3}"

for gpu in $GPUS; do
    rm -f /tmp/gpu_${gpu}_ready
    echo "Starting GPU $gpu..."
    CUDA_VISIBLE_DEVICES=$gpu nohup python3 -u src/worker.py $gpu > /tmp/gpu_${gpu}.log 2>&1 &
    echo "  PID: $!"
    for i in $(seq 1 60); do
        [ -f /tmp/gpu_${gpu}_ready ] && echo "  GPU $gpu ready!" && break
        sleep 1
    done
done

echo ""
echo "All workers launched!"
echo "Logs: tail -f /tmp/gpu_*.log"
echo "GPU:  nvidia-smi -l 5"
