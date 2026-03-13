#!/bin/bash
# Watchdog: monitor downloader + 8 Qwen3-ASR transcription workers.
# Restarts dead processes, resets stale DB entries, reports stats.
cd "$(dirname "$0")"

NUM_GPUS=8
PYTHON=./venv/bin/python3
BATCH_SIZE=8
MAX_TOKENS=22800

# --- Check downloader ---
if ! pgrep -f "src/downloader.py" > /dev/null 2>&1; then
    echo "[watchdog] Downloader DEAD — restarting..."
    nohup $PYTHON src/downloader.py --threads 16 --max-queue 1000 > /tmp/downloader.log 2>&1 &
    sleep 5
fi

# --- Check transcribers ---
for gpu in $(seq 0 $((NUM_GPUS - 1))); do
    log="/tmp/transcriber_gpu${gpu}.log"
    alive=false

    if [ -f "$log" ]; then
        age=$(( $(date +%s) - $(stat -c %Y "$log" 2>/dev/null || echo 0) ))
        if [ "$age" -lt 120 ]; then
            alive=true
        fi
    fi

    if [ "$alive" = false ]; then
        echo "[watchdog] Transcriber GPU $gpu DEAD — restarting..."
        CUDA_VISIBLE_DEVICES=$gpu nohup $PYTHON src/transcribe_qwen.py \
            --batch-size "$BATCH_SIZE" --max-tokens "$MAX_TOKENS" \
            > /tmp/transcriber_gpu${gpu}.log 2>&1 &
        sleep 5
    fi
done

# --- Reset stale DB entries ---
$PYTHON -c "
import sqlite3, os, glob
db = sqlite3.connect(os.path.expanduser('~/academic_transcriptions/massive_production.db'), timeout=30)
db.execute('PRAGMA journal_mode=WAL')

# Reset stale processing (downloader claimed but died)
n1 = db.execute(\"UPDATE videos SET status='pending' WHERE status='processing' AND processing_started_at < datetime('now', '-30 minutes')\").rowcount
# Reset stale transcribing (transcriber claimed but died)
n2 = db.execute(\"UPDATE videos SET status='downloaded' WHERE status='transcribing' AND processing_started_at < datetime('now', '-30 minutes')\").rowcount
if n1: print(f'[watchdog] Reset {n1} stale processing -> pending')
if n2: print(f'[watchdog] Reset {n2} stale transcribing -> downloaded')
db.commit()

# Quick stats
for r in db.execute('SELECT status, COUNT(*) FROM videos GROUP BY status ORDER BY COUNT(*) DESC'):
    print(f'  {r[0]}: {r[1]:,}')

# Recent throughput
r = db.execute('''SELECT COUNT(*), COALESCE(SUM(duration_seconds)/3600.0, 0)
    FROM videos WHERE status=\"completed\" AND completed_at > datetime(\"now\", \"-1 hour\")''').fetchone()
print(f'  Last hour: {r[0]} completed, {r[1]:.0f} audio hrs')

# Audio queue size
q = len(glob.glob(os.path.expanduser('~/academic_transcriptions/audio_queue/*')))
print(f'  Audio queue: {q} files')
"

# --- GPU status ---
echo "[watchdog] GPU status:"
nvidia-smi --query-gpu=index,utilization.gpu,memory.used --format=csv,noheader

# --- Latest from each transcriber ---
echo "[watchdog] Transcriber logs:"
for gpu in $(seq 0 $((NUM_GPUS - 1))); do
    last=$(grep "\[batch\]" /tmp/transcriber_gpu${gpu}.log 2>/dev/null | tail -1)
    if [ -n "$last" ]; then
        echo "  GPU $gpu: $last"
    else
        echo "  GPU $gpu: no batches yet"
    fi
done

# --- Downloader stats ---
echo "[watchdog] Downloader:"
last=$(grep "\[T" /tmp/downloader.log 2>/dev/null | grep "#" | tail -1)
echo "  $last"
