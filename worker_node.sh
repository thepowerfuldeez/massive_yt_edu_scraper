#!/bin/bash
set -euo pipefail
# =================================================================
# worker_node.sh — Single script to run a shard on a worker dev pod.
#
# All pods share /persistent. Each pod gets its own:
#   - DB (from shards/)
#   - workdir (audio downloads, tmp files)
#   - proxy_pool.txt
#   - parquet exports
#
# Usage:
#   ./worker_node.sh <shard_id>            # setup + launch (0-6)
#   ./worker_node.sh <shard_id> status     # show status
#   ./worker_node.sh <shard_id> stop       # kill everything
#   ./worker_node.sh <shard_id> export     # export parquet
# =================================================================

SHARD_ID="${1:?Usage: $0 <shard_id> [status|stop|export]}"
CMD="${2:-run}"

REPO="/persistent/poolside/oss/massive_yt_edu_scraper"
WORKDIR="/persistent/worker_${SHARD_ID}"
DB_PATH="${WORKDIR}/massive_production.db"
SHARD_SRC="${REPO}/shards/shard_$(printf '%02d' $SHARD_ID).db"
EXPORT_DIR="${REPO}/exports/shard_${SHARD_ID}"
PYTHON="${REPO}/venv/bin/python3"
LOGDIR="/tmp/worker_${SHARD_ID}"

NUM_GPUS=8
BATCH_SIZE=8
MAX_TOKENS=22800
DL_THREADS=12
DL_MAX_QUEUE=1000

log() { echo "[shard ${SHARD_ID}] $(date +%H:%M:%S) $*"; }

# ---- Setup ----
setup() {
    # 1. Install deps if venv missing
    if [ ! -x "$PYTHON" ]; then
        log "Installing dependencies..."
        cd "$REPO"
        uv venv venv --python 3.12
        uv pip install -e "." --python venv/bin/python3 --index-strategy unsafe-best-match
        log "Dependencies installed."
    fi

    # 2. System deps
    which ffmpeg > /dev/null 2>&1 || apt-get install -y ffmpeg > /dev/null 2>&1

    # 3. Create workdir
    mkdir -p "$WORKDIR/audio_queue"
    mkdir -p "$LOGDIR"

    # 4. Copy shard DB if not already there
    if [ ! -f "$DB_PATH" ]; then
        if [ ! -f "$SHARD_SRC" ]; then
            log "ERROR: Shard DB not found at $SHARD_SRC"
            exit 1
        fi
        log "Copying shard DB..."
        cp "$SHARD_SRC" "$DB_PATH"
        log "DB ready: $(${PYTHON} -c "
import sqlite3
db = sqlite3.connect('${DB_PATH}')
n = db.execute('SELECT COUNT(*) FROM videos WHERE status=\"pending\"').fetchone()[0]
print(f'{n:,} pending videos')
")"
    fi

    # 5. Symlink yt-dlp into workdir
    ln -sf "${REPO}/venv/bin/yt-dlp" "${WORKDIR}/yt-dlp"

    # 6. Check proxy_pool.txt
    if [ ! -f "${WORKDIR}/proxy_pool.txt" ]; then
        log "WARNING: No proxy_pool.txt in ${WORKDIR}/ — place 12 proxies there"
    fi
}

# ---- Launch ----
start_downloader() {
    if pgrep -f "downloader.py.*${DB_PATH}" > /dev/null 2>&1; then
        log "Downloader already running"
        return
    fi
    log "Starting downloader (${DL_THREADS} threads)..."
    DB_PATH="$DB_PATH" \
    WORK_DIR="$WORKDIR" \
    PROXY_FILE="${WORKDIR}/proxy_pool.txt" \
    nohup "$PYTHON" -c "
import os, sys
os.environ['DB_PATH'] = '${DB_PATH}'
# Patch module-level constants before import
import importlib.util
spec = importlib.util.spec_from_file_location('downloader', '${REPO}/src/downloader.py')
mod = importlib.util.module_from_spec(spec)
mod.DB_PATH = '${DB_PATH}'
mod.WORK_DIR = '${WORKDIR}'
mod.QUEUE_DIR = '${WORKDIR}/audio_queue'
mod.YTDLP = '${WORKDIR}/yt-dlp'
mod.PROXY_FILE = '${WORKDIR}/proxy_pool.txt'
mod.COOKIE_POOL_DIR = '${WORKDIR}/cookie_pool'
sys.argv = ['downloader', '--threads', '${DL_THREADS}', '--max-queue', '${DL_MAX_QUEUE}', '--queue-dir', '${WORKDIR}/audio_queue']
spec.loader.exec_module(mod)
" > "${LOGDIR}/downloader.log" 2>&1 &
    log "  PID: $! -> ${LOGDIR}/downloader.log"
}

start_transcribers() {
    log "Starting $NUM_GPUS transcribers..."
    for gpu in $(seq 0 $((NUM_GPUS - 1))); do
        if nvidia-smi --query-gpu=memory.used --format=csv,noheader,nounits -i $gpu 2>/dev/null | awk '{exit ($1 > 50000) ? 0 : 1}'; then
            log "  GPU $gpu already in use, skipping"
            continue
        fi
        log "  Starting GPU $gpu..."
        DB_PATH="$DB_PATH" \
        CUDA_VISIBLE_DEVICES=$gpu nohup "$PYTHON" -c "
import os, sys
os.environ['DB_PATH'] = '${DB_PATH}'
import sqlite3, glob, argparse, traceback, subprocess, json, base64, time
import urllib.request, urllib.error

DB_PATH = '${DB_PATH}'
QUEUE_DIR = '${WORKDIR}/audio_queue'

# Import and patch
sys.path.insert(0, '${REPO}/src')
import transcribe_qwen as tq
tq.DB_PATH = DB_PATH
tq.QUEUE_DIR = QUEUE_DIR

# Override get_db
_orig_get_db = tq.get_db
def patched_get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA busy_timeout=30000')
    return conn
tq.get_db = patched_get_db

sys.argv = ['transcribe_qwen', '--batch-size', '${BATCH_SIZE}', '--max-tokens', '${MAX_TOKENS}', '--queue-dir', QUEUE_DIR]
tq.main()
" > "${LOGDIR}/transcriber_gpu${gpu}.log" 2>&1 &
        log "  PID: $! -> ${LOGDIR}/transcriber_gpu${gpu}.log"
        sleep 5
    done
    log "All transcribers launched."
}

# ---- Stop ----
stop_all() {
    log "Stopping all workers for shard ${SHARD_ID}..."
    pkill -f "downloader.py.*${DB_PATH}" 2>/dev/null || true
    pkill -f "transcribe_qwen.*${DB_PATH}" 2>/dev/null || true
    # Kill any vLLM engine cores on our GPUs
    nvidia-smi --query-compute-apps=pid --format=csv,noheader 2>/dev/null | while read pid; do
        kill -9 "$pid" 2>/dev/null || true
    done
    sleep 2
    log "Stopped."
}

# ---- Status ----
show_status() {
    echo "============================================================"
    echo "Worker Node — Shard ${SHARD_ID}"
    echo "  Workdir: ${WORKDIR}"
    echo "  DB: ${DB_PATH}"
    echo "============================================================"

    "$PYTHON" -c "
import sqlite3, os, glob
db = sqlite3.connect('${DB_PATH}', timeout=30)
db.execute('PRAGMA journal_mode=WAL')
for r in db.execute('SELECT status, COUNT(*) FROM videos GROUP BY status ORDER BY COUNT(*) DESC'):
    print(f'  {r[0]:15s}: {r[1]:>10,}')
r = db.execute('SELECT COUNT(*), COALESCE(SUM(duration_seconds)/3600.0,0) FROM videos WHERE status=\"completed\"').fetchone()
print(f'  => {r[0]:,} completed ({r[1]:.0f} audio hrs)')
q = len(glob.glob('${WORKDIR}/audio_queue/*'))
print(f'  => Audio queue: {q} files')
" 2>/dev/null

    echo ""
    echo "--- Transcriber logs ---"
    for gpu in $(seq 0 $((NUM_GPUS - 1))); do
        last=$(grep "\[batch\]" "${LOGDIR}/transcriber_gpu${gpu}.log" 2>/dev/null | tail -1)
        if [ -n "$last" ]; then
            echo "  GPU $gpu: $last"
        fi
    done

    echo ""
    echo "--- Downloader ---"
    grep "\[T" "${LOGDIR}/downloader.log" 2>/dev/null | grep "#" | tail -2
}

# ---- Export ----
export_parquet() {
    log "Exporting parquet to ${EXPORT_DIR}..."
    mkdir -p "$EXPORT_DIR"
    DB_PATH="$DB_PATH" "$PYTHON" -c "
import os
os.environ['DB_PATH'] = '${DB_PATH}'
import sys
sys.path.insert(0, '${REPO}/src')
import export_parquet as ep
ep.DB_PATH = '${DB_PATH}'
ep.EXPORT_DIR = '${EXPORT_DIR}'
ep.export()
"
    log "Parquet files at: ${EXPORT_DIR}/"
}

# ---- Main ----
cd "$REPO"

case "$CMD" in
    run)
        setup
        start_downloader
        sleep 3
        start_transcribers
        log "All launched. Run: $0 $SHARD_ID status"
        ;;
    status)  show_status ;;
    stop)    stop_all ;;
    export)  export_parquet ;;
    setup)   setup ;;
    *)
        echo "Usage: $0 <shard_id> [run|status|stop|export|setup]"
        exit 1
        ;;
esac
