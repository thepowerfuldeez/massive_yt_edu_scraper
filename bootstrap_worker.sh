#!/bin/bash
set -euo pipefail
# =================================================================
# bootstrap_worker.sh — Run on each worker dev pod to start pipeline.
#
# Pulls shard DB + proxies from S3, installs deps, launches
# downloader + 8x GPU transcribers. Exports parquet on completion.
#
# Usage:
#   curl -sL <raw-url>/bootstrap_worker.sh | bash -s <worker_id>
#   # OR
#   ./bootstrap_worker.sh <worker_id>          # 0-6
#   ./bootstrap_worker.sh <worker_id> status
#   ./bootstrap_worker.sh <worker_id> stop
#   ./bootstrap_worker.sh <worker_id> export   # export parquet + upload to S3
# =================================================================

WORKER_ID="${1:?Usage: $0 <worker_id> [status|stop|export]}"
CMD="${2:-run}"

S3_BUCKET="poolside-dev-pods"
S3_PREFIX="yt-edu"
REPO_URL="git@github.com:thepowerfuldeez/massive_yt_edu_scraper.git"

REPO="/persistent/massive_yt_edu_scraper"
WORKDIR="/persistent/worker_${WORKER_ID}"
DB_PATH="${WORKDIR}/massive_production.db"
QUEUE_DIR="${WORKDIR}/audio_queue"
EXPORT_DIR="${WORKDIR}/exports"
PYTHON="${REPO}/venv/bin/python3"
LOGDIR="/tmp/worker_${WORKER_ID}"

NUM_GPUS=8
BATCH_SIZE=8
MAX_TOKENS=22800
DL_THREADS=12
DL_MAX_QUEUE=1000

log() { echo "[worker ${WORKER_ID}] $(date +%H:%M:%S) $*"; }

# ---- Setup ----
setup() {
    log "=== Setting up worker ${WORKER_ID} ==="

    # 1. Clone repo if needed
    if [ ! -d "$REPO" ]; then
        log "Cloning repo..."
        git clone "$REPO_URL" "$REPO"
    fi
    cd "$REPO"

    # 2. Create venv + install deps
    if [ ! -x "$PYTHON" ]; then
        log "Creating venv and installing deps..."
        uv venv venv --python 3.12
        uv pip install -e "." --python venv/bin/python3 --index-strategy unsafe-best-match
    fi

    # 3. System deps
    which ffmpeg > /dev/null 2>&1 || (log "Installing ffmpeg..." && apt-get install -y ffmpeg > /dev/null 2>&1)

    # 4. Create workdir
    mkdir -p "$WORKDIR" "$QUEUE_DIR" "$LOGDIR" "$EXPORT_DIR"

    # 5. Pull shard DB from S3 (only if missing)
    if [ ! -f "$DB_PATH" ]; then
        log "Pulling shard DB from S3..."
        aws s3 cp "s3://${S3_BUCKET}/${S3_PREFIX}/worker_${WORKER_ID}/massive_production.db" "$DB_PATH"
    fi

    # 6. Always sync proxies + cookies from S3 (may be updated)
    log "Syncing proxies + cookies from S3..."
    aws s3 cp "s3://${S3_BUCKET}/${S3_PREFIX}/worker_${WORKER_ID}/proxy_pool.txt" "${WORKDIR}/proxy_pool.txt" --quiet
    mkdir -p "${WORKDIR}/cookie_pool"
    aws s3 sync "s3://${S3_BUCKET}/${S3_PREFIX}/worker_${WORKER_ID}/cookie_pool/" "${WORKDIR}/cookie_pool/" --quiet

    # 7. Symlink yt-dlp
    ln -sf "${REPO}/venv/bin/yt-dlp" "${WORKDIR}/yt-dlp"

    # 8. Pull model weights from S3 (avoids HF rate limits)
    MODEL_CACHE="${HOME}/.cache/huggingface/hub/models--Qwen--Qwen3-ASR-1.7B"
    if [ ! -d "$MODEL_CACHE/blobs" ] || [ $(du -sm "$MODEL_CACHE" 2>/dev/null | cut -f1) -lt 3000 ]; then
        log "Pulling Qwen3-ASR model from S3..."
        mkdir -p "$MODEL_CACHE"
        aws s3 sync "s3://${S3_BUCKET}/${S3_PREFIX}/models/Qwen3-ASR-1.7B/" "$MODEL_CACHE/" --quiet
        log "Model cache ready ($(du -sh $MODEL_CACHE | cut -f1))"
    fi

    # 8. Show status
    log "Setup complete."
    "$PYTHON" -c "
import sqlite3
db = sqlite3.connect('${DB_PATH}', timeout=30)
n = db.execute('SELECT COUNT(*) FROM videos WHERE status=\"pending\"').fetchone()[0]
print(f'  DB: {n:,} pending videos')
"
    log "Proxies: $(wc -l < ${WORKDIR}/proxy_pool.txt) lines"
}

# ---- Downloader ----
start_downloader() {
    if pgrep -f "src/downloader.py.*--queue-dir.*worker_${WORKER_ID}" > /dev/null 2>&1; then
        log "Downloader already running"; return
    fi
    log "Starting downloader (${DL_THREADS} threads)..."

    WORK_DIR="$WORKDIR" \
    DB_PATH="$DB_PATH" \
    PROXY_FILE="${WORKDIR}/proxy_pool.txt" \
    nohup "$PYTHON" "${REPO}/src/downloader.py" \
        --threads "$DL_THREADS" \
        --max-queue "$DL_MAX_QUEUE" \
        --queue-dir "$QUEUE_DIR" \
        > "${LOGDIR}/downloader.log" 2>&1 &
    log "  PID: $! -> ${LOGDIR}/downloader.log"
}

# ---- Transcribers ----
start_transcribers() {
    log "Starting ${NUM_GPUS} transcribers..."

    for gpu in $(seq 0 $((NUM_GPUS - 1))); do
        mem=$(nvidia-smi --query-gpu=memory.used --format=csv,noheader,nounits -i $gpu 2>/dev/null | tr -d ' ')
        if [ -n "$mem" ] && [ "$mem" -gt 50000 ] 2>/dev/null; then
            log "  GPU $gpu in use, skipping"; continue
        fi
        log "  Starting GPU $gpu..."
        WORK_DIR="$WORKDIR" \
        DB_PATH="$DB_PATH" \
        HF_HUB_OFFLINE=1 \
        TRANSFORMERS_OFFLINE=1 \
        CUDA_VISIBLE_DEVICES=$gpu \
        nohup "$PYTHON" "${REPO}/src/transcribe_qwen.py" \
            --batch-size "$BATCH_SIZE" \
            --max-tokens "$MAX_TOKENS" \
            --queue-dir "$QUEUE_DIR" \
            > "${LOGDIR}/transcriber_gpu${gpu}.log" 2>&1 &
        log "    PID: $!"
        sleep 5
    done
    log "All transcribers launched."
}

# ---- Status ----
show_status() {
    echo "============================================================"
    echo "Worker ${WORKER_ID} Status"
    echo "============================================================"

    "$PYTHON" -c "
import sqlite3, os, glob
db = sqlite3.connect('${DB_PATH}', timeout=30)
db.execute('PRAGMA journal_mode=WAL')
for r in db.execute('SELECT status, COUNT(*) FROM videos GROUP BY status ORDER BY COUNT(*) DESC'):
    print(f'  {r[0]:15s}: {r[1]:>10,}')
r = db.execute('SELECT COUNT(*), COALESCE(SUM(duration_seconds)/3600.0,0) FROM videos WHERE status=\"completed\"').fetchone()
print(f'  => {r[0]:,} completed ({r[1]:.0f} audio hrs)')
q = len(glob.glob('${QUEUE_DIR}/*'))
print(f'  => Audio queue: {q} files')
" 2>/dev/null

    echo ""
    for gpu in $(seq 0 $((NUM_GPUS - 1))); do
        last=$(grep "\[batch\]" "${LOGDIR}/transcriber_gpu${gpu}.log" 2>/dev/null | tail -1)
        [ -n "$last" ] && echo "  GPU $gpu: $last"
    done

    echo ""
    grep "\[T" "${LOGDIR}/downloader.log" 2>/dev/null | grep "#" | tail -2

    echo ""
    nvidia-smi --query-gpu=index,utilization.gpu,memory.used --format=csv,noheader 2>/dev/null
}

# ---- Stop ----
stop_all() {
    log "Stopping worker ${WORKER_ID}..."
    pkill -f "downloader.py.*worker_${WORKER_ID}" 2>/dev/null || true
    pkill -f "transcribe_qwen.py.*worker_${WORKER_ID}" 2>/dev/null || true
    nvidia-smi --query-compute-apps=pid --format=csv,noheader 2>/dev/null | while read pid; do
        kill -9 "$pid" 2>/dev/null || true
    done
    sleep 2
    log "Stopped."
}

# ---- Export ----
do_export() {
    log "Exporting parquet..."
    "$PYTHON" -c "
import os, sys
sys.path.insert(0, '${REPO}/src')
import export_parquet as ep
ep.DB_PATH = '${DB_PATH}'
ep.EXPORT_DIR = '${EXPORT_DIR}'
ep.export()
"
    log "Uploading to S3..."
    aws s3 sync "${EXPORT_DIR}" "s3://${S3_BUCKET}/${S3_PREFIX}/exports/worker_${WORKER_ID}/" --exclude "*.md"
    log "Export complete: s3://${S3_BUCKET}/${S3_PREFIX}/exports/worker_${WORKER_ID}/"
}

# ---- Main ----
case "$CMD" in
    run)
        setup
        start_downloader
        sleep 5
        start_transcribers
        log "Pipeline running. Check: $0 $WORKER_ID status"
        ;;
    status)  show_status ;;
    stop)    stop_all ;;
    export)  do_export ;;
    setup)   setup ;;
    *)
        echo "Usage: $0 <worker_id> [run|status|stop|export]"
        exit 1
        ;;
esac
