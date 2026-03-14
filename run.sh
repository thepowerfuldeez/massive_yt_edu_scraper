#!/bin/bash
set -euo pipefail
# =================================================================
# run.sh — Master launcher for the full transcription pipeline
#
# Components launched:
#   1. Downloader      — N threads downloading audio to audio_queue/
#   2. Transcribers    — 8x Qwen3-ASR workers (one per GPU, offline vLLM)
#   3. Discovery       — CC channel crawl + YouTube Data API discovery
#
# Usage:
#   ./run.sh                 # launch everything
#   ./run.sh download        # downloader only
#   ./run.sh transcribe      # transcribers only
#   ./run.sh discover        # discovery only
#   ./run.sh status          # show status
#   ./run.sh stop            # kill all workers
# =================================================================

cd "$(dirname "$0")"
REPO="$(pwd)"

# Load env vars (.env has YT_API_KEY etc)
if [ -f .env ]; then
    set -a; source .env; set +a
fi

# --- Config ---
NUM_GPUS=8
BATCH_SIZE=8
MAX_TOKENS=22800
DL_THREADS=13
DL_MAX_QUEUE=1000
PYTHON="${REPO}/venv/bin/python3"
LOGDIR=/tmp

# --- Helpers ---
log() { echo "[run.sh] $(date +%H:%M:%S) $*"; }

check_venv() {
    if [ ! -x "$PYTHON" ]; then
        log "ERROR: venv not found. Run: uv venv venv --python 3.12 && source venv/bin/activate && uv pip install -e '.' --index-strategy unsafe-best-match"
        exit 1
    fi
}

ensure_dirs() {
    mkdir -p ~/academic_transcriptions/audio_queue
}

# --- Stop ---
stop_all() {
    log "Stopping all workers..."
    pkill -f "src/downloader.py" 2>/dev/null || true
    pkill -f "src/transcribe_qwen.py" 2>/dev/null || true
    pkill -f "src/discover_cc.py" 2>/dev/null || true
    pkill -f "src/discover_cc_api.py" 2>/dev/null || true
    sleep 2
    log "All stopped."
}

# --- Downloader ---
start_downloader() {
    if pgrep -f "src/downloader.py" > /dev/null 2>&1; then
        log "Downloader already running"
        return
    fi
    log "Starting downloader (${DL_THREADS} threads, max_queue=${DL_MAX_QUEUE})..."
    nohup "$PYTHON" src/downloader.py \
        --threads "$DL_THREADS" \
        --max-queue "$DL_MAX_QUEUE" \
        > "${LOGDIR}/downloader.log" 2>&1 &
    log "  PID: $! -> ${LOGDIR}/downloader.log"
}

# --- Transcribers ---
start_transcribers() {
    log "Starting $NUM_GPUS Qwen3-ASR transcribers (batch=${BATCH_SIZE}, max_tokens=${MAX_TOKENS})..."
    for gpu in $(seq 0 $((NUM_GPUS - 1))); do
        if pgrep -f "transcribe_qwen.*CUDA_VISIBLE_DEVICES=$gpu" > /dev/null 2>&1; then
            log "  GPU $gpu already running"
            continue
        fi
        log "  Starting GPU $gpu..."
        CUDA_VISIBLE_DEVICES=$gpu nohup "$PYTHON" src/transcribe_qwen.py \
            --batch-size "$BATCH_SIZE" \
            --max-tokens "$MAX_TOKENS" \
            > "${LOGDIR}/transcriber_gpu${gpu}.log" 2>&1 &
        log "  PID: $! -> ${LOGDIR}/transcriber_gpu${gpu}.log"
        # Stagger starts to avoid thundering herd on model download
        sleep 5
    done
    log "All transcribers launched."
}

# --- Discovery ---
start_discovery() {
    if pgrep -f "src/discover_cc.py" > /dev/null 2>&1; then
        log "CC discovery already running"
    else
        log "Starting CC discovery crawl..."
        nohup "$PYTHON" -u src/discover_cc.py > "${LOGDIR}/discover_cc.log" 2>&1 &
        log "  PID: $! -> ${LOGDIR}/discover_cc.log"
    fi

    if pgrep -f "src/discover_cc_api.py" > /dev/null 2>&1; then
        log "CC API discovery already running"
    else
        log "Starting CC API discovery..."
        nohup "$PYTHON" -u src/discover_cc_api.py > "${LOGDIR}/discover_cc_api.log" 2>&1 &
        log "  PID: $! -> ${LOGDIR}/discover_cc_api.log"
    fi
}

# --- Status ---
show_status() {
    echo "============================================================"
    echo "Pipeline Status"
    echo "============================================================"

    # Processes
    echo ""
    echo "--- Processes ---"
    if pgrep -f "src/downloader.py" > /dev/null 2>&1; then echo "  Downloader: RUNNING"; else echo "  Downloader: STOPPED"; fi
    for gpu in $(seq 0 $((NUM_GPUS - 1))); do
        # Check both log freshness and actual process existence
        logf="${LOGDIR}/transcriber_gpu${gpu}.log"
        alive=false
        if [ -f "$logf" ]; then
            age=$(( $(date +%s) - $(stat -c %Y "$logf" 2>/dev/null || echo 0) ))
            if [ "$age" -lt 300 ]; then
                alive=true
            fi
        fi
        # Also check process table
        if pgrep -f "CUDA_VISIBLE_DEVICES=$gpu.*transcribe_qwen" > /dev/null 2>&1; then
            alive=true
        fi
        # Check nvidia-smi for GPU memory usage (model loaded = ~120GB)
        mem=$(nvidia-smi --query-gpu=memory.used --format=csv,noheader,nounits -i $gpu 2>/dev/null | tr -d ' ')
        if [ -n "$mem" ] && [ "$mem" -gt 50000 ] 2>/dev/null; then
            alive=true
        fi
        if [ "$alive" = true ]; then
            echo "  Transcriber GPU $gpu: RUNNING"
        else
            echo "  Transcriber GPU $gpu: STOPPED"
        fi
    done
    pgrep -f "src/discover_cc" > /dev/null 2>&1 && echo "  Discovery: RUNNING" || echo "  Discovery: STOPPED"

    # DB stats
    echo ""
    echo "--- Database ---"
    "$PYTHON" -c "
import sqlite3, os, glob
db = sqlite3.connect(os.path.expanduser('~/academic_transcriptions/massive_production.db'), timeout=30)
db.execute('PRAGMA journal_mode=WAL')
for r in db.execute('SELECT status, COUNT(*) FROM videos GROUP BY status ORDER BY COUNT(*) DESC'):
    print(f'  {r[0]}: {r[1]:,}')
r = db.execute('SELECT COUNT(*), COALESCE(SUM(duration_seconds)/3600.0,0) FROM videos WHERE status=\"completed\"').fetchone()
print(f'  => {r[0]:,} completed ({r[1]:.0f} audio hrs)')
r = db.execute('SELECT COUNT(*), COALESCE(SUM(duration_seconds)/3600.0,0) FROM videos WHERE status=\"completed\" AND completed_at > datetime(\"now\", \"-1 hour\")').fetchone()
print(f'  => Last hour: {r[0]} completed ({r[1]:.0f} audio hrs)')
q = len(glob.glob(os.path.expanduser('~/academic_transcriptions/audio_queue/*')))
print(f'  => Audio queue: {q} files')
" 2>/dev/null

    # GPU
    echo ""
    echo "--- GPUs ---"
    nvidia-smi --query-gpu=index,utilization.gpu,memory.used,memory.total --format=csv,noheader 2>/dev/null || echo "  nvidia-smi unavailable"

    # Recent logs
    echo ""
    echo "--- Recent transcriber output ---"
    for gpu in $(seq 0 $((NUM_GPUS - 1))); do
        last=$(grep "\[batch\]" "${LOGDIR}/transcriber_gpu${gpu}.log" 2>/dev/null | tail -1)
        if [ -n "$last" ]; then
            echo "  GPU $gpu: $last"
        fi
    done

    echo ""
    echo "--- Recent downloader output ---"
    grep "\[T" "${LOGDIR}/downloader.log" 2>/dev/null | grep "#" | tail -3 || echo "  (none)"
}

# --- Main ---
check_venv
ensure_dirs

CMD="${1:-all}"
case "$CMD" in
    all)
        start_downloader
        sleep 3
        start_transcribers
        sleep 3
        start_discovery
        echo ""
        log "All components launched. Run './run.sh status' to monitor."
        ;;
    download)   start_downloader ;;
    transcribe) start_transcribers ;;
    discover)   start_discovery ;;
    status)     show_status ;;
    stop)       stop_all ;;
    *)
        echo "Usage: $0 {all|download|transcribe|discover|status|stop}"
        exit 1
        ;;
esac
