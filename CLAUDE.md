# CLAUDE.md — massive_yt_edu_scraper

## Architecture

Two-stage pipeline on 8x H200:
1. **Downloader** (`src/downloader.py`) — N CPU threads, yt-dlp → `audio_queue/`
2. **Transcribers** (`src/transcribe_qwen.py`) — 8x GPU, Qwen3-ASR-1.7B via offline vLLM (one model per GPU)
3. **Discovery** — CC channel crawl (yt-dlp) + YouTube Data API

SQLite WAL mode DB at `~/academic_transcriptions/massive_production.db`

## Setup (new machine)

```bash
uv venv venv --python 3.12
source venv/bin/activate
uv pip install -e "." --index-strategy unsafe-best-match
apt-get install -y ffmpeg   # needed for webm→opus conversion

# Config files (gitignored):
# - .env: YT_API_KEY=...
```

## Running

```bash
./run.sh                  # launch all: downloader + 8 transcribers + discovery
./run.sh status           # dashboard
./run.sh stop             # kill everything
./run.sh download         # downloader only
./run.sh transcribe       # transcribers only
./run.sh discover         # discovery only
bash watchdog.sh          # health check + restart dead workers (cron every 5m)
```

## S3 Multi-Machine

```bash
python3 src/s3_sync.py push   --bucket <bucket>                # upload DB
python3 src/s3_sync.py pull   --bucket <bucket>                # download DB
python3 src/s3_sync.py split  --bucket <bucket> --count 100000 # split work
python3 src/s3_sync.py merge  --remote-db /path/to/remote.db   # merge results
```

## yt-dlp Flags

Always pass: `--js-runtimes node --remote-components ejs:github --no-check-certificates`

## Download Pipeline

1. yt-dlp downloads low-bitrate audio (`-f "ba[abr<=96]/wa/ba"`) — native opus/webm
2. ffmpeg applies `atempo=1.2` for >30min videos (saves 17% GPU time)
3. Transcriber auto-converts webm/m4a → opus (soundfile can't read webm)

## Database Schema

```sql
CREATE TABLE videos (
  video_id TEXT PRIMARY KEY,
  title TEXT, url TEXT, course TEXT, university TEXT,
  duration_seconds INTEGER, status TEXT DEFAULT 'pending',
  priority INTEGER DEFAULT 5,
  transcript TEXT,
  processing_time_seconds REAL, speed_ratio REAL,
  completed_at DATETIME, processing_started_at DATETIME, error TEXT,
  description TEXT, youtube_license TEXT,
  license_risk TEXT DEFAULT 'yellow',
  content_category TEXT, channel_id TEXT, tags TEXT, published_time TEXT
);
```

Status flow: `pending → processing → downloaded → transcribing → completed/error`

## Key Scripts

| Script | Purpose |
|--------|---------|
| `run.sh` | Master launcher (all components) |
| `src/downloader.py` | Download-only pipeline (N threads) |
| `src/transcribe_qwen.py` | Qwen3-ASR batch transcriber (1 per GPU) |
| `src/worker.py` | Legacy faster-whisper worker |
| `src/s3_sync.py` | S3 push/pull/split/merge |
| `src/discover_cc.py` | CC content discovery (yt-dlp) |
| `src/discover_cc_api.py` | CC discovery (YouTube Data API) |
| `src/quality_filter.py` | 40+ reject patterns, priority scoring |
| `watchdog.sh` | Health check + auto-restart (cron) |

## Monitoring

```bash
./run.sh status                     # full dashboard
tail -f /tmp/transcriber_gpu0.log   # single transcriber
tail -f /tmp/downloader.log         # downloader
nvidia-smi --query-gpu=index,utilization.gpu,memory.used --format=csv,noheader
```

## Performance Notes

- 8x H200 with Qwen3-ASR: ~65-95x realtime per GPU (batch=8)
- Downloader: ~900/hr
- First batch per GPU is slow (CUDA graph capture + compile warmup)
- atempo 1.2x saves 17% GPU time per video
