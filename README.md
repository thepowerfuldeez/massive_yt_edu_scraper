# Massive YouTube Educational Transcription

Autonomous pipeline for transcribing YouTube educational content at scale.
Two-stage architecture: **downloader** (CPU, N threads) feeds **Qwen3-ASR** transcribers (8x GPU, offline vLLM).

## Quick Start (new machine)

```bash
# 1. Clone and install
git clone <repo-url> && cd massive_yt_edu_scraper
uv venv venv --python 3.12
source venv/bin/activate
uv pip install -e "." --index-strategy unsafe-best-match

# 2. Place config files
echo "YT_API_KEY=..." > .env               # for discovery scripts

# 3. Bootstrap or pull DB
python3 src/reconstruct_queue.py           # from HuggingFace datasets
# OR
python3 src/s3_sync.py pull --bucket <bucket>  # from S3

# 4. Launch everything
./run.sh                                   # downloader + 8 transcribers + discovery
./run.sh status                            # monitor
bash watchdog.sh                           # health check + restart dead workers
```

## Architecture

```
SQLite DB (WAL mode) ← single source of truth
├── Downloader (CPU, N threads)
│   ├── atempo 1.2x for >30min videos
│   └── Outputs to ~/academic_transcriptions/audio_queue/
├── Transcribers (8x GPU, Qwen3-ASR-1.7B via offline vLLM)
│   ├── Claims 'downloaded' files from DB
│   ├── Batch inference (sorted by duration)
│   └── Writes transcript to DB, deletes audio
├── Discovery Crawlers
│   ├── CC channel crawl (yt-dlp)
│   └── YouTube Data API (10K units/day, very efficient)
└── S3 Sync (multi-machine)
    ├── push/pull full DB
    ├── split pending work for remote machines
    └── merge completed transcriptions back
```

### Pipeline Flow

```
pending → [downloader] → downloaded → [transcriber] → completed
                                   → error (retry via watchdog)
```

## Requirements

- Python 3.12+, uv
- 8x NVIDIA GPUs (H200/H100/A100 — any CUDA GPU works)
- ffmpeg, ffprobe, node (for yt-dlp JS challenge)

## Installation

```bash
uv venv venv --python 3.12
source venv/bin/activate
uv pip install -e "." --index-strategy unsafe-best-match
```

Installs: torch (CUDA 12.8), vllm, qwen-asr, faster-whisper, yt-dlp, boto3, datasets, etc.

## Configuration


### Environment (`.env`)

```
YT_API_KEY=...   # YouTube Data API key (for discovery scripts)
```

## Running

```bash
# Full pipeline
./run.sh                  # launch all: downloader + transcribers + discovery
./run.sh status           # dashboard
./run.sh stop             # kill everything

# Individual components
./run.sh download         # downloader only
./run.sh transcribe       # transcribers only
./run.sh discover         # discovery only

# Health check (run via cron every 5 min)
bash watchdog.sh

# Monitor
tail -f /tmp/transcriber_gpu0.log
tail -f /tmp/downloader.log
```

### Alternative: vLLM Server Mode

Instead of offline mode (default), run a shared vLLM server:

```bash
./launch_vllm.sh 8                                     # DP=8
python3 src/transcribe_qwen.py --server http://localhost:8000  # API client
```

### Legacy: faster-whisper Workers

The original pipeline using faster-whisper (CTranslate2):

```bash
./launch.sh               # 8x GPU workers with integrated download+transcribe
```

## S3 Sync (Multi-Machine)

```bash
# Push DB checkpoint to S3
python3 src/s3_sync.py push --bucket my-bucket

# Pull latest DB to a new machine
python3 src/s3_sync.py pull --bucket my-bucket

# Split 100K pending videos for a remote machine
python3 src/s3_sync.py split --bucket my-bucket --count 100000

# Merge completed work from remote machine
python3 src/s3_sync.py merge --remote-db /path/to/remote.db
```

## Export

```bash
# Local parquet
python3 src/export_parquet.py

# Push to HuggingFace
python3 src/export_hf.py

# Push queue metadata to HuggingFace
python3 src/export_queue_hf.py
```

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

**Status flow:** `pending → processing → downloaded → transcribing → completed/error`
**Dispatched:** Videos split to a remote machine via `s3_sync.py split`

## Files

```
├── pyproject.toml             # Dependencies (uv)
├── run.sh                     # Master launcher
├── watchdog.sh                # Health check + auto-restart
├── launch_transcribers.sh     # Qwen3-ASR offline workers
├── launch_vllm.sh             # vLLM server mode
├── launch_discovery.sh        # Discovery crawlers
├── launch.sh                  # Legacy faster-whisper workers
├── .env                       # API keys (gitignored)
└── src/
    ├── downloader.py          # Download-only pipeline (N threads)
    ├── transcribe_qwen.py     # Qwen3-ASR batch transcriber
    ├── worker.py              # Legacy faster-whisper worker
    ├── s3_sync.py             # S3 push/pull/split/merge
    ├── discover_cc.py         # CC content discovery (yt-dlp)
    ├── discover_cc_api.py     # CC discovery (YouTube Data API)
    ├── discover_related.py    # Related video walking
    ├── quality_filter.py      # 40+ reject patterns, priority scoring
    ├── batch_license_scan.py  # YouTube API license scanning
    ├── reconstruct_queue.py   # Rebuild DB from HuggingFace datasets
    ├── export_parquet.py      # Export to local parquet
    ├── export_hf.py           # Export + push to HuggingFace
    └── export_queue_hf.py     # Queue metadata export
```

## Content Classification

| Risk | Description |
|------|-------------|
| green | CC-licensed or public domain (NPTEL, Khan, MIT OCW, Yale OYC) |
| yellow | Standard YouTube license, fair use for research |
| orange | Commercial/copyrighted, needs review |
| red | Non-educational — excluded from transcription |

## Performance Notes

- 8x H200: Qwen3-ASR batch transcription throughput TBD (benchmarking)
- Downloader: ~16 concurrent threads, backpressure at 1000 queued files
- Download is fast; transcription is the bottleneck

## Datasets

| Dataset | Description |
|---------|-------------|
| [massive-yt-edu-transcriptions](https://huggingface.co/datasets/thepowerfuldeez/massive-yt-edu-transcriptions) | Full transcripts |
| [massive-yt-edu-queue](https://huggingface.co/datasets/thepowerfuldeez/massive-yt-edu-queue) | 5.6M video metadata |

## License

MIT
