# Million Hour Transcription

Autonomous system to transcribe 1M+ hours of YouTube educational audio using multi-GPU distil-whisper.

## Architecture

- **4 GPU workers** with pipelined download (GPUs never idle waiting for downloads)
- **SQLite** as single source of truth — database IS the queue, deduplicated by video_id
- **distil-whisper/distil-large-v3** via HuggingFace transformers pipeline
- **Batched chunked inference** (`chunk_length_s=30`) for 130-200x realtime per GPU

## Hardware

Tested on: 2× RTX 5090 (32GB) + 2× RTX 4090 (24GB) = 113GB VRAM total.

## Quick Start

```bash
# Install deps
pip install torch transformers accelerate soundfile

# Download yt-dlp
curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o yt-dlp && chmod +x yt-dlp

# Initialize database and discover videos
python3 src/discover.py

# Launch all GPU workers
bash launch.sh
```

## Components

| File | Purpose |
|------|---------|
| `src/worker.py` | Single GPU worker with pipelined prefetch |
| `src/discover.py` | YouTube video discovery — searches + channel crawls |
| `launch.sh` | Launches workers sequentially (one GPU at a time to avoid OOM) |
| `monitor.py` | Real-time progress monitoring |

## Performance

| GPU | Batch Size | Speed |
|-----|-----------|-------|
| RTX 5090 | 24 | ~180x realtime |
| RTX 4090 | 16 | ~140x realtime |

Aggregate: ~640x realtime across 4 GPUs = **~10 hours of audio transcribed per minute**.

## Database Schema

```sql
CREATE TABLE videos (
    id INTEGER PRIMARY KEY,
    video_id TEXT UNIQUE,
    title TEXT,
    course TEXT,
    university TEXT,
    url TEXT,
    duration_seconds INTEGER,
    status TEXT DEFAULT 'pending',
    transcript TEXT,
    processing_time_seconds REAL,
    speed_ratio REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    priority INTEGER DEFAULT 5
);
```

## License

MIT
