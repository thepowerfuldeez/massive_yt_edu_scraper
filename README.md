# massive_yt_edu_scraper

Autonomous system to build a 100B+ token educational dataset from YouTube using multi-GPU distil-whisper transcription.

## Architecture

```
┌─────────────┐     ┌──────────┐     ┌─────────────────┐
│  Discovery   │────▶│  SQLite  │◀────│  GPU Workers    │
│  (yt-dlp)    │     │  Queue   │     │  (distil-whisper)│
│              │     │          │     │                  │
│ • searches   │     │ UNIQUE   │     │ • prefetch DL    │
│ • channels   │     │ video_id │     │ • chunk batched  │
│ • playlists  │     │ deduped  │     │ • 160x realtime  │
└─────────────┘     └──────────┘     └─────────────────┘
```

- **Pipelined GPU workers**: 2 download threads prefetch ahead so GPUs never idle
- **SQLite single source of truth**: database IS the queue, deduplicated by video_id
- **distil-whisper/distil-large-v3** via HuggingFace transformers pipeline
- **Batched chunked inference** (`chunk_length_s=30`, `return_timestamps=True`)

## Current Setup

**Hardware**: 2× RTX 5090 (32GB) + 2× RTX 4090 (24GB)

| GPU | Batch Size | Speed | VRAM Used |
|-----|-----------|-------|-----------|
| RTX 5090 | 24 | ~180x realtime | ~6.7GB |
| RTX 4090 | 16 | ~140x realtime | ~5.8GB |

**Aggregate**: ~640x realtime = **~10 hours of audio transcribed per minute**.

## Quick Start

```bash
pip install torch transformers accelerate soundfile

# Download yt-dlp
curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o yt-dlp && chmod +x yt-dlp

# Discover videos
python3 src/discover.py          # search queries
python3 src/discover_channels.py # channel crawling

# Launch GPU workers
bash launch.sh                   # all GPUs
bash launch.sh "0 1"             # specific GPUs

# Monitor
python3 monitor.py
```

## Scaling to 100B Tokens

### The Math

| Target | Audio Hours | Tokens | Notes |
|--------|-----------|--------|-------|
| 1B tokens | 83K hours | 1B | ~1 week on 4 GPUs |
| 10B tokens | 833K hours | 10B | Needs ~50 GPUs for 1 week |
| 100B tokens | 8.3M hours | 100B | Needs ~500 GPUs for 1 week |

**Assumptions**: ~150 words/min spoken → ~12K tokens/hour. Each GPU does ~160x realtime = ~3,840 hours/day.

### GPU Requirements by Timeframe

```
Target: 100B tokens = 8.3M hours of audio

Per GPU throughput: 160x realtime = 3,840 hours/day

GPUs needed:
  1 week:   8,300,000 / (3,840 × 7)  = 309 GPUs
  2 weeks:  8,300,000 / (3,840 × 14) = 155 GPUs
  1 month:  8,300,000 / (3,840 × 30) = 73 GPUs
  3 months: 8,300,000 / (3,840 × 90) = 25 GPUs
```

Any GPU with ≥6GB VRAM works (model uses ~5-6GB in fp16). Even RTX 3060s at ~80x would contribute.

### Scaling Architecture

For multi-node deployment, the system needs minimal changes:

```
1. SHARED QUEUE (replace SQLite with PostgreSQL or Redis)
   - Each node connects to central queue
   - Atomic claim: UPDATE ... WHERE status='pending' LIMIT 1 RETURNING video_id
   - Zero coordination needed between GPU workers

2. SHARED STORAGE (NFS/S3/GCS for transcripts)
   - Workers write completed transcripts to shared store
   - Or: each node writes to local SQLite, merge at end

3. DEPLOYMENT (one script per node)
   - Copy worker.py + yt-dlp to each node
   - Set DB_URL env var to central queue
   - Run: bash launch.sh "0 1 2 3 5 6 7"  # whatever GPUs available
   - Workers self-register and start pulling from queue

4. DISCOVERY (run on 1 node)
   - Only one discovery process needed
   - Feeds central queue
   - Can run on CPU-only machine
```

**Simplest multi-node approach** (no infra changes):
```bash
# Node 1: Run discovery + workers + SQLite (master)
# Node 2-N: Mount SQLite via NFS, run workers only
# SQLite WAL mode handles concurrent readers fine
# For >10 nodes, switch to PostgreSQL
```

**Cloud scaling** (fastest path to 100B):
```
- Spin up 80× spot instances with 4× A10G each (320 GPUs)
- ~$0.50/hr per A10G spot = $160/hr total
- 8.3M hours / (320 GPUs × 160x) / 24 = ~7 days
- Cost: 7 × 24 × $160 = ~$27K
- With 3090 spot instances: cheaper but slower
```

### Video Discovery at Scale

Current: 170K+ videos discovered. For 8.3M hours we need ~10M+ videos (avg ~50min each).

YouTube has an estimated 800M+ videos. Educational content is maybe 5-10% = 40-80M videos. More than enough.

Discovery strategies that scale:
1. **Related videos chain**: For each completed video, fetch related/recommended → exponential growth
2. **Playlist crawling**: One playlist = 50-500 videos, playlists reference other playlists
3. **Search pagination**: `ytsearch500:` queries across every subject × language × modifier
4. **Channel completions**: Crawl every video from 500+ educational channels
5. **Subtitles as proxy**: Videos with auto-generated English subtitles = spoken English content

### Output Format

Each completed video produces:
```json
{
  "video_id": "dQw4w9WgXcQ",
  "title": "MIT 6.006 Lecture 1: Algorithms and Computation",
  "transcript": "Welcome to 6.006. Today we're going to talk about...",
  "duration_seconds": 3600,
  "speed_ratio": 165.3,
  "source": "MIT OpenCourseWare"
}
```

Export: `sqlite3 data.db "SELECT video_id, transcript FROM videos WHERE status='completed'" > corpus.jsonl`

## Components

| File | Purpose |
|------|---------|
| `src/worker.py` | Single GPU worker with pipelined prefetch |
| `src/discover.py` | Search-based video discovery |
| `src/discover_channels.py` | Channel/playlist crawling (140+ edu channels) |
| `launch.sh` | Launch workers sequentially (avoids RAM OOM) |
| `monitor.py` | Real-time progress monitoring |

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
    status TEXT DEFAULT 'pending',  -- pending/processing/completed/error
    transcript TEXT,
    processing_time_seconds REAL,
    speed_ratio REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    priority INTEGER DEFAULT 5
);
```

## Progress

Live stats from current run (updated periodically):

| Metric | Value |
|--------|-------|
| Videos discovered | 172K+ |
| Videos transcribed | 312 |
| Hours transcribed | 804 |
| Est. tokens | ~9.6M |
| Avg speed | 160x realtime |
| GPUs active | 4 (2×5090 + 2×4090) |

## License

MIT
