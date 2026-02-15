# massive_yt_edu_scraper

Autonomous system to build a 100B+ token educational dataset from YouTube using multi-GPU distil-whisper.

**Live dataset**: [thepowerfuldeez/massive-yt-edu-transcriptions](https://huggingface.co/datasets/thepowerfuldeez/massive-yt-edu-transcriptions) (updated daily)

## Current Stats

| Metric | Value |
|--------|-------|
| Videos transcribed | 3,000+ (growing) |
| Audio hours | 1,850+ |
| Estimated tokens | ~25M+ |
| Videos in queue | 370K+ (growing) |
| Throughput | **~14,400 hours/day** on 4 GPUs |
| Avg speed per GPU | 50-76x realtime |

## Architecture

```
┌─────────────────┐     ┌──────────┐     ┌──────────────────┐
│  Discovery       │────▶│  SQLite  │◀────│  GPU Workers     │
│                  │     │  Queue   │     │                  │
│ • search queries │     │ UNIQUE   │     │ • 2 prefetch DL  │
│ • channel crawl  │     │ video_id │     │ • chunk batched  │
│ • related videos │     │ deduped  │     │ • 157x realtime  │
│ • playlist walk  │     │ quality  │     │ • per-GPU process│
│ • 15+ languages  │     │ filtered │     │                  │
└─────────────────┘     └──────────┘     └──────────────────┘
```

- **Pipelined GPU workers**: 4 download threads prefetch ahead per GPU, pre-load audio as numpy arrays — GPUs never idle
- **SQLite single source of truth**: database IS the queue, deduplicated by video_id, batch claims
- **distil-whisper/distil-large-v3.5** via HuggingFace transformers pipeline
- **Batched chunked inference** (`chunk_length_s=30`, `return_timestamps=True`)
- **Quality filtering**: 40+ reject categories, 3-tier educational priority boosting

## Performance

With **4 GPUs** (2× RTX 5090 + 2× RTX 4090), the system achieves:

- **~14,400 hours of audio transcribed per day**
- 50-76x realtime per GPU on long-form content
- Download-pipelined: 4 prefetch threads per GPU, numpy passthrough eliminates file I/O
- Estimated **1B tokens per week** at current throughput

| GPU | VRAM | Batch Size | Speed |
|-----|------|-----------|-------|
| RTX 5090 (×2) | 32GB | 32 | 51-76x realtime |
| RTX 4090 (×2) | 24GB | 24 | 45-72x realtime |

## Quality Filtering

Three-layer filter ensures only educational content enters the dataset:

### Reject Filter (~40 categories)
Chinese/Korean drama (短剧, 总裁, mafia boss CEO romance), music videos, ASMR, gaming (gameplay, let's play, speedrun), lifestyle vlogs (routines, hauls, GRWM), pranks/challenges, clickbait, religious preaching, sports highlights, mukbang, conspiracy content, kids non-educational, low-quality compilations.

### Educational Priority Boost (3 tiers)
- **P9** (highest): University numbered courses (MIT 6.006, CS229), MOOC brands (NPTEL, Coursera, OCW), academic conferences (ICML, NeurIPS, CVPR), foreign university markers (МФТИ, Vorlesung, 講義)
- **P8**: Lecture/course/tutorial keywords, known educational creators (3Blue1Brown, Numberphile, StatQuest, Professor Leonard), subject-specific terms (machine learning, thermodynamics, linear algebra)
- **P7**: Documentaries, explainers, deep dives, webinars

### Duration Gate
Minimum 15 minutes. Filters out shorts, clips, trailers, and low-depth content.

## Discovery Strategies

Multiple concurrent strategies to scale from 100K → 10M+ videos:

1. **Search saturation**: Subject × university × language × modifier combinatorial queries
2. **Channel exhaustion**: Crawl all videos from 140+ verified educational channels
3. **Related video chaining**: Use completed videos as seeds → search similar titles → exponential growth
4. **Playlist walking**: Scrape video pages for playlist links → crawl full playlists (50-500 videos each)
5. **Channel discovery**: Find new channels from video metadata → crawl entire channel
6. **Multi-language**: 15+ languages (English, Russian, German, French, Spanish, Portuguese, Japanese, Korean, Chinese, Arabic, Hindi, Polish, Turkish, Dutch, Swedish)

## Scaling to 100B Tokens

### GPU Requirements

```
100B tokens = 8.3M hours of audio
Per GPU: ~160x realtime = 3,840 hours/day

  1 week:   309 GPUs    (any ≥6GB VRAM)
  2 weeks:  155 GPUs
  1 month:   73 GPUs
  3 months:  25 GPUs
```

### Multi-Node Deployment

```bash
# Node 1 (master): discovery + workers + SQLite
python3 src/discover_related.py &
bash launch.sh

# Node 2-N: mount DB via NFS, run workers only
# For >10 nodes, swap SQLite for PostgreSQL
DB_PATH=/mnt/shared/data.db bash launch.sh "0 1 2 3 4 5 6 7"
```

### Cloud Scaling (fastest to 100B)
- 80× spot instances with 4× A10G each = 320 GPUs
- ~$0.50/hr per A10G → $160/hr total
- 8.3M hours / (320 × 160x) / 24 = ~7 days
- **Cost: ~$27K for 100B tokens**

## Quick Start

```bash
pip install torch transformers accelerate soundfile

# Download yt-dlp
curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o yt-dlp && chmod +x yt-dlp

# Discover videos
python3 src/discover.py              # search queries
python3 src/discover_channels.py     # channel crawling
python3 src/discover_related.py      # related + playlist walking

# Launch GPU workers (sequential model loading)
bash launch.sh                       # all GPUs
bash launch.sh "0 1"                 # specific GPUs

# Monitor progress
python3 monitor.py

# Export to HuggingFace
python3 src/export_hf.py
```

## Output Format

```json
{
  "video_id": "dQw4w9WgXcQ",
  "title": "MIT 6.006 Lecture 1: Algorithms and Computation",
  "transcript": "Welcome to 6.006. Today we're going to talk about...",
  "duration_seconds": 3600,
  "source": "MIT OpenCourseWare",
  "priority": 9,
  "speed_ratio": 165.3
}
```

## Components

| File | Purpose |
|------|---------|
| `src/worker.py` | Single GPU worker with pipelined prefetch |
| `src/quality_filter.py` | Comprehensive reject + priority filter |
| `src/discover.py` | Search-based discovery (subject × modifier) |
| `src/discover_channels.py` | Channel crawling (140+ edu channels) |
| `src/discover_related.py` | Related videos + playlist walking |
| `src/discover_10M.py` | Aggressive scaling (all strategies combined) |
| `src/export_hf.py` | Export completed transcripts to HuggingFace |
| `launch.sh` | Launch workers with sequential model loading |
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
    status TEXT DEFAULT 'pending',  -- pending/processing/completed/error/rejected
    transcript TEXT,
    processing_time_seconds REAL,
    speed_ratio REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    priority INTEGER DEFAULT 5      -- 9=university course, 8=lecture, 7=documentary, 5=default
);
```

## License

MIT
