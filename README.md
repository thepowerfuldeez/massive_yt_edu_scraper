# Massive YouTube Educational Transcription

Large-scale educational video transcription pipeline targeting **10M videos / 1M hours** of YouTube content.

## Stats

| Metric | Value |
|--------|-------|
| **Completed** | 15,500+ transcriptions (~13,800 audio hours) |
| **Tokens** | ~152M estimated (targeting 10B+) |
| **Queue** | 1.96M videos (1.83M pending) |
| **Discovery** | 1,150+ channels crawled |
| **Speed** | 220-280x realtime per GPU |

## Architecture

```
┌─────────────────────────────────────────────────┐
│                   SQLite DB (WAL)                │
│  videos table: pending → processing → completed  │
│  Atomic UPDATE...RETURNING for claim/dedup       │
└───────┬──────────────────────────┬──────────────┘
        │                          │
  ┌─────▼──────┐            ┌─────▼──────┐
  │ GPU Workers │            │ Discovery  │
  │ (4x GPUs)  │            │ Crawlers   │
  │             │            │            │
  │ 2 prefetch  │            │ Channel    │
  │ threads ea  │            │ crawler    │
  │ cookie rot  │            │ Related    │
  │ 1.2x speed  │            │ video      │
  └─────────────┘            └────────────┘
```

### GPU Workers (`src/worker.py`)

- **Engine**: faster-whisper (CTranslate2) with distil-large-v3.5
- **Audio**: 1.2x speedup via yt-dlp atempo filter (17% less to transcribe)
- **Settings**: beam_size=1, no VAD, condition_on_previous_text=False
- **Download**: 2 prefetch threads/GPU, cookie rotation, retry with exponential backoff
- **VRAM**: ~2.5GB per GPU

### Discovery

- `discover_channels_10M.py` — Extract channels from existing videos, crawl full catalogs, snowball via related channels. Main engine for scaling to 10M.
- `discover_related.py` — Exponential discovery via related videos and playlist walking from completed videos.

### Cookie Rotation

YouTube requires authenticated cookies for bulk downloads. Place Netscape-format cookie files in the data directory:

```
~/academic_transcriptions/
├── cookies_1.txt      # Account 1
├── cookies_2.txt      # Account 2
├── cookies_3.txt      # Account 3 (recommended: 3-4 for stability)
└── ...
```

Workers auto-discover `cookies*.txt` and rotate by GPU ID. Export cookies from a logged-in browser using extensions like "Get cookies.txt LOCALLY".

**Recommended**: 3-4 accounts for sustainable throughput without rate limiting.

## Hardware

| GPU | Model | VRAM | Avg Speed |
|-----|-------|------|-----------|
| 0 | RTX 5090 | 32GB | ~250x |
| 1 | RTX 4090 | 24GB | ~240x |
| 2 | RTX 5090 | 32GB | ~260x |
| 3 | RTX 4090 | 24GB | ~235x |

## Quick Start

```bash
# Install deps
pip install faster-whisper librosa numpy

# Set up data directory
mkdir -p ~/academic_transcriptions/tmp_gpu{0,1,2,3}
# Place cookies*.txt and yt-dlp binary in ~/academic_transcriptions/

# Launch workers
bash launch.sh

# Launch discovery
bash launch_discovery.sh

# Export to HuggingFace
python3 src/export_hf.py
```

## Dataset

Published on HuggingFace: [`thepowerfuldeez/massive-yt-edu-transcriptions`](https://huggingface.co/datasets/thepowerfuldeez/massive-yt-edu-transcriptions)

Daily auto-push at 6am London via cron.

## Files

```
├── README.md
├── PROGRESS.md              # Detailed progress log
├── launch.sh                # GPU worker launcher (sequential, waits for model load)
├── launch_discovery.sh      # Discovery crawler launcher
└── src/
    ├── worker.py            # GPU transcription worker
    ├── discover_channels_10M.py  # Channel-based discovery
    ├── discover_related.py  # Related video discovery
    ├── export_hf.py         # HuggingFace dataset export
    ├── monitor.py           # System monitoring
    └── quality_filter.py    # Video quality/education filter
```
