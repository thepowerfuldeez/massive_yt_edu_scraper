# Massive YouTube Educational Transcription

Large-scale educational video transcription pipeline targeting **10M videos / 1M hours** of YouTube content.

## Stats

| Metric | Value |
|--------|-------|
| **Completed** | 20,124 transcriptions (~16,625 audio hours) |
| **Tokens** | ~160M estimated (targeting 10B+) |
| **Queue** | 2.29M videos |
| **Discovery** | 1,150+ channels crawled |
| **Speed** | 200-250x realtime per GPU |
| **Throughput** | ~1,200 videos/hr combined |

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

### Pipeline

```
yt-dlp (cookie pool, 5 accounts) → ffmpeg atempo 1.2x → faster-whisper CTranslate2
```

### GPU Workers (`src/worker.py`)

- **Engine**: faster-whisper (CTranslate2) with distil-whisper/distil-large-v3.5
- **Audio**: 1.2x speedup via yt-dlp atempo filter (17% less to transcribe)
- **Settings**: beam_size=1, no VAD, condition_on_previous_text=False
- **Download**: 2 prefetch threads/GPU, cookie rotation (5 accounts), retry with exponential backoff
- **VRAM**: ~2.5GB per GPU
- **Claiming**: Mixed duration (shortest first for throughput)
- **Safety**: Process group kill for zombie prevention

### Discovery

- `discover_channels_10M.py` — Extract channels from existing videos, crawl full catalogs, snowball via related channels
- `discover_related.py` — Exponential discovery via related videos and playlist walking

### Cookie Rotation

YouTube requires authenticated cookies for bulk downloads. Place Netscape-format cookie files in the data directory:

```
~/academic_transcriptions/
├── cookies_1.txt
├── cookies_2.txt
├── cookies_3.txt
├── cookies_4.txt
└── cookies_5.txt
```

Workers auto-discover `cookies*.txt` and rotate by GPU ID.

## Hardware

| GPU | Model | VRAM | Avg Speed |
|-----|-------|------|-----------|
| 0 | RTX 5090 | 32GB | ~250x |
| 1 | RTX 4090 | 24GB | ~240x |
| 2 | RTX 5090 | 32GB | ~260x |
| 3 | RTX 4090 | 24GB | ~235x |

## Quick Start

```bash
pip install faster-whisper librosa numpy
mkdir -p ~/academic_transcriptions/tmp_gpu{0,1,2,3}
# Place cookies*.txt and yt-dlp binary in ~/academic_transcriptions/

bash launch.sh          # GPU workers
bash launch_discovery.sh # Discovery crawlers
python3 src/export_hf.py # Export to HuggingFace
```

## Dataset

Published on HuggingFace: [`thepowerfuldeez/massive-yt-edu-transcriptions`](https://huggingface.co/datasets/thepowerfuldeez/massive-yt-edu-transcriptions)

## Files

```
├── README.md
├── PROGRESS.md
├── launch.sh
├── launch_discovery.sh
└── src/
    ├── worker.py
    ├── discover_channels_10M.py
    ├── discover_related.py
    ├── export_hf.py
    ├── monitor.py
    └── quality_filter.py
```

## License

MIT
