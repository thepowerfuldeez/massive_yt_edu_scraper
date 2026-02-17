# Massive YouTube Educational Transcription

Large-scale educational video transcription pipeline targeting **1M hours** of YouTube content.

## Stats

| Metric | Value |
|--------|-------|
| **Completed** | 15,500+ transcriptions (~9,000+ audio hours) |
| **Tokens** | ~108M+ estimated (targeting 10B+) |
| **Queue** | 1.86M+ pending videos |
| **Throughput** | ~700 audio hours per wall-clock hour |
| **GPUs** | 2× RTX 5090 + 2× RTX 4090 |

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│  5 Discovery │────▶│  SQLite DB   │────▶│  4 GPU       │
│  Crawlers    │     │  (queue)     │     │  Workers     │
└─────────────┘     └──────────────┘     └──────────────┘
                           │                     │
                    ┌──────┴──────┐        ┌─────┴─────┐
                    │ Quality     │        │ faster-   │
                    │ Filter      │        │ whisper   │
                    │ (~40 rules) │        │ CTranslate2│
                    └─────────────┘        └───────────┘
```

### Pipeline

1. **Discovery** — 5 concurrent crawlers find educational videos via related videos, search, playlists, channels
2. **Quality Filter** — ~40 reject categories (gaming, music, vlogs, etc.), 3-tier priority (P9 university courses > P8 lectures > P5 default)
3. **Duration Filter** — ≥15 minutes only (deep educational content)
4. **Transcription** — faster-whisper (CTranslate2) with distil-large-v3.5, 1.2× audio speedup, no VAD
5. **Export** — Daily push to HuggingFace dataset

### Key Optimizations

- **faster-whisper over HF pipeline**: 3× faster (CTranslate2 fused kernels, 2.5GB VRAM vs 6-8GB)
- **1.2× audio speedup**: yt-dlp atempo filter, 17% less audio, negligible quality loss
- **No VAD**: benchmarked Silero VAD — adds overhead on dense lectures
- **beam_size=1, condition_on_previous_text=False**: max speed for batch workload
- **6 prefetch threads per GPU**: download pipeline stays saturated
- **SQLite queue**: atomic `UPDATE...RETURNING` claims, WAL mode, batch claims
- **JS runtime for yt-dlp**: `--js-runtimes node` required since Feb 2026 YouTube changes

### Performance (per GPU)

| GPU | Avg Speed | VRAM |
|-----|-----------|------|
| RTX 5090 | ~200× realtime | 2.5 GB |
| RTX 4090 | ~180× realtime | 2.4 GB |

## Structure

```
├── launch.sh              # Start 4 GPU workers
├── launch_discovery.sh    # Start 5 discovery crawlers
└── src/
    ├── worker.py           # GPU transcription worker (faster-whisper)
    ├── discover_related.py # Related video crawler
    ├── discover_10M.py     # Search-based discovery
    ├── scale_to_1M.py      # Playlist/channel scaling
    ├── discover_aggressive.py  # High-yield discovery
    ├── discover_mega.py    # Non-English + niche academic
    ├── quality_filter.py   # Educational content filter
    ├── export_hf.py        # HuggingFace dataset export
    ├── monitor.py          # System monitor
    └── aggregate_discovery.py  # Discovery aggregation
```

## Dataset

Published on HuggingFace: [`thepowerfuldeez/massive-yt-edu-transcriptions`](https://huggingface.co/datasets/thepowerfuldeez/massive-yt-edu-transcriptions)

Daily auto-push at 6am London.

## Requirements

- Python 3.10+
- faster-whisper, ctranslate2
- yt-dlp (with node.js for JS runtime)
- ffmpeg, librosa, numpy

## Usage

```bash
# Start transcription
./launch.sh

# Start discovery crawlers
./launch_discovery.sh

# Export to HuggingFace
python3 src/export_hf.py
```
