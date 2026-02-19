# Massive YouTube Educational Transcription

Autonomous pipeline for transcribing YouTube educational content at scale. Uses faster-whisper on multi-GPU to produce a large open educational text dataset.

## Current Stats

| Metric | Value |
|--------|-------|
| **Transcribed** | 59,344 videos (~35,876 audio hours) |
| **Characters** | 1.54B (~384M tokens) |
| **Queue** | 4.32M pending (4.54M total discovered) |
| **Speed** | 165–185× realtime per GPU |
| **Throughput** | ~550 videos/hr (4 GPUs) |

## Datasets

| Dataset | Description |
|---------|-------------|
| [massive-yt-edu-transcriptions](https://huggingface.co/datasets/thepowerfuldeez/massive-yt-edu-transcriptions) | Full transcripts (daily auto-push) |
| [massive-yt-edu-queue](https://huggingface.co/datasets/thepowerfuldeez/massive-yt-edu-queue) | 4.5M video metadata with content classification + license risk |

## Architecture

```
SQLite DB (WAL mode) ← single source of truth
├── GPU Workers (4×)
│   ├── 2 prefetch threads each (yt-dlp + ffmpeg 1.2× atempo → mp3)
│   └── faster-whisper CTranslate2 (distil-large-v3.5, beam=1, no VAD)
├── Discovery Crawlers
│   ├── Channel crawler (full catalog extraction, snowball via related)
│   ├── Related video walker (playlist + recommendation chains)
│   └── CC-focused discovery (known OCW channels + CC search filters)
├── License Scanner
│   ├── yt-dlp description + license field fetcher
│   └── YouTube Data API v3 batch scanner (50 IDs/request)
└── HuggingFace Export (daily cron)
```

### Key Design Decisions

- **faster-whisper over HF pipeline**: 3.3× faster, 2.5GB VRAM vs 6–8GB (CTranslate2 fused kernels)
- **1.2× audio speedup**: yt-dlp atempo filter — 17% less GPU work, negligible quality loss
- **No VAD**: Silero VAD benchmarked — adds overhead on dense educational lectures
- **beam_size=1**: Max throughput for batch workload
- **SQLite as queue**: Atomic `UPDATE...RETURNING` claims, WAL mode, no external queue service
- **Process group kill**: `start_new_session=True` + `os.killpg()` prevents zombie yt-dlp/ffmpeg
- **Post-processing skipped**: Whisper large-v3 already produces properly punctuated, capitalized text

## Content Classification

Every video is classified by content source and license risk:

| Risk | Count | Description |
|------|-------|-------------|
| 🟢 Green | 129K | CC-licensed or public domain (NPTEL, Khan, MIT OCW, Yale OYC, Taiwan OCW) |
| 🟡 Yellow | 3.99M | Standard YouTube license, fair use for research |
| 🟠 Orange | 300K | Commercial/copyrighted, needs review |
| 🔴 Red | 72K | Non-educational (gaming, music, vlogs) — excluded from transcription |

### Classification Method

1. **Channel/source name matching** — 207K channels classified via pattern matching (universities, conferences, govt agencies, etc.)
2. **Title analysis** — regex for course codes, "Lecture N", conference names, gaming terms
3. **Priority fallback** — P8+ videos assumed educational
4. **CC verification** — YouTube license field + description text mining + publisher website policy checks

### Known CC Sources (~72K videos)

- NPTEL/IIT (~39K) — CC-BY-SA 4.0 (Indian govt funded)
- Taiwan OCW: NTHU + NYCU (~15K) — CC-BY-NC-SA
- Khan Academy (~8.5K) — CC-BY-NC-SA 3.0
- Library of Congress (~5.3K) — Public domain
- MIT OCW — CC-BY-NC-SA 4.0 (verified from website)
- Yale OYC — CC-BY-NC-SA 3.0 (verified from website)

## Quality Filter

Videos must pass a two-stage filter:

1. **Duration**: ≥15 minutes (deep educational content only)
2. **Content**: 40+ reject categories (gaming, music, vlogs, ASMR, pranks, religious sermons, conspiracy, etc.)
3. **Priority boost**: P9 for university courses/conferences, P8 for lectures/edu creators, P7 for docs/explainers

## Hardware

| GPU | Model | VRAM | Avg Speed |
|-----|-------|------|-----------|
| 0 | RTX 5090 | 32GB | ~179× |
| 1 | RTX 5090 | 32GB | ~183× |
| 2 | RTX 4090 | 24GB | ~226× |
| 3 | RTX 4090 | 24GB | ~183× |

~2.5GB VRAM per GPU. Rest available for other workloads.

## Quick Start

```bash
pip install faster-whisper librosa numpy huggingface_hub

# Place yt-dlp binary in ~/academic_transcriptions/

bash launch.sh              # Start 4 GPU workers
bash launch_discovery.sh    # Start discovery crawlers
python3 src/export_hf.py    # Export + push to HuggingFace
```

## Files

```
├── README.md
├── LICENSING_ANALYSIS.md       # Full licensing report with outreach strategy
├── POSTPROCESSING_RESEARCH.md  # Why we skip post-processing
├── launch.sh                   # GPU worker launcher
├── launch_discovery.sh         # Discovery crawler launcher
├── watchdog.sh                 # Health check (runs via cron)
└── src/
    ├── worker.py               # GPU transcription worker
    ├── quality_filter.py       # Content quality/reject patterns
    ├── discover_related.py     # Related video + playlist discovery
    ├── discover_channels_10M.py # Channel-based bulk discovery
    ├── discover_safe.py        # CC-focused safe content discovery
    ├── discover_cc.py          # CC content chain discovery
    ├── fetch_descriptions.py   # Batch description + license fetcher
    ├── batch_license_scan.py   # YouTube Data API license scanner
    ├── export_hf.py            # Transcription dataset export
    ├── export_queue_hf.py      # Queue metadata export
    └── monitor.py              # Real-time progress monitor
```

## Fair Use Analysis

This dataset is built under fair use for ML research:

- **Transformative**: Audio → text, different medium and purpose
- **Factual content**: Educational lectures are factual, not creative works
- **No market substitution**: Text transcripts don't replace video lectures
- **Research purpose**: Dataset for training/evaluating language models

See [LICENSING_ANALYSIS.md](LICENSING_ANALYSIS.md) for the full legal framework and outreach strategy.

## License

MIT
