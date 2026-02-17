# Progress Log

## 2026-02-17

### Cookie Authentication & Rate Limiting
- YouTube now requires authenticated cookies for downloads ("Sign in to confirm you're not a bot")
- Set up cookie rotation system: worker auto-discovers `cookies*.txt` files in data dir
- GPU workers select cookies via `GPU_ID % num_cookies` for even distribution
- Tested with 2 cookie sets — sustainable at 2 prefetch threads per GPU (8 total concurrent downloads)
- **Recommendation**: 3-4 cookie files for robust rotation without hitting rate limits

### Worker Fixes
- Fixed missing yt-dlp binary (deleted during earlier cleanup) 
- Added retry with exponential backoff on download failures (3 retries, 5-20s backoff)
- Reduced prefetch threads from 6 to 2 per GPU (less YouTube pressure)
- Added 1-3s random sleep between downloads for rate-limit protection
- Cleaned up worker.py: removed debug prints, proper cookie rotation via glob

### Discovery Scaling
- Built channel-based discovery crawler (`discover_channels_10M.py`) for 10M target
- Strategy: extract channels from existing videos → crawl full channel catalogs → snowball via related channels
- +570K new videos overnight from channel crawling (1.28M → 1.96M)
- Removed saturated search-based crawlers (discover_10M, discover_aggressive, discover_mega, scale_to_1M)
- Active crawlers: `discover_channels_10M.py` (channel catalog crawling) + `discover_related.py` (exponential related-video chaining)

### Repo Cleanup
- Consolidated to 7 source files (worker, 2 discovery, export, monitor, quality_filter)
- Removed redundant discovery scripts and aggregation code
- Updated launch scripts for new crawler set

### Stats
- **Completed**: ~15,500 transcriptions (13,800+ audio hours, ~152M tokens)
- **Queue**: 1.96M total (1.83M pending)
- **Discovery**: 1,150+ channels crawled, 686 pending
- **Speed**: 220-280x realtime per GPU (faster-whisper distil-large-v3.5)
- **HuggingFace**: 15.5K transcriptions pushed (626MB JSONL)

## 2026-02-16

### Faster-Whisper Migration
- Migrated from HuggingFace transformers to faster-whisper (CTranslate2): 3.35x speedup
- Benchmarked VAD (Silero): adds overhead on dense lectures, disabled
- 1.2x audio speedup via yt-dlp atempo filter: 17% less audio to process
- beam_size=1, condition_on_previous_text=False for max batch throughput

### Quality & Export
- Verified transcript quality: 13.7 chars/s avg, 99% substantial (>1000 chars)
- Built HF export pipeline with proper schema (int durations, default empty strings)
- Published dataset: `thepowerfuldeez/massive-yt-edu-transcriptions`
- Daily auto-push cron at 6am London

### Discovery Foundation
- 5 concurrent discovery strategies reaching 1M+ queue
- Quality filter: ~40 reject categories, 3-tier priority (P9 university, P8 lectures, P7 docs)
- 88K junk videos retroactively rejected

## 2026-02-15

### System Setup
- Built pipelined GPU worker with prefetch threads + numpy passthrough
- All 4 GPUs verified running (2x RTX 5090, 2x RTX 4090)
- Created GitHub repo: `thepowerfuldeez/massive_yt_edu_scraper`
- Set up hourly monitoring cron + HF daily push cron
- Initial queue: 51,931 videos from 11 mining strategies
