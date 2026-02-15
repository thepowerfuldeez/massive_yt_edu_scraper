# Progress

## Current Stats (2026-02-15)
- **Completed:** 3,041 videos (1,852 hours)
- **Pending:** 369,834 videos  
- **Average speed:** 98x realtime (per GPU)
- **Model:** distil-whisper/distil-large-v3.5
- **Hardware:** 2x RTX 5090 + 2x RTX 4090

## Recent Performance (v3.5 upgrade)
- GPU 0 (5090): 51-61x realtime
- GPU 1 (4090): 45-55x realtime  
- GPU 2 (5090): 62-76x realtime
- GPU 3 (4090): 60-72x realtime
- Previous (v3): 33-40x realtime

## Optimizations Applied
- Upgraded distil-large-v3 → v3.5 (+10% speed)
- Pre-load audio as numpy in prefetch threads (skip file I/O on GPU path)
- 4 prefetch threads per GPU (was 2)
- Batch DB claims (10 per round-trip)
- Download as mp3 instead of wav (10x smaller)
- Optimal batch sizes: bs=32 (5090), bs=24 (4090)

## Dataset
- HuggingFace: [thepowerfuldeez/massive-yt-edu-transcriptions](https://huggingface.co/datasets/thepowerfuldeez/massive-yt-edu-transcriptions)
- Daily auto-push at 6am London

## Timeline
- 2026-02-14: Project started, initial pipeline
- 2026-02-15: 3K videos, v3.5 upgrade, 2x speed improvement, HF dataset published
