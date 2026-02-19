#!/usr/bin/env python3
"""Export completed transcriptions to HuggingFace dataset. Run daily via cron."""

import sqlite3, json, os

DB_PATH = os.environ.get("DB_PATH", os.path.expanduser("~/academic_transcriptions/massive_production.db"))
HF_REPO = os.environ.get("HF_REPO", "thepowerfuldeez/massive-yt-edu-transcriptions")
EXPORT_DIR = os.path.expanduser("~/academic_transcriptions/hf_export")
CHUNK_SIZE = 50_000


def export_jsonl():
    """Export all completed transcripts to JSONL files (chunked by 50K rows)."""
    os.makedirs(EXPORT_DIR, exist_ok=True)

    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row

    total = conn.execute(
        "SELECT count(*) FROM videos WHERE status='completed' AND transcript IS NOT NULL"
    ).fetchone()[0]
    print(f"Exporting {total:,} completed transcriptions...")

    stats = conn.execute(
        "SELECT SUM(length(transcript)), SUM(duration_seconds) "
        "FROM videos WHERE status='completed' AND transcript IS NOT NULL"
    ).fetchone()
    total_chars = int(stats[0] or 0)
    total_duration = int(stats[1] or 0)

    cursor = conn.execute(
        "SELECT video_id, title, course, university, url, duration_seconds, "
        "transcript, processing_time_seconds, speed_ratio, priority, "
        "content_category, license_risk, completed_at "
        "FROM videos WHERE status='completed' AND transcript IS NOT NULL "
        "ORDER BY priority DESC, completed_at"
    )

    exported = 0
    chunk_num = 0
    writer = None

    while True:
        rows = cursor.fetchmany(1000)
        if not rows:
            break
        for row in rows:
            if exported % CHUNK_SIZE == 0:
                if writer:
                    writer.close()
                chunk_num = exported // CHUNK_SIZE
                fpath = os.path.join(EXPORT_DIR, f"train-{chunk_num:05d}.jsonl")
                writer = open(fpath, "w")
                print(f"  Writing train-{chunk_num:05d}.jsonl...")

            record = {
                "video_id": row["video_id"] or "",
                "title": row["title"] or "",
                "text": row["transcript"] or "",
                "duration_seconds": int(row["duration_seconds"] or 0),
                "source": row["university"] or row["course"] or "",
                "url": row["url"] or "",
                "priority": int(row["priority"] or 5),
                "speed_ratio": round(float(row["speed_ratio"] or 0), 1),
                "content_category": row["content_category"] or "",
                "license_risk": row["license_risk"] or "",
            }
            writer.write(json.dumps(record, ensure_ascii=False) + "\n")
            exported += 1

    if writer:
        writer.close()

    conn.close()
    print(f"Exported {exported:,} records in {chunk_num + 1} file(s)")

    # Write dataset card
    if exported < 10_000:
        size_cat = "1K<n<10K"
    elif exported < 100_000:
        size_cat = "10K<n<100K"
    elif exported < 1_000_000:
        size_cat = "100K<n<1M"
    else:
        size_cat = "1M<n<10M"

    card = f"""---
license: mit
task_categories:
  - automatic-speech-recognition
  - text-generation
language:
  - en
  - ru
  - de
  - fr
  - es
  - pt
  - ja
  - ko
  - zh
  - ar
  - hi
tags:
  - education
  - lectures
  - transcription
  - youtube
  - whisper
size_categories:
  - {size_cat}
---

# Massive YouTube Educational Transcriptions

Large-scale educational content transcribed from YouTube using distil-whisper/distil-large-v3.5.

## Stats

- **Videos**: {exported:,}
- **Characters**: {total_chars:,} (~{total_chars // 4 // 1_000_000}M tokens)
- **Audio hours**: {total_duration // 3600:,}
- **Model**: faster-whisper (CTranslate2) with distil-large-v3.5
- **Hardware**: 2x RTX 5090 + 2x RTX 4090 at 165-185x realtime

## Fields

| Field | Description |
|-------|-------------|
| `video_id` | YouTube video ID |
| `title` | Video title |
| `text` | Full transcript |
| `duration_seconds` | Original audio duration (seconds) |
| `source` | Discovery source (channel/course/playlist) |
| `url` | YouTube URL |
| `priority` | Educational priority (9=university, 8=lecture, 7=documentary, 5=general) |
| `speed_ratio` | Transcription speed (realtime multiplier) |
| `content_category` | Content type (university_lecture, conference, individual_educator, etc.) |
| `license_risk` | License risk level (green/yellow/orange/red) |

## Content Categories

- **green**: CC-licensed or public domain (NPTEL, Khan Academy, MIT OCW, Yale OYC)
- **yellow**: Standard YouTube license, fair use for research
- **orange**: Commercial content, needs review
- **red**: Non-educational, excluded from transcription

## Methodology

1. **Discovery**: Channel crawling, related video walking, CC-focused search
2. **Quality filter**: 40+ reject categories, duration >= 15min, 3-tier priority scoring
3. **Transcription**: faster-whisper CTranslate2, 1.2x audio speedup, beam=1, no VAD
4. **Classification**: Channel name -> title regex -> priority fallback

## Code

[github.com/thepowerfuldeez/massive_yt_edu_scraper](https://github.com/thepowerfuldeez/massive_yt_edu_scraper)

## License

MIT
"""
    with open(os.path.join(EXPORT_DIR, "README.md"), "w") as f:
        f.write(card)

    return exported


def push_to_hf():
    """Push exported files to HuggingFace."""
    from huggingface_hub import HfApi
    api = HfApi()
    try:
        api.create_repo(HF_REPO, repo_type="dataset", exist_ok=True)
    except Exception as e:
        print(f"Repo creation: {e}")
    print(f"Pushing to {HF_REPO}...")
    api.upload_folder(
        folder_path=EXPORT_DIR, repo_id=HF_REPO,
        repo_type="dataset", commit_message="Update dataset export")
    print("Push complete!")


if __name__ == "__main__":
    exported = export_jsonl()
    if exported > 0:
        try:
            push_to_hf()
        except Exception as e:
            print(f"HF push failed (need `huggingface-cli login`): {e}")
            print(f"Files ready at: {EXPORT_DIR}/")
