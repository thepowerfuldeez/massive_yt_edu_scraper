#!/usr/bin/env python3
"""Export completed transcriptions to HuggingFace dataset. Run daily via cron."""

import sqlite3, json, os, tempfile, shutil
from pathlib import Path

DB_PATH = os.environ.get("DB_PATH", os.path.expanduser("~/academic_transcriptions/massive_production.db"))
HF_REPO = os.environ.get("HF_REPO", "thepowerfuldeez/massive-yt-edu-transcriptions")
EXPORT_DIR = os.path.expanduser("~/academic_transcriptions/hf_export")

def export_jsonl():
    """Export all completed transcripts to JSONL files (chunked by 50K rows)."""
    os.makedirs(EXPORT_DIR, exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    
    total = conn.execute("SELECT count(*) FROM videos WHERE status='completed' AND transcript IS NOT NULL").fetchone()[0]
    print(f"Exporting {total} completed transcriptions...")
    
    chunk_size = 50000
    chunk_num = 0
    exported = 0
    
    cursor = conn.execute(
        "SELECT video_id, title, course, university, url, duration_seconds, "
        "transcript, processing_time_seconds, speed_ratio, priority, completed_at "
        "FROM videos WHERE status='completed' AND transcript IS NOT NULL "
        "ORDER BY priority DESC, completed_at"
    )
    
    current_file = None
    writer = None
    
    while True:
        rows = cursor.fetchmany(1000)
        if not rows:
            break
        
        for row in rows:
            if exported % chunk_size == 0:
                if writer:
                    writer.close()
                chunk_num = exported // chunk_size
                fname = f"train-{chunk_num:05d}.jsonl"
                fpath = os.path.join(EXPORT_DIR, fname)
                writer = open(fpath, 'w')
                print(f"  Writing {fname}...")
            
            record = {
                "video_id": row["video_id"] or "",
                "title": row["title"] or "",
                "text": row["transcript"] or "",
                "duration_seconds": int(row["duration_seconds"] or 0),
                "source": row["university"] or row["course"] or "",
                "url": row["url"] or "",
                "priority": int(row["priority"] or 5),
                "speed_ratio": round(float(row["speed_ratio"] or 0), 1),
            }
            writer.write(json.dumps(record, ensure_ascii=False) + '\n')
            exported += 1
    
    if writer:
        writer.close()
    
    conn.close()
    print(f"Exported {exported} records in {chunk_num + 1} file(s)")
    
    # Write dataset card
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
  - {'1K<n<10K' if exported < 10000 else '10K<n<100K' if exported < 100000 else '100K<n<1M' if exported < 1000000 else '1M<n<10M'}
---

# Massive YouTube Educational Transcriptions

Large-scale educational content transcribed from YouTube using distil-whisper.

## Dataset Description

- **Source**: YouTube educational videos (lectures, courses, tutorials, conference talks)
- **Transcription**: distil-whisper/distil-large-v3 (batched chunked inference)
- **Quality filter**: ≥15 minutes, 40+ reject categories, 3-tier educational priority
- **Languages**: 15+ (primarily English, plus Russian, German, French, Spanish, and more)

## Stats

- **Videos**: {exported:,}
- **Estimated tokens**: ~{exported * 7000 // 1000000}M (avg ~7K tokens per video)
- **Audio hours**: ~{exported * 35 // 60:,} (avg ~35 min per video)

## Fields

| Field | Description |
|-------|-------------|
| `video_id` | YouTube video ID |
| `title` | Video title |
| `text` | Full transcript |
| `duration_seconds` | Audio duration |
| `source` | Channel/course/university |
| `url` | YouTube URL |
| `priority` | Educational priority (9=university course, 8=lecture, 7=documentary, 5=general) |

## Priority Distribution

- **P9**: University courses (MIT, Stanford, NPTEL), conference papers (ICML, NeurIPS)
- **P8**: Lectures, tutorials, known educational creators
- **P7**: Documentaries, explainers, deep dives
- **P5**: Educational content without strong title signals

## Hardware

Transcribed on 2× RTX 5090 + 2× RTX 4090 at ~13,000 hours/day.

## Code

[github.com/thepowerfuldeez/massive_yt_edu_scraper](https://github.com/thepowerfuldeez/massive_yt_edu_scraper)

## License

MIT
"""
    with open(os.path.join(EXPORT_DIR, "README.md"), 'w') as f:
        f.write(card)
    
    return exported

def push_to_hf():
    """Push exported files to HuggingFace."""
    from huggingface_hub import HfApi
    
    api = HfApi()
    
    # Create repo if needed
    try:
        api.create_repo(HF_REPO, repo_type="dataset", exist_ok=True)
    except Exception as e:
        print(f"Repo creation: {e}")
    
    # Upload all files
    print(f"Pushing to {HF_REPO}...")
    api.upload_folder(
        folder_path=EXPORT_DIR,
        repo_id=HF_REPO,
        repo_type="dataset",
        commit_message=f"Update dataset export",
    )
    print("Push complete!")

if __name__ == "__main__":
    exported = export_jsonl()
    if exported > 0:
        try:
            push_to_hf()
        except Exception as e:
            print(f"HF push failed (need `huggingface-cli login`): {e}")
            print(f"Files ready at: {EXPORT_DIR}/")
            print(f"Manual push: huggingface-cli upload {HF_REPO} {EXPORT_DIR} --repo-type dataset")
