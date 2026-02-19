#!/usr/bin/env python3
"""Export full enriched video queue to HuggingFace dataset."""

import sqlite3, json, os
from pathlib import Path

DB_PATH = os.environ.get("DB_PATH", os.path.expanduser("~/academic_transcriptions/massive_production.db"))
HF_REPO = "thepowerfuldeez/massive-yt-edu-queue"
EXPORT_DIR = os.path.expanduser("~/academic_transcriptions/hf_queue_export")

def export():
    os.makedirs(EXPORT_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row

    # Check which columns exist
    cols = [r[1] for r in conn.execute("PRAGMA table_info(videos)").fetchall()]
    has_category = "content_category" in cols
    has_risk = "license_risk" in cols

    select = """SELECT video_id, title, url, duration_seconds, status, priority,
                university, course"""
    if has_category:
        select += ", content_category"
    if has_risk:
        select += ", license_risk"
    select += " FROM videos ORDER BY priority DESC, id"

    total = conn.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
    print(f"Exporting {total:,} videos...")

    # Stats for README
    stats = {}
    if has_category:
        for r in conn.execute("SELECT content_category, COUNT(*) FROM videos GROUP BY content_category ORDER BY COUNT(*) DESC"):
            stats[r[0] or "null"] = r[1]
    if has_risk:
        risk_stats = {}
        for r in conn.execute("SELECT license_risk, COUNT(*) FROM videos GROUP BY license_risk ORDER BY COUNT(*) DESC"):
            risk_stats[r[0] or "null"] = r[1]

    status_stats = {}
    for r in conn.execute("SELECT status, COUNT(*) FROM videos GROUP BY status ORDER BY COUNT(*) DESC"):
        status_stats[r[0]] = r[1]

    priority_stats = {}
    for r in conn.execute("SELECT priority, COUNT(*) FROM videos GROUP BY priority ORDER BY priority DESC"):
        priority_stats[str(r[0])] = r[1]

    chunk_size = 500000
    chunk_num = 0
    exported = 0
    writer = None
    cursor = conn.execute(select)

    while True:
        rows = cursor.fetchmany(5000)
        if not rows:
            break
        for row in rows:
            if exported % chunk_size == 0:
                if writer:
                    writer.close()
                fname = f"train-{exported // chunk_size:05d}.jsonl"
                fpath = os.path.join(EXPORT_DIR, fname)
                writer = open(fpath, "w")
                print(f"  Writing {fname}...")
            record = {
                "video_id": row["video_id"] or "",
                "title": row["title"] or "",
                "url": row["url"] or "",
                "duration_seconds": int(row["duration_seconds"] or 0),
                "status": row["status"] or "",
                "priority": int(row["priority"] or 5),
                "source": row["university"] or row["course"] or "",
            }
            if has_category:
                record["content_category"] = row["content_category"] or "unknown"
            if has_risk:
                record["license_risk"] = row["license_risk"] or "yellow"
            writer.write(json.dumps(record, ensure_ascii=False) + "\n")
            exported += 1

    if writer:
        writer.close()

    conn.close()
    print(f"Exported {exported:,} records in {exported // chunk_size + 1} file(s)")

    # Category stats table
    cat_table = ""
    if stats:
        cat_table = "\n## Content Categories\n\n| Category | Count | % |\n|----------|------:|---:|\n"
        for cat, cnt in sorted(stats.items(), key=lambda x: -x[1]):
            cat_table += f"| `{cat}` | {cnt:,} | {cnt*100/exported:.1f}% |\n"

    risk_table = ""
    if has_risk and risk_stats:
        risk_table = "\n## License Risk Distribution\n\n| Risk | Count | % |\n|------|------:|---:|\n"
        for risk, cnt in sorted(risk_stats.items(), key=lambda x: -x[1]):
            emoji = {"green": "🟢", "yellow": "🟡", "orange": "🟠", "red": "🔴"}.get(risk, "⚪")
            risk_table += f"| {emoji} `{risk}` | {cnt:,} | {cnt*100/exported:.1f}% |\n"

    status_table = "\n## Status Distribution\n\n| Status | Count |\n|--------|------:|\n"
    for s, cnt in sorted(status_stats.items(), key=lambda x: -x[1]):
        status_table += f"| `{s}` | {cnt:,} |\n"

    priority_table = "\n## Priority Distribution\n\n| Priority | Count |\n|----------|------:|\n"
    for p, cnt in sorted(priority_stats.items(), key=lambda x: -int(x[0])):
        priority_table += f"| P{p} | {cnt:,} |\n"

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
tags:
  - education
  - lectures
  - youtube
  - queue
  - metadata
size_categories:
  - 1M<n<10M
---

# Massive YouTube Educational Video Queue

Full metadata for {exported:,} YouTube educational videos — the discovery queue for
[massive-yt-edu-transcriptions](https://huggingface.co/datasets/thepowerfuldeez/massive-yt-edu-transcriptions).

## Description

This dataset contains metadata and content categorization for ~4.4M YouTube videos identified
as potentially educational. Each video has been categorized by content source and license risk level.

## Fields

| Field | Description |
|-------|-------------|
| `video_id` | YouTube video ID |
| `title` | Video title |
| `url` | YouTube URL |
| `duration_seconds` | Video duration (0 if unknown) |
| `status` | Processing status (pending/completed/rejected/error) |
| `priority` | Educational priority (9=university, 8=lecture, 7=doc, 5=default) |
| `source` | Channel/university/course name |
| `content_category` | Content category (see below) |
| `license_risk` | License risk: green/yellow/orange/red |
{status_table}
{priority_table}
{cat_table}
{risk_table}

## License Risk Levels

- 🟢 **green** — Known CC/public domain license (MIT OCW, Khan Academy, etc.)
- 🟡 **yellow** — Likely fair use (educational/factual lectures)
- 🟠 **orange** — Uncertain, needs review
- 🔴 **red** — Risky (entertainment, copyrighted, non-educational)

## Code

[github.com/thepowerfuldeez/massive_yt_edu_scraper](https://github.com/thepowerfuldeez/massive_yt_edu_scraper)

## License

MIT — this metadata dataset. Individual video content has varying licenses as indicated by `license_risk`.
"""
    with open(os.path.join(EXPORT_DIR, "README.md"), "w") as f:
        f.write(card)

    return exported

def push():
    from huggingface_hub import HfApi
    api = HfApi()
    try:
        api.create_repo(HF_REPO, repo_type="dataset", exist_ok=True)
    except Exception as e:
        print(f"Repo: {e}")
    print(f"Pushing to {HF_REPO}...")
    api.upload_folder(
        folder_path=EXPORT_DIR,
        repo_id=HF_REPO,
        repo_type="dataset",
        commit_message="Update queue export with content categorization",
    )
    print("Push complete!")

if __name__ == "__main__":
    exported = export()
    if exported > 0:
        try:
            push()
        except Exception as e:
            print(f"Push failed: {e}")
            print(f"Files at: {EXPORT_DIR}/")
