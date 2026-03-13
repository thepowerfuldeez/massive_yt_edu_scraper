#!/usr/bin/env python3
"""Merge common-pile/youtube into the main queue.

Downloads the full dataset (non-streaming, parquet-backed), preserves all
metadata (description, tags, channel_id, published_time, duration), and
marks everything as green license.

Also upgrades existing videos that overlap to green.
"""
import os, sys, sqlite3, time, json

DB_PATH = os.environ.get("DB_PATH", os.path.expanduser("~/academic_transcriptions/massive_production.db"))
MIN_DURATION = 300  # 5 minutes

sys.path.insert(0, os.path.dirname(__file__))
from quality_filter import is_educational, get_priority


def merge():
    from datasets import load_dataset

    print("Downloading common-pile/youtube (full, non-streaming)...")
    t0 = time.time()
    ds = load_dataset("common-pile/youtube", split="train")
    total = len(ds)
    print(f"  Downloaded {total:,} rows in {time.time() - t0:.0f}s")

    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")

    # Get existing IDs for fast dedup
    existing = set(r[0] for r in conn.execute("SELECT video_id FROM videos").fetchall())
    print(f"  Existing videos in DB: {len(existing):,}")

    new_inserts = []
    upgrades = []
    skipped_short = 0
    skipped_rejected = 0
    dup_upgraded = 0
    dup_already_green = 0

    for i in range(total):
        row = ds[i]
        vid = row.get("id", "")
        if not vid:
            continue

        title = row.get("title", "") or ""
        dur = int(row.get("duration", 0) or 0)
        desc = row.get("description", "") or ""
        tags = row.get("tags") or []
        channel_id = row.get("channel_id", "") or ""
        published = str(row.get("published_time", "")) if row.get("published_time") else ""
        url = f"https://youtube.com/watch?v={vid}"

        if vid in existing:
            # Upgrade to green + fill metadata
            upgrades.append((channel_id, desc, json.dumps(tags) if tags else "",
                             published, vid))
            existing.add(vid)  # already there but no-op
            continue

        # Filter: duration
        if 0 < dur < MIN_DURATION:
            skipped_short += 1
            continue

        # Filter: quality
        if not is_educational(title, dur):
            skipped_rejected += 1
            continue

        pri = get_priority(title)

        new_inserts.append((
            vid, title, url, "common-pile/youtube", "",
            dur, "pending", pri, "green", "",
            channel_id, desc, json.dumps(tags) if tags else "",
            published, "creativeCommon",
        ))
        existing.add(vid)

        # Flush in batches
        if len(new_inserts) >= 10000 or len(upgrades) >= 10000:
            _flush(conn, new_inserts, upgrades)
            new_inserts.clear()
            upgrades.clear()

        if (i + 1) % 100000 == 0:
            print(f"  ... {i+1:,}/{total:,} | new: {len(existing) - len(set())} | short: {skipped_short:,} | rejected: {skipped_rejected:,}", flush=True)

    # Final flush
    if new_inserts or upgrades:
        _flush(conn, new_inserts, upgrades)

    # Stats
    final = conn.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
    green = conn.execute("SELECT COUNT(*) FROM videos WHERE license_risk='green'").fetchone()[0]
    pending = conn.execute("SELECT COUNT(*) FROM videos WHERE status='pending'").fetchone()[0]
    conn.close()

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.0f}s")
    print(f"  common-pile rows scanned: {total:,}")
    print(f"  Skipped (short <{MIN_DURATION}s): {skipped_short:,}")
    print(f"  Skipped (rejected): {skipped_rejected:,}")
    print(f"  Total DB: {final:,}")
    print(f"  Green: {green:,}")
    print(f"  Pending: {pending:,}")


def _flush(conn, inserts, upgrades):
    if inserts:
        conn.executemany(
            "INSERT OR IGNORE INTO videos "
            "(video_id, title, url, university, course, duration_seconds, "
            "status, priority, license_risk, content_category, "
            "channel_id, description, tags, published_time, youtube_license) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            inserts)

    if upgrades:
        conn.executemany(
            "UPDATE videos SET license_risk='green', youtube_license='creativeCommon', "
            "channel_id=COALESCE(NULLIF(?, ''), channel_id), "
            "description=COALESCE(NULLIF(?, ''), description), "
            "tags=COALESCE(NULLIF(?, ''), tags), "
            "published_time=COALESCE(NULLIF(?, ''), published_time) "
            "WHERE video_id=?",
            upgrades)

    conn.commit()


if __name__ == "__main__":
    merge()
