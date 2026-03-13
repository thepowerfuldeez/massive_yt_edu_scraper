#!/usr/bin/env python3
"""Reconstruct SQLite queue from HF datasets.

Step 1: Load thepowerfuldeez/massive-yt-edu-queue → rebuild videos table
Step 2: Merge common-pile/youtube (CC-licensed) → add new video IDs as green/pending
"""
import os, sys, sqlite3, time, json

DB_PATH = os.environ.get("DB_PATH", os.path.expanduser("~/academic_transcriptions/massive_production.db"))

def create_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS videos (
        video_id TEXT PRIMARY KEY,
        title TEXT,
        url TEXT,
        course TEXT,
        university TEXT,
        duration_seconds INTEGER,
        status TEXT DEFAULT 'pending',
        priority INTEGER DEFAULT 5,
        transcript TEXT,
        processing_time_seconds REAL,
        speed_ratio REAL,
        completed_at DATETIME,
        processing_started_at DATETIME,
        error TEXT,
        description TEXT,
        youtube_license TEXT,
        license_risk TEXT DEFAULT 'yellow',
        content_category TEXT
    )""")
    conn.commit()
    return conn


def step1_load_queue():
    """Load saved queue from thepowerfuldeez/massive-yt-edu-queue."""
    from datasets import load_dataset

    print("=== Step 1: Loading thepowerfuldeez/massive-yt-edu-queue ===")
    conn = create_db()

    ds = load_dataset("thepowerfuldeez/massive-yt-edu-queue", split="train")
    total = len(ds)
    print(f"  Dataset has {total:,} rows")

    batch = []
    inserted = 0
    skipped = 0

    for i, row in enumerate(ds):
        vid = row["video_id"]
        if not vid:
            skipped += 1
            continue

        url = row.get("url") or f"https://youtube.com/watch?v={vid}"
        batch.append((
            vid,
            row.get("title", ""),
            url,
            row.get("source", ""),   # maps to university
            "",                       # course
            int(row.get("duration_seconds", 0)),
            row.get("status", "pending"),
            int(row.get("priority", 5)),
            row.get("license_risk", "yellow"),
            row.get("content_category", ""),
        ))

        if len(batch) >= 10000:
            conn.executemany(
                "INSERT OR IGNORE INTO videos "
                "(video_id, title, url, university, course, duration_seconds, "
                "status, priority, license_risk, content_category) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                batch)
            conn.commit()
            inserted += len(batch)
            batch.clear()
            if inserted % 500000 == 0:
                print(f"  ... {inserted:,}/{total:,} inserted")

    if batch:
        conn.executemany(
            "INSERT OR IGNORE INTO videos "
            "(video_id, title, url, university, course, duration_seconds, "
            "status, priority, license_risk, content_category) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            batch)
        conn.commit()
        inserted += len(batch)

    # Reset processing → pending (stale claims from old machine)
    n = conn.execute(
        "UPDATE videos SET status='pending' WHERE status='processing'"
    ).rowcount
    conn.commit()
    if n:
        print(f"  Reset {n:,} stale 'processing' → 'pending'")

    actual = conn.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
    print(f"  Step 1 done: {actual:,} videos in DB (skipped {skipped:,} empty IDs)")
    conn.close()


def step2_merge_common_pile():
    """Merge common-pile/youtube — all CC-licensed, add as green/pending."""
    from datasets import load_dataset
    sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
    from quality_filter import is_educational, get_priority

    print("\n=== Step 2: Merging common-pile/youtube (CC-licensed) ===")
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")

    # Get existing video IDs for fast dedup
    existing = set(r[0] for r in conn.execute("SELECT video_id FROM videos").fetchall())
    print(f"  Existing videos: {len(existing):,}")

    ds = load_dataset("common-pile/youtube", split="train", streaming=True)

    batch = []
    new_count = 0
    dup_count = 0
    rejected_count = 0
    total_scanned = 0

    for row in ds:
        total_scanned += 1
        vid = row.get("id", "")
        if not vid:
            continue

        if vid in existing:
            dup_count += 1
            # Upgrade existing video to green if it was yellow
            batch.append(("_upgrade", vid))
            if len(batch) >= 10000:
                _flush_batch(conn, batch, existing)
                batch.clear()
            continue

        title = row.get("title", "")
        dur = int(row.get("duration", 0))

        # Apply quality filter — reject non-educational
        if not is_educational(title, dur):
            rejected_count += 1
            continue

        pri = get_priority(title)
        url = f"https://youtube.com/watch?v={vid}"

        batch.append(("_insert", vid, title, url, dur, pri))
        existing.add(vid)
        new_count += 1

        if len(batch) >= 10000:
            _flush_batch(conn, batch, existing)
            batch.clear()
            if new_count % 100000 == 0:
                print(f"  ... scanned {total_scanned:,} | new: {new_count:,} | dup: {dup_count:,} | rejected: {rejected_count:,}")

    if batch:
        _flush_batch(conn, batch, existing)

    conn.commit()
    final = conn.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
    green = conn.execute("SELECT COUNT(*) FROM videos WHERE license_risk='green'").fetchone()[0]
    print(f"\n  Step 2 done:")
    print(f"    Scanned:  {total_scanned:,}")
    print(f"    New:      {new_count:,}")
    print(f"    Dupes:    {dup_count:,}")
    print(f"    Rejected: {rejected_count:,}")
    print(f"    Total DB: {final:,}")
    print(f"    Green:    {green:,}")
    conn.close()


def _flush_batch(conn, batch, existing):
    inserts = []
    upgrades = []
    for item in batch:
        if item[0] == "_upgrade":
            upgrades.append((item[1],))
        elif item[0] == "_insert":
            _, vid, title, url, dur, pri = item
            inserts.append((vid, title, url, "common-pile/youtube", "",
                            dur, "pending", pri, "green", ""))

    if inserts:
        conn.executemany(
            "INSERT OR IGNORE INTO videos "
            "(video_id, title, url, university, course, duration_seconds, "
            "status, priority, license_risk, content_category) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            inserts)

    if upgrades:
        conn.executemany(
            "UPDATE videos SET license_risk='green', youtube_license='creativeCommon' "
            "WHERE video_id=? AND (license_risk IS NULL OR license_risk != 'green')",
            upgrades)

    conn.commit()


def print_stats():
    print("\n=== Final Stats ===")
    conn = sqlite3.connect(DB_PATH, timeout=30)
    for label, query in [
        ("Status", "SELECT status, COUNT(*) FROM videos GROUP BY status ORDER BY COUNT(*) DESC"),
        ("License risk", "SELECT license_risk, COUNT(*) FROM videos GROUP BY license_risk ORDER BY COUNT(*) DESC"),
        ("Priority", "SELECT priority, COUNT(*) FROM videos GROUP BY priority ORDER BY priority DESC"),
    ]:
        print(f"\n  {label}:")
        for row in conn.execute(query).fetchall():
            print(f"    {row[0] or 'null'}: {row[1]:,}")
    conn.close()


if __name__ == "__main__":
    t0 = time.time()
    step1_load_queue()
    step2_merge_common_pile()
    print_stats()
    print(f"\nTotal time: {time.time() - t0:.1f}s")
    print(f"DB at: {DB_PATH}")
