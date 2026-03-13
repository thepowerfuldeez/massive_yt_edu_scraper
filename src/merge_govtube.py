#!/usr/bin/env python3
"""Merge govtube videos from govtube.db into the main production queue.

Priority 6, green license. Skips duplicates, upgrades existing to green.
"""
import os, sqlite3, time

MAIN_DB = os.path.expanduser("~/academic_transcriptions/massive_production.db")
GOV_DB = os.path.expanduser("~/academic_transcriptions/govtube.db")


def merge():
    if not os.path.exists(GOV_DB):
        print(f"ERROR: {GOV_DB} not found")
        return

    print(f"Merging govtube → main queue")
    t0 = time.time()

    gov = sqlite3.connect(GOV_DB, timeout=30)
    main = sqlite3.connect(MAIN_DB, timeout=30)
    main.execute("PRAGMA journal_mode=WAL")

    # Ensure columns exist
    try:
        main.execute("ALTER TABLE videos ADD COLUMN channel_id TEXT")
    except:
        pass
    try:
        main.execute("ALTER TABLE videos ADD COLUMN tags TEXT")
    except:
        pass
    try:
        main.execute("ALTER TABLE videos ADD COLUMN published_time TEXT")
    except:
        pass

    # Get existing video IDs in main
    existing = set(r[0] for r in main.execute("SELECT video_id FROM videos").fetchall())
    print(f"  Main DB has {len(existing):,} videos")

    # Read all govtube videos
    rows = gov.execute(
        "SELECT video_id, title, url, channel, description, duration_seconds, "
        "status, priority, upload_date, categories, view_count, license_risk "
        "FROM videos"
    ).fetchall()
    print(f"  Govtube has {len(rows):,} videos")

    inserts = []
    upgrades = []
    skipped_completed = 0

    for row in rows:
        vid, title, url, channel, desc, dur, status, pri, upload, cats, views, lic = row

        if vid in existing:
            # Upgrade to green if not already
            upgrades.append((channel, desc, upload, vid))
        else:
            inserts.append((
                vid, title, url, dur, "pending", 6, "green",
                channel, desc, upload,
            ))

    # Flush inserts
    if inserts:
        main.executemany(
            "INSERT OR IGNORE INTO videos "
            "(video_id, title, url, duration_seconds, status, priority, license_risk, "
            "channel_id, description, published_time) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            inserts)
        print(f"  Inserted: {len(inserts):,}")

    # Upgrade existing to green + fill metadata
    if upgrades:
        main.executemany(
            "UPDATE videos SET license_risk='green', "
            "channel_id=COALESCE(NULLIF(?, ''), channel_id), "
            "description=COALESCE(NULLIF(?, ''), description), "
            "published_time=COALESCE(NULLIF(?, ''), published_time) "
            "WHERE video_id=?",
            upgrades)
        print(f"  Upgraded to green: {len(upgrades):,}")

    main.commit()

    # Stats
    total = main.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
    green = main.execute("SELECT COUNT(*) FROM videos WHERE license_risk='green'").fetchone()[0]
    pending = main.execute("SELECT COUNT(*) FROM videos WHERE status='pending'").fetchone()[0]
    print(f"\n  Main DB total: {total:,}")
    print(f"  Green license: {green:,}")
    print(f"  Pending: {pending:,}")
    print(f"  Done in {time.time() - t0:.1f}s")

    gov.close()
    main.close()


if __name__ == "__main__":
    merge()
