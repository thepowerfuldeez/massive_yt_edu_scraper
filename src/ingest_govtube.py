#!/usr/bin/env python3
"""Ingest storytracer/govtube_metadata into a separate SQLite DB.

1.17M U.S. government YouTube videos. Kept separate from main edu queue.
Assigned priority 6 (below educational P7-9, above default P5).
License: green (government/public domain content).
"""
import os, sys, sqlite3, time, json

DB_PATH = os.environ.get("GOVTUBE_DB", os.path.expanduser("~/academic_transcriptions/govtube.db"))
MIN_DURATION = 300  # 5 min


def create_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS videos (
        video_id TEXT PRIMARY KEY,
        title TEXT,
        url TEXT,
        channel TEXT,
        description TEXT,
        duration_seconds INTEGER,
        status TEXT DEFAULT 'pending',
        priority INTEGER DEFAULT 6,
        upload_date TEXT,
        categories TEXT,
        view_count INTEGER,
        license_risk TEXT DEFAULT 'green',
        transcript TEXT,
        processing_time_seconds REAL,
        speed_ratio REAL,
        completed_at DATETIME,
        processing_started_at DATETIME,
        error TEXT
    )""")
    conn.commit()
    return conn


def ingest():
    from huggingface_hub import hf_hub_download
    import pyarrow.parquet as pq

    print("=== Ingesting storytracer/govtube_metadata ===")
    t0 = time.time()
    conn = create_db()

    total_inserted = 0
    total_skipped_short = 0
    total_skipped_dup = 0
    batch = []

    for shard_idx in range(21):
        fname = f"data/parquet/data_{shard_idx:02d}.parquet"
        print(f"  Downloading shard {shard_idx:02d}...", end=" ", flush=True)
        f = hf_hub_download("storytracer/govtube_metadata", fname, repo_type="dataset")

        table = pq.read_table(f, columns=[
            "id", "title", "duration", "channel", "upload_date",
            "categories", "license", "view_count", "description",
        ])
        df = table.to_pandas()
        print(f"{len(df):,} rows", flush=True)

        for _, row in df.iterrows():
            vid = str(row.get("id", ""))
            if not vid:
                continue

            dur = int(row.get("duration", 0) or 0)
            if 0 < dur < MIN_DURATION:
                total_skipped_short += 1
                continue

            title = str(row.get("title", "") or "")
            channel = str(row.get("channel", "") or "")
            desc = str(row.get("description", "") or "")[:5000]
            upload = str(row.get("upload_date", "") or "")
            cats = row.get("categories")
            cats_str = json.dumps(list(cats)) if cats is not None and hasattr(cats, '__iter__') else ""
            views = int(row.get("view_count", 0) or 0)
            url = f"https://youtube.com/watch?v={vid}"

            batch.append((
                vid, title, url, channel, desc, dur,
                "pending", 6, upload, cats_str, views, "green",
            ))

            if len(batch) >= 10000:
                n = _flush(conn, batch)
                total_inserted += n
                total_skipped_dup += len(batch) - n
                batch.clear()

        # Flush remainder per shard
        if batch:
            n = _flush(conn, batch)
            total_inserted += n
            total_skipped_dup += len(batch) - n
            batch.clear()

    final = conn.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
    pending = conn.execute("SELECT COUNT(*) FROM videos WHERE status='pending'").fetchone()[0]
    conn.close()

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.0f}s")
    print(f"  Inserted: {total_inserted:,}")
    print(f"  Skipped (short): {total_skipped_short:,}")
    print(f"  Skipped (dup): {total_skipped_dup:,}")
    print(f"  Total in DB: {final:,}")
    print(f"  Pending: {pending:,}")
    print(f"  DB: {DB_PATH}")


def _flush(conn, batch):
    cur = conn.executemany(
        "INSERT OR IGNORE INTO videos "
        "(video_id, title, url, channel, description, duration_seconds, "
        "status, priority, upload_date, categories, view_count, license_risk) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        batch)
    conn.commit()
    return cur.rowcount


if __name__ == "__main__":
    ingest()
