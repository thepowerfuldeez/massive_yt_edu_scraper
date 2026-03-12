#!/usr/bin/env python3
"""Export completed transcriptions to local Parquet files.

Includes all fields from the DB — superset of what common-pile/youtube and
thepowerfuldeez/massive-yt-edu-transcriptions had.
"""
import sqlite3, os, time

DB_PATH = os.environ.get("DB_PATH", os.path.expanduser("~/academic_transcriptions/massive_production.db"))
EXPORT_DIR = "/persistent/poolside/oss/massive_yt_edu_scraper/exports"
CHUNK_SIZE = 50_000


def export():
    os.makedirs(EXPORT_DIR, exist_ok=True)

    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")

    total = conn.execute(
        "SELECT COUNT(*) FROM videos WHERE status='completed' AND transcript IS NOT NULL"
    ).fetchone()[0]

    stats = conn.execute(
        "SELECT SUM(length(transcript)), SUM(duration_seconds) "
        "FROM videos WHERE status='completed' AND transcript IS NOT NULL"
    ).fetchone()
    total_chars = int(stats[0] or 0)
    total_duration = int(stats[1] or 0)

    print(f"Exporting {total:,} transcriptions to Parquet...")
    print(f"  ~{total_chars // 4 // 1_000_000}M tokens, {total_duration // 3600:,} audio hours")

    cursor = conn.execute(
        "SELECT video_id, title, url, course, university, duration_seconds, "
        "transcript, processing_time_seconds, speed_ratio, priority, "
        "content_category, license_risk, channel_id, description, "
        "youtube_license, tags, published_time, completed_at "
        "FROM videos WHERE status='completed' AND transcript IS NOT NULL "
        "ORDER BY priority DESC, completed_at"
    )

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        print("Installing pyarrow...")
        import subprocess
        subprocess.check_call(["pip", "install", "pyarrow", "-q"])
        import pyarrow as pa
        import pyarrow.parquet as pq

    schema = pa.schema([
        ("video_id", pa.string()),
        ("title", pa.string()),
        ("text", pa.string()),
        ("url", pa.string()),
        ("duration_seconds", pa.float32()),
        ("source", pa.string()),
        ("channel_id", pa.string()),
        ("description", pa.string()),
        ("youtube_license", pa.string()),
        ("license_risk", pa.string()),
        ("content_category", pa.string()),
        ("priority", pa.int8()),
        ("speed_ratio", pa.float32()),
        ("tags", pa.string()),
        ("published_time", pa.string()),
        ("completed_at", pa.string()),
    ])

    exported = 0
    chunk_num = 0
    batch_rows = {col: [] for col in schema.names}
    t0 = time.time()

    while True:
        rows = cursor.fetchmany(5000)
        if not rows:
            break
        for row in rows:
            (video_id, title, url, course, university, duration_seconds,
             transcript, processing_time_seconds, speed_ratio, priority,
             content_category, license_risk, channel_id, description,
             youtube_license, tags, published_time, completed_at) = row

            batch_rows["video_id"].append(video_id or "")
            batch_rows["title"].append(title or "")
            batch_rows["text"].append(transcript or "")
            batch_rows["url"].append(url or "")
            batch_rows["duration_seconds"].append(float(duration_seconds or 0))
            batch_rows["source"].append(university or course or "")
            batch_rows["channel_id"].append(channel_id or "")
            batch_rows["description"].append(description or "")
            batch_rows["youtube_license"].append(youtube_license or "")
            batch_rows["license_risk"].append(license_risk or "")
            batch_rows["content_category"].append(content_category or "")
            batch_rows["priority"].append(int(priority or 5))
            batch_rows["speed_ratio"].append(round(float(speed_ratio or 0), 1))
            batch_rows["tags"].append(tags or "")
            batch_rows["published_time"].append(published_time or "")
            batch_rows["completed_at"].append(completed_at or "")
            exported += 1

            if exported % CHUNK_SIZE == 0:
                _write_chunk(batch_rows, schema, chunk_num, pq, pa)
                chunk_num += 1
                batch_rows = {col: [] for col in schema.names}

        if exported % 10_000 == 0:
            elapsed = time.time() - t0
            print(f"  {exported:,}/{total:,} ({exported*100//total}%) "
                  f"[{elapsed:.0f}s]", flush=True)

    # Write remaining
    if batch_rows["video_id"]:
        _write_chunk(batch_rows, schema, chunk_num, pq, pa)
        chunk_num += 1

    conn.close()
    elapsed = time.time() - t0
    print(f"\nDone: {exported:,} records in {chunk_num} file(s) [{elapsed:.0f}s]")

    # Show file sizes
    total_size = 0
    for f in sorted(os.listdir(EXPORT_DIR)):
        if f.endswith(".parquet"):
            sz = os.path.getsize(os.path.join(EXPORT_DIR, f))
            total_size += sz
            print(f"  {f}: {sz / 1024 / 1024:.1f} MB")
    print(f"  Total: {total_size / 1024 / 1024:.1f} MB")

    # Print column comparison
    print(f"\n--- Column comparison ---")
    print(f"common-pile/youtube had: id, title, duration, tags, channel_id, published_time, description")
    print(f"This export has all of those PLUS: text (transcript), url, source, youtube_license,")
    print(f"  license_risk, content_category, priority, speed_ratio, completed_at")


def _write_chunk(batch_rows, schema, chunk_num, pq, pa):
    fpath = os.path.join(EXPORT_DIR, f"train-{chunk_num:05d}.parquet")
    table = pa.table(batch_rows, schema=schema)
    pq.write_table(table, fpath, compression="zstd")
    n = len(batch_rows["video_id"])
    print(f"  Wrote {fpath} ({n:,} rows)", flush=True)


if __name__ == "__main__":
    export()
