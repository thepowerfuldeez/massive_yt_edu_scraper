#!/usr/bin/env python3
"""Sync SQLite DB and audio queue to/from S3 for multi-machine operation.

Operations:
  push   — Upload DB + queue manifest to S3
  pull   — Download DB from S3 (for cold-starting a new machine)
  split  — Export a slice of pending work to a standalone DB for another machine
  merge  — Merge completed transcriptions from a remote DB back into main

Usage:
    python3 src/s3_sync.py push   --bucket my-bucket [--prefix yt-edu]
    python3 src/s3_sync.py pull   --bucket my-bucket [--prefix yt-edu]
    python3 src/s3_sync.py split  --bucket my-bucket --count 100000 [--prefix yt-edu]
    python3 src/s3_sync.py merge  --db /path/to/remote.db
"""
import argparse, os, sqlite3, sys, time, shutil, tempfile

DB_PATH = os.path.expanduser("~/academic_transcriptions/massive_production.db")


def get_db(path=DB_PATH):
    conn = sqlite3.connect(path, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    return conn


def push(bucket, prefix, db_path):
    """Upload a checkpoint of the DB to S3."""
    import boto3

    s3 = boto3.client("s3")
    ts = time.strftime("%Y%m%d_%H%M%S")

    # Checkpoint via sqlite backup API (safe, no WAL issues)
    tmp = tempfile.mktemp(suffix=".db")
    conn = get_db(db_path)
    backup = sqlite3.connect(tmp)
    conn.backup(backup)
    backup.close()
    conn.close()

    key = f"{prefix}/db/massive_production_{ts}.db"
    print(f"Uploading {tmp} -> s3://{bucket}/{key}")
    s3.upload_file(tmp, bucket, key)

    # Also upload as "latest"
    latest_key = f"{prefix}/db/massive_production_latest.db"
    s3.upload_file(tmp, bucket, latest_key)
    os.unlink(tmp)

    # Upload stats manifest
    conn = get_db(db_path)
    stats = {}
    for row in conn.execute("SELECT status, COUNT(*) FROM videos GROUP BY status"):
        stats[row[0]] = row[1]
    completed = conn.execute(
        "SELECT COUNT(*), COALESCE(SUM(duration_seconds)/3600.0,0) "
        "FROM videos WHERE status='completed'"
    ).fetchone()
    stats["completed_count"] = completed[0]
    stats["completed_audio_hrs"] = round(completed[1], 1)
    conn.close()

    import json
    manifest = json.dumps(stats, indent=2)
    s3.put_object(Bucket=bucket, Key=f"{prefix}/manifest.json", Body=manifest)
    print(f"Manifest: {stats}")
    print("Push complete.")


def pull(bucket, prefix, db_path):
    """Download latest DB from S3."""
    import boto3

    s3 = boto3.client("s3")
    key = f"{prefix}/db/massive_production_latest.db"
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    if os.path.exists(db_path):
        backup = db_path + ".backup"
        print(f"Backing up existing DB to {backup}")
        shutil.copy2(db_path, backup)

    print(f"Downloading s3://{bucket}/{key} -> {db_path}")
    s3.download_file(bucket, key, db_path)

    conn = get_db(db_path)
    total = conn.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
    completed = conn.execute("SELECT COUNT(*) FROM videos WHERE status='completed'").fetchone()[0]
    pending = conn.execute("SELECT COUNT(*) FROM videos WHERE status='pending'").fetchone()[0]
    conn.close()
    print(f"Pulled DB: {total:,} total, {completed:,} completed, {pending:,} pending")


def split(bucket, prefix, count, db_path):
    """Export a slice of pending work as a standalone DB and upload to S3.

    The split DB contains:
    - `count` pending videos (claimed atomically from main DB)
    - Full schema so a remote worker can operate independently
    - Videos marked 'dispatched' in main DB to prevent double-processing
    """
    import boto3

    conn = get_db(db_path)
    ts = time.strftime("%Y%m%d_%H%M%S")
    split_path = tempfile.mktemp(suffix=".db")

    # Create split DB with same schema
    split_conn = sqlite3.connect(split_path)
    split_conn.execute("PRAGMA journal_mode=WAL")

    # Copy schema
    schema = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='videos'"
    ).fetchone()[0]
    split_conn.execute(schema)

    # Claim videos: prioritize green, then by priority
    rows = conn.execute(
        "SELECT * FROM videos WHERE status='pending' "
        "ORDER BY "
        "  CASE WHEN license_risk='green' THEN 0 ELSE 1 END, "
        "  priority DESC, "
        "  RANDOM() "
        "LIMIT ?",
        (count,),
    ).fetchall()

    if not rows:
        print("No pending videos to split.")
        split_conn.close()
        os.unlink(split_path)
        conn.close()
        return

    # Get column names
    cols = [desc[0] for desc in conn.execute("SELECT * FROM videos LIMIT 0").description]
    vid_idx = cols.index("video_id")
    vids = [r[vid_idx] for r in rows]

    # Insert into split DB as pending
    placeholders = ",".join(["?"] * len(cols))
    split_conn.executemany(f"INSERT INTO videos VALUES ({placeholders})", rows)
    split_conn.commit()
    split_conn.close()

    # Mark as dispatched in main DB
    batch_size = 500
    for i in range(0, len(vids), batch_size):
        batch = vids[i : i + batch_size]
        ph = ",".join(["?"] * len(batch))
        conn.execute(
            f"UPDATE videos SET status='dispatched' WHERE video_id IN ({ph})", batch
        )
    conn.commit()
    conn.close()

    # Upload
    s3 = boto3.client("s3")
    key = f"{prefix}/splits/split_{ts}_{count}.db"
    print(f"Uploading {len(rows):,} videos -> s3://{bucket}/{key}")
    s3.upload_file(split_path, bucket, key)
    os.unlink(split_path)
    print(f"Split complete: {len(rows):,} videos dispatched.")


def merge(remote_db_path, db_path):
    """Merge completed transcriptions from a remote DB back into main."""
    remote = get_db(remote_db_path)
    main = get_db(db_path)

    rows = remote.execute(
        "SELECT video_id, transcript, duration_seconds, processing_time_seconds, "
        "speed_ratio, completed_at FROM videos WHERE status='completed' AND transcript IS NOT NULL"
    ).fetchall()

    merged = 0
    for vid, transcript, dur, proc_time, speed, completed_at in rows:
        main.execute(
            "UPDATE videos SET status='completed', transcript=?, duration_seconds=?, "
            "processing_time_seconds=?, speed_ratio=?, completed_at=? "
            "WHERE video_id=? AND status IN ('pending', 'dispatched', 'processing')",
            (transcript, dur, proc_time, speed, completed_at, vid),
        )
        merged += 1

    # Also merge errors
    errors = remote.execute(
        "SELECT video_id, error FROM videos WHERE status='error'"
    ).fetchall()
    for vid, error in errors:
        main.execute(
            "UPDATE videos SET status='error', error=? "
            "WHERE video_id=? AND status IN ('pending', 'dispatched', 'processing')",
            (error, vid),
        )

    main.commit()
    remote.close()
    main.close()
    print(f"Merged {merged:,} completed + {len(errors):,} errors from {remote_db_path}")


def main():
    parser = argparse.ArgumentParser(description="S3 sync for multi-machine operation")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_push = sub.add_parser("push", help="Upload DB checkpoint to S3")
    p_push.add_argument("--bucket", required=True)
    p_push.add_argument("--prefix", default="yt-edu")
    p_push.add_argument("--db", default=DB_PATH)

    p_pull = sub.add_parser("pull", help="Download latest DB from S3")
    p_pull.add_argument("--bucket", required=True)
    p_pull.add_argument("--prefix", default="yt-edu")
    p_pull.add_argument("--db", default=DB_PATH)

    p_split = sub.add_parser("split", help="Split pending work to S3")
    p_split.add_argument("--bucket", required=True)
    p_split.add_argument("--prefix", default="yt-edu")
    p_split.add_argument("--count", type=int, default=100_000)
    p_split.add_argument("--db", default=DB_PATH)

    p_merge = sub.add_parser("merge", help="Merge remote completed DB into main")
    p_merge.add_argument("--remote-db", required=True, dest="remote_db")
    p_merge.add_argument("--db", default=DB_PATH)

    args = parser.parse_args()
    if args.cmd == "push":
        push(args.bucket, args.prefix, args.db)
    elif args.cmd == "pull":
        pull(args.bucket, args.prefix, args.db)
    elif args.cmd == "split":
        split(args.bucket, args.prefix, args.count, args.db)
    elif args.cmd == "merge":
        merge(args.remote_db, args.db)


if __name__ == "__main__":
    main()
