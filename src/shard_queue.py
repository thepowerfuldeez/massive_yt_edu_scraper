#!/usr/bin/env python3
"""Shard the pending queue into N standalone SQLite DBs for multi-node execution.

Each shard is a self-contained DB that a remote node can use independently.
Default: CC-licensed (green) videos only.

Usage:
    python3 src/shard_queue.py --shards 8                          # 8 shards, green only
    python3 src/shard_queue.py --shards 8 --all                    # 8 shards, all pending
    python3 src/shard_queue.py --shards 8 --output /tmp/shards     # custom output dir
    python3 src/shard_queue.py --shards 8 --upload --bucket my-s3  # upload to S3
"""
import argparse, os, sqlite3, time, sys

DB_PATH = os.path.expanduser("~/academic_transcriptions/massive_production.db")
SHARD_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "shards")


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    return conn


def get_schema(conn):
    return conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='videos'"
    ).fetchone()[0]


def shard(num_shards, output_dir, green_only=True, mark_dispatched=False):
    conn = get_db()
    schema = get_schema(conn)

    where = "status='pending'"
    if green_only:
        where += " AND license_risk='green'"

    total = conn.execute(f"SELECT COUNT(*) FROM videos WHERE {where}").fetchone()[0]
    per_shard = total // num_shards
    remainder = total % num_shards

    print(f"Sharding {total:,} {'green' if green_only else 'all'} pending videos into {num_shards} shards")
    print(f"  ~{per_shard:,} per shard (+{remainder} in last)")

    cols = [desc[0] for desc in conn.execute("SELECT * FROM videos LIMIT 0").description]
    placeholders = ",".join(["?"] * len(cols))

    os.makedirs(output_dir, exist_ok=True)

    # Fetch all IDs in priority order
    rows = conn.execute(
        f"SELECT * FROM videos WHERE {where} "
        "ORDER BY CASE WHEN license_risk='green' THEN 0 ELSE 1 END, priority DESC"
    ).fetchall()

    shard_paths = []
    all_vids = []

    for shard_idx in range(num_shards):
        start = shard_idx * per_shard + min(shard_idx, remainder)
        end = start + per_shard + (1 if shard_idx < remainder else 0)
        shard_rows = rows[start:end]

        shard_path = os.path.join(output_dir, f"shard_{shard_idx:02d}.db")
        shard_conn = sqlite3.connect(shard_path)
        shard_conn.execute("PRAGMA journal_mode=WAL")
        shard_conn.execute(schema)
        shard_conn.executemany(f"INSERT INTO videos VALUES ({placeholders})", shard_rows)
        shard_conn.commit()

        vid_idx = cols.index("video_id")
        shard_vids = [r[vid_idx] for r in shard_rows]
        all_vids.extend(shard_vids)

        # Stats
        green = shard_conn.execute("SELECT COUNT(*) FROM videos WHERE license_risk='green'").fetchone()[0]
        shard_conn.close()

        sz = os.path.getsize(shard_path)
        shard_paths.append(shard_path)
        print(f"  shard_{shard_idx:02d}.db: {len(shard_rows):,} videos ({green:,} green) [{sz/1024/1024:.1f} MB]")

    # Mark as dispatched in main DB
    if mark_dispatched:
        print(f"\nMarking {len(all_vids):,} videos as 'dispatched' in main DB...")
        batch_size = 500
        for i in range(0, len(all_vids), batch_size):
            batch = all_vids[i:i + batch_size]
            ph = ",".join(["?"] * len(batch))
            conn.execute(f"UPDATE videos SET status='dispatched' WHERE video_id IN ({ph})", batch)
        conn.commit()

    conn.close()
    return shard_paths


def upload_shards(shard_paths, bucket, prefix="yt-edu"):
    import boto3
    s3 = boto3.client("s3")
    ts = time.strftime("%Y%m%d_%H%M%S")
    for path in shard_paths:
        name = os.path.basename(path)
        key = f"{prefix}/shards/{ts}/{name}"
        print(f"  Uploading {name} -> s3://{bucket}/{key}")
        s3.upload_file(path, bucket, key)
    print(f"All shards uploaded to s3://{bucket}/{prefix}/shards/{ts}/")


def main():
    parser = argparse.ArgumentParser(description="Shard pending queue for multi-node execution")
    parser.add_argument("--shards", type=int, required=True, help="Number of shards")
    parser.add_argument("--output", type=str, default=SHARD_DIR, help="Output directory")
    parser.add_argument("--all", action="store_true", help="Include all pending (not just green)")
    parser.add_argument("--dispatch", action="store_true",
                        help="Mark sharded videos as 'dispatched' in main DB")
    parser.add_argument("--upload", action="store_true", help="Upload shards to S3")
    parser.add_argument("--bucket", type=str, default=None, help="S3 bucket for upload")
    args = parser.parse_args()

    green_only = not args.all
    paths = shard(args.shards, args.output, green_only=green_only, mark_dispatched=args.dispatch)

    if args.upload:
        if not args.bucket:
            print("ERROR: --bucket required for upload")
            sys.exit(1)
        upload_shards(paths, args.bucket)

    print(f"\nShards written to: {args.output}/")
    print(f"Each shard is a standalone SQLite DB — copy to remote node and run ./run.sh")


if __name__ == "__main__":
    main()
