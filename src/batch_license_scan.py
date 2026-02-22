#!/usr/bin/env python3
"""
Batch YouTube license scanner using YouTube Data API v3.
50 video IDs per request, ~500K videos/day within quota (10K units, 1 unit/request).
Stores youtube_license field and reclassifies CC videos to green.
"""

import sqlite3, os, sys, time, json, urllib.request, urllib.error

DB = os.path.expanduser("~/academic_transcriptions/massive_production.db")
API_KEY = os.environ.get("YT_API_KEY", "AIzaSyAtpiGxOrjltfZ2HOMh1FM0JYHnSr4uQTQ")
BATCH_SIZE = 50
# 10K quota/day, 1 unit per request, 50 IDs per request = 500K/day
# With 0.1s delay = ~18K requests/hr = 900K videos/hr (well within quota)
DELAY = 0.5  # gentle on SQLite — GPU workers need DB access
COMMIT_EVERY = 50  # commit every N batches


def get_db():
    conn = sqlite3.connect(DB, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def fetch_licenses(video_ids):
    """Fetch license status for up to 50 video IDs. Returns {video_id: license_str}."""
    ids = ",".join(video_ids)
    url = f"https://www.googleapis.com/youtube/v3/videos?part=status&id={ids}&key={API_KEY}"
    try:
        req = urllib.request.Request(url)
        resp = urllib.request.urlopen(req, timeout=30)
        data = json.loads(resp.read())
        result = {}
        for item in data.get("items", []):
            vid = item["id"]
            license_str = item.get("status", {}).get("license", "unknown")
            result[vid] = license_str
        # Videos not in response = deleted/private
        for vid in video_ids:
            if vid not in result:
                result[vid] = "unavailable"
        return result
    except urllib.error.HTTPError as e:
        body = e.read().decode() if hasattr(e, "read") else str(e)
        if "quotaExceeded" in body:
            print("QUOTA EXCEEDED — stopping.", flush=True)
            sys.exit(0)
        print(f"API error: {e.code} {body[:200]}", flush=True)
        return {}
    except Exception as e:
        print(f"Request error: {e}", flush=True)
        return {}


def scan_videos(mode="all"):
    """Scan videos for CC licenses.
    
    mode='all': scan everything without youtube_license set
    mode='completed': scan completed videos first
    """
    conn = get_db()

    # Ensure column exists
    cols = [r[1] for r in conn.execute("PRAGMA table_info(videos)").fetchall()]
    if "youtube_license" not in cols:
        conn.execute("ALTER TABLE videos ADD COLUMN youtube_license TEXT")
        conn.commit()
        print("Added youtube_license column", flush=True)

    if mode == "completed":
        where = "status='completed' AND (youtube_license IS NULL OR youtube_license = '')"
    else:
        where = "youtube_license IS NULL OR youtube_license = ''"

    total = conn.execute(f"SELECT count(*) FROM videos WHERE {where}").fetchone()[0]
    print(f"Scanning {total:,} videos (mode={mode})...", flush=True)

    scanned = 0
    cc_found = 0
    unavailable = 0
    batch_count = 0

    while True:
        rows = conn.execute(
            f"SELECT video_id FROM videos WHERE {where} LIMIT ?",
            (BATCH_SIZE,)
        ).fetchall()

        if not rows:
            break

        video_ids = [r[0] for r in rows]
        licenses = fetch_licenses(video_ids)

        if not licenses:
            time.sleep(5)
            continue

        for vid, lic in licenses.items():
            conn.execute(
                "UPDATE videos SET youtube_license = ? WHERE video_id = ?",
                (lic, vid)
            )
            if lic == "creativeCommon":
                conn.execute(
                    "UPDATE videos SET license_risk = 'green' WHERE video_id = ? AND license_risk != 'green'",
                    (vid,)
                )
                cc_found += 1
            if lic == "unavailable":
                unavailable += 1

        scanned += len(licenses)
        batch_count += 1

        if batch_count % COMMIT_EVERY == 0:
            conn.commit()

        if scanned % 5000 == 0:
            conn.commit()
            pct = scanned / total * 100 if total else 0
            rate = scanned / max(1, time.time() - start_time) * 3600
            print(f"  {scanned:,}/{total:,} ({pct:.1f}%) | CC: {cc_found:,} | "
                  f"unavail: {unavailable:,} | {rate:,.0f}/hr", flush=True)

        time.sleep(DELAY)

    conn.commit()
    conn.close()
    print(f"\nDone! Scanned {scanned:,} | CC found: {cc_found:,} | "
          f"Unavailable: {unavailable:,}", flush=True)


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"
    start_time = time.time()
    scan_videos(mode)
