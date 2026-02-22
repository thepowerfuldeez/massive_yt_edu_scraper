#!/usr/bin/env python3
"""Batch YouTube license scanner v2 — optimized for concurrent DB access."""
import sqlite3, os, sys, time, json, urllib.request, urllib.error

DB = os.path.expanduser("~/academic_transcriptions/massive_production.db")
API_KEY = os.environ.get("YT_API_KEY", "AIzaSyAtpiGxOrjltfZ2HOMh1FM0JYHnSr4uQTQ")
BATCH_SIZE = 50
DELAY = 0.5
COMMIT_EVERY = 50

def get_db():
    conn = sqlite3.connect(DB, timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=120000")
    return conn

def fetch_licenses(video_ids):
    ids = ",".join(video_ids)
    url = f"https://www.googleapis.com/youtube/v3/videos?part=status&id={ids}&key={API_KEY}"
    try:
        resp = urllib.request.urlopen(urllib.request.Request(url), timeout=30)
        data = json.loads(resp.read())
        result = {}
        for item in data.get("items", []):
            result[item["id"]] = item.get("status", {}).get("license", "unknown")
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

def scan_videos():
    conn = get_db()
    start = time.time()
    
    # Skip slow count — just start processing
    print("Starting license scan (skipping initial count)...", flush=True)
    
    scanned = 0
    cc_found = 0
    unavailable = 0
    batch_count = 0
    errors = 0
    max_errors = 20  # stop after 20 consecutive errors

    while True:
        try:
            rows = conn.execute(
                "SELECT video_id FROM videos WHERE (youtube_license IS NULL OR youtube_license = '') LIMIT ?",
                (BATCH_SIZE,)
            ).fetchall()
        except sqlite3.OperationalError as e:
            print(f"DB read error: {e}", flush=True)
            time.sleep(10)
            errors += 1
            if errors > max_errors:
                print("Too many DB errors, stopping.", flush=True)
                break
            continue

        if not rows:
            break

        video_ids = [r[0] for r in rows]
        licenses = fetch_licenses(video_ids)

        if not licenses:
            errors += 1
            if errors > max_errors:
                print("Too many API errors, stopping.", flush=True)
                break
            time.sleep(5)
            continue
        
        errors = 0  # reset on success

        try:
            for vid, lic in licenses.items():
                conn.execute("UPDATE videos SET youtube_license = ? WHERE video_id = ?", (lic, vid))
                if lic == "creativeCommon":
                    conn.execute("UPDATE videos SET license_risk = 'green' WHERE video_id = ? AND license_risk != 'green'", (vid,))
                    cc_found += 1
                if lic == "unavailable":
                    unavailable += 1
        except sqlite3.OperationalError as e:
            print(f"DB write error: {e}", flush=True)
            time.sleep(10)
            continue

        scanned += len(licenses)
        batch_count += 1
        
        if batch_count == 1:
            print(f"  First batch done: {len(licenses)} videos", flush=True)

        if batch_count % COMMIT_EVERY == 0:
            conn.commit()

        if scanned % 500 == 0:
            conn.commit()
            elapsed = max(1, time.time() - start)
            rate = scanned / elapsed * 3600
            print(f"  {scanned:,} scanned | CC: {cc_found:,} | unavail: {unavailable:,} | {rate:,.0f}/hr", flush=True)

        time.sleep(DELAY)

    conn.commit()
    conn.close()
    elapsed = time.time() - start
    print(f"\nDone! Scanned {scanned:,} | CC: {cc_found:,} | Unavail: {unavailable:,} | {elapsed/60:.1f} min", flush=True)

if __name__ == "__main__":
    scan_videos()
