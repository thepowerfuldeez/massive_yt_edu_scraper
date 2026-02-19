#!/usr/bin/env python3
"""
Batch YouTube license scanner using YouTube Data API v3.
50 video IDs per request, ~500K videos/day within quota.
Stores youtube_license field, reclassifies CC videos to green.
"""
import sqlite3, os, sys, time, json, urllib.request, urllib.error

DB = os.path.expanduser("~/academic_transcriptions/massive_production.db")
API_KEY = "AIzaSyAtpiGxOrjltfZ2HOMh1FM0JYHnSr4uQTQ"
BATCH_SIZE = 50  # Max per API request
QUOTA_LIMIT = 9500  # Stay under 10K daily

def get_db():
    conn = sqlite3.connect(DB, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn

def fetch_batch(video_ids):
    """Fetch license status for up to 50 videos."""
    ids_str = ",".join(video_ids)
    url = f"https://www.googleapis.com/youtube/v3/videos?part=status&id={ids_str}&key={API_KEY}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        resp = urllib.request.urlopen(req, timeout=15)
        data = json.loads(resp.read())
        results = {}
        for item in data.get("items", []):
            results[item["id"]] = item["status"]["license"]  # "creativeCommon" or "youtube"
        return results, None
    except urllib.error.HTTPError as e:
        body = e.read().decode()[:500]
        return {}, f"HTTP {e.code}: {body}"
    except Exception as e:
        return {}, str(e)

def main():
    conn = get_db()
    
    # Ensure column exists
    try:
        conn.execute("ALTER TABLE videos ADD COLUMN youtube_license TEXT DEFAULT ''")
    except sqlite3.OperationalError:
        pass
    
    total_no_license = conn.execute("""
        SELECT COUNT(*) FROM videos 
        WHERE (youtube_license IS NULL OR youtube_license = '' OR youtube_license LIKE 'CC%' OR youtube_license LIKE 'BSD%' OR youtube_license LIKE 'Public%')
        AND status IN ('completed', 'pending', 'processing')
    """).fetchone()[0]
    
    print(f"Videos to scan: {total_no_license:,}")
    print(f"Batch size: {BATCH_SIZE} | Quota limit: {QUOTA_LIMIT}")
    print(f"Max videos this run: {QUOTA_LIMIT * BATCH_SIZE:,}")
    print()
    
    api_calls = 0
    total_scanned = 0
    total_cc = 0
    total_youtube = 0
    total_missing = 0  # Deleted/private videos
    errors = 0
    start = time.time()
    
    while api_calls < QUOTA_LIMIT:
        # Get batch — prioritize completed, then pending
        rows = conn.execute("""
            SELECT video_id FROM videos 
            WHERE (youtube_license IS NULL OR youtube_license = '' OR youtube_license LIKE 'CC%' OR youtube_license LIKE 'BSD%' OR youtube_license LIKE 'Public%')
            AND status IN ('completed', 'pending', 'processing')
            ORDER BY 
                CASE WHEN status = 'completed' THEN 0 ELSE 1 END,
                RANDOM()
            LIMIT ?
        """, (BATCH_SIZE,)).fetchall()
        
        if not rows:
            print("All videos scanned!")
            break
        
        video_ids = [r[0] for r in rows]
        results, err = fetch_batch(video_ids)
        api_calls += 1
        
        if err:
            errors += 1
            if "quota" in err.lower() or "429" in err:
                print(f"\n⚠️ QUOTA EXCEEDED at call #{api_calls}: {err}")
                break
            if errors > 10:
                print(f"\n⚠️ Too many errors ({errors}), stopping")
                break
            print(f"  Error at call #{api_calls}: {err[:100]}")
            time.sleep(2)
            continue
        
        # Update DB
        cc_batch = 0
        for vid in video_ids:
            lic = results.get(vid)
            if lic is None:
                # Video not found (deleted/private)
                conn.execute("UPDATE videos SET youtube_license = 'unavailable' WHERE video_id = ?", (vid,))
                total_missing += 1
            elif lic == "creativeCommon":
                conn.execute("""
                    UPDATE videos SET youtube_license = 'creativeCommon', license_risk = 'green' 
                    WHERE video_id = ?
                """, (vid,))
                cc_batch += 1
                total_cc += 1
            else:
                conn.execute("UPDATE videos SET youtube_license = 'youtube' WHERE video_id = ?", (vid,))
                total_youtube += 1
            
            total_scanned += 1
        
        conn.commit()
        
        elapsed = time.time() - start
        rate = total_scanned / max(elapsed, 1)
        cc_pct = (total_cc / max(total_scanned - total_missing, 1)) * 100
        remaining = total_no_license - total_scanned
        eta_hrs = remaining / max(rate, 1) / 3600
        
        if api_calls % 50 == 0:
            print(f"  Calls: {api_calls:,} | Scanned: {total_scanned:,} | "
                  f"CC: {total_cc:,} ({cc_pct:.1f}%) | YT: {total_youtube:,} | "
                  f"Gone: {total_missing:,} | Rate: {rate:.0f}/s | "
                  f"ETA remaining: {eta_hrs:.1f}h")
    
    elapsed = time.time() - start
    print(f"\n{'='*60}")
    print(f"SCAN COMPLETE")
    print(f"{'='*60}")
    print(f"API calls: {api_calls:,}")
    print(f"Videos scanned: {total_scanned:,}")
    print(f"Creative Commons: {total_cc:,} ({(total_cc/max(total_scanned-total_missing,1))*100:.1f}%)")
    print(f"Standard YouTube: {total_youtube:,}")
    print(f"Deleted/Private: {total_missing:,}")
    print(f"Errors: {errors}")
    print(f"Time: {elapsed/60:.1f} minutes")
    print(f"Rate: {total_scanned/max(elapsed,1):.0f} videos/sec")
    
    print("\n--- Updated license_risk breakdown ---")
    for row in conn.execute("SELECT license_risk, COUNT(*) FROM videos GROUP BY license_risk ORDER BY COUNT(*) DESC"):
        print(f"  {row[0] or '(none)'}: {row[1]:,}")
    
    print("\n--- youtube_license breakdown ---")
    for row in conn.execute("SELECT youtube_license, COUNT(*) FROM videos GROUP BY youtube_license ORDER BY COUNT(*) DESC LIMIT 15"):
        print(f"  {row[0] or '(none)'}: {row[1]:,}")
    
    conn.close()

if __name__ == "__main__":
    main()
