#!/usr/bin/env python3
"""
Batch fetch video descriptions + license fields using yt-dlp.
No API key needed. Stores in DB, reclassifies CC-licensed videos.

Uses --print to extract just the fields we need (faster than --dump-json).
Processes in parallel with ThreadPoolExecutor.
"""
import sqlite3, os, sys, time, json, subprocess, re
from concurrent.futures import ThreadPoolExecutor, as_completed

DB = os.path.expanduser("~/academic_transcriptions/massive_production.db")
YTDLP = os.path.expanduser("~/academic_transcriptions/yt-dlp")
COOKIE_DIR = os.path.expanduser("~/academic_transcriptions/cookie_pool")
WORKERS = 4

CC_PATTERNS = re.compile(
    r'creative\s*commons|'
    r'\bcc[\s\-]by\b|'
    r'\bcc[\s\-]by[\s\-]nc\b|'
    r'\bcc[\s\-]by[\s\-]sa\b|'
    r'\bcc[\s\-]by[\s\-]nc[\s\-]sa\b|'
    r'licensed?\s+under\s+.*creative|'
    r'opencourseware|'
    r'open\s*course\s*ware|'
    r'ocw\.mit\.edu|'
    r'open\.edu/openlearn|'
    r'this\s+work\s+is\s+licensed\s+under|'
    r'public\s+domain',
    re.IGNORECASE
)

def get_db():
    conn = sqlite3.connect(DB, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn

def get_cookie_file(worker_id):
    """Get a cookie file for this worker."""
    cookies = sorted([f for f in os.listdir(COOKIE_DIR) if f.endswith('.txt')])
    if not cookies:
        return None
    return os.path.join(COOKIE_DIR, cookies[worker_id % len(cookies)])

def fetch_one(video_id, worker_id=0):
    """Fetch description + license for one video via yt-dlp."""
    cookie = get_cookie_file(worker_id)
    cmd = [YTDLP,
           f"https://youtube.com/watch?v={video_id}",
           "--dump-json", "--no-download", "--no-warnings", "--quiet",
           "--js-runtimes", "node",
           "--socket-timeout", "10"]
    if cookie:
        cmd.extend(["--cookies", cookie])
    
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        if r.returncode != 0:
            return video_id, None, None, "error"
        j = json.loads(r.stdout)
        desc = j.get("description", "")
        lic = j.get("license", "")  # "Creative Commons Attribution license (reuse allowed)" or None
        return video_id, desc, lic or "", "ok"
    except Exception as e:
        return video_id, None, None, str(e)

def classify_license(description, youtube_license):
    """Determine if video is CC-licensed."""
    if youtube_license and "creative" in youtube_license.lower():
        return "green"
    if description and CC_PATTERNS.search(description):
        return "green"
    return None

def main():
    conn = get_db()
    
    # Check how many need descriptions
    total_no_desc = conn.execute("""
        SELECT COUNT(*) FROM videos 
        WHERE (description IS NULL OR description = '')
        AND status IN ('completed', 'pending')
    """).fetchone()[0]
    
    total_completed_no_desc = conn.execute("""
        SELECT COUNT(*) FROM videos 
        WHERE (description IS NULL OR description = '')
        AND status = 'completed'
    """).fetchone()[0]
    
    print(f"Videos without descriptions: {total_no_desc:,} total, {total_completed_no_desc:,} completed")
    print(f"Using {WORKERS} parallel workers")
    print()
    
    total_fetched = 0
    total_cc_found = 0
    total_errors = 0
    batch_size = 100
    start_time = time.time()
    
    while True:
        # Get batch — prioritize completed videos
        rows = conn.execute("""
            SELECT video_id FROM videos 
            WHERE (description IS NULL OR description = '')
            AND status IN ('completed', 'pending')
            ORDER BY 
                CASE WHEN status = 'completed' THEN 0 ELSE 1 END,
                RANDOM()
            LIMIT ?
        """, (batch_size,)).fetchall()
        
        if not rows:
            print("No more videos without descriptions!")
            break
        
        video_ids = [r[0] for r in rows]
        
        # Parallel fetch
        results = []
        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = {executor.submit(fetch_one, vid, i % WORKERS): vid 
                      for i, vid in enumerate(video_ids)}
            for future in as_completed(futures):
                results.append(future.result())
        
        # Update DB
        cc_batch = 0
        err_batch = 0
        for vid, desc, lic, status in results:
            if status != "ok" or desc is None:
                err_batch += 1
                total_errors += 1
                # Mark as checked so we don't retry forever
                conn.execute("""
                    UPDATE videos SET description = '[unavailable]'
                    WHERE video_id = ? AND (description IS NULL OR description = '')
                """, (vid,))
                continue
            
            new_risk = classify_license(desc, lic)
            
            if new_risk == "green":
                lic_note = "creativeCommon" if lic and "creative" in lic.lower() else "cc_in_description"
                conn.execute("""
                    UPDATE videos SET description = ?, youtube_license = ?, license_risk = 'green'
                    WHERE video_id = ?
                """, (desc[:5000], lic_note, vid))  # Cap description at 5KB
                cc_batch += 1
                total_cc_found += 1
            else:
                conn.execute("""
                    UPDATE videos SET description = ?, youtube_license = ?
                    WHERE video_id = ?
                """, (desc[:5000], lic or "youtube", vid))
            
            total_fetched += 1
        
        conn.commit()
        
        elapsed = time.time() - start_time
        rate = total_fetched / max(elapsed, 1) * 3600
        cc_pct = (total_cc_found / max(total_fetched, 1)) * 100
        
        print(f"  Fetched: {total_fetched:,} | CC found: {total_cc_found:,} ({cc_pct:.1f}%) | "
              f"Errors: {total_errors:,} | Rate: {rate:.0f}/hr | "
              f"Elapsed: {elapsed/60:.1f}m")
        
        # Brief pause to avoid hammering YouTube
        time.sleep(1)
    
    # Final stats
    print(f"\n{'='*60}")
    print(f"FINAL STATS")
    print(f"{'='*60}")
    print(f"Total fetched: {total_fetched:,}")
    print(f"New CC found: {total_cc_found:,}")
    print(f"Errors: {total_errors:,}")
    
    print("\n--- Updated license_risk breakdown ---")
    for row in conn.execute("SELECT license_risk, COUNT(*) FROM videos GROUP BY license_risk ORDER BY COUNT(*) DESC"):
        print(f"  {row[0] or '(none)'}: {row[1]:,}")
    
    print("\n--- GREEN by youtube_license ---")
    for row in conn.execute("SELECT youtube_license, COUNT(*) FROM videos WHERE license_risk='green' GROUP BY youtube_license ORDER BY COUNT(*) DESC LIMIT 20"):
        print(f"  {row[0] or '(none)'}: {row[1]:,}")
    
    conn.close()
    print("\nDone!")

if __name__ == "__main__":
    main()
