#!/usr/bin/env python3
"""
CC-focused discovery: find videos related to known CC content.
Strategy:
1. Use completed GREEN/CC videos as seeds
2. Fetch related videos from those seeds
3. Crawl channels that have high CC rates
4. Search for CC-licensed educational content
"""
import sqlite3, subprocess, json, os, time, random, re, sys
from concurrent.futures import ThreadPoolExecutor, as_completed

DB_PATH = os.path.expanduser("~/academic_transcriptions/massive_production.db")
YTDLP = os.path.expanduser("~/academic_transcriptions/yt-dlp")
COOKIE_DIR = os.path.expanduser("~/academic_transcriptions/cookie_pool")
MIN_DURATION = 900

REJECT_PATTERNS = re.compile(
    r'\b(music video|official video|lyric|trailer|reaction|unboxing|prank|asmr|mukbang|'
    r'tiktok|shorts|#shorts|haul|vlog|grwm|day in my life|'
    r'fortnite|minecraft gameplay|roblox|gta v|gaming|let.s play|walkthrough|playthrough|'
    r'full movie|full episode|movie clip|behind the scenes|bloopers|'
    r'live stream|livestream|24 hour challenge|'
    r'compilation|funny moments|try not to laugh|satisfying|oddly satisfying|'
    r'sermon|prayer|worship|bible study|quran|gospel|church service)\b',
    re.IGNORECASE
)

EDU_BOOST = re.compile(
    r'\b(lecture|course|tutorial|class|seminar|workshop|bootcamp|masterclass|'
    r'university|professor|MIT|Stanford|Harvard|Yale|Berkeley|Oxford|Cambridge|'
    r'OpenCourseWare|NPTEL|khan academy|'
    r'introduction to|fundamentals|chapter \d|lesson \d|week \d|part \d|module \d)\b',
    re.IGNORECASE
)

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def get_cookie():
    cookies = sorted([f for f in os.listdir(COOKIE_DIR) if f.endswith('.txt')])
    if not cookies: return None
    return os.path.join(COOKIE_DIR, random.choice(cookies))

def is_good(title, duration):
    if duration and duration < MIN_DURATION: return False
    if REJECT_PATTERNS.search(title or ''): return False
    return True

def insert_videos(videos, source='cc_discovery'):
    if not videos: return 0
    conn = get_db()
    n = 0
    for v in videos:
        title = v.get('title', '')
        dur = v.get('duration') or 0
        vid = v.get('id', '')
        if not vid or not is_good(title, dur): continue
        pri = 9 if EDU_BOOST.search(title) else 7  # Higher base priority for CC-adjacent
        try:
            conn.execute(
                "INSERT INTO videos (video_id, title, course, university, url, duration_seconds, status, priority) "
                "VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)",
                (vid, title, v.get('playlist', ''), source,
                 f"https://youtube.com/watch?v={vid}", dur, pri))
            n += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit(); conn.close()
    return n

def get_cc_seeds(batch_size=100):
    """Get video IDs from GREEN/CC content as seeds."""
    conn = get_db()
    rows = conn.execute("""
        SELECT video_id FROM videos 
        WHERE license_risk = 'green' AND status = 'completed'
        ORDER BY RANDOM() LIMIT ?
    """, (batch_size,)).fetchall()
    if len(rows) < batch_size:
        # Also use pending GREEN
        more = conn.execute("""
            SELECT video_id FROM videos 
            WHERE license_risk = 'green' AND status = 'pending'
            ORDER BY RANDOM() LIMIT ?
        """, (batch_size - len(rows),)).fetchall()
        rows.extend(more)
    conn.close()
    return [r[0] for r in rows]

def get_cc_channels():
    """Find channels with high CC rates from scanned videos."""
    conn = get_db()
    # Find channels where >30% of scanned videos are CC
    # We use the youtube_license field from the API scan
    rows = conn.execute("""
        SELECT university, 
               COUNT(*) as total,
               SUM(CASE WHEN youtube_license = 'creativeCommon' THEN 1 ELSE 0 END) as cc_count
        FROM videos 
        WHERE youtube_license IN ('creativeCommon', 'youtube')
        AND university != '' AND university NOT LIKE '%crawl%' AND university NOT LIKE '%search%'
        GROUP BY university
        HAVING total >= 5 AND cc_count * 1.0 / total > 0.3
        ORDER BY cc_count DESC
        LIMIT 100
    """).fetchall()
    conn.close()
    return [(r[0], r[1], r[2]) for r in rows]

def fetch_related(video_id):
    """Fetch related/recommended videos from a seed."""
    cookie = get_cookie()
    cmd = [YTDLP, f"https://youtube.com/watch?v={video_id}",
           "--dump-json", "--no-download", "--no-warnings", "--quiet",
           "--js-runtimes", "node", "--socket-timeout", "15"]
    if cookie: cmd.extend(["--cookies", cookie])
    
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if r.returncode != 0: return []
        j = json.loads(r.stdout)
        
        # Get channel videos as related content
        channel_id = j.get('channel_id', '')
        channel_url = j.get('channel_url', '')
        results = []
        
        # Also try to get playlist entries if video is in a playlist
        for entry in j.get('entries', []) or []:
            if isinstance(entry, dict):
                results.append({
                    'id': entry.get('id', ''),
                    'title': entry.get('title', ''),
                    'duration': entry.get('duration', 0),
                    'playlist': f'related:{video_id}',
                })
        
        return results
    except:
        return []

def crawl_channel(channel_id, max_videos=300):
    """Crawl videos from a channel."""
    cookie = get_cookie()
    url = f"https://www.youtube.com/channel/{channel_id}/videos"
    cmd = [YTDLP, url,
           "--dump-json", "--flat-playlist", "--no-download",
           "--no-warnings", "--quiet",
           "--js-runtimes", "node", "--socket-timeout", "20",
           "--playlist-end", str(max_videos)]
    if cookie: cmd.extend(["--cookies", cookie])
    
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        videos = []
        for line in r.stdout.strip().split('\n'):
            if not line: continue
            try:
                j = json.loads(line)
                videos.append({
                    'id': j.get('id', ''),
                    'title': j.get('title', ''),
                    'duration': j.get('duration') or 0,
                    'playlist': f'cc_channel:{channel_id}',
                })
            except: pass
        return videos
    except:
        return []

def yt_search(query, max_results=100):
    """Search YouTube."""
    cookie = get_cookie()
    cmd = [YTDLP, f"ytsearch{max_results}:{query}",
           "--dump-json", "--flat-playlist", "--no-download",
           "--no-warnings", "--quiet",
           "--js-runtimes", "node", "--socket-timeout", "15",
           "--match-filter", f"duration >= {MIN_DURATION}"]
    if cookie: cmd.extend(["--cookies", cookie])
    
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        videos = []
        for line in r.stdout.strip().split('\n'):
            if not line: continue
            try:
                j = json.loads(line)
                videos.append({
                    'id': j.get('id', ''),
                    'title': j.get('title', ''),
                    'duration': j.get('duration'),
                    'playlist': f'cc_search:{query[:50]}',
                })
            except: pass
        return videos
    except:
        return []

# CC-focused search queries
CC_SEARCHES = [
    "creative commons lecture",
    "creative commons tutorial",
    "creative commons course",
    "open educational resources lecture",
    "OER lecture full",
    "opencourseware full lecture",
    "MIT OpenCourseWare",
    "NPTEL full course",
    "Khan Academy full course",
    "Yale open course",
    "Stanford Engineering Everywhere",
    "UC Berkeley full lecture",
    "IIT lecture NPTEL",
    "open university lecture",
    "TU Delft open course",
    "free university lecture full",
    "open access lecture series",
    "academic lecture creative commons",
    "machine learning lecture open",
    "computer science lecture open course",
    "mathematics lecture opencourseware",
    "physics lecture MIT open",
    "chemistry lecture open course",
    "biology lecture university open",
    "economics lecture open course",
    "history lecture yale open",
    "philosophy lecture open university",
    "engineering lecture NPTEL",
    "data science lecture full course free",
    "algorithms lecture full course",
]

def main():
    total_added = 0
    round_num = 0
    
    while True:
        round_num += 1
        round_added = 0
        
        # Phase 1: Related videos from CC seeds
        print(f"\n{'='*60}")
        print(f"ROUND {round_num} — Phase 1: CC seed related discovery")
        print(f"{'='*60}")
        
        seeds = get_cc_seeds(50)
        print(f"  Got {len(seeds)} CC seeds")
        
        for i, seed in enumerate(seeds[:20]):  # Don't do all 50, too slow
            videos = fetch_related(seed)
            n = insert_videos(videos, 'cc_related')
            round_added += n
            if videos:
                print(f"  [{i+1}] seed {seed}: {len(videos)} related, {n} new")
            time.sleep(random.uniform(1, 3))
        
        # Phase 2: Crawl high-CC-rate channels
        print(f"\n{'='*60}")
        print(f"ROUND {round_num} — Phase 2: High-CC-rate channel crawling")
        print(f"{'='*60}")
        
        cc_channels = get_cc_channels()
        print(f"  Found {len(cc_channels)} channels with >30% CC rate")
        
        for src, total, cc_count in cc_channels[:10]:
            pct = cc_count / total * 100
            # Try to extract channel ID from source field
            # These are stored in university field from discovery
            print(f"  Crawling source '{src[:50]}' ({cc_count}/{total} = {pct:.0f}% CC)")
            # Search for more from this source
            videos = yt_search(f"{src} lecture", 50)
            n = insert_videos(videos, f'cc_channel_expand:{src[:30]}')
            round_added += n
            print(f"    Found {len(videos)} videos, {n} new")
            time.sleep(random.uniform(2, 5))
        
        # Phase 3: CC-focused searches
        print(f"\n{'='*60}")
        print(f"ROUND {round_num} — Phase 3: CC-focused search queries")
        print(f"{'='*60}")
        
        queries = random.sample(CC_SEARCHES, min(15, len(CC_SEARCHES)))
        for i, q in enumerate(queries):
            print(f"  [{i+1}/{len(queries)}] Searching: {q}")
            videos = yt_search(q, 100)
            n = insert_videos(videos, 'cc_search')
            round_added += n
            print(f"    Found {len(videos)} videos, {n} new")
            time.sleep(random.uniform(3, 8))
        
        total_added += round_added
        
        conn = get_db()
        green_pending = conn.execute("SELECT COUNT(*) FROM videos WHERE license_risk='green' AND status='pending'").fetchone()[0]
        total_pending = conn.execute("SELECT COUNT(*) FROM videos WHERE status='pending'").fetchone()[0]
        conn.close()
        
        print(f"\n>>> Round {round_num}: +{round_added} new | Session total: {total_added} | GREEN pending: {green_pending:,} | Total pending: {total_pending:,}")
        
        time.sleep(60)  # Pause between rounds

if __name__ == "__main__":
    main()
