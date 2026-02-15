#!/usr/bin/env python3
"""
Exponential discovery via related videos and playlist walking.
Uses completed/queued videos as seeds → fetch related → find playlists → crawl playlists.
This is the key to going from 100K → 1M+ quality educational videos.
"""

import sqlite3, subprocess, json, os, time, random, re
from concurrent.futures import ThreadPoolExecutor, as_completed

DB_PATH = os.path.expanduser("~/academic_transcriptions/massive_production.db")
YTDLP = os.path.expanduser("~/academic_transcriptions/yt-dlp")
MIN_DURATION = 900  # 15 min

REJECT_PATTERNS = re.compile(
    r'\b(music video|official video|lyric|trailer|reaction|unboxing|prank|asmr|mukbang|'
    r'tiktok|shorts|#shorts|haul|vlog|grwm|day in my life|'
    r'fortnite|minecraft gameplay|roblox|gta v|gaming|let\'s play|walkthrough|playthrough|'
    r'full movie|full episode|movie clip|behind the scenes|bloopers|'
    r'live stream|livestream|24 hour challenge|'
    r'compilation|funny moments|try not to laugh|satisfying|oddly satisfying)\b',
    re.IGNORECASE
)

EDU_BOOST = re.compile(
    r'\b(lecture|course|tutorial|class|seminar|workshop|bootcamp|masterclass|'
    r'university|professor|MIT|Stanford|Harvard|Yale|Berkeley|Oxford|Cambridge|'
    r'OpenCourseWare|NPTEL|khan academy|'
    r'introduction to|fundamentals|chapter \d|lesson \d|week \d|part \d|module \d|'
    r'лекция|курс|Vorlesung|cours|lezione|wykład|강의|講義|课程)\b',
    re.IGNORECASE
)

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def is_good(title, duration):
    if duration and duration < MIN_DURATION:
        return False
    if REJECT_PATTERNS.search(title or ''):
        return False
    return True

def insert_videos(videos, source='related'):
    if not videos: return 0
    conn = get_db()
    n = 0
    for v in videos:
        title = v.get('title', '')
        dur = v.get('duration', 0)
        if not is_good(title, dur):
            continue
        pri = 8 if EDU_BOOST.search(title) else 5
        try:
            conn.execute(
                "INSERT INTO videos (video_id, title, course, university, url, duration_seconds, status, priority) "
                "VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)",
                (v['id'], title, v.get('course', ''), source,
                 f"https://youtube.com/watch?v={v['id']}", dur, pri))
            n += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit(); conn.close()
    return n

def get_seed_ids(batch_size=200):
    """Get seed video IDs from completed and high-priority pending."""
    conn = get_db()
    # Mix of completed (known good) and high-priority pending
    seeds = []
    rows = conn.execute(
        "SELECT video_id FROM videos WHERE status='completed' AND duration_seconds >= ? "
        "ORDER BY RANDOM() LIMIT ?", (MIN_DURATION, batch_size // 2)).fetchall()
    seeds.extend(r[0] for r in rows)
    rows = conn.execute(
        "SELECT video_id FROM videos WHERE status='pending' AND priority >= 8 "
        "ORDER BY RANDOM() LIMIT ?", (batch_size // 2,)).fetchall()
    seeds.extend(r[0] for r in rows)
    conn.close()
    return seeds

def fetch_video_page(video_id):
    """Fetch full video metadata including related videos and playlists.
    Uses yt-dlp to get the video page which includes recommendations."""
    results = {'related': [], 'playlists': [], 'channel': None}
    try:
        cmd = [YTDLP, f"https://youtube.com/watch?v={video_id}",
               "--dump-json", "--no-download", "--no-warnings", "--quiet",
               "--socket-timeout", "15"]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if r.returncode != 0:
            return results
        j = json.loads(r.stdout)
        
        # Extract channel URL for later crawling
        channel_id = j.get('channel_id')
        if channel_id:
            results['channel'] = f"https://www.youtube.com/channel/{channel_id}/videos"
        
        # Extract playlist IDs from description or tags
        desc = j.get('description', '')
        # Find playlist links in description
        playlist_matches = re.findall(r'(?:list=|/playlist\?list=)(PL[\w-]+)', desc)
        for pl in playlist_matches:
            results['playlists'].append(f"https://www.youtube.com/playlist?list={pl}")
        
        # If the video is part of a playlist
        playlist_id = j.get('playlist_id')
        if playlist_id and playlist_id.startswith('PL'):
            results['playlists'].append(f"https://www.youtube.com/playlist?list={playlist_id}")
        
    except:
        pass
    return results

def fetch_related_via_search(video_id, title):
    """Find related videos by searching for similar content."""
    if not title:
        return []
    # Clean title for search - take first meaningful part
    clean = re.sub(r'[|\-–—:]+.*$', '', title).strip()[:60]
    if len(clean) < 10:
        clean = title[:60]
    
    videos = []
    try:
        cmd = [YTDLP, f"ytsearch20:{clean}",
               "--flat-playlist", "--dump-json", "--no-warnings", "--quiet"]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        for line in r.stdout.strip().split('\n'):
            if not line.strip(): continue
            try:
                j = json.loads(line)
                vid = j.get('id', '')
                if vid and len(vid) == 11 and vid != video_id:
                    videos.append({
                        'id': vid,
                        'title': j.get('title', ''),
                        'duration': j.get('duration', 0),
                        'course': clean,
                    })
            except:
                pass
    except:
        pass
    return videos

def crawl_playlist(url):
    """Crawl a full playlist."""
    videos = []
    try:
        cmd = [YTDLP, url, "--flat-playlist", "--dump-json",
               "--no-warnings", "--quiet", "--playlist-end", "500"]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        for line in r.stdout.strip().split('\n'):
            if not line.strip(): continue
            try:
                j = json.loads(line)
                vid = j.get('id', '')
                if vid and len(vid) == 11:
                    videos.append({
                        'id': vid,
                        'title': j.get('title', ''),
                        'duration': j.get('duration', 0),
                    })
            except:
                pass
    except:
        pass
    return videos

def crawl_channel(url):
    """Crawl channel videos ≥15min."""
    videos = []
    try:
        cmd = [YTDLP, url, "--flat-playlist", "--dump-json",
               "--no-warnings", "--quiet",
               "--match-filter", f"duration > {MIN_DURATION}"]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        for line in r.stdout.strip().split('\n'):
            if not line.strip(): continue
            try:
                j = json.loads(line)
                vid = j.get('id', '')
                if vid and len(vid) == 11:
                    videos.append({
                        'id': vid,
                        'title': j.get('title', ''),
                        'duration': j.get('duration', 0),
                    })
            except:
                pass
    except:
        pass
    return videos

def current_stats():
    c = get_db()
    total = c.execute("SELECT count(*) FROM videos").fetchone()[0]
    edu = c.execute(f"SELECT count(*) FROM videos WHERE duration_seconds >= {MIN_DURATION}").fetchone()[0]
    c.close()
    return total, edu

# Track what we've already explored
EXPLORED_FILE = os.path.expanduser("~/academic_transcriptions/explored_seeds.txt")

def load_explored():
    try:
        with open(EXPLORED_FILE) as f:
            return set(f.read().split())
    except:
        return set()

def save_explored(explored):
    with open(EXPLORED_FILE, 'w') as f:
        f.write('\n'.join(explored))

def main():
    total, edu = current_stats()
    print(f"=== Related Video & Playlist Discovery ===")
    print(f"DB: {total} total, {edu} ≥15min")
    print(f"Strategy: seed → related search + page scrape → playlists → channel crawl\n")
    
    explored = load_explored()
    total_new = 0
    playlists_found = set()
    channels_found = set()
    round_num = 0
    
    while True:
        round_num += 1
        t, e = current_stats()
        print(f"\n=== Round {round_num} | DB: {t} total, {e} edu | New this session: +{total_new} ===\n")
        
        if t >= 10_000_000:
            print("🎯 10M reached!")
            break
        
        # Get fresh seeds (excluding already explored)
        seeds = get_seed_ids(300)
        seeds = [s for s in seeds if s not in explored]
        if not seeds:
            print("No unexplored seeds left. Waiting for more completions...")
            time.sleep(60)
            continue
        
        print(f"Processing {len(seeds)} seed videos...\n")
        
        # Step 1: Related video search (fast, high yield)
        print("  [Related search]")
        conn = get_db()
        seed_titles = {}
        for s in seeds[:100]:
            row = conn.execute("SELECT title FROM videos WHERE video_id=?", (s,)).fetchone()
            if row: seed_titles[s] = row[0]
        conn.close()
        
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures = {pool.submit(fetch_related_via_search, vid, title): vid 
                      for vid, title in seed_titles.items()}
            for f in as_completed(futures):
                vid = futures[f]
                explored.add(vid)
                try:
                    related = f.result()
                    if related:
                        n = insert_videos(related, source='related')
                        total_new += n
                        if n >= 3:
                            t, e = current_stats()
                            title = seed_titles.get(vid, vid)[:40]
                            print(f"    {title:40s} → +{n:3d} (edu: {e})")
                except:
                    pass
        
        # Step 2: Scrape video pages for playlists and channels
        print("\n  [Page scraping for playlists/channels]")
        batch = seeds[:50]
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = {pool.submit(fetch_video_page, vid): vid for vid in batch}
            for f in as_completed(futures):
                vid = futures[f]
                explored.add(vid)
                try:
                    info = f.result()
                    for pl in info.get('playlists', []):
                        playlists_found.add(pl)
                    ch = info.get('channel')
                    if ch:
                        channels_found.add(ch)
                except:
                    pass
        
        print(f"    Found {len(playlists_found)} playlists, {len(channels_found)} channels")
        
        # Step 3: Crawl new playlists
        if playlists_found:
            print(f"\n  [Crawling {min(len(playlists_found), 20)} playlists]")
            pl_batch = list(playlists_found)[:20]
            playlists_found -= set(pl_batch)
            
            with ThreadPoolExecutor(max_workers=4) as pool:
                futures = {pool.submit(crawl_playlist, pl): pl for pl in pl_batch}
                for f in as_completed(futures):
                    pl = futures[f]
                    try:
                        vids = f.result()
                        if vids:
                            n = insert_videos(vids, source=f'playlist')
                            total_new += n
                            if n > 0:
                                t, e = current_stats()
                                pl_short = pl.split('list=')[-1][:20] if 'list=' in pl else pl[-30:]
                                print(f"    PL:{pl_short:20s} +{n:4d} ({len(vids)} in playlist, edu: {e})")
                    except:
                        pass
        
        # Step 4: Crawl new channels (slower, do fewer per round)
        if channels_found:
            ch_batch = list(channels_found)[:5]
            channels_found -= set(ch_batch)
            print(f"\n  [Crawling {len(ch_batch)} channels]")
            
            with ThreadPoolExecutor(max_workers=3) as pool:
                futures = {pool.submit(crawl_channel, ch): ch for ch in ch_batch}
                for f in as_completed(futures):
                    ch = futures[f]
                    try:
                        vids = f.result()
                        if vids:
                            n = insert_videos(vids, source='channel_crawl')
                            total_new += n
                            if n > 0:
                                t, e = current_stats()
                                ch_name = ch.split('/')[-2] if '/videos' in ch else ch.split('/')[-1]
                                print(f"    CH:{ch_name:25s} +{n:4d} (edu: {e})")
                    except:
                        pass
        
        # Save progress
        save_explored(explored)
        
        # Brief pause between rounds
        time.sleep(2)

if __name__ == "__main__":
    main()
