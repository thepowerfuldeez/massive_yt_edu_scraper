#!/usr/bin/env python3
"""
Channel-based discovery to reach 10M+ videos.

Strategy:
1. Extract channel IDs from existing videos (completed + pending)
2. Crawl ENTIRE channel catalogs (all videos, not just search hits)
3. Discover related channels via featured channels / channel recommendations
4. Snowball: new channels → crawl → find more channels → repeat

This is the only way to 10x from 1.28M → 10M+.
"""

import sqlite3, subprocess, json, os, time, random, re, sys
from concurrent.futures import ThreadPoolExecutor, as_completed

DB_PATH = os.path.expanduser("~/academic_transcriptions/massive_production.db")
YTDLP = os.path.expanduser("~/academic_transcriptions/yt-dlp")
MIN_DURATION = 900
CHANNELS_DB = os.path.expanduser("~/academic_transcriptions/channels.db")

REJECT_PATTERNS = re.compile(
    r'\b(music video|official video|lyric|trailer|reaction|unboxing|prank|asmr|mukbang|'
    r'tiktok|shorts|#shorts|haul|vlog|grwm|day in my life|'
    r'fortnite|minecraft gameplay|roblox|gta v|gaming|let.s play|walkthrough|playthrough|'
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

# Channels that are definitely NOT educational
REJECT_CHANNELS = re.compile(
    r'\b(VEVO|Music|Records|Gaming|Gameplay|Clips|Highlights|'
    r'TV Show|Movie|Trailer|Entertainment|Comedy|Prank|'
    r'ASMR|Mukbang|Cooking Show|Reality|Drama|'
    r'News Network|ESPN|Sports|NBA|NFL|FIFA)\b',
    re.IGNORECASE
)

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def get_channels_db():
    conn = sqlite3.connect(CHANNELS_DB, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS channels (
        channel_id TEXT PRIMARY KEY,
        channel_name TEXT,
        video_count INTEGER DEFAULT 0,
        edu_video_count INTEGER DEFAULT 0,
        crawled_at TEXT,
        source TEXT DEFAULT 'extracted',
        status TEXT DEFAULT 'pending'
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS channel_discoveries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        from_channel TEXT,
        to_channel TEXT,
        discovered_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(from_channel, to_channel)
    )""")
    conn.commit()
    return conn

def insert_videos(videos, source='channel_crawl'):
    if not videos:
        return 0
    conn = get_db()
    n = 0
    for v in videos:
        title = v.get('title', '')
        dur = v.get('duration')
        if dur is not None and dur < MIN_DURATION:
            continue
        if REJECT_PATTERNS.search(title or ''):
            continue
        pri = 8 if EDU_BOOST.search(title) else 5
        vid = v.get('id', '')
        if not vid:
            continue
        try:
            conn.execute(
                "INSERT INTO videos (video_id, title, course, university, url, duration_seconds, status, priority) "
                "VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)",
                (vid, title, v.get('playlist', ''), source,
                 f"https://youtube.com/watch?v={vid}", dur or 0, pri))
            n += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    conn.close()
    return n


def extract_channels_from_db(batch_size=500):
    """Get random video IDs from DB, extract their channel IDs."""
    conn = get_db()
    cdb = get_channels_db()
    
    # Get already-known channel IDs
    known = set(r[0] for r in cdb.execute("SELECT channel_id FROM channels").fetchall())
    print(f"Already know {len(known)} channels")
    
    # Sample videos to extract channels from
    rows = conn.execute(
        "SELECT video_id FROM videos WHERE status IN ('completed', 'pending') "
        "ORDER BY RANDOM() LIMIT ?", (batch_size,)).fetchall()
    conn.close()
    
    new_channels = 0
    
    def extract_one(video_id):
        try:
            r = subprocess.run(
                [YTDLP, '--js-runtimes', 'node', '--print', 'channel_id',
                 '--print', 'channel', '--skip-download',
                 f'https://youtube.com/watch?v={video_id}'],
                capture_output=True, text=True, timeout=30)
            lines = r.stdout.strip().split('\n')
            if len(lines) >= 2 and lines[0].startswith('UC'):
                return lines[0], lines[1]
        except:
            pass
        return None, None
    
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(extract_one, r[0]): r[0] for r in rows}
        for f in as_completed(futures):
            ch_id, ch_name = f.result()
            if ch_id and ch_id not in known:
                known.add(ch_id)
                try:
                    cdb.execute(
                        "INSERT INTO channels (channel_id, channel_name, source) VALUES (?, ?, 'extracted')",
                        (ch_id, ch_name))
                    new_channels += 1
                except sqlite3.IntegrityError:
                    pass
    
    cdb.commit()
    cdb.close()
    print(f"Extracted {new_channels} new channels from {batch_size} videos")
    return new_channels


def crawl_channel_full(channel_id, channel_name=""):
    """Crawl ALL videos from a channel using yt-dlp flat playlist extraction."""
    try:
        r = subprocess.run(
            [YTDLP, '--js-runtimes', 'node', '--flat-playlist', '-J',
             '--no-warnings', '--extractor-args', 'youtube:approximate_date',
             f'https://www.youtube.com/channel/{channel_id}/videos'],
            capture_output=True, text=True, timeout=120)
        
        if r.returncode != 0:
            return [], []
        
        data = json.loads(r.stdout)
        entries = data.get('entries', [])
        
        videos = []
        for e in entries:
            vid = e.get('id') or e.get('url', '').split('=')[-1]
            if not vid or len(vid) != 11:
                continue
            videos.append({
                'id': vid,
                'title': e.get('title', ''),
                'duration': e.get('duration'),
                'playlist': channel_name or channel_id,
            })
        
        # Also try to get featured/related channels
        related_channels = []
        try:
            r2 = subprocess.run(
                [YTDLP, '--js-runtimes', 'node', '--flat-playlist', '-J',
                 '--no-warnings',
                 f'https://www.youtube.com/channel/{channel_id}/channels'],
                capture_output=True, text=True, timeout=60)
            if r2.returncode == 0:
                data2 = json.loads(r2.stdout)
                for e in data2.get('entries', []):
                    rid = e.get('channel_id') or e.get('id', '')
                    if rid.startswith('UC'):
                        related_channels.append((rid, e.get('title', '')))
        except:
            pass
        
        return videos, related_channels
    except Exception as ex:
        print(f"  Error crawling {channel_id}: {ex}")
        return [], []


def crawl_playlist(playlist_id):
    """Crawl a YouTube playlist."""
    try:
        r = subprocess.run(
            [YTDLP, '--js-runtimes', 'node', '--flat-playlist', '-J',
             '--no-warnings',
             f'https://www.youtube.com/playlist?list={playlist_id}'],
            capture_output=True, text=True, timeout=120)
        
        if r.returncode != 0:
            return []
        
        data = json.loads(r.stdout)
        videos = []
        for e in data.get('entries', []):
            vid = e.get('id') or e.get('url', '').split('=')[-1]
            if not vid or len(vid) != 11:
                continue
            videos.append({
                'id': vid,
                'title': e.get('title', ''),
                'duration': e.get('duration'),
            })
        return videos
    except:
        return []


def discover_playlists_from_channel(channel_id):
    """Get all playlists from a channel."""
    try:
        r = subprocess.run(
            [YTDLP, '--js-runtimes', 'node', '--flat-playlist', '-J',
             '--no-warnings',
             f'https://www.youtube.com/channel/{channel_id}/playlists'],
            capture_output=True, text=True, timeout=120)
        
        if r.returncode == 0:
            data = json.loads(r.stdout)
            playlists = []
            for e in data.get('entries', []):
                pid = e.get('id', '')
                if pid.startswith('PL') or pid.startswith('UU'):
                    playlists.append((pid, e.get('title', '')))
            return playlists
    except:
        pass
    return []


def is_educational_channel(channel_name, videos):
    """Heuristic: is this channel worth crawling?"""
    if REJECT_CHANNELS.search(channel_name or ''):
        return False
    
    if not videos:
        return True  # give benefit of doubt, filter at video level
    
    # Check sample of titles
    edu_count = sum(1 for v in videos[:50] if EDU_BOOST.search(v.get('title', '')))
    long_count = sum(1 for v in videos[:50] if (v.get('duration') or 0) >= MIN_DURATION)
    
    sample = min(len(videos), 50)
    if sample == 0:
        return True
    
    # At least 20% educational titles OR 30% long videos
    return (edu_count / sample >= 0.2) or (long_count / sample >= 0.3)


def main():
    print("=" * 60)
    print("=== CHANNEL-BASED 10M DISCOVERY ===")
    print("=" * 60)
    
    cdb = get_channels_db()
    total_start = get_db().execute("SELECT COUNT(*) FROM videos").fetchone()[0]
    print(f"Starting DB size: {total_start:,}")
    
    round_num = 0
    while True:
        round_num += 1
        round_start = time.time()
        
        # Phase 1: Extract channels from existing videos
        print(f"\n{'='*60}")
        print(f"=== ROUND {round_num} ===")
        print(f"{'='*60}")
        
        known_count = cdb.execute("SELECT COUNT(*) FROM channels").fetchone()[0]
        pending_channels = cdb.execute(
            "SELECT COUNT(*) FROM channels WHERE status='pending'").fetchone()[0]
        
        print(f"Known channels: {known_count:,} | Pending: {pending_channels:,}")
        
        # Extract more channels if we're running low on pending
        if pending_channels < 500:
            print(f"\n[Phase 1] Extracting channels from existing videos...")
            extract_channels_from_db(batch_size=1000)
            pending_channels = cdb.execute(
                "SELECT COUNT(*) FROM channels WHERE status='pending'").fetchone()[0]
            print(f"Pending channels after extraction: {pending_channels:,}")
        
        # Phase 2: Crawl pending channels
        print(f"\n[Phase 2] Crawling channels...")
        channels = cdb.execute(
            "SELECT channel_id, channel_name FROM channels WHERE status='pending' "
            "ORDER BY RANDOM() LIMIT 100").fetchall()
        
        round_new = 0
        new_channels_found = 0
        
        for i, (ch_id, ch_name) in enumerate(channels):
            videos, related = crawl_channel_full(ch_id, ch_name)
            
            # Check if educational
            if not is_educational_channel(ch_name, videos):
                cdb.execute(
                    "UPDATE channels SET status='rejected', video_count=?, crawled_at=CURRENT_TIMESTAMP "
                    "WHERE channel_id=?", (len(videos), ch_id))
                cdb.commit()
                print(f"  [{i+1}/{len(channels)}] {ch_name or ch_id[:12]} — REJECTED ({len(videos)} videos)")
                continue
            
            # Insert videos
            n = insert_videos(videos, source=ch_name or ch_id)
            round_new += n
            
            # Also crawl playlists for this channel
            playlists = discover_playlists_from_channel(ch_id)
            pl_new = 0
            for pid, pname in playlists[:20]:  # cap at 20 playlists per channel
                pl_videos = crawl_playlist(pid)
                pl_n = insert_videos([{**v, 'playlist': pname} for v in pl_videos], source=ch_name or ch_id)
                pl_new += pl_n
            
            round_new += pl_new
            
            # Register related channels
            for rch_id, rch_name in related:
                try:
                    cdb.execute(
                        "INSERT INTO channels (channel_id, channel_name, source) VALUES (?, ?, 'related')",
                        (rch_id, rch_name))
                    new_channels_found += 1
                except sqlite3.IntegrityError:
                    pass
                try:
                    cdb.execute(
                        "INSERT INTO channel_discoveries (from_channel, to_channel) VALUES (?, ?)",
                        (ch_id, rch_id))
                except sqlite3.IntegrityError:
                    pass
            
            # Mark channel as crawled
            cdb.execute(
                "UPDATE channels SET status='crawled', video_count=?, edu_video_count=?, "
                "crawled_at=CURRENT_TIMESTAMP WHERE channel_id=?",
                (len(videos), n, ch_id))
            cdb.commit()
            
            total_vids = len(videos)
            print(f"  [{i+1}/{len(channels)}] {ch_name or ch_id[:12]:40s} "
                  f"vids: {total_vids:>5} | +{n + pl_new:>4} new | "
                  f"playlists: {len(playlists):>3} | related_ch: {len(related):>3}")
            
            # Rate limit slightly
            time.sleep(0.5)
        
        # Phase 3: Search for MORE educational channels directly
        print(f"\n[Phase 3] Searching for educational channels...")
        channel_search_queries = [
            "university lectures", "online courses", "MOOC", "academic talks",
            "conference presentations", "MIT OpenCourseWare", "NPTEL",
            "CS lectures", "math lectures", "physics lectures", "biology lectures",
            "medical lectures", "engineering tutorials", "data science course",
            "machine learning course", "deep learning lectures",
            "programming tutorials", "algorithms course",
            "economics lectures", "history lectures", "philosophy lectures",
            "chemistry lectures", "astronomy lectures",
            "курсы лекции университет", "Vorlesung Universität",
            "cours magistral université", "lezioni università",
            "wykłady uniwersytet", "강의 대학교", "大学 講義",
            "IIT lectures", "Stanford online", "Yale courses",
            "Harvard lectures", "Oxford talks", "Cambridge lectures",
            "TED-Ed education", "Coursera lectures", "edX courses",
            "cybersecurity training", "AWS training", "Google tech talks",
            "PyCon talks", "JSConf talks", "GopherCon", "RustConf",
            "NeurIPS", "ICML", "CVPR", "ACL conference",
            "SIGCOMM", "USENIX", "DEF CON talks", "Black Hat",
        ]
        random.shuffle(channel_search_queries)
        
        for q in channel_search_queries[:15]:  # 15 per round
            try:
                r = subprocess.run(
                    [YTDLP, '--js-runtimes', 'node', '--flat-playlist', '-J',
                     '--no-warnings',
                     f'ytsearch20:{q} full course lecture'],
                    capture_output=True, text=True, timeout=60)
                
                if r.returncode == 0:
                    data = json.loads(r.stdout)
                    for e in data.get('entries', []):
                        ch_id = e.get('channel_id', '')
                        ch_name = e.get('channel', '')
                        if ch_id and ch_id.startswith('UC'):
                            try:
                                cdb.execute(
                                    "INSERT INTO channels (channel_id, channel_name, source) "
                                    "VALUES (?, ?, 'search')", (ch_id, ch_name))
                                new_channels_found += 1
                            except sqlite3.IntegrityError:
                                pass
                        
                        # Also insert the video itself
                        vid = e.get('id', '')
                        if vid and len(vid) == 11:
                            insert_videos([{
                                'id': vid,
                                'title': e.get('title', ''),
                                'duration': e.get('duration'),
                            }], source='channel_search')
                            round_new += 1
                
                cdb.commit()
            except:
                pass
            time.sleep(1)
        
        elapsed = time.time() - round_start
        db_total = get_db().execute("SELECT COUNT(*) FROM videos").fetchone()[0]
        pending = cdb.execute("SELECT COUNT(*) FROM channels WHERE status='pending'").fetchone()[0]
        crawled = cdb.execute("SELECT COUNT(*) FROM channels WHERE status='crawled'").fetchone()[0]
        
        print(f"\n=== ROUND {round_num} COMPLETE | "
              f"+{round_new:,} videos | +{new_channels_found} channels | "
              f"{elapsed:.0f}s ===")
        print(f"DB: {db_total:,} total | "
              f"Channels: {crawled:,} crawled, {pending:,} pending")
        
        if db_total >= 10_000_000:
            print(f"\n🎯 TARGET REACHED: {db_total:,} videos!")
            break
        
        # Brief pause between rounds
        time.sleep(2)


if __name__ == '__main__':
    main()
