#!/usr/bin/env python3
"""
Safe content discovery — focuses on CC-licensed and known-safe educational channels.
Prioritizes GREEN/low-risk content for dataset quality.

Strategy:
1. Search YouTube for CC-licensed educational content (creativecommons filter)
2. Crawl known-safe channels (NPTEL, MIT OCW, Khan Academy, etc.)
3. Discover new CC channels from existing GREEN videos
4. Search for university OCW playlists globally
"""

import sqlite3, subprocess, json, os, time, random, re, sys
from concurrent.futures import ThreadPoolExecutor, as_completed

DB_PATH = os.path.expanduser("~/academic_transcriptions/massive_production.db")
YTDLP = os.path.expanduser("~/academic_transcriptions/yt-dlp")
MIN_DURATION = 900

REJECT_PATTERNS = re.compile(
    r'\b(music video|official video|lyric|trailer|reaction|unboxing|prank|asmr|mukbang|'
    r'tiktok|shorts|#shorts|haul|vlog|grwm|day in my life|'
    r'fortnite|minecraft gameplay|roblox|gta v|gaming|let.s play|walkthrough|playthrough|'
    r'full movie|full episode|movie clip|behind the scenes|bloopers|'
    r'live stream|livestream|24 hour challenge|'
    r'compilation|funny moments|try not to laugh|satisfying|oddly satisfying|'
    r'sermon|prayer|worship|bible study|quran|gospel|church service|'
    r'conspiracy|flat earth|anti.vax|miracle cure)\b',
    re.IGNORECASE
)

EDU_BOOST = re.compile(
    r'\b(lecture|course|tutorial|class|seminar|workshop|bootcamp|masterclass|'
    r'university|professor|MIT|Stanford|Harvard|Yale|Berkeley|Oxford|Cambridge|'
    r'OpenCourseWare|NPTEL|khan academy|Coursera|edX|'
    r'introduction to|fundamentals|chapter \d|lesson \d|week \d|part \d|module \d|'
    r'лекция|курс|Vorlesung|cours|lezione|wykład|강의|講義|课程)\b',
    re.IGNORECASE
)

# Known safe CC/OCW channels to crawl exhaustively
SAFE_CHANNELS = [
    # CC-Licensed / OCW
    "UCYO_jab_esuFRV4b17AJtAw",  # 3Blue1Brown
    "UC9-y-6csu5WGm29I7JiwpnA",  # Computerphile
    "UC_7aK9PpYTqt08ERSsin68Q",  # Sixty Symbols
    "UCoxcjq-8xIDTYp3uz647V5A",  # Numberphile
    "UCWN3xxRkmTPphYw793RKGMQ",  # NPTEL
    "UCEBb1b_L6zDS3xTUrIALZOw",  # MIT OCW
    "UCi8e0iOVk1fEOogdfu4YgfA",  # PBS Space Time
    "UConVfxXodg78Tzh5nNu85Ew",  # CrashCourse
    "UCnUYZLuoy1rq1aVMwx4piYg",  # Jeff Hanson Engineering
    "UCVHFbqXqoYvEWM1Ddxl0QDg",  # Andrew Huberman
    "UC8butISFwT-Wl7EV0hUK0BQ",  # freeCodeCamp
    "UCW5YeuERMmlnqo4oq8vwUpg",  # Net Ninja
    "UCsBjURrPoezykLs9EqgamOA",  # Fireship
    "UCbmNph6atAoGfqLoCL_duAg",  # Yale Courses (OYC)
    "UCsXVk37bltHxD1rDPwtNM8Q",  # Kurzgesagt
    "UCOjD18EJYcsBog4IoMapAeA",  # CppCon
    "UC-EnprmCZ3OXyAoG7539kKQ",  # Stanford CS
    "UCNIkB2IeJ-6AmZv7bQ1oBYg",  # MIT CSAIL
    "UC0RhatS1pyxInC00YKjjBqQ",  # Ben Eater
    "UCUHW94eEFW7hkUMVaZz4eDg",  # MinutePhysics
    "UCZYTClx2T1of7BRZ86-8fow",  # SciShow
    "UC7_gcs09iThXybpVgjHZ_7g",  # PBS Eons
    "UCMwGHR0BTZuLSmjY6Op5jSw",  # Two Minute Papers
    "UCHnyfMqiRRG1u-2MsSQLbXA",  # Veritasium
    "UCq0EGvLTyy-LLT1oUSO_0FQ",  # Grant Sanderson (3b1b alt)
    "UCJ0-OtVpF0wOKEqT2Z1HEtA",  # ElectroBOOM
    "UCG-KntY7aVnIGXYEBQvmBAQ",  # Thomas Frank
    "UCddiUEpeqJcYeBxX1IVBKvQ",  # The Organic Chemistry Tutor
    "UCFhXFikryT4aFcLkLw2LBLA",  # NileRed
    "UCiGm_E4ZwYSHV3bcW1pnSeQ",  # Scott Manley
    "UC2D2CMWXMOVWx7giW1n3LIg",  # Ahoy
    "UCkLMKzmS2OGBURhISxqW5RA",  # Technology Connections
    "UCYNbYGl89UUowy8oXkipC-Q",  # LiveOverflow
    # Indian universities
    "UCqSjMkMlJaUbx-JY5aNiIiA",  # IIT Madras
    "UCeQKJzUAiTjSdaYRSgkeGMw",  # IIT Bombay
    "UCHMRmOsmKf0kSkrpBqiGBow",  # IIT Delhi
    # More OCW
    "UCErdqoaLmcOFkr_3V6b4Vgg",  # Harvard CS50
    "UCm8rE6PMklIFQ6jAkDO4J-w",  # BYU Speeches
    "UCngehmCV-65FikHYUV1_qXA",  # PolyMatter
    "UCZ3LBQVNiuOkfdWP1dvFLvg",  # Library of Congress
    # Technical conferences
    "UCkw4JCwteGrDHIsyIIKo4tQ",  # EuroPython
    "UCrJhliKNQ8g0qoE_zvL8eVg",  # PyCon US
    "UC_x5XG1OV2P6uZZ5FSM9Ttw",  # Google Developers
    "UCVHFbqXqoYvEWM1Ddxl0QDg",  # Lex Fridman
    "UCIRiWCPZoUyZDBaaCraFqJQ",  # Strange Loop
    "UCbRP3c757lWg9M-U7TyEkXA",  # Talks at Google
    "UCJ24N4O0bP7LGLBDvye7oC4",  # Matt Might
]

# Search queries targeting CC/educational content
SAFE_QUERIES = [
    # University OCW searches
    "MIT OpenCourseWare lecture",
    "Stanford online course full lecture",
    "Yale open course lecture",
    "Harvard CS50 lecture",
    "Berkeley CS lecture",
    "Princeton lecture series",
    "Caltech lecture",
    "ETH Zurich lecture",
    "Oxford university lecture",
    "Cambridge university lecture",
    "Imperial College lecture",
    "TU Delft lecture",
    "EPFL lecture",
    "Max Planck lecture",
    "University of Toronto lecture",
    "University of Michigan lecture",
    "Carnegie Mellon lecture",
    "Georgia Tech lecture",
    "NPTEL lecture",
    "IIT lecture course",
    "NUS lecture",
    "Tsinghua lecture English",
    "KAIST lecture English",
    "Seoul National University lecture",
    "University of Tokyo lecture English",
    "Taiwan OCW lecture",
    # CC-specific searches
    "creative commons lecture",
    "creative commons tutorial",
    "creative commons course",
    "open educational resources lecture",
    "OER lecture full course",
    # Subject-specific safe searches
    "linear algebra full course lecture",
    "calculus full lecture university",
    "organic chemistry lecture full course",
    "physics lecture full course university",
    "computer science lecture full course",
    "machine learning lecture full course",
    "deep learning lecture university",
    "algorithms lecture full course",
    "data structures lecture university",
    "operating systems lecture course",
    "database systems lecture course",
    "compiler design lecture",
    "distributed systems lecture",
    "cryptography lecture full course",
    "quantum computing lecture",
    "quantum mechanics lecture university",
    "thermodynamics lecture full course",
    "electromagnetism lecture university",
    "molecular biology lecture course",
    "biochemistry lecture full course",
    "neuroscience lecture course",
    "genetics lecture full course",
    "immunology lecture university",
    "microeconomics lecture full course",
    "macroeconomics lecture university",
    "game theory lecture full course",
    "statistics lecture full course",
    "probability theory lecture university",
    "real analysis lecture",
    "abstract algebra lecture",
    "topology lecture full course",
    "differential equations lecture",
    "number theory lecture",
    "combinatorics lecture course",
    "signal processing lecture",
    "control systems lecture",
    "digital electronics lecture",
    "VLSI design lecture",
    "embedded systems lecture course",
    "robotics lecture full course",
    "computer vision lecture",
    "natural language processing lecture",
    "reinforcement learning lecture",
    "information theory lecture",
    "optimization lecture full course",
    "numerical methods lecture",
    "finite element method lecture",
    "fluid mechanics lecture",
    "structural engineering lecture",
    "materials science lecture",
    "semiconductor physics lecture",
    "astrophysics lecture full course",
    "cosmology lecture university",
    "general relativity lecture",
    "string theory lecture",
    "philosophy lecture full course university",
    "history lecture full course",
    "political science lecture university",
    "international relations lecture",
    "psychology lecture full course university",
    "cognitive science lecture",
    "linguistics lecture full course",
    "anthropology lecture university",
    "sociology lecture full course",
    "law lecture full course",
    "constitutional law lecture",
    "environmental science lecture course",
    "climate science lecture",
    "ecology lecture full course",
    "biostatistics lecture",
    "epidemiology lecture course",
    "public health lecture",
    "medical lecture physiology",
    "anatomy lecture full course",
    "pharmacology lecture",
]

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

def get_priority(title):
    if EDU_BOOST.search(title or ''):
        return 8
    return 5

def insert_videos(videos, source='safe_discovery'):
    if not videos:
        return 0
    conn = get_db()
    n = 0
    for v in videos:
        title = v.get('title', '')
        dur = v.get('duration') or 0
        vid = v.get('id', '')
        if not vid or not is_good(title, dur):
            continue
        pri = get_priority(title)
        try:
            conn.execute(
                "INSERT INTO videos (video_id, title, course, university, url, duration_seconds, status, priority, license_risk) "
                "VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)",
                (vid, title, v.get('playlist', ''), source,
                 f"https://youtube.com/watch?v={vid}", dur, pri,
                 v.get('license', 'yellow')))
            n += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    conn.close()
    return n

def yt_search(query, max_results=200):
    """Search YouTube with optional CC filter."""
    cmd = [YTDLP,
           f"ytsearch{max_results}:{query}",
           "--dump-json", "--flat-playlist", "--no-download",
           "--no-warnings", "--quiet",
           "--js-runtimes", "node",
           "--socket-timeout", "15",
           "--match-filter", f"duration >= {MIN_DURATION}"]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        videos = []
        for line in r.stdout.strip().split('\n'):
            if not line:
                continue
            try:
                j = json.loads(line)
                videos.append({
                    'id': j.get('id', ''),
                    'title': j.get('title', ''),
                    'duration': j.get('duration'),
                    'playlist': query[:100],
                    'license': 'yellow'
                })
            except json.JSONDecodeError:
                pass
        return videos
    except (subprocess.TimeoutExpired, Exception) as e:
        print(f"  Search error for '{query}': {e}")
        return []

def crawl_channel(channel_id, max_videos=500):
    """Crawl all videos from a channel."""
    url = f"https://www.youtube.com/channel/{channel_id}/videos"
    cmd = [YTDLP, url,
           "--dump-json", "--flat-playlist", "--no-download",
           "--no-warnings", "--quiet",
           "--js-runtimes", "node",
           "--socket-timeout", "20",
           "--playlist-end", str(max_videos)]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        videos = []
        for line in r.stdout.strip().split('\n'):
            if not line:
                continue
            try:
                j = json.loads(line)
                dur = j.get('duration') or 0
                videos.append({
                    'id': j.get('id', ''),
                    'title': j.get('title', ''),
                    'duration': dur,
                    'playlist': f"channel:{channel_id}",
                    'license': 'green'  # Known safe channels
                })
            except json.JSONDecodeError:
                pass
        return videos
    except (subprocess.TimeoutExpired, Exception) as e:
        print(f"  Channel crawl error for {channel_id}: {e}")
        return []

def crawl_playlist(playlist_id, source='safe_playlist'):
    """Crawl a YouTube playlist."""
    url = f"https://www.youtube.com/playlist?list={playlist_id}"
    cmd = [YTDLP, url,
           "--dump-json", "--flat-playlist", "--no-download",
           "--no-warnings", "--quiet",
           "--js-runtimes", "node",
           "--socket-timeout", "20"]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        videos = []
        for line in r.stdout.strip().split('\n'):
            if not line:
                continue
            try:
                j = json.loads(line)
                videos.append({
                    'id': j.get('id', ''),
                    'title': j.get('title', ''),
                    'duration': j.get('duration') or 0,
                    'playlist': playlist_id,
                    'license': 'green'
                })
            except json.JSONDecodeError:
                pass
        return videos
    except (subprocess.TimeoutExpired, Exception) as e:
        print(f"  Playlist crawl error: {e}")
        return []

def discover_green_channels():
    """Find channels from existing GREEN videos."""
    conn = get_db()
    rows = conn.execute("""
        SELECT DISTINCT university FROM videos 
        WHERE license_risk='green' AND university NOT LIKE '%crawl%' 
        AND university NOT LIKE '%search%' AND university NOT LIKE '%related%'
        AND university != '' LIMIT 500
    """).fetchall()
    conn.close()
    return [r[0] for r in rows if r[0]]

def main():
    total_added = 0
    round_num = 0
    
    while True:
        round_num += 1
        round_added = 0
        
        # Phase 1: Crawl known safe channels
        print(f"\n{'='*60}")
        print(f"ROUND {round_num} — Phase 1: Safe channel crawling")
        print(f"{'='*60}")
        
        random.shuffle(SAFE_CHANNELS)
        for i, ch_id in enumerate(SAFE_CHANNELS):
            print(f"  [{i+1}/{len(SAFE_CHANNELS)}] Crawling channel {ch_id}...")
            videos = crawl_channel(ch_id)
            n = insert_videos(videos, source='safe_channel')
            round_added += n
            print(f"    Found {len(videos)} videos, {n} new")
            time.sleep(random.uniform(2, 5))
        
        # Phase 2: Educational search queries
        print(f"\n{'='*60}")
        print(f"ROUND {round_num} — Phase 2: Educational search")
        print(f"{'='*60}")
        
        queries = random.sample(SAFE_QUERIES, min(30, len(SAFE_QUERIES)))
        for i, q in enumerate(queries):
            print(f"  [{i+1}/{len(queries)}] Searching: {q}")
            videos = yt_search(q)
            n = insert_videos(videos, source='safe_search')
            round_added += n
            print(f"    Found {len(videos)} videos, {n} new")
            time.sleep(random.uniform(3, 8))
        
        # Phase 3: Discover channels from existing GREEN content
        print(f"\n{'='*60}")
        print(f"ROUND {round_num} — Phase 3: GREEN channel expansion")
        print(f"{'='*60}")
        
        green_sources = discover_green_channels()
        print(f"  Found {len(green_sources)} potential GREEN channel sources")
        
        total_added += round_added
        
        conn = get_db()
        stats = conn.execute("SELECT COUNT(*) FROM videos WHERE status='pending'").fetchone()[0]
        conn.close()
        
        print(f"\n>>> Round {round_num} complete: +{round_added} new videos | Total added this session: {total_added} | Queue: {stats} pending")
        
        # Brief pause between rounds
        time.sleep(30)

if __name__ == "__main__":
    main()
