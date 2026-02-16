#!/usr/bin/env python3
"""
Scale discovery to 10M+ educational videos.

Strategy:
1. RELATED VIDEO CHAINING: For every completed/queued video, fetch related videos → exponential growth
2. PLAYLIST EXPANSION: Find playlists containing known videos, crawl full playlists
3. CHANNEL EXHAUSTION: For every video found, crawl the full channel
4. SEARCH SATURATION: Combinatorial explosion of subject × language × year × modifier
5. QUALITY FILTER: Only keep videos ≥15 minutes (deep educational content)

Quality signals:
- Duration ≥ 15 minutes (filters out shorts, clips, trailers)
- Title keywords: lecture, course, tutorial, class, seminar, etc.
- Negative filter: music video, lyric, trailer, reaction, unboxing, prank, ASMR, mukbang
"""

import sqlite3, subprocess, json, os, time, random, re
from concurrent.futures import ThreadPoolExecutor, as_completed

DB_PATH = os.path.expanduser("~/academic_transcriptions/massive_production.db")
YTDLP = os.path.expanduser("~/academic_transcriptions/yt-dlp")
MIN_DURATION = 900  # 15 minutes

# Negative title patterns (case-insensitive)
REJECT_PATTERNS = re.compile(
    r'\b(music video|official video|lyric|trailer|reaction|unboxing|prank|asmr|mukbang|'
    r'tiktok|shorts|#shorts|haul|vlog|grwm|get ready with me|day in my life|'
    r'fortnite|minecraft gameplay|roblox|gta|gaming|let\'s play|walkthrough|playthrough|'
    r'full movie|full episode|movie clip|scene|behind the scenes|bloopers|'
    r'live stream|livestream|24 hour challenge|challenge video|'
    r'compilation|funny moments|best of|try not to laugh|satisfying|oddly satisfying)\b',
    re.IGNORECASE
)

# Positive title patterns (boost priority)
EDU_PATTERNS = re.compile(
    r'\b(lecture|course|tutorial|class|seminar|workshop|bootcamp|masterclass|'
    r'university|professor|MIT|Stanford|Harvard|Yale|Berkeley|Oxford|Cambridge|'
    r'OpenCourseWare|NPTEL|coursera|edx|khan academy|'
    r'introduction to|fundamentals|advanced|graduate|undergraduate|'
    r'chapter \d|lesson \d|week \d|part \d|module \d|'
    r'лекция|курс|университет|Vorlesung|cours magistral|lezione|wykład|강의|講義|课程)\b',
    re.IGNORECASE
)

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def is_educational(title, duration):
    """Filter: duration ≥ 15min AND not obviously non-educational."""
    if duration and duration < MIN_DURATION:
        return False
    if REJECT_PATTERNS.search(title or ''):
        return False
    return True

def get_priority(title):
    """Higher priority for obviously educational content."""
    if EDU_PATTERNS.search(title or ''):
        return 8
    return 5

def insert_videos(videos):
    if not videos: return 0
    conn = get_db()
    n = 0
    for v in videos:
        title = v.get('title', '')
        duration = v.get('duration', 0)
        if not is_educational(title, duration):
            continue
        try:
            conn.execute(
                "INSERT INTO videos (video_id, title, course, university, url, duration_seconds, status, priority) "
                "VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)",
                (v['id'], title, v.get('course', ''), v.get('src', ''),
                 f"https://youtube.com/watch?v={v['id']}", duration, get_priority(title)))
            n += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    conn.close()
    return n

def run_ytdlp(args, timeout=300):
    """Run yt-dlp and return parsed JSON entries."""
    cmd = [YTDLP] + args + ["--flat-playlist", "--dump-json", "--no-warnings", "--quiet"]
    videos = []
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        for line in r.stdout.strip().split('\n'):
            if not line.strip(): continue
            try:
                j = json.loads(line)
                vid = j.get('id', '')
                if vid and len(vid) == 11:  # YouTube video IDs are 11 chars
                    videos.append({
                        'id': vid,
                        'title': j.get('title', ''),
                        'duration': j.get('duration', 0),
                    })
            except json.JSONDecodeError:
                pass
    except (subprocess.TimeoutExpired, Exception):
        pass
    return videos

def search(query, n=500):
    vids = run_ytdlp([f"ytsearch{n}:{query}"], timeout=180)
    for v in vids:
        v['course'] = query
        v['src'] = 'search'
    return vids

def channel_videos(url):
    vids = run_ytdlp([url, "--match-filter", f"duration > {MIN_DURATION}"], timeout=600)
    src = url.split('/')[-1].replace('/videos', '')
    for v in vids:
        v['src'] = src
    return vids

def get_known_video_ids(limit=500):
    """Get random sample of known video IDs for related video discovery."""
    conn = get_db()
    rows = conn.execute(
        "SELECT video_id FROM videos WHERE duration_seconds >= ? ORDER BY RANDOM() LIMIT ?",
        (MIN_DURATION, limit)
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]

def get_known_channels():
    """Extract unique channel URLs from discovered videos."""
    conn = get_db()
    rows = conn.execute(
        "SELECT DISTINCT university FROM videos WHERE university LIKE 'http%' OR university LIKE '@%'"
    ).fetchall()
    conn.close()
    return [r[0] for r in rows if r[0]]

def current_count():
    c = get_db()
    n = c.execute("SELECT count(*) FROM videos").fetchone()[0]
    edu = c.execute(f"SELECT count(*) FROM videos WHERE duration_seconds >= {MIN_DURATION}").fetchone()[0]
    c.close()
    return n, edu

# === SEARCH QUERY GENERATORS ===

SUBJECTS = [
    "computer science", "machine learning", "deep learning", "artificial intelligence",
    "neural networks", "natural language processing", "computer vision", "reinforcement learning",
    "algorithms", "data structures", "operating systems", "compilers", "databases", "networking",
    "distributed systems", "cloud computing", "cybersecurity", "cryptography",
    "mathematics", "linear algebra", "calculus", "statistics", "probability", "discrete math",
    "real analysis", "complex analysis", "abstract algebra", "topology", "number theory",
    "differential equations", "optimization", "numerical methods", "graph theory",
    "physics", "quantum mechanics", "thermodynamics", "electromagnetism", "optics",
    "classical mechanics", "statistical mechanics", "general relativity", "particle physics",
    "quantum computing", "quantum field theory", "condensed matter", "astrophysics",
    "chemistry", "organic chemistry", "inorganic chemistry", "physical chemistry",
    "biochemistry", "analytical chemistry", "polymer chemistry", "electrochemistry",
    "biology", "molecular biology", "genetics", "cell biology", "microbiology",
    "neuroscience", "immunology", "ecology", "evolutionary biology", "bioinformatics",
    "medicine", "anatomy", "physiology", "pharmacology", "pathology", "epidemiology",
    "economics", "microeconomics", "macroeconomics", "econometrics", "game theory",
    "finance", "accounting", "behavioral economics", "development economics",
    "philosophy", "logic", "ethics", "metaphysics", "epistemology", "philosophy of mind",
    "psychology", "cognitive psychology", "clinical psychology", "social psychology",
    "developmental psychology", "neuropsychology", "behavioral psychology",
    "political science", "international relations", "constitutional law", "public policy",
    "sociology", "anthropology", "linguistics", "cognitive science",
    "engineering", "electrical engineering", "mechanical engineering", "civil engineering",
    "chemical engineering", "aerospace engineering", "biomedical engineering",
    "control systems", "signal processing", "robotics", "VLSI design",
    "data science", "big data", "information theory", "compiler design",
    "programming languages", "functional programming", "type theory", "formal verification",
    "history", "ancient history", "medieval history", "modern history", "art history",
    "world war", "cold war", "renaissance", "industrial revolution",
    "music theory", "film studies", "architecture", "design",
    "law", "criminal law", "international law", "corporate law", "intellectual property",
    "business strategy", "marketing", "management", "entrepreneurship", "supply chain",
    "environmental science", "climate science", "geology", "oceanography", "astronomy",
]

UNIVERSITIES = [
    "MIT", "Stanford", "Harvard", "Yale", "Princeton", "Berkeley", "Caltech", "Columbia",
    "Oxford", "Cambridge", "Imperial College", "ETH Zurich", "EPFL",
    "Carnegie Mellon", "Georgia Tech", "University of Michigan", "UIUC",
    "University of Toronto", "McGill", "Waterloo",
    "IIT", "NPTEL", "IISc", "BITS Pilani",
    "Tsinghua", "Peking University", "NUS", "NTU", "KAIST",
    "University of Tokyo", "Kyoto University",
    "TU Munich", "Max Planck", "University of Amsterdam",
    "Australian National University", "University of Melbourne",
    "Tel Aviv University", "Hebrew University", "Technion",
    "University of São Paulo", "UNAM",
]

LANGUAGES_QUERIES = {
    "russian": ["лекция {} полный курс", "курс {} университет", "{} лекции для студентов"],
    "german": ["Vorlesung {} komplett", "{} Kurs Universität", "{} Vorlesung deutsch"],
    "french": ["cours {} complet université", "{} cours magistral", "conférence {} français"],
    "spanish": ["curso {} completo universidad", "clase {} universidad", "conferencia {} español"],
    "portuguese": ["aula {} completa universidade", "curso {} faculdade", "{} aula universitária"],
    "italian": ["lezione {} completa università", "corso {} italiano", "{} lezione universitaria"],
    "japanese": ["{} 講義 大学", "{} 授業 完全版", "{} 講座 入門"],
    "korean": ["{} 강의 대학교", "{} 수업 완전판", "{} 강좌"],
    "chinese": ["{} 课程 大学", "{} 讲座 完整版", "{} 公开课"],
    "arabic": ["محاضرة {} جامعة", "دورة {} كاملة", "{} شرح مفصل"],
    "hindi": ["{} lecture IIT", "{} complete course Hindi", "NPTEL {} Hindi"],
    "polish": ["wykład {} uniwersytet", "kurs {} pełny", "{} wykład po polsku"],
    "turkish": ["{} ders üniversite", "{} kurs tam", "{} ders anlatım"],
    "dutch": ["college {} universiteit", "{} cursus volledig", "{} lezing"],
    "swedish": ["föreläsning {} universitet", "{} kurs komplett", "{} föreläsning"],
}

def generate_all_queries():
    queries = []
    
    # Subject + "full course/lecture" (English)
    for subj in SUBJECTS:
        queries.append(f"{subj} full course lecture")
        queries.append(f"{subj} university lecture series")
        queries.append(f"{subj} complete course 2024")
        queries.append(f"{subj} graduate course")
    
    # Subject + University
    for subj in SUBJECTS[:40]:  # top 40 subjects
        for uni in UNIVERSITIES[:15]:  # top 15 unis
            queries.append(f"{uni} {subj} lecture")
    
    # Multi-language
    for lang, templates in LANGUAGES_QUERIES.items():
        for subj in SUBJECTS[:25]:  # top 25 per language
            template = random.choice(templates)
            queries.append(template.format(subj))
    
    # Conference talks
    conferences = ["ICML", "NeurIPS", "CVPR", "ICLR", "ACL", "EMNLP", "AAAI", "KDD",
                   "ICRA", "SIGGRAPH", "CHI", "SOSP", "OSDI", "NSDI", "SIGCOMM",
                   "CCS", "IEEE", "ACM", "NIPS"]
    years = ["2020", "2021", "2022", "2023", "2024"]
    for conf in conferences:
        for year in years:
            queries.append(f"{conf} {year} talk presentation")
    
    # Professional/applied
    applied = [
        "AWS re:Invent talk", "Google I/O session", "Apple WWDC session",
        "PyCon talk", "RustConf talk", "GopherCon talk", "JSConf talk",
        "Strange Loop talk", "GOTO conference", "InfoQ presentation",
        "Defcon talk", "Black Hat presentation", "RSA conference",
        "YC Startup School lecture", "a]16z talk",
        "TED talk education", "TEDx university",
        "Royal Institution lecture", "World Science Festival",
        "Aspen Ideas talk", "Long Now Foundation seminar",
        "Santa Fe Institute lecture", "Perimeter Institute lecture",
    ]
    queries.extend(applied)
    
    random.shuffle(queries)
    return queries

def main():
    total, edu = current_count()
    print(f"Starting 10M discovery. DB: {total} total, {edu} ≥15min")
    print(f"Quality filter: ≥{MIN_DURATION//60}min, reject non-educational\n")
    
    total_new = 0
    
    # Phase 1: Exhaust channel crawling
    print("=== PHASE 1: Channel Exhaustion ===")
    # Crawl every known channel completely
    channels_done = set()
    
    # Get channels from discover_channels.py list
    from discover_aggressive import MEGA_CHANNELS
    all_channels = list(set(MEGA_CHANNELS))
    
    print(f"Crawling {len(all_channels)} channels (≥15min filter)...")
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(channel_videos, ch): ch for ch in all_channels}
        for i, f in enumerate(as_completed(futures)):
            ch = futures[f]
            try:
                vids = f.result()
                if vids:
                    n = insert_videos(vids)
                    total_new += n
                    if n > 0:
                        t, e = current_count()
                        print(f"  [{i+1}/{len(all_channels)}] {ch.split('/')[-2]:30s} +{n:5d} (total: {t}, edu: {e})")
            except:
                pass
    
    t, e = current_count()
    print(f"\nPhase 1 done: +{total_new} new. Total: {t}, ≥15min: {e}\n")
    
    # Phase 2: Massive search
    print("=== PHASE 2: Search Saturation ===")
    queries = generate_all_queries()
    print(f"Running {len(queries)} queries...\n")
    
    for i in range(0, len(queries), 3):
        batch = queries[i:i+3]
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = {pool.submit(search, q, 300): q for q in batch}
            for f in as_completed(futures):
                q = futures[f]
                try:
                    vids = f.result()
                    if vids:
                        n = insert_videos(vids)
                        total_new += n
                        if n > 0 and n >= 5:
                            t, e = current_count()
                            print(f"  [{i+1}/{len(queries)}] '{q[:45]:45s}' +{n:4d} (total: {t}, edu: {e})")
                except:
                    pass
        
        if (i // 3) % 50 == 0 and i > 0:
            t, e = current_count()
            print(f"\n  === Progress: {t} total, {e} ≥15min, +{total_new} new ===\n")
            if t >= 10_000_000:
                print("🎯 10M TARGET REACHED!")
                break
        
        time.sleep(0.5)
    
    t, e = current_count()
    print(f"\n{'='*60}")
    print(f"DISCOVERY COMPLETE")
    print(f"Total: {t}, Educational (≥15min): {e}")
    print(f"New: +{total_new}")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
