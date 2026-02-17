#!/usr/bin/env python3
"""Scale YouTube educational video discovery to 1M+ via yt-dlp search and channel crawling.
Inserts directly into massive_production.db with UNIQUE constraint dedup."""

import sqlite3, subprocess, json, os, sys, time, random, itertools
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

DB_PATH = os.path.expanduser("~/academic_transcriptions/massive_production.db")
YTDLP = os.path.expanduser("~/academic_transcriptions/yt-dlp")

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def insert_videos(videos):
    """Bulk insert, skip duplicates via UNIQUE constraint."""
    if not videos:
        return 0
    conn = get_db()
    inserted = 0
    for v in videos:
        try:
            conn.execute(
                "INSERT INTO videos (video_id, title, course, university, url, duration_seconds, status, priority) "
                "VALUES (?, ?, ?, ?, ?, ?, 'pending', ?) ",
                (v['id'], v.get('title',''), v.get('course',''), v.get('university',''),
                 f"https://youtube.com/watch?v={v['id']}", v.get('duration', 0), v.get('priority', 5))
            )
            inserted += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    conn.close()
    return inserted

def search_youtube(query, max_results=500):
    """Search YouTube via yt-dlp, return video metadata."""
    cmd = [YTDLP, f"ytsearch{max_results}:{query}", 
           "--flat-playlist", "--dump-json", "--no-warnings", "--quiet",
           "--match-filter", "duration > 120 & duration < 36000"]  # 2min-10hr
    videos = []
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        for line in result.stdout.strip().split('\n'):
            if not line.strip():
                continue
            try:
                j = json.loads(line)
                videos.append({
                    'id': j.get('id', ''),
                    'title': j.get('title', ''),
                    'duration': j.get('duration', 0),
                    'course': query,
                    'university': '',
                    'priority': 5
                })
            except json.JSONDecodeError:
                pass
    except (subprocess.TimeoutExpired, Exception) as e:
        print(f"  Search failed for '{query}': {e}")
    return videos

def get_channel_videos(channel_url, max_results=None):
    """Get all videos from a YouTube channel/playlist."""
    cmd = [YTDLP, channel_url, "--flat-playlist", "--dump-json", "--no-warnings", "--quiet",
           "--match-filter", "duration > 120 & duration < 36000"]
    if max_results:
        cmd.extend(["--playlist-end", str(max_results)])
    videos = []
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        for line in result.stdout.strip().split('\n'):
            if not line.strip():
                continue
            try:
                j = json.loads(line)
                videos.append({
                    'id': j.get('id', ''),
                    'title': j.get('title', ''),
                    'duration': j.get('duration', 0),
                    'course': '',
                    'university': channel_url.split('/')[-1] if '/' in channel_url else channel_url,
                    'priority': 5
                })
            except json.JSONDecodeError:
                pass
    except (subprocess.TimeoutExpired, Exception) as e:
        print(f"  Channel failed for '{channel_url}': {e}")
    return videos

# === SEARCH QUERIES (~500 results each, 1000+ queries = 500K+ videos) ===

# Major subjects x levels
SUBJECTS = [
    "computer science", "machine learning", "deep learning", "artificial intelligence",
    "mathematics", "linear algebra", "calculus", "statistics", "probability",
    "physics", "quantum mechanics", "thermodynamics", "electromagnetism", "astrophysics",
    "chemistry", "organic chemistry", "biochemistry", "molecular biology",
    "biology", "genetics", "neuroscience", "anatomy", "physiology",
    "economics", "microeconomics", "macroeconomics", "econometrics", "finance",
    "philosophy", "logic", "ethics", "metaphysics", "epistemology",
    "psychology", "cognitive science", "behavioral psychology", "clinical psychology",
    "history", "world history", "ancient history", "modern history", "art history",
    "engineering", "electrical engineering", "mechanical engineering", "civil engineering",
    "data science", "natural language processing", "computer vision", "reinforcement learning",
    "algorithms", "data structures", "operating systems", "compilers", "databases",
    "networking", "distributed systems", "cloud computing", "cybersecurity",
    "astronomy", "cosmology", "earth science", "geology", "environmental science",
    "political science", "sociology", "anthropology", "linguistics",
    "medicine", "pharmacology", "pathology", "immunology", "epidemiology",
    "business", "marketing", "management", "accounting", "entrepreneurship",
    "law", "constitutional law", "international law", "criminal law",
    "music theory", "film studies", "creative writing", "journalism",
]

MODIFIERS = [
    "lecture", "full course", "university lecture", "MIT OpenCourseWare",
    "Stanford lecture", "Harvard lecture", "Yale lecture", "Princeton lecture",
    "Oxford lecture", "Cambridge lecture", "Berkeley lecture", "Caltech lecture",
    "tutorial", "course", "class", "seminar", "workshop",
    "introduction to", "advanced", "graduate level", "undergraduate",
    "explained", "crash course", "masterclass", "bootcamp",
]

LANGUAGES = [
    ("", "english"),
    ("курс лекция", "russian"),
    ("Vorlesung Kurs", "german"),
    ("cours magistral", "french"),
    ("curso aula", "portuguese"),
    ("curso clase universidad", "spanish"),
    ("講義 授業", "japanese"),
    ("강의 대학", "korean"),
    ("讲座 课程 大学", "chinese"),
    ("lezione corso università", "italian"),
    ("wykład kurs", "polish"),
    ("föreläsning kurs", "swedish"),
    ("lezing cursus", "dutch"),
    ("محاضرة جامعة", "arabic"),
    ("व्याख्यान पाठ्यक्रम", "hindi"),
]

# Top educational channels to crawl
CHANNELS = [
    "https://www.youtube.com/@mitocw", "https://www.youtube.com/@stanford",
    "https://www.youtube.com/@YaleCourses", "https://www.youtube.com/@harvarduniversity",
    "https://www.youtube.com/@UCBerkeley", "https://www.youtube.com/@calaboratory",
    "https://www.youtube.com/@3blue1brown", "https://www.youtube.com/@numberphile",
    "https://www.youtube.com/@pbsspacetime", "https://www.youtube.com/@veritasium",
    "https://www.youtube.com/@SmarterEveryDay", "https://www.youtube.com/@minutephysics",
    "https://www.youtube.com/@TedEd", "https://www.youtube.com/@TEDx",
    "https://www.youtube.com/@kaboratory", "https://www.youtube.com/@Vsauce",
    "https://www.youtube.com/@crashcourse", "https://www.youtube.com/@KhanAcademy",
    "https://www.youtube.com/@freaboratory", "https://www.youtube.com/@academicearth",
    "https://www.youtube.com/@MicrosoftResearch", "https://www.youtube.com/@GoogleTechTalks",
    "https://www.youtube.com/@Computerphile", "https://www.youtube.com/@DeepMind",
    "https://www.youtube.com/@TwoMinutePapers", "https://www.youtube.com/@sentdex",
    "https://www.youtube.com/@StatQuest", "https://www.youtube.com/@ProfessorLeonard",
    "https://www.youtube.com/@DrTrefor", "https://www.youtube.com/@PatrickJMT",
    "https://www.youtube.com/@WalterLewin", "https://www.youtube.com/@eigenchris",
    "https://www.youtube.com/@MathTheBeautiful", "https://www.youtube.com/@blackpenredpen",
    "https://www.youtube.com/@TheMathSorcerer", "https://www.youtube.com/@Mathologer",
    "https://www.youtube.com/@physicsgirl", "https://www.youtube.com/@fermilab",
    "https://www.youtube.com/@sixtysymbols", "https://www.youtube.com/@periodicvideos",
    "https://www.youtube.com/@NileRed", "https://www.youtube.com/@BrainCraft",
    "https://www.youtube.com/@LexFridman", "https://www.youtube.com/@AndrewHubermanLab",
    "https://www.youtube.com/@araboratory", "https://www.youtube.com/@cs50",
    "https://www.youtube.com/@TheRoyalInstitution", "https://www.youtube.com/@WorldScienceFestival",
    "https://www.youtube.com/@InstituteForAdvancedStudy", "https://www.youtube.com/@KITP",
    "https://www.youtube.com/@Perimeter", "https://www.youtube.com/@IcermaLab",
    "https://www.youtube.com/@NPCOMPLETE", "https://www.youtube.com/@WelchLabs",
    "https://www.youtube.com/@NandMCompSci", "https://www.youtube.com/@BenEater",
    "https://www.youtube.com/@TechWithTim", "https://www.youtube.com/@CoreySchafer",
    "https://www.youtube.com/@maboratory", "https://www.youtube.com/@taboratory",
    # Playlists
    "https://www.youtube.com/playlist?list=PLUl4u3cNGP63WbdFxL8giv4yhgdMGaZNA",  # MIT 6.006
    "https://www.youtube.com/playlist?list=PLUl4u3cNGP60uVBMaoNERc6knT_MgPKS0",  # MIT 18.650
    "https://www.youtube.com/playlist?list=PLoROMvodv4rMiGQp3WXShtMGgzqpfVfbU",  # Stanford CS229
    "https://www.youtube.com/playlist?list=PLoROMvodv4rOSH4v6133s9LFPRHjEmbmJ",  # Stanford CS224N
    "https://www.youtube.com/playlist?list=PL3FW7Lu3i5JvHM8ljYj-zLfQRF3EO8sYv",  # Stanford CS231N
    "https://www.youtube.com/playlist?list=PLqYmG7hTraZDM-OYHWgPebj2MfCFzFObQ",  # DeepMind RL
]

def generate_queries():
    """Generate diverse search queries."""
    queries = []
    
    # Subject x modifier combinations  
    for subj in SUBJECTS:
        for mod in MODIFIERS[:8]:  # top 8 modifiers per subject
            queries.append(f"{subj} {mod}")
    
    # Non-english searches
    for lang_terms, lang_name in LANGUAGES[1:]:  # skip english
        for subj in SUBJECTS[:20]:  # top 20 subjects in each language
            queries.append(f"{subj} {lang_terms}")
    
    # Specific course searches
    specific = [
        "MIT 6.006", "MIT 6.034", "MIT 6.042", "MIT 18.06", "MIT 18.01", "MIT 18.02",
        "MIT 6.S191", "MIT 6.824", "MIT 6.828", "MIT 6.858", "MIT 6.046",
        "Stanford CS106A", "Stanford CS106B", "Stanford CS107", "Stanford CS161",
        "Stanford CS224W", "Stanford CS330", "Stanford CS234", "Stanford CS236",
        "Harvard CS50", "Harvard CS124", "Harvard STAT110",
        "CMU 15-213", "CMU 15-445", "CMU 10-301", "CMU 10-701", "CMU 11-785",
        "Berkeley CS61A", "Berkeley CS61B", "Berkeley CS61C", "Berkeley CS162", "Berkeley CS170",
        "Princeton COS226", "Caltech CS1", "Georgia Tech CS6515",
        "NPTEL machine learning", "NPTEL data structures", "NPTEL algorithms",
        "Coursera machine learning", "edX computer science",
        "conference talk ICML", "conference talk NeurIPS", "conference talk CVPR",
        "conference talk ICLR", "conference talk ACL", "conference talk EMNLP",
        "Google I/O talk", "Apple WWDC session", "AWS re:Invent talk",
        "PyCon talk", "RustConf talk", "GopherCon talk", "JSConf talk",
        "Strange Loop talk", "GOTO conference", "InfoQ presentation",
        "doctoral thesis defense", "PhD defense presentation",
        "academic keynote speech", "research seminar talk",
    ]
    queries.extend(specific)
    
    # Topic deep dives
    deep = [
        "transformer architecture explained", "attention mechanism tutorial",
        "backpropagation derivation lecture", "gradient descent optimization course",
        "Bayesian inference full course", "Markov chain Monte Carlo lecture",
        "convex optimization course", "information theory lecture",
        "group theory lecture mathematics", "topology course university",
        "real analysis lecture", "complex analysis course",
        "differential equations full course", "partial differential equations lecture",
        "abstract algebra university course", "number theory lecture",
        "quantum computing course", "quantum field theory lecture",
        "general relativity course", "special relativity lecture",
        "string theory lecture", "particle physics course",
        "organic chemistry full course", "physical chemistry lecture",
        "cell biology university lecture", "evolutionary biology course",
        "cognitive neuroscience lecture", "computational neuroscience course",
        "game theory full course", "decision theory lecture",
        "compiler design university course", "programming languages theory",
        "formal verification lecture", "type theory course",
        "category theory lecture", "lambda calculus tutorial",
        "cryptography full course", "network security lecture",
        "signal processing course", "control theory lecture",
        "robotics full course", "autonomous systems lecture",
        "natural language processing course 2024", "large language models lecture",
        "diffusion models tutorial", "generative AI course",
        "graph neural networks lecture", "geometric deep learning course",
    ]
    queries.extend(deep)
    
    random.shuffle(queries)
    return queries

def current_count():
    conn = get_db()
    n = conn.execute("SELECT count(*) FROM videos").fetchone()[0]
    conn.close()
    return n

def main():
    start_count = current_count()
    print(f"Starting discovery. Current DB: {start_count} videos")
    print(f"Target: 1,000,000+ videos\n")
    
    total_new = 0
    
    # Phase 1: Channel crawling (high quality, fast)
    print("=== PHASE 1: Channel Crawling ===")
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(get_channel_videos, ch): ch for ch in CHANNELS}
        for f in as_completed(futures):
            ch = futures[f]
            try:
                vids = f.result()
                if vids:
                    n = insert_videos(vids)
                    total_new += n
                    if n > 0:
                        print(f"  {ch.split('/')[-1]:30s} +{n:5d} (total new: {total_new})")
            except Exception as e:
                print(f"  {ch}: error {e}")
    
    print(f"\nPhase 1 done: +{total_new} new videos. DB total: {current_count()}\n")
    
    # Phase 2: Massive search queries
    print("=== PHASE 2: Search Queries ===")
    queries = generate_queries()
    print(f"Running {len(queries)} search queries...\n")
    
    batch_size = 4  # concurrent searches
    for i in range(0, len(queries), batch_size):
        batch = queries[i:i+batch_size]
        with ThreadPoolExecutor(max_workers=batch_size) as pool:
            futures = {pool.submit(search_youtube, q, 300): q for q in batch}
            for f in as_completed(futures):
                q = futures[f]
                try:
                    vids = f.result()
                    if vids:
                        n = insert_videos(vids)
                        total_new += n
                        if n > 0:
                            print(f"  [{i+1}/{len(queries)}] '{q[:40]:40s}' +{n:4d} (DB: {current_count()})")
                except Exception as e:
                    pass
        
        # Progress every 20 batches
        if (i // batch_size) % 20 == 0 and i > 0:
            db_total = current_count()
            print(f"\n  --- Progress: {db_total} total videos, +{total_new} new ---\n")
            if db_total >= 1_000_000:
                print("TARGET REACHED: 1M+ videos!")
                break
        
        # Rate limit gently
        time.sleep(0.5)
    
    final = current_count()
    print(f"\n{'='*60}")
    print(f"DISCOVERY COMPLETE")
    print(f"Started: {start_count}")
    print(f"Final:   {final}")
    print(f"New:     {total_new}")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
