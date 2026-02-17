#!/usr/bin/env python3
"""Mega discovery: target the long tail of educational YouTube.
Focuses on areas the other scripts miss:
1. Non-English educational content (massive untapped supply)
2. Professional/industry conferences
3. Government/institutional lectures
4. Niche academic fields
5. Historical/archived lectures
"""
import sqlite3, subprocess, json, os, time, random, re
from concurrent.futures import ThreadPoolExecutor, as_completed

DB_PATH = os.path.expanduser("~/academic_transcriptions/massive_production.db")
YTDLP = os.path.expanduser("~/academic_transcriptions/yt-dlp")
MIN_DURATION = 900

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def insert_videos(videos, source="mega"):
    if not videos: return 0
    conn = get_db()
    n = 0
    for v in videos:
        try:
            conn.execute(
                "INSERT INTO videos (video_id, title, course, university, url, duration_seconds, status, priority) "
                "VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)",
                (v['id'], v.get('title','')[:500], v.get('course', source), v.get('src',''),
                 f"https://youtube.com/watch?v={v['id']}", v.get('duration',0), v.get('priority',5)))
            n += 1
        except sqlite3.IntegrityError: pass
    conn.commit(); conn.close()
    return n

def yt_search(query, max_results=200):
    """Search YouTube via yt-dlp, filter ≥15min."""
    cmd = [YTDLP, f"ytsearch{max_results}:{query}", "--flat-playlist", "--dump-json",
           "--no-warnings", "--socket-timeout", "20"]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=180, cwd=os.path.dirname(YTDLP))
        results = []
        for line in r.stdout.strip().split('\n'):
            if not line.strip(): continue
            try:
                d = json.loads(line)
                dur = d.get('duration') or 0
                if dur >= MIN_DURATION:
                    results.append({'id': d['id'], 'title': d.get('title',''), 'duration': dur})
            except: pass
        return results
    except: return []

def crawl_channel(channel_id, max_videos=500):
    cmd = [YTDLP, f"https://www.youtube.com/channel/{channel_id}/videos",
           "--flat-playlist", "--dump-json", "--playlist-end", str(max_videos),
           "--no-warnings", "--socket-timeout", "20"]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=300, cwd=os.path.dirname(YTDLP))
        results = []
        for line in r.stdout.strip().split('\n'):
            if not line.strip(): continue
            try:
                d = json.loads(line)
                dur = d.get('duration') or 0
                if dur >= MIN_DURATION:
                    results.append({'id': d['id'], 'title': d.get('title',''), 'duration': dur})
            except: pass
        return results
    except: return []

# === SEARCH STRATEGIES ===

# 1. Non-English educational content (HUGE untapped pool)
NON_ENGLISH_QUERIES = [
    # Hindi (India = massive edu YouTube market)
    "IIT lecture hindi", "UPSC preparation lecture", "NEET physics lecture full",
    "class 12 chemistry hindi", "JEE advanced mathematics", "GATE preparation lecture",
    "Indian Institute of Technology lecture", "NPTEL hindi",
    "engineering mathematics hindi lecture", "B.Tech lecture hindi",
    # Spanish
    "universidad clase completa", "curso matemáticas universidad",
    "clase magistral física", "conferencia académica español",
    "ingeniería clase universidad", "medicina clase completa",
    "programación curso completo", "economía clase universitaria",
    # Portuguese (Brazil = huge market)
    "aula completa universidade", "curso programação completo",
    "aula de física universitária", "palestra acadêmica",
    "curso engenharia completo", "aula medicina",
    # Russian
    "лекция университет полная", "курс программирования",
    "лекция по физике мфти", "лекция математика вышка",
    "курс машинного обучения", "Технопарк лекция",
    "МФТИ лекция", "ВШЭ лекция экономика",
    # German
    "Vorlesung Universität vollständig", "Informatik Vorlesung",
    "Mathematik Vorlesung komplett", "Physik Vorlesung TU",
    "Maschinelles Lernen Vorlesung", "BWL Vorlesung",
    # French
    "cours magistral université", "cours complet mathématiques",
    "conférence physique université", "cours informatique complet",
    "cours médecine université", "cours économie complet",
    # Japanese
    "大学 講義 完全", "プログラミング 講座", "数学 講義 大学",
    "物理学 講義", "東京大学 講義", "機械学習 講座",
    # Korean
    "대학 강의 전체", "프로그래밍 강좌", "수학 강의",
    "물리학 강의", "컴퓨터과학 강의", "KAIST 강의",
    # Arabic
    "محاضرة جامعية كاملة", "دورة برمجة كاملة", "محاضرة رياضيات",
    "محاضرة فيزياء جامعة", "كورس هندسة", "محاضرة طب",
    # Turkish
    "üniversite ders tam", "programlama kursu", "matematik dersi",
    "fizik dersi üniversite", "mühendislik dersi",
    # Chinese
    "大学公开课完整", "计算机科学课程", "数学课完整版",
    "清华大学公开课", "北大公开课", "机器学习课程完整",
    # Polish
    "wykład uniwersytet pełny", "kurs programowania", "wykład matematyka",
    # Dutch
    "college universiteit volledig", "informatica college", "wiskunde college",
    # Swedish
    "föreläsning universitet", "programmering kurs", "matematik föreläsning",
    # Italian
    "lezione universitaria completa", "corso programmazione", "lezione fisica università",
    # Vietnamese
    "bài giảng đại học", "khóa học lập trình", "giảng dạy toán học",
    # Thai
    "บรรยาย มหาวิทยาลัย", "คอร์สเรียน โปรแกรมมิ่ง",
    # Indonesian
    "kuliah universitas lengkap", "kursus pemrograman", "kuliah matematika",
]

# 2. Professional/Industry conferences (high quality, often overlooked)
CONFERENCE_QUERIES = [
    # Tech conferences
    "PyCon talk full", "GopherCon talk", "RustConf talk", "JSConf talk",
    "KubeCon talk", "DockerCon presentation", "AWS re:Invent session",
    "Google I/O session", "WWDC session full", "Microsoft Build session",
    "Strange Loop talk", "QCon talk", "InfoQ presentation",
    "NDC conference talk", "Devoxx talk", "FOSDEM talk",
    # AI/ML conferences
    "NeurIPS talk", "ICML presentation", "CVPR talk", "ACL talk",
    "EMNLP presentation", "ICLR talk", "AAAI presentation",
    "machine learning conference talk", "deep learning workshop",
    # Science conferences
    "Royal Institution lecture", "TEDx science", "Perimeter Institute lecture",
    "Institute for Advanced Study lecture", "Santa Fe Institute lecture",
    "Fields Institute lecture", "KITP lecture",
    "Kavli lecture", "Nobel Prize lecture",
    # Business/Economics
    "World Economic Forum session", "Davos panel",
    "McKinsey talk", "Harvard Business School lecture",
    "Wharton lecture", "economics conference presentation",
    "central bank lecture", "Federal Reserve lecture",
    # Medical
    "medical conference presentation", "NIH lecture",
    "Grand Rounds lecture", "clinical lecture full",
    "NEJM lecture", "Lancet conference",
]

# 3. Institutional/Government lectures
INSTITUTIONAL_QUERIES = [
    "Library of Congress lecture", "Smithsonian lecture",
    "National Academy of Sciences lecture", "British Academy lecture",
    "Royal Society lecture", "NASA lecture full",
    "CERN lecture", "WHO seminar", "World Bank lecture",
    "United Nations lecture", "IMF seminar",
    "National Archives lecture", "Congressional Research Service",
    "judiciary lecture constitutional law",
    "military academy lecture", "West Point lecture",
    "Sandhurst lecture", "Naval War College lecture",
]

# 4. Niche academic fields
NICHE_QUERIES = [
    # Philosophy
    "philosophy lecture full course", "ethics lecture university",
    "existentialism lecture", "epistemology lecture", "metaphysics course",
    # Humanities
    "art history lecture full", "musicology lecture", "linguistics lecture",
    "anthropology lecture university", "archaeology lecture full",
    "classics lecture ancient", "medieval history lecture",
    # Social Sciences
    "sociology lecture university", "political science lecture full",
    "international relations lecture", "public policy lecture",
    "criminology lecture", "urban planning lecture",
    # Hard Sciences
    "organic chemistry lecture full", "quantum mechanics lecture",
    "astrophysics lecture full course", "geology lecture university",
    "molecular biology lecture", "genetics lecture full",
    "neuroscience lecture full", "immunology lecture",
    "biochemistry lecture university", "thermodynamics lecture full",
    "fluid mechanics lecture", "structural engineering lecture",
    # Math
    "topology lecture", "abstract algebra lecture full",
    "real analysis lecture", "number theory lecture",
    "differential equations lecture full", "probability theory lecture",
    "combinatorics lecture", "graph theory lecture full",
    # CS
    "distributed systems lecture", "database systems lecture full",
    "compilers lecture course", "operating systems lecture full",
    "computer architecture lecture", "cryptography lecture full",
    "formal verification lecture", "type theory lecture",
    "category theory programming", "functional programming lecture",
    # Engineering
    "control systems lecture full", "signal processing lecture",
    "VLSI design lecture", "embedded systems lecture",
    "power systems lecture", "telecommunications lecture",
    "aerospace engineering lecture", "materials science lecture",
    "chemical engineering lecture", "biomedical engineering lecture",
]

# 5. Massive educational channels to crawl
MEGA_CHANNELS = [
    # Indian educational (massive content)
    "UCrC8mOqsYMsaBGhm5bX6dQ",  # Physics Wallah
    "UCBwmMxybNva6P_5VmxjzwqA",  # Unacademy
    "UCpjMoEbIBPOKE-b7IIcvAqg",  # Khan Academy India
    "UCgowpYGpB8ux7k0cVKDQwPA",  # NPTEL Official
    "UCe7vBfMShmOHH1m_MZ0nOzg",  # CampusX
    # Global MOOCs
    "UCEBb1b_L6zDS3xTUrIALZOw",  # MIT OpenCourseWare (ensure full crawl)
    "UCSJbGtTlrDami-tDGPUV9-w",  # Academind
    "UCW5YeuERMmlnqo4oq8vwUpg",  # The Net Ninja
    "UC8butISFwT-Wl7EV0hUK0BQ",  # freeCodeCamp
    "UCvjgXvBlCQFSzEBjlkGSIVg",  # YaleCourses
    "UC0RhatS1pyxInC00YKjjBqQ",  # Georgia Tech
    "UCnUYZLuoy1rq1aVMwx4piYw",  # Crash Course
    "UCsooa4yRKGN_zEE8iknghZA",  # TED-Ed
    "UC9-y-6csu5WGm29I7JiwpnA",  # Computerphile
    "UCYO_jab_esuFRV4b17AJtAw",  # 3Blue1Brown
    "UCoxcjq-8xIDTYp3uz647V5A",  # Numberphile
    "UCIuqG-VTl0C-Z0FOMMODR7A",  # IISC Bangalore
    "UCJ0yBou72Lz9VqjlyCgJHOw",  # The Math Sorcerer
    "UCFe6jenM1Bc54qtBsIJGRZQ",  # PatrickJMT
    "UCsvqEJYEPiGKSKGqMXFnHTQ",  # Professor Leonard
    "UCWN3xxRkmTPphYit3f0l0Hw",  # Organic Chemistry Tutor
    "UC_SvYP0k05UKiJ_2ndB02IA",  # Jeff Delaney (Fireship)
    "UCsBjURrPoezykLs9EqgamOA",  # Fireship
    "UC4xKdmAXFh4ACyhpiQ_3qBw",  # Tech With Tim
    "UCZgt6AzoyjslHTC9dz0UoTw",  # Berkeley CS
    "UCi8e0iOVk1fEOogdfu4YgfA",  # Stanford Online
]

def run_search_batch(queries, source, priority=5):
    """Run a batch of search queries with thread pool."""
    total_new = 0
    for i, q in enumerate(queries):
        try:
            results = yt_search(q, max_results=100)
            for r in results:
                r['priority'] = priority
                r['src'] = source
            n = insert_videos(results, source)
            total_new += n
            if n > 0:
                print(f"  [{i+1}/{len(queries)}] '{q[:50]}' → +{n} new (total new: {total_new})", flush=True)
            time.sleep(random.uniform(1, 3))  # rate limit
        except Exception as e:
            print(f"  ERROR on '{q[:50]}': {e}", flush=True)
    return total_new

def run_channel_batch(channels, priority=8):
    """Crawl a batch of channels."""
    total_new = 0
    for i, ch_id in enumerate(channels):
        try:
            results = crawl_channel(ch_id)
            for r in results:
                r['priority'] = priority
                r['src'] = f'channel:{ch_id}'
            n = insert_videos(results, f'channel:{ch_id}')
            total_new += n
            if n > 0:
                print(f"  [CH {i+1}/{len(channels)}] {ch_id} → +{n} new", flush=True)
            time.sleep(random.uniform(2, 5))
        except Exception as e:
            print(f"  CH ERROR {ch_id}: {e}", flush=True)
    return total_new

# === MAIN ===
db = get_db()
total = db.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
edu = db.execute("SELECT COUNT(*) FROM videos WHERE status != 'rejected'").fetchone()[0]
db.close()
print(f"Starting mega discovery. DB: {total} total, {edu} edu", flush=True)

round_num = 0
while True:
    round_num += 1
    round_total = 0
    
    print(f"\n{'='*60}", flush=True)
    print(f"=== MEGA ROUND {round_num} ===", flush=True)
    print(f"{'='*60}", flush=True)
    
    # Phase 1: Non-English (biggest untapped pool)
    print(f"\n[Phase 1] Non-English educational content ({len(NON_ENGLISH_QUERIES)} queries)", flush=True)
    batch = random.sample(NON_ENGLISH_QUERIES, min(30, len(NON_ENGLISH_QUERIES)))
    n = run_search_batch(batch, "non-english-edu", priority=7)
    round_total += n
    print(f"  → Phase 1 total: +{n}", flush=True)
    
    # Phase 2: Conferences
    print(f"\n[Phase 2] Professional conferences ({len(CONFERENCE_QUERIES)} queries)", flush=True)
    batch = random.sample(CONFERENCE_QUERIES, min(20, len(CONFERENCE_QUERIES)))
    n = run_search_batch(batch, "conference", priority=8)
    round_total += n
    print(f"  → Phase 2 total: +{n}", flush=True)
    
    # Phase 3: Institutional
    print(f"\n[Phase 3] Institutional lectures ({len(INSTITUTIONAL_QUERIES)} queries)", flush=True)
    n = run_search_batch(INSTITUTIONAL_QUERIES, "institutional", priority=8)
    round_total += n
    print(f"  → Phase 3 total: +{n}", flush=True)
    
    # Phase 4: Niche academic
    print(f"\n[Phase 4] Niche academic ({len(NICHE_QUERIES)} queries)", flush=True)
    batch = random.sample(NICHE_QUERIES, min(30, len(NICHE_QUERIES)))
    n = run_search_batch(batch, "niche-academic", priority=7)
    round_total += n
    print(f"  → Phase 4 total: +{n}", flush=True)
    
    # Phase 5: Channel crawling (every 3rd round)
    if round_num % 3 == 1:
        print(f"\n[Phase 5] Mega channel crawling ({len(MEGA_CHANNELS)} channels)", flush=True)
        n = run_channel_batch(MEGA_CHANNELS, priority=8)
        round_total += n
        print(f"  → Phase 5 total: +{n}", flush=True)
    
    # Status
    db = get_db()
    total = db.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
    edu = db.execute("SELECT COUNT(*) FROM videos WHERE status != 'rejected'").fetchone()[0]
    db.close()
    
    print(f"\n=== MEGA ROUND {round_num} COMPLETE | +{round_total} new | DB: {total} total, {edu} edu ===", flush=True)
    
    # Shuffle queries for next round to hit different results
    random.shuffle(NON_ENGLISH_QUERIES)
    random.shuffle(CONFERENCE_QUERIES)
    random.shuffle(NICHE_QUERIES)
    
    time.sleep(10)
