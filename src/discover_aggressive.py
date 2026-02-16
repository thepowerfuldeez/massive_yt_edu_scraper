#!/usr/bin/env python3
"""Aggressive discovery to reach 1M+ videos. Runs alongside scale_to_1M.py.
Focuses on high-yield playlists, massive channels, and trending educational content."""

import sqlite3, subprocess, json, os, time, random
from concurrent.futures import ThreadPoolExecutor, as_completed

DB_PATH = os.path.expanduser("~/academic_transcriptions/massive_production.db")
YTDLP = os.path.expanduser("~/academic_transcriptions/yt-dlp")

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def insert_videos(videos):
    if not videos: return 0
    conn = get_db()
    n = 0
    for v in videos:
        try:
            conn.execute(
                "INSERT INTO videos (video_id, title, course, university, url, duration_seconds, status, priority) "
                "VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)",
                (v['id'], v.get('title',''), v.get('course',''), v.get('src',''),
                 f"https://youtube.com/watch?v={v['id']}", v.get('duration',0), v.get('priority',5)))
            n += 1
        except sqlite3.IntegrityError: pass
    conn.commit(); conn.close()
    return n

def yt_search(query, max_results=500):
    videos = []
    try:
        r = subprocess.run([YTDLP, f"ytsearch{max_results}:{query}", "--flat-playlist",
                           "--dump-json", "--no-warnings", "--quiet",
                           "--match-filter", "duration > 120 & duration < 36000"],
                          capture_output=True, text=True, timeout=180)
        for line in r.stdout.strip().split('\n'):
            if not line.strip(): continue
            try:
                j = json.loads(line)
                videos.append({'id': j.get('id',''), 'title': j.get('title',''),
                              'duration': j.get('duration',0), 'course': query, 'src': 'search'})
            except: pass
    except: pass
    return videos

def yt_channel(url, limit=None):
    videos = []
    cmd = [YTDLP, url, "--flat-playlist", "--dump-json", "--no-warnings", "--quiet",
           "--match-filter", "duration > 120 & duration < 36000"]
    if limit: cmd.extend(["--playlist-end", str(limit)])
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        for line in r.stdout.strip().split('\n'):
            if not line.strip(): continue
            try:
                j = json.loads(line)
                videos.append({'id': j.get('id',''), 'title': j.get('title',''),
                              'duration': j.get('duration',0), 'src': url.split('/')[-1]})
            except: pass
    except: pass
    return videos

def current_count():
    c = get_db(); n = c.execute("SELECT count(*) FROM videos").fetchone()[0]; c.close()
    return n

# === MASSIVE CHANNEL LIST ===
# These are channels with 100s-1000s of educational videos each
MEGA_CHANNELS = [
    # MOOCs and Universities (1000+ videos each)
    "https://www.youtube.com/@mitocw/videos",
    "https://www.youtube.com/@stanford/videos",
    "https://www.youtube.com/@YaleCourses/videos",
    "https://www.youtube.com/@npaboratory/videos",
    "https://www.youtube.com/@iaboratory/videos",
    "https://www.youtube.com/@UCBerkeley/videos",
    "https://www.youtube.com/@MIT/videos",
    "https://www.youtube.com/@harvarduniversity/videos",
    "https://www.youtube.com/@PrincetonUniversity/videos",
    "https://www.youtube.com/@columbiauniversity/videos",
    "https://www.youtube.com/@oxford/videos",
    "https://www.youtube.com/@cambridge_uni/videos",
    "https://www.youtube.com/@imperialcollege/videos",
    "https://www.youtube.com/@UCL/videos",
    "https://www.youtube.com/@KingsCollegeLondon/videos",
    "https://www.youtube.com/@LSEpublicevents/videos",
    "https://www.youtube.com/@edinburghuniversity/videos",
    "https://www.youtube.com/@uaboratory/videos",
    "https://www.youtube.com/@ETHzurich/videos",
    "https://www.youtube.com/@epaboratory/videos",
    "https://www.youtube.com/@taboratory/videos",
    "https://www.youtube.com/@uwaterloo/videos",
    "https://www.youtube.com/@mcgilluniversity/videos",
    "https://www.youtube.com/@UofT/videos",
    "https://www.youtube.com/@ANUchannel/videos",
    "https://www.youtube.com/@UNSW/videos",
    "https://www.youtube.com/@MelbourneUniversity/videos",
    "https://www.youtube.com/@NTUsg/videos",
    "https://www.youtube.com/@NUSingapore/videos",
    "https://www.youtube.com/@TsinghuaUniversity/videos",
    "https://www.youtube.com/@peaboratory/videos",
    "https://www.youtube.com/@UTokyo/videos",
    "https://www.youtube.com/@KAIST_official/videos",
    "https://www.youtube.com/@SNU_official/videos",
    "https://www.youtube.com/@TechnionIsrael/videos",
    "https://www.youtube.com/@WeizmannInstitute/videos",
    "https://www.youtube.com/@IITMadras/videos",
    "https://www.youtube.com/@IITBombay/videos",
    "https://www.youtube.com/@IITKanpur/videos",
    "https://www.youtube.com/@IITDelhi/videos",
    "https://www.youtube.com/@IISC_Bangalore/videos",
    # NPTEL (massive Indian MOOC - thousands of lectures)
    "https://www.youtube.com/@naboratory/videos",
    # Big educational creators (100-1000+ videos each)
    "https://www.youtube.com/@KhanAcademy/videos",
    "https://www.youtube.com/@KhanAcademyLabs/videos",
    "https://www.youtube.com/@freaboratory/videos",
    "https://www.youtube.com/@crashcourse/videos",
    "https://www.youtube.com/@TEDx/videos",
    "https://www.youtube.com/@TED/videos",
    "https://www.youtube.com/@TedEd/videos",
    "https://www.youtube.com/@Vsauce/videos",
    "https://www.youtube.com/@Vsauce2/videos",
    "https://www.youtube.com/@Vsauce3/videos",
    "https://www.youtube.com/@veritasium/videos",
    "https://www.youtube.com/@3blue1brown/videos",
    "https://www.youtube.com/@numberphile/videos",
    "https://www.youtube.com/@Computerphile/videos",
    "https://www.youtube.com/@sixtysymbols/videos",
    "https://www.youtube.com/@periodicvideos/videos",
    "https://www.youtube.com/@LexFridman/videos",
    "https://www.youtube.com/@AndrewHubermanLab/videos",
    "https://www.youtube.com/@ProfessorLeonard/videos",
    "https://www.youtube.com/@TheOrganicChemistryTutor/videos",
    "https://www.youtube.com/@DrTrefor/videos",
    "https://www.youtube.com/@MichelvanBiezen/videos",
    "https://www.youtube.com/@patrickjmt/videos",
    "https://www.youtube.com/@blackpenredpen/videos",
    "https://www.youtube.com/@TheMathSorcerer/videos",
    "https://www.youtube.com/@Mathologer/videos",
    "https://www.youtube.com/@MathTheBeautiful/videos",
    "https://www.youtube.com/@StatQuest/videos",
    "https://www.youtube.com/@SentDex/videos",
    "https://www.youtube.com/@TechWithTim/videos",
    "https://www.youtube.com/@CoreySchafer/videos",
    "https://www.youtube.com/@ArjanCodes/videos",
    "https://www.youtube.com/@mCoding/videos",
    "https://www.youtube.com/@BenEater/videos",
    "https://www.youtube.com/@WelchLabs/videos",
    "https://www.youtube.com/@Fireship/videos",
    "https://www.youtube.com/@TheCodingTrain/videos",
    "https://www.youtube.com/@TraversyMedia/videos",
    "https://www.youtube.com/@WebDevSimplified/videos",
    "https://www.youtube.com/@TheNetNinja/videos",
    "https://www.youtube.com/@programmingwithmosh/videos",
    "https://www.youtube.com/@BroCodez/videos",
    "https://www.youtube.com/@NeetCode/videos",
    "https://www.youtube.com/@CSDojo/videos",
    "https://www.youtube.com/@TechWithNana/videos",
    "https://www.youtube.com/@NetworkChuck/videos",
    "https://www.youtube.com/@DavidBombal/videos",
    "https://www.youtube.com/@JohnHammond/videos",
    # Research/Tech talks
    "https://www.youtube.com/@GoogleTechTalks/videos",
    "https://www.youtube.com/@MicrosoftResearch/videos",
    "https://www.youtube.com/@DeepMind/videos",
    "https://www.youtube.com/@OpenAI/videos",
    "https://www.youtube.com/@MetaAI/videos",
    "https://www.youtube.com/@NVIDIADeveloper/videos",
    "https://www.youtube.com/@TwoMinutePapers/videos",
    "https://www.youtube.com/@YannicKilcher/videos",
    "https://www.youtube.com/@AICoffeeBreak/videos",
    "https://www.youtube.com/@TheAIEpiphany/videos",
    "https://www.youtube.com/@AndrejKarpathy/videos",
    "https://www.youtube.com/@uaboratory/videos",
    # Science
    "https://www.youtube.com/@pbsspacetime/videos",
    "https://www.youtube.com/@SmarterEveryDay/videos",
    "https://www.youtube.com/@minutephysics/videos",
    "https://www.youtube.com/@SabineHossenfelder/videos",
    "https://www.youtube.com/@fermilab/videos",
    "https://www.youtube.com/@NileRed/videos",
    "https://www.youtube.com/@SteveMould/videos",
    "https://www.youtube.com/@AlphaPhoenix/videos",
    "https://www.youtube.com/@BranchEducation/videos",
    "https://www.youtube.com/@TheRoyalInstitution/videos",
    "https://www.youtube.com/@WorldScienceFestival/videos",
    "https://www.youtube.com/@InstituteForAdvancedStudy/videos",
    "https://www.youtube.com/@Perimeter/videos",
    "https://www.youtube.com/@KITP/videos",
    "https://www.youtube.com/@SantaFeInstitute/videos",
    # Business/Economics
    "https://www.youtube.com/@YCombinator/videos",
    "https://www.youtube.com/@StanfordGSB/videos",
    "https://www.youtube.com/@HarvardBusinessSchool/videos",
    "https://www.youtube.com/@WhartonSchool/videos",
    "https://www.youtube.com/@MITSloan/videos",
    "https://www.youtube.com/@LondonBusinessSchool/videos",
    # History/Humanities
    "https://www.youtube.com/@OverSimplified/videos",
    "https://www.youtube.com/@KingsandGenerals/videos",
    "https://www.youtube.com/@HistoryMatters/videos",
    "https://www.youtube.com/@EpicHistoryTV/videos",
    "https://www.youtube.com/@Kurzgesagt/videos",
    "https://www.youtube.com/@CGPGrey/videos",
    "https://www.youtube.com/@Wendoverproductions/videos",
    "https://www.youtube.com/@PolyMatter/videos",
    "https://www.youtube.com/@RealLifeLore/videos",
    # Medical/Health
    "https://www.youtube.com/@NinjaNerd/videos",
    "https://www.youtube.com/@ArmandoHasudungan/videos",
    "https://www.youtube.com/@osmosis/videos",
    "https://www.youtube.com/@DrMattMD/videos",
    "https://www.youtube.com/@MedCram/videos",
]

# High-volume search queries (things that return 500+ results)
BULK_QUERIES = [
    # Conference proceedings (thousands of talks)
    "ICML 2024 oral presentation", "ICML 2023 oral presentation",
    "NeurIPS 2024 spotlight", "NeurIPS 2023 spotlight",
    "CVPR 2024 oral", "CVPR 2023 oral",
    "ICLR 2024 talk", "ACL 2024 talk",
    "EMNLP 2023 talk", "AAAI 2024 presentation",
    "KDD 2024 talk", "ICRA 2024 presentation",
    "SIGGRAPH 2024 talk", "CHI 2024 presentation",
    # MOOCs full courses
    "Coursera full course free", "edX full course",
    "MIT OpenCourseWare full lecture series",
    "Stanford Online full course", "Yale Open Courses",
    "NPTEL full course", "NPTEL lecture series",
    "IIT lecture series complete", "IIT NPTEL",
    # Programming tutorials (massive volume)
    "Python full course 2024", "JavaScript full course",
    "React tutorial complete", "Node.js full course",
    "Docker tutorial complete", "Kubernetes full course",
    "AWS tutorial full", "Azure full course",
    "machine learning full course Python",
    "deep learning full course", "data science bootcamp",
    "SQL full course", "MongoDB tutorial complete",
    "Java full course", "C++ full course",
    "Rust programming tutorial", "Go programming full course",
    "Swift programming course", "Kotlin full course",
    "Flutter full course", "React Native tutorial",
    "system design interview", "coding interview preparation",
    "LeetCode solution explained", "algorithm tutorial complete",
    # Science lectures
    "physics lectures complete course playlist",
    "chemistry lectures university full",
    "biology lectures complete course",
    "mathematics lecture series full",
    "astronomy full course university",
    "geology lecture series", "environmental science lecture",
    "quantum physics lecture series",
    "thermodynamics complete course",
    "electromagnetism lecture university",
    # Professional development
    "project management full course", "PMP preparation",
    "Six Sigma training", "lean manufacturing course",
    "financial modeling course", "CFA preparation",
    "GMAT preparation full", "GRE preparation course",
    # Language (huge volume, clean audio)
    "English grammar full course", "IELTS preparation full",
    "TOEFL preparation complete", "Business English course",
    "Spanish full course", "French full course",
    "German full course", "Japanese full course",
    "Chinese Mandarin full course", "Korean full course",
    # Non-English educational content (massive untapped)
    "полный курс лекций университет",
    "лекции по программированию полный курс",
    "лекции по математике университет",
    "лекции по физике полный курс",
    "МФТИ лекции", "МГУ лекции", "ВШЭ лекции",
    "curso completo universidad", "aula completa faculdade",
    "Vorlesung komplett Universität", "cours complet université",
    "lezione completa università", "wykład pełny uniwersytet",
    "大学講義 完全版", "대학 강의 전체",
    "完整课程 大学", "محاضرات جامعة كاملة",
]

def main():
    start = current_count()
    print(f"Aggressive discovery starting. DB: {start} videos. Target: 1M+\n")
    total_new = 0
    
    # Phase 1: Channel crawling (8 parallel)
    print("=== PHASE 1: Mega Channel Crawl ===")
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(yt_channel, ch): ch for ch in MEGA_CHANNELS}
        done = 0
        for f in as_completed(futures):
            ch = futures[f]
            done += 1
            try:
                vids = f.result()
                if vids:
                    n = insert_videos(vids)
                    total_new += n
                    if n > 0:
                        print(f"  [{done}/{len(MEGA_CHANNELS)}] {ch.split('/')[-2]:30s} +{n:5d} ({current_count()} total)")
            except Exception as e:
                pass
    
    print(f"\nPhase 1: +{total_new} new. DB: {current_count()}\n")
    
    # Phase 2: Bulk search
    print("=== PHASE 2: Bulk Search ===")
    random.shuffle(BULK_QUERIES)
    for i, q in enumerate(BULK_QUERIES):
        vids = yt_search(q, 500)
        if vids:
            n = insert_videos(vids)
            total_new += n
            if n > 0:
                print(f"  [{i+1}/{len(BULK_QUERIES)}] '{q[:50]:50s}' +{n:4d} ({current_count()} total)")
        time.sleep(1)
        
        db_total = current_count()
        if db_total >= 1_000_000:
            print("\n🎯 TARGET REACHED: 1M+ videos!")
            break
    
    final = current_count()
    print(f"\n{'='*60}")
    print(f"DONE: {start} → {final} (+{total_new} new)")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
