#!/usr/bin/env python3
"""Classify unknown videos by title analysis - efficient version."""
import sqlite3
import re
import time
import sys

DB = '/home/george/academic_transcriptions/massive_production.db'

def build_rules():
    rules = []
    
    # RED: Gaming
    rules.append((re.compile(r'(?i)\b(minecraft|fortnite|valorant|league of legends|cs[:\s]?go|counter.?strike|dota\s*2|fifa\s*\d|gta|grand theft auto|call of duty|overwatch|apex legends|roblox|world of warcraft|elden ring|dark souls|skyrim|zelda|animal crossing|smash bros|diablo|path of exile|starcraft|rocket league|pubg|among us|genshin impact|honkai)\b'), 'gaming_entertainment', 'red'))
    rules.append((re.compile(r'(?i)\b(gameplay|let.?s play|playthrough|speedrun|gaming highlights|montage|kill compilation|victory royale|funny moments gaming)\b'), 'gaming_entertainment', 'red'))
    rules.append((re.compile(r'(?i)\b(reaction video|drama alert|prank gone|mukbang|try not to laugh)\b'), 'gaming_entertainment', 'red'))
    rules.append((re.compile(r'(?i)\b(official\s*(music\s*)?video|full\s*album|music\s*video|lyric\s*video|karaoke)\b'), 'gaming_entertainment', 'red'))
    
    # GREEN: OCW
    rules.append((re.compile(r'(?i)\b(mit\s*open\s*course|opencourseware|open\s*yale)\b'), 'university_ocw', 'green'))
    
    # ORANGE: Religious
    rules.append((re.compile(r'(?i)\b(sermon|bible\s*(study|teaching)|quran\s*(recit|tafsir)|church\s*service|worship\s*service|prayer\s*meeting|gospel\s*(message|preach)|sunday\s*service|friday\s*khutbah|pastor\s+\w+|devotional|praise\s*and\s*worship)\b'), 'religious', 'orange'))
    
    # ORANGE: News
    rules.append((re.compile(r'(?i)\b(breaking\s*news|news\s*headlines|press\s*conference|daily\s*news|news\s*bulletin|live\s*news)\b'), 'news_media', 'orange'))
    
    # YELLOW: University (specific)
    rules.append((re.compile(r'(?i)\b(MIT|Stanford|Harvard|Yale|Princeton|Caltech|Berkeley|CMU|Cornell|Columbia|Oxford|Cambridge|Imperial College|UCL|Edinburgh|ETH|EPFL)\s+(CS|EE|Math|Phys|Econ|Chem|Bio|Stat|Seminar|Lecture|Course)'), 'university_lecture', 'yellow'))
    rules.append((re.compile(r'(?i)\bIIT\s+(Bombay|Delhi|Madras|Kanpur|Kharagpur|Roorkee|Guwahati|Hyderabad|BHU|Indore|Mandi|Patna)'), 'university_lecture', 'yellow'))
    rules.append((re.compile(r'(?i)\b(NIT|IIIT|IISc|IISC|JNU|BHU|AMU|BITS\s*Pilani)\b'), 'university_lecture', 'yellow'))
    rules.append((re.compile(r'(?i)(University\s+of\s+\w+|(\w+\s+)?University|Universit[éà]|Universität|Университет|大[学學]|대학교).{0,30}(lecture|lec\b|class|course|seminar)'), 'university_lecture', 'yellow'))
    
    # Course codes
    rules.append((re.compile(r'(?i)\b(CS|MATH|PHYS|EE|ECE|EECS|CHEM|BIO|STAT|ECON|ME|CE|CHE|AE|MSE)\s*[0-9]{2,4}\b'), 'university_lecture', 'yellow'))
    
    # Lecture numbering
    rules.append((re.compile(r'(?i)\b(Lecture|Lec|Lect)\s*[#.]?\s*[0-9]{1,3}\b'), 'university_lecture', 'yellow'))
    rules.append((re.compile(r'(?i)Mod[\-\s]*\d+\s*Lec[\-\s]*\d+'), 'university_lecture', 'yellow'))
    rules.append((re.compile(r'(?i)\b(Module|Week|Unit)\s*[#.]?\s*[0-9]{1,2}\s*[\-:|]'), 'university_lecture', 'yellow'))
    
    # Conference
    rules.append((re.compile(r'(?i)\b(NeurIPS|ICML|ICLR|CVPR|ECCV|ICCV|EMNLP|AAAI|SIGCHI|KDD|PyCon|RustConf|GopherCon|JSConf|FOSDEM|Strange\s*Loop|QCon|Devoxx|SIGGRAPH|ICRA|IROS|USENIX|NAACL|INTERSPEECH|ICASSP)\b'), 'conference', 'yellow'))
    rules.append((re.compile(r'(?i)\b(keynote\s*(speech|address|talk)|invited\s*talk|paper\s*presentation|panel\s*discussion)\b'), 'conference', 'yellow'))
    
    # Research
    rules.append((re.compile(r'(?i)\b(seminar|colloquium|symposium)\s+(on\s+)?\w'), 'research_institute', 'yellow'))
    
    # Test prep
    rules.append((re.compile(r'(?i)\b(GATE\s*(20\d\d|CS|EC|EE|ME|CE)|UPSC|SSC\s*(CGL|CHSL|GD|MTS)|NEET|JEE\s*(Main|Advanced)|GRE|GMAT|IELTS|TOEFL|CAT\s*(20\d\d|Prep|Quant)|SAT\s*(Prep|Math)|CBSE|ICSE|Class\s*(9|10|11|12)\s*(Math|Phys|Chem|Bio|Science))\b'), 'coaching_test_prep', 'yellow'))
    rules.append((re.compile(r'(?i)\b(previous\s*year\s*(paper|question)|exam\s*prep|mock\s*test|practice\s*(test|paper)|solved\s*(paper|question)|board\s*exam|entrance\s*exam|competitive\s*exam)\b'), 'coaching_test_prep', 'yellow'))
    
    # Medical
    rules.append((re.compile(r'(?i)\b(surgery|surgical\s*technique|clinical\s*(case|exam)|diagnosis\s*of|treatment\s*of|medical\s*(lecture|education)|nursing\s*(lecture|education)|anatomy\s*(lecture|of\s+the)|physiology|pathology|pharmacology|radiology|cardiology|neurology|oncology|dermatology|gastroenterology|nephrology|ophthalmology|psychiatry|histology|microbiology\s*(lecture|of))\b'), 'medical_health', 'yellow'))
    
    # Corporate/Tech
    rules.append((re.compile(r'(?i)\b(AWS\s*(re:?Invent|Summit|tutorial|cert)|Azure\s*(tutorial|cert|fundamentals)|Google\s*Cloud|GCP\s*(tutorial|cert)|Kubernetes\s*(tutorial|course)|Docker\s*(tutorial|course)|Terraform\s*(tutorial|course)|DevOps\s*(tutorial|course))\b'), 'corporate_talks', 'yellow'))
    
    # Individual educator (broad)
    rules.append((re.compile(r'(?i)\b(tutorial|how\s+to\s+(build|make|create|install|setup|configure|deploy|use|code|program)|introduction\s+to|fundamentals\s+of|principles\s+of|basics\s+of|beginner.?s?\s*(guide|tutorial)|complete\s*(guide|course|tutorial)|crash\s*course|full\s*course|free\s*course|learn\s+\w+\s+in|deep\s*dive)\b'), 'individual_educator', 'yellow'))
    
    # Programming + tutorial
    rules.append((re.compile(r'(?i)\b(Python|JavaScript|Java|C\+\+|C#|Rust|Go|TypeScript|Swift|Kotlin|Ruby|PHP)\s+(tutorial|course|lecture|programming|for\s+beginners|full\s+course|crash\s+course)'), 'individual_educator', 'yellow'))
    
    # Framework + tutorial
    rules.append((re.compile(r'(?i)\b(React|Angular|Vue|Next\.?js|Django|Flask|FastAPI|Spring\s*Boot|Node\.?js|Laravel|TensorFlow|PyTorch|Flutter)\s+(tutorial|course|crash|project|for\s+beginners)'), 'individual_educator', 'yellow'))
    
    # ML/DS educational
    rules.append((re.compile(r'(?i)\b(machine\s*learning|deep\s*learning|artificial\s*intelligence|neural\s*network|natural\s*language\s*processing|computer\s*vision|data\s*science|data\s*structure|algorithm)\s*(tutorial|course|lecture|explained|introduction|fundamentals)'), 'individual_educator', 'yellow'))
    
    return rules

def main():
    conn = sqlite3.connect(DB)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-2000000")  # 2GB cache
    
    rules = build_rules()
    print(f"Built {len(rules)} rules", flush=True)
    
    # Get all unknown video ids, titles, priorities in one query
    print("Loading unknown videos...", flush=True)
    t0 = time.time()
    cur = conn.execute("""SELECT id, title, priority FROM videos 
                         WHERE (content_category = 'unknown' OR content_category IS NULL) 
                         AND title IS NOT NULL""")
    rows = cur.fetchall()
    print(f"Loaded {len(rows)} videos in {time.time()-t0:.1f}s", flush=True)
    
    # Classify
    t0 = time.time()
    updates = {}  # (cat, risk) -> [ids]
    unmatched = 0
    priority_classified = 0
    
    for i, (vid_id, title, priority) in enumerate(rows):
        if i % 200000 == 0 and i > 0:
            print(f"  {i}/{len(rows)}...", flush=True)
        
        matched = False
        for pat, cat, risk in rules:
            if pat.search(title):
                key = (cat, risk)
                if key not in updates:
                    updates[key] = []
                updates[key].append(vid_id)
                matched = True
                break
        
        if not matched:
            if priority and priority >= 8:
                key = ('unclassified_educational', 'yellow')
                if key not in updates:
                    updates[key] = []
                updates[key].append(vid_id)
                priority_classified += 1
            else:
                unmatched += 1
    
    classify_time = time.time() - t0
    total_classified = sum(len(ids) for ids in updates.values())
    print(f"\nClassified {total_classified} videos in {classify_time:.1f}s", flush=True)
    print(f"Unmatched (still unknown): {unmatched}", flush=True)
    print(f"Priority-based fallback: {priority_classified}", flush=True)
    
    # Apply updates
    print("\nApplying updates to DB...", flush=True)
    t0 = time.time()
    for (cat, risk), ids in updates.items():
        # Batch in chunks of 10000
        for j in range(0, len(ids), 10000):
            chunk = ids[j:j+10000]
            placeholders = ','.join('?' * len(chunk))
            conn.execute(f"UPDATE videos SET content_category = ?, license_risk = ? WHERE id IN ({placeholders})",
                        [cat, risk] + chunk)
        conn.commit()
        print(f"  {cat:30s} {risk:8s} {len(ids):>8d}", flush=True)
    
    # Set remaining NULLs
    conn.execute("UPDATE videos SET content_category = 'unknown', license_risk = 'yellow' WHERE content_category IS NULL")
    conn.commit()
    
    update_time = time.time() - t0
    print(f"Updates applied in {update_time:.1f}s", flush=True)
    
    # Step 3: Reject RED
    cur = conn.execute("UPDATE videos SET status = 'rejected' WHERE license_risk = 'red' AND status = 'pending'")
    conn.commit()
    print(f"\nRejected {cur.rowcount} RED videos from queue", flush=True)
    
    # Final stats
    print("\n=== FINAL CLASSIFICATION STATS ===", flush=True)
    cur = conn.execute("SELECT content_category, license_risk, COUNT(*), ROUND(SUM(duration_seconds)/3600.0) FROM videos GROUP BY content_category, license_risk ORDER BY COUNT(*) DESC")
    for row in cur.fetchall():
        cat = row[0] or 'NULL'
        risk = row[1] or 'NULL'
        print(f"  {cat:30s} {risk:8s} {row[2]:>8d} videos  {row[3]:>10.0f} hrs", flush=True)
    
    cur = conn.execute("SELECT COUNT(*), ROUND(SUM(duration_seconds)/3600.0) FROM videos WHERE content_category != 'unknown'")
    c = cur.fetchone()
    cur = conn.execute("SELECT COUNT(*), ROUND(SUM(duration_seconds)/3600.0) FROM videos")
    t = cur.fetchone()
    
    print(f"\nClassified: {c[0]:,} videos ({100*c[0]/t[0]:.1f}%), {c[1]:,.0f} hrs ({100*c[1]/t[1]:.1f}%)", flush=True)
    print(f"Still unknown: {t[0]-c[0]:,} videos", flush=True)
    
    print("\n=== RISK DISTRIBUTION ===", flush=True)
    cur = conn.execute("SELECT license_risk, COUNT(*), ROUND(SUM(duration_seconds)/3600.0) FROM videos GROUP BY license_risk ORDER BY COUNT(*) DESC")
    for row in cur.fetchall():
        print(f"  {row[0]:8s} {row[1]:>8d} videos  {row[2]:>10.0f} hrs", flush=True)
    
    conn.close()
    print("\nDone!", flush=True)

if __name__ == '__main__':
    main()
