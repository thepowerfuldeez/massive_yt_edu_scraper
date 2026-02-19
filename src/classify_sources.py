#!/usr/bin/env python3
"""Classify 180K+ unknown YouTube channel/course names - v2 with efficient batch updates."""

import sqlite3
import re
import sys
import time

DB_PATH = '/home/george/academic_transcriptions/massive_production.db'

RISK_MAP = {
    'university_ocw': 'green', 'university_lecture': 'yellow',
    'research_institute': 'yellow', 'museum_cultural': 'yellow',
    'government_public': 'yellow', 'conference': 'yellow',
    'tech_community': 'yellow', 'corporate_talks': 'yellow',
    'coaching_test_prep': 'yellow', 'individual_educator': 'yellow',
    'medical_health': 'yellow', 'non_english_edu': 'yellow',
    'mooc_platform': 'green', 'public_media': 'yellow',
    'religious': 'orange', 'gaming_entertainment': 'red', 'news_media': 'red',
}

def build_rules():
    rules = []
    def add(cat, patterns):
        for p in patterns:
            rules.append((cat, re.compile(p, re.IGNORECASE)))
    
    add('gaming_entertainment', [
        r'\bmtg\b', r'magic.?the.?gathering', r'\bvalorant\b', r'\bfortnite\b',
        r'\besport', r'\bgaming\b', r'\bminecraft\b', r'\broblox\b', r'\bgameplay\b',
        r'\blets?\s*play\b', r'\bspeedrun\b', r'\btwitch\b', r'\bstreamer\b',
        r'\bdarts\b', r'modus.+(?:series|darts)', r'reaction\s*vid', r'\bunboxing\b',
        r'\basmr\b', r'\bprank\b', r'\bfifa\b', r'call\s*of\s*duty', r'\bgta\b',
        r'\bpokemon\b', r'\bpokémon\b', r'strictly\s*better\s*mtg', r'bright\s*side',
        r'\bniche.*(?:game|survival)\b', r'\breacts?\b',
    ])
    add('religious', [
        r'\bchurch\b', r'\bministry\b', r'\bministries\b', r'\bgospel\b', r'\bsermon\b',
        r'\bmosque\b', r'\bdharma\b', r'\bfellowship\b(?!.*university)',
        r'\bbiblical\b', r'\bpastor\b', r'\bpreacher\b', r'\bprayer\b', r'\bworship\b',
        r'\bhymn', r'\bsewadars?\b', r'\bsatsang\b', r'\bkirtan\b', r'\bbhajan\b',
        r'\bjesus\b', r'\bfaith\s*(?:once|deliver|commun)',
        r'dove\s*television', r'joyce\s*meyer', r'\bsadhguru\b',
        r'\bqur.?an\b', r'bible\s*stud', r'\btheolog(?!.*university)',
        r'\bpentecostal\b(?!.*university)', r'\bbaptist\b(?!.*university)',
        r'\beveryday\s*saints\b', r'come\s*follow\s*me',
        r'\bevangel(?:ical|ism)\b(?!.*university)',
        r'\bislamic\b(?!.*university|.*institut)',
        r'\bchristian\b(?!.*university|.*college)',
    ])
    add('mooc_platform', [
        r'\bcoursera\b', r'\budemy\b', r'\bedx\b', r'khan\s*academy',
        r'\bpluralsight\b', r'\bskillshare\b', r'linkedin\s*learning',
        r'\bfreecodecamp\b', r'free\s*code\s*camp',
    ])
    add('university_ocw', [
        r'\bocw\b', r'open\s*course(?:ware)?', r"open\s*university'?s?\s*faculty",
        r'open\s*tuition', r'openmb',
    ])
    add('research_institute', [
        r'\bihes\b', r'\bipam\b', r'\bcern\b', r'perimeter\s*institut',
        r'max\s*planck', r'fields?\s*institut', r'simons\s*(?:foundation|institut|center)',
        r'\bictp\b', r'\bslac\b', r'milken\s*institut', r'\bbrookhaven\b', r'\bfermilab\b',
        r'institut[eo]?\s*(?:de|for|of)\s*(?:advanced|higher|theoretical|physical)',
        r'research\s*(?:institute|centre|center|lab)\b',
        r'new\s*centre\s*for\s*research', r'data\s*science\s*institut',
        r'columbia\s*data\s*science', r'breakthrough\s*science', r'strategic\s*studies',
        r'global\s*institute\s*for', r'\bindica\b', r'\bsniavideo\b',
        r'collège\s*de\s*france', r'kennedy\s*center',
        r'\bcopernicus\b', r'engaged\s*thought', r'video\s*history\s*of',
    ])
    add('university_lecture', [
        r'\buniversity\b', r'\buniversit[àäé]\b', r'\buniversid', r'\buniversit[ée]',
        r'\bcollege\b(?!.*humou?r)', r'\binstitut[eo]?\b(?!.*beauty|.*milken)',
        r'университет', r'大[學学]', r'대학교?',
        r'\biit\b', r'\bnit\b(?!.*pick)', r'\biisc\b',
        r'\bljiet\b', r'\bcsudhtv\b', r'sir\s*c\s*r\s*reddy',
        r'knight\s*campus', r'department\s*(?:of|de)', r'dept\.?\s*(?:of|de)',
        r'faculty\s*(?:of|de)', r'school\s*of',
        r'\bprofessor\b', r'\bprof\.\s', r'assistant\s*professor',
        r'\b(?:mit|stanford|harvard|yale|princeton|caltech|berkeley|oxford|cambridge)\b',
        r'\brsos\b', r'open\s*school', r'kv\s*teachers', r'\bkvs\b',
        r'\bpolytechnic\b', r'\bpolitécnic', r'\bdartmouth\b',
    ])
    add('conference', [
        r'\bconf\d', r'\bconference\b', r'\bsummit\b', r'\bsymposium\b',
        r'\bworkshop\b', r'\bfosdem\b', r'\bpycon\b', r'\brustconf\b',
        r'strange\s*loop', r'\bdefcon\b', r'black\s*hat', r'\bcppcon\b',
        r'\bgophercon\b', r'\bkubecon\b', r're:invent',
        r'\bwebinar\b', r'\bseminar\b(?!.*class)',
        r'bitkom\s*events', r'\btoastmasters?\b', r'\bminicurso\b',
        r'next\s*day\s*video', r'\bieee\b',
    ])
    add('corporate_talks', [
        r'google\s*(?:for|developer|cloud|tech|io|event|talk)',
        r'amazon\s*web\s*services', r'\baws\b(?!.*academy)',
        r'microsoft\s*(?:research|event|ignite|build)',
        r'meta\s*(?:ai|research|event)', r'apple\s*(?:wwdc|developer)',
        r'nvidia\s*(?:gtc|developer)', r'ibm\s*(?:research|think)',
        r'aws\s*japan', r'公式', r'\bneo4j\b', r'\byandex\b',
    ])
    add('tech_community', [
        r'engineers\.sg', r'women\s*who\s*code', r'\bmeetup\b',
        r'user\s*group', r'dev\s*communit', r'\bhackerspace\b',
        r'tech\s*talk', r'techgig', r'\bcoding\s*(?:dojo|club)',
        r'open\s*source\s*communit', r'grassroots\s*community',
    ])
    add('coaching_test_prep', [
        r'\bgate\b(?!.*foundation|.*bill)', r'\bupsc\b', r'\bssc\b(?!.*game)',
        r'\bneet\b', r'\bjee\b', r'\bielts\b', r'\btoefl\b',
        r'\bgmat\b', r'\bcoaching\b', r'\bclasses\b', r'\bacademy\b(?!.*science.*breakthrough)',
        r'\btutorial[s]?\b', r'test\s*(?:prep|series)', r'mock\s*test',
        r'practice\b.*question', r'question.*series', r'\bparcham\b',
        r'\bnabard\b', r'\brrb\b', r'\bntpc\b', r'\bgpat\b', r'\bwbssc\b',
        r'\btet\b(?!.*game)', r'online\s*solution', r'\bcursos?\b', r'\bcurriculo\b',
        r'study\s*(?:with|music|in\s*silence|material)',
        r'\blearn\b', r'\blesson\b', r'\bphonics\b', r'class\s*\d', r'ch[-.]?\d',
        r'chapter\s*\d', r'live\s*session', r'demo\s*class',
        r'(?:math|maths|physics|chemistry|biology)\s*(?:class|tutor|lesson)',
        r'\bvedantu\b', r'\bunacademy\b', r'\bbyju\b',
        r'exam\s*\d{4}', r'\bbackbenchers?\b', r'अध्याय',
        r'\bjkcs\b', r'jr\s*tutorial', r'creative\s*math',
        r'igcse\s*and\s*a\s*levels', r'\bdae\s*math\b',
        r'\bedu\b', r'\beducation\b', r'civil\s*services',
        r'\bvajiram\b', r'\brojgar\b', r'\bpathshala\b',
        r'maths?\s*point', r'\bmonk-?e\b',
    ])
    add('medical_health', [
        r'\bhospital\b', r'\bmedical\b(?!.*university)', r'\bmedicine\b',
        r'\bclinical\b', r'\bnursing\b', r'\bsurger[yia]', r'\bpharmac',
        r'\bdental\b', r'\bhealth\b(?!.*gaming)', r'\bhomoeopath\b',
    ])
    add('government_public', [
        r'\bgovernment\b', r'\bmunicipal\b', r'city\s*council',
        r'public\s*tv', r'\bc-?span\b', r'\bparliament\b',
        r'west\s*hartford\s*communit', r'\bconamaj\b',
        r'community\s*interactive',
    ])
    add('museum_cultural', [
        r'\bmuseum\b', r'\bgallery\b', r'\barchive\b', r'\barchiv\b',
        r'\blibrary\b', r'cultural\s*cent', r'архэ', r'центр',
        r'\bhistoric\b', r'\bheritage\b', r'\bartede\b',
    ])
    add('news_media', [
        r'\bnews\b', r'tv\s*channel', r'\bbroadcast', r'\bmedia\b(?!.*lab)',
        r'\btelevision\b(?!.*dove)', r'press\s*club',
    ])
    add('individual_educator', [
        r'^[A-Z][a-z]+ [A-Z][a-z]+$',
        r'^[A-Z][a-z]+ [A-Z]\.? [A-Z][a-z]+$',
        r'\bby\s+(?:dr|prof|mr|ms|mrs)\b', r'\bsir\s+\w+', r'\bdr\.\s*\w+',
        r'\bprof\.\s*\w+', r'\bcalculate\s*x\b', r'data\s*mastery',
        r'officiel', r'\blectur', r'\bcourse\b', r'\btraining\b',
        r'\bcertificat', r'\bhorse\s*power',
        r'crypto', r'\bhoudini\b', r'\bmathdocu\b',
        r'\bnarrates?\b', r'\bculture\s*vulture\b',
        r'\blotta\b', r'partik[üu]l',
    ])
    add('non_english_edu', [
        r'[\u0900-\u097F]', r'[\u4e00-\u9fff]', r'[\uac00-\ud7af]',
        r'[\u0600-\u06ff]', r'[\u0400-\u04ff]', r'[\u0980-\u09ff]',
        r'[\u0a00-\u0a7f]', r'[\u0b80-\u0bff]', r'[\u0c00-\u0c7f]',
        r'[\u0c80-\u0cff]', r'[\u0d00-\u0d7f]', r'[\u0e00-\u0e7f]',
        r'[\u3040-\u309f]', r'[\u30a0-\u30ff]',
    ])
    return rules

SKIP_SOURCES = {'channel_crawl', 'related', 'search', 'playlist', '', 'videos', 'Shorts'}

def classify_course(name, rules):
    if not name or name.strip() in SKIP_SOURCES:
        return None
    text = name.strip()
    for cat, pattern in rules:
        if pattern.search(text):
            if cat == 'religious' and re.search(r'university|college|institut', text, re.I):
                continue
            return (cat, RISK_MAP[cat])
    return None

def main():
    db = sqlite3.connect(DB_PATH)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA cache_size=-200000")  # 200MB cache
    c = db.cursor()
    
    c.execute("SELECT SUM(duration_seconds)/3600.0 FROM videos WHERE content_category IS NOT NULL AND content_category != '' AND content_category != 'unknown'")
    initial_hours = c.fetchone()[0] or 0
    c.execute("SELECT SUM(duration_seconds)/3600.0 FROM videos")
    total_hours = c.fetchone()[0] or 0
    print(f"Initial: {initial_hours:,.0f} / {total_hours:,.0f} hours classified ({initial_hours/total_hours*100:.1f}%)")
    sys.stdout.flush()
    
    # Get distinct unknown courses
    print("Querying unknown courses...")
    sys.stdout.flush()
    c.execute("SELECT DISTINCT course FROM videos WHERE content_category = 'unknown' AND course IS NOT NULL AND course != ''")
    courses = [r[0] for r in c.fetchall()]
    print(f"Got {len(courses)} distinct courses")
    sys.stdout.flush()
    
    rules = build_rules()
    print(f"Built {len(rules)} rules")
    
    # Classify all courses
    classifications = {}  # course -> (cat, risk)
    t0 = time.time()
    for name in courses:
        result = classify_course(name, rules)
        if result:
            classifications[name] = result
    
    # Special cases
    for course, cat, risk in [
        ('teach-in', 'conference', 'yellow'),
        ('Course Backup', 'individual_educator', 'yellow'),
        ('niche-academic', 'individual_educator', 'yellow'),
        ('institutional', 'university_lecture', 'yellow'),
        ('Default', 'individual_educator', 'yellow'),
    ]:
        if course not in classifications:
            classifications[course] = (cat, risk)
    
    elapsed = time.time() - t0
    print(f"Classified {len(classifications)}/{len(courses)} courses in {elapsed:.1f}s")
    sys.stdout.flush()
    
    # Category breakdown
    cat_counts = {}
    for cat, risk in classifications.values():
        cat_counts[cat] = cat_counts.get(cat, 0) + 1
    for cat, cnt in sorted(cat_counts.items(), key=lambda x: -x[1]):
        print(f"  {cat}: {cnt} courses")
    sys.stdout.flush()
    
    # Create temp table and do batch UPDATE via JOIN
    print("\nCreating temp classification table...")
    sys.stdout.flush()
    
    c.execute("DROP TABLE IF EXISTS temp_classifications")
    c.execute("CREATE TABLE temp_classifications (course TEXT PRIMARY KEY, content_category TEXT, license_risk TEXT)")
    
    # Insert classifications in batches
    batch = [(course, cat, risk) for course, (cat, risk) in classifications.items()]
    for i in range(0, len(batch), 10000):
        c.executemany("INSERT INTO temp_classifications VALUES (?, ?, ?)", batch[i:i+10000])
    db.commit()
    print(f"Inserted {len(batch)} classifications into temp table")
    sys.stdout.flush()
    
    # Now do a single UPDATE using subquery
    print("Updating videos table (single batch UPDATE)...")
    sys.stdout.flush()
    t0 = time.time()
    
    c.execute("""
        UPDATE videos 
        SET content_category = (SELECT tc.content_category FROM temp_classifications tc WHERE tc.course = videos.course),
            license_risk = (SELECT tc.license_risk FROM temp_classifications tc WHERE tc.course = videos.course)
        WHERE content_category = 'unknown'
        AND course IN (SELECT course FROM temp_classifications)
    """)
    updated = c.rowcount
    db.commit()
    elapsed = time.time() - t0
    print(f"Updated {updated:,} videos in {elapsed:.1f}s")
    sys.stdout.flush()
    
    # Update source_categories
    print("Updating source_categories...")
    c.execute("""
        UPDATE source_categories
        SET content_category = (SELECT tc.content_category FROM temp_classifications tc WHERE tc.course = source_categories.source_value),
            license_risk = (SELECT tc.license_risk FROM temp_classifications tc WHERE tc.course = source_categories.source_value)
        WHERE content_category = 'unknown'
        AND source_value IN (SELECT course FROM temp_classifications)
    """)
    db.commit()
    print(f"Updated {c.rowcount:,} source_categories")
    sys.stdout.flush()
    
    c.execute("DROP TABLE temp_classifications")
    db.commit()
    
    # === TIER 3: Title-based for remaining unknowns ===
    print(f"\n=== Tier 3: Title-based classification ===")
    sys.stdout.flush()
    
    c.execute("SELECT COUNT(*), COALESCE(SUM(duration_seconds),0)/3600.0 FROM videos WHERE content_category = 'unknown'")
    unk_count, unk_hours = c.fetchone()
    print(f"Still unknown: {unk_count:,} videos ({unk_hours:,.0f} hours)")
    sys.stdout.flush()
    
    title_updates = [
        ('university_lecture', 'yellow', [
            "% University%", "% College %", "%Professor %", "%Prof. %",
            "%Department of%", "%Faculty of%", "%School of %",
            "Lecture 1%", "Lecture 2%", "Lecture 3%", "Lecture 4%",
            "Lecture 5%", "Lecture 6%", "Lecture 7%", "Lecture 8%",
            "Lecture 9%", "Lecture 0%", "%| Lecture %",
        ]),
        ('conference', 'yellow', [
            "%Conference%", "%Symposium%", "%Keynote%",
            "%PyCon%", "%FOSDEM%", "%CppCon%", "%GopherCon%",
        ]),
        ('coaching_test_prep', 'yellow', [
            "% GATE %", "%UPSC%", "%NEET %", "%JEE %",
            "%Tutorial %", "%Chapter %",
        ]),
    ]
    
    tier3_total = 0
    for cat, risk, patterns in title_updates:
        like_clause = " OR ".join(["title LIKE ?" for _ in patterns])
        c.execute(f"UPDATE videos SET content_category=?, license_risk=? WHERE content_category='unknown' AND ({like_clause})",
                  [cat, risk] + patterns)
        cnt = c.rowcount
        if cnt:
            tier3_total += cnt
            print(f"  {cat}: {cnt} videos")
        db.commit()
    print(f"Tier 3 total: {tier3_total:,} videos")
    sys.stdout.flush()
    
    # === FINAL STATS ===
    print(f"\n{'='*60}")
    print("FINAL RESULTS")
    print(f"{'='*60}")
    
    c.execute("""SELECT content_category, COUNT(*), ROUND(SUM(duration_seconds)/3600.0)
                 FROM videos GROUP BY content_category ORDER BY SUM(duration_seconds) DESC""")
    rows = c.fetchall()
    
    total_classified = 0
    total_all = 0
    print(f"\n{'Category':<25} {'Videos':>10} {'Hours':>12}")
    print("-" * 50)
    for cat, count, hours in rows:
        hours = hours or 0
        total_all += hours
        if cat and cat != 'unknown' and cat != '':
            total_classified += hours
        print(f"{(cat or 'NULL'):<25} {count:>10,} {hours:>12,.0f}")
    
    print("-" * 50)
    pct = total_classified / total_all * 100 if total_all else 0
    print(f"\nClassified: {total_classified:,.0f} / {total_all:,.0f} hours ({pct:.1f}%)")
    
    c.execute("""SELECT license_risk, COUNT(*), ROUND(SUM(duration_seconds)/3600.0)
                 FROM videos WHERE content_category != 'unknown' AND content_category IS NOT NULL AND content_category != ''
                 GROUP BY license_risk ORDER BY SUM(duration_seconds) DESC""")
    print(f"\nRisk breakdown:")
    for risk, count, hours in c.fetchall():
        print(f"  {risk or 'NULL':<10} {count:>10,} videos  {hours:>10,.0f} hours")
    
    target = total_all * 0.5
    gap = target - total_classified
    print(f"\n50% target: {target:,.0f} hours | Current: {total_classified:,.0f} hours")
    if gap <= 0:
        print("✅ 50% TARGET REACHED!")
    else:
        print(f"❌ Need {gap:,.0f} more hours ({gap/total_all*100:.1f}%)")
    
    db.close()

if __name__ == '__main__':
    main()
