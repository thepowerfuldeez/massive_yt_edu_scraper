#!/usr/bin/env python3
"""
Comprehensive quality filter for educational YouTube content.
Used by all discovery scripts and can retroactively clean the queue.

Two components:
1. REJECT patterns - definitively non-educational (removed from queue)
2. EDU_BOOST patterns - high-confidence educational (priority 8-9)

Design: False negatives (missing some edu) are OK.
        False positives (letting junk through) are NOT OK for a quality dataset.
"""

import re

# ============================================================
# REJECT FILTER — Videos matching these are NOT educational
# ============================================================
# Organized by category. Each line is a separate pattern for clarity.

REJECT_PATTERNS = re.compile('|'.join([
    # === ENTERTAINMENT / DRAMA / FICTION ===
    r'短剧',           # Chinese short drama
    r'总裁',           # Chinese CEO romance drama  
    r'重生',           # Chinese rebirth/reborn drama
    r'甜宠',           # Chinese sweet romance
    r'完結',           # Chinese series ending
    r'大结局',         # Chinese grand finale
    r'#cdrama',
    r'#chinesedrama',
    r'#koreandrama',
    r'#kdrama',
    r'#泰剧',          # Thai drama
    r'mafia boss',
    r'CEO spoil',
    r'billionaire spoil',
    r'addicted to her',
    r'obsessed with her',
    r'dotes on her',
    r'pampered by',
    r'spoiled nightly',
    r'spoils? (me|her|him) boundless',
    r'reborn.{0,20}(princess|CEO|boss|marry|husband|wife)',
    r'humiliat.{0,30}(marry|wed|boss)',
    r'forced to (marry|wed)',
    r'betray.{0,20}(marry|wed|husband)',
    r'revenge.{0,20}(marry|wed|CEO|boss)',
    r'full movie\]',
    r'full movie[)\s]',
    r'\[Full Movie\b',
    r'Revenge Thriller',
    r'love story.{0,20}(CEO|boss|billionaire)',
    
    # === MUSIC ===
    r'\bmusic video\b',
    r'\bofficial (music )?video\b',
    r'\blyric(s)? video\b',
    r'\bofficial audio\b',
    r'\bofficial MV\b',
    r'\bcover song\b',
    r'\bkaraoke\b',
    r'\bremix\b',
    r'\bplaylist\s*[|·•-]\s*(chill|study|lofi|relax)',  # "playlist | chill" type
    r'\blofi\b.*\b(beats|hip hop|study|chill)\b',
    r'\bchill\b.*\b(beats|vibes|mix)\b',
    r'\b(lo-fi|lo fi)\b.*\b(beats|music)\b',
    r'\bstudy music\b',
    r'\brelaxing music\b',
    r'\bsleep music\b',
    r'\brain sounds?\b',
    r'\bwhite noise\b',
    r'\bnature sounds?\b.*\b(sleep|relax)',
    r'\bambient\b.*\b(music|sounds?)\b',
    r'\bconcert\b.*\b(live|full)\b',
    r'\bmusic for (study|sleep|relax|focus)',
    
    # === ASMR ===
    r'\bASMR\b',
    r'\basmr\b',
    r'\btingles\b',
    r'\bwhisper(ing|ed)?\b.*\b(roleplay|triggers)',
    r'\bsoft.spoken\b',
    
    # === GAMING ===
    r"\blet'?s?\s*play\b",
    r'\bgameplay\b',
    r'\bwalkthrough\b',
    r'\bplaythrough\b',
    r'\bspeedrun\b',
    r'\bfortnite\b',
    r'\bminecraft\b(?!.*\b(redstone|computer|tutorial|education))',  # allow Minecraft CS
    r'\broblox\b',
    r'\bgta\s*[v5]?\b',
    r'\bcall of duty\b',
    r'\bfifa\b\s*\d',
    r'\bleague of legends\b',
    r'\bvalorfortnite\b',
    r'\bgaming\b(?!.*\b(theory|industry|design|history|AI))',  # allow game theory/design
    r'\bstream highlight',
    r'\btwitch\b.*\b(stream|clip|highlight)',
    r'\bgamer\b(?!.*\b(gate|theory))',
    r'\besports?\b',
    
    # === LIFESTYLE / VLOGS ===
    r'\bvlog\b',
    r'\b(morning|night|skincare|gym) routine\b',
    r'\bget ready with me\b',
    r'\bgrwm\b',
    r'\bday in (my|the) life\b',
    r'\bwhat i eat\b',
    r'\b(room|house|apartment|closet|fridge) tour\b',
    r'\bgrocery haul\b',
    r'\bclothing haul\b',
    r'\btry.on haul\b',
    r'\bhaul\b.*\b(shein|zara|primark|h&m)',
    r'\bunboxing\b(?!.*\b(lab|equipment|kit))',  # allow lab equipment unboxing
    r'\bfamily vlog\b',
    r'\btravel vlog\b',
    r'\bcouples?\b.*\b(challenge|prank|react)',
    
    # === PRANKS / CHALLENGES / REACTION ===
    r'\bprank\b(?!.*\b(call|theory|psychology))',
    r'\b(24|48|72) hour challenge\b',
    r'\btry not to (laugh|cry|cringe)\b',
    r'\breaction\b(?!.*\b(mechanism|chemical|nuclear|rate|kinetics))',  # allow chem reactions
    r'\breact(s|ing)?\b.*\b(to|video|trailer)',
    r'\bchallenge video\b',
    r'\bfood challenge\b',
    r'\bsocial experiment\b(?!.*\b(research|psychology|study))',
    r'\bDhar Mann\b',
    
    # === TABLOID / CLICKBAIT / GOSSIP ===
    r'\bcelebrity\b.*\b(gossip|scandal|drama)',
    r'\b(tea|☕)\b.*\b(spill|drama)',
    r'\bexposed\b(?!.*\b(API|vulnerability|radiation|surface))',  # allow tech/science
    r'\bcancelled?\b(?!.*\b(class|course|flight|event))',
    r'\bdrama\b.*\b(update|alert|channel)',
    r'\bstorytime\b(?!.*\b(read|children|book))',
    
    # === RELIGIOUS PREACHING (not academic study of religion) ===
    r'\bsermon\b.*\b(sunday|church|pastor)',
    r'\bworship\b.*\b(live|service|night)',
    r'\bprayer\b.*\b(meeting|live|service)',
    r'\bbibl(e|ical)\b.*\b(devotion|worship|prayer)',
    # Note: academic religion studies (theology courses, philosophy of religion) are KEPT
    
    # === SPORTS (non-educational) ===
    r'\b(match|game) highlight',
    r'\bfull (match|game|fight)\b(?!.*\b(theory|analysis|lecture))',
    r'\b(NBA|NFL|FIFA|UFC|WWE|boxing|MMA)\b(?!.*\b(analytics|statistics|physics|economics))',
    r'\bgoals?\s*&?\s*highlights?\b',
    
    # === COOKING (non-educational, recipe-style) ===
    r'\brecipe\b(?!.*\b(algorithm|success|formula|molecular))',
    r'\bmukbang\b',
    r'\beating\b.*\b(challenge|show|sounds?)\b',
    r'\bwhat i (ate|cooked)\b',
    # Note: food science, molecular gastronomy, nutrition courses are KEPT
    
    # === CONSPIRACY / PSEUDOSCIENCE ===
    r'\bflat earth\b(?!.*\b(debunk|refut|myth))',
    r'\bchemtrail\b',
    r'\billuminati\b(?!.*\b(history|documentary|debunk))',
    r'\bQ\s*Anon\b',
    
    # === KIDS CONTENT (non-educational) ===
    r'\bnursery rhyme\b',
    r'\bkids (song|cartoon|animation)\b',
    r'\bcocomelon\b',
    r'\bbaby shark\b',
    r'\bpeppa pig\b',
    # Note: children's educational content (SciShow Kids, Khan Academy Kids) is KEPT
    
    # === LOW QUALITY SIGNALS ===
    r'#shorts\b',
    r'\bsatisfying\b.*\b(video|compilation)',
    r'\boddly satisfying\b',
    r'\bcompilation\b(?!.*\b(lecture|course|compiler))',
    r'\bfunny moments?\b',
    r'\bepic fail\b',
    r'\btop\s*\d+\s*(most|best|worst|scariest|craziest)',
    r'\b(you won\'?t believe|what happens next|gone wrong)\b',
]), re.IGNORECASE)

# ============================================================
# EDUCATIONAL BOOST — High-confidence educational content
# ============================================================

# Priority 9: Strongest educational signals
EDU_PRIORITY_9 = re.compile('|'.join([
    # === UNIVERSITY COURSES (numbered) ===
    r'\b(CS|EE|ECE|EECS|MATH|PHYS|CHEM|BIO|STAT|ECON)\s*\d{2,4}\b',
    r'\bMIT\s+\d+\.\d+',          # MIT 6.006, MIT 18.06 etc
    r'\b(6|18|8|7|14|15)\.\d{2,3}\b',  # MIT course numbers
    r'\bCS\d{3}\b',
    r'\bLecture\s+\d+\s*[\|:\-–]',  # "Lecture 1 |" "Lecture 12:"
    r'\bLec\s+\d+\b',
    r'\bWeek\s+\d+\s*[\|:\-–]',
    r'\bModule\s+\d+\b.*\b(lecture|class)',
    
    # === KNOWN MOOC/UNIVERSITY BRANDS ===
    r'\bMIT\s*OpenCourseWare\b',
    r'\bOCW\b',
    r'\bNPTEL\b',
    r'\bCoursera\b',
    r'\bedX\b',
    r'\bKhan Academy\b',
    r'\bStanford\s+(CS|EE|MATH|Online)',
    r'\bHarvard\s+(CS|STAT|Extension)',
    r'\bYale\s+(Open|Courses)',
    r'\bBerkeley\s+(CS|EE|MATH|STAT)',
    r'\bCaltech\b.*\b(lecture|course)',
    r'\bCarnegie\s+Mellon\b',
    r'\bGeorgia\s+Tech\b.*\b(lecture|course)',
    
    # === COURSE STRUCTURE SIGNALS ===
    r'\bfull course\b',
    r'\bcomplete course\b',
    r'\bcourse playlist\b',
    r'\blecture series\b',
    r'\blecture notes?\b',
    r'\bsyllabus\b',
    r'\bfinal exam\b.*\b(review|prep)',
    r'\bmidterm\b.*\b(review|prep)',
    r'\brecitation\b',
    r'\bproblem set\b',
    r'\boffice hours?\b.*\b(lecture|professor|prof)',
    
    # === ACADEMIC CONFERENCES ===
    r'\b(ICML|NeurIPS|NIPS|CVPR|ICLR|ACL|EMNLP|AAAI|IJCAI|KDD|SIGMOD|VLDB|SOSP|OSDI)\b',
    r'\b(NSDI|SIGCOMM|CCS|Oakland|USENIX|PLDI|POPL|ICFP|OOPSLA|ISCA|MICRO|ASPLOS)\b',
    r'\b(CHI|SIGGRAPH|RSS|ICRA|IROS|CoRL|ICCV|ECCV|WACV|INTERSPEECH|ICASSP)\b',
    r'\boral\s+presentation\b',
    r'\bspotlight\s+(talk|presentation)\b',
    r'\binvited\s+talk\b',
    r'\bkeynote\b.*\b(address|speech|lecture)',
    
    # === FOREIGN LANGUAGE COURSE MARKERS ===
    r'\bлекция\b',             # Russian: lecture
    r'\bкурс\b',              # Russian: course  
    r'\bуниверситет\b',        # Russian: university
    r'\bМФТИ\b',              # MIPT
    r'\bМГУ\b',              # Moscow State
    r'\bВШЭ\b',              # Higher School of Economics
    r'\bVorlesung\b',         # German: lecture
    r'\bKurs\b.*\bUniversität\b',
    r'\bcours\s+magistral\b',  # French: lecture course
    r'\bSorbonne\b',
    r'\bcorso\b.*\buniversità\b', # Italian
    r'\bwykład\b',             # Polish: lecture
    r'\bföreläsning\b',       # Swedish: lecture
    r'\b강의\b',              # Korean: lecture
    r'\b講義\b',              # Japanese: lecture
    r'\b授業\b',              # Japanese: class
    r'\b课程\b',              # Chinese: course (educational context)
    r'\b公开课\b',            # Chinese: open course
    
]), re.IGNORECASE)

# Priority 8: Good educational signals
EDU_PRIORITY_8 = re.compile('|'.join([
    # === GENERIC ACADEMIC ===
    r'\blecture\b',
    r'\bcourse\b(?!.*\bgolf\b)',  # not golf course
    r'\btutorial\b',
    r'\bseminar\b',
    r'\bworkshop\b(?!.*\b(wood|craft|DIY)\b)',  # not woodworking
    r'\bclass\b(?!.*\b(first|business|world)\b)',  # not "first class" / "world class"
    r'\bbootcamp\b',
    r'\bmasterclass\b',
    r'\bsyllabus\b',
    
    # === UNIVERSITIES ===
    r'\buniversity\b',
    r'\buniversité\b',
    r'\bUniversität\b',
    r'\buniversità\b',
    r'\buniversidad\b',
    r'\buniversidade\b',
    r'\bprofessor\b',
    r'\bprof\.\s+\w+',
    r'\bDr\.\s+\w+.*\b(lecture|talk|course)',
    
    # === SUBJECT DEPTH MARKERS ===
    r'\bintroduction to\b',
    r'\bintro to\b',
    r'\bfundamentals of\b',
    r'\bprinciples of\b',
    r'\badvanced\b.*\b(topics?|course|lecture)',
    r'\bgraduate\b.*\b(course|lecture|level|seminar)',
    r'\bundergraduate\b',
    r'\bpostgraduate\b',
    r'\bPhD\b.*\b(course|lecture|defense|thesis)',
    r'\bdoctoral\b.*\b(defense|thesis|dissertation)',
    
    # === CHAPTER/PART NUMBERING ===
    r'\bchapter\s+\d+\b',
    r'\bpart\s+\d+\b.*\b(lecture|course|tutorial|series)',
    r'\blesson\s+\d+\b',
    r'\bunit\s+\d+\b',
    r'\bsession\s+\d+\b',
    
    # === EDUCATIONAL CREATORS ===
    r'\b3Blue1Brown\b',
    r'\bNumberphile\b',
    r'\bComputerphile\b',
    r'\bStatQuest\b',
    r'\bThe Organic Chemistry Tutor\b',
    r'\bProfessor Leonard\b',
    r'\bMichel van Biezen\b',
    r'\bPatrick JMT\b',
    r'\bDr\.\s*Trefor\b',
    r'\bKurzgesagt\b',
    r'\bCrash Course\b',
    r'\bSciShow\b',
    r'\bPBS\s*(Space\s*Time|Eons|Infinite\s*Series)',
    r'\bVeritasium\b',
    r'\bMinute\s*(Physics|Earth)\b',
    r'\bSmarterEveryDay\b',
    r'\bBen Eater\b',
    r'\bAndrej Karpathy\b',
    r'\bYannic Kilcher\b',
    r'\bSabine Hossenfelder\b',
    
    # === TALK/PRESENTATION FORMATS ===
    r'\btechnical talk\b',
    r'\bresearch talk\b',
    r'\bcolloqu(ium|ia)\b',
    r'\bsympos(ium|ia)\b',
    r'\bacademic\b.*\b(talk|presentation|conference)',
    r'\bpanel\b.*\b(discussion|debate)',
    r'\bdistinguished\b.*\b(lecture|speaker)',
    r'\bpublic\b.*\blecture\b',
    r'\bTED\b(?!dy)',  # TED but not "Teddy"
    r'\bTEDx\b',
    r'\bGoogle\s+Tech\s+Talk\b',
    
    # === SPECIFIC SUBJECTS WITH DEPTH ===
    r'\balgorithm\b',
    r'\bdata\s+structure\b',
    r'\bmachine\s+learning\b',
    r'\bdeep\s+learning\b',
    r'\bneural\s+network\b',
    r'\breinforcement\s+learning\b',
    r'\bcomputer\s+vision\b',
    r'\bnatural\s+language\s+processing\b',
    r'\bquantum\s+(mechanics|computing|physics|field)\b',
    r'\bthermodynamics\b',
    r'\belectromagnet',
    r'\borganic\s+chemistry\b',
    r'\bbiochemistry\b',
    r'\bmolecular\s+biology\b',
    r'\blinear\s+algebra\b',
    r'\bcalculus\b',
    r'\bdifferential\s+equations?\b',
    r'\breal\s+analysis\b',
    r'\babstract\s+algebra\b',
    r'\btopology\b',
    r'\bnumber\s+theory\b',
    r'\bstatistical\s+(mechanics|learning|inference)\b',
    r'\bBayesian\b',
    r'\bMarkov\b',
    r'\bFourier\b',
    r'\bLaplace\b',
    r'\bHilbert\b',
    r'\bRiemann\b',
    r'\bEuler\b(?!.*\b(goal|fifa))',
    
]), re.IGNORECASE)

# Priority 7: Moderate educational signal
EDU_PRIORITY_7 = re.compile('|'.join([
    r'\bexplained\b',
    r'\bin depth\b',
    r'\bdeep dive\b',
    r'\bhow\s+.*\bwork',
    r'\bwhy\s+.*\bmatters?\b',
    r'\bhistory of\b',
    r'\bscience of\b',
    r'\bphysics of\b',
    r'\bmath(ematics)? of\b',
    r'\btheory of\b',
    r'\banalysis of\b',
    r'\bdocumentary\b',
    r'\bpodcast\b.*\b(interview|discussion|episode)',
    r'\bwebinar\b',
    r'\bpresentation\b',
    r'\bdemonstration\b',
    r'\bbreakdown\b(?!.*\b(mental|nervous|emotional))',
]), re.IGNORECASE)


def is_educational(title, duration=None):
    """Returns False if video should be rejected."""
    if duration and duration < 900:  # <15min
        return False
    if not title:
        return False
    if REJECT_PATTERNS.search(title):
        return False
    return True

def get_priority(title):
    """Returns priority 5-9 based on educational signals."""
    if not title:
        return 5
    if EDU_PRIORITY_9.search(title):
        return 9
    if EDU_PRIORITY_8.search(title):
        return 8
    if EDU_PRIORITY_7.search(title):
        return 7
    return 5


def retroactive_clean(db_path):
    """Apply filters retroactively to existing queue."""
    import sqlite3
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    
    # Get all pending videos
    rows = conn.execute("SELECT video_id, title, duration_seconds, priority FROM videos WHERE status='pending'").fetchall()
    
    rejected = 0
    upgraded = 0
    downgraded = 0
    
    for vid, title, dur, old_pri in rows:
        if not is_educational(title, dur):
            conn.execute("UPDATE videos SET priority=0, status='rejected' WHERE video_id=?", (vid,))
            rejected += 1
            continue
        
        new_pri = get_priority(title)
        if new_pri != old_pri:
            conn.execute("UPDATE videos SET priority=? WHERE video_id=?", (new_pri, vid))
            if new_pri > old_pri:
                upgraded += 1
            else:
                downgraded += 1
    
    conn.commit()
    
    stats = dict(conn.execute("SELECT status, count(*) FROM videos GROUP BY status").fetchall())
    pri_dist = conn.execute("SELECT priority, count(*) FROM videos WHERE status='pending' GROUP BY priority ORDER BY priority DESC").fetchall()
    
    conn.close()
    
    print(f"Retroactive clean complete:")
    print(f"  Rejected: {rejected}")
    print(f"  Upgraded priority: {upgraded}")
    print(f"  Downgraded priority: {downgraded}")
    print(f"  Status distribution: {stats}")
    print(f"  Priority distribution: {dict(pri_dist)}")


if __name__ == "__main__":
    import sys
    db = sys.argv[1] if len(sys.argv) > 1 else "massive_production.db"
    
    # Test on sample titles
    test_titles = [
        ("MIT 6.006 Lecture 1: Algorithms and Computation", True, 9),
        ("Stanford CS229 - Machine Learning - Lecture 1", True, 9),
        ("NeurIPS 2024 Oral: Attention Is All You Need", True, 9),
        ("NPTEL: Introduction to Machine Learning", True, 9),
        ("лекция по линейной алгебре МФТИ", True, 9),
        ("Vorlesung Algorithmen und Datenstrukturen", True, 9),
        ("3Blue1Brown: Essence of Linear Algebra", True, 8),
        ("Organic Chemistry Full Course", True, 8),
        ("Deep Learning Tutorial for Beginners", True, 8),
        ("The History of Ancient Rome - Documentary", True, 7),
        ("How Quantum Computers Work - Explained", True, 7),
        ("CEO Spoils Her After She Reborn As Princess", False, 5),
        ("【短剧】总裁的复仇", False, 5),
        ("ASMR Relaxing Math Lecture", False, 5),
        ("Fortnite Season 5 Gameplay", False, 5),
        ("Morning Routine GRWM", False, 5),
        ("Try Not To Laugh Challenge", False, 5),
        ("Official Music Video - Shape of You", False, 5),
        ("Lofi Hip Hop Beats to Study To", False, 5),
        ("Let's Play Minecraft Episode 47", False, 5),
        ("Mafia Boss Addicted to Her Innocence", False, 5),
        ("24 Hour Challenge in Walmart", False, 5),
    ]
    
    print("=== Filter Test ===")
    all_pass = True
    for title, expected_edu, expected_pri in test_titles:
        actual_edu = is_educational(title, 1800)
        actual_pri = get_priority(title)
        status = "✅" if actual_edu == expected_edu else "❌"
        pri_status = "✅" if (expected_edu and actual_pri >= expected_pri) or not expected_edu else "⚠️"
        if actual_edu != expected_edu:
            all_pass = False
        print(f"  {status} {pri_status} edu={actual_edu:5} pri={actual_pri} | {title[:65]}")
    
    print(f"\n{'All tests passed!' if all_pass else 'SOME TESTS FAILED'}")
    
    print(f"\n=== Applying retroactively to {db}")
    retroactive_clean(db)
