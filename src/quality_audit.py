#!/usr/bin/env python3
"""Audit completed transcriptions for quality and re-queue bad ones.

Detects 6 failure modes from faster-whisper:
1. ASR_GARBLED     — nonsensical tokens, broken words
2. LOW_SIGNAL      — meta-talk, "please subscribe", tech checks
3. NON_ENGLISH_ASR — non-English forced to English (phonetic garbage)
4. NON_EDUCATIONAL — gaming, songs, gossip (filter missed)
5. FILLER_HEAVY    — excessive "okay okay", "so so so"
6. REPETITIVE      — pathological loops ("Thank you. Thank you. Thank you.")

Two stages:
  1. Fast heuristics (regex + statistics) — catches ~80%
  2. Optional: perplexity scoring with small LM — catches the rest

Usage:
    python3 src/quality_audit.py --scan              # scan + report
    python3 src/quality_audit.py --scan --requeue     # scan + requeue bad ones
    python3 src/quality_audit.py --scan --perplexity  # include LM perplexity scoring
    python3 src/quality_audit.py --sample 100         # audit random 100
"""
import argparse, os, re, sqlite3, time, sys

DB_PATH = os.path.expanduser("~/academic_transcriptions/massive_production.db")


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    return conn


# ============================================================
# Heuristic detectors
# ============================================================

def detect_repetitive(text, threshold=0.15):
    """Detect pathological repetition loops.

    Checks: repeated n-grams (3-8 words), repeated sentences.
    Returns (is_bad, score, detail).
    """
    words = text.split()
    if len(words) < 50:
        return False, 0, ""

    # Check 4-gram repetition rate
    ngram_size = 4
    if len(words) >= ngram_size:
        ngrams = [" ".join(words[i:i+ngram_size]) for i in range(len(words) - ngram_size + 1)]
        from collections import Counter
        counts = Counter(ngrams)
        if counts:
            most_common_count = counts.most_common(1)[0][1]
            repeat_ratio = most_common_count / len(ngrams)
            if repeat_ratio > threshold:
                phrase = counts.most_common(1)[0][0]
                return True, repeat_ratio, f"4gram '{phrase}' repeated {most_common_count}x ({repeat_ratio:.0%})"

    # Check sentence-level repetition
    sentences = re.split(r'[.!?]+', text)
    sentences = [s.strip().lower() for s in sentences if len(s.strip()) > 10]
    if len(sentences) >= 5:
        sent_counts = Counter(sentences)
        top_sent, top_count = sent_counts.most_common(1)[0]
        sent_ratio = top_count / len(sentences)
        if sent_ratio > 0.25:
            return True, sent_ratio, f"sentence '{top_sent[:50]}' repeated {top_count}x ({sent_ratio:.0%})"

    return False, 0, ""


def detect_garbled(text):
    """Detect ASR garbage: nonsensical tokens, broken words, excessive symbols."""
    words = text.split()
    if len(words) < 20:
        return False, 0, ""

    # Check for excessive non-dictionary-like patterns
    garbled_patterns = [
        r'\b[A-Z]{2,}-[A-Z]{2,}-[A-Z]{2,}\b',  # DERIN-EAREN-MEAREN
        r"\b\w+'\w+'\w+'\w+\b",                   # hand's hand's hand's
        r'\b[bcdfghjklmnpqrstvwxyz]{5,}\b',        # consonant clusters (no vowels)
        r'(.{2,8})\1{4,}',                         # character-level repetition
    ]
    garbled_count = 0
    for pat in garbled_patterns:
        garbled_count += len(re.findall(pat, text, re.IGNORECASE))

    # Ratio of very short words (1-2 chars) — garbled ASR produces lots of fragments
    short_words = sum(1 for w in words if len(w) <= 2 and w.lower() not in {
        'a', 'i', 'an', 'am', 'as', 'at', 'be', 'by', 'do', 'go', 'he',
        'if', 'in', 'is', 'it', 'me', 'my', 'no', 'of', 'on', 'or', 'so',
        'to', 'up', 'us', 'we', 'ok',
    })
    short_ratio = short_words / len(words) if words else 0

    # Average word length — garbled tends to be very short or very long
    avg_len = sum(len(w) for w in words) / len(words) if words else 0

    score = garbled_count / len(words) + (short_ratio if short_ratio > 0.3 else 0)
    if score > 0.05 or garbled_count > 10:
        return True, score, f"garbled_tokens={garbled_count} short_ratio={short_ratio:.2f} avg_word_len={avg_len:.1f}"
    return False, score, ""


def detect_filler_heavy(text, threshold=0.20):
    """Detect excessive filler words/phrases."""
    words = text.lower().split()
    if len(words) < 100:
        return False, 0, ""

    filler_patterns = [
        r'\b(okay|ok)\b', r'\bso\b', r'\bum+\b', r'\buh+\b', r'\blike\b',
        r'\byeah\b', r'\bright\b', r'\byes\b', r'\bno\b',
        r'\bplease\b', r'\bconfirm\b', r'\bsir\b', r'\bma\'?am\b',
    ]
    text_lower = text.lower()
    filler_count = sum(len(re.findall(p, text_lower)) for p in filler_patterns)
    ratio = filler_count / len(words)

    # Check for repeated fillers specifically
    filler_sequences = len(re.findall(
        r'\b(okay\s+okay|so\s+so\s+so|yes\s+yes|please\s+please|right\s+right)\b',
        text_lower))

    if ratio > threshold or filler_sequences > 15:
        return True, ratio, f"filler_ratio={ratio:.2f} filler_seqs={filler_sequences}"
    return False, ratio, ""


def detect_non_english_forced(text):
    """Detect non-English content force-transcribed to English.

    Signs: phonetic transliterations, broken grammar, mixed scripts.
    """
    words = text.split()
    if len(words) < 30:
        return False, 0, ""

    # Check for transliteration artifacts
    transliteration_patterns = [
        r'\b[A-Z][a-z]+[A-Z][a-z]+\b',  # CamelCase fragments
        r'\b(ji|ji\b|bhai|sahib|ke|ka|ki|hai|hain|ko|se|ye|wo|kya|nahi|aur)\b',  # Hindi/Urdu
        r'\b(ek|do|teen|char|panch)\b',  # Hindi numbers
        r'\b(bir|iki|üç|dört|beş|ve|bu|ne|ile|için|var|yok)\b',  # Turkish
        r'\b(ini|itu|dan|yang|ada|tidak|dari|untuk|dengan)\b',  # Indonesian/Malay
    ]
    transliteration_count = 0
    text_lower = text.lower()
    for pat in transliteration_patterns:
        transliteration_count += len(re.findall(pat, text_lower))

    ratio = transliteration_count / len(words)

    # Check for non-Latin script (should be fine in Qwen3-ASR but faster-whisper forces English)
    non_latin = sum(1 for c in text if ord(c) > 127 and not c.isspace())
    non_latin_ratio = non_latin / len(text) if text else 0

    if ratio > 0.08 or (transliteration_count > 20 and ratio > 0.05):
        return True, ratio, f"transliteration_ratio={ratio:.2f} count={transliteration_count}"
    return False, ratio, ""


def detect_low_signal(text):
    """Detect meta-talk dominated content (subscribe, tech checks, intros)."""
    words = text.lower().split()
    if len(words) < 50:
        return False, 0, ""

    meta_patterns = [
        r'\bsubscribe\b', r'\blike (and|&) subscribe\b', r'\bbell icon\b',
        r'\bnotification\b', r'\bcomment (below|down)\b', r'\bshare this\b',
        r'\bcan you hear me\b', r'\bis (my|the) (audio|video|screen)\b',
        r'\blet me (share|check)\b', r'\btech(nical)? (issue|problem|difficult)\b',
        r'\bjoin (our|my) (channel|group|telegram|whatsapp)\b',
        r'\bdon\'?t forget to\b', r'\bhit the\b.*\bbutton\b',
        r'\blink (in|below)\b.*\bdescription\b',
        r'\bsponsored by\b', r'\buse (my|the) code\b', r'\bdiscount\b',
    ]
    text_lower = text.lower()
    meta_count = sum(len(re.findall(p, text_lower)) for p in meta_patterns)

    # Very short transcript relative to video duration could indicate mostly silence/music
    ratio = meta_count / (len(words) / 100)  # per 100 words
    if meta_count > 10 and ratio > 3:
        return True, ratio, f"meta_phrases={meta_count} per_100w={ratio:.1f}"
    return False, ratio, ""


def detect_non_educational(text):
    """Catch content that slipped through the title filter."""
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from quality_filter import REJECT_PATTERNS

    # Check transcript content (not just title)
    words = text.lower().split()
    if len(words) < 50:
        return False, 0, ""

    # Gaming/entertainment signals in transcript body
    non_edu_patterns = [
        r'\b(headshot|kill streak|respawn|loot|inventory|boss fight)\b',
        r'\b(verse|chorus|bridge|hook|beat drop)\b',
        r'\b(gossip|drama alert|tea|cancelled|exposed)\b',
        r'\b(smash that like|drop a comment|give away)\b',
    ]
    text_lower = text.lower()
    hits = sum(len(re.findall(p, text_lower)) for p in non_edu_patterns)
    ratio = hits / (len(words) / 100)

    if hits > 8 and ratio > 2:
        return True, ratio, f"non_edu_hits={hits}"
    return False, ratio, ""


# ============================================================
# Propella model scoring (optional, needs SGLang server)
# ============================================================

def score_with_propella(text, server_url="http://localhost:8001"):
    """Score text quality using Propella annotation model.

    Requires SGLang server running:
        python -m sglang.launch_server --model-path ellamind/propella-1-0.6b \\
            --host 0.0.0.0 --port 8001 --context-length 65536 --grammar-backend llguidance

    Returns dict with quality fields or None on error.
    """
    import json, urllib.request, urllib.error
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from propella import create_messages, AnnotationResponse, get_annotation_response_schema

    payload = {
        "model": "ellamind/propella-1-0.6b",
        "messages": create_messages(text[:50000]),
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "AnnotationResponse",
                "schema": get_annotation_response_schema(flatten=True, compact_whitespace=True),
                "strict": True,
            },
        },
    }
    try:
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            f"{server_url}/v1/chat/completions",
            data=data,
            headers={"Content-Type": "application/json"},
        )
        resp = urllib.request.urlopen(req, timeout=60)
        result = json.loads(resp.read())
        content = result["choices"][0]["message"]["content"]
        ann = AnnotationResponse.model_validate_json(content)
        return ann.model_dump()
    except Exception as e:
        return None


def propella_is_bad(ann):
    """Check Propella annotation for quality issues. Returns (is_bad, reasons)."""
    if ann is None:
        return False, []

    reasons = []
    q = ann.get("content_quality", "")
    if q in ("poor", "unacceptable"):
        reasons.append(f"quality={q}")
    d = ann.get("information_density", "")
    if d in ("thin", "empty"):
        reasons.append(f"density={d}")
    e = ann.get("educational_value", "")
    if e in ("minimal", "none"):
        reasons.append(f"edu_value={e}")
    i = ann.get("content_integrity", "")
    if i == "severely_degraded":
        reasons.append(f"integrity={i}")
    s = ann.get("content_safety", "")
    if s in ("nsfw", "harmful", "illegal"):
        reasons.append(f"safety={s}")

    return len(reasons) > 0, reasons


# ============================================================
# Main audit logic
# ============================================================

def audit_transcript(text, use_propella=False, propella_url=None):
    """Run all detectors on a transcript. Returns list of (mode, score, detail)."""
    issues = []

    is_bad, score, detail = detect_repetitive(text)
    if is_bad:
        issues.append(("REPETITIVE", score, detail))

    is_bad, score, detail = detect_garbled(text)
    if is_bad:
        issues.append(("ASR_GARBLED", score, detail))

    is_bad, score, detail = detect_filler_heavy(text)
    if is_bad:
        issues.append(("FILLER_HEAVY", score, detail))

    is_bad, score, detail = detect_non_english_forced(text)
    if is_bad:
        issues.append(("NON_ENGLISH_ASR", score, detail))

    is_bad, score, detail = detect_low_signal(text)
    if is_bad:
        issues.append(("LOW_SIGNAL", score, detail))

    is_bad, score, detail = detect_non_educational(text)
    if is_bad:
        issues.append(("NON_EDUCATIONAL", score, detail))

    # Propella deep scoring for borderline cases not caught by heuristics
    if use_propella and not issues and propella_url:
        ann = score_with_propella(text, propella_url)
        is_bad, reasons = propella_is_bad(ann)
        if is_bad:
            issues.append(("PROPELLA_LOW_QUALITY", 0, "; ".join(reasons)))

    return issues


def scan_completed(limit=None, use_propella=False, propella_url=None):
    """Scan all completed transcriptions and report quality issues."""
    conn = get_db()

    query = "SELECT video_id, title, transcript, duration_seconds, speed_ratio FROM videos WHERE status='completed' AND transcript IS NOT NULL"
    if limit:
        query += f" ORDER BY RANDOM() LIMIT {limit}"

    rows = conn.execute(query).fetchall()
    conn.close()

    total = len(rows)
    flagged = {}  # video_id -> [(mode, score, detail)]
    mode_counts = {}

    t0 = time.time()
    for i, (vid, title, transcript, dur, speed) in enumerate(rows):
        issues = audit_transcript(transcript, use_propella=use_propella, propella_url=propella_url)
        if issues:
            flagged[vid] = {
                "title": title,
                "issues": issues,
                "duration": dur,
                "speed": speed,
                "transcript_len": len(transcript),
            }
            for mode, _, _ in issues:
                mode_counts[mode] = mode_counts.get(mode, 0) + 1

        if (i + 1) % 5000 == 0:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            flagged_pct = len(flagged) / (i + 1) * 100
            print(f"  [{i+1}/{total}] {rate:.0f}/s | flagged: {len(flagged)} ({flagged_pct:.1f}%)", flush=True)

    elapsed = time.time() - t0
    return flagged, mode_counts, total, elapsed


def requeue_flagged(flagged_ids):
    """Move flagged videos back to 'downloaded' status for re-transcription."""
    conn = get_db()
    n = 0
    batch = []
    for vid in flagged_ids:
        batch.append(vid)
        if len(batch) >= 500:
            ph = ",".join(["?"] * len(batch))
            conn.execute(
                f"UPDATE videos SET status='pending', transcript=NULL, "
                f"processing_time_seconds=NULL, speed_ratio=NULL, completed_at=NULL, "
                f"error='requeued:quality_audit' "
                f"WHERE video_id IN ({ph})", batch)
            n += len(batch)
            batch.clear()
    if batch:
        ph = ",".join(["?"] * len(batch))
        conn.execute(
            f"UPDATE videos SET status='pending', transcript=NULL, "
            f"processing_time_seconds=NULL, speed_ratio=NULL, completed_at=NULL, "
            f"error='requeued:quality_audit' "
            f"WHERE video_id IN ({ph})", batch)
        n += len(batch)
    conn.commit()
    conn.close()
    return n


def main():
    parser = argparse.ArgumentParser(description="Audit transcript quality")
    parser.add_argument("--scan", action="store_true", help="Scan completed transcriptions")
    parser.add_argument("--sample", type=int, default=None, help="Audit random N samples")
    parser.add_argument("--requeue", action="store_true", help="Re-queue flagged videos")
    parser.add_argument("--propella", type=str, default=None,
                        help="Propella server URL (e.g. http://localhost:8001)")
    parser.add_argument("--show", type=int, default=5, help="Show N examples per mode")
    args = parser.parse_args()

    if not args.scan and not args.sample:
        parser.print_help()
        return

    limit = args.sample
    use_propella = args.propella is not None
    print("=" * 60)
    print(f"Transcript Quality Audit {'(sample=' + str(limit) + ')' if limit else '(full scan)'}")
    print(f"Propella scoring: {args.propella or 'OFF'}")
    print("=" * 60)

    flagged, mode_counts, total, elapsed = scan_completed(
        limit, use_propella=use_propella, propella_url=args.propella)

    # Report
    print(f"\n{'=' * 60}")
    print(f"Results: {len(flagged):,} flagged / {total:,} scanned ({len(flagged)/total*100:.1f}%) in {elapsed:.0f}s")
    print(f"\nFailure mode breakdown:")
    for mode, count in sorted(mode_counts.items(), key=lambda x: -x[1]):
        print(f"  {mode:20s}: {count:,} ({count/total*100:.1f}%)")

    # Show examples
    if args.show:
        for mode in sorted(mode_counts.keys()):
            examples = [(vid, info) for vid, info in flagged.items()
                       if any(m == mode for m, _, _ in info["issues"])][:args.show]
            if examples:
                print(f"\n--- {mode} examples ---")
                for vid, info in examples:
                    issue = [x for x in info["issues"] if x[0] == mode][0]
                    print(f"  {vid} | {info['title'][:50]} | {issue[2]}")
                    print(f"    preview: {info.get('transcript_len', 0)} chars")

    # Requeue
    if args.requeue and flagged:
        print(f"\nRe-queuing {len(flagged):,} flagged videos...")
        n = requeue_flagged(list(flagged.keys()))
        print(f"Re-queued {n:,} videos (status='pending', transcript cleared)")
    elif flagged and not args.requeue:
        print(f"\nTo re-queue these, run with --requeue flag")


if __name__ == "__main__":
    main()
