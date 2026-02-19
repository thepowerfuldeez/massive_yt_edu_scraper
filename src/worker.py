#!/usr/bin/env python3
"""GPU worker: faster-whisper (CTranslate2) + 1.2x audio speedup + cookie rotation.

Architecture:
- N prefetch threads download audio via yt-dlp with cookie rotation
- Main thread transcribes with faster-whisper on a single GPU
- SQLite is the queue: atomic UPDATE...RETURNING claims

Cookie rotation:
- Place Netscape-format cookie files in WORK_DIR (e.g. cookies_1.txt, cookies_2.txt)
- Worker selects cookies based on GPU_ID % num_cookies
- See README.md for cookie setup instructions
"""
import os, sys, time, random, sqlite3, subprocess, glob, threading, queue, traceback
import numpy as np

GPU_ID = int(sys.argv[1]) if len(sys.argv) > 1 else 0
DB_PATH = os.path.expanduser("~/academic_transcriptions/massive_production.db")
WORK_DIR = os.path.expanduser("~/academic_transcriptions")
YTDLP = os.path.join(WORK_DIR, "yt-dlp")
MODEL_ID = "distil-large-v3.5"
PREFETCH_DEPTH = 5
PREFETCH_THREADS = 2
CLAIM_BATCH = 15
AUDIO_SPEED = 1.2
MAX_DOWNLOAD_RETRIES = 3

# Cookie rotation: pool of real account cookies in cookie_pool/ directory.
# Each download thread picks a cookie round-robin from the pool.
# On consecutive failures, rotate to next cookie and back off.
import shutil

COOKIE_POOL_DIR = os.path.join(WORK_DIR, "cookie_pool")

def load_cookie_pool():
    """Load all cookie files from cookie_pool/ directory."""
    if not os.path.isdir(COOKIE_POOL_DIR):
        print(f"[GPU {GPU_ID}] WARNING: No cookie_pool/ directory found!", flush=True)
        return []
    files = sorted(glob.glob(os.path.join(COOKIE_POOL_DIR, "*.txt")))
    print(f"[GPU {GPU_ID}] Cookie pool: {len(files)} cookies loaded", flush=True)
    for f in files:
        print(f"  - {os.path.basename(f)}", flush=True)
    return files

_cookie_pool = load_cookie_pool()
_cookie_lock = threading.Lock()
_cookie_counter = GPU_ID  # offset so each GPU starts on a different cookie

def get_thread_cookie_file(thread_idx):
    """Return a per-thread cookie file copied from the pool (round-robin)."""
    global _cookie_counter
    if not _cookie_pool:
        return None
    with _cookie_lock:
        idx = _cookie_counter % len(_cookie_pool)
        _cookie_counter += 1
    src = _cookie_pool[idx]
    dst = os.path.join(WORK_DIR, f"cookies_gpu{GPU_ID}_t{thread_idx}.txt")
    shutil.copy2(src, dst)
    print(f"[GPU {GPU_ID}] Thread {thread_idx} using cookie: {os.path.basename(src)}", flush=True)
    return dst

def rotate_cookie(thread_idx):
    """Force-rotate to the next cookie in the pool."""
    return get_thread_cookie_file(thread_idx)

_FAIL_REFRESH_THRESHOLD = 5  # rotate cookies after this many consecutive failures


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# === Claim logic ===
claim_lock = threading.Lock()
claimed_queue = queue.Queue()


def refill_claims():
    conn = get_db()
    try:
        # Weighted random sampling across priorities and durations.
        # Bias toward higher priority and shorter videos (faster downloads),
        # but still sample variety for dataset diversity.
        # Strategy: pick from 4 buckets proportionally:
        #   - 40% high priority short (<60min, P8+P9)
        #   - 25% high priority long (60min+, P8+P9)
        #   - 25% default priority short (<60min, P5+P7)
        #   - 10% default priority long (60min+, P5+P7)
        # Within each bucket: random sampling via ORDER BY RANDOM()
        # Priority: GREEN (CC-licensed) > high priority > default
        # RED content is excluded entirely
        buckets = [
            ("license_risk = 'green'", 0.40),                              # CC-licensed first
            ("license_risk != 'red' AND priority >= 8 AND duration_seconds < 3600", 0.25),
            ("license_risk != 'red' AND priority >= 8 AND duration_seconds >= 3600", 0.05),
            ("license_risk != 'red' AND priority < 8 AND duration_seconds < 3600", 0.20),
            ("license_risk != 'red' AND priority < 8 AND duration_seconds >= 3600", 0.10),
        ]
        all_ids = []
        for cond, frac in buckets:
            n = max(int(CLAIM_BATCH * frac), 1)
            rows = conn.execute(
                f"SELECT video_id FROM videos WHERE status='pending' "
                f"AND (duration_seconds >= 900 OR duration_seconds IS NULL OR duration_seconds = 0) "
                f"AND ({cond}) ORDER BY RANDOM() LIMIT ?", (n,)
            ).fetchall()
            all_ids.extend(r[0] for r in rows)

        if not all_ids:
            # Fallback: grab anything pending (excluding RED)
            rows = conn.execute(
                "SELECT video_id FROM videos WHERE status='pending' "
                "AND (duration_seconds >= 900 OR duration_seconds IS NULL OR duration_seconds = 0) "
                "AND (license_risk IS NULL OR license_risk != 'red') "
                "ORDER BY RANDOM() LIMIT ?", (CLAIM_BATCH,)
            ).fetchall()
            all_ids = [r[0] for r in rows]

        if not all_ids:
            return []

        placeholders = ",".join("?" * len(all_ids))
        cur = conn.execute(
            f"UPDATE videos SET status='processing', processing_started_at=datetime('now') "
            f"WHERE video_id IN ({placeholders}) "
            f"RETURNING video_id, title", all_ids
        )
        rows = cur.fetchall()
        conn.commit()
        return rows
    except Exception as e:
        print(f"[GPU {GPU_ID}] Claim error: {e}", flush=True)
        return []
    finally:
        conn.close()


def get_claimed():
    with claim_lock:
        if claimed_queue.empty():
            for r in refill_claims():
                claimed_queue.put(r)
    try:
        return claimed_queue.get_nowait()
    except queue.Empty:
        return None


def mark_done(video_id, transcript, duration_s, transcribe_s):
    conn = get_db()
    speed = duration_s / transcribe_s if transcribe_s > 0 else 0
    conn.execute(
        "UPDATE videos SET status='completed', transcript=?, duration_seconds=?, "
        "processing_time_seconds=?, speed_ratio=?, completed_at=datetime('now') WHERE video_id=?",
        (transcript, duration_s, transcribe_s, speed, video_id))
    conn.commit()
    conn.close()


def mark_error(video_id, error):
    conn = get_db()
    conn.execute(
        "UPDATE videos SET status='error', error=? WHERE video_id=?",
        (str(error)[:500], video_id))
    conn.commit()
    conn.close()


# === Download ===
def download_audio(video_id, tmp_dir, cookie_file=None):
    """Download audio as mp3 at 1.2x speed, return (file_path, duration) or None.
    
    Pipeline: yt-dlp downloads webm → ffmpeg converts to mp3 with atempo=1.2x.
    The 1.2x speedup saves 17% GPU time. mp3 is smaller for temp storage.
    Uses process group kill to prevent zombie ffmpeg on timeout.
    """
    out_template = os.path.join(tmp_dir, f"{video_id}.%(ext)s")
    out_path = os.path.join(tmp_dir, f"{video_id}.mp3")

    for attempt in range(MAX_DOWNLOAD_RETRIES):
        try:
            cmd = [YTDLP, "--js-runtimes", "node"]
            if cookie_file:
                cmd += ["--cookies", cookie_file]
            cmd += [
                "-x", "--audio-format", "mp3", "--audio-quality", "5",
                "--postprocessor-args", f"ffmpeg:-filter:a atempo={AUDIO_SPEED}",
                "-o", out_template, "--no-playlist",
                "--socket-timeout", "30", "--retries", "3",
                "--no-warnings", "--no-check-certificates",
                f"https://www.youtube.com/watch?v={video_id}",
            ]
            # Popen with process group so timeout kills yt-dlp + ffmpeg children
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                     text=True, cwd=WORK_DIR, start_new_session=True)
            try:
                proc.communicate(timeout=900)
            except subprocess.TimeoutExpired:
                import signal
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except (OSError, ProcessLookupError):
                    proc.kill()
                proc.wait()
                raise

            if not os.path.exists(out_path):
                matches = [f for f in glob.glob(os.path.join(tmp_dir, f"{video_id}.*"))
                           if not f.endswith(".part")]
                out_path = matches[0] if matches else None

            if not out_path or not os.path.exists(out_path):
                if attempt < MAX_DOWNLOAD_RETRIES - 1:
                    wait = (2 ** attempt) * 5 + random.random() * 5
                    print(f"[GPU {GPU_ID}] Download failed {video_id}, "
                          f"retry {attempt+1}/{MAX_DOWNLOAD_RETRIES} in {wait:.0f}s", flush=True)
                    time.sleep(wait)
                    continue
                return None

            # Get duration via ffprobe (fast)
            try:
                probe = subprocess.run(
                    ["ffprobe", "-v", "quiet", "-show_entries", "format=duration",
                     "-of", "default=noprint_wrappers=1:nokey=1", out_path],
                    capture_output=True, text=True, timeout=10)
                duration = float(probe.stdout.strip()) * AUDIO_SPEED  # original duration
            except Exception:
                duration = 0

            # Clean up webm/intermediate files, keep only mp3
            for f in glob.glob(os.path.join(tmp_dir, f"{video_id}.*")):
                if f != out_path:
                    try:
                        os.unlink(f)
                    except OSError:
                        pass

            return out_path, duration

        except Exception as e:
            for f in glob.glob(os.path.join(tmp_dir, f"{video_id}.*")):
                try:
                    os.unlink(f)
                except OSError:
                    pass
            if attempt < MAX_DOWNLOAD_RETRIES - 1:
                wait = (2 ** attempt) * 5 + random.random() * 5
                print(f"[GPU {GPU_ID}] Download error {video_id}: {e}, "
                      f"retry {attempt+1}/{MAX_DOWNLOAD_RETRIES} in {wait:.0f}s", flush=True)
                time.sleep(wait)
            else:
                return None
    return None


# === Prefetch ===
prefetch_q = queue.Queue(maxsize=PREFETCH_DEPTH + 1)
tmp_dir = os.path.join(WORK_DIR, f"tmp_gpu{GPU_ID}")
os.makedirs(tmp_dir, exist_ok=True)


def prefetcher(thread_idx):
    cookie_file = get_thread_cookie_file(thread_idx)
    consec_fails = 0
    print(f"[GPU {GPU_ID}] Prefetch thread {thread_idx} started", flush=True)
    while True:
        try:
            if prefetch_q.qsize() >= PREFETCH_DEPTH:
                time.sleep(0.3)
                continue

            row = get_claimed()
            if not row:
                time.sleep(2)
                continue

            vid, title = row
            result = download_audio(vid, tmp_dir, cookie_file=cookie_file)
            if result is None:
                mark_error(vid, "download_failed")
                consec_fails += 1
                if consec_fails >= _FAIL_REFRESH_THRESHOLD:
                    print(f"[GPU {GPU_ID}] {consec_fails} consecutive download failures, rotating cookies...", flush=True)
                    cookie_file = rotate_cookie(thread_idx)
                    consec_fails = 0
                    time.sleep(10)
                continue

            consec_fails = 0  # reset on success
            audio_path, dur = result
            if dur < 5:
                mark_error(vid, f"too_short_{dur:.0f}s")
                try:
                    os.unlink(audio_path)
                except OSError:
                    pass
                continue

            prefetch_q.put((vid, title, audio_path, dur))
            time.sleep(random.uniform(1, 3))  # rate-limit protection
        except Exception as e:
            print(f"[GPU {GPU_ID}] Prefetch error: {e}", flush=True)


for _i in range(PREFETCH_THREADS):
    threading.Thread(target=prefetcher, args=(_i,), daemon=True).start()

# === Load model ===
print(f"[GPU {GPU_ID}] Loading faster-whisper {MODEL_ID} "
      f"(speed={AUDIO_SPEED}x, {PREFETCH_THREADS} prefetch)...", flush=True)

from faster_whisper import WhisperModel

t0 = time.time()
model = WhisperModel(MODEL_ID, device="cuda", compute_type="float16")
print(f"[GPU {GPU_ID}] Model loaded in {time.time()-t0:.1f}s", flush=True)

with open(f"/tmp/gpu_{GPU_ID}_ready", "w") as f:
    f.write(str(os.getpid()))

# === Main transcription loop ===
completed = 0
total_audio_s = 0
total_transcribe_s = 0
start_time = time.time()

while True:
    try:
        vid, title, audio_path, dur = prefetch_q.get(timeout=60)
    except queue.Empty:
        print(f"[GPU {GPU_ID}] Prefetch queue empty 60s, waiting...", flush=True)
        continue

    try:
        t0 = time.time()
        # faster-whisper reads opus/webm/m4a directly via internal ffmpeg
        segments, info = model.transcribe(
            audio_path, beam_size=1, vad_filter=False,
            word_timestamps=False, condition_on_previous_text=False)
        transcript = " ".join(s.text for s in segments).strip()
        transcribe_s = time.time() - t0

        # Clean up audio file after transcription
        try:
            os.unlink(audio_path)
        except OSError:
            pass

        if transcript:
            mark_done(vid, transcript, dur, transcribe_s)
            completed += 1
            total_audio_s += dur
            total_transcribe_s += transcribe_s
            speed = dur / transcribe_s if transcribe_s > 0 else 0

            if completed % 5 == 0 or completed <= 3:
                avg_speed = total_audio_s / total_transcribe_s if total_transcribe_s > 0 else 0
                hours_done = total_audio_s / 3600
                qsize = prefetch_q.qsize()
                rate_per_h = completed / ((time.time() - start_time) / 3600) if time.time() > start_time else 0
                print(f"[GPU {GPU_ID}] #{completed}: {dur/60:.1f}min->{transcribe_s:.1f}s={speed:.0f}x | "
                      f"avg={avg_speed:.0f}x | {hours_done:.1f}h | q={qsize} | {rate_per_h:.0f}/hr", flush=True)
        else:
            mark_error(vid, "empty_transcript")
    except Exception as e:
        print(f"[GPU {GPU_ID}] ERROR: {traceback.format_exc()}", flush=True)
        mark_error(vid, str(e))
        try:
            os.unlink(audio_path)
        except (OSError, NameError):
            pass
