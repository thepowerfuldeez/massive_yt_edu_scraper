#!/usr/bin/env python3

Architecture:
- Main thread transcribes with faster-whisper on a single GPU
- SQLite is the queue: atomic UPDATE...RETURNING claims

- Worker selects cookies based on GPU_ID % num_cookies
"""
import os, sys, time, random, sqlite3, subprocess, glob, threading, queue, traceback
import numpy as np

GPU_ID = int(sys.argv[1]) if len(sys.argv) > 1 else 0
INSTANCE = int(sys.argv[2]) if len(sys.argv) > 2 else 0  # 0 or 1 per GPU
WORKER_TAG = f"GPU {GPU_ID}.{INSTANCE}" if INSTANCE else f"GPU {GPU_ID}"
DB_PATH = os.path.expanduser("~/academic_transcriptions/massive_production.db")
WORK_DIR = os.path.expanduser("~/academic_transcriptions")
YTDLP = os.path.join(WORK_DIR, "yt-dlp")
MODEL_ID = "distil-large-v3.5"
PREFETCH_DEPTH = 5
PREFETCH_THREADS = 2
CLAIM_BATCH = 10
AUDIO_SPEED = 1.2
MAX_DOWNLOAD_RETRIES = 3

# Proxy rotation: proxy_pool.txt, one per line. Each GPU gets a primary proxy.
PROXY_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "proxy_pool.txt")

def load_proxy_pool():
    fast = []
    slow = []
    section = "fast"
    try:
        with open(PROXY_FILE) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if line.startswith("# Slow") or line.startswith("# slow") or line.startswith("# fallback"):
                    section = "slow"
                    continue
                if line.startswith("#"):
                    continue
                if section == "fast":
                    fast.append(line)
                else:
                    slow.append(line)
        total = len(fast) + len(slow)
        if total:
            print(f"[{WORKER_TAG}] Proxy pool: {len(fast)} fast + {len(slow)} fallback", flush=True)
        return fast, slow
    except FileNotFoundError:
        return [], []

_fast_proxies, _slow_proxies = load_proxy_pool()
_proxy_failures = {}  # proxy -> consecutive failure count
_proxy_lock = threading.Lock()

WORKER_IDX = GPU_ID * 2 + INSTANCE  # unique index 0..15 for 1:1 proxy mapping

def get_proxy():
    """Get best available proxy. Each worker instance gets its own dedicated proxy."""
    with _proxy_lock:
        # Try fast proxies first, starting from this worker's assigned one
        if _fast_proxies:
            for offset in range(len(_fast_proxies)):
                idx = (WORKER_IDX + offset) % len(_fast_proxies)
                proxy = _fast_proxies[idx]
                if _proxy_failures.get(proxy, 0) < 3:
                    return proxy
        # Fall back to slow proxies
        for proxy in _slow_proxies:
            if _proxy_failures.get(proxy, 0) < 5:
                return proxy
        # Everything failed — reset and try again
        _proxy_failures.clear()
        if _fast_proxies:
            return _fast_proxies[WORKER_IDX % len(_fast_proxies)]
        if _slow_proxies:
            return _slow_proxies[0]
        return None

def mark_proxy_ok(proxy):
    with _proxy_lock:
        _proxy_failures.pop(proxy, None)

def mark_proxy_fail(proxy):
    with _proxy_lock:
        _proxy_failures[proxy] = _proxy_failures.get(proxy, 0) + 1
        n = _proxy_failures[proxy]
        if n >= 3:
            print(f"[{WORKER_TAG}] Proxy {proxy.split('@')[-1]} failed {n}x, will try next", flush=True)

# Each download thread picks a cookie round-robin from the pool.
# On consecutive failures, rotate to next cookie and back off.
import shutil


        return []
    for f in files:
        print(f"  - {os.path.basename(f)}", flush=True)
    return files

_cookie_lock = threading.Lock()
_cookie_counter = GPU_ID * 2 + INSTANCE  # offset so each instance starts on a different cookie

def get_thread_cookie_file(thread_idx):
    """Return a per-thread cookie file copied from the pool (round-robin)."""
    global _cookie_counter
        return None
    with _cookie_lock:
        _cookie_counter += 1
    dst = os.path.join(WORK_DIR, f"cookies_gpu{GPU_ID}_t{thread_idx}.txt")
    shutil.copy2(src, dst)
    print(f"[{WORKER_TAG}] Thread {thread_idx} using cookie: {os.path.basename(src)}", flush=True)
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
                f"AND (duration_seconds >= 300 OR duration_seconds IS NULL OR duration_seconds = 0) "
                f"AND ({cond}) ORDER BY RANDOM() LIMIT ?", (n,)
            ).fetchall()
            all_ids.extend(r[0] for r in rows)

        if not all_ids:
            # Fallback: grab anything pending (excluding RED)
            rows = conn.execute(
                "SELECT video_id FROM videos WHERE status='pending' "
                "AND (duration_seconds >= 300 OR duration_seconds IS NULL OR duration_seconds = 0) "
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
        print(f"[{WORKER_TAG}] Claim error: {e}", flush=True)
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
    """Download audio as native opus/webm (no conversion), return (file_path, duration) or None.

    Pipeline: yt-dlp downloads low-bitrate audio directly (no mp3 conversion).
    faster-whisper reads opus/webm natively via internal ffmpeg.
    Uses process group kill to prevent zombie ffmpeg on timeout.
    """
    out_template = os.path.join(tmp_dir, f"{video_id}.%(ext)s")

    for attempt in range(MAX_DOWNLOAD_RETRIES):
        try:
            cmd = [YTDLP, "--js-runtimes", "node", "--remote-components", "ejs:github"]
            proxy = get_proxy()
            if proxy:
                cmd += ["--proxy", proxy]
            if cookie_file:
                cmd += ["--cookies", cookie_file]
            # Download lowest-bitrate audio directly — no conversion needed
            # faster-whisper reads opus/webm/m4a natively
            cmd += [
                "-f", "ba[abr<=96]/wa/ba",
                "-o", out_template, "--no-playlist",
                "--socket-timeout", "30", "--retries", "3",
                "--no-check-certificates",
                f"https://www.youtube.com/watch?v={video_id}",
            ]
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                     text=True, cwd=WORK_DIR, start_new_session=True)
            try:
                stdout, stderr = proc.communicate(timeout=900)
            except subprocess.TimeoutExpired:
                import signal
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except (OSError, ProcessLookupError):
                    proc.kill()
                proc.wait()
                raise

            # Find the output file (could be .webm, .m4a, .opus, etc.)
            matches = [f for f in glob.glob(os.path.join(tmp_dir, f"{video_id}.*"))
                       if not f.endswith(".part")]
            out_path = matches[0] if matches else None

            if not out_path or not os.path.exists(str(out_path)):
                err_msg = ""
                if stderr:
                    for line in stderr.strip().split("\n"):
                        if "ERROR" in line:
                            err_msg = line.strip()[-120:]
                            break
                if proxy:
                    mark_proxy_fail(proxy)
                if attempt < MAX_DOWNLOAD_RETRIES - 1:
                    wait = (2 ** attempt) * 5 + random.random() * 5
                    print(f"[{WORKER_TAG}] Download failed {video_id}, "
                          f"retry {attempt+1}/{MAX_DOWNLOAD_RETRIES} in {wait:.0f}s"
                          f"{' | ' + err_msg if err_msg else ''}", flush=True)
                    time.sleep(wait)
                    continue
                return None

            if proxy:
                mark_proxy_ok(proxy)

            # Get duration via ffprobe (before atempo)
            try:
                probe = subprocess.run(
                    ["ffprobe", "-v", "quiet", "-show_entries", "format=duration",
                     "-of", "default=noprint_wrappers=1:nokey=1", out_path],
                    capture_output=True, text=True, timeout=10)
                duration = float(probe.stdout.strip())
            except Exception:
                duration = 0

            return out_path, duration

        except Exception as e:
            for f in glob.glob(os.path.join(tmp_dir, f"{video_id}.*")):
                try:
                    os.unlink(f)
                except OSError:
                    pass
            if attempt < MAX_DOWNLOAD_RETRIES - 1:
                wait = (2 ** attempt) * 5 + random.random() * 5
                print(f"[{WORKER_TAG}] Download error {video_id}: {e}, "
                      f"retry {attempt+1}/{MAX_DOWNLOAD_RETRIES} in {wait:.0f}s", flush=True)
                time.sleep(wait)
            else:
                return None
    return None


# === Prefetch + async atempo ===
prefetch_q = queue.Queue(maxsize=PREFETCH_DEPTH + 1)
tmp_dir = os.path.join(WORK_DIR, f"tmp_gpu{GPU_ID}_{INSTANCE}")
os.makedirs(tmp_dir, exist_ok=True)


def apply_atempo(vid, title, audio_path, dur):
    """Apply atempo speedup for long videos, then put result in GPU queue."""
    if dur > 1800:
        sped_path = os.path.join(tmp_dir, f"{vid}_fast.opus")
        try:
            proc = subprocess.run(
                ["ffmpeg", "-y", "-i", audio_path,
                 "-filter:a", f"atempo={AUDIO_SPEED}",
                 "-vn", "-c:a", "libopus", "-b:a", "48k", sped_path],
                capture_output=True, text=True, timeout=300)
            if proc.returncode == 0 and os.path.exists(sped_path):
                os.unlink(audio_path)
                audio_path = sped_path
            else:
                try:
                    os.unlink(sped_path)
                except OSError:
                    pass
        except Exception:
            try:
                os.unlink(sped_path)
            except OSError:
                pass
    prefetch_q.put((vid, title, audio_path, dur))


# Thread pool for async atempo — doesn't block download threads
from concurrent.futures import ThreadPoolExecutor
_atempo_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"atempo_gpu{GPU_ID}_{INSTANCE}")


def prefetcher(thread_idx):
    cookie_file = get_thread_cookie_file(thread_idx)
    consec_fails = 0
    print(f"[{WORKER_TAG}] Prefetch thread {thread_idx} started", flush=True)
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
                    print(f"[{WORKER_TAG}] {consec_fails} consecutive download failures, rotating cookies...", flush=True)
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

            # Submit atempo async — download thread immediately starts next download
            _atempo_pool.submit(apply_atempo, vid, title, audio_path, dur)
            time.sleep(random.uniform(1, 3))  # rate-limit protection
        except Exception as e:
            print(f"[{WORKER_TAG}] Prefetch error: {e}", flush=True)


for _i in range(PREFETCH_THREADS):
    threading.Thread(target=prefetcher, args=(_i,), daemon=True).start()

# === Load model ===
print(f"[{WORKER_TAG}] Loading faster-whisper {MODEL_ID} "
      f"(speed={AUDIO_SPEED}x, {PREFETCH_THREADS} prefetch)...", flush=True)

from faster_whisper import WhisperModel

t0 = time.time()
model = WhisperModel(MODEL_ID, device="cuda", compute_type="float16")
print(f"[{WORKER_TAG}] Model loaded in {time.time()-t0:.1f}s", flush=True)

with open(f"/tmp/gpu_{GPU_ID}_{INSTANCE}_ready", "w") as f:
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
        print(f"[{WORKER_TAG}] Prefetch queue empty 60s, waiting...", flush=True)
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
                print(f"[{WORKER_TAG}] #{completed}: {dur/60:.1f}min->{transcribe_s:.1f}s={speed:.0f}x | "
                      f"avg={avg_speed:.0f}x | {hours_done:.1f}h | q={qsize} | {rate_per_h:.0f}/hr", flush=True)
        else:
            mark_error(vid, "empty_transcript")
    except Exception as e:
        print(f"[{WORKER_TAG}] ERROR: {traceback.format_exc()}", flush=True)
        mark_error(vid, str(e))
        try:
            os.unlink(audio_path)
        except (OSError, NameError):
            pass
