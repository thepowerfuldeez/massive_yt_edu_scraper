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
PREFETCH_DEPTH = 10
PREFETCH_THREADS = 2
CLAIM_BATCH = 15
AUDIO_SPEED = 1.2
MAX_DOWNLOAD_RETRIES = 3

# Per-thread cookie files to avoid concurrent write corruption by yt-dlp.
# yt-dlp rewrites cookie files on each run — sharing across threads causes corruption.
# We copy from a master file to per-thread copies on startup.
COOKIE_MASTER = None
for candidate in [
    os.path.join(WORK_DIR, "cookies_burner.txt"),
    os.path.join(WORK_DIR, "cookies_master.txt"),
    os.path.join(WORK_DIR, "cookies.txt"),
]:
    if os.path.exists(candidate):
        COOKIE_MASTER = candidate
        break
if not COOKIE_MASTER:
    candidates = sorted(glob.glob(os.path.join(WORK_DIR, "cookies*.txt")))
    COOKIE_MASTER = candidates[0] if candidates else None

import shutil

def get_thread_cookie_file(thread_idx):
    """Return a per-thread cookie file path, copied fresh from master."""
    if not COOKIE_MASTER:
        return None
    path = os.path.join(WORK_DIR, f"cookies_gpu{GPU_ID}_t{thread_idx}.txt")
    shutil.copy2(COOKIE_MASTER, path)
    return path

if COOKIE_MASTER:
    print(f"[GPU {GPU_ID}] Cookie master: {os.path.basename(COOKIE_MASTER)}", flush=True)
else:
    print(f"[GPU {GPU_ID}] WARNING: No cookie files found in {WORK_DIR}", flush=True)


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
        cur = conn.execute(
            "UPDATE videos SET status='processing', processing_started_at=datetime('now') "
            "WHERE video_id IN (SELECT video_id FROM videos WHERE status='pending' "
            "AND (duration_seconds >= 900 OR duration_seconds IS NULL OR duration_seconds = 0) "
            "ORDER BY priority DESC, id LIMIT ?) "
            "RETURNING video_id, title", (CLAIM_BATCH,)
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
def download_and_load(video_id, tmp_dir, cookie_file=None):
    """Download audio at 1.2x speed, return (audio_array, sr, original_duration) or None."""
    import librosa
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
                "--socket-timeout", "30", "--retries", "5",
                "--no-warnings", "--no-check-certificates",
                f"https://www.youtube.com/watch?v={video_id}",
            ]
            subprocess.run(cmd, capture_output=True, text=True, timeout=300, cwd=WORK_DIR)

            if not os.path.exists(out_path):
                matches = glob.glob(os.path.join(tmp_dir, f"{video_id}.*"))
                out_path = matches[0] if matches else None

            if not out_path or not os.path.exists(out_path):
                if attempt < MAX_DOWNLOAD_RETRIES - 1:
                    wait = (2 ** attempt) * 5 + random.random() * 5
                    print(f"[GPU {GPU_ID}] Download failed {video_id}, "
                          f"retry {attempt+1}/{MAX_DOWNLOAD_RETRIES} in {wait:.0f}s", flush=True)
                    time.sleep(wait)
                    continue
                return None

            audio, sr = librosa.load(out_path, sr=16000, mono=True)
            original_duration = (len(audio) / sr) * AUDIO_SPEED
            try:
                os.unlink(out_path)
            except OSError:
                pass
            return audio, sr, original_duration

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
    try:
        import librosa  # noqa: F811
    except Exception as e:
        print(f"[GPU {GPU_ID}] Prefetch init failed: {e}", flush=True)
        return

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
            result = download_and_load(vid, tmp_dir, cookie_file=cookie_file)
            if result is None:
                mark_error(vid, "download_failed")
                continue

            audio, sr, dur = result
            if dur < 5:
                mark_error(vid, f"too_short_{dur:.0f}s")
                continue

            prefetch_q.put((vid, title, audio, dur))
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
        vid, title, audio, dur = prefetch_q.get(timeout=60)
    except queue.Empty:
        print(f"[GPU {GPU_ID}] Prefetch queue empty 60s, waiting...", flush=True)
        continue

    try:
        t0 = time.time()
        segments, info = model.transcribe(
            audio, beam_size=1, vad_filter=False,
            word_timestamps=False, condition_on_previous_text=False)
        transcript = " ".join(s.text for s in segments).strip()
        transcribe_s = time.time() - t0

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
