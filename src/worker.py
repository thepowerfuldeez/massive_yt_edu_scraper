#!/usr/bin/env python3
"""GPU worker using faster-whisper (CTranslate2) + 1.2x audio speedup.

Optimizations over HF pipeline worker:
- CTranslate2 backend: fused C++/CUDA kernels, ~3-4x faster than PyTorch
- 1.2x audio speedup via yt-dlp atempo filter: 17% less audio to process
- No VAD (benchmarked: adds overhead on dense lectures)
- condition_on_previous_text=False: faster, no cross-segment dependency
- Combined: ~2x total throughput improvement over HF pipeline
"""
import os, sys, time, sqlite3, subprocess, glob, threading, queue, traceback
import numpy as np

GPU_ID = int(sys.argv[1]) if len(sys.argv) > 1 else 0
DB_PATH = os.path.expanduser("~/academic_transcriptions/massive_production.db")
WORK_DIR = os.path.expanduser("~/academic_transcriptions")
YTDLP = os.path.join(WORK_DIR, "yt-dlp")
MODEL_ID = "distil-large-v3.5"
PREFETCH_DEPTH = 15
PREFETCH_THREADS = 8
CLAIM_BATCH = 20
AUDIO_SPEED = 1.2  # Speed up audio by 20%

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

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
            rows = refill_claims()
            for r in rows:
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
        (transcript, duration_s, transcribe_s, speed, video_id)
    )
    conn.commit()
    conn.close()

def mark_error(video_id, error):
    conn = get_db()
    conn.execute(
        "UPDATE videos SET status='error', error=? WHERE video_id=?",
        (str(error)[:500], video_id)
    )
    conn.commit()
    conn.close()

def download_and_load(video_id, tmp_dir):
    """Download audio at 1.2x speed and load as numpy array.
    
    Uses yt-dlp postprocessor args to speed up audio via ffmpeg atempo filter.
    This gives us 20% less audio to transcribe with negligible quality loss for speech.
    """
    import librosa
    out_template = os.path.join(tmp_dir, f"{video_id}.%(ext)s")
    out_path = os.path.join(tmp_dir, f"{video_id}.mp3")
    try:
        cmd = [
            YTDLP, "-x", "--audio-format", "mp3", "--audio-quality", "5",
            # Speed up audio by 1.2x using ffmpeg atempo filter
            "--postprocessor-args", f"ffmpeg:-filter:a atempo={AUDIO_SPEED}",
            "-o", out_template, "--no-playlist",
            "--concurrent-fragments", "4",
            "--socket-timeout", "20", "--retries", "3", "--no-warnings", "--no-check-certificates",
            f"https://www.youtube.com/watch?v={video_id}"
        ]
        subprocess.run(cmd, capture_output=True, text=True, timeout=180, cwd=WORK_DIR)
        
        if not os.path.exists(out_path):
            matches = glob.glob(os.path.join(tmp_dir, f"{video_id}.*"))
            if matches:
                out_path = matches[0]
            else:
                return None
        
        audio, sr = librosa.load(out_path, sr=16000, mono=True)
        # Duration of sped-up audio
        sped_duration = len(audio) / sr
        # Original duration (before speedup) for DB
        original_duration = sped_duration * AUDIO_SPEED
        
        try: os.unlink(out_path)
        except: pass
        
        return audio, sr, original_duration
    except Exception as e:
        try: os.unlink(out_path)
        except: pass
        return None

# === Prefetch queue ===
prefetch_q = queue.Queue(maxsize=PREFETCH_DEPTH + 1)
tmp_dir = os.path.join(WORK_DIR, f"tmp_gpu{GPU_ID}")
os.makedirs(tmp_dir, exist_ok=True)

def prefetcher():
    import librosa
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
            result = download_and_load(vid, tmp_dir)
            if result is None:
                mark_error(vid, "download_failed")
                continue
            audio, sr, dur = result
            if dur < 5:
                mark_error(vid, f"too_short_{dur:.0f}s")
                continue
            prefetch_q.put((vid, title, audio, dur))
        except Exception as e:
            print(f"[GPU {GPU_ID}] Prefetch error: {e}", flush=True)

for i in range(PREFETCH_THREADS):
    t = threading.Thread(target=prefetcher, daemon=True, name=f"prefetch-{i}")
    t.start()

# === Load model ===
print(f"[GPU {GPU_ID}] Loading faster-whisper {MODEL_ID} (speed={AUDIO_SPEED}x, {PREFETCH_THREADS} prefetch)...", flush=True)

from faster_whisper import WhisperModel

t0 = time.time()
model = WhisperModel(MODEL_ID, device="cuda", compute_type="float16")
print(f"[GPU {GPU_ID}] Model loaded in {time.time()-t0:.1f}s", flush=True)

# Signal ready
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
            audio,
            beam_size=1,
            vad_filter=False,  # Benchmarked: VAD adds overhead on dense lectures
            word_timestamps=False,
            condition_on_previous_text=False,  # Faster, no cross-segment conditioning
        )
        transcript = " ".join(s.text for s in segments).strip()
        transcribe_s = time.time() - t0

        if transcript:
            mark_done(vid, transcript, dur, transcribe_s)
            completed += 1
            total_audio_s += dur
            total_transcribe_s += transcribe_s
            # speed_ratio is based on ORIGINAL duration vs transcribe time
            speed = dur / transcribe_s if transcribe_s > 0 else 0

            if completed % 5 == 0 or completed <= 3:
                avg_speed = total_audio_s / total_transcribe_s if total_transcribe_s > 0 else 0
                hours_done = total_audio_s / 3600
                qsize = prefetch_q.qsize()
                elapsed_h = (time.time() - start_time) / 3600
                rate_per_h = completed / elapsed_h if elapsed_h > 0 else 0
                print(f"[GPU {GPU_ID}] #{completed}: {dur/60:.1f}min->{transcribe_s:.1f}s={speed:.0f}x | "
                      f"avg={avg_speed:.0f}x | {hours_done:.1f}h | q={qsize} | {rate_per_h:.0f}/hr", flush=True)
        else:
            mark_error(vid, "empty_transcript")
    except Exception as e:
        print(f"[GPU {GPU_ID}] ERROR: {traceback.format_exc()}", flush=True)
        mark_error(vid, str(e))
