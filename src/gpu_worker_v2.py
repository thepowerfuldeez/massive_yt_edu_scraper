#!/usr/bin/env python3
"""GPU worker v2 — optimized for throughput.

Key improvements over v1:
- Download as opus (smaller, faster download) → pipe directly to model
- Batch claim 10 videos at once (fewer DB round-trips)
- 4 prefetch threads (saturate network)
- Pre-load audio with librosa in prefetch thread (CPU work off critical path)
- Skip ffprobe — get duration from loaded audio array
"""
import os, sys, time, sqlite3, subprocess, glob, threading, queue, traceback
import numpy as np

GPU_ID = int(sys.argv[1]) if len(sys.argv) > 1 else 0
DB_PATH = os.path.expanduser("~/academic_transcriptions/massive_production.db")
WORK_DIR = os.path.expanduser("~/academic_transcriptions")
YTDLP = os.path.join(WORK_DIR, "yt-dlp")
MODEL_ID = "distil-whisper/distil-large-v3.5"
BATCH_SIZE = 32 if GPU_ID in (0, 2) else 24
PREFETCH_DEPTH = 5
PREFETCH_THREADS = 4
CLAIM_BATCH = 10

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

# Claim multiple videos at once
claim_lock = threading.Lock()
claimed_queue = queue.Queue()

def refill_claims():
    """Batch-claim videos from DB."""
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
    """Get next claimed video, refilling batch if needed."""
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
    """Download audio and load as numpy array. Returns (audio_array, sample_rate, duration) or None."""
    import librosa
    
    out_path = os.path.join(tmp_dir, f"{video_id}.mp3")
    try:
        # Download as mp3 (much smaller than wav, fast to download)
        cmd = [YTDLP, "-x", "--audio-format", "mp3", "--audio-quality", "5",  # q5 is fine for speech
               "-o", out_path, "--no-playlist",
               "--socket-timeout", "30", "--retries", "2", "--no-warnings",
               f"https://www.youtube.com/watch?v={video_id}"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120, cwd=WORK_DIR)
        
        if not os.path.exists(out_path):
            matches = glob.glob(os.path.join(tmp_dir, f"{video_id}.*"))
            if matches:
                out_path = matches[0]
            else:
                return None
        
        # Load and resample to 16kHz in prefetch thread (CPU work, not on GPU critical path)
        audio, sr = librosa.load(out_path, sr=16000, mono=True)
        duration = len(audio) / sr
        
        # Clean up file immediately
        try: os.unlink(out_path)
        except: pass
        
        return audio, sr, duration
        
    except Exception as e:
        try: os.unlink(out_path)
        except: pass
        return None

# === Prefetch queue: pre-downloaded AND pre-loaded audio ===
prefetch_q = queue.Queue(maxsize=PREFETCH_DEPTH + 1)
tmp_dir = os.path.join(WORK_DIR, f"tmp_gpu{GPU_ID}")
os.makedirs(tmp_dir, exist_ok=True)

def prefetcher():
    import librosa  # import in thread
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

# Start prefetch threads
for i in range(PREFETCH_THREADS):
    t = threading.Thread(target=prefetcher, daemon=True, name=f"prefetch-{i}")
    t.start()

# === Load model ===
print(f"[GPU {GPU_ID}] Loading v3.5 model (batch_size={BATCH_SIZE}, {PREFETCH_THREADS} prefetch threads)...", flush=True)

import torch
from transformers import AutoModelForSpeechSeq2Seq, AutoProcessor, pipeline

t0 = time.time()
model = AutoModelForSpeechSeq2Seq.from_pretrained(
    MODEL_ID, torch_dtype=torch.float16, low_cpu_mem_usage=True, use_safetensors=True,
).to("cuda:0")
processor = AutoProcessor.from_pretrained(MODEL_ID)
pipe = pipeline(
    "automatic-speech-recognition", model=model,
    tokenizer=processor.tokenizer, feature_extractor=processor.feature_extractor,
    torch_dtype=torch.float16, device="cuda:0",
    batch_size=BATCH_SIZE, chunk_length_s=30,
)
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
        # Pass numpy array directly — no file I/O on GPU path
        result = pipe({"raw": audio, "sampling_rate": 16000}, return_timestamps=True)
        transcribe_s = time.time() - t0
        transcript = result["text"].strip()

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
                print(f"[GPU {GPU_ID}] #{completed}: {dur/60:.1f}min→{transcribe_s:.1f}s={speed:.0f}x | "
                      f"avg={avg_speed:.0f}x | {hours_done:.1f}h | q={qsize}", flush=True)
        else:
            mark_error(vid, "empty_transcript")
    except Exception as e:
        print(f"[GPU {GPU_ID}] ERROR: {traceback.format_exc()}", flush=True)
        mark_error(vid, str(e))
