#!/usr/bin/env python3
"""Single GPU worker with pipelined download. Run as: CUDA_VISIBLE_DEVICES=X python3 gpu_worker_pipelined.py X"""
import os, sys, time, sqlite3, subprocess, glob, threading, queue, traceback

GPU_ID = int(sys.argv[1]) if len(sys.argv) > 1 else 0
DB_PATH = os.path.expanduser("~/academic_transcriptions/massive_production.db")
WORK_DIR = os.path.expanduser("~/academic_transcriptions")
YTDLP = os.path.join(WORK_DIR, "yt-dlp")
MODEL_ID = "distil-whisper/distil-large-v3"
BATCH_SIZE = 24 if GPU_ID in (0, 2) else 16
PREFETCH_DEPTH = 3

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def claim_video():
    conn = get_db()
    try:
        cur = conn.execute(
            "UPDATE videos SET status='processing', processing_started_at=datetime('now') "
            "WHERE video_id = (SELECT video_id FROM videos WHERE status='pending' ORDER BY priority DESC, id LIMIT 1) "
            "RETURNING video_id, title"
        )
        row = cur.fetchone()
        conn.commit()
        return row
    except:
        return None
    finally:
        conn.close()

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

def download_audio(video_id, out_dir):
    """Download and convert to WAV. Returns (path, duration) or (None, 0)."""
    out_path = os.path.join(out_dir, f"{video_id}.wav")
    try:
        cmd = [YTDLP, "-x", "--audio-format", "wav", "--audio-quality", "0",
               "-o", out_path, "--no-playlist",
               "--socket-timeout", "30", "--retries", "2",
               f"https://www.youtube.com/watch?v={video_id}"]
        subprocess.run(cmd, capture_output=True, text=True, timeout=120, cwd=WORK_DIR)
        
        # Find output file (yt-dlp might add extensions)
        if not os.path.exists(out_path):
            matches = glob.glob(os.path.join(out_dir, f"{video_id}.*"))
            if matches:
                out_path = matches[0]
            else:
                return None, 0
        
        # Get duration
        dur_cmd = ["ffprobe", "-v", "quiet", "-show_entries", "format=duration",
                   "-of", "default=noprint_wrappers=1:nokey=1", out_path]
        dur = subprocess.run(dur_cmd, capture_output=True, text=True, timeout=10)
        duration = float(dur.stdout.strip()) if dur.stdout.strip() else 0
        return out_path, duration
    except Exception as e:
        return None, 0

# === Prefetch thread ===
prefetch_q = queue.Queue(maxsize=PREFETCH_DEPTH + 1)
tmp_dir = os.path.join(WORK_DIR, f"tmp_gpu{GPU_ID}")
os.makedirs(tmp_dir, exist_ok=True)

def prefetcher():
    while True:
        try:
            if prefetch_q.qsize() >= PREFETCH_DEPTH:
                time.sleep(0.5)
                continue
            row = claim_video()
            if not row:
                time.sleep(2)
                continue
            vid, title = row
            path, dur = download_audio(vid, tmp_dir)
            if path and dur > 5:
                prefetch_q.put((vid, title, path, dur))
            elif path:
                mark_error(vid, f"too_short_{dur:.0f}s")
                try: os.unlink(path)
                except: pass
            else:
                mark_error(vid, "download_failed")
        except Exception as e:
            print(f"[GPU {GPU_ID}] Prefetch error: {e}", flush=True)

# Start prefetch threads (multiple for parallelism)
for i in range(2):
    t = threading.Thread(target=prefetcher, daemon=True, name=f"prefetch-{i}")
    t.start()

# === Load model ===
print(f"[GPU {GPU_ID}] Loading model (batch_size={BATCH_SIZE})...", flush=True)

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
print(f"[GPU {GPU_ID}] Model loaded in {time.time()-t0:.1f}s. Prefetch running ({PREFETCH_DEPTH} deep, 2 threads).", flush=True)

# Signal ready (write to file so launcher can detect)
with open(f"/tmp/gpu_{GPU_ID}_ready", "w") as f:
    f.write(str(os.getpid()))

# === Main transcription loop ===
completed = 0
total_audio_s = 0
total_transcribe_s = 0
start_time = time.time()

while True:
    try:
        vid, title, path, dur = prefetch_q.get(timeout=60)
    except queue.Empty:
        print(f"[GPU {GPU_ID}] Prefetch queue empty 60s, waiting...", flush=True)
        continue

    try:
        t0 = time.time()
        result = pipe(path, return_timestamps=True)
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
                elapsed = time.time() - start_time
                hours_done = total_audio_s / 3600
                print(f"[GPU {GPU_ID}] #{completed}: {dur/60:.1f}min in {transcribe_s:.1f}s = {speed:.0f}x | "
                      f"avg={avg_speed:.0f}x | {hours_done:.1f}h done | prefetch={prefetch_q.qsize()}", flush=True)
        else:
            mark_error(vid, "empty_transcript")
    except Exception as e:
        print(f"[GPU {GPU_ID}] ERROR: {traceback.format_exc()}", flush=True)
        mark_error(vid, str(e))
    finally:
        try: os.unlink(path)
        except: pass
