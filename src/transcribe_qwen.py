#!/usr/bin/env python3
"""Qwen3-ASR transcription worker: batch transcription via vLLM DP=8 server.

Architecture:
- vLLM server runs Qwen3-ASR-1.7B with data_parallel_size=8 (one replica per GPU)
- This worker claims 'downloaded' files, groups by similar duration, sends batches
- Transcripts written to DB, audio files deleted

Usage:
    # 1) Start vLLM server (once):
    ./launch_vllm.sh

    # 2) Start transcription worker:
    python3 src/transcribe_qwen.py [--batch-size 16] [--server http://localhost:8000]
"""
import os, sys, time, sqlite3, glob, argparse, traceback, subprocess, json, base64
import urllib.request, urllib.error

DB_PATH = os.path.expanduser("~/academic_transcriptions/massive_production.db")
QUEUE_DIR = os.path.expanduser("~/academic_transcriptions/audio_queue")

# Tokens per minute of audio at P99 ≈ 330, with margin
TOKENS_PER_MIN = 380


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def get_duration_sorted_batch(batch_size):
    """Claim a batch of downloaded videos with similar durations.

    Picks a random duration bucket, then grabs batch_size files from it.
    This prevents draining all short videos first.
    """
    conn = get_db()
    # Pick a random bucket proportional to real workload mix
    import random
    buckets = [
        (0, 900, 0.30),       # <15m
        (900, 1800, 0.35),    # 15-30m
        (1800, 3600, 0.25),   # 30-60m
        (3600, 999999, 0.10), # >1h
    ]
    r = random.random()
    cumulative = 0
    lo, hi = 0, 999999
    for blo, bhi, weight in buckets:
        cumulative += weight
        if r <= cumulative:
            lo, hi = blo, bhi
            break

    rows = conn.execute(
        "SELECT video_id, title, duration_seconds FROM videos "
        "WHERE status='downloaded' AND duration_seconds >= ? AND duration_seconds < ? "
        "ORDER BY RANDOM() LIMIT ?",
        (lo, hi, batch_size * 3)
    ).fetchall()

    # Fallback: if chosen bucket is empty, grab anything
    if not rows:
        rows = conn.execute(
            "SELECT video_id, title, duration_seconds FROM videos "
            "WHERE status='downloaded' ORDER BY RANDOM() LIMIT ?",
            (batch_size * 3,)
        ).fetchall()

    # Filter to those with actual files, convert if needed, sort by duration
    candidates = []
    for vid, title, dur in rows:
        matches = glob.glob(os.path.join(QUEUE_DIR, f"{vid}.*"))
        if matches:
            path = ensure_loadable(matches[0])
            candidates.append((vid, title, dur or 0, path))
        if len(candidates) >= batch_size:
            break
    candidates.sort(key=lambda x: x[2])  # sort by duration within batch

    if not candidates:
        conn.close()
        return []

    # Already sorted by duration from SQL
    vids = [c[0] for c in candidates]
    placeholders = ",".join("?" * len(vids))
    conn.execute(
        f"UPDATE videos SET status='transcribing' WHERE video_id IN ({placeholders})",
        vids
    )
    conn.commit()
    conn.close()
    return candidates


def mark_done(video_id, transcript, duration_s, transcribe_s):
    conn = get_db()
    speed = duration_s / transcribe_s if transcribe_s > 0 else 0
    conn.execute(
        "UPDATE videos SET status='completed', transcript=?, duration_seconds=?, "
        "processing_time_seconds=?, speed_ratio=?, completed_at=datetime('now') "
        "WHERE video_id=?",
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


def cleanup_file(path):
    try:
        os.unlink(path)
    except OSError:
        pass


def ensure_loadable(path):
    """Convert webm/m4a to opus if needed (soundfile can't read webm)."""
    ext = os.path.splitext(path)[1].lower()
    if ext in (".opus", ".wav", ".flac", ".ogg"):
        return path
    opus_path = os.path.splitext(path)[0] + ".opus"
    try:
        proc = subprocess.run(
            ["ffmpeg", "-y", "-i", path, "-vn", "-c:a", "libopus", "-b:a", "48k", opus_path],
            capture_output=True, text=True, timeout=120)
        if proc.returncode == 0 and os.path.exists(opus_path):
            os.unlink(path)
            return opus_path
    except Exception:
        pass
    return path


def transcribe_via_api(server_url, audio_path, max_tokens):
    """Send audio file to vLLM OpenAI-compatible API, return transcript text."""
    with open(audio_path, "rb") as f:
        audio_bytes = f.read()
    audio_b64 = base64.b64encode(audio_bytes).decode()

    # Determine MIME type from extension
    ext = os.path.splitext(audio_path)[1].lower()
    mime_map = {".opus": "audio/opus", ".webm": "audio/webm", ".m4a": "audio/mp4",
                ".wav": "audio/wav", ".mp3": "audio/mpeg", ".ogg": "audio/ogg"}
    mime = mime_map.get(ext, "audio/wav")

    payload = {
        "model": "Qwen/Qwen3-ASR-1.7B",
        "messages": [{"role": "user", "content": [
            {"type": "audio_url", "audio_url": {
                "url": f"data:{mime};base64,{audio_b64}"
            }}
        ]}],
        "max_tokens": max_tokens,
        "temperature": 0.0,
    }

    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"{server_url}/v1/chat/completions",
        data=data,
        headers={"Content-Type": "application/json"},
    )

    resp = urllib.request.urlopen(req, timeout=600)
    result = json.loads(resp.read())
    text = result["choices"][0]["message"]["content"]

    # Parse ASR output (format: "<|language|>text")
    # The model outputs: <|en|> transcript text here
    if text and "|>" in text:
        text = text.split("|>", 1)[-1].strip()
    return text


def transcribe_batch_offline(model, batch):
    """Transcribe batch using offline vLLM model (qwen-asr wrapper)."""
    audio_paths = [b[3] for b in batch]
    results = model.transcribe(audio=audio_paths, language=None, return_time_stamps=False)
    return [r.text.strip() if r.text else "" for r in results]


def main():
    parser = argparse.ArgumentParser(description="Qwen3-ASR batch transcription worker")
    parser.add_argument("--batch-size", type=int, default=16,
                        help="Number of files per batch")
    parser.add_argument("--server", type=str, default=None,
                        help="vLLM server URL (e.g. http://localhost:8000). "
                        "If not set, uses offline mode with qwen-asr wrapper.")
    parser.add_argument("--gpu", type=int, default=None,
                        help="GPU ID for offline mode (default: all GPUs with TP)")
    parser.add_argument("--gpu-memory", type=float, default=0.85,
                        help="GPU memory utilization for offline mode")
    parser.add_argument("--max-tokens", type=int, default=22800,
                        help="Max new tokens (default: enough for ~60min audio)")
    parser.add_argument("--queue-dir", type=str, default=QUEUE_DIR)
    args = parser.parse_args()

    queue_dir = args.queue_dir

    print("=" * 60, flush=True)
    print("Qwen3-ASR Transcription Worker", flush=True)
    print(f"Batch size: {args.batch_size}", flush=True)
    print(f"Max tokens: {args.max_tokens}", flush=True)
    print(f"Queue dir: {queue_dir}", flush=True)

    model = None
    if args.server:
        print(f"Mode: API client -> {args.server}", flush=True)
    else:
        # Offline mode — CUDA_VISIBLE_DEVICES should be set externally
        visible = os.environ.get("CUDA_VISIBLE_DEVICES", "all")
        print(f"Mode: Offline (CUDA_VISIBLE_DEVICES={visible})", flush=True)

        print("Loading Qwen3-ASR-1.7B...", flush=True)
        t0 = time.time()
        from qwen_asr import Qwen3ASRModel
        model = Qwen3ASRModel.LLM(
            model="Qwen/Qwen3-ASR-1.7B",
            gpu_memory_utilization=args.gpu_memory,
            max_inference_batch_size=args.batch_size,
            max_new_tokens=args.max_tokens,
            max_model_len=args.max_tokens,
        )
        print(f"Model loaded in {time.time()-t0:.1f}s", flush=True)

    print("=" * 60, flush=True)

    completed = 0
    total_audio_s = 0
    total_transcribe_s = 0
    start_time = time.time()

    while True:
        batch = get_duration_sorted_batch(args.batch_size)
        if not batch:
            qsize = len(glob.glob(os.path.join(queue_dir, "*")))
            time.sleep(5 if qsize == 0 else 2)
            continue

        try:
            t0 = time.time()

            if model:
                # Offline batch
                transcripts = transcribe_batch_offline(model, batch)
            else:
                # API mode — send one by one (API doesn't batch natively)
                # Could parallelize with threads but keep simple for now
                transcripts = []
                for vid, title, dur, path in batch:
                    max_tok = max(int(TOKENS_PER_MIN * (dur or 1800) / 60 * 1.15), 2048)
                    max_tok = min(max_tok, args.max_tokens)
                    try:
                        text = transcribe_via_api(args.server, path, max_tok)
                        transcripts.append(text)
                    except Exception as e:
                        print(f"[API error] {vid}: {e}", flush=True)
                        transcripts.append("")

            batch_time = time.time() - t0
            per_item_time = batch_time / len(batch)

            for i, (vid, title, dur, path) in enumerate(batch):
                try:
                    transcript = transcripts[i].strip() if i < len(transcripts) else ""
                    if transcript:
                        mark_done(vid, transcript, dur or 0, per_item_time)
                        completed += 1
                        total_audio_s += dur or 0
                        total_transcribe_s += per_item_time
                    else:
                        mark_error(vid, "empty_transcript")
                except Exception as e:
                    mark_error(vid, f"result_error: {e}")
                cleanup_file(path)

            elapsed_hr = (time.time() - start_time) / 3600
            rate = completed / elapsed_hr if elapsed_hr > 0 else 0
            avg_speed = total_audio_s / total_transcribe_s if total_transcribe_s > 0 else 0
            audio_hrs = total_audio_s / 3600
            qsize = len(glob.glob(os.path.join(queue_dir, "*")))

            print(f"[batch] {len(batch)} in {batch_time:.1f}s "
                  f"({per_item_time:.1f}s/ea) | "
                  f"done={completed} | {audio_hrs:.1f}h | "
                  f"{avg_speed:.0f}x | {rate:.0f}/hr | q={qsize}",
                  flush=True)

        except Exception as e:
            print(f"[ERROR] {traceback.format_exc()}", flush=True)
            for vid, title, dur, path in batch:
                mark_error(vid, f"batch_error: {str(e)[:200]}")
                cleanup_file(path)
            time.sleep(5)


if __name__ == "__main__":
    main()
