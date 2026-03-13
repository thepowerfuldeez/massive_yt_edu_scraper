#!/usr/bin/env python3
"""Download-only pipeline: fetch audio, apply atempo, store in audio_queue/.

Architecture:
- N download threads (1 per proxy), no GPU needed
- Downloads audio via yt-dlp with proxy rotation
- Applies atempo for long videos (>30min)
- Saves ready-to-transcribe files in QUEUE_DIR
- Updates DB: status 'pending' -> 'processing' -> 'downloaded'
- Transcription server picks up 'downloaded' rows separately

Usage:
    python3 src/downloader.py [--threads 16] [--queue-dir /path/to/audio_queue]
"""
import os, sys, time, random, sqlite3, subprocess, glob, threading, queue, argparse, re, signal

DB_PATH = os.path.expanduser("~/academic_transcriptions/massive_production.db")
WORK_DIR = os.path.expanduser("~/academic_transcriptions")
YTDLP = os.path.join(WORK_DIR, "yt-dlp")
QUEUE_DIR = os.path.join(WORK_DIR, "audio_queue")
PROXY_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "proxy_pool.txt")

AUDIO_SPEED = 1.2
ATEMPO_THRESHOLD = 1800  # apply atempo only for >30min videos
MAX_DOWNLOAD_RETRIES = 3
CLAIM_BATCH = 15
MAX_QUEUE_FILES = 500  # pause downloading if queue has this many files

# Priority buckets for claim (same as worker.py)
BUCKETS = [
    ("license_risk = 'green'", 0.40),
    ("license_risk != 'red' AND priority >= 8 AND duration_seconds < 3600", 0.25),
    ("license_risk != 'red' AND priority >= 8 AND duration_seconds >= 3600", 0.05),
    ("license_risk != 'red' AND priority < 8 AND duration_seconds < 3600", 0.20),
    ("license_risk != 'red' AND priority < 8 AND duration_seconds >= 3600", 0.10),
]


# === Proxy pool ===
def load_proxy_pool():
    proxies = []
    try:
        with open(PROXY_FILE) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                proxies.append(line)
        print(f"[downloader] Loaded {len(proxies)} proxies", flush=True)
        return proxies
    except FileNotFoundError:
        return []

_proxies = load_proxy_pool()
_proxy_failures = {}
_proxy_lock = threading.Lock()


def get_proxy(thread_idx):
    """Each thread gets its own dedicated proxy, fallback on failure."""
    with _proxy_lock:
        if not _proxies:
            return None
        for offset in range(len(_proxies)):
            idx = (thread_idx + offset) % len(_proxies)
            proxy = _proxies[idx]
            if _proxy_failures.get(proxy, 0) < 5:
                return proxy
        _proxy_failures.clear()
        return _proxies[thread_idx % len(_proxies)]


def mark_proxy_ok(proxy):
    with _proxy_lock:
        _proxy_failures.pop(proxy, None)


def mark_proxy_fail(proxy):
    with _proxy_lock:
        _proxy_failures[proxy] = _proxy_failures.get(proxy, 0) + 1
        n = _proxy_failures[proxy]
        if n >= 3:
            ip = proxy.split("@")[-1] if "@" in proxy else proxy
            print(f"[downloader] Proxy {ip} failed {n}x", flush=True)


        return []
    # Exclude expired/ subdirectory
    files = [f for f in files if "/expired/" not in f]
    return files

_cookie_idx = 0
_cookie_lock = threading.Lock()


def get_cookie():
    global _cookie_idx
        return None
    with _cookie_lock:
        _cookie_idx += 1
    return f


# === DB helpers ===
def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


_claim_lock = threading.Lock()
_claimed_queue = queue.Queue()


def refill_claims():
    conn = get_db()
    try:
        all_ids = []
        for cond, frac in BUCKETS:
            n = max(int(CLAIM_BATCH * frac), 1)
            rows = conn.execute(
                f"SELECT video_id FROM videos WHERE status='pending' "
                f"AND (duration_seconds >= 300 OR duration_seconds IS NULL OR duration_seconds = 0) "
                f"AND ({cond}) ORDER BY RANDOM() LIMIT ?", (n,)
            ).fetchall()
            all_ids.extend(r[0] for r in rows)

        if not all_ids:
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
        print(f"[downloader] Claim error: {e}", flush=True)
        return []
    finally:
        conn.close()


def get_claimed():
    with _claim_lock:
        if _claimed_queue.empty():
            for r in refill_claims():
                _claimed_queue.put(r)
    try:
        return _claimed_queue.get_nowait()
    except queue.Empty:
        return None


def mark_downloaded(video_id, duration_s):
    conn = get_db()
    conn.execute(
        "UPDATE videos SET status='downloaded', duration_seconds=? WHERE video_id=?",
        (duration_s, video_id))
    conn.commit()
    conn.close()


def mark_error(video_id, error):
    conn = get_db()
    conn.execute(
        "UPDATE videos SET status='error', error=? WHERE video_id=?",
        (str(error)[:500], video_id))
    conn.commit()
    conn.close()


# === Download + atempo ===
def download_audio(video_id, tmp_dir, thread_idx, cookie_file=None):
    out_template = os.path.join(tmp_dir, f"{video_id}.%(ext)s")

    for attempt in range(MAX_DOWNLOAD_RETRIES):
        try:
            cmd = [YTDLP, "--js-runtimes", "node", "--remote-components", "ejs:github"]
            proxy = get_proxy(thread_idx)
            if proxy:
                cmd += ["--proxy", proxy]
            if cookie_file:
                cmd += ["--cookies", cookie_file]
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
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except (OSError, ProcessLookupError):
                    proc.kill()
                proc.wait()
                raise

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
                    time.sleep(wait)
                    continue
                return None

            if proxy:
                mark_proxy_ok(proxy)

            # Get duration via ffprobe
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
                time.sleep(wait)
            else:
                return None
    return None


def apply_atempo(audio_path, video_id, duration, tmp_dir):
    """Apply atempo speedup for long videos. Returns (final_path, effective_duration)."""
    if duration <= ATEMPO_THRESHOLD:
        return audio_path, duration

    sped_path = os.path.join(tmp_dir, f"{video_id}_fast.opus")
    try:
        proc = subprocess.run(
            ["ffmpeg", "-y", "-i", audio_path,
             "-filter:a", f"atempo={AUDIO_SPEED}",
             "-vn", "-c:a", "libopus", "-b:a", "48k", sped_path],
            capture_output=True, text=True, timeout=300)
        if proc.returncode == 0 and os.path.exists(sped_path):
            os.unlink(audio_path)
            return sped_path, duration / AUDIO_SPEED
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
    return audio_path, duration


def move_to_queue(audio_path, video_id):
    """Move completed audio file to the queue directory."""
    ext = os.path.splitext(audio_path)[1]
    dest = os.path.join(QUEUE_DIR, f"{video_id}{ext}")
    os.rename(audio_path, dest)
    return dest


def queue_size():
    """Count files currently in the queue directory."""
    try:
        return len(os.listdir(QUEUE_DIR))
    except OSError:
        return 0


# === Stats ===
_stats_lock = threading.Lock()
_stats = {"downloaded": 0, "errors": 0, "audio_hrs": 0.0, "start": time.time()}


def log_stats(thread_idx, video_id, duration):
    with _stats_lock:
        _stats["downloaded"] += 1
        _stats["audio_hrs"] += duration / 3600
        n = _stats["downloaded"]
        hrs = _stats["audio_hrs"]
        elapsed = (time.time() - _stats["start"]) / 3600
        rate = n / elapsed if elapsed > 0 else 0
        qsz = queue_size()
        if n % 10 == 0 or n <= 5:
            print(f"[T{thread_idx:02d}] #{n}: {video_id} {duration/60:.0f}min | "
                  f"{hrs:.0f}h total | {rate:.0f}/hr | queue={qsz}", flush=True)


# === Download thread ===
def download_thread(thread_idx):
    tmp_dir = os.path.join(WORK_DIR, f"tmp_dl_{thread_idx}")
    os.makedirs(tmp_dir, exist_ok=True)
    cookie_file = get_cookie()
    consec_fails = 0

    print(f"[T{thread_idx:02d}] Started (proxy: {get_proxy(thread_idx).split('@')[-1] if get_proxy(thread_idx) else 'none'})",
          flush=True)

    while True:
        try:
            # Backpressure: pause if queue is full
            while queue_size() >= MAX_QUEUE_FILES:
                time.sleep(5)

            row = get_claimed()
            if not row:
                time.sleep(2)
                continue

            vid, title = row
            result = download_audio(vid, tmp_dir, thread_idx, cookie_file=cookie_file)
            if result is None:
                mark_error(vid, "download_failed")
                consec_fails += 1
                if consec_fails >= 5:
                    cookie_file = get_cookie()
                    consec_fails = 0
                    time.sleep(10)
                continue

            consec_fails = 0
            audio_path, dur = result

            if dur < 5:
                mark_error(vid, f"too_short_{dur:.0f}s")
                try:
                    os.unlink(audio_path)
                except OSError:
                    pass
                continue

            # Apply atempo for long videos
            audio_path, effective_dur = apply_atempo(audio_path, vid, dur, tmp_dir)

            # Move to queue dir
            dest = move_to_queue(audio_path, vid)

            # Update DB
            mark_downloaded(vid, dur)  # store original duration

            log_stats(thread_idx, vid, dur)
            time.sleep(random.uniform(1, 3))

        except Exception as e:
            print(f"[T{thread_idx:02d}] Error: {e}", flush=True)
            time.sleep(5)


def _update_config(queue_dir, max_queue):
    global QUEUE_DIR, MAX_QUEUE_FILES
    QUEUE_DIR = queue_dir
    MAX_QUEUE_FILES = max_queue


def main():
    parser = argparse.ArgumentParser(description="Download-only pipeline for audio queue")
    parser.add_argument("--threads", type=int, default=len(_proxies) or 16,
                        help="Number of download threads (default: num proxies)")
    parser.add_argument("--queue-dir", default=QUEUE_DIR,
                        help="Output directory for audio files")
    parser.add_argument("--max-queue", type=int, default=MAX_QUEUE_FILES,
                        help="Pause downloading when queue has this many files")
    args = parser.parse_args()

    # Update module-level config from args
    _update_config(args.queue_dir, args.max_queue)
    os.makedirs(args.queue_dir, exist_ok=True)

    print(f"=" * 60, flush=True)
    print(f"Audio Downloader — {args.threads} threads", flush=True)
    print(f"Queue dir: {QUEUE_DIR}", flush=True)
    print(f"Max queue size: {MAX_QUEUE_FILES}", flush=True)
    print(f"Proxies: {len(_proxies)}", flush=True)
    print(f"=" * 60, flush=True)

    threads = []
    for i in range(args.threads):
        t = threading.Thread(target=download_thread, args=(i,), daemon=True)
        t.start()
        threads.append(t)
        time.sleep(0.5)  # stagger starts

    # Main thread: periodic stats
    try:
        while True:
            time.sleep(300)
            with _stats_lock:
                n = _stats["downloaded"]
                e = _stats["errors"]
                hrs = _stats["audio_hrs"]
                elapsed = (time.time() - _stats["start"]) / 3600
                rate = n / elapsed if elapsed > 0 else 0
            qsz = queue_size()
            print(f"\n[stats] {n} downloaded, {e} errors | {hrs:.0f} audio hrs | "
                  f"{rate:.0f}/hr | queue={qsz} | {elapsed:.1f}h elapsed\n", flush=True)
    except KeyboardInterrupt:
        print("\n[downloader] Shutting down...", flush=True)


if __name__ == "__main__":
    main()
