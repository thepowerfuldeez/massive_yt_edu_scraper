#!/usr/bin/env python3
"""Benchmark proxy speeds by downloading a short YouTube audio clip through each.

Usage:
    python3 src/proxy_bench.py                          # test all proxies in proxy_pool.txt
    python3 src/proxy_bench.py --proxy-file /path/to/proxies.txt
    python3 src/proxy_bench.py --workers 0,1,2          # pull proxy lists from S3 worker dirs
    python3 src/proxy_bench.py --all-workers             # test all 7 workers from S3

Output: ranked list of proxies by download speed (fastest first).
"""
import os, sys, time, subprocess, argparse, tempfile, shutil, glob, json
from concurrent.futures import ThreadPoolExecutor, as_completed

WORK_DIR = os.environ.get("WORK_DIR", os.path.expanduser("~/academic_transcriptions"))
YTDLP = os.path.join(WORK_DIR, "yt-dlp")
if not os.path.exists(YTDLP):
    YTDLP = shutil.which("yt-dlp") or "yt-dlp"

# Short, stable test videos (public domain / CC, ~2-5 min)
TEST_VIDEOS = [
    "jNQXAC9IVRw",   # "Me at the zoo" — first YouTube video, 19s
    "dQw4w9WgXcQ",   # Rick Astley, always available
    "9bZkp7q19f0",   # Gangnam Style
]

def test_proxy(proxy, video_id, timeout=60):
    """Download a short clip via proxy, return (proxy, seconds, bytes, error)."""
    tmp = tempfile.mkdtemp(prefix="proxy_bench_")
    try:
        cmd = [
            YTDLP,
            "--js-runtimes", "node", "--remote-components", "ejs:github",
            "--proxy", proxy,
            "-f", "ba[abr<=96]/wa/ba",
            "-o", os.path.join(tmp, "%(id)s.%(ext)s"),
            "--no-playlist", "--no-check-certificates",
            "--socket-timeout", "20", "--retries", "1",
            f"https://www.youtube.com/watch?v={video_id}",
        ]
        t0 = time.monotonic()
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=tmp)
        elapsed = time.monotonic() - t0

        files = [f for f in glob.glob(os.path.join(tmp, f"{video_id}.*"))
                 if not f.endswith(".part")]
        if files and os.path.getsize(files[0]) > 0:
            size = os.path.getsize(files[0])
            return proxy, elapsed, size, None
        else:
            err = ""
            if proc.stderr:
                for line in proc.stderr.strip().split("\n"):
                    if "ERROR" in line:
                        err = line.strip()[-120:]
                        break
            return proxy, elapsed, 0, err or "no_output"
    except subprocess.TimeoutExpired:
        return proxy, timeout, 0, "timeout"
    except Exception as e:
        return proxy, 0, 0, str(e)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def load_proxies(path):
    proxies = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                proxies.append(line)
    return proxies


def proxy_label(proxy):
    """Extract IP:port from proxy URL for display."""
    if "@" in proxy:
        return proxy.split("@")[-1]
    return proxy.replace("http://", "").replace("https://", "")


def bench_proxies(proxies, label="", rounds=2):
    """Benchmark a list of proxies, return sorted results."""
    print(f"\n{'='*60}")
    print(f"Benchmarking {len(proxies)} proxies {label}")
    print(f"{'='*60}")

    # Aggregate results across rounds
    results = {}  # proxy -> list of (elapsed, size)

    for rnd in range(rounds):
        vid = TEST_VIDEOS[rnd % len(TEST_VIDEOS)]
        print(f"\n  Round {rnd+1}/{rounds} (video={vid})...")

        with ThreadPoolExecutor(max_workers=min(len(proxies), 8)) as pool:
            futures = {pool.submit(test_proxy, p, vid): p for p in proxies}
            for fut in as_completed(futures):
                proxy, elapsed, size, err = fut.result()
                ip = proxy_label(proxy)
                if proxy not in results:
                    results[proxy] = []
                if err:
                    print(f"    FAIL {ip}: {err}")
                    results[proxy].append((999, 0))
                else:
                    speed_kbps = (size / 1024) / elapsed if elapsed > 0 else 0
                    print(f"    OK   {ip}: {elapsed:.1f}s, {size/1024:.0f}KB, {speed_kbps:.0f} KB/s")
                    results[proxy].append((elapsed, size))

    # Rank by average speed
    ranked = []
    for proxy, runs in results.items():
        ok_runs = [(e, s) for e, s in runs if s > 0]
        if ok_runs:
            avg_time = sum(e for e, s in ok_runs) / len(ok_runs)
            avg_size = sum(s for e, s in ok_runs) / len(ok_runs)
            avg_speed = (avg_size / 1024) / avg_time if avg_time > 0 else 0
            fail_rate = 1 - len(ok_runs) / len(runs)
            ranked.append((proxy, avg_time, avg_speed, fail_rate, len(ok_runs)))
        else:
            ranked.append((proxy, 999, 0, 1.0, 0))

    # Sort: fastest first (by avg speed descending)
    ranked.sort(key=lambda x: -x[2])

    print(f"\n{'='*60}")
    print(f"Results {label} (sorted by speed)")
    print(f"{'='*60}")
    print(f"{'Rank':>4}  {'Proxy IP':>25}  {'Avg Time':>8}  {'Speed KB/s':>10}  {'Fail%':>5}  {'OK':>3}")
    print(f"{'----':>4}  {'--------':>25}  {'--------':>8}  {'----------':>10}  {'-----':>5}  {'--':>3}")
    for i, (proxy, avg_t, avg_spd, fail_r, ok_n) in enumerate(ranked):
        ip = proxy_label(proxy)
        status = "DEAD" if ok_n == 0 else ""
        print(f"{i+1:>4}  {ip:>25}  {avg_t:>7.1f}s  {avg_spd:>9.0f}  {fail_r*100:>4.0f}%  {ok_n:>3}  {status}")

    return ranked


def pull_worker_proxies(worker_ids, bucket="poolside-dev-pods", prefix="yt-edu"):
    """Download proxy lists from S3 for given workers."""
    worker_proxies = {}
    for wid in worker_ids:
        tmp = f"/tmp/proxy_bench_worker_{wid}.txt"
        r = subprocess.run(
            ["aws", "s3", "cp", f"s3://{bucket}/{prefix}/worker_{wid}/proxy_pool.txt", tmp, "--quiet"],
            capture_output=True)
        if r.returncode == 0 and os.path.exists(tmp):
            proxies = load_proxies(tmp)
            worker_proxies[wid] = proxies
            print(f"Worker {wid}: {len(proxies)} proxies")
        else:
            print(f"Worker {wid}: no proxy file found")
    return worker_proxies


def main():
    parser = argparse.ArgumentParser(description="Benchmark proxy speeds for yt-dlp downloads")
    parser.add_argument("--proxy-file", default=None, help="Path to proxy_pool.txt")
    parser.add_argument("--workers", default=None, help="Comma-separated worker IDs to pull from S3")
    parser.add_argument("--all-workers", action="store_true", help="Test all workers 0-6 from S3")
    parser.add_argument("--rounds", type=int, default=2, help="Test rounds per proxy (default: 2)")
    parser.add_argument("--json", default=None, help="Write results as JSON to this path")
    args = parser.parse_args()

    all_results = {}

    if args.all_workers:
        worker_ids = list(range(7))
    elif args.workers:
        worker_ids = [int(x) for x in args.workers.split(",")]
    else:
        worker_ids = None

    # Test local proxy file
    if args.proxy_file or (not worker_ids):
        pf = args.proxy_file or os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "proxy_pool.txt")
        if os.path.exists(pf):
            proxies = load_proxies(pf)
            ranked = bench_proxies(proxies, label=f"(local: {pf})", rounds=args.rounds)
            all_results["local"] = [
                {"proxy": proxy_label(p), "avg_time": t, "speed_kbps": s, "fail_rate": f, "ok_runs": n}
                for p, t, s, f, n in ranked
            ]

    # Test worker proxies from S3
    if worker_ids is not None:
        wp = pull_worker_proxies(worker_ids)
        for wid, proxies in wp.items():
            ranked = bench_proxies(proxies, label=f"(worker {wid})", rounds=args.rounds)
            all_results[f"worker_{wid}"] = [
                {"proxy": proxy_label(p), "avg_time": t, "speed_kbps": s, "fail_rate": f, "ok_runs": n}
                for p, t, s, f, n in ranked
            ]

    # Summary
    if all_results:
        print(f"\n{'='*60}")
        print("SUMMARY — all sources")
        print(f"{'='*60}")
        all_proxies = []
        for src, results in all_results.items():
            for r in results:
                r["source"] = src
                all_proxies.append(r)
        all_proxies.sort(key=lambda x: -x["speed_kbps"])
        print(f"{'Rank':>4}  {'Source':>10}  {'Proxy IP':>25}  {'Speed KB/s':>10}  {'Fail%':>5}")
        for i, r in enumerate(all_proxies):
            print(f"{i+1:>4}  {r['source']:>10}  {r['proxy']:>25}  {r['speed_kbps']:>9.0f}  {r['fail_rate']*100:>4.0f}%")

    if args.json:
        with open(args.json, "w") as f:
            json.dump(all_results, f, indent=2)
        print(f"\nJSON results written to {args.json}")


if __name__ == "__main__":
    main()
