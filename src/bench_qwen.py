#!/usr/bin/env python3
"""Benchmark Qwen3-ASR: test TP vs DP with duration-sorted batches."""
import glob, subprocess, time, sys, os

QUEUE_DIR = os.path.expanduser("~/academic_transcriptions/audio_queue")

# tokens/min P99 ≈ 330, with 15% margin
TOKENS_PER_MIN = 380

def get_files_by_duration():
    files = glob.glob(os.path.join(QUEUE_DIR, "*"))
    results = []
    for f in files:
        p = subprocess.run(["ffprobe", "-v", "quiet", "-show_entries", "format=duration",
                           "-of", "default=noprint_wrappers=1:nokey=1", f],
                          capture_output=True, text=True, timeout=10)
        try:
            results.append((float(p.stdout.strip()), f))
        except:
            pass
    results.sort()
    return results


def make_uniform_batch(files_with_dur, target_dur_range, batch_size):
    """Pick batch_size files within target duration range."""
    lo, hi = target_dur_range
    candidates = [(d, f) for d, f in files_with_dur if lo <= d < hi]
    return candidates[:batch_size]


def run_test(model, batch, label):
    files = [f for _, f in batch]
    durations = [d for d, _ in batch]
    max_dur = max(durations)
    total_audio = sum(durations)

    print(f"\n{'='*60}", flush=True)
    print(f"TEST: {label}", flush=True)
    print(f"  Files: {len(files)}, dur range: {min(durations)/60:.0f}-{max_dur/60:.0f} min", flush=True)
    print(f"  Total audio: {total_audio/60:.0f} min", flush=True)

    t0 = time.time()
    results = model.transcribe(audio=files, language=None, return_time_stamps=False)
    elapsed = time.time() - t0

    ok = sum(1 for r in results if r.text and len(r.text.strip()) > 0)
    chars = [len(r.text) for r in results if r.text]

    print(f"  Result: {ok}/{len(files)} OK in {elapsed:.1f}s", flush=True)
    print(f"  Per file: {elapsed/len(files):.1f}s", flush=True)
    print(f"  Realtime: {total_audio/elapsed:.0f}x", flush=True)
    if chars:
        print(f"  Transcript chars: min={min(chars)} avg={sum(chars)//len(chars)} max={max(chars)}", flush=True)
    return total_audio, elapsed


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "tp8"
    batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 16

    all_files = get_files_by_duration()
    print(f"Queue: {len(all_files)} files", flush=True)

    from qwen_asr import Qwen3ASRModel

    if mode == "tp8":
        # Tensor parallel across 8 GPUs
        max_tok = int(TOKENS_PER_MIN * 60)  # 60 min max -> ~22800
        print(f"\nLoading TP=8, max_tokens={max_tok}, batch={batch_size}", flush=True)
        t0 = time.time()
        model = Qwen3ASRModel.LLM(
            model="Qwen/Qwen3-ASR-1.7B",
            gpu_memory_utilization=0.85,
            max_inference_batch_size=batch_size,
            max_new_tokens=max_tok,
            tensor_parallel_size=8,
        )
        print(f"Model loaded in {time.time()-t0:.1f}s", flush=True)

        total_audio_all = 0
        total_time_all = 0

        # Test each duration bucket
        for lo, hi, label in [(300,900,"5-15m"), (900,1800,"15-30m"), (1800,3600,"30-60m")]:
            batch = make_uniform_batch(all_files, (lo, hi), batch_size)
            if len(batch) < 4:
                print(f"\nSkipping {label}: only {len(batch)} files", flush=True)
                continue
            ta, te = run_test(model, batch, f"TP=8 | {label} | batch={len(batch)}")
            total_audio_all += ta
            total_time_all += te

        if total_time_all > 0:
            print(f"\n{'='*60}", flush=True)
            print(f"TP=8 AGGREGATE: {total_audio_all/60:.0f} min in {total_time_all:.0f}s "
                  f"= {total_audio_all/total_time_all:.0f}x realtime "
                  f"= {total_audio_all/total_time_all*3600/3600:.0f} audio hrs/hr", flush=True)

    elif mode == "dp1":
        # Single GPU — multiply by 8 for projected DP=8
        max_tok = int(TOKENS_PER_MIN * 60)
        print(f"\nLoading DP=1 (GPU 0), max_tokens={max_tok}, batch={batch_size}", flush=True)
        t0 = time.time()
        model = Qwen3ASRModel.LLM(
            model="Qwen/Qwen3-ASR-1.7B",
            gpu_memory_utilization=0.85,
            max_inference_batch_size=batch_size,
            max_new_tokens=max_tok,
        )
        print(f"Model loaded in {time.time()-t0:.1f}s", flush=True)

        total_audio_all = 0
        total_time_all = 0

        for lo, hi, label in [(300,900,"5-15m"), (900,1800,"15-30m"), (1800,3600,"30-60m")]:
            batch = make_uniform_batch(all_files, (lo, hi), batch_size)
            if len(batch) < 4:
                print(f"\nSkipping {label}: only {len(batch)} files", flush=True)
                continue
            ta, te = run_test(model, batch, f"DP=1 | {label} | batch={len(batch)}")
            total_audio_all += ta
            total_time_all += te

        if total_time_all > 0:
            print(f"\n{'='*60}", flush=True)
            print(f"DP=1 AGGREGATE: {total_audio_all/60:.0f} min in {total_time_all:.0f}s "
                  f"= {total_audio_all/total_time_all:.0f}x realtime", flush=True)
            print(f"DP=8 PROJECTED: {total_audio_all/total_time_all*8:.0f}x realtime "
                  f"= {total_audio_all/total_time_all*8*3600/3600:.0f} audio hrs/hr", flush=True)
