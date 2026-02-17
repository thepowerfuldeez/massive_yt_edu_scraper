# Bootstrap Discovery Scripts

These search-based discovery scripts are useful for **bootstrapping the initial queue** (first ~100K-1M videos). Once you have a substantial seed, the main discovery engines (`discover_channels_10M.py` and `discover_related.py`) are far more effective at scaling.

## Scripts

| Script | Strategy | Best For |
|--------|----------|----------|
| `scale_to_1M.py` | Combinatorial search queries (subject × language × modifier) | Initial seed building |
| `discover_10M.py` | Related video chaining + playlist expansion + channel crawling | Early scaling (10K→1M) |
| `discover_aggressive.py` | Mega channel crawls + bulk trending search | Fast queue growth |
| `discover_mega.py` | Non-English content, conferences, niche academic fields | Long tail discovery |
| `aggregate_discovery.py` | Merge external video ID lists into DB | One-time imports |

## Usage

Run from the repo root with the vllm-env activated:

```bash
source ~/vllm-env/bin/activate
python3 scripts/bootstrap/scale_to_1M.py
```

These scripts write directly to the same SQLite DB used by the main pipeline. They can run alongside the main workers safely (WAL mode + UNIQUE constraints handle dedup).

## When to Use

- **Fresh start**: Run 2-3 of these for a few hours to build a 100K+ seed queue
- **Then switch**: Kill these and run `src/discover_channels_10M.py` + `src/discover_related.py` for 10M+ scale
