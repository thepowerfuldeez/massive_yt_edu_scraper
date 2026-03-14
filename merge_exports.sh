#!/bin/bash
# Merge parquet exports from all shards + master into a single directory.
# Run from master node (all exports are on shared /persistent).
#
# Usage: ./merge_exports.sh [output_dir]

cd "$(dirname "$0")"
PYTHON=./venv/bin/python3
OUTPUT="${1:-exports/merged}"

mkdir -p "$OUTPUT"

echo "=== Merging exports from all shards ==="

# Master exports
if [ -d "exports" ] && ls exports/*.parquet > /dev/null 2>&1; then
    echo "  Master: $(ls exports/*.parquet 2>/dev/null | wc -l) files"
    cp exports/*.parquet "$OUTPUT/" 2>/dev/null
fi

# Shard exports
for d in exports/shard_*; do
    [ -d "$d" ] || continue
    n=$(ls "$d"/*.parquet 2>/dev/null | wc -l)
    shard=$(basename "$d")
    echo "  ${shard}: ${n} files"
    # Rename to avoid collisions: train-00000.parquet -> shard_01-train-00000.parquet
    for f in "$d"/*.parquet; do
        [ -f "$f" ] || continue
        base=$(basename "$f")
        cp "$f" "$OUTPUT/${shard}-${base}"
    done
done

# Also merge completed transcriptions from shard DBs back into master
echo ""
echo "=== Merging shard DBs into master ==="
for i in $(seq 0 6); do
    db="/persistent/worker_${i}/massive_production.db"
    if [ -f "$db" ]; then
        completed=$($PYTHON -c "
import sqlite3
db = sqlite3.connect('$db', timeout=30)
n = db.execute('SELECT COUNT(*) FROM videos WHERE status=\"completed\"').fetchone()[0]
print(n)
" 2>/dev/null)
        echo "  Shard $i: $completed completed"
        $PYTHON src/s3_sync.py merge --remote-db "$db" 2>&1 | grep -v "^$"
    fi
done

echo ""
echo "=== Summary ==="
total=$(ls "$OUTPUT"/*.parquet 2>/dev/null | wc -l)
size=$(du -sh "$OUTPUT" 2>/dev/null | cut -f1)
echo "  Output: $OUTPUT/ ($total parquet files, $size)"
echo "  Run master export to include merged data:"
echo "    $PYTHON src/export_parquet.py"
