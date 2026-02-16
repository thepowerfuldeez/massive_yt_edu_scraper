#!/bin/bash
# Launch all discovery scripts, skip any already running
cd /home/george/academic_transcriptions

for script in discover_related.py discover_10M.py scale_to_1M.py discover_aggressive.py discover_mega.py; do
    name=$(basename $script .py)
    if pgrep -f "$script" > /dev/null 2>&1; then
        echo "[ok] $name already running"
    else
        echo "[start] $name"
        nohup python3 -u /home/george/academic_transcriptions/$script > /tmp/${name}.log 2>&1 &
    fi
done

sleep 2
echo ""
echo "Running discovery scripts:"
ps aux | grep -E "discover_|scale_to" | grep python | grep -v grep | awk '{print "  " $12, $13}'
