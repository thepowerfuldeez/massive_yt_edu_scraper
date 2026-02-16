#!/bin/bash
# Launch all discovery scripts, skip any already running
# Cache files go to ~/academic_transcriptions (data dir, not in repo)
REPO=~/million-hour-transcription
DATA=~/academic_transcriptions

for script in discover_related discover_10M scale_to_1M discover_aggressive discover_mega; do
    if pgrep -f "$script" > /dev/null 2>&1; then
        echo "[ok] $script already running"
    else
        echo "[start] $script"
        cd "$DATA"
        nohup python3 -u "$REPO/src/${script}.py" > /tmp/${script}.log 2>&1 &
    fi
done

sleep 2
echo ""
echo "Running discovery scripts:"
ps aux | grep -E "discover_|scale_to" | grep python | grep -v grep | awk '{print "  " $12, $13}'
