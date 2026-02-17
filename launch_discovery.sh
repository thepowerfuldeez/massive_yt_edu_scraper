#!/bin/bash
# Launch discovery crawlers
cd ~/million-hour-transcription
source ~/vllm-env/bin/activate

# Kill old discovery processes
pkill -f "discover_channels_10M.py" 2>/dev/null
pkill -f "discover_related.py" 2>/dev/null
sleep 1

# Channel crawler (the main discovery engine for 10M target)
nohup python3 -u src/discover_channels_10M.py > /tmp/discover_channels_1.log 2>&1 &
echo "[discovery] Channel crawler started (PID: $!)"

# Related video crawler (exponential discovery from existing videos)
nohup python3 -u src/discover_related.py > /tmp/discover_related.log 2>&1 &
echo "[discovery] Related crawler started (PID: $!)"

echo "[discovery] All crawlers running"
