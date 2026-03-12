#!/bin/bash
# Launch discovery: CC channel crawl (yt-dlp) + CC API discovery (YouTube Data API)
cd /persistent/poolside/oss/massive_yt_edu_scraper
source venv/bin/activate

pkill -f "discover_cc.py" 2>/dev/null
pkill -f "discover_cc_api.py" 2>/dev/null
sleep 1

nohup python3 -u src/discover_cc.py > /tmp/discover_cc.log 2>&1 &
echo "[discovery] CC crawl started (PID: $!)"

nohup python3 -u src/discover_cc_api.py > /tmp/discover_cc_api.log 2>&1 &
echo "[discovery] CC API discovery started (PID: $!)"

echo "[discovery] Check /tmp/discover_cc.log and /tmp/discover_cc_api.log"
