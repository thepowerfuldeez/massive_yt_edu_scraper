#!/usr/bin/env python3
"""Discover videos from CC channels using YouTube Data API (not yt-dlp).

Uses channels.list + playlistItems.list to enumerate all videos from
known CC channels (from common-pile/youtube). Not affected by yt-dlp rate limits.

YouTube Data API quota: 10K units/day.
- channels.list: 1 unit per call
- playlistItems.list: 1 unit per call (50 items per page)
- search.list: 100 units per call (expensive, use sparingly)

Strategy: for each CC channel_id, get uploads playlist via channels.list,
then paginate playlistItems to get all video IDs.
"""
import sqlite3, os, sys, time, json, urllib.request, urllib.error, re, random

DB = os.path.expanduser("~/academic_transcriptions/massive_production.db")
API_KEY = os.environ.get("YT_API_KEY", "")
MIN_DURATION = 300

REJECT_PATTERNS = re.compile(
    r'\b(music video|official video|lyric|trailer|reaction|unboxing|prank|asmr|mukbang|'
    r'tiktok|shorts|#shorts|haul|vlog|grwm|day in my life|'
    r'fortnite|minecraft gameplay|roblox|gta v|gaming|let.s play|walkthrough|playthrough|'
    r'full movie|full episode|movie clip|behind the scenes|bloopers|'
    r'live stream|livestream|24 hour challenge|'
    r'compilation|funny moments|try not to laugh|satisfying|oddly satisfying|'
    r'sermon|prayer|worship|bible study|quran|gospel|church service|'
    r'conspiracy|flat earth|anti.vax|miracle cure)\b',
    re.IGNORECASE
)

EDU_BOOST = re.compile(
    r'\b(lecture|course|tutorial|class|seminar|workshop|bootcamp|masterclass|'
    r'university|professor|MIT|Stanford|Harvard|Yale|Berkeley|Oxford|Cambridge|'
    r'OpenCourseWare|NPTEL|khan academy|Coursera|edX|'
    r'introduction to|fundamentals|chapter \d|lesson \d|week \d|part \d|module \d)\b',
    re.IGNORECASE
)

EXPLORED_FILE = os.path.expanduser("~/academic_transcriptions/cc_api_explored.txt")


def get_db():
    conn = sqlite3.connect(DB, timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=120000")
    return conn


def api_get(url):
    """Make a YouTube Data API GET request."""
    try:
        resp = urllib.request.urlopen(urllib.request.Request(url), timeout=30)
        return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode() if hasattr(e, "read") else str(e)
        if "quotaExceeded" in body:
            print("QUOTA EXCEEDED — stopping for today.", flush=True)
            sys.exit(0)
        print(f"API error: {e.code} {body[:200]}", flush=True)
        return None
    except Exception as e:
        print(f"Request error: {e}", flush=True)
        return None


def get_uploads_playlist(channel_id):
    """Get the uploads playlist ID for a channel (costs 1 unit)."""
    url = (f"https://www.googleapis.com/youtube/v3/channels"
           f"?part=contentDetails,snippet&id={channel_id}&key={API_KEY}")
    data = api_get(url)
    if not data or not data.get("items"):
        return None, None
    item = data["items"][0]
    uploads = item.get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads")
    name = item.get("snippet", {}).get("title", "")
    return uploads, name


def list_playlist_videos(playlist_id, max_pages=20):
    """Paginate through a playlist to get all video IDs (1 unit per page, 50 per page)."""
    videos = []
    page_token = None
    for _ in range(max_pages):
        url = (f"https://www.googleapis.com/youtube/v3/playlistItems"
               f"?part=snippet&playlistId={playlist_id}"
               f"&maxResults=50&key={API_KEY}")
        if page_token:
            url += f"&pageToken={page_token}"

        data = api_get(url)
        if not data:
            break

        for item in data.get("items", []):
            snip = item.get("snippet", {})
            vid = snip.get("resourceId", {}).get("videoId", "")
            if vid:
                videos.append({
                    "id": vid,
                    "title": snip.get("title", ""),
                    "channel_id": snip.get("channelId", ""),
                    "published": snip.get("publishedAt", ""),
                })

        page_token = data.get("nextPageToken")
        if not page_token:
            break
        time.sleep(0.2)

    return videos


def insert_videos(videos, source="cc_api"):
    """Insert new videos, upgrade existing to green."""
    if not videos:
        return 0
    conn = get_db()
    n = 0
    for v in videos:
        title = v.get("title", "")
        vid = v.get("id", "")
        if not vid:
            continue
        if REJECT_PATTERNS.search(title):
            continue
        pri = 8 if EDU_BOOST.search(title) else 6
        ch_id = v.get("channel_id", "")
        published = v.get("published", "")
        try:
            conn.execute(
                "INSERT INTO videos "
                "(video_id, title, url, status, priority, license_risk, "
                "channel_id, published_time) "
                "VALUES (?, ?, ?, 'pending', ?, 'green', ?, ?)",
                (vid, title, f"https://youtube.com/watch?v={vid}",
                 pri, ch_id, published))
            n += 1
        except sqlite3.IntegrityError:
            # Already exists — upgrade to green
            conn.execute(
                "UPDATE videos SET license_risk='green' "
                "WHERE video_id=? AND license_risk != 'green'",
                (vid,))
    conn.commit()
    conn.close()
    return n


def load_explored():
    try:
        with open(EXPLORED_FILE) as f:
            return set(f.read().split())
    except FileNotFoundError:
        return set()


def save_explored(explored):
    with open(EXPLORED_FILE, "w") as f:
        f.write("\n".join(explored))


def get_cc_channel_ids():
    """Get unique channel_ids from CC-licensed videos, ordered by count."""
    conn = get_db()
    rows = conn.execute("""
        SELECT channel_id, COUNT(*) as cnt
        FROM videos
        WHERE youtube_license='creativeCommon'
          AND channel_id IS NOT NULL AND channel_id != ''
        GROUP BY channel_id
        ORDER BY cnt DESC
    """).fetchall()
    conn.close()
    return [(r[0], r[1]) for r in rows]


def main():
    print("=" * 60)
    print("CC Channel Discovery via YouTube Data API")
    print("=" * 60)

    cc_channels = get_cc_channel_ids()
    explored = load_explored()
    todo = [(ch, cnt) for ch, cnt in cc_channels if ch not in explored]

    print(f"Total CC channels: {len(cc_channels)}")
    print(f"Already explored: {len(explored)}")
    print(f"Remaining: {len(todo)}")
    print(f"API quota: ~{len(todo) * 22} units estimated (1 channel.list + ~20 playlistItems pages each)")
    print()

    total_new = 0
    total_upgraded = 0
    api_calls = 0

    for i, (ch_id, known_cc) in enumerate(todo):
        # Get uploads playlist
        uploads_id, ch_name = get_uploads_playlist(ch_id)
        api_calls += 1

        if not uploads_id:
            explored.add(ch_id)
            print(f"  [{i+1}/{len(todo)}] {ch_id} — no uploads playlist")
            continue

        # List all videos
        videos = list_playlist_videos(uploads_id)
        pages = (len(videos) + 49) // 50
        api_calls += max(pages, 1)

        # Insert
        n = insert_videos(videos, source=f"cc_api:{ch_name or ch_id}")
        total_new += n
        explored.add(ch_id)

        name_str = (ch_name or ch_id)[:40]
        print(f"  [{i+1}/{len(todo)}] {name_str:40s} "
              f"known_cc: {known_cc:>5} | found: {len(videos):>5} | +{n:>4} new | "
              f"api_calls: {api_calls}")

        # Save progress every 20 channels
        if (i + 1) % 20 == 0:
            save_explored(explored)
            conn = get_db()
            green = conn.execute("SELECT COUNT(*) FROM videos WHERE license_risk='green'").fetchone()[0]
            total = conn.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
            conn.close()
            print(f"\n  --- {i+1} channels done | +{total_new:,} new | "
                  f"GREEN: {green:,} | Total: {total:,} | API calls: {api_calls} ---\n")

        # Brief delay to be nice to the API
        time.sleep(0.3)

    save_explored(explored)

    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
    green = conn.execute("SELECT COUNT(*) FROM videos WHERE license_risk='green'").fetchone()[0]
    conn.close()

    print(f"\n{'='*60}")
    print(f"DONE — {len(todo)} channels crawled")
    print(f"  New videos: {total_new:,}")
    print(f"  API calls: {api_calls}")
    print(f"  Total in DB: {total:,}")
    print(f"  GREEN: {green:,}")


if __name__ == "__main__":
    main()
