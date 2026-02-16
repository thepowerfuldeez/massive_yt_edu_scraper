#!/usr/bin/env python3
"""Real-time progress monitor."""
import sqlite3, time, os

DB = os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "data.db"))

def monitor():
    while True:
        try:
            c = sqlite3.connect(DB, timeout=10)
            stats = dict(c.execute("SELECT status, count(*) FROM videos GROUP BY status").fetchall())
            total_hours = c.execute("SELECT coalesce(sum(duration_seconds)/3600.0, 0) FROM videos WHERE status='completed'").fetchone()[0]
            avg_speed = c.execute("SELECT coalesce(avg(speed_ratio), 0) FROM videos WHERE status='completed' AND speed_ratio > 0").fetchone()[0]
            recent = c.execute("SELECT avg(speed_ratio) FROM (SELECT speed_ratio FROM videos WHERE status='completed' AND speed_ratio > 0 ORDER BY completed_at DESC LIMIT 20)").fetchone()[0] or 0
            
            # Estimate tokens (avg ~12K tokens per hour of speech)
            est_tokens = total_hours * 12000
            
            completed = stats.get('completed', 0)
            pending = stats.get('pending', 0)
            errors = stats.get('error', 0)
            
            print(f"\r[{time.strftime('%H:%M:%S')}] "
                  f"Done: {completed} | Pending: {pending} | Errors: {errors} | "
                  f"Hours: {total_hours:.1f} | ~{est_tokens/1e6:.1f}M tokens | "
                  f"Speed: {recent:.0f}x recent, {avg_speed:.0f}x avg", end="", flush=True)
            c.close()
        except:
            pass
        time.sleep(5)

if __name__ == "__main__":
    monitor()
