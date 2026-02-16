#!/usr/bin/env python3
"""
Massive Content Aggregation System
Continuously aggregates discovered videos and feeds transcription pipeline
Target: 1M hours of educational content
"""

import json
import glob
import sqlite3
import time
import os
from pathlib import Path

class MassiveContentAggregator:
    def __init__(self):
        self.base_dir = Path('.')
        self.db_path = 'massive_production.db'
        self.discovery_files = []
        self.total_discovered = 0
        self.total_hours_estimated = 0
        
    def scan_discovery_files(self):
        """Scan for new discovery JSON files"""
        pattern = str(self.base_dir / '*_videos.json')
        playlist_pattern = str(self.base_dir / 'playlist_*.json')
        
        files = glob.glob(pattern) + glob.glob(playlist_pattern)
        new_files = [f for f in files if f not in self.discovery_files]
        
        if new_files:
            print(f"📁 Found {len(new_files)} new discovery files")
            self.discovery_files.extend(new_files)
            
        return new_files
    
    def process_discovery_file(self, file_path):
        """Process a single discovery JSON file"""
        if not os.path.exists(file_path):
            return 0
            
        processed = 0
        
        try:
            with open(file_path, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    if not line.strip():
                        continue
                        
                    try:
                        video_data = json.loads(line.strip())
                        
                        if self.add_video_to_database(video_data, file_path):
                            processed += 1
                            
                        # Progress update every 1000 videos
                        if processed % 1000 == 0:
                            print(f"  📊 {file_path}: {processed} videos processed...")
                            
                    except json.JSONDecodeError:
                        continue
                        
        except Exception as e:
            print(f"❌ Error processing {file_path}: {e}")
            
        return processed
    
    def add_video_to_database(self, video_data, source_file):
        """Add video to transcription database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            video_id = video_data.get('id')
            if not video_id:
                conn.close()
                return False
                
            title = video_data.get('title', 'Unknown')[:200]  # Truncate long titles
            duration = video_data.get('duration', 0)
            
            # Skip very short videos (< 2 minutes)
            if duration and duration < 120:
                conn.close()
                return False
            
            # Estimate duration if missing (average 15 minutes for educational content)
            if not duration:
                duration = 900  # 15 minutes estimate
            
            # Prioritize longer educational content
            priority = 3 if duration > 3600 else 4 if duration > 1800 else 5
            
            # Insert if not exists
            cursor.execute('''
                INSERT OR IGNORE INTO videos 
                (video_id, title, duration_seconds, status, priority)
                VALUES (?, ?, ?, 'pending', ?)
            ''', (video_id, title, duration, priority))
            
            inserted = cursor.rowcount > 0
            conn.commit()
            conn.close()
            
            if inserted:
                self.total_discovered += 1
                self.total_hours_estimated += duration / 3600
                
            return inserted
            
        except Exception as e:
            print(f"❌ Database error: {e}")
            return False
    
    def get_aggregation_stats(self):
        """Get current aggregation statistics"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('SELECT COUNT(*), SUM(duration_seconds)/3600.0 FROM videos WHERE status="pending"')
            pending_count, pending_hours = cursor.fetchone()
            
            cursor.execute('SELECT COUNT(*) FROM videos WHERE status="completed"')  
            completed_count = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM videos')
            total_count = cursor.fetchone()[0]
            
            conn.close()
            
            return {
                'total_videos': total_count,
                'pending_videos': pending_count or 0,
                'completed_videos': completed_count or 0,
                'pending_hours': pending_hours or 0,
                'discovery_files': len(self.discovery_files)
            }
            
        except Exception as e:
            print(f"❌ Stats error: {e}")
            return {}
    
    def continuous_aggregation(self):
        """Main continuous aggregation loop"""
        print("🚀 MASSIVE CONTENT AGGREGATION STARTED")
        print("="*60)
        
        while True:
            try:
                # Scan for new discovery files
                new_files = self.scan_discovery_files()
                
                # Process new discoveries
                for file_path in new_files:
                    print(f"\n📁 Processing: {os.path.basename(file_path)}")
                    processed = self.process_discovery_file(file_path)
                    print(f"✅ Added {processed} videos from {os.path.basename(file_path)}")
                
                # Show current stats
                stats = self.get_aggregation_stats()
                
                progress_toward_million = (stats.get('pending_hours', 0) / 1_000_000) * 100
                
                print(f"\n📊 AGGREGATION STATUS:")
                print(f"  🎥 Total Videos: {stats.get('total_videos', 0):,}")
                print(f"  ⏳ Pending: {stats.get('pending_videos', 0):,}")
                print(f"  ✅ Completed: {stats.get('completed_videos', 0):,}")
                print(f"  ⏰ Pending Hours: {stats.get('pending_hours', 0):,.0f}")
                print(f"  🎯 Progress to 1M: {progress_toward_million:.1f}%")
                print(f"  📁 Discovery Files: {stats.get('discovery_files', 0)}")
                
                # Check if we've reached massive scale
                if stats.get('pending_hours', 0) >= 1_000_000:
                    print("\n🎉 MILESTONE: 1 MILLION HOURS DISCOVERED!")
                    print("Ready for massive transcription!")
                elif stats.get('pending_hours', 0) >= 500_000:
                    print("\n🔥 MAJOR MILESTONE: 500K hours discovered!")
                elif stats.get('pending_hours', 0) >= 100_000:
                    print("\n⚡ Significant scale: 100K+ hours ready!")
                
                # Wait before next scan
                time.sleep(30)  # Check every 30 seconds
                
            except KeyboardInterrupt:
                print("\n👋 Aggregation stopped by user")
                break
            except Exception as e:
                print(f"❌ Aggregation error: {e}")
                time.sleep(60)

if __name__ == "__main__":
    aggregator = MassiveContentAggregator()
    aggregator.continuous_aggregation()