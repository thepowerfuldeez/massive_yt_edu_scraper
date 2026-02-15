# Progress Log

## 2026-02-15

### 14:00 — System Online
- 4 GPU workers running with pipelined download (prefetch depth 3)
- distil-whisper/distil-large-v3, chunk_length_s=30, batch_size 24/16
- Average speed: 160x realtime per GPU

### 14:30 — Discovery Scaling
- Initial queue: 52K videos
- Two discovery scripts running in parallel
- Queue grew to 172K+ within 30 minutes

### 15:00 — Quality Filter Added
- Added ≥15min duration filter (filters shorts, clips, trailers)
- Negative title filter (rejects music videos, gaming, ASMR, pranks, etc.)
- Positive priority boost for educational keywords (lecture, course, university, etc.)
- Deprioritized 87K short videos, 84K quality educational videos remain
- Workers now prioritize ≥15min content

### 15:30 — Status
- **2,171 videos transcribed**
- **~84K educational videos in queue (≥15min)**
- **86K+ pending hours of audio**
- Discovery scaling to 1M+ with quality filters
- GitHub repo live: thepowerfuldeez/massive_yt_edu_scraper

### Quality Strategy
To reach 10B tokens of *educational* content:
1. **Duration filter**: ≥15 minutes only (deep lectures, not clips)
2. **Title filtering**: Reject non-educational (gaming, music, vlogs, ASMR)
3. **Priority boosting**: Lectures, courses, university content get processed first
4. **Source curation**: 140+ verified educational channels, known MOOC playlists
5. **Multi-language**: 15+ languages for global educational coverage
