#!/usr/bin/env python3
"""
Post-processing pipeline for Whisper transcriptions.
Applies ITN, punctuation restoration, and truecasing using Phi-3-mini via vLLM.

Usage:
    CUDA_VISIBLE_DEVICES=2 python postprocess.py [--batch-size 10] [--max-chunk 1500] [--limit 100]

NOTE: Research showed Whisper already produces well-punctuated, cased output.
Post-processing improvement is marginal. See POSTPROCESSING_RESEARCH.md.
"""
import os
import sys
import time
import sqlite3
import argparse
import logging

os.environ.setdefault('CUDA_VISIBLE_DEVICES', '2')

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = os.path.expanduser('~/academic_transcriptions/massive_production.db')

SYSTEM_PROMPT = """Fix this transcript: convert spoken numbers to written form (e.g. "five hundred" → "500", "twenty twenty six" → "2026", "three point one four" → "3.14", "january first" → "January 1st"). Fix any punctuation or capitalization errors. Output ONLY the corrected text, nothing else."""


def ensure_column(db_path):
    """Add postprocessed_transcript column if it doesn't exist."""
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    try:
        cur.execute("ALTER TABLE videos ADD COLUMN postprocessed_transcript TEXT")
        con.commit()
        logger.info("Added postprocessed_transcript column")
    except sqlite3.OperationalError:
        pass  # Column already exists
    con.close()


def get_pending(db_path, limit=None):
    """Get transcripts that haven't been post-processed yet."""
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    q = """SELECT id, transcript FROM videos 
           WHERE transcript IS NOT NULL AND transcript != '' 
           AND postprocessed_transcript IS NULL
           ORDER BY id"""
    if limit:
        q += f" LIMIT {limit}"
    cur.execute(q)
    rows = cur.fetchall()
    con.close()
    return rows


def chunk_text(text, max_chunk=1500):
    """Split text into chunks at sentence boundaries."""
    chunks = []
    while len(text) > max_chunk:
        # Find last sentence boundary before max_chunk
        cut = max_chunk
        for sep in ['. ', '? ', '! ', '\n']:
            pos = text.rfind(sep, 0, max_chunk)
            if pos > max_chunk // 2:
                cut = pos + len(sep)
                break
        chunks.append(text[:cut])
        text = text[cut:]
    if text:
        chunks.append(text)
    return chunks


def process_batch(llm, tokenizer, texts, max_chunk=1500):
    """Process a batch of transcripts through the LLM."""
    from vllm import SamplingParams
    
    # Chunk all texts and track mapping
    all_prompts = []
    text_chunk_map = []  # (text_idx, chunk_idx)
    
    for i, text in enumerate(texts):
        chunks = chunk_text(text, max_chunk)
        for j, chunk in enumerate(chunks):
            messages = [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": chunk}
            ]
            prompt = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
            all_prompts.append(prompt)
            text_chunk_map.append((i, j))
    
    params = SamplingParams(max_tokens=max_chunk + 500, temperature=0)
    outputs = llm.generate(all_prompts, params)
    
    # Reassemble
    results = [""] * len(texts)
    for idx, output in enumerate(outputs):
        text_idx, chunk_idx = text_chunk_map[idx]
        result_text = output.outputs[0].text.strip()
        if chunk_idx > 0:
            results[text_idx] += " "
        results[text_idx] += result_text
    
    return results


def save_results(db_path, id_result_pairs):
    """Save post-processed transcripts to DB."""
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    for vid_id, result in id_result_pairs:
        cur.execute("UPDATE videos SET postprocessed_transcript = ? WHERE id = ?", (result, vid_id))
    con.commit()
    con.close()


def main():
    parser = argparse.ArgumentParser(description='Post-process Whisper transcriptions')
    parser.add_argument('--batch-size', type=int, default=10, help='Transcripts per batch')
    parser.add_argument('--max-chunk', type=int, default=1500, help='Max chars per LLM chunk')
    parser.add_argument('--limit', type=int, default=None, help='Max transcripts to process')
    parser.add_argument('--db', type=str, default=DB_PATH, help='Database path')
    parser.add_argument('--gpu-util', type=float, default=0.6, help='GPU memory utilization')
    parser.add_argument('--dry-run', action='store_true', help='Process but do not save')
    args = parser.parse_args()

    ensure_column(args.db)
    pending = get_pending(args.db, args.limit)
    
    if not pending:
        logger.info("No pending transcripts to process")
        return
    
    logger.info(f"Found {len(pending)} pending transcripts")

    from vllm import LLM
    from transformers import AutoTokenizer

    logger.info("Loading Phi-3-mini model...")
    llm = LLM(model="microsoft/Phi-3-mini-4k-instruct", 
              gpu_memory_utilization=args.gpu_util, 
              max_model_len=4096)
    tokenizer = AutoTokenizer.from_pretrained("microsoft/Phi-3-mini-4k-instruct")
    logger.info("Model loaded")

    total = len(pending)
    processed = 0
    start_time = time.time()

    for batch_start in range(0, total, args.batch_size):
        batch = pending[batch_start:batch_start + args.batch_size]
        ids = [r[0] for r in batch]
        texts = [r[1] for r in batch]

        try:
            results = process_batch(llm, tokenizer, texts, args.max_chunk)
            
            if not args.dry_run:
                save_results(args.db, list(zip(ids, results)))
            
            processed += len(batch)
            elapsed = time.time() - start_time
            rate = processed / elapsed * 3600
            eta = (total - processed) / (processed / elapsed) if processed > 0 else 0
            
            logger.info(
                f"Progress: {processed}/{total} ({processed/total*100:.1f}%) | "
                f"Rate: {rate:.0f}/hr | ETA: {eta/60:.1f}min"
            )

            if args.dry_run and processed >= args.batch_size:
                # Show sample in dry-run
                for i in range(min(2, len(results))):
                    logger.info(f"\n  BEFORE: {texts[i][:200]}")
                    logger.info(f"  AFTER:  {results[i][:200]}")
                break

        except Exception as e:
            logger.error(f"Batch error at {batch_start}: {e}")
            continue

    elapsed = time.time() - start_time
    logger.info(f"Done: {processed} transcripts in {elapsed:.0f}s ({processed/elapsed*3600:.0f}/hr)")


if __name__ == '__main__':
    main()
