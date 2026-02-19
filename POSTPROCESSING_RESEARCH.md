# Whisper Transcription Post-Processing Research

**Date**: 2026-02-19  
**Hardware**: RTX 5090 (GPU 2, 32GB VRAM)  
**Dataset**: 53,992 completed transcripts in `massive_production.db`

## Executive Summary

**Recommendation: Post-processing has very low ROI for this dataset.** Whisper large-v3 already produces well-punctuated, properly cased text. The main theoretical benefit (Inverse Text Normalization) has no production-grade fast solution. The LLM approach works but barely changes the output, suggesting Whisper's output is already good enough.

If post-processing is still desired, use **Phi-3-mini via vLLM** for a unified approach at ~8,800 transcripts/hour.

## Key Finding: Whisper Already Handles Punctuation & Casing

Sample Whisper outputs from the database:

> "Hello, I wanted to welcome you all here. I'm David Kaplan. I'm a professor in the Department of Physics and director of the Institute for Nuclear Theory."

> "Good morning students. Welcome to week three of human resource management. This week we're looking at chapter six and chapter seven."

Whisper large-v3 natively produces:
- ✅ Proper punctuation (periods, commas, question marks)
- ✅ Sentence-initial capitalization
- ✅ Proper noun capitalization (mostly)
- ❌ Number/currency normalization (writes "five hundred" not "$500")

## Approaches Evaluated

### 1. NeMo Text Processing (WFST-based ITN)
- **Library**: `nemo_text_processing` v1.1.0
- **Approach**: Weighted Finite-State Transducer for inverse text normalization
- **Result**: ❌ **UNUSABLE for batch processing**
  - CPU-only, no GPU acceleration
  - Hits Python recursion depth errors on transcripts >5K chars
  - Requires splitting text into sentences first (additional complexity)
  - Warning: "Your input is too long and could take a long time to normalize"
  - Would need `split_text_into_sentences()` preprocessing, dramatically slowing throughput
- **Quality**: Good for short strings (e.g., "five hundred dollars" → "$500")
- **Verdict**: Not viable. Production-grade for short utterances (ASR pipelines), not for bulk transcript processing.

### 2. deepmultilingualpunctuation (oliverguhr)
- **Model**: `oliverguhr/fullstop-punctuation-multilang-large` (~1.2GB)
- **Approach**: Token classification (BERT-based) for punctuation restoration
- **Speed**: ~36,000 chars/sec, ~44,000 transcripts/hour (3K char chunks)
- **VRAM**: ~2.7 GB
- **Result**: ⚠️ **Fast but unnecessary** — Whisper already punctuates well
  - Only adds punctuation, no ITN or truecasing
  - Would need to strip existing punctuation first, then re-add (destructive)
  - Test output: "hello, how are you doing today? i am fine and you?"
  - Does NOT handle ITN at all
- **Verdict**: Redundant for Whisper output. Would only be useful for ASR systems that output lowercase unpunctuated text.

### 3. Phi-3-mini-4k-instruct via vLLM (LLM approach)
- **Model**: Microsoft Phi-3-mini (3.8B params, BF16)
- **Approach**: Instruction-following LLM doing all 3 tasks
- **Speed**: ~880 tokens/sec, ~8,800 transcripts/hour (1.5K char chunks)
- **VRAM**: ~22 GB
- **Result**: ⚠️ **Works but barely changes anything**
  - Output is nearly identical to input for most transcripts
  - Occasionally fixes minor punctuation (adds hyphens, commas)
  - Some ITN but inconsistent
  - Risk of hallucination/text corruption on batch processing
- **Quality samples** (before → after differences minimal):
  - Added a comma: "My name is Hadoff and Hussles and today" → "My name is Hadoff and Hussles, and today"
  - Minor formatting: "actor critics" → "actor-critic methods"
- **Verdict**: Marginal improvement. Risk of introducing errors may outweigh benefits.

### 4. Not Tested (assessed theoretically)

| Approach | Why Not Tested | Assessment |
|----------|---------------|------------|
| recasepunc (Silero) | Not pip-installable, needs manual model download | Redundant — Whisper already cases |
| CTranslate2 models | No pre-built ITN models available | Would need custom training |
| Fine-tuned T5/BART | No pre-trained models for all 3 tasks | Would need training data + compute |
| Qwen2.5-3B | Not cached, slow download on this network | Similar to Phi-3 approach |
| Nemotron-Nano-30B-A3B | Cached but too large for ITN-only task | Overkill |
| ONNX-optimized models | No off-the-shelf ITN ONNX models | Would need conversion |

## Throughput Analysis

| Approach | Speed | Transcripts/hr | VRAM | Quality |
|----------|-------|----------------|------|---------|
| NeMo WFST ITN | ~unusable | N/A | 0 (CPU) | Good ITN, crashes on long text |
| deepmultilingualpunctuation | 36K chars/s | ~44,000 | 2.7 GB | Redundant (Whisper already punctuates) |
| Phi-3-mini (vLLM) | 880 tok/s | ~8,800 | 22 GB | Marginal improvement |
| **Do nothing** | ∞ | ∞ | 0 | Whisper output is already good |

**Target**: 10K+ transcripts/hour → Only "do nothing" and deepmultilingualpunctuation meet this. Phi-3 is close but uses significant GPU resources for minimal gain.

## Cost-Benefit Analysis

**Processing 53K existing transcripts with Phi-3:**
- Time: ~6 hours on GPU 2
- VRAM: 22 GB (leaves 10 GB headroom)
- Risk: LLM may occasionally corrupt text
- Benefit: Minor punctuation fixes, inconsistent ITN

**Processing 13K/day new transcripts:**
- Time: ~1.5 hours/day
- This is feasible but uses GPU 2 for ~6% of the day

**The fundamental problem**: Whisper's output is already quite good. The improvements from post-processing are marginal and inconsistent. The main gap (ITN for spoken numbers) has no fast, reliable solution.

## Recommendation

### Short-term: Do nothing
Whisper large-v3 output is production-quality for most use cases. The transcripts in the database already have proper punctuation, capitalization, and are highly readable.

### If ITN is specifically needed:
1. Use **regex-based ITN** for common patterns (dates, currency, percentages) — fast, deterministic, no GPU
2. For comprehensive ITN, wait for NeMo to add GPU acceleration or sentence-level batching
3. Consider a dedicated fine-tuned model specifically for ITN on academic transcripts

### If full post-processing is eventually desired:
Deploy Phi-3-mini via vLLM on GPU 2 with the provided `postprocess.py` script. Process during off-peak hours. But be aware the improvement is marginal.

## Implementation

See `src/postprocess.py` — ready to run but flagged as low-priority given findings.
