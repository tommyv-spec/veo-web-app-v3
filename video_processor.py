"""
Video Processing Module for Final Export
- Trim frames from start/end of clips
- Concatenate multiple clips
- Optional Voice Activity Detection (VAD) to remove silence
"""

import json
import os
import shlex
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Tuple, Optional
import logging

logger = logging.getLogger(__name__)

# FFmpeg binary path (will use system PATH by default)
FFMPEG_BIN = os.environ.get("FFMPEG_BIN", "ffmpeg")
FFPROBE_BIN = os.environ.get("FFPROBE_BIN", "ffprobe")


# ============================================================
# v708 — Zero word loss contract (Whisper-VAD + final-export audit)
# ============================================================
# Constraint: every script word from each clip's intended line must remain
# audible in the final exported mp4. Whisper-tiny mistranscribes accented /
# fast TTS output and v611 end-cap then trims real audio thinking the
# Whisper-hallucinated words are fillers. v708 enforces:
#   1. Per-clip word-presence tracking (raw Whisper transcript vs. script set)
#   2. Retry chain on missing words: tiny+default → tiny+hardened → medium
#   3. Hard failsafe: still missing → return full clip (no trim)
#   4. Filler confidence gate at v611 end-cap (probability + edit distance)
#   5. Final-export audit: medium Whisper over output mp4 vs. master script
# v708 supersedes v706's floor-guard which only protected against sub-floor
# durations and did NOT catch full-length clips where Whisper transcribed
# the wrong words (clip 13 of saffron export 2026-05-11_2218: 19w script,
# 3/10 matched, audio dropped via v611 filler-trim on phantom 'are/cannot'
# tokens p=0.54/0.57).
V708_TRUST_MIN_FOR_TRIM = 0.85          # below → no-trim full-clip fallback
V708_FILLER_MIN_PROB = 0.70             # v611 end-cap filler must exceed this
V708_FILLER_MIN_EDIT_DIST = 2           # filler must differ from any script word by ≥N chars
# v731 — Soft failsafe trust floor. When v708 detects missing script words OR
# trust < V708_TRUST_MIN_FOR_TRIM, original behavior was full-clip retain (no
# trim) — which kept Veo TTS filler (breath/uh/repeat-syllables) intact and
# made the rendered line objectionable to the listener. Operator preference
# stated 2026-05-14: filler retention is worse than losing a rare-vocab word
# (chemistry/brand names Whisper-tiny/small cannot transcribe even with
# initial_prompt bias). v729 splits failsafe into two tiers:
#   * trust < V731_TRIM_TRUST_FLOOR  → HARD failsafe (current full-clip path)
#   * trust ≥ V731_TRIM_TRUST_FLOOR  → SOFT failsafe: trim using ALL Whisper
#     words (matched + unmatched-rare-vocab) as anchors. Rare-vocab audio
#     survives at its mis-transcribed timestamp; obvious filler outside any
#     Whisper word still gets dropped at concat time.
V731_TRIM_TRUST_FLOOR = 0.50
# v709 — REMOVED V708_AUDIT_MODEL_SIZE. Post-concat Whisper-small re-transcribe
# produced false-positive missing words on TTS audio operator could hear
# clearly. Replaced with per-clip audit aggregation (see v709 block below +
# aggregation in process_video). Saves ~500MB RAM + ~15s wall time per export.
V708_ESCALATE_MODEL_SIZE = "small"      # pass-3 escalation model — same; tiny is the bottleneck
V708_HARDENED_WHISPER_KWARGS = {
    "temperature": 0.0,
    "no_speech_threshold": 0.4,
    "compression_ratio_threshold": 2.0,
    "log_prob_threshold": -0.8,
    "condition_on_previous_text": False,
}

# v709 — Per-clip word audit (replaces post-concat v708 Layer 4 audit).
# Per-clip Whisper-tiny pass already produces matched/missing/trust per Veo
# clip in `detect_speech_segments_whisper`. v709 plumbs that data outward via
# a caller-supplied sink dict, then aggregates across clips at concat time.
# No second Whisper model load. False positives from Whisper-small choking on
# TTS audio die at the source.
V709_PER_CLIP_TRUST_FLOOR = 0.85        # clip passes audit iff trust ≥ floor AND zero missing AND no failsafe


def _v709_tokenize(text: str) -> set:
    """v709 — Lowercase + split hyphenated compounds + strip punctuation.
    `blood-flow` → {'blood','flow'}; `mother's` → {'mothers'}; em-dash
    treated like hyphen. Used by per-clip pass for both script-set
    construction AND Whisper output normalization so the two are guaranteed
    to tokenize identically. Pre-v709 the script side stripped `-` to
    nothing (fusing `blood-flow` → `bloodflow`) while Whisper produced two
    tokens — set diff structurally could not match. Symptom: persistent
    `missing=['bloodflow']` reports despite the words being audible.
    """
    import re as _re
    s = (text or "").lower().replace("-", " ").replace("—", " ").replace("–", " ")
    s = _re.sub(r"[^\w\s]", "", s)
    return {t for t in s.split() if t}


def _v708_levenshtein(a: str, b: str, max_dist: int = 4) -> int:
    """Bounded Levenshtein distance. Returns ≥max_dist if exceeded.
    Cheap because token lengths ≤ ~16 chars. Used by v611 filler gate
    to suppress trims on words that are 1-edit variants of script words
    (Whisper-tiny mis-spellings of real script content)."""
    if a == b:
        return 0
    if abs(len(a) - len(b)) >= max_dist:
        return max_dist
    if not a:
        return min(len(b), max_dist)
    if not b:
        return min(len(a), max_dist)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(
                prev[j] + 1,        # delete
                cur[-1] + 1,        # insert
                prev[j - 1] + (ca != cb),  # substitute
            ))
        prev = cur
        if min(prev) >= max_dist:
            return max_dist
    return min(prev[-1], max_dist)


def _v708_min_edit_dist_to_script(token: str, script_words_set) -> int:
    """Min edit distance from `token` to any word in `script_words_set`."""
    import re as _re
    t = _re.sub(r'[^\w]', '', (token or '').lower())
    if not t or not script_words_set:
        return 99
    best = 99
    for sw in script_words_set:
        d = _v708_levenshtein(t, sw, max_dist=min(best, V708_FILLER_MIN_EDIT_DIST + 1))
        if d < best:
            best = d
            if best == 0:
                return 0
    return best



def run(cmd: List[str]) -> Tuple[int, str, str]:
    """Run a command and return (returncode, stdout, stderr)."""
    try:
        p = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,   # FFmpeg encode progress goes to stderr, not stdout
            stderr=subprocess.PIPE,
            text=True
        )
        _, err = p.communicate(timeout=600)  # 10 minute timeout
        return p.returncode, "", err
    except subprocess.TimeoutExpired:
        p.kill()
        return -1, "", "Command timed out"
    except Exception as e:
        return -1, "", str(e)


def ffprobe_json(path: Path) -> dict:
    """Get video metadata as JSON."""
    cmd = [
        FFPROBE_BIN, "-v", "error",
        "-print_format", "json",
        "-show_streams", "-show_format",
        str(path)
    ]
    # ffprobe writes JSON to stdout — capture it directly (not via run() which discards stdout)
    try:
        result = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            text=True, timeout=30
        )
        if result.returncode != 0:
            raise RuntimeError(f"ffprobe failed for {path}")
        return json.loads(result.stdout)
    except Exception as e:
        raise RuntimeError(f"ffprobe failed for {path}: {e}")


def get_fps(info: dict) -> float:
    """Extract FPS from ffprobe info."""
    vstreams = [s for s in info.get("streams", []) if s.get("codec_type") == "video"]
    if not vstreams:
        raise RuntimeError("No video stream found.")
    fr = vstreams[0].get("avg_frame_rate") or vstreams[0].get("r_frame_rate") or "30/1"
    num, den = fr.split("/")
    num, den = float(num), float(den)
    return num / den if den != 0 else 30.0


def get_duration(info: dict) -> float:
    """Extract duration from ffprobe info."""
    fmt = info.get("format", {})
    if "duration" in fmt and fmt["duration"] not in ("N/A", None):
        return float(fmt["duration"])
    for s in info.get("streams", []):
        if s.get("codec_type") == "video" and "duration" in s and s["duration"] not in ("N/A", None):
            return float(s["duration"])
    return 8.0  # Default assumption for Veo clips


def check_vad_available() -> bool:
    """Check if silence detection is available (requires only ffmpeg, which is always present)."""
    code, _, _ = run([FFMPEG_BIN, "-version"])
    return code == 0


def detect_speech_segments(
    video_path: Path,
    threshold: float = 0.75,
    min_silence_duration: float = 1.5,
    padding_before: float = 0.15,
    padding_after: float = 0.15
) -> List[Tuple[float, float]]:
    """
    Detect speech (non-silent) segments using ffmpeg's silencedetect filter.
    Returns list of (start_time, end_time) tuples for segments WITH speech/sound.

    Args:
        threshold:            Maps 0.0-1.0 to dB noise floor.
                              0.1  = -50dB (very sensitive, catches near-silence)
                              0.75 = -29dB (default, cuts ambient noise / breath)
                              1.0  = -20dB (aggressive, only clear loud speech)
        min_silence_duration: Minimum quiet gap to treat as silence (seconds).
        padding_before:       Seconds of silence to keep before each speech burst.
        padding_after:        Seconds of silence to keep after each speech burst.
    """
    # Map 0.0–1.0 threshold to a dB noise floor.
    # At 0.1 → -50dB (barely anything is silence)
    # At 0.75 → -29dB (good default)
    # At 1.0  → -20dB (only loud speech survives)
    db_floor = -50.0 + (threshold * 30.0)  # range: -50dB to -20dB
    noise_param = f"{db_floor:.1f}dB"

    info = ffprobe_json(video_path)
    total_duration = get_duration(info)

    # Run silencedetect — outputs silence start/end times to stderr
    cmd = [
        FFMPEG_BIN, "-i", str(video_path),
        "-af", f"silencedetect=noise={noise_param}:d={min_silence_duration:.3f}",
        "-f", "null", "-"
    ]
    _, _, stderr = run(cmd)

    # Parse silence periods from ffmpeg output
    silence_starts = []
    silence_ends = []
    for line in stderr.splitlines():
        if "silence_start:" in line:
            try:
                val = float(line.split("silence_start:")[-1].strip())
                silence_starts.append(val)
            except ValueError:
                pass
        elif "silence_end:" in line:
            try:
                val = float(line.split("silence_end:")[1].split("|")[0].strip())
                silence_ends.append(val)
            except ValueError:
                pass

    # Build silence periods, clamped to video duration
    silence_periods = []
    for i, start in enumerate(silence_starts):
        end = silence_ends[i] if i < len(silence_ends) else total_duration
        silence_periods.append((max(0.0, start), min(total_duration, end)))

    # Invert silence → speech segments, then apply padding
    speech_segments = []
    cursor = 0.0
    for s_start, s_end in silence_periods:
        if s_start > cursor:
            seg_start = max(0.0, cursor - padding_before)
            seg_end = min(total_duration, s_start + padding_after)
            speech_segments.append((seg_start, seg_end))
        cursor = s_end

    # Handle trailing speech after last silence
    if cursor < total_duration:
        seg_start = max(0.0, cursor - padding_before)
        speech_segments.append((seg_start, total_duration))

    # If no silence was detected at all, return the whole video as one segment
    if not silence_periods:
        print("[VAD] No silence detected — returning full video as single segment")
        speech_segments = [(0.0, total_duration)]

    print(f"[VAD] noise_floor={noise_param}, min_silence={min_silence_duration}s")
    print(f"[VAD] Detected {len(silence_periods)} silence periods → {len(speech_segments)} speech segments")
    for i, (s, e) in enumerate(speech_segments):
        print(f"[VAD]   segment {i+1}: {s:.3f}s → {e:.3f}s ({e-s:.3f}s)")

    return speech_segments


# v773.10.9 — module-level Whisper-base singleton (lazy).
# faster-whisper int8 base: ~150 MB resident, 2-3× lower WER than tiny.
# Loaded once per worker process; reused across all clips in all exports.
_WHISPER_BASE_MODEL = None


def _ensure_whisper_base():
    """Lazy-load faster-whisper base int8 model. Cached for the process lifetime.

    v865 — this ~150MB stays resident until the process dies, so it is part of
    the permanent baseline on a 2GB box. Logged on the one load that creates it
    so the baseline step is visible in the trace.
    """
    global _WHISPER_BASE_MODEL
    if _WHISPER_BASE_MODEL is None:
        from faster_whisper import WhisperModel
        try:
            import mem_guard as _mg865
            _mg865.log("before whisper-base load (permanent ~150MB)")
        except Exception:
            _mg865 = None
        _WHISPER_BASE_MODEL = WhisperModel("base", device="cpu", compute_type="int8")
        if _mg865 is not None:
            _mg865.log("after whisper-base load (now permanent)")
    return _WHISPER_BASE_MODEL


# v773.10.14 — silero-VAD singleton for speech-boundary extension on top
# of Whisper anchors. ~2 MB ONNX model, loaded once per process.
_SILERO_VAD_MODEL = None


def _ensure_silero_vad():
    """Lazy-load silero-VAD v5 ONNX model. Cached for the process lifetime."""
    global _SILERO_VAD_MODEL
    if _SILERO_VAD_MODEL is None:
        from silero_vad import load_silero_vad
        _SILERO_VAD_MODEL = load_silero_vad(onnx=True)
    return _SILERO_VAD_MODEL


# v773.10.12 — aeneas DTW fallback removed: aeneas 1.7.3.0 (2017) uses
# numpy.distutils which numpy 2.x deleted, so the wheel build fails on
# modern Python. Quality upgrade is now carried entirely by the
# Whisper-base + beam=5 + hotwords + full-script initial_prompt stack.
# If rare-vocab edges still slip through, next step is a manual MFCC-DTW
# implementation using torchaudio (already installed) + dtw-python.


# v773.10.6 — Whisper-tiny multi-anchor trim.
# Operator requirement: trim each clip to ONLY contain the known script line.
# Robustness over v773.10.5 (single first+last anchor):
#   1. rapidfuzz.fuzz.ratio for fuzzy match (handles 2-3 edit distance).
#   2. Phonetic (Metaphone) fallback for rare vocab (laureth, korella,
#      phenylethyl etc) Whisper-tiny gets phonetically right but spells wrong.
#   3. Multi-anchor scan: try first/quartile/middle/last script words.
#      Pick the start anchor from the EARLIEST matching script word that
#      anchors in the FIRST HALF of audio. Pick end anchor from the LATEST
#      matching word that anchors in the LAST HALF. Position-weighted: rejects
#      anchors that fall in the wrong half (drops false positives like "the"
#      matching in the middle when looking for the first word).
#   4. Sanity check: if start ≥ end, or kept range < 30 % of audio, fall back
#      to ffmpeg silencedetect (no script). Better cheap trim than nothing.
def _whisper_anchor_trim(
    video_path,
    dialogue_texts,
    whisper_model=None,
    language: str = "English",
):
    import tempfile, re as _re
    # v773.10.13 — tight cut: exactly +1 frame of audio buffer before the
    # first script word and after the last script word. Operator wants the
    # final export trimmed flush to the line boundaries with minimal safety.
    # At 24 fps, 1 frame = 1/24 ≈ 0.0417 s. Whisper-base + beam=5 + hotwords
    # (v773.10.12) made word timestamps tight enough for this to be viable.
    _FRAME_PAD = 1.0 / 24.0
    HEAD_PAD = _FRAME_PAD
    TAIL_PAD = _FRAME_PAD
    FUZZ_THRESHOLD = 78         # rapidfuzz.fuzz.ratio cut-off (0-100)
    PHONETIC_FALLBACK_MIN = 5   # script word length to attempt phonetic match
    MIN_KEEP_FRACTION = 0.30    # if kept < 30 % of audio, distrust + fallback

    info = ffprobe_json(video_path)
    total_duration = get_duration(info)

    # Flatten script → token list (lowercase, no punctuation)
    script_tokens = []
    for _line in (dialogue_texts or []):
        if not _line:
            continue
        _clean = _re.sub(r"[^\w\s'-]", "", _line.lower())
        _clean = _clean.replace("-", " ").replace("—", " ").replace("–", " ")
        for _t in _clean.split():
            if _t:
                script_tokens.append(_t)
    if not script_tokens:
        return [(0.0, total_duration)]

    # Extract 16kHz mono wav for Whisper
    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as _tmp:
        _audio_path = _tmp.name
    _cmd = [FFMPEG_BIN, "-y", "-i", str(video_path), "-ar", "16000",
            "-ac", "1", "-f", "wav", _audio_path]
    _code, _, _err = run(_cmd)
    if _code != 0:
        print(f"[WhisperAnchor] audio extract failed: {_err[:200]} — keeping full clip", flush=True)
        try:
            import os as _os_ext
            _os_ext.unlink(_audio_path)  # v773.10.18 — don't leak the temp wav on the fail path
        except Exception:
            pass
        return [(0.0, total_duration)]

    # v773.10.9 — use a dedicated Whisper-base lazy singleton (~150 MB int8).
    # 2-3× lower WER than tiny on the same audio, especially on rare vocab.
    # We IGNORE the caller-supplied tiny model: it's still pre-loaded by the
    # legacy V708/V731 callsite, which wastes ~50 MB while ALIGN_ENABLED=1,
    # but switching the caller is out of scope for this hot-fix.
    try:
        whisper_model = _ensure_whisper_base()

        _LANG_ISO = {"english": "en", "spanish": "es", "french": "fr",
                     "german": "de", "italian": "it", "portuguese": "pt"}
        _iso = _LANG_ISO.get(language.lower(), None)

        # initial_prompt: full script (faster-whisper truncates to 224 tokens
        # internally). Rare-vocab terms in the prompt get explicit decoder bias.
        _init = " ".join((t or "").strip() for t in dialogue_texts if (t or "").strip())

        # hotwords: 5+ char script tokens that are decoder-hard for tiny/base.
        # faster-whisper passes these as a separate generation bias on top of
        # initial_prompt — Whisper specifically attends to them at each step.
        _rare_tokens = sorted({
            _t for _t in script_tokens
            if len(_t) >= 5 and _t.isalpha() and _t not in (
                "where", "should", "blood", "drink", "drive", "boost",
                "boosts", "right", "stop", "taking", "never",
            )
        })
        _hotwords = " ".join(_rare_tokens)[:200] if _rare_tokens else None

        _segs, _ = whisper_model.transcribe(
            _audio_path,
            language=_iso,
            word_timestamps=True,
            beam_size=5,             # was 1 (greedy); 5 = ~10% WER drop
            vad_filter=True,
            condition_on_previous_text=False,
            initial_prompt=_init or None,
            hotwords=_hotwords,
        )
        whisper_words = []
        for _seg in _segs:
            if _seg.words:
                for _w in _seg.words:
                    _tt = _re.sub(r"[^\w']", "", (_w.word or "").strip().lower())
                    if _tt:
                        whisper_words.append({
                            "text": _tt,
                            "start": float(_w.start),
                            "end": float(_w.end),
                            "prob": float(getattr(_w, "probability", 1.0) or 1.0),
                        })
    except Exception as _e:
        print(f"[WhisperAnchor] transcribe failed: {_e!r} — keeping full clip", flush=True)
        return [(0.0, total_duration)]
    finally:
        try:
            import os as _os2
            _os2.unlink(_audio_path)
        except Exception:
            pass

    if not whisper_words:
        print("[WhisperAnchor] no whisper words — keeping full clip (no trim)", flush=True)
        return [(0.0, total_duration)]

    # v773.10.15 — dump Whisper transcription per clip so operator can
    # see what the model actually heard vs the script. Each entry is
    # "word@start" rounded to 2 decimals. Plus the joined plain text
    # on a second line for quick reading.
    _ww_compact = " ".join(f"{w['text']}@{w['start']:.2f}" for w in whisper_words)
    _ww_text = " ".join(w["text"] for w in whisper_words)
    print(f"[WhisperAnchor] heard ({len(whisper_words)}w): {_ww_text}", flush=True)
    print(f"[WhisperAnchor] timings: {_ww_compact}", flush=True)
    print(f"[WhisperAnchor] script  : {' '.join(script_tokens)}", flush=True)

    # Lazy-import rapidfuzz + metaphone (deps added in v773.10.6)
    try:
        from rapidfuzz import fuzz as _fuzz
    except ImportError:
        _fuzz = None
    try:
        from metaphone import doublemetaphone as _dmp
    except ImportError:
        _dmp = None

    def _phonetic_key(_t: str) -> str:
        if _dmp is None or len(_t) < PHONETIC_FALLBACK_MIN:
            return ""
        try:
            _a, _b = _dmp(_t)
            return _a or _b or ""
        except Exception:
            return ""

    # Pre-compute phonetic keys for all whisper words (one-shot cost)
    for _ww in whisper_words:
        _ww["phon"] = _phonetic_key(_ww["text"])

    def _best_match(_script_word, _whisper_pool):
        """Return (whisper_word_dict, score) of the best match, or (None, 0)."""
        if not _whisper_pool:
            return None, 0
        _best = None
        _best_score = 0
        # Pass 1: rapidfuzz exact-or-fuzzy
        if _fuzz is not None:
            for _ww in _whisper_pool:
                _s = _fuzz.ratio(_script_word, _ww["text"])
                if _s > _best_score:
                    _best, _best_score = _ww, _s
            if _best_score >= FUZZ_THRESHOLD:
                return _best, _best_score
        # Pass 2: phonetic (Metaphone) fallback for rare vocab
        _sp = _phonetic_key(_script_word)
        if _sp:
            for _ww in _whisper_pool:
                if _ww["phon"] and _ww["phon"] == _sp:
                    return _ww, 70  # phonetic match worth ~70 confidence
        return _best, _best_score

    audio_mid = total_duration / 2.0

    # === Start anchor: search first 5 script words for one that lands in
    # the FIRST HALF of audio. Position-weighting drops false positives.
    start_anchor = None
    start_score = 0
    start_src_word = None
    head_pool = [w for w in whisper_words if w["start"] <= audio_mid]
    for _sw in script_tokens[:5]:
        _w, _s = _best_match(_sw, head_pool)
        if _w and _s >= FUZZ_THRESHOLD - 8:  # softer threshold for phonetic
            if start_anchor is None or _w["start"] < start_anchor["start"]:
                start_anchor, start_score, start_src_word = _w, _s, _sw

    # === End anchor: search last 5 script words for one that lands in
    # the LAST HALF of audio.
    end_anchor = None
    end_score = 0
    end_src_word = None
    tail_pool = [w for w in whisper_words if w["end"] >= audio_mid]
    for _sw in reversed(script_tokens[-5:]):
        _w, _s = _best_match(_sw, tail_pool)
        if _w and _s >= FUZZ_THRESHOLD - 8:
            if end_anchor is None or _w["end"] > end_anchor["end"]:
                end_anchor, end_score, end_src_word = _w, _s, _sw

    # v773.10.12 — aeneas DTW fallback removed (build failure on numpy 2.x).
    # Quality is carried by Whisper-base + beam=5 + hotwords + full prompt.
    # When anchors still miss, fall back to keep-more policy (v773.10.8).
    if start_anchor is None and end_anchor is None:
        print(
            "[WhisperAnchor] NO script anchors at either end → keeping full clip "
            "(Whisper-base + hotwords couldn't match any of first/last 5 script words)",
            flush=True,
        )
        return [(0.0, total_duration)]
    if start_anchor is None:
        # Pseudo-anchor at audio start; effectively disables HEAD_PAD trim
        start_anchor = {"start": 0.0, "end": 0.0, "text": "<KEEP_FROM_0>"}
        start_src_word = "<NO_HEAD_MATCH>"
        print(
            "[WhisperAnchor] no head-half script match for first 5 script tokens → "
            "keeping clip from 0.0 (trust tail anchor only)",
            flush=True,
        )
    if end_anchor is None:
        # Pseudo-anchor at audio end; effectively disables TAIL_PAD trim
        end_anchor = {"start": total_duration, "end": total_duration, "text": "<KEEP_TO_END>"}
        end_src_word = "<NO_TAIL_MATCH>"
        print(
            f"[WhisperAnchor] no tail-half script match for last 5 script tokens → "
            f"keeping clip to {total_duration:.2f}s (trust head anchor only)",
            flush=True,
        )

    # v773.10.17 — track which edges are pseudo "keep-to-edge" placeholders.
    # When Whisper found no head/tail script match, the clip edge is NOT a real
    # speech boundary — it's dead air. The silero-VAD pass below is allowed to
    # SHRINK a pseudo edge to the true speech boundary (matched anchors keep the
    # over-include bias and may only extend).
    head_is_pseudo = (start_src_word == "<NO_HEAD_MATCH>")
    tail_is_pseudo = (end_src_word == "<NO_TAIL_MATCH>")

    # Monotonicity + sanity
    if end_anchor["end"] <= start_anchor["start"]:
        end_anchor = start_anchor

    keep_start = max(0.0, start_anchor["start"] - HEAD_PAD)
    keep_end = min(total_duration, end_anchor["end"] + TAIL_PAD)
    keep_frac = (keep_end - keep_start) / max(total_duration, 0.001)
    if keep_frac < MIN_KEEP_FRACTION:
        # Anchors look unreliable (<30 % of audio kept). Don't trust hallucinated
        # Whisper words — keep the FULL clip instead. Same principle as the
        # no-anchor-at-either-end path above: better to keep filler than to
        # cut real script audio.
        print(
            f"[WhisperAnchor] kept {keep_frac:.0%} of audio (<{MIN_KEEP_FRACTION:.0%}) — "
            f"anchors unreliable, keeping full clip (no trim)",
            flush=True,
        )
        return [(0.0, total_duration)]

    # v773.10.14 — silero-VAD safety-net: extend the keep window to encompass
    # the actual speech regions that overlap our Whisper anchor window. Silero
    # detects speech vs non-speech independently of any transcription, so it
    # catches audible script words that Whisper-base missed at the boundaries.
    # Only EXTENDS the window (never shrinks it) — same over-include bias.
    try:
        import tempfile as _tf
        import os as _os3
        with _tf.NamedTemporaryFile(suffix=".wav", delete=False) as _vt:
            _vad_audio_path = _vt.name
        _vc = [FFMPEG_BIN, "-y", "-i", str(video_path), "-ar", "16000",
               "-ac", "1", "-f", "wav", _vad_audio_path]
        _vc_code, _, _ = run(_vc)
        if _vc_code == 0:
            try:
                from silero_vad import get_speech_timestamps as _gst
                # v773.10.16 — load WAV via soundfile, not torchaudio.load:
                # torchaudio 2.11 routes through a separate `torchcodec`
                # package that isn't installed, raising ImportError on every
                # clip. soundfile is already in requirements.txt and reads
                # PCM WAV natively. Returns float32 1-D numpy → torch.from_numpy.
                import soundfile as _sf
                import torch as _torch
                _vad_model = _ensure_silero_vad()
                _data, _sr = _sf.read(_vad_audio_path, dtype="float32", always_2d=False)
                if _data.ndim > 1:
                    _data = _data.mean(axis=1)
                _wav = _torch.from_numpy(_data)
                _ts = _gst(
                    _wav, _vad_model,
                    sampling_rate=16000,
                    min_silence_duration_ms=400,
                    speech_pad_ms=int(_FRAME_PAD * 1000),
                )
                _orig_start, _orig_end = keep_start, keep_end
                # Collect every silero speech region that intersects the
                # current keep window, then take the union's outer edges.
                _overlap = []
                for _t in _ts:
                    _ss = _t["start"] / 16000.0
                    _se = _t["end"] / 16000.0
                    if _se >= keep_start and _ss <= keep_end:
                        _overlap.append((_ss, _se))
                if _overlap:
                    _ov = sorted(_overlap)
                    _spk_start = _ov[0][0]
                    _spk_end = _ov[-1][1]
                    # Always EXTEND to catch audible script words Whisper missed
                    # at the boundaries (over-include bias for matched anchors).
                    if _spk_start < keep_start:
                        keep_start = max(0.0, _spk_start - _FRAME_PAD)
                    if _spk_end > keep_end:
                        keep_end = min(total_duration, _spk_end + _FRAME_PAD)
                    # v773.10.18 — pseudo edges (no Whisper anchor match) sit on
                    # dead air. SHRINK them to the real speech boundary. Use the
                    # CONTIGUOUS speech run anchored to the matched (real) side,
                    # NOT max/min across ALL overlapping regions: with a pseudo
                    # edge pinned to the clip edge, every silero region overlaps,
                    # so a single trailing breath/ambient blip (separated from the
                    # script by a real silence gap) would otherwise pull the edge
                    # back out and re-introduce the dead air this is meant to kill.
                    # Walk from the matched side, stop at the first gap > _GAP.
                    _GAP = 0.6  # silence gap (s) that marks dead air vs in-line pause
                    if tail_is_pseudo:
                        # head is the matched edge → walk forward from the first
                        # region reaching keep_start; run_end = real speech tail.
                        _run_end = None
                        for _s, _e in _ov:
                            if _run_end is None:
                                if _e >= keep_start:
                                    _run_end = _e
                            elif _s - _run_end <= _GAP:
                                _run_end = max(_run_end, _e)
                            else:
                                break
                        if _run_end is not None:
                            _cand = min(total_duration, _run_end + _FRAME_PAD)
                            if _cand < keep_end:
                                keep_end = _cand
                    if head_is_pseudo:
                        # tail is the matched edge → walk backward from the last
                        # region reaching keep_end; run_start = real speech head.
                        _run_start = None
                        for _s, _e in reversed(_ov):
                            if _run_start is None:
                                if _s <= keep_end:
                                    _run_start = _s
                            elif _run_start - _e <= _GAP:
                                _run_start = min(_run_start, _s)
                            else:
                                break
                        if _run_start is not None:
                            _cand = max(0.0, _run_start - _FRAME_PAD)
                            if _cand > keep_start:
                                keep_start = _cand
                if keep_start != _orig_start or keep_end != _orig_end:
                    print(
                        f"[WhisperAnchor] silero-VAD adjusted "
                        f"(head_pseudo={head_is_pseudo} tail_pseudo={tail_is_pseudo}): "
                        f"[{_orig_start:.2f}, {_orig_end:.2f}] → "
                        f"[{keep_start:.2f}, {keep_end:.2f}]",
                        flush=True,
                    )
            finally:
                try:
                    _os3.unlink(_vad_audio_path)
                except Exception:
                    pass
    except Exception as _ve:
        print(f"[WhisperAnchor] silero extension skipped: {_ve!r}", flush=True)

    print(
        f"[WhisperAnchor] anchors "
        f"start={start_src_word!r}→'{start_anchor['text']}'@{start_anchor['start']:.2f}s "
        f"end={end_src_word!r}→'{end_anchor['text']}'@{end_anchor['end']:.2f}s "
        f"keep=[{keep_start:.2f}, {keep_end:.2f}] dur={keep_end-keep_start:.2f}s "
        f"(orig {total_duration:.2f}s, dropped {total_duration-(keep_end-keep_start):.2f}s)",
        flush=True,
    )
    return [(keep_start, keep_end)]


def detect_speech_segments_whisper(
    video_path: Path,
    min_silence_duration: float = 0.3,
    padding: float = 0.15,
    dialogue_texts: List[str] = None,
    language: str = "English",
    clip_boundaries: List[Tuple[float, float]] = None,
    cut_prefix_audio: bool = False,  # v542
    prefix_word: str = "only",  # v542
    whisper_model=None,  # v701y — caller-supplied model to skip 11x reload
    v709_audit_sink: dict = None,  # v709 — caller-owned dict to receive per-clip audit
) -> List[Tuple[float, float]]:
    """
    Detect speech segments using Whisper + dialogue-anchored matching.
    
    Strategy:
    1. Transcribe with word-level timestamps + probability scores
    2. Filter out low-probability words (hallucinations from ambient noise)
    3. Match remaining words against known dialogue script
    4. Only matched word timestamps define speech regions
    5. Group nearby words (0.3s gap) into segments, cut everything else
    
    Returns list of (start_time, end_time) tuples for segments WITH speech.
    """
    # v773 — forced-alignment pipeline behind feature flag.
    # When ALIGN_ENABLED=1 in env, route to transcript_alignment module.
    # Default (flag unset or "0") keeps legacy V708/V731 path below.
    import os as _os
    if _os.environ.get("ALIGN_ENABLED", "0").lower() in ("1", "true", "yes"):
        _mode = _os.environ.get("ALIGN_MODE", "whisper_anchor").lower()

        # v773.10.5 — ALIGN_MODE=whisper_anchor (DEFAULT).
        # Reuses the Whisper-tiny model the caller already pre-loaded.
        # Strategy:
        #   1. Transcribe clip audio → word list with timestamps.
        #   2. Find first script word that fuzzy-matches a Whisper word
        #      → first anchor. Same from the end → last anchor.
        #   3. Trim to [first_anchor.start - HEAD_PAD, last_anchor.end + TAIL_PAD].
        # Zero new RAM. Handles rare-vocab middle-misses cleanly: as long as
        # the first + last script word match anywhere in the Whisper output,
        # the trim boundary is correct even when chemistry/brand names in the
        # middle were misheard.
        if _mode == "whisper_anchor":
            _seg_list = _whisper_anchor_trim(
                video_path,
                dialogue_texts or [],
                whisper_model=whisper_model,
                language=language,
            )
            if v709_audit_sink is not None:
                _wa_dur = sum(e - s for s, e in _seg_list)
                v709_audit_sink.update({
                    "script_provided": bool(dialogue_texts and any(dialogue_texts)),
                    "backend": "whisper-anchor",
                    "script_words": sum(len((t or "").split()) for t in (dialogue_texts or [])),
                    "aligned_words": 0,
                    "low_confidence_words": [],
                    "audio_duration": 0.0,
                    "speech_duration": _wa_dur,
                    "trim_ratio": 1.0,
                    "fallback_reason": None,
                    "trust": 1.0,
                    "matched": 0,
                    "heard_words": 0,
                    "missing": [],
                    "failsafe": None,
                })
            print(
                f"[Align/v773] backend=whisper-anchor "
                f"segments={len(_seg_list)} kept={sum(e-s for s,e in _seg_list):.2f}s",
                flush=True,
            )
            return _seg_list

        # ALIGN_MODE=ffmpeg → pure silence removal (no script).
        # Standard auto-editor / jumpcutter approach. Zero RAM cost.
        if _mode == "ffmpeg":
            _segments = detect_speech_segments(
                video_path,
                threshold=0.75,
                min_silence_duration=0.5,
                padding_before=0.15,
                padding_after=0.20,
            )
            if v709_audit_sink is not None:
                _ffmpeg_dur = sum(e - s for s, e in _segments)
                v709_audit_sink.update({
                    "script_provided": bool(dialogue_texts and any(dialogue_texts)),
                    "backend": "ffmpeg-silencedetect",
                    "script_words": sum(len((t or "").split()) for t in (dialogue_texts or [])),
                    "aligned_words": 0,
                    "low_confidence_words": [],
                    "audio_duration": 0.0,
                    "speech_duration": _ffmpeg_dur,
                    "trim_ratio": 1.0,
                    "fallback_reason": None,
                    "trust": 1.0,
                    "matched": 0,
                    "heard_words": 0,
                    "missing": [],
                    "failsafe": None,
                })
            print(
                f"[Align/v773] backend=ffmpeg-silencedetect "
                f"segments={len(_segments)} speech_kept={sum(e-s for s,e in _segments):.2f}s",
                flush=True,
            )
            return _segments
        from transcript_alignment import detect_speech_segments_aligned as _new_path
        _script = " ".join(
            (t or "").strip() for t in (dialogue_texts or []) if (t or "").strip()
        )
        _segments, _audit = _new_path(
            audio_path=video_path,
            script_text=_script,
            language=language,
            padding=padding,
        )
        # v709 audit sink shape preserved for downstream consumer compatibility.
        if v709_audit_sink is not None:
            v709_audit_sink.update({
                "script_provided": _audit["script_provided"],
                "backend": _audit["backend"],
                "script_words": _audit["script_words"],
                "aligned_words": _audit["aligned_words"],
                "low_confidence_words": _audit["low_confidence_words"],
                "audio_duration": _audit["audio_duration"],
                "speech_duration": _audit["speech_duration"],
                "trim_ratio": _audit["trim_ratio"],
                "fallback_reason": _audit["fallback_reason"],
                # Legacy fields zeroed so old consumers don't break:
                "trust": 1.0 if _audit["backend"] == "mms-fa" else 0.0,
                "matched": _audit["aligned_words"],
                "heard_words": _audit["aligned_words"],
                "missing": [],
                "failsafe": None if _audit["fallback_reason"] is None else "FALLBACK",
            })
        print(
            f"[Align/v773] script={_audit['script_words']}w aligned={_audit['aligned_words']}w "
            f"low_conf={_audit['low_confidence_words']} "
            f"dur={_audit['audio_duration']:.2f}s speech={_audit['speech_duration']:.2f}s "
            f"trim_ratio={_audit['trim_ratio']:.2f} backend={_audit['backend']}",
            flush=True,
        )
        return _segments
    # === Legacy V708/V731 path below (default until v773.10 flips flag) ===
    import subprocess, tempfile, re
    
    info = ffprobe_json(video_path)
    total_duration = get_duration(info)
    
    # Extract audio for Whisper
    with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as tmp:
        audio_path = tmp.name
    
    # v709 — early-return helper. Populates caller's sink with a "Whisper
    # broke on this clip" marker so the post-concat aggregator surfaces it
    # instead of silently skipping. Mirrors HARD-failsafe shape so existing
    # downstream consumers see a familiar low-trust failed-clip entry.
    def _v709_mark_sink_failed(_reason: str) -> None:
        if v709_audit_sink is not None:
            v709_audit_sink.update({
                "script_provided": bool(dialogue_texts and any(dialogue_texts)),
                "trust": 0.0,
                "matched": 0,
                "script_words": 0,
                "heard_words": 0,
                "missing": [],
                "failsafe": "HARD",
                "error": _reason,
            })

    try:
        cmd = [FFMPEG_BIN, '-y', '-i', str(video_path), '-ar', '16000', '-ac', '1', '-f', 'wav', audio_path]
        code, _, err = run(cmd)
        if code != 0:
            print(f"[WhisperVAD] Audio extraction failed: {err[:200]}")
            _v709_mark_sink_failed(f"audio extract failed: {err[:200]}")
            return [(0.0, total_duration)]
        
        try:
            from faster_whisper import WhisperModel
            # v701x — tiny model (50MB) keeps Render under the 2GB ceiling.
            # v701y — accept caller-supplied model so the v691d per-clip
            # loop loads Whisper ONCE for all N clips instead of N times.
            # Loading tiny costs ~0.5s + ~50MB resident on each call; for
            # an 11-clip v698A run that's 5.5s wasted and 11x RSS churn
            # that glibc won't fully reclaim even with malloc_trim. Caller
            # owns the lifecycle when whisper_model is passed in.
            _model_owned_here = whisper_model is None
            if whisper_model is None:
                model = WhisperModel("tiny", device="cpu", compute_type="int8")
            else:
                model = whisper_model
            
            # Map language name to Whisper ISO code
            _LANG_MAP = {
                "english": "en", "spanish": "es", "french": "fr", "german": "de",
                "italian": "it", "portuguese": "pt", "dutch": "nl", "russian": "ru",
                "chinese": "zh", "japanese": "ja", "korean": "ko", "arabic": "ar",
                "hindi": "hi", "turkish": "tr", "polish": "pl", "swedish": "sv",
                "norwegian": "no", "danish": "da", "finnish": "fi", "greek": "el",
                "hebrew": "he", "thai": "th", "vietnamese": "vi", "indonesian": "id",
                "malay": "ms", "tagalog": "tl", "swahili": "sw", "czech": "cs",
                "romanian": "ro", "hungarian": "hu", "ukrainian": "uk",
            }
            whisper_lang = _LANG_MAP.get(language.lower(), None)  # None = auto-detect

            # v701q — initial_prompt biases the Whisper decoder toward
            # words it expects to hear. Without it, Whisper mistranscribes
            # rare/compound terms in Veo TTS output: "self-rising" → "all-
            # fries", "crispy-cream" → "crisp", "pull apart" → "pull-
            # apartles". Misheard script words land in the matcher's
            # UNMATCHED set, then their audio gets cut as filler →
            # user-visible "missing syllables" inside the line.
            # Passing the joined script as initial_prompt is the standard
            # whisper.cpp / faster-whisper fix: the decoder weighs those
            # tokens higher during beam search. faster-whisper truncates
            # the prompt at 224 tokens internally so we don't need to clip;
            # typical line corpus fits well under.
            _initial_prompt = None
            if dialogue_texts:
                _joined = " ".join(
                    (t or "").strip() for t in dialogue_texts if (t or "").strip()
                )
                _joined = _joined.strip()
                if _joined:
                    _initial_prompt = _joined
                    print(
                        f"[WhisperVAD] v701q initial_prompt: "
                        f"{len(_joined.split())} script words "
                        f"(first 80 chars: {_joined[:80]!r})",
                        flush=True,
                    )

            print(f"[WhisperVAD] Model: tiny | Language: {language} → whisper={whisper_lang or 'auto'}", flush=True)

            # === v708 — Zero word loss retry chain ===
            # Pre-build per-clip word lists + flattened script set (used by
            # every pass + downstream v611 trim discipline).
            # v709 — use _v709_tokenize so hyphenated compounds (`blood-flow`)
            # tokenize as 2 words and match Whisper's split output.
            _per_clip_words = []
            _v708_script_set = set()
            for _line in (dialogue_texts or []):
                if _line:
                    _cw_set = _v709_tokenize(_line)
                    _cw = sorted(_cw_set)  # _per_clip_words used by matcher; order doesn't matter for set ops, but preserve list shape
                    # NOTE: the dialogue matcher (_match_whisper_to_dialogue)
                    # consumes _per_clip_words as token lists. We preserve
                    # token-list shape via the cleaned-split below to keep
                    # matcher behavior identical; the *set* uses hyphen-aware
                    # tokenization for audit purposes only.
                    _cleaned = re.sub(r'[^\w\s]', '', _line.lower().replace('-', ' ').replace('—', ' ').replace('–', ' '))
                    _per_clip_words.append(_cleaned.split())
                    for _w in _cw_set:
                        _v708_script_set.add(_w)
                else:
                    _per_clip_words.append([])
            _total_expected = sum(len(_w) for _w in _per_clip_words)

            def _v708_run_pass(_pass_model, _pass_kwargs_override, _pass_label):
                """Returns (all_words, speech_words, missing_set, matched_count, trust)."""
                _base_kwargs = dict(
                    language=whisper_lang,
                    word_timestamps=True,
                    beam_size=1,
                    vad_filter=True,
                    condition_on_previous_text=False,
                    initial_prompt=_initial_prompt,
                )
                _base_kwargs.update(_pass_kwargs_override or {})
                _segs_iter, _info_w = _pass_model.transcribe(audio_path, **_base_kwargs)
                _aw = []
                for _seg in _segs_iter:
                    if _seg.words:
                        for _word in _seg.words:
                            _aw.append({
                                'text': _word.word.strip().lower(),
                                'start': _word.start,
                                'end': _word.end,
                                'probability': _word.probability,
                            })
                _sw = _aw
                _matched_count = 0
                if _total_expected > 0:
                    _matched = _match_whisper_to_dialogue(
                        _aw, _per_clip_words,
                        clip_boundaries=clip_boundaries,
                        cut_prefix_audio=cut_prefix_audio,
                        prefix_word=prefix_word,
                    )
                    if _matched:
                        _sw = _matched
                        _matched_count = len(_matched)
                _heard_set = set()
                for _w in _aw:
                    # v709 — hyphen-aware tokenization mirrors script side so
                    # `blood-flow` heard as `blood flow` matches set elements
                    # `{'blood','flow'}` from the script. Pre-v709 the
                    # whisper-side regex stripped `-` to nothing while the
                    # script-side did the same — both produced `bloodflow`
                    # only when input had a hyphen; Whisper produces two
                    # tokens so the set diff structurally couldn't match.
                    for _tt in _v709_tokenize(_w.get('text') or ''):
                        _heard_set.add(_tt)
                _missing = _v708_script_set - _heard_set
                _trust = (_matched_count / max(len(_v708_script_set), 1)) if _v708_script_set else 1.0
                print(
                    f"[WhisperVAD/v708] pass={_pass_label} raw={len(_aw)} "
                    f"matched={_matched_count} script={len(_v708_script_set)} "
                    f"trust={_trust:.2f} missing={sorted(_missing) if _missing else 'NONE'}",
                    flush=True,
                )
                return _aw, _sw, _missing, _matched_count, _trust

            # Pass 1 — caller-supplied (or just-loaded) model, default kwargs.
            _v708_attempts = [_v708_run_pass(model, {}, "1-default")]
            # Escalate ONLY when script provided AND missing non-empty.
            if _total_expected > 0 and _v708_attempts[-1][2]:
                _v708_attempts.append(
                    _v708_run_pass(model, V708_HARDENED_WHISPER_KWARGS, "2-hardened")
                )
                if _v708_attempts[-1][2]:
                    _medium_model = None
                    try:
                        from faster_whisper import WhisperModel as _WM_M
                        _medium_model = _WM_M(
                            V708_ESCALATE_MODEL_SIZE, device="cpu", compute_type="int8"
                        )
                        print(
                            f"[WhisperVAD/v708] pass=3 loaded escalation model "
                            f"'{V708_ESCALATE_MODEL_SIZE}'",
                            flush=True,
                        )
                        _v708_attempts.append(
                            _v708_run_pass(
                                _medium_model,
                                V708_HARDENED_WHISPER_KWARGS,
                                f"3-{V708_ESCALATE_MODEL_SIZE}",
                            )
                        )
                    except Exception as _me:
                        print(
                            f"[WhisperVAD/v708] escalation model load failed: "
                            f"{_me} — skipping pass 3",
                            flush=True,
                        )
                    finally:
                        if _medium_model is not None:
                            try:
                                del _medium_model
                            except Exception:
                                pass
                            try:
                                import gc as _gc
                                _gc.collect()
                                import ctypes as _ct
                                _libc = _ct.CDLL("libc.so.6")
                                _libc.malloc_trim(0)
                            except Exception:
                                pass

            # Pick best attempt: zero-missing wins; else highest trust.
            _best = None
            for _att in _v708_attempts:
                if _best is None:
                    _best = _att
                    continue
                _b_missing, _b_trust = _best[2], _best[4]
                _a_missing, _a_trust = _att[2], _att[4]
                if not _a_missing and _b_missing:
                    _best = _att
                elif _a_missing and not _b_missing:
                    pass
                elif _a_trust > _b_trust:
                    _best = _att

            all_words, speech_words, _best_missing, _best_matched, _best_trust = _best
            print(
                f"[WhisperVAD/v708] BEST pass: matched={_best_matched} "
                f"trust={_best_trust:.2f} "
                f"missing={sorted(_best_missing) if _best_missing else 'NONE'}",
                flush=True,
            )

            # v709 — populate caller-owned audit sink. Mirrors BEST pass.
            # Failsafe field updated in HARD/SOFT branches below. Aggregator
            # in process_video consumes this to build stats["v708_per_clip_audit"].
            if v709_audit_sink is not None:
                _heard_for_sink = set()
                for _w in all_words:
                    for _tt in _v709_tokenize(_w.get('text') or ''):
                        _heard_for_sink.add(_tt)
                v709_audit_sink.update({
                    "script_provided": bool(_v708_script_set),
                    "trust": float(_best_trust),
                    "matched": int(_best_matched),
                    "script_words": len(_v708_script_set),
                    "heard_words": len(_heard_for_sink),
                    "missing": sorted(_best_missing) if _best_missing else [],
                    "failsafe": None,
                })

            print(
                f"[WhisperVAD] Transcribed {len(all_words)} raw words "
                f"(total_duration={total_duration:.1f}s)",
                flush=True,
            )

            # v708 HARD FAILSAFE — if script provided AND (missing non-empty OR
            # trust below floor), abort all trimming for this clip. Return the
            # full clip [0, total_duration]. Caller's apply_vad will still run
            # ffmpeg trim+concat over a single segment = the entire clip.
            # Downstream v706 floor + v707/v619 etc. unaffected; v708 just
            # widens the policy from "minimum duration" to "every word kept".
            # v731 — Two-tier failsafe. HARD path only fires when trust is
            # genuinely garbage (< V731_TRIM_TRUST_FLOOR). For the "high-trust
            # + a few rare-vocab missing" case (clip 4 of dish-soap export
            # 2026-05-13_23:43: `laureth`/`probable` missing, trust > 0.85,
            # FAILSAFE retained full Veo clip with audible filler), promote
            # to SOFT failsafe: continue to the trim path using all_words
            # (raw Whisper output, includes mis-transcribed rare vocab) as
            # speech anchors instead of speech_words (matched-only). Rare
            # word's audio sits at the mis-transcribed token's timestamp,
            # so trim segments around it; obvious filler outside any
            # Whisper word still gets dropped.
            _v731_soft_failsafe = False
            if _total_expected > 0 and (_best_missing or _best_trust < V708_TRUST_MIN_FOR_TRIM):
                _reason = []
                if _best_missing:
                    _reason.append(f"missing={sorted(_best_missing)}")
                if _best_trust < V708_TRUST_MIN_FOR_TRIM:
                    _reason.append(
                        f"trust={_best_trust:.2f}<{V708_TRUST_MIN_FOR_TRIM:.2f}"
                    )
                # v731 (deprecated by v709.3): pre-v709.3 had a 2-tier
                # failsafe — HARD (no-trim) vs SOFT (trim with all-words
                # anchors). v709.3 collapses both to no-trim. Reason: real
                # export (job e13ef262 2026-05-28) showed SOFT failsafe was
                # dropping COMMON opening words (`do, not, drink, pomegranate,
                # juice` on clip 1; `never` on clip 3) because Whisper-tiny
                # silently omitted those words from `all_words` entirely
                # (not mis-transcribed — simply absent). Trim with all-words
                # anchors then had no anchor for the start of the line and
                # cut ~2s of leading audio. v731's premise (rare-vocab words
                # carry a mis-transcribed timestamp) held for `laureth`-class
                # cases but fails for common-vocab Veo TTS misses where
                # Whisper just doesn't emit a token. Losing real words is
                # worse than keeping breath/uh filler. v709.3: any failsafe
                # = full-clip retain. Track which kind for audit signal.
                if _best_trust < V731_TRIM_TRUST_FLOOR:
                    _fs_kind = "HARD"
                else:
                    _fs_kind = "SOFT"
                print(
                    f"[WhisperVAD/v709.3] FAILSAFE-{_fs_kind}: {', '.join(_reason)} "
                    f"(trust={_best_trust:.2f}) → no-trim, keep full clip "
                    f"{total_duration:.3f}s",
                    flush=True,
                )
                if v709_audit_sink is not None:
                    v709_audit_sink["failsafe"] = _fs_kind
                if _model_owned_here:
                    try:
                        del model
                        import gc as _gc2
                        _gc2.collect()
                    except Exception:
                        pass
                return [(0.0, total_duration)]

            if not all_words:
                print("[WhisperVAD] No words detected — returning full video")
                if _model_owned_here:
                    try:
                        del model
                        import gc as _gc3
                        _gc3.collect()
                    except Exception:
                        pass
                return [(0.0, total_duration)]

            if not speech_words:
                print("[WhisperVAD] No speech words survived filtering — returning full video")
                if _model_owned_here:
                    try:
                        del model
                        import gc as _gc4
                        _gc4.collect()
                    except Exception:
                        pass
                return [(0.0, total_duration)]
            
            # Log word-level timing for verification
            print(f"[WhisperVAD] Word timing ({len(speech_words)} words):", flush=True)
            for i, w in enumerate(speech_words):
                print(f"[WhisperVAD]   [{w['start']:.2f}s-{w['end']:.2f}s] '{w['text']}' (p={w['probability']:.2f})", flush=True)
            
            # === Step 3: Tighten word boundaries ===
            # Whisper inflates word `end` timestamps with breath/silence after speech.
            # Only tighten words that have another word close after them (mid-group).
            # Leave the LAST word before each gap untouched — it needs full duration.
            GAP_THRESHOLD = 0.4  # If next word starts > 0.4s after this word ends, this is a group boundary
            for i, w in enumerate(speech_words):
                is_last = (i == len(speech_words) - 1)
                if not is_last:
                    next_w = speech_words[i + 1]
                    gap_to_next = next_w['start'] - w['end']
                    if gap_to_next < GAP_THRESHOLD:
                        # Mid-group word — safe to tighten
                        raw_dur = w['end'] - w['start']
                        tight_dur = max(0.12, raw_dur * 0.85)
                        w['end'] = w['start'] + tight_dur
                    # else: last word before a gap — keep full duration
                # else: very last word — keep full duration
            
            # === Step 4: Group into speech segments ===
            # Apply per-word padding BEFORE grouping — tight cuts around speech
            # v497: padding is CLIP-AWARE. Previously applied 0.25/0.2s to
            # every word unconditionally, which caused 250ms of the previous
            # clip's video to bleed into the start of the next clip's
            # segment at every inter-clip boundary.
            #
            # v498: further tightened END pad. User reported 1-2 stale
            # frames still visible at clip cuts with EDGE_PAD=0.05s (50ms
            # = 1.2 frames at 24fps — exact match to the artifact). The
            # culprit was the END pad of the last word in each clip: the
            # subject's mouth KEEPS MOVING after the last script word
            # because the 25-word fill continues to be spoken. Cutting
            # 50ms after the last word shows 50ms of lip movement that
            # doesn't match the (cut) audio.
            #
            # Asymmetric fix: START pad stays at 50ms (safe — shows
            # subject's face a tiny moment before speaking, natural).
            # END pad drops to 20ms (~0.5 frame at 24fps, sub-perception).
            #
            # Policy:
            #   - Intra-clip: full 0.25/0.2s padding (natural breath room)
            #   - First word of a clip: clamp start-pad to EDGE_PAD_START
            #   - Last word of a clip: clamp end-pad to EDGE_PAD_END (smaller)
            #   - Hard clamp against clip_boundary range
            #
            # v500: relaxed EDGE_PAD back toward comfortable audio levels.
            # v498's 0.05/0.02s was too tight — audio felt abrupt. The
            # underlying "stale frames" bug that drove the tight values
            # was actually ffmpeg input seek snapping to wrong keyframes,
            # which v499 fixed with frame-accurate coarse+fine seek. With
            # that fix in place, we can afford wider EDGE_PAD because any
            # frames shown are legitimately from near the word's real
            # timestamp, not random debris.
            #
            # v504: start tightened further per user — 0.08s → 0.05s.
            #   EDGE_PAD_START: 0.05s = ~1.2 frames @ 24fps
            #   EDGE_PAD_END:   0.18s = ~4.3 frames @ 24fps (unchanged)
            WORD_PAD_START = 0.25
            WORD_PAD_END = 0.2
            # v554 — asymmetric edge padding per user feedback.
            # Previous v553 logic used EDGE_PAD_START=0.05, EDGE_PAD_END=0.18.
            # User report: "the beginning needs to be tighter and the end
            # needs to be a bit looser, because we collect some [audio]
            # from before the word and we cut the final word".
            # Translation: the START edge has been bleeding pre-word audio
            # (Veo TTS lead-ins like 'only' at the seam) — needs to clamp
            # HARD to the matched word's start. The END edge has been
            # cutting the consonant-decay tail of the last word — needs
            # MORE pad to preserve natural word endings.
            EDGE_PAD_START = 0.0    # 0ms — segment starts EXACTLY at first matched word
            EDGE_PAD_END = 0.35     # 350ms — generous tail for consonant decay + mouth close

            # v549 — compute unmatched (likely-hallucination) Whisper
            # words once, up front, so both the per-word padding
            # clamp (below) and the bridger (further down) can
            # consult the same list.
            #
            # The matcher returns the matched word DICTS (same objects,
            # not copies), so identity check via 'start' time is robust.
            # We keep only unmatched words with confidence ≥ 0.30 —
            # below that, Whisper is usually grasping at silence/breath
            # and shouldn't influence padding/bridging decisions.
            HALLUC_PROB_FLOOR = 0.30
            matched_starts = {w.get('start') for w in speech_words}
            unmatched_words = [
                w for w in all_words
                if w.get('start') not in matched_starts
                and w.get('probability', 0) >= HALLUC_PROB_FLOOR
            ]

            # Build a quick lookup: for each word, which clip does it belong
            # to (if boundaries provided)?
            def _find_clip(t):
                if not clip_boundaries:
                    return None
                for ci, (cs, ce) in enumerate(clip_boundaries):
                    if cs <= t < ce:
                        return ci
                return None

            words_ts = []
            for i, w in enumerate(speech_words):
                # v498: prefer the annotated clip_idx from the matcher
                # (set by _match_whisper_to_dialogue). Falls back to
                # timestamp-based inference only if no annotation exists
                # (legacy code paths / flat DP alignment).
                clip_idx = w.get('clip_idx')
                if clip_idx is None:
                    clip_idx = _find_clip(w['start'])

                # Is this word the first/last within its clip?
                is_first_in_clip = False
                is_last_in_clip = False
                if clip_idx is not None:
                    # Check neighbor's clip via annotation where possible
                    prev_clip = speech_words[i - 1].get('clip_idx') if i > 0 else None
                    if prev_clip is None and i > 0:
                        prev_clip = _find_clip(speech_words[i - 1]['start'])
                    next_clip = speech_words[i + 1].get('clip_idx') if i < len(speech_words) - 1 else None
                    if next_clip is None and i < len(speech_words) - 1:
                        next_clip = _find_clip(speech_words[i + 1]['start'])

                    if i == 0 or prev_clip != clip_idx:
                        is_first_in_clip = True
                    if i == len(speech_words) - 1 or next_clip != clip_idx:
                        is_last_in_clip = True

                # Apply asymmetric padding based on position in clip.
                pad_start = EDGE_PAD_START if is_first_in_clip else WORD_PAD_START
                pad_end = EDGE_PAD_END if is_last_in_clip else WORD_PAD_END

                start = max(0.0, w['start'] - pad_start)
                end = min(total_duration, w['end'] + pad_end)

                # v546 — neighbor-aware clamp. The previous v498 logic
                # hard-clamped (start, end) to the clip's (cs, ce)
                # boundary "so padding can NEVER cross into another
                # clip's video frames." That worked when the matcher
                # always attributed words whose timestamps fell strictly
                # inside their clip's boundary. v544's FIRST_WORD_PRE_
                # TOLERANCE explicitly broke that assumption — Veo can
                # render the first word of clip N+1 a few hundred
                # milliseconds before clip_boundary[N+1].start, and
                # the matcher correctly attributes it to clip N+1. The
                # old hard-clamp would then chop off the word's onset
                # because w['start'] < cs.
                #
                # Concrete failure observed: clip 2's 'cut' rendered
                # at 7.24s, attributed to clip 2 (boundary 7.7s+).
                # Old code clamped start = max(7.19, 7.7) = 7.7s,
                # silently dropping 460ms of 'cut' audio from the
                # export. The user heard "...listen to me [silence]
                # half a red cabbage" instead of "...listen to me cut
                # half a red cabbage".
                #
                # New strategy: clamp against NEIGHBORING WORDS, not
                # against clip boundaries. Padding is allowed to cross
                # a clip boundary as long as it doesn't run into the
                # previous/next word's range. This preserves every
                # word's actual audio while still preventing pad
                # overlap.
                if i > 0:
                    prev_w_end = speech_words[i - 1].get('end', 0)
                    # Don't let our start-pad reach back into the
                    # previous word's audio. Use the midpoint between
                    # prev word's end and our word's start as the
                    # earliest legal start. Falls back to prev_w_end
                    # if the gap is small (<50ms).
                    gap = w['start'] - prev_w_end
                    if gap > 0.05:
                        midpoint = prev_w_end + gap / 2.0
                        start = max(start, midpoint)
                    elif gap >= 0:
                        start = max(start, prev_w_end)
                if i < len(speech_words) - 1:
                    next_w_start = speech_words[i + 1].get('start', total_duration)
                    gap = next_w_start - w['end']
                    if gap > 0.05:
                        midpoint = w['end'] + gap / 2.0
                        end = min(end, midpoint)
                    elif gap >= 0:
                        end = min(end, next_w_start)

                # v549 — hallucination padding clamp. Even if the bridger
                # later refuses to merge across a hallucination (v548 Fix
                # A), the padding around a matched word can ALREADY have
                # crept into a hallucinated word's audio range. The Fix A
                # check only inspects the gap BETWEEN padded groups; it
                # can't see hallucinations sitting INSIDE the padding zone.
                #
                # Concrete failure observed: clip 2's 'chunks,' end-pad
                # (10.84s) absorbed part of a 'like' hallucination at
                # 10.65s, and 'it' start-pad covered a different stretch.
                # The bridger then merged the remaining narrow gap and
                # ~0.4s of garbage audio survived into the export.
                #
                # Fix: scan unmatched words near this matched word and
                # clamp padding to never cross an unmatched word's
                # audio range.
                #
                # v554 — ASYMMETRIC GUARDS per user feedback. The START
                # guard is aggressive (cut hard, no pre-word bleed). The
                # END guard is permissive (preserve consonant decay tail
                # which Whisper consistently underestimates). When an
                # unmatched word's onset sits 50-200ms after a matched
                # word's whisper-end, that's almost always because the
                # matched word's true acoustic end (including consonant
                # decay) extends INTO Whisper's reported start of the
                # next-token. Clamping the matched word's end-pad to
                # (unmatched_start - 0.02s) cuts the decay; allowing
                # ~150ms of tail-overlap preserves it without dragging
                # the unmatched word's actual content into the export.
                if unmatched_words:
                    GUARD_START = 0.02   # tight — no pre-word bleed
                    TAIL_OVERLAP = 0.15  # generous — let consonant decay extend past whisper-end
                    # v557 — when an unmatched word ends very close to
                    # the matched word's whisper-reported start (gap < 0.10s),
                    # Whisper has put the boundary in approximately the
                    # right place but the unmatched word's actual acoustic
                    # decay tail (especially vowel-like sonorants — the
                    # 'y' of 'only', the 'lly' of 'actually') extends
                    # PAST Whisper's reported end into the segment we're
                    # about to keep. Concrete failure observed: in clip 2
                    # of the Nuri export, 'only' ends at 8.16s and 'pick'
                    # starts at 8.16s — segment starts EXACTLY at 8.16s
                    # so the audio at 8.16s contains the 'y' decay tail
                    # of 'only' that Whisper truncated. User reports
                    # hearing the 'Y' at clip starts.
                    #
                    # Fix: when the unmatched word's whisper-end is within
                    # 0.10s of the matched word's whisper-start, shift the
                    # segment start FORWARD by FILLER_DECAY_TAIL_CLEAR
                    # seconds. This skips the bleed-in decay. We accept
                    # losing the very initial onset of the matched word
                    # (typically the consonant attack which is energetic
                    # but brief) — listeners notice 'y' bleed much more
                    # than a slightly-clipped consonant attack. This is
                    # research-backed: WhisperX paper shows Whisper's
                    # word boundaries have ±200ms precision against
                    # phoneme-level ground truth, so a 50ms forward shift
                    # is well within Whisper's known imprecision range.
                    FILLER_DECAY_TAIL_CLEAR = 0.05   # 50ms forward shift
                    FILLER_GAP_THRESHOLD = 0.10      # only if u_end is within 100ms of w_start
                    for u in unmatched_words:
                        u_start = u.get('start')
                        u_end = u.get('end')
                        if u_start is None or u_end is None:
                            continue

                        # Case 1: u sits BEFORE this word and our
                        # start-pad reaches back into u's range.
                        # Push start forward to u_end + GUARD_START, but
                        # never beyond w['start'] (don't shrink the
                        # matched word itself). v554: keep tight.
                        if u_end <= w['start'] and start < u_end:
                            start = min(w['start'], u_end + GUARD_START)

                        # v557 — Case 1b: u sits IMMEDIATELY before
                        # this word (gap < 100ms, the "back-to-back
                        # filler+matched" pattern). Whisper put the
                        # boundary at w['start'] but the actual decay
                        # tail of u bleeds INTO the audio at w['start'].
                        # Shift start forward 50ms to clear it.
                        # ONLY apply when this is the FIRST matched word
                        # of a segment-to-be (is_first_in_clip), so we
                        # don't shift mid-sentence word boundaries
                        # which the matcher has already pinned correctly.
                        if (is_first_in_clip
                                and u_end <= w['start']
                                and (w['start'] - u_end) < FILLER_GAP_THRESHOLD):
                            shifted = w['start'] + FILLER_DECAY_TAIL_CLEAR
                            # Don't shift past w['end'] (don't lose the whole word)
                            # and don't shift past the next matched word's start
                            # if there's one in the same group.
                            max_shift = w['end'] - 0.05  # leave at least 50ms of word
                            if i < len(speech_words) - 1:
                                next_w_start_safe = speech_words[i + 1].get('start', max_shift)
                                max_shift = min(max_shift, next_w_start_safe)
                            start = max(start, min(shifted, max_shift))

                        # Case 2: u sits AFTER this word and our
                        # end-pad reaches forward into u's range.
                        # v554: instead of pulling end back to
                        # (u_start - 0.02), allow the end-pad to extend
                        # up to TAIL_OVERLAP seconds INTO the unmatched
                        # word's start. This preserves the matched
                        # word's natural consonant decay. The tail
                        # overlap must NEVER reach u_end (then we'd
                        # pull in unmatched content); it caps at
                        # min(end, u_start + TAIL_OVERLAP, u_end - 0.05).
                        if u_start >= w['end'] and end > u_start:
                            tail_cap = min(u_start + TAIL_OVERLAP, u_end - 0.05)
                            end = max(w['end'], min(end, tail_cap))

                # v635 — HARD-CLAMP to clip physical boundaries (replaces
                # v546's soft-clamp). User reported a "ghost frame from end
                # of prior clip" at every seg boundary where the AI-rendered
                # audio bleeds across the physical clip cut.
                #
                # Concrete repro from user log: clip 8 physical range is
                # 54.1-61.8s but its first matched word "this" starts at
                # 53.90s (inside clip 7's physical window 46.4-54.1).
                # v546 saw `w['start']=53.90 < cs=54.1` and SKIPPED the
                # clamp, keeping seg8 trim start at 53.958. That trim
                # range pulled 142ms (~3.4 frames @ 24fps) of CLIP 7's
                # video into seg8's beginning. User sees those 3-4 frames
                # as "ghost from end of clip 7" — because they literally
                # are clip 7's last visual frames.
                #
                # Same issue affected seg10 (starts 69.542 vs clip 10 cs
                # 69.6 → 58ms / 1.4 frames of clip 9 leak).
                #
                # Tradeoff: when audio bleeds across a clip boundary
                # (AI-generation artifact), the bleeding portion is now
                # cut. e.g. "this" word audio at 53.90-54.62 → trim starts
                # at 54.1 → ~200ms of word's leading audio dropped. Word
                # may sound clipped. Acceptable because:
                #   (a) visual artifact was unacceptable to user
                #   (b) AI clips SHOULDN'T bleed across boundaries; if
                #       they do, that's an upstream gen issue
                #   (c) Whisper word.start often overshoots the actual
                #       phoneme start by 50-150ms anyway, so the audible
                #       loss is smaller than the timestamp suggests.
                if clip_idx is not None:
                    cs, ce = clip_boundaries[clip_idx]
                    start = max(start, cs)
                    end = min(end, ce)

                words_ts.append((start, end))

            # Group overlapping/adjacent padded words into segments
            speech_groups = []
            group_start, group_end = words_ts[0]
            
            for w_start, w_end in words_ts[1:]:
                if w_start <= group_end:
                    # Overlapping or touching — extend
                    group_end = max(group_end, w_end)
                else:
                    speech_groups.append((group_start, group_end))
                    group_start, group_end = w_start, w_end
            speech_groups.append((group_start, group_end))

            # v465: Bridge suspicious intra-clip gaps. Whisper occasionally
            # drops a word mid-clip (happened with "biology" in the user's
            # log — 21/25 whisper words matched, "biology" was one of the
            # 4 unmatched). That drop creates a 0.5-1.5s gap in the word
            # timing list, which the grouper above turns into a cut.
            # Audio for that silently-dropped word gets removed from the
            # export.
            #
            # Heuristic: if two adjacent speech groups have a gap under
            # BRIDGE_GAP_MAX seconds AND both sides fall inside the SAME
            # clip_boundary, assume the gap contains a dropped word — merge.
            # Gaps that cross a clip boundary are left alone; those are
            # legitimate inter-clip silence that SHOULD be cut.
            #
            # v548 Fix A — refuse to bridge a gap if it contains UNMATCHED
            # Whisper words. Concrete failure observed: in clip 2, between
            # script's 'chunks' (10.40s) and 'carries' (12.48s), Whisper
            # transcribed two hallucinated tokens ('like' and "it's,") that
            # the matcher correctly excluded as not-script. The old bridger
            # saw a 0.43s gap inside clip 2 and merged it, re-including the
            # hallucinated audio in the final export. The user heard
            # garbage between 'chunks' and 'carries'. The fix: if any
            # non-matched Whisper word with confidence ≥ 0.30 sits inside
            # the gap, the gap is NOT a dropped script word — it's filler
            # the matcher correctly rejected. Refuse the bridge and let
            # the segmenter cut the audio out.
            # v553 — tightened from 1.5s to 0.7s. With the new in-order
            # matcher every kept word is a verified script word, so a
            # gap inside a clip is one of two things: (a) Whisper
            # dropped a single fast function word — gap is typically
            # 0.2-0.5s, bridge it; or (b) Veo TTS mumbled or paused
            # mid-sentence — gap is typically 0.8s+, do NOT bridge.
            # 0.7s is the line between them. Concrete failure: clip 1
            # of the user's log had a 1.15s gap between 'sister' and
            # "he's gonna" where Veo mumbled — old 1.5s bridger merged
            # it into the export. New 0.7s limit keeps the mumble cut.
            BRIDGE_GAP_MAX = 0.7
            # Note: unmatched_words and HALLUC_PROB_FLOOR were computed
            # above (v549 hoisting) so the per-word padding clamp could
            # also use them. Reusing the same list here keeps logic
            # consistent between the two checks.

            if clip_boundaries and len(speech_groups) >= 2:
                bridged_groups = [speech_groups[0]]
                for (s, e) in speech_groups[1:]:
                    prev_s, prev_e = bridged_groups[-1]
                    gap = s - prev_e
                    if 0 < gap <= BRIDGE_GAP_MAX:
                        # Determine which clip each end belongs to
                        def _clip_for(t):
                            for ci, (cs, ce) in enumerate(clip_boundaries):
                                if cs <= t < ce:
                                    return ci
                            return -1
                        pre_clip = _clip_for(prev_e)
                        post_clip = _clip_for(s)
                        # Bridge only if both ends are within the same clip
                        # (or one end is right at the clip boundary — prev_e
                        # can equal the next clip's start due to padding).
                        if pre_clip == post_clip and pre_clip >= 0:
                            # v548 Fix A — check the gap for hallucinations
                            # before merging. Look for unmatched words whose
                            # timestamp falls inside (prev_e, s).
                            blocking = [
                                w for w in unmatched_words
                                if prev_e <= w.get('start', -1) < s
                            ]
                            if blocking:
                                # Don't bridge — the gap contains audio the
                                # matcher correctly excluded. Letting the
                                # bridger cover it would re-include the
                                # hallucinated audio in the export.
                                bad_w = blocking[0]
                                bad_text = bad_w.get('text', '?')
                                bad_p = bad_w.get('probability', 0)
                                print(f"[WhisperVAD] 🚫 Refused bridge {prev_e:.2f}s → {s:.2f}s in clip {pre_clip + 1} — gap contains unmatched '{bad_text}' (p={bad_p:.2f}) — likely Veo hallucination", flush=True)
                                bridged_groups.append((s, e))
                                continue
                            # Merge: extend the previous group's end to
                            # absorb the current group
                            bridged_groups[-1] = (prev_s, e)
                            print(f"[WhisperVAD] 🩹 Bridged intra-clip gap: {prev_e:.2f}s → {s:.2f}s ({gap:.2f}s) in clip {pre_clip + 1} — likely dropped word", flush=True)
                            continue
                    bridged_groups.append((s, e))
                speech_groups = bridged_groups

            # === v611 — strict matched-word containment (defense-in-depth) ===
            # Goal: every output segment's edges are bounded by matched-
            # word audio + tight tolerance, regardless of what padding /
            # bridging produced. Filler / hallucination / silence past
            # the LAST matched word in a segment, OR before the FIRST
            # matched word, gets cut.
            #
            # Why this is needed even after v548/v554/v557: those guards
            # operate on per-word padding (v554 unmatched_words guard)
            # and on bridge-time gap inspection (v548 Fix A) using the
            # HALLUC_PROB_FLOOR=0.30 confidence floor. They can't catch:
            #   (a) low-confidence (<0.30) hallucinations sitting in
            #       padded zones — invisible to per-word guard
            #   (b) the EDGE_PAD_END=0.35s tail past the clip-final word
            #       when no high-confidence unmatched word follows
            #   (c) the TAIL_OVERLAP=0.15s allowed bleed into unmatched-
            #       word range when consonant decay was assumed
            #
            # User goal (2026-05-06): "the final outcome is, i want to
            # maintain only the original lines mentioned in the video,
            # everything else is cut out."
            #
            # v611 fires AFTER bridging and uses ALL whisper words
            # (any confidence) as edge blockers. It only contracts
            # segments — it cannot extend them — so all earlier
            # preservation logic stays intact for the matched-word
            # interior. Asymmetric per v554 carryover: start guard
            # tight, end guard moderate (preserves consonant decay).
            STRICT_START_GUARD = 0.02       # 20ms after a pre-segment unmatched word's end
            STRICT_END_GUARD = 0.08         # 80ms before a post-segment unmatched word's start
            # v632 — REVERT v630. v630 dropped pad to 0.04s and clipped the
            # last word's consonant tail (sibilants /s/, /z/, /ʃ/ have
            # 150-300ms decay; vowel + breath tail also exceeds 1 frame).
            # Restored to v616c's 0.10s. The "ghost frame at clip boundary"
            # the user reported was NOT post-speech residual — it persists
            # even with this pad cut. Different bug, investigated separately.
            #
            # v616c — STRICT_FALLBACK_END_PAD = 0.10s = ~2.4 frames @ 24fps.
            # Per WhisperX (Bain 2023, arxiv 2303.00747), Whisper's
            # word.end overshoots phoneme end by 50-150ms (silence/breath
            # rolled in). The 0.10s margin keeps consonant decay safely.
            STRICT_FALLBACK_END_PAD = 0.10
            STRICT_MIN_END_TAIL = 0.05      # never trim below last_matched.end + 50ms

            matched_start_set = {w.get('start') for w in speech_words}
            # Use ALL whisper words (any confidence) for the final
            # guard, not just unmatched_words[≥0.30]. Low-confidence
            # whisper outputs ARE filler/hallucination/breath — exactly
            # what the user wants cut.
            all_unmatched_for_guard = [
                w for w in all_words
                if w.get('start') not in matched_start_set
            ]
            all_unmatched_for_guard.sort(key=lambda w: w.get('start', 0))

            contained_groups = []
            for (gs, ge) in speech_groups:
                inside_matched = [
                    w for w in speech_words
                    if (gs <= w.get('start', -1) < ge) or (gs < w.get('end', -1) <= ge)
                ]
                if not inside_matched:
                    print(f"[WhisperVAD] ⚠ v611: dropping unanchored segment {gs:.3f}s → {ge:.3f}s", flush=True)
                    continue

                first_matched = min(inside_matched, key=lambda w: w.get('start', 0))
                last_matched = max(inside_matched, key=lambda w: w.get('end', 0))
                lm_start = first_matched.get('start', gs)
                lm_end = last_matched.get('end', ge)

                new_start, new_end = gs, ge

                # End-cap: nearest unmatched word starting past lm_end and inside segment
                nearest_post = next(
                    (u for u in all_unmatched_for_guard
                     if u.get('start') is not None
                     and u['start'] >= lm_end
                     and u['start'] < new_end),
                    None
                )
                if nearest_post:
                    u_text = nearest_post.get('text', '?')
                    u_p = nearest_post.get('probability', 0)
                    # v708 — trim discipline gate. Reject filler if either:
                    #   (a) probability below V708_FILLER_MIN_PROB (untrusted)
                    #   (b) edit distance to ANY script word < V708_FILLER_MIN_EDIT_DIST
                    #       (likely a Whisper mis-spelling of a real script word)
                    # When rejected, fall through to fallback (lm_end + pad)
                    # which preserves audio safely.
                    _u_edit = _v708_min_edit_dist_to_script(u_text, _v708_script_set)
                    _u_reject = (u_p < V708_FILLER_MIN_PROB) or (_u_edit < V708_FILLER_MIN_EDIT_DIST)
                    if _u_reject:
                        print(
                            f"[WhisperVAD/v708] ✂ v611 end-cap REJECTED filler "
                            f"'{u_text}' p={u_p:.2f} edit_dist={_u_edit} "
                            f"(min_p={V708_FILLER_MIN_PROB:.2f} "
                            f"min_edit={V708_FILLER_MIN_EDIT_DIST}) → "
                            f"fall back to last-word+pad",
                            flush=True,
                        )
                        capped = min(new_end, lm_end + STRICT_FALLBACK_END_PAD)
                        if capped < new_end:
                            print(
                                f"[WhisperVAD] ✂ v611 end-cap (v708 fallback): "
                                f"{new_end:.3f}s → {capped:.3f}s",
                                flush=True,
                            )
                        new_end = capped
                    else:
                        min_end = lm_end + STRICT_MIN_END_TAIL
                        capped = max(min_end, nearest_post['start'] - STRICT_END_GUARD)
                        if capped < new_end:
                            print(f"[WhisperVAD] ✂ v611 end-cap: {new_end:.3f}s → {capped:.3f}s (filler '{u_text}' p={u_p:.2f} at {nearest_post['start']:.3f}s)", flush=True)
                        new_end = capped
                else:
                    capped = min(new_end, lm_end + STRICT_FALLBACK_END_PAD)
                    if capped < new_end:
                        print(f"[WhisperVAD] ✂ v611 end-cap (fallback): {new_end:.3f}s → {capped:.3f}s (no unmatched in tail, clamp to last-word + {STRICT_FALLBACK_END_PAD:.2f}s)", flush=True)
                    new_end = capped

                # Start-cap: nearest unmatched word ending before lm_start and inside segment
                pre_candidates = [
                    u for u in all_unmatched_for_guard
                    if u.get('end') is not None
                    and u['end'] <= lm_start
                    and u['end'] > new_start
                ]
                if pre_candidates:
                    nearest_pre = max(pre_candidates, key=lambda w: w.get('end', 0))
                    u_text = nearest_pre.get('text', '?')
                    u_p = nearest_pre.get('probability', 0)
                    # v708 — same trim-discipline gate on start-cap
                    _u_edit = _v708_min_edit_dist_to_script(u_text, _v708_script_set)
                    _u_reject = (u_p < V708_FILLER_MIN_PROB) or (_u_edit < V708_FILLER_MIN_EDIT_DIST)
                    if _u_reject:
                        print(
                            f"[WhisperVAD/v708] ✂ v611 start-cap REJECTED filler "
                            f"'{u_text}' p={u_p:.2f} edit_dist={_u_edit} → "
                            f"keep new_start={new_start:.3f}s unchanged",
                            flush=True,
                        )
                    else:
                        max_start = lm_start
                        pushed = min(max_start, nearest_pre['end'] + STRICT_START_GUARD)
                        if pushed > new_start:
                            print(f"[WhisperVAD] ✂ v611 start-cap: {new_start:.3f}s → {pushed:.3f}s (filler '{u_text}' p={u_p:.2f} at {nearest_pre['end']:.3f}s)", flush=True)
                        new_start = pushed

                if new_end > new_start:
                    contained_groups.append((new_start, new_end))
                else:
                    print(f"[WhisperVAD] ⚠ v611: segment collapsed {gs:.3f}s → {ge:.3f}s — keeping original", flush=True)
                    contained_groups.append((gs, ge))

            speech_groups = contained_groups

            # === v616a — mid-segment unbridge (split at intra-segment blockers) ===
            # User reported "extra frames added in the whisper exported final"
            # despite v611. Root cause: v611 only caps segment EDGES against
            # unmatched-word locations. It cannot remove a hallucination that
            # was BRIDGED INTO THE MIDDLE of a segment by the v548 bridger.
            #
            # Concrete leak path:
            #   matched word M1 at 5.0s
            #   hallucination H at 5.5s (conf 0.20 — below v548 HALLUC_PROB_FLOOR
            #     of 0.30, so v548 bridger doesn't block on it)
            #   matched word M2 at 6.0s
            #   v548 bridger sees gap M1.end(5.05) → M2.start(6.0) = 0.95s,
            #     within BRIDGE_GAP_MAX=0.7s? NO — 0.95 > 0.7. Bridger doesn't merge.
            #
            # Tighter case where v616a IS needed:
            #   M1 at 5.0s, M2 at 5.5s, H at 5.25s (conf 0.20).
            #   Gap M1.end(5.05) → M2.start(5.5) = 0.45s ≤ BRIDGE_GAP_MAX. Bridged.
            #   v611 sees one segment, looks past M2 for unmatched, doesn't
            #     see H (it's between the matched words, not past).
            #   H's audio ("um", "uh", breath, half-word fragment) survives
            #     in the middle of the segment.
            #
            # v616a fixes this: scan each contained segment for whisper words
            # (any conf ≥ UNBRIDGE_PROB_FLOOR=0.10) sitting between consecutive
            # matched words inside the segment. SPLIT the segment to exclude
            # them, producing two (or more) shorter segments with tight tails.
            #
            # Splitting (vs simply trimming) is the right move because the
            # matched words on each side of the blocker may be far apart in
            # source-time but should both stay in the export — we just want
            # to cut the blocker from between them.
            UNBRIDGE_PROB_FLOOR = 0.10
            unbridged_groups = []
            for (gs, ge) in speech_groups:
                inside_matched = sorted(
                    [w for w in speech_words
                     if w.get('start') is not None
                     and gs <= w['start'] < ge],
                    key=lambda w: w['start']
                )
                if len(inside_matched) <= 1:
                    unbridged_groups.append((gs, ge))
                    continue

                # Find blockers between consecutive matched words
                splits = []  # list of (cut_end, cut_resume_start) pairs
                for i in range(len(inside_matched) - 1):
                    m1 = inside_matched[i]
                    m2 = inside_matched[i + 1]
                    m1_end = m1.get('end', 0)
                    m2_start = m2.get('start', 0)
                    if m2_start <= m1_end:
                        continue  # touching or overlapping, no gap to scan
                    blockers = [
                        u for u in all_unmatched_for_guard
                        if u.get('start') is not None
                        and u.get('end') is not None
                        and u['start'] >= m1_end - 0.05
                        and u['end'] <= m2_start + 0.05
                        and u.get('probability', 1.0) >= UNBRIDGE_PROB_FLOOR
                    ]
                    if not blockers:
                        continue
                    cut_end = m1_end + STRICT_MIN_END_TAIL
                    cut_resume_start = m2_start - STRICT_START_GUARD
                    if cut_end < cut_resume_start:
                        splits.append((cut_end, cut_resume_start, blockers))

                if not splits:
                    unbridged_groups.append((gs, ge))
                    continue

                # Apply splits — emit (current_start, cut_end), then resume
                cur_start = gs
                for (cut_end, cut_resume_start, blockers) in splits:
                    if cut_end > cur_start:
                        unbridged_groups.append((cur_start, cut_end))
                    cur_start = cut_resume_start
                    blocker_summary = ", ".join(
                        f"'{b.get('text', '?')}'(p={b.get('probability', 0):.2f}@{b.get('start', 0):.2f}s)"
                        for b in blockers[:3]
                    )
                    print(f"[WhisperVAD] ✂ v616a unbridge: split at {cut_end:.3f}s → {cut_resume_start:.3f}s in segment [{gs:.3f}s, {ge:.3f}s] — blockers: {blocker_summary}", flush=True)
                if cur_start < ge:
                    unbridged_groups.append((cur_start, ge))

            speech_groups = unbridged_groups

            # === v616b — frame-snap segment boundaries to source video's frame grid ===
            # FFmpeg's -ss/-t with output seek (v499) is frame-accurate at the
            # decode level, but the requested timestamp can still land mid-
            # frame. When that happens libx264 may include an extra frame at
            # the boundary depending on encoder rounding. Snapping the segment
            # boundaries to the source's frame grid eliminates this entirely:
            # start snaps UP to the next frame boundary (we keep only complete
            # frames AFTER start), end snaps DOWN to the previous frame
            # boundary (we keep only complete frames BEFORE end).
            #
            # Source fps comes from ffprobe r_frame_rate (already read above
            # as `info`). Falls back to 24.0 if unparseable.
            src_fps = 24.0
            try:
                for stream in info.get("streams", []):
                    if stream.get("codec_type") == "video":
                        rf = stream.get("r_frame_rate", "24/1")
                        if "/" in rf:
                            num, den = rf.split("/", 1)
                            den_f = float(den)
                            if den_f > 0:
                                src_fps = float(num) / den_f
                        else:
                            src_fps = float(rf)
                        break
            except Exception:
                src_fps = 24.0

            import math as _math
            def _snap_ceil(t, fps):
                return _math.ceil(t * fps) / fps if fps > 0 else t

            def _snap_floor(t, fps):
                return _math.floor(t * fps) / fps if fps > 0 else t

            frame_snapped = []
            for (s, e) in speech_groups:
                s_snap = _snap_ceil(s, src_fps)
                e_snap = _snap_floor(e, src_fps)
                # If snapping collapses the segment (e.g. both round to the
                # same frame), keep the original to avoid a zero-length cut.
                # Pre-frame-snap segments < 1 frame are pathological anyway —
                # the matcher shouldn't produce them, but defensive.
                if e_snap > s_snap:
                    if abs(s_snap - s) > 0.001 or abs(e_snap - e) > 0.001:
                        print(f"[WhisperVAD] 🎬 v616b frame-snap: ({s:.3f}s, {e:.3f}s) → ({s_snap:.3f}s, {e_snap:.3f}s) @ {src_fps:.3f}fps", flush=True)
                    frame_snapped.append((s_snap, e_snap))
                else:
                    frame_snapped.append((s, e))

            speech_groups = frame_snapped

            # === v701p — 1-frame breathing room around each kept segment ===
            # v616b's frame-snap is correct for avoiding mid-frame cuts but
            # widens NOTHING — segments stay tight to the matched word's
            # acoustic edges. User-reported symptom: clipped onsets/offsets
            # (consonants like /t/ /k/ /s/ chopped). Add ONE source-frame on
            # each side, then merge segments whose widened intervals now
            # overlap or touch — so adjacent kept regions don't double-pad
            # the boundary and create a frame stutter. Bounded by the clip's
            # own [0, total_duration] window to prevent negative starts /
            # overshoots that would later be clamped silently.
            if src_fps > 0 and frame_snapped:
                _f = 1.0 / src_fps
                widened = []
                for (s, e) in frame_snapped:
                    ws = max(0.0, s - _f)
                    we = min(total_duration, e + _f)
                    widened.append((ws, we))
                # Merge any segments whose widened intervals overlap or
                # touch (prev_end >= curr_start). Touching counts as merge
                # to avoid a back-to-back zero-gap concat that produces a
                # one-frame visual stutter.
                merged_widened = []
                for (s, e) in sorted(widened):
                    if merged_widened and s <= merged_widened[-1][1]:
                        merged_widened[-1] = (
                            merged_widened[-1][0],
                            max(merged_widened[-1][1], e),
                        )
                    else:
                        merged_widened.append((s, e))
                if len(merged_widened) != len(widened):
                    print(
                        f"[WhisperVAD] 🫁 v701p widen+merge: {len(frame_snapped)} → "
                        f"{len(merged_widened)} segments (+{_f*1000:.0f}ms each side)",
                        flush=True,
                    )
                else:
                    print(
                        f"[WhisperVAD] 🫁 v701p widen: +{_f*1000:.0f}ms each side, "
                        f"{len(merged_widened)} segments unchanged",
                        flush=True,
                    )
                speech_groups = merged_widened

            result = speech_groups
            
            # Log results
            total_speech = sum(e - s for s, e in result)
            total_cut = total_duration - total_speech
            print(f"[WhisperVAD] Result: {len(result)} segments, {total_speech:.1f}s speech, {total_cut:.1f}s cut", flush=True)
            for i, (s, e) in enumerate(result):
                print(f"[WhisperVAD]   segment {i+1}: {s:.3f}s → {e:.3f}s ({e-s:.3f}s)")
            
            # v701y — only dispose if we created the model here.
            if _model_owned_here:
                del model
                import gc; gc.collect()
            return result
            
        except ImportError:
            print("[WhisperVAD] ❌ faster-whisper not installed — cannot detect speech")
            _v709_mark_sink_failed("faster_whisper unavailable")
            return [(0.0, total_duration)]
        except Exception as e:
            print(f"[WhisperVAD] ❌ Transcription error: {e}")
            import traceback; traceback.print_exc()
            # v709 — only overwrite sink if BEST-pass populate hasn't already run.
            # Sink dict will be empty {} if exception fired before BEST pass.
            if v709_audit_sink is not None and not v709_audit_sink:
                _v709_mark_sink_failed(f"whisper transcription error: {e}")
            return [(0.0, total_duration)]
    finally:
        import os
        if os.path.exists(audio_path):
            os.remove(audio_path)


def _locate_script_span(whisper_bucket: list, script_words: list,
                        fuzzy_threshold: float = 0.6,
                        script_skip_budget_pct: float = 0.10,
                        whisper_skip_budget_pct: float = 0.20) -> list:
    """v544 — strict contiguous-span locator with two-sided skip budgets.

    The v542 locator handled filler BEFORE and AFTER the script line
    well, but broke when Whisper transcribed extra filler words IN
    THE MIDDLE of the script line. Real-world example from clip 1
    of the user's log: script says "...this purple leaf — listen to
    me" but Whisper heard "...this purple leaf PAIN listen to me"
    (Veo's TTS added "pain" mid-line). The v542 walk hit 'pain' at
    sj=12, didn't find it in the script, only knew how to skip the
    SCRIPT side, broke the walk, and lost 'listen to me' from the
    output.

    v544 adds two new abilities:
      • WHISPER-SKIP BUDGET: when whisper[wi] doesn't match script[sj],
        first try skipping whisper[wi] (look at whisper[wi+1] vs
        script[sj]) before giving up. Budget is 20% of script length
        by default — enough to absorb 1-2 mid-line filler words but
        not enough to swallow a whole hallucinated sentence.
      • MERGE-MATCH: when whisper[wi] fuzzy-matches script[sj] AND
        also matches script[sj] + script[sj+1] concatenated (e.g.
        whisper "cabbages" vs script "cabbage" + "is"), advance sj
        by 2 instead of 1. This is the canonical Whisper transcription
        artifact for fast TTS speech where the closing fricative of
        word N becomes the opening of word N+1.

    The algorithm is otherwise identical to v542. We still pick the
    run with the highest matched-word count, tie-broken by shortest
    span (less filler kept in the output range).
    """
    from difflib import SequenceMatcher

    if not whisper_bucket or not script_words:
        return []

    n = len(whisper_bucket)
    m = len(script_words)
    script_skip_budget = max(1, int(round(m * script_skip_budget_pct)))
    whisper_skip_budget = max(1, int(round(m * whisper_skip_budget_pct)))

    w_texts = [w['text'].strip().lower() for w in whisper_bucket]
    # Strip trailing punctuation for cleaner fuzzy comparison
    w_texts_clean = [t.rstrip('.,!?;:') for t in w_texts]

    def _sim(a: str, b: str) -> float:
        return SequenceMatcher(None, a, b).ratio()

    def _effective_threshold(script_word: str) -> float:
        """v548 Fix B — short script words need a much higher fuzzy
        threshold to prevent false-positive matches from Veo
        hallucinations.

        Concrete failure: script's 'it' (2 chars) fuzzy-matches
        Whisper's hallucinated "it's," at ratio 0.67 — above the
        default 0.60 threshold — so the matcher accepts the
        hallucination as the script anchor and the real 'it'
        spoken later gets discarded as filler. The 60-second
        garbage 'like it's,' between 'chunks' and 'carries' then
        survives into the final export because it's flanked by
        accepted matched words.

        For script words ≤ 3 chars, require ratio ≥ 0.85.
        Examples of what this rejects:
          - 'it' vs "it's" (0.67) → rejected
          - 'is' vs 'this' (0.57) → rejected (was already)
          - 'a' vs 'and' (0.50) → rejected (was already)
          - 'no' vs 'now' (0.67) → rejected
        Examples of what still passes:
          - 'it' vs 'it' (1.00) → accepted
          - 'no' vs 'no' (1.00) → accepted
          - 'is' vs 'is' (1.00) → accepted
        Effectively short-script words now require near-exact
        match, which is appropriate — there are very few legitimate
        spelling variants of 2-3 character function words.
        """
        if len(script_word) <= 3:
            return 0.85
        return fuzzy_threshold

    def _is_merge_match(whisper_token: str, s1: str, s2: str) -> bool:
        """Did Whisper merge two script words into one? E.g.
        'cabbages' for 'cabbage is'. We check that the concatenation
        sim is meaningfully higher than the single-word sim — and is
        itself above 0.75 to avoid spurious merges."""
        merged = s1 + s2
        merged_sim = _sim(whisper_token, merged)
        single_sim = _sim(whisper_token, s1)
        return merged_sim >= 0.75 and merged_sim > single_sim + 0.05

    best_score = 0.0
    best_indices = []
    best_span = None

    # Try every possible starting index in the bucket.
    for start_i in range(n):
        # Quick gate: first whisper word must fuzzy-match first
        # script word, or first 2 script words. If neither matches,
        # this start position can't yield a contiguous run.
        # v548 — use per-script-word effective threshold (short
        # words need ≥0.85 to prevent hallucination false positives).
        first_sim = _sim(w_texts_clean[start_i], script_words[0])
        if first_sim < _effective_threshold(script_words[0]):
            # Allow one-word slip: maybe Whisper missed script[0],
            # so try matching whisper[start_i] against script[1].
            if m < 2 or _sim(w_texts_clean[start_i], script_words[1]) < _effective_threshold(script_words[1]):
                continue

        # Walk forward with two skip budgets.
        wi = start_i
        sj = 0
        s_skips_used = 0
        w_skips_used = 0
        matched_indices = []

        while wi < n and sj < m:
            wt = w_texts_clean[wi]
            s_cur = script_words[sj]
            sim_score = _sim(wt, s_cur)
            cur_threshold = _effective_threshold(s_cur)  # v548

            if sim_score >= cur_threshold:
                # Direct match. v544 — also check for MERGE-MATCH
                # before advancing. If whisper[wi] is actually
                # script[sj] + script[sj+1] glued together, advance
                # script pointer by 2.
                if sj + 1 < m and _is_merge_match(wt, s_cur, script_words[sj + 1]):
                    matched_indices.append(wi)
                    wi += 1
                    sj += 2  # consumed two script words with one whisper token
                else:
                    matched_indices.append(wi)
                    wi += 1
                    sj += 1
                continue

            # No direct match. Try the three fallback paths in order:

            # Option A: skip the SCRIPT word (Whisper missed it).
            # If whisper[wi] matches script[sj+1], advance script.
            if s_skips_used < script_skip_budget and sj + 1 < m:
                next_script = script_words[sj + 1]
                next_script_sim = _sim(wt, next_script)
                if next_script_sim >= _effective_threshold(next_script):
                    sj += 1
                    s_skips_used += 1
                    continue

            # Option B (NEW v544): skip the WHISPER word (filler).
            # If whisper[wi+1] matches script[sj], advance whisper.
            # This is what handles 'pain' inserted between 'leaf'
            # and 'listen', and 'it is' between 'bad' and 'nothing'.
            if w_skips_used < whisper_skip_budget and wi + 1 < n:
                next_whisper_sim = _sim(w_texts_clean[wi + 1], s_cur)
                if next_whisper_sim >= cur_threshold:
                    wi += 1
                    w_skips_used += 1
                    continue

            # Option C (NEW v544): skip TWO whisper words (back-to-back
            # filler). Whisper[wi+2] matches script[sj]. This handles
            # the 'it is' case (two consecutive filler words).
            if (w_skips_used + 1 < whisper_skip_budget
                    and wi + 2 < n):
                two_ahead_sim = _sim(w_texts_clean[wi + 2], s_cur)
                if two_ahead_sim >= cur_threshold:
                    wi += 2
                    w_skips_used += 2
                    continue

            # All fallbacks exhausted. The run is broken.
            break

        # Score this candidate run.
        if matched_indices:
            # v544 — score by ACTUAL ANCHOR COUNT (number of whisper
            # words that audibly match the script), not by sj (script
            # coverage which counts script-skips for free).
            #
            # Why: with sj scoring, a run that script-skips the first
            # word (e.g. 'if' p=0.14) and matches 14 others scores 15/17
            # — IDENTICAL to a run that ANCHORS all 15 including 'if'.
            # Tie-break by shortest span then favours the run that
            # SKIPPED 'if' because it has a tighter footprint. We want
            # the opposite: the run with more audible anchors wins,
            # even if its span is longer, because more anchors = more
            # confidence the audio actually contains the script.
            run_score = len(matched_indices) / m
            run_span = matched_indices[-1] - matched_indices[0] + 1

            # Require that we covered at least 70% of the script via
            # actual anchors to consider this a valid locate. Below
            # that, fall back to DP.
            if run_score >= 0.70:
                # Prefer:
                #   1. Higher anchor count (run_score)
                #   2. On tie: EARLIER start_i (includes more script-
                #      head, especially script[0] which is the strongest
                #      acoustic anchor and the one we'd lose otherwise)
                #   3. On tie: shorter span (less filler swept in)
                if best_indices:
                    best_start = best_indices[0]
                else:
                    best_start = None
                better = (run_score > best_score) or (
                    run_score == best_score and (
                        best_start is None
                        or matched_indices[0] < best_start
                        or (matched_indices[0] == best_start
                            and (best_span is None or run_span < best_span))
                    )
                )
                if better:
                    best_score = run_score
                    best_indices = matched_indices
                    best_span = run_span

    if not best_indices:
        return []

    return [whisper_bucket[i] for i in best_indices]


def _prune_span_boundaries(matched: list,
                            min_prob: float = 0.30,
                            max_word_dur: float = 1.5,
                            protect_first: bool = True,
                            protect_last: bool = True) -> list:
    """v544 — trim the head and tail of a matched span until both
    endpoints are clean.

    A word at the boundary is "dirty" if:
      • probability < min_prob (Whisper hallucinating, OR a real
        word delivered with low TTS confidence — the latter is a
        false positive we now defend against), OR
      • duration > max_word_dur (a 4-second "word" is the canonical
        Whisper-merged-hallucination signature)

    v544 changes:
      • The first/last word of the matched span is NEVER trimmed for
        low probability if protect_first/protect_last is True. Reason:
        the locator already validated that this word matches the
        script's first/last anchor. Trimming it because Whisper had
        low confidence would silently drop a legitimate script word
        — which is what cost us 'if' (p=0.14) in clip 1 of the v543
        export log. Duration-based trimming still applies even at
        anchors (a 4s word is hallucination regardless of position).
      • Mid-span words are still trimmed normally — this only applies
        at the boundaries.

    Trimming only happens at boundaries, never in the middle — we
    don't want to fragment a legitimate line just because one middle
    word came back low-confidence.

    Returns the trimmed list. Never returns an empty list if the
    input was non-empty (we always keep at least one word so the
    caller still has a span to use).
    """
    if not matched:
        return matched

    def _dur(w):
        return w.get('end', 0) - w.get('start', 0)

    def _is_dirty_for_prob(w):
        return w.get('probability', 1.0) < min_prob

    def _is_dirty_for_dur(w):
        return _dur(w) > max_word_dur

    def _is_dirty(w, anchor_protected: bool):
        # At a script-anchor position, only duration-dirty counts.
        # At a non-anchor position, both probability and duration
        # signals trigger trimming.
        if anchor_protected:
            return _is_dirty_for_dur(w)
        return _is_dirty_for_prob(w) or _is_dirty_for_dur(w)

    # Trim head. The first iteration treats matched[0] as anchor-
    # protected (when protect_first is True). Once we've moved past
    # it, subsequent positions are non-anchor and trim normally.
    head = 0
    while head < len(matched) - 1:
        is_first = (head == 0)
        anchor = is_first and protect_first
        if _is_dirty(matched[head], anchor):
            head += 1
        else:
            break

    # Trim tail. Same logic — last position is anchor-protected.
    tail = len(matched) - 1
    while tail > head:
        is_last = (tail == len(matched) - 1)
        anchor = is_last and protect_last
        if _is_dirty(matched[tail], anchor):
            tail -= 1
        else:
            break

    return matched[head:tail + 1]


def _align_dp(whisper_bucket: list, expected_words: list) -> list:
    """
    Align Whisper words to expected dialogue words using dynamic programming.
    
    Returns list of whisper word dicts that matched the expected sequence.
    
    This is a Needleman-Wunsch-style alignment that finds the globally optimal
    matching. It replaces all heuristics (lookahead, confirmation check, short-word
    exact match, skip penalty) with one clean algorithm.
    
    Scoring:
      - Match (fuzzy ≥ 0.6):  +1.0 (reward keeping a word)
      - Mismatch:             -0.3 (penalty for wrong match — never chosen over skip)
      - Skip whisper word:     0.0 (filler/hallucination — free to skip)
      - Skip expected word:   -0.1 (Whisper didn't hear it — small penalty)
    """
    from difflib import SequenceMatcher
    
    # Extract whisper texts once
    w_texts = []
    for w in whisper_bucket:
        t = w['text'].strip().lower()
        w_texts.append(t)
    
    n = len(w_texts)      # whisper words
    m = len(expected_words)  # expected words
    
    # Precompute similarity matrix (n x m)
    sim = []
    for i in range(n):
        row = []
        for j in range(m):
            row.append(SequenceMatcher(None, w_texts[i], expected_words[j]).ratio())
        sim.append(row)
    
    MATCH_REWARD = 1.0
    MISMATCH_PENALTY = -0.3
    SKIP_WHISPER = -0.001  # Near-zero cost — breaks ties toward fewer skips
    SKIP_EXPECTED = -0.1   # Small cost — Whisper missing a word is slightly bad
    MATCH_THRESHOLD = 0.6
    
    # DP table: dp[i][j] = best score aligning w_texts[:i] with expected[:j]
    # Traceback: 0=start, 1=match/mismatch(diagonal), 2=skip whisper(up), 3=skip expected(left)
    dp = [[0.0] * (m + 1) for _ in range(n + 1)]
    trace = [[0] * (m + 1) for _ in range(n + 1)]
    
    # Initialize: skipping expected words has cumulative penalty
    for j in range(1, m + 1):
        dp[0][j] = dp[0][j-1] + SKIP_EXPECTED
        trace[0][j] = 3
    # Skipping whisper words has tiny cost
    for i in range(1, n + 1):
        dp[i][0] = dp[i-1][0] + SKIP_WHISPER
        trace[i][0] = 2
    
    # Fill DP table
    for i in range(1, n + 1):
        for j in range(1, m + 1):
            # Option 1: align whisper[i-1] with expected[j-1]
            s = sim[i-1][j-1]
            # Positional bonus: prefer earlier matches (higher bonus for lower i)
            # This breaks ties when the same word appears twice in the bucket
            pos_bonus = (n - i + 1) * 0.0001
            match_score = dp[i-1][j-1] + ((MATCH_REWARD + pos_bonus) if s >= MATCH_THRESHOLD else MISMATCH_PENALTY)
            
            # Option 2: skip this whisper word (filler)
            skip_w = dp[i-1][j] + SKIP_WHISPER
            
            # Option 3: skip this expected word (Whisper didn't hear it)
            skip_e = dp[i][j-1] + SKIP_EXPECTED
            
            # Pick best
            if match_score >= skip_w and match_score >= skip_e:
                dp[i][j] = match_score
                trace[i][j] = 1
            elif skip_w >= skip_e:
                dp[i][j] = skip_w
                trace[i][j] = 2
            else:
                dp[i][j] = skip_e
                trace[i][j] = 3
    
    # Traceback: collect matched whisper indices
    matched_indices = set()
    i, j = n, m
    while i > 0 and j > 0:
        if trace[i][j] == 1:
            # Diagonal — only keep if it was a real match
            if sim[i-1][j-1] >= MATCH_THRESHOLD:
                matched_indices.add(i - 1)
            i -= 1
            j -= 1
        elif trace[i][j] == 2:
            i -= 1  # Skip whisper word
        else:
            j -= 1  # Skip expected word
    
    # Return matched whisper word dicts in order
    return [whisper_bucket[i] for i in sorted(matched_indices)]


def _match_in_order(whisper_bucket: list, script_words: list,
                    fuzzy_threshold: float = 0.80,
                    short_word_threshold: float = 0.95,
                    lookahead_window: int = 6) -> list:
    """v553 — strict in-order match. The single-purpose replacement for
    _locate_script_span + _align_dp.

    v596 — bounded lookahead window (default 6 whisper words per script
    word). Before v596 the matcher searched from ``wi`` to the END of
    the bucket; if a late-bleed whisper word (e.g. clip-3 audio bleeding
    into clip-2's window) happened to fuzzy-match a script word, ``wi``
    would jump arbitrarily far ahead and strand earlier valid matches
    behind the new pointer. Concrete failure observed: clip 2 of the
    2026-05-05 belly-fat-tonic decode — script "every man over forty
    hits this wall. metabolism quits. waistline doesn't" matched only
    3/11 because Whisper transcribed the clip-3-bleed "this" at j=9,
    advanced wi=10, then "wall" (j=2), "metabolism" (j=3), "quits"
    (j=4) were all stranded behind. With ``lookahead_window=8`` the
    same input yields ~5-6/11 matches because the late "this" is out
    of window, gets correctly classified as bleed/filler, and "wall"
    matches at j=2 with wi=2.

    Walk through ``script_words`` IN ORDER. For each script word, search
    forward in ``whisper_bucket`` from the current position UP TO
    ``wi + lookahead_window`` for a Whisper word whose lowercased text
    fuzzy-matches the script word above the threshold. When found, that
    Whisper word is kept and the search pointer advances past it.
    Whisper words between matches are NOT kept — they are filler /
    hallucination / Veo TTS lead-ins / cross-clip bleed that the user
    explicitly does not want in the export.

    Window sizing:
      A typical Veo TTS clip has 2-4 fast function words ("a", "the",
      "to", "of") that Whisper occasionally drops. ``lookahead_window=6``
      absorbs those drops while preventing the wild wi-jumps that
      cross-clip bleed creates. Increase the window for clips with
      heavy whisper drops (high-music backgrounds); decrease for clips
      with severe bleed (Veo's audio tail spillover). The default
      handles 95%+ of observed cases on the 2026-05 corpus.

    Why this replaces the v542/v544 contiguous-span locator:
      The previous locator returned a CONTIGUOUS RANGE of indices and
      let "skip budgets" absorb non-script words inside that range.
      The skipped Whisper words still ended up inside the kept
      time-span because segment building uses the matched range's
      first start to last end. That meant Veo's "only" / "actually
      i'm so addicted" / "in your body" lead-ins all surfaced in the
      final audio export, exactly the failure the user is reporting.

      The in-order matcher returns ONLY the indices of Whisper words
      that actually correspond to user script words. Non-script
      audio between matches lives in unmatched_words and gets cut by
      the segment builder's gap detection. Far simpler. No skip
      budgets. No "best span" scoring. No edge cases.

    Short-word handling:
      Words ≤ 3 chars need a stricter threshold (0.95) to prevent
      false positives like "it" matching "it's", or "is" matching
      "this". This is the same v548 Fix B logic from the old
      locator, kept here.

    Whisper-missed-word handling:
      If a script word can't be found within the lookahead window,
      the loop just skips that script word and continues to the next.
      Whisper occasionally drops fast function words; we don't try to
      bridge over the drop. With v596's bounded window, dropping a
      script word does NOT advance wi — the next script word still
      gets the full window from the same wi position.

    Returns the matched Whisper word dicts in time order.
    """
    from difflib import SequenceMatcher

    if not whisper_bucket or not script_words:
        return []

    # Pre-clean Whisper words for comparison (lowercase, strip punctuation
    # everywhere — leading, trailing, AND internal apostrophes/quotes).
    # The script side has internal punctuation stripped via
    # re.sub(r'[^\w\s]', '', line) at preprocessing in apply_vad, so e.g.
    # script "it's" becomes "its". To match consistently, do the same to
    # whisper here. Without this, whisper "it's" (sim=0.857 vs script
    # "its") fails the short-word threshold of 0.95 and the script word
    # is dropped — a real correctness issue observed on clip 5 of the
    # Nuri ED export.
    #
    # v554 — number normalization. The matcher previously failed when
    # the script said "twenty" or "twenty-five" but Whisper transcribed
    # "20" or "25" (Whisper's default behavior on numerals). Concrete
    # failure observed: in clip 1 of the Nuri ED export, script said
    # "...he was twenty-five again" but Whisper transcribed "25" — the
    # number sat between matched 'he\'s' (5.80s) and 'again' (6.42s)
    # as an unmatched hallucination, and the bridger refused to merge
    # over it, splitting the segment in half and cutting the spoken
    # "25" out of the export. Same failure in clip 8 with "twenty"
    # ("in over twenty years" vs Whisper's "20").
    #
    # Fix: normalize digits to spelled-out words on the Whisper side
    # before matching. We do digits→words (not the reverse) because
    # the script is author-controlled and almost always spells numbers
    # out for prosody control; Whisper's digit transcription is the
    # variable we need to align to that. Covers 0-99 which handles
    # nearly all human-spoken numbers in this domain (years, ages,
    # quantities). For numbers ≥100 we leave the digit form alone —
    # the matcher will still try a fuzzy match and may pass, and the
    # rare miss is acceptable.
    import re as _re
    _ONES = ['zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven',
             'eight', 'nine', 'ten', 'eleven', 'twelve', 'thirteen',
             'fourteen', 'fifteen', 'sixteen', 'seventeen', 'eighteen',
             'nineteen']
    _TENS = ['', '', 'twenty', 'thirty', 'forty', 'fifty', 'sixty',
              'seventy', 'eighty', 'ninety']
    def _digits_to_words(token: str) -> str:
        """Convert a numeric token like '25' → 'twentyfive'.
        Returns the original token unchanged if not a pure-digit number
        in 0-99 range."""
        if not token.isdigit():
            return token
        n = int(token)
        if n < 0 or n > 99:
            return token
        if n < 20:
            return _ONES[n]
        tens, ones = divmod(n, 10)
        if ones == 0:
            return _TENS[tens]
        # "twenty-five" — strip the hyphen since the punctuation strip
        # below would do the same. Result: "twentyfive".
        return _TENS[tens] + _ONES[ones]

    def _normalize(t):
        # Strip all non-word/non-whitespace chars (apostrophes, commas,
        # hyphens, periods) — same as before. So "twenty-five" → "twentyfive".
        cleaned = _re.sub(r"[^\w\s]", '', t.strip().lower())
        # If the result is a pure-digit token, expand to words to match
        # the script's spelled-out form.
        return _digits_to_words(cleaned)
    w_clean = [_normalize(w.get('text', '')) for w in whisper_bucket]

    kept_indices = []
    wi = 0  # whisper search pointer

    for s_word in script_words:
        s_clean = _normalize(s_word)
        if not s_clean:
            continue
        threshold = short_word_threshold if len(s_clean) <= 3 else fuzzy_threshold

        # v596: bounded lookahead — search whisper[wi : wi+lookahead_window]
        # NOT whisper[wi : end]. Prevents wi from jumping arbitrarily far
        # ahead when a cross-clip-bleed whisper word happens to fuzzy-match
        # a script word, stranding earlier valid script words behind the
        # advanced pointer.
        search_end = min(len(whisper_bucket), wi + lookahead_window)
        for j in range(wi, search_end):
            sim = SequenceMatcher(None, w_clean[j], s_clean).ratio()
            if sim >= threshold:
                kept_indices.append(j)
                wi = j + 1
                break
        # If not found within the window, the loop falls through to the
        # next script word without touching wi or kept_indices — Whisper
        # either missed this script word OR the audio is out of window
        # (likely cross-clip bleed). The next script word still picks up
        # from the same wi with a fresh window.

    return [whisper_bucket[i] for i in kept_indices]



def _match_whisper_to_dialogue(whisper_words: list, per_clip_words: list,
                                clip_boundaries: list = None,
                                cut_prefix_audio: bool = False,
                                prefix_word: str = "only") -> list:
    """
    Match Whisper words against dialogue script using time-partitioned alignment.

    1. Partition Whisper words into buckets by clip_boundaries (time ranges)
    2. For each bucket, try the strict contiguous-span locator first
       (v542 — _locate_script_span). If it finds a high-quality match,
       use it. Otherwise fall back to DP alignment (legacy behavior).
    3. Prune low-probability and over-long words from the head/tail of
       the matched span (v542 — _prune_span_boundaries).
    4. Boundary attribution pass: reject matched words whose Whisper
       timestamps fall outside the official clip range (v496 — prevents
       filler from clip N leaking into clip N+1 via the boundary bleed)
    5. v542 — when ``cut_prefix_audio`` is True and the script line
       starts with ``prefix_word``, drop the matched prefix word from
       the kept output. The prefix served its purpose (giving Veo a
       throwaway warm-up onset and the matcher a strong first-word
       anchor); now its audio is excluded from the export.

    Falls back to flat DP alignment if no clip_boundaries provided.
    """
    if not clip_boundaries or len(clip_boundaries) != len(per_clip_words):
        print(f"[WhisperVAD] ⚠ No clip boundaries — using flat DP alignment", flush=True)
        all_expected = []
        for cw in per_clip_words:
            all_expected.extend(cw)
        return _align_dp(whisper_words, all_expected)

    matched = []
    total_low_conf = 0
    total_cross_boundary_rejected = 0
    total_locator_hits = 0  # v542 — track how often the strict locator wins
    total_pruned = 0  # v542 — boundary words trimmed for low-prob/long-dur
    total_prefix_dropped = 0  # v542 — prefix words excluded from kept audio
    total_added_clips = 0  # v556 — clips bypassed because they're user-added uploads

    # v556 — placeholder text used by the lineup-upload endpoint at
    # main.py line 2972. When the user adds a clip via the UI, the
    # default dialogue_text is "(uploaded clip)". After preprocessing
    # in apply_vad (re.sub(r'[^\w\s]', '', line.lower())), the parens
    # are stripped and clip_words becomes ["uploaded", "clip"].
    # Detect this exactly so we can bypass script matching for added
    # clips and instead keep ALL whisper words from the clip's bucket
    # (segment builder will then cut only true silences between them).
    UPLOADED_PLACEHOLDER_TOKENS = ("uploaded", "clip")

    # v547 — widened from v544's 0.5s to 1.0s. Real-world Veo TTS
    # drift across clip boundaries can be up to 0.8s in either
    # direction (observed: 'comment' rendered at ~68.8s when clip 10
    # starts at 69.6s — exactly 0.8s of pre-rendering). The 0.5s pad
    # missed it. The strict locator's contiguous-match requirement
    # means a wider bucket window doesn't reintroduce filler bleed —
    # words can only be matched if they form a contiguous run with
    # the script's words, so a stray earlier-clip word can't sneak
    # in unless it happens to also be the start of this clip's
    # script line (which would mean it's correctly attributed).
    BUCKET_PAD_START = 1.0
    BUCKET_PAD_END = 0.5
    # Boundary attribution tolerance — a matched word's midpoint is
    # accepted if it falls within this many seconds of the clip range.
    # Slightly smaller than the bucket pad to be strictly stricter.
    ATTRIBUTION_TOLERANCE = 0.15
    # v547 — per-clip first-word tolerance widened to match new
    # BUCKET_PAD_START. The matcher already verified the word is
    # the clip's first script word; the wider tolerance just lets
    # us accept its physical timestamp even if Veo rendered the
    # word up to 1 second early.
    FIRST_WORD_PRE_TOLERANCE = 1.0
    LOW_CONFIDENCE_THRESHOLD = 0.5

    prefix_word_norm = (prefix_word or "").strip().lower()

    for clip_idx in range(len(per_clip_words)):
        clip_words = per_clip_words[clip_idx]
        if not clip_words:
            continue

        clip_start, clip_end = clip_boundaries[clip_idx]

        # Partition: get Whisper words within this clip's time range
        # (v496: tightened from 0.5/1.0 to prevent cross-clip filler bleed)
        bucket = [w for w in whisper_words
                  if clip_start - BUCKET_PAD_START <= w.get('start', 0) <= clip_end + BUCKET_PAD_END]

        if not bucket:
            print(f"[WhisperVAD]   clip {clip_idx+1}: no whisper words in range {clip_start:.1f}-{clip_end:.1f}s → skipped", flush=True)
            continue

        # v556 — added-clip bypass. When the user uploads a clip via
        # the lineup UI (POST /api/jobs/{job_id}/lineup/upload at
        # main.py line 2967), the default dialogue_text is "(uploaded clip)"
        # — a placeholder because the upload didn't come with a script.
        # After preprocessing in apply_vad (re.sub(r'[^\w\s]', '', line.lower())),
        # the parens are stripped and clip_words becomes ["uploaded", "clip"].
        # The audio in these clips is whatever the user wanted (already
        # correct from their perspective); the script matcher would
        # produce garbage by trying to find "uploaded" or "clip" in
        # the actual speech.
        #
        # Detection: clip_words is exactly ["uploaded", "clip"]. This
        # is a unique fingerprint — no legitimate scripted line will
        # ever produce this exact pair after preprocessing, so there
        # are no false-positive risks.
        #
        # Behavior on detect: bypass the script matcher entirely.
        # Take ALL whisper words from this clip's bucket whose
        # confidence is ≥ 0.30 (filters out Whisper grasping at
        # silence/breath, but keeps every real spoken word). Those
        # become the matched-words for segment building, which then
        # naturally pads/groups/bridges them while cutting silence
        # gaps ≥ 0.7s between them. Result: every spoken word from
        # the user's added clip is preserved, only true silences are
        # cut.
        clip_words_lower = tuple(w.lower() for w in clip_words)
        is_added_clip = (
            len(clip_words) == len(UPLOADED_PLACEHOLDER_TOKENS)
            and clip_words_lower == UPLOADED_PLACEHOLDER_TOKENS
        )

        if is_added_clip:
            total_added_clips += 1
            # Take all whisper words from bucket with confidence ≥ 0.30.
            # Filter: drop only the very-low-confidence Whisper-grasping-
            # at-silence tokens. Everything spoken is kept.
            bypass_words = [
                w for w in bucket
                if w.get('probability', 0) >= 0.30
            ]
            # Boundary attribution: only keep words whose midpoint is
            # actually inside the clip's range (with tolerance). Without
            # this, neighbor-clip bleed could pull words from clip N-1
            # or N+1 into this added clip.
            attrib_lo = clip_start - ATTRIBUTION_TOLERANCE
            attrib_hi = clip_end + ATTRIBUTION_TOLERANCE
            bypass_attributed = []
            for w in bypass_words:
                mid = (w.get('start', 0) + w.get('end', 0)) / 2.0
                if attrib_lo <= mid <= attrib_hi:
                    bypass_attributed.append(w)
            # Annotate clip_idx for downstream padding logic
            for w in bypass_attributed:
                w['clip_idx'] = clip_idx
            matched.extend(bypass_attributed)

            # Build the per-clip status log line. No "script X/Y"
            # number because there was no real script to match against.
            raw_text = " ".join([w['text'].strip() for w in bucket])
            print(f"[WhisperVAD]   clip {clip_idx+1} ({clip_start:.1f}-{clip_end:.1f}s): "
                  f"📦 added-clip bypass | kept {len(bypass_attributed)}/{len(bucket)} whisper words "
                  f"(matcher skipped — no real script)", flush=True)
            print(f"[WhisperVAD]     heard: {raw_text[:120]}", flush=True)
            continue

        # v553 — in-order strict matcher replaces the v542 locator + DP
        # fallback. Walks user's script in order, consumes whisper bucket
        # in order, exact-or-tight-fuzzy match per word. Filler / lead-ins
        # / padded-line Veo filler / hallucinations between matches are
        # NOT in the returned list, so segment building won't include
        # their audio in the export. See _match_in_order for full rationale.
        clip_matched = _match_in_order(bucket, clip_words)
        used_locator = bool(clip_matched)
        if used_locator:
            total_locator_hits += 1

        # v542 — prune over-long boundary words from the matched span.
        # v544 — when the strict locator found this span, every word
        # in it is a verified script anchor; we trust low-probability
        # but real words like the very-first 'if' (p=0.14, but a real
        # script word). Only the DURATION outlier filter applies in
        # that case (a 3-second "word" is still a hallucination
        # signature regardless of how the matcher found it). When DP
        # fallback was used the boundaries are softer, so apply the
        # full prob+duration filter.
        before_prune = len(clip_matched)
        if used_locator:
            clip_matched = _prune_span_boundaries(clip_matched, min_prob=0.0)
        else:
            clip_matched = _prune_span_boundaries(clip_matched)
        total_pruned += (before_prune - len(clip_matched))

        # v496: boundary attribution pass. DP alignment can match a
        # bucket word even if that word physically occurred in the
        # previous clip — the word text just happens to match a token
        # in the current clip's script. Reject any matched word whose
        # midpoint falls outside the clip's true range (with small
        # tolerance).
        # v544 — the FIRST matched word of each clip gets a wider
        # pre-clip tolerance (FIRST_WORD_PRE_TOLERANCE) because Veo's
        # TTS often renders the opening of a new line late in the
        # previous clip's audio. The locator already verified this
        # word matches the clip's first script word, so we trust the
        # attribution even if the timestamp is up to 1s pre-boundary.
        # v547 — the wider tolerance extends to ALL contiguously-pre-
        # boundary matched words, not just the literal first one. When
        # Veo renders the first few words of a new line early (e.g.
        # 'comment yes' at 68.8s when clip 10 starts at 69.6), the
        # locator correctly matches both — but the old strict-after-
        # first attribution would reject 'yes' because its midpoint
        # is just barely pre-boundary. Now the widened tolerance walks
        # forward from idx=0 until we hit a word that's actually inside
        # the clip range, giving each contiguous early word the same
        # trust as the first.
        attrib_lo_strict = clip_start - ATTRIBUTION_TOLERANCE
        attrib_lo_first = clip_start - FIRST_WORD_PRE_TOLERANCE
        attrib_hi = clip_end + ATTRIBUTION_TOLERANCE

        # Find the index of the first matched word whose midpoint is
        # at or after clip_start. All matched words BEFORE that index
        # are eligible for the wider FIRST_WORD tolerance.
        first_in_clip_idx = len(clip_matched)  # default: all pre-boundary
        for idx, w in enumerate(clip_matched):
            mid = (w.get('start', 0) + w.get('end', 0)) / 2.0
            if mid >= clip_start:
                first_in_clip_idx = idx
                break

        attributed = []
        cross_boundary = 0
        for idx, w in enumerate(clip_matched):
            mid = (w.get('start', 0) + w.get('end', 0)) / 2.0
            # Words at idx < first_in_clip_idx are the contiguous
            # pre-boundary block — they use the wider tolerance.
            lo = attrib_lo_first if idx < first_in_clip_idx else attrib_lo_strict
            if lo <= mid <= attrib_hi:
                attributed.append(w)
            else:
                cross_boundary += 1
        total_cross_boundary_rejected += cross_boundary

        # v542 — cut prefix audio. When the user enabled the prefix-
        # short-lines feature, the script line passed to us starts
        # with the prefix word (e.g. "only this is your problem").
        # The prefix served as the matcher's anchor; now strip its
        # audio from what we keep. Only drop if the FIRST attributed
        # word fuzzy-matches the prefix word — never drop a content
        # word by accident.
        if (cut_prefix_audio and prefix_word_norm and attributed
                and clip_words and clip_words[0] == prefix_word_norm):
            from difflib import SequenceMatcher
            first_text = attributed[0].get('text', '').strip().lower()
            if SequenceMatcher(None, first_text, prefix_word_norm).ratio() >= 0.7:
                attributed = attributed[1:]
                total_prefix_dropped += 1

        # v496: count low-confidence words (kept, but worth warning about)
        low_conf = [w for w in attributed
                    if w.get('probability', 1.0) < LOW_CONFIDENCE_THRESHOLD]
        total_low_conf += len(low_conf)

        # v498: annotate each attributed word with its clip_idx so the
        # downstream padding logic in apply_vad knows which clip a word
        # belongs to — based on SCRIPT match, not Whisper timestamp.
        for w in attributed:
            w['clip_idx'] = clip_idx

        matched.extend(attributed)

        # v496: rewritten log + v542: locator/prune/prefix indicators
        raw_text = " ".join([w['text'].strip() for w in bucket])
        script_matched = len(attributed)
        total_bucket = len(bucket)
        total_expected = len(clip_words)
        filler_discarded = total_bucket - len(clip_matched)

        missing_script = total_expected - script_matched
        # v542 — when the prefix was intentionally dropped, don't
        # report it as a "missing" word in the script.
        prefix_was_dropped = (cut_prefix_audio and prefix_word_norm
                              and clip_words and clip_words[0] == prefix_word_norm)
        if prefix_was_dropped:
            missing_script -= 1

        script_status = "✓" if missing_script == 0 else f"⚠ -{missing_script}"
        parts = [f"{script_status} script {script_matched}/{total_expected}"]
        if used_locator:
            parts.append("📍 in-order")
        if filler_discarded > 0:
            parts.append(f"✂ {filler_discarded} filler")
        if cross_boundary > 0:
            parts.append(f"⊘ {cross_boundary} cross-boundary")
        if low_conf:
            lc_words = ",".join([f"'{w['text']}'" for w in low_conf[:3]])
            parts.append(f"⚠ {len(low_conf)} low-conf ({lc_words})")
        if prefix_was_dropped:
            parts.append(f"🚫 prefix '{prefix_word_norm}' cut")
        summary = " | ".join(parts)
        print(f"[WhisperVAD]   clip {clip_idx+1} ({clip_start:.1f}-{clip_end:.1f}s): {summary}", flush=True)
        print(f"[WhisperVAD]     heard: {raw_text[:120]}", flush=True)

    # Final summary
    total_expected_all = sum(len(cw) for cw in per_clip_words)
    total_matched = len(matched)
    # v556 — added clips' "expected" count was based on placeholder
    # text ("uploaded clip" = 2 words) which has nothing to do with the
    # actual content. Subtract those from the unmatched count so the
    # summary doesn't falsely report missing script words.
    added_clip_placeholder_words = total_added_clips * len(UPLOADED_PLACEHOLDER_TOKENS) if total_added_clips else 0
    missing_all = total_expected_all - total_matched - total_prefix_dropped - added_clip_placeholder_words
    if missing_all < 0:
        missing_all = 0  # safety: short-script bypass detected via size, not placeholder
    if (missing_all > 0 or total_low_conf > 0 or total_cross_boundary_rejected > 0
            or total_locator_hits > 0 or total_pruned > 0 or total_prefix_dropped > 0
            or total_added_clips > 0):
        bits = []
        # v553 — in-order match hit-rate is the main quality signal
        if total_locator_hits > 0:
            bits.append(f"📍 {total_locator_hits}/{len(per_clip_words)} clips via in-order matcher")
        # v556 — added-clip bypasses
        if total_added_clips > 0:
            bits.append(f"📦 {total_added_clips} added-clip bypass")
        if total_pruned > 0:
            bits.append(f"✂ {total_pruned} boundary words pruned")
        if total_prefix_dropped > 0:
            bits.append(f"🚫 {total_prefix_dropped} prefix words cut from audio")
        if missing_all > 0:
            bits.append(f"⚠ {missing_all} script words not matched")
        if total_cross_boundary_rejected > 0:
            bits.append(f"⊘ {total_cross_boundary_rejected} cross-boundary rejected")
        if total_low_conf > 0:
            bits.append(f"{total_low_conf} low-confidence kept")
        print(f"[WhisperVAD] Summary: {total_matched}/{total_expected_all} script words matched"
              + ("  |  " + "  |  ".join(bits) if bits else ""), flush=True)

    return matched


# v709 — REMOVED `audit_final_export_words`. Post-concat Whisper-small
# re-transcribe produced false-positive missing words on TTS audio operator
# could hear clearly. See v709 aggregation in process_video for the
# replacement; per-clip Whisper-tiny pass now feeds final stats directly.


def apply_vad(
    src: Path,
    out: Path,
    threshold: float = 0.5,
    min_gap_duration: float = 1.0,
    silence_keep_duration: float = 0.3,
    silence_mode: str = "energy",
    progress_callback=None,
    dialogue_texts: List[str] = None,
    language: str = "English",
    clip_boundaries: List[Tuple[float, float]] = None,
    cut_prefix_audio: bool = False,  # v542
    prefix_word: str = "only",  # v542
    whisper_model=None,  # v701y — pre-loaded model from caller; skips 11x reload in per-clip loop
    v709_audit_sink: dict = None,  # v709 — caller-owned dict to receive per-clip audit
) -> dict:
    """
    Remove non-dialogue segments using Voice Activity Detection.
    Returns stats about the processing.

    Args:
        threshold:             VAD sensitivity (0-1). Higher = more aggressive detection.
        min_gap_duration:      Silence gaps shorter than this are ignored (seconds).
                               Only gaps >= this value are candidates for trimming.
        silence_keep_duration: How much silence to preserve at each detected cut point
                               (seconds). Split evenly: half retained before the next
                               speech burst, half after the previous one.
                               0.0 = cut tight to speech edges.
                               0.6 = leave a natural breath-length pause.
        silence_mode:          "energy" = ffmpeg silencedetect, "whisper" = speech-based.
        cut_prefix_audio:      v542 — when True and prefix_word matches the first
                               script word of a clip, that word's audio is excluded
                               from the kept range (used to silently strip the
                               "only" warm-up onset added by the prefix-short-lines
                               feature). The matcher still uses the prefix as an
                               anchor; only the audio is dropped.
        prefix_word:           v542 — which word to recognise as the prefix.
    """
    # Derive symmetric padding from silence_keep
    half_keep = silence_keep_duration / 2.0
    padding_before = half_keep
    padding_after  = half_keep

    info = ffprobe_json(src)
    original_duration = get_duration(info)
    
    if progress_callback:
        progress_callback("Analyzing audio for speech...")
    
    # Detect speech segments — route based on mode
    if silence_mode == "whisper":
        print(f"[VAD] Using Whisper speech detection (mode=whisper)", flush=True)
        speech_segments = detect_speech_segments_whisper(
            src,
            min_silence_duration=min_gap_duration,
            padding=max(padding_before, padding_after),
            dialogue_texts=dialogue_texts,
            language=language,
            clip_boundaries=clip_boundaries,
            cut_prefix_audio=cut_prefix_audio,  # v542
            prefix_word=prefix_word,  # v542
            whisper_model=whisper_model,  # v701y — pass through
            v709_audit_sink=v709_audit_sink,  # v709 — pass through to populate caller's dict
        )
    else:
        speech_segments = detect_speech_segments(
            src,
            threshold=threshold,
            min_silence_duration=min_gap_duration,
            padding_before=padding_before,
            padding_after=padding_after
        )
    
    # === v852 — SILENT CLIPS ARE KEEP-PROTECTED ===
    # Operator 2026-07-14: "the silent clips where i will edit the music are
    # excluded because silent... the clips can either be exported full or
    # according to the original video decoded where they had already a
    # precise duration."
    #
    # Root cause: VAD keeps only DETECTED-SPEECH spans. A `speaker: silent`
    # scene (meme beat / caught-cam footage / b-roll the operator scores in
    # post) carries dialogue_text='' → contributes ZERO speech words (whisper
    # mode) and ZERO audio energy (energy mode) → its span never enters the
    # keep-segment list → the trim+concat filter graph DELETES the whole clip
    # from the final export. Silent beats vanished from the stitched video.
    #
    # Fix: any clip whose dialogue_text is empty gets its FULL boundary span
    # injected as a protected keep-segment before the merge. Both operator
    # options fall out of this one change:
    #   (a) FULL           — no cut_mode; the whole rendered clip survives.
    #   (b) DECODED length — `cut_mode: timeline` + `target_duration_s` already
    #       ffmpeg-trimmed the file to the decoded beat duration in _trim_one
    #       (v668) BEFORE concat, so the protected span IS that duration.
    # Silence-removal still runs normally inside SPOKEN clips.
    _protected_spans = []
    if clip_boundaries and dialogue_texts:
        for _ci, (_cs, _ce) in enumerate(clip_boundaries):
            _txt = (dialogue_texts[_ci] or "").strip() if _ci < len(dialogue_texts) else ""
            if not _txt and _ce > _cs:
                _protected_spans.append((_cs, _ce))
        if _protected_spans:
            print(
                f"[VAD/v852] protecting {len(_protected_spans)} silent clip(s) "
                f"from VAD removal: "
                f"{[(f'{s:.2f}', f'{e:.2f}') for s, e in _protected_spans]}",
                flush=True,
            )
            logger.info(
                f"v852: {len(_protected_spans)} silent clip span(s) keep-protected"
            )

    if not speech_segments and not _protected_spans:
        # No speech detected - just copy the file
        logger.warning("No speech detected in video")
        import shutil
        shutil.copy(src, out)
        return {
            "original_duration": original_duration,
            "final_duration": original_duration,
            "segments_found": 0,
            "silence_removed": 0
        }

    # v852.1 — ALL-SILENT job fast path (review note on v852): when there is no
    # speech at all and every clip is protected, the keep-list would cover the
    # whole timeline anyway. Copy instead of running trim+concat, which would
    # re-encode the entire video for a byte-identical result.
    if not speech_segments and _protected_spans:
        print(
            f"[VAD/v852.1] all-silent export ({len(_protected_spans)} clip(s)) "
            f"— copying concat as-is, no re-encode",
            flush=True,
        )
        import shutil
        shutil.copy(src, out)
        return {
            "original_duration": original_duration,
            "final_duration": original_duration,
            "segments_found": len(_protected_spans),
            "silence_removed": 0,
        }

    # v852 — merge the protected silent spans in with the speech spans so the
    # keep-list covers them; overlapping/adjacent spans coalesce below.
    speech_segments = list(speech_segments) + _protected_spans

    # Merge overlapping segments
    merged = []
    for start, end in sorted(speech_segments):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))

    # v597: snap segment boundaries to frame multiples BEFORE extraction.
    # Whisper-VAD timestamps come from word-end times (e.g. 4.470s, 7.970s,
    # 14.800s) and are sub-frame at 24fps (where each frame = 0.04167s).
    # Without snapping, segment extraction with libx264 + CFR has to dup or
    # drop a partial frame at each boundary. The dup/drop decision varies
    # subtly between segments — visible to the user as "tweaking frames"
    # at every concat cut.
    #
    # Snap: round start DOWN to nearest frame, round end UP. This widens
    # each segment by at most one frame on each side (≈40ms total) — well
    # under the silence_keep_duration padding, so no dialogue is lost.
    #
    # v629 fix: SKIP this snap when silence_mode == "whisper". The whisper
    # path already runs v616b inside detect_speech_segments_whisper, which
    # snaps boundaries TIGHTLY (ceil start / floor end). Running v597's
    # WIDENING snap (floor start / ceil end) afterwards re-introduces
    # silence frames at boundaries due to IEEE float quirks: 1/24 isn't
    # exactly representable, so e.g. 185/24 in float is 7.708333333333333,
    # and `floor(7.7083... / 0.04166...)` rounds DOWN to 184 — undoing
    # v616b's snap and re-adding 1 silence frame at the segment start.
    # User-visible symptom: random frames inserted at hard-cut boundaries
    # in the final video. The bug is reproducible: sum of v616b's reported
    # segment durations differs from the final ffmpeg-output duration by
    # exactly the count of float-error-affected boundaries.
    if silence_mode != "whisper":
        src_fps = get_fps(ffprobe_json(src))
        if src_fps and src_fps > 0:
            import math as _math
            frame_dur = 1.0 / src_fps
            snapped = []
            for start, end in merged:
                snap_start = _math.floor(start / frame_dur) * frame_dur
                snap_end = _math.ceil(end / frame_dur) * frame_dur
                snapped.append((max(0.0, snap_start), snap_end))
            merged = snapped

    total_speech = sum(end - start for start, end in merged)
    
    if progress_callback:
        progress_callback(f"Found {len(merged)} speech segments ({total_speech:.1f}s)")
    
    # === v617 — Single-pass trim+concat filter graph ===
    # User report: "the frames i was seeing are not extra frame after or
    # before the words or segments, are just extra frames added randomly,
    # so hard cuts are between them."
    #
    # Root cause: the prior 2-stage pipeline (per-segment extract files +
    # concat-demuxer re-encode) was inserting duplicate frames at segment
    # boundaries via TWO mechanisms:
    #   (a) Per-segment encode with `-r {fps} -vsync cfr -t {duration}`
    #       — when segment duration isn't an integer-frame multiple,
    #       libx264 pads with duplicate frames at the segment END to
    #       align to the integer-frame count. Even floating-point error
    #       in the v597/v616b frame-snap (e.g. 0.45000001s instead of
    #       exactly 0.450s) causes 1 extra frame per segment.
    #   (b) Concat demuxer + `-vsync cfr` re-encode — at every segment
    #       boundary, the encoder sees a PTS gap and resolves it by
    #       duplicating the last frame of segment N to maintain CFR
    #       across the boundary into segment N+1.
    #
    # The fix replaces both stages with ONE ffmpeg invocation using the
    # trim + concat filter graph. ffmpeg decodes the source ONCE, the
    # trim filter selects each segment's PTS range with frame-accurate
    # precision (no encode-side rounding), the concat filter joins them
    # with seamless PTS continuity (no boundary insertion possible
    # because there are no encoder boundaries — it's one continuous
    # filter graph), and the output is encoded ONCE with consistent
    # PTS throughout.
    #
    # No temporary segment files. No concat-demuxer. No CFR rounding
    # at boundaries. Just trim + concat in one pass.
    if progress_callback:
        progress_callback(f"Extracting and joining {len(merged)} segments (single-pass)...")

    # Build the filter_complex. For each (start, end), emit:
    #   [0:v]trim=start=S:end=E,setpts=PTS-STARTPTS[vN];
    #   [0:a]atrim=start=S:end=E,asetpts=PTS-STARTPTS[aN];
    # Then concat them: [v0][a0][v1][a1]...concat=n=N:v=1:a=1[outv][outa]
    filter_parts = []
    concat_inputs = []
    for idx, (start, end) in enumerate(merged):
        # v629: 9-decimal precision (was 6). Frame-snapped boundaries like
        # 185/24 = 7.708333333333... can't fit in 6 decimals exactly. The
        # truncation "7.708333" lands sub-frame, which means ffmpeg's trim
        # filter could include or exclude the boundary frame depending on
        # the comparison's float-round behavior. 9 decimals covers the
        # full IEEE-double precision so the trim boundary lands EXACTLY
        # on the source frame's PTS.
        filter_parts.append(
            f"[0:v]trim=start={start:.9f}:end={end:.9f},setpts=PTS-STARTPTS[v{idx}]"
        )
        filter_parts.append(
            f"[0:a]atrim=start={start:.9f}:end={end:.9f},asetpts=PTS-STARTPTS[a{idx}]"
        )
        concat_inputs.append(f"[v{idx}][a{idx}]")
    filter_parts.append(
        f"{''.join(concat_inputs)}concat=n={len(merged)}:v=1:a=1[outv][outa]"
    )
    filter_complex = ";".join(filter_parts)

    cmd = [
        FFMPEG_BIN, "-y",
        "-i", str(src),
        "-filter_complex", filter_complex,
        "-map", "[outv]",
        "-map", "[outa]",
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "18",
        "-pix_fmt", "yuv420p",
        "-threads", "1",
        "-c:a", "aac", "-b:a", "128k",
        "-video_track_timescale", "90000",
        # NOTE: no -vsync flag here. The concat filter outputs continuous
        # PTS internally; libx264 just encodes whatever frames it receives
        # at the source's native rate. No dup/drop decisions at boundaries
        # because there ARE no encoder-visible boundaries — one filter
        # graph, one encoder pass.
        str(out)
    ]

    code, _, err = run(cmd)
    if code != 0:
        raise RuntimeError(f"Failed to extract+concat segments (filter): {err[:500]}")

    # Get final duration
    final_info = ffprobe_json(out)
    final_duration = get_duration(final_info)

    # v633 — diagnostic: verify pre-speed concat frame count matches sum
    # of (segment_duration * src_fps). Mismatch → v617 concat is producing
    # extra frames somewhere (the "ghost frame" the user reports).
    try:
        src_fps_diag = get_fps(ffprobe_json(src)) or 24.0
        expected_frames = sum(round((e - s) * src_fps_diag) for s, e in merged)
        cnt_cmd = [FFMPEG_BIN, "-i", str(out), "-map", "0:v:0",
                   "-c", "copy", "-f", "null", "-"]
        cnt_code, cnt_stdout, cnt_stderr = run(cnt_cmd)
        actual_frames = None
        for line in (cnt_stderr or "").splitlines():
            if "frame=" in line:
                import re as _re
                m = _re.search(r"frame=\s*(\d+)", line)
                if m:
                    actual_frames = int(m.group(1))
        print(f"[v633] PRE-SPEED concat: expected {expected_frames} frames "
              f"(sum of segs × {src_fps_diag}fps), actual {actual_frames}, "
              f"duration {final_duration:.6f}s, segs={len(merged)}", flush=True)
        if actual_frames is not None and actual_frames != expected_frames:
            print(f"[v633] ⚠ FRAME COUNT MISMATCH: "
                  f"{actual_frames - expected_frames:+d} frames "
                  f"in pre-speed concat output. v617 trim/concat is leaking.",
                  flush=True)
    except Exception as _e:
        print(f"[v633] diagnostic failed (non-fatal): {_e}", flush=True)

    # v634b — dump actual frame PTS sequence at seg boundaries in pre-speed
    # concat. v634a's -read_intervals returned empty; switch to one bulk
    # ffprobe call returning all frame PTS, then filter Python-side around
    # each boundary. Also use modern field name `pts_time` (pkt_pts_time
    # is deprecated in newer ffprobe).
    try:
        all_pts_cmd = [FFPROBE_BIN, "-v", "error",
                       "-select_streams", "v:0",
                       "-show_entries", "frame=pts_time",
                       "-of", "csv=p=0",
                       str(out)]
        import subprocess as _sp
        r = _sp.run(all_pts_cmd, capture_output=True, text=True, timeout=60)
        raw_lines = [x.strip() for x in r.stdout.splitlines() if x.strip()]
        all_pts = []
        for x in raw_lines:
            try:
                all_pts.append(float(x))
            except ValueError:
                pass
        print(f"[v634b] total v-frames in concat: {len(all_pts)}, "
              f"stderr-tail: {(r.stderr or '')[-200:]}", flush=True)
        if all_pts:
            print(f"[v634b] first 5: {all_pts[:5]} | last 5: {all_pts[-5:]}", flush=True)
            # compute boundary PTSes (cumulative seg durations)
            cum = 0.0
            boundary_pts = []
            for i, (s, e) in enumerate(merged[:-1]):
                cum += (e - s)
                boundary_pts.append((i + 1, cum))
            for seg_num, b_pts in boundary_pts:
                nearby = [p for p in all_pts if abs(p - b_pts) < 0.21]
                print(f"[v634b] boundary after seg{seg_num} expected≈{b_pts:.4f}s "
                      f"| got {len(nearby)} frames: {[f'{p:.4f}' for p in nearby]}",
                      flush=True)
    except Exception as _e:
        print(f"[v634b] boundary PTS dump failed (non-fatal): {_e}", flush=True)

    return {
        "original_duration": original_duration,
        "final_duration": final_duration,
        "segments_found": len(merged),
        "silence_removed": original_duration - final_duration
    }


def trim_video(
    src: Path,
    out: Path,
    frames_start: int = 0,
    frames_end: int = 0
) -> None:
    """Trim frames from start and end of video.
    
    Always re-encodes to ensure frame-accurate cutting.
    Uses veryfast preset (crf18) and memory-optimized settings for Render.
    """
    print(f"[VideoProcessor] trim_video: {src} -> {out}")
    print(f"[VideoProcessor]   frames_start={frames_start}, frames_end={frames_end}")
    
    if not src.exists():
        raise RuntimeError(f"Source file does not exist: {src}")
    
    info = ffprobe_json(src)
    fps = get_fps(info)
    duration = get_duration(info)
    
    print(f"[VideoProcessor]   fps={fps}, duration={duration}")
    
    cut_start_seconds = frames_start / fps
    cut_end_seconds = frames_end / fps
    target_duration = max(0.1, duration - cut_start_seconds - cut_end_seconds)
    
    print(f"[VideoProcessor]   cut_start={cut_start_seconds:.6f}s, cut_end={cut_end_seconds:.6f}s, target_duration={target_duration:.6f}s")
    
    # Re-encode with memory-optimized settings for Render (512MB limit).
    # CRITICAL: -ss AFTER -i (output seeking) = frame-accurate cut.
    # -ss before -i snaps to nearest keyframe → A/V drift and sometimes
    # one extra frame from the previous keyframe bleeds into the output
    # (user-visible as a "ghost frame" right after a cut).
    # -vsync cfr + -async 1 lock audio to video within each clip.
    # NO -ar flag — keep native sample rate from source clips (48000Hz).
    # Forcing 44100 causes mismatch with audio enhancement (48000Hz) → drift.
    # v465: restored the documented output-seek behavior. Previously
    # -ss was placed BEFORE -i (input seek), contradicting the comment
    # and causing the "one frame added after a cut" artifact the user
    # reported. Output seek costs extra decode time but is frame-accurate.
    cmd = [
        FFMPEG_BIN, "-y",
        "-i", str(src),
        "-ss", f"{cut_start_seconds:.6f}",   # output seek — after -i — frame-accurate
        "-t", f"{target_duration:.6f}",
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "18",
        "-pix_fmt", "yuv420p",
        "-threads", "1",
        "-c:a", "aac", "-b:a", "128k",
        "-max_muxing_queue_size", "512",
        "-avoid_negative_ts", "make_zero",
        str(out)
    ]
    
    print(f"[VideoProcessor]   Running ffmpeg (veryfast crf18, low memory)...")
    code, _, err = run(cmd)
    if code != 0:
        print(f"[VideoProcessor]   ERROR: {err}")
        raise RuntimeError(f"Failed to trim video: {err}")
    print(f"[VideoProcessor]   trim_video completed")


def render_text_card(
    output_path: Path,
    caption: str,
    bg_color: str = "black",
    duration_s: float = 1.0,
    width: int = 720,
    height: int = 1280,
    fontsize: int = 56,
    fontcolor: str = "white",
) -> None:
    """v681 — render a solid-color clip with a centered drawtext caption.

    Used for `scene_type=text_card` clips ("2 months later…", state
    labels, transition cards). NO Veo dispatch — ffmpeg generates the
    clip directly. Output is mp4 H.264 + silent AAC track so concat
    with audio-bearing peers stays in sync.

    Args:
        output_path: target mp4 path.
        caption: text shown on screen, centered. Special drawtext chars
            (`'`, `:`, `\\`) are escaped before being passed to ffmpeg.
        bg_color: solid background color name OR hex (CSS-style).
        duration_s: clip duration in seconds.
        width / height: output dimensions; defaults match the platform's
            9:16 vertical rendering target.
        fontsize / fontcolor: caption styling.
    """
    safe_caption = (
        (caption or "")
        .replace("\\", "\\\\")
        .replace(":", r"\:")
        .replace("'", r"\\'")
    )
    print(
        f"[v681/text_card] caption={caption!r} bg={bg_color} dur={duration_s:.2f}s",
        flush=True,
    )
    cmd = [
        FFMPEG_BIN, "-y",
        "-f", "lavfi",
        "-i", f"color=c={bg_color}:s={width}x{height}:d={duration_s:.6f}:r=30",
        "-f", "lavfi",
        "-t", f"{duration_s:.6f}",
        "-i", "anullsrc=channel_layout=stereo:sample_rate=44100",
        "-vf", (
            f"drawtext=text='{safe_caption}':"
            f"fontsize={fontsize}:fontcolor={fontcolor}:"
            f"x=(w-text_w)/2:y=(h-text_h)/2"
        ),
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "20",
        "-c:a", "aac", "-b:a", "128k", "-shortest",
        "-movflags", "+faststart",
        str(output_path),
    ]
    code, _, err = run(cmd)
    if code != 0:
        raise RuntimeError(f"render_text_card failed: {err[:300]}")


def swap_audio_with_speed_match(
    visual_path: Path,
    audio_path: Path,
    output_path: Path,
    speed_min: float = 1.0,
    speed_max: float = 2.0,
) -> Tuple[float, float, str]:
    """v698A Phase 4 — broll-with-voice helper.

    Takes a silent (or muted) visual_pair clip + an audio_pair's clean
    voiceover audio, produces a single mp4 where:
      - The visual is speed-adjusted to match the audio duration
      - The audio is the voiceover (replacing the visual's original audio)

    Speed-match rules (per v698A spec):
      natural_factor = T_visual / T_audio
      if natural_factor < speed_min (1.0):
        → broll is SHORTER than audio. Clamp speed to 1.0 (no slowdown).
          Visual plays at original speed and ends; ffmpeg pads with the
          last frame frozen for the remaining audio duration.
      if speed_min ≤ natural_factor ≤ speed_max:
        → speed = natural_factor. Output duration ≈ T_audio. Perfect.
      if natural_factor > speed_max (2.0):
        → broll is LONGER than even 2x sped-up. Apply 2x speed-up.
          Output duration after 2x = T_visual / 2 > T_audio.
          ffmpeg -shortest cuts the broll tail when audio ends.

    Returns (speed_factor_applied, output_duration, mode_label) where:
      mode_label ∈ {'natural', 'min_clamped_freeze_tail',
                    'max_clamped_audio_cut'}

    Raises RuntimeError if the input files are missing or ffmpeg fails.
    """
    if not visual_path.exists():
        raise RuntimeError(f"visual file missing: {visual_path}")
    if not audio_path.exists():
        raise RuntimeError(f"audio file missing: {audio_path}")

    t_visual = get_duration(ffprobe_json(visual_path))
    t_audio = get_duration(ffprobe_json(audio_path))
    if t_visual <= 0 or t_audio <= 0:
        raise RuntimeError(
            f"swap_audio_with_speed_match: invalid durations "
            f"visual={t_visual} audio={t_audio}"
        )

    natural_factor = t_visual / t_audio

    if natural_factor < speed_min:
        # Audio longer than visual. Don't slow visual. Visual plays at
        # 1.0x and freezes its last frame for the remaining audio time.
        speed = speed_min  # 1.0 — no setpts adjustment
        mode_label = "min_clamped_freeze_tail"
    elif natural_factor > speed_max:
        # Visual much longer than audio. Speed up to max, then cut tail.
        speed = speed_max
        mode_label = "max_clamped_audio_cut"
    else:
        # Natural fit — visual matches audio exactly when sped to factor.
        speed = natural_factor
        mode_label = "natural"

    print(
        f"[v698A/swap] visual={t_visual:.3f}s audio={t_audio:.3f}s "
        f"natural_factor={natural_factor:.3f} mode={mode_label} "
        f"speed_applied={speed:.3f}",
        flush=True,
    )

    # Build ffmpeg command per mode
    if mode_label == "min_clamped_freeze_tail":
        # Visual plays 1.0x then freezes last frame to fill audio length.
        # tpad=stop_mode=clone:stop_duration=<gap> on the video stream;
        # audio is taken from audio_path verbatim with -map 1:a:0;
        # -shortest ensures output ends at audio length (the longer one).
        gap = max(0.0, t_audio - t_visual)
        cmd = [
            FFMPEG_BIN, "-y",
            "-i", str(visual_path),
            "-i", str(audio_path),
            "-filter_complex",
            f"[0:v]tpad=stop_mode=clone:stop_duration={gap:.6f},setpts=PTS-STARTPTS,fps=24,format=yuv420p[v];"
            f"[1:a]aresample=async=1:first_pts=0,aformat=sample_fmts=fltp:sample_rates=48000:channel_layouts=stereo,asetpts=PTS-STARTPTS[a]",
            "-map", "[v]", "-map", "[a]",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "18",
            "-pix_fmt", "yuv420p",
            "-c:a", "aac", "-b:a", "128k", "-ar", "48000",
            "-shortest",
            "-movflags", "+faststart",
            str(output_path),
        ]
    elif mode_label == "max_clamped_audio_cut":
        # Speed visual to 2x; -shortest trims when audio ends.
        cmd = [
            FFMPEG_BIN, "-y",
            "-i", str(visual_path),
            "-i", str(audio_path),
            "-filter_complex",
            f"[0:v]setpts=PTS/{speed:.6f},setpts=PTS-STARTPTS,fps=24,format=yuv420p[v];"
            f"[1:a]aresample=async=1:first_pts=0,aformat=sample_fmts=fltp:sample_rates=48000:channel_layouts=stereo,asetpts=PTS-STARTPTS[a]",
            "-map", "[v]", "-map", "[a]",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "18",
            "-pix_fmt", "yuv420p",
            "-c:a", "aac", "-b:a", "128k", "-ar", "48000",
            "-shortest",
            "-movflags", "+faststart",
            str(output_path),
        ]
    else:
        # Natural fit — visual sped to match audio exactly.
        cmd = [
            FFMPEG_BIN, "-y",
            "-i", str(visual_path),
            "-i", str(audio_path),
            "-filter_complex",
            f"[0:v]setpts=PTS/{speed:.6f},setpts=PTS-STARTPTS,fps=24,format=yuv420p[v];"
            f"[1:a]aresample=async=1:first_pts=0,aformat=sample_fmts=fltp:sample_rates=48000:channel_layouts=stereo,asetpts=PTS-STARTPTS[a]",
            "-map", "[v]", "-map", "[a]",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "18",
            "-pix_fmt", "yuv420p",
            "-c:a", "aac", "-b:a", "128k", "-ar", "48000",
            "-movflags", "+faststart",
            str(output_path),
        ]

    code, _, err = run(cmd)
    if code != 0:
        err_tail = err[-1500:] if err else "<no stderr>"
        raise RuntimeError(
            f"swap_audio_with_speed_match ffmpeg failed (mode={mode_label}): "
            f"{err_tail[-500:]}"
        )

    out_dur = get_duration(ffprobe_json(output_path))
    print(
        f"[v698A/swap] output={out_dur:.3f}s (mode={mode_label})",
        flush=True,
    )
    return speed, out_dur, mode_label


def concat_videos(files: List[Path], output: Path) -> None:
    """Concatenate multiple videos into one.

    v692c: switched to the ffmpeg concat **filter** (one `-i` per input
    fed through `concat=n=N:v=1:a=1`) instead of the concat **demuxer**
    + `-r 24 -vsync cfr` re-encode. The demuxer-based path was producing
    output durations 7-8× the sum of input durations on Render's ffmpeg
    when inputs mixed framerates (text_card scenes render at 30fps via
    `color=...:r=30`, Veo clips at 24fps) — the per-input PTS handling
    under `-vsync cfr` mangled the timeline. v692b diagnostic confirmed:
    8 inputs summing to 31.9s produced a 233.5s concat output.

    The concat filter normalizes PTS internally (every input is decoded
    into a single filtergraph timeline), so the output duration matches
    the sum of input durations regardless of mixed framerates / audio
    sample rates / PTS oddities. Output framerate is forced via
    `fps=24` after concat for downstream playback consistency.

    Memory cost: ~30MB per active decoder × N inputs. For typical
    8-clip exports that's ~240MB peak — within Standard plan budget.

    v560 history (see prior docstring): originally `-c:v copy`, then
    re-encode + `-vsync cfr` to fix VFR durations. The re-encode fixed
    the stream-copy PTS-preservation bug but introduced this CFR-stretch
    bug for mixed-framerate inputs. Concat filter resolves both.
    """
    print(f"[VideoProcessor] concat_videos: {len(files)} files -> {output}")
    n = len(files)
    if n == 0:
        raise RuntimeError("concat_videos: no files to concatenate")

    # v692e — two-pass normalize-then-stream-copy strategy. v692d's single-pass
    # concat filter failed with "matches no streams" because one of the inputs
    # was missing an audio stream (Veo silent renders sometimes ship without
    # audio; Whisper-VAD's apply_vad can also drop audio in edge cases). The
    # concat filter binds [N:a:0] before running, so a single missing audio
    # stream rejects the entire 8-input graph.
    #
    # Pass 1: normalize EACH input to a common spec (1080x1920, 24fps, yuv420p,
    #   48kHz stereo h264+aac). Injects anullsrc when source has no audio so
    #   every normalized file has both streams. Single-input ffmpeg = simple,
    #   predictable, can't fail in cross-input ways.
    # Pass 2: concat demuxer + stream copy on the normalized files. Inputs are
    #   now guaranteed identical specs so the demuxer just splices packets,
    #   no PTS mangling, no encoder involvement.
    #
    # Memory: ~80MB per ffmpeg pass (sequential). Disk: ~5MB × N intermediates.
    # CPU: 1× normalize per input + ~0 for stream-copy concat.
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        normalized: List[Path] = []
        for i, f in enumerate(files):
            norm_out = td_path / f"normalized_{i:04d}.mp4"
            # Probe for audio stream presence
            has_audio = False
            try:
                _info = ffprobe_json(f)
                for s in _info.get("streams", []):
                    if s.get("codec_type") == "audio":
                        has_audio = True
                        break
            except Exception:
                has_audio = False  # safe default — inject silence

            if has_audio:
                cmd_norm = [
                    FFMPEG_BIN, "-y",
                    "-i", str(f),
                    "-vf",
                    "scale=1080:1920:force_original_aspect_ratio=decrease,"
                    "pad=1080:1920:(ow-iw)/2:(oh-ih)/2,setsar=1,"
                    "fps=24,format=yuv420p",
                    "-af",
                    "aresample=async=1:first_pts=0,"
                    "aformat=sample_fmts=fltp:sample_rates=48000:"
                    "channel_layouts=stereo",
                    "-c:v", "libx264", "-preset", "veryfast", "-crf", "18",
                    "-pix_fmt", "yuv420p",
                    "-threads", "1",
                    "-c:a", "aac", "-b:a", "128k", "-ar", "48000",
                    "-max_muxing_queue_size", "1024",
                    "-movflags", "+faststart",
                    str(norm_out),
                ]
                src_label = "video+audio"
            else:
                # Inject silent audio matching video duration
                cmd_norm = [
                    FFMPEG_BIN, "-y",
                    "-i", str(f),
                    "-f", "lavfi", "-i",
                    "anullsrc=channel_layout=stereo:sample_rate=48000",
                    "-vf",
                    "scale=1080:1920:force_original_aspect_ratio=decrease,"
                    "pad=1080:1920:(ow-iw)/2:(oh-ih)/2,setsar=1,"
                    "fps=24,format=yuv420p",
                    "-map", "0:v:0", "-map", "1:a:0",
                    "-shortest",
                    "-c:v", "libx264", "-preset", "veryfast", "-crf", "18",
                    "-pix_fmt", "yuv420p",
                    "-threads", "1",
                    "-c:a", "aac", "-b:a", "128k", "-ar", "48000",
                    "-max_muxing_queue_size", "1024",
                    "-movflags", "+faststart",
                    str(norm_out),
                ]
                src_label = "video-only (audio injected)"

            print(
                f"[VideoProcessor/v692e] normalize slot={i} ({src_label}) "
                f"-> {norm_out.name}",
                flush=True,
            )
            code, _, err = run(cmd_norm)
            if code != 0:
                err_tail = err[-1500:] if err else "<no stderr>"
                print(
                    f"[VideoProcessor/v692e] normalize slot={i} FAILED. "
                    f"stderr tail:\n{err_tail}",
                    flush=True,
                )
                raise RuntimeError(
                    f"v692e normalize failed for slot {i}: {err_tail[-300:]}"
                )
            normalized.append(norm_out)

        # Pass 2: concat demuxer + stream-copy on identical-spec files
        listfile = td_path / "concat_inputs.txt"
        with listfile.open("w", encoding="utf-8") as lf:
            for p in normalized:
                lf.write(f"file {shlex.quote(str(p))}\n")

        cmd_concat = [
            FFMPEG_BIN, "-y",
            "-f", "concat", "-safe", "0", "-i", str(listfile),
            "-c", "copy",
            "-movflags", "+faststart",
            str(output),
        ]
        print(
            f"[VideoProcessor/v692e] concat-demux + stream-copy on "
            f"{len(normalized)} normalized files",
            flush=True,
        )
        code, _, err = run(cmd_concat)
        if code != 0:
            err_tail = err[-1500:] if err else "<no stderr>"
            print(
                f"[VideoProcessor/v692e] stream-copy concat failed. "
                f"stderr tail:\n{err_tail}",
                flush=True,
            )
            raise RuntimeError(
                f"v692e concat failed: {err_tail[-300:]}"
            )
        # v782 diagnostic — confirm final export resolution after the 1080p
        # normalize bump. Remove once operator-side evidence lands that
        # uploaded HD clips export at 1080x1920 (not the old 720x1280).
        try:
            _oi = ffprobe_json(output)
            _vw = _vh = None
            for _s in _oi.get("streams", []):
                if _s.get("codec_type") == "video":
                    _vw, _vh = _s.get("width"), _s.get("height")
                    break
            print(
                f"[VideoProcessor/v782] concat_videos export resolution="
                f"{_vw}x{_vh} (target 1080x1920, normalize crf18/veryfast)",
                flush=True,
            )
        except Exception as _e:
            print(f"[VideoProcessor/v782] resolution probe failed: {_e}", flush=True)
        print(f"[VideoProcessor]   concat_videos completed")


def concat_videos_with_transitions(
    files: List[Path],
    output: Path,
    transition: str = "fade",
    transition_duration: float = 0.5,
    has_audio: bool = False,
) -> None:
    """
    Concatenate videos with fade transitions between clips.
    
    Uses a memory-efficient approach: applies fade-out/fade-in to each clip
    individually (one FFmpeg process per clip), then plain-concats the results.
    This uses ~30MB constant memory instead of loading all clips simultaneously
    (which causes OOM on 512MB instances with 10+ clips).
    
    Args:
        files: List of video file paths
        transition: FFmpeg xfade transition name (only 'fade' variants supported
                    in per-clip mode; others fall back to simple fade)
        transition_duration: Duration of each fade in seconds
        has_audio: Whether clips have audio (applies audio fade too)
    """
    if len(files) < 2:
        if files:
            import shutil
            shutil.copy(files[0], output)
        return
    
    td = transition_duration
    print(f"[VideoProcessor] concat_with_transitions: {len(files)} files, "
          f"transition={transition}, duration={td}s (per-clip fade mode)")
    
    # Get durations
    durations = []
    for f in files:
        info = ffprobe_json(f)
        d = get_duration(info)
        durations.append(d)
        print(f"[VideoProcessor]   {f.name}: {d:.2f}s")
    
    # Phase 1: Apply fade-in/fade-out to each clip individually
    # First clip: fade-out only at end
    # Middle clips: fade-in at start + fade-out at end
    # Last clip: fade-in only at start
    faded_dir = output.parent / "_faded_clips"
    faded_dir.mkdir(exist_ok=True)
    faded_files = []
    
    for i, (f, dur) in enumerate(zip(files, durations)):
        faded_path = faded_dir / f"faded_{i:04d}.mp4"
        is_first = (i == 0)
        is_last = (i == len(files) - 1)
        
        # Build filter
        vfilters = []
        afilters = []
        
        if not is_first:
            vfilters.append(f"fade=t=in:st=0:d={td:.3f}")
            if has_audio:
                afilters.append(f"afade=t=in:st=0:d={td:.3f}")
        
        if not is_last:
            fade_start = max(0, dur - td)
            vfilters.append(f"fade=t=out:st={fade_start:.3f}:d={td:.3f}")
            if has_audio:
                afilters.append(f"afade=t=out:st={fade_start:.3f}:d={td:.3f}")
        
        if vfilters:
            vf = ",".join(vfilters)
            af = ",".join(afilters) if afilters else None
            
            cmd = [FFMPEG_BIN, "-y", "-i", str(f)]
            cmd.extend(["-vf", vf])
            if af and has_audio:
                cmd.extend(["-af", af])
            cmd.extend([
                # v560: CFR + explicit framerate — same reason as the
                # speed adjustment fix in align_clip_to_duration. Even
                # though the input was already CFR after v560, the fade
                # filter graph can introduce frame-rate variability if
                # we don't pin the output rate explicitly. Belt and
                # braces — costs nothing and makes the pipeline robust
                # to upstream changes.
                "-r", "24", "-vsync", "cfr",
                "-c:v", "libx264", "-preset", "veryfast", "-crf", "18",
                "-pix_fmt", "yuv420p", "-threads", "1",
            ])
            if has_audio:
                cmd.extend(["-c:a", "aac", "-b:a", "128k"])
            cmd.append(str(faded_path))
            
            code, _, err = run(cmd)
            if code != 0:
                print(f"[VideoProcessor]   Fade failed for clip {i}: {err[:200]}")
                # Fall back to unfaded clip
                import shutil
                shutil.copy(f, faded_path)
        else:
            # No fades needed (shouldn't happen, but safety)
            import shutil
            shutil.copy(f, faded_path)
        
        faded_files.append(faded_path)
    
    print(f"[VideoProcessor]   ✓ Applied fades to {len(faded_files)} clips")
    
    # Phase 2: Plain concat the faded clips (stream copy — instant, no memory)
    concat_videos(faded_files, output)
    
    # Cleanup faded intermediates
    import shutil
    shutil.rmtree(faded_dir, ignore_errors=True)
    
    # Calculate expected duration
    total_dur = sum(durations)
    print(f"[VideoProcessor]   concat_with_transitions completed "
          f"(~{total_dur:.2f}s, per-clip fade mode)")


##############################################################################
# Master Audio Alignment (for assemble/import jobs)
##############################################################################

def transcribe_master_audio(audio_path: Path, initial_prompt: str = None) -> list:
    """
    Transcribe master audio with word-level timestamps using faster-whisper.
    Returns list of dicts: [{word, start, end}, ...]

    v701r — optional initial_prompt biases the decoder toward expected
    words. Same fix as v701q on the per-clip path: without it Whisper
    mistranscribes Veo TTS compound terms ("self-rising" → "all-fries")
    and the master-audio matcher can't align those script lines.
    """
    print(f"[MasterAlign] Transcribing master audio: {audio_path}")
    
    # Extract audio to WAV for Whisper (handles mp3/m4a/wav/etc)
    wav_path = audio_path.parent / f"{audio_path.stem}_whisper.wav"
    cmd = [
        FFMPEG_BIN, "-y", "-i", str(audio_path),
        "-vn", "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "1",
        str(wav_path)
    ]
    code, _, err = run(cmd)
    if code != 0:
        raise RuntimeError(f"Failed to extract audio from master: {err}")
    
    try:
        from faster_whisper import WhisperModel
        # v701za — bump master-audio Whisper back to "small". v701z's "tiny"
        # mis-transcribed enough words (donut-glaze run: HOOK / vp2 / vp3
        # / CTA) that find_line_in_master's Method 1 EXACT-substring match
        # failed for those lines and Method 2 sliding-window matched the
        # wrong master position with confidence 0.36-0.50 → clips placed
        # at the wrong timestamp → overlap warnings → final broll played
        # 6/11 clips out of order. Speaker's per-clip pass still uses
        # tiny (good enough with v701q initial_prompt + 11-clip scope),
        # but master-audio is ONE transcription over a longer continuous
        # audio where accuracy matters more.
        #
        # Memory budget after v701y/z sequencing: speaker's tiny is
        # disposed + malloc_trim before this load runs, so small (~250MB
        # resident) is the only Whisper instance live during the master
        # pass. Within 2GB ceiling.
        model = WhisperModel("small", device="cpu", compute_type="int8")
        if initial_prompt:
            print(
                f"[MasterAlign] v701r initial_prompt: "
                f"{len(initial_prompt.split())} script words "
                f"(first 80 chars: {initial_prompt[:80]!r})",
                flush=True,
            )
        segments, info = model.transcribe(
            str(wav_path),
            language="en",
            word_timestamps=True,
            initial_prompt=initial_prompt,
            beam_size=5,                            # v701zb — small benefits from beam>1
            condition_on_previous_text=False,       # v701zb — kill hallucination cascade
            # v701zc — DROP vad_filter=True. On a 55s assembled-concat
            # master Silero wrongly classifies portions of the HOOK +
            # opening voiceover lines as non-speech and skips them →
            # Whisper transcribes 64 words for a 127-word script →
            # find_line_in_master fails for the dropped lines →
            # fallback targets walk past master end. Speaker concat is
            # already speech-only (per-clip Whisper-VAD trimmed pads),
            # so a Silero pre-filter is redundant work that introduces
            # this very bug. Per-clip path keeps vad_filter=True because
            # individual ~8s clips contain Veo's prompt-pad silences
            # Silero handles correctly at that scale.
            # v701s — keep faster-whisper's default temperature ladder.
        )
        
        words = []
        for segment in segments:
            for w in segment.words:
                words.append({
                    "word": w.word.strip(),
                    "start": w.start,
                    "end": w.end,
                })
        
        print(f"[MasterAlign] Transcribed {len(words)} words, duration={info.duration:.1f}s")
        if words:
            print(f"[MasterAlign] First: '{words[0]['word']}' @ {words[0]['start']:.2f}s")
            print(f"[MasterAlign] Last:  '{words[-1]['word']}' @ {words[-1]['end']:.2f}s")
        
        del model
        # v701z — gc + malloc_trim so master-audio Whisper memory returns
        # to OS before the alignment + clip-processing phase starts.
        import gc; gc.collect()
        try:
            import ctypes as _ct
            _ct.CDLL("libc.so.6").malloc_trim(0)
        except Exception:
            pass
        return words
    finally:
        # Cleanup temp wav
        if wav_path.exists():
            wav_path.unlink()


def _normalize(text: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace."""
    import re
    text = text.lower().strip()
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text


def find_line_in_master(master_words: list, master_text: str, dialogue_text: str, 
                        search_from_word: int = 0) -> dict:
    """
    Find where a dialogue line appears in the master transcript.
    
    Strategy: use the FULL dialogue line as a substring search against
    the full master transcript text, then map the character position 
    back to word-level timestamps.
    
    Args:
        master_words: [{word, start, end}, ...] from Whisper
        master_text: Full concatenated master transcript (normalized)
        dialogue_text: The dialogue line to find
        search_from_word: Minimum word index to search from (for sequential ordering)
    
    Returns: {start: float, end: float, start_word_idx: int, end_word_idx: int, confidence: float}
    """
    import difflib
    
    if not master_words or not dialogue_text.strip():
        return None
    
    norm_dialogue = _normalize(dialogue_text)
    dial_words = norm_dialogue.split()
    
    if not dial_words:
        return None
    
    master_norm = [_normalize(w["word"]) for w in master_words]
    
    # Build character-position-to-word-index mapping
    # master_text is " ".join(master_norm), so we can map char offsets to word indices
    word_char_starts = []
    pos = 0
    for i, w in enumerate(master_norm):
        word_char_starts.append(pos)
        pos += len(w) + 1  # +1 for space
    
    def char_pos_to_word_idx(char_pos):
        """Map a character position in master_text to the nearest word index."""
        for i in range(len(word_char_starts) - 1, -1, -1):
            if word_char_starts[i] <= char_pos:
                return i
        return 0
    
    # Compute the search region in text (skip words before search_from_word)
    if search_from_word > 0 and search_from_word < len(word_char_starts):
        search_text_start = word_char_starts[search_from_word]
        search_text = master_text[search_text_start:]
    else:
        search_text_start = 0
        search_text = master_text
    
    # --- Method 1: Exact substring match (best case) ---
    exact_pos = search_text.find(norm_dialogue)
    if exact_pos >= 0:
        abs_start = search_text_start + exact_pos
        abs_end = abs_start + len(norm_dialogue)
        start_word = char_pos_to_word_idx(abs_start)
        end_word = char_pos_to_word_idx(abs_end)
        # Expand end_word to include the full last word
        end_word = min(end_word, len(master_words) - 1)
        
        print(f"[MasterAlign]   ✓ EXACT match: '{dialogue_text[:50]}...' → words {start_word}-{end_word}")
        return {
            "start": master_words[start_word]["start"],
            "end": master_words[end_word]["end"],
            "start_word_idx": start_word,
            "end_word_idx": end_word,
            "confidence": 1.0,
        }
    
    # --- Method 2: Sliding window with FULL LINE (handles Whisper mishearing a word) ---
    # Slide a window of len(dial_words) across master_norm, score each position
    window_len = len(dial_words)
    best_score = 0.0
    best_idx = search_from_word  # default to search start
    
    for i in range(search_from_word, max(search_from_word + 1, len(master_norm) - window_len + 1)):
        candidate = " ".join(master_norm[i:i + window_len])
        score = difflib.SequenceMatcher(None, norm_dialogue, candidate).ratio()
        if score > best_score:
            best_score = score
            best_idx = i
    
    # --- Method 3: If full-line window failed, try first-half + second-half ---
    # This handles cases where Whisper inserted/deleted words mid-line
    if best_score < 0.5 and len(dial_words) >= 6:
        half = len(dial_words) // 2
        first_half = " ".join(dial_words[:half])
        second_half = " ".join(dial_words[half:])
        
        # Find first half
        best_first_score = 0.0
        best_first_idx = search_from_word
        fh_len = half
        for i in range(search_from_word, max(search_from_word + 1, len(master_norm) - fh_len + 1)):
            candidate = " ".join(master_norm[i:i + fh_len])
            score = difflib.SequenceMatcher(None, first_half, candidate).ratio()
            if score > best_first_score:
                best_first_score = score
                best_first_idx = i
        
        # Find second half (must come after first half)
        best_second_score = 0.0
        best_second_idx = best_first_idx + fh_len
        sh_len = len(dial_words) - half
        for i in range(best_first_idx + fh_len - 2, max(best_first_idx + fh_len - 1, len(master_norm) - sh_len + 1)):
            candidate = " ".join(master_norm[i:i + sh_len])
            score = difflib.SequenceMatcher(None, second_half, candidate).ratio()
            if score > best_second_score:
                best_second_score = score
                best_second_idx = i
        
        split_score = (best_first_score + best_second_score) / 2
        if split_score > best_score:
            best_score = split_score
            best_idx = best_first_idx
            window_len = (best_second_idx + sh_len) - best_first_idx
            print(f"[MasterAlign]   Split match improved: {best_score:.2f} (first={best_first_score:.2f} second={best_second_score:.2f})")
    
    # v701zb — clamp indices into valid range. When sequential matching
    # is active and the cursor has advanced past the last master word,
    # best_idx can equal len(master_words) (no iteration entered the
    # loop). Returning master_words[len()] raises IndexError and kills
    # the whole broll pipeline. Defensive clamp + None return when the
    # search range was effectively empty.
    if not master_words:
        return None
    if best_idx >= len(master_words):
        print(f"[MasterAlign]   ✗ search exhausted for '{dialogue_text[:50]}...' (search_from_word={search_from_word}, master len={len(master_words)})")
        return None
    end_idx = min(best_idx + window_len - 1, len(master_words) - 1)
    if end_idx < best_idx:
        end_idx = best_idx

    quality = "✓ GOOD" if best_score > 0.6 else "⚠ WEAK" if best_score > 0.3 else "✗ POOR"
    print(f"[MasterAlign]   {quality} match ({best_score:.2f}): '{dialogue_text[:50]}...' → words {best_idx}-{end_idx} "
          f"('{' '.join(master_norm[best_idx:end_idx+1][:8])}...')")

    return {
        "start": master_words[best_idx]["start"],
        "end": master_words[end_idx]["end"],
        "start_word_idx": best_idx,
        "end_word_idx": end_idx,
        "confidence": best_score,
    }


def _word_matches(a: str, b: str) -> bool:
    """v825.8 — loose equality for a single normalized anchor word.

    Whisper is not stable across a long take: the same script word came back as
    "vessel" in one sentence and "vessels" in another on job d4b661a8. Exact
    equality dropped the overlay. Order: exact, then stem (one word is a prefix
    of the other, both >= 4 chars, differing by <= 2 trailing chars), then a
    close fuzzy ratio. Short words (<= 3 chars) demand exact so "a"/"an"/"and"
    never collide.
    """
    if a == b:
        return True
    if len(a) <= 3 or len(b) <= 3:
        return False
    lo, hi = (a, b) if len(a) <= len(b) else (b, a)
    if hi.startswith(lo) and (len(hi) - len(lo)) <= 2:
        return True
    import difflib
    return difflib.SequenceMatcher(None, a, b).ratio() >= 0.87


def _find_anchor_word(norms: list, target: str, lo: int, hi: int):
    """v825.8 — first index in [lo, hi] matching `target`. Exact pass over the
    whole window first, so an exact hit later always beats a fuzzy hit earlier.
    Returns None when neither pass matches."""
    if not target or lo > hi:
        return None
    hi = min(hi, len(norms) - 1)
    lo = max(lo, 0)
    for i in range(lo, hi + 1):
        if norms[i] == target:
            return i
    for i in range(lo, hi + 1):
        if _word_matches(norms[i], target):
            return i
    return None


def _align_scene_lines(master_words: list, master_text: str, scene_lines: list) -> list:
    """v825.9 — align each SCENE's spoken line to the master audio ONCE, using
    whichever candidate (Prompt A line or the Prompt-B reworded line) matches
    best. Returns a list index-aligned with scene_lines: each entry is
    {lo, hi, authored} (master word index range for that scene) or None.

    A sequential cursor keeps the scenes monotonic in the continuous master —
    scene i+1 is searched from scene i's end, so a short line can't backtrack
    onto an earlier scene.
    """
    out = []
    cur = 0
    for sc in scene_lines:
        authored = (sc.get("authored") or "").strip()
        cands = [c for c in (sc.get("candidates") or [authored]) if c and c.strip()]
        best = None
        for cand in cands:
            b = find_line_in_master(master_words, master_text, cand, search_from_word=cur)
            if b is not None and (best is None or b.get("confidence", 0) > best.get("confidence", 0)):
                best = b
        if best is not None:
            lo = best.get("start_word_idx", cur)
            hi = best.get("end_word_idx", len(master_words) - 1)
            out.append({"lo": lo, "hi": hi, "authored": authored})
            cur = max(cur, lo)  # advance; allow overlap, never rewind
        else:
            out.append(None)
    return out


def _authored_offset(authored_norms: list, word: str, at_or_after: int = 0):
    """First index of `word` in the authored line words at/after `at_or_after`,
    exact then loose (v825.8 `_word_matches`). None if absent."""
    if not word:
        return None
    for i in range(max(0, at_or_after), len(authored_norms)):
        if authored_norms[i] == word:
            return i
    for i in range(max(0, at_or_after), len(authored_norms)):
        if _word_matches(authored_norms[i], word):
            return i
    return None


def resolve_support_spans(master_words: list, support_inserts: list, scene_lines: list = None) -> list:
    """v825.1 / v825.8 / v825.9 — resolve each support insert to its [start,end]
    span in the master audio.

    v825.9 — LINE-RELATIVE placement (the robust primary path). Each support
    belongs to ONE spoken line (its `phrase` is a substring of that line). The
    line is aligned to the master audio using whichever prompt actually shipped
    (Prompt A or the Prompt-B reworded line — passed in `scene_lines`), so the
    line's master span is correct regardless of A/B. The still is then placed
    INSIDE that span:
      - if `start_word` / `end_word` survive in the spoken audio -> exact/fuzzy
        word times (most precise);
      - else -> PROPORTIONAL to where the anchor sits in the AUTHORED line
        (the anchor always exists in the authored text, so a Prompt-B rewording
        that dropped the literal word still lands the still at the right moment).
    This replaces the v825.8 whole-phrase fallback, which showed the still across
    the entire line when a word missed.

    `scene_lines` (optional): list, one entry per SCENE in order, each
    {"authored": "<Prompt-A line>", "candidates": ["<A line>", "<B line>", ...]}.
    When None (old callers / unit tests), falls back to the v825.8 phrase-region
    matching over the whole master.

    v825.1 — `start_word` / `end_word` authoritative over a drifted phrase.
    v825.8 — anchors matched loosely (stem/fuzzy); a located support never
    dropped for a single missing word.

    Returns [ {start, end, image_index, support_index, confidence} | None, ... ],
    index-aligned with support_inserts. None = not resolvable (caller skips it).
    """
    norms = [_normalize(w["word"]) for w in master_words]
    master_text = " ".join(norms)
    n = len(master_words)

    # v825.9 — pre-align every scene line, and index its authored words so a
    # support can be placed inside its owning line's span.
    scene_spans = None
    scene_auth_norms = None
    if scene_lines:
        scene_spans = _align_scene_lines(master_words, master_text, scene_lines)
        scene_auth_norms = [
            _normalize(sc.get("authored") or "").split() for sc in scene_lines
        ]

    def _own_scene(phrase_norm: str, sw: str, ew: str):
        """Index of the scene whose AUTHORED line owns this support. Prefer a
        phrase substring match; else the scene with the most anchor words."""
        if not scene_auth_norms:
            return None
        pj = phrase_norm.strip()
        # 1) phrase is a substring of exactly one authored line.
        hits = [i for i, aw in enumerate(scene_auth_norms)
                if pj and pj in " ".join(aw)]
        if len(hits) == 1:
            return hits[0]
        cand = hits if hits else range(len(scene_auth_norms))
        # 2) score by anchor-word presence (loose).
        best_i, best_score = None, 0
        for i in cand:
            aw = scene_auth_norms[i]
            score = sum(1 for t in (sw, ew) if _authored_offset(aw, t) is not None)
            if score > best_score:
                best_i, best_score = i, score
        return best_i

    spans = []
    cursor = 0
    for s in support_inserts:
        sw = _normalize(s.get("start_word") or "")
        ew = _normalize(s.get("end_word") or "")
        phrase = (s.get("phrase") or "").strip()
        phrase_norm = _normalize(phrase)

        # ---- v825.9 LINE-RELATIVE path (when scene_lines supplied) ----------
        if scene_spans is not None:
            si = _own_scene(phrase_norm, sw, ew)
            if si is not None and scene_spans[si] is not None:
                lo, hi = scene_spans[si]["lo"], scene_spans[si]["hi"]
                aw = scene_auth_norms[si]
                L = len(aw)
                span_len = hi - lo + 1

                def _prop(off, inclusive_end=False):
                    if L <= 1:
                        return hi if inclusive_end else lo
                    f = (off + (1 if inclusive_end else 0)) / L
                    idx = lo + int(round(f * span_len)) - (1 if inclusive_end else 0)
                    return min(hi, max(lo, idx))

                conf = 1.0
                # start: exact/fuzzy in the line span, else proportional.
                s_idx = _find_anchor_word(norms, sw, lo, hi)
                if s_idx is None:
                    off = _authored_offset(aw, sw)
                    s_idx = _prop(off if off is not None else 0)
                    conf -= 0.2
                # end: exact/fuzzy at/after start, else proportional.
                e_idx = _find_anchor_word(norms, ew, s_idx, hi)
                if e_idx is None:
                    off = _authored_offset(aw, ew, at_or_after=0)
                    e_idx = _prop(off if off is not None else L - 1, inclusive_end=True)
                    e_idx = max(e_idx, s_idx)
                    conf -= 0.2

                if conf < 1.0:
                    print(f"[Support] support {s.get('support_index')} placed line-relative "
                          f"in scene {si + 1} (words {lo}-{hi}, conf={conf:.1f}) — "
                          f"Prompt-B reword tolerated", flush=True)
                start_t = master_words[s_idx]["start"]
                end_t = master_words[e_idx]["end"]
                if end_t <= start_t:
                    end_t = start_t + 1.0
                spans.append({"start": start_t, "end": end_t, "image_index": s["image_index"],
                              "support_index": s["support_index"], "confidence": round(conf, 2)})
                cursor = e_idx + 1
                continue
            # else: no owning scene span -> fall through to the phrase path.
            print(f"[Support] support {s.get('support_index')} — no owning scene span, "
                  f"trying phrase match", flush=True)

        # ---- v825.8 phrase/anchor fallback (no scene_lines, or no owner) ----
        if not sw or not ew:
            b = find_line_in_master(master_words, master_text, phrase, search_from_word=cursor) if phrase else None
            if b is None:
                print(f"[Support] no start/end word and phrase unmatched (support {s.get('support_index')})")
                spans.append(None)
                continue
            end = b["end"] if b["end"] > b["start"] else b["start"] + 1.0
            spans.append({"start": b["start"], "end": end, "image_index": s["image_index"],
                          "support_index": s["support_index"], "confidence": b.get("confidence", 1.0)})
            cursor = b.get("end_word_idx", cursor) + 1
            continue

        region_lo = cursor
        region_hi = n - 1
        phrase_span = None
        if phrase:
            b = find_line_in_master(master_words, master_text, phrase, search_from_word=cursor)
            if b is not None:
                phrase_span = b
                region_lo = b.get("start_word_idx", cursor)
                region_hi = b.get("end_word_idx", n - 1)

        sidx = _find_anchor_word(norms, sw, region_lo, n - 1)
        start_idx = sidx if sidx is not None else None

        confidence = 1.0
        if start_idx is None:
            if phrase_span is None:
                print(f"[Support] start_word {sw!r} not found and no phrase window "
                      f"— DROPPED (support {s.get('support_index')})")
                spans.append(None)
                continue
            print(f"[Support] start_word {sw!r} not found — FALLBACK to phrase span "
                  f"words {region_lo}-{region_hi} (support {s.get('support_index')})")
            start_idx, end_idx = region_lo, region_hi
            confidence = min(0.5, phrase_span.get("confidence", 0.5))
        else:
            end_hi = min(n - 1, max(region_hi, start_idx + 40))
            end_idx = _find_anchor_word(norms, ew, start_idx, end_hi)
            if end_idx is None:
                if phrase_span is not None and region_hi >= start_idx:
                    print(f"[Support] end_word {ew!r} not found — FALLBACK to phrase end "
                          f"word {region_hi} (support {s.get('support_index')})")
                    end_idx = region_hi
                    confidence = 0.7
                else:
                    print(f"[Support] end_word {ew!r} not found after start and no phrase "
                          f"window — holding on start word (support {s.get('support_index')})")
                    end_idx = start_idx
                    confidence = 0.5

        start_t = master_words[start_idx]["start"]
        end_t = master_words[end_idx]["end"]
        if end_t <= start_t:
            end_t = start_t + 1.0
        spans.append({
            "start": start_t,
            "end": end_t,
            "image_index": s["image_index"],
            "support_index": s["support_index"],
            "confidence": confidence,
        })
        cursor = end_idx + 1

    # v825.5 — minimum HOLD + TAIL so a brief single-word overlay (oysters,
    # watermelon) doesn't flash and vanish before the word is even finished.
    # Extend each span's END to at least start+MIN_HOLD and at least
    # word-end+TAIL, but never into the next overlay (leave GAP). Never shorten.
    MIN_HOLD, TAIL, GAP = 1.3, 0.35, 0.05
    order = [i for i in sorted(range(len(spans)),
                               key=lambda k: (spans[k]["start"] if spans[k] else 1e9))
             if spans[i]]
    for pos, i in enumerate(order):
        s = spans[i]
        want = max(s["end"] + TAIL, s["start"] + MIN_HOLD)
        if pos + 1 < len(order):
            want = min(want, spans[order[pos + 1]]["start"] - GAP)
        s["end"] = max(s["end"], want)
    return spans


def calculate_clip_targets(master_words: list, dialogue_lines: list, master_duration: float = None, sequential: bool = False) -> list:
    """
    Given master word timestamps and ordered dialogue lines,
    compute each clip's target start time and duration.

    Each clip is matched independently against the full master transcript
    (clips may appear in a different order than the voiceover).
    Each clip covers only its own dialogue timestamps (gaps filled with black).

    v701t — `sequential` flag. When True, each line's search window starts
    after the prior line's end-word in the master. Required for broll
    where dialogue_lines are in master source order; without it, lines
    like "no fryer..." / "no cortisol..." / "no inflammatory..." (all
    starting with "no") match the SAME early position in master, causing
    every clip to collapse to one timestamp. Default False preserves the
    legacy assemble-job behavior (operator master, clips may be reordered).

    Returns: [{start: float, end: float, target_duration: float, confidence: float}, ...]
    """
    mode = "sequential" if sequential else "independent"
    print(f"[MasterAlign] Calculating targets for {len(dialogue_lines)} clips ({mode} matching)")

    if master_duration is None:
        master_duration = master_words[-1]["end"] if master_words else 30.0

    # v701t — sanity check on master length. If master_duration is wildly
    # shorter than the dialogue would naturally take (~3 words/s loose
    # baseline) something upstream over-trimmed the master audio. Log
    # loudly so the next debug session has the evidence at hand.
    _total_script_words = sum(len((l or "").split()) for l in dialogue_lines)
    _expected_min_s = _total_script_words / 4.0  # generous ~4 wps
    if master_duration < _expected_min_s * 0.6 and _total_script_words > 10:
        print(
            f"[MasterAlign] ⚠ master_duration={master_duration:.1f}s is short for "
            f"{_total_script_words} dialogue words (≥{_expected_min_s:.1f}s expected). "
            f"Upstream may have over-trimmed the master.",
            flush=True,
        )

    # Build full normalized master text for substring matching
    master_norm = [_normalize(w["word"]) for w in master_words]
    master_text = " ".join(master_norm)
    print(f"[MasterAlign] Master transcript ({len(master_words)} words, {master_duration:.1f}s): '{master_text[:120]}...'")

    # v701t — sequential cursor advances after each matched line so later
    # lines can't backtrack into earlier-line spans.
    _seq_cursor = 0
    targets = []
    for i, line in enumerate(dialogue_lines):
        _from = _seq_cursor if sequential else 0
        b = find_line_in_master(master_words, master_text, line, search_from_word=_from)

        if b is None:
            # v701zc — clamp fallback to master_duration. Previously each
            # unmatched line added 5s to prev_end, walking the cursor past
            # master end on multiple consecutive failures. Result: target
            # spans extended to 70s on a 55s master, then process_clip_for_alignment
            # speed-pad-trimmed clips into a nonsense timeline. Clamp to
            # the available remaining window; if no room remains, allocate
            # a 0.5s slot at master_end (clip will be cut anyway).
            prev_end = targets[-1]["end"] if targets else 0.0
            available = max(0.0, master_duration - prev_end)
            fallback_dur = min(5.0, available) if available > 0.5 else 0.5
            fallback_end = min(prev_end + fallback_dur, master_duration)
            targets.append({
                "start": prev_end,
                "end": fallback_end,
                "target_duration": fallback_end - prev_end,
                "confidence": 0.0,
            })
            # Don't advance cursor on a fallback — next line still gets
            # to search from the same position.
            continue

        start = b["start"]
        end = b["end"]

        if end <= start:
            end = start + 3.0

        targets.append({
            "start": start,
            "end": end,
            "target_duration": end - start,
            "confidence": b["confidence"],
        })

        # Advance sequential cursor past this match's end-word.
        if sequential:
            _seq_cursor = b.get("end_word_idx", _seq_cursor) + 1
    
    for i, t in enumerate(targets):
        print(f"[MasterAlign]   Clip {i}: {t['start']:.2f}s → {t['end']:.2f}s "
              f"(duration={t['target_duration']:.2f}s, conf={t['confidence']:.2f})")
    
    # Log gaps (sort by start time for gap analysis)
    sorted_targets = sorted(enumerate(targets), key=lambda x: x[1]["start"])
    if sorted_targets:
        first = sorted_targets[0][1]
        if first["start"] > 0.1:
            print(f"[MasterAlign]   GAP: 0.00s → {first['start']:.2f}s (intro black)")
        for j in range(len(sorted_targets) - 1):
            curr = sorted_targets[j][1]
            nxt = sorted_targets[j+1][1]
            gap = nxt["start"] - curr["end"]
            if gap > 0.05:
                print(f"[MasterAlign]   GAP: {curr['end']:.2f}s → {nxt['start']:.2f}s (black: {gap:.2f}s)")
            elif gap < -0.05:
                print(f"[MasterAlign]   ⚠ OVERLAP: clip {sorted_targets[j][0]} and {sorted_targets[j+1][0]} overlap by {-gap:.2f}s")
        last = sorted_targets[-1][1]
        trail = master_duration - last["end"]
        if trail > 0.1:
            print(f"[MasterAlign]   GAP: {last['end']:.2f}s → {master_duration:.2f}s (outro black)")
    
    return targets


def make_still_segment(image_path, duration: float, output_path,
                       width: int, height: int, fps: int = 24) -> dict:
    """v825 — render a still image as a `duration`-second SILENT video segment,
    letterboxed to width×height on black. Used to place a support image on the
    support track for its word-span. CFR + explicit fps for clean concat."""
    duration = max(0.1, float(duration))
    vf = (f"scale={width}:{height}:force_original_aspect_ratio=decrease,"
          f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2:black,setsar=1,fps={fps}")
    cmd = [
        FFMPEG_BIN, "-y", "-loop", "1", "-i", str(image_path),
        "-t", f"{duration:.6f}",
        "-vf", vf,
        "-r", str(fps), "-vsync", "cfr",
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "18",
        "-pix_fmt", "yuv420p", "-an",
        str(output_path),
    ]
    code, _, err = run(cmd)
    if code != 0:
        raise RuntimeError(f"make_still_segment failed: {err}")
    return {"duration": duration, "path": str(output_path)}


def process_clip_for_alignment(
    clip_path: Path,
    target_duration: float,
    output_path: Path,
    max_speed: float = 1.5,
) -> dict:
    """
    Adjust a single clip to match target_duration:
    - Speed factor 1.0–max_speed → speed up with setpts/atempo
    - Speed factor > max_speed (clip too long) → speed up at max_speed cap, then trim
    - Speed factor < 1.0 (clip too short) → boomerang loop until target_duration
    
    Output is always video-only (no audio).
    Returns: {method, speed_factor, original_duration, target_duration}
    """
    info = ffprobe_json(clip_path)
    clip_duration = get_duration(info)
    
    if target_duration <= 0:
        target_duration = clip_duration  # safety fallback
    
    speed_factor = clip_duration / target_duration
    
    print(f"[MasterAlign]   Clip {clip_path.name}: {clip_duration:.2f}s → {target_duration:.2f}s "
          f"(factor={speed_factor:.2f}×, max={max_speed:.1f}×)")
    
    result = {
        "original_duration": clip_duration,
        "target_duration": target_duration,
        "speed_factor": speed_factor,
    }
    
    min_speed = min(1.0, max_speed)  # Allow slight slowdown if max_speed < 1.0
    
    if min_speed <= speed_factor <= max_speed:
        # Comfortable range: adjust speed (speedup or slight slowdown)
        result["method"] = "speed"
        cmd = [
            FFMPEG_BIN, "-y", "-i", str(clip_path),
            "-filter:v", f"setpts={1/speed_factor:.6f}*PTS",
            "-an",  # strip audio
            # v560: force constant 24fps output. Without this, setpts=PTS/N
            # adjusts presentation timestamps but ffmpeg keeps the original
            # frame count, producing a variable-framerate clip whose
            # container duration says X seconds but whose internal packet
            # timestamps span the original (longer) duration. That mismatch
            # was invisible until the concat-with-transitions step did
            # stream-copy concat, where ffmpeg trusts internal PTS over
            # container metadata and the final video came out 4-5x too
            # long. Forcing CFR + explicit framerate makes the encoded
            # stream's internal timing match the container duration.
            "-r", "24", "-vsync", "cfr",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "18",
            "-pix_fmt", "yuv420p",
            "-t", f"{target_duration:.6f}",
            str(output_path)
        ]
    elif speed_factor > max_speed:
        result["method"] = "speed_and_trim"
        capped_speed = max_speed
        cmd = [
            FFMPEG_BIN, "-y", "-i", str(clip_path),
            "-filter:v", f"setpts={1/capped_speed:.6f}*PTS",
            "-an",  # strip audio
            # v560: same CFR fix as above
            "-r", "24", "-vsync", "cfr",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "18",
            "-pix_fmt", "yuv420p",
            "-t", f"{target_duration:.6f}",
            str(output_path)
        ]
        print(f"[MasterAlign]   Capped at {max_speed:.1f}× speed + trim to {target_duration:.2f}s "
              f"(raw clip {clip_duration:.2f}s would need {speed_factor:.2f}×)")
    else:
        # speed_factor < 1.0 — clip too short, need to extend via boomerang loop
        result["method"] = "loop"
        # Build a boomerang: forward + reverse, then trim to target
        # First create reversed clip
        reversed_path = output_path.parent / f"rev_{output_path.name}"
        cmd_rev = [
            FFMPEG_BIN, "-y", "-i", str(clip_path),
            "-vf", "reverse",
            "-an",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "18",
            "-pix_fmt", "yuv420p",
            str(reversed_path)
        ]
        code, _, err = run(cmd_rev)
        if code != 0:
            # Fallback: just slow down
            print(f"[MasterAlign]   Reverse failed, falling back to slowdown: {err[:100]}")
            result["method"] = "slowdown"
            slow_factor = max(0.5, speed_factor)  # don't go below 0.5×
            cmd = [
                FFMPEG_BIN, "-y", "-i", str(clip_path),
                "-filter:v", f"setpts={1/slow_factor:.6f}*PTS",
                "-an",
                # v560: CFR fix — same reason as the speedup path above
                "-r", "24", "-vsync", "cfr",
                "-c:v", "libx264", "-preset", "veryfast", "-crf", "18",
                "-pix_fmt", "yuv420p",
                "-t", f"{target_duration:.6f}",
                str(output_path)
            ]
            code, _, err = run(cmd)
            if code != 0:
                raise RuntimeError(f"Slowdown failed: {err}")
            return result
        
        # Concat forward + reverse + forward... until we exceed target_duration
        # Then trim to exact target
        loop_parts = []
        accumulated = 0.0
        toggle = True  # True = forward, False = reverse
        while accumulated < target_duration:
            loop_parts.append(str(clip_path) if toggle else str(reversed_path))
            accumulated += clip_duration
            toggle = not toggle
        
        # Write concat list
        concat_list = output_path.parent / f"loop_list_{output_path.stem}.txt"
        with open(concat_list, "w") as f:
            for p in loop_parts:
                f.write(f"file {shlex.quote(p)}\n")
        
        looped_path = output_path.parent / f"looped_{output_path.name}"
        cmd_concat = [
            FFMPEG_BIN, "-y",
            "-f", "concat", "-safe", "0", "-i", str(concat_list),
            "-an",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "18",
            "-pix_fmt", "yuv420p",
            str(looped_path)
        ]
        code, _, err = run(cmd_concat)
        if code != 0:
            raise RuntimeError(f"Loop concat failed: {err}")
        
        # Trim to exact target duration
        cmd = [
            FFMPEG_BIN, "-y", "-i", str(looped_path),
            "-t", f"{target_duration:.6f}",
            "-an",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "18",
            "-pix_fmt", "yuv420p",
            str(output_path)
        ]
        code, _, err = run(cmd)
        if code != 0:
            raise RuntimeError(f"Loop trim failed: {err}")
        
        # Cleanup temp files
        for tmp in [reversed_path, looped_path, concat_list]:
            try:
                Path(tmp).unlink()
            except Exception:
                pass
        
        return result
    
    # Run the command (speed or trim path)
    code, _, err = run(cmd)
    if code != 0:
        raise RuntimeError(f"Clip alignment ({result['method']}) failed: {err}")
    
    return result


def _generate_black_video(output_path: Path, duration: float, width: int, height: int, fps: float = 24.0) -> None:
    """Generate a silent black video of the given duration and resolution."""
    cmd = [
        FFMPEG_BIN, "-y",
        "-f", "lavfi", "-i", f"color=c=black:s={width}x{height}:r={fps:.2f}:d={duration:.6f}",
        "-an",
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "18",
        "-pix_fmt", "yuv420p",
        "-t", f"{duration:.6f}",
        str(output_path)
    ]
    code, _, err = run(cmd)
    if code != 0:
        raise RuntimeError(f"Black video generation failed: {err}")


def export_with_master_audio(
    clip_info: list,
    dialogue_lines: list,
    master_audio_path: Path,
    output_path: Path,
    frames_to_cut_start: int = 0,
    frames_to_cut_end: int = 0,
    transition: str = "none",
    transition_duration: float = 0.5,
    max_clip_speed: float = 1.5,
    min_gap_for_black: float = 2.0,
    sequential_alignment: bool = False,
    pre_computed_targets: list = None,  # v701zd — bypass Whisper master
) -> dict:
    """
    Full pipeline: align clips to master audio, speed-adjust, concat with black gaps, mux audio.
    
    Each clip plays ONLY during its dialogue line's timestamps.
    Gaps shorter than min_gap_for_black are filled by extending the previous clip.
    Larger gaps (intro, between clips, outro) are filled with black frames.
    
    1. Transcribe master audio (word timestamps)
    2. Match each dialogue line → get exact start/end timestamps
    3. Optionally trim each clip (start/end frames) first
    4. Speed-adjust / trim / loop each clip to match its dialogue duration
    5. Build timeline: black → clip → black → clip → ... → black
    6. Concat all segments (video only)
    7. Mux master audio on top
    
    Returns: dict with stats
    """
    print(f"[MasterAlign] === Starting master audio alignment (exact dialogue timing) ===")
    print(f"[MasterAlign] Clips: {len(clip_info)}, Master: {master_audio_path}")

    if len(clip_info) != len(dialogue_lines):
        raise ValueError(
            f"Clip count ({len(clip_info)}) != dialogue line count ({len(dialogue_lines)}). "
            f"Each clip must have a corresponding dialogue line."
        )

    # v701zd — when caller supplies pre_computed_targets (from speaker's
    # per-clip post-VAD durations) skip the Whisper master transcription
    # + fuzzy line matcher entirely. The Whisper-master path repeatedly
    # under-transcribed the 55s speaker concat (61 words for a 127-word
    # script across vad_filter on/off, model size small/tiny/base) which
    # bricked alignment of HOOK + opening voiceovers. Speaker pipeline
    # already produced authoritative per-clip durations in its per-clip
    # Whisper-VAD pass — those ARE the master timeline positions. No
    # second transcription needed.
    if pre_computed_targets is not None:
        if len(pre_computed_targets) != len(clip_info):
            raise ValueError(
                f"pre_computed_targets length ({len(pre_computed_targets)}) "
                f"must match clip_info length ({len(clip_info)})"
            )
        master_words = None
        targets = list(pre_computed_targets)
        # v773.10.17 — master_duration must reflect the ACTUAL master audio
        # length, not just the last target's end. When the speaker pipeline
        # has unpaired tail clips (no b-roll partner), the pre_computed_targets
        # cover only the b-roll-bearing positions; the trailing unpaired
        # speaker time must be rendered as black so the b-roll video stays
        # the same length as the speaker master audio.
        _targets_end = max((t.get("end", 0.0) for t in targets), default=0.0)
        try:
            _audio_info = ffprobe_json(master_audio_path)
            _audio_dur = get_duration(_audio_info)
        except Exception:
            _audio_dur = 0.0
        master_duration = max(_targets_end, _audio_dur)
        print(
            f"[MasterAlign] v701zd pre-computed targets (skip Whisper master): "
            f"targets_end={_targets_end:.2f}s audio_dur={_audio_dur:.2f}s "
            f"→ master_duration={master_duration:.2f}s, {len(targets)} clips",
            flush=True,
        )
        for _i, _t in enumerate(targets):
            print(
                f"[MasterAlign]   Clip {_i}: {_t['start']:.2f}s → {_t['end']:.2f}s "
                f"(duration={_t.get('target_duration', _t['end'] - _t['start']):.2f}s, "
                f"conf={_t.get('confidence', 1.0):.2f})",
                flush=True,
            )
    else:
        # Step 1: Transcribe master audio (legacy path)
        _master_prompt = " ".join((l or "").strip() for l in dialogue_lines if (l or "").strip()).strip()
        master_words = transcribe_master_audio(
            master_audio_path, initial_prompt=_master_prompt or None
        )
        if not master_words:
            raise RuntimeError("Master audio transcription produced no words")

        # Get actual master audio duration
        master_info = ffprobe_json(master_audio_path)
        master_duration = get_duration(master_info)
        print(f"[MasterAlign] Master audio duration: {master_duration:.2f}s (last word ends at {master_words[-1]['end']:.2f}s)")

        # Step 2: Calculate target durations (each clip = its own dialogue only)
        targets = calculate_clip_targets(
            master_words,
            dialogue_lines,
            master_duration=master_duration,
            sequential=sequential_alignment,
        )
    
    # Fill small gaps by extending the previous clip (avoids black flashes between words)
    # Sort clips by start time, then for each small gap, extend the earlier clip's end
    if min_gap_for_black > 0 and len(targets) > 1:
        sorted_indices = sorted(range(len(targets)), key=lambda i: targets[i]["start"])
        gaps_filled = 0
        for j in range(len(sorted_indices) - 1):
            curr_idx = sorted_indices[j]
            next_idx = sorted_indices[j + 1]
            gap = targets[next_idx]["start"] - targets[curr_idx]["end"]
            if 0 < gap < min_gap_for_black:
                old_end = targets[curr_idx]["end"]
                targets[curr_idx]["end"] = targets[next_idx]["start"]
                targets[curr_idx]["target_duration"] = targets[curr_idx]["end"] - targets[curr_idx]["start"]
                gaps_filled += 1
                print(f"[MasterAlign] Clip {curr_idx}: extended {old_end:.2f}s → {targets[curr_idx]['end']:.2f}s "
                      f"(filled {gap:.2f}s gap, threshold={min_gap_for_black:.1f}s)")
        # Also extend the last clip to the end of the audio if gap is small
        last_idx = sorted_indices[-1]
        trail = master_duration - targets[last_idx]["end"]
        if 0 < trail < min_gap_for_black:
            targets[last_idx]["end"] = master_duration
            targets[last_idx]["target_duration"] = targets[last_idx]["end"] - targets[last_idx]["start"]
            gaps_filled += 1
            print(f"[MasterAlign] Clip {last_idx}: extended to audio end {master_duration:.2f}s (filled {trail:.2f}s outro)")
        if gaps_filled:
            print(f"[MasterAlign] Filled {gaps_filled} small gap(s) (threshold: {min_gap_for_black:.1f}s)")
    
    # Get video resolution from first clip (for black frame generation)
    first_clip_info = ffprobe_json(Path(clip_info[0]["path"]))
    vid_stream = next((s for s in first_clip_info.get("streams", []) if s.get("codec_type") == "video"), None)
    vid_width = int(vid_stream.get("width", 720)) if vid_stream else 720
    vid_height = int(vid_stream.get("height", 1280)) if vid_stream else 1280
    vid_fps = get_fps(first_clip_info)
    print(f"[MasterAlign] Video format: {vid_width}x{vid_height} @ {vid_fps:.1f}fps")
    
    stats = {
        "master_audio_aligned": True,
        "clips_processed": len(clip_info),
        "master_words": len(master_words) if master_words is not None else 0,  # v701zd — pre-computed path has None
        "master_duration": master_duration,
        "clip_details": [],
        "black_segments": 0,
        "total_black_duration": 0.0,
        "pre_computed_targets": pre_computed_targets is not None,
    }
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Step 3-4: Process each clip (trim + speed-adjust)
        processed_clips = {}  # index → aligned_path
        for i, (info, target) in enumerate(zip(clip_info, targets)):
            clip_path = Path(info["path"])
            
            # Optional frame trimming first
            if frames_to_cut_start > 0 or frames_to_cut_end > 0:
                skip_start = info.get("skip_start_trim", False)
                actual_start = 0 if skip_start else frames_to_cut_start
                trimmed_path = temp_path / f"trimmed_{i:04d}.mp4"
                trim_video(clip_path, trimmed_path, actual_start, frames_to_cut_end)
                clip_path = trimmed_path
            
            # Speed-adjust to dialogue duration
            aligned_path = temp_path / f"aligned_{i:04d}.mp4"
            clip_result = process_clip_for_alignment(
                clip_path, target["target_duration"], aligned_path,
                max_speed=max_clip_speed,
            )
            clip_result["target_start"] = target["start"]
            clip_result["target_end"] = target["end"]
            clip_result["confidence"] = target["confidence"]
            stats["clip_details"].append(clip_result)
            
            processed_clips[i] = aligned_path
        
        # Step 5: Build timeline with black fills
        # Sort clips by their position in the master audio (clips may be in any order)
        sorted_indices = sorted(range(len(targets)), key=lambda i: targets[i]["start"])
        
        timeline_segments = []  # List of paths in chronological order
        cursor = 0.0
        MIN_BLACK = 0.04  # Minimum black segment duration (1 frame at 24fps)
        
        for i in sorted_indices:
            target = targets[i]
            clip_start = target["start"]
            clip_end = target["end"]
            
            # Gap before this clip?
            gap = clip_start - cursor
            if gap > MIN_BLACK:
                black_path = temp_path / f"black_pre_{i:04d}.mp4"
                print(f"[MasterAlign]   Black: {cursor:.2f}s → {clip_start:.2f}s ({gap:.2f}s)")
                _generate_black_video(black_path, gap, vid_width, vid_height, vid_fps)
                timeline_segments.append(black_path)
                stats["black_segments"] += 1
                stats["total_black_duration"] += gap
            elif gap < -MIN_BLACK:
                # Overlap — skip the overlapping portion (trim clip start forward)
                print(f"[MasterAlign]   ⚠ Clip {i} overlaps previous by {-gap:.2f}s — starting from cursor {cursor:.2f}s")
                clip_start = cursor
            
            # The clip itself
            timeline_segments.append(processed_clips[i])
            cursor = max(cursor, clip_end)
        
        # Trailing gap after last clip?
        trail = master_duration - cursor
        if trail > MIN_BLACK:
            black_path = temp_path / f"black_outro.mp4"
            print(f"[MasterAlign]   Black: {cursor:.2f}s → {master_duration:.2f}s ({trail:.2f}s)")
            _generate_black_video(black_path, trail, vid_width, vid_height, vid_fps)
            timeline_segments.append(black_path)
            stats["black_segments"] += 1
            stats["total_black_duration"] += trail
        
        print(f"[MasterAlign] Timeline: {len(timeline_segments)} segments "
              f"({len(processed_clips)} clips + {stats['black_segments']} black fills, "
              f"total black: {stats['total_black_duration']:.2f}s)")
        
        # Step 6: Concat all segments (video only)
        video_only_path = temp_path / "video_only.mp4"
        if transition and transition != "none" and stats["black_segments"] == 0:
            # Only use xfade transitions when there are no black gaps
            # (xfade between clip and black doesn't look good)
            concat_videos_with_transitions(
                timeline_segments, video_only_path,
                transition=transition,
                transition_duration=transition_duration,
                has_audio=False,
            )
        else:
            concat_videos(timeline_segments, video_only_path)
        
        # Step 7: Mux master audio on top
        print(f"[MasterAlign] Muxing master audio onto timeline video")
        cmd = [
            FFMPEG_BIN, "-y",
            "-i", str(video_only_path),
            "-i", str(master_audio_path),
            "-c:v", "copy",
            "-c:a", "aac", "-b:a", "192k",
            "-map", "0:v:0",
            "-map", "1:a:0",
            "-shortest",
            str(output_path)
        ]
        code, _, err = run(cmd)
        if code != 0:
            raise RuntimeError(f"Master audio mux failed: {err}")
    
    # Final stats
    final_info = ffprobe_json(output_path)
    stats["final_duration"] = get_duration(final_info)
    
    methods = [d["method"] for d in stats["clip_details"]]
    stats["methods_used"] = {m: methods.count(m) for m in set(methods)}
    
    print(f"[MasterAlign] === Complete ===")
    print(f"[MasterAlign] Final duration: {stats['final_duration']:.2f}s")
    print(f"[MasterAlign] Methods: {stats['methods_used']}")
    print(f"[MasterAlign] Black segments: {stats['black_segments']} ({stats['total_black_duration']:.2f}s)")

    return stats


def export_support_track(support_clips: list, master_duration: float,
                         output_path, width: int = 1080, height: int = 1920,
                         fps: int = 24) -> dict:
    """v825.3 — build the SILENT support-image track for post-production compositing.

    support_clips: [{image_index, path (still png), start, end}, ...] where
    start/end are ABSOLUTE seconds on the master timeline. Each still shows for
    [start,end]; the rest is black; total length == master_duration so the
    operator can drop it over the talking-head master. NO audio. Overlapping
    spans clamped last-wins (earlier end -> next start).

    v825.3 FIX (placement drift): each still is placed by a SINGLE ffmpeg
    overlay filtergraph — a black base of exact master_duration with
    `overlay=enable='between(t,start,end)'` per still — so every still lands at
    its EXACT absolute timestamp. The prior v825.0-.2 built the timeline by
    concatenating black-filler + still segments SEQUENTIALLY; per-segment
    frame-rounding accumulated (measured +0.5s..+1.6s drift on a 7-still 40s
    track), so overlays landed progressively LATE vs the spoken words. The
    filtergraph approach measured 0.00s drift on the same case.
    """
    clips = sorted([c for c in support_clips if c and c.get("path")], key=lambda c: c["start"])
    for i, c in enumerate(clips):
        c["start"] = max(0.0, min(c["start"], master_duration))
        c["end"] = max(c["start"] + 0.1, min(c["end"], master_duration))
        if i + 1 < len(clips):
            c["end"] = min(c["end"], clips[i + 1]["start"])

    # v867 — MEMORY. On Render's ~1-vCPU 2GB box this filtergraph OOM-killed the
    # WHOLE container (2026-07-23: [Mem/v866] showed cgroup used climbing 708MB
    # -> 2044MB in a steady linear ramp across this exact render while Python
    # RSS stayed flat ~557MB — i.e. the growth is inside ffmpeg, not us). Cause:
    # x264 on one slow CPU cannot drain frames as fast as the N-overlay chain
    # produces them, so ffmpeg's internal filter->encoder queue grows unbounded
    # for the whole render. Two changes attack exactly that, both verified to
    # keep byte-exact still placement (the v825.3 anti-drift guarantee):
    #
    #   1. FINITE inputs. Each still was `-loop 1 -i img` = an INFINITE full-
    #      master-length stream (N x master_duration x fps frames can be in
    #      flight). Replace with `-itsoffset START -loop 1 -framerate fps -t SPAN
    #      -i img`: the image exists only for its own visible window (~3s), so
    #      after it ends `overlay ...:eof_action=pass` passes the base through.
    #      Frames-in-flight drop from N*master to N*span (~24x less here).
    #   2. `ultrafast`. The encoder drains far faster than `veryfast`, so the
    #      queue cannot build on a slow consumer. crf 20 (was 18) — this is a
    #      SILENT overlay track the operator composites in post, so a hair more
    #      quantization is invisible and irrelevant.
    #   Plus `-threads 1 -filter_threads 1`: one slow vCPU gains nothing from
    #   more threads, and each thread carries its own frame buffers.
    inputs = ["-f", "lavfi", "-i",
              f"color=c=black:s={width}x{height}:r={fps}:d={master_duration:.6f}"]
    for c in clips:
        _span = max(0.05, float(c["end"]) - float(c["start"]))
        inputs += ["-itsoffset", f"{float(c['start']):.6f}",
                   "-loop", "1", "-framerate", str(fps), "-t", f"{_span:.6f}",
                   "-i", str(c["path"])]

    fc = []
    for idx in range(len(clips)):
        fc.append(
            f"[{idx+1}:v]scale={width}:{height}:force_original_aspect_ratio=decrease,"
            f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2:black,setsar=1,fps={fps}[s{idx}]"
        )
    prev = "0:v"
    for idx, c in enumerate(clips):
        fc.append(
            f"[{prev}][s{idx}]overlay=enable='between(t,{c['start']:.6f},{c['end']:.6f})'"
            f":eof_action=pass[o{idx}]"
        )
        prev = f"o{idx}"

    map_lbl = f"[{prev}]" if clips else "0:v"
    cmd = [FFMPEG_BIN, "-y", "-threads", "1", "-filter_threads", "1"] + inputs
    if fc:
        cmd += ["-filter_complex", ";".join(fc)]
    cmd += [
        "-map", map_lbl,
        "-t", f"{master_duration:.6f}",
        "-r", str(fps),
        "-c:v", "libx264", "-preset", "ultrafast", "-crf", "20",
        "-pix_fmt", "yuv420p", "-an",
        "-movflags", "+faststart", str(output_path),
    ]
    code, _, err = run(cmd)
    if code != 0:
        raise RuntimeError(f"export_support_track filtergraph failed: {(err or '')[-500:]}")

    return {
        "support_track": True,
        "stills_placed": len(clips),
        "master_duration": master_duration,
        "output": str(output_path),
    }


def export_final_video(
    clip_info: List[dict],
    output_path: Path,
    frames_to_cut_start: int = 0,   # Default: no start trim
    frames_to_cut_end: int = 7,     # Default: trim 7 frames from end (removes morph artifacts)
    remove_silence: bool = False,
    silence_mode: str = "energy",   # "energy" = ffmpeg silencedetect, "whisper" = speech-based
    vad_threshold: float = 0.75,
    silence_trigger: float = 1.5,   # Only gaps >= this duration are trimmed (seconds)
    silence_keep: float = 0.3,      # How much silence to preserve at each cut point (seconds)
    transition: str = "none",       # xfade transition type
    transition_duration: float = 0.5,
    progress_callback=None,
    dialogue_texts: List[str] = None,
    language: str = "English",
    cut_prefix_audio: bool = False,  # v542
    prefix_word: str = "only",  # v542
) -> dict:
    """
    Main export function: trim, concat, and optionally apply VAD.
    
    Args:
        clip_info: List of dicts with keys:
            - path: Path to video file
            - clip_index: Index of clip
            - skip_start_trim: Whether to skip trimming start frames
        output_path: Where to save the final video
        frames_to_cut_start: Frames to trim from start (default 0)
        frames_to_cut_end: Frames to trim from end (default 7 - removes morph artifacts)
        remove_silence: Whether to apply VAD silence removal
        vad_threshold: VAD sensitivity (0-1, higher = more aggressive detection)
        silence_trigger: Minimum silence gap to act on (seconds).
                         Pauses shorter than this are left completely untouched.
                         Default 1.5s — ignores brief breath pauses.
        silence_keep: How much silence to preserve at each trimmed cut point (seconds).
                      0.0 = cut right to the speech edge (very tight).
                      0.3 = leave a short natural pause (default, recommended).
                      0.8 = keep a noticeable beat of silence at each cut.
        progress_callback: Optional callback for progress updates
    
    Returns:
        dict with processing stats
    """
    print(f"[VideoProcessor] export_final_video called")
    print(f"[VideoProcessor] clip_info count: {len(clip_info)}")
    print(f"[VideoProcessor] output_path: {output_path}")
    
    if not clip_info:
        raise ValueError("No clips provided")
    
    # Check if any trimming is needed
    needs_trimming = frames_to_cut_start > 0 or frames_to_cut_end > 0
    # v681 — text_card clips need to be rendered via ffmpeg drawtext at
    # this stage (no Veo source exists for them). Force the per-clip
    # trim path so the renderer runs. Also disable remove_silence so
    # whisper-VAD can't collapse a silent text-card to zero seconds.
    # v691b — capture user's ORIGINAL VAD intent BEFORE any bypass guard
    # mutates remove_silence to False. The text_card and timeline bypasses
    # below both downgrade the global flag, but v691's per-clip Whisper-VAD
    # in the trim loop needs to know the user's original setting so it
    # can apply VAD to on-camera clips even when text_card / timeline
    # scenes coexist in the same export. Pre-v691b the save lived AFTER
    # the text_card bypass so the saved value was always False on
    # mixed-mode exports — Whisper-VAD never ran per-clip.
    _user_remove_silence = remove_silence
    _user_silence_mode = silence_mode

    has_text_cards = any(
        (c.get("scene_type") or "").lower() == "text_card"
        for c in clip_info
    )
    if has_text_cards:
        if not needs_trimming:
            print(
                "[VideoProcessor] Forcing per-clip trim path: text-card "
                "scene(s) require ffmpeg drawtext render",
                flush=True,
            )
            needs_trimming = True
        if remove_silence:
            print(
                "[VideoProcessor] Global VAD bypassed (text-card present); "
                "v691 will run Whisper-VAD per-clip on non-text_card clips with dialogue.",
                flush=True,
            )
            remove_silence = False
    # v668 — timeline-mode clips MUST go through the per-clip trim path so
    # ffmpeg can cut them to exactly target_duration_s. Force the trim path
    # on if any clip declares cut_mode='timeline' with a target duration.
    # Also disable remove_silence: timeline clips were anchor-trimmed to
    # exact duration; whisper-VAD would over-collapse silent transformation
    # montages. Caller should split into two exports if mixing modes.
    has_timeline_clips = any(
        (c.get("cut_mode") or "").lower() == "timeline" and (c.get("target_duration_s") or 0) > 0
        for c in clip_info
    )
    # _user_remove_silence + _user_silence_mode hoisted above (v691b).

    if has_timeline_clips:
        if not needs_trimming:
            print(
                "[VideoProcessor] Forcing per-clip trim path: timeline-mode clip(s) "
                "require ffmpeg trim",
                flush=True,
            )
            needs_trimming = True
        if remove_silence:
            print(
                "[VideoProcessor] Global VAD bypassed (mixed timeline + auto modes); "
                "v691 will run Whisper-VAD per-clip on non-timeline clips with dialogue.",
                flush=True,
            )
            remove_silence = False
    
    stats = {
        "clips_processed": len(clip_info),
        "frames_trimmed_start": frames_to_cut_start,
        "frames_trimmed_end": frames_to_cut_end,
        "vad_applied": remove_silence,
        "vad_silence_trigger": silence_trigger if remove_silence else None,
        "vad_silence_keep": silence_keep if remove_silence else None,
        "clips_with_start_trim_skipped": sum(1 for c in clip_info if c.get("skip_start_trim", False)),
        "pre_trimmed": not needs_trimming
    }
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Determine which files to concatenate
        if needs_trimming:
            # Parallel trimming: run up to 4 FFmpeg processes simultaneously.
            # Each process uses -threads 1 so memory stays bounded:
            # 4 workers × ~80MB each = ~320MB + Python baseline ~150MB ≈ 470MB (safe for 512MB).
            print(f"[VideoProcessor] Parallel trimming: {len(clip_info)} clips, 2 workers")
            files_to_concat = [None] * len(clip_info)  # pre-allocate to preserve order

            def _trim_one(idx_info):
                idx, info = idx_info
                clip_path = info["path"]
                skip_start = info.get("skip_start_trim", False)
                trimmed_file = temp_path / f"trimmed_{idx:04d}.mp4"
                actual_start_trim = 0 if skip_start else frames_to_cut_start

                # v681 — text_card scenes: render via ffmpeg drawtext.
                # NO Veo source video for these clips; clip_path is a
                # placeholder that's never read. Caption + bg_color come
                # from the assignment row (denorm'd by main.py to the
                # info dict). Default duration 1.0s when missing.
                # NOTE: target_duration_s is NOT used as a fallback —
                # it's a Veo render duration (4/6/8s buckets), wrong for
                # text-card transitions which default to 1.0s per spec.
                if (info.get("scene_type") or "").lower() == "text_card":
                    try:
                        render_text_card(
                            output_path=trimmed_file,
                            caption=info.get("caption") or "",
                            bg_color=info.get("bg_color") or "black",
                            duration_s=float(info.get("duration_s") or 1.0),
                        )
                    except Exception as e:
                        # Preserve concat slot order: emit a 1s black-clip
                        # fallback so files_to_concat[idx-1] gets populated.
                        # Without this, the pre-allocated slot stays None
                        # and concat would fail on slot order mismatch.
                        logger.warning(
                            f"render_text_card failed for clip "
                            f"{info.get('clip_index', idx)}: {e}; "
                            f"falling back to 1s black silent clip"
                        )
                        render_text_card(
                            output_path=trimmed_file,
                            caption="",
                            bg_color="black",
                            duration_s=1.0,
                        )
                    return idx - 1, trimmed_file

                # v668 — timeline-cut mode: ignore frame trim, ffmpeg-trim
                # the clip to exactly target_duration_s seconds. Used for
                # transformation montages where the cut should follow the
                # source-video timestamps captured at decode time, not
                # whisper-VAD speech detection. Falls back to legacy
                # frame trim when target_duration_s is missing or invalid.
                cm = (info.get("cut_mode") or "").lower()
                td = info.get("target_duration_s")
                if cm == "timeline" and td and td > 0:
                    logger.info(
                        f"Clip {info.get('clip_index', idx)}: cut_mode=timeline "
                        f"target_duration_s={td:.3f}s (ffmpeg-trim, frame-trim ignored)"
                    )
                    print(
                        f"[VideoProcessor/timeline] clip {info.get('clip_index', idx)} "
                        f"→ trim to {td:.3f}s",
                        flush=True,
                    )
                    cmd = [
                        FFMPEG_BIN, "-y",
                        "-i", str(clip_path),
                        "-t", f"{td:.6f}",
                        "-c:v", "libx264", "-preset", "veryfast", "-crf", "18",
                        "-c:a", "aac", "-b:a", "192k",
                        "-movflags", "+faststart",
                        str(trimmed_file),
                    ]
                    code, _, err = run(cmd)
                    if code != 0:
                        logger.warning(
                            f"Clip {info.get('clip_index', idx)}: timeline trim failed "
                            f"({err[:200]}); falling back to frame trim"
                        )
                        trim_video(clip_path, trimmed_file, actual_start_trim, frames_to_cut_end)
                else:
                    logger.info(f"Clip {info.get('clip_index', idx)}: start_trim={actual_start_trim}, end_trim={frames_to_cut_end}")
                    trim_video(clip_path, trimmed_file, actual_start_trim, frames_to_cut_end)
                    # v691d — per-clip Whisper-VAD MOVED out of this parallel
                    # path to a serial post-loop below. Pre-v691d two parallel
                    # workers each loaded the Whisper-small model
                    # (~250MB each) simultaneously → ~500MB peak → OOM on
                    # Render's free tier. Serial post-loop loads the model
                    # ONCE, reuses it across clips. Memory ceiling drops to
                    # ~250MB for the VAD pass.
                # Free the downloaded source file immediately after trimming
                try:
                    if clip_path.exists() and str(clip_path).startswith("/app/data/outputs"):
                        clip_path.unlink()
                except Exception:
                    pass
                return idx - 1, trimmed_file  # return 0-based index so we can sort

            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = {pool.submit(_trim_one, (idx, info)): idx
                           for idx, info in enumerate(clip_info, 1)}
                done = 0
                for fut in as_completed(futures):
                    slot, trimmed_file = fut.result()
                    files_to_concat[slot] = trimmed_file
                    done += 1
                    if progress_callback:
                        progress_callback(f"Trimming clips... {done}/{len(clip_info)}")

            print(f"[VideoProcessor] Parallel trim complete")

            # v691d — SERIAL per-clip Whisper-VAD pass. Runs AFTER parallel
            # trim completes so only ONE Whisper-small model load happens
            # at a time (peak ~250MB instead of 2× ~500MB on parallel).
            # Iterates clip_info in order, applies VAD only to clips that
            # match the v691 condition (user enabled whisper + clip has
            # dialogue + clip is NOT timeline-mode + NOT text_card).
            _per_clip_whisper_ran = False  # v701w — sentinel for final-pass skip
            if _user_remove_silence and (_user_silence_mode or "").lower() == "whisper":
                vad_targets = []
                for slot_zero, info in enumerate(clip_info):
                    cm = (info.get("cut_mode") or "").lower()
                    st = (info.get("scene_type") or "").lower()
                    if cm == "timeline" or st == "text_card":
                        continue
                    # v692f — pass ONLY the line (dialogue_text) to apply_vad,
                    # NOT line+pad. Veo renders the full prompt (line + pad)
                    # so the audio contains both, but the user only wants the
                    # line in the final output. By feeding Whisper just the
                    # line as the script, pad words come back as "unmatched"
                    # and get cut from the audio. This drops the pad trailer
                    # ("so you can see real results in your own life within
                    # just two months too.") and keeps only the line ("I can
                    # help. Comment GUIDE and I will send you...").
                    _ct = (info.get("dialogue_text") or "").strip()
                    if not _ct:
                        continue
                    vad_targets.append((slot_zero, info, _ct))

                if vad_targets:
                    _per_clip_whisper_ran = True  # v701w
                    print(
                        f"[VideoProcessor/v691d] running serial Whisper-VAD on "
                        f"{len(vad_targets)} clip(s) (single model load)",
                        flush=True,
                    )
                    # v701x — malloc_trim(0) helper. After each per-clip
                    # apply_vad, glibc holds onto freed pages instead of
                    # returning them to the OS. Render's 2GB ceiling is
                    # measured against RSS, not Python's heap, so unless
                    # we explicitly trim, peak RSS climbs across the loop.
                    def _mem_trim():
                        try:
                            import gc as _gc
                            _gc.collect()
                            import ctypes as _ct
                            _libc = _ct.CDLL("libc.so.6")
                            _libc.malloc_trim(0)
                        except Exception:
                            pass

                    # v701y — Load Whisper ONCE for the whole per-clip loop.
                    # Pre-v701y each iteration loaded + unloaded its own
                    # WhisperModel; even with tiny that's 11x ~50MB resident
                    # churn that glibc doesn't reliably reclaim → climbing
                    # RSS → eventual OOM. Single-load = constant memory plus
                    # ~5s saved on loader overhead. Passed down via the new
                    # whisper_model= kwarg on apply_vad +
                    # detect_speech_segments_whisper; receivers skip their
                    # internal load + skip the `del model` cleanup when the
                    # model is supplied from the outside (we own the
                    # lifecycle out here).
                    _shared_whisper = None
                    try:
                        from faster_whisper import WhisperModel as _WM
                        _shared_whisper = _WM("tiny", device="cpu", compute_type="int8")
                        print(
                            f"[VideoProcessor/v701y] pre-loaded Whisper-tiny once for "
                            f"{len(vad_targets)} clips (saves {len(vad_targets) - 1}× reload)",
                            flush=True,
                        )
                    except Exception as _wm_err:
                        print(
                            f"[VideoProcessor/v701y] shared model preload failed: "
                            f"{_wm_err} — falling back to per-clip load",
                            flush=True,
                        )
                        _shared_whisper = None

                    for slot_zero, info, full_text in vad_targets:
                        trimmed_file = files_to_concat[slot_zero]
                        if trimmed_file is None or not Path(trimmed_file).exists():
                            print(
                                f"[VideoProcessor/v691d] skip clip "
                                f"{info.get('clip_index', slot_zero)} — "
                                f"trimmed file missing",
                                flush=True,
                            )
                            continue
                        try:
                            _vad_out = Path(str(trimmed_file) + ".vad.mp4")
                            _v709_sink = {}  # v709 — per-clip audit sink; populated inside detect_speech_segments_whisper
                            apply_vad(
                                src=Path(trimmed_file),
                                out=_vad_out,
                                threshold=vad_threshold,
                                min_gap_duration=silence_trigger,
                                silence_keep_duration=silence_keep,
                                silence_mode="whisper",
                                dialogue_texts=[full_text],
                                language=language,
                                clip_boundaries=None,
                                cut_prefix_audio=cut_prefix_audio,
                                prefix_word=prefix_word,
                                whisper_model=_shared_whisper,  # v701y — reuse
                                v709_audit_sink=_v709_sink,  # v709 — fill with audit details
                            )
                            # v709 — stash audit dict on clip_info entry for post-concat aggregation.
                            # `info` is a reference into clip_info (constructed in vad_targets), so
                            # the assignment lands in the canonical clip_info[slot] dict.
                            info["_v709_audit"] = _v709_sink if _v709_sink else None
                            # v706 — Per-clip VAD floor guard. apply_vad has no
                            # minimum-duration safety: if the Whisper-tiny matcher
                            # returns 0-2 words (TTS lead-in, mispronunciation,
                            # fuzzy-match collapse on rare words), the kept segment
                            # is ~0.4-0.7s — a near-silent clip that drops most of
                            # the line's audio from the final concat. Concrete
                            # failure (export 20260511_213257_0bd174): clip 4
                            # 0.375s, clip 7 0.666s out of ~7s source → 2 lines of
                            # script lost. Fix: ffprobe pre-VAD + post-VAD; if
                            # post < MAX(MIN_KEEP_S, pre * MIN_KEEP_RATIO), DROP
                            # the VAD result and keep the pre-VAD trimmed file.
                            # Pre-VAD file is already frame-trimmed (skip_start +
                            # frames_to_cut_end applied) so fallback retains the
                            # line; we only lose silence-tightening on that one
                            # clip — vastly better than losing dialogue.
                            # v706.1 (2026-06-08) — script-length-aware floor.
                            # OLD floor = max(1.5, pre_d * 0.30) scaled off the
                            # FIXED Veo container (~7.73s), so a legit short line
                            # (5-6w recipe step ≈ 2s of speech) fell under the
                            # ~2.32s floor and got REJECTED back to the full-clip
                            # dead air — directly fighting the v773.10.17 silero
                            # pseudo-edge shrink (export 9fcc28 → 04acbc: clips
                            # 5/7/8 rejected at post=2.07/2.31/2.07 vs floor 2.319).
                            # The floor's real job is catching matcher COLLAPSE
                            # (Whisper heard 0-2 of N words → ~0.4-0.7s). That
                            # scales with the SCRIPT, not the container. 0.18s/word
                            # ≈ 5.5 wps is faster than any human, so the floor sits
                            # BELOW any real spoken duration and only a true
                            # collapse falls under it. Semantic missed-word safety
                            # is already carried by v708 trust gate + v709 audit.
                            MIN_KEEP_S = 1.0            # hard absolute anti-collapse floor
                            _word_count = len((full_text or "").split())
                            _vad_accepted = True
                            # v706.2 — probe the VAD OUTPUT first and in its own
                            # guard. A truncated/0-length _vad_out is exactly the
                            # case that makes ffprobe throw; the OLD single try
                            # then defaulted to ACCEPT, promoting the one bad file
                            # the floor exists to reject. On output-probe failure
                            # → REJECT (keep the known-good pre-VAD file).
                            try:
                                _post_d = float(get_duration(ffprobe_json(_vad_out)))
                            except Exception as _gd_err:
                                _vad_accepted = False
                                print(
                                    f"[VideoProcessor/v706] ⚠ clip "
                                    f"{info.get('clip_index', slot_zero)} VAD output "
                                    f"unprobeable ({_gd_err}) → REJECTED "
                                    f"(keeping pre-VAD trimmed file)",
                                    flush=True,
                                )
                            else:
                                try:
                                    _pre_d = float(get_duration(ffprobe_json(Path(trimmed_file))))
                                except Exception:
                                    _pre_d = -1.0  # log-only; floor uses post vs script
                                _floor = max(MIN_KEEP_S, _word_count * 0.18)
                                if _post_d < _floor:
                                    _vad_accepted = False
                                    print(
                                        f"[VideoProcessor/v706] ⚠ clip "
                                        f"{info.get('clip_index', slot_zero)} VAD "
                                        f"REJECTED: pre={_pre_d:.3f}s post={_post_d:.3f}s "
                                        f"floor={_floor:.3f}s ({_word_count}w script; matcher "
                                        f"likely missed words; keeping pre-VAD trimmed file)",
                                        flush=True,
                                    )

                            if _vad_accepted:
                                # v706.2 — rename INTO place first, unlink the
                                # pre-VAD file only after the rename succeeds. The
                                # old order (unlink then rename) lost the clip
                                # entirely if the rename raised. os.replace is
                                # atomic + overwrites the destination.
                                try:
                                    import os as _os_rep
                                    _os_rep.replace(str(_vad_out), str(trimmed_file))
                                    print(
                                        f"[VideoProcessor/v691d] clip "
                                        f"{info.get('clip_index', slot_zero)} → "
                                        f"per-clip Whisper-VAD applied",
                                        flush=True,
                                    )
                                except Exception as _rep_err:
                                    # Rename failed → pre-VAD file is still intact.
                                    # Keep it; drop the VAD output.
                                    print(
                                        f"[VideoProcessor/v706] ⚠ clip "
                                        f"{info.get('clip_index', slot_zero)} VAD "
                                        f"rename failed ({_rep_err}) → keeping pre-VAD file",
                                        flush=True,
                                    )
                                    try:
                                        _vad_out.unlink()
                                    except Exception:
                                        pass
                            else:
                                # Discard the VAD output, keep pre-VAD file in place.
                                try:
                                    _vad_out.unlink()
                                except Exception:
                                    pass
                            _mem_trim()  # v701x — return freed pages to OS between clips
                        except Exception as _vad_err:
                            print(
                                f"[VideoProcessor/v691d] per-clip VAD failed for "
                                f"clip {info.get('clip_index', slot_zero)}: "
                                f"{_vad_err} — keeping un-VAD-trimmed clip",
                                flush=True,
                            )

                    # v701y — dispose the shared Whisper model after the
                    # per-clip loop completes. malloc_trim returns the
                    # ~50MB resident + buffers back to the OS in one go.
                    if _shared_whisper is not None:
                        try:
                            del _shared_whisper
                        except Exception:
                            pass
                        _shared_whisper = None
                        _mem_trim()
                        print(
                            "[VideoProcessor/v701y] shared Whisper-tiny disposed; "
                            "memory returned to OS",
                            flush=True,
                        )
        else:
            # Clips are pre-trimmed - just use them directly (FAST PATH)
            print(f"[VideoProcessor] Using pre-trimmed clips (fast concat)")
            files_to_concat = [Path(info["path"]) for info in clip_info]
        
        # Concatenate
        if progress_callback:
            progress_callback("Finalizing video...")

        if remove_silence:
            concat_output = temp_path / "concatenated.mp4"
        else:
            concat_output = output_path

        # v692b — ffprobe each input to concat. Localizes whether a
        # 233s final output came from the trim stage (input files are
        # huge) or the concat stage (re-encode stretches them).
        try:
            _pre_concat_total = 0.0
            for _slot, _f in enumerate(files_to_concat):
                _d = None
                if _f and Path(_f).exists():
                    try:
                        _d = get_duration(ffprobe_json(_f))
                    except Exception as _ee:
                        _d = f"ERR:{_ee}"
                print(
                    f"[VideoProcessor/v692b] pre-concat slot={_slot} "
                    f"file={_f} dur={_d}",
                    flush=True,
                )
                if isinstance(_d, (int, float)):
                    _pre_concat_total += float(_d)
            print(
                f"[VideoProcessor/v692b] pre-concat sum_durations="
                f"{_pre_concat_total:.3f}s across {len(files_to_concat)} files",
                flush=True,
            )
        except Exception as _e:
            print(f"[VideoProcessor/v692b] diag failed (non-fatal): {_e}", flush=True)

        if transition and transition != "none":
            concat_videos_with_transitions(
                files_to_concat, concat_output,
                transition=transition,
                transition_duration=transition_duration,
                has_audio=True,
            )
        else:
            concat_videos(files_to_concat, concat_output)

        # v692b — ffprobe AFTER concat to confirm whether re-encode
        # itself stretched duration. If pre-sum ≈ 32 but post = 233,
        # bug is in concat_videos. If pre-sum ≈ 233, bug is upstream.
        try:
            _post_d = get_duration(ffprobe_json(concat_output))
            print(
                f"[VideoProcessor/v692b] post-concat duration={_post_d:.3f}s",
                flush=True,
            )
        except Exception as _e:
            print(f"[VideoProcessor/v692b] post-concat probe failed: {_e}", flush=True)

        # v701zd — record per-clip post-VAD durations for downstream broll.
        # The broll pipeline uses these to compute each clip's exact master
        # timeline position without re-transcribing the assembled concat
        # with Whisper. files_to_concat[i] is the i-th clip's trimmed file
        # in speaker_clip_info order; ffprobe gives its post-trim duration.
        # Sum-as-you-go gives cumulative start/end per clip in master time.
        try:
            _per_clip_durs = []
            for _slot, _f in enumerate(files_to_concat):
                _d = 0.0
                if _f and Path(_f).exists():
                    try:
                        _d = float(get_duration(ffprobe_json(_f)))
                    except Exception:
                        _d = 0.0
                _per_clip_durs.append(_d)
                _info = clip_info[_slot] if _slot < len(clip_info) else {}
                print(
                    f"[VideoProcessor/v701zd] post-vad clip {_slot} "
                    f"(clip_db_id={_info.get('_clip_db_id')} "
                    f"role={_info.get('clip_role')} "
                    f"scene_index={_info.get('scene_index')}): {_d:.3f}s",
                    flush=True,
                )
            stats["per_clip_post_vad_durations"] = _per_clip_durs
            stats["per_clip_post_vad_clip_db_ids"] = [
                (clip_info[_slot].get("_clip_db_id") if _slot < len(clip_info) else None)
                for _slot in range(len(files_to_concat))
            ]
            stats["per_clip_post_vad_paired_clip_ids"] = [
                (clip_info[_slot].get("paired_clip_id") if _slot < len(clip_info) else None)
                for _slot in range(len(files_to_concat))
            ]
        except Exception as _e:
            print(f"[VideoProcessor/v701zd] per-clip duration capture failed: {_e}", flush=True)
        
        # Step 3: Apply VAD (if enabled)
        # v668 — note: when has_timeline_clips=True, remove_silence was
        # forced False above so this branch is skipped. Stats reporting:
        if has_timeline_clips:
            stats["vad_applied"] = False
            stats["vad_skipped_reason"] = "timeline_clips_present"
        if remove_silence:
            # v701w — SKIP the final-pass apply_vad over the assembled
            # concat when the per-clip Whisper-VAD post-loop already
            # trimmed each clip individually. Final pass would load
            # Whisper-small a SECOND time and run it over the full ~50s
            # concat → Render OOM-kills at the 2GB ceiling (verified
            # 2026-05-11 with the donut-glaze v698A dual-output export).
            # Per-clip pass + concat is already "spoken-words-only" per
            # clip; no benefit to re-trimming the concatenation.
            if _per_clip_whisper_ran:
                print(
                    "[VideoProcessor/v701w] skipping final-pass VAD — per-clip "
                    "Whisper-VAD already trimmed each clip; promoting concat → "
                    "output_path directly (saves Whisper-small reload, ~500MB).",
                    flush=True,
                )
                import shutil as _sh
                if str(concat_output) != str(output_path):
                    _sh.move(str(concat_output), str(output_path))
                info = ffprobe_json(output_path)
                stats["final_duration"] = get_duration(info)
                stats["vad_applied"] = True
                stats["vad_pass"] = "per_clip_only_v708"

                # === v709 — Per-clip word audit aggregation ===
                # Replaces v708 Layer 4 post-concat Whisper-small re-transcribe.
                # Per-clip Whisper-tiny pass already produced matched/missing/
                # trust per clip in `_v709_audit`. Here we aggregate. No
                # second Whisper load → ~500MB RAM saved + ~15s wall time
                # saved per export. Hyphen-aware tokenizer (`blood-flow` →
                # `{blood, flow}`) kills the long-standing `bloodflow`
                # false-positive that v708 produced.
                try:
                    if progress_callback:
                        progress_callback("v709 word-completeness aggregation...")
                    per_clip_audit = []
                    # v709.2 — separate "real misses" from "Whisper-broke" missing.
                    # Clips with failsafe="HARD" (Whisper-tiny couldn't ASR the
                    # clip at all, trust<0.50) report a missing list that is
                    # PURE ASR FAILURE NOISE — those words ARE in the audio but
                    # Whisper just couldn't transcribe them. Treating them as
                    # "missing from final mp4" is what made v708 noisy. v709.2
                    # excludes HARD-failsafe clips from the union_missing set
                    # AND from audit_ok determination. SOFT-failsafe clips
                    # (trust≥0.50 but some script words missing) keep their
                    # missing list — those are higher-confidence partial
                    # detection failures that an operator might want to know
                    # about, but they DON'T flip audit_ok by themselves either
                    # (operator's ear is the final arbiter; aggregator just
                    # reports signal). Only clips with NO failsafe AND missing
                    # words can flip audit_ok to False.
                    union_missing_strict = set()      # only from no-failsafe clips
                    union_missing_advisory = set()    # from SOFT-failsafe clips
                    sum_script = 0
                    sum_heard = 0
                    hard_failsafe_clips = 0
                    soft_failsafe_clips = 0
                    audit_ok = True
                    for _ci in clip_info:
                        _cm = (_ci.get("cut_mode") or "").lower()
                        _st = (_ci.get("scene_type") or "").lower()
                        if _cm == "timeline" or _st == "text_card":
                            continue
                        _au = _ci.get("_v709_audit")
                        if not _au or not _au.get("script_provided"):
                            continue
                        entry = {
                            "clip_db_id": _ci.get("_clip_db_id"),
                            "scene_index": _ci.get("scene_index"),
                            "role": _ci.get("clip_role"),
                            "trust": float(_au.get("trust", 0.0) or 0.0),
                            "matched": int(_au.get("matched", 0) or 0),
                            "script_words": int(_au.get("script_words", 0) or 0),
                            "heard_words": int(_au.get("heard_words", 0) or 0),
                            "missing": list(_au.get("missing") or []),
                            "failsafe": _au.get("failsafe"),
                            "error": _au.get("error"),  # v709 — None on success, str on Whisper failure
                        }
                        per_clip_audit.append(entry)
                        sum_script += entry["script_words"]
                        sum_heard += entry["heard_words"]
                        _fs = entry["failsafe"]
                        if _fs == "HARD":
                            hard_failsafe_clips += 1
                            # Skip union update — Whisper couldn't ASR clip;
                            # missing list is meaningless. Per-clip detail
                            # still surfaces in v708_per_clip_audit for
                            # operator inspection.
                        elif _fs == "SOFT":
                            soft_failsafe_clips += 1
                            union_missing_advisory.update(entry["missing"])
                            # SOFT failsafe doesn't flip audit_ok either —
                            # trust≥0.50 means Whisper found most words; a
                            # few rare-vocab misses are likely ASR limits.
                        else:
                            # No failsafe → high-confidence signal.
                            # Any missing word here is the ONLY trustworthy
                            # "TTS may have dropped a word" indicator.
                            if entry["missing"]:
                                union_missing_strict.update(entry["missing"])
                                audit_ok = False
                            # Per-clip trust below floor without failsafe is
                            # very rare (failsafe fires at the trust floor),
                            # but defend against it.
                            if entry["trust"] < V709_PER_CLIP_TRUST_FLOOR:
                                audit_ok = False

                    if per_clip_audit:
                        # v709.2 — audit_ok now reflects only HIGH-CONFIDENCE
                        # missing-word signal. HARD/SOFT failsafe clips do NOT
                        # flip audit_ok by themselves; operator inspects
                        # per-clip detail. Backward-compat: v708_audit_missing_words
                        # exposes strict union (operator's actionable list);
                        # advisory list lives on the per-clip entries.
                        stats["v708_audit_ok"] = bool(audit_ok)
                        stats["v708_audit_missing_words"] = sorted(union_missing_strict)
                        stats["v708_audit_missing_words_advisory"] = sorted(union_missing_advisory)
                        stats["v708_audit_model"] = "tiny-per-clip-v709.2"
                        stats["v708_audit_script_words"] = sum_script
                        stats["v708_audit_heard_words"] = sum_heard
                        stats["v708_audit_error"] = None
                        stats["v708_per_clip_audit"] = per_clip_audit
                        # v773 — log summary works for both legacy + aligned audit shapes
                        for _i, _a in enumerate(per_clip_audit or []):
                            if "backend" in _a:
                                # New shape (v773 forced-alignment)
                                print(
                                    f"[ExportAudit/v773] clip={_i} "
                                    f"backend={_a.get('backend','?')} "
                                    f"script={_a.get('script_words',0)}w "
                                    f"aligned={_a.get('aligned_words',0)}w "
                                    f"low_conf={_a.get('low_confidence_words',[])} "
                                    f"trim_ratio={_a.get('trim_ratio',1.0):.2f}",
                                    flush=True,
                                )
                            else:
                                # Legacy V708 shape
                                print(
                                    f"[ExportAudit/v708] clip={_i} "
                                    f"trust={_a.get('trust',0):.2f} "
                                    f"matched={_a.get('matched',0)} "
                                    f"missing={_a.get('missing',[])}",
                                    flush=True,
                                )
                        print(
                            f"[v709-AUDIT] schema=v709.2 per-clip aggregate: "
                            f"clips={len(per_clip_audit)} "
                            f"script={sum_script} heard={sum_heard} "
                            f"strict_missing={len(union_missing_strict)} "
                            f"advisory_missing={len(union_missing_advisory)} "
                            f"hard_failsafe={hard_failsafe_clips} "
                            f"soft_failsafe={soft_failsafe_clips} "
                            f"ok={stats['v708_audit_ok']}",
                            flush=True,
                        )
                        if union_missing_strict:
                            print(
                                f"[v709-AUDIT] STRICT missing (no-failsafe clips, "
                                f"actionable): {sorted(union_missing_strict)}",
                                flush=True,
                            )
                        if union_missing_advisory:
                            print(
                                f"[v709-AUDIT] ADVISORY missing (SOFT-failsafe "
                                f"clips, likely ASR limits): "
                                f"{sorted(union_missing_advisory)}",
                                flush=True,
                            )
                        if hard_failsafe_clips:
                            _hard_clips = [
                                f"clip{e['scene_index']}(trust={e['trust']:.2f})"
                                for e in per_clip_audit if e["failsafe"] == "HARD"
                            ]
                            print(
                                f"[v709-AUDIT] HARD-failsafe clips (Whisper "
                                f"couldn't ASR; audio likely fine): "
                                f"{_hard_clips}",
                                flush=True,
                            )
                    else:
                        stats["v708_audit_ok"] = True
                        stats["v708_audit_missing_words"] = []
                        stats["v708_audit_missing_words_advisory"] = []
                        stats["v708_audit_model"] = "tiny-per-clip-v709.2"
                        stats["v708_audit_script_words"] = 0
                        stats["v708_audit_heard_words"] = 0
                        stats["v708_audit_error"] = "no dialogue clips to audit"
                        stats["v708_per_clip_audit"] = []
                        print(
                            f"[v709-AUDIT] schema=v709.2 no dialogue clips to aggregate",
                            flush=True,
                        )
                except Exception as _audit_err:
                    stats["v708_audit_ok"] = False
                    stats["v708_audit_missing_words"] = []
                    stats["v708_audit_missing_words_advisory"] = []
                    stats["v708_audit_model"] = "tiny-per-clip-v709.2"
                    stats["v708_audit_script_words"] = 0
                    stats["v708_audit_heard_words"] = 0
                    stats["v708_audit_error"] = f"v709 aggregation exception: {_audit_err}"
                    stats["v708_per_clip_audit"] = []
                    print(
                        f"[v709-AUDIT] non-fatal aggregation exception: {_audit_err}",
                        flush=True,
                    )

                # Skip the rest of the final-pass block
                if progress_callback:
                    progress_callback("Export complete!")
                return stats

            if not check_vad_available():
                raise RuntimeError(
                    "VAD requires torch and numpy. "
                    "Install with: pip install torch numpy"
                )

            # Compute per-clip time boundaries in the concatenated video
            clip_boundaries = None
            if dialogue_texts:
                clip_boundaries = []
                cursor = 0.0
                for f in files_to_concat:
                    try:
                        f_info = ffprobe_json(f)
                        dur = get_duration(f_info)
                    except Exception:
                        dur = 7.7  # fallback estimate
                    clip_boundaries.append((cursor, cursor + dur))
                    cursor += dur
                print(f"[VAD] Clip boundaries: {[(f'{s:.1f}',f'{e:.1f}') for s,e in clip_boundaries]}", flush=True)
            
            if progress_callback:
                progress_callback("Applying Voice Activity Detection...")
            
            vad_stats = apply_vad(
                concat_output,
                output_path,
                threshold=vad_threshold,
                min_gap_duration=silence_trigger,
                silence_keep_duration=silence_keep,
                silence_mode=silence_mode,
                progress_callback=progress_callback,
                dialogue_texts=dialogue_texts,
                language=language,
                clip_boundaries=clip_boundaries,
                cut_prefix_audio=cut_prefix_audio,  # v542
                prefix_word=prefix_word,  # v542
            )
            stats.update(vad_stats)
        else:
            # Get duration of final video
            info = ffprobe_json(concat_output)
            stats["final_duration"] = get_duration(info)
    
    if progress_callback:
        progress_callback("Export complete!")
    
    return stats