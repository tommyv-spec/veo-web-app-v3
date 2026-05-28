"""Forced alignment of known script text to audio.

v773 — replaces detect_speech_segments_whisper + _match_whisper_to_dialogue +
_align_dp + _locate_script_span + _prune_span_boundaries + V708 retry chain +
V731 SOFT/HARD failsafe + v709 audit sink.

Public surface:
    warmup() -> None
    release_audit_asr() -> None
    align_script_to_audio(audio_path, script_text, language) -> AlignmentResult
    detect_speech_segments_aligned(audio_path, script_text, language, padding) ->
        tuple[list[tuple[float, float]], dict]
    transcribe_for_audit(audio_path, language) -> list[tuple[str, float, float]]
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


# === Configuration knobs (module constants — no env required for defaults) ===
# Aligner: torchaudio.pipelines.MMS_FA (MMS-300M, pure-torch, no C extension)
AUDIT_MODEL_ID = "distil-large-v3"
HEAD_PAD = 0.15      # seconds kept before first aligned word
TAIL_PAD = 0.20      # seconds kept after last aligned word
GAP_TRIGGER = 0.60   # inter-word gap above this is a trim candidate
GAP_KEEP = 0.20      # seconds preserved per side at each trim
CONF_FLAG = 0.30     # words with conf below this flagged in audit


# === Public types ===
@dataclass
class AlignedWord:
    text: str
    start: float
    end: float
    confidence: float


@dataclass
class AlignmentResult:
    words: list[AlignedWord] = field(default_factory=list)
    audio_duration: float = 0.0
    backend: str = ""
    fallback_reason: Optional[str] = None


# === Lazy singletons (loaded on first use; warmup() pre-loads all) ===
_ALIGNER = None
_VAD = None
_AUDIT_ASR = None


def warmup() -> None:
    """Pre-load aligner + VAD + audit ASR. Call once at worker boot."""
    _ensure_aligner()
    _ensure_vad()
    _ensure_audit_asr()


# Whisper language code map (ISO 639-1)
_LANG_ISO_WHISPER = {
    "english": "en", "spanish": "es", "french": "fr", "german": "de",
    "italian": "it", "portuguese": "pt", "dutch": "nl", "russian": "ru",
    "chinese": "zh", "japanese": "ja", "korean": "ko", "arabic": "ar",
    "hindi": "hi", "turkish": "tr", "polish": "pl", "swedish": "sv",
    "norwegian": "no", "danish": "da", "finnish": "fi", "greek": "el",
    "hebrew": "he", "thai": "th", "vietnamese": "vi", "indonesian": "id",
    "malay": "ms", "tagalog": "tl", "swahili": "sw", "czech": "cs",
    "romanian": "ro", "hungarian": "hu", "ukrainian": "uk",
}


def _ensure_audit_asr():
    """Lazy-load faster-whisper distil-large-v3 int8 into module-level singleton."""
    global _AUDIT_ASR
    if _AUDIT_ASR is None:
        from faster_whisper import WhisperModel
        _AUDIT_ASR = WhisperModel(AUDIT_MODEL_ID, device="cpu", compute_type="int8")
    return _AUDIT_ASR


def release_audit_asr() -> None:
    """Free distil-large-v3 RSS after post-export audit. Aligner stays resident."""
    global _AUDIT_ASR
    if _AUDIT_ASR is not None:
        try:
            del _AUDIT_ASR
        except Exception:
            pass
        _AUDIT_ASR = None
        try:
            import gc
            gc.collect()
        except Exception:
            pass
        # On Linux, nudge glibc to return freed pages to OS.
        # No-op on Windows/macOS (libc.so.6 not present).
        try:
            import ctypes
            ctypes.CDLL("libc.so.6").malloc_trim(0)
        except Exception:
            pass


# === Aligner helpers ===

def _ensure_aligner():
    """Lazy-load MMS_FA bundle into module-level singleton.

    Probe result (torchaudio 2.7.1, verified from source):
      tokenizer(words_list) -> List[List[int]]  (char-level token ids per word)
      aligner(emission[0], targets) -> List[List[TokenSpan]]
        one List[TokenSpan] per word; TokenSpan has .start, .end (frame ints), .score (prob)
      Valid vocab: lowercase a-z + apostrophe only; '-' and '*' are blank/star tokens.
      F.merge_tokens exists in 2.7.1.
    """
    global _ALIGNER
    if _ALIGNER is None:
        import torch
        from torchaudio.pipelines import MMS_FA
        bundle = MMS_FA
        model = bundle.get_model(with_star=False).to("cpu")
        model.eval()
        _ALIGNER = {
            "bundle": bundle,
            "model": model,
            "tokenizer": bundle.get_tokenizer(),
            "aligner": bundle.get_aligner(),
            "sample_rate": bundle.sample_rate,
        }
    return _ALIGNER


def _load_audio_mono16k(audio_path: Path):
    """Load audio as mono float32 at 16 kHz. Returns (waveform, sr) — shape (1, T)."""
    import torchaudio
    wav, sr = torchaudio.load(str(audio_path))
    if wav.shape[0] > 1:
        wav = wav.mean(dim=0, keepdim=True)
    if sr != 16000:
        wav = torchaudio.functional.resample(wav, sr, 16000)
        sr = 16000
    return wav, sr


def _normalize_script_for_mms(script_text: str) -> list[str]:
    """Lowercase, strip non-vocab chars, split on whitespace.

    MMS_FA vocab: [a-z'] only. Hyphens/dashes split words; all other punctuation stripped.
    Each returned token must contain only [a-z'] chars — otherwise tokenizer raises KeyError.
    """
    import re
    # replace em/en/hyphen with space (treat as word boundary)
    cleaned = script_text.lower()
    cleaned = cleaned.replace("—", " ").replace("–", " ").replace("-", " ")
    # strip everything except lowercase letters, apostrophe, and whitespace
    cleaned = re.sub(r"[^a-z'\s]", "", cleaned)
    return [w for w in cleaned.split() if w]


def align_script_to_audio(
    audio_path: Path,
    script_text: str,
    language: str = "English",
) -> AlignmentResult:
    """Forced alignment: KNOWN script + audio -> per-script-word timestamps.

    Uses torchaudio.pipelines.MMS_FA (MMS-300M CTC model).
    Returns one AlignedWord per normalized script word with start/end in seconds
    and confidence in [0, 1].

    Does NOT catch exceptions internally — callers wire fallback at higher level.
    """
    import torch

    if not script_text.strip():
        return AlignmentResult(words=[], audio_duration=0.0, backend="empty-script")

    a = _ensure_aligner()
    waveform, sr = _load_audio_mono16k(audio_path)
    duration = float(waveform.shape[-1]) / sr

    words_list = _normalize_script_for_mms(script_text)
    if not words_list:
        return AlignmentResult(words=[], audio_duration=duration, backend="empty-script")

    with torch.inference_mode():
        emission, _ = a["model"](waveform)
    # emission shape: (1, T_frames, vocab) — squeeze batch dim for aligner
    emission_2d = emission[0]  # (T_frames, vocab)

    targets = a["tokenizer"](words_list)
    # targets: List[List[int]] — char token ids per word

    # aligner returns List[List[TokenSpan]], one list per word.
    # TokenSpan: .start (int frame), .end (int frame), .score (float, probability after exp())
    word_spans = a["aligner"](emission_2d, targets)

    # ratio: audio samples per emission frame.
    # emission shape is (1, T_frames, vocab); shape[1] is the time-dim, not batch.
    ratio = waveform.shape[-1] / emission.shape[1]

    aligned: list[AlignedWord] = []
    for w_text, spans in zip(words_list, word_spans):
        if not spans:
            aligned.append(AlignedWord(text=w_text, start=0.0, end=0.0, confidence=0.0))
            continue
        start_frame = spans[0].start
        end_frame = spans[-1].end
        # average per-token score (already in probability space)
        score = float(sum(s.score for s in spans) / len(spans))
        aligned.append(AlignedWord(
            text=w_text,
            start=float(start_frame * ratio / sr),
            end=float(end_frame * ratio / sr),
            confidence=score,
        ))

    return AlignmentResult(
        words=aligned,
        audio_duration=duration,
        backend="mms-fa",
        fallback_reason=None,
    )


def _audio_duration_seconds(audio_path: Path) -> float:
    import subprocess
    import json as _json
    res = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "json", str(audio_path)],
        capture_output=True, text=True, check=False,
    )
    if res.returncode != 0:
        return 0.0
    try:
        return float(_json.loads(res.stdout)["format"]["duration"])
    except Exception:
        return 0.0


def _build_audit(
    script_text: str,
    result: AlignmentResult,
    segments: list[tuple[float, float]],
) -> dict:
    audio_dur = result.audio_duration or 0.0
    speech_dur = sum(e - s for s, e in segments)
    low_conf = [w.text for w in result.words if w.confidence < CONF_FLAG]
    return {
        "script_provided": bool(script_text.strip()),
        "backend": result.backend,
        "script_words": len(script_text.split()) if script_text.strip() else 0,
        "aligned_words": len(result.words),
        "low_confidence_words": low_conf,
        "audio_duration": audio_dur,
        "speech_duration": speech_dur,
        "trim_ratio": (speech_dur / audio_dur) if audio_dur > 0 else 1.0,
        "fallback_reason": result.fallback_reason,
    }


def _ensure_vad():
    """Lazy-load silero-VAD v5.1.2 ONNX model into module-level singleton.

    silero_vad v5.1.2 exports:
      load_silero_vad(onnx=False) -> model
      get_speech_timestamps(audio_1d, model, sampling_rate, min_silence_duration_ms,
                            speech_pad_ms, ...) -> list[dict{start, end}]
    Timestamps returned in samples (not seconds); divide by sampling_rate for seconds.
    onnx=True selects ONNX runtime backend (faster inference, no GPU required).
    """
    global _VAD
    if _VAD is None:
        from silero_vad import load_silero_vad, get_speech_timestamps
        _VAD = {
            "model": load_silero_vad(onnx=True),
            "get_speech_timestamps": get_speech_timestamps,
        }
    return _VAD


def _silero_fallback(audio_path: Path, reason: str):
    """Coarse VAD-only segmentation when forced alignment unavailable.

    Returns ([(start_s, end_s), ...], AlignmentResult(backend='silero-fallback')).
    Timestamps from silero are in samples at 16 kHz; divided by 16000 for seconds.
    """
    duration = _audio_duration_seconds(audio_path)
    try:
        vad = _ensure_vad()
        waveform, sr = _load_audio_mono16k(audio_path)
        # silero expects 1-D float tensor at 16 kHz
        audio_1d = waveform.squeeze(0)
        ts = vad["get_speech_timestamps"](
            audio_1d,
            vad["model"],
            sampling_rate=16000,
            min_silence_duration_ms=int(GAP_TRIGGER * 1000),
            speech_pad_ms=int(GAP_KEEP * 1000),
        )
        if not ts:
            segments = [(0.0, duration)]
        else:
            segments = [
                (max(0.0, t["start"] / 16000), min(duration, t["end"] / 16000))
                for t in ts
            ]
    except Exception as e:
        segments = [(0.0, duration)]
        reason = f"{reason}; silero also failed: {e}"

    return segments, AlignmentResult(
        words=[],
        audio_duration=duration,
        backend="silero-fallback",
        fallback_reason=reason,
    )


def detect_speech_segments_aligned(
    audio_path: Path,
    script_text: str,
    language: str = "English",
    padding: float = 0.15,
) -> tuple[list[tuple[float, float]], dict]:
    """Returns (segments_to_keep, audit_dict). See spec section 4.2 audit_dict shape."""
    audio_path = Path(audio_path)

    if not script_text.strip():
        duration = _audio_duration_seconds(audio_path)
        empty_result = AlignmentResult(
            words=[], audio_duration=duration, backend="empty-script",
        )
        return [(0.0, duration)], _build_audit("", empty_result, [(0.0, duration)])

    try:
        result = align_script_to_audio(audio_path, script_text, language)
    except Exception as e:
        segments, fallback_result = _silero_fallback(audio_path, str(e))
        return segments, _build_audit(script_text, fallback_result, segments)

    if not result.words:
        # Aligner returned nothing (rare edge) — keep full audio
        full = [(0.0, result.audio_duration)]
        return full, _build_audit(script_text, result, full)

    # Step A: head/tail clamp around aligned span
    first = result.words[0]
    last = result.words[-1]
    keep_start = max(0.0, first.start - HEAD_PAD)
    keep_end = min(result.audio_duration, last.end + TAIL_PAD)

    # Step B: inter-word gap trim — drop gaps longer than GAP_TRIGGER,
    # keeping GAP_KEEP padding on each side.
    segments: list[tuple[float, float]] = []
    cur_start = keep_start
    for i in range(1, len(result.words)):
        gap = result.words[i].start - result.words[i - 1].end
        if gap > GAP_TRIGGER:
            seg_end = min(keep_end, result.words[i - 1].end + GAP_KEEP)
            if seg_end > cur_start:
                segments.append((cur_start, seg_end))
            cur_start = max(keep_start, result.words[i].start - GAP_KEEP)
    if keep_end > cur_start:
        segments.append((cur_start, keep_end))

    audit = _build_audit(script_text, result, segments)
    return segments, audit


def transcribe_for_audit(
    audio_path: Path,
    language: str = "English",
) -> list[tuple[str, float, float]]:
    """One-shot post-concat audit transcription using distil-large-v3 int8.

    Returns list of (word, start_sec, end_sec) tuples.
    Fallback: if faster-whisper returns segments without word-level timestamps
    (seg.words is None), each segment text is split on whitespace and the
    segment start/end timestamps are shared across its words.
    """
    model = _ensure_audit_asr()
    iso = _LANG_ISO_WHISPER.get(language.lower(), None)
    segs, _info = model.transcribe(
        str(audio_path),
        language=iso,
        word_timestamps=True,
        beam_size=1,
        vad_filter=True,
        condition_on_previous_text=False,
    )
    out: list[tuple[str, float, float]] = []
    for seg in segs:
        if seg.words:
            for w in seg.words:
                token = w.word.strip()
                if token:
                    out.append((token, float(w.start), float(w.end)))
        else:
            # Fallback: no per-word timestamps — split segment text, share timing.
            tokens = [t for t in seg.text.strip().split() if t]
            for token in tokens:
                out.append((token, float(seg.start), float(seg.end)))
    return out
