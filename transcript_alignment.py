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
    raise NotImplementedError


def release_audit_asr() -> None:
    """Free distil-large-v3 RSS after post-export audit. Aligner stays resident."""
    raise NotImplementedError


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


def detect_speech_segments_aligned(
    audio_path: Path,
    script_text: str,
    language: str = "English",
    padding: float = 0.15,
) -> tuple[list[tuple[float, float]], dict]:
    """Returns (segments_to_keep, audit_dict). See spec section 4.2 audit_dict shape."""
    raise NotImplementedError


def transcribe_for_audit(
    audio_path: Path,
    language: str = "English",
) -> list[tuple[str, float, float]]:
    """One-shot post-concat audit transcription using distil-large-v3 int8."""
    raise NotImplementedError
