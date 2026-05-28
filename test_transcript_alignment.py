"""Unit tests for transcript_alignment module — replaces V708/V731 heuristics."""
import json
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "tests" / "fixtures"


def test_module_public_api_exists():
    """Module exposes the 3 documented public functions + types."""
    import transcript_alignment as ta

    assert hasattr(ta, "align_script_to_audio")
    assert hasattr(ta, "detect_speech_segments_aligned")
    assert hasattr(ta, "transcribe_for_audit")
    assert hasattr(ta, "AlignedWord")
    assert hasattr(ta, "AlignmentResult")
    assert hasattr(ta, "warmup")
    assert hasattr(ta, "release_audit_asr")


def test_align_clean_returns_all_script_words():
    """Clean TTS fixture: aligner returns one entry per script word, monotonic, high conf."""
    import transcript_alignment as ta

    cfg = json.loads((FIXTURES / "align_clean.json").read_text())
    result = ta.align_script_to_audio(
        audio_path=FIXTURES / "align_clean.wav",
        script_text=cfg["script"],
        language="English",
    )

    script_words = cfg["script"].split()
    assert len(result.words) == len(script_words), (
        f"expected {len(script_words)} aligned words, got {len(result.words)}"
    )
    # Word texts match (case-insensitive, lowercase)
    for i, sw in enumerate(script_words):
        assert result.words[i].text.lower() == sw.lower()
    # Monotonic non-decreasing timestamps
    for i in range(1, len(result.words)):
        assert result.words[i].start >= result.words[i - 1].start
    # High confidence on clean audio
    avg_conf = sum(w.confidence for w in result.words) / len(result.words)
    assert avg_conf > 0.5, f"avg confidence {avg_conf} too low for clean audio"
    assert result.backend == "mms-fa"
    assert result.fallback_reason is None


def test_align_dropped_word_flagged_low_conf():
    """Word dropped from TTS should align with conf < CONF_FLAG."""
    import transcript_alignment as ta
    cfg = json.loads((FIXTURES / "align_dropped.json").read_text())
    result = ta.align_script_to_audio(
        audio_path=FIXTURES / "align_dropped.wav",
        script_text=cfg["script"],
        language="English",
    )
    script_words = cfg["script"].split()
    assert len(result.words) == len(script_words)
    dropped_idx = cfg["drop_word_idx"]
    dropped_conf = result.words[dropped_idx].confidence
    other_confs = [w.confidence for i, w in enumerate(result.words) if i != dropped_idx]
    avg_other = sum(other_confs) / len(other_confs) if other_confs else 1.0
    # Dropped word must either be below CONF_FLAG or be the lowest-confidence word
    # and meaningfully below the average of the rest.
    is_low_absolute = dropped_conf < ta.CONF_FLAG
    is_low_relative = dropped_conf < 0.5 * avg_other
    min_conf_idx = min(range(len(result.words)), key=lambda i: result.words[i].confidence)
    assert (is_low_absolute or is_low_relative) and min_conf_idx == dropped_idx, (
        f"dropped word '{script_words[dropped_idx]}' conf={dropped_conf:.3f} "
        f"not flagged: CONF_FLAG={ta.CONF_FLAG}, avg_other={avg_other:.3f}, "
        f"min_conf_idx={min_conf_idx} (expected {dropped_idx})"
    )


def test_align_rare_vocab_chemistry_terms():
    """Rare vocab (laureth, phenylethyl) still gets aligned with monotonic timestamps."""
    import transcript_alignment as ta
    cfg = json.loads((FIXTURES / "align_rare.json").read_text())
    result = ta.align_script_to_audio(
        audio_path=FIXTURES / "align_rare.wav",
        script_text=cfg["script"],
        language="English",
    )
    script_words = cfg["script"].split()
    assert len(result.words) == len(script_words)
    for i in range(1, len(result.words)):
        assert result.words[i].start >= result.words[i - 1].start


def test_filler_excluded_from_segments():
    """TTS with inserted 'uhh' filler: returned segments skip the filler gap."""
    import transcript_alignment as ta

    cfg = json.loads((FIXTURES / "align_filler.json").read_text())
    segments, audit = ta.detect_speech_segments_aligned(
        audio_path=FIXTURES / "align_filler.wav",
        script_text=cfg["script"],
        language="English",
        padding=0.15,
    )
    assert audit["backend"] == "mms-fa"
    assert audit["script_provided"] is True
    assert audit["script_words"] == len(cfg["script"].split())
    assert audit["aligned_words"] == audit["script_words"]
    # speech_duration should be less than full audio (filler trimmed)
    assert audit["speech_duration"] < audit["audio_duration"]
    # trim_ratio = speech / audio, must be in plausible window
    assert 0.3 < audit["trim_ratio"] < 0.99
    assert audit["fallback_reason"] is None


def test_segments_monotonic_and_in_bounds():
    """Returned segments are monotonic, non-overlapping, within audio bounds."""
    import transcript_alignment as ta

    cfg = json.loads((FIXTURES / "align_clean.json").read_text())
    segments, audit = ta.detect_speech_segments_aligned(
        audio_path=FIXTURES / "align_clean.wav",
        script_text=cfg["script"],
        language="English",
        padding=0.15,
    )
    assert len(segments) >= 1
    prev_end = 0.0
    for s, e in segments:
        assert 0.0 <= s < e
        assert s >= prev_end
        assert e <= audit["audio_duration"] + 0.01
        prev_end = e


def test_empty_script_returns_full_audio():
    """No script (text_card path) returns single full-audio segment with empty backend."""
    import transcript_alignment as ta

    segments, audit = ta.detect_speech_segments_aligned(
        audio_path=FIXTURES / "align_clean.wav",
        script_text="",
        language="English",
    )
    assert len(segments) == 1
    assert segments[0][0] == 0.0
    assert audit["script_provided"] is False
    assert audit["aligned_words"] == 0


def test_transcribe_for_audit_returns_words():
    """Audit ASR returns list of (word, start, end) tuples for clean fixture."""
    import transcript_alignment as ta

    words = ta.transcribe_for_audit(FIXTURES / "align_clean.wav", language="English")
    assert isinstance(words, list)
    assert len(words) >= 5  # script has 10 words; allow some compression
    for w, s, e in words:
        assert isinstance(w, str) and w
        assert 0.0 <= s <= e


def test_release_audit_asr_frees_singleton():
    """release_audit_asr clears the cached audit model."""
    import transcript_alignment as ta

    _ = ta.transcribe_for_audit(FIXTURES / "align_clean.wav", language="English")
    assert ta._AUDIT_ASR is not None
    ta.release_audit_asr()
    assert ta._AUDIT_ASR is None


def test_silero_fallback_when_aligner_raises(monkeypatch):
    """If aligner raises OOM, silero-VAD path returns coarse segments + audit flag."""
    import transcript_alignment as ta

    # Force the default-MMS-FA path so the failure mode under test is the
    # aligner raising, not ALIGN_MODE=silero short-circuiting.
    monkeypatch.delenv("ALIGN_MODE", raising=False)

    def boom(*args, **kwargs):
        raise RuntimeError("simulated aligner OOM")

    monkeypatch.setattr(ta, "align_script_to_audio", boom)

    segments, audit = ta.detect_speech_segments_aligned(
        audio_path=FIXTURES / "align_clean.wav",
        script_text="placeholder script text",
        language="English",
    )
    assert audit["backend"] == "silero-fallback"
    assert audit["fallback_reason"] is not None
    assert "OOM" in audit["fallback_reason"]
    assert len(segments) >= 1
    # silero must produce SOME speech; fixture is non-silent
    speech_dur = sum(e - s for s, e in segments)
    assert speech_dur > 0.5
    # silero-VAD singleton must be loaded by the fallback path (not the stub)
    assert ta._VAD is not None, "silero-VAD singleton must be loaded by the fallback path"


def test_align_mode_silero_skips_mms_fa(monkeypatch):
    """ALIGN_MODE=silero env routes to silero-VAD without touching MMS_FA at all."""
    import transcript_alignment as ta

    monkeypatch.setenv("ALIGN_MODE", "silero")

    # If MMS_FA were touched, this would raise — proves the path was skipped.
    def must_not_be_called(*args, **kwargs):
        raise AssertionError("MMS_FA path called despite ALIGN_MODE=silero")

    monkeypatch.setattr(ta, "align_script_to_audio", must_not_be_called)

    segments, audit = ta.detect_speech_segments_aligned(
        audio_path=FIXTURES / "align_clean.wav",
        script_text="any script",
        language="English",
    )
    assert audit["backend"] == "silero-fallback"
    assert "operator-forced" in audit["fallback_reason"]
    assert len(segments) >= 1
    speech_dur = sum(e - s for s, e in segments)
    assert speech_dur > 0.5
