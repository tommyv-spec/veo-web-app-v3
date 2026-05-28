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
