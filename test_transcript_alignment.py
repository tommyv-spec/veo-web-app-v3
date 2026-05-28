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
