"""v948 — the post-concat silence-hole sweep.

The per-clip VAD only ever trims a clip's own edges, so a pause in the middle
of a clip and the stack-up at a clip boundary both survive into the finished
file. The sweep runs on the assembled final and cuts every hole >= the
declared threshold down to a ~0.3s breath.

These cover the pure arithmetic (plan_silence_cuts). The ffmpeg wrapper around
it is exercised in production; what can go wrong silently is the interval
maths, so that is what is pinned here.
"""
import sys

import pytest

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

from video_processor import plan_silence_cuts


def _kept(segments):
    return sum(e - s for s, e in segments)


def test_no_holes_is_the_identity():
    """Nothing detected -> the whole file, one segment, no re-render."""
    assert plan_silence_cuts([], 30.0, 0.9) == [(0.0, 30.0)]


def test_hole_shorter_than_the_threshold_is_left_alone():
    """0.8s is a real pause at a 0.9s threshold, not dead air."""
    assert plan_silence_cuts([(5.0, 5.8)], 20.0, 0.9) == [(0.0, 20.0)]


def test_leading_hole_keeps_at_most_the_lead_max():
    """No word precedes a hole at 0:00, so the END of the silence survives —
    the run-up to the first word is capped at lead_max, not cut to a breath."""
    keeps = plan_silence_cuts([(0.0, 2.693)], 50.0, 0.9)
    assert keeps == [(pytest.approx(1.993), 50.0)]
    assert _kept(keeps) == pytest.approx(48.007)


def test_mid_hole_keeps_the_breath_at_the_front():
    """The breath sits right after the word that just ended."""
    keeps = plan_silence_cuts([(12.669, 14.300)], 30.0, 0.9)
    assert keeps == [(0.0, pytest.approx(12.969)), (pytest.approx(14.300), 30.0)]
    assert _kept(keeps) == pytest.approx(30.0 - 1.331)


def test_back_to_back_holes_merge_into_one_cut():
    """silencedetect can hand back touching / overlapping spans. The removals
    merge, so the plan never emits a zero-length or inverted segment."""
    keeps = plan_silence_cuts([(5.0, 7.0), (6.5, 9.0)], 20.0, 0.9)
    assert keeps == [(0.0, pytest.approx(5.3)), (pytest.approx(9.0), 20.0)]
    assert all(e > s for s, e in keeps)


def test_hole_at_eof_is_trimmed_to_a_breath():
    """Trailing dead air is cut like any other hole — a silence that runs to
    the end of the file has no silence_end line, and still gets swept."""
    keeps = plan_silence_cuts([(18.0, 20.0)], 20.0, 0.9)
    assert keeps == [(0.0, pytest.approx(18.3))]
    assert _kept(keeps) == pytest.approx(18.3)


def test_the_measured_v13_case():
    """The real file the operator swept by hand: 50.29s with five detections,
    one of them (0.732s) under the threshold and therefore untouched."""
    holes = [
        (0.0, 2.693),
        (12.669, 14.300),
        (21.287, 22.306),
        (23.263, 23.995),   # 0.732s — under 0.9s, stays
        (24.051, 25.532),
    ]
    keeps = plan_silence_cuts(holes, 50.29, 0.9)
    assert _kept(keeps) == pytest.approx(45.0, abs=0.5)
    # the short hole survives INSIDE a kept segment, not as a boundary
    assert any(s < 23.263 and e > 23.995 for s, e in keeps)
    # segments are ordered, non-empty and inside the file
    assert keeps == sorted(keeps)
    assert all(0.0 <= s < e <= 50.29 for s, e in keeps)


def test_a_file_that_is_all_silence_never_plans_to_nothing():
    """A sweep may shorten a file. It may never delete it. An all-silence file
    is one leading hole, so the lead_max rule keeps its last 0.7s."""
    keeps = plan_silence_cuts([(0.0, 10.0)], 10.0, 0.9)
    assert keeps == [(pytest.approx(9.3), 10.0)]
    assert _kept(keeps) > 0


def test_threshold_of_none_or_zero_is_off():
    assert plan_silence_cuts([(5.0, 9.0)], 20.0, 0) == [(0.0, 20.0)]
    assert plan_silence_cuts([(5.0, 9.0)], 20.0, None) == [(0.0, 20.0)]
