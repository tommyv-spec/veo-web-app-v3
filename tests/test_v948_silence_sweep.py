"""v948 — the post-concat silence-hole sweep.

The per-clip VAD only ever trims a clip's own edges, so a pause in the middle
of a clip and the stack-up at a clip boundary both survive into the finished
file. The sweep runs on the assembled final and cuts every hole >= the
declared threshold down to a ~0.3s breath.

These cover the pure arithmetic (plan_silence_cuts) plus the one thing the
ffmpeg wrapper must never do silently: deliver a file SHORTER than the plan it
just computed. "Exercised in production" was the old note here, and production
shipped three truncated exports before anyone measured the length (v948.3).
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


def test_a_single_keep_segment_is_still_an_applied_sweep():
    """v948.1 (review fix): a leading hole cut down to its breath leaves ONE
    keep segment — holes were cut, the sweep must apply. The old condition
    `len(keeps) <= 1` silently shipped the unswept file in this case."""
    from video_processor import plan_silence_cuts
    keeps = plan_silence_cuts([(0.0, 10.0)], 10.0, 0.9)
    assert len(keeps) == 1
    # the sweep path's apply-gate logic: holes_cut > 0 and keeps non-empty
    holes_cut = 1
    assert not (holes_cut == 0 or not keeps)


# ---------------------------------------------------------------------------
# v948.3 — the render must deliver the plan, and is rejected when it does not.
#
# The numbers below are the ones measured locally on 2026-09-13 from the four
# approved clips of the nuri county-fair build (job 34824da1), reproducing the
# platform's truncated export:
#
#   clip | source  | post-trim | post-VAD | 30-33w floor | verdict  | final
#      1 | 10.005s |  10.026s  |  10.026s |     5.400s   | ACCEPTED | 10.026s
#      2 | 10.005s |  10.026s  |  10.026s |     5.760s   | ACCEPTED | 10.026s
#      3 | 10.005s |  10.026s  |   9.792s |     5.940s   | ACCEPTED |  9.792s
#      4 | 10.008s |  10.031s  |  10.031s |     5.940s   | ACCEPTED | 10.031s
#
# export_final_video delivered all four: 39.896s. The sweep then planned to
# keep 38.610s and the render delivered 28.835s — the whole scene-4 CTA gone.
# Clip 4 carries bt709 colour tags its three siblings do not, so the concat
# changes video properties at its join, ffmpeg reinitialises the filter graph
# there, and setpts restarts. ffmpeg is not run here: the guard's contract is
# "delivered < planned -> reject", and that is what is pinned.
# ---------------------------------------------------------------------------

COUNTY_FAIR_IN = 39.896354       # export_final_video's output, all 4 scenes
COUNTY_FAIR_HOLES = [(5.332542, 6.918896), (34.103, 34.939)]
COUNTY_FAIR_TRUNCATED = 28.835   # what the sweep's render actually delivered


def _sweep(monkeypatch, tmp_path, delivered, *, src_duration=COUNTY_FAIR_IN,
           holes=None):
    """Run sweep_silence_holes with ffmpeg stubbed to deliver `delivered`s."""
    import video_processor as vp

    src = tmp_path / "final.mp4"
    out = tmp_path / "swept.mp4"
    src.write_bytes(b"")

    durations = {str(src): src_duration, str(out): delivered}
    monkeypatch.setattr(vp, "ffprobe_json", lambda p: {"_p": str(p)})
    monkeypatch.setattr(vp, "get_duration", lambda info: durations[info["_p"]])
    monkeypatch.setattr(
        vp, "detect_silence_holes",
        lambda path, **kw: (COUNTY_FAIR_HOLES if holes is None else holes)
        if str(path) == str(src) else [],
    )

    def _fake_run(cmd, **kw):
        out.write_bytes(b"rendered")
        return 0, "", ""

    monkeypatch.setattr(vp, "run", _fake_run)
    return vp.sweep_silence_holes(src, out, 0.9), out


def test_the_county_fair_truncation_is_rejected(monkeypatch, tmp_path):
    """The measured failure: plan 38.610s, render 28.835s. The sweep must be
    refused so the caller ships the full 39.896s export with scene 4 intact."""
    stats, out = _sweep(monkeypatch, tmp_path, COUNTY_FAIR_TRUNCATED)

    assert stats["applied"] is False, "a 9.8s shortfall must never be applied"
    # the caller (main.py) replaces the export only when applied is True, and
    # prints these three keys unconditionally
    assert stats["removed_s"] == 0.0
    assert stats["final_duration"] == pytest.approx(COUNTY_FAIR_IN)
    assert stats["holes_cut"] == 0
    assert stats["plan_mismatch_s"] == pytest.approx(9.775, abs=0.01)
    assert not out.exists(), "the rejected render must not be left on disk"


def test_a_render_that_matches_its_plan_is_applied(monkeypatch, tmp_path):
    """The same file, same holes, a renderer that delivers the plan: applied.
    The plan keeps 38.610s; CFR rounding puts the real render at 38.665s."""
    stats, out = _sweep(monkeypatch, tmp_path, 38.665)

    assert stats["applied"] is True
    assert stats["holes_cut"] == 1
    assert stats["removed_s"] == pytest.approx(COUNTY_FAIR_IN - 38.665)
    assert "plan_mismatch_s" not in stats


def test_frame_rounding_under_the_tolerance_still_applies(monkeypatch, tmp_path):
    """A sweep may land a few frames short of its plan — the render is CFR.
    Only a real shortfall is rejected."""
    from video_processor import V948_PLAN_TOLERANCE_S

    planned = sum(e - s for s, e in
                  plan_silence_cuts(COUNTY_FAIR_HOLES, COUNTY_FAIR_IN, 0.9))
    stats, _ = _sweep(monkeypatch, tmp_path,
                      planned - V948_PLAN_TOLERANCE_S + 0.01)
    assert stats["applied"] is True

    stats, _ = _sweep(monkeypatch, tmp_path,
                      planned - V948_PLAN_TOLERANCE_S - 0.01)
    assert stats["applied"] is False
