"""Lock-in tests for autoedit_pipeline.plan_caption_windows — the pure
placement planner. Operator hard rule: captions must NEVER cover a face,
NEVER cover the picture-in-picture insert, and should avoid the highest
motion area. Synthetic buckets only, no video/ffmpeg needed.
"""
import sys
import pathlib

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from autoedit_pipeline import plan_caption_windows, enforce_min_dwell  # noqa: E402


def bucket(t, faces=None, motion=None):
    return {"t": t, "faces": faces or [], "motion": motion or [0.0] * 10}


FACE = [0.30, 0.15, 0.70, 0.48]  # centered talking head


def test_never_covers_a_face():
    buckets = [bucket(t + 0.5, faces=[FACE]) for t in range(20)]
    windows = plan_caption_windows(buckets, chin=0.50, segs=[], pip_y=1050, dur=20)
    for a, b, off in windows:
        center = 0.5 + off
        assert center - 0.075 > 0.48 or center + 0.075 < 0.15, f"band at {center} covers the face"


FACE_LOW_SMALL = [0.30, 0.32, 0.70, 0.60]  # smaller face, lower in frame -- leaves the top clear


def test_avoids_pip_when_a_legal_band_exists():
    # Verified empirically (not hand-waved): with this face box the top candidate
    # (c=0.14, band 0.065-0.215) is valid for the whole PIP window -- a legal band
    # genuinely exists, so the planner must find it rather than overlap either the
    # face or the insert.
    segs = [(8.0, 12.0)]
    buckets = [bucket(t + 0.5, faces=[FACE_LOW_SMALL]) for t in range(20)]
    windows = plan_caption_windows(buckets, chin=0.50, segs=segs, pip_y=1050, dur=20)
    during = [w for w in windows if w[0] < 10 < w[1]]
    assert during, "no window covers t=10"
    center = 0.5 + during[0][2]
    pip_top, pip_bot = 1050 / 1920, 1500 / 1920
    face_top, face_bot = FACE_LOW_SMALL[1], FACE_LOW_SMALL[3]
    assert center + 0.075 < pip_top + 0.02 or center - 0.075 > pip_bot, "caption overlaps the PIP"
    assert center + 0.075 < face_top or center - 0.075 > face_bot, "caption overlaps the face"


def test_face_wins_when_face_and_pip_cannot_both_be_cleared():
    # Same impossible geometry as the original PIP test: a face spanning 33% of frame
    # height plus the default below-chin PIP band leaves no y-position that clears both
    # (verified: only a 0.032-tall gap exists where the 0.15-tall band would need to
    # fit). Documented rule (operator, verbatim: "never cover the main action or any
    # face -- never"): faces are never covered, no exceptions; the PIP is our own
    # inserted overlay and MAY be covered when the framing leaves no clear band.
    segs = [(8.0, 12.0)]
    buckets = [bucket(t + 0.5, faces=[FACE]) for t in range(20)]
    windows = plan_caption_windows(buckets, chin=0.50, segs=segs, pip_y=1050, dur=20)
    during = [w for w in windows if w[0] < 10 < w[1]]
    assert during, "no window covers t=10"
    center = 0.5 + during[0][2]
    assert center - 0.075 > 0.48 or center + 0.075 < 0.15, f"band at {center} covers the face"


def test_hysteresis_no_flapping():
    # face flickers every other second -- smoothing + lookahead must not flap
    buckets = [bucket(t + 0.5, faces=[FACE] if t % 2 == 0 else []) for t in range(30)]
    windows = plan_caption_windows(buckets, chin=0.50, segs=[], pip_y=1050, dur=30)
    assert len(windows) <= 3, f"flapping: {len(windows)} windows for a static scene"


# Two-shot / interview: a face high in frame AND a second face lower down.
# high face sits near the top of the frame, low face sits below the chin --
# together they leave no fully legal band anywhere (top hits the high face,
# below-chin/lower-third both hit the low face).
FACE_HIGH = [0.05, 0.10, 0.45, 0.40]
FACE_LOW = [0.55, 0.42, 0.95, 0.72]


def test_two_faces_two_shot_picks_least_overlapping_candidate():
    buckets = [bucket(t + 0.5, faces=[FACE_HIGH, FACE_LOW]) for t in range(20)]
    windows = plan_caption_windows(buckets, chin=0.50, segs=[], pip_y=1050, dur=20)
    # This framing genuinely has no legal band (every candidate overlaps one of the
    # two faces): below-chin (0.595) buries 288px of FACE_LOW, top (0.14) buries 221px
    # of FACE_HIGH, lower-third (0.70) buries only 182px of FACE_LOW -- the ladder's
    # least-face-overlap step must pick 0.70, not blindly default to cands[0].
    expected_offset = 0.70 - 0.5
    for a, b, off in windows:
        assert off == pytest.approx(expected_offset), f"expected least-overlap offset {expected_offset}, got {off}"


FACE_BIG = [0.30, 0.10, 0.70, 0.70]  # tall face; overlaps all three candidate bands, by different amounts


def test_picks_least_face_overlap_when_nothing_is_clear():
    # Measured overlaps (px, 1920-tall frame): below-chin (0.595) = 288, top (0.14) =
    # 221, lower-third (0.70) = 144. No candidate is face-clear here, so step 3 of the
    # ladder must win on SMALLEST overlap (0.70), not fall through to cands[0] (0.595,
    # the worst option) purely because it is first in priority order.
    buckets = [bucket(t + 0.5, faces=[FACE_BIG]) for t in range(20)]
    windows = plan_caption_windows(buckets, chin=0.50, segs=[], pip_y=1050, dur=20)
    expected_offset = 0.70 - 0.5
    overlap_px = {0.595: 288.0, 0.70: 144.0, 0.14: 220.8}
    for a, b, off in windows:
        chosen = round(off + 0.5, 3)
        assert off == pytest.approx(expected_offset), (
            f"expected the least-overlap candidate 0.70 (144px), got offset {off} "
            f"(candidate {chosen}, ~{overlap_px.get(chosen, '?')}px measured)"
        )


def test_no_window_shorter_than_two_seconds():
    # A squeeze-forced single-second switch (e.g. a face briefly growing, or a very
    # short PIP window) can hand back a window list with a 1s island sandwiched between
    # two other positions -- a caption that hops for one second and hops right back
    # reads as broken. enforce_min_dwell is the pure post-pass that must absorb it.
    # No faces in these buckets -> both neighbours cost 0px, so the tie-break rule
    # ("ties keep the earlier/left neighbour") is what's under test here too.
    buckets = [bucket(t + 0.5) for t in range(20)]
    windows = [(0.0, 10.0, 0.0), (10.0, 11.0, 0.2), (11.0, 20.0, 0.4)]
    merged = enforce_min_dwell(windows, buckets)
    for start, end, off in merged:
        assert end - start >= 2.0, f"window {start}-{end}@{off} is shorter than the 2.0s minimum dwell"
    assert merged == [(0.0, 11.0, 0.0), (11.0, 20.0, 0.4)], f"expected the 1s blip absorbed left (tie), got {merged}"
