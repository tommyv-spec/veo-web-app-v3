"""Lock-in tests for autoedit_pipeline.plan_caption_windows — the pure
placement planner. Operator hard rule: captions must NEVER cover a face,
NEVER cover the picture-in-picture insert, and should avoid the highest
motion area. Synthetic buckets only, no video/ffmpeg needed.
"""
import sys
import pathlib

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from autoedit_pipeline import plan_caption_windows  # noqa: E402


def bucket(t, faces=None, motion=None):
    return {"t": t, "faces": faces or [], "motion": motion or [0.0] * 10}


FACE = [0.30, 0.15, 0.70, 0.48]  # centered talking head


def test_never_covers_a_face():
    buckets = [bucket(t + 0.5, faces=[FACE]) for t in range(20)]
    windows = plan_caption_windows(buckets, chin=0.50, segs=[], pip_y=1050, dur=20)
    for a, b, off in windows:
        center = 0.5 + off
        assert center - 0.075 > 0.48 or center + 0.075 < 0.15, f"band at {center} covers the face"


def test_moves_off_the_pip_window():
    segs = [(8.0, 12.0)]
    buckets = [bucket(t + 0.5, faces=[FACE]) for t in range(20)]
    windows = plan_caption_windows(buckets, chin=0.50, segs=segs, pip_y=1050, dur=20)
    during = [w for w in windows if w[0] < 10 < w[1]]
    assert during, "no window covers t=10"
    center = 0.5 + during[0][2]
    pip_top, pip_bot = 1050 / 1920, 1500 / 1920
    assert center + 0.075 < pip_top + 0.02 or center - 0.075 > pip_bot, "caption overlaps the PIP"


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


def test_two_faces_two_shot_no_legal_band_falls_back_below_chin():
    buckets = [bucket(t + 0.5, faces=[FACE_HIGH, FACE_LOW]) for t in range(20)]
    windows = plan_caption_windows(buckets, chin=0.50, segs=[], pip_y=1050, dur=20)
    # This framing genuinely has no legal band (every candidate overlaps one
    # of the two faces), so the planner falls back to the below-chin
    # candidate (cands[0]) rather than silently picking a face-covering spot.
    expected_offset = min(0.50 + 0.095, 0.60) - 0.5
    for a, b, off in windows:
        assert off == pytest.approx(expected_offset), f"expected below-chin fallback offset {expected_offset}, got {off}"
