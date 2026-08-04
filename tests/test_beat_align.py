"""Tests for code/beat_align.py — the author-time beat bridge.

Deliberately librosa-free: every test drives the PURE functions with synthetic
beat grids, so the suite runs on any box (including CI/Render) even though
`analyze_song` needs librosa. That mirrors the module's own split.
"""
import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import beat_align as ba  # noqa: E402


def grid(bpm=120.0, n=200, start=0.0):
    period = 60.0 / bpm
    return np.arange(n) * period + start


# ------------------------------------------------------------------ md I/O

MD = """## §0
- **TEST:** x

### Scene 1
- **image:** image_1
- **target_duration_s:** 3.430
- **action_note:** a

### Scene 2
- **image:** image_2
- **target_duration_s:** 2.000
- **action_note:** b
"""


def test_read_build_returns_scenes_in_order(tmp_path):
    p = tmp_path / "b.md"
    p.write_text(MD, encoding="utf-8")
    _, scenes = ba.read_build(p)
    assert scenes == [(1, 3.43), (2, 2.0)]


def test_write_durations_roundtrip_preserves_everything_else(tmp_path):
    p = tmp_path / "b.md"
    p.write_text(MD, encoding="utf-8")
    text, _ = ba.read_build(p)
    out = ba.write_durations(text, [1.111, 2.222])
    assert "- **target_duration_s:** 1.111" in out
    assert "- **target_duration_s:** 2.222" in out
    # untouched fields survive
    assert "- **image:** image_1" in out and "- **action_note:** b" in out
    assert out.count("### Scene") == 2
    p.write_text(out, encoding="utf-8")
    _, scenes = ba.read_build(p)
    assert scenes == [(1, 1.111), (2, 2.222)]


def test_read_build_rejects_scene_without_duration(tmp_path):
    p = tmp_path / "b.md"
    p.write_text("### Scene 1\n- **image:** image_1\n", encoding="utf-8")
    with pytest.raises(SystemExit):
        ba.read_build(p)


# -------------------------------------------------------------------- snap

def test_snap_lands_every_cut_on_a_beat():
    bt = grid()
    sal = np.ones(len(bt))
    scenes = [(1, 3.43), (2, 4.17), (3, 2.0)]
    edges = ba.snap_boundaries(scenes, bt, sal, start_time=0.0)
    for e in edges:
        assert np.min(np.abs(bt - e)) < 1e-9


def test_snap_never_moves_a_cut_beyond_the_window():
    """The whole point of snap: bounded movement. Regression for the first
    implementation, which reached 1.6s away and turned 4.17s into 2.02s.

    NOTE the two different bounds. A single CUT moves at most +/-tol. A
    DURATION sits between two cuts that can move in opposite directions, so its
    bound is 2*tol. Asserting the cut bound on a duration is wrong and was the
    first version of this test."""
    bt = grid(bpm=123.0)
    sal = np.random.default_rng(0).random(len(bt))
    scenes = [(i, d) for i, d in enumerate([3.43, 4.17, 2.0, 5.77, 2.03], 1)]
    edges = ba.snap_boundaries(scenes, bt, sal, start_time=0.0)
    period = 60.0 / 123.0
    tol = 0.6 * period

    # every cut is within tol of its own ideal absolute time
    ideal = 0.0
    for (_, dur), cut in zip(scenes, edges[1:]):
        ideal += dur
        assert abs(cut - ideal) <= tol + 1e-6

    # every duration is within 2*tol of what the author wrote
    for authored, got in zip([d for _, d in scenes], np.diff(edges)):
        assert abs(got - authored) <= 2 * tol + 1e-6


def test_snap_error_does_not_compound():
    """Each boundary snaps to its own absolute ideal, so total drift stays
    inside one window rather than accumulating over N clips."""
    bt = grid(bpm=123.0)
    sal = np.random.default_rng(1).random(len(bt))
    scenes = [(i, 2.0) for i in range(1, 21)]  # 20 clips
    edges = ba.snap_boundaries(scenes, bt, sal, start_time=0.0)
    total = edges[-1] - edges[0]
    assert abs(total - 40.0) <= 0.6 * (60.0 / 123.0) + 1e-6


def test_snap_prefers_the_most_salient_beat_in_the_window():
    """With tol 0.6 beats the window spans 1.2 beats, so it holds two
    candidates only when the ideal falls near a midpoint. That is exactly when
    the salience preference gets to decide."""
    bt = grid(bpm=120.0)          # period 0.5s, tol 0.3s
    sal = np.zeros(len(bt))
    sal[5] = 1.0                  # beat at 2.5s is the strong one
    # ideal 2.25s sits midway: beats 2.0 and 2.5 are both 0.25s away, in window.
    edges = ba.snap_boundaries([(1, 2.25)], bt, sal, start_time=0.0)
    assert edges[1] == pytest.approx(2.5)


def test_snap_takes_the_nearest_when_the_window_holds_one_beat():
    bt = grid(bpm=120.0)
    sal = np.zeros(len(bt))
    sal[5] = 1.0                  # strong, but 0.5s away — outside the 0.3s window
    edges = ba.snap_boundaries([(1, 2.0)], bt, sal, start_time=0.0)
    assert edges[1] == pytest.approx(2.0)


def test_snap_never_emits_a_nonpositive_clip():
    bt = grid(bpm=120.0)
    sal = np.ones(len(bt))
    scenes = [(i, 0.05) for i in range(1, 6)]  # absurdly short
    edges = ba.snap_boundaries(scenes, bt, sal, start_time=0.0)
    assert all(b > a for a, b in zip(edges, edges[1:]))


# ------------------------------------------------------------------- solve

def test_solve_before_lands_the_last_cut_exactly_on_the_anchor():
    bt = grid(bpm=120.0)
    sal = np.ones(len(bt))
    anchor = 40
    idxs = ba.solve_boundaries(bt, sal, anchor, count=4, lo=0.5, hi=2.0, before=True)
    assert idxs[-1] == anchor
    assert len(idxs) == 5          # 4 clips -> 5 boundaries
    assert idxs == sorted(idxs)


def test_solve_after_starts_on_the_anchor():
    bt = grid(bpm=120.0)
    sal = np.ones(len(bt))
    anchor = 10
    idxs = ba.solve_boundaries(bt, sal, anchor, count=3, lo=0.5, hi=2.0, before=False)
    assert idxs[0] == anchor
    assert len(idxs) == 4
    assert idxs == sorted(idxs)


def test_solve_respects_min_and_max():
    bt = grid(bpm=120.0)
    sal = np.random.default_rng(2).random(len(bt))
    idxs = ba.solve_boundaries(bt, sal, 60, count=6, lo=0.8, hi=1.6, before=True)
    durs = np.diff([bt[i] for i in idxs])
    assert durs.min() >= 0.8 - 1e-6
    assert durs.max() <= 1.6 + 1e-6


def test_solve_raises_when_the_window_cannot_fit_the_clips():
    bt = grid(bpm=120.0, n=6)      # only ~2.5s of grid
    sal = np.ones(len(bt))
    with pytest.raises(SystemExit):
        ba.solve_boundaries(bt, sal, 5, count=10, lo=1.9, hi=2.0, before=True)


@pytest.mark.parametrize("n,drop_clip", [(9, 5), (9, 1), (9, 9), (5, 3), (2, 2)])
def test_solve_split_yields_exactly_n_plus_one_edges(n, drop_clip):
    """Regression: `drop_clip` STARTS on the drop, so it belongs to the AFTER
    block — after_count is n - drop_clip + 1, not n - drop_clip. The first
    version was off by one and raised IndexError building the edge list."""
    bt = grid(bpm=120.0, n=400)
    sal = np.ones(len(bt))
    anchor = 200
    before_count = drop_clip - 1
    after_count = n - drop_clip + 1
    assert before_count + after_count == n

    pre = ba.solve_boundaries(bt, sal, anchor, before_count, 0.5, 2.0, before=True)
    post = ba.solve_boundaries(bt, sal, anchor, after_count, 0.5, 2.0, before=False)
    edges = ([bt[i] for i in pre[:-1]] if pre else []) + [bt[i] for i in post]
    assert len(edges) == n + 1
    assert all(b > a for a, b in zip(edges, edges[1:]))
    # the nominated clip starts exactly on the drop
    assert edges[drop_clip - 1] == pytest.approx(bt[anchor])


def test_solve_zero_clips_is_empty():
    bt = grid()
    assert ba.solve_boundaries(bt, np.ones(len(bt)), 10, 0, 0.5, 2.0, before=True) == []


# ------------------------------------------------------------------ pacing

def test_pacing_targets_stay_in_range_and_vary():
    t = ba._pacing_targets(9, 0.5, 2.0, before=True)
    assert len(t) == 9
    assert min(t) >= 0.5 and max(t) <= 2.0
    assert len(set(round(x, 3) for x in t)) > 4      # not uniform slots


def test_pacing_before_accelerates_into_the_drop():
    t = ba._pacing_targets(8, 0.5, 2.0, before=True)
    assert t[0] > t[-1]        # long holds first, short cuts at the drop


def test_seg_score_rewards_salience_over_duration_fit():
    """v5's deliberate 2.8 vs 1.6 weighting: a strong beat at the wrong length
    beats a weak beat at the right length."""
    strong_wrong = ba._seg_score(1.0, dur=2.0, target=1.0, lo=0.5, hi=2.0)
    weak_right = ba._seg_score(0.0, dur=1.0, target=1.0, lo=0.5, hi=2.0)
    assert strong_wrong > weak_right
