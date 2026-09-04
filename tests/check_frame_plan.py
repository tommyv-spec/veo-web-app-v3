"""v698A.2.2 — unit tests for video_processor.frame_plan (pure, no ffmpeg).

The rule under test: a segment's length is a whole number of frames computed
from ABSOLUTE boundaries, never from its own duration. So the sum of the
segments up to boundary k is exactly round(boundary_k * fps) and the rounding
can never accumulate.

Run: python tests/check_frame_plan.py   (from code/)
"""
import math
import os
import sys

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from video_processor import frame_plan  # noqa: E402

FPS = 24.0


def F(t, fps=FPS):
    return int(round(t * fps))


def mk(pairs):
    return [{"start": s, "end": e, "target_duration": e - s} for s, e in pairs]


def clips(plan):
    return [p for p in plan if p["kind"] == "clip"]


def blacks(plan):
    return [p for p in plan if p["kind"] == "black"]


def check_boundaries(plan, label):
    """Every cumulative frame sum must land exactly on its absolute boundary."""
    acc = 0
    for seg in plan:
        acc += seg["frames"]
        assert acc == seg["end_f"], (
            f"{label}: cumulative {acc} != boundary {seg['end_f']} at {seg}"
        )
    return acc


# --- (a) the 17 REAL targets of d74ab616 export f17fd655 ---------------------
# Pulled 2026-09-04 from
#   GET https://kavenobuilder.com/api/jobs/
#       d74ab616-ab21-4054-b121-a386fc2d823b/export-status
#   -> result.stats.v698a_broll_stats.clip_details[].target_start / target_end
# in the order clip_details lists them (which is clip order, NOT time order).
# From the same record: master_duration = 54.48, black_segments = 0,
# methods_used = {"speed": 10, "speed_and_trim": 6, "loop": 1},
# final_duration = 54.442 (the drift this rule removes).
REAL_TARGETS = [
    (0.0, 8.4),
    (32.730051353097494, 36.16),
    (44.3, 48.04),
    (8.4, 12.26),
    (12.26, 14.16),
    (14.16, 15.24),
    (15.24, 18.48),
    (18.48, 19.74),
    (19.74, 22.06),
    (22.06, 25.08),
    (25.08, 27.74),
    (27.74, 32.8),
    (36.16, 37.68),
    (37.68, 39.18),
    (39.18, 44.3),
    (48.04, 49.5),
    (49.5, 54.48),
]
REAL_MASTER = 54.48   # v698a_broll_stats.master_duration on that export

plan = frame_plan(mk(REAL_TARGETS), REAL_MASTER, FPS)
assert len(clips(plan)) == 17, len(clips(plan))
assert len(blacks(plan)) == 0, blacks(plan)
total = check_boundaries(plan, "real targets")
assert total == F(REAL_MASTER) == 1308, (total, F(REAL_MASTER))
# the record's one overlap: clip 11 ends 32.80s, clip 1 starts 32.7300s.
# The later clip absorbs it; the earlier one is untouched.
by_index = {c["index"]: c for c in clips(plan)}
assert by_index[11]["frames"] == F(32.8) - F(27.74), by_index[11]
assert by_index[1]["start_f"] == F(32.8), by_index[1]        # pushed to the cursor
assert by_index[1]["frames"] == F(36.16) - F(32.8), by_index[1]
print(f"OK (a): 17 real targets -> 17 clips, 0 black, {total} frames "
      f"= {total / FPS:.3f}s = F({REAL_MASTER})")

# --- (g) the SAME targets under today's rule land 9 frames late -------------
# `-t {d}` keeps every frame whose start is < d, i.e. floor(d*fps)+1 frames.
# Verified locally on ffmpeg 8.1: a 1.90s target came out 46 frames / 1.9167s.
acc_t = 0
worst_late = 0
for s, e in sorted(REAL_TARGETS):
    acc_t += math.floor((e - s) * FPS) + 1
    worst_late = max(worst_late, acc_t - F(e))
assert worst_late >= 8, worst_late
assert acc_t - F(REAL_MASTER) == 9, (acc_t, F(REAL_MASTER))
print(f"OK (g): today's `-t` rule on the same targets ends {acc_t - F(REAL_MASTER)} "
      f"frames = {(acc_t - F(REAL_MASTER)) / FPS:.3f}s late (measured drift on the "
      f"delivered file: +0.35s); the frame plan ends exactly on F(master)")

# --- (b) a 0.3s gap becomes one black of 7 frames ---------------------------
plan = frame_plan(mk([(0.0, 1.0), (1.3, 2.3)]), 2.3, FPS)
assert [p["kind"] for p in plan] == ["clip", "black", "clip"], plan
assert blacks(plan)[0]["frames"] == 7, plan            # round(1.3*24) - 24 = 7
assert clips(plan)[0]["frames"] == 24
assert clips(plan)[1]["frames"] == F(2.3) - F(1.3)
check_boundaries(plan, "gap")
print("OK (b): a 0.3s gap -> one black of 7 frames")

# --- (c) a 0.5s overlap shortens the LATER clip by 12 frames ----------------
plan = frame_plan(mk([(0.0, 2.0), (1.5, 3.5)]), 3.5, FPS)
assert [p["kind"] for p in plan] == ["clip", "clip"], plan
first, second = clips(plan)
assert first["frames"] == 48, first                    # earlier clip untouched
assert first["start_f"] == 0 and first["end_f"] == 48
assert second["start_f"] == 48, second                 # starts at the cursor
assert second["frames"] == F(3.5) - 48 == 36, second   # 48 frames of window - 12
assert F(3.5) - F(1.5) - second["frames"] == 12, second
check_boundaries(plan, "overlap")
print("OK (c): a 0.5s overlap shortens the later clip by 12 frames, "
      "the earlier one is untouched")

# --- (d) a fully overlapped clip is skipped (absent from the plan) ----------
plan = frame_plan(mk([(0.0, 5.0), (1.0, 2.0), (5.0, 6.0)]), 6.0, FPS)
idxs = [c["index"] for c in clips(plan)]
assert idxs == [0, 2], idxs                            # clip 1 never appears
assert len(blacks(plan)) == 0, plan
assert check_boundaries(plan, "swallowed") == F(6.0)
print("OK (d): a fully overlapped clip is skipped, the timeline stays exact")

# --- (e) a trailing gap becomes a trailing black ----------------------------
plan = frame_plan(mk([(0.0, 2.0)]), 3.0, FPS)
assert [p["kind"] for p in plan] == ["clip", "black"], plan
assert blacks(plan)[0]["frames"] == 24, plan
assert check_boundaries(plan, "trail") == F(3.0)
# and no trailing black when the clips already reach the end
plan = frame_plan(mk([(0.0, 3.0)]), 3.0, FPS)
assert [p["kind"] for p in plan] == ["clip"], plan
# nor when the clips overrun the master (never a negative segment)
plan = frame_plan(mk([(0.0, 3.5)]), 3.0, FPS)
assert [p["kind"] for p in plan] == ["clip"], plan
print("OK (e): a trailing gap -> a trailing black; no negative / zero fills")

# --- (f) fps 30 computes its boundaries at 30 -------------------------------
plan = frame_plan(mk([(0.0, 1.0), (1.0, 2.5)]), 2.5, 30.0)
assert [c["frames"] for c in clips(plan)] == [30, 45], plan
assert check_boundaries(plan, "fps30") == F(2.5, 30.0) == 75
print("OK (f): fps=30 budgets 30 and 45 frames, total 75")

# --- half-a-frame is the worst case, at any fps and any boundary ------------
for fps in (24.0, 25.0, 30.0):
    cuts = [0.0]
    t = 0.0
    for k in range(40):
        t += 0.37 + 0.031 * k          # deliberately awkward, never frame-aligned
        cuts.append(t)
    plan = frame_plan(
        mk(list(zip(cuts[:-1], cuts[1:]))), cuts[-1], fps
    )
    acc = 0
    for seg, want_end in zip(plan, cuts[1:]):
        acc += seg["frames"]
        assert abs(acc / fps - want_end) <= 0.5 / fps + 1e-9, (fps, acc, want_end)
    assert acc == int(round(cuts[-1] * fps))
print("OK: 40 awkward boundaries at 24/25/30 fps — every cut within half a frame")

print("ALL OK check_frame_plan")
