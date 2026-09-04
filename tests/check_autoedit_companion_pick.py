"""v698A.2.4 — the auto-edit pairs the b-roll / support track with the SAME
export it uses as the base, never the oldest file that sorts first.

Replays d74ab616's real list-outputs (three exports on 2026-09-04) where the
old `next(...)` picked export 1's b-roll for export 3's speaker.

Run: python tests/check_autoedit_companion_pick.py  (from code/)
"""
import os
import sys

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from autoedit_pipeline import pick_companion  # noqa: E402

OUTS = [
    "final_broll_d74ab616_20260904_093057_f742de.mp4",
    "final_broll_d74ab616_20260904_095414_007d35.mp4",
    "final_broll_d74ab616_20260904_103908_86f08b.mp4",
    "final_export_d74ab616_20260904_093057_f742de.mp3",
    "final_export_d74ab616_20260904_093057_f742de.mp4",
    "final_export_d74ab616_20260904_095414_007d35.mp3",
    "final_export_d74ab616_20260904_095414_007d35.mp4",
    "final_export_d74ab616_20260904_103908_86f08b.mp3",
    "final_export_d74ab616_20260904_103908_86f08b.mp4",
    "clip_0_1.1.mp4",
]

# the base is export 3 -> its own b-roll, not the first that sorts
assert pick_companion(OUTS, "final_export_d74ab616_20260904_103908_86f08b.mp4", "final_broll_") \
    == "final_broll_d74ab616_20260904_103908_86f08b.mp4"
# the base is export 2 -> export 2's b-roll
assert pick_companion(OUTS, "final_export_d74ab616_20260904_095414_007d35.mp4", "final_broll_") \
    == "final_broll_d74ab616_20260904_095414_007d35.mp4"
# no support track at all -> None
assert pick_companion(OUTS, "final_export_d74ab616_20260904_103908_86f08b.mp4", "support_track_") is None
print("OK exact stem match wins")

# support tracks carry the export stem after the aspect-ratio tag (v825.6)
OUTS2 = OUTS + [
    "support_track_16x9_d74ab616_20260904_093057_f742de.mp4",
    "support_track_16x9_d74ab616_20260904_103908_86f08b.mp4",
    "support_track_9x16_d74ab616_20260904_103908_86f08b.mp4",
]
assert pick_companion(OUTS2, "final_export_d74ab616_20260904_103908_86f08b.mp4", "support_track_") \
    == "support_track_16x9_d74ab616_20260904_103908_86f08b.mp4"
print("OK support track matched on the export stem (first aspect ratio wins, as before)")

# a base with no companion of its own -> the NEWEST companion by name (stamps sort chronologically)
assert pick_companion(OUTS, "final_export_d74ab616_20260904_120000_aaaaaa.mp4", "final_broll_") \
    == "final_broll_d74ab616_20260904_103908_86f08b.mp4"
# nothing of that kind -> None
assert pick_companion(["final_export_x.mp4"], "final_export_x.mp4", "final_broll_") is None
# an mp4 only: a stray .json with the prefix is never picked
assert pick_companion(["final_broll_d74ab616_20260904_103908_86f08b.json"],
                      "final_export_d74ab616_20260904_103908_86f08b.mp4", "final_broll_") is None
print("OK fallback = newest, never a non-mp4")

print("ALL OK check_autoedit_companion_pick")
