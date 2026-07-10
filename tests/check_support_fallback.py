"""v825.8 — a support insert must not vanish because an anchor word missed.

Both cases below are replayed from job d4b661a8 (2026-07-10), where the export
logged:

    [Support] start_word 'number' not found (support 1)
    [Support] end_word 'vessels' not found after start (support 5)
    [Export][v825] support_track_16x9_... -> 4 stills @ 1920x1080   # expected 6

Case 1 — the DELIVERED audio drifted from the build's script. The markdown line
was "the number one food in the world to boost blood flow to your soldier and
make it bigger"; Whisper heard "the single best food on earth for pushing blood
flow to your soldier and make it bigger". `start_word: number` is absent from
the master audio entirely, so the banana comparison board never rendered. The
phrase still locates the region, so the span falls back to it.

Case 2 — Whisper is inconsistent WITHIN one take: "blood vessel" here, "blood
vessels" there. `end_word: vessels` missed on an exact compare. A stem match
resolves it without needing the phrase fallback.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import video_processor as vp


def words(text, t0=0.0, step=0.4):
    """Build a Whisper-shaped word list with even spacing."""
    out = []
    t = t0
    for w in text.split():
        out.append({"word": w, "start": round(t, 3), "end": round(t + step * 0.8, 3)})
        t += step
    return out


# ---- unit: the loose word comparator -------------------------------------
assert vp._word_matches("vessel", "vessels"), "stem: singular/plural must match"
assert vp._word_matches("vessels", "vessel"), "stem: order must not matter"
assert vp._word_matches("relaxes", "relaxes"), "exact must match"
assert not vp._word_matches("and", "an"), "short words must demand exact"
assert not vp._word_matches("number", "pushing"), "unrelated words must not match"
assert not vp._word_matches("beets", "blood"), "unrelated words must not match"
print("OK v825.8: _word_matches (exact / stem / rejects short + unrelated)")


# ---- case 2: stem match rescues 'vessels' -> 'vessel' ---------------------
master2 = words("this food naturally boosts nitric oxide which relaxes and widens "
                "your blood vessel sending circulation where it is needed")
sup2 = [{"support_index": 5, "image_index": 6, "start_word": "relaxes",
         "end_word": "vessels", "phrase": "relaxes and widens your blood vessels"}]
r2 = vp.resolve_support_spans(master2, sup2)[0]
assert r2 is not None, "support 5 was dropped again — stem match not applied"
assert r2["image_index"] == 6
# span must START on 'relaxes' (idx 7) and COVER 'vessel' (idx 12)
assert master2[7]["word"] == "relaxes" and master2[12]["word"] == "vessel"
assert abs(r2["start"] - master2[7]["start"]) < 1e-6, r2
assert r2["end"] >= master2[12]["end"], r2
assert r2["confidence"] == 1.0, f"stem match is a real match, not a fallback: {r2}"
print("OK v825.8: end_word 'vessels' resolves against transcribed 'vessel'")


# ---- case 1: absent anchor word falls back to the phrase span -------------
master1 = words("the single best food on earth for pushing blood flow to your "
                "soldier and make it bigger is not pomegranate")
sup1 = [{"support_index": 1, "image_index": 2, "start_word": "number", "end_word": "bigger",
         "phrase": "number one food in the world to boost blood flow to your soldier "
                   "and make it bigger"}]
r1 = vp.resolve_support_spans(master1, sup1)[0]
assert r1 is not None, (
    "support 1 was DROPPED — an absent start_word must fall back to the phrase "
    "span, not delete the overlay (this is the banana-board regression)"
)
assert r1["image_index"] == 2
assert r1["confidence"] <= 0.5, f"a phrase-span fallback must be marked low-confidence: {r1}"
assert r1["start"] < r1["end"], r1
print("OK v825.8: absent start_word falls back to the phrase span (banana board renders)")


# ---- a support with NO phrase window and NO word match is still dropped ---
master3 = words("completely unrelated narration about the weather today")
sup3 = [{"support_index": 9, "image_index": 3, "start_word": "pomegranate",
         "end_word": "pomegranate", "phrase": ""}]
r3 = vp.resolve_support_spans(master3, sup3)[0]
assert r3 is None, f"no phrase window + no word match must still drop: {r3}"
print("OK v825.8: unanchored + unmatched support is still dropped (no silent garbage)")


# ---- regression: the happy path is unchanged -----------------------------
master4 = words("it is not pomegranate it is not spinach and not even garlic or beets")
sup4 = [
    {"support_index": 1, "image_index": 3, "start_word": "pomegranate",
     "end_word": "pomegranate", "phrase": "pomegranate"},
    {"support_index": 2, "image_index": 5, "start_word": "garlic",
     "end_word": "beets", "phrase": "garlic or beets"},
]
r4 = vp.resolve_support_spans(master4, sup4)
assert r4[0] and r4[1], r4
assert r4[0]["confidence"] == 1.0 and r4[1]["confidence"] == 1.0, r4
assert r4[0]["end"] <= r4[1]["start"], f"held overlays must not overlap: {r4}"
print("OK v825.8: exact-anchor happy path unchanged, no overlap")

print("ALL v825.8 support-fallback checks pass")
