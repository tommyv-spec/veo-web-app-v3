"""v825.9 — a support still is placed inside its OWNING LINE's master span,
correct whether Prompt A or the reworded Prompt B actually shipped.

This reconstructs job d4b661a8 (2026-07-10): the operator exported with Prompt B
on every clip, so the master audio is the B wording, while the build's support
anchors were authored against the A wording. Four of seven anchors do NOT
survive B:

  support 1 (banana board): start 'number' -> B says "the single best food..."
  support 5 (flow chart):   start 'relaxes' -> B says "loosens and opens"
  support 6 (cortisol):     end 'shut' -> B says "closed"
  support 7 (comment CTA):  start 'comment' -> B says "drop the word yes"

The old literal-anchor matcher dropped these stills. Line-relative placement
keeps every one, timed inside the right line, because the LINE aligns (long
string, matches even reworded) and the anchor's position comes from the AUTHORED
line (where it always exists).
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import video_processor as vp

# The 8 lines actually SPOKEN in the export (Prompt B).
B_LINES = [
    "the single best food on earth for pushing blood into your soldier and helping it grow bigger",
    "it isn't pomegranate, it isn't spinach, and no, it isn't garlic or beets either",
    "most guys over forty in this country figure clean eating keeps their soldier going, but one food does far more",
    "this food naturally raises your nitric oxide, which loosens and opens your blood vessels",
    "that drives blood right where your soldier needs it, the two things behind a fuller bigger harder soldier most american guys never hear about",
    "it's also loaded with antioxidants like crocin and safranal that calm inflammation and keep cortisol from clamping your blood vessels closed",
    "so if you'd like to know exactly what it is, drop the word yes below and i'll send it right over to you",
    "just make sure you follow me first, otherwise my message won't reach you",
]

# The 8 AUTHORED lines (Prompt A) — what the build's `- **line:**` fields say.
A_LINES = [
    "the number one food in the world to boost blood flow to your soldier and make it bigger",
    "is not pomegranate, it's not spinach, and it's not even garlic or beets",
    "most american men over forty think just eating healthy keeps their soldier performing, but there's one food that does far more",
    "this food naturally boosts nitric oxide, which relaxes and widens your blood vessels",
    "that sends circulation exactly where your soldier needs it, the two key steps to a fuller, bigger, and harder soldier that most american men never get told about",
    "it's also packed with antioxidants like crocin and safranal that fight inflammation and stop cortisol from squeezing your blood vessels shut",
    "so if you want to know exactly what it is, comment the word yes and i'll send it straight to you",
    "but follow me first, or you won't get my message",
]

SUPPORTS = [
    {"support_index": 1, "image_index": 2, "start_word": "number", "end_word": "bigger",
     "phrase": "number one food in the world to boost blood flow to your soldier and make it bigger"},
    {"support_index": 2, "image_index": 3, "start_word": "pomegranate", "end_word": "pomegranate", "phrase": "pomegranate"},
    {"support_index": 3, "image_index": 4, "start_word": "spinach", "end_word": "spinach", "phrase": "spinach"},
    {"support_index": 4, "image_index": 5, "start_word": "garlic", "end_word": "beets", "phrase": "garlic or beets"},
    {"support_index": 5, "image_index": 6, "start_word": "relaxes", "end_word": "vessels", "phrase": "relaxes and widens your blood vessels"},
    {"support_index": 6, "image_index": 7, "start_word": "cortisol", "end_word": "shut", "phrase": "cortisol from squeezing your blood vessels shut"},
    {"support_index": 7, "image_index": 8, "start_word": "comment", "end_word": "yes", "phrase": "comment the word yes"},
]

# Build the master audio word list from the SPOKEN (B) lines, one continuous
# take at a steady 0.4s/word. Record each scene's [t0, t1] so we can assert a
# support lands in the right line.
STEP = 0.4
master = []
scene_time = []
t = 0.0
for line in B_LINES:
    ws = line.replace(",", "").split()
    t0 = t
    for w in ws:
        master.append({"word": w, "start": round(t, 3), "end": round(t + STEP * 0.8, 3)})
        t += STEP
    scene_time.append((t0, t))
    t += 0.3  # small gap between lines

scene_lines = [{"authored": a, "candidates": [a, b]} for a, b in zip(A_LINES, B_LINES)]

spans = vp.resolve_support_spans(master, SUPPORTS, scene_lines)

# owning scene (1-based clip) for each support, and whether the anchors survive B
OWNER = {1: 1, 2: 2, 3: 2, 4: 2, 5: 4, 6: 6, 7: 7}

assert all(s is not None for s in spans), (
    f"a support was dropped — line-relative placement must keep all 7: "
    f"{[i + 1 for i, s in enumerate(spans) if s is None]}"
)
print(f"OK v825.9: all {len(spans)} supports resolved (none dropped under Prompt B)")

for sp, sup in zip(spans, SUPPORTS):
    si = sup["support_index"]
    t0, t1 = scene_time[OWNER[si] - 1]
    # the whole span must sit within the owning line's time window (small slack)
    assert t0 - 0.5 <= sp["start"] <= t1 + 0.5, (
        f"support {si} start {sp['start']:.2f}s outside owning line "
        f"[{t0:.2f},{t1:.2f}] (image_{sup['image_index']})"
    )
    assert sp["start"] < sp["end"] <= t1 + 1.0, f"support {si} bad span {sp}"
    assert sp["image_index"] == sup["image_index"]
print("OK v825.9: every support lands inside its OWNING line's master span")

# the four Prompt-B-reworded anchors must be marked lower-confidence (proportional)
reworded = {1, 5, 6, 7}
for sp in spans:
    si = sp["support_index"]
    if si in reworded:
        assert sp["confidence"] < 1.0, (
            f"support {si} had a Prompt-B-dropped anchor and should be "
            f"proportional (conf < 1.0), got {sp['confidence']}"
        )
    else:
        assert sp["confidence"] == 1.0, (
            f"support {si} anchors survived B and should be exact "
            f"(conf 1.0), got {sp['confidence']}"
        )
print("OK v825.9: survived anchors exact (1.0), reworded anchors proportional (<1.0)")

# no two overlays overlap after the v825.5 hold pass
ordered = sorted((s for s in spans if s), key=lambda x: x["start"])
for a, b in zip(ordered, ordered[1:]):
    assert a["end"] <= b["start"] + 1e-6, f"overlays overlap: {a} / {b}"
print("OK v825.9: no overlap after the min-hold pass")

# back-compat: with NO scene_lines the v825.8 path still runs (exact anchors)
legacy_master = []
t = 0.0
for w in "is not pomegranate it is not spinach and garlic or beets".split():
    legacy_master.append({"word": w, "start": round(t, 3), "end": round(t + 0.3, 3)})
    t += 0.4
legacy = vp.resolve_support_spans(legacy_master, [
    {"support_index": 1, "image_index": 3, "start_word": "pomegranate", "end_word": "pomegranate", "phrase": "pomegranate"},
])
assert legacy[0] is not None and legacy[0]["confidence"] == 1.0, legacy
print("OK v825.9: back-compat — no scene_lines still resolves via v825.8 path")

print("ALL v825.9 line-relative checks pass")
