import os, sys, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import video_processor as vp

master_words = json.load(open(os.path.join(os.path.dirname(__file__), "fixtures", "support_master_words.json")))

# 1) clean case: phrase == start..end span. START anchors on "called"; the span
# COVERS "acid" and is extended to the v825.5 min-hold (never shorter than the word).
support = [
    {"support_index": 1, "image_index": 7, "start_word": "called",
     "end_word": "acid", "phrase": "called chlorogenic acid"},
]
spans = vp.resolve_support_spans(master_words, support)
assert len(spans) == 1 and spans[0] is not None, spans
s = spans[0]
assert 20.0 <= s["start"] <= 20.9, s          # anchored on "called"
assert s["end"] >= 21.9, s                    # covers "acid." (ends 21.94)
assert (s["end"] - s["start"]) >= 1.2, s      # v825.5 min-hold applied
assert s["image_index"] == 7
print("OK support spans (clean + min-hold)")

# 2) v825.1: start_word/end_word AUTHORITATIVE even when phrase DRIFTS.
# phrase leads with "is" — the span must still START on "called" (20.30), not "is".
drift = [
    {"support_index": 2, "image_index": 7, "start_word": "called",
     "end_word": "acid", "phrase": "is called chlorogenic acid"},
]
d = vp.resolve_support_spans(master_words, drift)[0]
assert d is not None, d
assert d["start"] >= 20.2, f"span must start on 'called' (20.30), not drifted 'is' (19.90): {d}"
assert d["end"] >= 21.9, d
print("OK support spans (start/end authoritative over drifted phrase)")

# 3) no phrase at all: pure start_word + end_word still resolves + covers the words.
nophrase = [
    {"support_index": 3, "image_index": 8, "start_word": "studies",
     "end_word": "shown", "phrase": ""},
]
p = vp.resolve_support_spans(master_words, nophrase)[0]
assert p is not None, p
assert 22.0 <= p["start"] <= 22.3, p          # "Studies" 22.16
assert p["end"] >= 23.1, p                    # covers "shown" (ends ~23.20)
print("OK support spans (start/end without phrase)")

# 4) v825.5: a single-word span is HELD to the min-hold (doesn't flash+vanish).
single = [
    {"support_index": 4, "image_index": 9, "start_word": "shown",
     "end_word": "shown", "phrase": "shown"},
]
sg = vp.resolve_support_spans(master_words, single)[0]
assert sg is not None, sg
assert (sg["end"] - sg["start"]) >= 1.2, f"single-word overlay must hold >= min-hold: {sg}"
print("OK support spans (single-word min-hold)")

# 5) two CLOSE single-word overlays must not overlap after the hold extension.
two = [
    {"support_index": 5, "image_index": 9, "start_word": "studies", "end_word": "studies", "phrase": "studies"},
    {"support_index": 6, "image_index": 9, "start_word": "shown", "end_word": "shown", "phrase": "shown"},
]
r = vp.resolve_support_spans(master_words, two)
assert r[0] and r[1], r
assert r[0]["end"] <= r[1]["start"], f"held overlays must not overlap: {r[0]} / {r[1]}"
print("OK support spans (no overlap after hold)")

print("ALL support-span checks pass")
