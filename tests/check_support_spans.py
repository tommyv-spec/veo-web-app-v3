import os, sys, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import video_processor as vp

master_words = json.load(open(os.path.join(os.path.dirname(__file__), "fixtures", "support_master_words.json")))

# 1) clean case: phrase == start..end span
support = [
    {"support_index": 1, "image_index": 7, "start_word": "called",
     "end_word": "acid", "phrase": "called chlorogenic acid"},
]
spans = vp.resolve_support_spans(master_words, support)
assert len(spans) == 1 and spans[0] is not None, spans
s = spans[0]
assert 20.0 <= s["start"] <= 20.9, s          # anchored on "called"
assert 21.4 <= s["end"] <= 22.1, s            # anchored on "acid."
assert s["image_index"] == 7
print("OK support spans (clean)")

# 2) v825.1: start_word/end_word are AUTHORITATIVE even when phrase DRIFTS.
# phrase leads with "is" (before the start_word) — old code would start the span
# at "is" (19.90); new code must anchor the span on "called" (20.30).
drift = [
    {"support_index": 2, "image_index": 7, "start_word": "called",
     "end_word": "acid", "phrase": "is called chlorogenic acid"},
]
d = vp.resolve_support_spans(master_words, drift)[0]
assert d is not None, d
assert d["start"] >= 20.2, f"span must start on 'called' (20.30), not drifted 'is' (19.90): {d}"
assert 21.4 <= d["end"] <= 22.1, d
print("OK support spans (start/end authoritative over drifted phrase)")

# 3) no phrase at all: pure start_word + end_word still resolves.
nophrase = [
    {"support_index": 3, "image_index": 8, "start_word": "studies",
     "end_word": "shown", "phrase": ""},
]
p = vp.resolve_support_spans(master_words, nophrase)[0]
assert p is not None, p
assert 22.0 <= p["start"] <= 22.3, p          # "Studies" 22.16
assert 22.8 <= p["end"] <= 23.3, p            # "shown" 22.80-23.20
print("OK support spans (start/end without phrase)")

print("ALL support-span checks pass")
