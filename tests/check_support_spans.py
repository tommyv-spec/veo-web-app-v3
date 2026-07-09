import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json, video_processor as vp

master_words = json.load(open("tests/fixtures/support_master_words.json"))
support = [
    {"support_index": 1, "image_index": 7, "start_word": "called",
     "end_word": "acid", "phrase": "called chlorogenic acid"},
]
spans = vp.resolve_support_spans(master_words, support)
assert len(spans) == 1, spans
s = spans[0]
assert s is not None, "phrase should match"
assert 20.0 <= s["start"] <= 20.9, s
assert 21.4 <= s["end"] <= 22.1, s
assert s["image_index"] == 7
print("OK support spans")
