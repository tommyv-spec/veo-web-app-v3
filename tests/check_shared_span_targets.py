"""v698A many-to-one — pin the CONTRACT the export's sub-span split depends on.

main.py builds its b-roll targets from pairing_resolver.split_span. If someone
later changes that function, the export silently starts stacking sharers on one
window again - which is the exact bug this whole change exists to remove. These
assertions are what main.py assumes.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pairing_resolver import split_span  # noqa: E402

# Three visuals share one spoken clip whose speaker-concat window is 10.0-19.0s.
spans = split_span(10.0, 19.0, ["short", "a much longer fragment here", "mid one"])
assert len(spans) == 3, spans
assert spans[0][0] == 10.0, spans
assert spans[-1][1] == 19.0, spans
for (a0, a1), (b0, b1) in zip(spans, spans[1:]):
    assert a1 == b0, f"windows must be contiguous: {a1} != {b0}"
    assert a0 < a1, f"window must be non-empty: {spans}"
# the longest fragment gets the widest window
widths = [b - a for a, b in spans]
assert widths[1] == max(widths), widths

# A group of ONE takes the whole window - this is why the export needs no
# special case for a single sharer, and why every non-empty group is treated
# as covering its source.
assert split_span(4.0, 7.5, ["only one"]) == [(4.0, 7.5)]

# No sharer window ever escapes its parent.
for a, b in split_span(2.0, 3.0, ["a", "b", "c", "d"]):
    assert 2.0 <= a < b <= 3.0, (a, b)

print("check_shared_span_targets: OK")
