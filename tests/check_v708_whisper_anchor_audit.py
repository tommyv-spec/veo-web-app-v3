"""v773.1 — the whisper-anchor backend fills the v708/v709 word audit for real.

Before this fix the `ALIGN_MODE=whisper_anchor` branch called
`_whisper_anchor_trim` WITHOUT `return_details=True` and wrote constant zeros
into `v709_audit_sink` (`heard_words: 0, matched: 0, missing: [], trust: 1.0`).
Every export therefore reported `v708_audit_heard_words: 0` and the roll-up
logged `[v709-AUDIT] DEAF` — the missing-word audit was blind by construction.

Unit-tests the shared comparison helper and statically asserts the branch now
asks for details and no longer carries the constant-zero block.

Run: python tests/check_v708_whisper_anchor_audit.py  (from code/)
"""
import os
import re
import sys

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import video_processor as vp  # noqa: E402

# ---------------------------------------------------------------- unit tests
# (a) every script token heard verbatim -> all matched, nothing missing
assert vp._v708_match_script(
    ["these", "are", "the", "exact"],
    ["these", "are", "the", "exact"],
) == (4, [], 1.0)

# (b) a rare brand word the transcriber spelled differently.
#     Only assert "missing" when the real rapidfuzz ratio is under the 78
#     cut-off — compute it here instead of assuming.
TOKENS = ["these", "are", "garnissa's"]
HEARD = ["these", "are", "garnices"]
from rapidfuzz import fuzz as _fz  # noqa: E402

best = max(_fz.ratio("garnissa's", h) for h in HEARD)
matched, missing, trust = vp._v708_match_script(TOKENS, HEARD)
if best < 78:
    assert (matched, missing) == (2, ["garnissa's"]), (matched, missing)
    assert abs(trust - 2 / 3) < 1e-9, trust
else:
    assert (matched, missing) == (3, []), (matched, missing)
    assert trust == 1.0, trust
print(f"OK unit: best ratio for \"garnissa's\" vs heard = {best:.2f} "
      f"(cut-off 78) -> matched={matched} missing={missing}")

# (c) nothing to check -> vacuous full trust, never a divide-by-zero
assert vp._v708_match_script([], []) == (0, [], 1.0)
assert vp._v708_match_script([], ["something", "heard"]) == (0, [], 1.0)

# (d) heard nothing at all against a real script -> everything missing,
#     trust 0.0. This is the case the old constant-zero sink could never see.
m, miss, tr = vp._v708_match_script(["one", "two"], [])
assert (m, miss, tr) == (0, ["one", "two"], 0.0), (m, miss, tr)
print("OK unit: _v708_match_script(matched, missing, trust) on 4 cases")

# -------------------------------------------------------------- static checks
SRC = open(os.path.join(ROOT, "video_processor.py"), encoding="utf-8").read()

# the whisper_anchor branch: from `if _mode == "whisper_anchor":` up to the
# next backend branch (`if _mode == "ffmpeg":`)
start = SRC.index('if _mode == "whisper_anchor":')
end = SRC.index('if _mode == "ffmpeg":', start)
BRANCH = SRC[start:end]

assert "return_details=True" in BRANCH, \
    "whisper_anchor branch must ask _whisper_anchor_trim for details"
assert "_v708_match_script(" in BRANCH, \
    "whisper_anchor branch must run the shared script-vs-heard comparison"

# the constant-zero block is gone: `"heard_words": 0` followed by `"missing": []`
CONST_ZERO = re.compile(r'"heard_words":\s*0\s*,\s*\n\s*"missing":\s*\[\]')
assert not CONST_ZERO.search(BRANCH), \
    "whisper_anchor branch still writes the constant-zero audit block"
assert '"matched": 0' not in BRANCH, \
    "whisper_anchor branch still hard-codes matched=0"
assert '"trust": 1.0' not in BRANCH, \
    "whisper_anchor branch still hard-codes trust=1.0"

# the sink gets the real numbers
for _needed in ('"aligned_words": _wa_matched', '"matched": _wa_matched',
                '"heard_words": len(_wa_heard)', '"missing": _wa_missing'):
    assert _needed in BRANCH, f"sink is missing {_needed}"

# the wav handed over by return_details is cleaned up by this branch
assert "wav_path" in BRANCH and "unlink" in BRANCH, \
    "return_details hands the caller the temp wav — this branch must delete it"

# the diagnostic print shows the audit working in the Render log
assert "heard={len(_wa_heard)}" in BRANCH and "matched={_wa_matched}" in BRANCH, \
    "[Align/v773] line must report heard/matched so the log proves the audit ran"

# the action backend calls the same helper (behaviour unchanged, one code path)
ACT = SRC[SRC.index('"backend": "action"') - 3000:SRC.index('"backend": "action"')]
assert "_v708_match_script(" in ACT, \
    "action backend must use the shared helper"
# the old inlined copy is gone from the action backend
assert "_fz.ratio(t, h) >= 78" not in ACT, \
    "action backend still has the inlined comparison"
print("OK static: branch asks for details, fills real numbers, cleans the wav; "
      "action backend shares the helper")

# the helper keeps the exact threshold and the ImportError fallback
HELPER = SRC[SRC.index("def _v708_match_script("):]
HELPER = HELPER[:HELPER.index("\ndef ", 1)]
assert ">= 78" in HELPER, "threshold moved off 78"
assert "except ImportError" in HELPER, "rapidfuzz ImportError fallback dropped"
print("OK static: helper keeps threshold 78 + the ImportError fallback")

print("ALL OK check_v708_whisper_anchor_audit")
