"""v960 — the transcript is corrected BEFORE the captions are drawn.

pycaps mishears the brand and capitalises like a sentence. The build declares
`autoedit_caption_case` and `autoedit_caption_words`; the run harvests the raw
transcript with one throwaway probe pass, applies the fixes, and seeds every
real pass with the corrected file. These checks pin the word walk (which is
tools/fbads_caption_case.py's, moved into the pipeline) and assert statically
that the caption stage really seeds `subtitle_data=`.

Run: python tests/check_v960_caption_fixes.py   (from code/)
"""
import os
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import autoedit_pipeline as ap  # noqa: E402


def doc(*texts):
    return {"segments": [{"lines": [{"words": [{"text": t} for t in texts]}]}]}


# ------------------------------------------------------------------ the plan
assert ap.caption_fix_plan(None, None) is None
assert ap.caption_fix_plan("", {}) is None
assert ap.caption_fix_plan("lower", None) == {"case": "lower", "words": {}}
assert ap.caption_fix_plan(None, {"Garnices": "Garnissa's"}) == \
    {"case": None, "words": {"garnices": "Garnissa's"}}
try:
    ap.caption_fix_plan("upper", None)
    raise AssertionError("caption_case 'upper' must be rejected")
except ap.AutoEditError as exc:
    assert "caption_case must be 'lower'" in str(exc), exc
try:
    ap.caption_fix_plan(None, ["garnices"])
    raise AssertionError("a list of words must be rejected")
except ap.AutoEditError as exc:
    assert "caption_words must be an object" in str(exc), exc
print("OK plan: None when nothing is declared; map only and case only both work")

# ------------------------------------------------------------- the word walk
LOWER_AND_MAP = ap.caption_fix_plan("lower", {"garnices": "Garnissa's",
                                              "nora": "Nora"})
fixed, changed = ap.apply_caption_fixes(
    doc("Garnices", "Hello,", "I'm", "(Nora)", "MUDDY"), LOWER_AND_MAP)
assert ap.caption_words_of(fixed) == \
    ["Garnissa's", "hello,", "I'm", "(Nora)", "muddy"], ap.caption_words_of(fixed)
# "(Nora)" already reads Nora, so it is not a CHANGE — three words moved.
assert changed == 3, changed

# the map alone leaves every word it does not name exactly as it was
MAP_ONLY = ap.caption_fix_plan(None, {"garnices": "Garnissa's"})
fixed, changed = ap.apply_caption_fixes(
    doc("Garnices", "Hello,", "I'm", "MUDDY"), MAP_ONLY)
assert ap.caption_words_of(fixed) == ["Garnissa's", "Hello,", "I'm", "MUDDY"]
assert changed == 1, changed

# the case rule alone keeps the I-forms; a build does not have to list them
CASE_ONLY = ap.caption_fix_plan("lower", None)
fixed, _ = ap.apply_caption_fixes(
    doc("I", "I'm", "I've", "I'd", "I'll", "Hello,", '"Quoted"'), CASE_ONLY)
assert ap.caption_words_of(fixed) == \
    ["I", "I'm", "I've", "I'd", "I'll", "hello,", '"quoted"'], ap.caption_words_of(fixed)

# the input document is never touched
src = doc("Garnices")
out, _ = ap.apply_caption_fixes(src, LOWER_AND_MAP)
assert src["segments"][0]["lines"][0]["words"][0]["text"] == "Garnices"
assert out is not src

# no plan = the document straight back, nothing counted
same, n = ap.apply_caption_fixes(src, None)
assert same is src and n == 0

# punctuation the tool's two edge sets do not know must not be DROPPED
assert ap.fix_caption_word("—Garnices…", LOWER_AND_MAP) == "—Garnissa's…"
assert ap.fix_caption_word("Hello,", CASE_ONLY) == "hello,"
assert ap.fix_caption_word("", CASE_ONLY) == ""
print("OK word walk: map wins, then the lower rule, I-forms kept, edge "
      "punctuation preserved, counts right")

# the digest names everything baked into the fixed transcript
d1 = ap.caption_fix_digest(LOWER_AND_MAP)
d2 = ap.caption_fix_digest(ap.caption_fix_plan("lower", {"garnices": "Garnissa"}))
assert d1 != d2 and len(d1) == 8, (d1, d2)
assert d1 == ap.caption_fix_digest(ap.caption_fix_plan(
    "lower", {"nora": "Nora", "GARNICES": "Garnissa's"})), "digest must be order-blind"
print("OK digest: keyed on the fixes, stable, order-blind")

# ------------------------------------------------------------------- statics
src = Path(ROOT, "autoedit_pipeline.py").read_text(encoding="utf-8")
body = src[src.index("def run_autoedit("):]
assert body.count("_seed = prepare_fixed_transcript(") == 2, \
    "both caption branches must build the seed"
assert body.count("_render_caption_pass(") == 2
assert body.count("subtitle_data=_seed") == 2, \
    "every caption pass must be seeded with the CORRECTED transcript"
assert "_cap_plan = caption_fix_plan(" in body
# a pass burned from the raw transcript can never be reused for a corrected one
dyn = src[src.index("def render_captions_dynamic("):]
dyn = dyn[:dyn.index("\ndef ")]
assert 'src_key = f"{src_key}_t{file_fingerprint(first_data)}"' in dyn, \
    "the pass name must carry the seeded transcript's fingerprint (§v938.1)"
print("OK static: both caption branches seed subtitle_data, and the pass name "
      "carries the transcript fingerprint")
print("v960 caption fixes: ALL OK")
