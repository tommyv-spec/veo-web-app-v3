"""v960 — the ✂️ Auto-Edit card obeys the build's declared `autoedit_*` block.

Before this, `_queue_autoedit_impl` folded THREE keys into a side dict and every
other read came off the unfolded request, so a declared field was applied only
by the auto-finish chain and silently ignored on a card click. These checks are
the proof that the fold now reaches `normalize_repairs`, that it runs before the
placement/offset handling (so a declared offset is range-checked instead of
sailing through to the renderer), and that a job declaring nothing is untouched.

Run: python tests/check_v960_declaration_fold.py   (from code/)
"""
import json
import os
import re
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import autoedit_qc  # noqa: E402
import main  # noqa: E402
from finishing_models import AutoEditRequest  # noqa: E402
from fastapi import HTTPException  # noqa: E402

derive = main.derive_autoedit_defaults

# ------------------------------------------------------- the repairs contract
# A job that declares none of the new fields must reach the worker with exactly
# the settings it reached it with before v960. The pre-v960 dict is written out
# here rather than derived, so a future edit to DEFAULT_REPAIRS cannot quietly
# agree with itself.
PRE_V960 = {
    "trim_start_s": 0.0, "trim_end_s": 0.0, "pip_enabled": True,
    "captions_enabled": True, "chroma_similarity": 0.10, "chroma_blend": 0.02,
    "music_filename": None, "music_db": -20.0, "audio_enhance": "voice",
    "hook_corner": None, "hook_bg": None, "overlay_spec": None,
}
empty = autoedit_qc.normalize_repairs()
for key, value in PRE_V960.items():
    assert empty[key] == value, (key, empty[key], value)
# ...and the ONLY thing v960 adds is three keys, all off.
assert set(empty) - set(PRE_V960) == {"caption_case", "caption_words", "text_overlays"}
assert all(empty[k] is None for k in ("caption_case", "caption_words", "text_overlays"))
print("OK repairs: every pre-v960 key keeps its value; the three new keys "
      "default to None")

# ---------------------------------------------------------------- the helper
# (a) the regression contract: a job that declared nothing is untouched apart
# from the one overlay_spec line that has always been there.
base = {"template": "korella", "captions_enabled": True, "overlay_spec": None}
assert derive(dict(base), None, set()) == base
assert derive({"template": "korella", "captions_enabled": True}, None, set()) == base
# an EXPLICIT overlay on a spec-less job is not eaten (the v944 pilot re-finish)
explicit = {"template": "korella", "captions_enabled": True, "overlay_spec": {"overlay": "readcaption"}}
assert derive(dict(explicit), None, {"overlay_spec"}) == explicit

# (b) a declared autoedit block folds when nothing was explicit
spec = {"captions": "none", "overlay": "none",
        "autoedit": {"audio_enhance": "off", "caption_case": "lower"}}
got = derive({"template": "korella", "captions_enabled": True}, spec, set())
assert got["audio_enhance"] == "off", got
assert got["caption_case"] == "lower", got

# (c) ...and folds NEITHER when both were sent explicitly
got = derive({"template": "korella", "captions_enabled": True,
              "audio_enhance": "voice", "caption_case": None},
             spec, {"audio_enhance", "caption_case"})
assert got["audio_enhance"] == "voice", got
assert got["caption_case"] is None, got

# (d) captions:/overlay: still derive exactly as they did
spec2 = {"captions": "garnissa", "overlay": "none"}
got = derive({"template": "korella", "captions_enabled": True}, spec2, set())
assert got["template"] == "garnissa" and got["captions_enabled"] is True, got
spec3 = {"captions": "none", "overlay": "readcaption", "overlay_age": "I'M 74"}
got = derive({"template": "korella", "captions_enabled": True}, spec3, set())
assert got["captions_enabled"] is False and got["overlay_spec"] == spec3, got
print("OK helper: regression contract, autoedit fold, captions/overlay derive")


# ------------------------------------------------------------ the queue path
class _Query:
    """Enough SQLAlchemy shape for _queue_autoedit_impl, and nothing else."""

    def __init__(self, first=None, rows=()):
        self._first, self._rows = first, list(rows)

    def filter(self, *a, **k):
        return self

    def order_by(self, *a, **k):
        return self

    def with_for_update(self, *a, **k):
        return self

    def first(self):
        return self._first

    def __iter__(self):
        return iter(self._rows)


class _FakeJob:
    def __init__(self, spec):
        self.id = "d74ab616-ab21-4054-b121-a386fc2d823b"
        self.user_id = "u1"
        self.finishing_spec = json.dumps(spec) if spec is not None else None


class _FakeExport:
    state = "done"


class _FakeDB:
    def __init__(self, job):
        self.job = job
        self.added = None

    def query(self, model):
        name = getattr(model, "__name__", str(model))
        if name == "AutoEditRun":
            return _Query(first=None, rows=[])
        if name == "ExportRun":
            return _Query(first=_FakeExport())
        return _Query(first=self.job)

    def add(self, row):
        self.added = row

    def commit(self):
        pass


DECLARED = {
    "captions": "garnissa",
    "overlay": "none",
    "autoedit": {
        "audio_enhance": "off",
        "caption_case": "lower",
        "caption_words": {"garnices": "Garnissa's"},
        "text_overlays": [{"text": "Free", "y": 0.175, "size": 62,
                           "from": 0.0, "until": None}],
    },
}


def queue(spec, req):
    """Call the real impl with a fake db, capturing what reaches normalize_repairs."""
    seen = {}
    real = autoedit_qc.normalize_repairs

    def spy(value=None):
        out = real(value)
        seen.update(out)
        return out

    autoedit_qc.normalize_repairs = spy
    try:
        job = _FakeJob(spec)
        run = main._queue_autoedit_impl(_FakeDB(job), job, req, "u1")
    finally:
        autoedit_qc.normalize_repairs = real
    return run, seen


# (e) the declaration reaches normalize_repairs on a request that sent none of
# it — including `captions: garnissa`, which only lands because the card did not
# name a template either.
run, seen = queue(DECLARED, AutoEditRequest(placement="dynamic"))
assert seen["audio_enhance"] == "off", seen["audio_enhance"]
assert seen["caption_case"] == "lower", seen["caption_case"]
assert seen["caption_words"] == {"garnices": "Garnissa's"}, seen["caption_words"]
assert seen["text_overlays"] == [{"text": "Free", "y": 0.175, "size": 62,
                                  "from": 0.0, "until": None}], seen["text_overlays"]
assert run.template == "garnissa", run.template

# (f) ...and the REQUEST wins wherever it spoke
run, seen = queue(DECLARED, AutoEditRequest(
    template="korella", audio_enhance="voice", caption_case=None,
    caption_words=None, text_overlays=None))
assert seen["audio_enhance"] == "voice", seen["audio_enhance"]
assert seen["caption_case"] is None, seen["caption_case"]
assert seen["caption_words"] is None, seen["caption_words"]
assert seen["text_overlays"] is None, seen["text_overlays"]
assert run.template == "korella", run.template

# (g) a job that declared nothing still reaches normalize_repairs with today's
# values — the regression contract, on the real path
run, seen = queue(None, AutoEditRequest())
assert seen["caption_case"] is None and seen["caption_words"] is None
assert seen["text_overlays"] is None and seen["audio_enhance"] == "voice"
assert seen["overlay_spec"] is None and run.template == "korella"

# (h) a DECLARED placement is honoured on a manual request
run, _ = queue({"captions": "none", "overlay": "none",
                "autoedit": {"placement": "constant"}},
               AutoEditRequest(template="korella"))
assert run.placement == "constant", run.placement

# (i) a DECLARED offset is validated, not waved through
try:
    queue({"captions": "none", "overlay": "none", "autoedit": {"offset": 0.9}},
          AutoEditRequest(template="korella"))
    raise AssertionError("a declared offset of 0.9 must be rejected")
except HTTPException as exc:
    assert exc.status_code == 400 and "between -0.45 and 0.45" in exc.detail, exc.detail

# ...and an in-range declared offset forces constant placement, exactly as an
# explicitly-sent one does
run, _ = queue({"captions": "none", "overlay": "none", "autoedit": {"offset": 0.2}},
               AutoEditRequest(template="korella"))
assert run.placement == "constant" and run.offset == 0.2, (run.placement, run.offset)
print("OK queue path: declaration reaches normalize_repairs, request wins, "
      "placement/offset honoured and range-checked")


# ------------------------------------------------------------------- statics
src = Path(ROOT, "main.py").read_text(encoding="utf-8")
start = src.index("def _queue_autoedit_impl(")
end = src.index("\n@app.post(\"/api/jobs/{job_id}/autoedit\")", start)
body = src[start:end]

i_derive = body.index("derive_autoedit_defaults(")
assert "AutoEditRequest(**" in body, \
    "_queue_autoedit_impl must REBUILD the request from the folded dict — " \
    "patching a few variables leaves every other read on the unfolded one"
i_rebuild = body.index("AutoEditRequest(**")
i_placement = body.index('placement = "constant" if req.offset is not None')
i_offset = body.index("Caption offset must be between -0.45 and 0.45")
i_hook = body.index("hook_corner_req, hook_bg_req = req.hook_corner")
assert i_derive < i_rebuild < i_placement < i_offset < i_hook, \
    (i_derive, i_rebuild, i_placement, i_offset, i_hook)
# every field the repairs dict reads must come off the rebuilt request
repairs_block = body[body.index("repairs = normalize_repairs("):]
repairs_block = repairs_block[:repairs_block.index("})")]
for field in ("caption_case", "caption_words", "text_overlays"):
    assert f'"{field}": req.{field}' in repairs_block, field
print("OK static: fold rebuilds the request, and runs before placement, the "
      "offset range check and the hook-layout inheritance")

# The card's payload is the measured blast radius. If someone adds a field here,
# the fold newly reaches one fewer field and §1 of the plan is out of date.
html = Path(ROOT, "static", "index.html").read_text(encoding="utf-8")
# index.html has four `const payload = {` literals; the auto-edit one is the
# only one that names the green-key strength.
blocks = [b for b in re.findall(r"const payload = \{(.*?)\n\s*\};", html, re.S)
          if "chroma_similarity" in b]
assert len(blocks) == 1, f"expected one auto-edit payload literal, found {len(blocks)}"
sent = re.findall(r"^\s+(\w+):", blocks[0], re.M)
EXPECTED = ["template", "placement", "offset", "trim_start_s", "trim_end_s",
            "pip_enabled", "captions_enabled", "chroma_similarity",
            "chroma_blend", "music_filename", "music_db"]
assert sent == EXPECTED, (
    f"the ✂️ Auto-Edit card now sends {sent}, not {EXPECTED} — the measured "
    f"blast radius of the v960 fold changed, so re-measure it before shipping")
print(f"OK static: the card still sends exactly {len(EXPECTED)} explicit fields")
print("v960 declaration fold: ALL OK")
