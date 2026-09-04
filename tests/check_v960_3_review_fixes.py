"""v960.3 — the three defects a post-deploy review pass found in the v960 set.

1. The drawtext filter carried a full directory path, so a work directory with
   an apostrophe in it (`C:\\Users\\O'Brien\\...`) broke the whole filter. No
   escaping fixes it — measured, see `render_text_overlays` — so the filter now
   names files by BARE BASENAME with ffmpeg's cwd set.
2. `prepare_fixed_transcript` deleted its throwaway probe video only on the
   success path, leaking a full-size mp4 whenever a caption render raised.
3. `_autoedit_caption_template_files` trusted every directory it listed. `..`
   was NOT the hole the reviewer suspected (iterdir yields no such entry and
   the filesystem refuses the name), but a symlink is, so containment is now
   checked rather than assumed.

Run: python tests/check_v960_3_review_fixes.py   (from code/)
"""
import os
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, str(ROOT))

import autoedit_pipeline as ap  # noqa: E402

# ---- 1. no directory path may reach the filter -----------------------------
plan = ap.text_overlay_plan([
    {"text": "Free", "y": 0.175, "size": 62},
    {"text": "Get Yours Now", "y": 0.455, "size": 64, "from": 47.5},
])
vf = ap.text_overlay_filter(plan, [Path("text_overlay_0.txt"), Path("text_overlay_1.txt")],
                            font_name="Montserrat-ExtraBold.ttf")
assert "/" not in vf.replace("(w-text_w)/2", ""), "a path separator reached the filter"
assert "\\" not in vf, "a backslash reached the filter"
assert "textfile='text_overlay_0.txt'" in vf and "fontfile='Montserrat-ExtraBold.ttf'" in vf
assert "h*0.175" in vf and "fontsize=62" in vf and "enable='gte(t,47.5)'" in vf
print("OK filter: bare basenames only, positions and timing unchanged")

# the helper stays honest about what it can do: it quotes and escapes a colon,
# and it must NOT pretend to handle an apostrophe (every escape was measured
# and every one drew nothing)
assert ap._drawtext_path("C:\\a\\b.ttf") == "'C\\:/a/b.ttf'"
assert r"'\''" not in ap._drawtext_path("it's.txt"), \
    "the POSIX quote dance parses but makes ffmpeg open the wrong file — do not reintroduce it"
print("OK _drawtext_path: colon escaped, no apostrophe theatre")

src = open(ROOT / "autoedit_pipeline.py", encoding="utf-8").read()
rto = src[src.find("def render_text_overlays("):]
rto = rto[:rto.find("\ndef ")]
assert "cwd=str(work)" in rto, "ffmpeg must run with its cwd set to the file directory"
assert "[Path(t.name) for t in textfiles]" in rto, "basenames, not full paths"
assert "font_name=font_local.name" in rto
assert "shutil.copy2(font, font_local)" in rto, "the font must sit beside the text files"
assert "if font_copied:" in rto and "font_local.unlink(missing_ok=True)" in rto, \
    "a copied font must be cleaned up — and only when this call copied it"
assert "Path(video_in).resolve()" in rto and "Path(video_out).resolve()" in rto, \
    "argv paths stay absolute; only filter-borne names become relative"
print("OK renderer: cwd set, font staged and cleaned, argv paths still absolute")

# ---- 2. the probe video is deleted even when the render raises -------------
pft = src[src.find("def prepare_fixed_transcript("):]
pft = pft[:pft.find("\ndef ")]
i_try = pft.find("        try:\n            render_captions(")
i_fin = pft.find("finally:", i_try)
i_unlink = pft.find("probe.unlink(missing_ok=True)", i_fin)
assert i_try != -1, "the probe render is not inside a try"
assert i_fin != -1 and i_unlink != -1, "no finally deletes the probe"
assert i_fin < i_unlink, "the unlink must be inside the finally"
print("OK probe: deleted in a finally, so a failed caption render leaks nothing")

# ---- 3. template directories are contained, and `..` was never the hole ----
import tempfile  # noqa: E402
with tempfile.TemporaryDirectory() as td:
    d = Path(td)
    (d / "sub").mkdir()
    names = {p.name for p in d.iterdir()}
    assert "." not in names and ".." not in names, \
        "iterdir would have to yield '..' for the reported traversal to exist"
try:
    (Path(td := tempfile.mkdtemp()) / "..").mkdir()
    raise AssertionError("the filesystem accepted a directory named '..'")
except OSError:
    pass
print("OK the reported '..' traversal is not reachable — iterdir and the OS both refuse it")

main_src = open(ROOT / "main.py", encoding="utf-8").read()
fn = main_src[main_src.find("def _autoedit_caption_template_files("):]
fn = fn[:fn.find("\n@app.")]
assert "code_dir = Path(__file__).parent.resolve()" in fn
assert "if code_dir not in d.resolve().parents:" in fn, \
    "a symlink out of caption_templates must be refused, not served"
assert "except OSError:" in fn, "a broken link must be skipped, not crash the route"
print("OK templates: containment checked against the resolved code dir")

print("ALL OK check_v960_3_review_fixes")
