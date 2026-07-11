"""v830 — cross-scene end-frame interpolation is EXPLICIT opt-in (`transition:
blend`), never "anything that isn't the string 'cut'".

Every server-side blend-decision site used `get("transition"...) != "cut"`.
That fired on `None`, `""`, and the literal string `"null"` (the markdown
parser stores `- **transition:** null` as "null", not None). So a clean
fresh/cut build got a spurious end frame binding the NEXT scene's image onto a
clip whenever a boundary transition wasn't the exact string "cut" — the operator
saw start+end frame pairs on clips that should have had a start frame only.

v782 declared blend explicit-opt-in for `clip_mode`, but these three sites kept
the `!= "cut"` transition test. v830 flips them to `== "blend"`. Zero shipped
builds use `transition: blend` (all 1409 transition bullets are cut/null; real
morphs use `end_frame_image`, the v718i path), so nothing loses intended
behavior.

Source-level guards: the blend decision is inline in ~200-line render loops and
cannot be imported in isolation. These fail loudly if the `!= "cut"` predicate
is reintroduced at any blend site.
"""
import ast
import os
import re

CODE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _read(name):
    with open(os.path.join(CODE, name), encoding="utf-8") as fh:
        return fh.read()


# 1) No blend-decision site may test `transition ... != "cut"`. We look for a
#    get("transition"...) immediately compared with != "cut" (comments naming
#    the old predicate are fine — they don't call .get()).
BLEND_ANTIPATTERN = re.compile(r'get\(\s*["\']transition["\'].*?\)\s*!=\s*["\']cut["\']')
for fname in ("worker.py", "main.py", "image_platform.py"):
    src = _read(fname)
    hits = BLEND_ANTIPATTERN.findall(src)
    assert not hits, (
        f"{fname}: a blend site still uses `get('transition') != 'cut'` "
        f"({len(hits)} hit(s)) — v830 requires `== 'blend'` so None/''/'null' "
        f"don't trigger a spurious cross-scene end frame"
    )
print("OK v830: no `get('transition') != 'cut'` blend predicate remains")


# 2) The three known cross-scene blend sites must test `<transition> == "blend"`.
#    The predicate appears two ways: `get("transition") == "blend"` and, via a
#    local, `next_transition == "blend"`. Count lines that compare a
#    transition value to "blend" (excluding the separate `clip_mode == "blend"`).
def _blend_optin_lines(src):
    out = []
    for ln in src.splitlines():
        s = ln.split("#", 1)[0]  # ignore comments
        if "clip_mode" in s:
            continue
        if re.search(r'transition["\')\s]*\)?\s*==\s*["\']blend["\']', s):
            out.append(ln.strip())
    return out

worker_hits = len(_blend_optin_lines(_read("worker.py")))
main_hits = len(_blend_optin_lines(_read("main.py")))
assert worker_hits >= 2, (
    f"worker.py: expected >=2 `transition == 'blend'` blend sites (the two "
    f"storyboard branches), found {worker_hits}"
)
assert main_hits >= 1, (
    f"main.py: expected >=1 `transition == 'blend'` blend site, found {main_hits}"
)
print(f"OK v830: explicit `== 'blend'` opt-in at all sites "
      f"(worker {worker_hits}, main {main_hits})")


# 3) The files still parse (a botched multi-line edit would break the render
#    loop silently until runtime).
for fname in ("worker.py", "main.py"):
    ast.parse(_read(fname))
print("OK v830: worker.py + main.py still parse")


# 4) v830.1 — the frontend (static/index.html) must not DEFAULT clip_mode or
#    transition to 'blend'. The backend fix only gates None/""/"null"; a payload
#    or state builder that fabricates the literal string 'blend' would still be
#    honored (blend is explicit opt-in). Every fallback default and every
#    hardcoded scene-creation default must be fresh/cut. Explicit user actions
#    (the cut<->blend toggle, the setClipMode('blend') button, the `=== 'blend'`
#    display checks) legitimately keep the word and use different syntax.
index_html = _read(os.path.join("static", "index.html"))
FE_ANTIPATTERNS = [
    (r"\|\|\s*'blend'", "a `|| 'blend'` fallback default (should be 'fresh'/'cut')"),
    (r"transition:\s*'blend'", "a `transition: 'blend'` object default (should be 'cut')"),
    (r"clipMode:\s*'blend'", "a `clipMode: 'blend'` object default (should be 'fresh')"),
    (r"\.clipMode\s*=\s*'blend'", "a `.clipMode = 'blend'` default assignment (should be 'fresh')"),
    (r"let\s+sceneTransition\s*=\s*'blend'", "a `let sceneTransition = 'blend'` init (should be 'cut')"),
    (r"let\s+clipMode\s*=\s*'blend'", "a `let clipMode = 'blend'` init (should be 'fresh')"),
]
for pat, desc in FE_ANTIPATTERNS:
    hits = re.findall(pat, index_html)
    assert not hits, (
        f"static/index.html: {len(hits)} occurrence(s) of {desc} — v830.1 "
        f"requires fresh/cut defaults so the editor never POSTs a fabricated "
        f"'blend' the backend then honors"
    )
print("OK v830.1: no clip_mode/transition defaults to 'blend' in the frontend")

print("ALL v830 transition-blend-optin checks pass")
