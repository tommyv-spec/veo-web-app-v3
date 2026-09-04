"""v960 — the burned banner and CTA card, declared by the build.

A second overlay engine beside v944's read-caption one: fixed pixels, fixed for
the whole video, read off the source frames. The constants come from
tools/fbads_burn_overlays.py, the tool that produced the accepted file.

The escaping check is pixel-exact rather than string-shaped on purpose. Four
different escapers were measured against a `textfile=` ground truth and every
one of them draws the WRONG glyphs for a string with an apostrophe, while
ffmpeg still exits 0 — so an assertion about backslashes in the filter string
would have passed on a broken renderer. The text now never enters the filter
string at all; the check is that the drawn frame matches the ground truth.

Run: python tests/check_v960_text_overlays.py   (from code/)
"""
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import autoedit_pipeline as ap  # noqa: E402

GARNISSA = [
    {"text": "Free", "y": 0.175, "size": 62},
    {"text": "Acrylic Painting Guide", "y": 0.222, "size": 62},
    {"text": "Get Yours Now", "y": 0.455, "size": 64, "from": 47.5},
    {"text": "Click The Link Below", "y": 0.505, "size": 64, "from": 47.5},
]

# -------------------------------------------------------------------- absent
assert ap.text_overlay_plan(None) is None
assert ap.text_overlay_plan([]) is None
assert ap.text_overlay_plan("") is None

# ------------------------------------------------------------------ rejected
BAD = [
    ([{"text": "x", "y": 0.4, "size": 62, "colour": "red"}], "colour"),
    ([{"text": "x", "y": 1.4, "size": 62}], "between 0.0 and 1.0"),
    ([{"text": "x", "y": 0.4, "size": 4}], "between 8 and 300"),
    ([{"text": "x", "y": 0.4, "size": 62, "from": 10, "until": 10}], "must be later than"),
    ([{"text": "x", "y": 0.4, "size": 62, "from": 10, "until": 5}], "must be later than"),
    ([{"text": "  ", "y": 0.4, "size": 62}], "non-empty string"),
    ([{"y": 0.4, "size": 62}], "non-empty string"),
    ([{"text": "x", "size": 62}], "fraction of frame height"),
    ([{"text": "x", "y": 0.4}], "whole number of pixels"),
    ([{"text": "x", "y": 0.4, "size": 62, "from": -1}], "cannot be negative"),
    ({"text": "x"}, "must be a list"),
    (["Free"], "must be an object"),
]
for value, needle in BAD:
    try:
        ap.text_overlay_plan(value)
        raise AssertionError(f"{value!r} must be rejected")
    except ap.AutoEditError as exc:
        assert needle in str(exc), f"{value!r}: {exc}"
print(f"OK validation: absent means none; {len(BAD)} bad shapes rejected, each "
      f"message naming the fault")

# -------------------------------------------------------------- the defaults
plan = ap.text_overlay_plan(GARNISSA)
assert len(plan) == 4
assert plan[0] == {"text": "Free", "y": 0.175, "size": 62, "from": 0.0, "until": None}
assert plan[2]["from"] == 47.5 and plan[2]["until"] is None

# ------------------------------------------------------------ the filter string
vf = ap.text_overlay_filter(plan, [f"t{i}.txt" for i in range(4)])
for needle in ("h*0.175", "h*0.222", "h*0.455", "h*0.505",
               "fontsize=62", "fontsize=64", "x=(w-text_w)/2",
               "enable='gte(t,47.5)'", "fontcolor=white",
               "shadowcolor=black@0.55:shadowx=3:shadowy=3",
               "borderw=1:bordercolor=black@0.35",
               "Montserrat-ExtraBold.ttf", "expansion=none"):
    assert needle in vf, needle
assert vf.count("drawtext=") == 4 and vf.count("enable=") == 2, vf
# the banner is on from the first frame: no `enable` on it at all
assert "enable" not in vf.split(",")[0]
# a window declares both ends
win = ap.text_overlay_filter(
    ap.text_overlay_plan([{"text": "x", "y": 0.4, "size": 40, "from": 2, "until": 5}]),
    ["t.txt"])
assert "enable='between(t,2.0,5.0)'" in win, win
until_only = ap.text_overlay_filter(
    ap.text_overlay_plan([{"text": "x", "y": 0.4, "size": 40, "until": 5}]), ["t.txt"])
assert "enable='lt(t,5.0)'" in until_only, until_only
# the text itself NEVER reaches the filter string — there is no separator left
# in it that could break the chain
for item in GARNISSA:
    assert item["text"] not in vf, item["text"]
print("OK filter: the tool's constants verbatim, the CTA gated from 47.5s, and "
      "no overlay text inside the filter string")

# ------------------------------------------- the pixels, against a ground truth
if not shutil.which("ffmpeg") or not Path(ap.TEXT_OVERLAY_FONT).exists():
    print("SKIP pixels: ffmpeg or the Montserrat font is not on this machine")
    print("v960 text overlays: ALL OK (pixel check skipped)")
    raise SystemExit(0)

import numpy as np  # noqa: E402
from PIL import Image  # noqa: E402

NASTY = ["Free", "Get Yours Now", "it's 50%: now", "Don't Miss Out",
         "a, b [c]; d=e", "back\\slash"]
tmp = Path(tempfile.mkdtemp(prefix="v960_overlay_"))
try:
    for raw in NASTY:
        one = ap.text_overlay_plan([{"text": raw, "y": 0.4, "size": 48}])
        tf = tmp / "given.txt"
        tf.write_text(raw, encoding="utf-8")
        # ground truth: the same drawtext, with the text handed over in a file
        # written by this test rather than by the renderer
        truth = ("drawtext=textfile='" + str(tf).replace("\\", "/").replace(":", r"\:")
                 + "':fontfile='" + ap.TEXT_OVERLAY_FONT.replace("\\", "/").replace(":", r"\:")
                 + "':" + ap.TEXT_OVERLAY_STYLE
                 + ":fontsize=48:x=(w-text_w)/2:y=h*0.4:expansion=none")
        frames = {}
        for name, filt in (("truth", truth),
                           ("built", ap.text_overlay_filter(one, [tf]))):
            dest = tmp / f"{name}.png"
            r = subprocess.run(
                ["ffmpeg", "-v", "error", "-f", "lavfi",
                 "-i", "color=c=black:s=1400x400:d=0.2", "-vf", filt,
                 "-frames:v", "1", "-y", str(dest)],
                capture_output=True, text=True, encoding="utf-8", errors="replace")
            assert r.returncode == 0, f"{raw!r} {name}: {r.stderr[:200]}"
            frames[name] = np.array(Image.open(dest).convert("L"))
        assert frames["truth"].max() > 0, f"{raw!r}: nothing was drawn"
        assert (frames["truth"] == frames["built"]).all(), \
            f"{raw!r}: the built filter drew different pixels than the ground truth"
    print(f"OK pixels: {len(NASTY)} strings drawn identically to the ground "
          f"truth, apostrophes / colons / percent signs included")

    # the renderer says which font is missing rather than letting ffmpeg guess
    saved = ap.TEXT_OVERLAY_FONT
    ap.TEXT_OVERLAY_FONT = str(tmp / "no-such-font.ttf")
    try:
        ap.render_text_overlays(tmp / "in.mp4", tmp / "out.mp4", plan)
        raise AssertionError("a missing font must fail loudly")
    except ap.AutoEditError as exc:
        assert "font is missing" in str(exc) and "no-such-font.ttf" in str(exc), exc
    finally:
        ap.TEXT_OVERLAY_FONT = saved
    print("OK missing font: named in the message, not an ffmpeg filter error")
finally:
    shutil.rmtree(tmp, ignore_errors=True)
print("v960 text overlays: ALL OK")
