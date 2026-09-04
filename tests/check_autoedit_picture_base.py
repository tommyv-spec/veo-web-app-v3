"""v698A.2.5 — the auto-edit takes the cutaway edit (`final_broll_`) as its
PICTURE when nothing else is composited over the frame; audio stays the
export's. Pure rules tested directly; the plumbing asserted statically.

Run: python tests/check_autoedit_picture_base.py  (from code/)
"""
import os
import re
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import autoedit_pipeline as ap  # noqa: E402
from autoedit_qc import build_qc_report  # noqa: E402

BASE = Path("final_export_d74ab616_20260904_103908_86f08b.mp4")
CUT = Path("final_broll_d74ab616_20260904_103908_86f08b.mp4")
SUP = Path("support_track_16x9_d74ab616_20260904_103908_86f08b.mp4")
ON = {"pip_enabled": True}
OFF = {"pip_enabled": False}
SEGS = [(3.0, 8.0), (20.0, 24.0)]

# (a) cutaway edit present, no hook, no band -> the cutaway edit is the picture
assert ap.choose_picture(BASE, CUT, 0.0, ap.band_in_use_for(ON, [], None)) == (CUT, "final_broll")
# (b) an explicit MP4 hook background never enters this rule: the rule only sees cutaway_edit
assert ap.choose_picture(BASE, CUT, 0.0, ap.band_in_use_for(ON, [], None)) == (CUT, "final_broll")
# (c) support track present but PIP off -> band not in use -> cutaway edit
assert ap.band_in_use_for(OFF, SEGS, SUP) is False
assert ap.choose_picture(BASE, CUT, 0.0, ap.band_in_use_for(OFF, SEGS, SUP)) == (CUT, "final_broll")
# (d) support track present, PIP on, windows found -> band in use -> export
assert ap.band_in_use_for(ON, SEGS, SUP) is True
assert ap.choose_picture(BASE, CUT, 0.0, ap.band_in_use_for(ON, SEGS, SUP)) == (BASE, "export")
# (e) green hook present -> export
assert ap.choose_picture(BASE, CUT, 2.4, False) == (BASE, "export")
# (f) no cutaway edit at all -> export
assert ap.choose_picture(BASE, None, 0.0, False) == (BASE, "export")
# (g) only an UNMATCHED final_broll_ exists -> exact pick is None -> export; the hook pick still falls back
OUTS = ["final_broll_d74ab616_20260904_093057_f742de.mp4", "final_export_d74ab616_20260904_103908_86f08b.mp4"]
assert ap.pick_companion(OUTS, BASE.name, "final_broll_", exact=True) is None
assert ap.pick_companion(OUTS, BASE.name, "final_broll_") == "final_broll_d74ab616_20260904_093057_f742de.mp4"
OUTS2 = OUTS + [CUT.name]
assert ap.pick_companion(OUTS2, BASE.name, "final_broll_", exact=True) == CUT.name
print("OK rules (a)-(g)")

# cache key: unchanged without a picture fingerprint, suffixed with one
k0 = ap.compose_cache_key(1050, True, 0.10, 0.02, None, -20.0, "abcdef12", None)
assert k0 == "y1050_p1_k0.100_b0.020_mnone_-20.0_aabcdef12_hcoff", k0
k1 = ap.compose_cache_key(1050, True, 0.10, 0.02, None, -20.0, "abcdef12", None, picture_fp="0123beef")
assert k1 == k0 + "_pic0123beef", k1
print("OK cache key: Korella names unchanged; cutaway picture suffixed")

# QC report: top-level picture_source, defaulting to export
r = build_qc_report([{"id": "x", "status": "pass", "message": "m"}])
assert r["picture_source"] == "export" and r["verdict"] == "READY"
r = build_qc_report([{"id": "x", "status": "pass", "message": "m"}], picture_source="final_broll")
assert r["picture_source"] == "final_broll"
r = build_qc_report([], picture_source="garbage")
assert r["picture_source"] == "export"
print("OK qc report carries picture_source")

# static plumbing
src = open(os.path.join(ROOT, "autoedit_pipeline.py"), encoding="utf-8").read()
i_fetch = src.find("def fetch_job_files(")
i_prep = src.find("def prepare_composition(")
fetch_body = src[i_fetch:i_prep]
assert 'cutaway_edit_fn = pick_companion(outs, base_fn, "final_broll_", exact=True)' in fetch_body
assert fetch_body.find('cutaway_edit_fn = pick_companion') < fetch_body.find("if hook_bg_filename:"), \
    "the exact pick must happen BEFORE the hook_bg override"
assert "hook_bg_filename" not in fetch_body[fetch_body.find("cutaway_edit_fn ="):fetch_body.find("cutaway_edit_fn =") + 120]
assert "return base, sup, music, broll, cutaway_edit" in fetch_body
prep = src[i_prep:src.find("def resolve_hook_corner(")]
assert "base, sup, music, broll, cutaway_edit = fetch_job_files(" in prep
assert "picture, picture_source = choose_picture(" in prep
assert "band_in_use_for(repairs, segs, sup)" in prep
assert "detect_layout(picture, dur, segs)" in prep and 'detect_layout(base' not in prep
assert 's.get("picture") != picture.name' in prep and 's["picture"] = picture.name' in prep
assert re.search(r"nocap = compose\(\s*picture, sup, work", prep), "compose must take the picture as input 0"
assert 'picture_fp=(file_fingerprint(picture) if picture_source == "final_broll" else None)' in prep
assert "picture, _ = trim_media(picture," in prep
assert "return nocap, trimmed_dur, segs, auto_offset, pip_y, chin, picture, audio, picture_source" in prep
# audio still comes from the export
assert "enhance_audio(base, work" in prep and "enhance_audio(picture" not in prep
run = src[src.find("def run_autoedit("):]
assert "base, audio, picture_source = prepare_composition(" in run
assert "picture_source=picture_source)" in run
assert "def run_quality_checks(" in src and 'picture_source: str = "export"' in src
assert "return build_qc_report(checks, picture_source=picture_source)" in src
# the local tool unpacks the new tuple
tool = open(os.path.join(os.path.dirname(ROOT), "tools", "capcut_autoedit.py"), encoding="utf-8").read()
assert "_audio, _picture_source = prepare_composition(" in tool
print("OK plumbing: picture flows to layout, compose, trim, occupancy and the QC record; audio stays the export's")

print("ALL OK check_autoedit_picture_base")
