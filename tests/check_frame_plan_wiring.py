"""v698A.2.2 — static wiring checks: the assembler really is driven by frames.

frame_plan itself is unit-tested in tests/check_frame_plan.py. This file checks
that the plan is actually WIRED: that the fitter is asked for exact frames, that
every produced segment is measured and repaired-or-raised, that the concat is
held to the same budget, and that a master-aligned b-roll can never take the
xfade path (an xfade overlaps adjacent clips and moves every cut).

Run: python tests/check_frame_plan_wiring.py   (from code/)
"""
import inspect
import os
import re
import sys

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import video_processor as vp  # noqa: E402

vp_src = open(os.path.join(ROOT, "video_processor.py"), encoding="utf-8").read()
main_src = open(os.path.join(ROOT, "main.py"), encoding="utf-8").read()

fit_src = inspect.getsource(vp.process_clip_for_alignment)
blk_src = inspect.getsource(vp._generate_black_video)
asm_src = inspect.getsource(vp.export_with_master_audio)
cat_src = inspect.getsource(vp.concat_videos)

# --- 1. the signatures: every new argument is OPTIONAL --------------------
sig_fit = inspect.signature(vp.process_clip_for_alignment)
assert "exact_frames" in sig_fit.parameters, sig_fit
assert sig_fit.parameters["exact_frames"].default is None, sig_fit
sig_blk = inspect.signature(vp._generate_black_video)
assert "frames" in sig_blk.parameters and sig_blk.parameters["frames"].default is None
sig_cat = inspect.signature(vp.concat_videos)
assert "expect_frames" in sig_cat.parameters
assert sig_cat.parameters["expect_frames"].default is None
assert callable(vp.frame_plan) and callable(vp.probe_frame_count)
print("OK 1: exact_frames / frames / expect_frames all optional, default to today")

# --- 2. the fitter ends on frames when asked, on seconds when not ---------
assert '"-frames:v", str(int(exact_frames))' in fit_src, fit_src[:0]
assert '"-t", f"{target_duration:.6f}"' in fit_src, "the seconds rule must survive for the legacy path"
# the shared length option, and its four users (speed / speed_and_trim /
# slowdown / the loop branch's final trim)
assert "_len_opt = (" in fit_src
assert fit_src.count("*_len_opt,") == 3, fit_src.count("*_len_opt,")
assert fit_src.count('*(["-frames:v", str(int(exact_frames))] if exact_frames else [])') == 1
# every ffmpeg command in the fitter carries exactly one length option
cmds = re.findall(r"cmd(?:_rev|_concat)? = \[\n(.*?)\n\s*\]", fit_src, re.S)
assert len(cmds) >= 5, len(cmds)
enders = [c for c in cmds if "_len_opt" in c or "-frames:v" in c]
assert len(enders) == 4, [c[:60] for c in enders]
print(f"OK 2: the fitter's 4 clip-ending commands take -frames:v when asked, -t when not "
      f"({len(cmds)} ffmpeg commands in the function)")

# --- 3. the black fill counts in frames -----------------------------------
assert '*(["-frames:v", str(frames)] if frames else ["-t", f"{duration:.6f}"])' in blk_src
assert "duration = frames / float(fps)" in blk_src
print("OK 3: _generate_black_video(frames=N) renders exactly N frames")

# --- 4. the assembler builds the plan and fits every clip to it -----------
assert "plan = frame_plan(targets, master_duration, plan_fps)" in asm_src
assert "_legacy_timeline = pre_computed_targets is None" in asm_src
assert "if not _legacy_timeline:" in asm_src
# every process_clip_for_alignment call inside the assembler passes exact_frames
calls = re.findall(r"process_clip_for_alignment\((.*?)\n\s*\)", asm_src, re.S)
assert len(calls) == 1, len(calls)
assert "exact_frames=_exact" in calls[0], calls[0]
assert "_exact = plan_frames_by_index.get(i) if not _legacy_timeline else None" in asm_src
# plan_fps is the frame rate the SEGMENT FILES carry (-r 24), not vid_fps
assert "plan_fps = 24.0" in asm_src
assert "_generate_black_video(\n                        black_path, seg[\"frames\"] / plan_fps," in asm_src
assert 'frames=seg["frames"],' in asm_src
print("OK 4: plan built once; the fitter and the black fills are driven by it")

# --- 5. per-segment probe, ONE repair, then raise -------------------------
enf_src = inspect.getsource(vp._enforce_frame_count)
assert "probe_frame_count(segment_path)" in enf_src
assert "tpad=stop_mode=clone:stop=" in enf_src, "the short case pads by cloning the last frame"
assert '"-frames:v", str(int(asked))' in enf_src, "the long case cuts to the asked count"
raises = re.findall(r"raise RuntimeError\(\s*f?\"(.*?)\"", enf_src, re.S)
assert any("asked {asked} frames, got {got2} after repair" in r for r in raises), raises
# exactly one repair attempt: the repair encode is not in a loop
assert enf_src.count("code, _, err = run(cmd)") == 1, enf_src.count("code, _, err = run(cmd)")
assert "while" not in enf_src and "for " not in enf_src
# the assembler calls it for clips AND for black fills
assert asm_src.count("_enforce_frame_count(") == 2, asm_src.count("_enforce_frame_count(")
print("OK 5: every segment is measured, repaired at most once, else RuntimeError "
      "naming asked/got")

# --- 6. the concat is held to the same budget -----------------------------
assert "probe_frame_count(norm_out)" in cat_src, "each normalised input is probed"
assert "probe_frame_count(output)" in cat_src, "the joined file is probed"
cat_raises = re.findall(r"raise RuntimeError\(\s*f\"(.*?)\"", cat_src, re.S)
assert any("concat: input {i} asked {_want}, got {_got}" in r for r in cat_raises), cat_raises
assert any("concat: joined asked {_want_total}" in r for r in cat_raises), cat_raises
# both probes sit BEFORE the mux (the assembler muxes after concat_videos returns)
i_join_probe = cat_src.find("probe_frame_count(output)")
i_drop = cat_src.find("drop_page_cache(output)")
assert 0 < i_join_probe < i_drop
print("OK 6: concat_videos(expect_frames=...) raises on any input or joined mismatch")

# --- 7. a master-aligned b-roll NEVER takes the xfade path ----------------
assert ("if (pre_computed_targets is None and transition and transition != \"none\"\n"
        "                and stats[\"black_segments\"] == 0):") in asm_src, \
    "the xfade branch must be gated on pre_computed_targets is None"
i_xfade = asm_src.find("concat_videos_with_transitions(\n                timeline_segments")
i_guard = asm_src.find("if (pre_computed_targets is None and transition")
assert 0 < i_guard < i_xfade, "the guard must come before the xfade call"
assert "ignored for " in asm_src and "master-aligned b-roll (v698A.2.2)" in asm_src, \
    "the ignored-transition log line must exist"
assert "concat_videos(timeline_segments, video_only_path,\n                          expect_frames=expect_frames)" in asm_src
print("OK 7: xfade is reachable only on the Whisper-master fallback; the b-roll "
      "logs the ignored transition and concats plain, with the budget")

# --- 8. the stats record, and the TEMP lines ------------------------------
assert 'stats["frame_plan"] = {' in asm_src
for key in ('"fps": plan_fps', '"segments": len(plan)', '"asked_frames": _plan_total',
            '"got_frames": 0', '"repaired": 0'):
    assert key in asm_src, key
assert 'clip_result["frames_got"] = _got' in asm_src
assert '"frames_asked": int(exact_frames) if exact_frames else None' in fit_src
# the DELIVERED file is measured, not assumed (the Step-7 `-shortest` mux
# costs the one tail frame; anything bigger is a different fault and is said)
assert '_fp["delivered_frames"] = probe_frame_count(output_path)' in asm_src
assert 'if abs(_fp["delivered_frames"] - _fp["asked_frames"]) > 1:' in asm_src
# the new TEMP lines exist (per segment + the summary), the old ones are gone
assert asm_src.count("[TEMP v698A.2.2]") >= 3, asm_src.count("[TEMP v698A.2.2]")
assert "[TEMP v698A.2]" not in main_src, "the v698A.2 TEMP prints must be gone from main.py"
assert re.search(r"\[TEMP v698A\.2\](?!\.)", vp_src) is None
# the v698A.2 evidence record itself stays
assert 'stats["v698a_word_alignment"] = _v698a_wa' in main_src
print("OK 8: stats['frame_plan'] + frames_asked/frames_got recorded; "
      "[TEMP v698A.2.2] in, [TEMP v698A.2] out, v698a_word_alignment kept")

# --- 9. the legacy branch still exists, untouched in shape ----------------
assert "MIN_BLACK = 0.04" in asm_src, "the legacy float timeline must survive"
assert "_generate_black_video(black_path, gap, vid_width, vid_height, vid_fps)" in asm_src
assert "_generate_black_video(black_path, trail, vid_width, vid_height, vid_fps)" in asm_src
assert 'black_path = temp_path / f"black_pre_{i:04d}.mp4"' in asm_src
assert 'black_path = temp_path / f"black_outro.mp4"' in asm_src
print("OK 9: the Whisper-master fallback keeps its float timeline and -t fitting")

print("ALL OK check_frame_plan_wiring")
