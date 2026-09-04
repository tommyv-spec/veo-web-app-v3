"""v698A.2 — static wiring checks on main.py's export path.

What has to be true, in order, inside _do_export_final:
  1. the v948 silence sweep runs BEFORE the v925 speaker-audio extraction
     (the cutaway file is built from the file that ships), and the old
     `v948_broll_stale` stamp is gone (it can no longer happen);
  2. the v926 mapping takes its speed ratio from `pre_sweep_duration` and maps
     every window through the sweep's own keep_segments (exact, never a ratio);
  3. the word pass comes AFTER that mapping and BEFORE export_with_master_audio,
     reuses v825's transcription + resolver under the v864 guards and lock, and
     any exception leaves method=chars (letter windows kept);
  4. the v825 stills step compares the file fingerprint before reusing words.

Run: python tests/check_v698a2_export_wiring.py  (from code/)
"""
import os
import re
import sys

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
src = open(os.path.join(ROOT, "main.py"), encoding="utf-8").read()


def pos(needle, start=0, label=None):
    i = src.find(needle, start)
    assert i >= 0, f"missing: {label or needle!r}"
    return i


# --- 1. sweep before the b-roll extraction; stale stamp gone -----------------
i_sweep = pos('from video_processor import sweep_silence_holes as _sweep948')
i_extract = pos('speaker_master_audio = broll_temp_dir / "speaker_master.mp3"')
assert i_sweep < i_extract, "v948 sweep must run BEFORE the speaker audio is extracted"
assert 'stats["v948_broll_stale"]' not in src, "v948_broll_stale can no longer happen; the stamp must be gone"
assert src.count('from video_processor import sweep_silence_holes as _sweep948') == 1, "the sweep must exist exactly once"
i_kept = pos('_v948_sweep = _sweep', i_sweep)
assert i_kept < i_extract
# the words + fingerprint locals are defined before the b-roll block
i_words_init = pos('_v698a_master_words = None')
i_fp_init = pos('_v698a_words_fp = None')
assert i_words_init < i_extract and i_fp_init < i_extract
print("OK 1: sweep before the b-roll pass, stale stamp gone, reuse locals initialised")

# --- 2. exact mapping through the sweep, ratio from pre_sweep_duration -------
i_map_block = pos('# === v926 — map the windows through what actually', i_extract)
i_pre_sweep = pos('float(stats.get("pre_sweep_duration") or 0.0)', i_map_block)
i_ratio_keep = pos('_pre_sweep_dur / _post_vad_dur', i_map_block)
i_ratio_glob = pos('_k = _pre_sweep_dur / _sum_durs', i_map_block)
i_step3 = pos('_map_through_vad_impl(_t["start"], _sweep_keeps)', i_map_block)
i_step3b = pos('_map_through_vad_impl(_t["end"], _sweep_keeps)', i_map_block)
i_keeps = pos('_sweep_keeps = _v948_sweep.get("keep_segments") or []', i_map_block)
assert i_pre_sweep < i_ratio_keep < i_ratio_glob < i_keeps < i_step3 < i_step3b
seg = src[i_map_block:i_step3]
assert '_speaker_final_dur / _post_vad_dur' not in seg, "speed ratio must not include the sweep's cuts"
assert '_k = _speaker_final_dur / _sum_durs' not in seg, "global ratio must not include the sweep's cuts"
print("OK 2: ratio from pre_sweep_duration; sweep mapped exactly through keep_segments")

# --- 3. the word pass: position, reuse of v825, guards, fallback -------------
i_window_log = pos("f\"[Export/v698A/broll]   window {_i}: \"", i_map_block)
i_wp = pos('# === v698A.2 — place each SHARED sentence', i_window_log)
i_export_call = pos('export_with_master_audio,\n                    clip_info=broll_clip_info', i_wp)
assert i_step3 < i_window_log < i_wp < i_export_call, "word pass must sit after the mapping and before the b-roll export"
wp = src[i_wp:i_export_call]
for name in (
    "transcribe_master_audio as _tma_w", "resolve_support_spans as _rss_w",
    "_align_scene_lines as _asl_w", "_normalize as _norm_w",
    "build_alignment_inputs as _bai_w", "sentence_container as _sc_w",
    "tile_fragment_windows as _tfw_w",
    "_v864_release()", "_v864_mem()", 'SUPPORT_TRACK_MIN_AVAIL_MB',
    "async with _V864_SUPPORT_LOCK:", "asyncio.to_thread(_tma_w, speaker_master_audio)",
    "_v698a_master_words = _mw_w", "_v698a_words_fp = (_st_w.st_size, _st_w.st_mtime_ns)",
    'stats["v698a_word_alignment"] = _v698a_wa',
    "if _afs_sharers and _pre_targets:",
    '"dialogue_text_b"', '"rendered_prompt_variant"',
):
    assert name in wp, f"word pass must contain {name!r}"
# the pass never raises: one try, whose except leaves method=chars
i_try = wp.find("try:")
i_exc = wp.find("except Exception as _wa_err:")
assert 0 < i_try < i_exc, "word pass must be wrapped in try/except"
exc_body = wp[i_exc:i_exc + 900]
assert '_v698a_wa["method"] = "chars"' in exc_body, "an exception must leave method=chars"
assert "letter windows kept" in exc_body
# the stats record is written after the try/except, so it exists on every path
assert wp.rfind('stats["v698a_word_alignment"] = _v698a_wa') > i_exc
# per-group fallback keeps its letters and says why (the log strings are split
# across f-string fragments, so count the two halves separately)
assert wp.count("method=chars") >= 3, wp.count("method=chars")
assert wp.count("reason=") >= 3, wp.count("reason=")
assert "method=words" in wp
# no clamping / no raise of the letter path inside the word pass
assert "raise RuntimeError(\n                                f\"v698A many-to-one sub-span split failed" not in wp
print("OK 3: word pass placed, reuses v825 + v864 guards, falls back to letters")

# --- 4. the stills step checks the fingerprint before reusing words ----------
i_v825 = pos("# v825 — timed support-image inserts", i_export_call)
i_reuse = pos("(_st_r.st_size, _st_r.st_mtime_ns) == _v698a_words_fp", i_v825)
i_transcribe = pos("_mw = await asyncio.to_thread(_tma, _sup_audio)", i_v825)
assert i_v825 < i_reuse < i_transcribe, "fingerprint check must come before the fallback transcription"
assert "if _mw is None:" in src[i_reuse:i_transcribe]
assert "master words NOT reused: output_path changed" in src[i_v825:i_transcribe]
print("OK 4: v825 step reuses the words only on a matching fingerprint")

# --- the resolver contract is untouched --------------------------------------
vp = open(os.path.join(ROOT, "video_processor.py"), encoding="utf-8").read()
assert "def resolve_support_spans(master_words: list, support_inserts: list, scene_lines: list = None) -> list:" in vp
assert "def _align_scene_lines(master_words: list, master_text: str, scene_lines: list) -> list:" in vp
assert re.search(r"\n\s+cur = max\(cur, lo\)  # advance; allow overlap, never rewind", vp), \
    "resolver cursor unchanged (the plan guards it instead of changing it)"
print("OK 5: resolver contract untouched")

print("ALL OK check_v698a2_export_wiring")
