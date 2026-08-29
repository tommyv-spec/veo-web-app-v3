# tests/test_v952_overlay_split_and_audio.py
#
# v952 — three defects the operator found on job bb159509 (noemi-cablefly), all
# measured on the delivered file before anything was changed:
#
#   1. The read-caption block sat across the man's chest. The occupancy engine
#      that placed it vetoes FACES and ranks MOTION and has no idea a body is
#      there — it reported "covers 0% of subject" for a block on his tattoos,
#      because age_cov/block_cov were hardcoded 0.0. And the tall block plus the
#      face veto leaves exactly ONE window (43.5%..53.0% on that clip), all of it
#      torso, so no re-ranking of a single slab can help. Splitting can: the
#      frame above his head (0%..22.7%) is empty and was never used.
#   2. `syntheticperformer` was stamped TWICE — compose() burns it on every frame
#      (v938.15) and this stage stamped it again.
#   3. The music stopped 1.70s before the end, because a short source was padded
#      with silence to fill Veo's fixed ~7.7s container.

import sys
import pathlib

import numpy as np
import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))


# --------------------------------------------------------------------------
# 1. the split layout
# --------------------------------------------------------------------------

def _standing_person_profile(H=1920):
    """A silhouette shaped like the clip this rule came from: empty above the
    head, widest across the chest, tapering down the legs."""
    prof = np.zeros(H, dtype=float)
    prof[int(0.22 * H):int(0.27 * H)] = 0.20      # head
    prof[int(0.27 * H):int(0.42 * H)] = 0.45      # face/neck band
    prof[int(0.42 * H):int(0.58 * H)] = 0.60      # chest — the WIDEST part
    prof[int(0.58 * H):int(0.80 * H)] = 0.35      # waist/thighs
    prof[int(0.80 * H):] = 0.20                   # legs
    return prof


def test_split_uses_the_empty_frame_above_the_head():
    import autoedit_pipeline as ap
    H = 1920
    prof = _standing_person_profile(H)
    scale = 1080 / 720.0
    age_h = ap.RC_SPEC["age"][0] * scale
    sp = ap.rc_split_layout(prof, H, scale, n_lines=4, has_route=True,
                            face_lo=0.271, face_hi=0.419,
                            age_top_frac=0.06, age_h=age_h)
    assert sp is not None
    # at least one line goes up top, and the top group lands in the empty band
    assert 1 <= sp["split_at"] <= 3
    assert sp["block_top"] / H < 0.271
    assert sp["top_cov"] < 0.05          # the band above the head is empty
    # the low group sits below the face and inside the Reels safe zone
    assert sp["block"] / H > 0.419
    assert sp["block"] / H < ap.RC_SAFE_BOTTOM


def test_split_beats_one_slab_on_this_shape():
    """The whole reason the rule exists: the single slab has no good window."""
    import autoedit_pipeline as ap
    H = 1920
    prof = _standing_person_profile(H)
    scale = 1080 / 720.0
    age_h = ap.RC_SPEC["age"][0] * scale
    line_h = ap.RC_SPEC["body"][0] * scale
    slab_h = 3 * ap.RC_BODY_PITCH * scale + line_h \
        + ap.RC_GAP_BODY_ROUTE * scale + ap.RC_SPEC["route"][0] * scale
    one = ap.rc_best_top(prof, H, slab_h, 0.419 + 0.015, ap.RC_SAFE_BOTTOM)
    sp = ap.rc_split_layout(prof, H, scale, 4, True, 0.271, 0.419, 0.06, age_h)
    assert one is not None and sp is not None
    assert one[1] - sp["cov"] >= ap.RC_SPLIT_MIN_GAIN


def test_no_split_without_a_silhouette_or_with_one_line():
    import autoedit_pipeline as ap
    scale = 1080 / 720.0
    age_h = ap.RC_SPEC["age"][0] * scale
    prof = _standing_person_profile()
    assert ap.rc_split_layout(None, 1920, scale, 4, True, 0.27, 0.42, 0.06, age_h) is None
    assert ap.rc_split_layout(prof, 1920, scale, 1, True, 0.27, 0.42, 0.06, age_h) is None


def test_no_split_when_there_is_no_room_above_the_face():
    """A face that starts right under the age line leaves no top band; the
    caller must keep its single block rather than get a bogus plan."""
    import autoedit_pipeline as ap
    scale = 1080 / 720.0
    age_h = ap.RC_SPEC["age"][0] * scale
    prof = _standing_person_profile()
    assert ap.rc_split_layout(prof, 1920, scale, 4, True,
                              face_lo=0.14, face_hi=0.40,
                              age_top_frac=0.06, age_h=age_h) is None


def test_band_coverage_reads_the_profile():
    import autoedit_pipeline as ap
    prof = np.zeros(1000, dtype=float)
    prof[400:600] = 1.0
    assert ap.rc_band_coverage(prof, 1000, 400, 200) == pytest.approx(1.0)
    assert ap.rc_band_coverage(prof, 1000, 0, 200) == pytest.approx(0.0)
    assert ap.rc_band_coverage(prof, 1000, 300, 200) == pytest.approx(0.5)


# --------------------------------------------------------------------------
# 2. the duplicate watermark
# --------------------------------------------------------------------------

def test_overlay_does_not_restamp_the_disclosure():
    """compose() already burns it on every frame — this stage must not add a
    second copy. That is why the corner carried two overlapping stamps."""
    import autoedit_pipeline as ap
    plan = ap.overlay_stage_plan({"overlay": "readcaption", "overlay_age": "I'M 74",
                                  "overlay_block": ["a", "b"]})
    assert plan["watermark"] is None


def test_overlay_watermark_is_still_declarable():
    import autoedit_pipeline as ap
    plan = ap.overlay_stage_plan({"overlay": "readcaption", "overlay_age": "I'M 74",
                                  "overlay_block": ["a"],
                                  "overlay_watermark": "somethingelse"})
    assert plan["watermark"] == "somethingelse"


# --------------------------------------------------------------------------
# 3. the music that stopped early
# --------------------------------------------------------------------------

def test_source_audio_loops_to_fill_the_render():
    """A 6s source over a 7.7s Veo container used to end in 1.7s of silence."""
    import main
    argv = main._v943_1_mux_argv("render.mp4", "source.mp4", "out.mp4",
                                 render_duration=7.7734)
    # -stream_loop applies to the input that FOLLOWS it, which must be the audio
    i = argv.index("-stream_loop")
    assert argv[i + 1] == "-1"
    assert argv[i + 2] == "-i" and argv[i + 3] == "source.mp4"
    # and the render still sets the length
    assert "-t" in argv and argv[argv.index("-t") + 1].startswith("7.7734")


def test_the_tail_is_faded_not_cut():
    import main
    argv = main._v943_1_mux_argv("r.mp4", "s.mp4", "o.mp4", render_duration=8.0)
    af = argv[argv.index("-af") + 1]
    assert "afade=t=out" in af
    assert f"st={8.0 - main.RC_AUDIO_FADE_S:.6f}" in af


def test_the_picture_is_never_re_encoded_or_shortened():
    """The bug this replaced cut FRAMES off the picture. The video stream is
    copied, and the output length comes from the render."""
    import main
    argv = main._v943_1_mux_argv("r.mp4", "s.mp4", "o.mp4", render_duration=7.5)
    assert argv[argv.index("-c:v") + 1] == "copy"
    assert argv[argv.index("-map") + 1] == "0:v:0"


def test_unprobed_duration_still_terminates():
    """With an endlessly looping audio input, something must stop the encode.
    -shortest does, because the VIDEO input is finite."""
    import main
    argv = main._v943_1_mux_argv("r.mp4", "s.mp4", "o.mp4", render_duration=None)
    assert "-shortest" in argv
    assert "-t" not in argv


def test_fade_never_exceeds_a_quarter_of_a_short_clip():
    import main
    argv = main._v943_1_mux_argv("r.mp4", "s.mp4", "o.mp4", render_duration=0.8)
    af = argv[argv.index("-af") + 1]
    assert "d=0.200000" in af          # 0.8 / 4, not the 0.35 default
