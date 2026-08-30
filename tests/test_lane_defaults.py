"""v957 — lane-aware export defaults, derived from the job's own clip rows.

The v951 authoring table said what a lane's settings should be; it relied on
the build DECLARING them. This derives the same answers from what the job
factually IS, so an undeclared job still opens the dialog on sane values.
Facts come from Clip rows: render_method, swap_audio, dialogue_text.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from auto_finish import derive_lane_defaults, export_modal_defaults  # noqa: E402


def test_silent_charswap_with_source_audio_is_the_music_lane():
    lane, derived = derive_lane_defaults(
        all_charswap=True, any_source_audio=True, has_speech=False)
    assert lane == "charswap-music"
    assert derived == {
        "smart_trim": False,
        "frames_to_cut_start": 7,
        "remove_silence": False,
        "playback_speed": 1.0,
    }


def test_silent_charswap_without_source_audio_is_still_the_swap_lane():
    lane, derived = derive_lane_defaults(
        all_charswap=True, any_source_audio=False, has_speech=False)
    assert lane == "charswap-silent"
    assert derived["smart_trim"] is False
    assert derived["frames_to_cut_start"] == 7
    assert derived["remove_silence"] is False
    assert derived["playback_speed"] == 1.0


def test_spoken_build_derives_nothing():
    """303 spoken builds predate this rule. Deriving remove_silence=True for
    them would change what an unattended export does. A spoken job opens on
    model defaults exactly as before; whisper stays a declared choice."""
    lane, derived = derive_lane_defaults(
        all_charswap=False, any_source_audio=False, has_speech=True)
    assert lane == "spoken"
    assert derived == {}


def test_mixed_job_derives_nothing():
    """A job with charswap AND spoken clips is no single lane — derive
    nothing rather than guess."""
    lane, derived = derive_lane_defaults(
        all_charswap=False, any_source_audio=True, has_speech=True)
    assert lane == "mixed"
    assert derived == {}


def test_modal_defaults_precedence_derived_under_declared():
    spec = {"export": {"frames_to_cut_start": 3}}
    out = export_modal_defaults(
        spec,
        job_facts={"all_charswap": True, "any_source_audio": True,
                   "has_speech": False})
    assert out["settings"]["smart_trim"] is False
    assert out["settings"]["remove_silence"] is False
    assert out["settings"]["playback_speed"] == 1.0
    assert out["settings"]["frames_to_cut_start"] == 3
    assert out["declared"] == ["frames_to_cut_start"]
    assert sorted(out["derived"]) == [
        "playback_speed", "remove_silence", "smart_trim"]
    assert out["lane"] == "charswap-music"


def test_modal_defaults_without_facts_is_byte_identical_to_v951():
    """Every existing caller that passes no facts keeps the exact v951
    contract — settings + declared, no lane, no derived keys applied."""
    out = export_modal_defaults({"export": {"smart_trim": False}})
    assert out["declared"] == ["smart_trim"]
    assert out["settings"]["smart_trim"] is False
    assert out.get("derived") == []
    assert out.get("lane") is None
