import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from auto_finish import auto_finish_on, all_clips_approved, derive_export_defaults


def test_auto_finish_on_only_when_declared_on():
    assert auto_finish_on({"auto_finish": "on"})
    assert not auto_finish_on({"auto_finish": "off"})
    assert not auto_finish_on({})           # absent key
    assert not auto_finish_on(None)         # no declared finishing at all


def test_all_clips_approved_requires_clips_and_unanimity():
    assert all_clips_approved(["approved", "approved"])
    assert not all_clips_approved(["approved", "rejected"])
    assert not all_clips_approved(["approved", None])   # undecided clip
    assert not all_clips_approved([])                    # a clipless job never fires


def test_derive_export_defaults_declaration_fills_gaps_explicit_wins():
    spec = {"export": {"remove_silence": True, "music_gain_db": -22.0}}
    req = {"remove_silence": False, "music_gain_db": 0.0, "smart_trim": True}
    out = derive_export_defaults(req, spec, request_was_explicit={"remove_silence"})
    assert out["remove_silence"] is False        # explicitly sent — wins
    assert out["music_gain_db"] == -22.0         # default value, not a choice — declaration wins
    assert out["smart_trim"] is True             # untouched
    assert req["music_gain_db"] == 0.0           # caller's dict not mutated


def test_derive_export_defaults_no_spec_is_identity():
    req = {"smart_trim": True}
    assert derive_export_defaults(req, None, set()) == req
    assert derive_export_defaults(req, {"captions": "none"}, set()) == req  # v944-only spec


# --- v951: export_modal_defaults -------------------------------------------
# The modal has to open on the VIDEO's settings, not the browser's. This is
# the decision behind that: model defaults, with the build's declared export_*
# folded on top, plus the names of the keys the build actually decided.

from auto_finish import export_modal_defaults


def test_export_modal_defaults_no_spec_is_pure_model_defaults():
    """A job that declared nothing must behave exactly as before v951."""
    out = export_modal_defaults(None)
    assert out["declared"] == []
    assert out["settings"]["smart_trim"] is True
    assert out["settings"]["remove_silence"] is False
    assert out["settings"]["playback_speed"] == 1.0
    assert out["settings"]["frames_to_cut_end"] == 7


def test_export_modal_defaults_folds_in_declared_fields():
    spec = {"auto_finish": "on",
            "export": {"smart_trim": False, "remove_silence": True,
                       "silence_mode": "whisper", "silence_trigger": 0.3}}
    out = export_modal_defaults(spec)
    assert out["settings"]["smart_trim"] is False
    assert out["settings"]["remove_silence"] is True
    assert out["settings"]["silence_mode"] == "whisper"
    assert out["settings"]["silence_trigger"] == 0.3
    # everything NOT declared stays at the model default
    assert out["settings"]["playback_speed"] == 1.0
    assert sorted(out["declared"]) == [
        "remove_silence", "silence_mode", "silence_trigger", "smart_trim"]


def test_export_modal_defaults_ignores_non_export_finishing_keys():
    """captions/overlay/auto_finish belong to v944/v947, not to the export."""
    spec = {"captions": "korella", "overlay": "readcaption",
            "overlay_age": "I'M 74", "auto_finish": "on"}
    out = export_modal_defaults(spec)
    assert out["declared"] == []
    assert "captions" not in out["settings"]


def test_export_modal_defaults_survives_a_junk_spec():
    """A corrupt spec degrades to 'declared nothing' — never raises. The modal
    opening is not the place to discover a bad build."""
    for junk in ("not a dict", 42, {"export": "not a dict"}, {"export": None}):
        out = export_modal_defaults(junk)
        assert out["declared"] == []
        assert out["settings"]["smart_trim"] is True
