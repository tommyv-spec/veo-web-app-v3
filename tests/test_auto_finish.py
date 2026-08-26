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
