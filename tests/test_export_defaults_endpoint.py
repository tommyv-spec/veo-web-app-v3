# tests/test_export_defaults_endpoint.py
#
# v951 — GET /api/jobs/{id}/export-defaults. The Export dialog calls this to
# learn what THIS video declared, so it can open on the video's settings
# instead of the browser's localStorage.
#
# Three things have to hold, and each is one test below:
#   1. A job with declared export_* returns them folded onto model defaults,
#      and names them in `declared` so the UI can show the operator.
#   2. A job that declared nothing returns plain model defaults with an empty
#      `declared` — 330+ existing builds must see no behaviour change.
#   3. A corrupt stored spec degrades to defaults instead of 500ing, matching
#      _job_finishing_spec's existing tolerance.
#
# The payload helper is tested rather than the route, because the route adds
# only the ownership guard (get_user_job, already covered by every other
# /api/jobs/{id} test) and a log line.

import json
import sys
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import image_platform  # noqa: F401 — registers image_nodes for the FKs
import main


class _Job:
    """The only two attributes the payload helper touches."""

    def __init__(self, spec):
        self.id = "bb159509-a3e6-43b6-903c-2d2bf5014a13"
        self.finishing_spec = spec


def test_declared_fields_are_folded_and_named():
    job = _Job(json.dumps({"auto_finish": "on",
                           "export": {"smart_trim": False,
                                      "remove_silence": True,
                                      "silence_mode": "whisper"}}))
    out = main._export_defaults_payload(job)
    assert out["settings"]["smart_trim"] is False
    assert out["settings"]["silence_mode"] == "whisper"
    assert out["settings"]["playback_speed"] == 1.0
    assert sorted(out["declared"]) == ["remove_silence", "silence_mode", "smart_trim"]


def test_job_with_no_declaration_gets_plain_defaults():
    out = main._export_defaults_payload(_Job(None))
    assert out["declared"] == []
    assert out["settings"]["smart_trim"] is True
    assert out["settings"]["remove_silence"] is False


def test_corrupt_stored_spec_degrades_to_defaults():
    """Broken JSON means 'declared nothing', not a 500 in the operator's face."""
    out = main._export_defaults_payload(_Job("{not json"))
    assert out["declared"] == []
    assert out["settings"]["smart_trim"] is True


def test_payload_gathers_facts_from_clip_rows():
    """_job_export_facts reads render_method / swap_audio / dialogue_text
    off the job's clips and _export_defaults_payload forwards them."""

    class _Clip:
        def __init__(self, rm, sa, txt):
            self.render_method, self.swap_audio, self.dialogue_text = rm, sa, txt

    clips = [_Clip("charswap", "source-original", ""),
             _Clip("charswap", None, "")]
    facts = main._job_export_facts(clips)
    assert facts == {"all_charswap": True, "any_source_audio": True,
                     "has_speech": False}

    spoken = [_Clip(None, None, "my soldier stood down")]
    assert main._job_export_facts(spoken) == {
        "all_charswap": False, "any_source_audio": False, "has_speech": True}

    # empty job: no lane claims
    assert main._job_export_facts([]) == {
        "all_charswap": False, "any_source_audio": False, "has_speech": False}
