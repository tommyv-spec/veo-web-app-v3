"""Tests for the v589 offline motion cross-check.

The report exists because the lean lane's named failure — a one-verb summary of
a multi-beat clip — is SILENT: such an artifact validates perfectly. It checks
EVIDENCE (v585 Farneback optical flow, an independent source that never saw the
prompt) rather than a word-count metric, because a checker that greps for a
word makes the word the target.

Run: python -m pytest test_v589_motion_cross_check.py -q
"""
import importlib.util
import json
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "v589_mcc", Path(__file__).parent / "v589_motion_cross_check.py")
mcc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mcc)


# ── beat counting ─────────────────────────────────────────────────────

@pytest.mark.parametrize("text,expected", [
    ("she lifts the jar, then twists the lid, then tips it toward the lens", 3),
    ("her hands lift the hem, then she turns it toward the lens", 2),
    ("she lifts the jar and twists the lid, then tips it", 3),
    ("the needle drops onto the fabric", 1),
    ("held still", 1),
    ("", 0),
])
def test_count_beats(text, expected):
    assert mcc.count_beats(text) == expected


def test_beat_counting_is_biased_toward_under_flagging():
    """Any comma or 'and' saves a value from being called single-beat.

    Deliberate: a genuinely single-beat 1.2s cutaway is CORRECT with one verb,
    and a flag that fires on correct answers gets ignored. Under-flagging means
    a flag means something.
    """
    assert mcc.count_beats("she talks to the camera, smiling") > 1
    assert mcc.count_beats("she talks to the camera") == 1


@pytest.mark.parametrize("text,expected", [
    ("held still", True), ("Held still.", True), ("held still, locked off", True),
    ("static", True), ("nothing moves", True), ("unchanged", True),
    ("she holds the jar still while talking", False),
    ("the static on the screen flickers", False),
    ("", False),
])
def test_is_explicit_no_motion(text, expected):
    assert mcc.is_explicit_no_motion(text) is expected


def test_says_unchanged_and_camera_move_detection():
    assert mcc.says_unchanged("unchanged from the start") is True
    assert mcc.says_unchanged("the jar sits open on the counter") is False
    assert mcc.names_a_camera_move("slow push in, steady") is True
    assert mcc.names_a_camera_move("fast whip pan to the right") is True
    assert mcc.names_a_camera_move("static, locked off") is False
    assert mcc.names_a_camera_move("") is False


# ── fixtures ──────────────────────────────────────────────────────────

def _shot(i, start, end, action, camera="static, locked off",
          end_state="the jar sits open on the counter"):
    return {"shot_index": i, "start": start, "end": end,
            "shot_type": "b-roll", "action": action, "camera_move": camera,
            "end_state": end_state, "on_screen_text": "none",
            "sell_function": "proof/demo"}


def _artifact(shots, version="adread.v1"):
    return {"schema_version": version, "observed_people": [], "shots": shots}


def _motion(rows):
    return [{"shot": i, "start": s, "end": e, "motion": lvl,
             "mean_flow_mag": mag} for i, s, e, lvl, mag in rows]


# ── the join ──────────────────────────────────────────────────────────

def test_high_flow_against_a_single_beat_action_is_a_contradiction():
    art = _artifact([
        _shot(1, 0.0, 2.0, "she talks"),                       # 1 beat, high
        _shot(2, 2.0, 4.0, "she lifts the jar, then tips it"),  # 2 beats, high
        _shot(3, 4.0, 6.0, "held still"),                      # no motion, high
        _shot(4, 6.0, 8.0, "she talks"),                       # 1 beat, LOW
    ])
    mot = _motion([(1, 0.0, 2.0, "high", 4.2), (2, 2.0, 4.0, "high", 3.1),
                   (3, 4.0, 6.0, "high", 9.9), (4, 6.0, 8.0, "low", 0.1)])
    rep = mcc.cross_check(art, mot)
    assert rep["joined"] == 4
    assert [r["shot"] for r in rep["class_a"]] == [3, 1]   # sorted by flow desc
    assert rep["class_a"][0]["flow"] == 9.9
    assert rep["class_a"][0]["beats"] == 0                 # explicit no-motion


def test_secondary_classes_end_state_and_camera_move():
    art = _artifact([
        _shot(1, 0.0, 2.0, "a, b, c", end_state="unchanged from the start"),
        _shot(2, 2.0, 4.0, "a, b, c", camera="slow push in, steady"),
    ])
    mot = _motion([(1, 0.0, 2.0, "high", 5.0), (2, 2.0, 4.0, "low", 0.05)])
    rep = mcc.cross_check(art, mot)
    assert [r["shot"] for r in rep["class_b"]] == [1]
    assert [r["shot"] for r in rep["class_c"]] == [2]
    assert rep["class_a"] == []          # multi-beat, correctly not flagged


def test_a_clean_artifact_reports_nothing():
    art = _artifact([_shot(i, i, i + 1,
                           "she lifts it, then turns it, then sets it down")
                     for i in range(1, 6)])
    mot = _motion([(i, i, i + 1, "high", 3.0) for i in range(1, 6)])
    rep = mcc.cross_check(art, mot)
    assert rep["class_a"] == [] and rep["class_b"] == [] and rep["class_c"] == []
    assert rep["joined"] == 5


def test_count_mismatch_is_itself_a_finding():
    art = _artifact([_shot(1, 0.0, 2.0, "she talks"),
                     _shot(2, 2.0, 4.0, "she talks")])
    mot = _motion([(1, 0.0, 2.0, "high", 4.0), (7, 9.0, 10.0, "high", 4.0)])
    rep = mcc.cross_check(art, mot)
    assert rep["missing_motion"] == [2]   # artifact shot with no motion entry
    assert rep["missing_shots"] == [7]    # motion entry with no artifact shot
    assert rep["joined"] == 1             # joined on the overlap only
    text = mcc.format_report(rep, Path("a.json"), Path("motion.json"))
    assert "do not describe the same clip list" in text
    assert "[2]" in text and "[7]" in text


def test_statistics_are_reported_not_gated():
    art = _artifact([
        _shot(1, 0.0, 3.0, "she talks"),                 # long, single beat
        _shot(2, 3.0, 7.0, "a, b, c"),                   # long, multi beat
        _shot(3, 7.0, 8.0, "she talks"),                 # short, single beat
    ])
    mot = _motion([(i, 0, 1, "medium", 0.5) for i in (1, 2, 3)])
    st = mcc.cross_check(art, mot)["stats"]
    assert st["long_clips"] == 2
    assert st["long_single_beat"] == [1]        # the short one is not counted
    assert st["long_single_beat_share"] == 0.5
    assert st["median_action_tokens"] > 0
    text = mcc.format_report(mcc.cross_check(art, mot), Path("a.json"),
                             Path("m.json"))
    assert "reported, not judged" in text
    assert "never fails a build" in text


def test_the_legacy_visual_shape_is_read_but_labelled():
    """An artifact from before the ACTION rework carries `visual`, which was
    never asked to record motion. Read it, but never present it as evidence
    about the current prompt."""
    shots = [{"shot_index": 1, "start": 0.0, "end": 2.0,
              "shot_type": "b-roll", "visual": "A woman holds up a dress.",
              "on_screen_text": "none", "who_on_camera": "a woman",
              "sell_function": "Hook", "production_method": "real UGC"}]
    rep = mcc.cross_check(_artifact(shots), _motion([(1, 0.0, 2.0, "high", 9.3)]))
    assert rep["field"] == "visual"
    assert rep["legacy_field"] is True
    assert [r["shot"] for r in rep["class_a"]] == [1]
    text = mcc.format_report(rep, Path("a.json"), Path("m.json"))
    assert "predates the ACTION rework" in text
    assert "NOT evidence about the current prompt" in text
    assert "`visual`" in text


# ── the honest edges, through main() ──────────────────────────────────

def test_missing_motion_json_says_so_and_skips(tmp_path, capsys):
    art = tmp_path / "stage4d_vlm.json"
    art.write_text(json.dumps(_artifact([_shot(1, 0.0, 2.0, "a")])),
                   encoding="utf-8")
    assert mcc.main([str(art)]) == 0          # never raises, never fails
    out = capsys.readouterr().out
    assert "no motion.json" in out
    assert "nothing worth inventing" in out


def test_a_stage4d_v2_artifact_exits_cleanly(tmp_path, capsys):
    art = tmp_path / "stage4d_vlm.json"
    art.write_text(json.dumps(_artifact([], version="stage4d.v2")),
                   encoding="utf-8")
    (tmp_path / "motion.json").write_text("[]", encoding="utf-8")
    assert mcc.main([str(art)]) == 0
    out = capsys.readouterr().out
    assert "skipped" in out
    assert "stage4d.v2" in out
    assert "motion_cross_check" in out        # says why: the heavy lane has one


def test_a_missing_artifact_is_the_one_hard_error(tmp_path, capsys):
    assert mcc.main([str(tmp_path / "nope.json")]) == 2
    assert "not found" in capsys.readouterr().err


def test_main_prints_the_report_and_still_exits_zero(tmp_path, capsys):
    art = tmp_path / "stage4d_vlm.json"
    art.write_text(json.dumps(_artifact([_shot(1, 0.0, 3.0, "she talks")])),
                   encoding="utf-8")
    (tmp_path / "motion.json").write_text(
        json.dumps(_motion([(1, 0.0, 3.0, "high", 8.0)])), encoding="utf-8")
    assert mcc.main([str(art)]) == 0          # a finding is NOT a failure
    out = capsys.readouterr().out
    assert "1 of 1 clips" in out
    assert "single beat" in out


def test_it_does_not_import_the_decode_engine():
    """Separate seam on purpose: a heuristic tweak here must never be able to
    touch prompt assembly, whose byte-identity is the ugc-reel invariant."""
    src = (Path(__file__).parent / "v589_motion_cross_check.py").read_text(
        encoding="utf-8")
    assert "import v589_video_understanding" not in src
    assert "from v589_video_understanding" not in src
