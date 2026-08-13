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
    ("the needle drops onto the fabric", 1),
    ("held still", 1),
    ("", 0),
])
def test_count_beats(text, expected):
    assert mcc.count_beats(text) == expected


def test_beat_counting_is_biased_toward_under_flagging():
    """Any comma saves a value from being called single-beat.

    Deliberate: a genuinely single-beat 1.2s cutaway is CORRECT with one verb,
    and a flag that fires on correct answers gets ignored. Under-flagging means
    a flag means something.
    """
    assert mcc.count_beats("she talks to the camera, smiling") > 1
    assert mcc.count_beats("she talks to the camera") == 1


@pytest.mark.parametrize("text", [
    "Hands stitch fabric with yellow needle and thread.",   # noun conjunction
    "Hands adjust a completed red and lace decorative bow assembly.",
    "Close-up of the elderly woman and girl sitting together.",
    "Hands lift and adjust patterned orange fabric on a cutting mat.",
    "Hands open and flip pages of a ring-bound printed guide book.",
    "The speaker smiles and nods warmly to the camera.",
])
def test_bare_and_is_not_a_beat_separator(text):
    """Checked against the real corpus: EVERY value that bare "and" split was a
    false positive — noun conjunctions ("needle and thread", "woman and girl")
    or compound verbs describing one continuous movement ("lift and adjust").
    Not one genuine sequential pair. Counting them as two beats hid real
    single-beat records from the flag AND corrupted the floor derivation."""
    assert mcc.count_beats(text) == 1


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


# ── the duration floor ────────────────────────────────────────────────

def test_a_short_clip_is_never_flagged_for_being_single_beat():
    """A 0.3s clip cannot hold two beats. Flagging it is noise, whatever the
    flow — shot 21 of the real ad was 0.3s at the highest flow in the ad."""
    art = _artifact([_shot(21, 0.0, 0.3, "Camera tilts up rapidly along spools.",
                           camera="fast tilt up")])
    mot = _motion([(21, 0.0, 0.3, "high", 16.87)])
    rep = mcc.cross_check(art, mot)
    assert rep["class_a"] == []
    assert rep["excluded_short"] == [21]
    assert rep["min_beat_seconds"] == 1.0


def test_the_floor_and_its_exclusions_are_printed_never_silent():
    """A silent filter is the exact thing this report exists to prevent."""
    art = _artifact([_shot(1, 0.0, 0.4, "she talks"),
                     _shot(2, 1.0, 3.0, "she talks")])
    mot = _motion([(1, 0.0, 0.4, "high", 9.0), (2, 1.0, 3.0, "high", 9.0)])
    text = mcc.format_report(mcc.cross_check(art, mot), Path("a.json"),
                             Path("m.json"))
    assert "at least 1.0s" in text
    assert "1 clip(s) excluded as too short" in text
    assert "shots [1]" in text
    assert "1 of 1 eligible clips" in text          # only shot 2 is eligible


def test_the_floor_is_configurable():
    art = _artifact([_shot(1, 0.0, 2.0, "she talks")])
    mot = _motion([(1, 0.0, 2.0, "high", 9.0)])
    assert len(mcc.cross_check(art, mot, min_beat_seconds=1.0)["class_a"]) == 1
    strict = mcc.cross_check(art, mot, min_beat_seconds=3.0)
    assert strict["class_a"] == []
    assert strict["excluded_short"] == [1]


def test_the_floor_does_not_swallow_a_locked_camera_with_fast_pixels():
    """High flow on a SHORT clip still has to be explained by something. If the
    camera is declared locked off and nothing is recorded moving, the two
    sources still disagree — that must not disappear into the floor."""
    art = _artifact([
        _shot(1, 0.0, 0.5, "held still", camera="static, locked off"),
        _shot(2, 1.0, 1.5, "held still", camera="fast whip pan right"),
    ])
    mot = _motion([(1, 0.0, 0.5, "high", 12.0), (2, 1.0, 1.5, "high", 12.0)])
    rep = mcc.cross_check(art, mot)
    assert rep["class_a"] == []                    # both below the floor
    assert [r["shot"] for r in rep["class_d"]] == [1]   # only the locked one
    text = mcc.format_report(rep, Path("a.json"), Path("m.json"))
    assert "camera held still" in text
    assert "nobody wrote it down" in text


def test_the_primary_flag_states_its_own_limit():
    """Flow cannot separate one long gesture from two beats. Shot 76 of the
    real ad (3.47s, flow 4.59) is one continuous gesture and lands here."""
    art = _artifact([_shot(76, 0.0, 3.47, "Wrinkled hands adjust wheel")])
    mot = _motion([(76, 0.0, 3.47, "high", 4.59)])
    rep = mcc.cross_check(art, mot)
    assert [r["shot"] for r in rep["class_a"]] == [76]
    text = mcc.format_report(rep, Path("a.json"), Path("m.json"))
    assert "cannot tell ONE long gesture from TWO" in text
    assert "never a verdict" in text


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
    # the ad's cutting rhythm is the denominator; print it next to the share
    assert st["median_clip_s"] == 3.0
    text = mcc.format_report(mcc.cross_check(art, mot), Path("a.json"),
                             Path("m.json"))
    assert "reported, not judged" in text
    assert "never fails a build" in text
    assert "median clip duration" in text
    assert "cutting rhythm" in text
    # the caveat the coordinator sharpened: 92% can be completely honest
    assert "legitimately sit near" in text
    assert "never against a" in text


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
    assert "1 of 1 eligible clips" in out
    assert "single beat" in out


def test_it_does_not_import_the_decode_engine():
    """Separate seam on purpose: a heuristic tweak here must never be able to
    touch prompt assembly, whose byte-identity is the ugc-reel invariant."""
    src = (Path(__file__).parent / "v589_motion_cross_check.py").read_text(
        encoding="utf-8")
    assert "import v589_video_understanding" not in src
    assert "from v589_video_understanding" not in src
