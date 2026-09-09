"""v965 — the clip contract's own tests.

What these pin, in one line: a contract cannot be built PARTIAL, cannot be
built WRONG for its lane, and cannot be READ through anything except the two
accessors -- and `read_contract_json` hands back the stored bytes unchanged,
because stage 3 proves the worker's copy is byte-equal to what was stored and
that proof is worthless if the getter re-serialises.
"""
import json
import pathlib
import sys

import pytest

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

_HERE = pathlib.Path(__file__).parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import clip_contract as cc  # noqa: E402


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

def _asset(role="start_frame", media="image", key="jobs/x/frames/a.png",
           url="https://h/api/local-worker/frames/x/a.png", origin="image_1"):
    return {"role": role, "media": media, "key": key, "url": url,
            "origin": origin}


def _contract(**over):
    base = {
        "clip_contract_version": 1,
        "input_mode": "frames",
        "isolate_project": False,
        "policy_fallback": ["prompt_b", "model_swap", "fail"],
        "veo_model": "Veo 3.1 - Lite [Lower Priority]",
        "duration_s": 8,
        "aspect_ratio": "9:16",
        "variants": 2,
        "resolution": "720p",
        "swap_mode": None,
        "swap_max_source_s": None,
        "assets": [_asset()],
    }
    base.update(over)
    return base


class _Row:
    """The bit of a Clip row the accessors touch. Duck-typed on purpose --
    `clip_contract.py` imports no project code, so it must not need the real
    mapped class to be testable."""

    def __init__(self, version=None, raw=None):
        self.clip_contract_version = version
        self.clip_contract_json = raw


# --------------------------------------------------------------------------
# 1. the shape cannot be partial or surprising
# --------------------------------------------------------------------------

def test_there_are_exactly_twelve_fields():
    """The count is the contract. The worker's ledger is checked against this
    same field list, so a field added here without an applier is caught by the
    parity test rather than by a silently unapplied setting."""
    assert len(cc.ClipContract.model_fields) == 12


def test_an_unknown_key_is_refused():
    with pytest.raises(Exception):
        cc.ClipContract(**_contract(sneaky="value"))


def test_a_missing_field_is_refused():
    """No optional WHAT fields. A field that does not apply carries an explicit
    null; leaving it out is not the same statement."""
    partial = _contract()
    del partial["resolution"]
    with pytest.raises(Exception):
        cc.ClipContract(**partial)


def test_an_asset_entry_has_exactly_five_keys():
    assert sorted(cc.AssetEntry.model_fields) == [
        "key", "media", "origin", "role", "url"]


def test_max_source_s_inside_an_asset_is_refused():
    """It is a top-level field. Putting it in an asset entry was one of the
    three incompatible spellings the fresh-eyes review found, and
    `extra="forbid"` is what stops that being a silent no-op."""
    bad = _asset()
    bad["max_source_s"] = 10
    with pytest.raises(Exception):
        cc.AssetEntry(**bad)


def test_a_face_cannot_be_a_video():
    with pytest.raises(Exception):
        cc.AssetEntry(**_asset(role="face", media="video"))


def test_a_swap_source_must_be_a_video():
    with pytest.raises(Exception):
        cc.AssetEntry(**_asset(role="swap_source", media="image"))


# --------------------------------------------------------------------------
# 2. values the Flow UI cannot express are refused at build time
# --------------------------------------------------------------------------

@pytest.mark.parametrize("field,value", [
    ("input_mode", "Ingredients"),      # right idea, wrong case: the UI label
    ("veo_model", "Veo 4"),
    ("duration_s", 7),
    ("variants", 9),
    ("swap_mode", "audio-led"),
    ("clip_contract_version", 2),
])
def test_an_undrivable_value_is_refused(field, value):
    with pytest.raises(Exception):
        cc.ClipContract(**_contract(**{field: value}))


def test_a_ladder_that_cannot_end_is_refused():
    """A policy ladder with no terminal rung is how a blocked clip retries for
    ever."""
    with pytest.raises(Exception):
        cc.ClipContract(**_contract(policy_fallback=["prompt_b"]))


def test_a_ladder_with_fail_in_the_middle_is_refused():
    with pytest.raises(Exception):
        cc.ClipContract(**_contract(
            policy_fallback=["fail", "model_swap", "fail"]))


def test_an_empty_ladder_is_refused():
    with pytest.raises(Exception):
        cc.ClipContract(**_contract(policy_fallback=[]))


# --------------------------------------------------------------------------
# 3. per-lane shape — every cell is an existing parser refusal, re-expressed
# --------------------------------------------------------------------------

def test_a_simple_clip_is_valid_with_one_start_frame():
    cc.validate_lane(cc.ClipContract(**_contract()), None)


def test_a_simple_clip_cannot_carry_a_face():
    c = cc.ClipContract(**_contract(assets=[
        _asset(), _asset(role="face", key="k2", origin="image_3")]))
    with pytest.raises(ValueError, match="must not carry a 'face'"):
        cc.validate_lane(c, None)


def test_a_section_clip_needs_one_or_two_faces():
    ok = cc.ClipContract(**_contract(input_mode="ingredients", assets=[
        _asset(),
        _asset(role="face", key="k2", origin="image_3"),
        _asset(role="face", key="k3", origin="image_2")]))
    cc.validate_lane(ok, "movie-section")

    three = cc.ClipContract(**_contract(input_mode="ingredients", assets=[
        _asset(),
        _asset(role="face", key="k2", origin="image_3"),
        _asset(role="face", key="k3", origin="image_2"),
        _asset(role="face", key="k4", origin="image_4")]))
    with pytest.raises(ValueError, match="1-2 'face'"):
        cc.validate_lane(three, "movie-section")


def test_a_section_clip_with_no_face_is_refused():
    c = cc.ClipContract(**_contract(input_mode="ingredients"))
    with pytest.raises(ValueError, match="needs a 'face'"):
        cc.validate_lane(c, "movie-section")


def _charswap(mode="video-led", extra=()):
    assets = [
        _asset(role="avatar", key="up/1.png", origin="the healer"),
        _asset(role="swap_source", media="video", key="src/a.mp4",
               origin="dance clip"),
    ]
    assets.extend(extra)
    return cc.ClipContract(**_contract(
        input_mode="ingredients", swap_mode=mode,
        swap_max_source_s=cc.SWAP_MAX_SOURCE_S, assets=assets))


def test_a_video_led_charswap_is_valid_without_a_start_frame():
    cc.validate_lane(_charswap("video-led"), "charswap")


def test_a_video_led_charswap_must_not_carry_a_start_frame():
    """The one per-lane cell that is NOT a restatement of an existing refusal.
    Today a start frame is sent on every clip and the video-led path ignores
    it; an asset nobody attaches is one the worker would have to judge."""
    c = _charswap("video-led", extra=[_asset()])
    with pytest.raises(ValueError, match="must not carry a 'start_frame'"):
        cc.validate_lane(c, "charswap")


def test_an_image_led_charswap_needs_its_start_frame():
    with pytest.raises(ValueError, match="needs exactly 1 'start_frame'"):
        cc.validate_lane(_charswap("image-led"), "charswap")
    cc.validate_lane(_charswap("image-led", extra=[_asset()]), "charswap")


def test_a_charswap_clip_must_declare_swap_mode():
    c = cc.ClipContract(**_contract(input_mode="ingredients", assets=[
        _asset(role="avatar", key="up/1.png", origin="the healer"),
        _asset(role="swap_source", media="video", key="src/a.mp4",
               origin="dance clip")]))
    with pytest.raises(ValueError, match="must declare swap_mode"):
        cc.validate_lane(c, "charswap")


def test_swap_mode_on_a_non_charswap_clip_is_refused():
    c = cc.ClipContract(**_contract(swap_mode="video-led"))
    with pytest.raises(ValueError, match="only for a charswap clip"):
        cc.validate_lane(c, None)


def test_reference_has_no_producer_in_v1():
    c = cc.ClipContract(**_contract(assets=[
        _asset(), _asset(role="reference", key="k9", origin="image_9")]))
    with pytest.raises(ValueError, match="no producer in contract v1"):
        cc.validate_lane(c, None)


def test_the_same_file_cannot_be_attached_twice():
    """Clicking an already-selected asset toggles it OFF, so a duplicate does
    not double-attach -- it silently removes one."""
    c = cc.ClipContract(**_contract(input_mode="ingredients", assets=[
        _asset(),
        _asset(role="face", key="dup.png", origin="image_3"),
        _asset(role="face", key="dup.png", origin="image_2")]))
    with pytest.raises(ValueError, match="appears twice"):
        cc.validate_lane(c, "movie-section")


def test_an_unknown_render_method_is_refused():
    with pytest.raises(ValueError, match="is not one of"):
        cc.validate_lane(cc.ClipContract(**_contract()), "interpolate")


# --------------------------------------------------------------------------
# 4. the accessors — the whole point is that nothing reads the column directly
# --------------------------------------------------------------------------

def test_a_pre_contract_row_reads_as_none():
    """NULL version means "made before the contract existed", which is the
    house NULL-means-legacy convention. It is not an error."""
    row = _Row(version=None, raw=None)
    assert cc.read_contract(row) is None
    assert cc.read_contract_json(row) is None


def test_a_row_with_a_stale_json_but_no_version_is_still_legacy():
    """The version column is the scope, not the blob. A row carrying JSON with
    no version stamp has not been admitted to the contract."""
    row = _Row(version=None, raw=json.dumps(_contract()))
    assert cc.read_contract(row) is None
    assert cc.read_contract_json(row) is None


def test_read_contract_json_returns_the_stored_bytes_unchanged():
    """The heart of stage 3's exit criterion A. Deliberately stored with odd
    key order and unusual separators: a pydantic round-trip would normalise
    both, and then byte-equality would prove nothing."""
    stored = ('{"assets":[{"role":"start_frame","media":"image",'
              '"key":"jobs/x/frames/a.png",'
              '"url":"https://h/api/local-worker/frames/x/a.png",'
              '"origin":"image_1"}],'
              '"clip_contract_version":1,"input_mode":"frames",'
              '"isolate_project":false,'
              '"policy_fallback":["prompt_b","model_swap","fail"],'
              '"veo_model":"Veo 3.1 - Lite [Lower Priority]","duration_s":8,'
              '"aspect_ratio":"9:16","variants":2,"resolution":"720p",'
              '"swap_mode":null,"swap_max_source_s":null}')
    row = _Row(version=1, raw=stored)

    assert cc.read_contract_json(row) == stored
    assert cc.read_contract_json(row) is row.clip_contract_json

    parsed = cc.read_contract(row)
    assert parsed.input_mode == "frames"
    assert parsed.assets[0].role is cc.Role.START_FRAME
    # and the round-trip really is NOT byte-identical, which is why the getter
    # returns the raw string
    assert parsed.model_dump_json() != stored


def test_a_stamped_row_with_unreadable_json_raises():
    """A clip that claims a version and cannot produce a contract is a fault,
    not a legacy row. Log-and-continue here would be failing open."""
    row = _Row(version=1, raw="{not json")
    with pytest.raises(Exception):
        cc.read_contract(row)


def test_a_stamped_row_with_an_empty_string_reads_as_none():
    assert cc.read_contract_json(_Row(version=1, raw="   ")) is None


def test_stamped_clip_filter_asks_the_column_not_the_blob():
    """It takes the mapped class instead of importing it, so this module keeps
    no project imports and the ONE version predicate still lives here."""
    class _Col:
        def isnot(self, other):
            return ("isnot", other)

    class _FakeClip:
        clip_contract_version = _Col()

    assert cc.stamped_clip_filter(_FakeClip) == ("isnot", None)


# --------------------------------------------------------------------------
# 5. the parser — line attachment, and the dangling fallback for a silent scene
#
# The convention is not new. A per-line bullet on a scene with ZERO lines is
# held in a `dangling_*` variable and emitted as a one-entry list, with three
# precedents in the same loop: v786 action_note, v861 clip_duration_s, v961
# veo_model (`code/image_platform.py:6425-6445`). These three copy it.
# --------------------------------------------------------------------------

import image_platform as ip  # noqa: E402


def _scenes(md):
    return ip._parse_scene_blocks_new(md, known_image_indexes={1, 2, 3})


SILENT = """
### Scene 1
- **image:** image_1
- **scene_type:** shot
- **speaker:** silent
- **input_mode:** ingredients
- **isolate_project:** true
- **policy_fallback:** prompt_b, fail
- **clip_duration_s:** 8
- **action_note:** she lifts the jar
"""

TWO_LINES = """
### Scene 1
- **image:** image_1
- **scene_type:** shot
- **line:** first line here
- **input_mode:** frames
- **isolate_project:** false
- **policy_fallback:** fail
- **clip_duration_s:** 4
- **line:** second line here
- **input_mode:** ingredients
- **isolate_project:** true
- **policy_fallback:** prompt_b, model_swap, fail
- **clip_duration_s:** 6
"""


def test_a_silent_shot_scene_keeps_its_contract_bullets():
    """No `line` bullet to attach to, and the scene still renders a clip. The
    dangling hold is what stops a declaration being parsed, validated and then
    thrown away."""
    s = _scenes(SILENT)[0]
    assert s["lines"] == []
    assert s["clip_input_modes"] == ["ingredients"]
    assert s["clip_isolate_projects"] == [True]
    assert s["clip_policy_fallbacks"] == [["prompt_b", "fail"]]
    # and the scene-level resolution, which is what actually reaches a Clip
    assert s["input_mode"] == "ingredients"
    assert s["isolate_project"] is True
    assert s["policy_fallback"] == ["prompt_b", "fail"]


def test_two_lines_in_one_scene_do_not_leak_into_each_other():
    s = _scenes(TWO_LINES)[0]
    assert len(s["lines"]) == 2
    assert s["clip_input_modes"] == ["frames", "ingredients"]
    assert s["clip_isolate_projects"] == [False, True]
    assert s["clip_policy_fallbacks"] == [
        ["fail"], ["prompt_b", "model_swap", "fail"]]
    # first declared value in the scene wins for the scene-level answer
    assert s["input_mode"] == "frames"


def test_a_scene_with_no_contract_bullets_is_untouched():
    """347 builds carry none of these. They must parse exactly as before."""
    s = _scenes("""
### Scene 1
- **image:** image_1
- **scene_type:** shot
- **line:** just a normal line
- **clip_duration_s:** 4
""")[0]
    assert s["clip_input_modes"] == [None]
    assert s["input_mode"] is None
    assert s["isolate_project"] is None
    assert s["policy_fallback"] is None


def test_the_parser_normalises_case_but_the_model_stays_strict():
    """Normalise at the edge, be strict inside -- the same shape `veo_model`
    uses (`normalize_veo_model` then `is_legal_veo_model`). An author writing
    the UI's label `Ingredients` gets it stored as `ingredients`; the model
    itself never accepts the capitalised form, because by the time a contract
    object exists the value has already been through the parser."""
    s = _scenes("""
### Scene 1
- **image:** image_1
- **scene_type:** shot
- **line:** a line
- **input_mode:** Ingredients
""")[0]
    assert s["input_mode"] == "ingredients"
    with pytest.raises(Exception):
        cc.ClipContract(**_contract(input_mode="Ingredients"))


@pytest.mark.parametrize("bullet,bad", [
    ("input_mode", "frames-tab"),
    ("isolate_project", "yes"),      # explicitly not accepted
    ("isolate_project", "1"),
    ("policy_fallback", "prompt_b"),             # never terminates
    ("policy_fallback", "fail, prompt_b"),       # fail before the end
    ("policy_fallback", "prompt_b, teleport, fail"),  # unknown rung
])
def test_a_bad_contract_bullet_fails_the_import(bullet, bad):
    md = f"""
### Scene 1
- **image:** image_1
- **scene_type:** shot
- **line:** a line
- **{bullet}:** {bad}
"""
    with pytest.raises(ValueError, match="v965"):
        _scenes(md)
