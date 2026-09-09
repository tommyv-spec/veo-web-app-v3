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
    with pytest.raises(ValueError, match="missing its 'face'"):
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


SILENT = """CLIP CONTRACT: v1
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

TWO_LINES = """CLIP CONTRACT: v1
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
    s = _scenes("""CLIP CONTRACT: v1
### Scene 1
- **image:** image_1
- **scene_type:** shot
- **line:** a line
- **input_mode:** Ingredients
- **isolate_project:** true
- **policy_fallback:** fail
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


# --------------------------------------------------------------------------
# 6. the opt-in — a build is only in scope if it says so
# --------------------------------------------------------------------------

IN_SCOPE_HEADER = "## §0\nCLIP CONTRACT: v1\n"

FULL = """
### Scene 1
- **image:** image_1
- **scene_type:** shot
- **line:** a line
- **input_mode:** ingredients
- **isolate_project:** true
- **policy_fallback:** prompt_b, fail
"""


def test_an_out_of_scope_build_is_never_judged():
    """345 of the 347 builds carry nothing. They must parse exactly as before,
    and come out unstamped."""
    s = _scenes(FULL.replace("- **input_mode:** ingredients\n", "")
                    .replace("- **isolate_project:** true\n", "")
                    .replace("- **policy_fallback:** prompt_b, fail\n", ""))[0]
    assert s["clip_contract_json"] is None
    assert s["clip_contract_version"] is None


def test_an_in_scope_build_is_stamped():
    s = _scenes(IN_SCOPE_HEADER + FULL)[0]
    assert s["clip_contract_version"] == cc.CONTRACT_VERSION
    d = cc.ClipContractDeclaration.model_validate_json(s["clip_contract_json"])
    assert d.input_mode == "ingredients"
    assert d.isolate_project is True
    assert d.policy_fallback == ["prompt_b", "fail"]


@pytest.mark.parametrize("drop", [
    "- **input_mode:** ingredients\n",
    "- **isolate_project:** true\n",
    "- **policy_fallback:** prompt_b, fail\n",
])
def test_an_in_scope_shot_scene_refuses_a_missing_bullet(drop):
    """Opting in and then leaving a scene undeclared is the exact hole the rule
    exists to close — the worker would have to guess again."""
    md = IN_SCOPE_HEADER + FULL.replace(drop, "")
    with pytest.raises(ValueError, match="CLIP CONTRACT: v1"):
        _scenes(md)


def test_a_text_card_scene_refuses_contract_bullets():
    """A card is drawn by ffmpeg and never submitted, so there is no tab to
    pick and no project to isolate."""
    md = IN_SCOPE_HEADER + """
### Scene 1
- **scene_type:** text_card
- **caption:** hello
- **bg_color:** #000000
- **input_mode:** frames
"""
    with pytest.raises(ValueError, match="text_card scenes take no input_mode"):
        _scenes(md)


def test_a_text_card_in_an_in_scope_build_is_fine_without_bullets():
    md = IN_SCOPE_HEADER + """
### Scene 1
- **scene_type:** text_card
- **caption:** hello
- **bg_color:** #000000
"""
    s = _scenes(md)[0]
    assert s["clip_contract_json"] is None
    assert s["clip_contract_version"] is None


# --------------------------------------------------------------------------
# 7. the carry — every surface between the parser and the Clip writer
#
# These are source-level assertions on purpose. The failure this guards is a
# BRANCH BEING MISSED, and the file's own comments say why that is the trap:
# "a binding that lives only on the per-scene payload arrives as None on every
# clip" (v718i.2, quoted at image_platform.py:11454-11458). A behavioural test
# that happens to exercise the spoken branch would pass while the silent branch
# was still empty. Counting the branches is the check that cannot be fooled.
# Same technique as code/test_charswap_render_method.py:35.
# --------------------------------------------------------------------------

IP_SRC = (_HERE / "image_platform.py").read_text(encoding="utf-8")


def test_the_declaration_is_read_from_the_scene_exactly_once():
    assert IP_SRC.count('_v965_contract = scene.get("clip_contract_json")') == 1
    assert IP_SRC.count('_v965_version = scene.get("clip_contract_version")') == 1


def test_all_three_payloads_carry_the_declaration():
    """One scene payload plus BOTH flat-row branches, spoken and silent."""
    assert IP_SRC.count('"clip_contract_json": _v965_contract,') == 3
    assert IP_SRC.count('"clip_contract_version": _v965_version,') == 3


def test_the_assignment_constructor_passes_it():
    """The half check_field_plumbing.py blocked this commit over: a column with
    no constructor kwarg is a value that is parsed and thrown away."""
    assert 'clip_contract_json=s.get("clip_contract_json")' in IP_SRC
    assert 'clip_contract_version=s.get("clip_contract_version")' in IP_SRC


def test_both_flat_row_branches_are_still_two():
    """The count above only means something while there are exactly two
    flat-row branches. If a third appears, this fails and someone has to look
    rather than quietly bump a number."""
    # the flat-row branches are the ones indented inside a per-line loop
    assert IP_SRC.count(
        "                # v965 - the clip contract declaration, denormed onto the line".replace(
            " - ", " — ")) == 2
    # and the scene payload is the one that is not
    assert IP_SRC.count(
        "            # v965 - the clip contract declaration.".replace(
            " - ", " — ")) == 1


# --------------------------------------------------------------------------
# 8. the API surface — pydantic drops what it is not told about
# --------------------------------------------------------------------------

def test_the_request_model_declares_and_validates_the_contract():
    """`main.py:302-306` states the rule this guards: an undeclared field
    reaches the Clip row as NULL and the whole chain looks wired while doing
    nothing. Both fields must be declared, and a string that cannot be read
    back must be refused HERE rather than on a render slot."""
    from main import DialogueLineInput

    assert "clip_contract_json" in DialogueLineInput.model_fields
    assert "clip_contract_version" in DialogueLineInput.model_fields

    good = cc.ClipContractDeclaration(
        clip_contract_version=1, input_mode="frames",
        isolate_project=False, policy_fallback=["fail"]).model_dump_json()
    m = DialogueLineInput(id=1, text="hi", clip_contract_json=good,
                          clip_contract_version=1)
    assert m.clip_contract_json == good

    for bad in ("{not json", '{"input_mode":"frames"}', '{"clip_contract_version":9}'):
        with pytest.raises(Exception):
            DialogueLineInput(id=1, text="hi", clip_contract_json=bad)

    # empty normalises to None, like every other optional field on the model
    assert DialogueLineInput(id=1, text="hi", clip_contract_json="  ").clip_contract_json is None


def test_the_response_model_surfaces_the_contract():
    """Without this the field is invisible over the API and stage 3 could not
    ask 'did this clip get stamped?' without going to the database."""
    from main import ClipResponse

    assert "clip_contract_json" in ClipResponse.model_fields
    assert "clip_contract_version" in ClipResponse.model_fields


def test_the_frontend_sends_it_too():
    """The field-plumbing checker is satisfied by the SERVER-side promote, so a
    miss in the browser payload would be completely silent: the build imports
    in scope, every surface looks wired, and the clip reaches the worker
    unstamped and renders by inference exactly as before."""
    html = (_HERE / "static" / "index.html").read_text(encoding="utf-8")
    assert "clip_contract_json: promoteMeta.clip_contract_json" in html
    assert "clip_contract_version:" in html


# --------------------------------------------------------------------------
# 9. hand-out — the contract is resolved PER LANE, and only for a stamped clip
# --------------------------------------------------------------------------

class _FakeClip:
    """Only the columns the hand-out helper reads."""

    def __init__(self, **kw):
        self.id = "c1"
        self.job_id = "job123"
        self.start_frame = "jobs/job123/frames/img1.png"
        self.end_frame = None
        self.render_method = None
        self.swap_mode = None
        self.swap_source_r2_key = None
        self.swap_avatar_upload_id = None
        self.face_ref_frames_json = None
        self.veo_model = "Veo 3.1 - Lite [Lower Priority]"
        self.veo_render_duration_s = 8
        self.aspect_ratio = "9:16"
        self.flow_variants_count = 2
        self.resolution = "720p"
        self.clip_contract_version = None
        self.clip_contract_json = None
        for k, v in kw.items():
            setattr(self, k, v)


def _decl(**kw):
    base = dict(clip_contract_version=1, input_mode="frames",
                isolate_project=False, policy_fallback=["fail"])
    base.update(kw)
    return cc.ClipContractDeclaration(**base).model_dump_json()


def test_an_unstamped_clip_payload_is_byte_identical():
    """The scoping mechanism. The worker asks 'did a contract arrive', so an
    unstamped clip must look exactly as it does today."""
    from main import _v965_attach_contract

    before = {"id": "c1", "start_frame_url": "u"}
    after = _v965_attach_contract(dict(before), _FakeClip(), "https://h", "local-worker")
    assert after == before


def test_a_stamped_simple_clip_gets_a_resolved_contract():
    from main import _v965_attach_contract

    clip = _FakeClip(clip_contract_version=1,
                     clip_contract_json=_decl(input_mode="frames"))
    out = _v965_attach_contract({}, clip, "https://h", "local-worker")
    c = out["clip_contract"]
    assert c["input_mode"] == "frames"
    assert c["resolution"] == "720p"
    assert [a["role"] for a in c["assets"]] == ["start_frame"]
    assert c["assets"][0]["key"] == "jobs/job123/frames/img1.png"
    # and the declaration travels byte-unchanged beside it
    assert out["clip_contract_declared"] == clip.clip_contract_json


def test_the_url_is_lane_specific_but_the_key_is_not():
    """Why the asset list cannot be stored on the row: the two lanes carry
    different credentials and get different urls for identical bytes."""
    from main import _v965_attach_contract

    def _for(lane):
        clip = _FakeClip(clip_contract_version=1, clip_contract_json=_decl())
        return _v965_attach_contract({}, clip, "https://h", lane)["clip_contract"]["assets"][0]

    a, b = _for("local-worker"), _for("user-worker")
    assert a["key"] == b["key"]
    assert a["url"] != b["url"]
    assert "/api/local-worker/" in a["url"] and "/api/user-worker/" in b["url"]


def test_a_movie_section_clip_carries_scene_then_faces_in_order():
    """Order is ATTACH order and it is normative: scene chip first, then faces
    in list order."""
    from main import _v965_attach_contract

    clip = _FakeClip(
        render_method="movie-section",
        clip_contract_version=1, clip_contract_json=_decl(input_mode="ingredients"),
        face_ref_frames_json=json.dumps(["jobs/job123/frames/f0.png",
                                         "jobs/job123/frames/f1.png"]))
    c = _v965_attach_contract({}, clip, "https://h", "local-worker")["clip_contract"]
    assert [a["role"] for a in c["assets"]] == ["start_frame", "face", "face"]
    assert [a["origin"] for a in c["assets"][1:]] == ["f0.png", "f1.png"]


def test_a_video_led_charswap_carries_avatar_and_video_and_no_start_frame():
    from main import _v965_attach_contract

    clip = _FakeClip(
        render_method="charswap", swap_mode="video-led",
        clip_contract_version=1, clip_contract_json=_decl(input_mode="ingredients"),
        swap_avatar_upload_id=77,
        swap_source_r2_key="swap-sources/u1/dance.mp4")
    c = _v965_attach_contract({}, clip, "https://h", "local-worker")["clip_contract"]
    roles = [a["role"] for a in c["assets"]]
    assert roles == ["avatar", "swap_source"]
    assert "start_frame" not in roles
    src = [a for a in c["assets"] if a["role"] == "swap_source"][0]
    assert src["media"] == "video"
    assert c["swap_mode"] == "video-led"
    assert c["swap_max_source_s"] == cc.SWAP_MAX_SOURCE_S


def test_an_image_led_charswap_keeps_its_start_frame():
    from main import _v965_attach_contract

    clip = _FakeClip(
        render_method="charswap", swap_mode="image-led",
        clip_contract_version=1, clip_contract_json=_decl(input_mode="ingredients"),
        swap_avatar_upload_id=77,
        swap_source_r2_key="swap-sources/u1/dance.mp4")
    c = _v965_attach_contract({}, clip, "https://h", "local-worker")["clip_contract"]
    assert "start_frame" in [a["role"] for a in c["assets"]]


def test_a_lane_violation_is_refused_at_hand_out():
    """The second door. The parser checks the author's tokens; this one sees
    the RESOLVED assets, and a charswap clip whose avatar never materialised
    would otherwise reach a render slot."""
    from main import _v965_attach_contract

    clip = _FakeClip(
        render_method="charswap", swap_mode="video-led",
        clip_contract_version=1, clip_contract_json=_decl(input_mode="ingredients"),
        swap_avatar_upload_id=None,
        swap_source_r2_key="swap-sources/u1/dance.mp4")
    with pytest.raises(ValueError, match="missing its 'avatar' asset"):
        _v965_attach_contract({}, clip, "https://h", "local-worker")


def test_both_polling_endpoints_call_the_same_helper():
    """The two endpoints hand-build their dicts independently; this is the one
    place the contract does not get a second, drifting implementation."""
    src = (_HERE / "main.py").read_text(encoding="utf-8")
    assert src.count("_v965_attach_contract(clip_data, clip, base_url, \"local-worker\")") == 1
    assert src.count("_v965_attach_contract(_clip_data, clip, base_url, \"user-worker\")") == 1


# --------------------------------------------------------------------------
# 10. the worker side — read, ledger, diag. Nothing applied yet.
#
# Read out of the real worker source and executed, the same way
# code/test_charswap_render_method.py:36 does it: importing flow_worker.py
# boots a browser driver. This runs the shipped code, not a paraphrase.
# --------------------------------------------------------------------------

WORKER_SRC = _HERE / "static" / "flow_worker.py"


def _worker_ns(*names):
    """The named functions PLUS the v965 module constants they read.

    The constants have to come along or the extracted function raises NameError
    on its first lookup — which is itself worth knowing, because it means the
    table and the function are genuinely coupled and neither works alone.
    """
    src = WORKER_SRC.read_text(encoding="utf-8")
    ns = {"os": __import__("os"), "json": json,
          "datetime": __import__("datetime").datetime,
          "_hashlib": __import__("hashlib"),
          # the constants block computes the diag path from __file__
          "__file__": str(WORKER_SRC)}
    # Anchor on the NAME, never the value. The first version of this sliced on
    # "V965_APPLY = False" and all 13 of these tests broke the moment the switch
    # was turned on — the helper was pinned to a value it does not care about.
    consts = src[src.index("\nV965_APPLY"):src.index("\ndef v965_write_diag(")]
    exec(consts, ns)  # noqa: S102 — our own file, on purpose
    for name in names:
        start = src.index(f"\ndef {name}(")
        rest = src[start + 1:]
        end = rest.index("\ndef ", 1)
        exec(rest[:end], ns)  # noqa: S102 — our own file, on purpose
    return ns


def test_the_worker_scopes_on_presence_not_on_a_version():
    """The worker never reads clip_contract_version to decide anything. The
    server attaches a contract only for a stamped clip, so 'a contract is here'
    and 'this clip is in scope' are the same statement, decided in one place."""
    ns = _worker_ns("v965_contract_of")
    of = ns["v965_contract_of"]
    assert of({"id": "c1"}) is None
    assert of({"id": "c1", "clip_contract": {}}) is None
    assert of({"id": "c1", "clip_contract": {"input_mode": "frames"}}) == {
        "input_mode": "frames"}
    assert of("not a dict") is None


def test_no_version_comparison_is_written_in_the_worker():
    """Step 1.10's static check in miniature, and the reason the worker scopes
    on presence: it imports nothing from code/, so it could not route a version
    predicate through the accessor even if it wanted one."""
    src = WORKER_SRC.read_text(encoding="utf-8")
    assert "clip_contract_version ==" not in src
    assert "clip_contract_version >" not in src
    assert "import clip_contract" not in src


def test_every_contract_field_gets_a_ledger_row():
    """The critical list IS the contract's own field list, so it cannot omit a
    field that exists. That is the structural fix for v945.15 — 'a verifier
    whose critical list omits the setting that varies is a rubber stamp for
    exactly that setting'."""
    ns = _worker_ns("v965_build_ledger")
    contract = json.loads(cc.ClipContract(
        clip_contract_version=1, input_mode="frames", isolate_project=True,
        policy_fallback=["fail"], veo_model="Veo 3.1 - Lite [Lower Priority]",
        duration_s=8, aspect_ratio="9:16", variants=2, resolution="720p",
        swap_mode=None, swap_max_source_s=None,
        assets=[{"role": "start_frame", "media": "image", "key": "k",
                 "url": "u", "origin": "image_1"}],
    ).model_dump_json())

    ledger = ns["v965_build_ledger"](contract)
    assert {r["field"] for r in ledger} == set(contract)
    assert len(ledger) == 12


def test_an_unknown_field_is_loud_not_skipped():
    """A key the server sent that this worker build has never heard of means
    the server is AHEAD of the worker. Skipping it silently is the failure."""
    ns = _worker_ns("v965_build_ledger")
    ledger = ns["v965_build_ledger"]({"something_new": "x"})
    assert ledger[0]["state"] == "UNAPPLIED"
    assert ledger[0]["action"] == "unknown-field"


def test_with_apply_off_every_ui_field_reads_unapplied():
    """The honest answer at plan stage 1: nothing was applied. If these came
    back APPLIED with the switch off, the ledger would be lying."""
    ns = _worker_ns("v965_build_ledger")
    ledger = {r["field"]: r for r in ns["v965_build_ledger"]({
        "input_mode": "frames", "resolution": "720p", "isolate_project": True,
        "policy_fallback": ["fail"], "assets": []})}
    assert ledger["input_mode"]["state"] == "UNAPPLIED"
    assert ledger["resolution"]["state"] == "UNAPPLIED"
    # and the ones with no control say so rather than pretending
    assert ledger["isolate_project"]["state"] == "HONOURED_BY_SCHEDULER"
    assert ledger["policy_fallback"]["state"] == "ARMED"
    assert ledger["assets"]["state"] == "NO_UI_ACTION"


def test_a_read_back_that_differs_is_its_own_state():
    """Set-and-differs is not the same fault as never-set, and telling them
    apart is the difference between 'the click missed' and 'the code never
    tried'."""
    ns = _worker_ns("v965_build_ledger")
    rows = {r["field"]: r for r in ns["v965_build_ledger"](
        {"resolution": "1080p"}, applied={"resolution": "720p"})}
    assert rows["resolution"]["state"] == "READ_BACK_DIFFERS"
    rows = {r["field"]: r for r in ns["v965_build_ledger"](
        {"resolution": "720p"}, applied={"resolution": "720p"})}
    assert rows["resolution"]["state"] == "APPLIED"


def test_both_switches_ship_off():
    """Stage 1 applies nothing and refuses nothing. Turning either on is a
    separate, deliberate commit."""
    src = WORKER_SRC.read_text(encoding="utf-8")
    assert "\nV965_APPLY = True\n" in src
    assert "\nV965_ASSERT = False\n" in src


# --------------------------------------------------------------------------
# 11. parity — the worker's table cannot fall behind the server's model
# --------------------------------------------------------------------------

def test_every_contract_field_has_a_worker_entry():
    """The parity check. If someone adds a 13th field to ClipContract and does
    not teach the worker what it is, this fails at CI instead of the field
    reading UNAPPLIED on a live clip and refusing it at stage 4."""
    ns = _worker_ns("v965_contract_of")
    covered = set(ns["V965_UI_FIELDS"]) | set(ns["V965_NO_UI_FIELDS"])
    missing = set(cc.ClipContract.model_fields) - covered
    assert not missing, (
        f"the worker has no entry for {sorted(missing)} — add it to "
        f"V965_UI_FIELDS or V965_NO_UI_FIELDS in static/flow_worker.py")
    stale = covered - set(cc.ClipContract.model_fields)
    assert not stale, (
        f"the worker names {sorted(stale)}, which the contract no longer has")


def test_the_worker_advertises_the_contract_level_on_every_poll():
    """A worker reads the served flow_worker.py once, when it starts. Shipping
    the advertisement BEFORE the server gate is what lets the server hold a
    stamped clip back from a worker running older code."""
    src = WORKER_SRC.read_text(encoding="utf-8")
    assert "&contract={V965_CONTRACT_LEVEL}" in src
    # all three poll urls go through the one builder, so one change covers them
    assert src.count("_worker_arms_q()") >= 3


# --------------------------------------------------------------------------
# 12. the allowlisted static check (F5)
#
# NOT a blanket ban on the string `clip_contract_json`: the column has to be
# declared, migrated, serialized and accepted, and this repo's own plan puts
# that name in image_platform.py, models.py and main.py. Banning it would fail
# by construction. The real target is scattered VALUE reads with hand-written
# null-guards — the way render_method is read at ~15 sites with no shared
# accessor, which is the drift v892.x is scarred from.
# --------------------------------------------------------------------------

# Files allowed to name the column at all, and why.
_PLUMBING_ALLOWED = {
    "clip_contract.py": "the accessors themselves",
    "image_platform.py": "schema, migrations, the parser and to_dict()",
    "models.py": "the ORM column and to_dict()",
    "main.py": "the API models and the hand-out helper",
    "test_clip_contract.py": "these tests",
    "check_field_plumbing.py": "the plumbing checker's own field list",
}

# What a VALUE read looks like when it bypasses the accessor.
_BANNED_READ_PATTERNS = (
    'clip.clip_contract_json',
    'getattr(clip, "clip_contract_json"',
    "getattr(clip, 'clip_contract_json'",
)


def test_no_module_reads_the_contract_value_outside_the_accessor():
    offenders = []
    for path in sorted((_HERE).glob("*.py")):
        if path.name in ("clip_contract.py", "test_clip_contract.py"):
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        for pat in _BANNED_READ_PATTERNS:
            if pat in text:
                offenders.append(f"{path.name}: {pat}")
    assert not offenders, (
        "[v965] contract field read outside the accessor: "
        + "; ".join(offenders)
        + ". Read it with clip_contract.read_contract_json(clip) — see "
          "CONTRACT.md section 5.3.")


def test_the_allowlist_is_about_plumbing_not_secrecy():
    """The check permits the name where the column genuinely has to live, so a
    future reader does not 'fix' it by hiding a legitimate declaration."""
    ip = (_HERE / "image_platform.py").read_text(encoding="utf-8")
    assert "clip_contract_json" in ip          # column + parser: allowed
    mn = (_HERE / "main.py").read_text(encoding="utf-8")
    assert "clip_contract_json" in mn          # API model: allowed
    # ...but neither of them reaches past the accessor for the VALUE
    assert "clip.clip_contract_json" not in mn


def test_an_indented_mention_does_not_opt_a_build_in():
    """A §0 declaration sits at column 0. `template_new_format.md` documents
    the opt-in inside an INDENTED html comment, and a build that quoted the
    line in a comment or a fenced block must not opt itself in silently."""
    md = "<!-- docs say:\n         CLIP CONTRACT: v1\n -->\n" + FULL
    # The build is NOT in scope. And because it carries the bullets anyway, the
    # parser now REFUSES it outright instead of ignoring them — the stronger
    # answer, because silently dropping a declaration the author wrote is
    # exactly the failure this rule exists to remove.
    with pytest.raises(ValueError, match="clip contract bullet"):
        _scenes(md)

    # with the bullets removed too, it simply parses as an ordinary build
    plain = md.replace("- **input_mode:** ingredients\n", "") \
              .replace("- **isolate_project:** true\n", "") \
              .replace("- **policy_fallback:** prompt_b, fail\n", "")
    assert _scenes(plain)[0]["clip_contract_version"] is None


def test_the_skeleton_itself_is_not_in_scope():
    """The real case that found this: the shipped skeleton mentions the opt-in
    and must not be read as an in-scope build."""
    src = (_HERE / "template_new_format.md").read_text(encoding="utf-8")
    assert "CLIP CONTRACT: v1" in src, "the skeleton should document the opt-in"
    import re as _re2
    assert not _re2.search(r"^CLIP CONTRACT:\s*v1\s*$", src, _re2.M), (
        "the skeleton has the opt-in at column 0 and would parse as in-scope")


# --------------------------------------------------------------------------
# 13. the applier — the contract becomes the SOURCE, and the ledger is honest
# --------------------------------------------------------------------------

class _Page:
    """Just enough page for the applier. No browser."""
    pass


def _apply_ns():
    return _worker_ns("v965_declared_input_mode", "v965_apply_contract",
                      "v965_build_ledger", "v965_ledger_from_page")


def test_a_declared_mode_is_read_not_derived():
    ns = _apply_ns()
    p = _Page()
    assert ns["v965_declared_input_mode"](p) is None      # nothing declared
    p._v965_contract = {"input_mode": "ingredients"}
    assert ns["v965_declared_input_mode"](p) is True
    p._v965_contract = {"input_mode": "frames"}
    assert ns["v965_declared_input_mode"](p) is False


def test_the_declared_mode_wins_over_the_old_inference():
    """The whole point. The old rule would infer Frames here (Omni, no end
    frame); the contract says Ingredients, and the contract wins."""
    src = WORKER_SRC.read_text(encoding="utf-8")
    i = src.index("def _omni_ingredients_mode(")
    body = src[i:src.index("\ndef ", i + 1)]
    assert '_v965_c.get("input_mode")' in body, (
        "the declared mode must be read INSIDE this function")
    assert body.index("_v965_contract") < body.index("is_omni("), (
        "the declared answer must be consulted BEFORE the inference")
    # and it must stay self-contained: the worker's tests lift this function
    # out as text and exec it, so a helper call here is a NameError in 30 of
    # them. This assertion is what stops someone tidying it into a call.
    assert "v965_declared_input_mode(" not in body, (
        "keep this inline — test_charswap_render_method.py execs this function "
        "alone and a helper call breaks 30 tests")


def test_apply_is_a_no_op_while_the_switch_is_off():
    """Stage 1 and 2 ship with V965_APPLY off; nothing may change until it is
    deliberately turned on."""
    ns = _apply_ns()
    # force the switch OFF here rather than depending on what shipped — this
    # test is about the off-behaviour, so it must not change meaning the day
    # the switch is turned on.
    ns["V965_APPLY"] = False
    p = _Page()
    assert ns["v965_apply_contract"](p, {"veo_model": "Omni Flash"}) is False
    assert not hasattr(p, "_veo_model")


def test_the_contract_becomes_the_source_when_the_switch_is_on():
    ns = _apply_ns()
    ns["V965_APPLY"] = True                     # the switch, in this namespace
    p = _Page()
    c = {"input_mode": "ingredients", "veo_model": "Omni Flash",
         "duration_s": 10, "resolution": "720p", "variants": 2}
    assert ns["v965_apply_contract"](p, c) is True
    assert p._veo_model == "Omni Flash"
    assert p._duration == "10"
    assert p._resolution == "720p"
    assert p._v965_contract is c


def test_the_ledger_reports_a_failed_pick_instead_of_hiding_it():
    """`Resolution` is absent from the settings pass's critical list, so a
    failed pick reports success today. The ledger is what makes it visible."""
    ns = _apply_ns()
    p = _Page()
    contract = {"input_mode": "frames", "resolution": "720p",
                "duration_s": 8, "veo_model": "Omni Flash",
                "aspect_ratio": "9:16", "variants": 2}
    p._v965_mode_key = "Frames"
    p._v965_applied = {"Frames": True, "Resolution": False, "Duration": True,
                       "Model": True, "Portrait": True, "Variants": True}
    rows = {r["field"]: r for r in ns["v965_ledger_from_page"](p, contract)}
    assert rows["resolution"]["state"] == "READ_BACK_DIFFERS"
    assert rows["duration_s"]["state"] == "APPLIED"
    assert rows["input_mode"]["state"] == "APPLIED"


def test_a_contract_does_not_leak_to_the_next_clip():
    """The page is reused across clips in a shared project. An unstamped clip
    following a stamped one must not inherit the declaration — that would look
    declared while being wrong, which is worse than the inference it replaces."""
    ns = _worker_ns("v965_contract_of", "v965_declared_input_mode",
                    "v965_apply_contract", "v965_build_ledger",
                    "v965_write_diag", "v965_observe_contract")
    ns["V965_APPLY"] = True
    p = _Page()

    ns["v965_observe_contract"]({"id": "c1", "clip_contract": {
        "input_mode": "ingredients", "clip_contract_version": 1}}, page=p)
    assert ns["v965_declared_input_mode"](p) is True

    ns["v965_observe_contract"]({"id": "c2"}, page=p)     # unstamped
    assert ns["v965_declared_input_mode"](p) is None, (
        "clip 2 inherited clip 1's contract")


def test_aspect_and_variants_are_driven_by_the_contract_not_hardcoded():
    """The bug this test exists for: `Portrait` was a hardcoded "9:16" and
    `Variants` came from a function ARGUMENT. A build declaring anything else
    would have been ignored while the ledger said APPLIED — a verifier
    rubber-stamping the setting that varies, which is v945.15 exactly."""
    src = WORKER_SRC.read_text(encoding="utf-8")
    i = src.index("def _v962_material_video_settings(")
    body = src[i:src.index("\ndef ", i + 1)]
    assert '_v962_pick_radio(page, "9:16", "Aspect"' not in body, (
        "aspect is hardcoded again — a declared aspect_ratio would be ignored")
    assert 'f"x{variants_count}", "Variants"' not in body, (
        "variants ignores the contract again")
    assert '_v965_cs.get("aspect_ratio")' in body
    assert '_v965_cs.get("variants")' in body
    # and it must still work with no contract at all
    assert 'or "9:16"' in body and "or variants_count" in body


# --------------------------------------------------------------------------
# 14. the refusal — V965_ASSERT must actually refuse, not just exist
# --------------------------------------------------------------------------

def test_the_assert_switch_is_read_by_a_real_refusal_path():
    """Codex found this: the switch existed and NOTHING read it except a
    diagnostic string, so turning it on would have promised fail-closed
    behaviour that did not exist."""
    src = WORKER_SRC.read_text(encoding="utf-8")
    i = src.index("def _v962_material_video_settings(")
    body = src[i:src.index("\ndef ", i + 1)]
    assert "if V965_ASSERT and _v965_bad:" in body, (
        "no refusal path reads V965_ASSERT")
    # the refusal must come BEFORE the pass reports success
    assert body.index("if V965_ASSERT and _v965_bad:") < body.index("return not missing")
    assert "return False" in body


def test_the_refusal_happens_before_generate_is_ever_clicked():
    """A refusal after the click would cost a render, which is the whole thing
    it exists to avoid. The settings pass runs before the Generate button by
    construction — assert that the refusal lives there and not in the click."""
    src = WORKER_SRC.read_text(encoding="utf-8")
    i = src.index("def _v962_material_video_settings(")
    body = src[i:src.index("\ndef ", i + 1)]
    assert "_V962_GENERATE_BTN" not in body, (
        "the settings pass should not be clicking Generate at all")
    assert "REFUSING before Generate" in body


def test_a_ledger_that_cannot_be_built_also_refuses_when_asserting():
    """Log-and-continue is failing open (v939.9). If the contract cannot be
    checked, it is not proven applied."""
    src = WORKER_SRC.read_text(encoding="utf-8")
    i = src.index("def _v962_material_video_settings(")
    body = src[i:src.index("\ndef ", i + 1)]
    tail = body[body.index("ledger write failed"):]
    assert "if V965_ASSERT:" in tail and "return False" in tail


# --------------------------------------------------------------------------
# 15. declared on the response model is NOT the same as populated
#
# The bug this section exists for cost an afternoon. `clip_contract_json` was
# declared on ClipResponse, so it appeared in the live OpenAPI schema and every
# clip returned it as None -- while the database row was correct all along. I
# read that None as "the platform dropped the contract" and went hunting a
# transport bug that did not exist. The codebase already knew: the comment
# beside `render_method=c.render_method` says "on the model is not enough and
# the row reads its default forever".
# --------------------------------------------------------------------------

def test_every_clipresponse_site_populates_the_contract():
    """A ClipResponse construction that names render_method must name the
    contract too. They are the same kind of field, written at the same sites,
    and one being present without the other is the exact hole that hid a
    correct pipeline behind a null."""
    src = (_HERE / "main.py").read_text(encoding="utf-8")
    sites = src.count("render_method=c.render_method,") + \
        src.count("render_method=clip.render_method,")
    # through the accessor, not the raw attribute: the repo's own static check
    # refuses a direct read even at a serialization site, and it is right to
    populated = src.count("clip_contract_json=_v965_read_json(")
    assert populated == sites, (
        f"{sites} ClipResponse site(s) name render_method but only {populated} "
        f"name clip_contract_json — a clip will read as unstamped while its row "
        f"is stamped")


def test_the_field_is_declared_on_the_response_model_too():
    """Both halves are needed: declared so it can be returned, populated so it
    actually is. This test is the pair to the one above."""
    src = (_HERE / "main.py").read_text(encoding="utf-8")
    i = src.index("class ClipResponse")
    body = src[i:src.index("\nclass ", i + 1)]
    assert "clip_contract_json" in body
    assert "clip_contract_version" in body


# --------------------------------------------------------------------------
# 16. v967 — a restore that can only make things worse is not a restore
# --------------------------------------------------------------------------

def test_the_golden_restore_refuses_to_overwrite_a_fresher_session():
    """Measured 2026-09-09: a rebuilt profile logged in and rendered, then the
    next launch restored the now-stale golden over it and sat at "Still waiting
    for login". Every sign-in rotates the token, so the golden a worker logged
    in FROM is dead the moment it logs in."""
    src = WORKER_SRC.read_text(encoding="utf-8")
    i = src.index("def restore_from_golden(")
    body = src[i:src.index("\ndef ", i + 1)]
    # the guard runs BEFORE the copy
    assert "[v967] SKIPPING golden restore" in body
    assert body.index("v967") < body.index("GOLDEN RESTORE: Restoring")
    # it compares the two cookie DBs, not a guess
    assert "cookies.sqlite" in body
    # and it stays overridable, because a genuinely dead session needs it
    assert "FORCE_GOLDEN_RESTORE" in body


def test_the_freshness_guard_fails_open():
    """A guard that cannot read the clock must not block a restore — that would
    turn a diagnostic into an outage."""
    src = WORKER_SRC.read_text(encoding="utf-8")
    i = src.index("def restore_from_golden(")
    body = src[i:src.index("\ndef ", i + 1)]
    tail = body[body.index("v967"):]
    assert "restoring anyway" in tail
