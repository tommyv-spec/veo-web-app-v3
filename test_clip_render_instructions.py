"""v969/v970/v971 — the clip block as a complete render instruction."""
import pytest

import image_platform as ip


def _scene(**bullets):
    """A minimal shot-scene block with the given extra bullets."""
    lines = [
        "### Scene 1",
        "",
        "- **scene_type:** shot",
        "- **image:** image_1",
        "- **target_duration_s:** 8",
        "- **action_note:** she lifts the jar. [Start beat] hands enter [End beat] jar held",
    ]
    lines += [f"- **{k}:** {v}" for k, v in bullets.items()]
    return "\n".join(lines) + "\n"


def test_attach_line_matching_the_derived_list_is_accepted():
    block = _scene(attach="image_1:start_frame")
    assert ip.parse_attach_line(block, scene_index=1,
                                derived=[("image_1", "start_frame")]) == [
        ("image_1", "start_frame")]


def test_attach_line_disagreeing_with_the_build_is_refused():
    block = _scene(attach="image_2:start_frame")
    with pytest.raises(ValueError) as exc:
        ip.parse_attach_line(block, scene_index=1,
                             derived=[("image_1", "start_frame")])
    msg = str(exc.value)
    assert "v969" in msg
    assert "image_2:start_frame" in msg      # what the author wrote
    assert "image_1:start_frame" in msg      # what the build actually says


def test_attach_line_order_is_significant():
    block = _scene(attach="image_3:face, image_2:face")
    with pytest.raises(ValueError) as exc:
        ip.parse_attach_line(block, scene_index=1,
                             derived=[("image_2", "face"), ("image_3", "face")])
    assert "order" in str(exc.value).lower()


def test_an_unknown_role_is_refused_by_name():
    block = _scene(attach="image_1:scene")
    with pytest.raises(ValueError) as exc:
        ip.parse_attach_line(block, scene_index=1,
                             derived=[("image_1", "start_frame")])
    assert "scene" in str(exc.value)
    assert "start_frame" in str(exc.value)   # the legal set is named


def test_a_malformed_entry_says_the_shape_it_wanted():
    block = _scene(attach="image_1")
    with pytest.raises(ValueError) as exc:
        ip.parse_attach_line(block, scene_index=1,
                             derived=[("image_1", "start_frame")])
    assert "image_N:role" in str(exc.value)


def test_an_out_of_scope_build_may_omit_the_line():
    """All 347 builds predate this rule. Absent + out of scope = untouched."""
    assert ip.parse_attach_line(_scene(), scene_index=1,
                                derived=[("image_1", "start_frame")],
                                in_scope=False) is None


def test_an_in_scope_shot_scene_MUST_carry_the_line():
    """A build that opted in declares the whole instruction or none of it —
    the same both-directions rule v965 already enforces on its three bullets."""
    with pytest.raises(ValueError) as exc:
        ip.parse_attach_line(_scene(), scene_index=1,
                             derived=[("image_1", "start_frame")],
                             in_scope=True)
    msg = str(exc.value)
    assert "v969" in msg
    assert "image_1:start_frame" in msg   # the line it should have written


def test_derived_simple_clip_is_start_frame_only():
    assert ip.derive_attach_tokens(
        image="image_1", end_frame_image=None, face_refs=[],
        render_method=None, swap_source_video=None) == [
        ("image_1", "start_frame")]


def test_derived_frames_pair_is_start_then_end():
    assert ip.derive_attach_tokens(
        image="image_1", end_frame_image="image_2", face_refs=[],
        render_method=None, swap_source_video=None) == [
        ("image_1", "start_frame"), ("image_2", "end_frame")]


def test_derived_movie_section_is_scene_then_faces_in_authored_order():
    assert ip.derive_attach_tokens(
        image="image_1", end_frame_image=None,
        face_refs=["image_3", "image_2"],
        render_method="movie-section", swap_source_video=None) == [
        ("image_1", "start_frame"), ("image_3", "face"), ("image_2", "face")]


def test_derived_video_led_charswap_is_avatar_then_source():
    assert ip.derive_attach_tokens(
        image="image_1", end_frame_image=None, face_refs=[],
        render_method="charswap", swap_source_video="clip.mp4",
        swap_mode="video-led") == [
        ("image_1", "avatar"), ("clip.mp4", "swap_source")]


def test_derived_image_led_charswap_ALSO_carries_a_start_frame():
    """`_v965_resolve_assets` appends the start frame for an image-led swap
    (`code/main.py:19322-19328`) and only for that mode. A mirror that omitted
    it would approve an attach line missing a file the worker is handed."""
    assert ip.derive_attach_tokens(
        image="image_1", end_frame_image=None, face_refs=[],
        render_method="charswap", swap_source_video="clip.mp4",
        swap_mode="image-led") == [
        ("image_1", "avatar"), ("clip.mp4", "swap_source"),
        ("image_1", "start_frame")]


def test_swap_mode_defaults_to_video_led_exactly_like_the_resolver():
    """`getattr(clip, "swap_mode", None) or "video-led"` is the resolver's
    default (`code/main.py:19323`); the mirror must not invent a different one."""
    assert ip.derive_attach_tokens(
        image="image_1", end_frame_image=None, face_refs=[],
        render_method="charswap", swap_source_video="clip.mp4",
        swap_mode=None) == [
        ("image_1", "avatar"), ("clip.mp4", "swap_source")]


def test_a_movie_section_scene_hands_over_NO_end_frame():
    """The resolver returns right after the faces (`main.py:19337-19346`), so
    a movie-section scene never hands over an end frame even when the build
    declares one. A mirror that listed it would name an asset the worker is
    never given, and would order it ahead of the faces."""
    assert ip.derive_attach_tokens(
        image="image_1", end_frame_image="image_9",
        face_refs=["image_3", "image_2"],
        render_method="movie-section", swap_source_video=None) == [
        ("image_1", "start_frame"), ("image_3", "face"), ("image_2", "face")]


def test_derived_order_matches_the_platform_resolver_docstring():
    """CONTRACT.md 2.4 and main._v965_resolve_assets both say: scene chip
    first, then faces in list order; start first, end second for a pair."""
    doc = ip.derive_attach_tokens.__doc__ or ""
    assert "attach order" in doc.lower()


# ---------------------------------------------------------------------------
# v970 — the three composer settings a clip could not say.
# ---------------------------------------------------------------------------

def _in_scope_build_with_scene(extra=""):
    """A whole minimal build that has opted in to the clip contract.

    `CLIP CONTRACT: v1` sits at COLUMN 0, because that is the opt-in the
    parser looks for (`image_platform.py`, `contract_in_scope`). One shot
    scene, the three v965 bullets, and the v969 attach line that mirrors
    `- **image:** image_1`. `extra` is whatever the test is about.
    """
    return (
        "CLIP CONTRACT: v1\n"
        "\n"
        "## Storyboard\n"
        "\n"
        "### Scene 1\n"
        "\n"
        "- **scene_type:** shot\n"
        "- **image:** image_1\n"
        "- **target_duration_s:** 8\n"
        "- **speaker:** silent\n"
        "- **action_note:** she lifts the jar. [Start beat] hands enter "
        "[End beat] jar held\n"
        "- **input_mode:** frames\n"
        "- **isolate_project:** true\n"
        "- **policy_fallback:** prompt_b, fail\n"
        "- **attach:** image_1:start_frame\n"
        + extra
    )


class _FakeClip:
    """A stand-in for the SQLAlchemy Clip row, with only the attributes
    `_v965_attach_contract` and `_v965_resolve_assets` actually read.

    Deliberately not the real model: importing it would drag the whole DB
    layer into a parser test, and the point here is the precedence rule, not
    the ORM.
    """

    def __init__(self, **kw):
        self.job_id = "job1"
        self.start_frame = "jobs/job1/frames/scene_01.png"
        self.end_frame = None
        self.render_method = None
        self.clip_contract_version = 1
        self.clip_contract_json = None
        self.veo_model = None
        self.veo_render_duration_s = 8
        self.aspect_ratio = None
        self.flow_variants_count = None
        self.resolution = None
        self.__dict__.update(kw)


def test_declared_aspect_variants_resolution_are_read():
    d = ip.parse_v970_composer_bullets(
        "- **aspect_ratio:** 9:16\n"
        "- **variants:** 2\n"
        "- **resolution:** 1080p\n", scene_index=1)
    assert d == {"aspect_ratio": "9:16", "variants": 2, "resolution": "1080p"}


def test_absent_bullets_declare_nothing():
    assert ip.parse_v970_composer_bullets("- **image:** image_1\n",
                                          scene_index=1) == {}


def test_an_unsupported_resolution_is_refused_with_the_legal_set():
    with pytest.raises(ValueError) as exc:
        ip.parse_v970_composer_bullets("- **resolution:** 4k\n", scene_index=1)
    assert "720p" in str(exc.value) and "1080p" in str(exc.value)
    assert "v970" in str(exc.value)


def test_variants_must_be_a_whole_number_in_range():
    for bad in ("0", "5", "two", "2.5"):
        with pytest.raises(ValueError) as exc:
            ip.parse_v970_composer_bullets(f"- **variants:** {bad}\n",
                                           scene_index=1)
        assert "v970" in str(exc.value)


def test_an_unsupported_aspect_is_refused():
    with pytest.raises(ValueError) as exc:
        ip.parse_v970_composer_bullets("- **aspect_ratio:** 4:3\n",
                                       scene_index=1)
    assert "9:16" in str(exc.value)


def test_the_x_prefixed_variant_count_is_accepted_like_v826():
    """`- **variants:** x2` is already legal on an `### Image N` block (v826,
    `image_platform.py:5374-5377`) and `x2` is the Flow overlay's own label.
    Two grammars for one bullet name would be a trap, so this one takes both.
    """
    assert ip.parse_v970_composer_bullets("- **variants:** x3\n",
                                          scene_index=1) == {"variants": 3}


def test_declared_v970_values_reach_the_stored_declaration():
    """Markdown in, clip_contract_json out. Fails if ANY hop is missing."""
    import json
    md = _in_scope_build_with_scene(
        "- **aspect_ratio:** 16:9\n"
        "- **resolution:** 1080p\n")            # variants deliberately absent
    scenes = ip._parse_scene_blocks_new(md, known_image_indexes={1})
    decl = json.loads(scenes[0]["clip_contract_json"])
    assert decl["aspect_ratio"] == "16:9"
    assert decl["resolution"] == "1080p"
    assert decl["variants"] is None            # undeclared stays undeclared


def test_an_in_scope_build_that_declares_nothing_stores_three_nulls():
    """The fallbacks stay: a build that says nothing renders as it does today,
    and the declaration says so explicitly rather than by omission."""
    import json
    scenes = ip._parse_scene_blocks_new(_in_scope_build_with_scene(),
                                        known_image_indexes={1})
    decl = json.loads(scenes[0]["clip_contract_json"])
    assert decl["aspect_ratio"] is None
    assert decl["variants"] is None
    assert decl["resolution"] is None


def test_a_bad_v970_value_is_refused_at_import_not_at_render():
    scenes_md = _in_scope_build_with_scene("- **resolution:** 4k\n")
    with pytest.raises(ValueError) as exc:
        ip._parse_scene_blocks_new(scenes_md, known_image_indexes={1})
    assert "v970" in str(exc.value)


def test_declared_composer_settings_beat_the_clip_row():
    """A build that says 1080p gets 1080p even when the row says 720p."""
    import main
    contract = main._v965_attach_contract(
        {}, _FakeClip(resolution="720p", flow_variants_count=2,
                      clip_contract_json='{"clip_contract_version":1,'
                                         '"input_mode":"frames",'
                                         '"isolate_project":true,'
                                         '"policy_fallback":["fail"],'
                                         '"aspect_ratio":null,'
                                         '"variants":null,'
                                         '"resolution":"1080p"}'),
        base_url="https://x", lane="user-worker")["clip_contract"]
    assert contract["resolution"] == "1080p"
    assert contract["variants"] == 2          # undeclared falls through
    assert contract["aspect_ratio"] == "9:16"  # undeclared falls through


def test_an_older_declaration_without_the_v970_keys_still_loads():
    """Forward-only: every declaration stored before v970 has no such keys,
    and `extra=forbid` would refuse a shape it did not expect. Absent keys
    must read as 'declared nothing', not as an error."""
    import main
    contract = main._v965_attach_contract(
        {}, _FakeClip(resolution="720p", flow_variants_count=3,
                      clip_contract_json='{"clip_contract_version":1,'
                                         '"input_mode":"frames",'
                                         '"isolate_project":true,'
                                         '"policy_fallback":["fail"]}'),
        base_url="https://x", lane="user-worker")["clip_contract"]
    assert contract["resolution"] == "720p"
    assert contract["variants"] == 3


def test_the_first_declared_value_in_a_spoken_scene_wins():
    """The scene in `_in_scope_build_with_scene` is SILENT, so it exercises
    the dangling path. This one has `- **line:**` bullets, so the values
    attach to a line and are collected in the parallel arrays instead — the
    other half of the v961 pattern, and the half a silent-only test misses.
    """
    import json
    md = (
        "CLIP CONTRACT: v1\n"
        "\n"
        "## Storyboard\n"
        "\n"
        "### Scene 1\n"
        "\n"
        "- **scene_type:** shot\n"
        "- **image:** image_1\n"
        "- **target_duration_s:** 8\n"
        "- **attach:** image_1:start_frame\n"
        # Every per-line bullet attaches to the line ABOVE it, so a two-line
        # scene declares the contract twice — which is exactly why these are
        # per-line arrays and not one scene-level value.
        "- **line:** first thing she says\n"
        "- **input_mode:** frames\n"
        "- **isolate_project:** true\n"
        "- **policy_fallback:** prompt_b, fail\n"
        "- **variants:** 4\n"
        "- **line:** second thing she says\n"
        "- **input_mode:** frames\n"
        "- **isolate_project:** true\n"
        "- **policy_fallback:** prompt_b, fail\n"
        "- **variants:** 1\n"
    )
    scenes = ip._parse_scene_blocks_new(md, known_image_indexes={1})
    assert scenes[0]["clip_variants"] == [4, 1]
    assert json.loads(scenes[0]["clip_contract_json"])["variants"] == 4


def test_a_text_card_may_not_carry_composer_settings():
    md = (
        "CLIP CONTRACT: v1\n"
        "\n"
        "## Storyboard\n"
        "\n"
        "### Scene 1\n"
        "\n"
        "- **scene_type:** text_card\n"
        "- **caption:** THE ONE SPICE\n"
        "- **bg_color:** #000000\n"
        "- **resolution:** 1080p\n"
    )
    with pytest.raises(ValueError) as exc:
        ip._parse_scene_blocks_new(md, known_image_indexes={1})
    assert "v970" in str(exc.value) and "text_card" in str(exc.value)


def test_a_build_that_never_opted_in_is_told_the_bullet_would_be_ignored():
    """The inverse refusal, and the one that bites hardest: a build that
    writes the bullet and forgets the §0 opt-in would render at the job's
    setting while its author believed otherwise."""
    md = (
        "## Storyboard\n"
        "\n"
        "### Scene 1\n"
        "\n"
        "- **scene_type:** shot\n"
        "- **image:** image_1\n"
        "- **target_duration_s:** 8\n"
        "- **speaker:** silent\n"
        "- **aspect_ratio:** 16:9\n"
    )
    with pytest.raises(ValueError) as exc:
        ip._parse_scene_blocks_new(md, known_image_indexes={1})
    assert "v970" in str(exc.value) and "CLIP CONTRACT: v1" in str(exc.value)
