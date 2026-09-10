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


# ---------------------------------------------------------------------------
# v971 — one generic per-clip audio source.
# ---------------------------------------------------------------------------

def _build_with(extra="", speaker="silent"):
    """A minimal build that has NOT opted in to the clip contract.

    v971 is forward-only and lane-neutral, so the scene here carries nothing
    but what a scene needs: no `CLIP CONTRACT: v1`, no v965 bullets, no
    attach line. `speaker` is a parameter because the legality of
    `audio: scene:N` depends on it -- a voiceover scene also needs a line
    (v698A Gate 11) and gets its Gate 9 answer from the audio bullet itself.
    """
    line = ""
    if speaker == "voiceover":
        line = "- **line:** she says it plainly\n"
    return (
        "## Storyboard\n"
        "\n"
        "### Scene 1\n"
        "\n"
        "- **scene_type:** shot\n"
        "- **image:** image_1\n"
        "- **target_duration_s:** 8\n"
        f"- **speaker:** {speaker}\n"
        + line
        + extra
    )


def _charswap_build_with(extra=""):
    """The same minimal build, with the v943 swap trio so the charswap-only
    sources are legal on it."""
    return (
        "## Storyboard\n"
        "\n"
        "### Scene 1\n"
        "\n"
        "- **scene_type:** shot\n"
        "- **image:** image_1\n"
        "- **target_duration_s:** 8\n"
        "- **speaker:** silent\n"
        "- **render_method:** charswap\n"
        "- **swap_source_video:** source_clip.mp4\n"
        "- **swap_mode:** video-led\n"
        + extra
    )


def test_render_is_legal_everywhere():
    for method in (None, "charswap", "movie-section"):
        assert ip.parse_audio_source("render", scene_index=1,
                                     render_method=method) == ("render", None)


def test_source_original_stays_charswap_only():
    assert ip.parse_audio_source("source-original", scene_index=1,
                                 render_method="charswap") == (
        "source-original", None)
    with pytest.raises(ValueError) as exc:
        ip.parse_audio_source("source-original", scene_index=1,
                              render_method=None)
    msg = str(exc.value)
    assert "v971" in msg
    assert "source-original" in msg
    assert "charswap" in msg          # says WHY, and where it IS legal


def test_scene_reference_parses_its_number():
    assert ip.parse_audio_source("scene:4", scene_index=1, render_method=None,
                                 speaker_mode="voiceover") == ("scene", 4)


def test_scene_reference_is_refused_outside_voiceover():
    """`audio_from_scene` is forwarded for voiceover lines only (the
    scene_speaker_mode guard on the flat clip rows in image_platform.py);
    anywhere else it is dropped, so accepting it would be a declaration that
    silently does nothing."""
    with pytest.raises(ValueError) as exc:
        ip.parse_audio_source("scene:4", scene_index=1, render_method=None,
                              speaker_mode="on-camera")
    msg = str(exc.value)
    assert "voiceover" in msg and "v971" in msg


def test_a_scene_reference_to_itself_is_refused():
    with pytest.raises(ValueError) as exc:
        ip.parse_audio_source("scene:1", scene_index=1, render_method=None,
                              speaker_mode="voiceover")
    assert "itself" in str(exc.value)


def test_a_malformed_scene_reference_says_the_shape():
    with pytest.raises(ValueError) as exc:
        ip.parse_audio_source("scene:four", scene_index=1, render_method=None,
                              speaker_mode="voiceover")
    assert "scene:N" in str(exc.value)


def test_an_unknown_source_names_every_legal_one():
    with pytest.raises(ValueError) as exc:
        ip.parse_audio_source("ambient", scene_index=1, render_method=None)
    msg = str(exc.value)
    for legal in ("render", "source-original", "scene:N", "none"):
        assert legal in msg


def test_none_keeps_its_v943_1_meaning_and_stays_charswap_only():
    assert ip.parse_audio_source("none", scene_index=1,
                                 render_method="charswap") == ("none", None)
    # No general silence step exists, so `none` on an ordinary clip would be
    # accepted and do nothing. Refuse until a consumer is written.
    with pytest.raises(ValueError) as exc:
        ip.parse_audio_source("none", scene_index=1, render_method=None)
    assert "v971" in str(exc.value)


def test_absent_is_distinct_from_every_declared_source():
    assert ip.parse_audio_source(None, scene_index=1,
                                 render_method=None) is None


def test_the_table_is_the_only_place_legality_is_written():
    """Adding a style must be a row, not a new branch. If someone adds a
    source with an `if` instead, this catches it."""
    for src, spec in ip.V971_AUDIO_SOURCES.items():
        assert {"lanes", "speakers", "arg", "consumer"} <= set(spec), src


def test_every_source_names_a_consumer_that_actually_reads_it():
    """The rule this table exists to enforce: a source is legal only where
    something reads it. A row whose consumer text is empty is a row that will
    import cleanly and vanish."""
    for src, spec in ip.V971_AUDIO_SOURCES.items():
        assert spec["consumer"].strip(), src


def test_source_original_still_writes_the_swap_audio_column():
    """v943.1 behaviour is unchanged — the bullet is the same, the column is
    the same, only the legality check moved into the table."""
    md = _charswap_build_with("- **audio:** source-original\n")
    scenes = ip._parse_scene_blocks_new(md, known_image_indexes={1})
    assert scenes[0]["swap_audio"] == "source-original"


def test_audio_scene_n_writes_the_same_column_as_the_old_bullet():
    a = ip._parse_scene_blocks_new(
        _build_with("- **audio:** scene:2\n", speaker="voiceover"),
        known_image_indexes={1, 2})
    b = ip._parse_scene_blocks_new(
        _build_with("- **audio_from_scene:** 2\n", speaker="voiceover"),
        known_image_indexes={1, 2})
    assert a[0]["audio_from_scene"] == b[0]["audio_from_scene"] == 2


def test_the_two_spellings_disagreeing_is_an_error_not_a_silent_winner():
    with pytest.raises(ValueError) as exc:
        ip._parse_scene_blocks_new(
            _build_with("- **audio:** scene:2\n- **audio_from_scene:** 3\n",
                        speaker="voiceover"),
            known_image_indexes={1, 2, 3})
    assert "disagree" in str(exc.value)


def test_the_new_spelling_inherits_the_anchor_mutual_exclusion():
    """v698A refuses `audio_from_scene` beside `voiceover_anchor_image`, but
    that check sits inside the OLD bullet's parse. A second spelling that
    skipped it would be a way around the rule."""
    with pytest.raises(ValueError) as exc:
        ip._parse_scene_blocks_new(
            _build_with("- **audio:** scene:2\n"
                        "- **voiceover_anchor_image:** image_9\n",
                        speaker="voiceover"),
            known_image_indexes={1, 2, 9})
    assert "voiceover_anchor_image" in str(exc.value)


def test_each_declaration_alone_still_passes():
    """The refusal above must be about the PAIR, not about either half."""
    ip._parse_scene_blocks_new(
        _build_with("- **audio:** scene:2\n", speaker="voiceover"),
        known_image_indexes={1, 2})
    ip._parse_scene_blocks_new(
        _build_with("- **voiceover_anchor_image:** image_9\n",
                    speaker="voiceover"),
        known_image_indexes={1, 9})


def test_render_writes_no_override_at_all():
    s = ip._parse_scene_blocks_new(_build_with("- **audio:** render\n"),
                                   known_image_indexes={1})[0]
    assert s["swap_audio"] is None and s["audio_from_scene"] is None
