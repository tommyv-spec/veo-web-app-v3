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


# --- the three sibling rules refuse a text_card the same way ---------------
#
# v970 already did. v971 did NOT -- `- **audio:** render` was ACCEPTED on a
# card (measured 2026-09-11) -- and v969 refused it only as a confusing
# mismatch against an empty derived list. A card is drawn by ffmpeg and never
# reaches the composer, so all three are meaningless there, and gate 7 cannot
# refuse what the parser accepts without becoming stricter than the parser.

_TEXT_CARD = """CLIP CONTRACT: v1
### Scene 1
- **scene_type:** text_card
- **caption:** hello there
- **bg_color:** #000000
"""


def _parse_card(extra=""):
    import io, contextlib
    md = _TEXT_CARD + (extra + "\n" if extra else "")
    with contextlib.redirect_stdout(io.StringIO()):
        return ip._parse_scene_blocks_new(md, known_image_indexes={1})


def test_a_bare_text_card_still_parses():
    """The guard below must refuse the BULLET, never the card itself."""
    assert len(_parse_card()) == 1


@pytest.mark.parametrize("bullet,rule", [
    ("- **attach:** image_1:start_frame", "v969"),
    ("- **audio:** render", "v971"),
    ("- **audio:** source-original", "v971"),
    ("- **resolution:** 1080p", "v970"),
    ("- **variants:** 2", "v970"),
    ("- **aspect_ratio:** 9:16", "v970"),
])
def test_a_render_bullet_on_a_text_card_is_refused_by_name(bullet, rule):
    with pytest.raises(ValueError) as exc:
        _parse_card(bullet)
    msg = str(exc.value)
    assert "text_card" in msg, msg
    assert rule in msg, msg
    assert "ffmpeg" in msg, msg


# ---------------------------------------------------------------------------
# The gates. A rule with no gate is a suggestion.
#
# Two programs read a build before it ships: the authoring auditor
# (`.claude/skills/build-video/audit_build.py`, run as gate 6 of
# `tools/run_build_checks.py`) and the pre-ship platform linter
# (`code/verify_video_format.py`, gate 7). Neither can check that the attach
# line MATCHES -- only the parser resolves image tokens -- so both check
# placement, and gate 7 also checks shape and role because it can import the
# leaf table. These tests prove each refusal FIRES, and that a legal build
# still passes: a gate that cannot fail is not a gate, and one that fails a
# legal build is worse than none.
# ---------------------------------------------------------------------------

def _load_auditor():
    import importlib.util
    import pathlib
    path = (pathlib.Path(__file__).resolve().parents[1]
            / ".claude" / "skills" / "build-video" / "audit_build.py")
    spec = importlib.util.spec_from_file_location("audit_build_under_test", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _audit_v969(md, tmp_path):
    """Run ONLY c_v969_attach_line over `md`, via a real Build object."""
    audit = _load_auditor()
    p = tmp_path / "b.md"
    p.write_text(md, encoding="utf-8")
    return audit, audit.c_v969_attach_line(audit.Build(str(p)))


def test_auditor_flags_an_attach_line_on_a_text_card(tmp_path):
    md = ("## Storyboard\n\n### Scene 1\n\n"
          "- **scene_type:** text_card\n"
          "- **caption:** the end\n"
          "- **attach:** image_1:start_frame\n")
    audit, (status, msg) = _audit_v969(md, tmp_path)
    assert status == audit.FAIL, msg
    assert "v969" in msg and "text_card" in msg, msg


def test_auditor_flags_an_in_scope_shot_scene_with_no_attach_line(tmp_path):
    md = ("CLIP CONTRACT: v1\n\n## Storyboard\n\n### Scene 1\n\n"
          "- **scene_type:** shot\n- **image:** image_1\n")
    audit, (status, msg) = _audit_v969(md, tmp_path)
    assert status == audit.FAIL, msg
    assert "1" in msg and "v969" in msg, msg


def test_auditor_passes_an_opted_in_build_that_declares_the_line(tmp_path):
    md = ("CLIP CONTRACT: v1\n\n## Storyboard\n\n### Scene 1\n\n"
          "- **scene_type:** shot\n- **image:** image_1\n"
          "- **attach:** image_1:start_frame\n")
    audit, (status, msg) = _audit_v969(md, tmp_path)
    assert status == audit.PASS, msg


def test_auditor_leaves_every_existing_build_alone(tmp_path):
    """347 of 348 builds declare no opt-in and no attach line. The check must
    PASS them, not WARN and not FAIL -- a gate that fires on the whole corpus
    teaches everyone to ignore it."""
    md = ("## Storyboard\n\n### Scene 1\n\n"
          "- **scene_type:** shot\n- **image:** image_1\n")
    audit, (status, msg) = _audit_v969(md, tmp_path)
    assert status == audit.PASS, msg


def test_the_auditor_registers_the_check(tmp_path):
    audit = _load_auditor()
    assert any(row[0] == "v969_attach_line" for row in audit.CHECKS)


# --- gate 7: code/verify_video_format.py -----------------------------------

_GATE7_BUILD = """# t
## Pre-Flight Checklist
### 1. x
## Images
### Image 1
- **Image prompt:**
```
wide shot of a sunlit kitchen, she stands at the counter
```
## Storyboard
{scenes}
## Google Omni Final Prompts
### Clip 1.1
**Text prompt:**
```
she lifts the jar and says "american men over sixty are doing this"
```
**Prompt B (policy fallback):**
```
she lifts the jar and says "men in america past sixty do this"
```
"""

_GATE7_SHOT = """### Scene 1

- **image:** image_1
- **speaker:** on-camera
- **line:** american men over sixty are doing this every morning
- **clip_duration_s:** 6
- **action_note:** she lifts the jar [Start beat]
"""

_GATE7_SECTION = """### Scene 1

- **image:** image_1
- **render_method:** movie-section
- **face_refs:** image_2, image_3
- **speaker:** on-camera
- **line:** wow if my husband looked like you i would never leave the house then he should do what i do
- **clip_duration_s:** 10
- **action_note:** she watches him lift the sack [Start beat]
"""

_GATE7_CARD = """### Scene 1

- **scene_type:** text_card
- **caption:** the end
- **bg_color:** black
"""


def _gate7(scenes, tmp_path, header=""):
    """Run gate 7 in-process and return only its WARN / FAIL findings.

    The report's first line is the file path and pytest names the temp folder
    after the test, so reading the whole report finds a rule number in the path
    itself. Read the findings.
    """
    import contextlib
    import io
    import verify_video_format as v
    p = tmp_path / "b.md"
    p.write_text(header + _GATE7_BUILD.format(scenes=scenes), encoding="utf-8")
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        code = v.lint(str(p))
    findings = "\n".join(ln for ln in buf.getvalue().splitlines()
                         if ln.strip().startswith(("WARN", "FAIL")))
    return code, findings


def test_gate7_leaves_a_build_that_declares_none_of_the_new_bullets_alone():
    """Every one of the 348 existing builds is this shape."""
    import pathlib
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        _, findings = _gate7(_GATE7_SHOT, pathlib.Path(d))
    for rule in ("v969", "v970", "v971"):
        assert rule not in findings, findings


@pytest.mark.parametrize("bullet,rule", [
    ("- **attach:** image_1:start_frame", "v969"),
    ("- **aspect_ratio:** 9:16", "v970"),
    ("- **variants:** 2", "v970"),
    ("- **resolution:** 1080p", "v970"),
    ("- **audio:** render", "v971"),
])
def test_gate7_refuses_a_render_bullet_on_a_text_card(bullet, rule, tmp_path):
    code, findings = _gate7(_GATE7_CARD + bullet + "\n", tmp_path)
    assert code != 0
    assert rule in findings and "text_card" in findings, findings


def test_gate7_refuses_an_unknown_attach_role(tmp_path):
    code, findings = _gate7(_GATE7_SHOT + "- **attach:** image_1:scene\n",
                            tmp_path)
    assert code != 0
    assert "v969" in findings and "'scene'" in findings, findings
    assert "start_frame" in findings, findings   # the legal set is named


def test_gate7_refuses_a_malformed_attach_entry(tmp_path):
    code, findings = _gate7(_GATE7_SHOT + "- **attach:** image_1\n", tmp_path)
    assert code != 0
    assert "v969" in findings and "image_N:role" in findings, findings


def test_gate7_accepts_a_swap_source_filename_in_the_attach_line(tmp_path):
    """A charswap attaches its SOURCE VIDEO by filename, so demanding
    `image_N` on the left of the colon would be a false FAIL."""
    code, findings = _gate7(
        _GATE7_SHOT + "- **attach:** image_1:avatar, curls.mp4:swap_source\n",
        tmp_path)
    assert "v969" not in findings, findings


def test_gate7_refuses_an_in_scope_shot_scene_with_no_attach_line(tmp_path):
    code, findings = _gate7(_GATE7_SHOT, tmp_path, header="CLIP CONTRACT: v1\n")
    assert code != 0
    assert "v969" in findings and "Scene 1" in findings, findings


def test_gate7_refuses_a_v970_bullet_on_a_build_that_never_opted_in(tmp_path):
    code, findings = _gate7(_GATE7_SHOT + "- **resolution:** 1080p\n", tmp_path)
    assert code != 0
    assert "v970" in findings and "CLIP CONTRACT: v1" in findings, findings


def test_gate7_refuses_an_unknown_audio_source(tmp_path):
    code, findings = _gate7(_GATE7_SHOT + "- **audio:** ambient\n", tmp_path)
    assert code != 0
    assert "v971" in findings, findings
    for legal in ("render", "source-original", "scene:N", "none"):
        assert legal in findings, findings


def test_gate7_refuses_source_original_off_a_charswap_scene(tmp_path):
    code, findings = _gate7(_GATE7_SHOT + "- **audio:** source-original\n",
                            tmp_path)
    assert code != 0
    assert "v971" in findings and "charswap" in findings, findings


def test_gate7_refuses_a_scene_reference_outside_voiceover(tmp_path):
    code, findings = _gate7(_GATE7_SHOT + "- **audio:** scene:4\n", tmp_path)
    assert code != 0
    assert "v971" in findings and "voiceover" in findings, findings


def test_gate7_refuses_a_scene_reference_to_itself(tmp_path):
    """`sn` is a STRING in this linter. Passed through as one, the table's
    self-reference check compares 1 to '1' and never fires."""
    scene = _GATE7_SHOT.replace("- **speaker:** on-camera",
                                "- **speaker:** voiceover")
    code, findings = _gate7(scene + "- **audio:** scene:1\n", tmp_path)
    assert code != 0
    assert "v971" in findings and "itself" in findings, findings


def test_gate7_no_longer_fails_audio_render_on_a_movie_section_scene(tmp_path):
    """THE REGRESSION THIS TASK FIXES. The old v959 line hard-failed ANY
    `- **audio:**` bullet on a movie-section scene. v971's `render` is legal on
    every lane, so the parser accepts this build — and a linter stricter than
    the parser is a false FAIL."""
    code, findings = _gate7(_GATE7_SECTION + "- **audio:** render\n", tmp_path)
    assert "v971" not in findings, findings
    assert "audio" not in findings, findings


def test_gate7_still_refuses_audio_none_on_a_movie_section_scene(tmp_path):
    """The half of the old check that was RIGHT. `none` is v943.1's
    charswap-only value and a section scene has no swap source."""
    code, findings = _gate7(_GATE7_SECTION + "- **audio:** none\n", tmp_path)
    assert code != 0
    assert "only means something on a charswap scene" in findings, findings


def test_gate7_reads_the_roles_and_the_audio_table_from_the_leaf_module():
    """Not a literal copy in this file. The whole argument for the move: a
    hand-written second list is what went stale when `render` became legal on
    every lane."""
    import clip_contract
    import verify_video_format as v
    assert v._V969_ATTACH_ROLES is clip_contract.V969_ATTACH_ROLES
    assert v._v971_parse_audio_source is clip_contract.parse_audio_source
    assert v._V969_V971_IMPORT_ERROR is None


def test_the_parser_and_the_linter_share_one_audio_table():
    import clip_contract
    assert ip.V971_AUDIO_SOURCES is clip_contract.V971_AUDIO_SOURCES
    assert ip.V969_ATTACH_ROLES is clip_contract.V969_ATTACH_ROLES


# ---------------------------------------------------------------------------
# tools/stamp_clip_contract.py -- the stamper (spec:
# docs/superpowers/plans/2026-09-11-clip-contract-stamper.md)
#
# One tool, one promise: a build is never momentarily invalid. Writing the §0
# opt-in puts the build in scope and an in-scope shot scene missing a contract
# bullet is a hard FAIL in the parser AND in build-checks gate 7, so the opt-in
# and every bullet land in ONE write, or none.
# ---------------------------------------------------------------------------
import os
import re
import subprocess
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO / "tools") not in sys.path:
    sys.path.insert(0, str(_REPO / "tools"))

import stamp_clip_contract as stamper  # noqa: E402

# A real movie-section build: seven shot scenes, every one of them carrying
# `face_refs`, so the attach mirror has a non-trivial order to get right.
_REAL_BUILD = (
    _REPO / "videos"
    / "nuri-korella-ed-farmers-market-pumpkin-crate-farmer64-shopper-admires-"
      "healer-stall-handoff-morning-shot-movie-section-comment-growth-v1.md"
)

_CONTRACT_BULLETS = ("input_mode", "isolate_project", "policy_fallback", "attach")


def _read(path):
    with open(path, "r", encoding="utf-8", newline="") as fh:
        return fh.read()


def _unstamped(text):
    """The same build with every contract declaration removed.

    Task 10 will stamp a real build for real. These tests must keep testing the
    STAMPER rather than quietly turning into no-ops the day that lands, so the
    fixture is always taken back to its pre-contract state first.
    """
    out = []
    bullet = re.compile(
        r"^\s*[-*]\s*\*\*(?:%s)\s*:\*\*" % "|".join(_CONTRACT_BULLETS))
    for line in text.splitlines(keepends=True):
        if line.rstrip("\r\n").strip() == stamper.CONTRACT_LINE:
            continue
        if bullet.match(line):
            continue
        out.append(line)
    return "".join(out)


@pytest.fixture()
def real_build_text():
    if not _REAL_BUILD.is_file():
        pytest.skip("the movie-section fixture build is gone")
    return _unstamped(_read(_REAL_BUILD))


# --- §7 invariant snapshots. Cheap text captures; none of them needs the parser.

def _scene_headers(t):
    return re.findall(r"^###\s+Scene\s+\d+\s*$", t, re.M)


def _image_headers(t):
    return re.findall(r"^###\s+Image\s+\d+", t, re.M)


def _clip_headers(t):
    return re.findall(r"^###\s+Clip\s+\d+\.\d+", t, re.M)


def _line_fields(t):
    return re.findall(r"^\s*[-*]\s*\*\*line:\*\*", t, re.M)


def _target_duration_sum(t):
    return sum(int(v) for v in re.findall(
        r"^\s*[-*]\s*\*\*target_duration_s:\*\*\s*(\d+)", t, re.M))


def _scene_to_image(t):
    """Every scene's `- **image:** image_N`, in scene order. The ORDERED
    mapping, not a set -- a swap between two scenes keeps every count equal."""
    out = []
    for block in re.split(r"(?=^###\s+Scene\s+\d+\s*$)", t, flags=re.M)[1:]:
        m = re.search(r"^\s*[-*]\s*\*\*image:\*\*\s*(\S+)", block, re.M)
        out.append(m.group(1) if m else None)
    return out


def _s0_declarations(t):
    """The `## §0` block's column-0 declaration lines."""
    m = re.search(r"^##\s*§0\b", t, re.M)
    assert m, "the fixture has no §0 section"
    rest = t[m.end():]
    end = re.search(r"^##\s", rest, re.M)
    body = rest[:end.start()] if end else rest
    return [ln for ln in body.splitlines()
            if ln[:1] not in ("", " ", "\t", "-", "*", "#", "`", ">")]


# --- S-1.1 -----------------------------------------------------------------

def test_stamping_a_real_build_opts_in_at_column_0(real_build_text):
    out, notes = stamper.plan_file(real_build_text, "fixture")
    assert notes, "the unstamped fixture should need work"
    assert "\nCLIP CONTRACT: v1\n" in out
    assert "\n CLIP CONTRACT: v1" not in out      # never indented
    assert len(re.findall(r"^CLIP CONTRACT:\s*v1\s*$", out, re.M)) == 1


def test_every_shot_scene_gets_all_four_bullets(real_build_text):
    out, _ = stamper.plan_file(real_build_text, "fixture")
    blocks = re.split(r"(?=^###\s+Scene\s+\d+\s*$)", out, flags=re.M)[1:]
    assert len(blocks) == 7
    for block in blocks:
        for name in _CONTRACT_BULLETS:
            assert re.search(r"^\s*[-*]\s*\*\*%s:\*\*" % name, block, re.M), (
                name, block[:120])


def test_the_attach_mirror_is_the_production_derivation(real_build_text):
    """Never re-derived locally. A movie-section scene lists the scene chip and
    then the faces IN THE AUTHOR'S ORDER, and nothing after them."""
    out, _ = stamper.plan_file(real_build_text, "fixture")
    block = re.split(r"(?=^###\s+Scene\s+\d+\s*$)", out, flags=re.M)[4]
    assert "- **attach:** image_4:start_frame, image_5:face, image_2:face" in block


# --- S-1.2 -----------------------------------------------------------------

def test_stamping_is_idempotent(real_build_text):
    once, _ = stamper.plan_file(real_build_text, "fixture")
    twice, notes = stamper.plan_file(once, "fixture")
    assert notes == []
    assert twice == once


# --- S-1.3 -- all SEVEN §7 invariants, not five ----------------------------

def test_the_five_counted_invariants_are_unchanged(real_build_text):
    out, _ = stamper.plan_file(real_build_text, "fixture")
    assert _scene_headers(out) == _scene_headers(real_build_text)
    assert _image_headers(out) == _image_headers(real_build_text)
    assert _clip_headers(out) == _clip_headers(real_build_text)
    assert _line_fields(out) == _line_fields(real_build_text)
    assert _target_duration_sum(out) == _target_duration_sum(real_build_text)


def test_the_ordered_scene_to_image_mapping_is_unchanged(real_build_text):
    out, _ = stamper.plan_file(real_build_text, "fixture")
    before = _scene_to_image(real_build_text)
    assert before and all(before)
    assert _scene_to_image(out) == before


def test_section0_gains_exactly_the_opt_in_and_nothing_else(real_build_text):
    """A verbatim compare cannot pass -- stamping necessarily adds the opt-in to
    §0, so the test would fail on its own success. Remove exactly that ONE line
    from the after-snapshot, then require the two to be identical."""
    out, _ = stamper.plan_file(real_build_text, "fixture")
    before = _s0_declarations(real_build_text)
    after = _s0_declarations(out)
    assert after.count(stamper.CONTRACT_LINE) == 1
    assert before.count(stamper.CONTRACT_LINE) == 0
    after.remove(stamper.CONTRACT_LINE)
    assert after == before


# --- S-1.4 -----------------------------------------------------------------

_MINIMAL = """## §0 Citations Check

SCAFFOLD CONTRACT: v1

## Images

### Image 1
- **Image prompt:**

```
a jar on a counter
```

## Storyboard

### Scene 1
- **image:** image_1
- **scene_type:** shot
- **speaker:** on-camera
- **line:** the words of this clip in order lowercase
- **target_duration_s:** 8
- **action_note:** [Start beat] hands enter [End beat] jar held

### Scene 2
- **scene_type:** text_card
- **caption:** the card says this
- **bg_color:** #000000
- **duration_s:** 1.5
"""


def test_a_text_card_gains_nothing():
    out, _ = stamper.plan_file(_MINIMAL, "fixture")
    card = re.split(r"(?=^###\s+Scene\s+\d+\s*$)", out, flags=re.M)[2]
    assert "text_card" in card
    for name in _CONTRACT_BULLETS:
        assert ("**%s:**" % name) not in card


# --- S-1.5 -----------------------------------------------------------------

def test_no_emitted_line_carries_an_inline_annotation(real_build_text):
    """`_parse_bullet_field` keeps everything after the value, so a trailing
    `# ...` becomes part of it and the build stops importing. The skeleton's
    Scene 7 documents every bullet that way; its lines must never be copied."""
    before = set(real_build_text.splitlines())
    out, _ = stamper.plan_file(real_build_text, "fixture")
    added = [ln for ln in out.splitlines() if ln not in before]
    assert added
    for line in added:
        assert "#" not in line, line


def test_the_emitted_bullets_survive_the_production_parser(real_build_text):
    """Not a shape check: every emitted value goes back through the checker
    that would refuse it at import."""
    import clip_contract
    out, _ = stamper.plan_file(real_build_text, "fixture")
    modes = re.findall(r"^\s*[-*]\s*\*\*input_mode:\*\*\s*(.+?)\s*$", out, re.M)
    assert modes
    for m in modes:
        clip_contract.check_input_mode(m)
    for lad in re.findall(
            r"^\s*[-*]\s*\*\*policy_fallback:\*\*\s*(.+?)\s*$", out, re.M):
        clip_contract.check_ladder([r.strip() for r in lad.split(",")])
    for iso in re.findall(
            r"^\s*[-*]\s*\*\*isolate_project:\*\*\s*(.+?)\s*$", out, re.M):
        assert iso in ("true", "false")


def test_no_v970_and_no_audio_bullet_is_ever_written(real_build_text):
    before = set(real_build_text.splitlines())
    out, _ = stamper.plan_file(real_build_text, "fixture")
    added = [ln for ln in out.splitlines() if ln not in before]
    for line in added:
        for banned in ("aspect_ratio", "variants", "resolution", "audio"):
            assert ("**%s:**" % banned) not in line, line


# --- S-1.6 -----------------------------------------------------------------

def test_a_half_stamped_build_converges(real_build_text):
    """The state a two-pass stamper would ship. Re-running must land on exactly
    the fully stamped bytes, not beside them."""
    full, _ = stamper.plan_file(real_build_text, "fixture")
    victim = re.search(r"^\s*[-*]\s*\*\*attach:\*\*.+?\n", full, re.M).group(0)
    half = full.replace(victim, "", 1)
    assert half != full
    again, notes = stamper.plan_file(half, "fixture")
    assert notes, "the missing attach line should be reported"
    assert again == full


# --- S-1.7 -----------------------------------------------------------------

def _run(args, tmp_path):
    env = dict(os.environ)
    env["PYTHONPATH"] = str(_REPO / "code")
    return subprocess.run(
        [sys.executable, str(_REPO / "tools" / "stamp_clip_contract.py")] + args,
        capture_output=True, text=True, encoding="utf-8", errors="replace",
        cwd=str(_REPO), env=env)


def test_check_mode_changes_no_bytes_and_exits_1(tmp_path):
    target = tmp_path / "build.md"
    with open(target, "w", encoding="utf-8", newline="") as fh:
        fh.write(_MINIMAL)
    before = target.read_bytes()
    proc = _run(["--check", str(target)], tmp_path)
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert target.read_bytes() == before
    assert "WOULD STAMP" in proc.stdout


def test_check_mode_on_a_stamped_build_exits_0(tmp_path):
    target = tmp_path / "build.md"
    stamped, _ = stamper.plan_file(_MINIMAL, "fixture")
    with open(target, "w", encoding="utf-8", newline="") as fh:
        fh.write(stamped)
    proc = _run(["--check", str(target)], tmp_path)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "already fully stamped" in proc.stdout


# --- input_mode: the derivation that must not change what a clip DOES -------

def _with(bullets, base=_MINIMAL):
    return base.replace(
        "- **target_duration_s:** 8",
        "\n".join(bullets) + "\n- **target_duration_s:** 8", 1)


def test_a_veo_clip_with_an_end_frame_stamps_frames_not_ingredients():
    """The shipped inference needs an OMNI model AND an end frame. A Veo clip
    with an end frame runs on Frames today, and a stamp must not move it."""
    text = _with(["- **end_frame_image:** image_2",
                  "- **veo_model:** Veo 3.1"])
    out, _ = stamper.plan_file(text, "fixture")
    assert "- **input_mode:** frames" in out
    assert "ingredients" not in out


def test_an_omni_clip_with_start_and_end_frames_stamps_ingredients():
    text = _with(["- **end_frame_image:** image_2",
                  "- **veo_model:** Omni Flash"])
    out, _ = stamper.plan_file(text, "fixture")
    assert "- **input_mode:** ingredients" in out
    assert "- **attach:** image_1:start_frame, image_2:end_frame" in out


def test_an_end_frame_with_no_declared_model_refuses_the_file_by_scene():
    """The effective model is then a JOB setting the markdown does not carry,
    and the two candidates disagree. Refuse; do not invent a default."""
    text = _with(["- **end_frame_image:** image_2"])
    with pytest.raises(stamper.Refusal) as exc:
        stamper.plan_file(text, "fixture")
    joined = " ".join(exc.value.reasons)
    assert "Scene 1" in joined
    assert "veo_model" in joined


def test_a_clip_with_no_end_frame_stamps_frames_without_a_declared_model():
    """Both candidate models agree here, so there is nothing to refuse."""
    out, _ = stamper.plan_file(_MINIMAL, "fixture")
    assert "- **input_mode:** frames" in out


# --- PREFLIGHT: never promote an existing bad value -------------------------

def test_an_invalid_existing_input_mode_refuses_the_whole_file():
    """The half-stamped danger: a bad value sitting inert becomes ACTIVE the
    moment the opt-in lands beside it. Refuse the FILE, before a byte is
    written -- not the one scene."""
    text = _with(["- **input_mode:** Ingredient"])
    with pytest.raises(stamper.Refusal) as exc:
        stamper.plan_file(text, "fixture")
    joined = " ".join(exc.value.reasons)
    assert "Scene 1" in joined and "input_mode" in joined
    assert "'Ingredient'" in joined or '"Ingredient"' in joined
    assert "frames" in joined and "ingredients" in joined       # what was expected


def test_an_existing_attach_that_disagrees_refuses_the_whole_file():
    text = _with(["- **attach:** image_2:start_frame"])
    with pytest.raises(stamper.Refusal) as exc:
        stamper.plan_file(text, "fixture")
    joined = " ".join(exc.value.reasons)
    assert "attach" in joined and "image_1:start_frame" in joined


def test_a_ladder_that_cannot_end_refuses_the_whole_file():
    text = _with(["- **policy_fallback:** prompt_b"])
    with pytest.raises(stamper.Refusal) as exc:
        stamper.plan_file(text, "fixture")
    assert "fail" in " ".join(exc.value.reasons)


def test_a_yes_isolate_project_refuses_the_whole_file():
    text = _with(["- **isolate_project:** yes"])
    with pytest.raises(stamper.Refusal) as exc:
        stamper.plan_file(text, "fixture")
    assert "isolate_project" in " ".join(exc.value.reasons)


def test_the_v970_preflight_accepts_v826s_x2_spelling():
    """The MARKDOWN validator, not the leaf checker. `clip_contract.check_variants`
    takes an int; calling it on raw markdown rejects `x2` or raises TypeError."""
    text = _with(["- **variants:** x2"])
    out, _ = stamper.plan_file(text, "fixture")
    assert "- **variants:** x2" in out          # untouched, and not refused


def test_a_bad_variants_value_still_refuses():
    text = _with(["- **variants:** x9"])
    with pytest.raises(stamper.Refusal) as exc:
        stamper.plan_file(text, "fixture")
    assert "variants" in " ".join(exc.value.reasons)


def test_a_refused_file_is_left_untouched_on_disk(tmp_path):
    target = tmp_path / "build.md"
    text = _with(["- **input_mode:** Ingredient"])
    with open(target, "w", encoding="utf-8", newline="") as fh:
        fh.write(text)
    before = target.read_bytes()
    proc = _run([str(target)], tmp_path)
    assert proc.returncode == 2, proc.stdout + proc.stderr
    assert "REFUSED" in proc.stdout
    assert target.read_bytes() == before


def test_a_contract_bullet_above_the_first_line_is_refused_not_duplicated():
    """The parser discards a contract bullet written before the scene's first
    `- **line:**`, so the scene would read as undeclared the moment the opt-in
    lands. Say so instead of stamping a second copy beside it."""
    text = _MINIMAL.replace(
        "- **speaker:** on-camera",
        "- **speaker:** on-camera\n- **input_mode:** frames", 1)
    with pytest.raises(stamper.Refusal) as exc:
        stamper.plan_file(text, "fixture")
    assert "BEFORE the first" in " ".join(exc.value.reasons)


# --- the write itself -------------------------------------------------------

def test_crlf_is_preserved_and_no_bare_newline_is_introduced():
    """A Python read/write pair renormalises CRLF silently and the diff becomes
    the whole file. This repo has been caught by that before."""
    text = _MINIMAL.replace("\n", "\r\n")
    out, _ = stamper.plan_file(text, "fixture")
    assert "\r\n" in out
    assert re.search(r"(?<!\r)\n", out) is None


def test_the_stamp_is_one_write_and_leaves_no_temp_file(tmp_path):
    target = tmp_path / "build.md"
    with open(target, "w", encoding="utf-8", newline="") as fh:
        fh.write(_MINIMAL)
    changed, notes = stamper.stamp_file(target)
    assert changed and notes
    assert sorted(p.name for p in tmp_path.iterdir()) == ["build.md"]
    assert "CLIP CONTRACT: v1" in target.read_text(encoding="utf-8")


def test_multiple_clips_in_one_scene_each_get_their_own_trio():
    """A two-line scene is two clips, and a contract bullet attaches to the
    closest PRECEDING line. One trio for the scene would leave clip 1
    undeclared and fail the import."""
    text = _MINIMAL.replace(
        "- **action_note:** [Start beat] hands enter [End beat] jar held",
        "- **action_note:** [Start beat] hands enter [End beat] jar held\n"
        "- **line:** and the second clip says this\n"
        "- **action_note:** [Start beat] she turns [End beat] she smiles", 1)
    out, _ = stamper.plan_file(text, "fixture")
    scene = re.split(r"(?=^###\s+Scene\s+\d+\s*$)", out, flags=re.M)[1]
    assert len(re.findall(r"^\s*[-*]\s*\*\*input_mode:\*\*", scene, re.M)) == 2
    assert len(re.findall(r"^\s*[-*]\s*\*\*attach:\*\*", scene, re.M)) == 1
    # each trio sits BELOW its own line, never above the first one
    order = re.findall(r"\*\*(line|input_mode)\b", scene)
    assert order == ["line", "input_mode", "line", "input_mode"]
