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


def test_derived_order_matches_the_platform_resolver_docstring():
    """CONTRACT.md 2.4 and main._v965_resolve_assets both say: scene chip
    first, then faces in list order; start first, end second for a pair."""
    doc = ip.derive_attach_tokens.__doc__ or ""
    assert "attach order" in doc.lower()
