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
