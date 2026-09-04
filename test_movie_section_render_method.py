"""v959 — the movie-section render method, and the proof it changed nothing else.

Same contract as code/test_charswap_render_method.py: a build that says nothing
about movie-section must parse exactly as it did before.

What is covered TODAY is the parser plus the import latch. The promote and
worker sections arrive with Tasks 4-5, which append to this file — that is why
`_worker_function` and the json / pathlib imports already sit here.
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
WORKER_SRC = _HERE / "static" / "flow_worker.py"


def _worker_function(name):
    """Execute ONE top-level worker function from the shipped source (importing
    flow_worker.py boots a browser driver). Same helper as the charswap tests."""
    src = WORKER_SRC.read_text(encoding="utf-8")
    start = src.index(f"\ndef {name}(")
    rest = src[start + 1:]
    end = rest.index("\ndef ", 1)
    ns = {"re": __import__("re"), "json": json, "time": __import__("time"),
          "os": __import__("os")}
    exec(rest[:end], ns)  # noqa: S102 — our own file, on purpose
    return ns[name]


LEGACY_SCENE = """### Scene 1

- **image:** image_1
- **speaker:** on-camera
- **line:** american men over sixty are doing this every morning
- **clip_duration_s:** 6
- **action_note:** she lifts the jar [Start beat]
"""

SECTION_SCENE = """### Scene 1

- **image:** image_1
- **render_method:** movie-section
- **face_refs:** image_2, image_3
- **speaker:** on-camera
- **line:** wow if my husband looked like you i would never leave the house then he should do what i do
- **clip_duration_s:** 10
- **action_note:** she watches him lift the sack one-handed [Start beat]
"""

SECOND_SECTION = SECTION_SCENE.replace("### Scene 1", "### Scene 2").replace(
    "- **clip_duration_s:** 10", "- **clip_duration_s:** 8")

CHARSWAP_SCENE = """### Scene 1

- **image:** image_1
- **render_method:** charswap
- **swap_source_video:** raw/refs/curls.mp4
- **swap_mode:** image-led
- **speaker:** on-camera
- **line:** american men over sixty are doing this every morning
- **clip_duration_s:** 6
"""

SILENT_SCENE = """### Scene 2

- **image:** image_1
- **speaker:** silent
- **action_note:** the jar sits on the counter [Mid-clip beat]
- **clip_duration_s:** 6
"""

TEXT_CARD_WITH_METHOD = """### Scene 2

- **scene_type:** text_card
- **render_method:** movie-section
- **caption:** later
- **bg_color:** black
"""


def _parse(md, known=(1, 2, 3)):
    from image_platform import _parse_scene_blocks_new
    return _parse_scene_blocks_new(md, set(known))


# --- 1. a build with no new bullets parses to all-None new fields ------------

def test_legacy_scene_has_movie_section_fields_none():
    scene = _parse(LEGACY_SCENE)[0]
    assert scene["render_method"] is None
    assert scene["face_refs"] == []


def test_legacy_scene_keeps_every_other_field():
    scene = _parse(LEGACY_SCENE)[0]
    assert scene["scene_index"] == 1
    assert scene["image_index"] == 1
    assert scene["lines"] == ["american men over sixty are doing this every morning"]
    assert scene["clip_durations"] == [6]


def test_section_scene_parses_method_refs_and_window():
    scene = _parse(SECTION_SCENE)[0]
    assert scene["render_method"] == "movie-section"
    assert scene["face_refs"] == [2, 3]
    assert scene["clip_durations"] == [10]


# --- 2. the parser fails CLOSED ---------------------------------------------

def test_unknown_render_method_still_hard_fails():
    with pytest.raises(ValueError, match="not a known method"):
        _parse(SECTION_SCENE.replace("movie-section", "movie_section", 1))


def test_face_refs_without_method_hard_fails():
    md = LEGACY_SCENE + "- **face_refs:** image_2\n"
    with pytest.raises(ValueError, match="only means something"):
        _parse(md)


def test_face_refs_on_a_charswap_scene_hard_fails():
    """A swap clip renders from a source video, so it has nowhere to put a
    face chip. The bullet there is a mistake, not a no-op."""
    md = CHARSWAP_SCENE + "- **face_refs:** image_2\n"
    with pytest.raises(ValueError, match="only means something"):
        _parse(md)


def test_section_without_face_refs_hard_fails():
    md = "\n".join(l for l in SECTION_SCENE.splitlines() if "face_refs" not in l)
    with pytest.raises(ValueError, match="hold identity"):
        _parse(md)


def test_three_face_refs_hard_fails():
    with pytest.raises(ValueError, match="takes 1-2"):
        _parse(SECTION_SCENE.replace("image_2, image_3", "image_2, image_3, image_1"))


def test_face_ref_equal_to_scene_image_hard_fails():
    with pytest.raises(ValueError, match="is the scene's own"):
        _parse(SECTION_SCENE.replace("image_2, image_3", "image_1, image_3"))


def test_unknown_face_ref_hard_fails():
    with pytest.raises(ValueError, match="not a defined"):
        _parse(SECTION_SCENE.replace("image_2, image_3", "image_2, image_9"))


def test_repeated_face_ref_hard_fails():
    with pytest.raises(ValueError, match="repeats"):
        _parse(SECTION_SCENE.replace("image_2, image_3", "image_2, image_2"))


def test_face_ref_token_must_be_image_n():
    with pytest.raises(ValueError, match=r"is not .image_N"):
        _parse(SECTION_SCENE.replace("image_2, image_3", "img_2, image_3"))


def test_section_scene_refuses_swap_bullets():
    with pytest.raises(ValueError, match="swap"):
        _parse(SECTION_SCENE + "- **swap_mode:** video-led\n")


def test_section_scene_refuses_a_swap_source_on_its_own():
    with pytest.raises(ValueError, match="does not take"):
        _parse(SECTION_SCENE + "- **swap_source_video:** raw/refs/curls.mp4\n")


def test_audio_bullet_on_a_section_scene_says_to_drop_it():
    """The v943.1 message tells the author to declare the swap trio instead.
    A movie-section scene can never grow that trio, so it needs its own fix."""
    with pytest.raises(ValueError, match="drop the bullet"):
        _parse(SECTION_SCENE + "- **audio:** source-original\n")


def test_text_card_declaring_the_method_hard_fails():
    with pytest.raises(ValueError, match="take no render_method"):
        _parse(TEXT_CARD_WITH_METHOD)


def test_silent_scene_in_a_section_build_names_the_dead_end():
    """'Declare it on every shot scene too' is advice a silent scene cannot
    take — it has no words to carry. The message has to say that."""
    with pytest.raises(ValueError, match="no silent scenes"):
        _parse(SECTION_SCENE + "\n" + SILENT_SCENE)


def test_section_with_two_lines_hard_fails():
    md = SECTION_SCENE + "- **line:** a second line here\n- **clip_duration_s:** 10\n"
    with pytest.raises(ValueError, match="one clip"):
        _parse(md)


def test_section_without_window_hard_fails():
    md = "\n".join(l for l in SECTION_SCENE.splitlines() if "clip_duration_s" not in l)
    with pytest.raises(ValueError, match="clip_duration_s"):
        _parse(md)


def test_section_window_must_be_8_or_10():
    with pytest.raises(ValueError, match="clip_duration_s"):
        _parse(SECTION_SCENE.replace("- **clip_duration_s:** 10", "- **clip_duration_s:** 6"))


def test_mixed_build_hard_fails():
    md = SECTION_SCENE + "\n" + LEGACY_SCENE.replace("### Scene 1", "### Scene 2")
    with pytest.raises(ValueError, match="all shot scenes"):
        _parse(md)


def test_text_card_is_exempt_from_all_or_none():
    card = ("### Scene 2\n- **scene_type:** text_card\n- **caption:** later\n"
            "- **bg_color:** black\n")
    scenes = _parse(SECTION_SCENE + "\n" + card)
    assert [s["render_method"] for s in scenes] == ["movie-section", None]


def test_two_sections_parse():
    scenes = _parse(SECTION_SCENE + "\n" + SECOND_SECTION)
    assert [s["clip_durations"] for s in scenes] == [[10], [8]]


# --- the import latch: declared, but the render arm is not shipped ----------

def test_the_arm_is_not_shipped_yet():
    """The parser understands the method before anything can render it. While
    this is False the import route refuses a build that declares it."""
    import image_platform
    assert image_platform.MOVIE_SECTION_ARM_SHIPPED is False


def test_the_import_route_carries_the_latch():
    """The latch sits in the batch-import route, which needs a database session
    and a request body, so no unit test can call it. Pin its text instead —
    the same way test_charswap_render_method pins the worker's arm condition.

    Read only the route, not the whole module: the condition has to be in THAT
    function, and the wording of the 400 is prose that may be reworded without
    breaking anything."""
    import inspect

    import image_platform
    src = inspect.getsource(image_platform._import_scene_table_impl)
    assert "if not MOVIE_SECTION_ARM_SHIPPED and any(" in src
    # The constant has to be inside that same condition — a latch that reads
    # some OTHER method's name is not this latch. `):` closes the `any(...)`.
    latch = src[src.index("if not MOVIE_SECTION_ARM_SHIPPED and any("):]
    condition = latch[:latch.index("):") + 2]
    assert "MOVIE_SECTION_RENDER_METHOD" in condition
    assert "render arm is not shipped yet" in src


# --- 3. the columns exist on both rows and serialise ------------------------

def test_clip_row_has_face_ref_frames_column():
    from models import Clip
    assert "face_ref_frames_json" in Clip.__table__.columns


def test_assignment_row_has_face_ref_node_ids_column():
    from image_platform import ImageSceneAssignment
    assert "face_ref_node_ids_json" in ImageSceneAssignment.__table__.columns
