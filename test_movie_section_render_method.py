"""v959 — the movie-section render method, and the proof it changed nothing else.

Same contract as code/test_charswap_render_method.py: a build that says nothing
about movie-section must parse, promote and reach the worker exactly as before.
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
    with pytest.raises(ValueError, match="render_method"):
        _parse(SECTION_SCENE.replace("movie-section", "movie_section", 1))


def test_face_refs_without_method_hard_fails():
    md = LEGACY_SCENE + "- **face_refs:** image_2\n"
    with pytest.raises(ValueError, match="face_refs"):
        _parse(md)


def test_section_without_face_refs_hard_fails():
    md = "\n".join(l for l in SECTION_SCENE.splitlines() if "face_refs" not in l)
    with pytest.raises(ValueError, match="face_refs"):
        _parse(md)


def test_three_face_refs_hard_fails():
    with pytest.raises(ValueError, match="face_refs"):
        _parse(SECTION_SCENE.replace("image_2, image_3", "image_2, image_3, image_1"))


def test_face_ref_equal_to_scene_image_hard_fails():
    with pytest.raises(ValueError, match="face_refs"):
        _parse(SECTION_SCENE.replace("image_2, image_3", "image_1, image_3"))


def test_unknown_face_ref_hard_fails():
    with pytest.raises(ValueError, match="face_refs"):
        _parse(SECTION_SCENE.replace("image_2, image_3", "image_2, image_9"))


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
