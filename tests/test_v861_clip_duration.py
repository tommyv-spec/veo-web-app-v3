"""v861 — per-clip duration bucket math."""
import pytest

from clip_duration import (
    ALLOWED_CLIP_DURATIONS_S,
    VEO_API_DURATIONS_S,
    pick_clip_duration_s,
    resolve_clip_duration_s,
    veo_api_duration_s,
)


@pytest.mark.parametrize("words,expected", [
    (0, 4), (1, 4), (11, 4),          # <=11 -> 4s
    (12, 6), (16, 6),                  # 12-16 -> 6s
    (17, 8), (24, 8),                  # 17-24 -> 8s
    (25, 10), (28, 10),                # 25-28 -> 10s
    (29, 10), (60, 10),                # >28 -> 10s (auditor FAILs the build)
])
def test_pick_clip_duration_s_buckets(words, expected):
    assert pick_clip_duration_s(words) == expected


def test_operator_anchor_points():
    """The 4 points the operator specified on 2026-07-16."""
    assert pick_clip_duration_s(11) == 4    # "less than 12 words is 4 seconds"
    assert pick_clip_duration_s(16) == 6    # "16 is 6 seconds"
    assert pick_clip_duration_s(24) == 8    # "24 is 8 seconds"
    assert pick_clip_duration_s(28) == 10   # "around 28 words we 10 seconds"


def test_allowed_values():
    assert ALLOWED_CLIP_DURATIONS_S == (4, 6, 8, 10)
    assert VEO_API_DURATIONS_S == (4, 6, 8)


def test_veo_api_duration_s():
    assert veo_api_duration_s(4) == 4
    assert veo_api_duration_s(6) == 6
    assert veo_api_duration_s(8) == 8
    assert veo_api_duration_s(10) == 8   # Veo API has no 10s bucket
    assert veo_api_duration_s(None) is None


def test_veo_api_duration_s_returns_int():
    result = veo_api_duration_s(4.0)
    assert result == 4
    assert isinstance(result, int)


@pytest.mark.parametrize("bad", [0, -5, 2, 7, 9, 12, 4.5, "8", "abc", [8], True])
def test_veo_api_duration_s_rejects_out_of_range(bad):
    """Below-range input must NOT silently fold UP to the longest bucket."""
    with pytest.raises(ValueError, match="not in"):
        veo_api_duration_s(bad)


def test_veo_api_duration_s_error_names_the_callers_field():
    """The message must name the field the caller actually read from.

    worker.py feeds this a DB column (clips.veo_render_duration_s), so a
    hardcoded "clip_duration_s" would send the operator hunting through
    markdown for a value that came from the database.
    """
    with pytest.raises(ValueError, match=r"veo_render_duration_s 7 not in .*v861"):
        veo_api_duration_s(7, field_name="veo_render_duration_s")


def test_veo_api_duration_s_error_default_field_name():
    with pytest.raises(ValueError, match=r"clip_duration_s 7 not in .*v861"):
        veo_api_duration_s(7)


def test_resolve_errors_name_their_own_fields():
    with pytest.raises(ValueError, match=r"clip_duration_s 7 not in .*v861"):
        resolve_clip_duration_s(explicit=7, anchor_bucket=None, line_text="hi")
    with pytest.raises(ValueError, match=r"anchor_bucket 7 not in .*v861"):
        resolve_clip_duration_s(explicit=None, anchor_bucket=7, line_text="hi")


def test_resolve_precedence_explicit_wins():
    assert resolve_clip_duration_s(
        explicit=6, anchor_bucket=8,
        line_text=" ".join(["w"] * 60)) == 6


def test_resolve_precedence_anchor_beats_wordcount():
    assert resolve_clip_duration_s(explicit=None, anchor_bucket=8, line_text="two words") == 8


def test_resolve_precedence_wordcount_when_no_anchor():
    # 18 words -> 8s bucket
    assert resolve_clip_duration_s(
        explicit=None, anchor_bucket=None,
        line_text=" ".join(["w"] * 18)) == 8


def test_resolve_returns_none_for_silent_scene():
    """No explicit, no anchor, no words -> NULL -> job default duration applies."""
    assert resolve_clip_duration_s(explicit=None, anchor_bucket=None, line_text="") is None
    assert resolve_clip_duration_s(explicit=None, anchor_bucket=None, line_text=None) is None


def test_resolve_returns_none_for_whitespace_only_line():
    """A blank markdown bullet parses to whitespace -> silent scene."""
    assert resolve_clip_duration_s(explicit=None, anchor_bucket=None, line_text="   ") is None
    assert resolve_clip_duration_s(explicit=None, anchor_bucket=None, line_text="\n\t ") is None


def test_resolve_rejects_bad_explicit():
    with pytest.raises(ValueError, match="not in"):
        resolve_clip_duration_s(explicit=7, anchor_bucket=None, line_text="hello")


@pytest.mark.parametrize("bad", [7, 0, -3, 5.237, 6.7, 4.5, 10.9, "6", "abc", [6], True])
def test_resolve_rejects_bad_explicit_types(bad):
    """Validate the RAW value before coercing — 6.7 must not silently become 6."""
    with pytest.raises(ValueError, match="not in"):
        resolve_clip_duration_s(explicit=bad, anchor_bucket=None, line_text="hello")


def test_resolve_explicit_float_whole_number_normalizes_to_int():
    """4.0 is exactly 4 -> accepted, but returned as a real int."""
    result = resolve_clip_duration_s(explicit=4.0, anchor_bucket=None, line_text="hello")
    assert result == 4
    assert isinstance(result, int)


@pytest.mark.parametrize("bad", [7, 0, -3, 99, 5.237, 4.5, "8", "abc", [8], True])
def test_resolve_rejects_bad_anchor_bucket(bad):
    """Same gate as explicit — a wiring slip must not reach the render path."""
    with pytest.raises(ValueError, match="not in"):
        resolve_clip_duration_s(explicit=None, anchor_bucket=bad, line_text="two words")


def test_resolve_anchor_bucket_normalizes_to_int():
    result = resolve_clip_duration_s(explicit=None, anchor_bucket=8.0, line_text="two words")
    assert result == 8
    assert isinstance(result, int)


def test_resolve_wordcount_returns_int():
    result = resolve_clip_duration_s(
        explicit=None, anchor_bucket=None, line_text=" ".join(["w"] * 18))
    assert isinstance(result, int)


# --- markdown parser: the `- **clip_duration_s:**` bullet -------------------
# Per-LINE, not per-scene: it attaches to the closest preceding `- **line:**`,
# the same rule v644's `pad` bullet uses.

from image_platform import _parse_scene_blocks_new

_MD_ONE = """### Scene 1
- **image:** image_1
- **clip_mode:** fresh
- **line:** your soldier will not wake up in the morning anymore
- **clip_duration_s:** 6
- **action_note:** she lifts the banana. [Start beat]
"""

_MD_TWO_LINES = """### Scene 1
- **image:** image_1
- **line:** first line here
- **clip_duration_s:** 4
- **line:** second line is quite a lot longer than the first one right here
- **clip_duration_s:** 8
"""

_MD_NONE = """### Scene 1
- **image:** image_1
- **line:** no duration bullet on this one
"""


def test_parse_clip_duration_single_line():
    scenes = _parse_scene_blocks_new(_MD_ONE, {1})
    assert scenes[0]["clip_durations"] == [6]


def test_parse_clip_duration_attaches_per_line():
    scenes = _parse_scene_blocks_new(_MD_TWO_LINES, {1})
    assert scenes[0]["lines"] == [
        "first line here",
        "second line is quite a lot longer than the first one right here",
    ]
    assert scenes[0]["clip_durations"] == [4, 8]


def test_parse_clip_duration_absent_is_none():
    scenes = _parse_scene_blocks_new(_MD_NONE, {1})
    assert scenes[0]["clip_durations"] == [None]


def test_parse_clip_duration_rejects_bad_value():
    md = _MD_ONE.replace("- **clip_duration_s:** 6", "- **clip_duration_s:** 7")
    with pytest.raises(ValueError, match="not in"):
        _parse_scene_blocks_new(md, {1})


@pytest.mark.parametrize("bad", ["6.5", "4.9", "10.9", "6 or 8", "6s", "6 seconds",
                                 "8 (16 words)", "6,", "four", "-4", ""])
def test_parse_clip_duration_rejects_anything_but_a_bare_int(bad):
    """A leading-int regex would silently truncate `6.5` -> 6 and pick the first
    number out of `6 or 8`. That is the exact coercion hole clip_duration.py's
    _validated_duration_s closes; the parser must not reopen it one module away.
    An author can be WRONG about this value, so it is never guessed at."""
    md = _MD_ONE.replace("- **clip_duration_s:** 6", f"- **clip_duration_s:** {bad}")
    with pytest.raises(ValueError, match="clip_duration_s"):
        _parse_scene_blocks_new(md, {1})


def test_parse_clip_duration_bad_value_names_the_right_bucket():
    """The error computes the answer from the line's word count instead of
    reprinting the bucket table (which would drift from _BUCKETS silently)."""
    md = _MD_ONE.replace("- **clip_duration_s:** 6", "- **clip_duration_s:** 7")
    with pytest.raises(ValueError) as exc:
        _parse_scene_blocks_new(md, {1})
    msg = str(exc.value)
    # _MD_ONE's line is 10 words -> 4s bucket.
    assert "10 words" in msg
    assert "use 4" in msg


_MD_SILENT = """### Scene 1
- **image:** image_1
- **speaker:** silent
- **clip_duration_s:** 6
- **action_note:** the banana rots on the counter. [Start beat]
"""


def test_parse_clip_duration_on_silent_scene_is_kept():
    """v786's dangling action_note precedent: a silent scene has no `line` to
    attach to but still renders a clip, so its duration must survive as a
    1-entry list. Dropping it would make prepare_batch_for_video's silent
    branch (which reads clip_durations[0]) permanently dead."""
    scenes = _parse_scene_blocks_new(_MD_SILENT, {1})
    assert scenes[0]["lines"] == []
    assert scenes[0]["clip_durations"] == [6]


def test_parse_clip_duration_silent_scene_still_validates():
    md = _MD_SILENT.replace("- **clip_duration_s:** 6", "- **clip_duration_s:** 7")
    with pytest.raises(ValueError, match="not in"):
        _parse_scene_blocks_new(md, {1})


_MD_PAD_AND_DURATION = """### Scene 1
- **image:** image_1
- **line:** first line here
- **pad:** and that is the honest truth about it
- **clip_duration_s:** 4
- **line:** second line here
- **clip_duration_s:** 6
- **pad:** which is why it matters so much
"""


def test_parse_pad_and_clip_duration_do_not_interfere():
    """Both attach to the closest preceding line, in either order. Proves the
    two parallel arrays stay independent and index-aligned with lines."""
    scenes = _parse_scene_blocks_new(_MD_PAD_AND_DURATION, {1})
    s = scenes[0]
    assert s["clip_durations"] == [4, 6]
    assert s["pads"] == [
        "and that is the honest truth about it",
        "which is why it matters so much",
    ]
    assert len(s["lines"]) == len(s["pads"]) == len(s["clip_durations"]) == 2
