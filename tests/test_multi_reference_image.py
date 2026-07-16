"""v859 — image-block parser accepts ONE or TWO chain parents.

    - **reference_image:** image_3            -> [3]
    - **reference_image:** image_3, image_2   -> [3, 2]

Backward compat is the load-bearing constraint: the legacy scalar key
``reference_image`` keeps meaning exactly what it meant pre-v859 (= the
FIRST entry, or None), because ~10 downstream call sites read it. The
full list rides on the NEW key ``reference_images``.

The markdown fixture is built with .replace() rather than an f-string:
the fixture embeds triple-backtick fences, and keeping it a plain
(non-f) string avoids brace/fence quoting fights entirely.
"""

import pytest

from image_platform import _parse_image_blocks_new, _parse_scene_blocks_legacy


_MD_TEMPLATE = """## Images

### Image 1
- **reference_image:** none
- **Image prompt:**
```
A photo of a kitchen.
Aspect ratio 9:16.
```

### Image 2
- **reference_image:** image_1
- **Image prompt:**
```
The same kitchen, wider.
Aspect ratio 9:16.
```

### Image 3
__REF_LINE__
- **Image prompt:**
```
The same kitchen at night.
Aspect ratio 9:16.
```
"""


def _md(ref_line):
    return _MD_TEMPLATE.replace("__REF_LINE__", ref_line)


def _md4(ref_line, on_image=3):
    """4-image doc so a TRUE forward ref (ref > index) is expressible.

    _md puts the ref on the highest image, which makes image_N a SELF ref
    (==) and image_N+1 fail the existence check first — so the `>` half of
    `ref >= image_index` is unreachable there. This fixture exists purely to
    reach it.
    """
    blocks = []
    for n in (1, 2, 3, 4):
        ref = ref_line if n == on_image else (
            "- **reference_image:** none" if n == 1 else "- **reference_image:** image_1"
        )
        blocks.append(
            f"### Image {n}\n{ref}\n- **Image prompt:**\n"
            "```\nA photo of a kitchen.\nAspect ratio 9:16.\n```\n"
        )
    return "## Images\n\n" + "\n".join(blocks)


def _third(ref_line):
    imgs = _parse_image_blocks_new(_md(ref_line))
    return [i for i in imgs if i["image_index"] == 3][0]


def test_single_ref_unchanged():
    third = _third("- **reference_image:** image_1")
    assert third["reference_image"] == 1
    assert third["reference_images"] == [1]


def test_two_refs_parsed_in_order():
    third = _third("- **reference_image:** image_2, image_1")
    assert third["reference_images"] == [2, 1]
    assert third["reference_image"] == 2   # legacy key = first entry


def test_none_gives_empty_list():
    third = _third("- **reference_image:** none")
    assert third["reference_image"] is None
    assert third["reference_images"] == []


def test_three_refs_rejected():
    with pytest.raises(ValueError, match="at most 2"):
        _parse_image_blocks_new(_md("- **reference_image:** image_2, image_1, image_2"))


# --- Backward-compat: DECORATED values (trailing author notes) -------------
# The pre-v859 regex captured a single \S+ token, so a trailing note was
# simply never seen: "none (location shift)" -> "none" -> None. v859 widened
# the capture to reach a comma list, so it must re-drop the note explicitly
# or it hard-fails a real input class that used to work. 10 decode docs in
# raw/videos/ carry exactly this shape.

def test_decorated_none_is_tolerated_like_pre_v859():
    imgs = _parse_image_blocks_new(_md("- **reference_image:** none (location shift kitchen to office)"))
    third = [i for i in imgs if i["image_index"] == 3][0]
    assert third["reference_image"] is None
    assert third["reference_images"] == []


def test_decorated_ref_is_tolerated_like_pre_v859():
    imgs = _parse_image_blocks_new(_md("- **reference_image:** image_1 (keep the counter)"))
    third = [i for i in imgs if i["image_index"] == 3][0]
    assert third["reference_image"] == 1
    assert third["reference_images"] == [1]


def test_malformed_token_still_raises():
    with pytest.raises(ValueError, match="bad reference_image token"):
        _parse_image_blocks_new(_md("- **reference_image:** image_x"))


def test_duplicate_refs_rejected():
    # Exactly 2 entries, so the ">2" guard does NOT fire — this is the only
    # test that actually reaches the duplicate branch. Banana 2 down-weights
    # duplicate refs and it wastes a slot (the v520/v522 lesson).
    with pytest.raises(ValueError, match="duplicate reference_image entries"):
        _parse_image_blocks_new(_md("- **reference_image:** image_1, image_1"))


def test_blank_value_is_none_like_pre_v859():
    # Regression: the old capture bled onto the next line and raised
    # "bad reference_image token '-'". Blank must fall through to None.
    imgs = _parse_image_blocks_new(_md("- **reference_image:**"))
    third = [i for i in imgs if i["image_index"] == 3][0]
    assert third["reference_image"] is None
    assert third["reference_images"] == []


# --- v859 validation: EVERY chain parent is checked, not just the first -----
# Pre-v859 only the scalar (= first entry) was validated, so a bad SECOND ref
# imported silently and blew up later at generation time.

def test_second_ref_unknown_is_rejected():
    with pytest.raises(ValueError, match="image_9 which doesn't exist"):
        _parse_image_blocks_new(_md("- **reference_image:** image_1, image_9"))


def test_second_ref_self_is_rejected():
    # _md puts the ref line on Image 3, the HIGHEST image in the fixture, so
    # "image_3" here is a SELF ref: this test covers the == case of
    # `ref >= image_index`. The > case (a TRUE forward ref) needs a doc with a
    # higher image to point at — see test_true_forward_ref_is_rejected.
    with pytest.raises(ValueError, match="forward/self references not allowed"):
        _parse_image_blocks_new(_md("- **reference_image:** image_1, image_3"))


def test_true_forward_ref_is_rejected():
    # Reaches the `>` half of `ref >= image_index`: image_4 EXISTS here, so the
    # existence check passes and the forward check is what must fire.
    # Mutation check: this test dies if `>=` is weakened to `==`.
    with pytest.raises(ValueError, match="forward/self references not allowed"):
        _parse_image_blocks_new(_md4("- **reference_image:** image_1, image_4", on_image=3))


def test_true_forward_ref_rejected_single_legacy_ref():
    # Same branch via the LEGACY single-ref shape — proves v859 didn't only
    # protect the new list form.
    with pytest.raises(ValueError, match="forward/self references not allowed"):
        _parse_image_blocks_new(_md4("- **reference_image:** image_3", on_image=2))


def test_valid_two_refs_pass_validation():
    imgs = _parse_image_blocks_new(_md("- **reference_image:** image_2, image_1"))
    third = [i for i in imgs if i["image_index"] == 3][0]
    assert third["reference_images"] == [2, 1]


# --- v859 scope guard: multi-ref is NEW-FORMAT only -------------------------
# The LEGACY scene parser is the other format: its scenes carry
# reference_image directly (the new format binds via "- **image:** image_N"
# and has no reference_image at all). Multi-ref is not ported there — but it
# must not half-apply either.

LEGACY_MULTIREF = '''### Scene 1
- **reference_image:** none
**Image prompt:**
```
A kitchen.
```

### Scene 2
- **reference_image:** image_1, image_1
**Image prompt:**
```
The same kitchen.
```
'''


def test_legacy_scene_parser_refuses_multiref():
    # Pre-v859 this silently captured "image_1," via an UNANCHORED regex -> 1,
    # dropping entry 2 with no error. Multi-ref is a new-format image-block
    # feature; refuse it here loudly rather than half-applying it.
    with pytest.raises(ValueError, match="multi-reference"):
        _parse_scene_blocks_legacy(LEGACY_MULTIREF)
