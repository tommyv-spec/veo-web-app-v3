"""v858 — image-block parser accepts ONE or TWO chain parents.

    - **reference_image:** image_3            -> [3]
    - **reference_image:** image_3, image_2   -> [3, 2]

Backward compat is the load-bearing constraint: the legacy scalar key
``reference_image`` keeps meaning exactly what it meant pre-v858 (= the
FIRST entry, or None), because ~10 downstream call sites read it. The
full list rides on the NEW key ``reference_images``.

The markdown fixture is built with .replace() rather than an f-string:
the fixture embeds triple-backtick fences, and keeping it a plain
(non-f) string avoids brace/fence quoting fights entirely.
"""

import pytest

from image_platform import _parse_image_blocks_new


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
