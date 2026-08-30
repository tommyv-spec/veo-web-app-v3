"""v956 — overlay_block requires overlay_sense: the denial lines must be
DERIVED from the clip's feat, and the author shows the derivation.

Twice a block was carried from another lane instead of derived: "No Brutal
Workouts" over heavy dumbbell curls (the curls pilot), then "No Injections /
No Fillers" — beauty-domain denials — over a 78-year-old's reformer mobility
feat (martha `8eb6b63e`, published 2026-08-30 before anyone noticed). The
prose rule existed in readcaption-caption-engine; prose did not hold.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from image_platform import parse_finishing_section  # noqa: E402


BASE = """# build

## Finishing

- **captions:** none
- **overlay:** readcaption
- **overlay_age:** I'M 78
"""


def _with(extra):
    return BASE + extra + "\n## Next\n"


def test_block_without_sense_hard_fails_naming_v956():
    with pytest.raises(ValueError, match="v956"):
        parse_finishing_section(_with(
            "- **overlay_block:** No Injections / No Fillers / 7 Boring Things"))


def test_block_with_derivation_passes_and_sense_stays_out_of_the_spec():
    spec = parse_finishing_section(_with(
        "- **overlay_block:** No Surgeries / No Pain Pills / 7 Boring Things\n"
        "- **overlay_sense:** reformer mobility at 78 -> viewer suspects "
        "surgery or meds keep her moving -> each denial answers that"))
    assert spec["overlay_block"] == [
        "No Surgeries", "No Pain Pills", "7 Boring Things"]
    # authoring evidence only — must NOT ride to the worker via overlay_spec
    assert "overlay_sense" not in spec


def test_sense_without_the_derivation_arrow_fails():
    with pytest.raises(ValueError, match="v956"):
        parse_finishing_section(_with(
            "- **overlay_block:** No Surgeries / No Pain Pills\n"
            "- **overlay_sense:** it makes sense, trust me"))


def test_unicode_arrow_is_accepted():
    spec = parse_finishing_section(_with(
        "- **overlay_block:** No Steroids / No Peptides\n"
        "- **overlay_sense:** heavy lifting at 60 → viewer suspects gear "
        "→ denials answer it"))
    assert "overlay_block" in spec


def test_overlay_without_block_needs_no_sense():
    spec = parse_finishing_section(_with("- **overlay_footer:** (READ CAPTION)"))
    assert spec["overlay"] == "readcaption"
    assert "overlay_block" not in spec


def test_no_finishing_section_still_returns_none():
    assert parse_finishing_section("# build\n\n## Storyboard\n") is None
