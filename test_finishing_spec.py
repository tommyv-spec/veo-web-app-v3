"""v944 — the Finishing section: overlays + captions declared in the build.

The contract mirrors v943: absent section = NULL spec = byte-identical legacy
behavior; a present section is validated hard at import, never guessed at
render time."""
import sys
import pytest

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

FINISHING_OK = """## Finishing

- **captions:** none
- **overlay:** readcaption
- **overlay_age:** I'M 74
- **overlay_block:** No supplements / No gym / 5 boring things / to feel 40
- **overlay_footer:** (READ CAPTION)
"""


def _parse(md):
    from image_platform import parse_finishing_section
    return parse_finishing_section(md)


# ---------------------------------------------------------------------------
# Task 1 — the parser
# ---------------------------------------------------------------------------

def test_absent_section_is_none():
    assert _parse("# a build with no finishing\n\n### Scene 1\n") is None


def test_full_section_parses():
    spec = _parse(FINISHING_OK)
    assert spec == {
        "captions": "none",
        "overlay": "readcaption",
        "overlay_age": "I'M 74",
        "overlay_block": ["No supplements", "No gym", "5 boring things", "to feel 40"],
        "overlay_footer": "(READ CAPTION)",
    }


def test_captions_none_alone_is_valid():
    spec = _parse("## Finishing\n\n- **captions:** none\n")
    assert spec == {"captions": "none", "overlay": "none"}


def test_unknown_captions_value_hard_fails():
    with pytest.raises(ValueError, match="captions"):
        _parse("## Finishing\n\n- **captions:** rainbow-sparkle\n")


def test_unknown_overlay_engine_hard_fails():
    with pytest.raises(ValueError, match="overlay"):
        _parse("## Finishing\n\n- **overlay:** stickers\n")


def test_readcaption_without_age_hard_fails():
    with pytest.raises(ValueError, match="overlay_age"):
        _parse("## Finishing\n\n- **overlay:** readcaption\n")


def test_overlay_fields_without_engine_hard_fail():
    with pytest.raises(ValueError, match="overlay"):
        _parse("## Finishing\n\n- **overlay_age:** I'M 74\n")


def test_section_stops_at_the_next_header():
    """A bullet under the NEXT section is not a finishing field. Without the
    boundary the parser would read the whole rest of the build."""
    spec = _parse("## Finishing\n\n- **captions:** none\n\n## Images\n\n"
                  "- **overlay:** readcaption\n")
    assert spec == {"captions": "none", "overlay": "none"}


def test_known_caption_template_is_accepted():
    """The allowed set is the pipeline's own template list, not a copy of it.
    'korella' is a local style; 'word-focus' is a builtin."""
    assert _parse("## Finishing\n\n- **captions:** korella\n")["captions"] == "korella"
    assert _parse("## Finishing\n\n- **captions:** word-focus\n")["captions"] == "word-focus"
