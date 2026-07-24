"""v865 — the clip parser must read BOTH the legacy Veo header and the new
Google Omni header, and must pull the dialogue line out of an Omni-shaped body."""
from veo_prompt_overrides import parse_veo_prompts_block

_OMNI_BUILD = """## Storyboard

## Google Omni Final Prompts (per clip)

### Clip 1.1 — Scene 1, Line 1 (HOOK)
**Start frame:** Image 1
**Text prompt:**
```
Create an 8-second vertical 9:16 realistic UGC video.

Dialogue: The main AI generated character speaks clearly in a warm confident American accent, saying exactly: "your soldier will not wake up" then stops speaking and holds the final expression in silence for the rest of the clip.

Negative Constraints: No text overlays. No captions.
```

**Prompt B (policy fallback — use ONLY if Prompt A trips a policy violation; identical prompt with the spoken line reworded):**
```
Create an 8-second vertical 9:16 realistic UGC video.

Dialogue: The main AI generated character speaks clearly in a warm confident American accent, saying exactly: "down there stays asleep every morning" then stops speaking and holds the final expression in silence for the rest of the clip.

Negative Constraints: No text overlays. No captions.
```

## Pre-Flight Checklist
"""

_LEGACY_BUILD = _OMNI_BUILD.replace(
    "## Google Omni Final Prompts (per clip)",
    "## Veo 3.1 Final Prompts (per clip)",
)


def test_omni_header_is_parsed():
    clips = parse_veo_prompts_block(_OMNI_BUILD)
    assert (1, 1) in clips, "Omni header not recognised by the section regex"


def test_legacy_veo_header_still_parsed():
    clips = parse_veo_prompts_block(_LEGACY_BUILD)
    assert (1, 1) in clips, "legacy Veo header regressed"


def test_omni_body_yields_reworded_prompt_b_line():
    clips = parse_veo_prompts_block(_OMNI_BUILD)
    cd = clips[(1, 1)]
    assert cd["prompt_b_line"] == "down there stays asleep every morning"
    assert cd["prompt_b_line"] != "your soldier will not wake up"


def test_negative_constraints_do_not_steal_the_line():
    """The Negative Constraints block sits AFTER the dialogue and must contain
    no double quotes, or it would become the 'last quoted span'."""
    clips = parse_veo_prompts_block(_OMNI_BUILD)
    import re
    a = clips[(1, 1)]["text_prompt"]
    assert re.findall(r'"([^"]+)"', a)[-1] == "your soldier will not wake up"
