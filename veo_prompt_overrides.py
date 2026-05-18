"""
veo_prompt_overrides.py — v572

Parses the optional `## Veo 3.1 Final Prompts (per clip)` section that
sits at the bottom of decoded scene-table markdown files. When present,
each clip's text prompt + negative prompt is taken VERBATIM from the
markdown and fed to Veo, bypassing build_prompt's auto-construction.

This is the "decoded markdown is the single source of truth" path:
the LLM that decoded the source video also wrote the per-clip Veo
prompts, and the platform should ship them through unmodified.

When the section is absent, behavior is unchanged — build_prompt
constructs the prompt from action_note + voice profile + dialogue +
defaults, exactly as before.

Public API:
    parse_veo_prompts_block(md_text) -> Dict[Tuple[int, int], dict]
        Returns a mapping keyed by (scene_index, line_index_within_scene),
        each value is {"text_prompt": str, "negative_prompt": str | None}.
        Empty dict if no section found.

    attach_veo_prompts_to_scenes(scenes, prompts_by_key) -> None
        Mutates each scene dict in-place, adding a `veo_prompts` list
        parallel to the scene's `lines`/`action_notes` lists. Each entry
        is None (no override) or {"text_prompt": str, "negative_prompt": str | None}.

    compose_final_prompt(text_prompt, negative_prompt) -> str
        Concatenates the two into the single string Veo expects.
        Negative prompt is appended as a "Negative prompt:" trailer
        so the existing single-string Veo API path keeps working.

Markdown format (extension to template_new_format):

    ## Veo 3.1 Final Prompts (per clip)

    ### Clip 1.1 — Scene 1, Line 1
    **Start frame:** Image 1
    **Text prompt:**
    ```
    [verbatim Veo text prompt]
    ```
    **Negative prompt:**
    ```
    [verbatim negative prompt]
    ```

    ### Clip 1.2 — Scene 1, Line 2
    ...

The clip header `### Clip {scene}.{line}` is the authoritative key.
The "— Scene N, Line M" descriptive suffix is informational only and
is not parsed. `**Start frame:** Image N` is informational only —
the platform already knows which image powers each scene from the
`### Scene N` block's `image:` field. **Negative prompt:** is
optional (the section may end after the text prompt fence).
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple


# --- Regex anchors ---------------------------------------------------------

# Header for the section. Tolerant of capitalization and trailing punctuation.
_SECTION_HEADER_RE = re.compile(
    r"^##\s+Veo\s*3\.?1\s+Final\s+Prompts\b.*$",
    re.MULTILINE | re.IGNORECASE,
)

# Per-clip header. Captures scene index + optional line index from
# `### Clip S` or `### Clip S.L`. v682i — line index made optional so
# decode-side artifacts that emit one clip per scene (e.g. silent +
# text_card + on-camera 1:1 with scenes — like the Donna decode where
# every scene has exactly one clip and the markdown writes them as
# `### Clip 1 — Scene 1 (silent)` rather than `### Clip 1.1`) parse
# correctly. Pre-v682i these artifacts hit zero matches → empty
# prompts_by_key → no veo_prompt_override on any line at submission.
_CLIP_HEADER_RE = re.compile(
    r"^###\s+Clip\s+(\d+)(?:\.(\d+))?\b[^\n]*$",
    re.MULTILINE | re.IGNORECASE,
)

# Field labels — bold ** wrapping is required to match the rest of the
# scene-table convention.
_TEXT_PROMPT_LABEL_RE = re.compile(
    r"\*\*Text\s+prompt\s*:\*\*",
    re.IGNORECASE,
)
_NEGATIVE_PROMPT_LABEL_RE = re.compile(
    r"\*\*Negative\s+prompt\s*:\*\*",
    re.IGNORECASE,
)

# Triple-backtick fenced block. Optional language hint after the opening
# fence is consumed and ignored. Captures the inner content verbatim
# (no whitespace stripping inside the fence — content is yielded as-is
# except for outer trim).
_FENCE_RE = re.compile(
    r"```[^\n]*\n(.*?)\n```",
    re.DOTALL,
)


# --- Internals -------------------------------------------------------------


def _slice_section(md_text: str) -> Optional[str]:
    """Locate the `## Veo 3.1 Final Prompts` section and return its body
    (the text between the section header and the next `## ` heading or
    end-of-file). Returns None if the section is absent.
    """
    m = _SECTION_HEADER_RE.search(md_text)
    if not m:
        return None
    body_start = m.end()
    next_section = re.search(
        r"^##\s+(?!#)",  # next "## " heading (not "### ")
        md_text[body_start:],
        flags=re.MULTILINE,
    )
    if next_section:
        return md_text[body_start : body_start + next_section.start()]
    return md_text[body_start:]


def _split_into_clip_blocks(section_body: str) -> List[Tuple[int, int, str]]:
    """Split the section body into per-clip blocks. Each entry is
    (scene_index, line_index, block_text). The block_text is the
    content of one `### Clip S.L` block, ending at the next `### Clip`
    header or end-of-section.
    """
    matches = list(_CLIP_HEADER_RE.finditer(section_body))
    if not matches:
        return []
    blocks: List[Tuple[int, int, str]] = []
    for i, m in enumerate(matches):
        scene_idx = int(m.group(1))
        # v682i — line index optional; default to 1 when absent.
        line_idx = int(m.group(2)) if m.group(2) else 1
        block_start = m.end()
        if i + 1 < len(matches):
            block_end = matches[i + 1].start()
        else:
            block_end = len(section_body)
        blocks.append((scene_idx, line_idx, section_body[block_start:block_end]))
    return blocks


def _extract_fenced_content(block: str, label_re: re.Pattern) -> Optional[str]:
    """Find a `**Label:**` marker followed by a triple-backtick fenced
    block, return the fenced content (with outer whitespace trimmed but
    inner whitespace preserved). Returns None if either the label or
    the fence is missing.
    """
    label_m = label_re.search(block)
    if not label_m:
        return None
    after_label = block[label_m.end():]
    fence_m = _FENCE_RE.search(after_label)
    if not fence_m:
        return None
    return fence_m.group(1).strip()


# v750.1 (NEW 2026-05-18 late): v750 codified `### Clip N.M` headers + bolded
# field labels but operators commonly emit UNFENCED prose after `**Text prompt:**`
# (no triple-backtick fences around the prompt body). The artifact at
# `raw/decoded_follow_me_health_remedies_tongue_ginger_turmeric.md` is the
# surfacing case — 5 Clip blocks with `**Text prompt:**` + `**Negative prompt:**`
# bolded labels but multi-paragraph prose body NOT wrapped in fences.
# Pre-v750.1 `_extract_fenced_content` rejected these blocks → empty
# prompts_by_key → veo_prompt_override null on every line → build_prompt
# auto-construction fires + ignores the operator's v750 verbatim prompts.
# v750.1 adds unfenced fallback: after the label, capture content until
# the NEXT bolded label (`**X prompt:**` / `**Start frame:**` / etc.) or the
# next `### Clip` header or end of block — strip outer whitespace.
_NEXT_BOLD_LABEL_RE = re.compile(
    r"^\s*\*\*[A-Za-z][A-Za-z _-]*:\s*\*\*",
    re.MULTILINE,
)
_NEXT_CLIP_HEADER_RE = re.compile(
    r"^###\s+Clip\s+\d+(?:\.\d+)?\b",
    re.MULTILINE,
)


def _extract_unfenced_content(block: str, label_re: re.Pattern) -> Optional[str]:
    """Find a `**Label:**` marker and capture the UNFENCED prose body that
    follows until the next bolded label / next clip header / end of block.
    Returns None if the label is absent or the captured body is empty.

    v750.1: complements `_extract_fenced_content` for operators who emit
    v750 Clip blocks with bolded field labels but multi-paragraph prose
    bodies NOT wrapped in triple-backtick fences.
    """
    label_m = label_re.search(block)
    if not label_m:
        return None
    after_label = block[label_m.end():]
    # Find boundary: next bolded label OR next ### Clip header.
    boundaries = []
    nb = _NEXT_BOLD_LABEL_RE.search(after_label)
    if nb is not None:
        boundaries.append(nb.start())
    nc = _NEXT_CLIP_HEADER_RE.search(after_label)
    if nc is not None:
        boundaries.append(nc.start())
    if boundaries:
        end = min(boundaries)
        body = after_label[:end]
    else:
        body = after_label
    body = body.strip()
    if not body:
        return None
    return body


def _extract_prompt_content(block: str, label_re: re.Pattern) -> Optional[str]:
    """v750.1: try fenced extraction first; fall back to unfenced prose.
    Returns None if the label is absent OR both fenced + unfenced
    extraction yield empty content.
    """
    fenced = _extract_fenced_content(block, label_re)
    if fenced is not None:
        return fenced
    return _extract_unfenced_content(block, label_re)


# --- Public API ------------------------------------------------------------


def parse_veo_prompts_block(md_text: str) -> Dict[Tuple[int, int], Dict[str, Optional[str]]]:
    """Parse the optional `## Veo 3.1 Final Prompts (per clip)` section.

    Returns a dict keyed by (scene_index, line_index) — both 1-based as
    they appear in the markdown — mapping to:
        {"text_prompt": str, "negative_prompt": str | None}

    Empty dict if the section is absent. A clip block missing its
    `**Text prompt:**` fence is silently skipped (a malformed entry
    shouldn't fail the whole import — it just falls back to auto-build).
    """
    section = _slice_section(md_text)
    if section is None:
        return {}

    out: Dict[Tuple[int, int], Dict[str, Optional[str]]] = {}
    for scene_idx, line_idx, block in _split_into_clip_blocks(section):
        # v682i — try the labeled format first (`**Text prompt:**`
        # before the fence). If absent, fall back to the bare-fence
        # format where the clip block has a single ```fence``` directly
        # after the clip header, optionally with an inline
        # "Negative prompt: ..." trailer line inside the same fence.
        # Decode artifacts (Donna, etc.) use the bare-fence shape;
        # the labeled shape is the original lift-side convention.
        #
        # v750.1 (NEW 2026-05-18 late): the labeled extraction now uses
        # `_extract_prompt_content` which tries fenced FIRST then falls back
        # to UNFENCED prose. v750 codified `### Clip N.M` headers + bolded
        # `**Text prompt:**` / `**Negative prompt:**` labels but operators
        # commonly emit multi-paragraph prose AFTER the label WITHOUT
        # triple-backtick fences (see surfacing case at
        # raw/decoded_follow_me_health_remedies_tongue_ginger_turmeric.md).
        # Pre-v750.1 these blocks silently lost their text_prompt /
        # negative_prompt → veo_prompt_override null on every line →
        # build_prompt auto-construction fires + ignores operator's v750
        # verbatim prompts.
        text_prompt = _extract_prompt_content(block, _TEXT_PROMPT_LABEL_RE)
        negative_prompt: Optional[str]
        if text_prompt is not None:
            # Labeled format — read negative from its own fenced OR unfenced body.
            negative_prompt = _extract_prompt_content(block, _NEGATIVE_PROMPT_LABEL_RE)
        else:
            # Bare-fence format — first fence in block is the whole prompt.
            fence_m = _FENCE_RE.search(block)
            if fence_m is None:
                # Clip block without any fence is unusable.
                continue
            full_content = fence_m.group(1).strip()
            # Split on a line starting with "Negative prompt:" (or
            # "Negative Prompt:") into text + negative. If absent, the
            # entire fence content is the text prompt.
            split_m = re.search(
                r"^\s*Negative\s+prompt\s*:\s*(.*)$",
                full_content,
                flags=re.MULTILINE | re.IGNORECASE | re.DOTALL,
            )
            if split_m:
                text_prompt = full_content[: split_m.start()].rstrip()
                negative_prompt = split_m.group(1).strip()
            else:
                text_prompt = full_content
                negative_prompt = None
            if not text_prompt:
                # Empty text content — skip.
                continue
        # Empty negative prompt fence ("```\n\n```") collapses to "" —
        # treat that the same as missing (no override on the negative
        # side, the auto-built negative defaults still apply downstream).
        if negative_prompt == "":
            negative_prompt = None
        out[(scene_idx, line_idx)] = {
            "text_prompt": text_prompt,
            "negative_prompt": negative_prompt,
        }
    return out


def attach_veo_prompts_to_scenes(
    scenes: List[Dict[str, Any]],
    prompts_by_key: Dict[Tuple[int, int], Dict[str, Optional[str]]],
) -> None:
    """Mutate each scene dict in-place, adding a `veo_prompts` list
    parallel to its `lines` list. Each entry is None (no override) or
    a {"text_prompt", "negative_prompt"} dict.

    The clip key in markdown is 1-based: `### Clip 3.2` means scene 3,
    second line within that scene. We map that to the scene's
    scene_index (which is also 1-based as parsed) and the line position
    within the scene (1-based: line 1, line 2, ...).

    Scenes with no overrides registered get a `veo_prompts` list of
    [None, None, ...] — same length as `lines` — so downstream code
    can iterate uniformly. Scenes with the `veo_prompts` key already
    populated are left untouched (idempotent).
    """
    for scene in scenes:
        if "veo_prompts" in scene and scene["veo_prompts"] is not None:
            continue
        scene_index = scene.get("scene_index")
        lines = scene.get("lines") or []
        per_line: List[Optional[Dict[str, Optional[str]]]] = []
        for li in range(1, len(lines) + 1):
            override = prompts_by_key.get((scene_index, li))
            per_line.append(override)

        # v682f — silent / text_card scenes have no `- **line:**` bullets
        # but DO have a `### Clip N — Scene N` entry in the markdown's
        # `## Veo 3.1 Final Prompts` section (every scene gets a clip
        # entry, including the silent ones — clip 1 = bedroom Donna+husband
        # silent, clip 5 = text card no-render placeholder, etc.).
        # Pre-v682f the per_line list was empty (range(1, 1)=empty) so
        # the parsed prompt was discarded. Now we look up (scene_index, 1)
        # for zero-line scenes and emit a single-entry per_line so the
        # synthetic flat-row injection downstream can pick up the
        # silent-clip Veo prompt.
        if not lines:
            override = prompts_by_key.get((scene_index, 1))
            if override is not None:
                per_line = [override]

        scene["veo_prompts"] = per_line


def compose_final_prompt(
    text_prompt: str,
    negative_prompt: Optional[str],
) -> str:
    """Concatenate the per-clip text prompt and negative prompt into the
    single string the existing Veo API path expects.

    Format:
        <text_prompt>

        Negative prompt: <negative_prompt>

    If `negative_prompt` is None or empty, only the text prompt is
    returned (no trailer). The existing build_prompt baked negatives
    into the same string at the end of the prompt — this preserves
    that single-string interface so worker.py and the Flow backend
    don't need any changes.
    """
    text_prompt = (text_prompt or "").strip()
    if not text_prompt:
        # An empty override is effectively no override — the caller
        # should treat this as a fall-through to auto-build, but we
        # return an empty string here in case the caller wants to
        # detect it.
        return ""
    negative_prompt = (negative_prompt or "").strip() if negative_prompt else ""
    if not negative_prompt:
        return text_prompt
    return f"{text_prompt}\n\nNegative prompt: {negative_prompt}"


# --- Self-test (run module directly) --------------------------------------

if __name__ == "__main__":
    sample = """
**Video:** Test
**Persona:** Tester

---

## Storyboard

### Scene 1
- **image:** image_1
- **line:** hello world
- **action_note:** wave at camera

### Scene 2
- **image:** image_2
- **line:** line two a
- **action_note:** action a
- **line:** line two b
- **action_note:** action b

---

## Veo 3.1 Final Prompts (per clip)

### Clip 1.1 — Scene 1, Line 1
**Start frame:** Image 1
**Text prompt:**
```
Static handheld camera. She waves and says: hello world.
Ambient: room tone. (no subtitles, no captions)
```
**Negative prompt:**
```
no montage, no cutaways, no burnt-in text.
```

### Clip 2.1 — Scene 2, Line 1
**Text prompt:**
```
Static camera. She says: line two a.
```

### Clip 2.2 — Scene 2, Line 2
**Text prompt:**
```
Static camera. She says: line two b.
```
**Negative prompt:**
```
no montage, no cutaways.
```
"""
    parsed = parse_veo_prompts_block(sample)
    print("Parsed keys:", sorted(parsed.keys()))
    for k, v in sorted(parsed.items()):
        print(f"  {k}: text={v['text_prompt'][:50]!r}, neg={v['negative_prompt']!r}")

    fake_scenes = [
        {"scene_index": 1, "lines": ["hello world"], "action_notes": ["wave at camera"]},
        {"scene_index": 2, "lines": ["line two a", "line two b"],
         "action_notes": ["action a", "action b"]},
    ]
    attach_veo_prompts_to_scenes(fake_scenes, parsed)
    print()
    print("After attach:")
    for s in fake_scenes:
        print(f"  scene {s['scene_index']}: veo_prompts =")
        for i, vp in enumerate(s["veo_prompts"], 1):
            if vp:
                print(f"    line {i}: text={vp['text_prompt'][:40]!r}, neg={vp['negative_prompt']!r}")
            else:
                print(f"    line {i}: <no override — falls back to build_prompt>")

    print()
    print("compose_final_prompt with both:")
    print("---")
    print(compose_final_prompt("hello", "no foo"))
    print("---")
    print("compose_final_prompt without negative:")
    print("---")
    print(compose_final_prompt("hello", None))
    print("---")
