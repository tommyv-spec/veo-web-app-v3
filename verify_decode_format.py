#!/usr/bin/env python3
"""Pre-ship mechanical linter for raw/videos/decoded_*.md decode artifacts.

Sibling of verify_video_format.py (which lints generate-side videos/*.md).
This one lints the DECODE side: it proves the decode emitted the required
structure INCLUDING the `## Adaptation-extraction` canary block (the per-frame
register/prop/proxy/mood reads + the cross-frame chain/angle map) that the
GENERATE rules depend on. It also proves the v622 hero-symptom intensity ledger
and the v790 shown-beats ledger exist. The shown-beats ledger gives every source
process step and meaningful object a stable ID plus frame/clip evidence, so a
later build can prove it did not compress the demonstration into a vague action.
The visual judgment stays human-confirmed (decode-grammar-checklist.md); this
script catches missing, empty, malformed, or contradictory data.

Usage:  python code/verify_decode_format.py raw/videos/decoded_<id>.md
Exit 0 = pass, 1 = fail, 2 = bad invocation.
"""
import re
import sys

REQUIRED_SECTIONS = [
    "## Pre-Flight Decode Checklist",
    "## Ingredients",
    "## Images",
    "## Storyboard",
    "## Comprehension",
    "## Adaptation-extraction",      # NEW 2026-06-04 canary — the extraction layer
    # v865 — either title is valid: the legacy Veo name or the Google Omni name
    # (Omni Flash is the live render model). A tuple = "any variant present passes".
    ("## Veo 3.1 Final Prompts", "## Google Omni Final Prompts"),
]

ANCHOR_RE = re.compile(
    r"(?:"
    r"\b\d+(?:\.\d+)?\s*(?:%|percent|inches?|cm|mm|feet|foot|thirds?|quarters?|halves?|times?)\b"
    r"|\b(?:half|third|quarter|two[- ]thirds?|three[- ]quarters?)\s+(?:of\s+)?(?:the\s+)?(?:frame|body|screen)\b"
    r"|\b(?:fills?|occupies|covers?|spans?|projects?|extends?|reaches?|dwarfs?|dwarfing|larger\s+than|smaller\s+than|relative\s+to|compared\s+(?:with|to)|past\s+the|beyond\s+the|(?:nearly\s+)?erases?|erasing\s+the|visible\s+through)\b"
    r")",
    re.I,
)


def _clean_cell(value):
    return re.sub(r"[*_`]", "", value).strip()


def _table_rows(block):
    """Return four-cell data rows from the first Markdown table in block."""
    rows = []
    for line in block.splitlines():
        if not line.strip().startswith("|"):
            continue
        cells = [_clean_cell(c) for c in line.strip().strip("|").split("|")]
        if len(cells) != 4:
            continue
        if cells[0].lower() == "hero symptom / carrier":
            continue
        if all(re.fullmatch(r":?-{3,}:?", c) for c in cells):
            continue
        rows.append(cells)
    return rows


def _lint_intensity_ledger(adaptation_body, fails):
    """Mechanical half of v622. Source-frame accuracy remains a human check."""
    m = re.search(
        r"^###\s+Hero-symptom intensity ledger\s*$\n(.*?)(?=^###\s|^##\s|\Z)",
        adaptation_body,
        re.S | re.M | re.I,
    )
    if not m:
        fails.append("## Adaptation-extraction missing `### Hero-symptom intensity ledger`")
        return

    rows = _table_rows(m.group(1))
    if not rows:
        fails.append("hero-symptom intensity ledger has no four-column data row")
        return

    none_rows = [r for r in rows if r[0].strip().lower() == "none observed"]
    if none_rows:
        if len(rows) != 1 or any(c.strip().lower() != "n/a" for c in none_rows[0][1:]):
            fails.append("`none observed` must be the only ledger row and use n/a in all other cells")
        return

    for symptom, anchor, intensity, headroom in rows:
        prefix = f"hero symptom `{symptom}`"
        if not ANCHOR_RE.search(anchor):
            fails.append(
                f"{prefix} lacks a literal comparison anchor "
                "(frame fraction, body/object relation, measurement, coverage, count, or projection)"
            )

        im = re.search(r"\b([1-5])\s*/\s*5\b", intensity)
        if not im:
            fails.append(f"{prefix} intensity must contain 1/5, 2/5, 3/5, 4/5, or 5/5")

        hm = re.fullmatch(r"\s*(YES|NO)\s*", headroom, re.I)
        if not hm:
            fails.append(f"{prefix} exaggeration headroom must be exactly YES or NO")
        elif im and im.group(1) == "5" and hm.group(1).upper() == "YES":
            fails.append(f"{prefix} is 5/5 viral-max but says headroom YES")


def _lint_shown_beats_ledger(adaptation_body, fails):
    """Mechanical half of v790 source-beat capture. Frame truth stays human."""
    m = re.search(
        r"^###\s+Shown beats ledger\s*$\n(.*?)(?=^###\s|^##\s|\Z)",
        adaptation_body,
        re.S | re.M | re.I,
    )
    if not m:
        fails.append("## Adaptation-extraction missing `### Shown beats ledger`")
        return

    rows = []
    for line in m.group(1).splitlines():
        if not line.strip().startswith("|"):
            continue
        cells = [_clean_cell(c) for c in line.strip().strip("|").split("|")]
        if len(cells) != 4:
            continue
        if cells[0].lower() == "source beat id":
            continue
        if all(re.fullmatch(r":?-{3,}:?", c) for c in cells):
            continue
        rows.append(cells)

    if not rows:
        fails.append("shown-beats ledger has no four-column data row")
        return

    none_rows = [r for r in rows if r[0].strip().lower() == "none observed"]
    if none_rows:
        if len(rows) != 1 or any(c.strip().lower() != "n/a" for c in none_rows[0][1:]):
            fails.append("`none observed` must be the only shown-beats row and use n/a in all other cells")
        return

    expected_ids = [f"SB{i}" for i in range(1, len(rows) + 1)]
    actual_ids = [row[0].upper() for row in rows]
    if actual_ids != expected_ids:
        fails.append(
            "shown-beats IDs must be unique and ordered SB1, SB2, SB3... "
            f"(found: {', '.join(actual_ids)})"
        )

    for beat_id, evidence, action, objects in rows:
        prefix = f"shown beat `{beat_id}`"
        if not re.search(r"\b(?:clip|frame)\b", evidence, re.I):
            fails.append(f"{prefix} evidence must name a source clip or frame")
        if not action.strip() or action.strip().lower() in {"n/a", "none"}:
            fails.append(f"{prefix} must name the visible action or process step")
        if not objects.strip():
            fails.append(f"{prefix} must name the visible objects, or say `none`")


def lint(path):
    try:
        with open(path, encoding="utf-8") as source:
            t = source.read()
    except OSError as e:
        print(f"FAIL: cannot read {path}: {e}")
        return 1

    fails, warns = [], []

    # 1. required top-level sections present (incl the canary)
    # v865 — a tuple entry passes when ANY of its title variants is present.
    for s in REQUIRED_SECTIONS:
        variants = s if isinstance(s, tuple) else (s,)
        if not any(v in t for v in variants):
            fails.append("missing required section: " + " or ".join(variants))

    # 2. the canary block must be non-empty + carry the extraction content
    m = re.search(r'^##\s+Adaptation-extraction.*?$(.*?)(?=^##\s|\Z)', t,
                  re.S | re.M)
    if m:
        adaptation_body = m.group(1)
        body = adaptation_body.lower()
        need = {"register": "per-frame register read",
                "proxy": "symptom-proxy state read",
                "chain": "recurring-character chain map",
                "angle": "HOOK-angle / swap-layer map"}
        for kw, label in need.items():
            if kw not in body:
                fails.append(f"## Adaptation-extraction missing the {label} (keyword '{kw}')")
        _lint_intensity_ledger(adaptation_body, fails)
        _lint_shown_beats_ledger(adaptation_body, fails)

        # v887a — audio design read (WARN-only, forward-only 2026-08-03:
        # required on NEW decodes, the 148 pre-v887 decodes are never retro-failed)
        if "### audio design read" not in adaptation_body.lower():
            warns.append("v887a: no `### Audio design read` block "
                         "(required on NEW decodes; pre-v887 decodes exempt)")

    # v887c — style register + source aspect frontmatter (WARN-only, forward-only 2026-08-03)
    if not re.search(r'^style_register:', t, re.M):
        warns.append("v887c: no `style_register:` frontmatter key "
                     "(required on NEW decodes; pre-v887 decodes exempt)")
    if not re.search(r'^aspect_source:', t, re.M):
        warns.append("v887c: no `aspect_source:` frontmatter key "
                     "(required on NEW decodes; pre-v887 decodes exempt)")

    # 3. NO negative-prompt block (2026-06-04 no-negatives standing rule)
    if re.search(r'^\s*\*\*Negative(\s+prompt)?:\*\*', t, re.M):
        fails.append("**Negative prompt:** / **Negative:** block present - banned "
                     "(2026-06-04 no-negatives rule); bake constraints into the positive Text prompt")

    # 4. Veo section in v750 format (Text prompt), not legacy Text:/Voice:
    if "## Veo" in t:
        if "**Text prompt:**" not in t and re.search(r'\*\*Text:\*\*', t):
            warns.append("Veo uses legacy **Text:** - should be v750 **Text prompt:**")

    # 5. cardinality parse + no duplicate image numbers
    imgs = re.findall(r'^###\s+Image\s+(\d+)', t, re.M)
    scenes = re.findall(r'^###\s+Scene\s+(\d+)\s*$', t, re.M)
    clips = re.findall(r'^###\s+Clip\s+', t, re.M)
    if len(imgs) != len(set(imgs)):
        dupes = sorted({i for i in imgs if imgs.count(i) > 1})
        fails.append(f"duplicate ### Image N numbers: {dupes}")

    status = "FAIL" if fails else "PASS"
    print(f"{path}")
    print(f"  Images {len(imgs)} / Scenes {len(scenes)} / Clips {len(clips)} | {status}")
    for f in fails:
        print("  FAIL:", f)
    for w in warns:
        print("  WARN:", w)
    return 1 if fails else 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: python code/verify_decode_format.py raw/videos/decoded_<id>.md")
        sys.exit(2)
    sys.exit(lint(sys.argv[1]))
