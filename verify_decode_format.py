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

# A LITERAL anchor: a measured amount, a fraction of the frame, a coverage or
# projection verb, an explicit comparison, an in-frame A/B control, or a spatial
# relation to a named landmark. It must still reject bare severity words
# ("severe", "noticeable", "5/5") — that is the whole point of the check.
#
# 2026-09-01: widened after it rejected three genuine anchors in one decode batch
# (measured: only 3 decodes in 207 failed on it, so this loosens nothing that was
# passing). The misses were (a) inflected verbs — the list had `covers?` but the
# row said "covering", (b) an in-frame A/B control, which is the STRONGEST anchor
# a side-by-side demo can have ("floats while the identical fish beside it reaches
# the tank bottom"), and (c) a body-to-body comparison quoted from the script
# ("chest sagging lower than your wife's"). A checker that rejects real evidence
# teaches writers to reword for the parser — the exact failure mode that
# `check_ingest._has_evidence_locator` carries its own warning comment about.
ANCHOR_RE = re.compile(
    r"(?:"
    # a measured quantity
    r"\b\d+(?:\.\d+)?\s*(?:%|percent|inches?|cm|mm|feet|foot|thirds?|quarters?|halves?|times?)\b"
    # a fraction of the frame / body / screen
    r"|\b(?:half|third|quarter|two[- ]thirds?|three[- ]quarters?)\s+(?:of\s+)?(?:the\s+)?(?:frame|body|screen)\b"
    # a coverage or projection verb, any inflection
    r"|\b(?:fill|cover|span|project|extend|reach|dwarf|engulf|pack|overhang|protrud)(?:e|es|s|ed|ing)?\b"
    r"|\boccup(?:y|ies|ied|ying)\b"
    # an explicit comparison ("lower than", "wider than"), minus the false friends
    r"|\b(?!(?:other|rather)\b)\w+er\s+than\b"
    r"|\b(?:relative\s+to|compared\s+(?:with|to)|than\s+the\s+(?:other|second|identical))\b"
    # an in-frame A/B control — the reference state visible in the same shot
    r"|\bwhile\s+the\s+(?:other|second|identical|same)\b"
    r"|\bbeside\s+(?:it|him|her|them)\b"
    # a spatial relation to a named landmark
    r"|\b(?:past|beyond|above|below|over|under)\s+(?:the|his|her|their|its)\b"
    r"|\b(?:nearly\s+)?erases?\b|\berasing\s+the\b|\bvisible\s+through\b"
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


OVERLAY_WORDS = re.compile(
    r"composite|green[- ]?screen|keyed|picture[- ]in[- ]picture|\bpip\b|split[- ]screen|"
    r"cut[- ]?out|inset|overlay|b[- ]?roll|insert panel|letterbox", re.I)


def _lint_overlay_ledger(adaptation_body, full_text, new_era, fails, warns):
    """v938.4 — everything an editor needs to REBUILD the composition.

    v938.3 made the corner speaker's box measurable. But a composed video is
    more than its one keyed layer: b-roll inserts sit in a specific box for
    specific seconds, stills get placed over the frame, the caption band moves,
    a watermark occupies a corner. Replicating the video needs all of it, and
    the decode was recording none of it — the operator's point: *"not just the
    corner thing, but also the info we need to replicate a composed video, with
    brolls or images — where they place the images, all the details."*

    So: one row per overlaid element, with a MEASURED box and the window it
    holds for. Same philosophy as every other ledger here — the checker cannot
    judge whether a box is right, so it requires the declaration and requires
    it to carry numbers.

    Only demanded when the decode itself shows overlay evidence, so a plain
    single-take talking head is never asked for one. `| none observed |` is an
    explicit, legal answer for a source that composites nothing.
    """
    if not OVERLAY_WORDS.search(full_text):
        return
    m = re.search(
        r"^###\s+Edit-layer overlay ledger\s*$\n(.*?)(?=^###\s|^##\s|\Z)",
        adaptation_body, re.S | re.M | re.I)
    if not m:
        msg = ("v938.4: this decode shows composite/overlay/b-roll work but "
               "`## Adaptation-extraction` has no `### Edit-layer overlay ledger` — one row per "
               "overlaid element (`element | source | box | window (s) | layer | notes`) with the "
               "box as fractions of the frame, or a single `| none observed | n/a | n/a | n/a | "
               "n/a | n/a |` row. Without it the composition cannot be rebuilt: knowing a b-roll "
               "exists does not say where it sat or for how long. Measure with tools/grid_frames.py")
        (fails if new_era else warns).append(msg)
        return

    rows = []
    for line in m.group(1).splitlines():
        if not line.strip().startswith("|"):
            continue
        cells = [_clean_cell(c) for c in line.strip().strip("|").split("|")]
        if len(cells) != 6:
            continue
        if cells[0].lower() == "element":
            continue
        if all(re.fullmatch(r":?-{3,}:?", c) for c in cells):
            continue
        rows.append(cells)

    if not rows:
        (fails if new_era else warns).append(
            "v938.4: overlay ledger has no six-column data row "
            "(`element | source | box | window (s) | layer | notes`)")
        return

    if len(rows) == 1 and rows[0][0].strip().lower() == "none observed":
        return

    vague = [r[0] for r in rows if len(re.findall(r"\d", r[2])) < 2]
    if vague:
        (fails if new_era else warns).append(
            "v938.4: overlay ledger rows %s have no measured box — write fractions of the "
            "frame (`x[0..0.43W] y[0.57H..H]`), not 'lower third' or 'small inset'. "
            "The adjective is the failure mode this ledger exists to fix"
            % ", ".join(repr(v) for v in vague[:4]))
    no_window = [r[0] for r in rows if not re.search(r"\d", r[3])]
    if no_window:
        (fails if new_era else warns).append(
            "v938.4: overlay ledger rows %s have no window — say WHEN each element is on "
            "screen in seconds (`12.6-16.8`, or `whole` for a persistent element). An overlay "
            "with no timing cannot be placed back on a timeline"
            % ", ".join(repr(v) for v in no_window[:4]))


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

    # The decode's own `created:` date is the era boundary for every
    # forward-only rule below. Parsed ONCE here because two separate checks now
    # need it and they run in different blocks.
    V938_DATE = "2026-08-22"        # v938.3 / v938.4 — measured composite + overlay geometry
    created = ""
    cm = re.search(r'^created:\s*([0-9]{4}-[0-9]{2}-[0-9]{2})', t, re.M)
    if cm:
        created = cm.group(1)
    geom_era = bool(created) and created >= V938_DATE

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
        _lint_overlay_ledger(adaptation_body, t, geom_era, fails, warns)

        # v887a — audio design read (WARN-only, forward-only 2026-08-03:
        # required on NEW decodes, the 148 pre-v887 decodes are never retro-failed)
        if "### audio design read" not in adaptation_body.lower():
            warns.append("v887a: no `### Audio design read` block "
                         "(required on NEW decodes; pre-v887 decodes exempt)")

    # v887c — style register frontmatter (WARN-only, forward-only 2026-08-03)
    if not re.search(r'^style_register:', t, re.M):
        warns.append("v887c: no `style_register:` frontmatter key "
                     "(required on NEW decodes; pre-v887 decodes exempt)")
    # v887c — source aspect frontmatter (WARN-only, forward-only 2026-08-03)
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

    # 6. v890b - a decode's ### Image N block carries the SAME level of detail as
    #    a build's: a fenced reconstruction prompt you could regenerate the source
    #    frame from, not a one-line description. Two jobs depend on it - recreating
    #    the frame with our variables swapped, and comparing backgrounds, palettes,
    #    props and wardrobe field-by-field across the siblings of a decode family.
    #    (Operator 2026-08-12: "i need to be able to recreate the images from these
    #    descriptions... also the decode should have the same level of details,
    #    because that's what we use to compare variables and constants.")
    #    Forward-only: decodes written before this rule keep their thin blocks.
    if imgs:
        # Forward-only, made MECHANICAL: a decode written on or after the rule
        # date must comply (FAIL); anything older keeps its thin blocks (WARN).
        # Prose forward-only rules do not hold - the honest-count rule was broken
        # twice in the commit that added it - so the era boundary is a date the
        # linter reads, not a habit.
        V890B_DATE = "2026-08-12"
        # `created` and geom_era (v938, 2026-08-22) are parsed once at the top
        # of lint() — two blocks need them now.
        new_era = bool(created) and created >= V890B_DATE

        # Field names may carry an annotation before the colon, e.g.
        # `- **on-screen text (POST overlay, not rendered):**` - match the NAME.
        REQUIRED_FIELDS = ("background", "palette", "wardrobe", "props",
                           "on-screen text", "camera", "lighting")

        def _has_field(body_lc: str, field: str) -> bool:
            return re.search(r'^\s*[-*]\s*\*\*%s\b[^*:]*:' % re.escape(field),
                             body_lc, re.M) is not None
        blocks = re.split(r'^###\s+Image\s+\d+', t, flags=re.M)[1:]
        thin, missing_fields = [], []
        for num, body in zip(imgs, blocks):
            body = re.split(r'^##\s+', body, flags=re.M)[0]
            if not ("**Image prompt:**" in body and "```" in body):
                thin.append(num)
                continue
            absent = [f for f in REQUIRED_FIELDS if not _has_field(body.lower(), f)]
            if absent:
                missing_fields.append("Image %s (%s)" % (num, ", ".join(absent)))

        problems = []
        if thin:
            problems.append(
                "### Image %s carry no fenced `- **Image prompt:**` reconstruction block"
                % ", ".join(thin)
            )
        if missing_fields:
            problems.append(
                "comparison fields missing on %s" % "; ".join(missing_fields)
            )
        if problems:
            msg = (
                "v890b: %s - a decode's image blocks must be REGENERABLE (fenced prompt) and "
                "COMPARABLE (background/palette/wardrobe/props/on-screen text/camera/lighting), "
                "same detail level as a build's; compose from stage4d frame_inventory + "
                "start_frame_spec. Captions stay in the on-screen-text FIELD, never in the prompt."
                % " AND ".join(problems)
            )
            (fails if new_era else warns).append(msg)

        # v890d - a composite frame is TWO renders. When any image block names an
        # assembled production method, the decode must carry both layers with the
        # pairing declared, or the operator has nothing to assemble.
        composite_words = re.compile(
            r"green[- ]?screen|keyed|composite|picture[- ]in[- ]picture|\bpip\b|split[- ]screen|"
            r"cut[- ]?out|overlaid layer", re.I)
        layered = re.compile(r"\*\*composite_layer:\*\*", re.I)
        # v890d.1 - every object in an assembled frame belongs to exactly ONE
        # layer, and proximity does not decide which. Both a human reader and the
        # 1fps VLM pass put the source's microphone on the shouting foreground
        # woman because it sat by her shoulder; one frame showed it monochrome,
        # cropped by her matte edge and aimed at the man behind her - it was his,
        # on the plate. Wrong layer = the object renders into the wrong prompt, so
        # the assembled frame carries two of it and the wrong person gets what it
        # means. The checker cannot judge an attribution, so it requires the
        # DECLARATION: each layer block names the tell that put its contents
        # there. An author made to name a tell has to go look for one.
        evidence_field = re.compile(r"\*\*layer_evidence:\*\*", re.I)
        # v938.3 - a composite's FOREGROUND layer must carry MEASURED geometry,
        # not an adjective. Decodes have been recording "small PiP bottom-left"
        # for a year; when the edit layer finally needed the number, nobody had
        # it and the frames had to be re-measured from scratch. They turned out
        # to be strikingly consistent - 15 of 16 corner composites in the corpus
        # sit bottom-LEFT at ~37-40% of frame width and ~29-33% of height, flush
        # to both edges, for the hook only - which is exactly the kind of fact a
        # decode exists to capture and prose threw away.
        #
        # Same philosophy as layer_evidence above: the checker cannot judge
        # whether a box is correct, so it requires the DECLARATION, and requires
        # it to contain NUMBERS. An author made to write a fraction has to go
        # measure one. Overlay a grid on the frame first:
        #   ffmpeg -i f.jpg -vf "drawgrid=w=iw/10:h=ih/10:t=2:c=red@0.9" g.jpg
        # (tools/grid_frames.py does this for a whole frame directory.)
        geom_field = re.compile(r"\*\*composite_geometry:\*\*", re.I)
        bg_layer = re.compile(r"\*\*composite_layer:\*\*\s*background[- ]?plate", re.I)
        no_geometry, vague_geometry = [], []
        comp_blocks, layer_blocks, no_evidence = [], 0, []
        for num, body in zip(imgs, blocks):
            body = re.split(r'^##\s+', body, flags=re.M)[0]
            if layered.search(body):
                layer_blocks += 1
                if not evidence_field.search(body):
                    no_evidence.append(num)
                # the plate is full-frame by definition; only the layer IN FRONT
                # has a box worth recording
                if not bg_layer.search(body):
                    m = re.search(
                        r'^\s*[-*]\s*\*\*composite_geometry:\*\*(.*?)(?=^\s*[-*]\s*\*\*|\Z)',
                        body, re.M | re.S | re.I)
                    if not m:
                        no_geometry.append(num)
                    elif len(re.findall(r'\d', m.group(1))) < 2:
                        vague_geometry.append(num)
            method = re.search(r'^\s*[-*]\s*\*\*production method[^*:]*:\*\*(.*)$',
                               body, re.M | re.I)
            if method and composite_words.search(method.group(1)):
                comp_blocks.append(num)
        if no_evidence:
            msg = (
                "v890d.1: ### Image %s carry `- **composite_layer:**` but no "
                "`- **layer_evidence:**` - name, for the objects in this layer, the tell that "
                "puts them HERE (colour grade, which edge occludes which, what moves with the "
                "subject, where a mic or a hand actually points). An object is not on a layer "
                "because it sits near someone who is"
                % ", ".join(no_evidence)
            )
            (fails if new_era else warns).append(msg)
        if no_geometry:
            msg = (
                "v938.3: ### Image %s carry a foreground `- **composite_layer:**` but no "
                "`- **composite_geometry:**` - record the MEASURED box, the edges it is flush "
                "to, when it starts and ends, and what the plate behind it is doing "
                "(e.g. `box: x[0..0.43W] y[0.57H..H] | flush: left, bottom | window: 0-2.5s "
                "(hook only) | plate: sharp full-frame`). Overlay a grid on the frame and read "
                "it off: tools/grid_frames.py, or ffmpeg -vf drawgrid=w=iw/10:h=ih/10"
                % ", ".join(no_geometry)
            )
            (fails if geom_era else warns).append(msg)
        if vague_geometry:
            msg = (
                "v938.3: ### Image %s have `- **composite_geometry:**` with almost no numbers in "
                "it - 'small PiP bottom-left' is the description the corpus already had and it is "
                "what forced a re-measure. Write fractions of the frame, not adjectives"
                % ", ".join(vague_geometry)
            )
            (fails if geom_era else warns).append(msg)
        if comp_blocks and layer_blocks < 2:
            msg = (
                "v890d: ### Image %s declare an assembled/composite production method but the "
                "decode carries %d `- **composite_layer:**` block(s) - a composite is TWO renders "
                "(background-plate + foreground-keyed), each with its own prompt and its own clip, "
                "or there is nothing to assemble"
                % (", ".join(comp_blocks), layer_blocks)
            )
            (fails if new_era else warns).append(msg)

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
