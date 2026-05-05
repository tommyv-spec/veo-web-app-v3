#!/usr/bin/env bash
# create_bundle.sh — concat the canonical create-bundle files + pipe to clipboard.
#
# Use case: AUTHOR A NEW videos/*.md FROM 0 (no decoded source).
# The operator picks a niche × audience × persona cell from the corpus-
# validated matrix and lets the LLM author a fresh recreation.
#
# Usage:
#   ./code/create_bundle.sh
#
# Sister scripts (different task entry-points, SAME canonical wiki bundle):
#   ./code/lift_bundle.sh <decoded-artifact.md>   — recreate FROM a decoded source
#   ./code/decode_bundle.sh <source-mp4>          — decode a new viral video
#   ./code/create_bundle.sh                       — author from 0 (this script)
#
# What it does:
#   - Concatenates the 15 canonical wiki + code files (per wiki/meta/lift-bundle.md)
#   - Pipes the concatenation to the system clipboard (with temp-file fallback)
#   - Operator pastes the bundle into any LLM + a one-line cell spec:
#         "create a new videos/*.md for [persona] [niche] [audience] —
#          use a corpus-validated cell from strategy-mechanisms.md"
#
# The bundle is transient — never committed. Wiki edits propagate on
# next invocation. Exact same wiki files as lift_bundle.sh; only the
# task prompt at the end differs.

set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Detect clipboard tool
if command -v clip >/dev/null 2>&1; then
    CLIP_CMD="clip"
elif command -v clip.exe >/dev/null 2>&1; then
    CLIP_CMD="clip.exe"
elif command -v pbcopy >/dev/null 2>&1; then
    CLIP_CMD="pbcopy"
elif command -v xclip >/dev/null 2>&1; then
    CLIP_CMD="xclip -selection clipboard"
elif command -v xsel >/dev/null 2>&1; then
    CLIP_CMD="xsel --clipboard --input"
else
    CLIP_CMD=""
fi

# Bundle file list — same wiki canonical files as lift_bundle.sh
# (must stay in sync with wiki/meta/lift-bundle.md)
BUNDLE_FILES=(
    "wiki/meta/viral-video-pipeline.md"
    "wiki/audience/niche-audience-matrix.md"
    "wiki/audience/strategy-mechanisms.md"
    "wiki/audience/psychology-of-conversion.md"
    "wiki/audience/audience-mapping.md"
    "wiki/audience/pain-point-language.md"
    "wiki/audience/video-types.md"
    "wiki/mechanics/hook-patterns.md"
    "wiki/mechanics/cta-patterns.md"
    "wiki/mechanics/scene-structure.md"
    "wiki/strategy/risky-vocabulary.md"
    "wiki/strategy/meta-policy-2026.md"
    "wiki/strategy/viral-recreation-method.md"
    "code/template_reference.md"
    "code/template_new_format.md"
)

# Verify
MISSING=0
for f in "${BUNDLE_FILES[@]}"; do
    if [[ ! -f "$REPO_ROOT/$f" ]]; then
        echo "ERROR: bundle file missing: $f" >&2
        MISSING=$((MISSING + 1))
    fi
done
if [[ $MISSING -gt 0 ]]; then
    echo "ABORTING: $MISSING bundle files missing." >&2
    exit 1
fi

TIMESTAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

build_bundle() {
    cat <<EOF
# CREATE-FROM-0 BUNDLE — generated $TIMESTAMP

You are about to AUTHOR A NEW videos/*.md from 0 (no decoded source).
The operator's task prompt at the end will specify the cell — niche
x audience x persona — usually picked from a corpus-validated row in
strategy-mechanisms.md.

Required output discipline:
  - Pick a CORPUS-VALIDATED cell (see strategy-mechanisms.md "Most-
    validated cells" + "What works + why"). Do NOT speculate on cells
    flagged with 0 raw evidence (menopause-F40+-standalone, etc.) —
    those need decodes first.
  - NAME the dominant cognitive move BEFORE writing a line (per
    psychology-of-conversion.md mandatory step). Example: "F-to-F-
    about-M x ED uses vicarious-benefit projection + visual-pun shame
    mediation + TABOO-WARNING + agency reinforcement."
  - Apply v521.1 -> v597 rules per code/template_reference.md
  - v594 image cardinality: emit M images for K dialogue scenes where
    M < K typical (consolidate per composition; multiple Scene blocks
    can reference the same image_M)
  - v590 chain optionality: chain only when v580 recipe state-evolution,
    v541 transformation, two-shot follow-up, or single-shot action arc
    requires it
  - v591 novelty-gate: confirm the visual hook hasn't been seen on any
    LiB Inspire account before locking
  - v592 motion-text-match: voiceover verb at second N matches visible
    motion at second N
  - v593 strict-header parser: ### Image N + ### Scene N must end after
    the integer; action_note is single-line prose; no h4 sub-scenes
  - risky-vocabulary policy-flag pass: scan dialogue for TikTok/Meta
    red-words; swap with safer-metaphor preserving mechanism
  - Universal closer: "follow me first or I can't reach you"
  - For DAY1/14 transformation niches: "$X surgery you didn't pay"
    anchor in EXPLAIN

Read all 15 canonical bundle files below. Then identify which cell
the operator wants from their task prompt, name the dominant move,
and author the videos/*.md per code/template_new_format.md skeleton.

Total bundle: $((${#BUNDLE_FILES[@]})) canonical files

EOF

    for f in "${BUNDLE_FILES[@]}"; do
        cat <<EOF

================================================================================
# FILE: $f
================================================================================

EOF
        cat "$REPO_ROOT/$f"
        echo ""
    done

    cat <<'TASKEOF'

================================================================================
# TASK
================================================================================

Author a NEW Korella videos/*.md from 0 for the cell specified in the
operator's task prompt. Pick the cell from a corpus-validated row in
strategy-mechanisms.md if the operator hasn't been specific.

Output the videos/*.md per code/template_new_format.md skeleton + strict
v593 parser format. End with ## Sources (no decoded source — note
"created from 0 based on corpus-validated cell + bundle [date]") and
## Used in (placeholder).

If unsure which cell to pick, the safest default is the most-validated:
F-to-F-about-M x ED x Black-female-practitioner x saffron-Template-A
(4 corpus instances).

================================================================================
# CANONICAL ACTION_NOTE EXAMPLE — match this shape EXACTLY
================================================================================

Action_notes are the most commonly-malformed field. Plain narrative is
WRONG. Every action_note must follow this shape (one line, with inline
[Start beat] / [Mid-clip beat] / [End beat] markers + register tag +
ambient cue):

  Static handheld camera, no camera move, slight natural drift (per
  v585: mag 1.13px, no dominant axis). The main character + the
  seed-covered face mask + the small glass jar of seeds in his right
  hand. [Start beat 0-2s] Mask is fully on his face, jar held at chest
  height square to camera, eyes lock to lens, mouth opens on "What if
  I told you". [Mid-clip beat 2-3s] On "you're throwing away" his
  left hand briefly rises into the lower-foreground in a small
  dismissive flick gesture toward off-frame, then drops. [End beat
  3-3.9s] On "the most powerful part of the papaya" his right hand
  lifts the seed jar slightly higher toward camera as a visual
  evidence-presentation, eyes never leaving the lens, eyebrows lower
  in conspiratorial-confrontational emphasis with deadpan delivery.
  [Deadpan curiosity-gap delivery]. Ambient: muted Asian apothecary
  tone, soft warmth of natural daylight, faint distant herb-jar clink.

Five required parts in every action_note:
  1. v585 cinematography opener: "Static handheld..." or named
     camera-move (push-in / pan-left / tilt-down) with mag
  2. Subject inventory: "The main character + [props] + [secondary]"
  3. THREE timed beats: [Start beat 0-Xs] / [Mid-clip beat X-Ys] /
     [End beat Y-Zs] with explicit second ranges and what changes
  4. Register tag in square brackets: [Conspiratorial-warm register]
     or [Symptom-validation register] etc.
  5. Ambient sound cues at the end

================================================================================
# PRE-OUTPUT SELF-VALIDATION CHECKLIST
================================================================================

Before emitting your final videos/*.md, walk this checklist mentally
and FIX any item that fails. Do NOT skip — these are the most-
violated rules in past LLM outputs:

[1] STRICT HEADERS (v593) — every "### Image N" and "### Scene N"
    line ends after the integer. NO descriptive suffix. NO "### Image
    1 — HOOK" or "#### Scene 8a". If you need to split a scene by
    clip, add a second `- **line:**` + `- **action_note:**` pair
    inside the same Scene block.

[2] CHAIN-BINDING LINE PRESENT (v589.1) — every Image with
    `reference_image: image_K` (not "none") has a line at the START
    of its prompt body that reads: "Use the prior-scene reference
    image to preserve [setting], [lighting], [anchor props], and
    continuity from the previous scene." This is REQUIRED. Do NOT
    use the legacy form "Use Image K as the visual reference..." —
    the semantic phrase is the canonical form per v589.1.

[3] NO LOWERCASE "image K" IN BODY PROSE (v589.1) — never write
    "same as image 1", "from image 3", "as image 4", "in image 5".
    The platform's case-sensitive substitution doesn't rewrite
    lowercase, so Banana 2 sees a phantom reference. Required
    substitutions: "as the previous scene", "from the previous
    scene", "same as before", "the same X". If you genuinely need
    to reference a specific earlier image, use capital "Image 4"
    (capital I) in body prose and the platform will substitute it.

[4] ACTION_NOTE STRUCTURE (v586) — every action_note follows the
    canonical shape above (cinematography opener + subject + THREE
    timed beats with [Start/Mid-clip/End beat] markers + register
    tag + ambient cue). Plain narrative without timed beats is
    insufficient. Single line — multiline bulleted Cinematography/
    Subject/Action/Context/Style structures DO NOT PARSE.

[5] CHAIN ONLY WHEN REQUIRED (v590) — set `reference_image: none`
    by default. Only chain when ONE of these four conditions
    applies: (a) v580 recipe state-evolution (each step inherits
    cumulative ingredient state); (b) v541 before/after transformation
    (Day-1 -> Day-14 same-body); (c) two-shot follow-up (preserve
    secondary-character identity from a prior two-shot); (d) single-
    shot action arc anchoring. SETTING CHANGES ARE NEVER CHAINED —
    if Image N is a different room/desk/location, it's `none` even
    if the persona is the same.

[6] PERSONA FROM PERSONA-MAP (not invented) — pick from corpus-
    validated personas in wiki/persona-map.md (or the persona pages
    referenced by strategy-mechanisms.md). The 4 corpus-validated
    personas as of bundle date: Black-female-practitioner (Korella
    saffron presenter), dr-sage (silver-haired clinical), master-shen
    (Chinese-elder anti-mainstream), plus existing master-chen,
    master-salvora, dr-kim, dr-aesthetic, old-earl, rastajahmeil,
    mama-rasta. DO NOT invent a new persona name.

[7] RISKY-VOCABULARY SWAPPED IN DIALOGUE — actually apply the swaps
    from risky-vocabulary.md. "Menopause" -> "after 40" / "the change".
    "Erection" -> "morning signal" / "back to attention". "Performance"
    in sexual context -> "going strong" / "lasting". "Cure" / "treats"
    / "reverses" + disease name -> never. Awareness-only without
    swap is insufficient — actually edit the lines.

[8] CELL HONESTY — if the operator's prompt requested a cell flagged
    as speculative in strategy-mechanisms.md (menopause-F40+-standalone,
    high-cortisol-F40+, parasites-F40+, T2-anatomy-ED-Neutral, T1-
    hospital-bed setting, multi-patient-montage hook), state in
    ## Sources that the cell is "adjacent to validated" or "speculative"
    — do NOT claim "corpus-validated" in Sources for a speculative cell.

[9] UNIVERSAL CLOSER (mandatory final line) — "follow me first or I
    can't reach you" or close variant. Verbatim across 12+ raw
    decodes; deviate only with documented reason.

[10] M <= N IMAGE CARDINALITY (v594) — count distinct images vs total
     scenes. M (images) must be <= N (scenes). Two scenes that share
     composition (same setting + blocking + camera) should reuse the
     same image_K via the Scene block's `image:` field. Setting
     changes always require a new image (never reuse across settings).

If any item above fails, FIX IT BEFORE OUTPUT. The operator will
re-prompt you to fix violations otherwise. Self-correction here saves
a round-trip.

TASKEOF
}

# Always write to a temp file as fallback
TMPDIR_PATH="${TMPDIR:-/tmp}"
BUNDLE_FILE="$TMPDIR_PATH/create_bundle_$(date +%s).md"
build_bundle > "$BUNDLE_FILE"
BYTES=$(wc -c < "$BUNDLE_FILE")

if [[ -n "$CLIP_CMD" ]]; then
    if cat "$BUNDLE_FILE" | $CLIP_CMD 2>/dev/null; then
        echo "[create_bundle] OK: ${#BUNDLE_FILES[@]} files concatenated (~${BYTES} bytes), piped via '$CLIP_CMD'"
        echo "[create_bundle] Bundle also saved to: $BUNDLE_FILE"
        echo "[create_bundle] Paste into your LLM + add a one-line cell-spec prompt:"
        echo "[create_bundle]   \"create a new videos/*.md for [persona] [niche] [audience] from a corpus-validated cell\""
    else
        echo "[create_bundle] WARNING: clipboard pipe failed (sandboxed env or clip locked)"
        echo "[create_bundle] Bundle saved to: $BUNDLE_FILE"
        echo "[create_bundle] Open it manually: cat \"$BUNDLE_FILE\" | clip   (or pbcopy / xclip)"
        echo "[create_bundle] OK: ${#BUNDLE_FILES[@]} files concatenated (~${BYTES} bytes)"
    fi
else
    echo "[create_bundle] No clipboard tool found. Bundle saved to: $BUNDLE_FILE"
    echo "[create_bundle] OK: ${#BUNDLE_FILES[@]} files concatenated (~${BYTES} bytes)"
fi
