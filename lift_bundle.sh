#!/usr/bin/env bash
# lift_bundle.sh — concat the canonical lift-bundle files + pipe to clipboard.
#
# Usage:
#   ./code/lift_bundle.sh <decoded-artifact.md>
#
# Example:
#   ./code/lift_bundle.sh raw/decoded_healthylifesage_DX7iVuRMzUM.md
#
# What it does:
#   - Concatenates the 14 canonical lift-bundle files (per wiki/meta/lift-bundle.md)
#   - Appends the decoded source artifact at the end
#   - Pipes the concatenation to the system clipboard (clip / pbcopy / xclip)
#   - Operator pastes the bundle into any LLM + a one-line task prompt:
#         "lift this for [persona] [niche] [audience]"
#
# The bundle is transient (clipboard only, never committed). The canonical
# files remain single source of truth — wiki edits propagate on next invocation.
#
# Bundle list is documented in wiki/meta/lift-bundle.md and must stay in sync
# with that page. Update both when adding/removing files (e.g. when a new
# v-rule introduces a new must-read page).

set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DECODED="${1:-}"

if [[ -z "$DECODED" ]]; then
    cat <<'EOF' >&2
Usage: lift_bundle.sh <decoded-artifact.md>

Example:
  ./code/lift_bundle.sh raw/decoded_healthylifesage_DX7iVuRMzUM.md

The script concatenates the canonical lift-bundle (14 files per
wiki/meta/lift-bundle.md) + the decoded source, pipes to clipboard,
ready to paste into any LLM (Gemini / GPT-4o / Claude API / etc.).
EOF
    exit 1
fi

# Resolve decoded path (relative or absolute)
if [[ -f "$DECODED" ]]; then
    DECODED_FULL="$DECODED"
elif [[ -f "$REPO_ROOT/$DECODED" ]]; then
    DECODED_FULL="$REPO_ROOT/$DECODED"
else
    echo "ERROR: decoded artifact not found: $DECODED" >&2
    exit 1
fi

# Detect clipboard tool (Windows Git Bash / macOS / Linux / fallback to stdout)
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
    echo "[lift_bundle] WARNING: no clipboard tool found (clip/clip.exe/pbcopy/xclip/xsel)" >&2
    echo "[lift_bundle]          dumping bundle to stdout instead" >&2
fi

# Bundle file list — must match wiki/meta/lift-bundle.md table
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

# Verify all bundle files exist before starting
MISSING=0
for f in "${BUNDLE_FILES[@]}"; do
    if [[ ! -f "$REPO_ROOT/$f" ]]; then
        echo "ERROR: bundle file missing: $f" >&2
        MISSING=$((MISSING + 1))
    fi
done
if [[ $MISSING -gt 0 ]]; then
    echo "ABORTING: $MISSING bundle files missing. Update wiki/meta/lift-bundle.md if files moved." >&2
    exit 1
fi

# Compose the bundle
TIMESTAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
TOTAL_FILES=$((${#BUNDLE_FILES[@]} + 1))

build_bundle() {
    cat <<EOF
# LIFT BUNDLE — generated $TIMESTAMP

You are about to lift a decoded viral video into a Korella videos/*.md
production-ready recreation. Apply v521.1 -> v597 rules per the
deduplication architecture documented in code/template_reference.md.

Required output discipline:
  - v594 image cardinality: emit M images for N source shots where M <= N
    (consolidate per composition; multiple Scene blocks can reference
    the same image_M)
  - v590 chain optionality: chain only the 4 exceptions (v580 recipe
    state-evolution, v541 transformation, two-shot follow-up,
    single-shot action arc)
  - v591 novelty-gate: confirm visual hook hasn't been seen on any LiB
    Inspire account before locking
  - v592 motion-text-match: voiceover verb at second N matches visible
    motion at second N
  - v593 strict-header parser: ### Image N and ### Scene N must end
    after the integer (no descriptive suffix); action_note is single-line
    prose; multi-line splits stay within ONE Scene block via two
    line+action_note pairs
  - risky-vocabulary policy-flag pass: scan for TikTok/Meta red-words;
    swap with safer-metaphor preserving mechanism
  - psychology-of-conversion mandatory step: NAME the dominant cognitive
    move BEFORE writing a line

Read all 14 canonical bundle files below, plus the decoded source at
the end. Then output the videos/*.md.

Total bundle: $TOTAL_FILES files ($((${#BUNDLE_FILES[@]})) canonical + 1 decoded source)

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

    cat <<EOF

================================================================================
# DECODED SOURCE: $DECODED
================================================================================

EOF
    cat "$DECODED_FULL"
    echo ""
    cat <<'TASKEOF'

================================================================================
# TASK
================================================================================

Lift the decoded source above into a Korella videos/*.md. Apply v594 image
cardinality (consolidate to distinct compositions, M <= N). Apply v590 chain
optionality. Apply v591 novelty-gate before locking HOOK. Apply v592
motion-text-match per scene. Run risky-vocabulary policy-flag pass on
dialogue. Name the dominant psychological move per psychology-of-conversion
before writing a line.

Output the videos/*.md per code/template_new_format.md skeleton + strict
v593 parser format. End with ## Sources (referencing the decoded source
above) and ## Used in (placeholder).

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

When LIFTING from a decoded source: the action_note timed beats should
mirror what the decoded source actually shows. If the decoded artifact
already contains beat-level descriptions, port them; if it shows raw
shot timestamps + dense-frame walks, synthesize the three timed beats
from those frames. Do NOT collapse the decoded detail into vague
narrative — preserve specificity.

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

[6] PERSONA CONSISTENCY WITH DECODED SOURCE — the persona in the
    lift should match the decoded source's persona archetype unless
    the operator's prompt explicitly retargets. If the decoded
    source shows a Black-female-practitioner and the operator says
    "lift this for [persona] [niche] [audience]" without a persona
    override, keep the same archetype. Pick from corpus-validated
    personas in wiki/persona-map.md — DO NOT invent a new persona
    name.

[7] RISKY-VOCABULARY SWAPPED IN DIALOGUE — actually apply the swaps
    from risky-vocabulary.md. "Menopause" -> "after 40" / "the change".
    "Erection" -> "morning signal" / "back to attention". "Performance"
    in sexual context -> "going strong" / "lasting". "Cure" / "treats"
    / "reverses" + disease name -> never. Awareness-only without
    swap is insufficient — actually edit the lines.

[8] FIDELITY TO DECODED SOURCE — the lift should preserve the
    source's STRUCTURE (hook beat, recipe steps, transformation arc,
    CTA placement) and its DOMINANT MECHANISM (vicarious-benefit,
    visual-pun, taboo-warning, conspiracy, etc.). Naming the move
    BEFORE writing per psychology-of-conversion is mandatory. The
    Korella adaptation changes the SURFACE (persona swap if
    requested, recipe swap to corpus-validated, niche-language
    swap) — not the underlying mechanism.

[9] UNIVERSAL CLOSER (mandatory final line) — "follow me first or I
    can't reach you" or close variant. Verbatim across 12+ raw
    decodes; deviate only with documented reason.

[10] M <= N IMAGE CARDINALITY (v594) — count distinct images vs total
     scenes. M (images) must be <= N (scenes). Two scenes that share
     composition (same setting + blocking + camera) should reuse the
     same image_K via the Scene block's `image:` field. Setting
     changes always require a new image (never reuse across settings).
     If the decoded source has K source shots but they consolidate
     to M < K compositions, the lift should output M images — do
     not 1:1 mirror redundant decoded shots.

[11] HOOK-IMAGE POWER TEST (v598) — THIS IS THE BIGGEST VIRAL LEVER.
     Every other rule above is a ZERO-MULTIPLIER if the HOOK image
     doesn't stop the scroll. Before locking Image 1 / Scene 1, all
     FIVE questions must answer YES:

     Q1. PHYSICAL OBJECT in the hook? (foreground prop being held /
         manipulated / shown — banana / seed jar / papaya / anatomy
         torso / alarm clock / faucet / pill bottle / saffron thread).
         Persona talking with empty hands FAILS — refuse the draft.

     Q2. VISIBLE MOTION at second 0-2s? (SLAM / POUR / SPRAY / GRIND /
         SMASH / DROP / LIFT / REVEAL / SQUEEZE / TWIST or rapid
         camera move). Static talking-head FAILS per Milen 2026-04-23
         "always in the hook there should be motion. being too static
         is just boring."

     Q3. VISUAL PUN / METAPHOR / HYPER-SPECIFIC SCENE-MIRROR?
         (banana = penis / papaya = vitality / cabbage = visceral fat
         / alarm clock 3:47am = nocturia / soaked pillow = hot flash /
         saffron thread = restored vitality). The hook activates an
         associative leap.

     Q4. NOVELTY-GATE (v591) — has this exact visual been seen on LiB
         Inspire or any operator's account? Banana-smash + banana-
         tape-measure are OVERUSED in male-ED. If yes-seen, refuse and
         pivot to symptom-show / A-vs-B-compare / outside-niche-viral-
         recreation.

     Q5. MOTION-TEXT-MATCH (v592) — verb-at-second-N matches visible-
         motion-at-second-N? "smash" -> visible smash. "watch" ->
         reveal motion. "you wake up at 3am" -> alarm-clock cut.
         Voiceover-over-static-shot FAILS.

     Q6. BACKGROUND-AUTHORITY MATCHES PERSONA? (≥2 anchor props
         visible 0-2s frame). Setting must signal the persona's
         authority type. Corpus-grounded persona x setting pairings
         (24-decode evidence):

         - CLINICAL DOCTOR -> T2 exam room OR T2 diploma office;
           anchors: diploma + US flag + anatomy poster + equipment
           cart + exam stool + IV pole; BREAKS in domestic kitchen
         - FOLK-WISDOM ELDER -> T0/T1 honey-oak bench OR rustic
           kitchen; anchors: honey-oak/barn-board wall + 3+ herb jars
           + ceramic teapot + window outdoors + patina; BREAKS in
           sterile clinic
         - RETAIL-WITNESS OPERATOR -> T0-retail Costco/Walmart;
           anchors: store signage + fluorescent industrial ceiling +
           blurred shoppers + actual store-stocked product; BREAKS in
           studio
         - CARIBBEAN HERBALIST -> T0/T1 Caribbean sunroom; anchors:
           bamboo wall + Rasta+US flags + 3+ herb jars + honey-oak
           table + amber light; BREAKS without cultural anchors
         - MODERN-CLINIC SEXY-DOCTOR / KORELLA F-to-F-about-M ->
           T0 clean kitchen (HOOK) + T2 office (OUTRO) DUAL-FLIP
           required; BREAKS with single-setting reduction (4-corpus-
           instance evidence)

     Q7. SETTING RESONATES WITH AUDIENCE? Audience x setting matrix:
         - WOMEN 40+ -> kitchen + clinical-exam DUAL-FLIP
         - MALE 40-70 US (Mike) -> retail warehouse + clinic +
           luxury-apartment OUTRO (avoid pure home-kitchen)
         - F-to-F-about-M -> kitchen + office DUAL-FLIP (Korella
           canonical)
         - NEUTRAL/MIXED -> T2 clinical authority
         - BLACK WOMEN -> Caribbean sunroom + bamboo + flags

         REJECT if background not visible / ambiguous / contradicts
         persona / "generic talking-head studio with bokeh."

     LIFT-SPECIFIC: if the decoded source's HOOK already passes Q1-Q7,
     port its visual structure exactly (same prop class, same motion
     verb, same metaphor, SAME SETTING + SAME ANCHOR PROPS). If the
     decoded source's HOOK FAILS the test (e.g. it's a generic
     talking-head opener with weak background), DO NOT replicate the
     failure — the lift should UPGRADE the hook by:

     (1) Pulling foreground prop + motion from the corpus surrogate
         library in template_reference.md §"Corpus-grounded surrogate
         library" matched to the decoded source's niche.
     (2) Pulling background + anchor props from the persona x setting
         authority pairings matched to the decoded source's persona
         archetype (or Korella-adapted persona if retargeting).
     (3) Preserving the decoded source's underlying MECHANISM
         (vicarious-benefit, visual-pun, taboo-warning, conspiracy)
         while upgrading the visual surface.

     If ANY of Q1-Q7 fail, REJECT the hook and propose 3 alternatives
     pulling from the corpus surrogate library + persona x setting
     authority pairings before emitting the videos/*.md.

If any item above fails, FIX IT BEFORE OUTPUT. The operator will
re-prompt you to fix violations otherwise. Self-correction here saves
a round-trip.

TASKEOF
}

# Always write to a temp file as fallback (clipboard access can fail in
# sandboxed/CI environments). Then attempt clipboard; on failure, point
# the operator to the file.
TMPDIR_PATH="${TMPDIR:-/tmp}"
BUNDLE_FILE="$TMPDIR_PATH/lift_bundle_$(date +%s).md"
build_bundle > "$BUNDLE_FILE"
BYTES=$(wc -c < "$BUNDLE_FILE")

if [[ -n "$CLIP_CMD" ]]; then
    if cat "$BUNDLE_FILE" | $CLIP_CMD 2>/dev/null; then
        echo "[lift_bundle] OK: $TOTAL_FILES files concatenated (~${BYTES} bytes), piped via '$CLIP_CMD'"
        echo "[lift_bundle] Bundle also saved to: $BUNDLE_FILE"
        echo "[lift_bundle] Paste into your LLM + add a one-line task prompt:"
        echo "[lift_bundle]   \"lift this for [persona] [niche] [audience]\""
    else
        echo "[lift_bundle] WARNING: clipboard pipe failed (sandboxed env or clip locked)"
        echo "[lift_bundle] Bundle saved to: $BUNDLE_FILE"
        echo "[lift_bundle] Open it manually: cat \"$BUNDLE_FILE\" | clip   (or pbcopy / xclip)"
        echo "[lift_bundle] OK: $TOTAL_FILES files concatenated (~${BYTES} bytes)"
    fi
else
    echo "[lift_bundle] No clipboard tool found. Bundle saved to: $BUNDLE_FILE"
    echo "[lift_bundle] OK: $TOTAL_FILES files concatenated (~${BYTES} bytes)"
fi
