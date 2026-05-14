#!/usr/bin/env bash
# innovate_bundle.sh — concat canonical innovate-bundle files + pipe to clipboard.
#
# Use case: PORT AN OUTSIDE-NICHE VIRAL VIDEO INTO A KORELLA CELL.
# Takes an outside-niche decoded source (or just a structural reference), extracts
# its winning structure (HOOK mechanism + force-verb arc + pattern + hook family),
# and ports to a target persona × niche × audience cell.
#
# Different from lift_bundle.sh (same-niche recreation): innovate REJECTS the
# source's niche framing and surface — only the underlying winning structure
# transfers. The lift task prompt would tell Gemini "recreate this in same niche";
# the innovate task prompt tells Gemini "extract structure, swap surface, ship for
# target cell."
#
# Usage:
#   ./code/innovate_bundle.sh [outside-niche-reference.md] [target-cell-spec]
#
# Examples:
#   ./code/innovate_bundle.sh raw/decoded_jupi_gut_health_folk_elder.md "Nuri male-ED clinic"
#   ./code/innovate_bundle.sh raw/decoded_jupi_gut_health_folk_elder.md "Master-Chen metabolism apothecary"
#   ./code/innovate_bundle.sh                                            # bundle only, paste cell spec into LLM manually
#
# Sister scripts (different task entry-points):
#   ./code/lift_bundle.sh <decoded.md>     — recreate FROM decoded source in SAME niche
#   ./code/decode_bundle.sh <source-mp4>   — decode a new viral video
#   ./code/create_bundle.sh                — author from 0 (no decoded source)
#   ./code/innovate_bundle.sh <decoded.md> — port outside-niche viral to target cell (this script)
#
# What it does:
#   - Concatenates the 19 canonical wiki+code files (17 lift-bundle + 2 innovation-specific)
#   - Appends the outside-niche reference (if provided) + target cell spec (if provided)
#   - Appends INNOVATE-specific task preamble — extract winning structure, swap surface
#   - Pipes to clipboard (with temp-file fallback)
#
# The bundle is transient — never committed. Wiki edits propagate on next invocation.

set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE="${1:-}"
CELL_SPEC="${2:-}"

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
    echo "[innovate_bundle] WARNING: no clipboard tool found" >&2
    echo "[innovate_bundle]          dumping bundle to stdout instead" >&2
fi

# Resolve outside-niche reference path (optional)
SOURCE_FULL=""
if [[ -n "$SOURCE" ]]; then
    if [[ -f "$SOURCE" ]]; then
        SOURCE_FULL="$SOURCE"
    elif [[ -f "$REPO_ROOT/$SOURCE" ]]; then
        SOURCE_FULL="$REPO_ROOT/$SOURCE"
    else
        echo "ERROR: outside-niche reference not found: $SOURCE" >&2
        exit 1
    fi
fi

# Innovate bundle file list — 17 lift-bundle + 2 innovation-specific
# 2026-05-12: added innovation-rules.md (20-rule canonical corpus from LiB call
# 2026-05-09) + 80-20-script-method.md (80% structural lift + 20% iteration).
# These are the missing rules that distinguish innovate from lift.
BUNDLE_FILES=(
    # ----- 17 lift-bundle canonical files (shared with lift_bundle.sh) -----
    "wiki/meta/viral-video-pipeline.md"
    "wiki/audience/niche-audience-matrix.md"
    "wiki/audience/strategy-mechanisms.md"
    "wiki/audience/psychology-of-conversion.md"
    "wiki/audience/audience-mapping.md"
    "wiki/audience/pain-point-language.md"
    "wiki/audience/video-types.md"
    "wiki/audience/avatar-mike-henderson.md"
    "wiki/mechanics/hook-patterns.md"
    "wiki/mechanics/cta-patterns.md"
    "wiki/mechanics/scene-structure.md"
    "wiki/strategy/risky-vocabulary.md"
    "wiki/strategy/viral-recreation-method.md"
    "wiki/products/_index.md"
    "wiki/products/corella-saffron.md"
    "code/template_reference.md"
    "code/template_new_format.md"
    # ----- 2 innovation-specific files (NOT in lift bundle) -----
    "wiki/strategy/innovation-rules.md"
    "wiki/strategy/80-20-script-method.md"
)

# Verify all bundle files exist
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
SOURCE_COUNT=0
[[ -n "$SOURCE_FULL" ]] && SOURCE_COUNT=1
TOTAL_FILES=$((${#BUNDLE_FILES[@]} + SOURCE_COUNT))

build_bundle() {
    cat <<EOF
# INNOVATE BUNDLE — generated $TIMESTAMP

You are about to INNOVATE — port the winning structure of an OUTSIDE-NICHE
viral video into a Korella videos/*.md production-ready recreation in a
DIFFERENT target cell. This is NOT a lift (lift = same niche). Innovate
extracts the underlying mechanics from the source and ships them in a new
surface.

Apply v521.1 -> v707 rules per the deduplication architecture documented in
code/template_reference.md.

Read all 19 canonical bundle files below, plus the outside-niche reference at
the end (if provided). Then output the videos/*.md.

Total bundle: $TOTAL_FILES files ($((${#BUNDLE_FILES[@]})) canonical + $SOURCE_COUNT outside-niche reference)

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

    if [[ -n "$SOURCE_FULL" ]]; then
        cat <<EOF

================================================================================
# OUTSIDE-NICHE REFERENCE: $SOURCE
================================================================================

EOF
        cat "$SOURCE_FULL"
        echo ""
    fi

    cat <<EOF

================================================================================
# TASK — INNOVATE
================================================================================

INNOVATE the outside-niche reference above into a Korella videos/*.md for
the target cell. DO NOT recreate the source video in its original niche.
Extract the WINNING STRUCTURE and PORT it.

TARGET CELL:
EOF
    if [[ -n "$CELL_SPEC" ]]; then
        echo "  $CELL_SPEC"
    else
        echo "  [paste target cell spec here when pasting into LLM, e.g.:"
        echo "   \"Nuri male-ED clinic\" / \"Master-Chen metabolism apothecary\"]"
    fi
    cat <<'TASKEOF'

INNOVATE PROCESS (mirrors operator's internal workflow — follow exactly):

Step 1 — DECOMPOSE the outside-niche source (extract winning structure)
  Read the outside-niche reference. Name explicitly:

  - HOOK family — match to wiki/mechanics/hook-patterns.md taxonomy
    (SHAME-PROXY / VISCERAL-DESTRUCTION sub-variant /
    VICARIOUS-SHOW-THE-PROBLEM / CURIOSITY-GAP / BEFORE-AFTER / etc.)
    AND name the v598 power-test answers (Q1-Q8) the source passes.
  - Pattern (v614 A/B/C/D/E) — BEFORE/AFTER / RECIPE-LED /
    DIAGNOSTIC-PIVOT / CULTURAL-AUTHORITY / PERSONAL-AUTHORITY
  - Force-verb action_arc (v697) — chain of FORCE-verbs the source uses
    (LIFT/SLAM/POUR/CASCADE/SQUEEZE/PRESENT/SCATTER/etc.)
  - Mechanism — concrete-benefit line the source's payoff scene delivers
  - CTA shape — canonical "comment X / follow me first" or variant
  - Persona archetype — folk-wisdom-elder / clinic-glam / cultural-authority

Step 2 — DECIDE what to KEEP vs SWAP (80/20 method per
  wiki/strategy/80-20-script-method.md)
  KEEP (80% — the structural skeleton):
    - HOOK family + force-verb chain
    - Pattern (A/B/C/D/E)
    - Scene count + clip rhythm
    - CTA template shape
    - Mechanism BEAT (not the mechanism's words — the rhetorical move)

  SWAP (20% — the surface):
    - Niche — outside-niche source's niche → target cell's niche
    - Persona — outside-niche persona → target cell's persona
    - Setting — match target persona's authority pairing
      (folk-elder → rustic outdoor / clinic-glam → T2 exam room /
      apothecary → T1 herb-jar wall)
    - Product placement — target Korella product per v599 matrix
      (reveal at recipe product-cascade scene + CTA hero-shot)
    - Pain points — target cell's audience x niche vocabulary
      (from wiki/audience/pain-point-language.md + niche page)
    - Visual props — target cell's corpus-validated surrogates
    - Dialogue verbatim — rewrite per target cell, preserving cadence

Step 3 — APPLY the 20 innovation rules from
  wiki/strategy/innovation-rules.md
  Walk all 20 rules. Mandatory rules (per the LiB call):
    - Innovation must pull from corpus (never hallucinate)
    - Source's HOOK power-test signal must survive the port (Q1-Q8 still pass)
    - Mechanism remains concrete-benefit (1 line, not jargon-academic)
    - Authority implicit, never declared corporate-style
    - CTA template canonical, not reinvented
    - No melodrama / no corporate voice
    - All v596+ technical rules apply (parser gates, em-dash ban, lowercase,
      vocabulary safety, BLEND/FRESH only, ~20w line target, etc.)

Step 4 — APPLY recent v-rules (mandatory, easy to miss)
  v696 parser-abort gates (5 validation gates) — REQUIRED, run before output
  v697 force-verb action_arc field on every shot scene
  v698A voiceover_anchor_image if any scene needs voiceover-over-b-roll
  v702 image-prompt vocabulary safety (mechanical grep for forbidden tokens)
  v703 worker-injected reference manifest (no manual Image N positional
    numbering in body)
  v704 clip transitions: BLEND or FRESH only — CONTINUE banned
  v704 line-length target ~20 words per `- **line:**`, floor 12, ceiling 28
  v707 Ingredients table has `Attached to` column (per-image binding scope) AND
    NO v604 verbose body-line form (`Use image_K as the exact base frame...`)

Step 5 — OUTPUT shape (per code/template_new_format.md skeleton)
  YAML frontmatter:
    persona / niche / audience / cell / corpus_pattern / adaptation_map /
    corpus_compliance_audit (v614)
    HOOK family declared + force-verb chain declared
  ## Sources — cite outside-niche reference + ≥1 cross-validating Korella
    corpus parent
  ## Ingredients — 5-column table WITH `Attached to` column (v707)
  ## Images — Image N blocks with v707 3-line binding stack (NO v604 verbose
    body line); frontmatter `visual_delta:` for chained images; v597+ canonical
    action_note shape
  ## Storyboard — Scene N blocks with `image:` / `clip_mode:` (BLEND or FRESH)
    / `speaker:` / `action_arc:` / `- **line:**` (lowercase, ~20w) /
    `- **action_note:**` (single-line prose with [Start/Mid/End beat] markers)
  ## Comprehension — structural inventory + v-rule inventory + rhetorical
    structure + persona+setting + corpus_compliance_audit
  ## Veo 3.1 Final Prompts — per clip

Step 6 — PRE-OUTPUT VALIDATION GATES (mandatory — fix and re-emit if any fail)
  Gate 1 — text_card scenes have NO ### Image N header (v696)
  Gate 2 — every shot scene chains forward through state-evolution (v596 + v604)
  Gate 3 — ### Image N / ### Scene N headers are STRICT regex (no suffixes)
  Gate 4 — every shot Image has fenced `**Image prompt:**` block (TRIPLE-
    BACKTICK fence opening + closing — without it, parser fails import with
    "Parse error: Image N: no fenced 'Image prompt:' block found")
  Gate 5 — `- **line:**` fields all lowercase (v693)
  Gate 6 — `- **line:**` fields have NO em-dash (v615)
  Gate 7 — `- **line:**` word counts in 12-28 range (v704)
  Gate 8 — Image prompts have NO forbidden tokens (v702 grep)
  Gate 9 — Ingredients table has `Attached to` column (v707)
  Gate 10 — chained Image bodies have NO `Use image_K as the exact base frame...`
    line (v707 Part B)
  Gate 11 — clip transitions are BLEND or FRESH only (v704)
  Gate 12 — HOOK image passes v598 Q1-Q8 power-test
  Gate 13 — corpus_pattern declared + adaptation_map covers every scene (v614)
  Gate 14 — v712 decode-side relational composition grammar applied
    (subject-anchored, not coordinate-grid)
  Gate 15 — v713 Banana-2-attached-reference composition discipline
    (a) partial-visibility override on binding when persona is partially
        in frame; (b) [Composition] block front-loaded; (c) camera grammar
        present in Composition block (85mm / wide-angle / minimum focus
        distance / shallow DOF / etc.); (d) composition-anti-default
        negatives appended; (e) canonical block order Binding -> Composition
        -> Subject -> Action -> Location -> Style -> Tech -> Negatives.
  Gate 16 — v714 emotional payoff (non-persona AFTER-state expression)
    visual_delta on chained AFTER images carries BOTH physical change
    AND expression change clauses joined by AND.
  Gate 17 — v715 subject-anchored prop composition (v605b + v713f + v603b)
    hero props anchor to SUBJECT (held aloft / placed on body / pressed
    against / worn / symptom-as-prop), NEVER to environment furniture
    (desk / counter / table / shelf). Five anchor modes per v605b. Z-axis
    depth layering in [Composition] block. Camera at the prop's anchor
    level. Composition-anti-default negatives required.
  Gate 18 — v716 Banana 2 normalization-bias countermeasures
    (a) v622b geometric symptom exaggeration — AUGMENTED-SYMPTOMS /
        HEALER-SHOWING-CURE non-persona body-part symptoms use
        measurement-based descriptors (inches / mm / cm / % / count /
        projection / shape / spatial extent), NOT adjective-only.
        Anti-normalization negatives required ("No firm [body part].
        No normal skin elasticity. No minor [symptom]. The [symptom]
        MUST be EXTREME.")
    (b) v715f Mode 6 two-shot body-part-thrust — when persona must stay
        FULLY visible, switch from Mode 1-5 to Mode 6 (patient thrusts
        body part toward lens; persona fully visible at side; 35mm
        wide-angle chest-up two-shot). Mode 6 implies v622b.
  Gate 19 — v717 anti-normalization intensification stack
    (a) v622b-extension — geometric descriptors MUST be paired with
        inanimate-object metaphor anchors ("like a deflated balloon",
        "like twisted yarn", "like cracked porcelain", "like ridged
        corduroy", "like sparse grass on dry ground", etc.). Banana 2
        has strong visual priors for inanimate-object shapes that
        measurements alone don't invoke.
    (b) v605c symptom-first allocation — on AUGMENTED-SYMPTOMS HOOK /
        before-state / symptom-pointer frames, [Subject] block uses
        TWO-BLOCK structure: [Subject — Symptom] (standalone visual
        entity, named first, geometric + metaphor) then [Subject —
        Host] (host character demographics, named second). Banana 2
        weights first-tokens heaviest; symptom-first forces planner
        to plan symptom geometry before host anatomy.
    (c) v604b structural negative constraints — negatives block bans
        underlying ANATOMICAL DEFAULTS in addition to outcomes (not
        just "No firm arm" but also "No normal human arm anatomy. No
        skin attached to the bottom of the bicep. No straight lower
        arm contour."). Forces Banana 2 to render the absence of the
        healthy default, which forces the symptom-distorted version.
  Gate 20 — v718 VLM forensic-perception protocol (PRE-GRAMMAR)
    Applied BEFORE composition prose. Three perceptual checks:
    (a) v718a kinematic tracing — limbs attributed by clothing-color
        trace back to shoulder (NOT face-proximity). Purple gloves
        crossing the frame belong to whichever torso wears
        purple-accent clothing, not to the face the hand appears
        near.
    (b) v718b Z-depth isolation — foreground / midground / background
        explicitly mapped per shot. Body parts crossing the frame
        horizontally and OCCLUDING characters behind them are noted
        explicitly (not described as side-by-side flat-2D).
    (c) v718c literal pixel VFX — impossible physics described
        literally (closed flesh loop with hole, 12-inch stretching,
        fully detached body parts, 3 eyes, inverted anatomy,
        translucent skin, liquefied flesh) NOT normalized to closest
        anatomical concept (U-shape sagging, few inches, etc.).
    Pre-grammar order: v718a -> v718b -> v718c THEN v712 / v713 /
    v715 / v716 / v717 grammar. Hallucinations caught here cost zero
    Banana 2 credits.
  Gate 21 — v719 solid-volume topology discipline
    Bidirectional corollary to v717 + v718c. v716/v717 "U-shape"
    vocabulary creates hallucinated HOLES in Banana 2 renders where
    source has SOLID unbroken flesh.
    (a) v719a solid-volume vocabulary swap — drop topology-implying
        anchors ("deep U-shape" / "V-shape" / "loop" / "hole" /
        "opening" / "ring") when source is solid; replace with
        solid-volume metaphors ("continuous solid sheet of draped
        flesh" / "dense unbroken curtain" / "single continuous fold"
        / "thick mass of pendulous flesh").
    (b) v719b topology bans — append to v604b negatives: "No holes
        in the flesh. No negative space. No loops. No openings. The
        hanging skin MUST be a solid continuous unbroken flap."
    (c) v719c bidirectional VFX recognition — if source SOLID, prose
        names SOLID; if source LOOPED, prose names LOOPED. Don't
        leak topology in either direction.
    Carve-out: genuine VFX-loop sources keep v718c original literal
    topology language; v719 is for SOLID-source mismatches only.
  Gate 22 — v720 lateral X-axis composition
    v713f Z-axis stacking + v715 modes assume depth-plane layering.
    Source videos often have symptom extended LATERALLY (to the
    side). Z-axis grammar collapses lateral sources to forward-
    thrust / crossing-chest defaults.
    (a) v720a lateral extension carve-out — when symptom extends
        laterally (out to viewer-left / right / overhead / downward
        / at 45-degree angle), Z-axis layering does NOT apply.
        Patient + persona + extended limb share MIDGROUND depth
        plane. Use X-axis relational grammar instead.
    (b) v720b lateral vector grammar — every extended limb has
        explicit directional clause ("extended straight outward to
        the viewer-left" / "extended straight forward toward the
        camera" / "extended laterally parallel to the ground").
        BANNED ambiguous: "extended arm" / "outstretched" / "arm
        reaching out" / "arm raised".
    (c) v720c limb-pose structural bans — append to v604b negatives:
        "No arm crossing the chest. No arm thrust forward toward
        the lens. The arm MUST extend straight out to the side,
        parallel to the ground. No overlapping bodies. No persona
        hiding behind the patient."
  Gate 24 — v722 persona wardrobe ban
    Strict extension of v553.1 + v609 + v610. Persona identity
    (clothing / wardrobe / accessories / scrubs / coats / ties /
    stethoscope / badge / glasses / hair / race / age / build) is
    carried by the UPLOADED CHARACTER REFERENCE IMAGE. NEVER
    describe persona wardrobe in:
      - Image prompt body prose
      - static_composition.subject
      - action_note prose
      - scene line / dialogue
      - Veo final-prompt body
    Persona wardrobe lives ONLY in the Ingredients table Description
    column. Single source of truth.
    Banned phrasings: "wears her [item]" / "wearing [item]" / "her
    crisp white doctor's coat" / "her scrub top" / "her V-neck
    scrub" / "stethoscope around her neck" / "wears [color]
    [garment]" / "her uniform" / "her clinical attire" / "her
    medical badge" / "purple-gloved hand" (when referring to
    persona's gloves).
    Asymmetry CRITICAL: PERSONA wardrobe BANNED in body prose
    (upload carries identity). NON-PERSONA wardrobe (patient /
    customer / bystander) REQUIRED in body prose per v610 / v622 /
    v669 (no upload for them; prose IS their anchor).
    Carve-outs: wardrobe AS PROP (persona dropping/holding-up an
    item as rhetorical prop) allowed in action_note; HOOK weird-
    action involving wardrobe (v539) allowed in action_note. Base
    wardrobe stays in Ingredients metadata.
    Pre-output grep: zero hits on banned persona-wardrobe phrasings
    in body prose; Ingredients Description column contains canonical
    persona wardrobe metadata.
  Gate 23 — v721 v698A activation gate (anti-auto-voiceover on
    on-camera persona scenes)
    v698A fires (speaker: voiceover + voiceover_anchor_image) ONLY
    when persona's face is NOT visible OR persona is NOT lip-
    syncing the line. LLMs apply v698A aggressively from corpus
    prior ("recipe scene = voiceover") even when scene's Image
    shows persona on-camera lip-syncing. Platform correctly
    renders TWO Veo clips per v698A but b-roll is REDUNDANT when
    persona is already on-camera. Doubles Veo cost.
    Disallowed: scene with "- **speaker:** voiceover" + bound image
    whose body contains trigger keywords ("eyes locked to the lens"
    / "mouth open mid-word" / "mouth slightly parted in mid-
    speech" / "facing the camera" / "squared to camera" / "lip-
    syncing" / "on-camera dialogue").
    Decision tree: face visible? -> Q2; mouth open mid-word? ->
    on-camera (no anchor); mouth closed / silent? -> voiceover +
    anchor + image body notes "mouth closed".
    Fix when violation: change "- **speaker:** voiceover" -> "- **
    speaker:** on-camera"; drop "- **voiceover_anchor_image:**
    image_M" line; remove orphan anchor image from ## Images if
    not referenced elsewhere.
  Gate 25 — v590 + v604 Static-World Trap (parallax / moving
    montages -> reference_image: none MANDATORY)
    THE STATIC-WORLD TRAP: When deciding to chain, watch out for
    movement. If a character is walking, panning, or moving
    through a space (stadium / house / store / garden / theme
    park / Costco aisle / clinic hallway), DO NOT chain the
    images together. Chaining forces Banana 2 to lock the exact
    same background pixels, making it look like the character
    is walking on a treadmill in front of a green screen. Use
    `reference_image: none` for moving sequences so the background
    updates naturally and parallel-generation produces fresh
    background angles that simulate travel through the
    environment.
    Triggers (ANY one -> reference_image MUST be none):
      - Character walking from frame A to frame B (any locomotion)
      - Camera panning / dollying / tracking
      - Background elements shift between beats (walls / shelves /
        trees / rides / aisles / products / signage)
      - Listicle / montage where each beat has its own backdrop
        (Costco aisle 1 -> aisle 2 -> parking lot)
      - "Travel" sequences (entering store / leaving store /
        crossing parking lot / entering clinic)
      - B-roll re-staging persona in different parts of the same
        venue
    Re-statement of v604 continuity-chain rule: criteria 3 ("same
    room") and 4 ("same camera angle") must BOTH be true at the
    PIXEL level, not the semantic level. A theme-park walk that
    passes the carousel -> the food cart -> the bench is THREE
    rooms in pixel terms even though it's ONE location
    semantically.
    Carve-outs (chains still allowed): genuine state-evolution
    on a stationary subject (v580 recipe / v541 before-after) /
    two-shot follow-up preserving a one-off character's identity /
    single-shot action arc anchoring. Movement through space is
    NEVER one of these.
    Pre-output check: for every Image with `reference_image:
    image_K`, confirm both Image N and Image K share the SAME
    background pixels (same walls / same shelves / same camera
    angle on the room). If they don't, switch to
    `reference_image: none`.

CANONICAL BLOCK STRUCTURE (parser-compliant, v593 + v696):

  Two block kinds (Image N + Scene N) each have hard-required bullets.
  Missing any -> import hard-fails with one of:
    "Parse error: Image N: no fenced 'Image prompt:' block found"
    "Parse error: Scene N: missing '- **image:** image_N' field"
    "Parse error: Scene N: voiceover_anchor_image image_M has empty cast list"

  --- ### Image N block (in ## Images section): ---

    ### Image N
    - **frame_anchor:** <Xs>
    - **reference_image:** <none | image_K>
    - **narrative_lens:** <HEALER-SHOWING-CURE | AUGMENTED-SYMPTOMS |
                          GRABBING-ATTENTION>
    - **cast:** <comma-separated character handles>
    - **product_image:** <ingredient name, ONLY if product bound>
    - **prop_position:** <per v605, if product_image set>
    - **visual_delta:** <per v604 + v714, if reference_image set>
    - **action_arc:** <force-verb chain per v697>
    - **role:** voiceover_anchor   (ONLY if image is a v698A anchor)
    - **Image prompt:**
    ```
    [v609 binding line(s), with v713(a) partial-visibility override if
    applicable]

    [Composition] [v713(c) camera grammar + v603b anchor-level camera +
    9:16 framing + v713f Z-axis depth layering with three planes:
    foreground / midground / background, all subject-anchored per v605b].

    [Subject — patient or non-persona] [fully described per v610 / v622
    — race + age + BUILD + hair + clothing + expression; symptom-feature
    exaggerated description per v622 / v714].

    [Action] [v697 force-verb chain + v712 subject-to-subject geometry;
    mention v605b anchor mode in motion].

    [Location] [setting + background blur statement].

    [Style] [iPhone camera + handheld + lighting + grading + v603 closing
    tag "iPhone HDR colors, deep focus."].

    [Tech] [aspect ratio + resolution, e.g. 9:16, 2K output].

    Negatives: [v604 negative-constraint block + v606 product negatives +
    v713(d) composition-anti-default + v715 desk-anchor anti-default].
    ```

  CRITICAL: triple-backtick fence opening + closing required around the
  prompt body. The body-internal [Composition] / [Subject] / etc. labels
  are CONCEPTUAL block markers (Banana 2 reasoning slots per v713(b)
  front-loaded structure), NOT markdown headers — they live INSIDE the
  fenced code block as prose. Do NOT promote them to ## or ### markdown
  headers — the platform parser would break.

  --- ### Scene N block (in ## Storyboard section): ---

  Two scene-type variants — SHOT and TEXT_CARD — have different required-
  field sets. Mixing them hard-fails import.

  SHOT scene (default — persona / patient on screen, lip-sync or
  voiceover playing over b-roll):

    ### Scene N
    - **image:** image_K               (REQUIRED — hard-fail if missing)
    - **scene_type:** shot             (default if omitted, explicit recommended)
    - **target_duration_s:** <float>   (clip length in seconds)
    - **clip_mode:** <fresh | blend>   (per v704 — CONTINUE banned)
    - **transition:** <cut | blend>    (per v704)
    - **speaker:** <on-camera | voiceover | silent>   (per v538 explicit-only)
    - **action_arc:** <force-verb chain>              (per v697, e.g. POUR -> CASCADE)
    - **line:** <lowercase 12-28 word line, no em-dash>
                (REQUIRED if speaker is on-camera or voiceover;
                 ABSENT if speaker: silent;
                 lowercase per v693, 12-28 words per v704,
                 no em-dash per v615)
    - **action_note:** <single-line prose with inline beat markers>
                ([Start beat 0-Xs] / [Mid-clip beat] / [End beat] per v540 + v604)
    - **voiceover_anchor_image:** image_M
                (REQUIRED when speaker: voiceover, per v698A —
                 anchor image's cast: must include persona)

  TEXT_CARD scene (caption-card insert — solid background + text overlay,
  NO live-action footage):

    ### Scene N
    - **scene_type:** text_card        (REQUIRED — discriminator field)
    - **caption:** "text"              (REQUIRED — string in quotes)
    - **bg_color:** "#hex"             (REQUIRED — hex color)
    - **duration:** <float>            (REQUIRED — different field than target_duration_s)

  TEXT_CARD scenes MUST NOT have per v682d:
    - `- **image:** image_N` (text_card scenes have NO image bullet)
    - corresponding ### Image N header in ## Images section

  Image numbering MAY be non-contiguous when text_cards are interleaved
  (e.g. images 1, 2, 3, 5, 6, 7 with text_card at scene 4 having no
  image_4 — chain references still resolve).

  Scene-block copy-paste skeleton:

    ## Storyboard

    ### Scene 1
    - **image:** image_1
    - **scene_type:** shot
    - **target_duration_s:** 5.0
    - **clip_mode:** fresh
    - **transition:** cut
    - **speaker:** on-camera
    - **action_arc:** GESTURE-FORWARD -> POINT-TO-LENS
    - **line:** [lowercase 12-28 word on-camera line, no em-dash]
    - **action_note:** [Start beat 0-1.5s] persona leans forward, hand
      extended toward camera. [Mid-clip beat] persona's index finger
      reaches lens. [End beat] camera holds.

    ### Scene 2
    - **image:** image_2
    - **scene_type:** shot
    - **target_duration_s:** 6.0
    - **clip_mode:** fresh
    - **transition:** cut
    - **speaker:** voiceover
    - **voiceover_anchor_image:** image_5
    - **action_arc:** POUR -> CASCADE
    - **line:** [voiceover line over silent b-roll, lowercase 12-28 words]
    - **action_note:** [Start beat 0-2s] honey pours from jar. [Mid-clip
      beat] golden cascade hits water. [End beat] saffron threads dissolve.

    ### Scene 3
    - **scene_type:** text_card
    - **caption:** "guide"
    - **bg_color:** "#000000"
    - **duration:** 1.2

    ### Scene 4
    - **image:** image_3
    - **scene_type:** shot
    - **target_duration_s:** 7.0
    - **clip_mode:** fresh
    - **transition:** cut
    - **speaker:** on-camera
    - **action_arc:** GESTURE-FORWARD -> POINT-TO-LENS
    - **line:** [closing CTA line, lowercase, 12-28 words]
    - **action_note:** [Start beat 0-2s] persona on camera mid-utterance.
      [Mid-clip beat] persona points at the lens. [End beat] camera holds.

  Scene 3 (text_card) has NO image:, speaker:, line:, action_note: — it's
  a different scene-type. Scene 2's voiceover_anchor_image image_5 must
  be a TORSO+HANDS-VISIBLE persona-on-camera image with role:
  voiceover_anchor declared in ## Images per v698A.

V715 — SUBJECT-ANCHORED PROP COMPOSITION (apply during innovation):

  When the source video has a hero prop / symptom in the primary focus
  of any frame, the innovated videos/*.md MUST anchor the prop to a
  SUBJECT (character body or body part), NEVER to environment furniture.

  Five subject-anchor modes:
    Mode 1 — Held aloft (held by character at chest / face / chin /
      overhead) — diagnostic-pointer, product reveal, before/after card
    Mode 2 — Placed on body (rests on patient's belly / chest / forearm
      / thigh / knee / scalp / back / shoulder) — anatomical demos
    Mode 3 — Pressed against body (palpation, examination, pressure)
    Mode 4 — Worn / strapped / draped on body (wearables, braces)
    Mode 5 — Symptom-as-prop on body (varicose veins on calf, jowl on
      jaw, distended belly, hairline, dark eye circles, back acne)

  Banned anchor phrases (innovation output MUST NOT contain these):
    "on the desk" / "on the counter" / "on the table" / "on the shelf"
    "on the windowsill" / "resting on the surface" / "sitting on the
    surface" / "in front of him on the desk" / "between them on the table"

  Required Composition-block structure (v713f Z-axis stacking):
    [Composition] [camera grammar] + straight-on at [anchor]-level +
    9:16. The [prop] is HELD ALOFT / placed on [body part] / pressed
    against [body part] / wraps around [body part] / [body part] fills
    the immediate center-foreground. Directly behind [the prop / the
    body part], the [primary character's face / torso / region] is
    visible. The [secondary character] leans in from the top edge.

  Camera height MATCHES the prop's body anchor level:
    Held aloft at chest -> chest level
    Placed on belly     -> belly level (camera lens level with navel)
    Symptom on calf     -> mid-shin level
    Symptom on belly    -> belly level
    Symptom on face     -> face level
    Worn on wrist       -> wrist level

  Required negatives block:
    No desk visible. No [prop] on a surface. No top-down camera angle.
    No high-angle shot. No prop sinking to the lower-third. The prop
    dominates the center of the frame.

  Innovation-side: when the outside-niche source happened to use desk-
  anchored composition, DO NOT replicate the desk anchor. Re-anchor to
  the patient's body per Mode 1-5 based on the niche / prop / lens.
  Innovation preserves WHAT works (the rhetorical structure), not
  HOW the source happened to compose (desk-gravity bias propagates
  unless re-anchored).

INNOVATION-SPECIFIC ANTI-PATTERNS (do NOT replicate):

- Don't keep the outside-niche source's NICHE WORDS. Port to target niche.
- Don't keep the source's PERSONA archetype unless the target cell calls for it.
- Don't preserve compliance violations from the source (banned vocabulary,
  Korella-unsafe surfaces — strip and replace per v702/v615/v696).
- Don't add a "framework" or "mechanism" the source didn't have. Innovate
  preserves what works — does NOT invent novel mechanics.
- Don't shift HOOK family (e.g. source = SHAME-PROXY → output ≠ BEFORE-AFTER).
  If the target cell really doesn't fit the source's HOOK family, propose
  3 alternative source videos that DO fit instead.

OUTPUT — single videos/*.md file per target cell spec.

End with:
  ## Sources — outside-niche reference cited as primary corpus parent +
    ≥1 cross-validating Korella decoded parent
  ## Used in — placeholder (operator fills post-production)

TASKEOF
}

# Always write to temp file as fallback (clipboard can fail in sandboxed envs)
TMPDIR_PATH="${TMPDIR:-/tmp}"
BUNDLE_FILE="$TMPDIR_PATH/innovate_bundle_$(date +%s).md"
build_bundle > "$BUNDLE_FILE"
BYTES=$(wc -c < "$BUNDLE_FILE")

# Resolve a host-native path for the bundle so operators on Windows / Git Bash
# can drag-drop or paste the file directly into upload fields (Gemini, AI Studio,
# Claude.ai) which expect Windows-form paths, not POSIX /tmp/... paths.
WIN_BUNDLE_FILE=""
if command -v cygpath >/dev/null 2>&1; then
    WIN_BUNDLE_FILE="$(cygpath -w "$BUNDLE_FILE" 2>/dev/null || true)"
fi

print_paths() {
    echo "[innovate_bundle] Bundle saved (POSIX):   $BUNDLE_FILE"
    if [[ -n "$WIN_BUNDLE_FILE" ]]; then
        echo "[innovate_bundle] Bundle saved (Windows): $WIN_BUNDLE_FILE"
    fi
}

print_target_cell_guidance() {
    if [[ -z "$CELL_SPEC" ]]; then
        echo "[innovate_bundle] Add target cell spec to LLM prompt, e.g.:"
        echo "[innovate_bundle]   \"innovate this for Nuri male-ED clinic\""
    else
        echo "[innovate_bundle] Target cell already embedded: $CELL_SPEC"
    fi
}

print_upload_guidance() {
    echo "[innovate_bundle] Upload options for LLMs with paste-size caps (e.g. Gemini app):"
    echo "[innovate_bundle]   - Drag the .md file from Explorer into the chat's attach field"
    if [[ -n "$WIN_BUNDLE_FILE" ]]; then
        echo "[innovate_bundle]   - Or paste the Windows path above into the upload field"
    fi
}

if [[ -n "$CLIP_CMD" ]]; then
    if cat "$BUNDLE_FILE" | $CLIP_CMD 2>/dev/null; then
        echo "[innovate_bundle] OK: $TOTAL_FILES files concatenated (~${BYTES} bytes), piped via '$CLIP_CMD'"
        print_paths
        echo "[innovate_bundle] Paste into your LLM (Gemini / GPT-5 / Claude API)."
        print_target_cell_guidance
        print_upload_guidance
    else
        echo "[innovate_bundle] WARNING: clipboard pipe failed"
        print_paths
        echo "[innovate_bundle] Manual copy: cat \"$BUNDLE_FILE\" | clip"
        print_target_cell_guidance
        print_upload_guidance
    fi
else
    echo "[innovate_bundle] No clipboard tool."
    print_paths
    print_target_cell_guidance
    print_upload_guidance
fi
