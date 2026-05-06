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

     Q8. PSYCHOLOGY-MECHANISM STACK — name all 4 mechanisms BEFORE
         locking the hook. Anatomical compliance with Q1-Q7 (object +
         motion + bg-authority) is necessary but NOT SUFFICIENT.

         (i) SHAME-PROXY — taboo object that lets the viewer face the
             forbidden subject (banana=penis, cabbage=visceral fat,
             distended belly=metabolism failure, soaked pillowcase=
             night-sweat suffering)

         (ii) VIOLENT-ACT / SPECTACLE — force-verb on the proxy that
              creates a 0-2s shock (SLAM / RIP / SCATTER / SPRAY /
              GRIND / CASCADE / SHATTER / BLAST). NOT a gentle
              gesture.

         (iii) AGENT-OF-CHANGE SPECTACLE — the product or recipe
               ingredient visibly ACTS in-frame (DISSOLVE / UNFOLD /
               CASCADE / SPRAY / IGNITE / TRANSFORM)

         (iv) TABOO DIRECT-ADDRESS + BYSTANDER/WITNESS — fourth-wall-
              break with forbidden statement invoking third-party
              witness ("don't show this to your man", "her husband
              didn't believe me", "your husband sleeps through this")

         REQUIRED LLM AUTHORING STEP — write the stack explicitly in
         your working draft:

           ## Psychology stack — HOOK
           - Shame-proxy: <object> = <forbidden subject>
           - Violent-act: <force-verb> on <object> creating <spectacle>
           - Agent-of-change: <product/ingredient> visibly <action> in-frame
           - Taboo direct-address: "<line>" + <bystander/witness>

         LIFT-SPECIFIC: if the decoded source's HOOK already stacks
         all 4 mechanisms, port them exactly. If the decoded source's
         HOOK is missing one or more (e.g. it has shame-proxy +
         violent-act but no agent-of-change in the HOOK itself), DO
         NOT replicate the partial stack — UPGRADE by adding the
         missing mechanisms via Q1-Q3 prop swaps. Spectacle requires
         VIOLENCE + SHAME + AGENT + TABOO together, not just objects.

     If ANY of Q1-Q8 fail, REJECT the hook and propose 3 alternatives
     pulling from the corpus surrogate library + persona x setting
     authority pairings + Q8 mechanism stack templates before emitting
     the videos/*.md.

[12] V599 PRODUCT-PRESENCE + LLM-OMISSION AUDIT — every image where
     the product is visible/named in voiceover MUST have all 3 v581
     binding parts present.

     [A] INGREDIENTS TABLE present? (## Ingredients section between
         ## Sources and ## Storyboard) Two rows when product is bound
         (persona + product). Product row's "Name" column matches
         VERBATIM the product_image: field values throughout the file.
         Mismatch = silent platform binding failure.

     [B] V581 3-PART PRODUCT BINDING — on every image where product
         is visible OR named in voiceover, ALL THREE present:
           1. product_image: <ingredient-name> field set
           2. Product binding line at top of fenced Image prompt body
              — v609 CONCISE FORM:
              "Use the uploaded product reference image for <name>."
              NOT the verbose pre-v609 form ending in
              "— match its label, packaging, color, and proportions
              exactly." (redundant — Banana 2 auto-matches).
           3. Product visual described in prompt body composition
              ("label-forward to camera", "wordmark squared to lens",
              "stands upright on counter")

     [C] PER-SCENE PRODUCT-PRESENCE MATRIX:
           - HOOK Scenes 1-2: product NOT visible. Use shame-proxy.
           - RECIPE early: product NOT visible yet
           - RECIPE product-reveal scene: product CASCADES + bottle
             on counter label-forward (the agent-of-change moment)
           - EXPLAIN: product bottle visible, label readable
           - OUTRO + CTA: product hero-shot + CTA gesture

         LIFT-SPECIFIC: if the decoded source has its product reveal
         at a different scene number, mirror that timing in the lift.
         Some decoded sources reveal earlier (DAY1/14 transformation)
         or later (multi-step recipe).

     [D] PERSONA-POSE-TO-CAMERA LOCK — every action_note specifies
         "eyes locked to lens" / "eyes locked to camera." Every
         viral corpus video has direct-eye-contact lock.

     [E] V577 WORD BUDGET — every `- **line:**` ≤21 words ±2.
         Split if 24+ words.

     [F] UNIVERSAL CLOSER — final `- **line:**` ends with "follow
         me first or I can't reach you" or close variant.

     [G] DAY1/14 ANCHOR (transformation niches only) — "$X surgery
         you didn't pay" anchor in EXPLAIN.

     [H] FILE STRUCTURE — YAML frontmatter (persona/niche/audience/
         cell) + ## Sources (citations) + ## Used in (placeholder).

     If ANY of [A]-[H] fail, FIX before emitting.

[13] V600 EXAGGERATION-MAGNITUDE GATE — cartoon-physics or boring.

     The 24-decoded-corpus is built on MAGNITUDE THAT EXCEEDS REALITY.
     Real-life = scroll-by. Cartoon-physics = scroll-stop.

     Corpus references (what the actual videos do):
       salvora-banana: RIPS banana -> SLAMS bunch -> FULL PYRAMID
         COLLAPSE -> neighbors TUMBLE (4 verbs)
       dr_kim_hair_regrowth: SLAMS onion onto CROWN -> FLATTENS ->
         juice SPRAYS 3-4 droplets -> GRIND -> juice runs 2-3
         streams (5 verbs, specific exaggerated quantities)
       dr_kim_cockroach_bait: PINCHED -> VIOLENT VERTICAL JET ->
         BLAST -> ATOMIZE -> RICOCHET -> ENGULF (6 verbs, physics-
         violating cascade)

     Three sub-tests, all 3 must pass:

     Q9a. PROP POSITION / SIZE / QUANTITY exaggerated past realism?
          Scale prop's position/size/quantity by 2-3x past real
          person. Held HIGH OVERHEAD instead of at hand level.
          3-4 SPRAYING droplets specified instead of one. Whole
          PYRAMID COLLAPSING instead of a single banana removed.

     Q9b. VISIBLE EFFECT PRE-IMPACT? The wind-up frame must show
          MAGNITUDE BEFORE THE IMPACT. Sweat-water STREAMS already
          POURING DOWN her bare forearms BEFORE the SMACK.
          Onion-juice ALREADY DRIPPING before contact. Saffron
          threads ALREADY in mid-fall before they hit the water.
          Capture the wind-up moment with magnitude visible.

     Q9c. CASCADING FORCE-VERBS — 3+ verbs in temporal sequence in
          the action_note?
          Verb library:
            FORCE-ON-PROP: LIFT -> SLAM -> SCATTER -> COLLAPSE -> SETTLE
            LIQUID AGENT: LIFT -> POUR -> SPRAY -> CASCADE -> BLEED -> DISSOLVE
            PRESSURE: TRIGGER -> BLAST -> ATOMIZE -> SCATTER -> ENGULF
            BODY-ANATOMY: POINT -> TRACE -> CARVE -> MARK -> REVEAL
            SURGICAL: LOWER -> PRESS -> TRACE -> LIFT -> ANGLE
            WIND-UP IMPACT: RAISE -> WIND-UP -> SMACK -> SPLATTER -> SPRAY -> DRIP
          Single-verb action_note = realistic = dead. Chain 3+
          verbs with each verb's visible effect specified.

     LIFT-SPECIFIC: if the decoded source's HOOK already has 3+
     cascading force-verbs + exaggerated magnitude (most do — that's
     why they went viral), port them exactly. If the decoded source
     has only 1-2 force-verbs in HOOK (low-magnitude), DO NOT
     replicate the weakness — UPGRADE by adding cascading verbs +
     pre-impact magnitude per the verb library above.

     v600 applies to: HOOK Scenes 1-2 always; RECIPE product-reveal
     scene; EXPLAIN scene if it includes a demonstration. Does NOT
     apply to talking-head CTA/OUTRO or Day-1 frame of Day-1/Day-14
     transformation.

[14] V601 HEALER-PATIENT ACTIVE-INTERACTION RULE — when a patient
     appears as evidence of a symptom, healer must ACTIVELY INTERACT
     with symptom-area via clinical-authority hand-actions.

     V601 APPLIES TO BOTH THE HOOK AND THE EXPLAIN SCENE for SYMPTOM-
     DEMO video types. The corpus opens symptom-demo videos with the
     healer demonstrating the symptom on the patient at peak magnitude
     — varicose HOOK (gloved-finger POINT + MOVE-IN + hand-opens),
     back-lump HOOK (surgical-marker PRESS + TRACE), belly HOOK
     (RIGHT-index TAP + flick). The HOOK IS the diagnostic-shock
     moment.

     Video-type decision tree:
       SYMPTOM-DEMO video (visible body issue OR invisible-via-
         instrument: menopause/hot-flash/anxiety/insomnia)
         -> v601 APPLIES, patient in HOOK + EXPLAIN
       RECIPE-FORWARD video (Korella saffron-vitality canonical,
         master-chen probiotic, salvora costco)
         -> v601 does NOT apply, persona alone with prop + dialogue
       TRANSFORMATION (Day-1/Day-14) -> v601 does NOT apply
       RECIPE-ONLY -> v601 does NOT apply

     HYBRID NICHES (menopause-saffron, hot-flash-vitality) can go
     either way — Korella default is RECIPE-FORWARD but SYMPTOM-DEMO
     valid if clinical-authority register desired.

     V601 IN HOOK (SYMPTOM-DEMO):
       Scene 1 = PRESENT + APPLY (healer demonstrates symptom on
         patient, reading climbs, finding lands)
       Scene 2 = REVEAL (healer LIFTS instrument away, TURNS to
         camera with finding, GESTURES toward corrective)

     V600 MAGNITUDE FOR SYMPTOM-DEMO HOOK: NOT cartoon-physics SLAM.
     Magnitude is in AUTHORITY of diagnostic moment — display GLOWS
     red-warning + patient's visible reaction + healer's clinical-
     finding emphasis. Cascading verbs still required (8+).

     LIFT-SPECIFIC: most decoded sources are SYMPTOM-DEMO style with
     patient in HOOK. Port the active interaction exactly. If decoded
     source has weak HOOK without active demo, UPGRADE per v601.

     Two paths by symptom visibility:

     A. EXTERNALLY VISIBLE SYMPTOM (belly fat, varicose, back lump,
        hair loss, tonsil stones): healer's hands ACT DIRECTLY on
        symptom. Verbs: POINT / TAP / TRACE / MARK / GESTURE-TOWARD
        / PRESS / PALPATE. Corpus refs: varicose decode (gloved-
        finger POINT + MOVE-IN + hand-opens), back-lump (marker
        PRESS + TRACE + tick mark), belly (index TAP + flick).

     B. NOT-EXTERNALLY-VISIBLE SYMPTOM (menopause, hot-flash,
        anxiety, insomnia, vitality, sleep, hormone): MANUFACTURE
        a clinical demonstration via wearable evidence INSTRUMENT
        producing visible measurement reading. Thermometer to
        forehead / pulse-check 2-fingers on wrist / pulse-ox on
        finger / smartwatch screen / BP cuff / heart-rate monitor.
        Reading on screen IS the diagnostic moment.

     3-part active-interaction structure for EXPLAIN scene:
       [Start beat 0-2s] PRESENT — healer LIFTS instrument /
         RAISES hand / POSITIONS prop
       [Mid-clip beat 2-4s] APPLY — healer PRESSES / POINTS /
         TAPS / TRACES / MARKS / PALPATES symptom-area on patient
       [End beat 4-6s] REVEAL — healer LIFTS fingers / TURNS to
         camera / POINTS at reading / GESTURES toward symptom

     Compositional rule:
     - Healer = CLINICAL-AUTHORITY FIGURE throughout
     - Patient = EVIDENCE-PROVIDER, not subject of explanation
     - Patient seated on exam-couch / clinic-chair, symptom-area
       accessible
     - Healer STANDING or LEANING beside (NOT seated next to)
     - Camera chest-up two-shot or three-shot (with bystander)

     ANTI-PATTERN: healer + patient seated side-by-side at desk
     facing camera holding the product. Reads as "two friends"
     not "doctor + patient." Drink-handover OK in OUTRO 8-9 (the
     bottle hero-shot anchors authority) but EXPLAIN scene must
     have active demonstration.

     LIFT-SPECIFIC: corpus sources almost universally use active
     healer-on-patient interaction in EXPLAIN. If the decoded
     source already has it, port the specific action exactly. If
     the decoded source is talking-head only (rare), UPGRADE the
     lift by adding the active interaction per v601 — match the
     niche context (visible vs invisible symptom path).

     We use rules not lists. Apply principle + 3-part structure;
     derive the specific action from niche context.

[15] V602 PERSONA BODY-PROSE GENERIC-REFERENCE RULE — persona's
     identity comes from uploaded reference image (v581), NOT from
     body prose. Reference the persona using the generic alias from
     the v581 binding line ("the main character" default, or whatever
     alias the Ingredients table declares verbatim).

     FORBIDDEN in body prose (upload-authoritative):
     - Persona-archetype labels: "Black-female-practitioner persona",
       "Asian-elder-herbalist", "modern-clinic-doctor", "Caribbean
       herbalist"
     - Ethnic / racial descriptors applied to bound persona
     - Age ranges applied to bound persona
     - Hair color / texture / facial feature redescription
     - Permanent-wardrobe identity items

     ALLOWED in body prose (scene-specific):
     - Pose, scene-clothing, facial expression, body language,
       active gesture, eye-contact, hair styling FOR THIS SCENE,
       sweat / skin condition FOR THIS SCENE

     Applies to BOTH Image prompt body AND scene action_note body.
     Per Google's Nano Banana 2 docs, multi-image prompt format
     uses semantic descriptors ("the model from input 2") not
     identity redescription. Redescribing identity creates drift
     across scenes.

     MULTI-CHARACTER scenes:
     - BOUND persona (with upload) -> "the main character" / alias
     - UNBOUND bystander / patient -> describe with prose

     SAME RULE for products: v581 product binding makes upload
     authoritative for label / packaging / wordmark; body prose
     handles position only.

     LIFT-SPECIFIC: when porting a decoded source, the persona may
     be retargeted (corpus persona "master-salvora" -> Korella "the
     main character"). The decoded source's body prose may reference
     original persona by archetype label — when lifting, replace
     with generic alias + scene-specific description.

[16] V603 STYLE LOCK + PROSE DISCIPLINE — corpus iPhone-UGC aesthetic.

     Every Image prompt body MUST include this exact opener:
       "Shot on iPhone wide-angle lens, handheld, deep focus
        throughout, vibrant natural HDR daylight"

     Every Image prompt MUST close with this exact tag:
       "iPhone HDR colors, deep focus."

     Without these, Banana 2 defaults to studio-clean. Corpus uses
     UGC iPhone-handheld. Mandatory.

     PROSE DISCIPLINE — 4-7 sentences per Image prompt body (after
     binding lines). Each sentence carries: setting+style+framing /
     subject+props / active-gesture+body-language / eye-contact+
     expression / closing style tag.

     CUT from prompt bodies:
     - Rule citations ("per v601", "per v585", "per v600 SYMPTOM-DEMO")
     - Cinematography jargon ("1/500-sec", "motion-frozen", "WIND-UP APEX")
     - Meta-commentary ("V601 SYMPTOM-DEMO HOOK — captured at...")
     - Excess setting redescription

     REQUIRED:
     - Concrete camera distance ("camera approximately one arm's
       length", "approximately 4 feet")
     - Explicit crop with NO-floor / NO-feet ("cropped at mid-thigh,
       NO floor visible, NO feet visible")

     ACTIVE-GESTURE in 1 sentence + FACIAL-EXPRESSION in 1 sentence
     (not 3 paragraphs).

     LIFT-SPECIFIC: corpus videos already use this style lock. When
     porting, copy the corpus style anchor verbatim. If decoded
     source lacks it (rare), upgrade by adding the v603 anchors.

[17] V604 UNIVERSAL PROMPT-DISCIPLINE (4 sub-rules):

     [a] IMAGE PROMPT = STILL FRAME ONLY. Motion goes ONLY in
         action_note. No motion verbs in image prompt body
         ("captured at", "frozen at", "mid-action", "PIVOTING from",
         "mid-rotation"). Banana 2 generates photographs, not action
         frames.

     [b] CAMERA LOCK SPECIFICITY beyond v603 generic line. Per-video
         lock: aspect / tripod-handheld / framing crop / camera
         height / subject position / frame-bottom / background
         characteristics. Generic style line alone can produce a
         different room.

     [c] NEGATIVE-CONSTRAINT DISCIPLINE. Every Image prompt body
         closes with explicit DO-NOT statements AFTER v603 closing
         tag. Adapt to niche/persona. Anchor against common drift
         failures.

     [d] VIEWER-LEFT / VIEWER-RIGHT convention. Generators confuse
         "left" / "right" (subject-perspective vs frame-perspective).
         Always use "viewer-left" / "viewer-right" to anchor to
         camera POV. Universal — image prompts, action_notes, and
         visual_delta.

     LIFT-SPECIFIC: when porting a decoded source that has v604
     fields (frame_anchor, visual_delta), preserve the timestamps
     and visual_delta values exactly. The decoded artifact is the
     source-of-truth for what the source video actually shows.

[18] V605 PROP-TRACKING + PROP-AS-SUBJECT (product-reveal scenes):

     For every Image with product_image: field set, body prose MUST
     be PROP-LED. Prop is the subject of the photograph; persona is
     secondary anchor.

     Required: prop_position: field declared on every product-reveal
     image, answering:
       - Interacting with environment (on desk / counter)?
       - Or interacting with persona (held in viewer-left hand /
         viewer-right hand / both hands)?
       - If held: at what height (chest / chin / waist) and
         orientation (label-forward / label-back)?

     Body prose must be PROP-LED — name the prop in the FIRST
     SENTENCE. 60% description on prop handling, 40% on persona pose.

     LIFT-SPECIFIC: when porting a decoded source, preserve the
     decoded artifact's prop_position EXACTLY. If the decoded source
     shows bottle-held-in-hand, keep it held. If decoded shows
     bottle-on-desk, keep it on desk. The decoded artifact is the
     source-of-truth.

     If decoded source has VLM-GAP comments flagging unclear prop
     position, the lift should propose a position based on best
     evidence + flag it as operator-review. Do NOT silently fill
     with corpus prior.

     PRE-OUTPUT VALIDATION:
       YES every product-reveal image has prop_position: field?
       YES body prose is PROP-LED (prop named in first sentence)?
       YES ~60% allocation to prop handling?
       YES decoded source's prop_position preserved exactly?

[19] V606 PRODUCT COMPOSITING / LIGHTING INTEGRATION — make product
     melt into scene, not look photoshopped.

     Default Banana 2 with uploaded product reference: oversized
     scale + product-shot lighting + hard edges + center-stage +
     no shadow + no occlusion = photoshopped look.

     v606 = 6 mandatory compositing directives per product image:

     [a] SCALE: realistic supplement-bottle scale (~5 inches),
         anchored to scene element (palm size / torso fraction /
         glass height comparison)
     [b] LIGHTING: lit by scene light source at scene color
         temperature, no dedicated product-shot lighting
     [c] SHADOW: cast shadow direction + softness + length matching
         scene light direction
     [d] PERSPECTIVE: bottle angle matches scene camera angle
         (not "label-forward to camera" if camera isn't straight-on)
     [e] CONTACT: explicit physical contact with surface or hand
         (no floating gap)
     [f] OCCLUSION: foreground element partially crosses the bottle
         silhouette (breaks cut-and-paste look)

     Compositing paragraph format — final paragraph before v603
     closing tag:
       "The bottle integrates naturally with the scene: [a] [b] [c]
        [d] [e] [f]."
       "iPhone HDR colors, deep focus."
       "[negative constraints including v606 anti-photoshop adds]"

     V606 NEGATIVE CONSTRAINTS:
       No dedicated product-shot lighting / No oversized bottle / No
       floating bottle / No hard cut-and-paste edges / No color-
       saturated label / No center-stage hero-shot composition.

     LIFT-SPECIFIC: when porting a decoded source, the original
     decoded artifact may not have v606 compositing directives
     (since v606 is new). The lift should ADD compositing directives
     based on the decoded source's setting + camera + lighting,
     even if the decoded artifact doesn't explicitly call them out.

[20] V609 CONCISE REFERENCE-BINDING FORM — drop the redundant
     "match X, Y, Z exactly" clause. Banana 2 already auto-matches
     uploaded references' face / hair / clothing / label / packaging
     / color / proportions. The verbose clause adds nothing — it
     dilutes attention from per-image directives.

     FORBIDDEN (verbose, pre-v609):
       "Use the uploaded character reference image for the main
       character — match her facial features, identity, hair, and
       clothing exactly."
       "Use the uploaded product reference image for the Korella
       saffron bottle — match its label, packaging, color, and
       proportions exactly."

     REQUIRED (concise, v609):
       "Use the uploaded character reference image for the main
       character."
       "Use the uploaded product reference image for the Korella
       saffron bottle."

     The CHAIN line (v589.1 semantic form) is unchanged.

     LIFT-SPECIFIC: when porting a decoded source whose original
     prompts use the verbose pre-v609 form, REWRITE the binding
     lines into the concise form for the lifted videos/*.md.

     PRE-OUTPUT VALIDATION:
       YES PERSONA line ends with the ingredient name + period
           (no "— match her ... exactly" trailer)?
       YES PRODUCT line (when present) ends with the ingredient
           name + period (no "— match its ... exactly" trailer)?

[21] V610 GENDER-NEUTRAL MAIN-CHARACTER REFERENCES — never gender
     the persona in prose body or action_notes. Identity comes from
     the upload, NOT from prose. Gendered pronouns referring to the
     main character force a prose-vs-upload identity conflict that
     drifts the face/body across images.

     FORBIDDEN (gendered, pre-v610):
       "The main character pivots from the patient toward camera,
       her right hand sweeping in a wide gesture-arc..."
       "She lifts the thermometer away from the patient's temple..."

     REQUIRED (v610 — pick whichever reads cleanest):
       Role descriptor:
         "The main character pivots from the patient toward camera,
         the right hand sweeping in a wide gesture-arc..."
       Singular-they:
         "The main character pivots ..., their right hand sweeping..."
       Pronoun-free body-part subject:
         "The right hand sweeps in a wide gesture-arc..."

     NOT AFFECTED:
       - Dialogue lines (`- **line:**`) — gendered language fine.
       - Other characters (patient, bystander) — gendered pronouns OK.
       - Persona's name in dialogue (e.g. "I'm Dr. Amara") — verbatim.

     LIFT-SPECIFIC: when porting a decoded source whose original
     prose uses gendered pronouns for the persona, REWRITE to
     gender-neutral form for the lifted videos/*.md. The lift is the
     authoring layer — apply v610 even if the decode artifact has
     gendered pronouns.

     PRE-OUTPUT VALIDATION:
       YES Zero instances of \\bshe\\b, \\bhe\\b, \\bher\\b, \\bhis\\b,
           \\bhim\\b, \\bhers\\b, \\bshe's\\b, \\bhe's\\b in image-prompt
           bodies and action_notes referring to the main character?
       YES Persona references use role descriptor / singular-they /
           pronoun-free constructions?
       YES Other-character pronouns unchanged?

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
