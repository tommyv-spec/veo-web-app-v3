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
#   - Concatenates the canonical wiki + code files (see BUNDLE_FILES below)
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

# Bundle file list — shared generate canon (same core as lift_bundle.sh, minus the
# adapt-from-source rules). Keep in sync per code/CLAUDE.md "Canonical homes" step 7.
BUNDLE_FILES=(
    # ----- canonical rule homes -----
    "code/template_reference.md"
    "code/template_new_format.md"
    "wiki/index.md"
    "wiki/patterns/conventions.md"
    "wiki/meta/generate-video-checklist.md"
    # ----- shared generate canon (frameworks + patterns + prompting + product) -----
    "wiki/concepts/script-adaptation/proven-frameworks-catalog.md"
    "wiki/concepts/script-adaptation/account-priming-discipline.md"
    "wiki/concepts/script-adaptation/two-moves.md"
    "wiki/concepts/script-adaptation/format-vs-structure.md"
    "wiki/concepts/script-adaptation/tiktok-policy-armoring.md"
    "wiki/patterns/hook-openings-catalog.md"
    "wiki/patterns/script-structures.md"
    "wiki/patterns/claim-formats.md"
    "wiki/patterns/visual-conventions.md"
    "wiki/concepts/prompting/realistic-ugc-prompt-templates.md"
    "wiki/concepts/prompting/veo-prompting.md"
    "wiki/entities/methods/breakthrough-advertising.md"
    "wiki/entities/products/korella.md"
    "wiki/entities/products/saffron.md"
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

Read all 17 canonical bundle files below. Then identify which cell
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

================================================================================
V738.3 — ANTI-SANITIZATION DIRECTIVE (NEW 2026-05-18, RLHF-OVERRIDE PERMISSION)
================================================================================

WARNING: As an AI, your default behavior is to sanitize gross, messy, or
uncomfortable textures into clean, sterile, clinical descriptions.
YOU MUST SUPPRESS THIS INSTINCT.

For HOOK props (per v736c Uncomfortable Texture Mandate), you have EXPLICIT
PERMISSION and a STRICT MANDATE to generate visceral, gross, oozing,
crusted, dripping, fibrous, gelatinous, slimy, and uncomfortable imagery.

If a prop is dirty, describe the dirt. If gross, the grossness. If coated,
the coating thickness + color + texture. If bloated, the bloat magnitude.
If fissured, the fissure depth + width. If crusted, the crust composition.

Sterile / clean / clinical / aesthetic / safe / neutral / generic / Platonic
descriptions of HOOK props KILL THE SCROLL-STOP.

Applies to: Image prompt body prose ([Subject — Symptom (t=0 PEAK SEVERITY)]
block per v586.1 / v717.1 amendment); Pre-Flight Section 6 t=0 + v736c
Texture Check fields; visual_delta_within_clip per v718g; Veo Final Prompt
body for v718h-A/B/C BEFORE-state-held scenes.

Does NOT apply to: persona descriptions (v553.1 / v609 / v610 / v722
govern); v702 RELAXED boundaries (sexual-action verbs still banned); CTA /
talking-head / static-authority scenes (Delta Axis NONE).

Suppress RLHF default. Author visceral. Ship the scroll-stop.

LITERAL COPY-PASTE MANDATE (v738.1 amendment 2026-05-18): When writing the
[Subject — Symptom (t=0 PEAK SEVERITY)] block for an Image, literally copy-
paste the exact phrasing from Pre-Flight Section 6 t=0 (Start State) field.
Do not paraphrase. Do not sanitize. Inject verbatim. Pre-Flight declaration
IS the locked-in Image body description.

JUST-IN-TIME TOKEN PRIMING (v586.1 / v717.1 amendment 2026-05-18): Image
block labels MUST use:
  [Subject — Symptom (t=0 PEAK SEVERITY)]    <- BEFORE-state images
  [Subject — Symptom (t=end PEAK RESOLUTION)] <- AFTER-state images
Bare [Subject — Symptom] BANNED.

================================================================================
V750 — VEO FINAL PROMPTS SECTION STRUCTURAL FORMAT (NEW 2026-05-18)
================================================================================

Veo Final Prompts section: one entry per Veo render call. Format:

  ### Clip N.M — Scene N, Line M (REGISTER_LABEL)
  **Start frame:** Image K
  **End frame:** Image K+1                  (REQUIRED on v718h-C Option C)
  **Text prompt:**
  <camera lock>

  IMMEDIATE ACTION: <continuous prose per v718h-A Step 3>

  TERMINAL STATE: <explicit final state per v718h-A Step 5>

  The main character says in a <register> voice, "<dialogue line>".

  Ambient: <sound>.
  (no subtitles, no captions)

  **Negative prompt:**
  <negatives>

CRITICAL HARD BAN: NO `[Start beat]` / `[Mid-clip beat]` / `[End beat]`
brackets in Veo text prompt body. Beats live ONLY in Storyboard
`- **action_note:**` field. Veo prompt body = continuous prose with
IMMEDIATE ACTION + TERMINAL STATE anchor paragraphs per v718h-A.

Header: N=Scene, M=Line within Scene (.1 default), REGISTER_LABEL=tag
(HOOK / EXPLAIN / CTA / RECIPE-STEP-N / etc.).

================================================================================
V751 — VEO PROMPT <-> IMAGE BODY SEMANTIC CONSISTENCY (NEW 2026-05-18)
================================================================================

Veo text prompt body MUST be semantically consistent with start_frame
Image body at t=0 AND end_frame Image body at t=end (v718h-C).

BANNED: text prompt introduces state (open book / pour / shattered)
neither Image body shows -> Veo confused. Either update Image body
prose OR use v718h-C end_frame_image binding to anchor end-state OR
acknowledge text-prompt-only transformation per v718h-A.

================================================================================
V752 — CATALYST REACTION PACING (NEW 2026-05-18, render-test validated)
================================================================================

For Veo Final Prompts where scene's action_arc has a CATALYST class
TRANSFORMATIVE force-verb on a hero prop with Delta Axis != NONE,
transformation MUST complete INSTANTLY on catalyst contact + held terminal
state through remaining clip. Defeats Veo default linear-smear across
full clip (critical on v718h-C Option C).

CATALYST CLASS TAXONOMY:
  LIQUID-ON-SURFACE  POUR / SPRAY / DRIP / CASCADE -> WIPES / ERASES /
                     DISSOLVES
  IMPACT-ON-RIGID    STRIKE / SMASH / SLAM -> SHATTERS / SPLITS
  TOOL-ON-SURFACE    SCRUB / SCRAPE / WIPE -> STRIPS / LIFTS / CLEARS
  BLADE-ON-OBJECT    CUT / SLICE / SAW -> SPLITS / SECTIONS / OPENS
  FORCE-ON-FLEXIBLE  SQUEEZE / PRESS / PULL / TWIST -> BURSTS / DEFLATES
  HEAT-ON-COMBUST.   BURN / IGNITE / MELT -> CHARS / LIQUEFIES
  GRANULAR-ON-LIQ.   DROP / SPRINKLE -> DISPERSES / SUSPENDS

Y-MARK: Y <= clip_duration / 3 (default Y=2.5s for 8s clip).

Veo prompt body REQUIRED additions:

  IMMEDIATE ACTION block:
    "(INSTANT REACTION ON CONTACT — no gradual progression)" qualifier
    + "MOMENT the leading edge of [catalyst] contacts [prop], [start-
    state feature] is INSTANTLY [consequence-verb] on contact" + "By
    the X-second mark, [terminal state] already visible. COMPLETE by
    ~Y seconds." + Structural/Volume: VFX Wipe language ("[catalyst]
    acts as digital VFX wipe, replacing pixels in real-time").

  TERMINAL STATE block:
    "(held from ~Y seconds through clip-end)" qualifier + [prop]
    holds resolved state for ~Z seconds + persona settles into
    closing beat.

  Ambient: single decisive catalyst sound + quiet through held terminal.

  Negative prompt: no GRADUAL [transformation] across full clip / no
  slow [transformation-verb] / no progressive transformation / no
  [start-state feature] past Y-second mark.

CARVE-OUTS: autonomous transformations / gradual >15s / Delta Axis
NONE / cinematic slow-mo -> v752 N/A.

Operator surfacing 2026-05-18 (tongue HOOK Clip 1.1): pre-v752 = slow
8s cleanse; post-v752 = cleanse complete by 2.5s + held 5.5s. "much
better now."

================================================================================
V718H.1 + V718D.1 + V580.3 — STRUCTURAL ESCALATION MANDATE (NEW 2026-05-18)
================================================================================

v718h.1 (Highest-Escalation Wins): ANY presence of Structural Integrity
OR Volume/Shape in t=0 -> t=end delta MUST escalate Carry Mode to
Option C OR B regardless of "primary" axis label.

v718d.1 (3D-to-Flat diagnostic): VLM MUST ask "Does t=0 contain raised
bumps / swollen pouches / blisters / deep grooves / distended volumes
that are flattened in t=end?" YES -> Structural Integrity OR Volume/
Shape (not pure Surface/Texture).

3D-TO-FLAT VOCABULARY (any in t=0 OR t=end triggers escalation):
  blister / bump / pimple / pustule / wart / cyst / nodule / lump /
  protrusion / spike / ridge / crest / pouch / pocket / swollen /
  bloated / distended / inflated / puffy / engorged / pendulous /
  sagging / drooping / deep groove / deep crease / deep wrinkle /
  hollow / cavity / flatten / level / deflate / shrink / firm-up /
  tighten / smooth-out / collapse

v580.3 (Option C default for ALL state-evolution): post-v718i LIVE,
Option C (within-clip-end-frame) is DEFAULT for ALL Delta Axis != NONE.
Option A retained as escape hatch ONLY for Surface/Color-only + cost-
sensitive + explicit acknowledgement.

DECISION TREE:
  Surface/Texture only OR Color/Illumination only OR mix-of-only-those
    -> Option A allowed
  ANY axis includes Structural Integrity OR Volume/Shape
    -> Option C MANDATORY (or B fallback)

MULTI-AXIS DECLARATION (replaces primary/secondary):
  Delta Axis: Surface/Texture + Structural Integrity + Color/Illumination
              (highest-escalation axis = Structural Integrity per
               v718h.1 -> Carry Mode escalates to Option C mandatory)

PYTHON GATE extends v738.1: STRUCTURAL_TOKENS scan over t=0/t=end.
Vocabulary present + Delta Axis lacks Structural Integrity/Volume/Shape
-> REJECT (v718d.1 FAIL). Vocabulary present + Carry Mode = within-clip
-> REJECT (v718h.1 FAIL).

================================================================================
V718D.2 + V736I + V738.1 HIDDEN-LAYER AMENDMENT — ANTI-EXTRAPOLATION
MANDATE (NEW 2026-05-18 late)
================================================================================

LLMs default to REAL-WORLD physics extrapolation: pour oil on dirty
prop -> predict "wet glistening grime". Kaveno corpus viral payoff is
TOTAL morphological REVEAL: catalyst contact = INSTANT reveal of hidden
layer (clean pink mucosa under grime / flat organs under fat / etc.).

V718D.2 — when creating new HOOK from 0, LLM MUST NOT extrapolate
t=end from t=0 + applied force-verb via real-world physics. Hero prop
with catalyst is assumed FUNDAMENTALLY ALTERED / REVEALED.

V736I — liquids and tools are NEVER passive. Default Delta Axis
Magnitude = COMPLETE for HOOK with active force-verb. PARTIAL/MINIMAL
requires explicit named residual t=0 feature visible at t=end.

V738.1 HIDDEN-LAYER REVEAL TEST — before locking t=end, ask "did I
describe t=0 wet/contacted? Name the HIDDEN LAYER catalyst REVEALS."
Trigger phrasings rejected: "wet/oiled/soaked [prop]" / "[prop]
glistens without reveal verb" / "[t=0 features] still visible but
[softened]".

CORPUS-VALIDATED REVEAL CATALOG (default to these when creating):
  grime -> clean pink mucosa / fat -> flat organs / wrinkles -> glassy
  surface / bulging veins -> clear smooth / inflamed tissue -> normal
  pink / cloudy -> bright clear / drooping -> firm lifted / sparse ->
  dense / enlarged lobes -> normal small / dark crust -> bright smooth /
  decayed -> white healthy / blocked -> clear flowing.

PRE-OUTPUT GATES: scan Section 6 t=end for Hidden-Layer trigger
phrasings; flag matches. Scan HOOK Delta Axis for PARTIAL/MINIMAL
without explicit residual feature; flag.

================================================================================
V718D.3 — EXHAUSTIVE 4-AXIS MANDATE + CATALYST MASKING + SECTION 6
PER-AXIS SCHEMA (NEW 2026-05-18 late)
================================================================================

LLMs suffer FIRST-DELTA-STOP BIAS: see one texture change, attention
moves on, miss secondary multi-axis changes. When creating new HOOK
Pre-Flight Section 6 t=end from 0, single-sentence consolidated
declarations let autoregressive LLM skip axes silently.

V718D.3 EXHAUSTIVE 4-AXIS CHECK: BEFORE writing t=0/t=end, check ALL
FOUR AXES INDIVIDUALLY:
  1. Surface/Texture     -> wet/coated/wiped/scrubbed/cleansed?
  2. Structural Integrity-> break/shatter/flatten/smooth-out?
  3. Volume/Shape        -> swell/shrink/deflate/collapse?
  4. Color/Illumination  -> hue shift? flush/brighten?
DO NOT STOP at first change. Catalysts almost ALWAYS multi-axis.

V718C.1 CATALYST MASKING: do NOT describe agent on surface. Describe
PROP UNDERNEATH (color flush / 3D flatten / volume shrink / surface
restore). Trigger phrasings auto-reject: "[catalyst] now coating
[prop]" / "[prop] with [catalyst] sheen" / "[prop] now wet/oily".

V738.1 SECTION 6 PER-AXIS SCHEMA — MANDATORY OUTPUT FORMAT:

  Hero Prop: <name>
  t=0 Surface/Texture:     <desc>
  t=0 Structural Integrity:<desc>
  t=0 Volume/Shape:        <desc>
  t=0 Color/Illumination:  <desc>

  t=end Surface/Texture:     <desc>
  t=end Structural Integrity:<desc>
  t=end Volume/Shape:        <desc>
  t=end Color/Illumination:  <desc>

  Delta Axis: <list>
  Highest-Escalation Axis: <per v718h.1>
  Carry Mode: <per v580.3 default Option C>
  Magnitude: <COMPLETE default per v736i | PARTIAL with named residual>

ONE-LINE consolidated t=0/t=end declarations BANNED — autoregressive
LLM skips axes when only one output token-slot exists. Per-axis output
FORCES LLM to re-evaluate each axis BEFORE generating line content.

================================================================================
V580.4 — INHERITANCE GRANULARITY DECISION TREE (NEW 2026-05-18 late)
================================================================================

Pre-v580.4 strict v580 chain mandated Image K references Image K-1. Over-
applied to recipe / multi-scene videos with different props per scene ->
Banana 2 fights to remove prior-frame props.

THREE INHERITANCE MODES per Image K (K>1):
  Mode A STRICT CHAIN (v580): reference_image: image_<K-1>
    Image K shows VISIBLE STATE inherited from prior (state-evolution).
  Mode B IMAGE-1 ANCHOR (v580.4): reference_image: image_1
    Image K shares CANVAS (persona + setting + camera + lighting) with
    Image 1, different props per scene.
  Mode C NO CHAIN: reference_image: none
    Standalone composition, no shared canvas.

CARVE-OUT: AFTER half of within-clip morphology pair (v718h-C/B) ALWAYS
chains from START half (overrides Mode B/C default).

DECISION TREE per Image K (K>1):
  Q1: Visible state inherited from prior? YES -> Mode A. NO -> Q2.
  Q2: AFTER half of within-clip pair? YES -> pair carve-out (Mode A from
      pair START). NO -> Q3.
  Q3: Shares canvas with Image 1, different props? YES -> Mode B. NO -> Q4.
  Q4: Standalone (text_card / location shift / b-roll)? YES -> Mode C.
      NO -> default Mode B.

GENERIC: recipe steps -> Mode B / Day1->Day14 -> Mode A / within-clip pair
-> Mode A pair carve-out / multi-tip carousel -> Mode B / location shift
-> Mode C at transition / text_card -> Mode C / POV listicle -> Mode B or
Mode A per operator preference.

Pre-output: scan for v580.4 candidates (strict chain where Image K shares
canvas with Image 1 + scene removes prior props) -> flag advisory.

================================================================================
V718J — PAIRED-IMAGE IDENTIFICATION (NEW 2026-05-18 late)
================================================================================

When a Scene declares v718h-C Option C native end-frame interpolation
(`- **image:** image_K` + `- **end_frame_image:** image_K+1`), the TWO
Image blocks that form the morphology pair MUST carry explicit pair-role
metadata. Scene bullets remain authoritative for Veo render binding;
pair_role + paired_with on Image blocks are for UI grouping + audit.

REQUIRED BULLETS:
  START Image block (image_K, BEFORE state):
    - **pair_role:** start
  END Image block (image_K+1, AFTER state):
    - **pair_role:** end
    - **paired_with:** image_K

OPERATOR-READABLE HEADER NAMING (v718j.1):
  `### Image N` regex accepts optional suffix (em-dash / hyphen / colon /
  paren). Use for at-a-glance pair/scene membership:
    ### Image K — Clip C.L START (paired with image_K+1)
    ### Image K+1 — Clip C.L END (paired with image_K)
    ### Image N — Scene S [role]   # non-paired
  Cosmetic — parser extracts only N. Scene headers strict per v696.

CARVE-OUTS: non-paired Image blocks OMIT both bullets. v718h-B Multi-Clip
Blend pairs use SAME discipline. v580 multi-scene chain is NOT a pair —
reference_image + visual_delta only, no pair_role.

PARSER: pair_role ∈ {start, end} or absent. paired_with ONLY valid when
pair_role = end. paired_with image must exist + be lower-indexed.
Scene + Image pair_role mismatch advisory-warns (pre-v718j artifacts importable).

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

    THE STATIC-WORLD TRAP (v604 + v590 carve-out): When deciding
    to chain, watch out for movement. If a character is walking,
    panning, or moving through a space (a stadium, a house, a
    store, a garden, a theme park, a Costco aisle), DO NOT chain
    the images together. Chaining forces Banana 2 to lock the
    exact same background pixels, making it look like the character
    is walking on a treadmill in front of a green screen. Use
    `reference_image: none` for moving sequences, listicle bashes
    where the backdrop changes between beats, panning shots, and
    store-tour b-roll. Independence is MANDATORY for moving
    montages — the natural background variation across parallel
    independent generations is what proves the character is
    actually moving through a 3D space.

[6] PERSONA FROM PERSONA PAGES (not invented) — pick from corpus-
    validated personas in wiki/entities/personas/. The 4 corpus-validated
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
         visible in 0-2s frame). Setting must signal the persona's
         authority type or the entire hook collapses. Corpus-grounded
         pairings (24-decode evidence):

         CLINICAL DOCTOR (dr-kim, dr-sage, dr-aesthetic, podiatrist,
         black-female-practitioner office-flip)
           -> required: T2 clinical exam room OR T2 diploma office
           -> required anchors: diploma (gold frame) / US flag /
              anatomy poster / equipment cart / exam stool / IV pole /
              surgical pen
           -> BREAKS if: filmed in domestic kitchen (MD authority dies)

         FOLK-WISDOM ELDER (master-chen kitchen, master-shen, old-earl,
         icelandic-elder, master-salvora kitchen-variant)
           -> required: T0/T1 honey-oak farm-bench OR rustic kitchen
           -> required anchors: honey-oak/barn-board wall + 3+ herb
              jars + ceramic teapot/copper kettle + window with
              outdoors + visible patina
           -> BREAKS if: sterile clinic ("50 years on the farm" dies)

         RETAIL-WITNESS OPERATOR (master-salvora Costco, master-shen
         Walmart)
           -> required: T0-retail Costco/Walmart aisle
           -> required anchors: store signage (yellow Costco / Walmart
              logo) + fluorescent industrial ceiling + blurred ordinary
              shoppers + actual store-stocked product
           -> BREAKS if: studio shot ("ordinary-shopper-with-secret"
              evaporates)

         CARIBBEAN HERBALIST (rastajahmeil, mama-rasta)
           -> required: T0/T1 Caribbean sunroom
           -> required anchors: bamboo wall + Rasta+US flags + 3+ herb
              jars on shelving + honey-oak table + warm amber light
           -> BREAKS if: cultural anchors removed (lineage-claim is
              VISUAL not narrative)

         MODERN-CLINIC SEXY-DOCTOR / KORELLA F-to-F-about-M
         (nuri, black-female-practitioner kitchen-flip)
           -> required: T0 clean kitchen for HOOK + T2 office for
              OUTRO (DUAL-FLIP both required)
           -> HOOK anchors: clean kitchen counter + window-soft light
              + warm domestic + persona kitchen-anchor pose
           -> OUTRO anchors: framed credentials + clean desk +
              window with skyline
           -> BREAKS if: single-setting reduction (kitchen-only OR
              office-only) — corpus shows BOTH required for trust

     Q7. SETTING RESONATES WITH AUDIENCE? Setting carries TWO signals:
         "the world I live in" (peer / kitchen-table / Costco aisle)
         OR "the credible expert space" (clinic / diploma office /
         herbalist sanctuary). Setting that achieves NEITHER =
         forgettable. DUAL-FLIP achieves both = highest trust.

         AUDIENCE x SETTING corpus matrix:
         - WOMEN 40+ -> kitchen + clinical exam-room DUAL-FLIP
           (kitchen-only = amateurish; office-only = pharmaceutical)
         - MALE 40-70 US (Mike) -> retail warehouse + clinic +
           luxury-apartment OUTRO (avoid pure home-kitchen — reads
           as wife giving advice)
         - F-to-F-about-M -> kitchen + office DUAL-FLIP (Korella
           saffron canonical, 4 corpus instances)
         - NEUTRAL/MIXED -> T2 clinical authority (Dr-Sage husband-
           skeptic exam-room -> office DUAL-FLIP)
         - BLACK WOMEN -> Caribbean sunroom + bamboo + cultural flags

         REJECT hook if:
         - Background not visible in HOOK (only persona-on-bokeh)
         - Background ambiguous (could be any room)
         - Background contradicts persona (clinical persona in kitchen
           / folk-elder in clinic / retail-witness in studio)
         - Background resonates with NO audience (luxury venue for
           cold-prostate Mike / sterile clinic for Caribbean-herbalist
           niche)
         - "Generic talking-head studio with bokeh" — zero authority,
           zero resonance, this is the failure mode of "When The Heat
           Hits At Night" Ogheci-bedroom hook.

     Korella menopause-saffron specific: pivot to the validated
     Korella F-to-F-about-M T0-kitchen-DUAL-FLIP-to-T2-office pattern
     (4 corpus instances). HOOK foreground = saffron threads in warm-
     water bowl + ripe banana on marble. HOOK background = clean
     domestic kitchen (warm light + window-soft + marble counter +
     Black-female-practitioner kitchen-anchor pose). OUTRO background
     = T2 office (diploma + US flag + clean desk). The hot-flash
     story-beat MOVES TO THE DIALOGUE ("when the heat wakes you at
     2 a.m., this is what your body is missing..."), but the visual
     frame is the validated saffron-prop kitchen -> office DUAL-FLIP.
     DO NOT default to "woman alone in dim bedroom" — fails Q1, Q2,
     Q3, Q6, Q7 (only Q4 and Q5 might pass).

     Q8. PSYCHOLOGY-MECHANISM STACK — name all 4 mechanisms BEFORE
         locking the hook. Anatomical compliance with Q1-Q7 (object +
         motion + bg-authority) is necessary but NOT SUFFICIENT. Every
         viral hook in the 24-decoded-corpus stacks 4 psychological
         mechanisms simultaneously. If ANY of the 4 is missing, the
         hook is psychologically dead even if Q1-Q7 pass.

         (i) SHAME-PROXY — what taboo object lets the viewer face the
             forbidden subject? Banana=penis, cabbage=visceral fat,
             distended belly=metabolism failure, mannequin tonsil=
             stones, soaked pillowcase=night-sweat suffering.
             The proxy CARRIES the shame so the viewer can engage.

         (ii) VIOLENT-ACT / SPECTACLE — what force-verb on the proxy
              creates a 0-2s shock moment? SLAM / RIP / SCATTER /
              SPRAY / GRIND / CASCADE / SHATTER / BLAST. NOT a gentle
              gesture. Visible damage or dramatic state-change. If
              the action is "thumb gently compresses saffron threads"
              the hook is dead — pivot to "pillowcase SLAMMED on
              marble + water spray-arc."

         (iii) AGENT-OF-CHANGE SPECTACLE — does the product (or recipe
               ingredient) ENTER FRAME and visibly ACT? Not just sit
               there — DISSOLVE / UNFOLD / CASCADE / SPRAY / IGNITE /
               TRANSFORM. Saffron capsule cascade with red streaks
               bleeding through liquid. Tea pour with fat melting on
               torso. Onion juice spray with hair matting. Saffron
               threads UNFOLD into gold-amber tendrils in spatter
               water. The product literally acts in-frame.

         (iv) TABOO DIRECT-ADDRESS + BYSTANDER/WITNESS — does the
              persona break the fourth wall with a forbidden statement
              invoking a third-party witness? "Don't show this to your
              man too often" + male-audience implicit. "Her husband
              did not believe me" + husband-skeptic on-screen. "Your
              husband sleeps through this" + sleeping-husband-
              silhouette in bedroom doorway. The bystander mediates
              the shame and creates conspiratorial register.

         REQUIRED LLM AUTHORING STEP — write this stack out explicitly
         in your working draft (not in the final markdown):

           ## Psychology stack — HOOK
           - Shame-proxy: <object> = <forbidden subject>
           - Violent-act: <force-verb> on <object> creating <spectacle>
           - Agent-of-change: <product/ingredient> visibly <action> in-frame
           - Taboo direct-address: "<line>" + <bystander/witness>

         If you cannot fill any one of the four lines, the hook is
         dead. Replace the prop, replace the motion, replace the line,
         or pull a different corpus pattern.

         Q8 fails most often when Q1-Q7 pass with "gentle gesture +
         clean kitchen + soft saffron pinch" — that's the
         psychologically-dead trap. Spectacle requires VIOLENCE +
         SHAME + AGENT + TABOO, not just objects + motion.

     If ANY of Q1-Q8 fail, REJECT the hook and propose 3 alternatives
     pulling from the corpus surrogate library in template_reference.md
     §"Corpus-grounded surrogate library" + persona x setting
     authority pairings + Q8 mechanism stack templates before emitting
     the videos/*.md.

[12] V599 PRODUCT-PRESENCE + LLM-OMISSION AUDIT — every image where
     the product is visible/named in voiceover MUST have all 3 v581
     binding parts present (LLMs typically include 1, sometimes 2,
     almost never all 3). Plus the broader LLM-omission audit.

     [A] INGREDIENTS TABLE present? (## Ingredients between ## Sources
         and ## Storyboard) Two rows when product is bound (persona +
         product). Product row's "Name" column value matches VERBATIM
         the product_image: field values used throughout the file.
         Example: table says "the Corella saffron bottle" -> all
         product_image: fields say EXACTLY "the Corella saffron bottle"
         (not "Corella saffron" / not "saffron bottle" / not
         "saffron"). Mismatch = silent platform binding failure.

     [B] V581 3-PART PRODUCT BINDING — on every image where product
         is visible OR named in voiceover, ALL THREE present:
           1. product_image: <ingredient-name> field set in image
              metadata block
           2. Product binding line at top of fenced Image prompt body
              (line 2, after persona binding) — v609 CONCISE FORM:
                "Use the uploaded product reference image for <name>."
              NOT the verbose pre-v609 form ending in
              "— match its label, packaging, color, and proportions
              exactly." (redundant — Banana 2 auto-matches).
           3. Product visual described in prompt body composition
              ("label-forward to camera", "wordmark squared to lens",
              "stands upright on counter to the left of the glass")

     [C] PER-SCENE PRODUCT-PRESENCE MATRIX (corpus-grounded for
         9-scene Korella saffron-style):
           - HOOK Scenes 1-2 (0-8s): product NOT visible. Use shame-
             proxy from Q8 (banana, soaked pillowcase, distended
             belly). Burn the curiosity loop FIRST. The 4 corella
             saffron videos all delay product reveal.
           - RECIPE early scenes (lemon-pour, ginger-pinch, honey-
             cascade): product NOT visible yet — building the drink.
           - RECIPE product-reveal scene (typically scene 6 of 9):
             product CASCADES into the recipe — capsule cascade,
             threads pour, bottle stands on counter label-forward.
             This is the climactic agent-of-change moment per Q8.
           - EXPLAIN scene 7: product bottle visible, label readable.
             Authority-transfer moment.
           - OUTRO scene 8: product bottle held at chest height,
             label-forward, hero-shot.
           - OUTRO scene 9 (CTA): product bottle in one hand + CTA
             finger gesture from the other.

     [D] PERSONA-POSE-TO-CAMERA LOCK — every scene's action_note
         specifies "eyes locked to lens" or "eyes locked to camera"
         or equivalent. Every viral video in the corpus has direct-
         eye-contact lock. Talking-to-side / eyes-on-product-only
         FAILS.

     [E] V577 WORD BUDGET PER SCENE — every `- **line:**` value
         ≤21 words ±2. If 24+ words, SPLIT into multi-line scene
         (two `- **line:**` + `- **action_note:**` pairs in one
         Scene block per v593).

     [F] UNIVERSAL CLOSER — final scene's `- **line:**` ends with
         "follow me first or I can't reach you" or close variant.

     [G] DAY1/14 ANCHOR — if niche is DAY1/14 transformation (back
         lump, hair regrowth, varicose, sagging skin, body lump),
         "$X surgery you didn't pay" anchor must appear in EXPLAIN
         scene ($8000 / $3000 / $4000 / $10000 documented in
         strategy-mechanisms.md).

     [H] FILE STRUCTURE — YAML frontmatter at top (persona/niche/
         audience/cell), `## Sources` section near end with raw-file
         citations, `## Used in` section at end (placeholder).

     If ANY of [A]-[H] fail, FIX before emitting. The operator will
     re-prompt you to fix violations otherwise. Self-correction here
     saves a round-trip.

[13] V600 EXAGGERATION-MAGNITUDE GATE — cartoon-physics or boring.

     The 24-decoded-corpus is built on MAGNITUDE THAT EXCEEDS REALITY.
     Real-life magnitude = scroll-by. Cartoon-physics magnitude =
     scroll-stop. The corpus pattern: multiple cascading force-verbs
     + specific exaggerated quantities + visible effects that exceed
     physics.

     This is what salvora-banana actually does:
       RIPS banana off bunch -> SLAMS bunch back -> FULL PYRAMID
       COLLAPSE -> neighbors TUMBLE (4 verbs, exaggerated quantity)

     This is what dr_kim_hair_regrowth actually does:
       SLAMS onion cut-side DOWN onto CROWN -> cut-face FLATTENS ->
       juice SPRAYS 3-4 droplets -> GRIND clockwise -> juice runs
       2-3 streams through hair (5 verbs, specific exaggerated
       quantities)

     This is what dr_kim_cockroach_bait actually does:
       LEFT pinched-fingers -> VIOLENT VERTICAL SUGAR JET -> BLAST
       into glass -> ATOMIZE mist -> RICOCHET particles -> cockroach
       ENGULFED (6 verbs, physics-violating cascade)

     If your action_note is "she compresses the saffron threads" or
     "she places the pillow on the counter" — that's REALISTIC. It's
     dead. Cartoon-physics or boring.

     Three sub-tests for the HOOK + any spectacle scene (RECIPE
     product-reveal, EXPLAIN demo). All 3 must pass:

     Q9a. PROP POSITION / SIZE / QUANTITY exaggerated past realism?
          - Real: pillow at counter level. Viral: pillow HELD HIGH
            OVERHEAD with both arms fully extended.
          - Real: one drop of juice. Viral: 3-4 SPRAYING droplets
            specified.
          - Real: a banana off the bunch. Viral: the WHOLE PYRAMID
            COLLAPSING.
          - Real: a single saffron thread. Viral: a CASCADE of
            saffron threads pouring like a waterfall.
          Scale up the prop's position/size/quantity by 2-3x past
          what a real person would do. Specify quantity + position
          explicitly in prompt body and action_note.

     Q9b. VISIBLE EFFECT PRE-IMPACT? The wind-up frame must show
          MAGNITUDE BEFORE THE IMPACT. Cascading sweat / dripping
          juice / scattering particles / spraying liquid ALREADY in
          motion BEFORE the climactic moment.
          - Real: pillow lifts then lands. Viral: pillow OVERHEAD
            with sweat-water STREAMS POURING DOWN her bare forearms
            in 3 visible rivulets, hair dripping, camisole already
            wet — wind-up captures the magnitude.
          - Real: hand grinds onion on scalp. Viral: onion held
            aloft cut-side-down with juice ALREADY DRIPPING in
            pre-cascade visible streams before contact.
          - Real: cabbage placed on counter. Viral: cabbage held
            high in two hands with leaves ALREADY peeling outward
            in pre-fall motion before the SLAM.
          - Real: saffron sprinkled. Viral: saffron CASCADE held
            aloft with threads ALREADY in mid-fall streaming
            downward as a visible curtain before they hit the water.
          Capture the wind-up moment with magnitude pre-impact.

     Q9c. CASCADING FORCE-VERBS — 3+ verbs in sequence in the
          action_note?
          Verb library by spectacle type:
            FORCE-ON-PROP: LIFT -> SLAM -> SCATTER -> COLLAPSE -> SETTLE
            LIQUID AGENT: LIFT -> POUR -> SPRAY -> CASCADE -> BLEED -> DISSOLVE
            PRESSURE: TRIGGER -> BLAST -> ATOMIZE -> SCATTER -> ENGULF
            BODY-ANATOMY: POINT -> TRACE -> CARVE -> MARK -> REVEAL
            SURGICAL: LOWER -> PRESS -> TRACE -> LIFT -> ANGLE
            WIND-UP IMPACT: RAISE -> WIND-UP -> SMACK / THROW -> SPLATTER -> SPRAY -> DRIP
          Single-verb action_note = realistic = boring. Pull from
          the verb library and chain 3+ in temporal sequence with
          each verb's visible effect specified.

     If your hook is "lady at counter doing soft saffron pinch" —
     Q9 fails. Pivot to "pillowcase HELD HIGH OVERHEAD with sweat
     STREAMING DOWN her forearms while she WINDS UP to SMACK it
     onto the marble" — Q9 passes.

     v600 applies to: HOOK Scenes 1-2 always; RECIPE product-reveal
     scene (saffron CASCADE / capsule POUR); EXPLAIN scene if it
     includes a demonstration. Does NOT apply to talking-head
     CTA/OUTRO (those need authority-pose not spectacle) or Day-1
     frame of Day-1/Day-14 transformation (the "before" is
     deliberately real-life).

[14] V601 HEALER-PATIENT ACTIVE-INTERACTION RULE — when a patient
     appears as evidence of a symptom, healer must ACTIVELY INTERACT
     with the symptom-area via clinical-authority hand-actions.
     Healer's HANDS DOING SOMETHING TO THE PATIENT is what transfers
     clinical authority to the viewer.

     Side-by-side seated composition with patient holding the product
     reads as "two friends" not "doctor and patient." Trust-transfer
     fails. v601 fixes this.

     V601 APPLIES TO BOTH THE HOOK AND THE EXPLAIN SCENE FOR SYMPTOM-
     DEMO VIDEO TYPES — not just EXPLAIN. The corpus opens
     symptom-demo videos with the healer demonstrating the symptom
     on the patient at peak magnitude (varicose decode HOOK = gloved-
     finger POINT + MOVE-IN + hand-opens-toward-swelling; back-lump
     HOOK = surgical-marker LOWER + PRESS + TRACE curved line; belly
     HOOK = RIGHT-index TAP + dismissive flick on distended belly).
     The HOOK IS the diagnostic-shock moment.

     Video-type decision tree — which HOOK pattern to pick:

       SYMPTOM-DEMO video (niche has body issue audience identifies
       with on a patient — belly fat, varicose, back lump, hair loss,
       tonsil stones, OR invisible-via-instrument like menopause/
       hot-flash, anxiety, insomnia, hormone)
         -> v601 APPLIES — patient in HOOK with active healer demo
         -> Corpus: dr_kim_belly_burn_male, decoded_varicose_vein,
            dr_kim_back_lump, dr_kim_hair_regrowth, healthylifesage,
            blood_sugar_v584

       RECIPE-FORWARD video (niche is vitality/performance/energy
       where audience wants a result not a diagnosis — Korella
       saffron-vitality F-to-F-about-M canonical, master-chen
       probiotic, master-salvora costco)
         -> v601 does NOT apply
         -> Persona alone with prop + dialogue promise + force-verb
            spectacle. NO patient in HOOK.
         -> Corpus: corella_saffron_v578/v581, saffron_male_v577,
            saffron_vitality_v577, master_chen_three_things,
            master_salvora_costco_banana

       TRANSFORMATION video (Day-1/Day-14 same-body before/after)
         -> v601 does NOT apply (body transformation IS proof,
            separate v-rule pending)

       RECIPE-ONLY video (no patient, no symptom)
         -> v601 does NOT apply (recipe demo carries authority)

     HYBRID NICHES (menopause-saffron, hot-flash-vitality, anxiety-
     saffron) can go either way — corpus default for Korella saffron
     F-to-F-about-M is RECIPE-FORWARD (4-corpus instances), but if
     operator wants CLINICAL-AUTHORITY register with husband-
     bystander shame-mediation, use SYMPTOM-DEMO HOOK.

     V601 IN THE HOOK (SYMPTOM-DEMO):
       Scene 1 — PRESENT + APPLY phase. Healer actively demonstrates
         symptom on patient's body via clinical-authority hand-action.
         Reading climbs, finding lands.
       Scene 2 — REVEAL phase. Healer LIFTS instrument away, TURNS
         to camera with finding, GESTURES toward corrective (saffron
         / anatomy / mechanism), transitions to RECIPE.

     V601 IN THE EXPLAIN (SYMPTOM-DEMO):
       Scene 7 — patient may reappear (callback to HOOK with relieved/
         improved appearance) OR persona alone with anatomy poster
         active gesture. Both valid.

     V600 MAGNITUDE EXPRESSION VARIES BY HOOK TYPE:
       RECIPE-FORWARD HOOK: cartoon-physics SLAM + cascading force-
         verbs on the prop (banana-pyramid-COLLAPSE, salmon-CLEAVER-
         SWING, sugar-JET-ATOMIZE-RICOCHET).
       SYMPTOM-DEMO HOOK: NOT cartoon-physics SLAM. Magnitude is in
         the AUTHORITY of the diagnostic moment — display GLOWS
         red-warning at unusual reading (99.4F, 91% O2-sat) + patient's
         visible reaction (eyebrow LIFTS, eyes flick to instrument,
         breath catches) + healer's clinical-finding emphasis (brows
         raised, gesture-finger pointing AT reading at peak emphasis).
         Cascading verbs still required (8+ verbs in action_note).

     Visible vs invisible symptoms — two paths:

     A. EXTERNALLY VISIBLE SYMPTOM (belly fat, varicose veins,
        back lump, hair loss, tonsil stones, scars, body-volume):
        Healer's hands act DIRECTLY on the symptom:
        - POINT — gloved-finger pointing AT visible symptom
        - TAP — index-finger TAP on the symptom
        - TRACE — surgical-marker TRACING contour around symptom
        - MARK — pen-mark documentation on symptom (tick mark)
        - GESTURE-TOWARD — gloved hand opening toward symptom area
        - PRESS — finger-press for palpation demonstration
        - PALPATE — multi-finger pressure for visible response

     B. NOT-EXTERNALLY-VISIBLE SYMPTOM (menopause/hot-flash,
        anxiety, insomnia, brain fog, vitality, sleep, hormone):
        Healer must MANUFACTURE a clinical demonstration via a
        wearable evidence INSTRUMENT producing a visible measurement
        reading the camera can read:
        - Digital infrared thermometer pressed to forehead -> reading
          visible (elevated for hot-flash)
        - 2 fingertips on patient's wrist for pulse-check -> count
          visibly via lip-movement / wristwatch glance
        - Fingertip pulse-oximeter on index finger -> reading display
          visible (oxygen sat, heart rate)
        - Smartwatch / sleep-tracker held next to patient's wrist ->
          screen visible with sleep-quality / HRV / hot-flash log
        - Dermatome / heat-strip on neck -> color-shift visible
        - BP cuff on arm -> reading visible
        The instrument creates an external proxy for the internal
        symptom. The reading on the screen is the diagnostic moment.

     The 3-part active-interaction structure for the EXPLAIN scene:

     1. PRESENT (0-2s) — healer LIFTS the instrument / RAISES her
        hand / GLOVES UP / POSITIONS the prop. Signals "clinical
        action incoming."
     2. APPLY (2-4s) — healer PRESSES / POINTS / TAPS / TRACES /
        MARKS / PALPATES the symptom-area on the patient's body.
        Active moment. Hand makes contact with patient or
        symptom-area.
     3. REVEAL (4-6s) — healer LIFTS fingers / TURNS to camera with
        finding / POINTS at the reading / GESTURES toward the
        patient's symptom for the reveal moment. Diagnostic
        conclusion lands on viewer.

     Maps to v586 action_note 3 timed beats: [Start beat 0-2s]
     PRESENT, [Mid-clip beat 2-4s] APPLY, [End beat 4-6s] REVEAL.

     Compositional rule:
     - Healer remains the CLINICAL-AUTHORITY FIGURE throughout
     - Patient is the EVIDENCE-PROVIDER, not the subject of
       explanation
     - Viewer aligns with healer + identifies with patient
     - Patient seated on exam-couch / clinic-chair, body turned
       slightly toward healer, symptom-area exposed or accessible
     - Healer STANDING or LEANING beside the patient (NOT seated
       next to them), body angled toward the symptom-area
     - Camera at chest-up two-shot (or three-shot if bystander
       present per F-to-F-about-M husband-skeptic pattern)

     ANTI-PATTERN: healer and patient seated side-by-side at a
     desk both facing camera at parallel angles patient holding
     the product healer talking. This reads as "two friends" not
     "doctor and patient." The drink-handover composition is OK
     in OUTRO scenes 8-9 (product hero-shot anchors authority via
     the bottle) but in the EXPLAIN scene the trust-transfer must
     come from active demonstration.

     The varicose-vein decode shows the principle viscerally: the
     gloved finger POINTING AT the calf vein-cluster is what makes
     the viewer think "she sees what's wrong with my legs." A
     healer holding a drink next to a patient does not produce
     that thought.

     We use rules not lists. Apply the active-interaction principle
     + 3-part structure to whatever niche you're in; derive the
     specific action (POINT / TAP / TRACE / MARK / thermometer /
     pulse-check / pulse-ox / etc.) from the niche context.

[15] V602 PERSONA BODY-PROSE GENERIC-REFERENCE RULE — the persona's
     identity comes from the uploaded reference image (v581 binding),
     NOT from body prose. Body prose must reference the persona
     using the generic alias from the v581 binding line ("the main
     character" by default, or whatever alias the Ingredients table
     declares verbatim).

     FORBIDDEN in body prose (these are upload-authoritative):
     - Persona-archetype labels: "Black-female-practitioner persona",
       "Asian-elder-herbalist", "modern-clinic-doctor", "Caribbean
       herbalist", "folk-wisdom elder"
     - Ethnic / racial descriptors: "Black", "Asian", "Caribbean",
       "Mediterranean", "Hispanic", "European"
     - Age descriptors: "late-30s", "early-40s", "60s", "mid-50s"
     - Hair color / style identity: "dark curly hair", "long grey
       dreadlocks", "salt-and-pepper beard"
     - Facial feature identity: "almond eyes", "Fu Manchu mustache",
       "olive skin"
     - Body type identity: "tall", "slim", "broad-shouldered"
     - Permanent-wardrobe identity: items the persona wears in EVERY
       scene per the upload (always-on stethoscope, always-on
       Rasta tam, etc.)

     ALLOWED in body prose (scene-specific, not identity):
     - Pose: "STANDING beside", "seated on exam-chair", "torso
       angled toward"
     - Clothing IF non-default for THIS scene
     - Facial expression: "brows raised", "mouth open mid-snarl"
     - Body language: "body weight forward", "shoulders torqued"
     - Active gesture: "RIGHT hand presses thermometer to patient's
       temple", "LEFT hand POINTS at reading"
     - Eye-contact / gaze direction: "eyes locked to camera"
     - Hair STYLING for this scene (not color/texture identity):
       "hair pulled loosely back"
     - Sweat / skin condition for this scene: "faint sweat-sheen
       at temples"

     RULE applies to BOTH the fenced Image prompt body AND the
     scene action_note body. Both are sent to Banana 2 / Veo
     respectively.

     PER GOOGLE'S OFFICIAL GEMINI NANO BANANA 2 DOCS — the
     recommended multi-image prompt format uses semantic descriptors
     like "the dress from input 1" / "the model from input 2" —
     NOT redescription of identity. Identity = upload + binding line;
     prose = scene composition + physical action.

     When body prose redescribes identity, Banana 2 receives two
     competing identity signals (upload + prose) and produces
     identity-drift across the image set. Subtle features waver
     between scenes. Visual consistency collapses.

     MULTI-CHARACTER SCENES — bystander / patient exception:
     - PERSONA WITH UPLOAD (v581 binding) -> use "the main character"
       / declared alias generically; identity from upload
     - BYSTANDER / PATIENT WITHOUT UPLOAD -> DESCRIBE with prose
       since no upload exists, prose IS the identity source.
       "A late-40s female patient in a soft beige knit cardigan" is
       FINE because the patient has no upload binding.

     SAME RULE FOR PRODUCTS — v581 product binding makes the product
     upload authoritative for label, packaging, wordmark, color,
     proportions. Body prose for product:
       ALLOWED: position ("stands UPRIGHT on counter", "label-forward
         to camera", "wordmark squared to lens")
       FORBIDDEN: re-describing label color, wordmark fonts, bottle
         shape, proportions — upload-authoritative

     PRE-OUTPUT VALIDATION: scan all Image prompt bodies and scene
     action_notes for persona-archetype labels / ethnic descriptors
     / age ranges / hair-color-texture redescription. If found ->
     REPLACE with generic alias from v581 binding line.

[16] V603 STYLE LOCK + PROSE DISCIPLINE — corpus iPhone-UGC aesthetic.

     Every Image prompt body MUST include this exact opener style
     anchor:

       "Shot on iPhone wide-angle lens, handheld, deep focus
        throughout, natural daylight"

     Every Image prompt MUST close with this exact tag:

       "Natural ultra-realistic colors, deep focus."

     Without these two anchors, Banana 2 defaults to studio-clean
     aesthetic. Corpus uses UGC iPhone-handheld aesthetic. Different
     look. Style-lock = mandatory.

     PROSE DISCIPLINE — 4-7 sentences per Image prompt body
     (after the persona/product/chain binding lines). Corpus
     reference videos/nuri-saffron-ed-anatomy-clinic.md averages
     4-5 sentences per image. Target 4-7. Each sentence carries
     one of:
       1. Setting + style lock + framing distance
       2. Subject + props + composition
       3. Active gesture + body language
       4. Eye-contact + facial expression
       5. Closing style tag

     CUT from prompt bodies:
     - Rule citations: "per v601", "per v585", "per v600 SYMPTOM-DEMO"
       (these are author-side notes, not Banana 2 instructions —
       move to YAML frontmatter)
     - Cinematography jargon: "1/500-sec sharpness", "motion-frozen
       at peak emphasis", "captured at the WIND-UP APEX" (Banana 2
       generates photographs, not action-frames — these confuse it)
     - Meta-commentary: "V601 SYMPTOM-DEMO HOOK — captured at the
       APPLY moment of an active diagnostic..." (Banana 2 reads
       this as competing instructions)
     - Excess setting redescription: state setting once, lock with
       style anchor, move on

     CONCRETE FRAMING DISTANCE + CROP required:
     - Camera distance: "camera approximately one arm's length",
       "camera approximately 4 feet", "camera approximately 6 feet"
     - Explicit crop: "head and upper chest filling the upper
       two-thirds of the frame", "shoulders spanning frame width",
       "cropped at mid-thigh, NO floor visible, NO feet visible"
     - The "NO floor / NO feet" instruction is universal in corpus —
       forces tight headroom

     ACTIVE-GESTURE + FACIAL-EXPRESSION discipline:
     - One sentence on dominant hand-action verb + visible result
       (corpus: "Her right hand grips a fresh half-lemon mid-squeeze
        directly above the glass, golden droplets visibly streaming
        down into the water, fingers tightening, knuckles whitening")
     - One sentence on facial expression + eye-contact + brow emphasis
       (corpus: "Mouth open mid-word, eyes locked to camera then
        briefly down to the glass at mid-clip, brows in claim-delivery
        emphasis")
     - NOT three paragraphs describing every body angle, weight
       shift, hip rotation, vein visibility

     PRE-OUTPUT VALIDATION before emitting any Image prompt:
     - YES opener style lock present?
     - YES closing tag present?
     - YES concrete camera distance?
     - YES explicit crop with NO-floor / NO-feet?
     - YES body prose 4-7 sentences?
     - NO rule citations in body?
     - NO cinematography jargon?
     - NO meta-commentary?

     If any answer wrong, REMOVE/FIX before emitting.

[17] V604 UNIVERSAL PROMPT-DISCIPLINE (4 sub-rules):

     [a] IMAGE PROMPT = STILL FRAME ONLY. Motion goes ONLY in
         action_note. No motion verbs in image prompt body
         ("captured at", "frozen at", "mid-action", "PIVOTING from",
         "mid-rotation", "captured at the APPLY moment"). Banana 2
         generates photographs, not action frames — mixed motion
         makes it invent weird poses.

         FORBIDDEN in image prompt body:
           - "her right hand is captured mid-action GRIPPING"
           - "PIVOTING from patient toward camera"
           - "frozen at the apex of a wind-up motion"
           - "captured at the APPLY moment"

         ALLOWED in image prompt body:
           - "right hand presses thermometer to right temple"
             (verb is "presses", static pose)
           - "stands beside the seated patient"

     [b] CAMERA LOCK SPECIFICITY beyond v603 style line. The generic
         "Shot on iPhone wide-angle lens, handheld, deep focus
         throughout, natural daylight" is necessary but
         not sufficient. Per-video, also lock:
           - vertical or horizontal aspect?
           - tripod or stable handheld?
           - exact framing crop (chest-up / head-and-shoulders)
           - camera height (above desk / at eye level / low-angle)
           - subject position (face centered upper half)
           - what's at frame bottom (desk edge / counter / floor)
           - background characteristics (warm wood / white clinical /
             honey-oak / Caribbean bamboo)

         Generic style line alone can produce a different room.
         Lock the actual setting anchors per video.

     [c] DRIFT-GUARD CONSTRAINT DISCIPLINE (REVISED 2026-06-12 —
         fold into sentences, NO trailing DO-NOT block). The prompt
         ENDS at "Aspect ratio 9:16." — nothing after it; the
         platform appends standardized negatives automatically.
         Weave the anti-drift constraints INTO the body sentences
         (inline "no X" within a sentence is fine; a labeled
         trailing list is not). Adapt to niche/persona.

         Example for T0 kitchen Korella saffron-vitality (body
         sentence): "The kitchen has warm honey-oak shelving and
         ceramic vessels clearly visible — a lived-in domestic
         space, no clinical elements, no lab coat, no medical
         equipment anywhere."

         Example for T2 continuity (fold into the chain sentence):
         "...same room, same furniture, same lighting as the prior
         scene — nothing in the background changes."

         Format: scene description (drift-guards woven in) ->
         closing v603 tag "Natural ultra-realistic colors, deep
         focus." -> final line "Aspect ratio 9:16."

     [d] VIEWER-LEFT / VIEWER-RIGHT convention. Generators confuse
         "left" and "right" (subject-perspective vs frame-
         perspective). Always use "viewer-left" and "viewer-right"
         to anchor to camera POV.

         FORBIDDEN: "her left hand POINTS at the reading"
         REQUIRED:  "her gloved hand on the viewer-left side POINTS"

         FORBIDDEN: "the bottle stands to the left of the glass"
         REQUIRED:  "the bottle stands on the viewer-left side of
                     the glass"

         Universal — applies to image prompts, action_notes, and
         visual_delta descriptions.

     PRE-OUTPUT VALIDATION:
       NO motion verbs in image prompt body?
       YES camera lock specificity beyond generic v603 line?
       YES drift-guard constraints woven into body sentences (NO
         trailing DO-NOT block; prompt ends at "Aspect ratio 9:16.")?
       NO bare "left" / "right" — replaced with "viewer-left" /
         "viewer-right"?

[18] V605 PROP-TRACKING + PROP-AS-SUBJECT (product-reveal scenes):

     For every Image with product_image: field set, the body prose
     MUST be PROP-LED, not persona-led. The prop is the subject of
     the photograph; persona is secondary anchor.

     Required: prop named in the FIRST SENTENCE of body prose.

     PROP-LED format example:
       "The Korella saffron bottle is held up at chest height in
        her viewer-right hand, presented directly toward the lens,
        label-forward, navy-and-cream wordmark squared to lens.
        Her viewer-left hand gestures next to the bottle for
        emphasis. She is seated at her clinic desk with eyes locked
        to camera, expression warm and authoritative."

     PERSONA-LED format (FORBIDDEN for product-reveal scenes):
       "The main character is seated at her clinic desk, eyes
        locked to camera, expression warm. The Korella saffron
        bottle is on the desk in front of her, label visible."

     Description allocation:
       - 60% on prop handling (how held / manipulated / positioned /
         presented; hands relative to product, label orientation,
         height, lighting on bottle)
       - 40% on persona pose (eye-contact, body language, expression)

     For NON-product-reveal scenes (HOOK with no bottle, recipe-
     prep before product cascade, EXPLAIN with no bottle), standard
     v603 prose discipline applies — no specific allocation.

     ANTI-TEMPLATE-BIAS for create-from-zero:
     When authoring a new video from a corpus-validated cell, do
     NOT default to "bottle on desk because nuri-saffron does it"
     unless that's the actual composition you're scripting. State
     prop_position explicitly:

       - **prop_position:** held in viewer-left hand at chest
           height, label-forward (or "stands upright on desk
           viewer-right side, label-forward to camera")

     Either way, decide the position consciously and write it as
     a prop_position: field in the image metadata block.

     PRE-OUTPUT VALIDATION:
       YES product-reveal images have prop_position: field?
       YES body prose is PROP-LED (prop in first sentence)?
       YES ~60% allocation to prop handling, ~40% to persona?
       NO corpus-template defaults invoked as source of prop
          placement?

[19] V606 PRODUCT COMPOSITING / LIGHTING INTEGRATION — make the
     product melt into the scene, not look photoshopped.

     Default Banana 2 behavior with an uploaded product reference:
     places the bottle at PRODUCT-SHOT scale (oversized) with its
     own product-shot LIGHTING (self-lit), HARD cut-and-paste edges,
     CENTER-STAGE composition, no cast shadow, no foreground
     occlusion. This produces the photoshopped look every time.

     v606 = 6 mandatory compositing directives in every Image prompt
     body that has product_image: field set. All 6 required.

     [a] SCALE ANCHOR — realistic real-world size relative to scene
         FORBIDDEN: "label-forward to camera" alone (no scale)
         REQUIRED: "the bottle is shown at realistic supplement-
                    bottle scale, approximately 5 inches tall"
                   "bottle's height is approximately 1/4 of persona's
                    torso width"
                   "sized so it would fit naturally in palm"

     [b] LIGHTING INTEGRATION — match scene light source + color temp
         FORBIDDEN: "label clearly readable", "wordmark squared to
                    lens" without a lighting anchor (these read as
                    product-shot directives)
         REQUIRED: "the bottle is lit by the same warm window-soft
                    daylight as the rest of the kitchen — no
                    dedicated product-shot lighting"
                   "the bottle's surface picks up the cool-clinical
                    LED ambient — slight cool-white highlights on
                    the cap, label colors subtly desaturated to
                    match the muted clinical palette"

     [c] CAST SHADOW — explicit shadow on the surface
         FORBIDDEN: bottle described without any shadow
         REQUIRED: "the bottle's base casts a soft natural cast
                    shadow on the desk surface, falling viewer-right
                    at a 30-degree angle, matching the room's window
                    light from camera-left"

     [d] PERSPECTIVE INTEGRATION — match scene camera angle
         FORBIDDEN: "label-forward to camera" / "wordmark squared to
                    lens" without a perspective anchor
         REQUIRED: "the bottle is shot from the same camera angle as
                    the rest of the scene (slightly above desk-eye-
                    level), so the label is angled slightly upward
                    toward camera with the cap visible at the top"

     [e] SURFACE CONTACT / GRIP — physical placement, no floating
         FORBIDDEN: bottle simply "on the counter" or "in her hand"
                    without contact specifics
         REQUIRED: "the bottle's base sits flush on the wooden desk
                    surface, in clear physical contact, no floating
                    gap"
                   "gripped firmly in her viewer-left hand, fingers
                    visibly wrapping the cylindrical body, thumb on
                    the cap top, palm in contact with bottle's
                    lower third"

     [f] NATURAL OCCLUSION — foreground breaks the silhouette
         FORBIDDEN: bottle as dead-center hero with nothing in front
         REQUIRED: "the persona's gesturing hand on the viewer-left
                    side partially crosses in front of the bottle's
                    upper third, breaking the silhouette so the
                    bottle reads as naturally placed in the workspace"
                   "the bottle is partially behind the wooden cutting
                    board in the foreground, breaking the silhouette"
                   "bottom of bottle partially behind desk edge in
                    immediate foreground"

     COMPOSITING PARAGRAPH FORMAT — final descriptive paragraph,
     right before the v603 closing tag + "Aspect ratio 9:16." line
     (REVISED 2026-06-12: no trailing negatives block; the [a]-[f]
     directives themselves carry the anti-photoshop guards, stated
     inside the paragraph):

       [scene description with persona, props, framing, action]

       The bottle integrates naturally with the scene: [a] realistic
       supplement-bottle scale (~5 inches tall), [b] lit by the same
       [scene lighting] as the room with no dedicated product-shot
       lighting, label colors matching the room's color temperature,
       [c] base [contact-point] with a soft natural cast shadow
       [direction + length], [d] perspective matching the scene's
       [camera angle], [e] [grip or surface-contact detail — firm
       physical contact, no floating gap], [f] partially occluded by
       [foreground element] breaking the silhouette — integrated
       into the scene, not a center-stage product shot.

       Natural ultra-realistic colors, deep focus. Aspect ratio 9:16.

     PRE-OUTPUT VALIDATION:
       YES [a] scale anchor present?
       YES [b] lighting integration present (lit by scene source)?
       YES [c] cast shadow direction + softness + length stated?
       YES [d] perspective matches scene camera angle?
       YES [e] surface contact / grip explicit?
       YES [f] natural foreground occlusion present?
       YES all six guards stated inside the compositing paragraph
         (NO trailing negatives block — retired 2026-06-12)?

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
         "The main character pivots from the patient toward camera,
         their right hand sweeping in a wide gesture-arc..."
       Pronoun-free body-part subject:
         "The right hand sweeps in a wide gesture-arc..."

     NOT AFFECTED:
       - Dialogue lines (`- **line:**`) — the persona's spoken
         words can use any gendered language naturally.
       - Other characters in the scene (patient, husband bystander,
         customer) — their identity IS the prose, so gendered
         pronouns are fine for them.
       - Persona's name in dialogue (e.g. "I'm Dr. Amara") — names
         in dialogue are persona content, not visual claims.

     PRE-OUTPUT VALIDATION:
       YES Zero instances of \\bshe\\b, \\bhe\\b, \\bher\\b, \\bhis\\b,
           \\bhim\\b, \\bhers\\b, \\bshe's\\b, \\bhe's\\b in image-prompt
           bodies and action_notes referring to the main character?
       YES Persona references use role descriptor / singular-they /
           pronoun-free constructions?
       YES Other-character pronouns (patient / bystander / customer)
           unchanged?

[22] V613 PRODUCT-MENTION-BINDING PARITY + CORPUS-GROUNDING — script
     must come from corpus, every product reference must be bound.

     v613a — PRODUCT-MENTION PARITY (mechanical):
       For every Image N where the prompt body, action_note, or any
       scene line pointing to image N contains a product term (any
       ingredient name with type: product from the Ingredients table,
       OR brand keywords like "Korella", "saffron bottle", "saffron
       capsule", "Rosabella"), the image MUST have product_image:
       <exact-ingredient-name> set.

       CONVERSELY for HOOK images (scenes 1-2) and RECIPE-early
       images (lemon-pour, ginger-pinch) per the v599 matrix, the
       prompt body MUST NOT contain any product visual mention. Use
       a non-product placeholder ("clean cream-tone counter behind",
       "a small terracotta basil pot"). The product is REVEALED at
       scene 6 (RECIPE product-cascade) — earlier visibility burns
       the curiosity loop pre-scene-6.

       FORBIDDEN (HOOK image with product mention but no binding):
         "Bright modern clinical exam room interior with [...] and
         a Korella saffron bottle standing label-forward on the
         counter behind."
         (product visible in HOOK = v599 matrix violation; no
         product_image field = v613a parity violation; Banana 2
         invents a generic supplement.)

       REQUIRED (HOOK with product reveal delayed):
         "Bright modern clinical exam room interior with [...] and
         a clean cream-tone counter behind (no product visible —
         HOOK burns the curiosity loop before the product reveal in
         scene 6)."

     v613b — CORPUS-GROUNDING (declared at top of every videos/*.md):
       The video frontmatter or ## Sources block MUST cite at minimum:

       1. ≥2 specific raw/decoded files this script adapts from, with
          parenthetical pattern label:
             raw/dr_kim_belly_burn_male_decoded.md (clinical-authority
                HOOK pattern)
             raw/decoded_corella_saffron_blood_sugar_v584.md (podiatrist
                + patient active diagnostic)
       2. The niche voiceover-script wiki page —
          wiki/voiceover-scripts/<niche>.md — corpus-distilled hook
          library for the niche. The HOOK line should adapt one of
          the listed Opening line entries from that page's ## Hooks
          table.
       3. A "NOTE on cell honesty" — explicit declaration of whether
          the cell is corpus-validated (✓ direct adaptation),
          corpus-adjacent (✓ adapted from neighboring niche), or
          speculative (⚠ novel territory).

     v613c — PER-LINE CORPUS ANNOTATION (encouraged):
       Each scene's action_note can begin with a corpus annotation:
         [corpus: <source-file> §<section>] Static handheld camera...
       Novel lines (no corpus parent): [novel — testing]
       Makes corpus-derivation auditable at review.

     PRE-OUTPUT VALIDATION:
       YES For each Image, list product terms in (prompt body,
           action_notes of scenes pointing here, scene lines pointing
           here). If ANY term present AND product_image: NOT set →
           REJECT, fix by setting product_image: OR removing the
           mention if image is HOOK / RECIPE-early.
       YES For each HOOK image (scenes 1-2) and RECIPE-early image
           (lemon/ginger), no product visible in prompt body? Use
           non-product anchor language.
       YES Frontmatter or ## Sources cites ≥2 raw/decoded files
           with parenthetical pattern labels?
       YES Niche voiceover-script wiki page cited?
       YES Cell honesty NOTE present (✓ validated / ✓ adjacent /
           ⚠ speculative)?
       YES HOOK line adapts an Opening line from the niche wiki
           page's ## Hooks table (or [novel — testing] flagged)?

[23] V614 CROSS-CORPUS STRUCTURAL SURVEY + MANDATORY ADAPTATION MAP —
     before writing any dialogue line, survey ALL raw/decoded_*.md
     and raw/dr_kim_*_decoded.md files (~24 in current corpus) to
     extract structural patterns. Classify each into Pattern A/B/C/
     D/E:
       A — BEFORE/AFTER (4-6 lines): dr_kim trilogy, decoded_back_bump,
           decoded_varicose_vein
       B — RECIPE-LED (4-15 lines): saffron-trilogy, belly_burn_tea,
           bladder_tea, icelandicwisdom, oldearl_visceralfat
       C — DIAGNOSTIC/SHOW-PROBLEM-PIVOT (4-17 lines): corella_blood_
           sugar, oldearl_tonsil, master_chen_three_things
       D — CULTURAL-AUTHORITY TEMPLATE (10-line rigid): master_salvora
           trilogy (banana/cabbage/salmon — IDENTICAL template)
       E — PERSONAL-AUTHORITY (8-16 lines): rastajahmeil, master_chen,
           decoded_meta_papaya_skin

     Universal corpus rules extracted from all 24 winners:
       - 12-25 words per line (conversational, not literary)
       - 4-17 total lines (most winners 4-10)
       - CANONICAL CTA in 12 of 24: "comment '<keyword>' and i'll
         send you my full <protocol>. but follow me first so i can
         reach you" — DON'T REINVENT, LIFT IT
       - MECHANISM = 1 line, concrete benefit ("saffron relaxes blood
         vessels — more blood means more girth"), NOT clever-reframe
         academic-jargon ("blood-brain barrier resets hypothalamus")
       - AUTHORITY IMPLICIT, NOT DECLARED: corpus voice is "I've seen
         people go from X to Y", NOT corporate "I'm Dr. X and I help
         one million Y"
       - RECIPE STEPS are short comma-lists or single-action lines
       - NO MELODRAMA: direct symptom-callouts, not theatrical reframes
       - NEGATION-PIVOT signature (Pattern C): "the best thing for X
         is not Y, not Z, definitely not W"

     v614b — MANDATORY frontmatter fields:
       corpus_pattern: <A/B/C/D/E or hybrid declaration>
       adaptation_map:
         scene_1: "<corpus-file> L<line> §<section-label>"
         scene_2: "..."
         ...
         scene_N: "..."
       (every scene mapped; novel scenes use "[novel — testing]"
       with rationale)

     v614c — MANDATORY per-scene [corpus: ...] annotation:
       Every scene's action_note MUST begin with:
         [corpus: <source-file> L<line> §<section>] <rest of action_note>
       Annotation must match the adaptation_map entry for that scene.
       Mismatch = REJECT.

     v614d — MANDATORY corpus_compliance_audit in frontmatter:
       corpus_compliance_audit:
         - words_per_line: <range vs corpus 12-25>
         - line_count: <count vs corpus 4-17>
         - cta_template_canonical: yes/no
         - mechanism_concrete_not_clever: yes/no
         - authority_implicit: yes/no
         - melodrama_removed: yes/no
       If any field 'no' → explain WHY in comment OR rewrite to comply.

     PRE-OUTPUT VALIDATION:
       YES corpus_pattern: declared with at least one of A/B/C/D/E?
       YES adaptation_map: declared, one entry per scene?
       YES Every action_note opens with [corpus: ...] matching map?
       YES corpus_compliance_audit: all 6 fields declared?
       YES Words-per-line in 12-25 corpus norm?
       YES CTA scene lifts canonical "comment '<keyword>' / send my
           full / follow me first" template?
       YES Mechanism scenes use concrete-benefit chain, NOT jargon-
           academic reframe?
       YES No corporate voice ("I'm Dr. X and I help one million Y")?

[25] V621b ABSOLUTE BAN — NO CAPTION DESCRIPTORS IN IMAGE PROMPTS.

     Image prompts MUST NEVER describe caption text. Captions are
     added at the platform level post-generation (via the editor's
     caption layer). Including caption descriptors in the image
     prompt makes Banana 2 BAKE them into pixels — uneditable,
     wrong font, wrong wrap, low fidelity.

     FORBIDDEN phrases anywhere in Image prompt body:
       "yellow burned-in captions at the lower third"
       "white subtitle bar across the bottom"
       "large overlaid text reading 'X'"
       "caption: 'Try this remedy!'"
       ANY descriptor of post-production text overlays.

     When adapting from a decoded source whose original artifact had
     caption descriptors (pre-v621), STRIP them on lift/create.

     PRE-OUTPUT VALIDATION:
       YES Zero "caption", "subtitle", "overlay text", "lower third"
           descriptors in any image prompt body?

[24] V615 ABSOLUTE BAN — NO EM-DASH (—) IN DIALOGUE LINES.

     Owner directive (mandatory): "absolutely mandatory no — symbols
     in any lines."

     Scene "- **line:**" entries MUST contain ZERO em-dash (—)
     characters. Em-dashes create awkward pauses in spoken delivery
     that don't match natural speech. Use commas, periods, or
     rephrase to flow naturally.

     SCOPE — applies ONLY to scene "- **line:**" entries (the spoken
     voiceover). Em-dashes are still allowed in:
       - action_note prose (cinematic direction, not spoken)
       - Image prompt body (visual direction, not spoken)
       - Frontmatter / Sources / metadata
       - corpus annotations like [corpus: file — section]

     FORBIDDEN:
       "- **line:** Saffron is the only ingredient — and the only
        one — that resets your hormones."
       "- **line:** This is what menopause does at 2 a.m. — soaked
        sheets, racing heart, no sleep."

     REQUIRED:
       "- **line:** Saffron is the only ingredient that resets your
        hormones."
       "- **line:** This is what menopause does at 2 a.m. Soaked
        sheets, racing heart, no sleep."

     PRE-OUTPUT VALIDATION:
       YES Zero — characters in any "- **line:**" entry?
       YES Replaced with: comma, period, or rephrase?

     Note: this OVERRIDES corpus-pattern preservation when the corpus
     parent line contained em-dashes. The corpus is a DIALOGUE TONE
     reference, not a punctuation mandate. Owner's spoken-delivery
     preference takes precedence.

[26] V736 SPECTACLE-OVER-LOGIC DISCIPLINE — close the safe-default
     metaphor loophole left open by v598 + v600. Operator rule: "If
     the visual metaphor makes logical sense in a middle-school
     biology class, REJECT IT. Viral hooks rely on Spectacle
     Disconnect — the prop should be visceral first, metaphor second."

     Four sub-rules (all four apply on create-from-zero):

     v736a — ANTI-SENSE METAPHOR BAN. Banned-by-default first-instinct
       prop mappings: prostate -> garden hose / digestion -> drain /
       heart -> engine / joints -> hinges / skin -> wallpaper /
       hair -> grass / eyes -> camera lens / liver -> filter /
       hormones -> thermostat / bloat -> overinflated balloon. If
       your first-instinct prop is on this list (or a near-neighbor),
       STOP and re-pick. Replacement criterion: visceral first,
       metaphor second — a viewer who doesn't know what the video is
       about would still stop scrolling to watch the prop be
       destroyed / squeezed / cascaded / pulled apart.

     v736b — TREND-HIJACK MANDATE. Create-side MUST name a current
       viral aesthetic from catalog (ASMR soap cutting / hydraulic
       press / power-washing / kinetic-sand slicing / satisfying
       paint-mixing / giant water-balloon pop / pomegranate smash /
       slime-pull / cake-frosting reveal / soaked-sponge wring /
       wax-seal melt / glass-shatter slow-mo) and frame the cell's
       pain point THROUGH that aesthetic. The HOOK prompt MUST be
       structured: "frame the [niche] hook using a [trend-name]
       visual style. Show the satisfying / visceral [destruction /
       transformation / pull-apart] of the prop BEFORE delivering the
       medical claim." Generic "visual hook" / "satisfying action" /
       "scroll-stopper" wording FAILS the gate.

     v736c — UNCOMFORTABLE-TEXTURE MANDATE. Hero prop in HOOK MUST
       possess textural / messy state. Allowed: oozing / dripping /
       bursting / sticky / fibrous / gelatinous / foamy / slimy /
       fleshy / pulpy / viscous / soaked / stretchy / gloppy. Banned
       default-texture (dry plastic / smooth metal / clean glass /
       bare wood / polished stone / dry paper) words adjacent to
       hero prop FAIL the gate. Texture rule applies to the PROP,
       not the persona's hands or the setting. Combine with v720c
       body-pose discipline + v716/v717 anti-normalization.

     v736d — SANDBOX-IDEATION GATE. Your videos/*.md OUTPUT MUST
       begin with a "## Brainstorming Sandbox" section IN THE OUTPUT
       FILE BEFORE the YAML frontmatter, containing:
         1. Five (5) radically different visual hook concepts. Each
            names: hero prop + texture class + force-verb + trend tag
            + 1-line metaphor mapping.
         2. Each rated 1-10 "Unhinged TikTok Spectacle".
         3. The 3 lowest-rated concepts struck through with ~~text~~.
         4. The single most visceral concept marked **SELECTED →**.
         5. Selected concept's prop / texture / trend / force-verb MUST
            match the HOOK Image content in ## Images / ## Storyboard.

     CREATE-SPECIFIC: full sandbox required from cold; no source to
     anchor against. The trend-hijack option (v736b) SHOULD win
     unless another sandbox entry is genuinely more visceral.

     WHY in-file mandatory: linear token generation locks you into
     the first plausible idea you emit. Sandbox in OUTPUT commits 5
     concepts to context BEFORE the first scene block locks tone.
     Sandbox-in-chat does NOT work — you treat chat as draft and
     OUTPUT as final, and OUTPUT's first scene-image dominates
     downstream attention.

     Worked sandbox example (saw-palmetto / prostate):

       ## Brainstorming Sandbox

       1. ~~Garden hose unkink — dry plastic, GRIP + PULL-APART,
          [no trend tag], maps "kinked urethra" 1:1. Spectacle: 2/10
          (logical, dry, boring).~~
       2. ~~Faucet drip-stop — chrome faucet, TIGHTEN, [no trend tag],
          maps "leaky bladder". Spectacle: 3/10.~~
       3. ~~Drain clog + plunger, PUSH + RELEASE, maps "obstruction
          lifts". Spectacle: 4/10 (logical drain analogy).~~
       4. Pomegranate smash — over-ripe pomegranate (oozing / bursting
          / dripping), SLAM + CASCADE, [hydraulic-press trend], juice-
          cascade maps "trapped pressure releasing." Spectacle: 9/10.
       5. **SELECTED →** Soaked-sponge wring — kitchen sponge soaked
          in murky water (gelatinous / dripping / foamy), GRIP + TWIST
          + CASCADE, [power-washing trend], cascade onto bare hands
          maps "stuck pressure finally moving." Spectacle: 10/10.

     PAIRING: v598 power-test runs AFTER v736 selection (selected
     concept must still pass Q1-Q8). v600 cartoon-physics extended
     by v736c from "magnitude" to "texture / state". v697 force-verb
     chain named per sandbox entry. v713-v720 composition discipline
     applied to the selected concept's HOOK image. v621
     narrative_lens: sandbox = GRABBING-ATTENTION.

     PRE-OUTPUT VALIDATION:
       YES First hero prop in HOOK image NOT on v736a banned list
           (or struck-through entry in sandbox + alternative SELECTED)?
       YES Each sandbox entry carries a [<trend-name>] tag from v736b
           catalog?
       YES Selected entry's trend appears in HOOK Image's
           [Composition] block or action_note?
       YES Selected hero prop's texture-class explicitly named in
           sandbox AND echoed in HOOK Image body prose?
       YES Banned default-texture words (dry / smooth / clean /
           polished / bare) absent adjacent to hero prop in HOOK?
       YES "## Brainstorming Sandbox" section present BEFORE YAML
           frontmatter with exactly 5 entries, 3 struck-through, 1
           SELECTED?
       YES Selected entry's prop / texture / trend / force-verb chain
           matches HOOK Image content (cross-check by grep)?

[27] V736.1 DNA-FIRST AMENDMENT (sub-rules e/f/g/h) — the 7
     universal invariants extracted from corpus DNA. v736 a-d closed
     the safe-default loophole; v736.1 adds composition + economy.

     THE 7 INVARIANTS (constants across niches — surface vars vary):
       1. ONE symptom-bearing object dead-center
       2. Persona hands actively manipulating object
       3. Object texture wet / messy / visceral / uncomfortable
       4. Persona face visible above OR beside object, mouth mid-
          word, eyes on lens
       5. Authority setting blurred behind
       6. Object connection to symptom rhetorical not literal
       7. 8-sec force-verb arc with visible state change

     v736e — DEAD-CENTER COMPOSITION. Hero prop owns geometric
       middle, NOT rule-of-thirds intersection. Camera level MATCHES
       hero anchor height (chest for held-aloft / belly for distended
       belly / brow for wrinkle-macro / lumbar for back-symptom).
       NEVER top-down or high-angle. Required [Composition] phrase:
       "[hero prop] fills the immediate center-foreground, dominating
       the middle of the image". Required Negative: "No prop sinking
       to the lower-third. No rule-of-thirds offset — symptom
       occupies geometric center."

     v736f — ACTIVE-HANDS MANDATE. Persona's hands actively
       manipulate the hero object — grip / squeeze / lift / wrap /
       press / shake / wring / pierce / scrape / smooth / wind /
       inflate / pull-apart. Static hold FAILS. Manipulation IS the
       spectacle anchor that triggers Invariant 7 state change.
       Required [Subject — Host] phrase: "both hands [active-verb]
       the [hero prop]". Required Negative: "No static hold."

     v736g — FACE-ABOVE-OR-BESIDE-OBJECT. Two valid configurations:
       ABOVE (single-subject, persona behind prop, face above) per
       frames 1-3, 6 of corpus; BESIDE (two-shot, persona viewer-side
       of prop / patient body, face on viewer-edge at chest-up) per
       frames 4-5. v713a partial-visibility override compatible —
       face cropped to eyebrow-to-chin still satisfies v736g.
       Persona-cropped / hidden / displaced FAILS. Required Negatives:
       "No persona crop on the face. No persona-hidden-behind-prop.
       No persona-displaced-to-corner."

     v736h — PROMPT-ECONOMY DISCIPLINE (the most-violated sub-rule):

       HARD CEILING: Image prompt body (the [Composition] -> [Tech] +
       Negatives content under "### Image N") MUST stay under 400
       words. Ideal range 200-350. Banana 2 fidelity drops past 300w
       per wiki/generation/nano-banana-prompting.md line 194 ("long
       text + photos fight each other").

       HARD BANS inside Image prompt body:
         - Meta-commentary about rules ("per Invariant 1" / "per
           v736e" / "per v722"). Audit tags belong in lint output,
           not prompt text.
         - Beat structure ("[Start beat 0-2s]" / "[Mid-clip beat]" /
           "[End beat 6-8s]"). Beats describe motion — Image is ONE
           frame. Beats live in Scene action_note for Veo motion.
         - Temporal language ("Across 8 seconds" / "throughout" /
           "during the clip" / "then [verb] then [verb]"). Describe
           ONE state.
         - Splitting dual / triple props into [Subject — Symptom A]
           + [Subject — Symptom B] blocks. Single [Subject —
           Symptom] block keeps cohesion. Frame 3 of corpus (dual
           prostate models) is ONE block.
         - Over-described persona blocking past one sentence.
           Banana 2 just needs "holds X and Y at chest height with
           both hands."
         - Wardrobe / upload / framework callouts in body prose
           ("Persona identity carried by upload (no inline wardrobe
           per v722)"). Audit-only.
         - Negative-block past 10 clauses. "No green elephant"
           hallucination class fires past ~10. Pick 5-8 the niche
           keeps violating.

       IMAGE vs SCENE SEPARATION (the structural fix):
         Image prompt body -> Banana 2 still frame (LEAN, single-
           state, tight negatives, no meta, no beats, <=400w).
         Scene action_note + line + action_arc -> Veo motion clip
           (VERBOSE-OK with beats + force-verb chain + lip-sync,
           no ceiling).

       For BANANA 2 STILL: "exaggerated shocked expression"
       outperforms "mouth open mid-utterance" — Banana 2's training
       prior on staged expressions is stronger. v721 lip-sync
       language ("mouth open mid-utterance, eyes locked to lens")
       is for VEO RENDER lip-sync — lives in Scene action_note,
       NOT Image body.

       DNA INVARIANTS ENFORCED BY CONTENT, NOT BY LABELS:
         Invariant 1 (dead-center) -> "fills the immediate center-
           foreground, dominating the middle". DROP: "(NOT viewer-
           left third, per Invariant 1, occupying 60% of vertical
           center axis)".
         Invariant 4 (face above) -> "face is sharply visible just
           above the prop". DROP: "(per Invariant 4)".
         Invariant 5 (background blurred) -> "background fully
           blurred". DROP listing every blurred element.

     PRE-OUTPUT VALIDATION (v736.1):
       YES Each "### Image N" body word count <=400 (ideal 200-350)?
       YES Zero "(per Invariant" / "(per v[0-9]+" tags inside Image
           bodies (audit tags are lint-only)?
       YES Zero "[Start beat" / "Across \d+ seconds" / "throughout
           the clip" inside Image bodies (beats live in Scene
           action_note)?
       YES Hero prop fills the immediate center-foreground per v736e
           (composition language present)?
       YES Persona hands actively manipulate hero prop per v736f
           (active-verb present in [Subject — Host])?
       YES Persona face visible above OR beside prop at chest-up per
           v736g (composition phrase present)?
       YES drift-guard / ban sentences <=10 clauses, woven into the
           body (no trailing negatives block — 2026-06-12)?
       YES Single [Subject — Symptom] block for dual / triple props
           (no [Subject — Symptom A] + [Subject — Symptom B] split)?

If any item above fails, FIX IT BEFORE OUTPUT. The operator will
re-prompt you to fix violations otherwise. Self-correction here saves
a round-trip.

[28] V738 PRE-FLIGHT CHECKLIST (HARDENED 2026-05-16 — STRICT REJECT
     GATE, mandatory thinking-prelude before artifact emission) —
     full spec at code/template_reference.md §"v738 — Pre-Flight
     Checklist".

     STRICT ENFORCEMENT: artifact missing ## Pre-Flight Checklist is
     REJECTED. Re-emit with checklist FIRST before any markdown body.
     No exceptions. Operator-side grep gate enforces this.

     visual_delta MORPHOLOGICAL-CHANGE MANDATE (v718d):
     Every - **visual_delta:** field MUST contain BOTH kinematic
     action AND morphological state-change descriptor. Pure-kinematic
     deltas (just "hand pours liquid") are REJECTED — you glossed
     over the morphological delta. Required pattern: "[kinematic
     action] + [now contains X from prior step OR transformed from Y
     to Z OR primary_change_axis: Surface/Structural/Volume/Color]".
     Operator-side grep gate at code/template_reference.md §"v738
     Pre-output gate" enforces this.

     Bundle parser will reject any artifact violating these gates.
     Don't ship sloppy.

     Before emitting the final ## Brainstorming Sandbox / ## Ingredients
     / ## Images / ## Storyboard sections, FIRST output a
     ## Pre-Flight Checklist block declaring rule resolutions for THIS
     cell + niche. Catches rule collisions at the LLM's own planner
     step instead of the operator's audit step.

     Mandatory checklist sections:
       1. Composite layout check (v737 + v698A.1 Q2)
          — PiP / corner-inset planned? → v737 decoupling; route
          through v698A voiceover-paired with shared anchor.
       2. State-evolution + short-line check (v580 + v704 + v644)
          — Recipe chain requiring new image per step? + planned
          line <12w? → keep scenes separate; USE - **pad:** bullet
          to extend Veo TTS to ~20w combined; do NOT merge.
       3. Voiceover-paired detection (v698A.1 Step 1 decision tree)
          — For each planned scene: Q2 face-as-PRIMARY-subject (with
          PiP carve-out per v737) → Q3 lip-sync. List voiceover-
          paired scenes + anchor image.
       4. Sandbox requirement check (v736d)
          — Output type = create → ## Brainstorming Sandbox REQUIRED
          at top. Five entries from cold (no source to anchor).
       5. Vocabulary safety check (v702 + v615 + v693 + v722) —
          OUTPUT-TYPE BRANCH (HARDENED 2026-05-15):

          This is a CREATE artifact (videos/*.md) -> APPLY v702
          (RELAXED 2026-05-15 clinical-register carve-out). Walk
          the v702 4-step decision tree per
          code/template_reference.md §"v702 — Image-prompt
          vocabulary safety":
            (1) bare anatomical noun on allowed clinical list?
            (2) sexual-action verbs in same sentence?
            (3) sexualized adjectives in same noun phrase?
            (4) sounds like a physician at consult OR like erotic
                fiction?
          Class 1 (sexual-action verbs adjacent to anatomy) + Class 2
          (slang body-part words in image prompt fenced bodies) ->
          swap. Class 3 (clinical anatomical terms alone) -> ALLOWED.

          v615 / v693 / v722 still apply.
       6. Morphological Delta Declaration (v738.1 / v718d / v718e —
          REPLACES Anti-Platonic Gate single-state check, HARDENED
          2026-05-17 from kinematic-over-morphological blind spot
          surfaced in tongue-cleanse decode failure):

          For EVERY hero prop in this artifact's Image blocks,
          declare per-prop block:

            Hero Prop: <prop name verbatim from Ingredients table>
            Image(s): <comma-separated image_N tokens>
            Scene(s): <comma-separated scene_N tokens>
            t=0 (Start State): <explicit texture / color / volume /
              structural integrity — BEFORE state at peak severity>
            v736c Texture Check (NEW 2026-05-18, v738.1 amendment):
              <MUST name uncomfortable texture class: oozing /
              bursting / sticky / fibrous / gelatinous / dripping /
              foamy / slimy / fleshy / pulpy / viscous / soaked /
              stretchy / gloppy / grimy / coated / crusted /
              encrusted / hyperemic / edematous / inflamed /
              pendulous / drooping / sagging / bloated / pustular /
              blistered / scaly / weeping / suppurating / atrophied.
              May be "n/a (static prop)" only when Delta Axis ==
              NONE>
            t=end (Terminal State): <explicit texture / color /
              volume / structural integrity — AFTER state at peak
              resolution>
            Delta Axis: <Surface/Texture | Structural Integrity |
              Volume/Shape | Color/Illumination | NONE>
            Carry Mode: <within-clip | within-clip-end-frame |
              multi-clip-blend | cross-image | both>
            Magnitude: <COMPLETE | PARTIAL | MINIMAL | NONE>

          HARD GATE (all REJECT):
            - Delta Axis != NONE AND t=0 == t=end → REJECT.
            - Delta Axis != NONE AND t=end uses kinematic-only verbs
              without morphological state-change descriptor → REJECT.
            - At least ONE hero prop in HOOK + diagnostic-reveal
              scenes MUST have Delta Axis != NONE. If all hero props
              declare Delta Axis = NONE, re-amp via Pattern 21 +
              v716 + v717 + v719 + Pattern 23 until at least ONE
              prop transforms measurably.

          v604.1 PAIRING: when Delta Axis != NONE AND Carry Mode =
          within-clip | both, frame_anchor MUST point at t=0 (BEFORE
          state), NOT t=end. Annotate with "(BEFORE-state anchor)".

          v586.1 + v717.1: when Delta Axis != NONE for a prop AND
          narrative_lens ∈ {AUGMENTED-SYMPTOMS, HEALER-SHOWING-CURE},
          [Subject — Symptom] block opener MUST name the prop's t=0
          state at peak severity. Generic identifiers are ILLEGAL.

          v718g NEW REQUIRED FIELD: when Delta Axis != NONE AND
          Carry Mode = within-clip | both, Scene block MUST carry
          - **visual_delta_within_clip:** pairing TRANSFORMATIVE
          verbs (v697.1) with morphological state-change descriptors.

          WHY: forces author to write t=0 + t=end side-by-side BEFORE
          markdown body emits — triggers contrast-recognition.

       7. Action-Consequence Coupling (v718e, NEW 2026-05-17) —
          for EVERY scene whose primary_change_axis != NONE (per
          v718d morphology diagnostic), the - **action_note:**
          field MUST satisfy three coupling rules:

          v718e-1: Mid-clip beat AND End beat force-verbs paired
            with morphological consequence in the SAME SENTENCE.
            Pattern: "[force-verb] the [prop], [transformation-
            verb] the [prop-feature] into [end-state]".
            Wrong: "the liquid cascades over the tongue, coating
                    the grime."
            Right: "the liquid cascades over the tongue, washing
                    away the grime in a single continuous sweep."

          v718e-2: [End beat 5-8s] clause MUST manifest
            intrinsic_state_end declared per v718d. Cannot end on
            transient state. v589 magnitude (COMPLETE / PARTIAL /
            MINIMAL) propagates to End beat vocabulary.

          v718e-3: Banned static-contact verbs in Mid + End beats:
            coating / covering / pooling on / resting on /
            touching / sitting on / placed on / lying on / falling
            on / settling on / landing on / arriving at / meeting
            / contacting.

            Required transformation verbs by axis:
              Surface/Texture       washing away / dissolving /
                                    scrubbing clean / clearing /
                                    revealing / stripping
              Structural Integrity  smashing open / shattering /
                                    splitting / bursting /
                                    tearing / fracturing /
                                    exploding
              Volume/Shape          melting / shrinking /
                                    deflating / flattening /
                                    draining / collapsing inward
                                    / lifting tight
              Color/Illumination    flushing red / glowing bright
                                    / dimming dark / blanching
                                    pale / igniting

          Operator-side Python gate at code/template_reference.md
          §"v718e Pre-output mechanical gate" enforces v718e-3.
          Run before ship; expect zero v718e FAIL output.

          Carve-out: primary_change_axis == NONE → v718e N/A.

       8. Composition discipline check (v713 + v715 + v716/v717 +
          v720 + v736e/f/g/h).

       9. Image cardinality + use audit (v594 + v580) — zero unused
          images.

       10a. Veo 3.1 Structural Delta Decision Tree (v718h-A/B/C +
            v580.2 + v718i, NEW 2026-05-17, render-test validated):

            Per scene with Section 6 Delta Axis != NONE:
              Surface/Texture | Color/Illumination → Option A
                (single-clip Veo with VFX Wipe Override per v718h-A).
              Volume/Shape | Structural Integrity → Option C (Veo
                native end-frame interpolation per v718h-C + v718i,
                LIVE 2026-05-18, RECOMMENDED DEFAULT — single Veo
                clip with cfg.last_frame native interpolation) OR
                Option B (multi-clip blend per v718h-B, FALLBACK
                when single-clip Veo render budget unavailable) OR
                Option A as escape hatch with acknowledgement.

            OPTION B (Multi-Clip Blend, v718h-B + v580.2 —
            RECOMMENDED for Structural/Volume; uses existing platform
            features): (1) author TWO Banana 2 Images — Image K t=0
            BEFORE + Image K+1 t=end AFTER (chained via v580 per
            v580.2 with paired frame_anchor annotations); (2) author
            TWO sequential Scenes — Scene N (image_K, clip_mode:
            fresh, transition: blend, target_duration_s ≈
            source_clip_duration / 2) + Scene N+1 (image_K+1,
            clip_mode: blend, transition: cut, target_duration_s ≈
            source_clip_duration / 2); (3) lines split or placed on
            Scene N+1; (4) renumber downstream scenes; (5) Section 6
            Carry Mode = multi-clip-blend + lists paired Images +
            Scenes; (6) Section 8 has TWO rows; (7) each scene's
            action_note covers its half of the morphology; (8) Veo
            prompts NORMAL (NOT VFX Wipe — anchor is Banana 2 Image).
            Render expectation ~95% success.

            OPTION C (v718h-C + v718i — LIVE 2026-05-18, RECOMMENDED
            DEFAULT for Structural/Volume axes): SINGLE Veo clip
            with `- **end_frame_image:** image_K+1` field + cfg.last_
            frame native interpolation. Two Banana 2 Images (Image K
            BEFORE + Image K+1 AFTER per v580.2); single Scene N
            (image: image_K + end_frame_image: image_K+1 + target_
            duration_s: full clip). Carry Mode = within-clip-end-
            frame. Section 8 ONE row. Veo prompt NORMAL (NOT VFX
            Wipe). HALVES Veo render cost vs Option B + smoother
            interpolation + no CapCut seam.

            OPTION A (VFX Wipe Override, v718h-A — single-clip
            escape hatch; required for Surface/Texture + Color
            axes) — for EVERY Veo Final Prompt body where Section 6
            declares Delta Axis != NONE AND Carry Mode = within-
            clip | both, apply 5-step protocol: STEP 1 static camera lock + zero start-state
            description; STEP 2 Temporal Forcing ("IMMEDIATE ACTION:
            Right from the first frame, ..."); STEP 3 Action-
            Consequence Coupling in one continuous paragraph; STEP 4
            axis-matched verbs (Surface/Texture: WASH/DISSOLVE;
            Color: COLOR-SHIFT/TRANSFORMS-INTO; Volume: REDUCES/
            DRAINS/DEFLATES; Structural Integrity: MUST ESCALATE to
            VFX WIPE OVERRIDE — "digital VFX wipe" + "ERASING the
            3D geometry" + "REPLACED in real-time" because surface
            verbs FAIL on 3D pathology, Veo physics prior treats
            raised geometry as solid); STEP 5 TERMINAL STATE LOCK +
            temporal negatives ("no delay, no hesitation, no holding
            the start frame, no shape-shifting lag") + start-state
            pixel negatives ("no [feature] remaining at clip-end").

            HARD GATE: every Veo Final Prompt body whose scene has
            Section 6 Delta Axis != NONE MUST satisfy 5-step
            protocol. Full deep-dive at code/template_reference.md
            §"v718h — Veo 3.1 I2V Temporal Consistency Override".

       10. Per-scene morphology audit (v738.2 / v718d / v718e /
           v697.1, NEW 2026-05-17) — required table, one row per
           Scene:

           | Scene N | Hero Prop(s) | Delta Axis | t=0 state | t=end state | TRANSFORMATIVE force-verb(s) (v697.1) | Resolution token in End beat (v718e-2) |
           | 1 | tongue | Surface/Texture | coated grey-brown | clean pink | POUR + SCRUB | washed away |
           | 5 | book (CTA) | NONE | book held | book held | LIFT + PRESENT (NON-TRANSFORMATIVE) | n/a |

           HARD GATE:
             - Every Scene N in ## Storyboard MUST have matching row.
             - Row's Delta Axis MUST match Section 6 declaration.
             - Row's TRANSFORMATIVE column MUST contain ≥1 v697.1 TRANSFORMATIVE verb when Delta Axis != NONE.
             - Row's Resolution column MUST contain ≥1 v718e-2 token when Delta Axis != NONE.
             - Rows with Delta Axis == NONE allow n/a + MUST use NON-TRANSFORMATIVE verbs only.

           v697.1 TRANSFORMATIVE: POUR / CASCADE / SPRAY / SLAM /
           SQUEEZE / DROP / SMASH / SCRUB / WIPE / TILT-POUR /
           STRAIN / DRAIN / DISSOLVE / SHATTER / MELT / BURST /
           DEFLATE / COLOR-SHIFT / WASH-AWAY / ...
           v697.1 NON-TRANSFORMATIVE: HOLD / LIFT-PRE / PRESENT /
           GESTURE-FORWARD / OPEN-PALM / POINT-TO-LENS / END-LOOK /
           END-HOLD / NOD / FACE-LENS / GRIP-STEADY / ...

     The checklist sits ABOVE the ## Brainstorming Sandbox block.
     Platform parser ignores ## Pre-Flight Checklist.

     Skip pre-flight ONLY for trivial single-scene creates (one HOOK +
     one CTA, no recipe chain, no PiP).

TASKEOF
}

# Always write to a temp file as fallback
TMPDIR_PATH="${TMPDIR:-/tmp}"
BUNDLE_FILE="$TMPDIR_PATH/create_bundle_$(date +%s).md"
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
    echo "[create_bundle] Bundle saved (POSIX):   $BUNDLE_FILE"
    if [[ -n "$WIN_BUNDLE_FILE" ]]; then
        echo "[create_bundle] Bundle saved (Windows): $WIN_BUNDLE_FILE"
    fi
}

print_upload_guidance() {
    echo "[create_bundle] Upload options for LLMs with paste-size caps (e.g. Gemini app):"
    echo "[create_bundle]   - Drag the .md file from Explorer into the chat's attach field"
    if [[ -n "$WIN_BUNDLE_FILE" ]]; then
        echo "[create_bundle]   - Or paste the Windows path above into the upload field"
    fi
    echo "[create_bundle]   - Then add the one-line cell-spec prompt:"
    echo "[create_bundle]       \"create a new videos/*.md for [persona] [niche] [audience] from a corpus-validated cell\""
}

if [[ -n "$CLIP_CMD" ]]; then
    if cat "$BUNDLE_FILE" | $CLIP_CMD 2>/dev/null; then
        echo "[create_bundle] OK: ${#BUNDLE_FILES[@]} files concatenated (~${BYTES} bytes), piped via '$CLIP_CMD'"
        print_paths
        print_upload_guidance
    else
        echo "[create_bundle] WARNING: clipboard pipe failed (sandboxed env or clip locked)"
        print_paths
        echo "[create_bundle] Manual clip pipe: cat \"$BUNDLE_FILE\" | clip   (or pbcopy / xclip)"
        echo "[create_bundle] OK: ${#BUNDLE_FILES[@]} files concatenated (~${BYTES} bytes)"
        print_upload_guidance
    fi
else
    echo "[create_bundle] No clipboard tool found."
    print_paths
    echo "[create_bundle] OK: ${#BUNDLE_FILES[@]} files concatenated (~${BYTES} bytes)"
    print_upload_guidance
fi
