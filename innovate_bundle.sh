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

# Innovate bundle file list — shared generate canon + innovation-specific adapt rules
# (innovation-moves / preserve-swap / real-adapt-not-reskin / cross-gender) that
# distinguish innovate from lift. Plus the matching per-niche page auto-appended below.
# Keep in sync per code/CLAUDE.md "Canonical homes" step 7.
BUNDLE_FILES=(
    # ----- canonical rule homes (shared) -----
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
    # ----- innovation-specific adaptation rules (NOT in create bundle) -----
    "wiki/concepts/script-adaptation/innovation-moves.md"
    "wiki/concepts/script-adaptation/preserve-swap-framework.md"
    "wiki/concepts/script-adaptation/real-adapt-not-reskin.md"
    "wiki/concepts/script-adaptation/cross-gender-adaptation.md"
)

# ----- auto-append the matching per-niche asset-bank page (root CLAUDE.md §6.0) -----
# Niche page = verbatim language bank + proven hooks + shame-proxy + worked-build
# precedents. For innovate, the TARGET niche is in the cell spec (NOT the source).
NICHE_HAYSTACK="$(printf '%s' "$CELL_SPEC" | tr '[:upper:]' '[:lower:]')"
NICHE_MAP=(
    "ed:erectile|male[ _-]?ed|\\bed\\b|soldier"
    "testosterone:testosterone|low[ _-]?t\\b"
    "prostate-health:prostate"
    "hair-loss:hair[ _-]?loss|balding"
    "belly-fat:belly[ _-]?fat"
    "weight-loss-saggy-legs:saggy[ _-]?leg|weight[ _-]?loss"
    "cellulite:cellulite"
    "crepey-skin:crepey|crepe[ _-]?skin"
    "puffy-face:puffy[ _-]?face"
)
for entry in "${NICHE_MAP[@]}"; do
    slug="${entry%%:*}"; pat="${entry#*:}"
    if printf '%s' "$NICHE_HAYSTACK" | grep -qiE "$pat"; then
        npage="wiki/entities/niches/${slug}.md"
        [[ -f "$REPO_ROOT/$npage" ]] && BUNDLE_FILES+=("$npage") && \
            echo "[innovate_bundle] niche page auto-added: $npage" >&2
        break
    fi
done

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

  IMMEDIATE ACTION: <continuous prose paragraph per v718h-A Step 3>

  TERMINAL STATE: <explicit final state per v718h-A Step 5>

  The main character says in a <register> voice, "<dialogue line>".

  Ambient: <sound description>.
  (no subtitles, no captions)

  **Negative prompt:**
  <comma-separated negatives>

CRITICAL HARD BAN: NO `[Start beat 0-Xs]` / `[Mid-clip beat X-Ys]` /
`[End beat Y-Zs]` brackets in Veo text prompt body. Beats live ONLY
in Storyboard `- **action_note:**` field. Veo prompt body = continuous
prose with IMMEDIATE ACTION + TERMINAL STATE anchor paragraphs.

Veo 3.1 renders continuous instruction; beat brackets confuse temporal
interpretation OR parse as on-screen text.

Header: N=Scene, M=Line within Scene (.1 for single-line),
REGISTER_LABEL=tag (HOOK / EXPLAIN / CTA / RECIPE-STEP-N / etc.).

================================================================================
V751 — VEO PROMPT <-> IMAGE BODY SEMANTIC CONSISTENCY (NEW 2026-05-18)
================================================================================

Veo text prompt body MUST be semantically consistent with start_frame
Image's body prose at t=0 AND end_frame Image's body prose at t=end
(when Option C).

BANNED drift: text prompt introduces state (open book / pour / shattered)
neither Image body shows -> Veo confused.

Transformations: v718h-A drives via continuous prose + VFX Wipe;
v718h-B paired Images + 2 clips; v718h-C paired Images + SINGLE clip
with end_frame_image binding. Text prompt body must align with chosen
Option's Image bindings.

If text prompt mentions state neither Image body shows -> REJECT.

================================================================================
V752 — CATALYST REACTION PACING (NEW 2026-05-18, render-test validated)
================================================================================

For Veo Final Prompts where scene's action_arc has a CATALYST class
TRANSFORMATIVE force-verb on a hero prop with Delta Axis != NONE,
transformation MUST complete INSTANTLY on catalyst contact + held terminal
state through remaining clip. Defeats Veo's default linear-smear behavior
across full clip even with end_frame anchored (critical on v718h-C).

CATALYST CLASS TAXONOMY:
  LIQUID-ON-SURFACE  POUR / SPRAY / DRIP / CASCADE -> WIPES / ERASES /
                     DISSOLVES / WASHES-AWAY
  IMPACT-ON-RIGID    STRIKE / SMASH / SLAM / HAMMER -> SHATTERS /
                     SPLITS / FRACTURES / EXPLODES
  TOOL-ON-SURFACE    SCRUB / SCRAPE / WIPE / BRUSH -> STRIPS / LIFTS /
                     CLEARS / REVEALS
  BLADE-ON-OBJECT    CUT / SLICE / SAW -> SPLITS / SECTIONS / OPENS
  FORCE-ON-FLEXIBLE  SQUEEZE / PRESS / PINCH / PULL / TWIST ->
                     BURSTS / DEFLATES / RELEASES
  HEAT-ON-COMBUST.   BURN / IGNITE / FLAME / MELT -> CHARS / BLACKENS /
                     LIQUEFIES
  GRANULAR-ON-LIQ.   DROP / SPRINKLE / SHAKE-INTO -> DISPERSES /
                     SUSPENDS / DISSOLVES

Y-MARK HEURISTIC: Y <= clip_duration / 3 (default Y=2.5s for 8s clip).

REQUIRED Veo prompt body additions:

  IMMEDIATE ACTION block:
    "IMMEDIATE ACTION (INSTANT REACTION ON CONTACT — no gradual
    progression):" + "The MOMENT the leading edge of [catalyst]
    contacts [prop], [start-state feature] is INSTANTLY [consequence-
    verb]" + "By the X-second mark, [terminal state] already visible.
    COMPLETE by ~Y seconds." + Structural/Volume: VFX Wipe language.

  TERMINAL STATE block:
    "TERMINAL STATE (held from ~Y seconds through clip-end):"
    + [prop] holds resolved state for ~Z seconds + persona settles.

  Ambient: single decisive catalyst sound + quiet through held terminal.

  Negative prompt: no GRADUAL [transformation] across full clip / no
  slow [transformation-verb] / no progressive transformation / no
  [start-state feature] past Y-second mark.

CARVE-OUTS: autonomous transformations / genuinely gradual processes
>15s / Delta Axis NONE / explicit slow-motion -> v752 N/A.

WHY CRITICAL ON OPTION C: Veo cfg.last_frame native interpolation
defaults to linear smear; v752 explicit timing + VFX Wipe Override
fight this. Operator surfacing case (tongue HOOK Clip 1.1, 2026-05-18):
post-v752 prompt = cleanse complete by 2.5s + held 5.5s. Operator
verdict: "much better now."

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
layer (clean pink mucosa under grime / flat organs under fat / clear
artery under clog / etc.).

V718D.2 — when innovating new HOOK, LLM MUST NOT extrapolate t=end
from t=0 + applied force-verb via real-world physics. Hero prop with
a catalyst applied is assumed FUNDAMENTALLY ALTERED / REVEALED.

V736I — liquids and tools are NEVER passive. Default Delta Axis
Magnitude = COMPLETE for HOOK with active force-verb. PARTIAL/MINIMAL
requires explicit named residual t=0 feature visible at t=end.

V738.1 HIDDEN-LAYER REVEAL TEST — before locking t=end, ask "did I
describe t=0 wet/contacted? Name the HIDDEN LAYER catalyst REVEALS."
Trigger phrasings rejected: "wet/oiled/soaked [prop]" / "[prop]
glistens without reveal verb" / "[t=0 features] still visible but
[softened]".

CORPUS-VALIDATED REVEAL CATALOG (default to these when innovating):
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
moves on, miss secondary multi-axis changes. When innovating new HOOK
Pre-Flight Section 6 t=end, single-sentence consolidated declarations
let autoregressive LLM skip axes silently.

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
Blend pairs use SAME discipline (Image K = start, Image K+1 = end +
paired_with). v580 multi-scene chain is NOT a pair — reference_image +
visual_delta only, no pair_role.

PARSER: pair_role ∈ {start, end} or absent. paired_with ONLY valid when
pair_role = end. paired_with image must exist + be lower-indexed.
Scene + Image pair_role mismatch advisory-warns (pre-v718j artifacts importable).

================================================================================

INNOVATE PROCESS (mirrors operator's internal workflow — follow exactly):

Step 1 — DECOMPOSE the outside-niche source (extract winning structure)
  Read the outside-niche reference. Name explicitly:

  - HOOK job — derive what the opening makes the viewer notice, feel,
    ask and wait for. Then compare it with the open hook bank. Use
    `UNLISTED — <source-derived hook job>` when no bank label fits.
  - BODY SHAPE — derive the source's actual section jobs and causal
    order. A/B/C/D/E remain optional historical search tags only.
  - Force-verb action_arc (v697) — chain of FORCE-verbs the source uses
    (LIFT/SLAM/POUR/CASCADE/SQUEEZE/PRESENT/SCATTER/etc.)
  - Mechanism — concrete-benefit line the source's payoff scene delivers
  - CTA shape — canonical "comment X / follow me first" or variant
  - Persona authority — name the source's actual trust signal and its
    visible anchors; use an unlisted source-derived name when needed

Step 1.5 — RETRIEVE THE FULL UNION + CREATE THE EVIDENCE PACKET
  Before choosing the direction, run semantic recall, inspect graph
  connections, then file-search the current processed corpus across wiki/
  and videos/. Ingest any relevant raw orphan before using it. List every
  relevant same-niche, cross-niche, same-style and cross-style hit.

  Create `docs/content-packets/<video-slug>.md` from the packet contract.
  Give every relevant hit exactly one disposition: USED, PROTECTED,
  REJECTED, or NOT APPLICABLE. State unavailable retrieval services and
  the evidence boundary. Record operator locks, evidence rank, one primary
  gap, one exact from→to PRIMARY DELTA, and a falsifier. The packet owns
  retrieval and choice; it does not duplicate the final beat map.

  The matching build §0 must point back with:
    EVIDENCE PROGRAM: v1
    EVIDENCE PACKET: docs/content-packets/<video-slug>.md
    PACKET STATUS: DIRECTION LOCKED | PRODUCTION COMPLETE | RESULT RECORDED

Step 2 — DECIDE what to KEEP vs SWAP (preserve-swap method per
  wiki/concepts/script-adaptation/preserve-swap-framework.md)
  KEEP (80% — the structural skeleton):
    - HOOK job + force-verb chain
    - source section jobs + causal order
    - Scene count + clip rhythm
    - CTA template shape
    - Mechanism BEAT (not the mechanism's words — the rhetorical move)

  SWAP (20% — the surface):
    - Niche — outside-niche source's niche → target cell's niche
    - Persona — outside-niche persona → target cell's persona
    - Setting — rebuild the source authority function in a §8-safe home,
      retail, outdoor, premium-wellness or cultural setting; no clinic,
      scrubs, exam room, diploma wall or medical-authority staging
    - Product placement — target Korella product per v599 matrix
      (reveal at recipe product-cascade scene + CTA hero-shot)
    - Pain points — target cell's audience x niche vocabulary
      (from the per-niche asset-bank page wiki/entities/niches/<niche>.md)
    - Visual props — target cell's corpus-validated surrogates
    - Dialogue verbatim — rewrite per target cell, preserving cadence

Step 3 — APPLY the innovation moves from
  wiki/concepts/script-adaptation/innovation-moves.md
  (default = Grain-2: keep BODY verbatim, innovate HOOK only; run the
  make-sense / function-transfer test on every borrowed atom).
  Mandatory rules:
    - Innovation must pull from corpus (never hallucinate)
    - Source's hook job and v873 coherence/tension/filter contract survive
      the port; v598's old Q6/Q8 lists apply only when their family triggers
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
  §0 decision block:
    persona / niche / audience / cell / METHOD / BODY SHAPE
    EVIDENCE PROGRAM / EVIDENCE PACKET / PACKET STATUS / PRIMARY DELTA
    SCRIPT DECISION MAP / SCRIPT LOCK / VISUAL LOCK / SOURCE VISUAL BEATS
    hook job declared + force-verb chain declared
  ## Sources — cite outside-niche reference + ≥1 cross-validating Korella
    corpus parent
  ## Ingredients — 5-column table WITH `Attached to` column (v707)
  ## Images — Image N blocks with v707 3-line binding stack (NO v604 verbose
    body line); frontmatter `visual_delta:` for chained images; v597+ canonical
    action_note shape
  ## Storyboard — Scene N blocks with `image:` / `clip_mode:` (BLEND or FRESH)
    / `speaker:` / `action_arc:` / `- **line:**` (lowercase, ~20w) /
    `- **action_note:**` (single-line prose with [Start/Mid/End beat] markers)
  ## Comprehension — structural inventory + v-rule inventory + source-led
    causal structure + persona/setting authority read
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
  Gate 12 — HOOK passes v873 coherence + tension + audience filter; apply
    v598 family-conditional checks only when their evidence trigger is present.
    Declare the carrier on one line, routing segment last:
    `HOOK CARRIER: <what it is> | kind: visible|audible|readable | delivers:
    tension|proof|interruption — <how> | reads at opening speed because: <the
    first-second cue> | next question opened: <the question> | family: catalyst
    | text-card | overlay | UNLISTED — <source-derived name>`. The family routes
    the catalyst check: family: catalyst means the action opens clip 1, while
    every other family must stage the declared carrier in Scene 1. Declaring a
    family is not an exemption.
  Gate 13 — Evidence Packet exists, every retrieval row has one of the four
    dispositions, packet/build paths point to each other, and PRIMARY DELTA
    matches exactly: `python tools/check_content_packet.py <build.md>` PASS
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
    (c) v604b structural bans — the closing body sentences (no
        trailing negatives block since 2026-06-12) ban underlying
        ANATOMICAL DEFAULTS in addition to outcomes (not just "No
        firm arm" but also "No normal human arm anatomy. No skin
        attached to the bottom of the bicep. No straight lower arm
        contour."). Forces Banana 2 to render the absence of the
        healthy default, which forces the symptom-distorted version.
  Gate 19b — v791 HOOK safe-area composition grammar (HOOK / image_1
    ONLY). The HOOK image's fenced prompt opens with ONE camera
    sentence naming the LENS + height lock (v791.2): "A vertical
    9:16 smartphone photo shot on an iPhone ultra-wide 0.5x lens
    (13mm equivalent), [view] from [distance], the lens level with
    the [person]'s raised hand", then the hero layer (NEVER
    "foreground"/"closest to the lens" — bottom-drop): "[Person]
    raises his open palm to his own [eye/chin] level and extends it
    straight toward the camera, presenting [prop + plain
    state-words] on the flat of his hand — the huge foreshortened
    hand and the [prop] sit at the very middle of the frame; his
    face right behind and just above his hand, the top of his head
    touching the top edge of the frame" -> person right behind it
    -> secondary people -> background "sharp and fully visible in
    the wide view" -> house realism block + "Aspect ratio 9:16." BANNED: thirds/grid words, crop-boundary
    negotiation, trailing zone-bans, "clearly visible" on secondary
    elements. CCTV/wide staging: action at exact center, "slightly
    closer than the cam would really allow." Close-but-small render
    -> EDIT, don't re-roll. Deep-dive: template_reference.md §v791.
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
    tag "Natural ultra-realistic colors, deep focus."].

    [Tech] [aspect ratio + resolution, e.g. 9:16, 2K output].

    (No terminal Negatives row — retired 2026-06-12. The v604
    drift-guards + v606 compositing guards + v713(d) composition-
    anti-default + v715 desk-anchor anti-default are woven into the
    [Composition] / [Subject] / [Action] sentences above. The [Tech]
    block ends the prompt — "Aspect ratio 9:16." is the final line.)
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

  Required anti-default sentences (woven into the body — no trailing
  negatives block since 2026-06-12):
    No desk visible. No [prop] on a surface. No top-down camera angle.
    No high-angle shot. No prop sinking to the lower-third. The prop
    dominates the center of the frame.

  Innovation-side: when the outside-niche source happened to use desk-
  anchored composition, DO NOT replicate the desk anchor. Re-anchor to
  the patient's body per Mode 1-5 based on the niche / prop / lens.
  Innovation preserves WHAT works (the rhetorical structure), not
  HOW the source happened to compose (desk-gravity bias propagates
  unless re-anchored).

V736 — SPECTACLE-OVER-LOGIC DISCIPLINE (apply BEFORE writing markdown):

  CORE THESIS: LLMs default to safe / logical / probable. Viral hooks
  need the OPPOSITE — unsafe / absurd / improbable. v598 power-test +
  v600 cartoon-physics enforce FORMAT but leave a loophole: "safe"
  metaphor selection (prostate = garden hose, digestion = drain,
  heart = engine). v736 closes the loophole.

  OPERATOR RULE: "If the visual metaphor makes logical sense in a
  middle-school biology class, REJECT IT. Viral hooks rely on
  Spectacle Disconnect — the prop should be viscerally interesting
  first, and a metaphor second."

  Four sub-rules (all four MUST fire on every innovate output):

  ----------------------------------------------------------------
  v736a — ANTI-SENSE METAPHOR BAN
  ----------------------------------------------------------------

  Banned-by-default first-instinct prop mappings:
    Prostate / urinary    -> garden hose / faucet / pipe / kink / drip
    Digestion / gut       -> drain / clog / plunger / toilet / blocked sink
    Heart / circulation   -> engine / motor / oil-filter / pump / valve
    Joints / arthritis    -> hinges / rusted gears / WD-40 can
    Skin / wrinkles       -> wallpaper / peeling paint / cracked clay
    Hair / scalp          -> grass on dry ground / lawn / thinning carpet
    Eyes / vision         -> camera lens / foggy windshield / dirty mirror
    Liver / detox         -> water filter / sponge-as-filter / drain trap
    Hormones / mood       -> switch / dial / thermostat / fuse-box
    Bloat / weight        -> overinflated balloon / stuffed sack

  If your first-instinct prop is on this list (or a near-neighbor),
  STOP and re-pick. Replacement criterion: the prop should be visceral
  first, metaphor second — meaning a viewer who doesn't know what the
  video is about would still stop scrolling to watch the prop be
  destroyed / squeezed / cascaded / pulled apart.

  ----------------------------------------------------------------
  v736b — TREND-GRAMMAR DECISION
  ----------------------------------------------------------------

  Start with the selected sources' own visual grammar. Use a current
  viral aesthetic only when retrieval finds relevant evidence and its
  camera/action grammar transfers to the target hook. The bank below
  is open comparison evidence, not a mandatory menu:

    ASMR soap cutting (curls of soap, satisfying slice)
    Hydraulic press crushing (industrial press flattens / explodes)
    Power-washing dirty rugs / driveways / patio furniture
    Kinetic-sand slicing (clean knife through compressed sand)
    Satisfying paint-mixing (bucket pour, marbled colors)
    Giant water-balloon pops (slow-mo membrane burst)
    Pomegranate / fruit smash (juice cascade, seeds scattering)
    Slime-pull / slime-stretch (impossible-elastic stretch, pop)
    Cake-frosting reveal (knife smooths uneven surface to mirror)
    Sponge-wring (thick murky water cascade from saturation)
    Wax-seal melt / candle-melt (controlled drip, hardening)
    Glass-shatter slow-mo (fragments suspended, light catches)

  Every sandbox concept declares:
  TREND GRAMMAR: <bank slug | UNLISTED — source-derived name | NOT
  APPLICABLE — source mechanism supplies the grammar> | evidence:
  <source> | transfer: <the exact camera/action grammar used>.

  A named trend with no evidence or no transferred grammar fails. A
  source-owned mechanism needs no pasted trend. When applicable, the
  HOOK prompt describes the transferred composition, lighting,
  framing and action rather than merely naming the trend.

  ----------------------------------------------------------------
  v736c — UNCOMFORTABLE-TEXTURE MANDATE
  ----------------------------------------------------------------

  Hero prop in HOOK MUST possess textural / messy / slightly
  uncomfortable physical state.

  Allowed texture classes:
    oozing / dripping / running / bursting / exploding / popping
    sticky / tacky / gummy / fibrous / stringy / pulpy
    gelatinous / viscous / jelly-like / foamy / frothy / bubbling
    slimy / mucousy / gloppy / fleshy / pulpy / meaty
    soaked / saturated / dripping-wet / stretchy / elastic / tearing

  Banned default-texture classes (LLM "safe" reach):
    dry plastic / bare plastic / smooth metal / polished steel / chrome
    clean glass / clear acrylic / bare wood / sanded surface
    polished stone / marble / dry paper / cardboard

  Texture rule applies to the PROP, not the persona's hands or the
  setting. Persona may wear gloves; setting may be sterile clinic.
  The prop being acted on must have texture.

  Replacement examples:
    Garden hose            -> soaked sponge violently wrung
    Stress ball            -> over-ripe persimmon bursting under thumb
    Plastic anatomical model -> raw chicken liver sliding off cutting board
    Clean ice cube         -> melting popsicle leaving sticky drip trails
    Polished metal pipe    -> honey-glazed donut squashed flat
    Dry sponge             -> soaked dishrag wrung over a pan

  Combine with v720c body-pose discipline + v716/v717 anti-normalization
  for max impact.

  ----------------------------------------------------------------
  v736d — SANDBOX-IDEATION GATE (the most critical sub-rule)
  ----------------------------------------------------------------

  Your videos/*.md OUTPUT MUST begin with a "## Brainstorming Sandbox"
  section IN THE OUTPUT FILE (NOT in chat) BEFORE the YAML frontmatter.
  The sandbox MUST contain:

    1. Five (5) radically different visual hook concepts. Each concept
       names: hero prop + triggered texture state (v736c) + action
       (v697) + TREND GRAMMAR declaration (v736b) + 1-line mapping.

    2. Each concept rated 1-10 on "Unhinged TikTok Spectacle" — 10 =
       absurd / visceral / can't-look-away; 1 = boring / corporate /
       biology-class diagram.

    3. The 3 lowest-rated (most logical / safe) concepts MUST be struck
       through with ~~text~~.

    4. The single most visceral / scroll-stopping concept MUST be
       marked **SELECTED →**.

    5. The selected concept's hero prop / triggered texture / force-verb
       and TREND GRAMMAR decision MUST match what appears in the HOOK.

  WHY mandatory in-file (not chat-side): linear token generation locks
  YOU into the FIRST plausible idea you emit. By forcing the sandbox
  INTO the output file BEFORE the markdown body begins, you commit 5
  concepts to the context window and can self-evaluate before the
  first scene block locks tone. Sandbox-in-chat does NOT work — you
  treat chat as draft and OUTPUT as final, and the OUTPUT's first
  scene-image dominates downstream attention.

  Worked sandbox example (saw-palmetto / prostate):

    ## Brainstorming Sandbox

    1. ~~Garden hose unkink — dry plastic hose, GRIP + PULL-APART
       force-verb, TREND GRAMMAR: NOT APPLICABLE — direct source
       action | evidence: source concept | transfer: grip-and-pull
       close-up, maps "kinked urethra" 1:1.
       Spectacle: 2/10 (logical, dry, boring).~~
    2. ~~Faucet drip-stop — chrome faucet, TIGHTEN force-verb,
       TREND GRAMMAR: NOT APPLICABLE — direct source action | evidence:
       source concept | transfer: mid-drip tightening close-up,
       maps "leaky bladder". Spectacle: 3/10
       (clean metal, predictable).~~
    3. ~~Drain clog clear — drain + plunger, PUSH + RELEASE,
       "satisfying clog clears", maps "obstruction lifts".
       Spectacle: 4/10 (logical drain analogy).~~
    4. Pomegranate smash — over-ripe pomegranate (oozing / bursting /
       dripping per v736c), SLAM + CASCADE force-verb, TREND GRAMMAR:
       UNLISTED — source-derived fruit-impact macro | evidence: source
       concept | transfer: tight impact-and-cascade framing, juice-cascade maps "trapped pressure
       releasing." Spectacle: 9/10.
    5. **SELECTED →** Soaked-sponge wring — kitchen sponge soaked in
       murky water (gelatinous / dripping / foamy per v736c), GRIP +
       TWIST + CASCADE force-verb, TREND GRAMMAR: UNLISTED —
       source-derived sustained wring-and-cascade macro | evidence:
       source concept | transfer: sustained two-hand wring close-up,
       cascade onto practitioner's bare hands maps "stuck pressure
       finally moving." Spectacle: 10/10 (texture + cascade +
       visible release; sustains through full 8s force-verb arc).

  CARVE-OUTS:

    - HOOK image only. Body / mechanism / RESULT / CTA scenes don't
      need sandbox treatment.
    - Select on audience fit, immediate proof/tension, source-function
      transfer and filmable action — never because a trend tag exists.
    - When the outside-niche source HOOK already passes v736a + v736b
      + v736c, sandbox MUST cite the source as one of the 5 entries
      and may select it as winner.

  PAIRING:
    v598 power-test runs AFTER v736 selection (selected concept must
    still pass Q1-Q8). v600 cartoon-physics extended by v736c from
    "magnitude" to "texture / state". v697 force-verb chain named per
    sandbox entry. v713-v720 composition discipline applied to the
    selected concept's HOOK image rendering. v621 narrative_lens:
    sandbox is GRABBING-ATTENTION (the spectacle IS the rhetorical
    move).

  ----------------------------------------------------------------
  v736.1 — DNA-FIRST AMENDMENT (sub-rules e/f/g/h)
  ----------------------------------------------------------------

  Corpus DNA extraction across 6 viral hooks (chicken-in-pot /
  honeycomb / dual-prostate-models / shirtless-strain / pickle-vs-
  belly / hanging-peanut-sack) surfaces 7 universal invariants every
  viral hook satisfies. v736 a-d closed the safe-default loophole;
  v736.1 adds e/f/g/h to enforce composition + economy.

  THE 7 INVARIANTS (constants across niches — surface variables vary):

    1. ONE symptom-bearing object dead-center
    2. Persona hands actively manipulating object
    3. Object texture wet / messy / visceral / uncomfortable
    4. Persona face visible above OR beside object, mouth mid-word,
       eyes on lens
    5. Authority setting blurred behind
    6. Object connection to symptom rhetorical not literal
    7. 8-sec force-verb arc with visible state change

  v736e — DEAD-CENTER COMPOSITION:
    Hero prop owns geometric middle of the frame, NOT rule-of-thirds
    intersection. Camera level MATCHES hero anchor height (chest for
    held-aloft / belly for distended belly / brow for wrinkle-macro /
    lumbar for back-symptom). NEVER top-down or high-angle.

    Required [Composition] phrase pattern:
      "[hero prop] fills the immediate center-foreground, dominating
       the middle of the image"
      OR
      "[hero prop] fills the immediate center of the frame, occupying
       60% of the frame's vertical center axis"

    Required Negative:
      "No prop sinking to the lower-third. No rule-of-thirds offset
       — symptom occupies geometric center."

  v736f — ACTIVE-HANDS MANDATE:
    Persona's hands actively manipulate the hero object. Static hold
    fails. Required active-verbs (one or more): grip / squeeze / lift
    / wrap / hang / measure / point / press / pierce / shake / wring
    / scrape / smooth / wind / inflate / pull-apart.

    The active manipulation IS the spectacle anchor that triggers
    Invariant 7 visible state change.

    Required [Subject — Host] phrase: "both hands [active-verb] the
    [hero prop]" OR "[hand position] [active-verb] [hero prop]".

    Required Negative: "No static hold — persona's hands MUST
    [active-verb] the [hero prop]."

  v736g — FACE-ABOVE-OR-BESIDE-OBJECT:
    Persona face visible at chest-up framing — either ABOVE prop
    (single-subject, frames 1, 2, 3, 6 of corpus) OR BESIDE on
    viewer-edge (two-shot, frames 4, 5 of corpus). v713a partial-
    visibility override compatible (face cropped to eyebrow-to-chin
    still satisfies v736g). Persona-cropped / hidden / displaced
    FAILS.

    Required [Composition] phrase:
      "the main character's face is sharply visible just above the
       prop at chest-up framing"
      OR
      "the main character's face is sharply visible at chest-up
       framing on the viewer-[left/right] of the prop"

    Required Negatives: "No persona crop on the face. No persona-
    hidden-behind-prop. No persona-displaced-to-corner."

  v736h.1 — SHORT-PROMPT ECONOMY (the most-violated sub-rule):

    LENGTH: simple one-action HOOK 80-150 words; complex two-person
    or A/B frame 120-200. WARN above 200. FAIL above 250. The old
    200-350 target and 400-word ceiling are retired for new/modified
    Image prompts because too many instructions dilute the hero.
    See wiki/generation/nano-banana-prompting.md line 194 ("long
    text + photos fight each other").

    SIX-BLOCK ORDER: camera -> hero -> one action/contact point ->
    character relationship -> max-three-layer eye path -> natural
    smartphone look + aspect ratio. Use at most three important
    visual traits per subject and one brand marker unless brand is
    the hero. If deleting a phrase does not change the visible frame,
    delete it.

    HARD BANS inside Image prompt body:

      - Meta-commentary about rules ("per Invariant 1" / "per v736e"
        / "per v722"). Audit tags belong in lint output, not prompt
        text.
      - Beat structure ("[Start beat 0-2s]" / "[Mid-clip beat]" /
        "[End beat 6-8s]"). Beats describe motion across time —
        Banana 2 renders ONE still frame.
      - Temporal language ("Across 8 seconds" / "throughout" /
        "during the clip" / "then [verb] then [verb]"). Image is one
        frame — describe ONE state.
      - Splitting dual / triple props into separate [Subject —
        Symptom A] + [Subject — Symptom B] blocks. Single [Subject
        — Symptom] block treats them as ONE composition; split
        invites Banana 2 to render them MORE separated, losing
        cohesion. Frame 3 of corpus (dual prostate models) is ONE
        Subject block.
      - Over-described persona blocking past one sentence. Banana 2
        just needs "holds X and Y at chest height with both hands"
        — not "stands behind in midground, left hand cupping from
        below, right hand cupping from below, both lifted to chest-
        level facing the lens."
      - Wardrobe / upload / framework callouts in body prose
        ("Persona identity carried by upload (no inline wardrobe
        per v722)"). Audit-only.
      - Negative-block past 10 clauses. Past ~10 the "no green
        elephant" hallucination class fires. Pick the 5-8 negatives
        Banana 2 keeps violating in this niche.

    IMAGE vs SCENE SEPARATION (the structural fix):

      Image prompt body -> Banana 2 still frame (LEAN, six blocks,
        max three depth layers, no meta, no beats, <=250 words).
      Scene action_note + line + action_arc -> Veo motion clip
        (VERBOSE-OK with beats + force-verb chain + lip-sync
        discipline, no ceiling).

    For BANANA 2 STILL: "exaggerated shocked expression" outperforms
    "mouth open mid-utterance" because Banana 2's training prior is
    stronger on staged expressions. v721 lip-sync language ("mouth
    open mid-utterance, eyes locked to lens") is for VEO RENDER lip-
    sync — lives in Scene action_note, NOT Image body.

    DNA INVARIANTS ENFORCED BY CONTENT, NOT BY LABELS:

      Invariant 1 (dead-center) -> "fills the immediate center-
        foreground, dominating the middle". DROP: "(NOT viewer-left
        third, NOT viewer-right third — per Invariant 1, occupying
        60% of vertical center axis)".
      Invariant 4 (face above) -> "face is sharply visible just
        above the prop". DROP: "(per Invariant 4)".
      Invariant 5 (background blurred) -> "background fully blurred".
        DROP listing every blurred element.

    PRE-OUTPUT GATES (v736h.1, mandatory):

      gate 1 — word-count check on each "### Image N" body:
        Count words inside each fenced Image prompt.
        Expect: simple 80-150w; complex 120-200w; WARN >200w;
        FAIL >250w.

      gate 2 — meta-commentary ban inside Image bodies:
        grep -nE '\(per (Invariant|v[0-9]+)' videos/<file>.md
        Expect: zero hits inside "### Image N" blocks.

      gate 3 — temporal-language ban inside Image bodies:
        grep -nE '\[Start beat|\[Mid-clip beat|\[End beat|Across \d+ seconds|throughout the clip' videos/<file>.md
        Expect: hits ONLY in "### Scene N" action_note blocks,
        NEVER in "### Image N" blocks.

  Validated 2026-05-14 via dual-prostate HOOK A/B test on Banana 2:
  lean ~250w original beat bloated ~700w rewrite on Banana 2 fidelity
  (dual organs cohesive vs separated; contrast clear vs diluted;
  dripping fluid rendered vs lost). v736h.1 codifies the lesson.

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

PRE-FLIGHT CHECKLIST (v738 — HARDENED 2026-05-16 to STRICT REJECT GATE — mandatory thinking-prelude before artifact emission):

STRICT ENFORCEMENT: artifact missing ## Pre-Flight Checklist is REJECTED. Re-emit with checklist FIRST before any markdown body. No exceptions. Operator-side grep gate enforces this rule.

visual_delta MORPHOLOGICAL-CHANGE MANDATE (v718d): every - **visual_delta:** field MUST contain BOTH kinematic action AND morphological state-change descriptor. Pure-kinematic deltas (just "hand pours liquid") are REJECTED — you glossed over the morphological delta. Required pattern: "[kinematic action] + [now contains X from prior step OR transformed from Y to Z OR primary_change_axis: Surface/Structural/Volume/Color]". Operator-side grep gate at code/template_reference.md §"v738 Pre-output gate" enforces this.


  Before emitting the final ## Brainstorming Sandbox / ## Ingredients /
  ## Images / ## Storyboard sections, FIRST output a
  ## Pre-Flight Checklist block declaring rule resolutions for THIS
  source + cell + niche. The checklist primes context with explicit
  collision resolutions BEFORE markdown headers lock in.

  Mandatory checklist sections (full spec at code/template_reference.md
  §"v738 — Pre-Flight Checklist"):

    1. Composite layout check (v737 + v698A.1 Q2)
       — PiP / green-screen / corner-inset present? → v737 decoupling
       protocol; strip persona from b-roll Image bodies; route through
       v698A voiceover-paired with shared anchor.

    2. State-evolution + short-line check (v580 + v704 + v644)
       — Recipe / Day-N / progressive-symptom chain requiring new image
       per step? + verbatim line per step <12w? → v580/v644 carve-out:
       keep separate scenes; USE - **pad:** bullet to extend Veo TTS
       to ~20w combined; do NOT merge scenes.

    3. Voiceover-paired detection (v698A.1 Step 1 decision tree)
       — For each shot: Q1 voiceover overlap → Q2 face-as-PRIMARY
       (with PiP carve-out per v737) → Q3 lip-sync. List voiceover-
       paired scenes + anchor image declaration.

    4. Sandbox requirement check (v736d)
       — Output type = innovate → ## Brainstorming Sandbox REQUIRED at
       top per v736d. Five entries, three struck-through, one SELECTED.

    5. Vocabulary safety check (v702 + v615 + v693 + v722) —
       OUTPUT-TYPE BRANCH (HARDENED 2026-05-15):

       This is an INNOVATE artifact (videos/*.md) -> APPLY v702
       (RELAXED 2026-05-15 clinical-register carve-out). Walk the
       v702 4-step decision tree per code/template_reference.md.
       Class 1 (sexual-action verbs adjacent to anatomy) + Class 2
       (slang body-part words in image prompt fenced bodies) -> swap.
       Class 3 (clinical anatomical terms alone — prostate / testes /
       penis / vagina / uterus / etc.) -> ALLOWED.

       v615 / v693 / v722 still apply.
       — Forbidden tokens? em-dashes? lowercase? wardrobe in
       Ingredients only?

    6. Morphological Delta Declaration (v738.1 / v718d / v718e —
       REPLACES Anti-Platonic Gate single-state check, HARDENED
       2026-05-17 from kinematic-over-morphological blind spot
       surfaced in tongue-cleanse decode failure):

       For EVERY hero prop in this artifact's Image blocks, declare
       per-prop block:

         Hero Prop: <prop name verbatim from Ingredients table>
         Image(s): <comma-separated image_N tokens>
         Scene(s): <comma-separated scene_N tokens>
         t=0 (Start State): <explicit texture / color / volume /
           structural integrity at frame_anchor — BEFORE state at
           peak severity>
         v736c Texture Check (NEW 2026-05-18, v738.1 amendment): <MUST
           name uncomfortable texture class from v736c catalog:
           oozing / bursting / sticky / fibrous / gelatinous /
           dripping / foamy / slimy / fleshy / pulpy / viscous /
           soaked / stretchy / gloppy / grimy / coated / crusted /
           encrusted / hyperemic / edematous / inflamed / pendulous
           / drooping / sagging / bloated / pustular / blistered /
           scaly / weeping / suppurating / atrophied — pick closest
           match. May be "n/a (static prop, no morphology)" only
           when Delta Axis == NONE>
         t=end (Terminal State): <explicit texture / color / volume /
           structural integrity at end of scene's clip — AFTER state
           at peak resolution>
         Delta Axis: <Surface/Texture | Structural Integrity |
           Volume/Shape | Color/Illumination | NONE>
         Carry Mode: <within-clip | within-clip-end-frame |
           multi-clip-blend | cross-image | both>
         Magnitude: <COMPLETE | PARTIAL | MINIMAL | NONE> per v589

       HARD GATE (all REJECT):
         - Delta Axis != NONE AND t=0 == t=end → REJECT (contradiction).
         - Delta Axis != NONE AND t=end relies on generic kinematic-
           only verbs without morphological state-change descriptor
           → REJECT (kinematic-over-morphological blind spot).
         - At least ONE hero prop in HOOK + diagnostic-reveal scenes
           MUST have Delta Axis != NONE. Viral hooks rely on
           ABNORMALITY + visible transformation. If all hero props
           declare Delta Axis = NONE on HOOK, re-amp via Pattern 21
           + v716 + v717 + v719 + Pattern 23 diagnostic-anchor stack
           until at least ONE prop transforms measurably.

       v604.1 PAIRING: when Delta Axis != NONE AND Carry Mode =
       within-clip | both, frame_anchor MUST point at t=0 (BEFORE
       state), NOT t=end (AFTER state). Annotate with "(BEFORE-state
       anchor)" tag. Veo cannot animate backward.

       v586.1 + v717.1 IMAGE BODY DISCIPLINE: when Section 6 declares
       Delta Axis != NONE for a prop AND narrative_lens ∈
       {AUGMENTED-SYMPTOMS, HEALER-SHOWING-CURE}, [Subject — Symptom]
       block opener MUST name the prop's t=0 state at peak severity.
       Banned: "An anatomical tongue model." Required: "An anatomical
       tongue model coated in a thick, dry, pale-yellow film,
       papillae buried under the grime layer."

       v718g NEW REQUIRED FIELD: when this scene's hero prop Delta
       Axis != NONE AND Carry Mode = within-clip | both, the Scene
       block MUST carry a - **visual_delta_within_clip:** field
       pairing TRANSFORMATIVE verbs (v697.1) with morphological
       state-change descriptors (v718d 4-axis vocabulary).

       WHY: forcing the author to write t=0 + t=end side-by-side
       BEFORE generating markdown body triggers contrast-recognition.

    7. Action-Consequence Coupling (v718e, NEW 2026-05-17) — for
       EVERY scene whose primary_change_axis != NONE, the
       - **action_note:** field MUST satisfy three coupling rules:

       v718e-1: Mid-clip beat AND End beat force-verbs paired with
         morphological consequence in the SAME SENTENCE. Pattern:
         "[force-verb] the [prop], [transformation-verb] the
         [prop-feature] into [end-state]".
         Wrong: "the liquid cascades over the tongue, coating the
                 grime."
         Right: "the liquid cascades over the tongue, washing away
                 the grime in a single continuous sweep."

       v718e-2: [End beat 5-8s] clause MUST manifest the
         intrinsic_state_end declared per v718d. Cannot end on
         transient state. v589 magnitude (COMPLETE / PARTIAL /
         MINIMAL) propagates.

       v718e-3: Banned static-contact verbs in Mid + End beats:
         coating / covering / pooling on / resting on / touching /
         sitting on / placed on / lying on / falling on / settling
         on / landing on / arriving at / meeting / contacting.

         Required transformation verbs by axis:
           Surface/Texture       washing away / dissolving /
                                 scrubbing clean / clearing /
                                 revealing / stripping
           Structural Integrity  smashing open / shattering /
                                 splitting / bursting / tearing /
                                 fracturing / exploding
           Volume/Shape          melting / shrinking / deflating /
                                 flattening / draining / collapsing
                                 inward / lifting tight
           Color/Illumination    flushing red / glowing bright /
                                 dimming dark / blanching pale /
                                 igniting

         Operator-side Python gate at code/template_reference.md
         §"v718e Pre-output mechanical gate" enforces v718e-3.
         Run before ship; expect zero v718e FAIL output.

         Carve-out: primary_change_axis == NONE → v718e N/A.

    8. Composition discipline check (v713 + v715 + v716/v717 + v720 +
       v736e/f/g/h)
       — HOOK: dead-center + active hands + face above-or-beside +
       body <=250w. B-roll: pure (no persona). Anchor: role +
       cast + chest-up + open-palm.

    9. Image cardinality + use audit (v594 + v580)
       — Declared images count = ?  Referenced images count = ?
       Zero unused images?

    10a. Veo 3.1 Structural Delta Decision Tree (v718h-A/B/C + v580.2
         + v718i, NEW 2026-05-17, render-test validated) — for EVERY
         scene with Section 6 Delta Axis != NONE, choose authoring
         path based on Delta Axis:

         Surface/Texture | Color/Illumination → Option A (single-
           clip Veo with VFX Wipe Override per v718h-A).
         Volume/Shape | Structural Integrity → Option C (Veo native
           end-frame interpolation per v718h-C + v718i, LIVE
           2026-05-18, RECOMMENDED DEFAULT — single Veo clip with
           cfg.last_frame native interpolation) OR Option B (multi-
           clip blend per v718h-B, FALLBACK when single-clip Veo
           render budget unavailable) OR Option A as escape hatch
           with explicit acknowledgement.

         OPTION B (Multi-Clip Blend, v718h-B + v580.2 — RECOMMENDED
         for Structural/Volume axes; uses existing platform features,
         no parser changes needed):

           1. Author TWO Banana 2 Images per v580.2:
              Image K   = t=0 BEFORE state (frame_anchor at t=0,
                          annotated "(BEFORE-state anchor — paired
                          with image_K+1 for v718h-B/C)")
              Image K+1 = t=end AFTER state (reference_image:
                          image_K, visual_delta declares
                          morphological transformation per v718e
                          coupling, frame_anchor at t=end,
                          annotated "(AFTER-state anchor — paired
                          end frame for v718h-B/C)")
           2. Author TWO sequential Scenes bound to one source clip
              duration:
              Scene N   = image: image_K, clip_mode: fresh,
                          transition: blend, target_duration_s ≈
                          source_clip_duration / 2
              Scene N+1 = image: image_K+1, clip_mode: blend,
                          transition: cut, target_duration_s ≈
                          source_clip_duration / 2
           3. Lines split across the pair OR placed entirely on
              Scene N+1 with Scene N silent.
           4. Renumber downstream scenes (every scene index > N
              gets scene_index + 1).
           5. Pre-Flight Section 6 declares Carry Mode = multi-
              clip-blend + Image(s) field lists both image_K +
              image_K+1 + Scene(s) field lists both scene_N +
              scene_N+1.
           6. Pre-Flight Section 8 audit table has TWO rows for the
              paired pattern.
           7. Each Scene's action_note carries the segment of
              transformation that happens in its half.
           8. Veo prompts for Scene N + Scene N+1 carry NORMAL Veo
              prompts (NOT VFX Wipe Override — morphological anchor
              is the Banana 2 Image, not text-prompt steering).

           Render expectation: ~95%+ success rate (vs ~60-70% for
           Option A on structural axes); CapCut crossfades seam at
           midpoint into smooth morphological transition.

         OPTION C (Veo Native End-Frame Interpolation, v718h-C +
         v718i — LIVE 2026-05-18, RECOMMENDED DEFAULT for Structural/
         Volume axes): SINGLE Veo clip with TWO Banana 2 Images via
         explicit end-frame binding. (1) Author Image K (t=0 BEFORE)
         + Image K+1 (t=end AFTER) per v580.2 (same as Option B).
         (2) Author SINGLE Scene N with image: image_K +
         end_frame_image: image_K+1 + target_duration_s: full clip
         duration. clip_mode: fresh, transition: cut. (3) Pre-Flight
         Section 6 Carry Mode = within-clip-end-frame. (4) Section 8
         has ONE row for the scene. (5) Veo prompt: NORMAL (NOT VFX
         Wipe — morphological anchors are the paired Banana 2
         Images). Platform parser binds end_frame_image: → ImageNode.id
         → Clip.end_frame_image_node_id → worker explicit_end_frame
         override → veo_generator cfg.last_frame at line 2605.
         Pros: HALVES Veo render cost vs Option B (1 clip vs 2);
         continuous interpolation, no CapCut seam. Cons: Veo
         interpolation may deviate from action_note semantics
         slightly (mitigated by text-prompt steering).

         OPTION A (VFX Wipe Override, v718h-A — single-clip escape
         hatch; required for Surface/Texture + Color/Illumination
         axes; escape hatch for Structural/Volume) — for EVERY Veo
         Final Prompt body where Section 6 declares Delta Axis !=
         NONE AND Carry Mode = within-clip | both, apply 5-step
         protocol:

         STEP 1: Static camera lock + zero start-state description.
         STEP 2: Temporal Forcing in first action sentence
           ("IMMEDIATE ACTION: Right from the first frame, ...").
         STEP 3: Action-Consequence Coupling in one continuous
           paragraph (motion + result + replacement target chained).
         STEP 4: Axis-matched verb framing —
           Surface/Texture: WASH/DISSOLVE/SCRUB/REVEAL.
           Color/Illumination: COLOR-SHIFT/TRANSFORMS-INTO/SATURATES.
           Volume/Shape: REDUCES/DRAINS/SHRINKS/DEFLATES.
           Structural Integrity: MUST ESCALATE to VFX WIPE OVERRIDE
             ("digital VFX wipe" + "ERASING the 3D geometry" +
             "REPLACED in real-time"). Surface verbs FAIL on 3D
             structural pathology — Veo physics prior treats raised
             geometry as solid; VFX framing invokes video-editing
             prior which allows real-time geometry replacement.
         STEP 5: TERMINAL STATE LOCK ("TERMINAL STATE: By clip-end,
           ...") + temporal negatives ("no delay, no hesitation, no
           holding the start frame, no shape-shifting lag") +
           start-state pixel negatives ("no [feature] remaining at
           clip-end, no [feature] surviving the [catalyst]") +
           structural negatives if applicable ("no 3D [feature]
           remaining, no fluid flowing over the [feature]").

         HARD GATE: every Veo Final Prompt body whose scene has
         Section 6 Delta Axis != NONE MUST satisfy 5-step protocol.
         Full deep-dive at code/template_reference.md §"v718h — Veo
         3.1 I2V Temporal Consistency Override".

    10. Per-scene morphology audit (v738.2 / v718d / v718e / v697.1,
        NEW 2026-05-17) — required table, one row per Scene:

        | Scene N | Hero Prop(s) | Delta Axis | t=0 state (concise) | t=end state (concise) | TRANSFORMATIVE force-verb(s) in action_arc (v697.1) | Resolution token in End beat (v718e-2) |
        | 1 | tongue | Surface/Texture | coated grey-brown | clean pink | POUR + SCRUB | washed away |
        | 5 | book (CTA) | NONE | book held | book held | LIFT + PRESENT (NON-TRANSFORMATIVE) | n/a |

        HARD GATE:
          - Every Scene N in ## Storyboard MUST have a matching row.
          - Row's Delta Axis MUST match Section 6 declaration for the same prop.
          - Row's TRANSFORMATIVE column MUST contain ≥1 v697.1 TRANSFORMATIVE-class verb when Delta Axis != NONE.
          - Row's Resolution column MUST contain ≥1 v718e-2 resolution token when Delta Axis != NONE.
          - Rows with Delta Axis == NONE allow n/a + MUST use NON-TRANSFORMATIVE verbs only.

        v697.1 TRANSFORMATIVE subclass: POUR / CASCADE / SPRAY /
        SLAM / SQUEEZE / DROP / SMASH / SCRUB / WIPE / TILT-POUR /
        STRAIN / DRAIN / DISSOLVE / SHATTER / MELT / BURST /
        DEFLATE / COLOR-SHIFT / WASH-AWAY / ...
        v697.1 NON-TRANSFORMATIVE subclass: HOLD / LIFT-PRE /
        PRESENT / GESTURE-FORWARD / OPEN-PALM / POINT-TO-LENS /
        END-LOOK / END-HOLD / NOD / FACE-LENS / GRIP-STEADY / ...

  The checklist is operator-facing audit material — it sits ABOVE the
  ## Brainstorming Sandbox block in the output file. Platform parser
  ignores ## Pre-Flight Checklist (parser anchors are
  ## Brainstorming Sandbox / ## Ingredients / ## Images / ## Storyboard
  / ## Veo 3.1 Final Prompts / ## Comprehension / ## Sources).

  Skip pre-flight ONLY for trivial single-scene videos (one HOOK + one
  CTA, no recipe chain, no PiP).

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
