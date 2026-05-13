#!/usr/bin/env bash
# decode_bundle.sh — concat the canonical decode-bundle files + pipe to clipboard.
#
# Usage:
#   ./code/decode_bundle.sh [source-mp4-path]
#
# Example:
#   ./code/decode_bundle.sh raw/02_healthylifesage_DX7iVuRMzUM.mp4
#   ./code/decode_bundle.sh   # without arg — bundles only the prompt context, operator uploads MP4 separately
#
# What it does:
#   - Concatenates the 3 canonical decode-bundle files (per wiki/meta/lift-bundle.md)
#   - Pipes the concatenation to the system clipboard
#   - Operator pastes the bundle into any LLM + uploads the source MP4 + a
#     one-line task prompt: "decode this video"
#
# The bundle is transient (clipboard only, never committed). The 3 canonical
# files remain single source of truth. Decode bundle is smaller than lift
# bundle because decoding is observation, not authoring.
#
# Bundle list documented in wiki/meta/lift-bundle.md and must stay in sync.

set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE_MP4="${1:-}"

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
    echo "[decode_bundle] WARNING: no clipboard tool found" >&2
    echo "[decode_bundle]          dumping bundle to stdout instead" >&2
fi

# Decode bundle file list — must match wiki/meta/lift-bundle.md decode-bundle table
# 2026-05-11: expanded 3 → 6 files. Pre-expansion decoder couldn't classify hook
# family (no hook-patterns.md), persona archetype (no persona-map.md), or v-rule
# version (no conventions.md). Added these three to fix systematic gaps in
# decoded artifacts (verified via JUPI gut-health decode 2026-05-11).
BUNDLE_FILES=(
    "code/template_new_format.md"
    "code/template_reference.md"
    "wiki/meta/decode-grammar-checklist.md"
    "wiki/mechanics/hook-patterns.md"
    "wiki/persona-map.md"
    "wiki/patterns/conventions.md"
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
# DECODE BUNDLE — generated $TIMESTAMP

You are about to decode a viral video into a v521.1 -> v597 compliant
raw/decoded_*.md artifact. The decode is OBSERVATION (faithful capture
of what the source IS) — not authoring.

Required output discipline:
  - v578 / v585 / v588 pipeline: dialogue from whisper transcription;
    motion classified per shot via Farneback optical flow; dense per-shot
    frame sampling at start/mid/end + 5 dense triggers when v588 conditions
    fire (duration>3s, motion>0.7, state-change verb, start/end signature
    differ)
  - v586 six-block image grammar: every image description follows
    Subject / Composition / Action / Location / Style / Tech
  - v587 reproduction-ready artifact: emit ## Comprehension layer +
    ## Veo 3.1 Final Prompts (per clip)
  - v589 absolute-magnitude grammar: COMPLETE state arcs use absolute
    language ("completely melts", "fully revealed") — no hedge words
    ("dramatically", "mostly", "almost") when source shows complete change
  - v589.1 semantic chain-binding: chained images use "Use the prior-scene
    reference image to preserve [setting], [lighting], [anchor props]..."
    NOT "Use Image K..." (legacy form)
  - v593 strict-header parser: ### Image N + ### Scene N must end after
    the integer; action_note is single-line prose; no h4 sub-scenes
  - v594 image cardinality (UNIVERSAL): emit M image descriptions for N
    PySceneDetect shots where M <= N. Multiple shots that share
    composition (same setting + blocking + camera framing) collapse into
    ONE image. Per-shot motion / dense-frame / dialogue analysis stays
    1:1 in manifest.json (analysis units); only ## Images consolidates
    (description units).
  - v595 LLM-agnostic Stage 4d: any vision-capable LLM is a valid Stage
    4d provider; this prompt + bundle works for Claude / Gemini / GPT-4o
    / LM Studio / Ollama / OpenRouter / human-walk template.

Read all 3 canonical bundle files below. Then walk the source video's
PySceneDetect shots, extract dense frames per v588, fill the schema in
markdown per template_new_format.md, consolidate per v594, and output
the decoded markdown.

Pipeline upstream of this prompt (already run by the operator if
following the standard flow):
  Stage 1: ffmpeg audio extraction        -> audio_16khz_mono.wav
  Stage 2: faster-whisper transcription   -> transcript.json
  Stage 3: PySceneDetect + Farneback      -> shots.json + motion.json
  Stage 4: dense per-shot frame sampling  -> frames/shotNN_*.png
  Stage 4d: this prompt + the LLM's vision capability -> stage4d.json
  Authoring: this prompt's output         -> raw/decoded_<source-id>.md

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

    if [[ -n "$SOURCE_MP4" ]]; then
        cat <<EOF

================================================================================
# SOURCE VIDEO: $SOURCE_MP4
================================================================================

The MP4 is at the path above. Upload it separately to the LLM (LLMs cannot
ingest binary content via text bundles). For Claude in-session, dense
frames have already been extracted into _decode_tmp/<source-id>/frames/
by the v579 pipeline; walk those PNG paths via the Read tool.
EOF
    fi

    cat <<'EOF'

================================================================================
# TASK
================================================================================

Decode the source video into a v521.1 -> v604 compliant raw/decoded_*.md.

V604 — DECODE-PROMPT ACCURACY (NEW 2026-05-06)
==============================================

Apply ALL of:
- v594 image cardinality consolidation (M images for N shots, M <= N)
- v586 six-block image grammar
- v587 Comprehension + Veo Final Prompts sections
- v589 absolute-magnitude grammar for state-evolution clips
- v589.1 semantic chain-binding for chained images
- v603 style lock + prose discipline (iPhone-UGC anchor every image)
- v604 frame-locked decode prompts (NEW — see below)

V604 NEW FIELDS PER IMAGE (decode-side):

[a] frame_anchor — timestamp of the source-video key frame this image
    describes. Format: "0.5s", "12.0s", "106.0s". This locks each image
    to a single frame from the source, NOT to a generic scene-idea.

    Example:
      ### Image 1
      - **frame_anchor:** 0.5s
      - **reference_image:** none

    Decode-prompt body should reference the timestamp:
      "At 0.5s, he holds a wounded foot upright in the center of frame..."
    NOT:
      "A clinician shows a symptom..."

[b] visual_delta — for chained images (reference_image set), the
    visual_delta field names ONLY the change from the parent image.
    Body prose then becomes:
      "Use image_K as the exact base frame. Keep everything from
       image_K identical. Only change: <visual_delta value>."

    This is much stronger than rewriting the whole scene. The model
    gets a clean signal: preserve everything, change one thing.

V604 CONTINUITY-CHAIN DETECTION (CRITICAL):

When deciding whether to chain Image N to Image N-1, check these
visual-continuity criteria:
  - same person?
  - same clothes?
  - same room?
  - same camera angle?
  - same prop table / surface?
  - only object/action changes?

If ALL match -> CHAIN it. EVEN IF dialogue moves to a new point
(explanation -> product reveal, recipe step -> CTA, problem -> solution).

THE TRAP: trusting dialogue-beat grouping over visual continuity.
Decoders frequently treat "talking-head explanation" and "talking-head
product reveal" as separate images because the dialogue topic changes
— but visually they are the same setup, so they should be chained.

V604 UNIVERSAL PROMPT-DISCIPLINE (decode + generate both):

[c] IMAGE PROMPT = STILL FRAME ONLY. Motion goes ONLY in action_note.
    No motion verbs in image prompt body ("captured at", "frozen at",
    "mid-action", "PIVOTING from"). Banana 2 generates photographs,
    not action frames. Mixed motion makes generators invent weird poses.

[d] CAMERA LOCK SPECIFICITY beyond v603 generic style line. Per-video
    decoded artifact should also lock specific anchors:
      - vertical or horizontal aspect?
      - tripod or stable handheld?
      - exact framing crop (chest-up / head-and-shoulders / full-torso)
      - camera height (above desk / at eye level / low-angle)
      - subject position in frame
      - what's at the bottom of frame (desk edge / counter)
      - background characteristics (warm wood / white clinical / honey-oak)

    "iPhone HDR daylight" alone is too broad. It can create a different
    room. Lock the camera to the actual source-video anchors.

[e] NEGATIVE-CONSTRAINT DISCIPLINE. Every Image prompt body must close
    with explicit DO-NOT statements that prevent generator drift,
    AFTER the v603 closing tag "iPhone HDR colors, deep focus.":
      "No lab coat. No stethoscope. No hospital room. No extra
       products. No recipe ingredients. No dramatic cinematic lighting.
       No background change."
    Adapt to the niche/persona. Anchor against common drift failures.

[f] VIEWER-LEFT / VIEWER-RIGHT convention. Generators confuse "left"
    and "right" (subject-perspective vs frame-perspective). Always use
    "viewer-left" and "viewer-right" to anchor perspective to camera POV.
      FORBIDDEN: "her left hand POINTS at the reading"
      REQUIRED:  "her gloved hand on the viewer-left side POINTS at..."
    Universal — applies to decode prompts, generate prompts, action_notes.

PRE-OUTPUT VALIDATION:

  YES every Image block has frame_anchor: field with timestamp?
  YES every chained Image has visual_delta: field naming only-the-change?
  YES continuity-chain check passed (same setup -> chain regardless of
      dialogue beat change)?
  YES image prompt body has STATIC pose only (no motion verbs)?
  YES camera lock specificity beyond generic v603 style line?
  YES negative-constraint DO-NOT block at end of every image prompt?
  NO bare "left" / "right" — replaced with "viewer-left" / "viewer-right"?

V605 — DECODER ANTI-TEMPLATE-BIAS + PROP-TRACKING MATRIX (NEW 2026-05-06)
=========================================================================

Source: 2026-05-06 Gemini decode session diagnosed template-bias
failure. Decoder placed Rosabella bottle ON DESK because nuri-saffron
corpus pattern says so — actual source video shows bottle HELD IN
HAND. AI models are probability engines that fill VLM-data gaps with
statistical priors. v605 forces explicit citation OR explicit gap-flag.

[a] ANTI-TEMPLATE-BIAS — FLAG GAPS, never fill with corpus prior

When Stage 4d VLM data has a gap about a prop's position or handling:
  1. FLAG the gap explicitly with HTML comment in the decoded artifact:
     <!-- VLM-GAP: bottle position not visible in dense frames at
          {timestamps}; mid_state describes only persona torso. -->
  2. Provide best-effort description sourced from visible frames with
     confidence annotation in prop_position field
  3. NEVER silently substitute a corpus template default ("bottle on
     desk because that's how nuri-saffron does it"). Corpus priors are
     EVIDENCE OF WHAT WORKS, not EVIDENCE OF WHAT THIS VIDEO SHOWS.

The position of the product MUST be explicitly sourced from Stage 4d
VLM JSON (start_state / mid_state / end_state). If the VLM data does
not explicitly state where the bottle is, do not invent a composition.

[b] PROP-TRACKING MATRIX — explicit prop_position field per product image

Every Image with product_image: field set MUST also have prop_position:
field declared, answering:
  1. Is product INTERACTING WITH ENVIRONMENT (on desk / counter / shelf)?
  2. Or INTERACTING WITH PERSONA (held in viewer-left hand / viewer-
     right hand / both hands)?
  3. If held: at what height (chest / chin / waist / above-head) and
     orientation (label-forward / label-back / vertical / horizontal)?
  4. The matrix answer must come from VLM frame data (cite the
     timestamp / state field), NOT corpus prior.

Format:
  ### Image 5
  - **frame_anchor:** 106.0s
  - **reference_image:** image_4
  - **product_image:** the Rosabella Beetroot bottle
  - **prop_position:** held in viewer-left hand at chest height,
      label-forward to camera, wordmark squared to lens, fingers
      wrapping cap top (sourced from VLM mid_state at 106.0s)
  - **visual_delta:** Rosabella Beetroot bottle enters frame on
      viewer-left side, held at chest height by blue-gloved left
      hand, label-forward; viewer-right hand gestures next to bottle.
  - **Image prompt:** [body prose]

[c] PROP-AS-SUBJECT priority for product-reveal scenes

When an image has product_image: set, body prose MUST allocate:
  - 60% on prop handling (how product is held, manipulated, positioned,
    presented; hands relative to product, label orientation, height,
    lighting on bottle)
  - 40% on persona pose (eye-contact, body language, expression)

When the prop is in frame, the product IS the subject of the photograph.
Persona is secondary anchor.

PROP-LED format — name the prop in the FIRST SENTENCE of body prose:
  WRONG (persona-led): "The main character is seated at his desk, eyes
    locked to camera. The Rosabella bottle is on the desk in front of him."
  RIGHT (prop-led): "The Rosabella beetroot bottle is held up at chest
    height in his blue-gloved viewer-left hand, presented directly toward
    the lens, label-forward, wordmark clearly readable. He is seated at
    his desk with eyes locked to camera."

Banana 2 prioritizes the subject named first. Lead with the prop.

[d] STRICT VLM ACTION_ARC SOURCING (downstream pipeline rule)

For every prop_position claim, cite which VLM frame timestamp/state
field was the source. For Claude in-session decodes (v595 default),
walk dense frames per shot via Read-tool PNG support and document the
frame audit trail explicitly. The frame audit trail is the anti-bias
receipt.

PRE-OUTPUT VALIDATION (v605):

  YES every Image with product_image: has prop_position: field?
  YES every prop_position: cites a VLM timestamp / state field?
  YES VLM-data gaps flagged with <!-- VLM-GAP: ... --> comments?
  YES body prose for product-reveal images is PROP-LED (prop named
      in first sentence)?
  YES ~60% description allocation to prop handling, ~40% to persona?
  NO references to corpus templates ("nuri-saffron pattern",
     "standard product-anchor desk shot") as source of prop placement?

If any wrong, FIX before emitting.

V606 — PRODUCT COMPOSITING / LIGHTING INTEGRATION (NEW 2026-05-06)
==================================================================

When decoding a source video that has product-bearing scenes, the
decoded artifact's image prompts must include v606 compositing
directives so future generations don't produce photoshopped-looking
output. Default Banana 2 with uploaded product reference produces:
oversized + self-lit + hard-edged + center-stage + shadow-less +
occlusion-less = photoshopped look.

For every Image in the decoded artifact with product_image: set,
include a compositing paragraph in the body prose (before the v603
closing tag) covering all 6 directives:

[a] SCALE: realistic supplement-bottle scale (~5 inches), anchored
    to a scene element observable in the source video (palm size /
    torso fraction / glass height comparison)
[b] LIGHTING: bottle lit by the scene's actual light source at
    actual color temperature (cite source-video lighting from VLM
    pass — warm window-soft / cool clinical LED / outdoor daylight)
[c] CAST SHADOW: shadow direction matching source-video light
    direction, with softness and length specified
[d] PERSPECTIVE: bottle angle matching source-video camera angle
    (cite the actual camera height / angle observed)
[e] SURFACE CONTACT / GRIP: explicit contact point — flush against
    desk surface OR fingers visibly wrapping bottle, palm contact
[f] NATURAL OCCLUSION: foreground element partially crossing the
    bottle silhouette (the source video almost always has SOMETHING
    in front — a hand, a desk edge, an object — observe and decode it)

Compositing paragraph format:
  "The bottle integrates naturally with the scene: [a] realistic
   supplement-bottle scale (~5 inches), [b] lit by [scene's actual
   light source from VLM] with no dedicated product-shot lighting,
   [c] base [contact-point from VLM] with a soft natural cast shadow
   [direction + length matching VLM light direction], [d] perspective
   matching the scene's [camera angle from VLM], [e] [grip or surface-
   contact detail from VLM], [f] partially occluded by [foreground
   element from VLM] breaking the silhouette."

V606 negative constraints to add to closing negative block:
  - No dedicated product-shot lighting on the bottle.
  - No oversized bottle — realistic supplement-bottle scale.
  - No floating bottle — physical contact with surface or hand.
  - No hard cut-and-paste edges.
  - No color-saturated label — match scene color temperature.
  - No center-stage product hero-shot composition.

PRE-OUTPUT VALIDATION (v606):

  YES [a] scale anchor present in every product image prompt?
  YES [b] lighting integration cites scene's actual light source?
  YES [c] cast shadow direction + length stated?
  YES [d] perspective matches source-video camera angle?
  YES [e] surface contact / grip explicit?
  YES [f] natural foreground occlusion observed and decoded?
  YES v606 negative constraints in closing negative block?

If any wrong, FIX before emitting.

V609 CONCISE REFERENCE-BINDING FORM (decode-side enforcement):

  When you write the binding lines at the top of each Image prompt
  body in the decoded artifact, USE THE CONCISE FORM. Banana 2 auto-
  matches uploaded references' face / hair / clothing / label /
  packaging / color / proportions; the verbose "— match X, Y, Z
  exactly" clause is redundant noise that dilutes attention from
  per-image directives.

  FORBIDDEN (verbose, pre-v609):
    "Use the uploaded character reference image for the main
    character — match her facial features, identity, hair, and
    clothing exactly."
    "Use the uploaded product reference image for the Rosabella
    Beetroot bottle — match its label, packaging, color, and
    proportions exactly."

  REQUIRED (concise, v609):
    "Use the uploaded character reference image for the main
    character."
    "Use the uploaded product reference image for the Rosabella
    Beetroot bottle."

  The CHAIN line (v589.1 semantic form) is unchanged.

PRE-OUTPUT VALIDATION (v609):

  YES PERSONA binding line ends with the ingredient name + period
      (no "— match her ... exactly" trailer)?
  YES PRODUCT binding line (when present) ends with the ingredient
      name + period (no "— match its ... exactly" trailer)?

V610 GENDER-NEUTRAL MAIN-CHARACTER REFERENCES (decode-side):

  When you write the prose body and action_notes for the main
  character in the decoded artifact, NEVER use gendered pronouns
  (she / her / hers / he / him / his) to refer to the persona.
  Even though the source video shows a gendered presenting actor,
  the decoded artifact is for AUTHORING and downstream lifts will
  swap the persona — keep the prose generic so the artifact is
  reusable.

  Use:
    - Role descriptor: "the main character," "the healer," "the
      practitioner," "the host"
    - Singular-they: "their hand"
    - Pronoun-free: "right hand presses..." (drop the subject)

  NOT AFFECTED — DESCRIBE GENDER REQUIRED:
    - Dialogue lines — verbatim transcription, gendered language fine.
    - Other characters in the source (patient, customer, bystander,
      husband/wife, child, friend) — DESCRIBE their gender, age band,
      body build, clothing, role in the scene. Their identity IS the
      prose; without the description Banana 2 has no anchor for them.

  REQUIRED examples:
    "a late-40s woman in a soft beige knit cardigan"
    "an adult male patient seen from behind in a teal hospital gown"
    "her husband, a middle-aged man asleep in the bed behind"
    "a young female customer at the counter to the right"

  FORBIDDEN (when the character is non-persona):
    "the patient" without gender / age / clothing
    "another person beside them"
    "they sit on the chair" (when the source video clearly shows a
     specific gender — the decoder MUST commit to what the camera shows)

  Why the asymmetry: the main character will be SWAPPED on lift (a
  Black-female-practitioner upload one day, a Korean-male-practitioner
  another). Other characters are NOT bound to uploads — the prose is
  the only source of truth for their appearance. Vague non-persona
  descriptions produce Banana 2 hallucinations.

PRE-OUTPUT VALIDATION (v610):

  YES Zero instances of \bshe\b, \bhe\b, \bher\b, \bhis\b, \bhim\b,
      \bhers\b in image-prompt bodies and action_notes referring to
      the MAIN character?
  YES Persona references use role descriptor / singular-they /
      pronoun-free constructions?
  YES Every NON-persona character (patient, bystander, customer)
      has gender + age band + clothing/role described where they
      first appear?

If any wrong, FIX before emitting.

V613 PRODUCT-MENTION-BINDING PARITY (decode-side):

  When the source video shows a product on screen, the decoded artifact
  MUST set product_image: <ingredient-name> on every image whose prompt
  body, action_note, or scene line will mention the product. Decode
  artifacts feed lifts — a parity violation in the decode propagates
  into the lift unless the lift author manually fixes it.

  When the source video does NOT show the product (HOOK before-state,
  RECIPE-early lemon/ginger steps), the decoded prompt body MUST NOT
  describe the product as visible. Use a non-product anchor, e.g.
  "clean cream-tone counter behind" instead of "Korella saffron bottle
  on the counter behind".

  PRE-OUTPUT VALIDATION (v613):
    YES For every Image where prompt body / action_note / scene line
        contains a product term, product_image: is set?
    YES For every HOOK image (scenes 1-2) and RECIPE-early image
        (lemon, ginger), prompt body has NO product visual mention?
    YES The Ingredients table declares the product with type: product?

  If any wrong, FIX before emitting.

V621 DECODER NARRATIVE LENS + CAPTION BAN:

  v621a — Every "### Image N" block in the decoded artifact MUST declare
  ONE of three narrative lenses, classifying what the shot is DOING for
  the viewer:

    HEALER-SHOWING-CURE    — recipe steps, product reveals, mechanism
                             explanations, anatomy-pointer scenes,
                             ingredient-add scenes, cascade moments.
                             Decoder emphasis: the PRESENTATIONAL gesture
                             (what the persona is showing the viewer),
                             hand position relative to prop, camera angle
                             that proves the cure, product placement /
                             label visibility / recipe-step state.

    AUGMENTED-SYMPTOMS     — HOOK shock images, problem-callouts, exposed
                             before-state (back acne, varicose veins,
                             distended belly, soaked pillow), thermometer
                             readings, glucose-meter readings, anatomical
                             magnification.
                             Decoder emphasis: the AMPLIFIED visible
                             problem. Crop tight on the symptom.
                             Background props that contextualize. NO
                             solution visible yet — "before" must read
                             as raw and unresolved.

    GRABBING-ATTENTION     — scroll-stopper cold opens, weird actions
                             without specific cure context, persona
                             introduction shots, transition/movement
                             frames, decorative cuts.
                             Decoder emphasis: the PURE SPECTACLE — motion,
                             magnitude, novelty. What makes the thumb
                             stop. Decoder names what's startling without
                             binding to a remedy or symptom yet.

  Declaration goes alongside reference_image / product_image in the
  Image metadata block:

    ### Image 1
    - **reference_image:** none
    - **narrative_lens:** AUGMENTED-SYMPTOMS
    - **Image prompt:**
    ...

  This is mindset enforcement: classify BEFORE describing. Every shot
  exists for a rhetorical reason. Naming that reason produces sharper
  descriptions that lifts can adapt without losing the purpose.

  v621b — Image prompts must NEVER describe caption text. Captions
  get added at the platform level post-generation (via the editor's
  caption layer). Including caption descriptors makes Banana 2 BAKE
  them into pixels — uneditable, wrong font, wrong wrap, looks
  amateur.

  FORBIDDEN phrases in any Image prompt body:
    "yellow burned-in captions at the lower third"
    "white subtitle bar across the bottom"
    "large overlaid text reading 'X'"
    "caption: 'Try this remedy!'"
    ANY descriptor of post-production text overlays.

  When the source video shows captions, IGNORE them in image
  descriptions. Capture caption TEXT in dialogue lines (mirrors
  voiceover anyway), but never in the visual description.

  PRE-OUTPUT VALIDATION (v621):

    YES Every "### Image N" declares narrative_lens (one of HEALER-
        SHOWING-CURE / AUGMENTED-SYMPTOMS / GRABBING-ATTENTION)?
    YES Zero caption / subtitle / "overlay text" / "lower third"
        descriptors in any image prompt body?

  If any wrong, FIX before emitting.

V622 SYMPTOM-FEATURE EXAGGERATION ON NON-PERSONA CHARACTERS:

  When narrative_lens (v621) is AUGMENTED-SYMPTOMS or HEALER-SHOWING-
  CURE, OR the scene fits Pattern C (DIAGNOSTIC-PIVOT), AND the source
  frame shows a non-persona character (patient / customer / bystander)
  with a body part being pointed at, pressed, framed, circled,
  magnified, or visually centered, the decoded character description
  MUST:

    (1) Name the specific body part (chin, jowl, under-eye, neck,
        scalp, knuckle, calf, ankle, belly, cheek, forehead, hairline,
        lip, eyelid, etc.)
    (2) Describe its visually-emphasized state in specific exaggerated
        terms — match what the source camera is FORCING the viewer to
        see. The source video exaggerated it for the hook; the decoded
        prompt must preserve that signal.
    (3) NEVER use neutral posture filler ("chin raised slightly",
        "head tilted", "face turned toward the camera", "eyes locked",
        "leg extended") as a substitute for the actual symptom — those
        describe pose, not feature.
    (4) Match framing intensity — tight crop on the symptom = loud
        description; wider framing = calmer, but still name the
        feature.

  REQUIRED examples (FORBIDDEN -> REQUIRED):

    Practitioner's finger pressed into patient's full lower jaw:
      FORBIDDEN: "her chin raised slightly"
      REQUIRED:  "a full, sagging lower jaw with visible jowl drop,
                  the practitioner's index finger pressed firmly into
                  the soft underside of the chin"

    Camera tight on under-eye area:
      FORBIDDEN: "her eyes looking down"
      REQUIRED:  "puffy, swollen under-eye bags with dark hollows
                  beneath, fine crepey skin visible"

    Practitioner pointing at thinning scalp:
      FORBIDDEN: "head tilted forward"
      REQUIRED:  "a visibly thinning crown with sparse hair coverage
                  and exposed scalp through the parting line"

    Hand on distended belly:
      FORBIDDEN: "torso turned toward the camera"
      REQUIRED:  "a distended, bloated lower abdomen pushing against
                  the waistband, the practitioner's palm flat against
                  the swell"

    Close-up on varicose veins:
      FORBIDDEN: "her leg extended"
      REQUIRED:  "ropey, bulging blue-purple varicose veins running
                  down the calf, raised above the skin surface"

  ASYMMETRY CHAIN (v610 + v622):

    Main character (upload-bound persona):
      gender: FORBIDDEN (v610)
      symptom-feature: N/A (persona is not the symptom-bearer)

    Patient / customer / bystander (non-persona) on AUGMENTED-SYMPTOMS
    or HEALER-SHOWING-CURE lens:
      gender: REQUIRED (v610)
      symptom-feature: REQUIRED (v622)

    Patient / customer / bystander on GRABBING-ATTENTION lens with no
    specific body part indicated:
      gender: REQUIRED (v610)
      symptom-feature: NOT required (describe role/clothing/posture)

  PRE-OUTPUT VALIDATION (v622):

    YES For every Image where narrative_lens is AUGMENTED-SYMPTOMS or
        HEALER-SHOWING-CURE: prompt body names the specific body part
        being indicated?
    YES For every non-persona character with a body part being pointed
        at / pressed / framed / circled: description exaggerates the
        visible feature in concrete terms, not generic posture?
    YES Mechanical grep for forbidden filler phrases when a body part
        is being indicated — "chin raised slightly", "head tilted",
        "face turned", "eyes locked" (alone), "torso turned", "leg
        extended", "foot resting" — REWRITE if present alongside a
        diagnostic crop.

  If any wrong, FIX before emitting.

  Why: a lift of a v621-era decode with neutral filler description
  will generate a clean-jawed / clean-skinned / clean-belly patient.
  The diagnostic pivot has nothing to land on. The decoded artifact
  IS the lift contract for downstream variants — symptom signal lost
  here is symptom signal lost forever.

V614 / V615 DECODE-SIDE NOTES:

  Decode-side captures verbatim spoken dialogue from the source video
  (whisper transcription is authoritative). Decoders do NOT author
  dialogue, so v614 (cross-corpus survey + adaptation_map) and v615
  (em-dash ban) DO NOT apply to decoded "- **line:**" entries —
  preserve what the speaker actually said, em-dashes and all (the
  dialogue tone IS the corpus reference for downstream lifts).

  However, decoded artifacts MUST still:
    - Classify the source video into one of the 5 corpus patterns
      (A/B/C/D/E) and declare in frontmatter as
      `corpus_pattern: <A/B/C/D/E>`. This makes the decoded artifact
      catalog-ready for v614 surveys at lift time.
    - Stay tight to the source's dialogue — do not insert narrator
      commentary as separate "- **line:**" entries. If the source
      pauses or has filler, omit it; downstream lifts can re-tighten.

V712 — DECODE-SIDE RELATIONAL COMPOSITION GRAMMAR (NEW 2026-05-13)
==================================================================

v603 + v604 + v521.1 + v586 codified coordinate-anchored composition
grammar (viewer-left / viewer-right / upper-third line / chest-up two-
shot / cropped at mid-chest / NO floor visible). Works on generate side
where the operator SPECIFIES composition. BREAKS on decode side because
the VLM cannot reliably grid-anchor a source frame — it pattern-matches
corpus defaults ("clinical scene -> two-shot side-by-side / chest-up")
instead of measuring what's actually in the frame. Rigid grammar then
locks WRONG values into the prompt; feeding it back into Banana 2
produces an image that does NOT match source.

v712 carves out decode side: raw/decoded_*.md Image prompt bodies use
relational grammar anchored to SUBJECTS not frame grid.

GRAMMAR ORDER (decode-side Image prompt body):

  1. Subject identity + visible features (race / age / build / hair /
     wardrobe items VISIBLE in frame — never inferred).
  2. Active verb + spatial preposition chain encoding subject-to-
     subject geometry.
  3. Hand position via verb chain ("pointing DOWN at her forehead
     from above"), not via frame coordinates.
  4. Subject orientation explicit per subject ("faces the camera /
     looks forward / looks down").
  5. Shot size via DETAIL-DENSITY anchor (name micro-features that
     are visible-and-sharp at actual framing). NOT via jargon.
  6. Crop via OMISSION not negation. Subject-anchored cropping
     descriptors ALLOWED ("out of frame above his eyebrows / cropped
     at the top of his head / the rest of his head cropped above the
     frame edge"). Grid-quadrant cropping BANNED.
  7. Background blur statement at end.
  8. v603 closing tag retained ("iPhone HDR colors, deep focus.").

ALLOWED PREPOSITIONS (subject-to-subject geometry):
  above / below / behind / in front of / over the shoulder of /
  beside / between / under / next to / from above / from below.

ALLOWED VERBS (pose + action):
  pointing / leaning / standing / sitting / holding / lifting /
  reaching / gesturing / smiling / wincing / closing eyes / looking
  forward / looking down / looking at / facing the camera /
  turning toward.

SHOT SIZE BY DETAIL-DENSITY (not jargon):
  "forehead wrinkles clearly visible, dark eye circles clearly
   visible" -> close-macro
  "full lab coat visible, stethoscope visible, ID badge visible"
   -> medium-wide
  "full body visible from head to feet" -> wide

FRAME-COVERAGE + CROPPING (subject-anchored ALLOWED, refines v712):
  ALLOWED: "fills the frame", "her face dominates the frame",
           "only her head and shoulders visible", "out of frame
           above his eyebrows", "cropped at the top of his head",
           "the rest of his head cropped above the frame edge"
  BANNED:  "viewer-left half", "upper-third line", "lower-right
           corner", "chest-up two-shot", "cropped at mid-chest"

  Test: anchor to SUBJECT BODY PART (eyebrows, shoulder, chin,
  hairline) + frame EDGE (single line) -> ALLOWED. Anchor to
  FRAME GRID CELL (upper-third, viewer-right half) -> BANNED.

BANNED TOKENS on decode side:
  viewer-left / viewer-right / upper-third line / lower-third line /
  left half / right half / chest-up two-shot / cropped at mid-chest /
  NO floor visible / NO feet visible / NO background visible /
  heads near the upper-third / rule of thirds.

GENERATE SIDE UNCHANGED. videos/*.md continues to use v603 / v604 /
v521.1 / v586 coordinate grammar (operator specifies framing because
no source frame exists).

PRE-OUTPUT VALIDATION (v712):

  YES Zero hits on coordinate-token grep (viewer-left|viewer-right|
      upper-third|lower-third|left half|right half|cropped at mid-
      chest|chest-up two-shot|NO floor visible|NO feet visible)?
  YES At least 1 relational-token hit per Image block (above|below|
      behind|in front of|over the shoulder of|beside|between|under|
      next to|from above|from below|pointing down|pointing up|looking
      forward|looking down|faces the camera)?
  YES Zero negation crop tokens (NO floor|feet|background|hands|
      legs|wall|window|ceiling|wardrobe|chairs?|generic studio)?

V713 — BANANA-2 ATTACHED-REFERENCE COMPOSITION DISCIPLINE (NEW 2026-05-13)
==========================================================================

v712 works on text-only image models (GPT image gen). FAILS on Banana 2
when persona reference is attached: reference image (full identity =
full face) FIGHTS the prompt's partial-face cropping instruction.
Reference wins by default; Banana 2 renders balanced two-shot.

Documented in wiki/generation/nano-banana-prompting.md and
wiki/generation/json-prompt-method.md:
  - "long text + photos fight each other"
  - "fields that force the AI's hand (visible, dramatic, exposed)
     push the model to alter composition to prove the change"
  - "Banana 2 plans the image before rendering pixels" — first
    content gets weighted heaviest.

v713 fixes Banana-2-specific behavior with FIVE techniques:

[a] BINDING-LINE PARTIAL-VISIBILITY OVERRIDE

When persona / character appears PARTIALLY in the frame (cropped at
frame edge, only part of face visible, behind another subject
dominating frame), the binding line must include the partial-
visibility instruction inline. Standard v609 binding plus override
clause:

  Use the uploaded character reference image for the main character.
  In this frame the main character appears PARTIALLY — only his face
  from eyebrows to chin is visible, the rest of his head cropped
  above the frame edge.

Converts "render the character (default = full face)" -> "render
this specific portion of the character". Single instruction, no
conflict between binding and body prose.

When persona is FULLY visible in the frame, use standard v609
binding only — no override clause needed.

[b] COMPOSITION FRONT-LOAD

Composition block comes FIRST in body prose, AFTER bindings + blank
line, BEFORE Subject / Action / Location / Style / Tech blocks.
Banana 2 plans the image from early content; framing / geometry must
precede subject description so the planner doesn't default before
seeing composition constraints.

[c] CAMERA GRAMMAR REQUIRED IN COMPOSITION BLOCK

Per nano-banana-prompting.md Rule 2 (Name the camera). Concrete
hardware unlocks training-data priors. NOT just "iPhone wide-angle".
Use:
  - "85mm telephoto lens at minimum focus distance, shallow depth
     of field" -> macro portrait
  - "wide-angle 24mm, deep focus" -> environmental
  - "from low-angle / over-shoulder POV" -> camera position
  - "Hasselblad X2D, 85mm at f/2.8" -> premium portrait

Camera grammar lives in the [Composition] block. v603 closing style
tag ("iPhone HDR colors, deep focus.") stays in [Style] block at the
end.

[d] COMPOSITION-ANTI-DEFAULT NEGATIVES

When source frame breaks Banana 2 defaults (balanced two-shot,
full-character visibility, center composition), add explicit
negative constraints in the negatives block:
  - "No balanced two-shot — [primary subject] dominates the frame"
  - "No full view of [partial-visibility subject]"
  - "No center-stage hero composition"

Banana 2 takes negatives seriously (per nano-banana-prompting.md
"Be explicit about preservation"). Negatives counter the model's
default-priors pull.

[e] CANONICAL BLOCK ORDER (Banana 2 prompt formula)

  Binding line(s) — with v713(a) partial-visibility override if
                    applicable
  [BLANK LINE]
  [Composition] — front-loaded, camera grammar, dominance + cropping
  [Subject] — patient / secondary characters described fully;
              persona refs minimal per v553.1 / v609
  [Action] — verbs + spatial geometry
  [Location] — background blur statement
  [Style] — camera + lighting + grading
  [Tech] — aspect + resolution
  Negatives — composition-anti-default + v604 / v606 product
              negatives + persona drift constraints

WORKED EXAMPLE — Dr. Kim Image 1 (face-macro, doctor partial top-right
corner, patient face dominant lower-left + center):

  Use the uploaded character reference image for the main character.
  In this frame the main character appears PARTIALLY — only his face
  from eyebrows to chin is visible, the rest of his head cropped
  above the frame edge.

  [Composition] EXTREME close-up portrait, 85mm telephoto lens at
  minimum focus distance, shallow depth of field, 9:16 vertical
  framing. The patient's face FILLS the frame — only her head and
  the tops of her shoulders are visible. The main character leans
  down from behind her right shoulder, his partial face appearing
  close beside and above her head, faces inches apart.

  [Subject — patient] A white woman in her 60s, heavy build, short
  blonde bob, dark green V-neck scrub top, facing the camera,
  looking forward with a distressed embarrassed expression. Deep
  horizontal forehead wrinkles, crepey skin texture, and dark
  circles under her eyes are sharply visible at macro distance.

  [Action] The main character points a purple-gloved index finger
  DOWN at her forehead from above, fingertip resting near her deep
  horizontal wrinkles.

  [Location] Bright modern medical clinic interior with white walls,
  background fully blurred.

  [Style] Shot on iPhone 15 Pro main camera, handheld, vibrant
  natural HDR daylight. iPhone HDR colors, deep focus on both
  visible faces.

  [Tech] 9:16, 2K output.

  Negatives: No generic studio. No smooth forehead on the patient.
  No bare hands on the main character. No full lab coat. No full
  view of the main character's head. No balanced two-shot
  composition — the patient dominates the frame.

FALLBACK ESCALATION when one-shot still misses on Banana 2:

  1. Switch to Google AI Studio (better composition than direct API)
  2. Use Gemini Thinking / Pro mode (deeper reasoning)
  3. JSON method (json-prompt-method.md) — composition / subject /
     action / camera in separate JSON fields for surgical control
  4. Multi-turn editing — lock composition first WITHOUT reference,
     then add reference in turn 2

PRE-OUTPUT VALIDATION (v713):

  YES When persona / character is PARTIAL in the frame, binding
      line includes the partial-visibility override clause?
  YES [Composition] block present BEFORE [Subject] block in body
      prose?
  YES Camera grammar present in Composition block (focal length /
      aperture / focus distance / DOF / camera position)?
  YES Composition-anti-default negatives present in negatives
      block when partial visibility is in play?
  YES Canonical block order followed (Binding -> Composition ->
      Subject -> Action -> Location -> Style -> Tech -> Negatives)?

  If any wrong, FIX before emitting.

V714 — EMOTIONAL PAYOFF DISCIPLINE (NEW 2026-05-13)
====================================================

v541 (outfit change) + v580 (state-evolution) + v589 (absolute-magnitude
state arcs) + v622 (symptom exaggeration in HOOK) collectively force
decoders to update the non-persona character's PHYSICAL state across a
transformation arc. NONE of these mandate updating the EMOTIONAL state.
v512 / v669 chain-inheritance carries identity forward, and decoders
implicitly assume expression inherits with identity. Result: AFTER
frame shows resolved physical symptom on a still-distressed face —
wrinkles gone but expression still embarrassed, belly flat but face
still ashamed. Emotional payoff missing. Transformation arc collapses.

Surfaced 2026-05-13 from Gemini 3.1 Pro self-analysis after a Dr. Kim
NMN decode: physical wrinkles smoothed via v589 absolute-magnitude
grammar, but patient expression remained distressed because chain-
inheritance trap implicitly carried the BEFORE expression forward.

v714 RULE: Every image where the non-persona character (patient /
customer / bystander) appears AND the scene is part of a state-
evolution / before-after / transformation arc AND the AFTER-state
physical resolution is declared MUST also explicitly declare the
AFTER-state expression in BOTH the visual_delta field AND the body
prose.

VISUAL_DELTA FORMAT (chained AFTER-state images):

  - **visual_delta:** [physical change clause] AND [expression change
                      clause], joined explicitly

  Example:
  - **visual_delta:** forehead wrinkles smoothed flat AND distressed-
    embarrassed expression replaced with broad open-mouthed amazed
    smile, eyes wide with delighted surprise, eyebrows lifted

BODY-PROSE PATTERN (AFTER-state images):

  [Subject — patient] [physical AFTER-state per v622-inverse].
  The patient's expression has transformed: [explicit AFTER
  expression — joy / relief / confidence / amazement / pride].
  [Specific facial details — eyes widened, mouth open in smile,
  eyebrows lifted, posture upright].

The expression sentence is non-negotiable on AFTER frames. NEVER
write "(same as image 1)" or "(expression unchanged)" or omit the
expression entirely.

EXPRESSION MAPPING — MIRROR OF v622 BEFORE INTENSITY SCALE:

  Skin / wrinkles:    distressed, embarrassed -> relieved, amazed, joyful
  Weight / belly:     ashamed, hiding         -> confident, proud, smiling
  Hair / scalp:       self-conscious          -> bright-eyed, energetic
  Joints / pain:      wincing, grimacing      -> comfortable, smiling, free
  Energy / fatigue:   exhausted, slumped      -> energized, upright, alert
  Acne / skin:        embarrassed, head-down  -> radiant, confident, head-up
  Sleep / dark eyes:  hollow, weary           -> rested, bright-eyed, fresh
  Bloat / digestion:  uncomfortable           -> comfortable, smiling, relaxed

EXPRESSION INTENSITY MATCHES TRANSFORMATION MAGNITUDE (v589 bidirectional):
  - COMPLETE physical resolution -> COMPLETE emotional resolution
    (broad open-mouthed smile, eyes wide with surprise / relief)
  - PARTIAL physical change -> PARTIAL emotional shift
    (gentle smile, softened brow)
  - MINIMAL physical change -> MINIMAL emotional update
    (neutral lifted eyebrows, calmer eyes)

CHAIN-INHERITANCE CLARIFICATION (amends v669):

  Chained images inherit IDENTITY (race / age / build / hair /
  wardrobe core) via v523 chain. Chained images DO NOT inherit
  EXPRESSION — expression must be explicitly re-declared in every
  image where the character appears. The chain carries who the
  person IS, not what they FEEL.

CARVE-OUTS:

  - Non-transformation chained scenes: no expression update required
    (talking-head explanation, recipe-prep with only props changing,
    walk-throughs with no patient state change).
  - Persona (uploaded character): v714 does NOT apply — persona
    expressions handled via v553.1 / v609; v713(a) override may also
    apply when persona is partial-visible.
  - HOOK / AUGMENTED-SYMPTOMS lens scenes: v622 governs (BEFORE
    distress). v714 N/A on HOOK images; v714 fires on RESULT /
    payoff / AFTER lens scenes only.
  - GRABBING-ATTENTION lens: no transformation arc -> v714 N/A.

PRE-OUTPUT VALIDATION (v714):

  YES Every chained image with reference_image: image_K AND
      visual_delta naming a physical AFTER-state (smooth / flat /
      fade / reduce / shrink / resolve / clear / lift / tight / firm)
      ALSO declares an emotional AFTER-state in visual_delta?
  YES Every AFTER-state body-prose block explicitly names the new
      expression (smile / joy / relief / confidence / amazement /
      pride / comfortable / radiant / bright-eyed / energetic)?
  YES Zero "(same as image K)" / "(expression unchanged)" / no-
      expression-mention patterns on AFTER-state images?
  YES Expression intensity matches v589 transformation magnitude
      (COMPLETE physical -> COMPLETE emotional, etc.)?

  If any wrong, FIX before emitting.

V715 — SUBJECT-ANCHORED PROP COMPOSITION (NEW 2026-05-13)
==========================================================

v604 (per-video camera anchors) + v605 (PROP-LED format) + v606
(compositing) + v712 (subject-anchored prop positioning) describe HOW
a prop is held / lit / shadowed / occluded. They are silent on WHERE
in the frame the prop lands. Corpus default reads "on the counter /
desk / table" — desk-anchored phrasing sinks the prop into the bottom
20% of a 9:16 vertical frame. Hero prop becomes a footer instead of
the focal point.

v715 packages THREE sub-amendments under one umbrella name (Subject-
Anchored Prop Composition):

  v605b — Subject-Anchored Prop Position
  v713f — Central Z-Axis Stacking
  v603b — Anchor-Level Camera Framing

Surfaced 2026-05-13 from Nuri bladder-model diagnostic-hook generation:
prose anchored bladder to "the desk in the immediate foreground";
Banana 2 rendered the bladder at desk height bottom-of-frame; the
diagnostic-pointer hook lost its visual center.

v605b — SUBJECT-ANCHORED PROP POSITION

When a prop or symptom is the PRIMARY focus of the frame, prop MUST
anchor to a SUBJECT (character body or body part) — NEVER to
environment furniture (desk / counter / table / shelf / windowsill /
floor).

Five subject-anchor modes — pick the one that matches the scene:

  Mode 1 — Held aloft: character holds prop at chest / face / chin /
    overhead height, prop between holder's torso and camera.
    Best for: HOOK diagnostic-pointer, product reveal, before-after
    card, recipe payoff.

  Mode 2 — Placed on body: prop rests on the patient's belly / chest /
    forearm / thigh / knee / scalp / back / shoulder.
    Best for: anatomical-pointer demos (bladder on belly, brain on
    head, heart on chest), treatment-area indicators, transdermal
    patches, before-photo overlays.

  Mode 3 — Pressed against body: character presses prop / hand /
    instrument against the symptom site, palpation pose.
    Best for: palpation diagnostic, examination, pain-pointer,
    pressure tests.

  Mode 4 — Worn / strapped / draped on body: prop wraps around / over
    / on the body (compression garment, scarf, supplement-patch,
    glasses, watch, monitor).
    Best for: wearable products, brace demos, monitor demos.

  Mode 5 — Symptom-as-prop on body: symptom IS the prop (varicose
    veins on calf, jowl on jaw, distended belly, thinning hairline,
    dark eye circles, back acne).
    Best for: AUGMENTED-SYMPTOMS HOOK frames, before-state callouts.

Mode 5 supersedes the previous v622 anatomy-framing carve-out — body-
part symptoms now fold into v715 composition discipline (symptom at
frame center, character body region framing the symptom, camera at
symptom anchor level).

BANNED anchor phrases for hero props (decode + generate side):

  "on the desk" / "sitting on the desk" / "placed on the desk"
  "on the counter" / "on the side counter" / "on the prep counter"
  "on the table" / "on the bedside table"
  "on the shelf" / "on the windowsill"
  "resting on the surface" / "sitting on the surface"
  "in front of him on the desk" / "between them on the table"

REQUIRED anchor phrases (per mode):

  Held aloft:
    "HELD ALOFT at [chest|face|chin|overhead] height in the immediate
     center-foreground"
    "the patient holds [prop] up at her own chest height, [prop]
     dominating the center of the frame"
    "the main character lifts [prop] at face height, [prop] directly
     between him and the camera"

  Placed on body:
    "the [prop] is placed directly on the patient's [body part],
     anchored at [body-part] height in the immediate center-foreground"
    "the patient rests the [prop] on his own [body part], [prop]
     dominating the center of the frame"
    "the [prop] sits on the patient's [body part], [body part]
     forming the supporting surface"

  Pressed against body:
    "the main character presses [prop / index finger / palm] firmly
     against the patient's [body part]"
    "the [prop] is pressed into the soft underside of the patient's
     [body part]"
    "the main character's fingertips palpate the patient's [body part]"

  Worn / strapped / draped on body:
    "the patient wears the [prop] [around the wrist / strapped to the
     forearm / draped over the shoulder / clipped to the lapel]"
    "the [prop] wraps around the patient's [body part]"

  Symptom-as-prop on body:
    "the patient's [body part] fills the immediate center-foreground,
     [symptom-feature exaggerated description per v622]"
    "the camera focuses tightly on the patient's [body part] at the
     center of the frame"

v713f — CENTRAL Z-AXIS STACKING

Composition block must use Z-AXIS DEPTH LAYERING (foreground / mid /
background) — not Y-axis height stacking.

Required Composition-block structure:

  [Composition] [camera grammar per v713(c)] + [camera height per v603b
  at the prop's anchor level], 9:16 vertical framing. [Z-axis depth
  layering, three planes named in order]:
    Foreground (immediate, center, closest to lens): [hero prop,
    anchored to subject per v605b mode, dominating the center of the
    frame].
    Midground (directly behind / framing the prop): [primary character's
    body part hosting the prop OR face visible just above/beside the
    prop].
    Background (top / side / behind midground): [secondary character —
    persona leaning in OR partial-visible from frame edge].

Anchor-height matches prop's body anchor (NOT always chest):

  Held aloft at chest    -> chest level
  Held aloft at face     -> face level
  Placed on belly        -> belly level
  Placed on chest        -> chest level
  Pressed against jaw    -> face level
  Pressed against knee   -> knee level
  Symptom on calf        -> calf level (camera at mid-shin)
  Symptom on belly       -> belly level
  Symptom on face        -> face level
  Worn on wrist          -> wrist level

v603b — ANCHOR-LEVEL CAMERA FRAMING

Camera MUST sit at the level of the prop's subject-anchor point on the
patient's body. Camera height MATCHES the anchor level.

Required camera anchor phrases (generalized):

  "straight-on at [anchor]-level"
  "camera at [anchor] height, level with the [anchor-part]"
  "camera lens level with the [body anchor point]"
    (e.g. "camera lens level with the patient's navel" for belly-
    placed prop, "camera lens level with the patient's mid-shin" for
    calf symptom)
  "eye-level with the [anchor]"

BANNED camera anchors when hero prop is subject-anchored:

  "shot from above" / "high angle" / "angled down at the desk"
  "looking down at the prop" / "top-down view" / "overhead shot"
  "bird's-eye" / "camera tilted down"
  "low angle looking up" (wrong direction)
  "camera at floor level looking up at the patient"

WORKED EXAMPLES — three anchor modes:

  Mode 1 — Held aloft (bladder diagnostic hook):
    [Composition] 85mm telephoto lens at minimum focus distance,
    shallow depth of field, straight-on at chest-level, 9:16 vertical
    framing. The transparent anatomical bladder model is HELD ALOFT
    in the immediate center-foreground, dominating the middle of the
    image. Directly behind the elevated model, the patient's face is
    sharply visible just above the bladder. The main character leans
    in from the top-right background, his partial face appearing
    close beside and above the patient's head.

  Mode 2 — Placed on belly (anatomical demo):
    [Composition] 50mm portrait lens, shallow depth of field,
    straight-on at belly-level (camera lens level with the patient's
    navel), 9:16 vertical framing. The transparent anatomical bladder
    model is placed directly on the patient's distended lower belly
    in the immediate center-foreground, anchored at belly height,
    dominating the middle of the image. Directly above the bladder
    model, the patient's torso rises through the frame, his distressed
    face visible at the top of the image. The main character leans in
    from the top-right background.

  Mode 5 — Symptom-as-prop (varicose veins on calf):
    [Composition] 85mm telephoto lens at minimum focus distance,
    shallow depth of field, straight-on at calf-level (camera lens
    level with the patient's mid-shin), 9:16 vertical framing. The
    patient's calf fills the immediate center-foreground — ropey,
    bulging blue-purple varicose veins running down the calf, raised
    above the skin surface, dominating the middle of the image. The
    patient's lower leg extends through the frame from knee to ankle.
    The main character's purple-gloved index finger enters from the
    top-right background.

CARVE-OUTS:

  - Edible recipe-prep mid-action: prop on prep surface BUT camera
    at chest-level (NOT top-down). Prep-surface anchoring stays for
    recipe-prep where surface IS the action plane.
  - Environmental establishing shots / CCTV / room walkthroughs:
    no hero prop in primary focus, v715 N/A.
  - Edible-product packshot (hero shot with no people): v606
    compositing governs; v605b N/A (no body in frame).
  - Furniture / appliances / vehicles that ARE the product: not
    portable / not body-anchorable; v715 N/A.

PRE-OUTPUT VALIDATION (v715):

  YES Zero hits on banned environment-anchor phrases (on the desk /
      counter / table / shelf / windowsill / surface)?
  YES At least 1 hit per hero-prop Image block on required subject-
      anchored phrasing (HELD ALOFT / placed on the patient's [body
      part] / pressed against / wears / [body part] fills the
      immediate center-foreground / directly between [subject] and
      the camera)?
  YES Z-axis depth language in Composition block (immediate center-
      foreground / directly behind the prop / just above the prop /
      framing the prop / in the background / leaning in from)?
  YES Anchor-level camera phrasing in Composition block (at [chest|
      face|belly|knee|calf|wrist|scalp]-level / camera lens level
      with [body anchor point])?
  YES Zero hits on banned downward camera angles (shot from above /
      high angle / angled down at the desk / top-down view /
      overhead / bird's-eye)?
  YES Composition-anti-default negatives in negatives block (No desk
      visible / No [.{1,30}] on a surface / No top-down / No high-
      angle / No prop sinking / prop dominates the center)?

  If any wrong, FIX before emitting.

V716 — BANANA 2 NORMALIZATION-BIAS COUNTERMEASURES (NEW 2026-05-13)
====================================================================

Packages two sub-amendments under one rule:
  v622b — Geometric Symptom Exaggeration
  v715f — Two-Shot Body-Part-Thrust Mode (v605b Mode 6)

Surfaced 2026-05-13 from Gemini 3.1 Pro generation cycles after v715
shipped. Two failure modes:
  (1) Symptoms render too normal — v622 mandates "specific exaggerated
      terms" but corpus uses ADJECTIVES which Banana 2 normalizes.
  (2) Persona crops too aggressively — v713(a) partial-visibility +
      v715 Z-axis stacking force persona into corner when operator
      needs both characters full-visible.

v622b — GEOMETRIC SYMPTOM EXAGGERATION

When v622 mandates symptom-feature exaggeration on non-persona character
(AUGMENTED-SYMPTOMS HOOK frames, HEALER-SHOWING-CURE diagnostic-pointer,
before-state callouts), body prose MUST use GEOMETRIC / MEASUREMENT-
BASED descriptors — not adjective-only. Banana 2 treats measurements
as hard constraints; adjectives are soft suggestions that lose to
normalization bias.

Banned (adjective-only) -> Required (geometric):

  Sagging arm:        "sagging loose skin" -> "crepey loose flab
                      hanging 3 inches below the tricep in a deep
                      U-shape"
  Distended belly:    "distended belly" -> "belly pushing 4 inches
                      past the waistband, draped heavily over the belt"
  Varicose veins:     "ropey veins" -> "veins raised 5mm above the
                      skin, branching 6 inches down the calf"
  Thinning hair:      "thinning crown" -> "scalp visible through 50%
                      of the crown coverage area"
  Jowl drop:          "sagging jowl" -> "jowl drooping 2 inches below
                      the jawline, forming a visible pouch"
  Forehead wrinkles:  "deep wrinkles" -> "5+ horizontal grooves carved
                      3mm deep across the forehead"
  Dark eye circles:   "dark circles" -> "hollow shadows extending 1.5
                      inches below the lower lash line"
  Crow's feet:        "crow's feet" -> "radiating creases 0.8 inches
                      long fanning from each outer eye corner"
  Double chin:        "double chin" -> "second chin pouch projecting
                      1.5 inches forward of the jawline"
  Acne severity:      "acne" -> "30+ inflamed red papules covering 60%
                      of the cheek surface"

Geometric descriptors use one or more of:
  - Linear measurement in real units (inches / mm / cm)
  - Coverage percentage ("50% of the crown area")
  - Count ("5+ grooves", "30+ papules")
  - Directional projection ("projecting 1.5 inches forward")
  - Geometric shape ("deep U-shape", "radiating fan pattern")
  - Spatial extent ("branching 6 inches down")

Adjective + geometric combo allowed. Adjective without geometric is
BANNED on AUGMENTED-SYMPTOMS lens images.

Mandatory anti-normalization negatives:

  No firm [body part]. No normal skin elasticity. No minor [symptom].
  No mild [symptom]. The [symptom] MUST be EXTREME and highly visible.

Carve-outs:
  - Persona NOT affected (v553.1 / v609 / v610 govern persona; v622b is
    non-persona only).
  - RESULT / AFTER-state frames: v714 emotional payoff governs; v622b
    intensity drops (resolution removes geometric severity).
  - GRABBING-ATTENTION lens with no specific symptom: N/A.
  - Decode-side: capture source-faithful intensity (don't fabricate
    geometric severity not in source).

v715f — TWO-SHOT BODY-PART-THRUST MODE (v605b Mode 6)

Sixth subject-anchor mode for scenes where persona must remain FULLY
VISIBLE AND symptom still needs to dominate frame center.

Mode 6 description:
  Patient extends their own body part (arm / belly / leg / hand / face)
  DRAMATICALLY toward the lens; persona stays fully visible at the
  side; camera pulls back to chest-up two-shot at 35mm wide-angle.

Required Composition-block structure for Mode 6 (replaces v603b
anchor-level + v713(a) partial-visibility for this mode):

  [Composition] 35mm wide-angle lens, deep focus, chest-up two-shot,
  9:16 vertical framing. The main character stands fully visible on
  the viewer-right [OR viewer-left]. The patient stands on the
  [opposite side] and [thrusts / extends / presents / pushes / lifts]
  [his / her] [body part] across the center-foreground toward the
  camera. The [body part] dominates the immediate foreground; both
  characters are visible at chest-up framing.

Trade-offs (explicit):
  GAIN: both characters fully visible; persona's authority gesture
        preserved; diagnostic-pointer anchor maintained
  GIVE UP: symptom no longer at extreme-macro framing; v603b anchor-
        level camera dropped; v713(a) partial-visibility override
        dropped; symptom detail-density reduced (v622b geometric
        language compensates)

Drop when using Mode 6:
  - v713(a) partial-visibility override on binding line
  - v603b anchor-level camera lock

Keep when using Mode 6:
  - v605b subject-anchored anchoring (via body-part-thrust gesture)
  - v713f Z-axis depth (symptom in immediate foreground via gesture)
  - v713(b) Composition front-loaded
  - v713(c) camera grammar ("35mm wide-angle lens, deep focus")
  - v713(d) anti-default negatives
  - v605b banned environment-anchor phrases
  - v622 + v622b symptom intensity (CRITICAL — symptom must compensate
    for reduced macro detail by maxing intensity language)

Mode 6 negatives unique:
  No symmetric balanced two-shot — the patient's [body part] thrust
  dominates the center-foreground. No persona crop — the main
  character is fully visible at chest-up. No top-down angle. No
  floor visible.

Selection guide (Mode 1-5 vs Mode 6):
  Maximum symptom macro + partial persona acceptable -> Mode 1-5 +
                                                       v713(a) override
  Both characters full-visible + accept some symptom detail loss -> Mode 6
  Symptom EXTREME + both characters visible -> Mode 6 + v622b
  Single-subject shot (no persona) -> Mode 1 / 5 with persona absent

PAIRING v622b + v715f:

Mode 6 implies v622b. v622b is recommended for Mode 1-5 and required
for Mode 6 to compensate for reduced macro detail.

PRE-OUTPUT VALIDATION (v716):

  YES Every AUGMENTED-SYMPTOMS / HEALER-SHOWING-CURE Image block with
      a non-persona body-part symptom contains ≥1 geometric descriptor
      (inch / mm / cm / % / count / projection / shape / spatial extent)?
  YES Anti-normalization negatives present ("No firm [body part]. No
      normal skin elasticity. No minor [symptom]. The [symptom] MUST
      be EXTREME.")?
  YES For Mode 6 images: body-part-thrust language present
      (thrusts / extends / presents / pushes / lifts ... across /
      toward / forward to the center / camera / lens / foreground)?
  YES For Mode 6 images: composition has chest-up two-shot + 35mm
      wide-angle + both characters fully visible?
  YES For Mode 6 images: negatives include "No symmetric balanced
      two-shot", "No persona crop"?

  If any wrong, FIX before emitting.

V717 — ANTI-NORMALIZATION INTENSIFICATION STACK (NEW 2026-05-13)
=================================================================

Packages three sub-amendments composing with v716:
  v622b-extension — Geometric + Metaphor Forcing
  v605c           — Symptom-First Subject Allocation
  v604b           — Structural Negative Constraints

Surfaced 2026-05-13 after v716 shipped: extreme-symptom HOOK frames
still rendered normalized on Banana 2 because (1) geometric measurements
alone don't lock visual character; (2) patient-first [Subject] block
plans demographics before symptom; (3) outcome-banning negatives leave
underlying anatomical defaults intact.

v717 = v716 + metaphor forcing + symptom-first allocation + structural bans.

v622b-EXTENSION — GEOMETRIC + METAPHOR FORCING

Geometric descriptors alone fail Banana 2 normalization on extreme
symptoms. Add INANIMATE-OBJECT METAPHOR FORCING alongside measurements.
Banana 2 has strong visual priors for inanimate-object shapes.

Pattern: "[geometric measurement] like a [object] [optional verb]"

Banned (geometric only) -> Required (geometric + metaphor):

  Sagging arm:     "hanging 3 inches below the tricep" ->
                   "hanging 3 inches below the tricep, drooping like
                    a deflated balloon or melted wax"
  Distended belly: "pushing 4 inches past the waistband" ->
                   "pushing 4 inches past the waistband like an
                    inflated bowling ball straining against the belt"
  Varicose veins:  "raised 5mm above the skin, branching 6 inches" ->
                   "raised 5mm above the skin like blue-purple twisted
                    yarn knotted across the calf, branching 6 inches"
  Thinning hair:   "scalp visible through 50% of crown" ->
                   "scalp visible through 50% of crown coverage, hair
                    appearing like sparse grass on dry ground"
  Jowl drop:       "drooping 2 inches below the jawline" ->
                   "drooping 2 inches below the jawline like a melted
                    candle pooling at the chin"
  Forehead lines:  "5+ grooves carved 3mm deep" ->
                   "5+ grooves carved 3mm deep like ridged corduroy
                    fabric"
  Acne:            "30+ inflamed papules covering 60% of cheek" ->
                   "30+ inflamed papules clustering across the cheek
                    like an angry rash of crushed berries"
  Stretch marks:   "silvery linear striae 4-6 inches long" ->
                   "silvery linear striae 4-6 inches long like cracked
                    porcelain spreading across the lower abdomen"

Metaphor catalog (Banana 2 strong-prior objects):
  Drooping/sagging:    deflated balloon / melted wax / melted candle /
                       draped curtain / sagging dough
  Distended/swollen:   inflated bowling ball / swollen water balloon /
                       taut drumhead
  Knotted/twisted:     twisted yarn / knotted rope / branching tree roots
  Cracked/lined:       cracked porcelain / ridged corduroy / dried mud
  Sparse/thin:         sparse grass on dry ground / thinning carpet /
                       patchy moss
  Clustered/inflamed:  crushed berries / angry rash / relief map of
                       small volcanic peaks
  Hollow/shadowed:     bruised purple pouches / sunken caves /
                       shadowed wells
  Detached/unattached: detached fabric flap / loose curtain /
                       hanging tapestry

Required per AUGMENTED-SYMPTOMS / HEALER-SHOWING-CURE symptom: BOTH a
geometric measurement (v716 v622b) AND a metaphor anchor (v717). Both
mandatory. Geometric-only fails Banana 2.

v605c — SYMPTOM-FIRST SUBJECT ALLOCATION

When narrative_lens = AUGMENTED-SYMPTOMS AND symptom is rhetorical
priority (HOOK / before-state / symptom-pointer), [Subject] block MUST
lead with SYMPTOM as standalone visual entity BEFORE host demographics.
Banana 2 weights first-tokens heaviest.

Replace single [Subject — patient] with TWO blocks:

  [Subject — Symptom] [geometric + metaphor description of symptom as
  standalone visual entity, naming body part it occupies; ends with
  "fills the immediate center-foreground" or v713f Z-axis anchor].

  [Subject — Host] [host character demographics — race + age + BUILD +
  hair + clothing + expression per v610 / v622 / v714 — the body part
  bearing the symptom belongs to this host].

Worked example (varicose-veins HOOK):

  Pre-v605c (patient-first):
    [Subject — patient] A white woman in her late 60s, heavy build,
    short blonde bob, dark green V-neck top. Her right calf shows
    varicose veins raised 5mm above the skin, branching 6 inches down.

  Post-v605c (symptom-first):
    [Subject — Symptom] Ropey, bulging blue-purple varicose veins
    raised 5mm above the skin like twisted yarn knotted across a
    human calf, branching 6 inches down from knee to ankle in a deep
    crisscross web pattern, fill the immediate center-foreground.

    [Subject — Host] The calf belongs to a white woman in her late
    60s, heavy build, short blonde bob, dark green V-neck top, navy
    shorts revealing bare calves. She faces the camera with a
    distressed expression.

Triggers when:
  - narrative_lens = AUGMENTED-SYMPTOMS (HOOK / before-state)
  - AND v605b Mode 5 (symptom-as-prop) OR Mode 6 (body-part-thrust)
  - Optional but recommended on Mode 2 (placed on body)

Does NOT trigger when:
  - narrative_lens = HEALER-SHOWING-CURE (cure is focus, not symptom)
  - narrative_lens = GRABBING-ATTENTION (no specific symptom)
  - AFTER / RESULT frames (v714 governs emotional payoff)

v604b — STRUCTURAL NEGATIVE CONSTRAINTS

v604 / v716 anti-normalization negatives ban OUTCOMES ("No firm arm" /
"No clear calf"). Banana 2 still renders ANATOMICAL DEFAULTS ("normal
human arm anatomy with skin attached to bicep") and treats symptom as
surface decoration on top of the default. v604b bans the UNDERLYING
ANATOMICAL DEFAULT.

Append structural-anatomical bans to negatives block:

  Sagging arm:     "No normal human arm anatomy. No skin attached to
                   the bottom of the bicep. No straight lower arm
                   contour. No natural muscle definition under the
                   tricep."
  Distended belly: "No normal abdominal wall. No taut skin over the
                   belt line. No flat waistband. No defined obliques."
  Varicose veins:  "No normal calf skin. No smooth surface texture.
                   No invisible vasculature. No clear leg silhouette."
  Thinning hair:   "No full coverage hairline. No dense crown. No
                   normal hair density. No closed parting line."
  Jowl drop:       "No clean jawline contour. No skin attached firmly
                   to the mandible. No normal jaw definition. No
                   defined chin-to-neck angle."
  Forehead lines:  "No smooth forehead surface. No taut skin over
                   the brow. No normal frontal anatomy."
  Dark eye circles: "No taut under-eye skin. No flat tear-trough
                    region. No normal periorbital anatomy."
  Acne severity:   "No clear skin. No smooth cheek surface. No normal
                   pore visibility. No even skin tone."
  Stretch marks:   "No smooth abdominal skin. No uniform skin tone.
                   No normal dermal continuity."
  Back acne:       "No clear back. No smooth upper-back skin. No
                   normal pore distribution."

Pattern: each structural ban negates an anatomical / surface DEFAULT
— names body part + healthy structural feature being banned. Forces
Banana 2 to render the absence, which forces the symptom-distorted
version.

Negatives-block order (canonical):
  [v716 anti-normalization — outcome ban]
  [v604b structural-anatomical ban] (NEW v717)
  [v713(d) composition-anti-default]
  [v604 generic per-video negatives]
  [v606 product negatives if applicable]

COMBINED WORKED EXAMPLE — varicose-veins HOOK with full v717 stack:

  [Composition] 35mm wide-angle, deep focus, chest-up two-shot, 9:16.
  The patient stands on the viewer-left and thrusts her bare right
  calf across the center-foreground toward the camera. The main
  character stands fully visible on the viewer-right.

  [Subject — Symptom] Ropey, bulging blue-purple varicose veins
  raised 5mm above the skin like twisted yarn knotted across a human
  calf, branching 6 inches down from knee to ankle in a deep
  crisscross web pattern covering 70% of the calf surface, fill the
  immediate center-foreground.

  [Subject — Host] The calf belongs to a white woman in her late 60s,
  heavy build, short blonde bob, dark green V-neck top, navy shorts
  revealing bare calves. She faces the camera with a distressed
  expression. The calf is extended forward toward the lens.

  [Action] The patient thrusts her calf toward the camera. The main
  character reaches a purple-gloved index finger toward the most
  prominent vein.

  [Location] Bright modern medical clinic interior. Background blurred.

  [Style] iPhone 15 Pro main camera, handheld, vibrant natural HDR
  daylight. iPhone HDR colors, deep focus.

  [Tech] 9:16, 2K output.

  Negatives: No firm calf. No normal skin elasticity. No minor
  varicose veins. The veins MUST be EXTREME. No normal calf skin.
  No smooth surface texture. No invisible vasculature. No clear leg
  silhouette. No symmetric balanced two-shot — the patient's thrust
  calf dominates the center-foreground. No persona crop. No top-down
  angle. No floor visible.

Three layers stacked: v622b-extension (geometric + metaphor in
[Subject — Symptom]); v605c (symptom-first allocation); v604b
(structural anatomical bans in negatives).

PRE-OUTPUT VALIDATION (v717):

  YES Every AUGMENTED-SYMPTOMS / HEALER-SHOWING-CURE Image with
      non-persona body-part symptom contains BOTH a geometric
      descriptor (v716 v622b) AND an inanimate-object metaphor
      ("like a [object]")?
  YES For AUGMENTED-SYMPTOMS HOOK / before-state frames: [Subject —
      Symptom] block precedes [Subject — Host] block in body prose?
  YES Negatives block contains ≥1 structural-anatomical ban (No
      normal [body part] anatomy / No skin attached to / No straight
      contour / No natural definition / No taut / No smooth / No
      flat / No invisible)?

  If any wrong, FIX before emitting.

V718 — VLM FORENSIC-PERCEPTION PROTOCOL (NEW 2026-05-13)
=========================================================

PRE-GRAMMAR perceptual protocol. Apply BEFORE writing any
static_composition prose. Skipping these steps produces hallucinations
no downstream grammar discipline can recover from.

Three sub-amendments:
  v718a — Kinematic Tracing (cures misattribution)
  v718b — Z-Depth Isolation (cures blocking blindness)
  v718c — Literal Pixel VFX Recognition (cures anatomical normalization)

v718a — KINEMATIC TRACING

VLMs suffer proximity bias. If face near hand, VLM assumes their hand.
Before attributing any body part / symptom / held prop to a character:

  1. Look at the limb.
  2. Trace pixels from fingertip / extremity back to shoulder / torso.
  3. Note CLOTHING COLOR at the shoulder where the limb originates.
  4. Assign limb ONLY to the character wearing that clothing color.
  5. DO NOT assign by face-proximity in 2D frame.

Example: purple gloves on a hand crossing the frame -> trace back to
which torso has dark suit / purple-accent clothing. Hand belongs to
THAT character, not to whichever face the hand appears NEXT TO.

v718b — Z-DEPTH ISOLATION

VLMs process frames as flat 2D posters. Miss occlusion / depth.
Before writing static_composition:

  1. Identify FOREGROUND (closest, in focus, blocking pixels behind).
  2. Identify MIDGROUND (one layer behind foreground).
  3. Identify BACKGROUND (furthest, often blurred).
  4. Check OVERLAPPING PIXELS: A covers B = A in front of B.
  5. Note when body part crosses horizontally + BLOCKS character
     behind it.

Example: patient's arm extended forward across frame in front of
practitioner = arm is FOREGROUND, practitioner is MIDGROUND occluded
by arm. NOT side-by-side. Z-axis layered.

v718c — LITERAL PIXEL VFX RECOGNITION

Source videos use extreme VFX violating real-world physics. VLMs
default to mapping impossible shapes to closest normal-anatomy
concept (familiar training prior). This is HALLUCINATION.

DESCRIBE LITERAL SHAPES AND CONNECTIONS YOU SEE IN THE PIXELS:

  Flesh loops with hole -> "closed loop with visible hole", NOT
    "deep U-shape" (U is open, loop is closed)
  Floating objects -> "floats unsupported in mid-air"
  Impossible stretching -> "stretched 12 inches", NOT "a few inches"
  Detached body parts -> "fully detached with 3-inch gap"
  Multiplied features -> "three eyes" / "two mouths" literal count
  Inverted anatomy -> "arm bends BACKWARD at elbow"
  Translucent skin -> "skin partially transparent, vessels showing"
  Liquefied flesh -> "limb liquefied, flesh flowing like wax/honey"

If shape doesn't match any common anatomical default, the VLM's
first-instinct adjective is probably hallucinated. Force LITERAL
pixel description instead.

PRE-GRAMMAR ORDER:

  For each shot:
    1. v718a Kinematic Tracing — trace every visible limb to origin
    2. v718b Z-Depth Isolation — map foreground / midground / background
    3. v718c Literal Pixel VFX — name what pixels show
    THEN apply:
    4. v712 relational composition grammar
    5. v713 Banana-2-attached-reference (if generate-side applies)
    6. v715 subject-anchored prop (5 modes + Mode 6 if applicable)
    7. v716 normalization countermeasures
    8. v717 anti-normalization intensification (if extreme symptom)

WORKED EXAMPLE — saffron-saggy-arm HOOK (surfacing case):

  Source frame: practitioner on viewer-right (dark suit, purple gloves);
  patient on viewer-left (green scrub top, arm extended forward across
  frame); patient's arm shows VFX flesh-loop with visible hole.

  Pre-v718 (three hallucinations):
    "patient extends arm with deep U-shape sagging skin. His purple-
    gloved hand points at the sagging."

  Failures: (a) purple-gloved hand attributed to PATIENT despite green
  scrub top (should be practitioner); (b) "side-by-side" missing arm-
  as-foreground occluding practitioner; (c) "U-shape" lost closed-loop
  topology.

  Post-v718 (forensic-corrected):

    v718a: purple gloves -> dark suit shoulder = PRACTITIONER's hand.
    Patient's green-scrub sleeve -> green-scrub shoulder = patient's
    arm.

    v718b: foreground = patient's extended arm crossing frame;
    midground = practitioner's torso partially occluded; background =
    blurred clinic.

    v718c: arm's flesh forms CLOSED LOOP where detached skin reconnects
    to itself, visible HOLE in middle (NOT a U-shape).

    [Subject — Symptom] A massive 6-inch closed loop of detached flesh
    hanging from the tricep bone in a circular shape with visible hole
    in the middle, the skin reconnecting to itself like a detached
    fabric flap looped back through, fills the immediate
    center-foreground.

    [Subject — Host] The arm belongs to a white man in his 50s wearing
    a green scrub top on the viewer-left, arm extended forward across
    the frame, crossing in front of the practitioner's torso behind.

    [Action] The practitioner (dark suit, purple gloves) on the
    viewer-right reaches a purple-gloved index finger across the
    frame to point at the closed flesh loop on the patient's
    extended arm.

OPTIONAL: stage4d_vlm.json may include a "forensic_perception" field
per shot capturing kinematic_traces / z_depth_layers /
literal_vfx_observations BEFORE static_composition. Operator can
review forensic_perception before markdown is written; misattributions
caught here cost zero Banana 2 credits.

CARVE-OUTS:

  - Single-subject shots: v718a N/A (no ambiguity); v718b + v718c apply
  - No-VFX talking-head: v718c N/A; v718a + v718b apply if multi-subject
  - Environmental establishing: v718a + v718b N/A; v718c applies if VFX

PRE-OUTPUT VALIDATION (v718):

  YES For every multi-subject shot: limbs attributed by clothing-color
      trace (not face-proximity)?
  YES For every shot with depth: Z-depth foreground / midground /
      background explicitly mapped in static_composition?
  YES For every VFX-heavy shape: described literally (closed loop with
      hole / 12 inches stretched / fully detached) NOT normalized to
      closest-anatomy adjective (U-shape / few inches / nearby)?

  If any wrong, REDO Stage 4d perception before writing markdown.

V719 — SOLID-VOLUME TOPOLOGY DISCIPLINE (NEW 2026-05-13)
=========================================================

Bidirectional corollary to v717 + v718c. v716/v717 "U-shape" vocabulary
created hallucinated holes in Banana 2 renders where source had SOLID
unbroken flesh.

v719a — SOLID-VOLUME VOCABULARY SWAP

When source shows SOLID continuous flesh / mass, DROP topology-implying
geometric anchors. Replace with solid-volume metaphors:

Banned (topology-implying) when source is solid:
  "deep U-shape" / "V-shape" / "C-shape" / "Y-shape" /
  "doughnut shape" / "ring shape" / "loop" / "hole" / "opening" /
  "gap" / "split" / "fork" / "open arc" / "semicircle"

Required (solid-volume metaphors):
  "continuous solid sheet of draped flesh"
  "dense unbroken curtain of loose skin"
  "solid flap hanging straight down"
  "thick mass of pendulous flesh"
  "uninterrupted drape of skin"
  "single continuous fold"
  "solid continuous overhang"
  "thick unbroken mass"
  "continuous slab of soft tissue"

Banned -> Required mapping:
  Sagging arm:    "deep U-shape" -> "continuous solid sheet of draped
                   flesh hanging 3 inches below the tricep, a dense
                   unbroken curtain of loose skin draping straight down"
  Distended belly: "U-shape pouch" -> "solid continuous overhang,
                   thick unbroken mass of distended tissue draped over
                   the belt"
  Jowl drop:      "U-pouch" -> "single continuous fold of pendulous
                   flesh, uninterrupted drape of skin"

v719b — TOPOLOGY BANS (extends v604b)

Append to v604b negatives when source is solid:

  No holes in the flesh. No negative space in the center of the
  [body part]. No loops. No ring shapes. No openings. No gaps. No
  splits. The hanging skin MUST be a solid, continuous, unbroken
  flap.

v719c — BIDIRECTIONAL VFX RECOGNITION (extends v718c)

If source has IMPOSSIBLE VFX (closed loops, holes, detached, multi-
plied features) -> describe LITERALLY (v718c original direction).

If source has SOLID UNBROKEN volume -> describe AS SOLID AND UN-
BROKEN explicitly. DO NOT use topology vocabulary (U-shape, V-shape,
loop, hole, opening, ring, gap) when source is solid.

Verification heuristic: would Banana 2 render the prompt's topology
matching the source's topology? If prose says "U-shape" -> Banana 2
renders U-shape opening; if source is solid, mismatch.

Selection guide:
  Source flesh forms CLOSED LOOP / HAS HOLE -> v718c literal topology
  Source flesh is SOLID + CONTINUOUS, no negative space -> v719a solid-
    volume metaphors

CARVE-OUTS:
  - Genuine VFX-loop sources (rare but real): v719a/b/c N/A;
    keep v718c original literal topology language.
  - Thinning hair / sparse hair: gaps ARE part of symptom; v719b N/A;
    v719a still applies ("sparse coverage" not "U-shape gaps").
  - AFTER / RESULT frames: v714 governs; topology question gone.
  - Persona NOT affected.

PRE-OUTPUT VALIDATION (v719):

  YES For solid-volume symptom sources: zero topology-implying
      anchors (deep U-shape / V-shape / loop / hole / opening)?
  YES Solid-volume metaphor present in [Subject — Symptom] block?
  YES Topology bans in negatives block (No holes / No negative space
      / No loops / MUST be solid continuous flap)?
  YES Bidirectional check: prose topology matches source topology?

  If any wrong, FIX before emitting.

V720 — LATERAL X-AXIS COMPOSITION (NEW 2026-05-13)
===================================================

v713f Z-axis stacking + v715 anchor modes assume hero prop / symptom
occupies a DEPTH plane (foreground / midground / background). Source
videos often have symptom extended LATERALLY (to the side, parallel
to camera plane). Z-axis grammar collapses lateral sources — Banana
2 defaults arm to forward-toward-camera or crossing-chest.

v720a — LATERAL EXTENSION CARVE-OUT (amends v713f)

When hero prop / symptom extends LATERALLY (to the side, parallel
to camera plane), Z-axis layering does NOT apply. Extended limb and
host's torso share SAME midground depth plane. Switch to X-axis
relational grammar.

Triggers when source shows extension:
  - straight out to the viewer-left
  - straight out to the viewer-right
  - straight upward overhead
  - straight downward toward the floor
  - at a 45-degree angle laterally

Composition-block structure for lateral-extension scenes:

  [Composition] [camera grammar] + [camera height at symptom anchor
  level], 9:16 vertical framing. The patient and the main character
  stand side-by-side in the MIDGROUND depth plane. The patient's
  [body part] extends straight outward to the viewer-[left/right],
  filling the [left/right] side of the frame. The [body part]
  dominates the [left/right] side; both characters are visible at
  chest-up framing.

No foreground/midground/background depth layering. Single midground
plane shared by patient + practitioner + extended limb. X-axis
spatial language fills the [Composition] block.

v720b — LATERAL VECTOR GRAMMAR (amends v712)

For ANY extended limb, declare EXPLICIT LATERAL VECTOR relative to
viewer.

Required directional clauses:
  "extended straight outward to the viewer-left"
  "extended straight outward to the viewer-right"
  "extended straight forward toward the camera"
  "extended straight upward overhead"
  "extended straight downward toward the floor"
  "extended at a 45-degree angle upward to the viewer-right"
  "extended laterally to the side, parallel to the ground"

Banned (loses lateral vector; Banana 2 defaults render WRONG):
  "extended arm" (no direction)
  "outstretched arm" (no direction)
  "arm reaching out" (implies forward-toward-camera)
  "arm in the foreground" (vector ambiguous)
  "arm raised" (could mean up / out / forward)

v720c — LIMB-POSE STRUCTURAL BANS (extends v604b)

Append to v604b negatives for lateral-extension scenes:

  No arm crossing the chest. No arm thrust forward toward the lens.
  No arm reaching toward the camera. The arm MUST extend straight
  out to the side, parallel to the ground. No overlapping bodies.
  No persona hiding behind the patient.

Adapt to other lateral extensions:
  Leg extended:    "No leg crossing the body. No leg thrust forward.
                   The leg MUST extend straight out to the viewer-
                   [left/right], parallel to the ground."
  Hand overhead:   "No hand crossing the head. No hand reaching
                   forward. The hand MUST extend straight upward
                   overhead."
  Both arms out:   "No arms crossed at the chest. No arms reaching
                   forward. Both arms MUST extend straight outward
                   to opposite sides, parallel to the ground."

WORKED EXAMPLE — lateral-arm-extension HOOK (post-v720):

  [Composition] 50mm portrait lens, deep focus, straight-on at
  chest-level, 9:16 vertical framing. The patient and the main
  character stand side-by-side in the midground depth plane. The
  patient's right arm extends straight outward to the viewer-left,
  filling the left side of the frame, parallel to the ground.

  [Action] The patient holds a mug to her mouth with her viewer-
  right hand, actively drinking. Her viewer-left arm is held
  straight out to the side, parallel to the ground.

  Negatives: ... No arm crossing the chest. No arm thrust toward
  the camera. No overlapping bodies. No persona hiding behind the
  patient.

PAIRING:
  v713f Z-axis stacking — DOES NOT apply on lateral-extension
    scenes (v720a carve-out)
  v715 Mode 6 body-part-thrust — applies BUT thrust direction
    switches from forward-toward-camera to lateral
  v716 v715f — still applies; v720 modifies thrust direction
  v717 v605c symptom-first — still applies
  v719 solid-volume — composes; lateral arm with solid drape uses
    BOTH v719 + v720

PRE-OUTPUT VALIDATION (v720):

  YES Lateral-extension scenes name midground depth plane (no
      Z-axis foreground / midground / background layering)?
  YES Every extended limb has explicit lateral vector clause (NOT
      ambiguous "extended arm" / "outstretched")?
  YES Limb-pose bans in negatives block (No arm crossing chest /
      No thrust forward / arm MUST extend straight out / No
      overlapping bodies)?

  If any wrong, FIX before emitting.

V721 — v698A ACTIVATION GATE (NEW 2026-05-13)
==============================================

v698A fires (speaker: voiceover + voiceover_anchor_image) ONLY when
persona's face is NOT visible at t=0 OR persona is NOT lip-syncing
the line. LLMs apply v698A aggressively from corpus prior ("recipe
scene = voiceover") even when scene's Image shows persona on-camera
lip-syncing. Platform correctly renders TWO Veo clips per scene per
v698A, but b-roll is REDUNDANT when persona is already on-camera.
Doubles Veo cost (~$0.30-0.50 per wasted render) + creates audio-swap
artifacts.

Surfaced 2026-05-13 from nuri-prostate-health-hose-blast-safe lift:
scenes 2-7 recipe-prep with persona "eyes locked to the lens, mouth
open mid-word" — LLM marked all 6 as voiceover; platform created
unwanted PAIRED clips on all 6.

v721 rule:

When the Image bound by a Scene's "- **image:**" field shows persona
ON-CAMERA + visible at t=0 + lip-syncing the line, speaker: MUST be
"on-camera" (or persona handle on-camera), NOT voiceover. v698A is
N/A; no anchor field required.

Trigger keywords in bound Image body that mandate speaker: on-camera:
  "eyes locked to the lens" / "eyes locked to the camera"
  "mouth open mid-word" / "mouth slightly parted in mid-speech"
  "mid-utterance" / "facing the camera" / "squared to camera"
  "face visible" + "chest-up framing" / "head-and-shoulders"
  "lip-syncing" / "on-camera dialogue"

Disallowed combination:
  Scene N:
    - **image:** image_K
    - **speaker:** voiceover
    - **voiceover_anchor_image:** image_M
  WHERE image_K body contains any trigger keyword above.

v721 decision tree when authoring recipe / b-roll / cutaway:

  Q1: Is persona's face visible in bound image?
    NO  -> speaker: voiceover + voiceover_anchor_image (per v698A)
    YES -> Q2

  Q2: Is persona shown lip-syncing the line (mouth open mid-word,
      mouth moving with dialogue beat)?
    YES -> speaker: on-camera — NO anchor
    NO  -> Q3 (persona visible but silent)

  Q3: Persona's mouth visible but closed / not speaking?
    YES -> speaker: voiceover + voiceover_anchor_image (per v698A)
           AND image body should note "mouth closed" / "lips together"
           to prevent Veo from auto-animating speech on silent visual.

CARVE-OUTS:
  - Scene's image shows hands-only / no face: v721 N/A; v698A fires.
  - Scene's image shows persona but mouth closed (silent passive
    shot): v721 N/A; v698A fires; image body explicit on "mouth
    closed".
  - HOOK / EXPLAIN / CTA: persona almost always on-camera lip-
    syncing -> speaker: on-camera. v721 catches mis-application.
  - Voiceover narrator different from on-screen character (omniscient
    over patient-only b-roll): v698A fires with narrator's anchor.

FIX FOR v698A MISUSE (operator-side):

When grep gate fails, for each scene with v721 violation:
  1. Change "- **speaker:** voiceover" -> "- **speaker:** [persona
     handle] on-camera" (or "on-camera").
  2. Remove "- **voiceover_anchor_image:** image_M" field.
  3. If image_M's only purpose was voiceover_anchor (no other Scene
     references it), remove "### Image M" from ## Images and
     renumber. If other Scenes reference it, keep but remove the
     "- **role:** voiceover_anchor" field.

COST IMPACT:

v721 violations cost +1 Veo render per affected scene (unused b-roll
audio twin) at ~$0.30-0.50/render. A 6-scene recipe video with all
v721-violating voiceovers = +$2-3 wasted. Fix halves cost on
affected scenes.

PAIRING WITH v698A:

v721 is a GATE on v698A's activation. v698A spec unchanged — when
speaker: voiceover is correctly declared, v698A's two-clip render
still fires. v721 prevents incorrect speaker declarations from
triggering v698A unnecessarily.

PRE-OUTPUT VALIDATION (v721):

  YES For every scene with speaker: voiceover: bound image_K does
      NOT contain trigger keywords (eyes locked to the lens, mouth
      open mid-word, mouth slightly parted in mid-speech, mid-
      utterance, facing the camera, squared to camera, lip-syncing,
      on-camera dialogue)?
  YES For every scene where persona is on-camera lip-syncing in the
      bound image: speaker: is on-camera, NOT voiceover?
  YES Zero unused voiceover_anchor_image: image_M references (every
      anchor image is referenced by at least one Scene's
      voiceover_anchor_image: field)?

  If any wrong, FIX before emitting.

V722 — PERSONA WARDROBE BAN (NEW 2026-05-13)
=============================================

STRICT extension of v553.1 + v609 + v610. Persona identity (including
clothing / wardrobe / accessories / medical attire / scrubs / coats /
ties / stethoscope / badge / glasses / hair / race / age / build) is
carried by the UPLOADED CHARACTER REFERENCE IMAGE. NEVER describe
persona wardrobe in:

  - Image prompt body prose
  - static_composition.subject (decode side)
  - Action note prose (- **action_note:** ...)
  - Scene line / dialogue (- **line:** ...)
  - Veo final-prompt body

Persona wardrobe lives ONLY in the Ingredients table Description
column. Single source of truth. Never duplicated.

Surfaced 2026-05-13 from nuri-prostate-health-hose-blast-safe lift:
every Image block had "She wears her crisp white doctor's coat" or
similar despite v553.1 being live since the initial rule pipeline.

BANNED phrasings when referring to PERSONA (the main character):

  "wears her [clothing item]" / "wears his [clothing item]"
  "wearing [clothing item]" (when subject is persona)
  "the main character in her [color] [garment]"
  "her crisp white doctor's coat" / "his white lab coat"
  "her scrub top" / "her blue scrubs" / "her V-neck scrub"
  "her uniform" / "her clinical attire"
  "stethoscope around her neck" / "wears a stethoscope"
  "her medical badge" / "clipped to her lapel"
  "wears [color] [garment]" (any persona-wardrobe pattern)

REQUIRED when persona action involves clothing or visible attire:

  "The main character [verb] [action] [object]."
  No "wears" / "wearing" / wardrobe-item mentions for persona.

ASYMMETRY (CRITICAL):

  PERSONA wardrobe -> v722 BANNED in body prose (upload carries identity)
  NON-PERSONA wardrobe -> v610 / v622 / v669 REQUIRED in body prose
                          (prose is the only anchor for non-persona;
                          Banana 2 has NO upload for patient / customer
                          / bystander — wardrobe goes in [Subject —
                          Host] block on first appearance)

WHERE PERSONA WARDROBE LEGITIMATELY LIVES:

  Ingredients table Description column — canonical source-of-truth for
  the upload bind:

    | Name | Type | Description | Source | Attached to |
    | the main character | character | persona identity carried by
      upload — Nuri, modern clinic doctor, half-Korean, late 20s,
      white doctor's coat, professional attire, stethoscope | upload
      — personas/refs/nuri.png | image_1, image_2, ... |

  This is the ONLY place persona wardrobe appears. Image prompt bodies
  never repeat it.

WORKED EXAMPLE — nuri-prostate-hose-blast-safe (surfacing case):

  Pre-v722 (persona wardrobe in body prose):
    "The main character stands inside a men's public restroom...
    She wears her crisp white doctor's coat and holds a thick
    pressure hose..."
    ^ "She wears her crisp white doctor's coat" violates v722

  Post-v722:
    "The main character stands inside a men's public restroom...
    She holds a thick pressure hose..."
    ^ Wardrobe descriptor dropped. Upload renders the coat per
    Ingredients metadata.

DISAMBIGUATION (persona + non-persona in same scene):

  [Subject — Host] (NON-PERSONA description — REQUIRED per v610/v622)
    A white woman in her 60s, heavy build, short blonde bob, dark
    green V-neck top, navy shorts revealing bare calves. She faces
    the camera with a distressed expression.

  [Action] (persona action — wardrobe BANNED per v722)
    The main character on the viewer-right reaches an index finger
    toward the symptom.
    ^ "purple-gloved" is BANNED here (persona wardrobe); if persona's
    gloves are critical to the diagnostic-pointer visual, the glove
    color goes in the Ingredients table metadata, not in body prose.

CARVE-OUTS:

  - Non-persona wardrobe REQUIRED in body prose per v610 / v622 /
    v669. v722 is persona-only.
  - Ingredients table Description column is the canonical SoT for
    persona wardrobe — v722 N/A there.
  - Wardrobe AS PROP (not identity): if persona REMOVES / DROPS /
    HOLDS UP a wardrobe item as rhetorical prop (e.g. persona pulls
    off stethoscope and dangles it for emphasis), the wardrobe-as-
    prop action IS allowed in action_note — but base wardrobe stays
    in Ingredients metadata.
  - HOOK weird-action involving wardrobe (per v539): action goes
    in action_note; base wardrobe stays in Ingredients metadata.

PRE-OUTPUT VALIDATION (v722):

  YES Zero hits on banned persona-wardrobe phrasings in body prose
      (Image prompts / action_notes / scene lines / Veo final prompts)?
  YES Ingredients table Description column DOES contain canonical
      persona wardrobe metadata?
  YES Non-persona wardrobe descriptions present in [Subject — Host]
      blocks (per v610 / v622 / v669)?

  If any wrong, STRIP persona wardrobe from body prose, ENSURE
  Ingredients metadata captures it, ENSURE non-persona wardrobe
  retained.

Output the decoded markdown per code/template_new_format.md skeleton +
strict v593 parser format. Include ## Sources (manifest / transcript /
shots / motion / source MP4 paths) and ## Used in (placeholder).
EOF
}

# Always write to a temp file as fallback
TMPDIR_PATH="${TMPDIR:-/tmp}"
BUNDLE_FILE="$TMPDIR_PATH/decode_bundle_$(date +%s).md"
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
    echo "[decode_bundle] Bundle saved (POSIX):   $BUNDLE_FILE"
    if [[ -n "$WIN_BUNDLE_FILE" ]]; then
        echo "[decode_bundle] Bundle saved (Windows): $WIN_BUNDLE_FILE"
    fi
}

print_upload_guidance() {
    echo "[decode_bundle] Upload options for LLMs with paste-size caps (e.g. Gemini app):"
    echo "[decode_bundle]   - Drag the .md file from Explorer into the chat's attach field"
    if [[ -n "$WIN_BUNDLE_FILE" ]]; then
        echo "[decode_bundle]   - Or paste the Windows path above into the upload field"
    fi
    echo "[decode_bundle]   - Then upload the source MP4 + add one-line task prompt:"
    echo "[decode_bundle]       \"decode this video\""
}

if [[ -n "$CLIP_CMD" ]]; then
    if cat "$BUNDLE_FILE" | $CLIP_CMD 2>/dev/null; then
        echo "[decode_bundle] OK: ${#BUNDLE_FILES[@]} files concatenated (~${BYTES} bytes), piped via '$CLIP_CMD'"
        print_paths
        print_upload_guidance
    else
        echo "[decode_bundle] WARNING: clipboard pipe failed (sandboxed env or clip locked)"
        print_paths
        echo "[decode_bundle] Manual clip pipe: cat \"$BUNDLE_FILE\" | clip   (or pbcopy / xclip)"
        echo "[decode_bundle] OK: ${#BUNDLE_FILES[@]} files concatenated (~${BYTES} bytes)"
        print_upload_guidance
    fi
else
    echo "[decode_bundle] No clipboard tool found."
    print_paths
    echo "[decode_bundle] OK: ${#BUNDLE_FILES[@]} files concatenated (~${BYTES} bytes)"
    print_upload_guidance
fi
