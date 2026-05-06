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
BUNDLE_FILES=(
    "code/template_new_format.md"
    "code/template_reference.md"
    "wiki/meta/decode-grammar-checklist.md"
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

    cat <<EOF

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

if [[ -n "$CLIP_CMD" ]]; then
    if cat "$BUNDLE_FILE" | $CLIP_CMD 2>/dev/null; then
        echo "[decode_bundle] OK: ${#BUNDLE_FILES[@]} files concatenated (~${BYTES} bytes), piped via '$CLIP_CMD'"
        echo "[decode_bundle] Bundle also saved to: $BUNDLE_FILE"
        echo "[decode_bundle] Paste into your LLM + upload the source MP4 + add a one-line task prompt:"
        echo "[decode_bundle]   \"decode this video\""
    else
        echo "[decode_bundle] WARNING: clipboard pipe failed (sandboxed env or clip locked)"
        echo "[decode_bundle] Bundle saved to: $BUNDLE_FILE"
        echo "[decode_bundle] Open it manually: cat \"$BUNDLE_FILE\" | clip   (or pbcopy / xclip)"
        echo "[decode_bundle] OK: ${#BUNDLE_FILES[@]} files concatenated (~${BYTES} bytes)"
    fi
else
    echo "[decode_bundle] No clipboard tool found. Bundle saved to: $BUNDLE_FILE"
    echo "[decode_bundle] OK: ${#BUNDLE_FILES[@]} files concatenated (~${BYTES} bytes)"
fi
