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
    cat <<EOF

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
EOF
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
