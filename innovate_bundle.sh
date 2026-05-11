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
  Gate 4 — every shot Image has fenced `**Image prompt:**` block
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

if [[ -n "$CLIP_CMD" ]]; then
    if cat "$BUNDLE_FILE" | $CLIP_CMD 2>/dev/null; then
        echo "[innovate_bundle] OK: $TOTAL_FILES files concatenated (~${BYTES} bytes), piped via '$CLIP_CMD'"
        echo "[innovate_bundle] Bundle also saved to: $BUNDLE_FILE"
        echo "[innovate_bundle] Paste into your LLM (Gemini / GPT-5 / Claude API)."
        if [[ -z "$CELL_SPEC" ]]; then
            echo "[innovate_bundle] Add target cell spec to LLM prompt, e.g.:"
            echo "[innovate_bundle]   \"innovate this for Nuri male-ED clinic\""
        else
            echo "[innovate_bundle] Target cell already embedded: $CELL_SPEC"
        fi
    else
        echo "[innovate_bundle] WARNING: clipboard pipe failed"
        echo "[innovate_bundle] Bundle saved to: $BUNDLE_FILE"
        echo "[innovate_bundle] Manual copy: cat \"$BUNDLE_FILE\" | clip"
    fi
else
    echo "[innovate_bundle] No clipboard tool. Bundle saved to: $BUNDLE_FILE"
fi
