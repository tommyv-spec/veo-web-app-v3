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
              (line 2, after persona binding):
                "Use the uploaded product reference image for <name>
                — match its label, packaging, [color/wordmark], and
                proportions exactly."
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
