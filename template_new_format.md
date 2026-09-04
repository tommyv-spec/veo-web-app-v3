<!--
  KAVENO SCENE TABLE — NEW FORMAT (v696)
  Fill in below. Key conventions (see template_reference.md for full docs):

  ============================================================
  v696 — HARD-FAIL PRE-OUTPUT VALIDATION GATES (read before write)
  ============================================================
  The platform parser (code/image_platform.py:parse_scene_table) is
  STRICT and silent on failure — bad headers don't error, they're
  skipped, and the import returns "Parse error: No scenes found"
  or "Parse error: Image N: no fenced 'Image prompt:' block found".
  These four classes of errors keep recurring across LLM-authored
  decodes + lifts; treat them as PRE-OUTPUT VALIDATION GATES that
  MUST pass before writing any markdown:

  GATE 1 — text_card scenes have NO `### Image N` header (v682d)
    A text_card scene exists ONLY as a `### Scene N` block in
    `## Storyboard` with `scene_type: text_card` + `caption:` +
    `bg_color:` + `duration:`. It MUST NOT have a corresponding
    `### Image N` block in `## Images`. Image numbering is
    NON-CONTIGUOUS by design — image_1, image_2, image_3, image_5,
    image_6 (no image_4) is correct when scene 5 is a text_card.
    Concrete failure: writing `### Image 4` + `scene_type: text_card`
    in `## Images` → parser looks for the mandatory fenced
    "Image prompt:" block, doesn't find it, aborts the whole import.
    The text_card scene block in `## Storyboard` ALSO has NO
    `- **image:**` bullet (no image to reference — the card is
    rendered server-side by ffmpeg drawtext).

  GATE 2 — every shot scene chains forward through state-evolution
    (v580 + v604 visual continuity)
    When N consecutive scenes share setting + persona + camera angle
    and the only delta is prop / state / ingredient change (recipe
    steps, before/after, transformation arc), each scene from #2
    onward MUST have `reference_image:` pointing at the previous
    image AND a non-empty `visual_delta:` line naming ONLY what
    changed vs. the parent. text_card scenes ARE NOT a chain
    breaker — chain references skip across them: image_3 is parent
    of image_5 if image_4 is a text_card. Concrete failure: when
    image_5's reference_image is missing (or `none`) on a recipe
    chain, Banana 2 generates a fresh kitchen variation with new
    cabinet colors / different counter / different lighting, and
    the audience sees the cut as "different kitchen" instead of
    "next recipe step in the same kitchen."

  GATE 3 — `### Image N` / `### Scene N` headers are STRICT regex
    `^###\s+Image\s+(\d+)\s*$` and `^###\s+Scene\s+(\d+)\s*$`. The
    line ends immediately after the integer. Descriptive suffixes
    like `### Scene 1 — HOOK clinical-exam (~5s)` are silently
    skipped. h4 splits like `#### Scene 8a` / `#### Scene 8b` are
    rejected. Splitting one scene across two clips is done by
    adding a SECOND `- **line:** / - **action_note:**` pair inside
    the same `### Scene N` block, never via h4 sub-scenes.

  GATE 4 — every shot Image block has a fenced `**Image prompt:**`
    code block, EVERY scene block has `- **image:** image_N`
    (except text_card scenes which have neither bullet nor parent
    image). Missing the fenced block is the most common parser
    abort.

  GATE 6 — every shot scene has `- **action_arc:**` field with a
    `→`-separated force-verb chain (v697 Rule A). The chain renders
    into BOTH the action_note (each beat tagged with the active verb
    in CAPITALS) AND the Veo final prompt's "Across the X seconds"
    section. Force-verb library: FORCE-ON-PROP (LIFT / SLAM / SLAP /
    KNOCK-OUT / PUSH-AWAY / SCATTER / RIP / SHATTER) / LIQUID-AGENT
    (POUR / DRIZZLE / SPRAY / CASCADE / DISSOLVE / RIBBON) / PRESSURE
    (TRIGGER / BLAST / ATOMIZE / ENGULF / IGNITE) / BODY-ANATOMY
    (POINT / TRACE / MARK / PALPATE / PRESS / REVEAL) /
    WIND-UP-IMPACT (RAISE / WIND-UP / SMACK / THROW / SPLATTER) /
    CONFRONT (STEP-FORWARD / LEAN-IN / TURN / LOCK-EYES / GRIP /
    BEND) / GESTURE (RAISE-HAND / GESTURE-FORWARD / POINT-TO-LENS /
    OPEN-PALM / LOWER) / RECIPE-MOTION (TILT / KNEAD / FOLD / WHISK /
    DIP / PINCH / PULL-APART). HOOK shots MUST be walked at 5 frames
    minimum (start / q1 / mid / q3 / end) since SLAM-class spectacles
    peak at q3.

  GATE 7 — non-persona characters (bystander / extra / patient) have
    BUILD / WEIGHT / BODY-TYPE in their image prompt body (v697
    Rule B). Order: race → age → BUILD → hair → clothing → expression.
    Use one of: OVERWEIGHT / HEAVY BUILD, SLIM / ATHLETIC BUILD,
    STOCKY / MUSCULAR BUILD, PLUS-SIZE / CURVY BUILD, SKINNY / GAUNT
    BUILD. Without this, Banana 2 generates average-build defaults
    and the hook's audience-resonance + shame-proxy signal collapses.

  GATE 8 — backgrounds get ONE descriptive sentence; foreground +
    composition + people get rich detail. Pre-v697 image prompts
    spent 3-4 sentences re-describing kitchen cabinets / counter /
    window light; v697 collapses background to one opener and spends
    the saved attention budget on foreground people + props +
    expression beats.

  GATE 9 — scene with `speaker: voiceover` MUST have
    `voiceover_anchor_image:` field pointing at an image_N defined in
    `## Images` (v698A). The anchor image is the start frame for the
    audio-twin Veo render; persona's face must be visible at t=0
    there. Without the anchor field set, the platform cannot
    render the audio source.

  GATE 10 — voiceover_anchor_image's `cast:` MUST contain a persona
    character (the main character). The anchor must show the
    persona on-camera; b-roll / VFX images are NOT valid anchors
    (no face for Veo to lip-sync to).

  GATE 11 — every voiceover scene MUST have a `- **line:**` field
    (lowercase per v693). The line is the voiceover spoken by the
    audio-twin clip; without it there is no audio content to render.

  GATE 12 — voiceover line word count MUST fit the visual scene's
    `target_duration_s`. Rough rule: words ≤ 2.6 × target_duration_s.
    A visual scene with target_duration_s=4.5 fits ~12 words. If the
    line exceeds this, either split the scene OR shorten the line.
    Whisper-VAD trims audio at export to match the visual; over-budget
    lines lose tail words.

  GATE 13 — every image with `role: voiceover_anchor` MUST have BOTH
    a torso-framing keyword (`torso` / `waist-up` / `chest-up`) AND
    a hands-visible keyword (`hands at chest` / `hands visible` /
    `open-palm gesture` / `hands in frame`) in its prompt body.
    Veo lip-syncs better when the persona has natural gestural
    articulation; static-still torso renders awkward. Tight bottom
    crop (no floor / no feet / no counter-front) per v603.

  GATE 14 — text_card detection discipline (v699). Before emitting any
    `scene_type: text_card` scene, verify ALL FIVE of:
      (1) PySceneDetect anchors it as its OWN shot (NOT the tail of
          another shot — check shots.json)
      (2) ≥80% solid background, no live-action footage visible
      (3) sustained ≥0.5s duration (not a flicker / 1-frame cut-to-
          black)
      (4) audio is silent OR pure SFX — NO continuing voiceover from
          surrounding scenes
      (5) caption text dominates visible content (not a small overlay
          on live frame)
    If ANY criterion fails, it's NOT a text_card. Common false
    positives: karaoke caption fading IN at a prior shot's tail
    (criterion 1 fails — same PySceneDetect shot), cut-to-black
    flicker (criterion 3 fails), logo splash (criterion 5 fails).
    Karaoke captions are decode-only per v621 — record them on the
    surrounding shot's `- **caption:**` bullet, NOT as a separate
    text_card scene.

  GATE 5 — `- **line:**` field is FULLY LOWERCASE (v693)
    Veo TTS over-emphasizes capitalized words ("GUIDE" → shouted),
    Whisper-VAD then drops the over-emphasized syllables → the
    intended word is missing from the final audio. Even Title-Case
    sentence starts trigger this in edge cases. Use lowercase
    throughout: "comment guide and i will send you the recipe."
    not "Comment GUIDE and I will send you the recipe."

  Pre-output verification command (run BEFORE pushing):
    python -c "
    import re
    t = open('videos/<file>.md', encoding='utf-8').read()
    images = sorted(int(m.group(1)) for m in re.finditer(r'^###\\s+Image\\s+(\\d+)\\s*$', t, re.MULTILINE))
    scenes = sorted(int(m.group(1)) for m in re.finditer(r'^###\\s+Scene\\s+(\\d+)\\s*$', t, re.MULTILINE))
    print(f'Images: {images}')
    print(f'Scenes: {scenes}')
    text_card_scenes = re.findall(r'### Scene (\\d+)\\s*\\n[^#]*scene_type:\\s*text_card', t)
    print(f'text_card scenes: {text_card_scenes}')
    print(f'Image numbering NON-CONTIGUOUS (text_card gap is OK): {images}')
    "

  ============================================================

  • Image prompts: always open with "Shot on iPhone with wide-angle lens, handheld,
    deep focus throughout, natural daylight." Never cinema camera specs.
    UGC-REALISM STANDARD (house standard — wiki [[realistic-ugc-prompt-templates]]; src raw/Basic prompt templates...):
      our look is a phone-shot UGC photo, NOT professional photography. Every image prompt must satisfy:
        - NO blur anywhere — background, walls, furniture all SHARP. NO bokeh, NO shallow depth of field. ("deep focus" = correct.)
        - ONLY natural lighting, no artificial lighting.
        - colors ultra-realistic — NOT oversaturated, not too warm/cold/dark/bright. (Drop "vibrant HDR / warm palette" exaggeration.)
        - realism bank: visible pores, skin texture, single hair strands, subtle wrinkles, imperfections, realistic fabric textures, reflections + shadows.
        - close with "looks like a natural smartphone photo, not professional photography."
      BANNED in image prompts: bokeh, shallow depth of field, f/1.8|f/5.6, softbox, three-point/studio lighting, cinematic, film grain, editorial.
      FIELD-ORDER TEMPLATE (write the prompt body as these natural sentences, in this order — the operator's proven prompt-document format; NOT bracket [Composition]…[Tech] tags. This field-order ALSO walks the v586 six blocks, so it is v586-compliant + the house format):
        "Use the uploaded character reference image for the main character.
         [main character pose/identity-via-upload]. The person is [position/action]. The camera is [eye/chest/waist/selfie/low/high/front/side position]. The person is [distance: close/medium/chest-up/etc + relative depth if multi-subject]. The person is [looking direction]. The person is [action toward camera: talking/presenting/demonstrating]. [OPTIONAL second/third person, with their own depth]. The person is located in [environment]. The room contains [background details]. The main focus of the image is [X]. The composition clearly directs attention toward [main subject]. The lighting is ONLY natural lighting, no artificial lighting at all. There is absolutely no blur anywhere in the image. Everything is in perfectly sharp focus. The realism is top-tier: visible pores, realistic skin texture, single hair strands, subtle wrinkles, imperfections, realistic fabric textures, reflections and shadows. The colors are ultra-realistic, not oversaturated, not too warm, not too cold, not too dark and not too bright. The image looks like a natural smartphone photo, not professional photography. [safety/positive frame]. Aspect ratio 9:16."
      Recreating a proven frame: match the ORIGINAL depth + attention order (DISTANCE/MAIN-FOCUS fields), don't default avatar-front; use innocent/positive wording, no risk-word negatives. See [[realistic-ugc-prompt-templates]] §recreating-a-frame.
  • Main character: NEVER described — character reference is passed externally to
    Nano Banana 2 on every generation as IMAGE 1. Refer to them as "the main
    character". Describe only pose, expression direction, position in frame.
  • Product (v573): when the video has a branded product whose label/packaging
    must render correctly, it gets uploaded as a clean isolated reference at
    Flow slot 1 (Flow's "Image 2" positionally). In product frames (hero
    close-ups, label-visible holds, pours where the bottle is in frame, end-
    card reveals) the prompt invokes the product BY NAME — "the [brand]
    bottle, label-forward to camera" — same convention as persona ("the
    main character"). The platform's name-binding logic attaches the product
    upload to those scenes. In non-product frames the product is simply not
    mentioned. If a scene's reference_image chain points to an earlier image
    where the product is already locked-in correctly, the chain carries it
    through and the product does NOT need to be re-mentioned. See ISOLATED-
    REFERENCE RULE below and the v573 product-frame section in
    template_reference.md.
  • Secondary characters (scene-specific one-offs): describe fully on first appearance.
  • Framing: describe COMPOSITION (where elements sit in frame), not aspect ratio.
    Match the source video's framing 1:1.
  • Camera distance: intimate by default — "camera approximately one arm's length
    from him, upper body filling the upper two-thirds of the frame". Even "pulled
    back" seated shots stay tight: 3-4 feet, cropped at the knees, NO feet, NO
    floor visible. Kaveno content never shows a full body — there's no room on
    9:16 with burned-in captions.
  • reference_image: scene-to-scene visual parent. Image 1 = none. Sequential beats
    chain tightly. Register changes point back to image 1.
  • reference_image (v859): ONE or TWO parents — `image_N` or `image_N, image_M`.
    Two = the frame inherits different things from different parents:
      entry 1 -> NAME it "the prior-scene reference image" in the prompt (pose/objects)
      entry 2 -> NAME it "the body reference image" in the prompt (the body)
    BOTH MUST BE NAMED or the unnamed one is blended as generic context — the
    translator only rewrites those exact phrases; "as in the reference image"
    binds nothing. Max 2 (slot 0 = the persona upload). Duplicates, forward refs
    and a 3rd entry are hard errors. Canonical: template_reference.md §v859.
  • EXPLICIT REFERENCE BINDINGS (v581, supersedes v552 + v573 manifest auto-prepend):
    each `Image prompt:` fenced block MUST begin with explicit binding lines that
    declare which uploads are used for what. Three line types in this order:
    (1) PERSONA — always present, every image:
        "Use the uploaded character reference image for the main character."
    (2) PRODUCT — only when the image binds the product upload (i.e. when the
        new `product_image:` top-level field is set on this image):
        "Use the uploaded product reference image for [product ingredient name]."
    (v609 — concise form. Banana 2 auto-matches face / clothing / label /
        packaging / color / proportions when an upload is referenced;
        the verbose "— match X, Y, Z exactly" clause is redundant noise
        that dilutes attention from per-image directives.)
    (3) CHAIN — only when `reference_image:` is not `none` (v589.1
        SEMANTIC FORM — preferred; the v581 "Use Image N..." form is
        kept as a legacy backward-compat alias):
        "Use the prior-scene reference image to preserve the [setting],
        [lighting], [anchor props], and continuity from the previous
        scene."
        Why semantic over numbered: each Banana 2 generation is
        independent — at job time only persona+product+chain are
        attached (max 3 inputs). A markdown reference like "Image 4"
        means nothing to Banana 2 unless rewritten by the platform
        (case-sensitive substitution \\bImage K\\b → flow slot+1).
        The semantic phrase "the prior-scene reference image" is
        understood by Banana 2 as a role descriptor AND substituted
        by the platform to "Image M" at emission — robust to direct-
        paste testing in Flow or Gemini.
    One blank line separator after the bindings, then the visual description as
    before. Banana 2 reads bindings as instructions and description as content.
    Pre-v581 markdowns where the platform auto-prepended these lines at job
    emission must be migrated — this is a hard cutover, no backward shim. New
    parallel field `product_image:` (top-level metadata, parallel to
    `reference_image:`) declares the product binding for parser convenience —
    optional, only on images that bind the product, value is the product
    ingredient name verbatim from the table. Persona binding does NOT get a
    parallel `character_image:` field because every image binds the persona —
    declaring it on every row is noise. Veo prompts stay unchanged: bottle/
    persona named only when action operates on them ("she taps the [bottle]
    label"), no appearance description (start frame carries that). Slot
    resolution at emission is unchanged from v573 — platform substitutes
    "the uploaded character/product reference image" → "Image 1"/"Image 2" via
    targeted string substitution and rewrites body "image K" chain refs to
    Flow's actual slot positions, just no longer prepends the manifest header
    (it's already in the markdown body). See template_reference for the full
    wording template, migration steps, and the rationale (visibility, author
    control, single source of truth).
  • SCENE GRANULARITY: don't over-split. Same context + same action + same image =
    ONE scene with multiple `- **line:** / - **action_note:**` pairs inside.
    Only trigger a new scene when the image, the action, or the setting changes.
  • LINE GRANULARITY (v577): don't over-split a scene into multiple `- **line:**`
    bullets when one line would carry the whole scene. Each `- **line:**` becomes
    ONE Veo clip = ONE 8-second generation. At natural clinical-authority pacing
    (~2.6 words/sec, 158 wpm) a clip comfortably fits ~21 words with ~2 words of
    tolerance on either side (opening pause + closing breath). RULE: if a scene's
    TOTAL dialogue is ≤21 words, write it as ONE line — not 2-4 short fragments.
    Split into multiple lines ONLY when total scene dialogue exceeds ~23 words
    AND each split lands on a natural syntactic boundary (sentence/clause/comma)
    producing lines that are each ≥10 words and syntactically complete. Costs
    of over-splitting: each line = one Veo generation ($), multi-line scenes
    share the same start frame anyway (no visual benefit), and rapid lip-sync
    re-cuts feel unnatural. Bad pattern (4 lines × 6 words avg, total 21 words):
    "if you pee when you laugh, / or feel the urge to go all the time, / or
    wake up at night to pee, / trust me, take this seriously" — 4 clips for
    21 words = wasteful. Good pattern (1 line, 21 words): "if you pee when
    you laugh, or feel the urge to go all the time, or wake up at night to
    pee, trust me, take this seriously" — 1 clip, same content, same start
    frame, smoother delivery.
  • DECODING SOURCE VIDEOS (v579): when the input is an MP4/MOV, the decoder
    MUST run a four-stage extraction pipeline before writing any markdown —
    NEVER reconstruct dialogue from sparse frame samples + caption OCR.
    Stages: (1) ffmpeg → 16kHz mono PCM WAV, (2) whisper.cpp + GGML model →
    timestamped dialogue segments (the AUTHORITATIVE script source), (3)
    PySceneDetect AdaptiveDetector(threshold=3.0) → real shot boundaries
    (NOT fixed-interval frame samples — those miss cuts and over-sample
    held shots), (4) view-tool inspection of frames at shot midpoints →
    setting / composition / pose per shot. Align all four signals into a
    unified timestamp-keyed manifest, then author the markdown FROM the
    manifest. Required setup: `pip install scenedetect pywhispercpp
    opencv-python --break-system-packages`. The user typically uploads a
    GGML model file (e.g. `ggml-base.en-q5_1.bin`); HuggingFace download
    is blocked in most sandboxes. The decoded markdown's top-of-file
    comment block MUST include a "PIPELINE USED FOR THIS DECODE (v579)"
    audit trail listing which stages were run. Pre-v579 anti-patterns to
    avoid: caption-driven script reconstruction (karaoke captions are
    aggressive paraphrases, missing 30-50% of spoken words), fixed-
    interval frame sampling (misses cuts between samples, over-samples
    held shots), inferring brand name from visible bottle alone (audio
    is the spoken-brand truth — they may differ), reconstructing CTA
    word from "what supplement promos usually say" (always source from
    whisper). OCR is optional cross-validation only — for brand-label
    verification — never primary content extraction. The user MAY opt
    out of OCR ("captions are generated post-render, not from the
    source"); note this in the decode header. See
    template_reference.md for the full pipeline rationale, code
    snippets, and the unified-manifest schema.
  • MOTION CAPTURE (v585, extends v579 Stage 4): the decode pipeline
    samples MULTIPLE frames per shot (not just the midpoint) and runs
    OpenCV Farneback optical flow to classify camera moves —
    static / static-handheld-with-drift / push-in / pull-back /
    pan-left / pan-right / tilt-up / tilt-down — with magnitude
    grounded in the flow data. Action_notes carry the classified
    move into the markdown ("slow pull-back over first 2s") instead
    of defaulting to "static handheld" everywhere. Calibration
    thresholds (30fps, downscale=4): magnitude < 0.5px = static;
    < 1.5px = static-handheld-with-drift; > 3px with dominant axis =
    labeled motion class. Sign conventions: zoom_rate > 0 = push-in,
    dx > 0 = pan-left, dy > 0 = tilt-up. Pipeline file:
    `v585_pipeline.py`. Setup: `pip install opencv-python
    --break-system-packages`. v585 does NOT touch the reference
    manifest — v581 explicit bindings remain live.
  • DESCRIPTION GRAMMAR PARITY (v586, extends v579 Stage 4): every
    decoded image description MUST follow the canonical Nano Banana 2
    six-block grammar — Subject / Composition / Action / Location /
    Style / Tech — and every action_note MUST follow the canonical
    Veo 3.1 five-block grammar — Cinematography / Subject / Action /
    Context / Style & Ambiance. Same vocabulary the platform's
    prompt-builder emits at GENERATION time. Mandatory dimensions per
    image (extends v521.1 pin-down): (1) SUBJECT — pose, eye
    direction, mouth state, expression beat (persona referenced by
    name per v553.1, never inline-described); (2) COMPOSITION —
    frame partition (where head / eyes land per rule of thirds),
    depth layers (foreground / middle / background populated
    explicitly), crop boundary (where bottom / top / sides cut),
    foreshortening note if wide-angle, two-shot vs single, headroom;
    (3) ACTION — current gesture, hand positions, eye tracking;
    (4) LOCATION — setting + every anchor prop with explicit position
    ("the saffron bottle stands upright on the counter to the left
    of the glass, label-forward to camera"); (5) STYLE — lighting
    direction (where the source is, hard / soft, color temp), color
    palette, mood; (6) TECH — camera type / lens (iPhone wide-angle
    by default per v553), distance from subject in feet or
    arm-lengths, focus depth (deep / shallow, where focus lands),
    motion blur if any. Mandatory dimensions per action_note:
    (1) CINEMATOGRAPHY — camera-move classification grounded in v585
    flow data; (2) SUBJECT — main character + secondary characters +
    key props named; (3) ACTION — three motion beats (start →
    mid-clip → end-beat) with explicit timing within the 8-second
    clip; (4) CONTEXT — setting carry-over with anchor-prop reuse;
    (5) STYLE & AMBIANCE — register + ambient sound cues. Why this
    rule exists: pre-v586 decodes routinely captured pose and setting
    but missed object positions ("ginger pieces visible to the right"
    became "ginger nearby"), foreshortening cues, focus depth, and
    lighting direction. Banana 2 hallucinates the missing fields when
    the prompt is vague — decode quality and generate quality are
    the same problem. The bidirectional rule cycle: improving the
    decode-side grammar improves the generate-side grammar (same
    fields, same checklist) — see template_reference.md for the
    deep-dive and examples. Migration: pre-v586 decodes in
    raw/decoded_*.md are valid as-is; new decodes from this point
    forward MUST satisfy the v586 checklist. The v579 pipeline
    Stage 4 view-tool prompt is updated to walk the six blocks
    per frame.
    v622 intensity-calibration amendment (2026-07-22): every new
    decode MUST also emit this table under `## Adaptation-extraction`:

      ### Hero-symptom intensity ledger
      | Hero symptom / carrier | Literal observed scale + comparison anchor | Intensity | Exaggeration headroom |
      |---|---|---|---|
      | <symptom> | <frame/body/object relation> | <1/5-5/5> | <YES or NO> |

    Use one row per hero symptom. `big`, `large`, or `bloated` alone
    is not a comparison anchor. If no bodily symptom is visible, emit
    `none observed | n/a | n/a | n/a`. A 5/5 viral-max symptom MUST
    have headroom NO. The ledger records the SOURCE ceiling so a later
    step-up does not invent intensity that was already present.
    v790 shown-beats amendment (2026-07-22): every new decode MUST also
    emit this table under `## Adaptation-extraction`:

      ### Shown beats ledger
      | Source beat ID | Frame / clip evidence | Shown action / process step | Meaningful objects visibly present |
      |---|---|---|---|
      | SB1 | <clip/frame> | <one visible action or process step> | <every meaningful object in that beat, or none> |

    List the source in viewing order as SB1, SB2, SB3... Separate make,
    mix, pour, press, hold/display, and apply steps instead of replacing
    them with one gist verb. A named bottle, box, tool, product, or proxy
    held or standing on screen is part of the row even when no line names
    it. Every row cites a clip or frame. If the video truly shows no
    demonstrated process and no meaningful object, emit one row:
    `none observed | n/a | n/a | n/a`. Never omit the ledger.
    SOURCE-DERIVED STRUCTURE MAP + LOGIC CARD (canonical beat unit,
    2026-09-01): every new decode MUST also emit `### Source-derived
    structure map` and `### Source logic card` under
    `## Adaptation-extraction` — both are SHOWN in full in that section
    below. One map row = one viewer-state change, and that row IS the
    beat unit every downstream count uses (adapt fidelity, SCRIPT
    DECISION MAP rows, compression checks). Derive the sections from
    the frames and speech BEFORE naming any framework; a camera cut
    alone is not a section boundary. Measured 2026-09-01: only 36 of
    207 decodes carried these, because the requirement lived in the
    prose grammar and was never shown in this skeleton.
  • GENERATE-SIDE CHAIN OPTIONALITY (v590, parallel-generation
    enablement, GENERATE-SIDE ONLY):
    ASYMMETRIC RULE — applies to generate side ONLY (videos/*.md
    for our own videos / variants / adaptations). Decode-side
    (raw/decoded_*.md) MUST chain faithfully to mirror the source.
    On the generate side, set reference_image: none for every scene
    EXCEPT those requiring tight pixel-level continuity. Persona
    locks via Flow slot 0 upload; product (when bound) via slot 1;
    the v586 description grammar carries the rest. Slight natural
    background variation between independent scenes is desirable —
    avoids the AI-flattened "every scene looks identical" tell.
    CHAIN REQUIRED on generate side (the exceptions):
      (1) v580 recipe state-evolution — each step inherits glass +
          counter + cumulative ingredient state (chain consecutive
          recipe images, e.g. 3→4→5→6→7);
      (2) v541 before/after transformation — same patient + same
          setting, only outfit/skin/visible-state differ (chain
          Day-1 → Day-14 image pair);
      (3) Single-shot action arc — start frame and end frame within
          ONE clip (when single-clip dual-frame anchoring lands as
          a future v-rule);
      (4) Two-shot follow-up — close-up preserving the identity/
          pose of a secondary character from a prior two-shot scene.
    Everything else (HOOK / CONTEXT / EXPLAIN / AUTHORITY / single-
    frame PRODUCT / CTA / FOLLOW): reference_image: none. Each
    independent scene's image prompt is SELF-SUFFICIENT — full v586
    six-block walk, setting + anchor props described inline since
    no chain carries them.
    Throughput math (8-scene script): all-chained sequential ≈
    4 min Banana 2 + 10 min Veo (16-22 min total). v590-applied
    (3 chains in recipe + 5 independents) ≈ 1 min Banana 2 (parallel
    via the platform's existing parallel_slots, default 2, max 6)
    + 4 min Veo (parallel) — 6-8× faster ship.
    Why generate-only: decode = WHAT the source is (chain mirrors
    source); generate = WHAT WE SHIP (chain is a per-scene tradeoff
    between identity-anchor strength vs. generation throughput).
    The bidirectional rule cycle holds for grammar (v586/v540/v577/
    v589/v589.1) but NOT for chain topology — that's where the
    asymmetry sits. Migration: existing videos/*.md authored
    under chain-everywhere v523 default can be re-audited and
    chains relaxed where v590 conditions allow.
  • VLM VIDEO UNDERSTANDING (Stage 4d, free local) +
    STATE-EVOLUTION ARC GRAMMAR FITTED TO EXISTING PLATFORM BLEND +
    ABSOLUTE-MAGNITUDE GRAMMAR (v589, three coordinated halves):
    HALF A — Stage 4d VLM video understanding (decode-side,
    provider-agnostic + free local default): adds a structural
    backstop AFTER the v588 dense-frame human-walk. Pipeline file
    `code/v589_video_understanding.py` cascades providers in
    order: (1) LM Studio (free local, recommended): user opens
    the LM Studio app with a vision-capable model loaded (e.g.
    gemma-4-E2B-it-GGUF with mmproj) and enables the local server
    at http://localhost:1234. Script auto-detects the model and
    sends dense frames + transcript via OpenAI-compatible API.
    Zero per-call cost; runs on CPU. (2) Gemini API (paid
    fallback): when GEMINI_API_KEY is set, native MP4 upload at
    1fps + audio + per-second timestamps. ~$0.01 per 45s decode
    on gemini-2.5-flash. (3) Human-walk template (always
    available): when no automated provider is configured, the
    script writes a stage4d_vlm.json template skeleton with empty
    fields per shot + dense frames listed + dialogue. The human-
    walking decoder LLM session (Claude in chat) walks the dense
    frames and fills in the JSON manually. The v589 STRUCTURAL
    rule holds — the schema is produced, just by a human walker
    instead of an API. The VLM JSON (whichever provider produced
    it) becomes AUTHORITATIVE for visual action arcs, parallel to
    whisper.cpp being authoritative for dialogue.
    HALF B — State-evolution arc grammar fitted to the existing
    platform blend mechanism (generate-side): the platform
    already supports interpolation between two different frames
    via clip_mode=blend, where the NEXT SCENE's image is the
    end_frame of the current clip and `generate_transition_cue()`
    in code/veo_generator.py:883 narrates the metamorphosis.
    Two valid patterns:
    (B1) MULTI-CLIP state evolution — when the arc spans
        adjacent shots/scenes naturally (e.g. v580 recipe steps),
        use clip_mode=blend between adjacent scenes; the next
        scene's image IS the end-state of the current clip. The
        platform already supports this — no new field, no parser
        change.
    (B2) SINGLE-CLIP state evolution — when the arc is contained
        within ONE shot (e.g. the icelandicwisdom 6s fat-melt
        HOOK), the current platform has NO same-scene end_frame
        anchor — Veo gets only the start_frame + action narrative
        + transition_cue. RISK: Veo may produce partial changes.
        Mitigation TODAY: Half C absolute-magnitude grammar in
        the action_note + an explicit anti-failure-mode clause
        baked into the positive Text prompt (e.g. "the fat melts
        completely off the upper torso — zero residual yellow on
        the upper-abdominal organs at clip-end"). (Veo Negative
        prompt blocks are retired — standing rule 2026-06-04.)
    PLATFORM-FUTURE (unnumbered, not yet shipped — v590 was
    reassigned to chain-optionality): extend the
    parser to support an `image_end:` field on the scene block
    so single-clip state-evolution arcs can anchor Veo on TWO
    visual states in ONE clip. Earlier first-pass v589 introduced
    `### Image N_end` — RETRACTED: parser regex is `### Image
    (\d+)` (integer-only); image_N_end would not parse.
    HALF C — Absolute-magnitude grammar (both sides, applies to
    all state-evolution clips regardless of platform support):
    action_notes describing visible state-evolution end-states
    MUST use absolute language when the source shows COMPLETE
    change. FORBIDDEN hedge words when magnitude is COMPLETE:
    "dramatically", "mostly", "almost", "substantially reduced",
    "largely". REQUIRED absolute alternatives: "completely melts
    away", "fully revealed", "entirely dissolves", "the X is
    gone", "every Y is visible". Hedge language reserved ONLY
    for genuinely partial states. State-evolution clips encode
    the absolute requirement explicitly in the positive Text
    prompt (no Negative prompt block — standing rule 2026-06-04).
    Why: surfaced from second-pass review of @icelandicwisdom
    belly-fat HOOK + course-correction on platform alignment.
    User pushback: "it all melts completely while we say
    'dramatically reduced' — we need it more powerful." Then:
    "check how we structure the video in the platform — for now
    we don't use explicit end frame, we use the blend (between
    clips and between scenes) ... and we need a free way for the
    pipeline to actually understand the video." Half A free-
    local path + Half B fitted to the existing blend mechanism +
    Half C absolute grammar address all three constraints
    together. See template_reference "VLM video understanding +
    state-evolution arc grammar (v589)" section for the deep-
    dive.
  • DENSE PER-SHOT FRAME SAMPLING FOR ACTION-ARC CAPTURE (v588,
    extends v585 Stage 4): every shot's view-tool inspection MUST
    view at minimum start (t=start+0.1s), midpoint, and end
    (t=end-0.1s) frames — three frames, three states. Additional
    dense-sampling triggers — view 5+ frames evenly distributed
    when ANY of these signal an action arc within the shot:
    (1) shot duration > 3s; (2) v585 optical-flow magnitude > 0.7px
    (drift with motion); (3) the dialogue overlapping the shot
    mentions a verb-of-state-change ("squeeze in", "pour", "drop",
    "add", "stir", "mix", "spread", "press"); (4) the start-frame
    and end-frame visual signatures DIFFER (state evolution
    detected). When any trigger fires, the action_note's three
    motion beats are GROUNDED in distinct frames (start beat =
    start-frame snapshot, mid-clip beat = midpoint-frame snapshot,
    end beat = end-frame snapshot) — three real visual references,
    not one frame extrapolated. Why: the midpoint frame catches
    one slice of the arc and the decoder writes a static
    description that misses the WHOLE point of the shot. v588
    fixes the gap that v586 + v585 left open. Bug it prevents:
    the @icelandicwisdom belly-fat HOOK was misread on first pass
    as "pointing at anatomy" because the midpoint frame caught
    only mid-pour; the actual action was a fat-melt prop-violence
    state-evolution arc visible only when start (fat-draped
    torso), mid (pour cascading), and end (fat melted, organs
    revealed) frames were inspected together. v588 enforces the
    dense walk so action arcs are captured. See template_reference
    "Dense per-shot frame sampling (v588)" section for the
    sampling protocol, anti-patterns, and the worked example.
  • REPRODUCTION-READY ARTIFACT (v587, extends v586): every decoded
    script MUST include two structured sections after the Storyboard
    section, promoting methodology metadata (previously hidden in the
    HTML-comment header) into machine-parseable wiki sections.
    Decode and generate templates become structurally symmetric —
    the bidirectional cycle made literal at the artifact level.
    (A) `## Comprehension` — five required subsections:
      (1) Structural inventory — total scenes / clips / duration;
          per-scene block tags from the canonical block vocabulary
          (HOOK / TITLE / RECIPE / TRANSFORMATION / EXPLAIN /
          ANATOMY / RESULT / AUTHORITY / PRODUCT / CTA / FOLLOW).
      (2) v-rule inventory — table mapping every applicable v-rule
          to how this video uses it. Entry per rule:
          `applied — <variant or specifics>` /
          `NOT APPLICABLE — <reason>` /
          `partial — <which dimensions covered>`.
          At minimum cover the live rules in the conventions index
          (v521.1, v523, v523.1 / v573 + v581, v528, v538, v539,
          v540, v541, v544, v553, v553.1, v573, v577, v579, v580,
          v581, v584, v585, v586).
      (3) Rhetorical structure — HOOK type (force-verb /
          clinical-markup / diagnostic-press / symptom-curiosity /
          banana-pun / weird-action-on-prop / etc.); frame
          (recipe-as-claim / before-after-transformation /
          authority-stack / curiosity-gap / 5-truths-listicle /
          etc.); payoff structure (timeline-promise / climax-position
          / authority-anchor / outfit-change-time-jump / etc.); CTA
          structure (comment-keyword / comment-plus-follow combined
          / link-in-bio / DM-trigger / etc.).
      (4) Angle / audience signal — niche; primary audience (gender
          + age band); secondary audience if any; symptom or
          aspiration (what the viewer wants to fix or gain);
          emotional register (warm / fierce / clinical / desperate /
          hopeful / etc.).
      (5) Persona archetype + setting tier — archetype label
          (modern-clinic-doctor / holistic-healer / old-grandma /
          sexy-doctor / rastafarian-uncle / etc.); setting tier
          (Tier-0 selfie-arm / Tier-1 single-setting / Tier-2
          multi-setting); specific settings used.
    (B) `## Veo 3.1 Final Prompts (per clip)` — one fenced block
        per clip, fully assembled with Cinematography + Action
        narrative + Dialogue ("The main AI generated character says in a [qualifier] voice, "...".") +
        Ambient + "(no subtitles, no captions)". NO `**Negative
        prompt:**` block — RETIRED per operator standing rule
        2026-06-04 (overrides the old v750 separate-negative
        requirement): omit it entirely and bake critical
        constraints affirmatively into the positive Text prompt
        (e.g. "one continuous shot, clean ambient, no music, no
        background noise"; source-specific guards as positive
        sentences — "he is alone in frame" for solo videos, "the
        scene stays in the clinic" for clinic-only videos).
    The image prompts in `## Images` already serve as ready-to-run
    Banana 2 reproduction prompts (per v586). v587 adds the
    Comprehension layer + Veo final-prompts symmetry — the decode
    artifact is now a complete reproduction package: WHAT happens
    (Storyboard) + HOW it works (Comprehension v-rule inventory) +
    WHY it works (Comprehension rhetorical + angle subsections) +
    HOW TO REPRODUCE IT (Images for Banana 2 + Veo final prompts
    for Veo 3.1). Migration: pre-v587 decodes valid as-is — no
    retrofit needed; HTML-comment headers remain as historical
    record. New decodes from this commit forward MUST emit both
    sections.
  • IMAGE ECONOMY: if two phases of ONE physical action (e.g. "about to pour" +
    "mid-pour") can be captured in a single image, use one image — usually the
    mid-action frame. Drop redundant setup images.
  • RECIPE/PROCESS STATE-EVOLUTION (v580): inverse of IMAGE ECONOMY for multi-
    step processes. When a scene depicts a multi-step process where each step
    changes the visible state of a foreground prop (recipe ingredient adds,
    skincare layering, makeup application, painting, mechanical assembly), each
    step gets its OWN start image whose state shows the cumulative result of
    all prior steps. Same image for all N clips of a recipe means by clip N the
    start frame still shows the t=0 clean glass while dialogue talks about
    adding ingredient N to a glass that should already contain ingredients
    1..N-1. Veo will either ignore the dialogue and animate against the clean
    glass (recipe never visually progresses) or hallucinate prior ingredients
    mid-clip (state pop). Fix: each step gets its own `### Image M` block; each
    step's reference_image points to the prior step's image so Banana 2
    generates each as a state-evolution preserving glass position, lighting,
    persona pose, counter layout. Each step gets its own sub-scene `### Scene
    N.M` block with `clip_mode: fresh` and `transition: cut` (prop state has
    visibly changed; cut lets Veo treat as hard state shift). Action_note
    describes the COMPLETION of the current step's action. Inverse of v577's
    same-image-multi-line pattern, which is for held-pose gesture-only scenes
    where the prop does NOT change. APPLIES TO: cooking, skincare routines,
    makeup tutorials, crafting/painting layers, assembly sequences, plant
    care multi-step. DOES NOT APPLY TO: held-pose talking-head scenes (CTA
    stacks, authority explanation), b-roll montages, gesture-only scenes
    where the prop is static. See template_reference for the canonical
    5-step recipe pattern table and the per-step image-content schema.
  • BACKGROUND BREVITY: composition matters more than background detail. One
    anchor phrase per setting ("bright modern kitchen", "outdoor garden with
    foliage"). Don't enumerate shelves, jars, flags, appliances, visible-through-
    window houses. For non-establishing images, just say "same setting as image N"
    and let the reference_image chain carry it visually.
  • TRANSITIONS + CLIP_MODE (v544): two separate fields. `clip_mode` controls
    Veo's first frame for THIS clip (`fresh` = use this scene's image, `blend`
    = use the previous clip's last frame). `transition` controls the edit
    BETWEEN clips (`cut` = hard cut, `blend` = cross-dissolve, `null` = scene 1).
    DEFAULT FOR BOTH IS THE SAFE OPTION: `clip_mode: fresh`, `transition: cut`.
    Use `transition: blend` ONLY when two adjacent clips have a tiny visual
    delta — same camera, same persona pose, an ingredient moved a small
    distance (the kettle lifted 4 inches, the hand drifted from chest to
    mouth). If multiple things changed, or a transformation is happening,
    or the camera/setting shifted, use cut. ALWAYS CUT on transformation
    reveals (Day 1 → Day 14) — blend produces visible morph artifacts and
    breaks the time-jump. ALWAYS CUT on setting/register changes (HOOK →
    RECIPE → OUTRO). ALWAYS `clip_mode: fresh` on action scenes — `blend`
    forces Veo to start from the previous clip's transient mid-motion frame,
    which produces stiff or contorted character motion. (This INVERTS the
    earlier "default to blend" guidance — that was theoretically right but
    wrong in practice; cross-dissolves between non-trivial deltas produce
    morph artifacts that read as render glitches.)
    v782 (2026-06-09): the PLATFORM CODE default now matches this guidance.
    Before v782 the backend defaulted a MISSING clip_mode to `blend` and a
    MISSING transition to `blend` (main.py + worker.py), so a build that
    OMITTED these fields silently got cross-scene start→end interpolation on
    every clip — the doc said fresh/cut but the code did blend. v782 flips
    every backend default to `clip_mode: fresh` + `transition: cut`; `blend`
    is now EXPLICIT opt-in only. STILL: every shot scene MUST emit both
    `- **clip_mode:** fresh` and `- **transition:** cut` (or `null` on scene 1)
    EXPLICITLY — never rely on the default. A within-clip morph uses the
    v718h-C `end_frame_image:` bullet, NOT clip_mode blend.
  • SPEAKER (v538): on-camera is the default. Voiceover is opt-in only.
      - **speaker:** voiceover   ← off-screen narrator delivers the line; visible
                                   subjects stay silent with closed lips. Use ONLY
                                   for: HOOK before/after states (patient on a
                                   scale, daughter on a couch — persona narrates
                                   from off-screen), hand-only product close-ups
                                   (RECIPE pours where only the hand is visible),
                                   ANATOMY semi-transparent overlays, establishing
                                   shots with no human subject.
      - **speaker:** on-camera   ← the visible main character speaks the line,
                                   lip-sync ON. This is the default — you can
                                   omit the field entirely and get the same
                                   behavior. Specifying it is purely for
                                   readability.
    There is NO auto-detection. There is NO "auto" value. If you don't write
    `voiceover`, you get on-camera. Voiceover is the special case — reserve it
    only for the situations listed above.
  • HOOK WEIRD-ACTION (v539): every HOOK scene that opens cold MUST contain a
    concrete weird action performed on a physical prop. Not a feeling, not a
    posture, not a glance — a verb, a force, a thing happening to a thing
    that wouldn't normally happen. Smash, throw, snap, dunk, pour, stomp,
    hurl, sweep, slice, squash, drop, fling, slam, tear. In multi-beat HOOKs
    (3+ before-states), at least ONE scene must contain the weird action —
    typically scene 2, with scene 1 setting up the problem character and
    scene 3 showing resolution.
  • OUTFIT CHANGE ON TIME-JUMP (v541): whenever a video has a Day 1 → Day 14
    (or any other before/after) transformation cut on the same secondary
    character in the same setting, the patient's OUTFIT must change between
    the two timestamps. Same person + same setting + DIFFERENT clothes =
    "two weeks went by". Same outfit before/after reads as a render trick.
    Match the change to what's in frame: change the bra (not the pants) for
    a back transformation, change the pants (patient stays shirtless) for a
    belly transformation, change the shirt (head-bowed pose only sees the
    shirt) for a hair transformation. In the Day 14 image prompt, write a
    paragraph titled `OUTFIT CHANGE — DAY 14:` with explicit "different from
    image 1's [old outfit]" language so Veo doesn't inherit the Day 1 outfit
    through the reference_image chain. Skip this rule if the SETTING also
    changes between Day 1 and Day 14 — setting-change already signals time
    passing.
  • PRODUCT FRAMES (v573): when the source video has a branded product on
    screen with the label visible, identify which scenes are "product
    frames" — hero close-ups, label-visible holds at chest height, mid-pour
    shots where the bottle is in frame, end-card product reveals, anywhere
    the actual packaging needs to render correctly. In those image prompts
    refer to the product BY NAME (e.g. "the Salvora Rhodiola Rosea bottle",
    "the [brand] box", "the product") — same name-based convention as
    persona ("the main character"). Do NOT write "matching image 2's
    label" in body text — "image 2" is the Flow positional slot used in
    the platform's manifest header, not a body-text convention; the
    markdown's `### Image 2` is a separate generated scene image and the
    overlap would confuse the renumbering pass. The platform binds named
    products to the product upload slot via the same ingredient-binding
    logic that already binds "the main character" to the persona slot.
  • V610 GENDER-NEUTRAL PERSONA REFERENCES: prose body and action_notes
    must NEVER use gendered pronouns (she / her / hers / he / him / his)
    to refer to the main character. Identity comes from the upload,
    NOT from the prose. Use the role descriptor ("the main character,"
    "the healer," "the practitioner"), the singular "they / their," or
    pronoun-free constructions ("right hand presses ..." instead of
    "she presses with her right hand"). Other characters (a patient,
    husband bystander, customer) keep gendered pronouns — their identity
    IS the prose. Dialogue (`- **line:**`) is unaffected; the persona's
    name in dialogue stays verbatim.
    For non-product frames (HOOK before-states with no product, ANATOMY
    overlays, talking-head closeups with the product offscreen) DO NOT
    mention the product — leaving the product slot unbound for those
    scenes is correct and saves a reference. For chain-continuation
    scenes where the product is already locked-in by the previous frame
    (e.g. scene 5 follows scene 4 with `reference_image: image_4` and
    the bottle hasn't moved), DO NOT re-mention the product by name —
    chain inheritance carries it visually. Re-binding the product in
    every scene is the wrong move; it can bleed product geometry into
    compositions where the product shouldn't dominate. The Ingredients
    table below the document header declares whether a product is
    uploaded; if no product upload exists, fall back to inline label
    description (the v523.1 pattern).
  • ISOLATED-REFERENCE RULE (v573, supersedes v523.1): the persona is uploaded
    as IMAGE 1. A branded product (when applicable) is uploaded as IMAGE 2.
    Every other prop — mannequin, kettle, jar, generic bottle, pen, onion,
    etc. — gets described INLINE in image prompts. The test for any candidate
    upload: "do I have a clean isolated photo of just this one object on a
    neutral background?" Persona reference sheets and product hero shots pass.
    Anchor-scene crops fail. In practice persona always passes; product
    passes when there's a clean hero shot of just the bottle/box on neutral
    background. Everything else stays inline. Control comes from CLEAN
    references, not many references — uploading busy-anchor scenes makes
    Nano Banana 2 reproduce the entire scene every time the prop is
    referenced (the wall, the floor, the neighbors). Add a note paragraph
    below the Ingredients table listing what was considered and rejected,
    so the decision is visible to future readers.
  • ACTION_NOTES (v540 — VEO START-FRAME RULE; extended v573): each clip is
    generated by Veo using the start frame image + the action_note text. Veo
    can SEE the start frame, so the action_note must NOT describe the starting
    state — it must describe ONLY what changes across the 8 seconds. And each
    clip is a sealed unit, so action_notes must NEVER reference other clips
    ("same composition as image N", "continuation on image M", "same kitchen
    as scene 4"). Each action_note stands alone and describes only the
    motion + expression evolution + voiceover-line-anchored beats. v573:
    action_notes ALSO never re-describe the product's label, typography,
    color, or packaging — the product appearance is already locked into the
    start frame by Nano Banana 2 via the product upload. Refer to the product
    by short generic name only ("the bottle", "the box", "the product") and
    describe motion: how it's held, where it moves, what hits or pours from it.
-->

## Example — one scene with multiple lines

### Scene 7
- **image:** image_8
- **clip_mode:** blend
- **transition:** cut
- **visual register:** OUTRO — direct-address CTA
- **rhythm tier:** warm-authority (11w + 10w)
- **speaker:** on-camera
- **line:** drop a comment and I will send you my complete protocol
- **action_note:** Main character lifts the book up at chest height, presenting forward to camera, warm smile, direct eye contact as he delivers the comment-word CTA.
- **line:** but you must be following me so I can reach you
- **action_note:** Same book-held-up pose continues — slight lean forward for emphasis, warm grandfatherly inviting expression as he delivers the follow-prompt to close.

---

## Support Inserts (v825) + per-image framing (v826)

**v825 — timed support-image inserts (talking-head videos only).** For a video that is ONE continuous talking head plus many still images that pop in over the speech (timed to the words), add a `## Support Inserts` section AFTER the Storyboard. Each `### Support N` places an EXISTING `### Image N` still on a SEPARATE silent track for the word-span `[start_word … end_word]`. It has NO `- **line:**` — it borrows the talking-head audio. Export emits TWO files: `talking_head.mp4` + `support_track.mp4` (stills on black, same length); you composite them in post.

```markdown
## Support Inserts

### Support 1
- **image:** image_7
- **start_word:** not
- **end_word:** turmeric
- **phrase:** not turmeric      ← OPTIONAL (v825.1). start_word→end_word DEFINE the overlay's span; phrase only disambiguates WHICH occurrence when a word repeats.
```

Rules: `### Support N` = integer header (strict, like Scene). `image` must reference an image defined in `## Images`. A support still is NOT a text_card and gets NO `### Image` header of its own — it reuses one. Support stills generate in the normal Banana pass (every `### Image N` → an image render) and are never sent to Veo (no scene references them).

**v826 — per-image framing + variants (optional bullets on any `### Image N`):**

```markdown
### Image 7
- **aspect_ratio:** 16:9        ← one of 16:9 / 4:3 / 1:1 / 3:4 / 9:16. Omit → batch default (usually 9:16).
- **variants:** x2              ← x1–x4 = how many variants to generate. Omit → batch default.
- **Image prompt:**
...
```

Support-image stills usually differ from the 9:16 talking head — set their real framing here (16:9 charts/food photos, 3:4 screenshots, 1:1 before/after pairs, 9:16 phone screenshots).

---

**Video:** 
**Persona:** 
**Setting:** 
**Duration:** 60s
**Structure:** 
**Video mode:** storyboard
**Auto-split:** off

<!-- v867: every NEW build declares its test axes in §0 (one line, 10 keys, kebab-case values,
     `none` when absent — never omit a key). Value bank: wiki/synthesis/video-variable-taxonomy.md.
     - **TEST AXES:** shell=talking-head | chore=none | observer=none | hero_age=64 | proxy=banana | destruction=none | recipe_stack=none | voiced=vo | setting_anchor=attic-studio | caption_style=yellow-highlight
     Decode side: same 11 fields (plus `watermark: present|absent`) as YAML frontmatter keys. -->

<!-- v877: every winner-derived build and approved view-proxy test adds a WINNER DECISION block in §0 before METHOD: GOAL, EVIDENCE SIGNAL, one PRIMARY GAP, one exact PRIMARY DELTA, coherence-only SUPPORTING REPAIRS, SMALLEST ROUTE, WHY NOT SMALLER, and INNOVATION NEED. METHOD must match SMALLEST ROUTE. -->

<!-- v883: every Movie Style / interaction-scene build adds this §0 contract before Images/Scenes.
     Standard lane:
     MOVIE STYLE: yes
     MOVIE STYLE VARIANT: standard
     MOVIE STYLE EMOTION: admiration | jealousy | cheating | betrayal | UNLISTED — <source-derived name>
     VISIBLE AFTER-STATE: <what the hook already shows>
     MOVIE STYLE SEQUENCE: trigger=Scene N | answer=Scene N | wrong guess=Scene N | age=Scene N | press=Scene N | reveal=Scene N | body open=Scene N
     AGE PAYLOAD: <literal spoken age words> | scene=N
     PAIN HANDOFF: <one exact body part/euphemism> | press scene=N | body open scene=N
     Betrayal uses: event | aftermath | comfort | age | need question | reveal | body open,
     plus `press scene=n/a`. Canonical: template_reference.md §v883. -->


---

## Ingredients

Persona is always uploaded as image 1 (Flow slot 0). Product, when present in the video as a branded item with a visible label, is uploaded as image 2 (Flow slot 1). The "image 1 / image 2" labels here refer to the platform's positional slots — body text references the persona and product BY NAME, not by slot number. Everything else (mannequin, kettle, generic bottles, etc.) is described inline in the image prompts and is NOT listed here.

| Name (used in prompts) | Type | Description | Source | Attached to |
|---|---|---|---|---|
| `the main character` | character | Persona — no description needed (passed externally) | External persona upload — Flow slot 0 (Image 1) | `image_1, image_2, ..., image_N` (typically every image) |
| `[patient name, e.g. Donna]` | patient | (v681) Recurring named non-speaker — testimonial subject who appears across BEFORE / AFTER / multiple scenes. NO description here; identity comes from the upload (same v602 rule as persona). | External patient upload — Flow slot N | `image_K, image_L, ...` (only images where patient appears) |
| `[extra label, e.g. husband]` | extra | (v681) One-shot bystander — appears in exactly ONE scene, no upload, identity carried in prose per v669 (race + age + build + clothing). Reference column = `—`. | (no upload) | `image_K` (single image — extras are one-shot) |
| `the [product name]` | product | [Brief product description — brand, label color, container shape. Used as fallback if upload fails.] | External product upload — Flow slot 1 (Image 2) | `image_K, image_N` (typically only product-reveal scene + CTA hero-shot per v599 matrix) |

**The `Attached to` column declares per-image binding scope (v707).** Platform reads via `_parse_ingredients_block` (v618a header-aware, recognizes column via substring match `attached`), populates per-ingredient `attached_to: list[int]`, binding loop resolves `parent_edges[i] = ingredients_with_image_i_in_attached_to`. Value format: comma-separated `image_N` tokens (lowercase `image_` prefix + integer); range form `image_K-image_N` accepted. Missing column falls back to v619 auto-infer (N1-N5 normalization) for backward compatibility with pre-v707 artifacts.

**Note on cast types (v681):**

> **THE TYPE IS DECIDED BY THE UPLOAD, NOT BY WHO TALKS (v618b — read this before typing any row).**
> `type=character` / `type=product` mean **"this ingredient HAS AN UPLOAD on the platform"**. The importer reads ANY character/product row whose **Source cell is non-empty** as *a declared Reference that must already resolve to an uploaded ImageNode*, and **hard-fails the whole import** if it does not:
> `Import error: Ingredient(s) with type=character/product declare a Reference path in the Ingredients table but no matching upload exists on the platform. Unresolved ingredients: • 'Dale' (type=character, declared Reference: (no upload))`
> Writing `(no upload)` / `none` / `—` in that cell does **not** help — a non-empty *string* is not an empty *cell*, and it reads as a declared path.
> **A speaking non-persona with no upload is `type=extra` + Source `inline` + scene `- **speaker:** on-camera`.** Speaking does NOT make something a `character` row. Per v573 (below), the ONLY uploaded ingredients are the persona and the branded product.
> Corpus proof: `videos/nuri-korella-ed-locked-board-pill-dismissal-korella-saffron-selling-v2.md` ships two §14.3 testimonial men who each speak their own on-camera line as `extra` / `inline` / `speaker: on-camera`.
> **Veo corollary:** an `extra` speaker's clip says **"The man speaks clearly in a … American accent, saying exactly: …"**. Do NOT use the v665 subject *"The main AI generated character"* — that phrase exists to BIND THE PERSONA UPLOAD as the lip-syncer, so using it on a testimonial renders the persona's face onto that character. (v698A voiceover twins are the one place a non-persona-cast scene legitimately binds the persona.)
> *Added 2026-07-17 after the line below caused a real import rejection: it said only `character` rows can speak, never said `character` means "uploaded", so two testimonial men were typed `character` with no upload.*

- `character` rows are the UPLOAD-BACKED speaking cast (the persona; multi-persona = v682). They may appear as a `- **speaker:**` value. **They are not the only rows that can speak** — an `extra` speaks via `- **speaker:** on-camera` on a scene whose `cast:` lists that extra (see the box above). Single uploaded character per video in v681.
- `patient` rows are recurring non-speakers. **Reference column is OPTIONAL (v681e):**
  - **Upload-backed**: Reference points to a real isolated photo (e.g. `patients/refs/donna.png`). Identity binds via the upload. Recommended when the photo exists.
  - **Anchor-scene (no upload)**: Reference = `—`. The FIRST scene that has `cast: <patient>` becomes the anchor. Banana 2 generates the patient on that scene from the image_prompt body's identity prose (race + age + build + hair + clothing per v669 — REQUIRED on the first scene). Subsequent scenes with `cast: <patient>` chain back to the anchor's chosen variant via v512 — Flow uses the anchor scene's render as the reference. NO upload required, NO `reference_image:` needed for face continuity.
- `extra` rows are prose-only, NO upload, Source `inline`. Identity in the image prompt per v669 (race → age → BUILD → hair → clothing → expression). Typically one-shots (Donna's husband on the bed scrolling his phone) — **and this is also the correct type for a one-shot SPEAKER** such as a §14.3 testimonial man: `type=extra` + `inline` + `speaker: on-camera` + "The man speaks…" in the Veo prompt.

**Note on stripped-down ingredients (v573):** Only the persona and the product are declared as uploaded ingredients. [List of inline-described props, e.g. "the kettle", "the half-lemon", "the surgical pen"] are all described inline in scene prompts — none would pass the isolated-reference test (no clean isolated photos available for those). When the video has no branded product on screen, drop the product row entirely and omit product mentions from all prompts.

---

## Images

### Image 1
- **frame_anchor:** 0.5s         # v667 — source-video timestamp where this composition first appears (decode side; from manifest.json shot start_time)
- **reference_image:** none
- **Image prompt:**

<!-- v791 — HOOK images (image_1 / Scene 1 start frame) use the safe-area composition grammar INSTEAD of opening with the generic line below. Order: (1) ONE camera sentence first (v791.2): "A vertical 9:16 smartphone photo shot on an iPhone ultra-wide 0.5x lens (13mm equivalent), [view type] from [distance], the lens level with the [person]'s raised hand." (2) Hero layer (v791.2 — NEVER "foreground"/"closest to the lens", drops to the bottom edge): "[Person] raises his open palm to his own [eye/chin] level and extends it straight toward the camera, presenting [hero prop + plain state-words] on the flat of his hand — the huge foreshortened hand and the [prop] sit at the very middle of the frame; his face right behind and just above his hand, the top of his head touching the top edge of the frame." (3) Person right behind it. (4) Secondary people. (5) Background setting last. (6) House realism block + "Aspect ratio 9:16." NO thirds/grid vocabulary, NO crop-boundary negotiation, NO trailing zone-bans. Deep-dive: template_reference.md §v791. -->

```
Shot on iPhone with wide-angle lens, handheld, deep focus throughout, natural daylight, slight wide-angle perspective distortion at the edges of frame. [One brief anchor phrase for the setting — e.g. "Outdoor garden with blurred foliage" or "Bright modern kitchen with a warm honey-oak tabletop in the lower foreground".] [Describe framing: subject's position in the frame, shoulders-spanning, head placement.] [Describe any secondary characters fully on first appearance only — gendered pronouns are fine here, these are prose-described not upload-bound.] [Describe the main character's pose, hand positions, gesture — NEVER face/hair/beard/glasses/wardrobe AND NEVER gendered pronouns referring to the main character (v602 + v610: identity comes from the upload, not prose).] [Describe the foreground props in active use, their state and position in frame.] [Describe expression direction: mouth open mid-word, eyes tracking, warm teaching smile.] Natural ultra-realistic colors, deep focus. Aspect ratio 9:16.
```

### Image 2
- **frame_anchor:** 3.0s         # v667 — when this state first appears in source
- **reference_image:** image_1
- **visual_delta:** [v667 — one sentence naming the concrete change vs image_1: prop / pose / wardrobe / expression. Required on every chained image.]
- **Image prompt:**
```
The main character, same [setting anchor] as image 1, same framing — shot on iPhone wide-angle lens, handheld, deep focus throughout, natural daylight. [What's changed: new pose, new action, new prop state.] [Expression direction.] Natural ultra-realistic colors, deep focus. Aspect ratio 9:16.
```

### Image 3 — example with product (v573)
- **frame_anchor:** 7.5s         # v667
- **reference_image:** image_2
- **visual_delta:** The main character now holds the product bottle label-forward at chest height; everything else preserved.
- **Image prompt:**
```
The main character, same [setting anchor] as image 2, same framing — shot on iPhone wide-angle lens, handheld, deep focus throughout, natural daylight. The main character holds the [product name — e.g. "the Salvora Rhodiola Rosea bottle"] label-forward to camera, [describe how the product is held: at chest height with the cap thumb-up, mid-pour over a glass, etc.]. [Expression direction.] Natural ultra-realistic colors, deep focus. Aspect ratio 9:16.
```
*(Name-based product reference. The platform binds "the [product name]" to the product upload at slot 1. Use this pattern only for product frames. In non-product scenes, omit the product mention entirely — chain via reference_image as in Image 2 above.)*

---

## Storyboard

### Scene 1
- **image:** image_1
- **scene_type:** shot                           # v681 — shot | text_card. (omitted) defaults to shot. text_card scenes use a different bullet set (see Scene 4 below).
- **cast:** the main character                   # v681 — comma-separated Ingredients Name values present in this scene. Optional but encouraged when 2+ cast members. Empty/absent → v509 prompt-scan fallback (legacy behavior).
- **caption:**                                   # v681 — source on-screen caption (decode-only capture; lift bundle ignores per v621). Leave blank for owned-content authoring.
- **clip_mode:** fresh
- **transition:** null
- **visual register:** HOOK
- **rhythm tier:** 
- **speaker:** the main character on-camera      # v681 — <character_name from Ingredients> <on-camera|silent>. (omitted) defaults to <persona> on-camera. NO `voiceover` in v681 (deferred to v682).
- **cut_mode:** auto      # v668 — whisper | timeline | auto. v852: on a `speaker: silent` scene, OMIT for FULL-length export, or set `timeline` + `target_duration_s` to export the clip at the decoded beat duration. Silent clips are keep-protected from VAD either way (v852). Default `auto` picks `timeline` for bracket-annotation lines (`[music plays]`, `[SFX:...]`) and `whisper` otherwise. Set explicitly when overriding.
- **line:** 
- **clip_duration_s:** 4      # v861 — MANDATORY on every spoken line: 4 | 6 | 8 | 10, matching THIS line's word count (<=11w=4s · 12-16w=6s · 17-24w=8s · 25-28w=10s; over 28w is a v831 violation — split into 2 clips). Attaches to the `- **line:**` above it (same rule as `pad`), so a 2-line scene can hold 2 different durations. Bare integer only. Flow renders a real 10s clip; the Veo API has no 10s bucket and folds 10→8. Absent → the parser auto-picks from the word count and logs `[v861/auto]`, but the /build auditor FAILs the build.
- **pad:**           # v644 — optional suffix added AFTER line in Veo prompt only (target line+pad ≈ 20 words; pad is cut from final video by whisper-VAD)
- **action_note:** 

### Scene 2
- **image:** image_2
- **clip_mode:** fresh
- **transition:** cut
- **visual register:** HOOK
- **rhythm tier:** 
- **speaker:** voiceover
- **line:** 
- **action_note:** 

### Scene 3
- **image:** image_3
- **clip_mode:** fresh
- **transition:** cut
- **visual register:** RECIPE
- **rhythm tier:** 
- **speaker:** on-camera
- **line:** 
- **action_note:** 

### Scene 4 — example text_card scene (v681)
- **scene_type:** text_card                    # v681 — text-card transition; ffmpeg drawtext renders this clip server-side, skipping Nano Banana 2 + Veo. NO image / cast / line bullets allowed.
- **caption:** 2 months later...               # required when scene_type=text_card
- **bg_color:** black                          # required when scene_type=text_card; CSS color name OR hex (`#000000`)
- **duration:** 1.0s                           # optional; defaults to 1.0s when omitted

### Scene 5 — example charswap scene (v945)
- **image:** image_1                             # the clip's start frame; in swap_mode=image-led this is the frame the source's movement is applied to
- **scene_type:** shot
- **speaker:** silent                            # a charswap scene may carry AT MOST ONE `- **line:**` (clips are 1:1 with lines; two lines would fan ONE source into two renders → hard fail)
- **render_method:** charswap                    # v945 — only value. Any other value HARD-FAILS at import. Omit the bullet entirely for the normal render path.
- **swap_source_video:** source-muted-9.8s.mp4   # v945 — the declared asset name. Uploaded by `send_to_platform.py --swap-source NAME=path`, or the name resolves as a path (beside the build → repo root → cwd). ≤12.0s, mp4 with a video stream, ≤80MB.
- **swap_mode:** video-led                       # v945 — video-led | image-led. video-led = replace the character in the real video. image-led = apply the source's movement to the CHOSEN start frame (refused without one).
- **audio:** source-original                     # v945.1 — source-original | none (default none). CHARSWAP SCENES ONLY (hard-fails elsewhere, even as `none`). The render is ALWAYS silent; this re-muxes the stored source's own track at EXPORT, so point swap_source_video at the WITH-audio cut.
- **target_duration_s:** 10
- **clip_duration_s:** 10
- **clip_mode:** fresh
- **transition:** cut
- **action_note:** [what the swapped character does across the clip, with the beat markers]

<!-- v945 — the three swap bullets are all-or-nothing: declaring one or two of them HARD-FAILS at
     import naming what is missing. `## Ingredients` must resolve to EXACTLY ONE character row bound
     to an upload — that face is the swap; zero or several is a 400 ("a swap never guesses a face").
     The Veo prompt for a swap clip states the INTENTION of the swap, not a performance.
     NOTE ON THE NUMBER: the shipped code, its error messages and its tests all say `v943`; that
     number was claimed a day earlier by a different rule, so canon is v945. Do not rename the code.
     Deep-dive: template_reference.md §v945. -->

### Scene 6 — example movie-section scene (v959)
- **image:** image_1                             # v959 — the WIDE anchor; it becomes the scene chip on the Ingredients tab (§5c). Every section scene of the build points at the anchor or at an image whose `reference_image:` chain reaches it.
- **scene_type:** shot                           # v959 — a text_card may NEVER carry render_method (it is drawn by ffmpeg, not rendered as a clip). A SILENT shot scene is refused in a section build: make it a text_card or fold the b-roll into a section.
- **render_method:** movie-section               # v959 — the second accepted value of the v945 hook. ALL shot scenes of the build carry it, or none: mixing his section grammar with our one-speaker clips HARD-FAILS at import.
- **face_refs:** image_2, image_3                # v959 — 1 or 2 `### Image N` blocks, close-ups of THIS scene, each carrying `reference_image: image_1`. Never the scene's own image, never repeated, never on a scene without the method. They ride to the worker as extra chips beside the scene chip.
- **cast:** the man in the grey cut-off shirt, the woman in the olive t-shirt
- **speaker:** on-camera
- **clip_duration_s:** 10                        # v959 — 8 or 10, MANDATORY. It is the pacing window the §5b numbers divide by; missing or any other value HARD-FAILS.
- **target_duration_s:** 10
- **clip_mode:** fresh
- **transition:** cut
- **line:** wow if my husband looked like you i would never leave the house then he should do what i do   # v959 — EXACTLY ONE line bullet, holding every spoken word of the section in order, lowercase. It must equal the quoted spans of Clip 6.1 in order — whisper aligns to it at export.
- **action_note:** [Start beat 0-4s] ... [Mid-clip beat 4-8s] ... [End beat 8-10s] ...

<!-- v959 — §0 of the build carries one extra declaration: `MOVIE SECTION ANCHOR: image_K — wide
     because <why>`, and the `### Image K` prompt must itself name wide framing (wide / full shot /
     two-shot / head to toe). One section = one clip = one line. The swap trio (swap_source_video /
     swap_mode / audio) belongs to charswap alone and HARD-FAILS on a section scene.
     STATUS: import is latched OFF (`MOVIE_SECTION_ARM_SHIPPED = False`, image_platform.py) until
     the worker arm ships — a build declaring the method is refused with a 400. Authoring, the
     linter and the auditor all work today.
     Deep-dive: template_reference.md §v959. Prompt doctrine:
     wiki/concepts/prompting/movie-style-prompting.md §5 / §5b / §5c. -->

---

## Finishing

Job-level, ONE section per build, anywhere at top level (the parser stops it at the next `##` header). **Absent section = exactly the pre-v944 behavior** — the auto-edit runs its defaults (template `korella`, captions ON). Every value is validated hard at import.

```markdown
## Finishing

- **captions:** none                             # v944 — `none`, or a caption template name validated against the PIPELINE's own list (local caption_templates/ styles + built-in pycaps names; e.g. `korella`, `word-focus`). Unknown value HARD-FAILS and the message lists the known ones. Absent → none.
- **overlay:** readcaption                       # v944 — none | readcaption. Absent → none. Any `overlay_*` field with no engine declared HARD-FAILS.
- **overlay_age:** I'M 74                        # v944 — REQUIRED when overlay=readcaption.
- **overlay_block:** No Steroids / No Peptides / 7 Boring Things / I do daily to move like I'm 35
                                                 # v944 — the denial/count lines, split on " / " (space-slash-space). Optional.
- **overlay_footer:** (READ CAPTION)             # v944 — optional; the engine defaults to "(READ CAPTION)".
- **overlay_pitch:** 49                          # v944.1 — optional whole number, 30..120. Default 49, MEASURED from the account's posted winner. Declare it only to run a deliberate spacing test.
- **auto_finish:** on                            # v947 — on|off, absent = off. on => the LAST clip approval queues the export with the export_* settings below; the finished export then queues the auto-edit with the v944 fields + autoedit_* settings. A manual export click on an auto_finish job ALSO chains. Publishing is NEVER part of the chain (explicit operator go, always).
- **export_remove_silence:** true                # v947 — ANY ExportSettings field, prefixed `export_`. Validated at import through the real model; unknown name or bad value HARD-FAILS. Only declared fields are stored — everything else keeps the platform default of the day the export runs.
- **export_music_filename:** bed.mp3             # file must exist in the job at export time (checked then, not at import). Writing `none` on a nullable field HARD-FAILS — omit the field instead.
- **export_beat_pins:** {"3": 2.47}              # JSON values allowed
- **autoedit_pip_enabled:** false                # v947 — ANY AutoEditRequest field, prefixed `autoedit_`, EXCEPT template / captions_enabled / overlay_spec (say those via the v944 captions:/overlay*: fields). Declared values beat the job's stored-run inheritance.
```

<!-- v944 — NEVER declare placement. Where the text sits is the engine's job: it measures the
     subject, never crosses the face, keeps to the organic 6-79% zone, and moves the age line and
     the block as separate elements. The build says WHAT the overlay says, not where.
     The rotating content (the denials, and caption item #1) must COHERE with what the clip shows —
     a "No Brutal Workouts" denial over heavy dumbbell curls contradicts the frame. Canonical:
     wiki/synthesis/readcaption-caption-engine.md §"Coherence rule".
     Deep-dive: template_reference.md §v944. -->

<!-- v947 — any unrecognized or malformed bullet in ## Finishing HARD-FAILS at import AND at lint
     (verify_video_format). HTML-commented bullets are inert. Deep-dive: template_reference.md §v947. -->

---

## Comprehension

The reproduction-ready analysis layer (per v587). Five required subsections — every decoded script must fill all five. Decoders can also use this template as the comprehension layer when authoring a new variant — the same structure works in both directions.

### Structural inventory

- **Total**: <N> scenes, <M> clips, ~<T>s duration
- **Per-scene block tags** (canonical vocabulary: HOOK / TITLE / RECIPE / TRANSFORMATION / EXPLAIN / ANATOMY / RESULT / AUTHORITY / PRODUCT / CTA / FOLLOW):
  - Scene 1: <BLOCK_TAG>
  - Scene 2: <BLOCK_TAG>
  - Scene 3: <BLOCK_TAG>

### v-rule inventory

| v-rule | Status | How this video uses it |
|---|---|---|
| v521.1 pin-down | applied | <head/shoulder/crop/distance anchors per persona-primary image> |
| v523 reference chaining | applied | <chain pattern: sequential / snap-back / mixed> |
| v523.1 + v573 + v581 isolated refs | applied | <persona uploaded; product uploaded if branded; everything else inline> |
| v528 clip-mode default | applied | <one full thought per clip, ≤25w cap> |
| v538 speaker mode | applied | <on-camera all / voiceover scene N / mixed> |
| v539 HOOK weird-action | applied | <variant: force-verb / symptom-curiosity / clinical-markup / diagnostic-press / banana-pun / etc.> |
| v540 action_note discipline | applied | <motion-only confirmed; three-beat structure> |
| v541 outfit-change | applied / NOT APPLICABLE | <Day 1 → Day 14 outfit swap if transformation; NOT APPLICABLE if single-day> |
| v544 transitions / clip_mode | applied | <fresh+cut default; continue chains in <which scenes>; no blends> |
| v553 selfie-vlog framing | applied / NOT APPLICABLE | <tight-close default> |
| v553.1 persona never inline-described | applied | <"the main character" used throughout> |
| v573 + v581 product binding | applied / NOT APPLICABLE | <branded product in scenes <X, Y, Z> with `product_image:` field + PRODUCT binding line; NOT APPLICABLE if no branded product> |
| v577 line word budget | applied | <line word counts: max <X>w; multi-line scenes only when total >23w on syntactic boundary> |
| v580 recipe state-evolution | applied / NOT APPLICABLE | <each prep step gets own image; chain preserves glass+lighting+counter> |
| v585 motion capture | applied | <camera-move classifications grounded in flow data per shot> |
| v586 description grammar parity | applied | <every image walks Subject/Composition/Action/Location/Style/Tech; every action_note walks Cinematography/Subject/Action/Context/Style&Ambiance> |

### Rhetorical structure

- **HOOK type**: <force-verb / clinical-markup / diagnostic-press / symptom-curiosity / banana-pun / weird-action-on-prop / bottle-sweep / ...>
- **Frame**: <recipe-as-claim / before-after-transformation / authority-stack / curiosity-gap / 5-truths-listicle / banana-measurement / ...>
- **Payoff structure**: <timeline-promise (day 3 / day 7 / day 14) / climax-position / authority-anchor / outfit-change-time-jump / ...>
- **CTA structure**: <comment-keyword "<WORD>" / comment+follow combined / link-in-bio / DM-trigger / ...>

### Angle / audience signal

- **Niche**: <belly-fat / ED / hair-regrowth / menopause / varicose-veins / etc.>
- **Primary audience**: <gender + age band, e.g. men 40+>
- **Secondary audience** (if any): 
- **Symptom / aspiration**: <what the viewer wants to fix or gain>
- **Emotional register**: <warm / fierce / clinical / desperate / hopeful / curious / etc.>

### Persona archetype + setting tier

- **Persona archetype**: <modern-clinic-doctor / holistic-healer / old-grandma / sexy-doctor / rastafarian-uncle / aesthetic-doctor / etc.>
- **Setting tier**: <Tier-0 selfie-arm / Tier-1 single-setting / Tier-2 multi-setting>
- **Specific settings used**: <kitchen, anatomy-clinic, bedroom, etc.>

### Transfer surface (v758)

The portable-abstraction layer — what makes this video INNOVATABLE. For each load-bearing element only (the ones that carry virality; cross-ref the v598 mechanism stack), split the abstract mechanism from the concrete instance and tag transferability. To innovate later, read ONLY the abstract column and ask "where else does this mechanism go?"

| Load-bearing element | Abstract mechanism (portable, NO niche/prop noun) | Concrete instance (this video) | Transferability |
|---|---|---|---|
| <hook prop> | <e.g. phallic-food shame-proxy> | <e.g. banana + frowny card> | <niche-bound (male shame) / niche-agnostic> |
| <catalyst> | <e.g. violent agent-of-change pour> | <e.g. tea poured → liver melts> | <niche-agnostic> |
| <frame> | <e.g. taboo direct-address + bystander> | <e.g. "don't show your man too often"> | <niche-agnostic> |

- Abstract cell carries NO niche/prop noun (strip "banana" → "phallic-food").
- Only mechanism-carrying elements belong here — not every surface detail.
- Tag each row `niche-agnostic` or `niche-bound (<binder>)`.

---

## Adaptation-extraction

> **MANDATORY on every decode — `verify_decode_format.py` hard-FAILs without this exact
> `## Adaptation-extraction` heading.** It was described in prose two sections up and never
> SHOWN here until 2026-08-24, and the result was that decodes emitted it only by luck: of one
> 13-video batch, 5 carried it and 7 did not, all from the same prompt. The model copies what
> the skeleton shows, so the skeleton has to show it.
>
> **The four bullets are CONTAINERS, not claims.** Write all four even when one does not apply
> to this source — the linter matches the bracketed keyword (`register`, `proxy`, `chain`,
> `angle`), so a missing line is a hard FAIL while `none observed` passes. Inventing a symptom
> to fill a container is worse than writing `none observed`.

- **Frame register read:** <the register these frames sit in, e.g. spectacle event / clinical demo / domestic chore>
- **Symptom-proxy state read:** proxy: <what stands in for the invisible symptom, and its state — or `none observed`>
- **Recurring-character chain map:** chain: <who recurs across frames and what anchors them — or `none observed`>
- **HOOK-angle / swap-layer map:** angle: <the hook angle + which layer a port would swap — or `none observed`>

### Source-derived structure map

> **MANDATORY on every decode — and the SAME omission as the heading above, one year on.**
> Measured 2026-09-01: only **36 of 207** decodes carry this section, and the 36 that do also
> carry the logic card below — they land together, by luck, when the model happens to follow the
> prose grammar. The requirement was written in `wiki/meta/decode-grammar-checklist.md` and
> reached the decoder only through that prose; it was never SHOWN here, in the file that defines
> the shape the model fills in. Same lesson as the 2026-08-24 note above: **the model copies what
> the skeleton shows.**
>
> This map is the CANONICAL BEAT UNIT. One row = one viewer-state change (a new question, job,
> owner, visible state, proof mode, or offer). Anything that counts a source's "beats" — adapt
> fidelity, a SCRIPT DECISION MAP row count, a compression check — counts ROWS OF THIS TABLE.
> Scene headers count cuts and Shown-beats rows count actions; neither is the beat unit. A decode
> without this map is UNMEASURABLE on beats.
>
> Derive the sections from the frames, speech and actions BEFORE any framework label. A boundary
> exists when a causal fact changes; a camera cut alone is not a boundary, and one continuous shot
> may hold several sections. Use as many rows as the source needs. Mark an uncertain boundary
> `INFERRED`. The final row explains the payoff/CTA rather than inventing a next section.

| Section ID | Clip / frame / time evidence | Input state or open question | What happens | Section job | New output or open question | Why the next section follows |
|---|---|---|---|---|---|---|
| S1 | <exact evidence> | <what is true before> | <observed action, line or change> | <job in this video> | <what is newly true or withheld> | <dependency that earns S2> |

### Source logic card

> Written ONLY from the rows above — never from a catalog example. The optional family labels are
> attached afterward for retrieval; `UNLISTED — <plain source-derived name>` is valid and leaves the
> decode complete.
>
> **Emit each field EXACTLY ONCE, as a `- **FIELD:** value` bullet.** This card is shown both here
> and in the decode grammar, and the first decode after this section was added rendered every field
> twice (once with the leading `- `, once without). The table above merged cleanly; only the bullet
> list doubled. One bullet per field.

- **FORMAT SHELL / DELIVERY STYLE:**
- **SOURCE SECTIONS / EXACT ORDER:**
- **SOURCE CAUSAL CHAIN:**
- **SECTION BOUNDARY EVIDENCE:**
- **HOOK JOB / SCROLL-STOP MECHANIC:**
- **VIEWER ENTRY / PRIMARY TENSION:**
- **OPEN LOOP / WITHHELD ANSWER:**
- **PROOF / CREDIBILITY DEVICE:**
- **PIVOT / HANDOFF:**
- **BODY JOB / PERSUASION SHAPE:**
- **HOOK→BODY SEAM:**
- **LOAD-BEARING INVARIANTS:**
- **SAFE SWAP VARIABLES:**
- **BREAK CONDITIONS:**
- **OPTIONAL HOOK FAMILY LABEL + CONFIDENCE:**
- **OPTIONAL BODY FAMILY LABEL + CONFIDENCE:**
- **OPTIONAL CONDITIONAL CHECK(S) APPLIED:**
- **EVIDENCE STATUS + CONFIDENCE:**

### Hero-symptom intensity ledger

One data row per hero symptom, or a single `none observed` row with `n/a` in the other three
columns. The anchor column is matched by a REGEX, so it must be LITERAL — a measurement, a
fraction of the frame, or a spatial relation (`fills`, `dwarfs`, `spans`, `two-thirds of the
frame`, `larger than`). A mood word like "severe" does not match.

**The anchor column is matched by a REGEX. Copy the SHAPE of these example rows.** A mood word
("severe", "visible", "noticeable") does NOT match and is a hard FAIL — every one of these was a
real failure on 2026-08-24. The anchor must contain a measurement, a fraction of the frame, a
count, or one of these relation words: `fills` · `covers` · `spans` · `occupies` · `dwarfs` ·
`extends` · `reaches` · `larger than` · `smaller than` · `compared to` · `past the` · `beyond the`.

| Hero symptom / carrier | Literal observed scale + comparison anchor | Intensity | Exaggeration headroom |
| --- | --- | --- | --- |
| <symptom or `none observed`> | <literal anchor or `n/a`> | <1/5-5/5 or `n/a`> | <YES/NO or `n/a`> |

**Now the same table FILLED IN, because the line above is a legend and the model copies examples.**
The anchor column is matched by a REGEX: it must contain a measurement, a fraction of the frame, a
count, or one of these relation words — `fills` · `covers` · `spans` · `occupies` · `dwarfs` ·
`extends` · `reaches` · `larger than` · `smaller than` · `compared to` · `past the` · `beyond the`.

| Hero symptom / carrier | Literal observed scale + comparison anchor | Intensity | Exaggeration headroom |
| --- | --- | --- | --- |
| loose skin on the thigh | the fold **spans** roughly one third of the thigh's width and hangs **past the** knee line | 3/5 | YES |
| clogged gut (pipe proxy) | brown sludge **fills** about 70% of the transparent pipe's bore | 4/5 | YES |
| none observed | n/a | n/a | n/a |

WRONG, and these exact strings were rejected by the linter on 2026-08-24: `severe cellulite` ·
`loose skin / the wife` · `clogged gut`. Each names the symptom in the column meant to MEASURE it,
and a mood word ("severe", "visible", "noticeable") never matches.

### Shown beats ledger

Every beat the video SHOWS. The evidence column must name a source clip or frame — `clip 3`,
`g_007` — never a bare description.

| Source beat ID | Frame / clip evidence | Shown action / process step | Meaningful objects visibly present |
| --- | --- | --- | --- |
| SB1 | <clip N / frame g_NNN> | <what is done on screen> | <objects in frame> |

**Filled in.** The evidence column holds the clip id and NOTHING else; the description goes in the
next column. `SB1 | she pours the tea` is a hard FAIL.

| Source beat ID | Frame / clip evidence | Shown action / process step | Meaningful objects visibly present |
| --- | --- | --- | --- |
| SB1 | clip 1 | unscrews the jar and tips it over the bowl | glass jar, ceramic bowl |
| SB2 | g_007 | holds the pipe up to the lens, sludge visible | transparent pipe |
| SB3 | frames g_012-g_015 | wipes the counter clear, sets the mug down | cloth, mug |

### Edit-layer overlay ledger

One row per overlay actually burned into the frame. If there are none, write a single row
reading `none observed` across the columns rather than dropping the block.

| element | source | box | window (s) | layer | notes |
| --- | --- | --- | --- | --- | --- |
| <caption / arrow / sticker> | <burned text / graphic> | x[..W] y[..H] | <whole or a-b> | front/back | <colour, outline, motion> |

### Audio design read

Required on NEW decodes (v887a; older decodes are exempt and never retro-failed). What the ear
gets: music bed, voice treatment, diegetic sound, silence, and where each starts and stops.

---

## Google Omni Final Prompts (per clip)

> **v865 (2026-07-24):** the per-clip body is the Google Omni master block — twelve labelled blocks, no `IMMEDIATE ACTION:` / `TERMINAL STATE:` anchors. Both the Prompt A body and the Prompt B body sit INSIDE a triple-backtick fence (Prompt B has no unfenced parser fallback — `code/veo_prompt_overrides.py:396`). The `Negative Constraints:` block is prose inside the Text prompt and is REQUIRED; it is NOT the retired `**Negative prompt:**` field. The spoken line must be the ONLY double-quoted span in each prompt. Canonical fill map: `code/template_omni_master.md`; deep-dive: `code/template_reference.md` §v865.

What the platform renders per clip: one Omni generation = ONE 8-second video, built from the start-frame image + the Storyboard action_note + the line. The section title may also read `## Veo 3.1 Final Prompts` on legacy builds — the parser accepts both.

UGC AUDIO + VOICE STANDARD (house standard — wiki [[realistic-ugc-prompt-templates]] §4; matches the runtime isolated-voice enforcement in `flow_backend.py`/`veo_generator.py`):
  - VOICE: American accent, delivered fast and natural; the `Voice:` + `Dialogue:` blocks carry it.
  - AUDIO: **no music and no background noise** — clean isolated VO. The `Audio:` block reads "no music, no background noise (clean isolated voice)"; the runtime already forces isolated voice.
  - The spoken line ends with an explicit post-speech silence cue (v810) so Omni does not loop filler.

### Clip 1.1 — Scene 1, Line 1 (HOOK)
**Start frame:** Image 1
**End frame:** Image 2
**Text prompt:**
```
Create an 8-second vertical 9:16 realistic UGC video.

Quality / Fidelity Lock: Use the exact same lighting, texture, and iPhone image quality as the attached start-frame image. Do not sharpen the footage. Do not enhance skin texture. Do not apply any AI beauty filter, skin smoothing, HDR effect, cinematic polish, artificial clarity, or stylized color grade. Keep the video raw, organic, and true to the reference image, with the same warm kitchen daylight, natural softness, realistic exposure, imperfect smartphone texture, and authentic home-kitchen atmosphere.

Reference: Use the attached start-frame image as the exact identity and scene reference for Nuri, the young Asian-American holistic healer. Preserve her facial structure, skin tone, hair, and cobalt-blue v-neck dress exactly. Keep her standing at the marble kitchen island holding the amber bottle. Preserve the same look and overall realism from the reference.

Scene: A warm modern American kitchen in daytime. Keep the environment consistent with the start frame: marble island, pantry shelves behind, the amber bottle and a short cucumber in frame.

Camera: Static iPhone perspective, vertical 9:16, eye-level medium shot. Match the framing closely to the start frame: Nuri from the waist up with the island and both hands visible. One continuous take. Locked-off phone camera feel with only very subtle natural micro-movement. No cuts, no zooms, no whip pans.

Ending Camera Beat: None — the framing holds for the whole clip.

Performance / Action: Nuri pours the thin dark oil stream from the amber bottle onto the short cucumber in the man's fist and it runs several times longer and thicker, as she delivers the line. Natural blinking, subtle head movement, and believable mouth movement. The performance feels candid and authentic.

Voice: A warm, confident, playful woman's voice, lived-in and natural. Delivery is warm and playful, unpolished, not acted or announcer-like.

Dialogue: One speaker, one turn.
TURN 1 — SPEAKER: the main AI generated character. She speaks clearly in a warm playful American accent, saying exactly: "this is what black seed oil does to your soldier" then stops speaking and stays silent for the rest of the clip, holding the final expression.

Audio: no music, no background noise (clean isolated voice). The voice sounds close and real to the phone microphone, not studio-polished.

Style: Authentic TikTok/Reels UGC. Raw smartphone footage. Natural skin texture. Slightly imperfect realism.

Negative Constraints: No text overlays. No captions. No subtitles. No logos. No VFX. No 3D. No cartoon. No beauty filter. No face morphing. No identity drift. No flicker. No jitter. No warped hands. No extra fingers. Do not change her cobalt-blue dress, the amber bottle, the cucumber, or the kitchen layout. Avoid excessive camera movement.
```
**Prompt B (policy fallback — Prompt A with the spoken line reworded, v821):**
```
[Prompt A written out in FULL, byte-identical, EXCEPT the Dialogue line, which reads: The main AI generated character speaks clearly in a warm playful American accent, saying exactly: "watch what black seed oil can do for your soldier" then stops speaking and stays silent for the rest of the clip, holding the final expression.]
```

**Two people talking — split the SCENE, then use the short body (v872, confirmed by two first-try renders 2026-07-29).**

**Scene level first.** A beat where two people speak is **TWO scenes**, never one scene with a merged `- **line:**`. One `### Scene N` = one `- **line:**` = one mouth. The answer scene needs its OWN start frame favouring that speaker (a reverse angle) — an image that favours the other speaker cannot be reused. Budget +1 scene, +1 clip, +1 line, +1 image, and recompute the duration sum (v861 buckets). The auditor HARD-FAILS a clip prompt carrying two speech spans, because that is a merged scene reaching the artifact.

**Prompt level.** The spoken part is ONE sentence — Google's documented grammar, descriptor first:

```
The <visual descriptor> says, "<line>"
```

> Legacy single-speaker form (pre-v872), kept verbatim — old builds still carry it:

```text
Dialogue: The main AI generated character speaks clearly in a warm playful American accent, saying exactly: "this is what black seed oil does to your soldier" then stops speaking and stays silent for the rest of the clip, holding the final expression.
```

Descriptor = what the frame shows, garment colour first ("the woman in the blue dress", "the muscular man in the canvas overalls"). NEVER a name — the model has never seen "Nuri". NEVER a bare "the woman"/"the man" when two of that gender are in frame. NO `TURN` labels, NO timestamps, NO `saying exactly:`, NO turn-order plumbing, and **NEVER name a non-speaker** ("X and Y keep their lips closed" seeds those mouths — the §v808 negative-mention trap).

The whole short body that shipped:

```
Animate the attached start-frame image into a <N>-second vertical 9:16 realistic UGC video. Handheld iPhone at chest height with slight natural drift. One continuous take, no cuts, no zooms.

Keep the exact lighting, texture and imperfect iPhone quality of the attached image. No sharpening, no skin smoothing, no beauty filter, no HDR, no cinematic polish, no colour grade. Keep every face, outfit and object as in the image.

<ONE sentence of motion — only what changes over the clip.>

The <visual descriptor> says, "<line>"

<Ambient audio in one line>. No music. No subtitles, no captions.
```

CUT because the start frame already carries them: the `Reference:` identity paragraph, the `Scene:` environment paragraph, the t=0 camera layout, `Ending Camera Beat`, `Style:`, the long negative list. Accent, when it must be locked, goes in its OWN sentence before the speech line — never between `says,` and the quote. Legacy twelve-block bodies still parse and still need their `saying exactly:` + silence clause; forward-only. Deep-dive: template_reference.md §v872.

### Clip 6.1 — movie-section (v959): his shape, not the twelve-block body

**Only on a scene that declares `- **render_method:** movie-section`.** A section clip is NOT a variant of our body — it is a different grammar, and the two must never be mixed inside one build (all shot scenes or none). The whole prompt is the `Setting:` paragraph, then a contiguous run of timestamped beats carrying BOTH speakers, then one tail line. Both speakers in one clip is exactly what v872 forbids on our path — that is why the auditor switches grammars at build level instead of per clip.

**Start frame:** Image 1
**Face refs:** Image 2, Image 3
**Text prompt:**
```
Setting: The loading lot of an American hardware store on a bright day, by the open tailgate of a dusty white pickup. A grey-bearded muscular older man, 60, extremely fit for his age, wears a grey cut-off shirt with the sleeves cut away, holding a heavy paper sack of concrete mix easily in one hand. Behind him a woman in her late 30s, an ordinary shopper in an olive t-shirt with a small store basket, has stopped and is staring at his arm.

00:00 - 00:04
[She stops behind him, staring at the heavy sack hanging from his one hand.]

Woman:
"wow, if my husband looked like you i would never leave the house"

00:04 - 00:06
[The man turns his head from the truck bed and looks at her with an easy smile.]

Man:
"then he should do what i do"

camera switches between faces more often
```
**Prompt B (policy fallback — Prompt A with the last spoken line reworded, v821):**
```
[Prompt A written out in FULL, byte-identical, EXCEPT the LAST quoted span, reworded: "he should just do what i do" — same meaning, different words. The Setting paragraph and every beat stay byte-identical.]
```

What the gates check on this block: the fence starts with `Setting:`, and that paragraph is byte-identical on every section clip of the build · at least one `MM:SS - MM:SS` beat · NO house tokens at all (`Animate the attached`, `Quality / Fidelity Lock`, `Negative Constraints`, `saying exactly`, `No music`, `no background noise`, `Create an N-second`) · the scene's `- **line:**` equals the quoted spans in order · the pacing: no beat over 4.0 words per second, no beat carrying words in a zero-length span, the section at 2.0 w/s of the window or more (his density is 2.3–3.3), the beats spanning no more than 110 % of the window · exactly one `### Clip N.x` for the scene. The tail line is where fault remedies accumulate across reruns; "camera switches between faces more often" is what produces the intra-clip close-up rotation. Deep-dive: template_reference.md §v959; prompt doctrine: `wiki/concepts/prompting/movie-style-prompting.md` §5 / §5b / §5c.

*(One Clip block per `- **line:**` in the Storyboard. Multi-line scenes get one Clip block per line, sharing the same Start frame. EVERY shot scene with a spoken line carries a `**Prompt B ...:**` label + its own fence directly under the Text-prompt fence — Prompt B is the FULL Prompt A copied byte-identical EXCEPT the quoted line, which is REWORDED (different words, same meaning + same selling power). `verify_video_format.py` hard-FAILs if B is missing, if B's body differs from A's body, or if B's line equals A's line. When a source has a known failure mode, bake the guard into the positive Text prompt as an affirmative sentence — e.g. "he is alone in frame for the full clip" for solo videos. Deep-dive: template_reference.md §v865 + §v821, prior Prompt-B shape §v805.)*
## Anchor-Format Prompts (IMMEDIATE ACTION / TERMINAL STATE — reference, selectable)

> **v871 — every build emits this second section too; bold labels keep it inert to the render parser + clip-count; the operator selects which set renders per video in the Batch overview.** The same clips as the Omni section, written in the prior anchor format. Bold `**Clip N.M**` labels (NOT `### Clip`) + a header carrying no "Final Prompts" token, so the render parser and clip-counter ignore it. Both Prompt A and Prompt B are fenced. Deep-dive: `code/template_reference.md` §v871.

**Clip 1.1 — Scene 1, Line 1 (HOOK)**
**Start frame:** Image 1
**Text prompt:**
```
IMMEDIATE ACTION: Nuri pours the thin dark oil stream from the amber bottle onto the short cucumber in the man's fist as she starts the line. TERMINAL STATE: the cucumber now runs several times longer and thicker, held steady as she finishes. The main AI generated character speaks clearly in a warm playful American accent, saying exactly: "this is what black seed oil does to your soldier" then stops speaking and stays silent for the rest of the clip, holding the final expression.
```
**Prompt B (policy fallback — Prompt A with the spoken line reworded, v821):**
```
IMMEDIATE ACTION: Nuri pours the thin dark oil stream from the amber bottle onto the short cucumber in the man's fist as she starts the line. TERMINAL STATE: the cucumber now runs several times longer and thicker, held steady as she finishes. The main AI generated character speaks clearly in a warm playful American accent, saying exactly: "watch what black seed oil can do for your soldier" then stops speaking and stays silent for the rest of the clip, holding the final expression.
```
