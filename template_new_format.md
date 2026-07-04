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

**Video:** 
**Persona:** 
**Setting:** 
**Duration:** 60s
**Structure:** 
**Video mode:** storyboard
**Auto-split:** off

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
- `character` rows speak. Only `character` rows can appear as `- **speaker:**` values. Single character per video in v681 (multi-speaker dialogue = v682).
- `patient` rows are recurring non-speakers. **Reference column is OPTIONAL (v681e):**
  - **Upload-backed**: Reference points to a real isolated photo (e.g. `patients/refs/donna.png`). Identity binds via the upload. Recommended when the photo exists.
  - **Anchor-scene (no upload)**: Reference = `—`. The FIRST scene that has `cast: <patient>` becomes the anchor. Banana 2 generates the patient on that scene from the image_prompt body's identity prose (race + age + build + hair + clothing per v669 — REQUIRED on the first scene). Subsequent scenes with `cast: <patient>` chain back to the anchor's chosen variant via v512 — Flow uses the anchor scene's render as the reference. NO upload required, NO `reference_image:` needed for face continuity.
- `extra` rows are prose-only one-shots (Donna's husband on the bed scrolling his phone). NO upload, NO `Reference`. Identity in image prompt per v669.

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
- **cut_mode:** auto      # v668 — whisper | timeline | auto. Default `auto` picks `timeline` for bracket-annotation lines (`[music plays]`, `[SFX:...]`) and `whisper` otherwise. Set explicitly when overriding.
- **line:** 
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

## Veo 3.1 Final Prompts (per clip)

What the platform's prompt-builder will emit per scene at job emission. One Veo generation per clip = ONE 8-second video. Each prompt is the **assembled** form of camera spec + action narrative + dialogue + ambient — built from the start-frame image + action_note + line in the Storyboard section. **NO `**Negative prompt:**` block on any clip** — RETIRED per operator standing rule 2026-06-04 (overrides the old v750 separate-negative requirement): omit it entirely and bake critical constraints affirmatively into the positive Text prompt ("one continuous shot", "clean ambient, no music, no background noise", "(no subtitles, no captions)").

UGC AUDIO + VOICE STANDARD (house standard — wiki [[realistic-ugc-prompt-templates]] §4; matches the runtime isolated-voice enforcement in `flow_backend.py`/`veo_generator.py`):
  - VOICE: the speaker talks fast, dynamic, emotive, passionate, expressive, **English with an American accent** (compose with the v642 voice-qualifier tokens; ADD the American-accent clause).
  - AUDIO: **no music and no background noise** — clean isolated VO. The `Ambient:` line below should read "no music, no background noise" (NOT a room-tone/ambient-cue description); the runtime already forces isolated voice.
  - HANDHELD only: add "the main character does NOT move the arm extended to the side of frame, because that hand holds the camera."
  - PACING: ~2 lines of speech per clip (add a short filler sentence if the line is too short; trim in CapCut). Keep ≤8s VO (v577 / production-execution).

### Clip 1.1 — Scene 1, Line 1 (<block tag, e.g. HOOK>)
**Start frame:** Image 1
**Text prompt:**
```
[Cinematography — camera move classification grounded in v585 flow data, e.g. "Static handheld camera, no camera move, slight natural drift."]

[Action narrative — three-beat motion description with explicit timing within the 8s window: start beat 0-2s, mid-clip beat 3-5s, end beat 5-8s. Built from the Storyboard's action_note for this scene.]

The main AI generated character says in a [voice qualifier] voice, "[exact dialogue from the Storyboard's line for this scene][space + pad text from the Storyboard's pad bullet, if present — v644]". (v642+v665: quoted text triggers Veo lip-sync; subject is always "The main AI generated character" — never "She" / "He" / "the man" / "the woman" / persona name — so Veo binds dialogue to the persona-reference upload regardless of perceived gender or wardrobe in the start frame; voice qualifier composes 1-3 tokens from {pace: measured/deliberate/brisk/clipped}, {volume: low/quiet/projected/breathy}, {emotion: weary/serious/urgent/calm/warm/clinical/authoritative/cold/disgusted}. Multi-adjective form uses commas, e.g. "serious, urgent voice". For `**speaker:** voiceover` scenes use: A voiceover with [voice quality] speaks in a [tone] tone, "[line]". See template_reference.md §"Dialogue lip-sync trigger and voice qualifier syntax (v642)" + §"Speaker-subject normalization (v665)" for the full rubric.)

Ambient: no music, no background noise (clean isolated voice).
(no subtitles, no captions)
```
**Prompt B (policy fallback — v821: FULL copy of Prompt A with ONLY the quoted line reworded; different words, same meaning + same selling power, to slip past Flow's classifier):**
```
[Cinematography — SAME as Prompt A, verbatim]

[Action narrative — SAME as Prompt A, verbatim; keep the IMMEDIATE ACTION + camera prose unchanged]

The main AI generated character says in a [voice qualifier] voice (American accent): "[the SAME dialogue said in DIFFERENT words — reword only the quoted line; still lowercase (v693), no em-dash (v615), no v806/v796 banned wording][pad text if present — v644]".

Ambient: no music, no background noise (clean isolated voice).
(no subtitles, no captions)
```

### Clip 2.1 — Scene 2, Line 1 (<block tag>)
**Start frame:** Image 2
**Text prompt:**
```
[Cinematography]

[Action narrative — three beats with timing]

The main AI generated character says in a [voice qualifier] voice, "[exact dialogue]".

Ambient: no music, no background noise (clean isolated voice).
(no subtitles, no captions)
```

*(Repeat one Clip block per `- **line:**` in the Storyboard section. Multi-line scenes get one Clip block per line, sharing the same Start frame. NO Negative prompt block on any clip — standing rule 2026-06-04. When the source has a known failure mode, bake the guard into the positive Text prompt as an affirmative sentence — e.g. "the scene stays in the clinic for the full clip" for clinic-only videos, "he is alone in frame for the full clip" for solo videos.)*

*(v821 SUPERSEDES the old v805 voice-only shape for NEW builds — EVERY shot scene with a spoken line carries a `**Prompt B (policy fallback ...):**` label + fence directly under its Text-prompt fence. Prompt B is now the FULL Prompt A copied verbatim EXCEPT the quoted line, which is REWORDED (different words, same meaning + same selling power). The gen-time prominent-people block is usually the LINE tripping the audio classifier, not the face — so the fix changes the words, keeps the action. `verify_video_format.py` hard-FAILs if B is missing, if B's body differs from A's body, or if B's line equals A's line. The worker re-submits the same clip once with Prompt B on a gen-time prominent block, then terminal-fails. Upload-time face reject still swaps the IMAGE (v815), not the line. Old shipped builds keep their voice-only B; the worker still accepts it. Deep-dive: template_reference.md §v821, prior shape §v805.)*
