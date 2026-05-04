<!--
  KAVENO SCENE TABLE — NEW FORMAT (v586)
  Fill in below. Key conventions (see template_reference.md for full docs):

  • Image prompts: always open with "Shot on iPhone with wide-angle lens, handheld,
    deep focus throughout, vibrant natural HDR daylight." Never cinema camera specs.
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
        "Use the uploaded character reference image for the main character —
        match her facial features, identity, hair, and clothing exactly."
    (2) PRODUCT — only when the image binds the product upload (i.e. when the
        new `product_image:` top-level field is set on this image):
        "Use the uploaded product reference image for [product ingredient name] —
        match its label, packaging, color, and proportions exactly."
    (3) CHAIN — only when `reference_image:` is not `none`:
        "Use Image N as the visual reference for the previous scene — preserve
        the [setting], [lighting], [anchor props], and continuity from there."
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

| Name (used in prompts) | Type | Description | Source |
|---|---|---|---|
| `the main character` | character | Persona — no description needed (passed externally) | External persona upload — Flow slot 0 (Image 1) |
| `the [product name]` | product | [Brief product description — brand, label color, container shape. Used as fallback if upload fails.] | External product upload — Flow slot 1 (Image 2) |

**Note on stripped-down ingredients (v573):** Only the persona and the product are declared as uploaded ingredients. [List of inline-described props, e.g. "the kettle", "the half-lemon", "the surgical pen"] are all described inline in scene prompts — none would pass the isolated-reference test (no clean isolated photos available for those). When the video has no branded product on screen, drop the product row entirely and omit product mentions from all prompts.

---

## Images

### Image 1
- **reference_image:** none
- **Image prompt:**
```
Shot on iPhone with wide-angle lens, handheld, deep focus throughout, vibrant natural HDR daylight, slight wide-angle perspective distortion at the edges of frame. [One brief anchor phrase for the setting — e.g. "Outdoor garden with blurred foliage" or "Bright modern kitchen with a warm honey-oak tabletop in the lower foreground".] [Describe framing: subject's position in the frame, shoulders-spanning, head placement.] [Describe any secondary characters fully on first appearance only.] [Describe the main character's pose, hand positions, gesture — NEVER their face/hair/beard/glasses/wardrobe.] [Describe the foreground props in active use, their state and position in frame.] [Describe expression direction: mouth open mid-word, eyes tracking, warm teaching smile.] iPhone HDR colors, deep focus.
```

### Image 2
- **reference_image:** image_1
- **Image prompt:**
```
The main character, same [setting anchor] as image 1, same framing — shot on iPhone wide-angle lens, handheld, deep focus throughout, vibrant natural HDR daylight. [What's changed: new pose, new action, new prop state.] [Expression direction.] iPhone HDR colors, deep focus.
```

### Image 3 — example with product (v573)
- **reference_image:** image_2
- **Image prompt:**
```
The main character, same [setting anchor] as image 2, same framing — shot on iPhone wide-angle lens, handheld, deep focus throughout, vibrant natural HDR daylight. The main character holds the [product name — e.g. "the Salvora Rhodiola Rosea bottle"] label-forward to camera, [describe how the product is held: at chest height with the cap thumb-up, mid-pour over a glass, etc.]. [Expression direction.] iPhone HDR colors, deep focus.
```
*(Name-based product reference. The platform binds "the [product name]" to the product upload at slot 1. Use this pattern only for product frames. In non-product scenes, omit the product mention entirely — chain via reference_image as in Image 2 above.)*

---

## Storyboard

### Scene 1
- **image:** image_1
- **clip_mode:** fresh
- **transition:** null
- **visual register:** HOOK
- **rhythm tier:** 
- **speaker:** voiceover
- **line:** 
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
