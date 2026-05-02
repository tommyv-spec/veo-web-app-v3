# Kaveno Scene Table Reference

## Two formats accepted

The parser auto-detects which format you're using based on whether `### Image N` headers are present.

- **New format** (`template_new_format.md`): images and scenes are separate. Scenes reference images by index. A single image can power multiple scenes. Each scene can have multiple voiceover lines.
- **Legacy format** (`template_legacy_format.md`): each `### Scene N` is BOTH an image and a single-line scene. 1:1 mapping. Simpler but less flexible.

Use the new format unless you're porting from older scripts.

## Document header (both formats)

All optional except where noted. Placed at the top of the markdown.

| Field | Example | Notes |
|---|---|---|
| `**Video:**` | `4-23.1 The Balloon That Can't Hold Air` | Appears as batch name prefix |
| `**Persona:**` | `Ogechi` | Informational, stored with batch |
| `**Setting:**` | `Tier 2 — Rustic barn workshop` | Informational |
| `**Duration:**` | `60s` | Target video length in seconds |
| `**Structure:**` | `10 scenes, 8 images — HOOK/RECIPE/ANATOMY` | Informational, free text |
| `**Video mode:**` | `storyboard` / `auto-cycle` / `simple` | Default: `storyboard` |
| `**Auto-split:**` | `off` / `on` | Default: `off` |

## Image block (new format only)

```
### Image 1
- **reference_image:** none                          # or "image_N" to chain from another
- **product_image:** the Corella saffron bottle      # OPTIONAL — only on images that bind a product upload (v581)
- **Image prompt:**
`` `
Use the uploaded character reference image for the main character — match her facial features, identity, hair, and clothing exactly.
Use the uploaded product reference image for the Corella saffron bottle — match its label, packaging, color, and proportions exactly.
Use Image N as the visual reference for the previous scene — preserve the [setting], lighting, [anchor props], and continuity from there.

A middle-aged man in a barn, soft morning light, holding a herb bundle...
`` `
```

**Required**: `### Image N` header with integer N starting at 1, and the fenced `**Image prompt:**` block. As of v581, the prompt body MUST begin with explicit reference-binding lines (see "Explicit reference bindings (v581)" section below for the exact wording template).

**Optional fields**:
- `reference_image` — defaulting to `none`. If set to `image_K`, this image will be generated using image K as a visual parent. Forward/self references are rejected.
- `product_image` (v581) — only on images that bind the product upload. Value is the product ingredient name verbatim from the Ingredients table (e.g. `the Corella saffron bottle`). When present, the platform binds the product upload to this image and resolves "the uploaded product reference image" in the prompt body to the correct Flow slot at emission. Absent on images that don't show the product (e.g. recipe scenes before the product reveal, talking-head scenes with no bottle in frame).

## Scene block

### New format

```
### Scene 1
- **image:** image_1                     # REQUIRED — must point to a defined image
- **clip_mode:** fresh                   # or "blend"
- **transition:** null                   # or "cut" / "blend"
- **visual register:** HOOK              # free-form; first word before em-dash is stored
- **rhythm tier:** authority (17w)       # free-form
- **speaker:** on-camera                 # v537 — "on-camera" / "voiceover" / "auto" (default)
- **line:** First voiceover line here    # one per dialogue beat
- **action_note:** Specific action for the line above
- **line:** Second voiceover line
- **action_note:** Action for line 2
```

**Required**: `### Scene N` header, `- **image:** image_N`, and at least one `- **line:**` bullet.

**Optional**: `clip_mode`, `transition`, `visual register`, `rhythm tier`, `speaker`. Each `- **action_note:**` attaches to the `- **line:**` immediately preceding it. The `speaker` field applies to ALL lines in the scene (since they share the same image, they share the same on-camera/voiceover state).

**Line word budget (v577)**: each `- **line:**` becomes ONE Veo clip = ONE 8-second generation, holding **~21 words at natural pacing (±2 tolerance)**. If a scene's TOTAL dialogue is ≤21 words, write it as ONE line — not multiple short fragments. Multi-line scenes are reserved for scenes whose dialogue exceeds the single-clip budget (>23 words) AND can be split on a natural syntactic boundary into chunks that are each ≥10 words and syntactically complete. See "Line granularity within a scene" below for full details + bad/good examples.

### Legacy format

```
### Scene 1
- **reference_image:** none
- **text:** The voiceover line for this scene
- **scene_transition:** null             # or "cut" / "blend"
- **clip_mode:** fresh                   # or "blend"
- **visual register:** HOOK
- **rhythm tier:** authority (17w)
- **speaker:** on-camera                 # v537 — "on-camera" / "voiceover" / "auto"
- **action_note:** Specific physical action for this beat
- **Image prompt:**
`` `
Image prompt text goes here
`` `
```

**Required**: `### Scene N` header + fenced `**Image prompt:**` block.

**All other fields optional**.

## Accepted values

| Field | Valid values |
|---|---|
| `clip_mode` | `fresh`, `blend` (first word only — trailing notes OK) |
| `transition` / `scene_transition` | `null`, `none` (→ stored as "null"), `cut`, `blend`, or any other token (first word kept) |
| `visual register` | Free text. First token before em-dash is stored in the column. |
| `rhythm tier` | Free text |
| `reference_image` / `image` | `none` or `image_N` where N is another defined image index |
| `speaker` (v538) | `voiceover` is the ONLY value that triggers off-screen narration. Anything else — `on-camera`, missing, empty, `auto`, or any unrecognized token — produces on-camera dialogue with lip-sync. Synonyms for voiceover: `vo`, `voice-over`, `narrator`, `narration`, `off-screen`. |

## Speaker mode — explicit-only (v538)

The `speaker` field controls whether the dialogue line is delivered by the on-camera main character (lip-sync ON) or by an off-screen voiceover narrator (lip-sync OFF; visible subjects keep their lips closed).

**v538 rule (current)**: voiceover fires **only** when the markdown explicitly says `**speaker:** voiceover`. There is no auto-detection. There is no phrase-match fallback. There is no implicit voiceover behavior. If you don't write `voiceover`, you get on-camera — every time.

This is a deliberate inversion of the v517-v537 behavior, which ran a hardcoded phrase-match detector (`_detect_voiceover_only()`) whenever the markdown didn't specify. The detector matched phrases like `"hand rises into frame"`, `"fingers gripping"`, `"close-up of the held"`, `"her shoulders span"` — and silently misrouted any RECIPE close-up that happened to mention fingers gripping a lemon, or any male-persona script using `"his shoulders"` instead of the gendered female phrase. The misrouting was silent: no error, no warning, just the wrong Veo prompt rendered for the wrong scene. v538 cuts that fallback entirely.

**Use `voiceover` when** the visible subject in the frame is NOT the speaker:
- HOOK before-state shots (patient on a scale, daughter on a couch, customer in their driveway) where the persona narrates over a silent secondary character.
- HOOK after-state shots where the persona is not in the frame and the visual is the transformed secondary character alone.
- Hand-only product close-ups where only the persona's hand is visible — no face for Veo to lip-sync to.
- ANATOMY semi-transparent overlays — no real human face in frame.
- Establishing shots with no human subject (a clinic interior, a kitchen, a grocery aisle).

**Default to on-camera (omit the field, or write `on-camera`) when** the visible main character IS the speaker:
- Talking-head selfie addressing the lens (REVEAL beats, OUTRO CTAs, explanation scenes).
- Two-shots with the persona on camera as the dominant speaker.
- RECIPE scenes where the persona is in frame at the counter, doing work AND speaking simultaneously.
- Anything where you want the line lip-synced to the visible main character's mouth.

**Practical rule of thumb**: if you have to think about whether to use voiceover, you almost certainly want on-camera. Voiceover is the special case — reserve it for the explicit list above.

**Multi-line scenes**: a scene with multiple `- **line:**` bullets has a single `- **speaker:**` value that applies to all lines, since all lines share the same image (and therefore the same on-camera/voiceover state).

**Storage**: the value is denormalized onto the `image_nodes.speaker_mode` column at import time, then propagated through `dialogue_json` to the runtime prompt-builder. Pre-v537 nodes have NULL in this column, which now (v538) means on-camera — same as if you'd written nothing. To flip a pre-existing node to voiceover, re-import the markdown with the field set, or update the DB column directly.

## The three "visual register" segments

Conventional categories used by the production pipeline:
- **HOOK** — bright flat overexposed neutral white lighting; real-world elements with violent impossible reactions
- **RECIPE** — amateur iPhone handheld, flat fluorescent, cluttered counter
- **ANATOMY** — semi-transparent muscular figures on blurred real-world backgrounds with color-coded energy effects

You can use any other register name — the parser just stores whatever you put there.

## Notes

- Forward/self image references are rejected (image 3 referencing image 5 is invalid)
- `### Image N` indexes don't have to be sequential but must all be unique
- An import with 0 scenes or 0 images fails with a clear error
- `Image prompt` blocks must use triple-backtick fences; language hint after opening fence is optional
- All free-form text fields (rhythm tier, visual register, transition notes, etc.) are stored as TEXT with no length cap as of v489

---

## Decoding source videos — pipeline-based extraction (v579)

When the input is a source MP4/MOV file, the decoder MUST run a four-stage extraction pipeline to produce ground truth before writing the markdown. Visual reconstruction from sparse frame samples + caption OCR alone produces inaccurate decodes — wrong dialogue, missed beats, hallucinated brand names, wrong CTA words. This rule supersedes any "look at frames + read captions + reconstruct" approach.

### Why this rule exists

Two empirical failure modes from pre-v579 decodes that this rule prevents:

1. **Caption-driven script reconstruction is unreliable.** Karaoke-style burned-in captions reveal one word at a time with overlap; OCR picks up partial letters from the previous and next words and misses framing context. A whisper transcript of the actual audio is the authoritative script source, full stop. OCR is for cross-validation of brand names and label text only.

2. **Fixed-interval frame sampling misses shot boundaries.** Extracting frames every 2s or 4s skips cuts and oversamples held shots. PySceneDetect finds the actual shot boundaries from the visual signal — the frames sampled at those boundaries are representative of each distinct shot's content. Without this, the decoder either misses scenes (if the cut falls between sample points) or fabricates scenes (if a held shot looks like two scenes because of small lighting drift).

The combined effect of these failures: pre-v579 decodes routinely got the broad structure right but the specifics wrong — wrong brand names, missed timeline-promise beats, wrong CTA keywords, softened or invented dialogue, and missing narrative beats entirely.

### The mandatory four-stage pipeline

| Stage | Tool | Input | Output |
|---|---|---|---|
| 1 | `ffmpeg` | source video | 16kHz mono PCM WAV |
| 2 | `whisper.cpp` (via `pywhispercpp`) + GGML model | extracted WAV | timestamped dialogue segments |
| 3 | `scenedetect` `AdaptiveDetector` | source video | shot boundary timestamps |
| 4 | `view` tool on extracted frames at shot boundaries | source video + boundaries | setting/composition/pose per shot |

These four signals are then aligned by timestamp into a unified manifest before any markdown is written. The decoder reads from the manifest, not from the video directly.

**Required setup commands** (one-time per environment):
```
pip install scenedetect pywhispercpp opencv-python pytesseract --break-system-packages
# tesseract + ffmpeg are typically pre-installed
```

A whisper.cpp GGML model file is required. The user typically uploads one (e.g. `ggml-base.en-q5_1.bin`); if absent the decoder cannot run stage 2 and must ask the user to provide one. The HuggingFace download path is blocked in most sandboxes — do not attempt to fetch the model from there.

### Stage 1 — audio extraction

```
ffmpeg -i <source.mp4> -vn -ac 1 -ar 16000 -c:a pcm_s16le /tmp/audio.wav
```

`-ac 1` mono, `-ar 16000` 16 kHz, `-c:a pcm_s16le` raw PCM — exactly what whisper.cpp wants. No re-encoding, no compression artifacts.

### Stage 2 — whisper.cpp transcription

```python
from pywhispercpp.model import Model
m = Model('<path-to-ggml-base.en-q5_1.bin>', n_threads=4)
segments = m.transcribe('/tmp/audio.wav')
for s in segments:
    # t0/t1 are in centiseconds (1/100s)
    print(f"[{s.t0/100:6.2f}s -> {s.t1/100:6.2f}s] {s.text.strip()}")
```

The output is the authoritative script. The decoder treats the whisper transcript as ground truth even when it conflicts with what the on-screen captions or the bottle label say — those visual signals are for cross-validation and brand-name verification only.

### Stage 3 — shot boundary detection

```python
from scenedetect import detect, AdaptiveDetector
scenes = detect('<source.mp4>', AdaptiveDetector(adaptive_threshold=3.0))
for i, (start, end) in enumerate(scenes, 1):
    print(f"Shot {i}: {start.get_seconds():.2f}s -> {end.get_seconds():.2f}s")
```

`AdaptiveDetector(adaptive_threshold=3.0)` is the recommended default — it catches both hard cuts AND the b-roll cuts inside composite layouts that `ContentDetector` misses. For talking-head videos with no b-rolls, `ContentDetector(threshold=27)` with fewer detected shots is fine, but adaptive is the safe default.

### Stage 4 — frame inspection at shot boundaries

For each detected shot, sample the midpoint frame:
```
mkdir -p /tmp/video_frames
ffmpeg -i <source.mp4> -vf "fps=2" /tmp/video_frames/frame_%04d.png
```
Then read frames whose timestamp falls inside each shot's `[start, end]` range using the `view` tool. The midpoint frame of a shot is the safest representative — start-frame and end-frame can be transition frames during cuts.

For each shot record: setting (kitchen / office / bedroom / etc.), composition (talking head / b-roll composite / product hold / etc.), persona pose, foreground props, presence/absence of product. The shot's representative frame answers all of these directly.

### Stage 5 — unified manifest

Align the four signals into a single table before writing the markdown. Each row of this table is one detected shot:

```
| Shot | Start | End | Setting | Composition | Bottle | Dialogue (from whisper, overlapping the shot's [start,end]) |
```

Multiple whisper segments can overlap a single long shot (e.g. a 22-second composite recipe shot covers 4 dialogue segments). One short shot can split a single whisper segment across two rows. The manifest is the source of truth from which the markdown is authored.

### Pre-v579 anti-patterns the decoder must avoid

1. **Skipping the pipeline because "the captions look readable."** Captions are graphic overlays designed for muted-scroll viewing — they're aggressive paraphrases of the actual audio, often missing 30-50% of the spoken words and adding emphasis words that aren't said. Always run whisper.
2. **Sampling frames every 4s and treating each as a scene.** This was the v544–v577 default. It misses cuts that fall between samples and over-samples held shots. Always use scene detection.
3. **Inferring brand name from the bottle visible in the frame alone.** The visible bottle in the source may be a different brand from the one named in the audio (the source creator may be using a placeholder bottle, or the video may be a re-cut). The whisper transcript is the script ground truth; OCR of the bottle label is one signal among many. When they disagree, the audio wins for the spoken brand name and the markdown reflects the audio.
4. **Reconstructing CTA words from memory of "what these videos usually say."** The actual CTA word matters — "comment HEALTH below" vs "comment YES" vs "type the word VITALITY" are three different decodes. Always source the CTA word from the whisper transcript, not from training-data priors about how supplement promos work.
5. **Missing timeline-promise beats** ("by day 3...", "by day 7..."). These beats are short (~5-7 seconds each) and easy to miss when sampling sparsely. Whisper's word-level timestamps make them impossible to miss.
6. **Treating OCR as primary content extraction.** OCR of karaoke captions is high-noise (animated word reveals overlap, partial letters get picked up). Use OCR ONLY for cross-validating brand names against the bottle label and the audio.

### When OCR IS still useful (cross-validation only)

The decoder MAY skip OCR if (a) brand name is unambiguous in the whisper transcript and the user has stated they don't need caption decoding, or (b) the user has explicitly opted out (e.g. "OCR is not needed because we generate captions later"). In default operation, OCR runs as a fifth optional stage that only validates brand names visible on labels — it never produces script content.

### Decoder output requirements (when source is a video)

The decoded markdown MUST include in its top-of-file comment block:

```
PIPELINE USED FOR THIS DECODE (v579 video-understanding):
  1. ffmpeg → 16kHz mono PCM audio extraction
  2. whisper.cpp + GGML model → N timestamped dialogue segments
  3. PySceneDetect AdaptiveDetector → M detected shot boundaries
  4. Frame inspection at shot midpoints → setting / composition / pose per shot
  All four signals were aligned by timestamp into a unified manifest
  before the decode below was authored.
```

This serves as both an audit trail (so a reviewer can verify the decode was pipeline-derived) and as a self-check for the decoder LLM (the act of writing this comment forces the pipeline to actually be run).

If the user has explicitly opted out of OCR or any other stage, note that in the comment block as well: "OCR was intentionally skipped per user instruction — captions in the adapted version will be generated post-render, not from the source."

### Reusability

The pipeline is reusable for any future video. The user's whisper.cpp GGML model file persists across decode sessions (re-uploadable but typically already present in `/mnt/user-data/uploads/`). PySceneDetect, ffmpeg, and tesseract are environment-level installs. Once set up, decoding a new source video is roughly 2-5 minutes of pipeline execution + however long it takes to write the markdown from the resulting manifest.

---

## Image prompt conventions (Nano Banana 2)

Image prompts are written for Nano Banana 2 with a persona's character reference image passed externally on every generation. The conventions below keep prompts tight, subject identity locked, and output style consistent.

### Camera and lens — iPhone wide-angle
Every image prompt opens with a consistent camera/lens spec:

> Shot on iPhone with wide-angle lens, handheld, deep focus throughout, vibrant natural HDR daylight, slight wide-angle perspective distortion at the edges of frame.

This gives the recognizable small-sensor-plus-wide-lens look: deep focus throughout (no cinematic bokeh), slight barrel distortion at the edges, punchy HDR color. Do NOT use cinema camera specs (Sony FX6, 50mm f/1.8, shallow DOF, cinematic 4K grade) unless the source video actually has that look — they'll push the output toward a glossy commercial aesthetic that breaks the TikTok-native feel.

### Main character — never described
The persona's character reference image is passed externally to Nano Banana 2 on every single generation, including Image 1. Image prompts must NEVER describe the main character's face, hair, beard, glasses, skin, age, ethnicity, or wardrobe. Refer to them only as **"the main character"** (or `he` / `she` as a pronoun) and describe only what the reference image can't supply:

- **Pose** — hand positions, body orientation, what they're holding
- **Expression direction** — "mouth slightly open mid-word", "eyes tracking the pour", "warm grandfatherly smile", "eyebrows raised"
- **Position in frame** — "leaning in from the right third", "chest and head fill the upper two-thirds", "standing directly behind the bench"

Other characters in the scene (scene-specific one-offs — a patient being treated, a customer, an extra) have NO external reference, so describe them fully in the first image they appear in. Subsequent images can then reference them via *"same [patient/subject] as image X"*.

### Product — referenced by name in product frames (v573)
When the video has a branded product whose label/packaging must render correctly, that product is uploaded as a clean isolated reference at Flow slot 1 (positionally, Flow's "Image 2"). Image prompts in **product frames** invoke the product BY NAME — same convention as persona — and the platform's name-binding logic attaches the upload to those scenes.

**Naming convention.** Use a descriptive name that uniquely identifies the product, capitalized normally:
- `the Salvora Rhodiola Rosea bottle`
- `the Yogi DeTox tea box`
- `the Costco green tea extract bottle`
- `the [brand] gummies bottle`

Generic short forms like `the product` or `the bottle` work too but are less specific; if the video has multiple branded items (a Costco-aisle survey), pick a name distinctive enough that the binder can't confuse it with an inline-described prop.

**What goes in the body.** In a product frame, write the product's role in the composition: *"the main character holds the Salvora Rhodiola Rosea bottle label-forward to camera at chest height"*. Don't re-describe the label, color, or typography — that's what the upload carries. The Ingredients table entry holds a brief description as a fallback, but the prompt body trusts the upload.

**What does NOT go in the body.**
- Positional references to the product as "image 2" — that's the platform's slot identity, not a body-text convention. Use the descriptive name instead
- Detailed label specs ("white plastic bottle with navy SALVORA wordmark in serif type, painted lily illustration on the right") — the upload carries this; describing it in body text causes Nano Banana 2 to try to re-render it from text on top of the upload, which produces drift
- The product's name in scenes where the product isn't visible — leaves the upload bound to a frame that doesn't show it, which can bleed product geometry into the composition

See the **Product frames** section below the isolated-reference rule for the full per-scene decision flow.

### Framing — describe composition, not aspect ratio
Aspect ratio is handled by the pipeline (vertical 9:16 for Kaveno). Do NOT specify it in prompts. Instead, describe **composition** — how the main subject and other elements occupy the frame. Match the framing 1:1 to what the source video shows in that beat.

Be specific about where things sit:
- "His shoulders nearly span the full width of the frame"
- "Head at the upper third line, chest filling the middle of the composition"
- "Ingredients arranged in a tight horizontal row along the bottom edge of the frame, close to camera"
- "Only the near edge of the workbench visible at the very bottom of the frame"
- "Zen garden visible through the window on the right side of frame, background softly compressed by the close framing"

Avoid generic shot-size words alone ("wide medium shot", "close-up"). They underspecify and lead to generic results.

### Camera distance — intimate by default, even when "pulled back"
Kaveno talking-head scenes are shot close. Even the "pulled back" OUTRO seated shots are still tight by normal video standards — the camera never shows a full body, floor, or feet. Social short-form 9:16 with burned-in captions has no room for wide compositions.

Distance and crop guidelines by scene type:

**Tight close-up** (RECIPE talking-head at table, HOOK close prop-reveal):
- Camera approximately one arm's length (~3 feet)
- Upper body fills the upper two-thirds of the frame, shoulders span frame width
- Foreground props in immediate foreground, close to camera
- Crop at mid-chest or waist

**Tight medium** (OUTRO seated talking-head, any "pulled back" shot):
- Camera 3–4 feet from subject, NOT 5+ feet
- Subject fills the **upper three-quarters of the frame**
- Head near the upper edge of frame, torso dominating the middle
- Crop at the knees — **NO feet, NO floor, NO empty space above the head**
- Always include "NO feet, NO floor" explicitly in the prompt — nano banana defaults to wider framing without it

Specify distance and crop explicitly — generic "medium shot" underspecifies:

- "Tight close framing, camera positioned approximately one arm's length from him" ✓
- "Tight medium framing — camera approximately 3-4 feet from him, subject filling the upper three-quarters of the frame, cropped at the knees with NO feet visible, NO floor visible" ✓
- "Medium shot" / "pulled back" / "wide shot" — DON'T use alone ✗

For foreground props (ingredients, tools, a mixing bowl), place them in the **immediate foreground close to camera** — "only the near edge of the workbench visible" — rather than "laid out across a wide table". Wide tables read as distance and make the subject feel small.

Full-body shots (feet visible, floor visible) are NOT used in Kaveno content. If you feel tempted to describe one, the prompt is wrong.

### Background vs. composition — priority
Composition (how the subject is placed in the frame, their pose, their action, the props in the immediate foreground) matters far more than background detail. Describe the setting with **one brief anchor phrase per new location** and let the image generator do the rest.

Over-describing backgrounds is counterproductive because:
- The `reference_image` chain already inherits setting visually from prior images
- Nano Banana 2 renders plausible setting context from a brief phrase — it doesn't need an inventory
- Detailed setting descriptions burn prompt budget and dilute attention from what matters (the subject, the action, the foreground props)

**Good setting anchors** — this is all the background needs:
- "Outdoor tropical garden setting with blurred tree foliage"
- "Bright modern domestic kitchen interior with a warm honey-oak wooden tabletop"
- "Indoor apothecary consultation room with shelves of herb jars on the left and a bright window on the right"
- "Warm Japandi apothecary interior, cream plaster walls"

**Over-descriptions to avoid:**
- Shelf contents: "jars in shades of brown, tan, beige, cream, and amber containing dried herbs, roots, bark, and seeds"
- Visible through windows: "a beige-and-white American single-family home with shutters visible across the way, a bit of front lawn"
- Small decorative items that aren't the scene's focus: rolled scrolls, flags, ceramic crocks of utensils, potted herbs
- Appliance inventory: "stainless-steel microwave mounted on white upper cabinetry, black stainless range stovetop"
- Wall colors, flooring details, ceiling moldings — none of this matters unless it IS the scene

**When specificity IS warranted:**
- A prop actively in use in this scene (the mixing bowl, the coconut, the book being presented)
- A branded item whose label needs to render correctly (the "EXTRA VIRGIN OLIVE OIL" bottle)
- An element critical to the persona's identity or setting category (e.g. the Zen garden view IS the Japandi aesthetic — mention it briefly)
- The hero prop whose details carry narrative weight (the open book's contents, the kidney model's state)

**For non-establishing images:** just write "same [setting type] as image N" — the visual parent does the work. Don't re-enumerate the background elements you already described once.

### Transitions and clip_mode — cut + fresh by default, blend only on tiny visual deltas (v544)

**The two fields and what they actually control.**
- **`clip_mode`** is how Veo gets the **first frame** for this clip:
  - `fresh` — Veo uses this scene's image as its start frame. Clean slate. The character is free to move from whatever pose the image shows; there's no carry-over from the previous clip's final state.
  - `blend` — Veo uses the **previous clip's last frame** as the start frame for this one (the scene's own image gets ignored as a start frame). This forces physical continuity at the splice point — useful when the previous clip ended mid-action and you want this one to pick it up exactly. Risky when the previous clip's final pose is unpredictable.
- **`transition`** is how the **edit between this clip and the next** gets rendered in post:
  - `cut` — hard cut, the splice is instantaneous and clean.
  - `blend` — cross-dissolve over the splice. Two clips fade through each other for the transition duration.
  - `null` — first scene only (no prior clip to transition from).

These two fields are independent. `clip_mode` decides what Veo sees at frame 0; `transition` decides what the editor does at the boundary between clip N and clip N+1.

**The default: `clip_mode: fresh`, `transition: cut`.**
This is the conservative path that works for almost every scene. Each clip generates from its own image with no carry-over. Cuts between clips are clean and don't try to reconcile two different visual states. If you don't have a specific reason to use blend, use cut + fresh.

**Why we changed from "default blend" (the old rule) to "default cut" (v544).**
The old rule said "default to blend; reserve cut for setting/register changes." That was theoretically right — a cohesive recipe-pour sequence does feel smoother with cross-dissolves. But in practice, `transition: blend` produces visible **morph artifacts** whenever the visual delta between two clips is non-trivial: faces stretching, hands ghosting, props duplicating mid-dissolve. The cross-dissolve is a literal pixel cross-fade, not a smart morph; if pixel A is a doctor's hand and pixel B is a patient's belly, the dissolve produces a frame where you see both at half opacity, which reads as a render glitch.

`clip_mode: blend` (using the previous clip's last frame as the next clip's start frame) is even riskier: Veo's last frame from clip N is rarely a clean composition the way an image-generated start frame is. It's mid-motion, often mid-blink, often with the persona's hands in a transient position. Forcing clip N+1 to start from that frame inherits whatever weirdness was at the tail of clip N — and Veo then has to reconcile that uncomposed start with the new scene's intended pose, which produces stiff or contorted motion in the first second.

**When to actually use blend (the narrow case).**
Use `transition: blend` ONLY when the visual delta between clip N's final state and clip N+1's start state is **very small** — small enough that a cross-dissolve looks like real motion continuity instead of a render artifact. Concretely:
- Same camera angle, same framing, same persona pose
- An ingredient moved a small distance (e.g., the kettle was lifted 4 inches; the persona's hand drifted from chest to mouth)
- No other significant change

The test: if you put the two start frames side by side and the only difference is a small object position or a slight head-tilt, blend is safe. If multiple things moved, or the camera shifted, or the persona changed posture, use cut.

`clip_mode: blend` (carrying Veo's last frame forward) is even more restrictive: only use it when a single physical action genuinely spans two clips and you need the boundary to be invisible (e.g., a long pour that won't fit in one 8-second clip). Even then, prefer to redesign the scene so each clip is a complete action and use cut between them.

**Action scenes: always `clip_mode: fresh`.**
When the character is doing something — gesturing, picking up a prop, demonstrating an action — the character must move freely from the start frame Veo can see clearly. Using `blend` here forces Veo to start from whatever transient mid-motion frame the previous clip ended on, and the character ends up frozen, contorted, or moving stiffly because the model is working from a bad start frame. Always `fresh` for action.

**Transformations: always `transition: cut`.**
For Day 1 → Day 14 transformation cuts (or any other before/after time-jump on the same character/setting), the visual delta is huge — the body changed, the outfit changed (per v541), maybe the lighting shifted slightly. A blend would cross-dissolve the two states in a way that reads as a visible morph artifact, breaking the "two weeks went by" illusion. The cut is what signals time passing; the blend would signal "same shot, post-processed in two ways."

**Setting and register changes: always `transition: cut`.**
HOOK → RECIPE, RECIPE → ANATOMY, ANATOMY → OUTRO, indoor → outdoor — all of these involve a different room, different camera setup, different framing. A blend across these would dissolve the entire composition into a different one, which is the canonical morph-artifact failure mode. Earn the snap with a clean cut.

**Standard 7-scene template under v544:**
- Scene 1: `clip_mode: fresh`, `transition: null` (opening)
- Scene 2: `clip_mode: fresh`, `transition: cut` (HOOK reveal — same setting but the prop state changed enough to want a clean cut)
- Scene 3: `clip_mode: fresh`, `transition: cut` (HOOK → RECIPE setting change)
- Scene 4: `clip_mode: fresh`, `transition: blend` (RECIPE pour 1 → pour 2 — same camera, same hand, the kettle lifted slightly. Tiny delta — blend is safe here.)
- Scene 5: `clip_mode: fresh`, `transition: blend` (RECIPE pour 2 → pour 3 — same conditions as above)
- Scene 6: `clip_mode: fresh`, `transition: cut` (RECIPE → OUTRO setting change)
- Scene 7: `clip_mode: fresh`, `transition: cut` (OUTRO seated → OUTRO CTA — different gesture, different head position, blend would smear)

Note that `clip_mode` is `fresh` everywhere by default. The only field that varies is `transition`, and even that defaults to `cut` except in the narrow continuous-pour case where two adjacent images differ by just a kettle-lift or a hand-drift.

**Standard 4-scene transformation template (HOOK 1 / TRANSFORMATION 1 / OUTRO 2) under v544:**
- Scene 1: `clip_mode: fresh`, `transition: null` (Day 1 opener)
- Scene 2: `clip_mode: fresh`, `transition: cut` (Day 1 → Day 14 — never blend a transformation)
- Scene 3: `clip_mode: fresh`, `transition: cut` (clinic → OUTRO setting change)
- Scene 4: `clip_mode: fresh`, `transition: cut` (OUTRO instructional → OUTRO CTA — different gesture)

**Quick decision flow.**
1. Is this scene 1? → `transition: null`
2. Did the setting, register, or camera angle change from the previous scene? → `transition: cut`
3. Is this a transformation or before/after reveal? → `transition: cut`
4. Did multiple things change between the two start frames (persona pose + prop state + camera)? → `transition: cut`
5. Did exactly ONE small thing change (a prop moved a few inches, a hand drifted)? → `transition: blend` is allowed — but `cut` is still fine and never wrong
6. `clip_mode`: always `fresh` unless you have a specific reason for `blend` (rare; usually a redesign-the-scene signal)

### Scene granularity — don't over-split
Consecutive voiceover beats that share the **same context** AND the **same physical action** pack into ONE scene with multiple `- **line:** / - **action_note:**` pairs — not multiple scenes with the same image. The Kaveno new format explicitly supports multi-line scenes for exactly this reason.

Trigger a new scene only when:
- The **image changes** (different visual moment), OR
- The **physical action genuinely changes** (e.g. "holding prop" → "pouring from prop" → "setting prop down" = three actions worth three scenes), OR
- The **setting/register changes** (HOOK → RECIPE, indoor → outdoor, etc.)

Do NOT trigger a new scene when:
- The voiceover switches to a new sentence but the visual and action don't change (e.g. a held pose carrying two voiceover beats while the speaker keeps talking to camera)
- A karaoke caption break occurs within one continuous on-screen action
- The speaker slightly shifts hand position or expression without changing what they're doing

**Bad pattern** (what NOT to do):
```
### Scene 11: image_9 "drop a comment and I will send you my protocol"
### Scene 12: image_9 "but you must be following me so I can reach you"
```
Same image, same held pose. These are TWO scenes doing the work of ONE.

**Good pattern**:
```
### Scene 7
- **image:** image_9
- **line:** drop a comment and I will send you my protocol
- **action_note:** Holds the book up at chest height...
- **line:** but you must be following me so I can reach you
- **action_note:** Same book-held pose, slight lean forward for emphasis...
```

### Line granularity within a scene — don't over-split short scenes (v577)

Companion rule to "Scene granularity" above. Same idea applied one level down: each `- **line:**` becomes ONE Veo clip = ONE 8-second generation. If a whole scene's dialogue fits inside one clip's word budget, it should be ONE line — not 2-4 short fragments.

**The word budget.** At natural clinical-authority pacing the math is straightforward: ~158 words/min = ~2.6 words/sec, so an 8-second clip with ~2 seconds of margin (opening pause + closing breath) holds **~21 words comfortably, ±2 words tolerance**. Past ~23 words the line either rushes (no breath room, reads as auctioneer) or runs over and Veo trims the tail.

**The rule.**

| Scene total dialogue | Number of lines |
|---|---|
| ≤ 21 words | ONE line |
| 22-23 words | ONE line (borderline — judgment call, default to one) |
| 24+ words | Split — see below |

When splitting is necessary (24+ words):
- Each split must land on a **natural syntactic boundary** — sentence end, clause end, comma, conjunction. Never break mid-phrase.
- Each resulting line should be **~10-21 words** and **syntactically complete** as spoken.
- Avoid lines under ~10 words unless they're a deliberate punch beat (CTA closer, hook turn). The decoder LLM should not produce 3-word fragments.

**Why this matters.**
- Each `- **line:**` triggers a separate Veo generation. 4 lines = 4× the Veo cost of 1 line.
- All lines in the same scene share the same start frame and same action_note image — splitting buys you nothing visually, just lip-sync re-cuts.
- Rapid re-cuts on a held pose feel jittery and inhuman. One smooth take of 21 words reads cleaner than four cuts of 5 words.
- Over-splitting bloats the `## Veo 3.1 Final Prompts (per clip)` section 4× without adding any prompt detail per clip.

**Bad pattern** (bladder-tea v1, 4 lines totaling 21 words — should be 1 line):
```
### Scene 1
- **image:** image_1
- **line:** if you pee when you laugh,
- **action_note:** [...]
- **line:** or feel the urge to go all the time,
- **action_note:** [...]
- **line:** or wake up at night to pee,
- **action_note:** [...]
- **line:** trust me, take this seriously,
- **action_note:** [...]
```
Same image, same held talking-head pose, 21 words total. Each fragment averages 5 words — well under the floor — and creates 4 Veo generations where 1 would do. Three lip-sync cuts within a static held shot read as broken pacing.

**Good pattern** (consolidated, 1 line, 21 words, 1 clip):
```
### Scene 1
- **image:** image_1
- **line:** if you pee when you laugh, or feel the urge to go all the time, or wake up at night to pee, trust me, take this seriously
- **action_note:** Main character delivers the symptom-stack hook talking-head, left hand drifts up into open-palm explanatory gesture over the first 2 seconds, mid-clip on "wake up at night to pee" the eyebrows lift in concerned emphasis, end beat hand drops to chest as "take this seriously" lands.
```
Same content, same start frame, one smooth 8-second take. Veo handles the comma pauses naturally. Action_note still has three motion beats — they just live inside one clip's timeline instead of being split across four.

**Acceptable split** (genuinely too long for one clip — total 35 words, splits cleanly at natural sentence boundary into 17+18):
```
### Scene 5
- **image:** image_5
- **line:** the secret is that your body holds onto fat differently after 40, and willpower alone won't move it without the right metabolic signal,
- **action_note:** [...]
- **line:** that's why these three ingredients work — they unlock the lipase enzymes your body already has but isn't using, the way they did when you were thirty.
- **action_note:** [...]
```
Two lines, each 17-18 words, each syntactically complete, splits on the natural "...won't move it without the right metabolic signal, / that's why these three ingredients work..." pivot. Both lines individually read fine as standalone clips.

**Anti-pattern variants** the decoder must also avoid:
- Caption-driven splits — splitting on every karaoke caption beat (each caption is 3-7 words; that is NOT a clip boundary, it's a graphic overlay)
- Punctuation-driven splits — splitting on every comma (commas are pause cues for Veo, not clip boundaries)
- Sentence-driven splits when sentences are short — two 8-word sentences in the same scene = one 16-word line, not two clips
- 1-2 word lines like "three days," "olive oil," "a glass of water" — these are caption fragments, not voiceover beats. Merge them into the surrounding full sentence.

**Decoder workflow.** When decoding a source video, count the words in each scene's dialogue. If ≤21 words, write ONE `- **line:**`. If >23 words, find the natural syntactic break and split, ensuring each part is ≥10 words. The action_note for a multi-beat single line should describe the gesture's three motion beats (start / mid-clip on specific words / end beat) referencing the spoken text — Veo syncs gestures to dialogue words inside one clip the same way it syncs across multiple clips.

### Image economy — merge phases of one action
If two phases of ONE physical action (e.g. "pre-pour setup" + "mid-pour") can be captured in a single image, use **one image** — typically the **mid-action frame**, since it naturally implies the starting state while showing the motion. The setup image is almost always redundant.

Examples of redundant setup images that should be dropped:
- "Coconut held above empty glass, about to pour" when the next image is "Coconut mid-pour streaming into glass" → keep only the pour
- "Kidney-with-stones held up + glass tipped ready" when the next image is "Pouring liquid over kidney, stones washing off" → keep only the mid-pour (which still shows remaining stones on one side)
- "Hands raised introducing ingredients" when the next image is "Pouring the first ingredient" → keep only the pour

Exception — keep both images when the setup image carries visual information the action image loses:
- An ingredient hero display that's the whole point of the beat (e.g., showcasing a branded product)
- A before/after pair where the "before" state IS the narrative hook (e.g., stones-covered kidney shown whole BEFORE any pour has started — but even here, the mid-pour usually captures enough of the "before" that the setup is unneeded)

**Practical check**: look at two adjacent images. If dropping the earlier one and letting the later one carry the full beat doesn't hurt the story, drop it. Re-anchor the next image's `reference_image` field accordingly.

### Recipe / process state-evolution — each step needs its own image (v580)

Companion rule to "Image economy" above. Image economy says **merge two phases of ONE action** into a single image (drop the setup, keep the mid-action). v580 says the OPPOSITE for multi-step processes: **DO NOT merge multiple distinct steps into a single image**. Each step that changes the visible state of a foreground prop needs its own start image showing the cumulative state at that step.

**Why this rule exists.**

In multi-step recipe / assembly / makeup / painting / care-routine scenes, the foreground prop physically evolves through the sequence — water → lemon-water → ginger-water → honey-water → saffron-water. If the decoder uses ONE image as the start frame for ALL N clips in the sequence (the v577 same-image-multi-line pattern), then by clip N the start frame shows the t=0 state (clean glass) while the dialogue is talking about adding ingredient N to a glass that should already contain ingredients 1 through N-1. Veo will either (a) ignore the dialogue and animate the action against the clean glass anyway, producing a video where the recipe never actually progresses visually, or (b) try to compensate by hallucinating prior ingredients into the glass mid-clip, producing an inconsistent state pop. Neither is acceptable.

The fix: each prep step gets its own explicit start image whose state reflects the cumulative result of all prior steps. The clip then animates step N specifically, with Veo honoring the explicit start frame.

**The rule.**

When a scene depicts a multi-step process where each step changes the visible state of a foreground prop, EACH STEP gets:
1. Its own `### Image N` block
2. A start frame whose state shows the cumulative result of all prior steps + the current step's ingredient/tool already in position (mid-action moment of the current step)
3. A `reference_image` that points to the immediately prior step's image — Nano Banana 2 generates each successive image as a state-evolution from the prior, preserving the prop position, lighting, persona pose, counter layout, and background continuity
4. Its own `### Scene N.M` storyboard block (sub-scene numbering preserves the conceptual grouping — RECIPE step 1, step 2, step 3, etc. — while each sub-scene gets its own image reference)
5. `clip_mode: fresh` because the start frame is an explicit new image, not a continuation of the prior clip's end frame; Veo honors the new start frame
6. `transition: cut` between steps because the foreground prop state has visibly changed (new ingredient is now in the glass) — the cut lets Veo treat the transition as a hard prop-state change, not a continuous motion
7. The action_note describes the COMPLETION of step N's action over the 8 seconds — given the start frame already shows the ingredient mid-action (e.g. lemon mid-squeeze, honey strand mid-pour), the clip animates the rest of the action through to the end-state where the ingredient is fully incorporated into the glass

**Image content per step (canonical recipe pattern, 5-step build).**

| Step image | Glass state at t=0 | Ingredient state at t=0 | Persona pose at t=0 |
|---|---|---|---|
| Step 1 (water base) | clean steaming warm water | none in hand | hands hovering above glass, ready |
| Step 2 (lemon) | clean warm water (water still mostly clear, first drops of juice JUST falling) | half-lemon mid-squeeze in right hand above glass | gripping lemon, fingers tightening |
| Step 3 (ginger after lemon) | yellow lemon-tinted water | pinch of grated ginger between thumb and forefinger above glass | right hand pinched ginger over glass, about to release |
| Step 4 (honey after lemon+ginger) | yellow ginger water with tiny ginger flecks visible | wooden honey dipper above glass with golden honey strand mid-pour, glistening amber strand falling | right hand holding honey dipper tilted over glass |
| Step 5 (saffron after lemon+ginger+honey) | rich amber honey-water (uniform amber, ginger flecks lightly settled) | open two-piece saffron capsule held above glass mid-pour with deep-crimson saffron powder cascading down, red-orange streaks instantly bleeding through the amber liquid as it hits | both hands holding the open capsule halves over glass |

Each successive image's `reference_image` points to the prior step's image — so the persona's pose, the glass position, the counter layout, and the lighting all stay locked while only the cumulative ingredient state evolves.

**Reference chain for the 5-step recipe pattern (assuming Image 1 is the kitchen HOOK before recipe):**
- Image 2 (water) → reference_image: image_1 (kitchen continuity, glass replaces hook prop)
- Image 3 (lemon) → reference_image: image_2 (state evolution from water)
- Image 4 (ginger) → reference_image: image_3 (state evolution from lemon)
- Image 5 (honey) → reference_image: image_4 (state evolution from ginger)
- Image 6 (saffron) → reference_image: image_5 (state evolution from honey)

**clip_mode and transition for state-evolution sub-scenes.**

| Field | Value | Why |
|---|---|---|
| `clip_mode` | `fresh` | Each step has an explicit new start frame; Veo must honor it, not continue from prior clip's end |
| `transition` | `cut` | The prop state has visibly changed (new ingredient now visible); cut lets Veo treat this as a hard state shift not continuous motion |

This is the inverse of the v577 same-image-multi-line pattern (continue + null). The two rules cover different scenarios: same-image-multi-line is for held-pose talking-head scenes where only the persona's gesture varies (CTA stack, authority explanation); per-step-image is for state-evolution scenes where the prop physically changes.

**When v580 applies:**
- Cooking recipes (drinks, meals, smoothies, teas — anything with sequential ingredient adds)
- Skincare routines (cleanser → toner → serum → moisturizer)
- Makeup tutorials (foundation → contour → blush → highlight → setting spray)
- Crafting (paint base → mid-coat → top-coat; sketch → ink → color → shading)
- Mechanical assembly (component A → mounted on B → tightened with C → tested)
- Plant care multi-step (water → fertilizer → prune → support stake)
- Any process where the viewer must SEE the visible state of the central prop evolve through the steps

**When v580 does NOT apply:**
- Held-pose talking-head scenes (authority explanation, CTA delivery, hook talking-head) — gesture variation across clips is handled within action_notes, single image is correct
- B-roll cutaway scenes where the prop state is incidental (decorative pour, artistic montage)
- Scenes where the persona's GESTURE changes but the prop does not (e.g., persona holds the same bottle through 4 CTA clips, gesturing differently each clip — this is v577 territory, ONE image with 4 action_notes is correct)

**Practical decoder workflow.**

When the v579 pipeline output identifies a multi-step prep sequence in the source video (whisper segments contain ingredient-adding language: "start with...", "squeeze in...", "add a teaspoon of...", "mix in...", "now take..."), check whether each segment changes the state of a single tracked prop. If yes, that's a v580 sequence — each segment becomes its own image + its own sub-scene. If no (e.g., the persona is just gesturing while talking about why saffron is good), v577 same-image-multi-line is correct.

A typical 5-ingredient recipe sequence under v580 will use 5 images and 5 sub-scenes. Image generation cost is 5× the v577 single-image equivalent, but Veo runtime cost is unchanged (still 5 clips), and the resulting videos visibly show the prep evolving — which is the entire point of a recipe video. The cost is worth it for any prep sequence that is itself a hero beat in the video.

### HOOK action intensity — violence first, persona second
HOOK scenes live or die in the first 1.5 seconds. The viewer is scrolling and the image must do visual work before they read the caption or hear the voice. The persona's body language is **secondary**; what matters is **what's happening to the prop** — and in HOOK demonstration scenes, the prop must be visibly UNDER FORCE.

The trap most prompts fall into: writing the persona's pose with care ("hand mid-LOWER toward the mouth", "expression mid-word with eyebrows raised") while describing the prop in clinical, gentle language ("stones tumbling out, rolling toward the lip"). That's gravity, not pressure. Gravity reads as a science exhibit. Pressure reads as a hook.

**The HOOK action calibration test** — for any HOOK demonstration where a force is applied to a prop, ask: *if I deleted every word about the persona, would the prop description ALONE make a viewer stop scrolling?* If no, the prop description is too tame.

For force-driven demonstrations (water flosser, pressure wash, pour-over-stones, anything-meets-cleansing-liquid), use **violent ejection language**:

✅ "Stones ERUPT outward in violent arcs from the mouth, mid-flight motion blur on individual stones, several launching toward camera, water atomizing into mist on impact, droplets ricocheting off the retractor, debris scattering across the workbench surface in real time"

❌ "Stones tumbling out, several rolling onto the plastic tongue toward the lower lip, falling away in real time"

Specific calibration vocabulary that pushes intensity up:
- **Verbs of force**: erupt, blast, eject, launch, ricochet, scatter, spray-blast, expel, fling, propel
- **Trajectory specifics**: "in violent arcs", "outward toward camera", "across a 12-inch radius", "in multiple directions", "tracking diagonal paths"
- **Mid-flight markers**: "captured at the peak of ejection", "individual stones frozen mid-air", "motion blur trailing behind each stone", "water droplets atomizing in the strobe of HDR daylight"
- **Impact secondaries**: "debris ricocheting off [retractor/glass/counter]", "splash splatter on the workbench surface", "fine mist visible against the dark background"
- **Volume markers**: "a dense cloud of dislodged stones", "5-7 stones simultaneously airborne", "continuous ejection stream, not a single piece"

**Re-balance the action_note structure for HOOK violence beats**: instead of three persona beats (lift → glide → settle), write three PROP beats with the persona's role compressed into a single phrase:

```
- **action_note:** [Prop violence opening]: stones ERUPT from the mouth in
  violent outward arcs, 5-7 visible mid-flight, water atomizing into mist on
  impact with the retractor. [Persona compressed]: trigger held down, head
  tilted forward over the chaos, eyes wide with proud emphasis locked to
  camera. [Prop violence climax]: a final cluster of stones BLASTS free in
  one peak ejection as the line lands, debris visibly ricocheting off the
  workbench surface.
```

Note the persona block is one sentence, not the bulk. The viewer's eye tracks the chaos.

This rule applies specifically to:
- Water-flosser, pressure-wash, jet-spray demonstrations
- Pour-over-impurity reveal beats (liquid pouring over stones/grime/buildup that visibly washes off violently)
- "Smash"-style HOOK beats (object collision, splatter, breakage)
- Any HOOK where the visual story is "force applied → debris ejected"

It does NOT apply to:
- Calm-demonstration RECIPE pours (chamomile tea into a glass) — gentle is correct there
- OUTRO talking-head CTAs — persona action carries the beat

For HOOK before-state shots where the script is showing the patient's defeat/exhaustion, see the next section — those scenes need their OWN form of weird action.

### HOOK weird-action requirement (v539) — every HOOK needs a concrete physical event

The "violence first" rule above isn't enough on its own. Most HOOK before-states default to **emotional/posture beats** — patient slumped, eyes downcast, sighing into hands, slow chewing, dejected look at a scale. Those are character beats, not visual events. The viewer's eye doesn't pin to a slumped shoulder; it pins to a prop being **smashed, thrown, dunked, snapped, slammed, ripped, or otherwise weird-actioned**.

The rule: **every HOOK scene that opens cold MUST contain a concrete weird action performed on a physical prop**. Not a feeling, not a posture, not a glance — a verb, a force, a thing happening to a thing that wouldn't normally happen.

Concrete weird-action vocabulary:
- **Smash** — banana flattened against the exam table, fast food bag slammed onto the counter, supplement bottle stomped flat
- **Throw / fling / hurl** — banana hurled at the wall, supplement bottle tossed into a trash can on screen, before-photo crumpled and chucked
- **Snap / break** — pencil snapped in half on "no progress", measuring tape snapped, supplement capsule split open and powder spilled
- **Tear / rip** — diet plan ripped in half, before-photo torn up, gym membership card ripped
- **Dunk / submerge** — banana dunked into a glass of water that visibly recoils, before-photo dunked into a sink
- **Pour / dump on** — protein shake dumped onto the scale, kale dumped over a fast-food bag, water poured over a stack of supplement bottles
- **Stomp / crush** — fast food bag crushed under a foot, supplement bottle crushed flat in a fist
- **Sweep / swipe** — a row of supplement bottles swept off the desk in one swipe, fragments and pills scattering
- **Cut / slice** — a measuring tape cut in half with scissors, a fast food cup sliced open and contents spill out
- **Squash / mash** — small banana squeezed in a fist until pulp drips through fingers
- **Drop / shatter** — a glass jar of "fad supplements" dropped onto the counter, fragments and pills scattering

The action must:
1. **Use a verb of force** — smash, throw, snap, dunk, pour, stomp, hurl, slice, squash, drop, fling, slam, tear, sweep
2. **Have a visible prop consequence** — the banana SPLITS open, the photo is RIPPED, the supplement bottles SCATTER. Veo needs to see the consequence in the frame, not just the action.
3. **Be physically possible in the persona's setting** — kitchen → smash, dump, slice; gym → throw, slam; consultation room → rip, snap, swipe-off-desk; airport → throw photo
4. **Be appropriate to the script's tone** — the persona is angry/frustrated/disapproving in the HOOK, not playful. The weird action expresses that.

For scripts with multi-beat HOOKs (3+ before-state scenes), at least ONE of the scenes must contain the weird action. The others can be character beats that build to it. But the SMASH/THROW scene typically lands at scene 2 of a 3-scene HOOK — scene 1 sets up the problem character, scene 2 does the weird action that punctuates the frustration, scene 3 (often the after-state) shows the resolution.

For scripts with secondary-character before-states (patient on a scale, daughter on stairs, customer at car door), the weird action can be performed by either:
- **The persona** (off-screen, hand reaches in from frame edge to do the smash/throw — voice narrates over it). Best for clinic/consultation settings where the persona is establishing authority.
- **The patient themselves** (frustrated patient slams the fast food bag, throws the scale, hurls the gym membership). Best when the visual story is "this patient was at their breaking point".

Anti-patterns this rule replaces:
- ❌ "Patient slumps into hands and exhales heavily" — that's a posture, not a weird action
- ❌ "Patient takes a slow heavy bite from the sandwich" — that's chewing, not weird-action chewing
- ❌ "Patient looks down at the scale and shakes his head" — that's defeat, not weird-action defeat
- ❌ "Persona holds the small banana frustrated" — that's holding, not weird-action holding
- ✅ "Patient SLAMS the fast food bag down on the desk, contents bursting out across the surface" — weird-action defeat
- ✅ "Persona SMASHES the small banana flat against the exam table, peel splitting open, fragments scattering across the white paper" — weird-action frustration
- ✅ "Patient HURLS the bathroom scale into the wall, scale spinning across the floor with the readout still flashing" — weird-action defeat
- ✅ "Off-screen hand SWEEPS the row of supplement bottles off the desk, bottles tumbling and scattering across the floor" — weird-action authority dismissal

Combined with the existing rules:
- **`speaker: voiceover`** stays correct for these — the persona's narration plays over the silent visible weird action. The patient/persona doing the weird action doesn't speak; the off-screen narrator delivers the line.
- **Action_note structure** still uses 3-beat motion, but the beats describe the **prop's destruction trajectory**: setup-grip → impact-moment → debris-aftermath.
- **The persona is compressed** to a single phrase ("expression hardens into frustration", "head shakes once in disgust") — most of the action_note budget goes to the prop's behavior under force.

### Prop-as-subject vs persona-as-subject — when to flip the priority
A scene's "subject" is whatever the viewer's eye should track. Default Kaveno scenes are persona-as-subject: the persona is in selfie composition, foregrounded, doing all the action. The prop description is brief because the viewer is reading the persona's face.

Some scenes invert that — the **prop is the subject** and the persona is incidental:
- Force-driven HOOK demonstrations (see HOOK action intensity above)
- Hand-only product close-ups (only a hand and a bottle, no face — the bottle IS the subject)
- ANATOMY overlays (the semi-transparent body figure is the subject; persona may be entirely off-frame)
- Before/after secondary-character reveals (the patient/daughter/customer is the subject; persona narrates from off-screen)

When writing prop-as-subject scenes:
1. **Flip the prompt's word budget**: spend 60-70% of the prompt on the prop's state, motion, and behavior. Spend 20-30% on the persona's compressed pose. The remainder on framing/setting.
2. **Open with the prop**, not the persona. "The mannequin head dominates the frame; from its open mouth, stones erupt..." reads better than "The main character leans in from the right while in front of him stones erupt..."
3. **Set `speaker: voiceover`** (v537) for hand-only and ANATOMY scenes — the persona isn't the visible speaker. For force-HOOK scenes where the persona IS in frame addressing camera while operating the device, keep `speaker: on-camera`.
4. **The action_note follows the same priority flip** — lead with what the prop is doing, compress the persona's beat into a single subordinate clause.

This is the explicit version of a rule that's been implicit. When in doubt: ask "what's the viewer actually watching here?" If the answer is "the persona's face" → persona-as-subject (default). If the answer is "the thing the persona is doing TO something else" → prop-as-subject (flip the priority).

### Outfit-change rule on time-jump cuts (v541) — Day 1 vs Day 14 must look different
Whenever a video has a Day 1 → Day 14 (or any other before/after time-jump) transformation cut on the same secondary character in the same setting, the patient's **outfit must change** between the two timestamps. This is a hard rule, not a stylistic preference.

**Why.** When the only thing that changes between two clips is the body itself (back smooth, belly flat, hair restored, etc.), the brain reads it as a render trick — "they just photoshopped the same image twice." When the outfit also changes, the brain reads it as a chronological skip — "she changed clothes, two weeks went by, this is what happened in those two weeks." This is the same psychological mechanism behind every legitimate before/after photo on social media: the subject wears different clothes in the after shot. Same-outfit before/after is the signature of fake before/afters; different-outfit before/after carries the implicit credibility marker of real time having passed.

**How to apply.**
1. **Day 1 image**: describe the outfit fully in the image prompt as part of the patient description. Same as before.
2. **Day 14 image**: describe a **different** outfit explicitly in that image's prompt. Use a paragraph titled `OUTFIT CHANGE — DAY 14:` so the change is obvious to anyone reading the prompt and to Veo when it processes the prompt text.
3. The change must be visually obvious **in the framed area**. Don't change the patient's pants if the camera only sees the upper body — change the shirt instead. Match the change to what's in frame.
4. **Body parts that aren't the visual subject of the transformation can keep their clothes.** In a back-area transformation, the leggings stay (legs not compared) and the bra changes (back is in frame). In a belly transformation, the patient is shirtless on both days (belly is the subject) and the *pants* change. In a hair transformation, the *shirt* changes (the shirt is in frame at the bottom of the high-angle shot) — pants don't matter because they're not in frame.
5. **Use explicit "different from image 1's X" language** in the Day 14 prompt so Veo doesn't accidentally inherit the Day 1 outfit through the reference_image chain. Reference images carry persona; the outfit needs to be explicitly broken in the prompt text.

**Example outfit changes from the Dr. Kim transformation series.**

| Video | Day 1 outfit | Day 14 outfit |
|---|---|---|
| Back-lump (female) | Nude-tone bra (back clasp visible) + black leggings | BLACK racer-back sports bra + same black leggings |
| Belly-burn (male, front-facing) | Shirtless + dark grey gym shorts | Shirtless + LIGHT GREY heather joggers (full-length, drawstring) |
| Hair-regrowth (male, head-bowed) | Faded grey "I ❤️ COSTCO" t-shirt | Plain SOLID NAVY crew-neck t-shirt |

**What NOT to do.**
- Don't change the outfit so dramatically the patient becomes unrecognizable (different style, completely different silhouette). Keep silhouette and proportions consistent so the persona-locking via reference_image still reads.
- Don't change persona-locking accessories (e.g., a bracelet established as an identity marker for the patient). Change the shirt instead.
- Don't over-narrate the outfit change in the script. The script focuses on the BODY transformation; the outfit speaks for itself visually.
- Don't apply this rule when the SETTING also changes (e.g., clinic → outdoor). Setting-change already signals the time-jump; outfit-change on top would be redundant. The rule is specifically for **same-setting** time-jumps.

**Implementation pattern for the Day 14 image prompt:**

```
CRITICAL VISUAL CHANGE — TIME JUMP TO DAY 14: [body transformation description]

OUTFIT CHANGE — DAY 14: [new outfit description, with explicit "different from image 1's [old outfit]" reference]

Same person, same [persona-locking features], same [pose], — but [body transformation summary] AND [outfit change summary].
```

**Related rules.** The v539 weird-action rule (in-clip prop-collapse) already provides editorial weird-action via the Day 1 → Day 14 cut itself; the outfit-change rule reinforces that the cut is intentional and substantive (rather than a rendering glitch). Persona reference_image chaining still locks face/hair/identity; outfit-change is described inline per-image to deliberately break visual sameness while keeping identity intact.

### Isolated-reference rule (v573, supersedes v523.1) — persona AND product are the sanctioned uploads; everything else stays inline
Nano Banana 2 documentation says it accepts up to 14 reference object images and 5 character sheets per generation. That's the model's capacity. Our practical constraint is different: **a reference image only works as an ingredient if it's a clean isolated photo of just that one object on a neutral background.** When the prop's only available image is its anchor scene (a busy composition with other objects, walls, floors, lighting), uploading that as the prop's "ingredient" would make Nano Banana 2 try to reproduce the entire busy composition every time the prop is referenced — including the wall, the floor, the neighboring jars, the lighting, and so on. The result is worse than describing the prop in prompt text from scratch.

**The isolated-reference test.** Before declaring anything in the `## Ingredients` table, ask: *"Do I have a clean isolated photo of just this one object on a neutral background, with no surrounding scene?"* If yes, it can be an ingredient. If no, describe it inline in the image prompt text instead.

**Two things now pass the test in practice (v573 update):**

1. **The persona** — built specifically as a clean character sheet (front-facing, neutral background, signature outfit) for exactly this purpose. Always present, always slot 0 / Flow's "Image 1".
2. **A branded product** — when the video has a real branded supplement, food, or consumer product whose label legibility matters AND a clean product-hero photo of just that bottle/box on a neutral background is available. Slot 1 / Flow's "Image 2". This was previously bootstrapped from inline text descriptions (the v523.1 pattern); v573 promotes it to a full clean upload, parallel to the persona, when a hero shot exists.

**Everything else still fails the test.** The mannequin head, the kettle, the half-lemon, the surgical pen, the onion half, generic glassware, anatomy props — only exist as part of their anchor scenes. Uploading the anchor scene as the "kettle" reference uploads the kettle PLUS the workbench it sat on, PLUS the wall behind it. Nano Banana 2 can't isolate the kettle from that context; it copies all of it. These stay inline.

**Why this beats "more references = more control."** It's tempting to think that more reference images give the model more control, but the rule is built on the inverse: control comes from **clean** references, not from many references. One clean persona reference + one clean product reference + detailed prompt text outperforms more anchor-scene-as-prop references, because the latter introduces visual noise the model can't filter.

**Practical pattern.**
- The `## Ingredients` table contains TWO rows when a product is present: persona + product. ONE row when there's no product (persona only).
- Both uploaded ingredients are referenced **by name** in image prompt bodies — `the main character`, `the [brand] bottle`. The platform binds named ingredients to their respective upload slots (persona → slot 0, product → slot 1) via the same name-matching logic for both.
- Every other prop is described inline in the image prompts where it appears. First appearance gets a full description with enough specificity for the model to render it consistently from text (e.g., *"a hammered copper kettle with a curved gooseneck spout, tarnish patina at the rim, sitting on the upper-left of the workbench"*).
- Subsequent images in the same register can refer back: *"the same hammered copper kettle as image 3"*. The `reference_image` chain inherits the kettle's specific look visually without needing to re-describe.
- A `**Note on stripped-down ingredients (v573):**` paragraph below the Ingredients table explicitly lists what was considered and rejected as ingredients, so future readers understand the decision. Format: *"Only the persona and the product are declared as uploaded ingredients. The [list of props] are all described inline in scene prompts — none would pass the isolated-reference test."*

**The exception to the exception.** If a video has multiple branded products (e.g. a competitor-comparison video showing five bottles), only one slot is available — pick the hero/featured product for the upload, describe the other four inline. The platform currently caps at three parents per scene total (persona + product + chain), so a multi-product layout is constrained anyway.

**Pre-v573 decoded files** (the salvora_*, circulation_* set in the project) were authored under v523.1 with products bootstrapped from inline descriptions. Those files keep working — the decoder reads inline product text and Nano Banana 2 renders the label from text, which is less reliable than an upload but still functional. Only NEW decodes use the v573 two-upload pattern.

### Product frames — when to bind the product upload (v573)

The product upload (slot 1, Flow's "Image 2") is bound to a scene by mentioning the product BY NAME in that scene's image prompt body. Same name-binding logic that already attaches the persona to scenes via "the main character." The decision is per-scene, not per-video: most videos have product frames AND non-product frames, and the product upload should only be bound to the former.

**Definition of a product frame.** A scene where the actual packaging needs to render correctly — where label typography, brand color, container shape, and proportions matter to the composition. Concretely:

- **Hero close-ups**: product fills the upper two-thirds of frame, label rotated forward to camera, hand visible gripping it
- **Label-visible holds**: persona holds the product at chest height, label squared to lens, often during a "this is what works" beat
- **Mid-pour shots**: bottle tilted with contents pouring out, but the bottle itself is in frame and identifiable
- **End-card reveals**: OUTRO scene where the product is presented as the call-to-action anchor (alone on a counter, in the persona's hand, foregrounded against a clean background)
- **Side-by-side comparisons**: persona holding the product next to a competitor while explaining the difference
- **Receipt/packaging shots**: product visible in a shopping cart, on a counter as the bag is unpacked, etc.

**What is NOT a product frame.** Anywhere the product isn't the visual focus or doesn't need to be label-readable:

- **HOOK before-states with no product on screen** (patient on a scale, daughter on a couch, customer in a driveway) — the product hasn't appeared yet
- **ANATOMY semi-transparent overlays** (kidney models, vein diagrams) — product not in frame
- **Talking-head closeups with the product offscreen** — persona explaining a problem before introducing the product
- **Background visibility only** — the product is on a shelf in the background, blurred, not the focus. Don't bind the upload here; the chain inheritance plus a brief "shelves of supplement bottles in the background" handles it
- **Persona-only OUTRO without the product** — direct-to-camera CTA, no product in hand

**Chain inheritance — don't re-bind unnecessarily.** If scene 5 has `reference_image: image_4` and scene 4 had the product locked in correctly via the upload, the chain from image 4 carries the product visually into image 5. Re-mentioning the product by name in scene 5's prompt would re-bind the upload — wasting a slot and potentially producing geometry artifacts in compositions where the product shouldn't dominate.

**The decision flow.**
1. Is the product label-readable in this frame of the source video? → product frame
2. Is the product on screen but not label-focused (background, blurred, partial)? → not a product frame; the chain handles continuity
3. Is the product offscreen or not in this scene at all? → not a product frame; do not mention the product
4. If product frame AND the previous scene already locked the product in via the upload AND this scene's `reference_image` chains from there AND the product hasn't moved or rotated → still NOT a re-bind; let the chain carry it

**Body convention.** Refer to the product by descriptive name — `the Salvora Rhodiola Rosea bottle`, `the Yogi DeTox tea box`, `the [brand] gummies bottle`. The platform binds the name to the product upload at slot 1. This parallels the persona's `the main character` binding to slot 0. Do NOT use positional references like "matching image 2's label" in body text — "image 2" is the Flow slot used in the manifest header (the platform-side prepend), not a body-text convention. The markdown's own `### Image 2` is a separate generated scene image, and overlapping the two would confuse the body-renumber pass.

**Action_note rule for product scenes.** Action_notes describe motion only — Veo can SEE the start frame so the product's appearance is already locked in. Refer to the product in action_notes by short generic name (`the bottle`, `the box`, `the product`) and describe how it MOVES across the 8 seconds: held up, tilted, poured, set down, rotated. Never re-describe the label, color, typography, or proportions in the action_note — that's image-prompt territory, not Veo territory.

**Multi-product videos.** If a video features multiple branded products (a Costco-aisle survey, a competitor comparison), only ONE product gets the upload slot — pick the hero/featured product (typically the brand the video is selling). Describe the others inline with full label specs. The platform's three-parent cap (persona + product + chain) makes anything more complex fail import anyway.

### Action_note discipline (v528 + v540): write for Veo, not for the script reader

The action_note is what the video pipeline sends to Veo as the text prompt for that clip. Veo does NOT see the script you wrote, the visual register, the rhythm tier, or any other scene metadata. It sees:
1. **The start frame image** (rendered earlier from the image prompt by Nano Banana 2)
2. **The voiceover line** (in a "[Speaker] says: 'X'" block built by the prompt-builder)
3. **The action_note text verbatim** (as the motion brief)

That's it. Eight seconds of video gets generated from those three inputs. Everything you've written elsewhere in the markdown is invisible to Veo.

#### Veo start-frame rule (v540) — describe the motion, NOT the start state
The start frame already shows Veo the starting composition: where the subject sits, what they hold, how the room looks, what their expression is at t=0. **Re-stating that information in the action_note wastes prompt budget and can actively confuse the model** — it competes with what Veo already sees in the image, and Veo may try to reconcile contradictions instead of just animating from the frame.

Per the official Veo prompting guidance: the prompt should not waste words describing the empty room (start frame) or the decorated room (end frame); its entire focus should be on detailing the transformation process that connects them. This guidance is specifically for first-and-last-frame mode but applies just as strongly to single-start-frame mode — Veo can SEE the starting state.

**The action_note should describe what changes during the 8 seconds, not what's already visible at t=0.**

❌ Bad (static-state restatement, what Veo already sees):
> "Patient stands at the foot of the exam table holding the small banana horizontally with the yellow tape measure draped alongside it, her male patient's legs visible in the foreground."

✅ Good (motion-only, describes the 8 seconds):
> "Patient's fingers tighten on the banana over the first 2 seconds, knuckles whitening. Mid-clip she SQUEEZES — peel splits between her knuckles, flesh oozes through her fingers, drips toward the exam paper. End beat: she opens her hand, palm up, revealing the squashed pulp as the line lands."

The bad version answers "what does the frame look like?" (Veo can see). The good version answers "what happens during the 8 seconds?" (the only thing Veo needs from text).

You can mention props by name to anchor what Veo should focus on (the banana, the chrome flosser, the seat-back) but never describe their state at t=0 — only describe how they change.

#### Single-clip independence rule (v540) — never reference other scenes
Each Veo generation is its own sealed unit. Veo does NOT see the previous clip you generated. It does NOT know what "image 2" looked like. It does NOT remember the kitchen from scene 4. **Every action_note must stand alone.**

❌ Bad (assumes Veo remembers prior clips):
- "Same composition as image 4 — jar now half-full with amber mixed liquid."
- "Continuation on image 2 — the tape-pull resolves."
- "Same room, same camera, same patient legs in foreground."
- "Same composition holds through the close."

✅ Good (self-contained, describes only this clip's motion):
- "Hand pours honey from the wooden dipper, golden ribbon falling steadily for 4 seconds. Mid-clip the empty dipper taps against the jar rim. End beat: eyes lock to camera with a satisfied 'this is the secret' smile."
- "Hands release the tape ends, gaze snaps to camera with proud authority. Mid-clip the patient nods beside her in validating endorsement. End beat: her hand drops to her hip as the line lands."

Cross-clip references like "same composition", "continuation on image N", "same kitchen as image M" should NEVER appear in an action_note. Even when they would make sense to a human reader, they're invisible context to Veo and burn budget that should describe the actual motion.

The reference_image chain on Image N+1 (in the image prompt section) handles visual continuity for image generation — that's where Nano Banana 2 looks for "same setting as image M". The action_note operates downstream of that and is purely about motion-during-this-clip.

#### What the action_note SHOULD contain (v540)
1. **Motion across time** — describe what changes between t=0 and t=8s. Use beat markers ("over the first 2 seconds", "mid-clip", "end beat") to anchor the timing.
2. **Voiceover-line anchors** — connect motion to specific spoken words ("on 'comment salvora'", "as the line lands", "when 'nothing worked' lands") so Veo syncs the gesture to the dialogue.
3. **Expression evolution** — how the face/body changes during the clip ("eyebrows lift on emphasis", "smile broadens at the close", "rage drains into defeat").
4. **Concrete verbs of force/motion** — squeezes, slams, hurls, pours, tilts, sweeps, lifts, drops, rotates, pivots, snaps, taps, presses, glides, turns, opens, closes.
5. **Three-beat structure** — start beat (first 2-3s) → middle beat (3-5s) → end beat (5-8s). Each beat has its own concrete physical event.

The persona's identity, outfit, setting, and starting pose all come from the start frame. The action_note just says what they DO.

#### Practical action_note skeleton (v540)
```
[Start beat — 0-3s motion that initiates the action.] [Middle beat — 3-5s
the main physical event, anchored to a voiceover phrase.] [End beat — 5-8s
the resolution/aftermath, anchored to the line ending.]
```

Example (good):
> "Right-hand fingers tighten on the small banana over the first 2 seconds, knuckles whitening. Mid-clip on 'stuck with this for years' she SQUEEZES — peel splits between her knuckles, pale flesh oozes out and drips toward the exam paper. End beat: hand opens to reveal squashed pulp on her palm, free left hand sweeps outward toward camera as the line lands."

Note: no setting description, no outfit description, no "same composition", no static positioning. Just the 8-second motion.

### Reference chaining strategy
The `reference_image` field is for scene-to-scene visual parent (setting/pose/action continuity). The persona's character reference is passed separately — so `reference_image` is NOT where character identity comes from.

Default chaining pattern:
- **Image 1**: `reference_image: none`. Canonical anchor — establishes the opening HOOK setting and any secondary characters.
- **HOOK continuation**: chain sequentially (`image 2 → image 1`) so pose or action progression inherits tightly.
- **Register change** (HOOK → RECIPE → OUTRO): point back to image 1 (or a relevant earlier anchor) for subject lock, then describe the new setting fully in the text prompt.
- **Sequential action within one register** (multi-step RECIPE pours, dialogue beats in one setting): chain sequentially so each frame inherits the evolving state (e.g. bowl contents stratifying as ingredients are added).
- **OUTRO clean-table shots**: point to the RECIPE establishing image (not the final pour image) so the bench composition inherits without pour-state clutter.

### Explicit reference bindings — author-controlled, written into the markdown (v581, supersedes v552 + v573 manifest auto-prepend)

Flow's Nano Banana 2 indexes uploaded references positionally — slot 0 is "Image 1", slot 1 is "Image 2", etc. — and does NOT auto-figure-out what each ref is for. Per the published Nano Banana 2 / Flow prompt guides (Leonardo.ai, LTX Studio, Google DEV docs), each reference NEEDS an explicit role assignment in the prompt — *"Use Image 1 for the person's face. Use Image 2 for the pose."* — otherwise Flow blends all attached references as generic visual context.

**v581 changes the contract.** Instead of the platform invisibly auto-prepending these role-assignment lines at job emission (the v552/v573 behavior), the markdown author writes the bindings explicitly at the top of the image prompt body. This is the single source of truth for what each upload is for in each scene; the platform stops prepending and only does targeted slot substitution at emission.

**Why this changed.**

1. **Visibility.** Pre-v581, the binding was invisible in the markdown — the actual prompt sent to Banana 2 only existed in the platform's emission code, and authors couldn't preview or audit it without reading source. Hidden behavior is hard to debug and hard to teach.
2. **Author control.** Some scenes need different binding emphasis — a product-hero close-up wants the product line first; a persona-emoting scene wants the persona line first. Auto-prepended manifests had a fixed order. Explicit bindings let the author reorder and reword per-scene.
3. **One source of truth.** Pre-v581, the binding signal was distributed across the Ingredients table type column + name-match heuristics in prompts + the platform's `_build_per_slot_manifest()` function. Three places to maintain consistency. With v581, the binding is in one place (the prompt body) and a parallel structural hint (`product_image:` field) for parser convenience.

**The explicit-binding wording template.**

The first lines of every `Image prompt:` fenced block — before the visual description — declare which uploads bind and what role they serve. Three line types, applied in order:

1. **Persona binding (always present, every image):**
   ```
   Use the uploaded character reference image for the main character — match her facial features, identity, hair, and clothing exactly.
   ```
   Replace "the main character" with whatever persona alias your Ingredients table uses. Replace "her" with the appropriate pronoun for the persona.

2. **Product binding (only when the image binds the product upload — i.e. when the `product_image:` field is set on this image):**
   ```
   Use the uploaded product reference image for the [product ingredient name] — match its label, packaging, color, and proportions exactly.
   ```
   Replace `[product ingredient name]` with the verbatim ingredient name from the table — e.g. `the Corella saffron bottle`, `the Salvora bottle`, `the Karela saffron bottle`.

3. **Chain reference binding (only when `reference_image:` is not `none`):**
   ```
   Use Image N as the visual reference for the previous scene — preserve the [setting], [lighting cue], [anchor props], and [continuity element] from there.
   ```
   Replace `N` with the chain target's image number. Replace the bracketed elements with whatever continuity matters for this image — e.g. *"preserve the kitchen setting, marble counter, fiddle leaf, and morning daylight from there"*.

After these binding lines (one blank line separator), the visual description starts as before. Banana 2 reads the binding lines as instructions and the description as content.

**The structural hint field — `product_image:` (v581).**

Parallel to the existing `reference_image:` field, the `product_image:` field declares the product binding at the top-level metadata. It's optional and only present on images that bind the product upload. Value is the product ingredient name verbatim from the table.

```
### Image 8
- **reference_image:** image_7
- **product_image:** the Corella saffron bottle
- **Image prompt:**
` ` `
[explicit bindings + description]
` ` `
```

The `product_image:` field gives:
- A clear at-a-glance signal to the human author / reviewer that this image binds the product
- A machine-readable hint for the parser (more reliable than scanning the prompt body for ingredient name matches)
- A consistent place to encode the binding regardless of how the in-prompt wording varies

Persona binding does NOT get a parallel `character_image:` field because every image binds the persona — declaring it on every row is noise. Persona is implicit / always-on; product is conditional / declared explicitly.

**Slot resolution at emission.**

The platform still resolves "the uploaded character reference image" / "the uploaded product reference image" to Flow's actual slot positions at emission time, but via targeted string substitution rather than manifest prepend:

- `the uploaded character reference image` → `Image 1` (Flow slot 0)
- `the uploaded product reference image` → `Image 2` (Flow slot 1, when bound)

Slot ordering is unchanged from v573:
- **Slot 0 (Flow's Image 1)** — persona upload, every scene
- **Slot 1 (Flow's Image 2)** — product upload, scenes with `product_image:` field set
- **Slot 2+ (Flow's Image 3+)** — chain reference + other named ingredient anchors

The chain reference still uses positional `Image N` references in the prompt body. The platform rewrites these at emission to Flow's actual slot — e.g. *"Use Image 7 as the visual reference"* in the markdown becomes *"Use Image 3 as the visual reference"* at emission if Image 7 ends up at Flow slot 2 (because slots 0 and 1 are persona + product).

**Veo prompts — bindings are NOT included.**

Veo prompts mention the persona / product by name only when the action operates on them (*"left hand holds the Corella saffron bottle and taps the label twice"*) — purely so Veo knows which object the action targets. Veo does not directly read the product upload; the product appearance is locked by the start frame (the Banana 2 image, which has the product appearance baked in via the explicit binding above). Adding redundant binding lines to Veo prompts dilutes the motion-focused prompt with no benefit.

**Migration from v580 to v581.**

For each `### Image N` block in an existing decoded markdown:

1. If the image binds the product upload, add a `- **product_image:** [ingredient name]` field below `reference_image`.
2. Prepend three binding lines to the top of the `Image prompt:` fenced block — persona always, product if `product_image:` is set, chain if `reference_image:` is not `none`.
3. Add ONE blank line between the binding lines and the existing visual description.

Action_notes and Veo prompts stay unchanged in structure (mention the product by name only when the action operates on it).

**Hard cutover.**

There is no v552/v573 → v581 backward-compatibility shim. Existing markdowns that lack explicit bindings will not have product upload binding under v581 because the platform stops auto-prepending. They must be migrated. The migration is mechanical (prepend three lines per image block) and can be scripted.

### Prompt length
With character descriptions removed and v581 explicit bindings prepended, prompts are typically **120–220 words** (the binding lines add 30-60 words depending on how many uploads bind). Image 1 (establishing) runs longer (~250–350 words) since it sets up setting, secondary characters, and mood on top of the bindings. Subsequent images are shorter because they inherit via `reference_image` and need only describe what's changing (action, pose, bowl state, empty-vs-full).

### Anchor phrases worth repeating
When continuity matters across a sequence, reuse exact anchor phrases across prompts:
- *"same Japandi apothecary background"*
- *"same honey-oak workbench"*
- *"same framing as image N"*
- *"iPhone HDR colors, deep focus"*

Consistent phrasing across prompts helps Nano Banana 2 lock in continuity even when the reference image alone isn't enough.

### Anti-patterns to avoid
- ❌ Cinema camera specs (FX6, 50mm f/1.8, shallow DOF, cinematic bokeh) — breaks the iPhone look
- ❌ Re-describing the main character's face, hair, beard, glasses, or wardrobe — redundant with the character reference, and can drift if paraphrased
- ❌ Aspect ratio in prompts (9:16, vertical portrait) — handled by the pipeline
- ❌ "Wide medium shot" / "establishing wide" as lone framing instructions — they produce distant, empty compositions
- ❌ Describing a table or bench "spanning the lower third" or "across the frame" — pushes the subject back and dwarfs them
- ❌ Generic lighting ("beautiful lighting", "professional lighting") — specify direction, source, and color temp
- ❌ Enumerating every background element (jar colors, shelf contents, visible-through-window houses, microwave/stovetop/crock inventory) — use a brief anchor phrase and let the image-gen fill in the rest
- ❌ Re-describing the full background on every image in a sequence — the `reference_image` chain inherits it visually; just say "same [setting] as image N"
- ❌ Full-body framing with feet and floor visible — Kaveno content is 9:16 with burned-in captions, there's no room. Crop at mid-thigh at the widest
- ❌ "Pulled back" / "medium shot" without explicit distance and crop — defaults to wide. Always specify: "camera 3-4 feet", "cropped at knees", "NO feet, NO floor"
- ❌ ALL-CAPS words in `line:` fields for emphasis (e.g., `"HEART"`, `"DRAIN"`, `"STOP"`) — write them as normal lowercase words. Caps are a visual-styling choice made downstream in captions/post, not part of the voiceover script. The `line:` field is the spoken script.
- ❌ `transition: blend` on transformation cuts (Day 1 → Day 14 reveals). The visual delta is too large; the cross-dissolve produces a visible morph artifact and breaks the time-jump illusion. Always cut on transformations. (Updated v544 — earlier guidance suggested defaulting to blend; that was wrong in practice. See the Transitions section.)
- ❌ `clip_mode: blend` on action scenes. Forces Veo to start from the previous clip's transient last frame instead of the scene's intended composition; produces stiff or contorted character motion. Always `fresh` for action.
- ❌ Creating multiple scenes for one held pose — if the image and action don't change, pack the voiceover beats as multiple `- **line:**` entries under a single scene
- ❌ Keeping a "setup" image next to an "action" image that already implies the setup — the mid-action image almost always captures both states; drop the setup
- ❌ One-word or two-word `line:` fields ("three days", "olive oil", "a glass") — these are caption fragments, not voiceover beats. Merge them into the surrounding full-sentence line on a single scene
- ❌ (v577) Splitting a scene into multiple `- **line:**` bullets when the scene's TOTAL dialogue is ≤21 words. Each `- **line:**` becomes one Veo clip = one 8-second generation; a 21-word line speaks comfortably in 8 seconds at natural pacing. Splitting 21 words into 4 fragments creates 4× the Veo cost, 3 unnecessary lip-sync re-cuts on a held pose, and zero visual benefit (all lines share the same start frame). Only split when total scene dialogue exceeds ~23 words AND each split lands on a natural syntactic boundary producing lines that are each ≥10 words and syntactically complete. See Line granularity within a scene above
- ❌ HOOK demonstration prompts that describe debris/stones/buildup with **gravity language** ("tumbling out", "rolling onto", "falling away") when the actual physics is **pressure/force language** ("erupting", "blasting", "ejecting in violent arcs"). Gravity reads as a science exhibit; pressure reads as a hook. See HOOK action intensity above
- ❌ HOOK action_notes that read like persona-as-subject (three persona beats: lift → glide → settle) when the scene is prop-as-subject (force applied → debris ejected). Flip the budget — most of the action_note should describe the prop's behavior under force, the persona's beat compressed to one phrase
- ❌ Setup-pose HOOK scenes ("hand mid-LOWER toward the mouth, about to spray") that lead the action scene. The setup is dead weight on a hook — the very first image should already be in mid-violent-action. The pre-spray pose belongs deleted, not as Image 1
- ❌ HOOK before-states that rely on emotional beats alone — patient slumped, sighing, looking down at a scale, slow chewing, hand-on-face, head-shakes-of-defeat. These are postures, not weird actions. Every HOOK scene that opens cold needs a concrete physical event on a prop (smash, throw, snap, dunk, pour, stomp, sweep, slice, drop, hurl). See HOOK weird-action requirement above
- ❌ Action_notes that describe the start frame's contents ("Patient stands at the exam table holding the banana", "Same kitchen, same counter, same lighting") — Veo already SEES the start frame. Re-stating it wastes prompt budget and can confuse the model. The action_note should describe ONLY what changes during the 8 seconds. See Veo start-frame rule above
- ❌ Action_notes that reference other clips ("same composition as image 4", "continuation on image 2", "same room as scene 5") — Veo generates each clip in isolation and has no memory of prior clips. Cross-clip references are invisible context that burns budget. The reference_image chain handles visual continuity for image generation upstream; action_notes should be self-contained motion briefs. See Single-clip independence rule above
- ❌ (v573) Using positional `image 2` references in image prompt body text to refer to the product upload (e.g., *"the product matching image 2's label exactly"*). The body convention is NAME-BASED for ingredients (`the main character`, `the [brand] bottle`) — number references are reserved for chain continuity (`as image 1`, `matching image 4`), where the number indexes the markdown's own `### Image N` headers. Mixing the two creates an ambiguity the body-renumber pass can't safely resolve. The product's slot identity (Flow's "Image 2") lives in the platform's manifest header, not in body text
- ❌ (v573) Mentioning the product by ingredient name in scenes where the product isn't the visual focus (HOOK before-states with no product in frame, ANATOMY overlays, persona-only OUTRO talking-head). Binding the product upload to those scenes wastes a slot, can produce visible product geometry artifacts in compositions where the product shouldn't appear, and risks the product bleeding into the persona's pose. The decision is per-scene: bind only when the source video shows the product label-readable in that frame
- ❌ (v573) Re-binding the product in chain-continuation scenes where the product is already locked-in by the previous frame (scene 5 follows scene 4 with `reference_image: image_4`, the bottle hasn't moved or rotated). Chain inheritance carries the product visually; re-mentioning by name causes the upload to attach again, costing a slot and potentially perturbing the composition. Bind once per product appearance, then chain
- ❌ (v573) Action_notes that re-describe the product's label, typography, color, or proportions ("the white plastic bottle with the navy SALVORA wordmark"). Veo SEES the start frame — the product appearance is already locked. Action_notes describe motion only: how the product is held, where it moves, what hits or pours from it. Use short generic naming in action_notes (`the bottle`, `the box`, `the product`) and let the start frame carry visual identity
- ❌ (v573) Declaring more than one product in the Ingredients table as an upload. Only ONE product gets the upload slot — pick the hero/featured product. Other products in the same video (competitor comparisons, multi-product shelves) are described inline. The platform's three-parent cap (persona + product + chain) makes anything more complex fail at import
- ❌ (v573) Treating the product as a required upload for every video. Many videos have no branded product on screen at all (pure educational, anatomy-focused, or persona-led content) — drop the product row from the Ingredients table entirely and omit product mentions from prompts. The product upload is opt-in per video, not a default
