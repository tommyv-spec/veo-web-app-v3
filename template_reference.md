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
Use the uploaded character reference image for the main character.
Use the uploaded product reference image for the Corella saffron bottle.
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

## Decode-side description grammar parity (v586)

**Extends v579 Stage 4.** Every decoded image description MUST follow the canonical Nano Banana 2 six-block grammar, and every action_note MUST follow the canonical Veo 3.1 five-block grammar. This is the **same vocabulary the platform's prompt-builder emits at generation time** — decode-side and generate-side speak one language.

### Why this rule exists

Pre-v586 decodes routinely captured the right pose and the right setting but missed:

1. **Object positions** — "ginger pieces visible to the right" was getting compressed to "ginger nearby". Banana 2 then placed the ginger in random locations.
2. **Foreshortening cues** — wide-angle composition cues ("the banana foreshortened larger in the lower-center foreground because it's closest to the lens") were getting flattened to "banana in foreground". Without the foreshortening note, Banana 2 renders flat-perspective compositions that don't match the source visual signature.
3. **Focus depth** — rarely stated. Banana 2 defaults to mid-depth focus and softens the foreground anchor.
4. **Lighting direction** — "from the glass-door window in the right side of frame" was getting compressed to "natural daylight". Banana 2 then applies generic flat lighting.
5. **Crop boundaries** — "cropped at lower ribs, NO floor visible" was sometimes implicit. Without it, Banana 2 widens the frame and adds floor.

Decode quality and generate quality are the same problem. A vague decode produces a vague generate prompt produces a hallucinated image. v586 enforces the decode side to match the precision the generate side already requires.

### Mandatory dimensions per image (the six-block checklist)

Every `### Image N` block's `Image prompt:` body must capture all six blocks. The order of blocks in the prose can vary; what matters is that each block is present and specific.

| Block | Dimensions to capture |
|---|---|
| **(1) Subject** | Pose, eye direction (locked to lens / tracking down to glass / etc.), mouth state (open mid-word / closed / etc.), expression beat (clinical-disgust / payoff-reveal / informative-warm / etc.). Persona referenced by name ("the main character") per v553.1 — never inline-described. Secondary characters described fully on first appearance per v523.1. |
| **(2) Composition** | Frame partition (where head and eyes land per rule of thirds — typically upper two-thirds), depth layers (every visible element placed in foreground / middle / background), crop boundary (where bottom / top / sides cut — "cropped at lower ribs", "NO floor visible", "cropped at mid-thigh"), foreshortening note when wide-angle (e.g. "the banana foreshortened larger in the lower-center foreground because it's closest to the lens"), single vs two-shot, headroom. |
| **(3) Action** | Current gesture, hand positions (left / right, height, holding what), eye tracking (locked to lens vs tracking to a prop), expression beat. The static-frame action — the snapshot of motion mid-clip. |
| **(4) Location** | Setting + every anchor prop with **explicit position**. Not "anatomy posters in the background" but "two large muscular-anatomy and skeletal-anatomy posters on the white wall behind, multiple framed medical certifications on the left at shoulder height, two small American desk-flag stands at the lower-left of frame, glass display cabinet of medical instruments off to the right". |
| **(5) Style** | Lighting direction ("vibrant natural HDR daylight from a glass-door window in the right side of frame"), color palette ("warm white walls, clean modern morning-kitchen light"), mood. |
| **(6) Tech** | Camera type / lens (iPhone wide-angle by default per v553), distance from subject in feet or arm-lengths ("camera approximately one arm's length", "approximately 4 to 5 feet from the desk"), focus depth (deep / shallow, where focus lands — "deep focus throughout"), motion blur if relevant ("motion blur trailing behind the falling cluster"). |

### Mandatory dimensions per action_note (the five-block checklist)

Every `- **action_note:**` body must capture all five blocks of the Veo 3.1 grammar. action_notes describe motion only (per v540) — no start-state restating, no cross-clip refs.

| Block | Dimensions to capture |
|---|---|
| **(1) Cinematography** | Camera-move classification grounded in v585 optical-flow data ("static handheld camera, no camera move, slight natural drift" / "slow pull-back over first 2 seconds" / "subtle push-in over the full 8s" / etc.). Magnitude where flow data supports it. |
| **(2) Subject** | Main character + every secondary character + every key prop named in the action narrative. |
| **(3) Action** | Three motion beats — `[start beat 0-2s]` → `[mid-clip beat 3-5s]` → `[end beat 5-8s]` — with explicit timing within the 8-second window. Each beat states what moves. |
| **(4) Context** | Setting carry-over from the start frame, with anchor-prop reuse (the prop that was on the desk in the start frame remains visible in the action narrative). |
| **(5) Style & Ambiance** | Register tag (`[clinical-disgust held steady]` / `[informative-warm]` / `[payoff-reveal climactic]` / etc.) + ambient sound cues that Veo's audio path can pick up ("SHARP CLATTER of plastic supplement bottles striking metal", "soft hiss of steam rising from the warm water"). |

### v586 worked example — Image upgrade

The same source-video frame, decoded pre-v586 vs decoded under v586:

**Pre-v586 (vague):**
> The main character at her clinic desk facing camera. Anatomy posters in the background. She is mid-explanation. iPhone wide-angle, deep focus.

**v586 (compliant):**
> *(Subject)* The main character is seated at her clinic desk facing camera, mouth open mid-word, eyes locked to camera lens, eyebrows raised in confident-authoritative emphasis. *(Composition)* Head and upper torso fill the upper two-thirds of frame, shoulders span frame width, cropped at mid-thigh, NO floor visible, NO feet visible. *(Action)* Right hand resting palm-down on the desk, left hand mid-gesture in the air at chest height — palm open and angled slightly upward in a warm explanatory gesture. *(Location)* Bright modern anatomy clinic interior with two large muscular-anatomy and skeletal-anatomy posters on the white wall behind, multiple framed medical certifications on the left at shoulder height, two small American desk-flag stands at the lower-left of frame, and a glass display cabinet of medical instruments off to the right. *(Style)* Vibrant natural HDR daylight from a side window out of frame to the right, warm-white walls, clean modern clinic palette. *(Tech)* Shot on iPhone wide-angle lens, handheld, camera approximately one arm's length from her at chest level, deep focus throughout, slight wide-angle perspective distortion at the edges of frame.

The v586 version answers every visual question Banana 2 will ask: where does each prop sit, where does the subject's eyeline land, how far is the camera, where is the light source, how deep is the focus. Banana 2 stops hallucinating. The same prose form is also what the generate side writes when authoring a NEW script — so the grammar is one language, used in both directions.

### v586 worked example — action_note upgrade

**Pre-v586 (vague):**
> She gestures with her left hand and explains. Camera handheld.

**v586 (compliant):**
> *(Cinematography)* Static handheld camera, no camera move, slight natural drift (per v585 flow data: magnitude < 0.5px). *(Subject)* The main character at her desk; the row of unbranded supplement bottles in the lower foreground. *(Action)* `[Start beat 0-2s]` She squares to camera, both hands relaxed at her sides, head tilting very slightly forward into the lens. `[Mid-clip beat 3-5s]` Right hand opens into a brief instructional sweep at chest height then drops. `[End beat 5-8s]` She leans an inch closer to camera, brows lift in invitation, the corner of her mouth rises into a small knowing half-smile. *(Context)* Same anatomy clinic interior as the start frame; anatomy posters and certifications visible behind throughout. *(Style & Ambiance)* `[Confident-authoritative direct-address]`. Ambient: bright modern anatomy clinic tone, soft fluorescent overhead hum mixed with natural daylight from a side window, faint distant HVAC hum, settled silence after the bottle-sweep.

### How v586 wires into the v579 pipeline

The v579 Stage 4 frame-inspection prompt is updated. Old prompt: *"For each shot record: setting (kitchen / office / bedroom / etc.), composition (talking head / b-roll composite / product hold / etc.), persona pose, foreground props, presence/absence of product."*

New prompt: *"For each shot, walk the v586 six-block image checklist (Subject / Composition / Action / Location / Style / Tech) AND the v586 five-block action_note checklist (Cinematography / Subject / Action / Context / Style & Ambiance). Capture every dimension explicitly. Object positions must be specified ('ginger pieces on a wooden cutting board to the right of the glass'), not paraphrased ('ginger nearby'). Foreshortening must be noted when wide-angle. Lighting direction must name the light source's position. Focus depth must be stated. Crop boundaries must be explicit."*

### Migration

Pre-v586 decodes in `raw/decoded_*.md` are valid as-is — they were authored under earlier rules. From this commit forward, new decodes MUST satisfy the v586 checklist. The wiki's lint pass can flag pre-v586 decoded images that lack one or more of the six blocks, but the lint is advisory not blocking.

### Bidirectional rule cycle (the-cycle.md)

v586 is the canonical proof of bidirectionality:
- **Decode side** — every new decoded image follows the six-block grammar; this captures the source's visual signature with the precision the generate side needs.
- **Generate side** — every new authored image already follows the same six-block grammar (Nano Banana 2's native vocabulary); the platform's prompt-builder emits these blocks.
- **One language, both directions.** Improving the decode-side checklist improves the generate-side checklist — they are the same checklist.

---

## Generate-side chain optionality — parallel-generation enablement (v590)

**ASYMMETRIC RULE — applies to GENERATE-SIDE only.** This is the first v-rule that breaks the bidirectional-cycle symmetry (v589.1 was about the chain-binding-line wording — same-grammar-both-sides). v590 is about chain TOPOLOGY — which scenes need chains at all — and the right answer differs between observation (decode) and execution (generate).

### The two sides

| Side | Chain policy | Why |
|---|---|---|
| **Decode** (`raw/decoded_*.md`) | Chain faithfully to mirror the source | The decode is observational — if the viral video kept tight continuity scene-to-scene, we record that. If it didn't, we don't invent it. Faithfulness wins. |
| **Generate** (`videos/*.md` for our own videos / variants / adaptations) | `reference_image: none` for every scene EXCEPT those genuinely requiring tight pixel-level continuity | The generate-side template is execution — chain is a per-scene tradeoff between identity-anchor strength vs. generation throughput. Optimize per-scene for shipping. |

### Why generate-side chain optionality matters

**Persona is locked via the upload (Flow slot 0).** Identity is preserved across every generation that gets the persona reference attached. **Product (when bound) is locked via slot 1.** Brand consistency holds. **The v586 description grammar carries the rest** — setting, anchor props, lighting, mood, framing.

Slight natural background variation between independent scenes is **desirable**:
- Avoids the AI-flattened "every scene looks identical" tell that betrays generation.
- Different angles, slightly different positions, naturalistic camera-shift between cuts — these are properties of real video, not bugs.
- A chained-everywhere shoot looks artificial; a varied-but-anchored shoot looks human.

### Chain REQUIRED on generate side (the four exceptions)

1. **v580 recipe state-evolution** — each prep step inherits glass + counter + cumulative ingredient state from the prior step. Chain consecutive recipe images (typical pattern: 3→4→5→6→7 for a 5-step recipe). The chain anchors the literal jar/glass/counter so ingredient accumulation reads visibly.
2. **v541 before/after transformation** — same patient, same setting, only outfit/skin/visible-state differ. Chain Day-1 → Day-14 image pair (or any equivalent before-after pair). Chain anchors the patient's identity beyond what the persona+product uploads cover.
3. **Single-shot action arc** — start frame and end frame within ONE clip share composition. Chain when the single-clip dual-frame anchoring lands as a separate v-rule (currently a v589 PLATFORM-FUTURE candidate).
4. **Two-shot follow-up** — when a close-up scene must preserve the exact identity/pose of a secondary character introduced in a prior two-shot scene. Chain the close-up to the two-shot anchor; the secondary character's identity is now locked beyond what the persona+product uploads cover (because secondary characters are NOT uploaded as references).

### Everything else: independent

HOOK / CONTEXT / EXPLAIN / AUTHORITY / single-frame PRODUCT (bottle hero) / CTA / FOLLOW — set `reference_image: none`. The persona+product uploads + the v586 description carry the rest. Each independent scene's image prompt must be **self-sufficient**: full six-block walk, setting + anchor props described inline since no chain carries them.

### Throughput math (typical 8-scene script)

| Pattern | Banana 2 | Veo 3.1 | Total |
|---|---|---|---|
| All-chained (every scene `reference_image: image_K`) | sequential ≈ 4 min | sequential ≈ 10 min | 16-22 min |
| v590-applied (3 chains in recipe + 5 independents) | parallel ≈ 1 min (via `parallel_slots`, default 2, max 6) | parallel ≈ 4 min | 5-7 min |

**6-8× faster ship** on typical script structure. Bigger scripts (e.g. the 10-scene Nuri saffron-ED listicle) gain more.

### How to apply when authoring a `videos/*.md`

1. **Walk the storyboard.** For each scene, classify: independent (HOOK/CONTEXT/EXPLAIN/AUTHORITY/PRODUCT-single-frame/CTA/FOLLOW) or chain-required (recipe state-evolution / before-after / single-shot action / two-shot follow-up).
2. **Independent scenes:** set `reference_image: none`. Verify the image prompt is self-sufficient — full v586 six-block walk with the setting + anchor props described inline (since no chain carries them).
3. **Chain-required scenes:** set `reference_image: image_K` per the existing v523/v589.1 rule. Use the v589.1 semantic chain-binding line.
4. **Per-scene secondary characters** (one-offs introduced in a single scene, not appearing elsewhere): described fully on first appearance per v523.1. NO chain needed unless the character appears in a follow-up scene.
5. **Run the platform.** Non-chained scenes start in parallel via `parallel_slots`. Chained scenes generate sequentially within their chain group but multiple chain groups run in parallel relative to each other.

### What's RETAINED (every other v-rule applies unchanged)

- v553.1 persona-never-inline-described — every scene
- v581 + v589.1 binding lines (PERSONA always; PRODUCT when bound; CHAIN when chained)
- v586 six-block image grammar + five-block action_note grammar — every scene
- v540 motion-only action_notes — every scene
- v577 line word budget — every scene
- v589 absolute-magnitude grammar — every state-evolution clip
- v589.1 semantic chain-binding line — every chained scene

v590 is purely about CHAIN TOPOLOGY (which scenes have chains). Everything else is invariant.

### Worked example — Nuri saffron-ED listicle template

The `videos/nuri-saffron-ed-anatomy-clinic.md` (10 scenes / 10 clips) authored under chain-everywhere v523 default chains all 10 images sequentially. Re-audit under v590:

| Scene | Block tag | v590 verdict | reference_image |
|---|---|---|---|
| 1 | HOOK (bottle-sweep) | Independent | `none` |
| 2 | TITLE-CARD (listicle frame) | Independent | `none` |
| 3 | RECIPE TRUTH-1 (water) | Recipe state-evolution start | `none` (recipe head) |
| 4 | RECIPE TRUTH-2 (lemon) | Chain-required (state evolution) | `image_3` |
| 5 | RECIPE TRUTH-3 (ginger) | Chain-required (state evolution) | `image_4` |
| 6 | RECIPE TRUTH-4 (honey) | Chain-required (state evolution) | `image_5` |
| 7 | RECIPE TRUTH-5 (saffron, climax) | Chain-required (state evolution) | `image_6` |
| 8 | AUTHORITY (with patient) | Two-shot follow-up — patient identity is one-off, no chain needed (described fully inline per v523.1) | `none` |
| 9 | PRODUCT (bottle hero) | Independent | `none` |
| 10 | CTA | Independent | `none` |

Result: 4 chains within the recipe (Scenes 4-7) + 6 independents (Scenes 1, 2, 3, 8, 9, 10). The 6 independents start in parallel; the recipe chain runs sequentially within itself. **Worst-case sequential dependency** drops from 10 stages to 5 stages (6 parallel + 5 sequential within recipe chain) — roughly **2× faster** on this specific script.

### Why generate-only: the bidirectional asymmetry

The cycle has held bidirectional for grammar — every v-rule about HOW to describe a frame, an action, a state-change applies equally on both sides because the GRAMMAR is the same language. v590 is the first rule about TOPOLOGY (the structure of dependencies between scenes) and topology IS asymmetric:

- The **decode** records what was — including any chain dependency the source happened to use, however incidental.
- The **generate** chooses what to ship — including which chain dependencies actually pay for themselves in identity anchoring vs. cost in throughput.

This asymmetry is documented explicitly in [[the-cycle]]: grammar is symmetric, topology is not.

### Migration

**Existing `videos/*.md` artifacts** authored under chain-everywhere v523 default can be re-audited and chains relaxed where v590 conditions allow. The Nuri saffron template (above) is the canonical worked migration.

**New templates** from this commit forward use v590 chain-optionality from the start. Authors walk the storyboard once, classify each scene as independent or chain-required, set `reference_image:` accordingly.

**Decoded scripts in `raw/decoded_*.md`** are NOT migrated — they remain faithful to source.

---

## VLM video understanding + state-evolution arc grammar fitted to platform blend + absolute-magnitude grammar (v589)

**Three coordinated halves.** v586 codified per-frame description grammar parity. v587 added the comprehension layer + Veo final-prompts symmetry. v588 added dense per-shot frame sampling. v589 closes the remaining gaps: a structural VLM backstop for action-arc detection (with a free local path), state-evolution arc grammar fitted to the platform's existing blend mechanism, and absolute-magnitude grammar that stops prompts from hedging.

### Why the rule exists

Surfaced from the second-pass review of the @icelandicwisdom belly-fat HOOK (May 2026). After v588 corrected the static "points at anatomy" miss to a fat-melt prop-violence arc, the rewrite still hedged: *"the fat is now DRAMATICALLY REDUCED, the abdominal organs clearly visible, only residual yellow fat at the very bottom edge."*

User pushback: *"it all melts completely while we say 'dramatically reduced' — we need it more powerful, the reverse-engineering needs to actually understand what's happening."*

Verdict: dense-frame human-walk plus six-block grammar still wasn't enough. Three things needed:
1. A **structural VLM backstop** that catches what the human eye underestimates
2. The **Veo 3.1 First/Last-Frame ("S/E Frame") workflow** so generation matches the source's actual end-state visually rather than via prose alone
3. **Absolute-magnitude grammar** so prompts stop hedging when the actual state change is COMPLETE

### Half A — Stage 4d VLM video understanding (decode-side, provider-agnostic + free local default)

Adds a structural backstop AFTER the v588 dense-frame human-walk. Pipeline file `code/v589_video_understanding.py` cascades providers in priority order:

**(1) LM Studio (free local, recommended)** — user opens the LM Studio app with a vision-capable model loaded (e.g. `gemma-4-E2B-it-GGUF` with mmproj — already cached on most dev machines after a single LM Studio install) and enables the local server at `http://localhost:1234`. Script auto-detects the running server via `GET /v1/models`, picks a vision-capable model from the available list, and sends dense frames + transcript via OpenAI-compatible `/v1/chat/completions`. Zero per-call cost; runs on CPU.

**(2) Gemini API (paid fallback)** — when `GEMINI_API_KEY` is set, native MP4 upload at 1fps + audio + per-second timestamps. ~$0.01 per 45s decode on `gemini-2.5-flash`. Free tier covers many decodes/day.

**(3) Human-walk template (always available)** — when no automated provider is configured, the script writes a `stage4d_vlm.json` template skeleton with empty fields per shot + dense frame paths listed + dialogue overlapping each shot. The human-walking decoder LLM session (Claude in chat) walks the dense frames produced by v588 and fills in the JSON manually. The v589 STRUCTURAL rule still holds — the schema is produced, just by a human walker instead of an API.

The VLM JSON (whichever provider produced it) becomes the **authoritative source for visual action arcs**, parallel to whisper.cpp being authoritative for dialogue.

**The VLM JSON schema** (per `code/v589_video_understanding.py`):

```json
{
  "shot": 1,
  "start": 0.0, "end": 6.47,
  "summary": "<one sentence: rhetorical function + visible action>",
  "static_composition": {
    "subject": "<persona pose + eye direction + mouth state + expression>",
    "framing": "<camera distance + frame partition + depth layers + crop>",
    "anchor_props_with_positions": "<every visible prop and its EXACT position>",
    "lighting_and_palette": "<lighting direction + color palette + mood>"
  },
  "action_arc": {
    "has_state_evolution": true,
    "start_state": "<foreground prop / subject look at shot start>",
    "mid_state": "<what's happening at midpoint>",
    "end_state": "<foreground prop / subject look at shot end>",
    "magnitude": "COMPLETE",
    "verbs_observed": ["pour", "melt", "reveal"]
  },
  "audio": "<dialogue + ambient sound cues + register>",
  "veo_reproduction_hints": {
    "use_first_last_frame_workflow": true,
    "start_image_caption": "<one paragraph Nano Banana 2 description of optimal start frame>",
    "end_image_caption": "<one paragraph Nano Banana 2 description of optimal end frame>",
    "transition_prompt": "<one paragraph Veo 3.1 transition prompt with absolute magnitude language>"
  },
  "human_walk_corrections": "<flags any aspect where a midpoint-only human walk would under-describe the arc>"
}
```

**The VLM JSON becomes the AUTHORITATIVE source for visual action arcs** — parallel to whisper.cpp being authoritative for dialogue. When Gemini reports *"the fat completely melts away and the abdominal organs are fully revealed"* but the human-walk wrote *"dramatically reduced,"* the VLM correction wins (or at minimum the discrepancy is flagged).

**Cost**: ~300 tokens/sec at default media resolution; a 45s video ≈ 13.5K input tokens + 1-2K output tokens. On `gemini-2.5-flash` that's well under $0.01 per decode. Free tier covers many decodes per day.

**Setup (LM Studio path — recommended free local)**:
1. Install LM Studio from `lmstudio.ai`.
2. In LM Studio, download `lmstudio-community/gemma-4-E2B-it-GGUF` (or any vision-capable GGUF — Qwen2.5-VL, LLaVA, etc.). Vision capability requires the `mmproj` projector file alongside the main model.
3. Load the model in LM Studio.
4. Enable the local server (Developer tab → Start Server). Default URL `http://localhost:1234`.

**Setup (Gemini fallback)**:
```bash
pip install google-genai
export GEMINI_API_KEY=...   # https://ai.google.dev/
```

**Run** (auto-cascades through providers):
```bash
python code/v589_video_understanding.py path/to/source.mp4 \
    --shots _decode_tmp/.../shots.json \
    --transcript _decode_tmp/.../transcript.json
# → writes stage4d_vlm.json next to the video (or per --out)
```

To force a specific provider: `--provider lmstudio | gemini | template`.

**Reconciliation discipline**: the decoder runs Stage 4d AFTER the v588 dense-frame walk and reconciles the two sources before authoring the markdown. The VLM JSON is archived alongside the v579 manifest in `raw/decode_artifacts/<source-id>/stage4d_vlm.json` for audit. When the human-walk template path is used, the decoder LLM session fills in the template by walking the dense frames + dialogue — same schema, same authority.

### Half B — State-evolution arc grammar fitted to the existing platform blend mechanism (generate-side)

The platform's existing emission model already supports interpolation between two different frames via `clip_mode: blend`, where the NEXT SCENE's image is the end_frame of the current clip. `code/veo_generator.py:883` provides `generate_transition_cue()` which narrates the metamorphosis between two different frames whenever start_frame ≠ end_frame.

**The hard parser constraint**: `### Image N` headers require integer `N` (regex `### Image (\d+)`). Scene blocks have ONE `image:` field. There is no `image_end:` field today. An earlier first-pass v589 introduced `### Image N_end` and a same-scene end-frame field — RETRACTED, because neither would parse. The right pattern fits the platform as-is.

**Two valid patterns for state evolution**:

#### (B1) Multi-clip state evolution

When the action arc spans naturally across adjacent shots/scenes (e.g. v580 recipe steps — water → lemon → ginger → honey → saffron — each step in its own image), use `clip_mode: blend` between adjacent scenes. The next scene's image IS the end-state of the current clip; the platform's `generate_transition_cue()` narrates the metamorphosis. **The platform already supports this — no new field, no parser change.** The Korella saffron decoded scripts already use this pattern (with `clip_mode: continue` instead of `blend` for tight chains, but the principle is the same).

#### (B2) Single-clip state evolution (PLATFORM-LIMITATION)

When the action arc is contained within ONE shot (e.g. the @icelandicwisdom 6-second fat-melt HOOK where the entire arc — fat-draped torso → liquid poured → fat completely melts → organs fully revealed — happens within one continuous Veo clip), the current platform has **NO same-scene end_frame anchor**. Veo gets only the start_frame + action narrative + transition_cue.

**Risk**: Veo may produce partial state changes when the source shows complete change.

**Mitigation TODAY (until v590 platform extension lands)**:
1. Half C absolute-magnitude grammar throughout the action_note ("completely melts away", "fully revealed", "entirely dissolves").
2. Explicit anti-failure-mode clause appended to the negative prompt — e.g. *"no partial fat removal — fat must completely melt off the upper torso, no residual yellow ON the upper-abdominal organs at clip-end."*
3. Three timed beats in the action narrative explicitly stating the end-state per the VLM JSON's `end_state` field (so Veo has prose anchors at `[00:05–00:08]` even without a visual end-frame anchor).

#### PLATFORM-FUTURE candidate (unnumbered, not yet shipping — v590 was reassigned to chain-optionality)

Extend the platform parser to support an `image_end:` field on the scene block (parallel to the existing `reference_image:` and `product_image:` fields) so single-clip state-evolution arcs can anchor Veo on TWO visual states in ONE clip. The extension would:

- Allow a scene block to specify `image_end: image_N_b` (or similar non-conflicting integer-suffix scheme) referencing a sibling `### Image` block that holds the end-state composition.
- Surface `start_frame` and `end_frame` to the worker as TWO different images for that single scene/clip.
- Reuse `generate_transition_cue()` to narrate the metamorphosis between them.

Until v590 ships, single-clip state-evolution clips ship with the Half C mitigations alone.

**Static talking-head clips with no state evolution** continue to use single-image start frames as before. The Half B rule applies ONLY to clips whose VLM `action_arc.has_state_evolution: true`.

### Half C — Absolute-magnitude grammar (both sides)

action_notes describing visible state-evolution end-states MUST use absolute language when the source shows complete change. The VLM's `magnitude` field (COMPLETE / PARTIAL / MINIMAL) is the parser-grade signal that gates which language tier the action_note uses.

**Forbidden hedge words when magnitude is COMPLETE**:
- "dramatically" (e.g. "dramatically reduced")
- "mostly" (e.g. "mostly visible")
- "almost" (e.g. "almost completely")
- "substantially reduced"
- "largely"
- "for the most part"

**Required absolute alternatives**:
- "completely melts away"
- "fully revealed"
- "entirely dissolves"
- "the [X] is gone"
- "the [Y] now shows the [Z] fully"
- "all of the [X]"
- "every [Y] is visible"

**Hedge language reserved ONLY for genuinely partial states** — e.g. "partially squeezed" if the lemon retains half its juice on screen, or "the fat is reduced to half" if that's what the source actually shows.

The negative prompt for state-evolution clips should ALSO encode the absolute-magnitude requirement explicitly — e.g. *"no partial fat removal — fat must completely melt off the upper torso, no residual yellow ON the upper-abdominal organs, no anatomical organs still hidden by fat at clip-end."*

### Half D — Image-number reference discipline (case-sensitive substitution rule)

**Why this rule exists**: the platform's `_resolve_flow_prompt_bindings` function in `image_platform.py` does case-sensitive regex substitution to translate the markdown's `Image K` references to Flow's actual slot positions at job emission. The pattern is `\bImage {K}\b` — capital I + integer + word boundaries. **Lowercase `image K` is NOT substituted.** When a body has lowercase descriptive references like *"Same X interior as image 5"* or *"the jar from image 3"*, those stay raw in the prompt sent to Banana 2 — and Banana 2 has only persona + product + chain attached for a given generation (max 3 inputs). Lowercase numbered references → phantom links → confused Banana 2.

**The rule**:

| Where in the prompt | Format | Why |
|---|---|---|
| **v581 chain binding line** (single sentence at the top of the Image prompt body) | `Use Image K as the visual reference for the previous scene — preserve <setting>, <lighting>, <anchor props>, continuity from there.` | Capital I; gets rewritten by `_resolve_flow_prompt_bindings` to Flow's actual slot number. **REQUIRED format** — do not paraphrase. |
| **Body prose (descriptive)** | `Same X interior as the previous scene` / `from the previous scene` / `same as before` / `the same X` / direct setting description | Semantic; no number reference at all. Banana 2 reads "the previous scene" as content (it has the chain ref attached) and renders accordingly. |

**Forbidden in body prose**:
- `Same X as image K` (lowercase i)
- `from image K` / `in image K` / `of image K` / `the jar in image K` / `same as image K`
- Any lowercase `image \d+` pattern outside the v581 binding line

**Required substitutions** when authoring or auditing:
- `as image K` → `as the previous scene` (or `as before`)
- `from image K` → `from the previous scene`
- `in image K` → `in the previous scene`
- `of image K` → `of the previous scene`

**Confirmed by Gemini Nano Banana 2 official prompting docs** (ai.google.dev/gemini-api/docs/image-generation): the recommended multi-image prompt format uses semantic descriptors like "the dress from input 1", "the model from input 2" — NOT positional `Image 1` / `Image 2` references in body prose. The v581 binding line is the one exception (it gets rewritten to a positional reference Banana 2 understands as "Image 1" = first attached image, etc.).

**Migration**: pre-v589.D decoded scripts may contain lowercase `image K` body-prose references. Audit on re-use; the `_fix_image_refs.py` script (or equivalent regex sweep) can clean them in one pass while preserving the v581 binding lines.

### Worked example — @icelandicwisdom HOOK Clip 1.1 (single-clip B2 pattern)

Pre-v589 (v588-corrected, still hedging):
> *"By clip-end the fat is now DRAMATICALLY REDUCED, the abdominal organs clearly visible, only residual yellow fat at the very bottom edge of the torso melting downward in slow drips."*

v589-compliant (single-clip B2 pattern — no same-scene end_frame anchor today; Half C absolute-magnitude grammar + negative-prompt failure-mode ban do the work):
> *"On 'you are on ozempic' the pour completes — the mug pulls back to the right, COMPLETELY DRAINED. By clip-end the upper abdomen of the torso is ENTIRELY CLEARED of fat: every anatomical organ (the pale-pink stomach, the liver, the coiled small intestine, the colon, the kidneys behind) is FULLY REVEALED and sharply visible to camera, no yellow remaining ON the upper torso itself. Only puddled melted fat REMAINS on the desk below the torso..."*

Negative prompt clause: *"...no partial fat removal — fat must completely melt off the upper torso, no residual yellow ON the upper-abdominal organs at clip-end."*

If/when v590 platform extension ships, the same Clip 1.1 will be upgradeable to B2-with-anchor: keep the same prose, add `image_end:` referencing a separate end-state image showing organs FULLY REVEALED + melted fat puddled on desk. Veo then has BOTH absolute prose AND a visual end-state anchor. Until then, Half C carries.

### Bidirectional implication

v589 closes the bidirectional cycle for state-evolution actions:
- **Decode side**: Stage 4d VLM is the structural backstop for capturing what the source actually shows
- **Generate side**: Veo 3.1 First/Last-Frame + absolute-magnitude grammar is the structural mechanism for producing what we actually intend
- **Same checklist, both directions**: every state-evolution clip on either side must answer: what's the start state? what's the end state? what's the magnitude (COMPLETE / PARTIAL / MINIMAL)? what verbs operate?

### Migration

Pre-v589 decodes valid as-is — flag for re-pass when re-using as parents for state-evolution variants. New decodes from this commit forward MUST: (1) run Stage 4d when GEMINI_API_KEY is available and reconcile findings with the human walk; (2) emit start+end image pairs for every state-evolution clip; (3) use absolute-magnitude language in action_notes whose end-state magnitude is COMPLETE per the VLM JSON.

When `GEMINI_API_KEY` is NOT available in the decode session's environment, the decoder notes it in the HTML pipeline audit trail and proceeds with v588 dense-walk as the action-arc source. Half B (S/E-Frame for state-evolution) and Half C (absolute-magnitude) still apply — they don't depend on the API.

---

## Dense per-shot frame sampling (v588)

**Extends v585 Stage 4 (motion capture).** v585 added optical-flow camera-move classification per shot. v586 added the per-frame description grammar parity. But v585 + v586 together still allowed the decoder to inspect ONLY the midpoint frame per shot — and that is insufficient when the shot contains a visible **state-evolution arc within the prop**.

### The bug v588 prevents

Real example from the @icelandicwisdom belly-fat decode (May 2026): the HOOK shot was a 6.47s fat-melt prop-violence sequence — the persona POURED a glass of warm tea onto a fat-draped anatomical torso, and the yellow prosthetic fat **visibly melted on contact**, revealing the abdominal organs by clip-end. The midpoint frame at 3.23s caught only the mid-pour state with the fat partially-reduced; without seeing the START frame (fat-fully-draped) or the END frame (fat-mostly-melted-away, organs revealed), the first-pass decoder wrote *"the persona points at the gut/belly area of an anatomical model"* — a static gesture description that **completely missed the prop-violence + visible-payoff arc that was the ENTIRE point of the HOOK**.

The fix: see all three frames before authoring the description.

### The mandatory minimum: start / midpoint / end

Every shot's view-tool inspection MUST view at minimum these three frames:
- **Start** at `t = shot.start + 0.1s` — the opening visual state
- **Midpoint** at `t = (shot.start + shot.end) / 2` — the central beat
- **End** at `t = shot.end - 0.1s` — the closing visual state

Three frames, three states. The action_note's three motion beats — start beat (0-2s), mid-clip beat (3-5s), end beat (5-8s) — are then GROUNDED in three distinct visual references rather than one frame extrapolated.

### Additional dense-sampling triggers

View **5+ frames evenly distributed across the shot** when ANY of these signal an action arc within the shot:

1. **Shot duration > 3s** — long enough to contain an arc.
2. **v585 optical-flow magnitude > 0.7px** — drift with motion (some movement happening, even if classified as static-handheld-with-drift).
3. **Dialogue overlapping the shot mentions a verb-of-state-change** — `squeeze in`, `pour`, `drop`, `add`, `stir`, `mix`, `spread`, `press`, `pull`, `squeeze`, `crack`, `melt`, `dissolve`, `unfold`, `apply`, `wipe`, etc. The verb is the action-arc signal.
4. **Start-frame and end-frame visual signatures DIFFER** — comparing start and end frames reveals state evolution (fat-draped → fat-melted; clear-water → amber-tea; lemon-held-high → lemon-mid-squeeze; bottle-on-counter → bottle-in-hand). When the start ≠ end, sample densely between to catch the transition.

When any trigger fires, sample at least 5 frames at `t = shot.start + 0.1, shot.start + 0.25 * duration, midpoint, shot.start + 0.75 * duration, shot.end - 0.1`. View all of them before writing the description.

### How to apply

```python
# Stage 4a — frame extraction (with v588 dense sampling)
import cv2
cap = cv2.VideoCapture(SRC)
fps = cap.get(cv2.CAP_PROP_FPS)
for shot in shots:
    duration = shot.end - shot.start
    # v588 minimum: start / mid / end
    base_times = [shot.start + 0.1, (shot.start + shot.end) / 2, shot.end - 0.1]
    # v588 dense triggers
    needs_dense = (duration > 3.0
                   or motion[shot.id].magnitude > 0.7
                   or any(verb in shot.dialogue.lower()
                          for verb in ["squeeze", "pour", "drop", "add", "stir",
                                       "mix", "spread", "press", "pull", "crack",
                                       "melt", "dissolve", "unfold", "apply", "wipe"])
                   or visual_diff(start_frame, end_frame) > THRESHOLD)
    times = base_times if not needs_dense else \
            [shot.start + 0.1] + [shot.start + p * duration for p in [0.25, 0.5, 0.75]] + [shot.end - 0.1]
    for t in times:
        cap.set(cv2.CAP_PROP_POS_FRAMES, int(t * fps))
        ok, frame = cap.read()
        if ok:
            cv2.imwrite(f"frames/shot{shot.id:02d}_t{t:.2f}s.png", frame)
```

Then view-tool every saved frame for that shot before authoring the image prompt + action_note.

### Anti-patterns the rule prevents

| Anti-pattern | Bug it produces |
|---|---|
| Inspecting only the midpoint frame | Static-snapshot description; the action arc is missed entirely |
| Skipping the end frame because "the start frame is sufficient" | The PAYOFF of the action is missed (fat-melted, lemon-squeezed, ingredients-added, stirring-done) |
| Treating the shot as a single visual state when v585 reports flow > 0.7px | Drift signal indicates motion within the shot — could be a hand entering frame, a prop changing state, an off-camera light shifting — needs dense walk |
| Ignoring verb-of-state-change in the overlapping dialogue | The dialogue is the action signal; "squeeze in" guarantees a state change in the prop, regardless of what the midpoint shows |
| Viewing 3+ frames but writing the action_note as if from one snapshot | Three frames, three beats — anchor each motion beat to its source frame |

### Bidirectional implication

v588 is decode-side. But the same principle applies to generate-side authoring: when authoring a new image prompt for a multi-state action scene (e.g. recipe state-evolution per v580, transformation per v541, or any prop-violence HOOK with visible payoff in-clip), the AUTHOR must mentally walk start / mid / end before writing — same checklist, applied imaginatively rather than retrospectively. The grammar is one language, both directions. See [[the-cycle]].

### Promotion candidates surfaced via v588

The fat-melt-on-anatomical-torso HOOK class observed in the @icelandicwisdom decode is a **stronger v539 sub-variant** than separate HOOK-then-RESULT structures because the prop-violence and the visible payoff happen in the SAME 6-second clip. Once 5/5 evidence accumulates across decoded scripts, promote as a named v539 sub-variant (e.g. v539-fat-melt or v539-payoff-in-clip).

### Migration

Pre-v588 decodes that inspected only midpoint frames remain valid as historical record; flag them for action-arc audit when re-using as parents for new variants. New decodes from this commit forward MUST satisfy v588 dense sampling.

---

## Reproduction-ready decode artifact (v587)

**Extends v586.** v586 ensured that every decoded image description was structurally rich enough for Banana 2 to re-render the source frame. v587 closes the symmetry: every decoded script is now a **complete reproduction package** with the same shape as a generate-side script.

### What problem v587 solves

Pre-v587 decoded scripts were rich on the **what** (storyboard narrative + image prompts) but the **how/why/reproduction** lived in HTML-comment blocks at the top of the file. That was decoder-dependent (some sessions captured it richly, others didn't), wasn't machine-parseable for wiki-side ingest, and made the decode and generate templates structurally asymmetric. The generate-side template already had `## Veo 3.1 Final Prompts (per clip)`; the decode side did not.

The bidirectional rule cycle requires that decode and generate speak the same language. v586 made the per-frame description grammar identical. v587 makes the *artifact-level structure* identical — both templates now produce an output that:

1. States WHAT happens (Storyboard narrative)
2. States HOW it works (Comprehension v-rule inventory)
3. States WHY it works (Comprehension rhetorical + angle subsections)
4. States HOW TO REPRODUCE IT (Images for Banana 2 + Veo final prompts for Veo 3.1)

### The two required sections

#### `## Comprehension`

Five required subsections, all five must be present and filled:

**(1) Structural inventory.** Total scenes / clips / duration. Per-scene block tag from the canonical block vocabulary: HOOK / TITLE / RECIPE / TRANSFORMATION / EXPLAIN / ANATOMY / RESULT / AUTHORITY / PRODUCT / CTA / FOLLOW. The block tag is what the scene is *doing rhetorically*, not what's on screen. A "doctor in clinic" scene could be HOOK or AUTHORITY or PRODUCT depending on its rhetorical role.

**(2) v-rule inventory.** A table mapping every applicable live v-rule (per `code/template_reference.md` and the wiki's [[conventions]] index) to how this specific video uses it. Three statuses:
- `applied — <variant or specifics>` — rule is present and active
- `NOT APPLICABLE — <reason>` — rule doesn't match this video's structure (e.g. v541 outfit-change is NOT APPLICABLE for single-day videos)
- `partial — <which dimensions covered>` — rule is partially honored

The inventory makes promotion-discipline auditing mechanical: a wiki audit can scan for which rules are applied across N scripts and surface which patterns are hardening into broad practice (5/5 → ready to promote a new variant) vs. one-off (1/5 → stays an example, not a rule).

**(3) Rhetorical structure.** Four named axes:
- HOOK type (force-verb / clinical-markup / diagnostic-press / symptom-curiosity / banana-pun / weird-action-on-prop / bottle-sweep / ...)
- Frame (recipe-as-claim / before-after-transformation / authority-stack / curiosity-gap / 5-truths-listicle / banana-measurement / ...)
- Payoff structure (timeline-promise / climax-position / authority-anchor / outfit-change-time-jump / ...)
- CTA structure (comment-keyword / comment-plus-follow combined / link-in-bio / DM-trigger / ...)

These axes feed [[hook-patterns]], [[cta-patterns]], and the niche playbooks. Filling them on every decode means cross-script pattern surfacing is automatic.

**(4) Angle / audience signal.** Niche, primary audience (gender + age band), secondary audience, symptom or aspiration, emotional register. The targeting axis. Drives [[audience]] folder cross-cuts.

**(5) Persona archetype + setting tier.** Archetype label per [[persona-map]], tier per [[structure-tiers]] (Tier-0 selfie-arm / Tier-1 single-setting / Tier-2 multi-setting), specific settings used. The persona-and-setting axis. Drives [[personas]] and [[settings]] cross-cuts.

#### `## Veo 3.1 Final Prompts (per clip)`

One fenced block per clip. Each clip = one Veo generation = one 8-second video. The prompt is the **assembled** form built from the start-frame image + action_note + line:

```
### Clip N.M — Scene N, Line M (<block tag>)
**Start frame:** Image N
**Text prompt:**
` ` `
[Cinematography — camera move classification grounded in v585 flow data]

[Action narrative — three motion beats with timing]

The main AI generated character says in a [voice qualifier] voice, "[exact dialogue from Storyboard]".

(Voiceover variant when `**speaker:** voiceover` is set on the scene:
A voiceover with [voice quality] speaks in a [tone] tone, "[exact dialogue from Storyboard]".)

Ambient: [setting tone + ambient sound cues].
(no subtitles, no captions)
` ` `
**Negative prompt:**
` ` `
no montage, no cutaways, no scene cuts, no flashbacks, no emotional escalation, no cinematic transitions, no burnt-in text, no captions, no on-screen titles, no face distortion, no morphing, no warping, no duplicate limbs, no extra fingers, no inconsistent lighting, no composite split-screen layouts, no disembodied hands.
` ` `
```

The canonical 12-element negative is the default. Append source-specific bans when the source has a known failure mode — e.g. `no composite split-screen layouts, no disembodied hands` for b-roll-heavy sources where Veo defaults to disembodied-hand recipe shots; `no second person in frame` for solo videos; `no kitchen background` for clinic-only videos.

### When the source is a video being decoded

The Veo Final Prompts in a decoded script are **reproduction prompts** — feeding them to Veo 3.1 should re-render a clip very close to the source clip. This is the ultimate test of "did we understand what's happening": if the assembled prompt re-renders the source faithfully, comprehension is complete; if it doesn't, something in the description grammar (v586) or the action_note (v540) was insufficient.

This makes the decode artifact self-validating. A reviewer can pick any clip from the source and any clip prompt from the decode, run the prompt through Veo, and compare frames.

### When the source is an idea being authored

The Veo Final Prompts in a generate-side script are **generation prompts** — the literal text Veo will see when the operator runs the script. The platform's prompt-builder will assemble equivalent prompts at job emission from the Storyboard section, so the explicit Veo Final Prompts section in the markdown is review-only by default — a discipline check that lets a human catch v540 violations (start-state restating, cross-clip refs, action_notes that describe what Veo already sees in the start frame) before generation burns credits. If a clip's pre-assembled prompt diverges from what the platform builds, the **Storyboard section wins** — the Veo final-prompts section is a derived view, not a source of truth.

### Migration

Pre-v587 decoded scripts in `raw/decoded_*.md` are valid as-is — they retain their HTML-comment headers as historical record. No retrofit required. New decodes from this commit forward MUST emit both `## Comprehension` and `## Veo 3.1 Final Prompts (per clip)` as structured sections.

The wiki's lint pass can flag pre-v587 decoded scripts that lack the structured sections, but the lint is advisory not blocking — the scripts remain functional under the older shape.

---

## Novelty-gate before HOOK lock (v591)

**Source: Milen Stanchev 2026-04-23 LiB innovation call.** The cheapest freshness filter.

Before authoring locks the HOOK, answer one question:

> **Have I seen this exact visual on any LiB Inspire account?**

- **Yes** → discard. Source a fresh visual via `wiki/strategy/viral-recreation-method.md` §Step 1 (1M-likes floor + outside-LiB-pool sourcing). Don't ship.
- **No** → ship.

This gate is the cheapest novelty check available — 30 seconds, prevents shipping a banana-smash-equivalent the audience has already skipped past. Pairs with the screenshot-AI-ideate workflow in `wiki/strategy/viral-recreation-method.md`.

Source quote (Milen 2026-04-23 ~00:03:51):

> *"The same angles, but with extremely different visual hook. Something that the market has not seen. Something you don't go and see on the LiB Inspire account."*

Generate-side rule. The decode side observes whatever the source filmed; freshness is a generate-side production decision.

---

## Motion-text-match in HOOK (v592)

**Source: Milen Stanchev 2026-04-23 LiB innovation call (~00:23:35).** The verb-object in the voiceover at second N must be matched by a visible motion in-frame at second N.

| Voiceover verb | Required visible motion |
|---|---|
| smash | visible smash motion |
| pour | visible pour |
| squeeze | visible squeeze |
| show | physical reveal |
| add | visible adding |

No abstract voiceover-over-static-shot in HOOK scenes. **Anti-pattern** (Milen ~00:46:33): static prop in hand while voiceover talks about the prop ("this banana") — flagged as "boring." Either the prop moves or the voiceover changes.

### Generalization beyond v539

v539 was weird-action-prop specific (SLAM/POUR/SPRAY/SMASH/GRIND). v592 generalizes to **verbal-only HOOKs** (SYMPTOM-CASCADE / NUMBERED-LIST / NOT-X DISMISSAL): the visual must still have motion (zoom, dolly, hands gesturing on emphasis words, prop reveal mid-line). Static talking-head at second 0–3 fails the gate.

### Decode-side application

When decoding, flag motion-text-match VIOLATIONS as anti-patterns in the action_note (the source author's failure, NOT a parser failure). Example: *"Static prop in hand while voiceover names the prop — v592 violation in source; reproduction should add motion."*

### Generate-side application

Per-scene action_note must verify v592 — the verb at second N has a visible motion at the corresponding beat marker in the action_note. Example: *"squeeze" verb lands on visible peak-squeeze beat in mid-clip — verified.*

---

## Image cardinality — universal (v594)

**Decoded shots ≠ generated images.** PySceneDetect detects *histogram cuts* (camera nudges, gesture peaks, small zoom shifts) — it does NOT detect compositions. **A producer films N setups; PySceneDetect logs M cuts where M ≥ N.** Image cardinality matches what was actually filmed (the producer's setup count), NOT what PySceneDetect threshold-detected.

### The three cardinalities (must be distinguished — every artifact, decode + generate)

| Cardinality | What it counts | Where it lives |
|---|---|---|
| **PySceneDetect shots (N)** | Histogram cuts | `manifest.json` — analysis units (motion / dense-frame / dialogue overlap) |
| **Distinct compositions (M, M ≤ N)** | Setups the producer actually filmed | `## Images` section — one `### Image M` block per composition |
| **Dialogue beats / clips (K)** | Voiceover units / Veo render units | `## Storyboard` (generate) / `## Veo 3.1 Final Prompts` (decode) |

### Decision per shot/scene

For each shot/scene during authoring, ask:

1. **Does setting / camera position / blocking change vs. the prior shot/scene?** — if YES, new image
2. **Is there a visible state-evolution** (recipe ingredient added, transformation Day-1→Day-14, prop transformed)? — if YES, new chained image (v580 / v541 / v590 chain-required)
3. **Otherwise** — reuse the prior image via `- **image:** image_N` pointing to the shared image. Veo's action_note arc handles the gesture / expression / dialogue variation within the clip from a single visual anchor.

Combined: **distinct composition OR state-evolution → new image; otherwise reuse**.

### Universal application — both decode and generate sides

Originally drafted as generate-side-only ("decode-side stays 1:1 with PySceneDetect shots — faithful observation"). **Revised 2026-05-05 to be universal**: faithful observation = describing what the source IS (the producer's actual setup count), NOT blindly transcribing PySceneDetect output. A 12-image decoded artifact for a 5-composition source is *less accurate* than a 5-image artifact, because the source HAS 5 compositions.

- **Decode** (`raw/decoded_*.md`): `## Images` section emits M descriptions; per-shot motion / dense-frame / dialogue analysis stays 1:1 in `manifest.json`.
- **Generate** (`videos/*.md`): `## Images` section emits M descriptions; `## Storyboard` scene blocks reference shared images via `- **image:** image_N` (multiple scenes can map to the same image).

### Typical cardinality

| Pattern | Image count |
|---|---|
| 3 talking-head scenes at same desk with different gestures | **1 image** (gesture variation in Veo) |
| 4 recipe scenes with cumulative ingredient state-evolution | **4 chained images** (v580) |
| 2 scenes with patient before/after outfit-change | **2 chained images** (v541) |
| HOOK in clinical room + EXPLAIN in office (different setting) | **2 distinct images** |
| Empty-desk close-up with declamatory then tent then palms-up gestures | **1 image** (gestures handled by Veo) |

For talking-head + recipe-pivot videos: typically **3-6 images for 8-12 shots**. For long-form videos with listicle structure: **6-8 images for 30-50 shots**.

### What stays per-shot (analysis grammars — these don't consolidate)

- v585 motion classification
- v588 dense-frame action-arc walk
- v589 Half C state magnitude
- Dialogue overlap timestamps
- Per-clip Veo final prompts (one per dialogue clip — each notes which underlying image it uses)

Only the `## Images` section consolidates.

### Worked examples

- `decoded_healthylifesage_DX7iVuRMzUM.md`: 12 PySceneDetect shots, 12 dialogue beats, **5 compositions** (clinic 3-person blocking shots 1-3 / cinnamon-pour shot 4 / lemon-squeeze shot 5 chain / lemon-aloft EXPLAIN shot 6 chain / empty-desk close-up shots 7-12).
- `decoded_herbal.health.tips_DX5QQZOhRd1.md`: 44 PySceneDetect shots, 25 dialogue beats, **6 compositions** (parking-lot HOOK shots 1-2 / Walmart-Dawn portrait shots 3-11 / Palmolive 12-19 / Joy 20-26 / Ajax 27-34 / car-with-Walmart-windshield 35-44). 86% compression ratio.
- `videos/dr-sage-belly-liver-husband-bystander.md` (generate-side): 8 scenes, **5 images** (clinic 3-person Scenes 1-3 / cinnamon-pour Scene 4 / lemon-squeeze Scene 5 chain / EXPLAIN Scene 6 chain / empty-desk Scenes 7-8).

### Compounds with v590 (generate side)

v590 says "chain only the 4 exceptions"; v594 says "generate only the distinct compositions." Together they minimize chain depth AND image count.

### Why this matters

| Reason | Impact |
|---|---|
| **Banana 2 generation cost** | Fewer generations per video (generate side) |
| **Visual consistency** | Banana 2 silently drifts persona / setting / bystander details across "near-identical" compositions when forced to generate them separately; sharing an anchor eliminates drift |
| **Faster ship time** | Fewer parallel slots needed |
| **Decode quality** | M-image artifact for an M-composition source IS more accurate than N-image artifact |

### Migration

Pre-v594 decoded artifacts (1:1 shot-to-image) are valid as historical record. New decodes from this commit forward MUST consolidate per composition. Existing `videos/*.md` artifacts authored under shot-to-image cardinality should be re-audited and consolidated where possible.

---

## Hybrid clip cutting — whisper vs timeline (v668)

**Source: 2026-05-08 owner directive** *"some cutted according to what is said in the markdown and some according to silence or whisper... we have transformation scenes, and then spoken scenes."*

A single video can mix two cut policies in the final-export trim step. Decode-side and lift-side both encode the choice per scene so downstream concat applies the correct trim to each clip independently.

### The two modes

| `cut_mode` | When to use | How the trim is computed |
|---|---|---|
| `whisper` | On-camera dialogue, voiceover narration with real spoken words | Existing apply_vad path: whisper transcribes the rendered clip, matches script words, trims to speech segments + decay pad |
| `timeline` | Transformation montages, music-only beats, SFX-only beats, beats with bracket annotation lines (`[upbeat music plays]`, `[SFX: door slams]`), scene's `line` is empty or annotation-only | Trim window from `frame_anchor` deltas between consecutive images. Scene N runs from `image_N.frame_anchor` to `image_{N+1}.frame_anchor`. Veo renders 4/6/8s, post-render trim cuts to the exact anchor delta. |
| `auto` (default when `cut_mode` is omitted) | Most scenes | Detection rule below |

### Auto-detection rule

For every scene, compute `cut_mode = auto` choice as:

```
if line is empty OR line matches /^\[.+\]$/ OR line.lower() in {"(silent)", "(no dialogue)", "(music)", "(sfx only)"}:
    cut_mode = "timeline"
else:
    cut_mode = "whisper"
```

The bracket-annotation pattern catches `[upbeat music plays]`, `[SFX: glass clink]`, `[ambient]`, etc. — these are stage directions for Veo's audio path, not spoken words, so whisper has nothing to match against.

### Storyboard syntax

```yaml
### Scene 1
- **image:** image_1
- **speaker:** voiceover
- **cut_mode:** timeline           # optional; auto-detected from `line` when omitted
- **line:** [upbeat music plays]
- **action_note:** ...

### Scene 5
- **image:** image_5
- **speaker:** on-camera
- **cut_mode:** whisper             # optional; default for on-camera with real dialogue
- **line:** If you want to know the recipe of the juice I gave to Josh,
- **action_note:** ...
```

When `cut_mode` is omitted, the auto-detection runs and picks `timeline` or `whisper` based on the line content. Explicit value overrides auto-detection — useful for edge cases (e.g. on-camera scene where the persona mouths along to the music; force `timeline` to keep the music beat aligned regardless of whisper output).

### Applied to a transformation video (your reference example)

5-scene transformation + CTA, 17s total:

| Scene | Speaker | Line | Auto cut_mode | Trim source |
|---|---|---|---|---|
| 1 | voiceover | `[upbeat music plays]` | `timeline` | frame_anchor 0.5s → 3.0s = 2.5s |
| 2 | voiceover | `[upbeat music plays]` | `timeline` | 3.0s → 5.0s = 2.0s |
| 3 | voiceover | `[upbeat music plays]` | `timeline` | 5.0s → 8.0s = 3.0s |
| 4 | voiceover | `[upbeat music plays]` | `timeline` | 8.0s → 12.0s = 4.0s |
| 5 (×3 lines) | on-camera | spoken CTA dialogue | `whisper` | whisper-VAD trim per line |

Final export concatenates all clips with their respective trims applied — same `apply_vad` filter-graph pipeline (v617), just a per-clip mode switch at the trim-decision step.

### Decode-side: emit cut_mode at decode time

When authoring `raw/decoded_*.md`, the decoder emits the `cut_mode` only when the auto-detection would be WRONG for the source's actual delivery (rare). Default behaviour: omit the field; downstream auto-detection from the line content is correct.

Concrete decode-side cases that warrant explicit `cut_mode:`:

- Source has `[music]` on the line but the persona is clearly singing along on-camera with whisper-detectable words → explicit `cut_mode: timeline` (lock to music beats, don't try to whisper-trim a sung line).
- Source has spoken voiceover that whisper consistently fails to transcribe (heavy accent, low SNR, language whisper isn't loaded with) → explicit `cut_mode: timeline` (rely on the source's own pacing instead).

### Lift-side / generate-side: emit cut_mode at authoring time

The video author adds `cut_mode: timeline` explicitly to:

- Transformation montage scenes (Day 1 → Day 30 → Day 67 → Day 120) where dialogue is just `[music]` — auto-detection already gets these right but explicit form is documentation for future-you.
- Hook scenes that need a precise N-second beat regardless of whisper's word-count interpretation (e.g. a 1.5s shock beat that whisper would extend to 4s+ if a stray cough leaked through).

### Validation gate

Before emitting any `videos/*.md` or `raw/decoded_*.md`:

- ✅ Every scene with `cut_mode: timeline` has both `frame_anchor` (its own) AND a successor scene's `frame_anchor` (or relies on the doc-level `total_duration` for the last scene)
- ✅ Every scene with `cut_mode: whisper` has at least one `- **line:**` with real spoken words (not bracket-annotation-only)
- ✅ When `cut_mode` is omitted, the auto-detection rule above is what the platform applies — author has read it and accepts the default

### Platform wiring (deferred to v668 lift implementation)

Decode-side / generate-side / lift-side rules land here NOW (this commit). The platform-side wiring (parser column, Clip.cut_mode field, apply_vad branch on cut_mode) ships separately when ready — same shape as v667's deferred lift-side wiring. The decode and authoring rules are forward-compatible: `cut_mode` is read by future platform code and ignored by current code, so nothing breaks pre-deploy.

---

## Per-image timestamp + delta metadata (v667) — decode-side first

**Source: 2026-05-08 owner directive** *"let's optimize the time frame extraction first from the decode, so we can later on learn how to recreate those videos in our system."*

Every `### Image N` block in `raw/decoded_*.md` MUST include two metadata bullets above the image-prompt body:

```
### Image N
- **frame_anchor:** <Xs>      # source-video timestamp (manifest.json shot start_time)
- **reference_image:** image_K | none
- **visual_delta:** <one-sentence prose>   # required when reference_image != none
- **Image prompt:**
…
```

`frame_anchor` carries the precise PySceneDetect shot start time for the FIRST shot mapped to this image (per v594 image cardinality, multiple shots may collapse into one image — use the EARLIEST shot's `start_time` from `manifest.json`).

`visual_delta` is the minimal one-sentence diff vs the prior chained image — required on every chained image, omitted on chain root. Lift-side eventually uses both:
- frame_anchor → orders the storyboard by source timeline (transformation videos: Day 1 / Day 30 / Day 67 / Day 120)
- visual_delta → drives Veo first-and-last-frame morph between consecutive frames

This rule is the FIRST half of the transformation-video pipeline (decode side). The lift-side platform parser does NOT yet read these fields — that's the SECOND half (deferred). Capturing the data at decode time means future-lift can be turned on without re-decoding the corpus.

Full rule + validation gate + worked examples: `wiki/meta/decode-grammar-checklist.md` §"Image metadata fields (v667)".

Optional third bullet: `- **narrative_lens:** <LENS>` — corpus-folklore tag for the rhetorical role (AUGMENTED-SYMPTOMS / HEALER-SHOWING-CURE / CTA-AUTHORITY / CONSPIRATORIAL-WHISPER). Documented separately in v621.

---

## LLM-agnostic Stage 4d decode interface (v595)

**Generalizes v589 Half A from a fixed cascade to a provider-agnostic interface contract.** Any vision-capable LLM that satisfies the Stage 4d input/output contract is a valid decode provider.

### The interface contract

**INPUT** the LLM receives:
1. PySceneDetect `shots.json` (timestamps + boundaries)
2. v588 dense-extracted PNG frames per shot (start + midpoint + end + 5 dense when v588 triggers fire)
3. v578 whisper `transcript.json` (per-segment dialogue + timestamps)
4. v585 `motion.json` (per-shot Farneback classification)
5. v589 Stage 4d prompt template specifying the schema

**OUTPUT** the LLM must produce — `stage4d.json`:

```json
{
  "shots": [
    {
      "shot_index": 1,
      "start": 0.0, "end": 5.30,
      "static_composition": "<v586 six-block compact description>",
      "action_arc": {
        "start_state": "<observable state at t=start+0.1>",
        "mid_state": "<observable state at midpoint>",
        "end_state": "<observable state at t=end-0.1>",
        "magnitude": "COMPLETE | PARTIAL | MINIMAL",
        "verbs_observed": ["<verb1>", "<verb2>", ...]
      },
      "audio": {"ambient": "<sound cues>", "music": "<music notes or 'none'>"},
      "veo_reproduction_hints": {
        "use_blend_to_next_scene": false,
        "needs_platform_future_image_end": false,
        "transition_prompt": "<cue>"
      }
    }
  ]
}
```

After Stage 4d output lands, **v594 consolidation** runs over it: per-shot `static_composition` strings cluster into per-composition image descriptions for the `## Images` section.

### Provider catalog

Providers ranked by recommended priority for a typical operator session:

| Rank | Provider | Cost | Setup | When to use |
|---|---|---|---|---|
| **1** | **Claude in-session** (Claude Code Read tool with PNG image support) | Free for operator (Claude Code subscription) | None — already running | **Default when operating inside Claude Code.** Used for the 2026-05-05 healthylifesage + herbal.health.tips decodes. |
| 2 | **LM Studio** (local, free) | Free | Install [LM Studio](https://lmstudio.ai); vision-capable GGUF (e.g. `gemma-4-E2B-it-GGUF` with mmproj); local server at port 1234 | Headless / batch decoding |
| 3 | **Gemini API** | ~$0.01 per 45s on `gemini-2.5-flash` | Set `GEMINI_API_KEY`; native MP4 upload at 1fps + audio + per-second timestamps | Best for motion-heavy / multi-character videos |
| 4 | **OpenAI GPT-4o-vision** | ~$0.01-0.02 per dense frame | Set `OPENAI_API_KEY`; image-by-image API | Operator already has OpenAI billing |
| 5 | **Anthropic Claude API direct** | Similar to GPT-4o per image | Set `ANTHROPIC_API_KEY`; image-by-image API | Headless / non-Claude-Code automation |
| 6 | **Ollama local with vision model** | Free | `ollama serve` + `llava` / `llama3.2-vision` | Operator wants free + headless |
| 7 | **OpenRouter** | Paid gateway | Single API key abstracts over many providers | Operator wants provider flexibility behind one bill |
| 8 | **Human-walk template** (fallback) | Free | None | Always-available fallback |

### Provider selection rule

```
if operating inside a Claude Code session:
    → use Claude-in-session (provider 1)  # default
elif LM Studio local server is up at localhost:1234:
    → use LM Studio (provider 2)
elif GEMINI_API_KEY is set:
    → use Gemini API (provider 3)
elif OPENAI_API_KEY is set:
    → use GPT-4o-vision (provider 4)
elif ANTHROPIC_API_KEY is set:
    → use Claude API direct (provider 5)
elif Ollama is running locally:
    → use Ollama vision model (provider 6)
elif operator has OpenRouter configured:
    → use OpenRouter (provider 7)
else:
    → write the human-walk template (provider 8) and prompt the operator to fill it
```

### Invocation

For provider 1 (Claude-in-session): no script needed. Claude walks the dense frames using the Read tool on `_decode_tmp/<source-id>/frames/shotNN_<label>_<t>s.png`, fills the schema in markdown, then v594-consolidates per composition while authoring the decoded artifact directly.

For providers 2–7 (automated):
```bash
python code/v589_video_understanding.py path/to/source.mp4
# auto-cascades through providers; force one with --provider lmstudio|gemini|openai|anthropic|ollama|openrouter|template
```

For provider 8 (human-walk template):
```bash
python code/v589_video_understanding.py path/to/source.mp4 --provider template
# writes _decode_tmp/<source-id>/stage4d_vlm.json with empty fields + frame paths
```

### What stays unchanged regardless of provider

- v578 whisper transcription (purpose-built for speech)
- v585 Farneback optical flow (purpose-built for motion)
- v588 ffmpeg dense frame extraction (purpose-built for frame I/O)
- v594 composition consolidation (deterministic post-processing)

Only the Stage 4d VLM step is provider-agnostic.

### Why v595 vs locking to v589 Half A

v589 Half A was implementation-locked to specific providers (LM Studio → Gemini → human-walk). v595 makes it a contract: any vision LLM that satisfies the schema is valid. The implementation script `code/v589_video_understanding.py` may need new `--provider {openai,anthropic,ollama,openrouter}` flags added when those providers are first used; for Claude-in-session no script change is ever needed (Claude reads frames directly via the Read tool).

### Migration

Existing `code/v589_video_understanding.py` cascade (LM Studio → Gemini → human-walk) is a v595-compliant subset. New providers can be added without breaking existing decodes.

---

## Hook-image power test (v598) — the single biggest viral lever

**Source: 2026-05-06 ChatGPT-output audit.** A bundle-driven create produced a menopause hot-flash video titled "When The Heat Hits At Night" with a generic woman-in-dim-bedroom HOOK image. No prop. No motion. No visual pun. No taboo trigger. No physical evidence. The 430KB of upstream rules were technically respected but the HOOK had zero scroll-stop power. **The hook image is the variable that decides 50 views vs 5M views.** Every other rule (chain-binding, action_note grammar, M ≤ N cardinality, persona consistency) is a ZERO-multiplier if the HOOK doesn't stop the scroll.

### The test (5 questions, all must answer YES before locking the HOOK image)

Before emitting `### Image 1` for any video, the LLM must walk through these five questions. If ANY answers NO, the hook is too weak to ship — replace it.

**Q1. PHYSICAL OBJECT in the hook?**

Is there a foreground prop being held, manipulated, or shown to camera (banana, seed jar, papaya, cabbage, salmon, pill bottle, mannequin tonsils, back-model, gold coin, stack of money, alarm clock, faucet, tape measure, surgical pen)?

Corpus rule: **NEVER a hook with no prop.** Every viral video in the 27-decode corpus has a foreground physical object in the first 1-2 seconds. Persona-talking-head with empty hands fails this test.

If NO — STOP. Find a prop that visually surrogates the niche (see Q3 metaphor library below).

**Q2. VISIBLE MOTION at second 0-2s?**

Not gestures — actual prop motion or state-change. SLAM / POUR / SPRAY / GRIND / SMASH / DROP / LIFT / REVEAL / SQUEEZE / TWIST. Or rapid camera move (push-in, dolly, whip-pan). Or visible prop transformation (banana wilting → upright, fat melting, stones dislodging, drop falling, mask lifting).

Corpus rule: **static talking-head at second 0-3 fails.** Per Milen 2026-04-23: *"Always in the hook, there should be motion. Being too static is just boring."* Even verbal-hook formats (SYMPTOM-CASCADE, NUMBERED-LIST) need motion — gesture-on-emphasis-words, prop-reveal-mid-line, zoom-in.

If NO — STOP. Add a weird-action verb to the action_note that creates visible motion at the start beat.

**Q3. VISUAL PUN, METAPHOR, or HYPER-SPECIFIC SCENE-MIRROR?**

The hook image should activate an associative leap that makes the topic pattern-match instantly. Three valid forms:

- **Visual pun** — banana = penis (ED), papaya/seed-jar = vitality, cabbage/fat-blob = visceral fat, water-flosser-on-tonsil-mannequin = stones, gold-coin = restored worth, drooping cucumber = limp
- **Niche metaphor object** — anatomy torso for visceral, back-model for sagging skin, faucet/hose for prostate stream, scalp-model for hair
- **Hyper-specific scene-mirror** — alarm clock 3:47am (nocturia), urinal cropped above waist (BPH-shame), wife-buying-for-husband UGC kitchen, two-different-men split-screen (62-vs-62 vitality contrast)

Corpus library of working puns/metaphors:
| Niche | Working visual surrogate |
|---|---|
| ED / male performance | Banana (now overused — use peeled banana / cucumber / tape-measure split-screen) |
| Belly fat | Anatomy torso + yellow fat blobs / purple-cabbage broth / belt-notch |
| Hair loss | Onion-half twist on scalp-model / Day-1-vs-Day-14 split |
| Tonsil stones | Water-flosser on mannequin tonsils with stone-deposits |
| Back lump | Female back-model with bra-line lump / Day-1-vs-Day-14 |
| Joint inflammation | Costco salmon / cabbage SLAM in produce aisle |
| Prostate / nocturia | Alarm clock 3:47am / faucet drip → fire hose / urinal crop |
| Hot flash / menopause | Thermometer climbing / ice-pack on forehead / soaked pillow / fan-on-bed / glass of water knocked-over at 2am |
| High cortisol | Belt-tightening / jeans-zipper-fail / stairs-slow-mo |
| Liver detox | Yellow torso-organ + bile-color shift |
| Vitality / "morning signal" | Saffron threads + warm-water-bowl / gold-coin in palm |
| Brain fog | Glass of water cloudy → clear / fogged window finger-trace |

If NO — STOP. The hook is just "person talks about a problem." Pick a surrogate from the table or invent one with the same associative-leap mechanic.

**Q4. NOVELTY GATE (v591) — has this exact visual been seen on LiB Inspire or any operator's account?**

Walk it: have I seen *this exact composition* on any account in the niche? If yes, discard. If no, ship.

**Banana-smash and banana-tape-measure are now overused** in male-ED (Milen 2026-04-23). Use only with a fundamentally different visual frame (split-screen, time-lapse, anatomical overlay) or pivot to symptom-show-open / A-vs-B-compare / outside-niche-viral-recreation.

If a corpus persona uses a specific prop heavily (e.g. master-salvora's Costco produce SLAM) and that operator is still active on LiB Inspire, that exact prop+setting combo fails the gate for new accounts.

If NO (i.e. yes, it's been seen) — STOP. Source a fresh visual via [[viral-recreation-method]] §Step 1.

**Q5. MOTION-TEXT-MATCH (v592) — does the verb in the voiceover at second N match a visible motion at second N?**

Verb "smash" → visible smash motion at the same second. Verb "pour" → visible pour. Verb "show" / "watch" → physical reveal or push-in. Verb "you wake up at 3 a.m." → cut to alarm clock OR character-rising-from-bed motion.

Anti-pattern: voiceover says *"this is what high blood pressure looks like"* over a static torso shot — the voiceover names a thing but the visual doesn't reveal it. Either change the voiceover to match the static visual, or add a reveal motion that matches the voiceover.

If NO — STOP. Resync verb-and-motion at the same beat.

### Hard rule for niches with no inherent visual

Some niches have no inherent visual surrogate (hot flash, brain fog, anxiety, insomnia, internal pain). These niches are HOOK-HARD — the corpus has fewer working examples and most operator failures occur here.

**For HOOK-HARD niches the LLM MUST manufacture a visual** — never default to "person talks about feeling X." Required: pick from the niche-specific surrogate table above (Q3) or one of these four manufactured-hook forms:

1. **Wearable evidence prop** — thermometer, pulse-oximeter, sleep-tracker on wrist showing alarming reading
2. **Environmental witness prop** — soaked pillow / sheets-in-the-dryer / fan-set-to-high / glass-of-water-at-bedside-knocked-over
3. **Body-anatomy surrogate** — visualization torso showing sweat-glands firing / brain-cross-section foggy → clear / hormone-graph crashing
4. **Scene-mirror cold-open** — hyper-specific time + place that mirrors viewer's literal nightly experience (same as Hook #4 in Male 40-70 ranked list — "3:47 a.m. dark bedroom")

The Korella saffron-vitality angle for menopause-coded videos has a built-in visual: **saffron threads + warm-water bowl + gold-color reveal** as the prop, not the hot-flash itself. Lean into the validated F→F-about-M cell with the saffron prop rather than fighting the unvisualizable internal symptom.

### What this fixes

The "When The Heat Hits At Night" failure mode: LLM picked an internal symptom (hot flash) as the hook subject without manufacturing a visual surrogate. Persona Ogheci was alone in a dim bedroom — no prop, no motion, no pun. Even if every other v-rule was respected, the video would die in the algorithm.

v598 elevates the hook-image-power decision from "implicit" (buried in hook-patterns.md catalog) to "explicit gate that blocks output." The bundle TASK block now self-validates against this test before emitting.

### Q6. BACKGROUND AUTHORITY MATCH — does the setting confer the persona's authority?

**Source: 2026-05-06 owner observation** *"and study also the backgrounds, especially because they have to resonate with the audience and have to give authority."* The HOOK frame is not just the foreground prop — the BACKGROUND signals what kind of authority the persona has. A clinical persona in a kitchen sounds like a stranger; a folk-wisdom elder in a sterile clinic sounds like a fraud. The setting MUST match the persona's authority type or the entire hook collapses.

**Corpus-grounded persona × setting authority pairings** (24-decode audit, 2026-05-06):

| Persona archetype | Required setting | Required visible anchors (0-2s) | What BREAKS authority |
|---|---|---|---|
| **Clinical doctor** (dr-kim, dr-sage, dr-aesthetic, podiatrist-blood-sugar, black-male-doctor, black-female-practitioner office-flip) | T2 clinical exam room or T2 diploma office | At least ONE: framed diploma (gold frame, classical type) / US flag / anatomy poster / equipment cart (white drawers) / exam stool / IV pole / surgical pen in hand | Filming in domestic kitchen → MD authority collapses; viewer reads "stranger giving advice" |
| **Folk-wisdom elder** (master-chen, master-salvora kitchen-variant, old-earl, icelandic-elder, master-shen) | T0/T1 honey-oak farm-bench OR Caribbean sunroom OR rustic kitchen | Honey-oak/barn-board wall + 3+ herb jars + ceramic teapot/copper kettle + window with outdoors visible + visible patina/weathering | Filming in sterile clinic → "50 years on the farm" credibility dies; lived-experience authority requires lived-environment props |
| **Retail-witness operator** (master-salvora Costco-variant, master-shen Walmart-variant) | T0-retail Costco produce / Walmart aisle | Visible store signage (Costco yellow spark / Walmart logo) + fluorescent industrial ceiling + blurred ordinary shoppers (social-proof) + actual store-stocked product on display | Filming in studio → "ordinary-shopper-with-secret" credibility evaporates; secret-in-plain-sight requires real public space |
| **Caribbean herbalist** (rastajahmeil, mama-rasta) | T0/T1 Caribbean sunroom | Bamboo wall + Rasta + US flags + 3+ herb jars on shelving + honey-oak table + warm amber light | Removing dreadlocks/tam/flags → ancestral lineage-claim is visual not narrative; persona without visual lineage anchors fails |
| **Modern-clinic sexy-doctor** (nuri, black-female-practitioner kitchen-flip) | T0 clean kitchen for HOOK + T2 office for OUTRO (DUAL-FLIP) | HOOK: clean kitchen counter + window-soft light + warm domestic; OUTRO: framed credentials + clean desk + window with skyline | Single-setting reduction (kitchen-only OR office-only) → loses 33% trust per corpus dual-flip evidence; both warmth + credentials needed |

**Hook-time background discipline (0-2s requirement)**:
- The 0-2s frame MUST contain ≥2 props signaling the authority type — never ambiguous
- Lighting mood must match: clinical = cool-white LED / folk = warm domestic gold / retail = fluorescent harsh / luxury = window-bright
- Persona body language must match: clinical = positioned with equipment-or-patient / folk = hands-on-work / retail = operating-product / luxury = relaxed

### Q7. AUDIENCE RESONANCE — does the setting look like the audience's world or a credible authority space?

The setting carries TWO simultaneous signals: it must either look like *"the world I live in"* (peer / kitchen-table / Costco aisle resonance) OR *"the credible expert space I trust"* (clinic / diploma office / herbalist sanctuary). Setting that achieves neither = forgettable. Setting that achieves both via DUAL-FLIP = highest trust per corpus.

**Corpus-validated audience × setting resonance matrix**:

| Audience axis | Resonates with | Bridges to via DUAL-FLIP | Anti-resonance (avoid) |
|---|---|---|---|
| **Women 40+ (largest segment)** | Domestic bright kitchen (warm peer) + clinical exam-room (credible) | Kitchen-warmth → office-credentials hard cut | Kitchen-only = amateurish; office-only = cold/pharmaceutical |
| **Male 40-70 US (Mike Henderson)** | Retail warehouse (accessible "I shop here") + clinic (male-doctor-male-patient = shame removed) + luxury apartment (success-status fantasy) | Costco accessibility → clinical proof | Pure home-kitchen authority (reads as wife giving advice not "the guy") |
| **F→F-about-M** | Kitchen + office DUAL-FLIP (Korella saffron canonical: Black-female-practitioner kitchen-warmth → diploma office credentials) | Kitchen recipe-warm → office credibility-anchor | Single-setting fails per 4-corpus-instance evidence; bedroom is non-resonant for transactional/conspiratorial F→F register |
| **Neutral / male+female** | T2 clinical authority (Dr-Sage husband-skeptic exam room → office cinnamon-pour) | Exam-room-with-bystander → solo-office recipe | Pure folk-wisdom register has narrower resonance for mixed audiences |
| **Black women** | Caribbean sunroom (ancestral) + bamboo + cultural flags | (single-setting often sufficient when persona+lineage anchors are strong) | Generic clinic = lineage authority lost |

**Hook-image rejection criteria (Q6+Q7 combined)**:

- Background not visible in HOOK frame? → REJECT — operator sees only persona-floating-on-blur, no authority signal
- Background visible but ambiguous (could be any room)? → REJECT — needs ≥2 anchor props
- Background contradicts persona archetype? → REJECT (clinical persona in kitchen / folk-elder in clinic / retail-witness in studio)
- Background resonates with NO audience (luxury venue for Mike Henderson cold-prostate ad, sterile clinic for Black-women-Caribbean-herbalist niche)? → REJECT
- Background is "generic talking-head studio with bokeh"? → REJECT — zero authority signal, zero resonance, this is the failure mode of the "When The Heat Hits At Night" Ogheci-bedroom hook

### Q8. PSYCHOLOGICAL MECHANISM STACK — name the 4-mechanism wrapper before locking the hook

**Source: 2026-05-06 owner observation** *"there's no action, nothing happens. you have just a list of hooks, but you didn't understand the psychology behind it."* The first v598 ship gave Q1-Q7 (object + motion + pun + novelty + match + bg-authority + audience-resonance) but missed the **psychology** that makes the corpus videos actually viral. A hook can pass Q1-Q7 (lady at counter doing nice things with saffron, with proper background) and still be **psychologically dead** because no shame is being mediated, no taboo is being violated, no agent is acting, no curiosity loop is open.

Every viral hook in the 24-decoded-video corpus stacks **four psychological mechanisms simultaneously**. Q8 forces the LLM to NAME each mechanism explicitly before locking the hook image; if any of the four is missing, the hook is psychologically dead even if Q1-Q7 pass.

**The 4-mechanism stack (corpus-derived):**

| Mechanism | What it does | Corpus examples |
|---|---|---|
| **Shame-proxy** | Taboo object lets the viewer face the forbidden subject without the shame of confronting it directly. The proxy CARRIES the shame; the persona/audience can therefore TALK about the underlying problem. | Banana = penis (corella saffron, salvora costco banana). Cabbage = visceral fat (salvora costco cabbage). Distended belly = metabolism failure (dr_kim_belly_burn_male). Mannequin tonsil-stones = bad breath shame (oldearl_tonsil_healer). Soaked pillowcase = night-sweat suffering (proposed for menopause-saffron). |
| **Violent-act / spectacle** | Force-verb on the proxy creates a 0-2s shock moment that interrupts the scroll. NOT a gentle gesture — a SLAM, RIP, SPRAY, GRIND, SHATTER, CASCADE, that creates visible damage or dramatic state-change. | Banana-bunch SLAM-COLLAPSE pyramid (salvora costco banana). Onion-SLAM-on-crown + JUICE-SPRAY 3-4 droplets (dr_kim_hair_regrowth_male). Sugar-VIOLENT-VERTICAL-JET-ATOMIZE engulfs cockroach (dr_kim_cockroach_bait). Salmon-SLAM + ICE-SPLASH + CLEAVER swing (salvora_costco_salmon). Surgical-marker PRESS + TRACE on back lump (dr_kim_back_lump). |
| **Agent-of-change spectacle** | The product (or the recipe ingredient) ENTERS THE FRAME and visibly ACTS — dissolves, unfolds, ignites, transforms, sprays. The viewer sees the agent doing something, not just sitting there. | Tea POURED on torso → fat liquefies (belly-fat tea). Saffron capsule CASCADES into glass → red-orange streaks bleed through liquid (nuri-saffron-ed). Onion juice SPRAYS into hair → matting visible (dr_kim_hair_regrowth_male). Surgical marker LINE traced → curved contour appears (dr_kim_back_lump). Sugar JET fills jar → cockroach engulfed (dr_kim_cockroach_bait). |
| **Taboo direct-address** + bystander/witness | The persona breaks the fourth wall with a forbidden statement, often invoking a third-party witness/bystander (husband-asleep, wife-buying-for-husband, your-doctor-doesn't-know) to mediate the shame and create a conspiratorial register. | "Don't show this to your man too often" (rastajahmeil_fat_melt). "Her husband did not believe me" (decoded_healthylifesage). "What I just told my urologist left him speechless" (corpus #7). "Your husband sleeps through this" (proposed for menopause-saffron). |

**Required LLM authoring step (before locking the hook):**

Output an explicit "Psychology stack" block in your reasoning (not in the final markdown — in the working draft) naming each of the four mechanisms and what fills it for THIS hook:

```
## Psychology stack — HOOK
- Shame-proxy: <object> = <forbidden subject>
- Violent-act: <force-verb> on <object> creating <visible spectacle>
- Agent-of-change: <product/ingredient> visibly <action> in-frame
- Taboo direct-address: "<line>" + <bystander/witness>
```

If you cannot fill any one of the four lines, the hook is dead. Replace the prop, replace the motion, replace the line, or pull a different corpus pattern.

**Corpus-grounded mechanism stacks per niche** (use as templates):

| Niche | Shame-proxy | Violent-act | Agent-of-change | Taboo direct-address + bystander |
|---|---|---|---|---|
| ED / saffron-vitality (F→F-about-M) | Banana = penis | Saffron capsule CASCADE into glass + red streaks bleed | Saffron streaks dissolving into amber water | "Add up to two inches to your banana overnight" + husband as implicit listener |
| Belly fat / visceral clinical | Distended belly | RIGHT-index TAP + dismissive flick | Tea POUR over torso = fat liquefies | "He came in with this dad bod" + husband-skeptic bystander |
| Hair loss (male) | Bald-spot crown | Onion-SLAM + GRIND + JUICE-SPRAY | Onion-juice MATTING visible in hair | "Your wife stopped looking at your hairline weeks ago" + wife-as-bystander |
| Tonsil stones | Mannequin-tonsil with stone-deposits | Water-flosser SPRAY-blast | Stones DISLODGE + float away | "If your breath smells like this you're ruining every conversation" |
| Back lump (DAY1/14) | Bra-line back lump | Surgical-marker PRESS + TRACE | Marker line CARVES contour around lump | "She didn't pay $8000 for surgery" + wife-as-self-reflexive |
| **Menopause / hot-flash → saffron-vitality** | **Soaked pillowcase = night-sweat suffering** | **Pillowcase SLAM on marble + water spray-arc + saffron tendrils SPATTER** | **Saffron threads UNFOLD gold-amber tendrils in the spatter water** | **"Your husband sleeps through this. Saffron is what wakes you up." + sleeping-husband-silhouette in doorway** |

**Worked example — the menopause-saffron rewrite (post Q1-Q8)**:

A first-pass v598 hook ("woman in clean kitchen with saffron bowl, gentle pinch") passed Q1-Q7 but FAILED Q8 (no shame-proxy, no violent-act, no agent-of-change moment, no bystander). The Q8-corrected version stacks all four:
- Shame-proxy: soaked white silk pillowcase carrying the night-sweat suffering
- Violent-act: pillowcase SLAMMED onto marble — water droplets in mid-air arc-spraying across the saffron bowl
- Agent-of-change: saffron threads visibly UNFOLDING into gold-amber tendrils in the spatter water (the product literally acts)
- Taboo direct-address: "Your husband sleeps through this. Saffron is what wakes you up." + sleeping-husband-silhouette visible through bedroom doorway in soft-focus background

Q8 is what separates "lady doing nice things" from "scroll-stop spectacle." Anatomical compliance with Q1-Q7 is necessary but not sufficient — without the 4-mechanism psychology stack, the hook is dead.

### Decision rule summary (expanded — 8 questions)

```
For every videos/*.md draft, Image 1 / Scene 1 must pass all 8:
  Q1. PHYSICAL OBJECT in HOOK foreground?         — yes/no
  Q2. VISIBLE MOTION 0-2s?                        — yes/no
  Q3. PUN / METAPHOR / SCENE-MIRROR?              — yes/no
  Q4. NOVELTY (v591) PASSED?                      — yes/no
  Q5. MOTION-TEXT-MATCH (v592)?                   — yes/no
  Q6. BACKGROUND-AUTHORITY MATCHES PERSONA?       — yes/no  (≥2 anchor props visible 0-2s)
  Q7. SETTING RESONATES WITH AUDIENCE?            — yes/no  (peer-world OR credible-authority OR DUAL-FLIP)
  Q8. PSYCHOLOGY STACK — all 4 filled?            — yes/no  (shame-proxy + violent-act + agent-of-change + taboo direct-address)

If ALL yes → ship.
If ANY no → reject hook. Q8 fails most often when Q1-Q7 pass with
"gentle gesture + clean kitchen + soft saffron pinch" — that's the
psychologically-dead trap.
```

---

## Decoder narrative lens + caption ban (v621)

**Source: 2026-05-06 owner directive** *"for the decode always make the decoder look at the video from the perspective of: the healer is showing a cure, or showing augmented symptoms or grabbing attention... and specify to not include captions in the decoded images or the created images."*

Two unrelated additions bundled into one v-rule because both surfaced from the same example (`raw_decoded_01_amish_house_back_acne_mask.md` had no narrative-lens framing AND included `"yellow burned-in captions at the lower third"` in its image description, which would generate baked-in captions if used as a Banana 2 prompt).

### v621a — Decoder narrative lens (3 categories)

When decoding a competitor video, every image description must be framed through ONE of three narrative lenses. The lens shapes how the decoder DESCRIBES the image — which details to emphasize, which to skip.

| Lens | When | Decoder emphasis |
|---|---|---|
| **HEALER-SHOWING-CURE** | recipe steps, product reveals, mechanism explanations, anatomy-pointer scenes, ingredient-add scenes, the cascade moment | The PRESENTATIONAL gesture — what the persona is showing the viewer. Hand position relative to prop. Camera angle that proves the cure. Product placement, label visibility, recipe-step state. Mid-action POSE that demonstrates the remedy. |
| **AUGMENTED-SYMPTOMS** | HOOK shock images, problem-callouts, exposed before-state (back acne, varicose veins, distended belly, soaked pillow), thermometer readings, glucose-meter readings, anatomical magnification | The AMPLIFIED visible problem — what the camera is forcing the viewer to see. Crop tight on the symptom. Background props that contextualize (medical room, kitchen, garden). NO solution visible yet — the "before" must read as raw and unresolved. |
| **GRABBING-ATTENTION** | scroll-stopper cold opens, weird actions without specific cure context, persona introduction shots, transition/movement frames, decorative cuts | The PURE SPECTACLE — motion, magnitude, novelty (per v600 cartoon-physics). What makes the thumb stop. Decoder names what's startling without binding it to a remedy or symptom yet. |

**Per-image declaration**. Every `### Image N` block in a decoded `raw/decoded_*.md` artifact must include:

```
- **narrative_lens:** HEALER-SHOWING-CURE
```

(or `AUGMENTED-SYMPTOMS` or `GRABBING-ATTENTION`). The field goes alongside `reference_image:` and `product_image:` in the metadata block.

This is a **decoder-side mindset enforcement**, not a parser-required field. The platform parser ignores it. But the decoder has to CLASSIFY before describing — that's the whole point.

**Why this matters**. Pre-v621 decoders described images as flat scene-inventories ("the persona stands at the left, the patient sits on the right, the kitchen is in the background"). Post-v621 the decoder asks "what is this shot DOING for the viewer?" first, then describes. The result is sharper image prompts that downstream lifts can adapt without losing the rhetorical purpose of the shot.

### v621b — Caption ban (decode + create + lift)

Image prompts must NEVER describe caption text that appears in the source video.

**FORBIDDEN phrases in any image prompt body**:
- *"yellow burned-in captions at the lower third"*
- *"white subtitle bar across the bottom"*
- *"large overlaid text reading 'X'"*
- *"caption: 'Try this remedy!'"*
- ANY descriptor of post-production text overlays

**Why**: captions get added at the platform level (post-generation, via the video editor's caption layer). Including caption descriptors in the prompt makes Banana 2 BAKE the caption into the pixels — which then can't be edited, translated, or A/B-tested. Pixel-baked captions are also low-fidelity (usually wrong font, wrong wrap, wrong timing) and look amateur.

**Scope**:
- **Decoder** (`raw/decoded_*.md`): when the source video shows captions, IGNORE them in image descriptions. Capture caption TEXT in the dialogue lines (it usually mirrors the spoken voiceover anyway), but never in the visual description of any image.
- **Create / Lift** (`videos/*.md`): same — never describe captions in image prompts. Captions are produced separately by the video editor from the dialogue lines.

**Concrete example** (from the amish-house-back-acne decode):

```
Pre-v621b (FORBIDDEN):
"Style: natural iPhone HDR, bright homemade remedy look, clinical-shock hook,
yellow burned-in captions at the lower third."

Post-v621b (REQUIRED):
"Style: natural iPhone HDR, bright homemade remedy look, clinical-shock hook."
```

Same description; caption descriptor removed.

### Pre-output validation gate

Before emitting any decoded artifact OR any videos/*.md draft:

- ✅ **Every image declares `narrative_lens:`** (decode-side; create/lift can declare it too as documentation but not required by parser).
- ✅ **Zero caption descriptors** anywhere in image prompt bodies. Mechanical check: grep for `caption`, `captions`, `subtitle`, `subtitles`, `overlay text`, `lower third` — should return zero hits.

### What v621 does NOT change

- v614 corpus-pattern + adaptation_map — preserved (decoders still classify into Pattern A/B/C/D/E AND into one of the 3 narrative lenses; both classifications are useful).
- v615 em-dash ban in dialogue — preserved.
- v619 auto-infer normalization — preserved (operates on `### Image` blocks regardless of narrative_lens).
- Caption HANDLING in the platform — captions are still added post-generation by the editor; v621b just bans them from the image-generation prompt.

---

## Non-persona character identity prose is mandatory (v669) — close the v602 over-application loophole

**Source: 2026-05-08 owner directive.** Decoder/author over-applied v602 (persona description ban) and stripped identity descriptors from a non-persona patient ("Josh" — visible Black male in source). Result: Nano Banana 2 hallucinated the patient's identity differently across the 4 transformation frames; the patient's race / build / face drifted between Day 1 and Day 120, killing the transformation continuity.

### The misread

v602 forbids racial / ethnic / age descriptors for **the persona** because the persona's identity comes entirely from the uploaded reference image. v602 does NOT extend to non-persona characters. For non-persona characters there is NO UPLOAD — the prose is the only source of truth Banana 2 can bind to.

The author/decoder's fault mode: applies v602's gender-neutral discipline to ALL humans in the prompt out of habit, leaving the non-persona character un-anchored.

### The rule

Every non-persona character described in an image-prompt body MUST be anchored with these three identity descriptors at FIRST appearance:

1. **Race / ethnicity** — `Black male`, `East Asian female`, `White elderly woman`, `Latino young man`. Use plain demographic language; do NOT euphemize ("brown-skinned", "fair complexion") because Banana 2's training data binds to the demographic terms directly.
2. **Visible age band** — `young adult` (20s) / `middle-aged` (40-50s) / `older` (60+) / `elderly` (70+). Skip when the source frame is too tight to show an age signal.
3. **Body type / build** — `heavy-set`, `slender`, `athletic`, `frail`, `muscular`. Required when the body is the subject (patient transformation, before/after).

When the character chains across multiple images (transformation montage, multi-shot dialogue), the identity descriptors lock at FIRST appearance. Subsequent images preserve via reference_image continuity AND a one-line restatement of the locked identity to prevent Banana 2's drift between independent generations.

### The fix-pattern from the failure case

❌ Pre-v669 (Josh as patient, transformation montage):
```
A heavy-set patient sits drinking from a glass. His exposed midsection features a massive
distended belly...
```

✅ Post-v669:
```
A heavy-set Black male patient (mid-40s) sits drinking from a glass. His exposed
midsection features a massive distended belly...
```

In every chained image (Day 30, Day 67, Day 120):
```
The same Black male patient (mid-40s), now slimmer / now flat-stomached / now shredded.
[describe the visual delta]
```

### Asymmetry chain (v602 + v610 + v622 + v669)

| Character role | Race / ethnicity | Age band | Build | Gender pronouns |
|---|---|---|---|---|
| Main character (upload-bound persona) | FORBIDDEN (v602) | FORBIDDEN (v602) | FORBIDDEN (v602) | FORBIDDEN (v610) |
| Patient / customer / bystander (non-persona, no upload) | **REQUIRED (v669)** | **REQUIRED (v669)** | **REQUIRED (v669) when body is subject** | REQUIRED (v610) |
| Symptom-bearer on AUGMENTED-SYMPTOMS / HEALER-SHOWING-CURE lens | **REQUIRED (v669)** | **REQUIRED (v669)** | **REQUIRED (v669) — body IS the subject** | REQUIRED (v610) + symptom feature (v622) |

The persona is upload-swapped at lift time; the patient is NOT — the patient's prose IS the only source of truth for Banana 2 to anchor on. Vague non-persona descriptions on diagnostic / transformation scenes produce drifted Banana 2 output and the visual continuity collapses.

### Pre-output validation gate

Before emitting any image-prompt body containing a non-persona character:

- ✅ First-appearance image: race + age + build (when applicable) explicitly named
- ✅ Chained appearances: identity restated in one line, then the visual_delta
- ✅ No use of v602's gender-neutral / identity-stripped phrasing for non-persona characters
- ✅ Race uses plain demographic terms (`Black male`, `East Asian female`), not euphemisms

### What v669 does NOT change

- v602 main-character description ban — preserved unchanged. Persona identity still comes from the upload, never from prose.
- v610 main-character pronoun ban — preserved unchanged.
- v622 symptom-feature exaggeration — still required; v669 adds the identity layer underneath the symptom layer.
- v553.1 never-describe-persona — preserved unchanged.

---

## Mechanism in active use across transformation montages (v670)

**Source: 2026-05-08 owner directive.** Decoded prompt for a 4-frame transformation montage (Day 1 → Day 30 → Day 67 → Day 120) had the patient HOLDING the juice glass in static poses across all 4 frames. The actual rhetorical mechanism of a transformation video is *the patient consuming the cure WHILE the transformation happens*. By defaulting to a static "holding" pose, the decoded action_note flattened the scene's mechanism — the visual causal chain (drink → result) was lost.

Owner: *"the visual story of the montage is literally him consuming the cure while the transformation happens. By leaving out the active drinking, I missed the actual mechanism of the video."*

### The rule

When a scene is part of a TRANSFORMATION MONTAGE (multi-frame chained progression with consistent persona/patient and a named cause/mechanism — juice, capsule, cream, supplement, etc.), the action_note for EACH frame MUST show the mechanism in **active use**, not static possession. The static "holding" pose is a default trap.

| ❌ Static (forbidden in montage) | ✅ Active (required in montage) |
|---|---|
| "the patient holds the juice glass" | "the patient takes a steady drink from the glass, glass tipped to lips mid-swallow" |
| "the patient sits with the bottle" | "the patient pours saffron drops into the glass, mid-pour" |
| "the cream is on the counter" | "the patient applies the cream to the affected area, fingertips spread mid-motion" |
| "the capsule is in his hand" | "the patient swallows the capsule, head tilted back, water glass following" |

### Per-frame motion variation

The mechanism repeats across frames but the MOTION VARIES so each frame reads as a separate moment, not a re-render of the same pose:

```
Image 1 (Day 1):    raises glass to lips, mid-tilt — first sip beat
Image 2 (Day 30):   mid-swallow with throat motion visible — sustained-use beat
Image 3 (Day 67):   lowering glass after drink, glass at chest — completion beat
Image 4 (Day 120):  raising glass in toast/celebration — confident-mastery beat
```

Each frame is the SAME mechanism (drinking) but a DIFFERENT moment of the action — preserves continuity AND signals time progression.

### action_note three-beat enforcement

In a transformation-montage scene, the three beats each show a phase of the mechanism in use:

```
[Start beat 0-2s]  Patient raises the glass, tilts to lips.
[Mid-clip beat 3-5s]  Mid-swallow, glass tipped fully, throat motion visible.
[End beat 5-8s]  Glass lowered, expression of relief / satisfaction.
```

NOT acceptable in a transformation montage:

```
[Start beat]  Patient sits holding the glass.
[Mid-clip beat]  Patient continues holding the glass.
[End beat]  Patient still holds the glass.
```

### When v670 does NOT apply

- Non-transformation scenes (HOOK without before/after, single-state CTA, single-scene context shot) — static poses are fine when there's no causal chain to show.
- The PERSONA's actions (the practitioner gesturing toward the patient) — v670 governs the patient/customer's active use of the mechanism, not the persona's commentary.
- Dialogue-only scenes where the mechanism is named verbally but not visible (rare in this corpus).

### Pre-output validation gate

Before emitting any transformation-montage scene's action_note:

- ✅ Each frame's three-beat action shows the mechanism in motion, not static possession
- ✅ Across the montage, motion varies frame-to-frame (raise → swallow → lower → toast, not all "holds")
- ✅ The mechanism's named cause appears in at least 2 of the 3 beats per frame (anchors the visual to the rhetorical claim)

### Lift-side authoring + decode-side observation

Decode side: when observing a transformation montage in a source video, the decoder writes per-frame motion (drinking-mid-tilt / mid-swallow / lowering / toasting) — never collapses to "patient holds glass". v588 dense-frame walk + v589 Half C state magnitude provide the per-frame motion data; v670 is the prose-side discipline to use that data.

Lift side / generate side: when authoring a transformation montage from scratch, the author writes the mechanism's active use into each frame's action_note BEFORE writing dialogue or framing — the active use IS the rhetorical spine of the montage.

### What v670 does NOT change

- v585 motion classification — preserved (still classifies camera + subject motion per shot).
- v588 dense-frame walk — preserved (provides the per-frame state-arc data v670 turns into prose).
- v589 absolute-magnitude grammar — preserved (v670 governs ACTION verbs; v589 governs the resulting STATE).
- v540 action_note three-beat structure — preserved (v670 fills in WHAT each beat shows, doesn't change the three-beat shape).
- Non-montage scenes — unchanged; static poses fine when not in a transformation chain.

---

## Symptom-feature exaggeration on non-persona characters (v622)

**Source: 2026-05-06 owner observation.** Decoded prompt for `amish-house` chin-pointing scene read *"her chin raised slightly and her eyes locked to the camera."* The actual source frame shows a patient with a notably full / sagging lower-jaw + jowl drop, and the practitioner's index finger is pressed firmly into the underside of that chin. The whole rhetorical point of the scene is "IF YOUR CHIN LOOKS [LIKE THIS]" — a C-DIAGNOSTIC-PIVOT lifted on the AUGMENTED-SYMPTOMS lens. The decoder flattened the diagnostic feature into generic posture filler. A lift of that prompt would generate a clean-jawed patient and the diagnostic pivot would have nothing to land on.

Owner: *"we need to exagerate the patient characteristics mentioned in the scene."*

**Bug class.** v621 enforces lens classification (HEALER-SHOWING-CURE / AUGMENTED-SYMPTOMS / GRABBING-ATTENTION) but does not enforce *symptom-feature description* at the per-character level. The decoder identifies the lens correctly and then writes the patient as if the lens were neutral. v622 closes the gap.

### The rule

When a scene's `narrative_lens` is `AUGMENTED-SYMPTOMS` OR `HEALER-SHOWING-CURE`, **OR** when the scene fits Pattern C (DIAGNOSTIC-PIVOT), **AND** the source frame shows a non-persona character with a body part being pointed at, pressed, framed, circled, magnified, or visually centered, the decoded character description MUST:

1. **Name the body part** being indicated (chin, jowl, under-eye, neck, scalp, knuckle, calf, ankle, belly, cheek, forehead, hairline, lip, eyelid, etc.).
2. **Describe its visually-emphasized state in specific exaggerated terms** — match what the source camera is forcing the viewer to see. The source video EXAGGERATED it for the hook; the decoded prompt must preserve that signal.
3. **Never use neutral posture filler** as a substitute for the actual symptom description. Phrases like *"chin raised slightly"*, *"head tilted"*, *"face turned toward the camera"*, *"eyes looking down"* are forbidden when the frame is a tight crop on a symptom — they describe the *pose*, not the *feature*.
4. **Match the framing intensity.** If the source crops tight on the symptom, the description must be loud about it. If the source uses wider framing, the description can be calmer — but still names the feature.

### REQUIRED examples

| Source frame shows | FORBIDDEN (neutral filler) | REQUIRED (symptom-exaggerated) |
|---|---|---|
| Practitioner's finger pressed into a patient's full lower jaw | "her chin raised slightly" | "a full, sagging lower jaw with visible jowl drop, the practitioner's index finger pressed firmly into the soft underside of the chin" |
| Camera tight on under-eye area | "her eyes looking down" | "puffy, swollen under-eye bags with dark hollows beneath, fine crepey skin visible" |
| Practitioner pointing at a thinning scalp | "head tilted forward" | "a visibly thinning crown with sparse hair coverage and exposed scalp through the parting line" |
| Hand on a distended belly | "torso turned toward the camera" | "a distended, bloated lower abdomen pushing against the waistband, the practitioner's palm flat against the swell" |
| Close-up on varicose veins | "her leg extended" | "ropey, bulging blue-purple varicose veins running down the calf, raised above the skin surface" |
| Practitioner inspecting back acne | "her back facing the camera" | "an upper back covered in red, raised, inflamed acne lesions clustered across the shoulder blades" |
| Hand on a swollen ankle | "foot resting on the floor" | "a noticeably swollen ankle with stretched, shiny skin and faint pitting, almost erasing the ankle bone" |

### Asymmetry chain (v610 + v622)

| Character role | Gender description | Symptom-feature description |
|---|---|---|
| Main character (upload-bound persona) | FORBIDDEN (v610) | N/A — persona is not the symptom-bearer; the patient is |
| Patient / customer / bystander (non-persona) on AUGMENTED-SYMPTOMS or HEALER-SHOWING-CURE lens | REQUIRED (v610) | REQUIRED (v622) |
| Patient / customer / bystander on GRABBING-ATTENTION lens with no specific body part indicated | REQUIRED (v610) | NOT required — describe role/clothing/posture instead |

The persona is upload-swapped at lift time; the patient is NOT — the patient's prose IS the only source of truth for Banana 2 to anchor on. Vague non-persona descriptions on diagnostic scenes produce generic Banana 2 output, and the diagnostic pivot collapses.

### Pre-output validation gate (decoder-side)

Before emitting any `raw/decoded_*.md`:

- ✅ For every Image whose `narrative_lens:` is `AUGMENTED-SYMPTOMS` or `HEALER-SHOWING-CURE`: does the prompt body name the **specific body part** being indicated?
- ✅ For every Image where the source frame shows a non-persona character with a body part being pointed at / pressed / framed / circled: does the description **exaggerate the visible feature** in concrete terms, not generic posture?
- ✅ Mechanical check (negative): grep the body for these forbidden filler phrases when a body part is being indicated — `"chin raised slightly"`, `"head tilted"`, `"face turned"`, `"eyes locked"` (alone), `"torso turned"`, `"leg extended"`, `"foot resting"`. If present AND the source frame is a tight diagnostic crop, REWRITE.

### What v622 does NOT change

- v610 main-character gender ban — preserved (persona descriptions stay gender-neutral; v622 only governs non-persona feature description).
- v621 narrative-lens classification — preserved (v622 builds on the lens; doesn't replace it).
- Dialogue lines — preserved verbatim from source whisper transcription.
- GRABBING-ATTENTION lens scenes without a specific symptom — no exaggeration required (no symptom = nothing to exaggerate).
- Persona uploads — still bound by v607 + v619 N4. v622 governs prose only, not binding mechanics.

### Lift-side application

When lifting from a decoded source whose original artifact predates v622 (decoded with neutral posture filler), the lift author MUST upgrade the symptom description to match v622 — re-watch the source frame at the timestamp, identify the actual visible feature, and rewrite the patient's description before lifting into `videos/*.md`. A lift that propagates v621-era neutral filler will produce a generic generated patient and the diagnostic-pivot rhetoric will fail at video time.

---

## Auto-infer + normalize image bindings (v619) — feature delivery, not error rejection

**Source: 2026-05-06 owner directive** *"let's make also the rules stronger and more precise for which images we need in the markdown... from the latest video menopause saffron i can see image2 didn't include the product image, even if it mentions it... and make sure the whole process of image creation actually respects this logic. i don't want errors handling i want the feature to be delivered properly, so focus on doing that perfectly."*

The pre-v619 stack had v581 (binding mechanics), v599 (product-presence matrix), v607 (character force-bind), v613a (parity advisory), v618a/b (parser + fail-fast). The bug: those rules required the markdown to be self-consistent. When the author wrote sloppy markdown (mentioned product in body but forgot `product_image:`, or set `product_image:` but skipped the v581 binding line), the platform either silently dropped bindings or rejected the import. **Neither delivers the feature.**

v619's job is to **deliver the feature**: every image, regardless of how sloppy the markdown is, ends up at generation time with the right references attached. The platform AUTO-FIXES gaps instead of failing.

### Where it runs

In `image_platform.py:import_video_md`, between `_resolve_uploaded_ingredients` (line ~3878) and the per-image binding loop (line ~4033). Operates on the parsed `images` list IN PLACE, so all downstream logic sees normalized data.

### Five normalization operations (per image)

| Op | Trigger | Action |
|---|---|---|
| **N1** | `product_image` empty BUT prompt body has v581 product binding line | Extract product name from `"Use the uploaded product reference image for X."` → set `product_image: X` |
| **N2** | `product_image` empty BUT body mentions a brand keyword (any token ≥ 4 chars from a `type=product` ingredient name, e.g. "korella", "saffron", "bottle") | Auto-set `product_image: <matching-ingredient-name>` |
| **N3** | `product_image` set BUT v581 product binding line missing from prompt body | Auto-prepend `"Use the uploaded product reference image for X."` |
| **N4** | v581 character binding line missing from prompt body | Auto-prepend `"Use the uploaded character reference image for the main character."` |
| **N5** | `reference_image: N` is forward-ref (N ≥ image_index) or N not declared | Set to `None` (drop chain), log warning |

### What v619 does NOT do

- **No HTTPException.** v619 is the feature-delivery layer. Bad markdown gets repaired silently. The one unrecoverable case (declared `Reference` path with no upload) is still v618b's fail-fast.
- **Doesn't rewrite ingredients.** Ingredients table stays canonical. Only the per-image `prompt` and `product_image:` are normalized.
- **Doesn't touch already-correct markdown.** Idempotent — running v619 on a clean import is a no-op (verified in synthetic test 6).
- **Doesn't infer character ingredients.** v607 force-bind already attaches the character edge on every image. N4 just ensures the binding LINE is in the prompt body so Banana 2 sees the slot reference.

### Validation

6 synthetic test cases pass:

1. Body mentions "Korella saffron bottle" with no `product_image:` → N2 auto-sets field, N3 prepends binding line.
2. `product_image:` set but no binding line → N3 prepends.
3. v581 binding line present but `product_image:` empty → N1 extracts name from line.
4. No character binding line → N4 auto-prepends.
5. Forward chain ref (`reference_image: image_5` from Image 2) → N5 drops.
6. Already-correct image (full bindings + matching `product_image:`) → idempotent, no change.

### Logging

Every normalization step logs at INFO with the rule number:

```
[image_platform] v619 N2: Image 2: auto-set product_image='the Korella saffron bottle' from body mention of 'korella'
[image_platform] v619 N3: Image 2: auto-prepended product binding line for 'the Korella saffron bottle'
[image_platform] v619 N4: Image 1: auto-prepended character binding line
[image_platform] v619 N5: Image 2: reference_image=5 is invalid (forward ref or undeclared) — dropping chain
```

This makes auto-fixes auditable at import time. If an author wonders why their image came back with auto-attached bindings, the log shows exactly which N-rule fired.

### End-to-end correctness

Combining v607 + v618 + v619, the platform now guarantees:

- Every image gets the character ref attached (v607 force-bind)
- Every image with product brand keywords in body gets the product ref attached (v619 N2)
- Every image with `product_image:` set gets the v581 binding line in body (v619 N3)
- Ingredients table is parsed correctly regardless of column order (v618a)
- Missing uploads are caught loud at import (v618b)
- Forward / undeclared chain refs are dropped (v619 N5)
- v607 character force-bind ensures the edge exists even if N4 didn't touch the prompt

This is the FEATURE delivered: from any reasonable markdown, the platform produces correctly-bound images at generation time.

---

## Header-aware ingredients parser + fail-fast upload validation (v618)

**Source: 2026-05-06 owner observation** (with screenshot of menopause-saffron Image 7): *"why this image from the menopause saffron video didn't include main character?... check both the image worker, the platform or the video markdown... and find where is best to enforce it. make a future-proof, ondurate decision."*

The Image 7 reference panel showed only `korella.jpg` as parent; the persona reference was missing entirely. Banana 2 generated a generic woman in a doctor's coat instead of the Black-female-practitioner persona.

### Root cause

Two-stage failure in `image_platform.py`:

1. **`_parse_ingredients_block` was column-position-locked.** The pre-v618 parser hard-coded column positions as `Name | Type | Description | Source`. The test video used `# | Type | Name | Reference`. The parser silently produced rows with `name="1"` (the `#` index column) and `description="the main character"` (the actual name shoved into the description slot).

2. **`_resolve_uploaded_ingredients` then couldn't match the persona by name** — there's no upload registered under `"1"`, only under `"the main character"`. Returned empty `ingredient_nodes`. v607 force-bind looked for `type='character'` ingredients but found nothing under that name. No persona edge attached. Worker pulled only the product ref. Image 7 generated a generic face.

### Where to enforce — the future-proof answer

| Layer | Verdict |
|---|---|
| **Markdown** | Author writes the Ingredients table once. Already correct in test video (`# | Type | Name | Reference`). The parser was the wrong layer to be brittle. ❌ Not the fix-site. |
| **Worker (image_worker.py)** | Just consumes `input_images` from API. No authoring intent. ❌ Wrong layer. |
| **Platform (image_platform.py)** | Single point of enforcement. All Job-creation paths funnel through `import_video_md` / `promote_batch_to_video`. ✅ **The right layer.** |

### v618a — Header-aware ingredient parser

Detect column positions from the header row's keywords (case-insensitive):

| Output field | Header keyword(s) (substring match) |
|---|---|
| `name` | `name` |
| `type` | `type` |
| `description` | `description`, `desc` |
| `source` | `source`, `reference`, `ref`, `path` |

Parse subsequent rows using the detected positions. Tolerate extra columns (e.g. `#` index, `Notes`) — they're ignored. Reject the table only when the header has neither `name` nor `type` columns.

Both layouts now work:

```
| Name | Type | Description | Source |
|---|---|---|---|
| the main character | character | ... | personas/refs/X.png |
```

AND:

```
| # | Type | Name | Reference |
|---|---|---|---|
| 1 | character | the main character | personas/refs/X.png |
```

5 synthetic test cases pass: test-video format, legacy docstring format, minimal `Name | Type` only, weird 5-col order with `Reference` first, and missing-`name` rejection.

### v618b — Fail-fast validation at import

After `_resolve_uploaded_ingredients` returns `ingredient_nodes`, walk the parsed ingredients. For every row where:

- `type == "character"` OR `type == "product"`, AND
- `source` (Reference path) is non-empty, AND
- name is NOT in `ingredient_nodes` (no upload resolved)

→ raise `HTTPException(400)` with a clear list of unresolved ingredients pointing to the missing uploads.

This converts the previous SILENT failure (generic face/bottle in generation) into a LOUD failure (import rejected before any DB rows are created), telling the author exactly what's missing:

```
Ingredient(s) with type=character/product declare a Reference path in
the Ingredients table but no matching upload exists on the platform.
Upload each Reference file via the Persona / Product picker UI before
importing this video, OR pass `ingredient_node_ids` mapping the
ingredient name → uploaded ImageNode id.
Unresolved ingredients:
  • 'the main character' (type=character, declared Reference: personas/refs/black-female-practitioner.png)
```

### What v618 does NOT change

- v607 force-bind logic — preserved (now actually fires because parser correctly registers the character ingredient).
- v612 promote-to-video persistence — preserved.
- Parser output schema (`{name, type, description, source}`) — back-compat maintained.
- ingredient_node_ids resolution path — preserved.
- Uploads themselves — v618b detects missing uploads but doesn't auto-upload. Auto-upload from the markdown's Reference paths is a future v619 candidate (would require auth + multipart upload from the import path; tractable but bigger surface).

### Why v618b's strict failure is the right behavior

Pre-v618, the platform tolerated missing uploads by treating the ingredient as an "anchor-scene ingredient" (no upload, the first scene that mentions it defines its appearance). For `description` types this is correct. For `character`/`product` types with a `Reference` path declared, the author CLEARLY intended an upload — silently substituting an anchor scene produces generic content the author didn't ask for. Better to fail loud and have the author fix the upload than ship generic faces / generic bottles into a video the author thought was correctly bound.

If a future use case legitimately wants `type=character` without an upload (rare — would mean a character whose appearance is defined entirely by prose), the author can simply omit the `Reference` column — v618b only fails when a Reference path was declared but didn't resolve.

---

## Whisper export — single-pass trim+concat filter graph (v617)

**Source: 2026-05-06 owner clarification** *"the frames i was seeing are not extra frame after or before the words or segments, are just extra frames added randomly, so hard cuts are between them, does this clarify?"*

The pre-v617 2-stage pipeline (per-segment extract files + concat-demuxer re-encode) was **inserting duplicate frames at segment boundaries** via two mechanisms:

1. **Per-segment encode rounds duration UP**. `-r {fps} -vsync cfr -t {duration}` makes libx264 pad with duplicate frames at segment END to align to integer-frame counts. Even floating-point error in the v597/v616b frame-snap (e.g. 0.45000001s instead of 0.450s) → 1 extra frame per segment.
2. **Concat demuxer + `-vsync cfr` re-encode**. At each segment boundary, the encoder sees a PTS gap and resolves it by duplicating the last frame of segment N to maintain CFR continuity into segment N+1.

Different from v611-v616's "edge leak" — these are **inserted duplicates** between hard cuts, mid-segment.

### The fix — replace 2 stages with 1 ffmpeg invocation

```
ffmpeg -y -i source.mp4 -filter_complex "
  [0:v]trim=start=S1:end=E1,setpts=PTS-STARTPTS[v0];
  [0:a]atrim=start=S1:end=E1,asetpts=PTS-STARTPTS[a0];
  [0:v]trim=start=S2:end=E2,setpts=PTS-STARTPTS[v1];
  [0:a]atrim=start=S2:end=E2,asetpts=PTS-STARTPTS[a1];
  ...
  [v0][a0][v1][a1]...concat=n=N:v=1:a=1[outv][outa]
" -map "[outv]" -map "[outa]"
  -c:v libx264 -preset ultrafast -crf 23 -pix_fmt yuv420p
  -c:a aac -b:a 128k
  output.mp4
```

- `trim` filter cuts at exact PTS — frame-accurate when timestamps land on frame boundaries (v616b ensures this).
- `concat` filter joins with seamless PTS continuity — no boundary insertion possible, **there are no encoder-visible boundaries** (one filter graph, one encoder pass).
- No `-vsync cfr` flag — the concat filter outputs continuous PTS internally; libx264 just encodes whatever frames it receives. No dup/drop decisions at boundaries.

### What v617 removes

- The `tempfile.TemporaryDirectory()` block + per-segment `.mp4` extraction.
- The `concat_list.txt` + `-f concat -safe 0` demuxer step.
- The double `-vsync cfr` flag (per-segment + concat) that was the duplicate-frame source.
- The `-async 1` audio resync (concat filter handles audio sync intrinsically).

### What v617 keeps

- v499/v597/v616b frame-snap and tight matched-word containment — feed cleaner timestamps to the trim filter.
- All v611-v616 segment-computation logic — the filter graph just consumes the final speech_groups list.
- Source fps detection (still needed by v616b's frame-snap before this stage).

### Validation

Filter_complex generation tested for 3-segment case at 6-decimal precision (`1.234567`, `8.000000`, `15.111111`) → 438-char filter string. Well under ffmpeg's filter-graph length limits (~tens of MB). For typical 9-segment videos: ~1.3KB filter string.

### What v617 does NOT change

- v498-v616 segment computation logic — preserved.
- Energy-mode silence detection — separate code path.

---

## Whisper export — mid-segment unbridge + frame-snap + tighter fallback (v616) — close the last leak path

**Source: 2026-05-06 owner observation** *"sometimes i can see some extra frames added in the whisper exported final.. there's still something wrong... check the whole logic and maybe check online what's the best approach... you know what i need and want."*

v611 introduced strict matched-word containment as a defense-in-depth post-pass. It worked for segment EDGES — capping segment.start and segment.end against the nearest unmatched whisper word. But the user kept seeing extra frames. v616 closes the THREE remaining leak paths.

### Online research (Whisper word-boundary state of the art, 2024-2026)

Per [WhisperX (Bain et al., Interspeech 2023)](https://arxiv.org/abs/2303.00747) and follow-up work:

- **Whisper's free transcription has ±200ms word-boundary error** (corpus-wide measurement against ground-truth phoneme alignment).
- **Whisper consistently OVERSHOOTS word.end** by 50-150ms — it appends trailing breath/silence into the reported word boundary because the decoder treats acoustic energy roll-off as part of the word.
- **WhisperX's forced-aligner approach** (wav2vec2 phoneme-level alignment on top of Whisper transcription) achieves ±20-100ms accuracy under strict tolerance — that's the gold standard.
- A [2025 Whisper internal-aligner paper](https://arxiv.org/abs/2509.09987) found that filtering Whisper's own attention heads while teacher-forcing characters can match WhisperX accuracy without the wav2vec2 dependency.

The pragmatic implication for our pipeline: **don't trust Whisper's `word.end` to be the actual phoneme end.** Pad less, snap more, and unbridge mid-segment garbage that the bridger merged in.

### Three leak paths v611 didn't close

**Leak 1 — Mid-segment hallucination bridged by v548**:

The v548 bridger merges intra-clip gaps ≤ `BRIDGE_GAP_MAX=0.7s` and only refuses to bridge when an unmatched whisper word with `confidence ≥ HALLUC_PROB_FLOOR=0.30` sits in the gap. **Lower-confidence hallucinations slip through.** Concrete failure:

```
matched word M1 at 5.0s end=5.05s
hallucination H at 5.25s end=5.40s (conf=0.20 — below 0.30 floor)
matched word M2 at 5.5s end=5.6s
gap M1.end → M2.start = 0.45s ≤ 0.7s → bridger merges
result: one segment [5.0s, 5.95s] containing H's audio in the middle
```

v611 then walks this segment, finds matched words M1+M2, looks PAST M2 for unmatched words, finds none. **H is between M1 and M2, not past M2 — v611's edge-only check doesn't see it.** H survives in the export.

**Leak 2 — Whisper word.end overshoot under fallback pad**:

When no high-conf unmatched word follows the last matched word in a segment, v611's fallback caps end at `last_matched.end + STRICT_FALLBACK_END_PAD=0.18s`. But Whisper consistently OVERSHOOTS `word.end` by 50-150ms (per WhisperX research). So a fallback of 180ms ON TOP OF an already-overshot word.end gives us 230-330ms of post-word audio — silence, breath, ambient. At 24fps that's 5-8 extra frames per clip-end.

**Leak 3 — Mid-frame cuts**:

The existing v499 + v597 pipeline uses output seek (frame-accurate) + fps lock + CFR concat. But the segment timestamps fed to ffmpeg can land MID-FRAME. When a frame at 7.917s spans `[7.917s, 7.958s)` and we ask ffmpeg to cut at 7.928s (mid-frame), libx264's behavior is encoder-version dependent. Most builds keep the frame because the cut is past frame's start; some keep it AND its successor because of B-frame reorder buffer interactions.

### The v616 fix — three layers

**v616a — Mid-segment unbridge (split at intra-segment blockers)**:

After v611's edge cap, walk each contained segment. For every consecutive pair of matched words inside the segment, scan the gap between them for any whisper word with `confidence ≥ UNBRIDGE_PROB_FLOOR=0.10` (much lower floor than the v548 bridger's 0.30). If found, SPLIT the segment at that point — emit `[current_start, m1.end + STRICT_MIN_END_TAIL]` as one sub-segment, then resume at `m2.start - STRICT_START_GUARD` for the next.

The lower 0.10 floor catches faint hallucinations that the bridger ignored. Splitting (not just trimming) is correct because the matched words on both sides should both stay — we just need to surgically remove the blocker between them.

```python
UNBRIDGE_PROB_FLOOR = 0.10
for each segment:
    inside_matched = sorted matched words inside this segment
    for each consecutive pair (m1, m2):
        blockers = [u for u in all_unmatched
                    if m1.end <= u.start and u.end <= m2.start
                    and u.probability >= UNBRIDGE_PROB_FLOOR]
        if blockers:
            split: emit [seg_start, m1.end + 0.05]
            resume: seg_start = m2.start - 0.02
    emit [seg_start, seg_end]
```

**v616b — Frame-snap segment boundaries to source fps grid**:

Read source fps from ffprobe (`r_frame_rate`). Snap each segment's `start` UP to the next frame boundary (we keep only complete frames AFTER start) and `end` DOWN to the previous frame boundary (we keep only complete frames BEFORE end).

```python
src_fps = parse from ffprobe r_frame_rate
for (s, e) in speech_groups:
    s_snap = ceil(s * fps) / fps    # round UP — exclude pre-start partial frame
    e_snap = floor(e * fps) / fps   # round DOWN — exclude post-end partial frame
    # Defensive: if snap collapses segment, keep original
    if e_snap > s_snap: emit (s_snap, e_snap)
    else: emit (s, e)
```

This eliminates encoder-rounding ambiguity at cut boundaries. The cuts always land cleanly on frame edges, so libx264 has no choice about which frame to include.

**v616c — Tighten STRICT_FALLBACK_END_PAD from 0.18s to 0.10s**:

When no unmatched word follows the last matched word, cap the segment end at `last_matched.end + 0.10s` instead of `+ 0.18s`. Per WhisperX research, Whisper's word.end already overshoots by 50-150ms — so 100ms additional pad is enough to preserve typical fricative consonant decay (50-80ms) without preserving the silence/breath Whisper rolled into word.end. At 24fps, this is ~2.4 frames vs the previous ~4.3 frames.

If consonant decay gets clipped on specific phonemes after this change (mostly word-final fricatives or aspirated stops), raise back to 0.14. But 0.18 was almost certainly too generous given the documented overshoot.

### Validation

Synthetic test cases (5 cases all pass):

| Case | Before | After v616 | Expected |
|---|---|---|---|
| Mid-segment hallucination at conf 0.20 | `[(5.0, 5.95)]` (bridged single segment) | `[(5.0, 5.10), (5.48, 5.95)]` | Split at hallucination ✓ |
| Clean segment (no blockers) | `[(5.0, 5.95)]` | `[(5.0, 5.95)]` | Pass-through ✓ |
| Sub-floor noise (conf 0.05) | `[(5.0, 5.95)]` | `[(5.0, 5.95)]` | Below 0.10 floor — pass-through ✓ |
| Frame-snap @ 24fps end=7.928s | `(5.0, 7.928)` | `(5.0, 7.917)` | floor(7.928×24)/24=7.917 ✓ |
| Frame-snap @ 30fps start=5.013s | `(5.013, 7.945)` | `(5.033, 7.933)` | ceil(5.013×30)/30=5.033 ✓ |

### Future work — forced alignment (v617+)

The proper "best approach" per online research is to replace free transcription with forced alignment. We have the SCRIPT TEXT (it's in `dialogue_texts`); a wav2vec2 forced-aligner can align that script to audio with ±20-100ms accuracy vs Whisper's ±200ms.

That would make most of the v498→v616 patches obsolete:
- No more matcher needed (alignment IS the matching)
- No more padding decisions (alignment gives true boundaries)
- No more bridge/unbridge (no false-positive intermediate hallucinations to bridge)

The cost is adding a wav2vec2 dependency (`whisperx` Python package, or a lightweight Modal-hosted forced-aligner). Worth doing if v616 still leaks. **For now: v616 closes the leak paths within the existing free-transcription pipeline.** If a future user report comes in showing extra frames AFTER v616 in production, escalate to v617 = WhisperX integration.

### Tuning guide if v616 over-trims

If after deploy users report **clipped consonant decay** on word endings:
1. Raise `STRICT_FALLBACK_END_PAD: 0.10 → 0.14` (preserves more fricative tail)
2. Raise `STRICT_END_GUARD: 0.08 → 0.05` (closer to next blocker)

If users report **clipped onsets** at segment starts (rare):
1. Lower `STRICT_START_GUARD: 0.02 → 0.0` (no buffer past pre-segment unmatched word)

If the **mid-segment unbridge over-aggressively splits** legitimate segments (when Whisper transcribed a borderline real word that happens to not match the script):
1. Raise `UNBRIDGE_PROB_FLOOR: 0.10 → 0.20` (only split on stronger hallucinations)

All four constants live at the top of the v611 / v616 blocks in `code/video_processor.py:detect_speech_segments_whisper`.

### What v616 does NOT change

- v498-v557 matcher / padder / bridger logic — preserved.
- v611 edge-cap defense — preserved (v616 fires AFTER v611, on the v611-contained segments).
- v499/v597 ffmpeg output-seek + CFR + fps-lock — preserved (v616b's frame-snap COMPLEMENTS them by feeding cleaner timestamps in).
- Energy-mode silence detection — separate code path; v616 only fires when `silence_mode="whisper"`.

---

## Dialogue lip-sync trigger and voice qualifier syntax (v642)

**Source: 2026-05-07 owner review** + cross-reference against Google Vertex AI docs (`Clippings/Veo on Vertex AI video generation prompt guide.md`) + project wiki (`wiki/generation/veo-prompting.md`, `wiki/generation/kaveno-veo-bridge.md`).

The pre-v642 canonical rule was:

```
She says with [register]: [exact dialogue from Storyboard].
```

**Two problems vs the documented Veo 3.1 behaviour:**

1. **No quotation marks around the dialogue.** Per `wiki/generation/veo-prompting.md` §"Audio gotchas": *"Quotation marks trigger lip sync — use them for spoken lines."* Without quotes Veo may render the line as off-screen narration rather than synchronising the on-camera character's lips. Vertex AI's own examples (interrogation-room detective, "We have to leave now", weary-voice detective, etc.) all use quotes.

2. **`with [register]` is not a documented Veo syntax.** Vertex examples use either:
   - `says in a [qualifier] voice, "..."` (single qualifier — e.g. *weary*)
   - `speaks in a [adjective], [adjective] tone, "..."` (multi-qualifier — e.g. *serious, urgent*)
   - `says: "..."` (bare colon form)
   - `with a [accent] accent, speaks in a [emotion] tone, "..."` (accent variant)

   `with [free-form register]` was a project-internal label that maps onto a wider distribution of deliveries — Veo often defaults to neutral / rushed / flat because the cue doesn't anchor on its training-vocabulary words.

### v642 canonical syntax (replaces L791 placeholder)

**On-camera dialogue (default — `**speaker:** on-camera` or unset):**

```
The main AI generated character says in a [voice qualifier] voice, "[exact dialogue from Storyboard]".
```

**Voiceover (when `**speaker:** voiceover`):**

```
A voiceover with [voice quality] speaks in a [tone] tone, "[exact dialogue from Storyboard]".
```

Both forms wrap the spoken text in `"…"` so Veo's lip-sync trigger fires (or is correctly suppressed for voiceover, where the dialogue is in quotes but the `voiceover` opener tells Veo to keep on-camera lips closed — the speech overlay pattern from Vertex's *"a voiceover with a polished British accent speaks in a serious, urgent tone"* example).

### Voice qualifier vocabulary (controlled list, Vertex-grounded)

Pick **1-3 adjectives** that Veo's TTS reliably maps onto. Single-word and two-word combinations are most stable; three-word stacks dilute.

| Family | Tokens (use one or compose) |
|---|---|
| Pace | `measured` · `deliberate` · `slow` · `brisk` · `rushed` · `clipped` · `drawn-out` |
| Volume / register | `quiet` · `low` · `lowered` · `intimate` · `projected` · `raised` · `breathy` · `flat-monotone` · `chest-voice` · `head-voice` |
| Emotion | `weary` · `serious` · `urgent` · `calm` · `energetic` · `enthusiastic` · `angry` · `confident` · `cold` · `warm` · `clinical` · `authoritative` · `concerned` · `disgusted` |
| Conviction | `assertive` · `softly assertive` · `firm` · `gentle but firm` · `matter-of-fact` |

Compose like Vertex examples: *"weary voice"* (1 token) · *"serious, urgent tone"* (2 tokens, comma-separated) · *"calm, measured, authoritative voice"* (3 tokens, max).

**Audio direction phrases** (alternate form, from project's Lib Course notes — `wiki/generation/kaveno-veo-bridge.md` Phase 4): `talks with enthusiasm`, `talks angry`, `talks with emotion`. Lower-precision than the `says in a [X] voice` form, but useful when the qualifier is a single emotion word.

### Worked old → new examples (from real `videos/*.md` files)

| Old (no quotes, free-form register) | New (quoted, Vertex-grounded qualifier) |
|---|---|
| `She says with serious clinical-teaching authority: this is high blood sugar.` | `The main AI generated character says in a measured, low, authoritative voice, "this is high blood sugar".` |
| `She says with confident-authoritative direct-address: there are five truths every doctor should tell you.` | `The main AI generated character says in a confident, brisk, direct voice, "there are five truths every doctor should tell you".` |
| `She says with cold clinical-authoritative disgust: these won't fix what's actually broken.` | `The main AI generated character says in a cold, clinical, disgusted voice, "these won't fix what's actually broken".` |
| `He says with deadpan curiosity-gap delivery (English): What if I told you...` | `The main AI generated character says in a deadpan, curious voice, "what if I told you you're throwing away the most powerful part of the papaya".` |
| `She says with warm-authoritative-CTA closing emphasis: comment "stamina" and I'll send you the full protocol.` | `The main AI generated character says in a warm, authoritative voice, "comment stamina and I'll send you the full protocol".` |

(Note last example: the `"stamina"` keyword inside the line had to be unquoted — nested quotes break the lip-sync parser. Use single keyword without quotes inside the dialogue, or use single quotes `'stamina'` if a marker is needed.)

### Language tag placement

Pre-v642 outputs sometimes appended `(English)` — and one file double-stamped `(English) (English):`. Rule:

- If the persona's language is the default (English in this project), **omit the language tag**.
- If non-English, place the tag in the voice qualifier itself: `says in an Italian-accented, weary voice, "..."`. Don't append `(Language)` after the colon — that's not Vertex syntax.

### Nested quotes inside dialogue

Veo's parser uses `"…"` as the lip-sync trigger boundary. Never nest double quotes inside the dialogue line. If a keyword needs marking, drop the inner quotes and rely on context (`comment stamina and I'll send the protocol`) or use single quotes (`comment 'stamina'`).

### Pre-output validation gate

Before emitting any `videos/*.md` Veo Final Prompt:

- ✅ Dialogue text is wrapped in `"…"` (on-camera AND voiceover both quoted)
- ✅ Voice qualifier sits **between** `says`/`speaks` and the quoted dialogue, not after the colon
- ✅ Qualifier uses tokens from the controlled vocabulary above (1-3 adjectives, comma-separated when ≥2)
- ✅ No `(English)` or `(Language)` annotations dangling outside the qualifier
- ✅ No nested double-quotes inside the dialogue text
- ✅ The line itself still satisfies the v615 em-dash ban
- ✅ For `voiceover` scenes: `A voiceover [...] speaks in a [tone] tone, "..."` form used (the `voiceover` opener is mandatory — keeps Veo from animating lip-sync on the visible subject)

### Why this is doc-grounded, not speculation

Every element is traceable to a documented source:

- **Quotes for lip sync** ← `wiki/generation/veo-prompting.md` L155 + `wiki/generation/kaveno-veo-bridge.md` L61
- **`says in a [qualifier] voice` form** ← Vertex AI guide line 354 + Ultimate Guide line 189 (*"says in a weary voice"*)
- **Multi-adjective tone form** ← Vertex AI guide line 354 (*"speaks in a serious, urgent tone"*)
- **Voiceover + voice quality + tone** ← Vertex AI guide line 354 (*"a voiceover with a polished British accent speaks in a serious, urgent tone"*)
- **Audio direction phrase alternative** ← `wiki/generation/kaveno-veo-bridge.md` L100 (Lib Course — *"talks with enthusiasm"*)
- **Master template** ← `wiki/generation/kaveno-veo-bridge.md` L120 (`Character A says: "..."`)

### What v642 does NOT change

- Action_note prose — actions remain scene-dependent per v540.
- The `**speaker:**` field semantics (v537/v538) — still controls on-camera vs voiceover routing.
- The `**visual register:**` and `**rhythm tier:**` storyboard fields — visual register tagging only, separate from spoken delivery.
- Em-dash ban (v615) on the spoken line text — still applies inside the quoted portion.
- Ambient block, cinematography line, negative prompt block — unchanged.

---

## Dialogue best-practices supplement (v643) — web-grounded

**Source: 2026-05-07 owner directive** *"search online the best practice for the dialogue and prompt structure for Veo 3.1 and update the rules and docs."* Verified across five independent sources: Google Vertex AI docs, Google DeepMind Veo prompt guide, GlobalGPT Veo 3.1 dialogue guide (2026-02-11), veo3ai.io 2026 native-audio guide, skywork.ai lip-sync prompting guide. Convergent rules below.

### v643.1 — Camera angle directly affects lip-sync quality

| Shot type | Lip-sync quality | When to use |
|---|---|---|
| Close-up / Medium Close-Up | **High** | Default for any clip with on-camera dialogue |
| Head-and-Shoulders | **High** | Standard talking-head — preferred for our content |
| Medium shot (waist up) | Medium | OK for two-person dialogue framing |
| Wide / establishing | **Low — avoid for dialogue** | Mouth too small for Veo to animate accurately |
| Profile (side view) | Medium | Side view is harder; only when scene demands |

**Project rule:** every scene with on-camera dialogue (`speaker: on-camera`, default) MUST be framed Close-Up or Head-and-Shoulders in the cinematography line. Wide shots are reserved for HOOK / ENVIRONMENT / B-roll beats with `speaker: voiceover`.

### v643.2 — Disambiguate the speaker when multiple humans are in frame

When the scene has ≥2 visible humans (persona + bystander, persona + customer, etc.), the dialogue cue MUST start with a specific-descriptor identification, not a generic pronoun. Veo otherwise routes the audio to the wrong character's mouth.

| ❌ Wrong (ambiguous) | ✅ Right (disambiguated) |
|---|---|
| `She says in a calm voice, "this is the moment"` | `The main AI generated character (in the white coat) says in a calm voice, "this is the moment"` |
| `He says in a weary voice, "..."` | `The seasoned detective says in a weary voice, "..."` |

For solo on-camera scenes (one human visible), `She says…` / `He says…` / `The main character says…` is fine.

### v643.3 — Multilingual dialogue handling

If the persona's language is non-English, write the spoken text in the target language **inside the quotes**. Veo handles accent + lip-sync automatically. Do NOT append `(Italian)` / `(Spanish)` / etc. as a parenthetical after the quoted line.

```
✅ The main AI generated character says in a warm voice, "ciao a tutti, oggi parliamo di salute"
❌ The main AI generated character says in a warm voice (Italian): "ciao a tutti..."
❌ The main AI generated character says in a warm voice, "ciao a tutti..." (Italian)
```

The `(English) (English):` typo observed in pre-v642 outputs (asian-elder file) is forbidden. Default-language scenes get NO language tag at all.

### v643.4 — Multi-speaker scenes — prefer one speaker per clip

Veo 3.1 can theoretically handle multi-speaker dialogue, but third-party testing (Replicate 2025, skywork.ai, veo3ai) confirms tight lip-sync degrades when two speakers appear in one ≤8s clip. Project rule:

- **One speaker per clip**: each `- **line:**` is its own scene with its own image. The other character (if visible) keeps lips closed (`The bystander stays silent with closed lips` in the action_note).
- **Conversation = chain of clips**: speaker A's line in clip N, speaker B's reply in clip N+1, with continuity provided by `clip_mode: continue` and a shared setting reference image.
- Multi-speaker single-clip is allowed only when both speakers are off-screen voiceovers (no lip-sync target) — uncommon in this project.

### v643.5 — Audio block ordering inside the Veo prompt body

Veo parses the audio elements in the order they appear in the prompt. Project canonical order (matches all five sourced guides):

```
[Cinematography line]

[Action narrative — three motion beats]

She says in a [voice qualifier] voice, "[exact dialogue]".    ← DIALOGUE first

(SFX line, when scene-specific — e.g. "SFX: tongue-depressor tap on heel skin, twice, mid-clip.")    ← SFX second

Ambient: [setting tone + low-priority background sounds].    ← AMBIENT last
(no subtitles, no captions)
```

Reasoning: Veo gives priority to whatever audio element is mentioned first. Dialogue placed before SFX/Ambient stays clean and audible; dialogue buried after a wall of ambient text gets buried in the mix. The "no subtitles, no captions" line stays at the end of the body block.

### v643.6 — Negative-audio additions for clean dialogue takes

The canonical 12-element negative prompt block (still mandatory, unchanged from v642 base) handles visual artifacts. Append audio-side negatives **only when the scene's dialogue MUST be the dominant audio** (i.e. all our talking-head Korella/Saffron/etc. videos):

```
no background music, no overlapping speakers, no extra dialogue, no fake applause, no robotic narration, no cartoon sound effects
```

Add as one comma-separated chunk at the end of the canonical 12-element negative block. Do NOT add audio negatives to scenes where music/ambience IS desirable (rare in this project but possible — wedding montage, action montage, etc.).

### v643.7 — The five common mistakes that break native-audio Veo clips

Per veo3ai.io 2026 audio guide, ranked by impact:

1. **Too much audio in too short a clip.** A 5-8s window can NOT hold dialogue + music + SFX + ambient + transition sting. Pick one primary audio focus per clip; everything else stays subtle.
2. **Not specifying who speaks.** Veo guesses, often wrong. See v643.2.
3. **Long lines.** v577 already enforces ~21-word per-clip budget at 158 wpm; lines that push past 23 words risk lip-stop or audio cut-off mid-sentence.
4. **Audio that doesn't match visible action.** If the action_note shows the persona pouring liquid, an SFX line about "phone ringing in background" creates dissonance. SFX must be tied to a visible action in the scene.
5. **Forgetting silence.** Some payoff/CTA clips work better with very low ambient and zero music. Don't pad every clip with sound.

### v643.8 — Per-scene review checklist (pre-output gate, supplements v642)

Before emitting any `videos/*.md` Veo Final Prompt, run the v642 7-check gate AND these v643 additions:

- ✅ Cinematography line specifies Close-Up / Medium Close-Up / Head-and-Shoulders for any on-camera dialogue scene
- ✅ When ≥2 humans in frame, dialogue cue starts with a specific-descriptor identification (v643.2)
- ✅ Non-English text is written in the target language INSIDE the quotes (v643.3) — no dangling language tags
- ✅ Each `- **line:**` is its own scene (multi-speaker conversations are CHAINED across scenes, not packed in one) (v643.4)
- ✅ Audio order in body: Dialogue → optional SFX → Ambient → "(no subtitles, no captions)"
- ✅ For talking-head scenes: audio negatives appended (`no background music, no overlapping speakers, no extra dialogue`) (v643.6)
- ✅ Each scene has ONE primary audio focus, not five competing layers (v643.7.1)
- ✅ Word count of spoken line ≤21 (already enforced by v577) — confirms no lip-stop risk (v643.7.3)

### v643.9 — Sources verified

Every rule above traces to ≥2 of these sources:

- **Google Vertex AI Veo prompt guide** — `Clippings/Veo on Vertex AI video generation prompt guide.md` (official Google docs)
- **Google DeepMind Veo prompt guide** — https://deepmind.google/models/veo/prompt-guide/ (official Google research docs)
- **Google Cloud Ultimate Prompting Guide for Veo 3.1** — `Clippings/Ultimate prompting guide for Veo 3.1.md` (official Google blog 2025-11)
- **GlobalGPT Veo 3.1 dialogue guide** — https://www.glbgpt.com/hub/how-to-make-characters-speak-in-veo-3-1-the-ultimate-guide-to-dialogue-audio-lip-sync/ (community 2026-02-11, references Replicate + Vertex)
- **veo3ai.io native-audio guide** — https://www.veo3ai.io/blog/veo-3-native-audio-prompt-guide-2026 (community 2026-Q1)
- **skywork.ai lip-sync prompting** — https://skywork.ai/blog/how-to-prompt-lip-synced-dialogue-google-veo-3/ (community 2025-10, lip-sync verification methodology)
- **Project wiki:** `wiki/generation/veo-prompting.md`, `wiki/generation/kaveno-veo-bridge.md` (already aligned)

Convergence ≥2 sources for each rule prevents single-source over-fitting.

### What v643 does NOT change (preserves v642 + earlier scope)

- Dialogue cue syntax stays `The main AI generated character says in a [qualifier] voice, "[line]"` (v642 + v665)
- Voice qualifier vocabulary table (v642) unchanged
- Voiceover variant `A voiceover with [quality] speaks in a [tone] tone, "[line]"` (v642) unchanged
- Action_note prose stays scene-dependent (v540)
- Em-dash ban inside quoted line (v615) still applies
- Word-count budget per line ≤21 (v577) still applies
- 12-element canonical negative prompt block stays the visual baseline; audio negatives are an APPEND when scene-appropriate

---

## Audio-padding suffix for short dialogue lines (v644)

**Source: 2026-05-07 owner observation** + Veo 3.1 audio-experimental tier behaviour confirmed via web audit (LaoZhang Flow guide, veo3ai.io 2026 native-audio guide, Google Flow help docs).

### Problem

Veo 3.1's audio path on `Veo 3.1 Fast [Lower Priority]` tier fails to generate speech for short dialogue lines (≤~10 words) on a high-percentage of attempts. The error returned is `Audio generation failed. Please try a different prompt or send feedback.`, observed at the 27% checkpoint (audio-synthesis stage) on every attempt regardless of content. Confirmed not content-related (`hello everybody` fails the same way as medical-content lines).

Per Google's documentation: *"Audio generation in Flow is still experimental on Veo 3.1, and Google says low-quality audio can cause the video not to generate."* Google's veo3ai.io guide adds: *"Speech generation performs better with longer text transcripts in your prompt."*

### Solution — pad short lines to ~20 words

Append a per-scene `- **pad:**` bullet to bring the Veo-prompt dialogue to ~20 words total. The pad text is suffix-only (always AFTER the line, never before). The platform reads both bullets:

- `- **line:**` — the keeper. The whisper-VAD's source-of-truth for matching. Stays in the final cut.
- `- **pad:**` — the suffix appended to the Veo prompt only. Veo speaks it; the existing whisper-VAD logic doesn't match it against the script, so it's automatically cut from the final video.

### Storyboard syntax

```
### Scene N
- **image:** image_N
- **line:** this is high blood sugar.
- **pad:** Pay attention to this so you can spot it on yourself early.
- **action_note:** ...
```

Word count check (operator authoring discipline):

| Bullet | Words in example |
|---|---|
| `line` | 5 |
| `pad` | 11 |
| **Total Veo prompt dialogue** | **16** (target: 20 ± 2) |

### Veo prompt assembly

The Veo Final Prompt's spoken-dialogue line is built from `line + " " + pad` (concatenation with a single space):

```
The main AI generated character says in a measured low chest-voice clinical authority,
  "this is high blood sugar. Pay attention to this so you can spot it on yourself early".
```

The whisper-VAD script is `line` only. After Veo renders the 8-second clip:
- Whisper transcribes the full audio
- Existing v542-era matcher locates `["this", "is", "high", "blood", "sugar"]` in the transcript
- Anything outside the matched span is treated as filler and trimmed by `apply_vad`
- Final video contains only the keeper words

NO code changes to the existing whisper-VAD pipeline. The platform's parser stores `line` and `pad` separately; the Veo-prompt build path concatenates; the whisper-script construction path uses `line` alone — both paths already exist.

### When to use `pad:`

| Line word count | Pad required? |
|---|---|
| ≥ 15 words | NO (Veo audio path reliable enough) |
| 10-14 words | OPTIONAL (recommended if testing shows audio failure) |
| ≤ 9 words | **REQUIRED** to bring total to ~20 words |

Most CTA / hook / payoff lines fall in the ≤9 category. Most context / explain lines fall in 10-14. Recipe-step lines often hit 15+ on their own.

### Pad content discipline

The pad text gets spoken by Veo during synthesis. Even though it's cut from the final video, the persona is on-camera saying it. Therefore:

1. **In-character with persona**: same vocabulary register as the line — clinical/practitioner persona uses clinical pad, conspiratorial persona uses conspiratorial pad
2. **No new claims**: pad must NOT introduce a new health/product/benefit claim that the script didn't already make. Pad is filler, not content. (E.g., for the line `"this is high blood sugar"`, pad SHOULD be `"Pay attention to this and remember what I'm showing you"`. Pad SHOULD NOT be `"and the cure is saffron taken at sunrise"` — that's a claim, not filler.)
4. **Em-dash ban (v615) applies**: pad text is dialogue too — same comma/period/rephrase rules
3. **No risky tokens**: avoid Veo RAI trigger words (`fire`, `shot`, `strike`, `blood` outside compound terms, `kill`, etc.) in the pad — same constraints as the keeper line
5. **Persona-consistent ending**: pad should end on a natural mouth-close beat so the lip-sync transitions cleanly

Good pads for various registers:

| Persona register | Sample pad templates |
|---|---|
| Clinical-authority | "Pay attention to this so you can spot it on yourself early." · "Remember this carefully because it matters for your recovery." |
| Warm-conspiratorial | "Trust me on this and remember what I just showed you." · "Take this seriously because most people miss it completely." |
| Recipe-instructional | "Try this tonight before bed and tell me how you feel tomorrow." · "Make a habit of this and you will see the difference fast." |
| CTA-closing | "Save this so you can come back to it whenever you need to." · "Share this with someone who needs to see it before tomorrow." |

### What v644 does NOT change

- v539 / v542 (`prefix_short_enabled`, `cut_prefix_audio`) — those remain available as a job-level fallback for the older single-word warm-up case. Not used by v644 — `pad:` is per-scene, multi-word, and suffix-only.
- v615 em-dash ban — applies to BOTH `line` and `pad` text
- v577 word budget (~21 words per clip at natural pacing) — pad helps the Veo prompt REACH that budget; it is not an exemption from it
- v642+v665 dialogue cue syntax (`The main AI generated character says in a [qualifier] voice, "..."`) — quoted text is `line + " " + pad`; voice qualifier and surrounding structure unchanged
- Whisper-VAD code (`code/video_processor.py`) — no changes; existing matcher trims the pad words automatically as unmatched filler

### Pre-output validation gate

Before emitting any `videos/*.md` clip prompt:

- ✅ Every scene with `len(line.split()) ≤ 9` has a `- **pad:**` bullet
- ✅ Every scene with `9 < len(line.split()) < 15` either has `- **pad:**` OR is flagged for testing
- ✅ For scenes with `pad`, total `len(line.split()) + len(pad.split())` is in the range `[18, 22]` (20 ± 2)
- ✅ Pad text does not introduce a new claim beyond what the line says
- ✅ Pad text passes the v615 em-dash check
- ✅ Pad text avoids RAI trigger tokens (`blood` standalone, `fire`, `shot`, `strike`, etc.)

### Sources

- Google Flow help: *"Audio generation in Flow is still experimental on Veo 3.1"* (cited via LaoZhang Flow guide 2026)
- veo3ai.io 2026: *"Speech generation performs better with longer text transcripts in your prompt"*
- Veo 3.1 RAI audio-filter false-positive issue: googleapis/js-genai #1272 (Jan 2026)
- Project precedent: v539 / v542 (single-word warm-up + cut_prefix_audio) — `code/veo_generator.py:1053` + `code/video_processor.py:1836`

---

## Speaker-subject normalization (v665) — "the main AI generated character"

**Source: 2026-05-07 owner directive** *"in the prompt that we generate, both in the platform and in the rules for the decode, lift etc... we have to say that 'the main AI generated character says'..."*

### Rule

The dialogue cue's subject is ALWAYS the literal phrase **"the main AI generated character"** (capitalized as **"The main AI generated character"** at sentence start). NEVER:

- pronouns (`She` / `He` / `They`)
- gendered nouns (`the woman` / `the man`)
- persona names (`Dr. Sage` / `Nuri` / `Master Shen`)
- generic role labels (`the doctor` / `the practitioner` / `the speaker`)
- pre-v665 generic (`the main character` — drops the "AI generated" qualifier)

### Why

Pre-v665 prompts used `She says` / `He says` / `the main character`. Three failure modes observed:

1. **Wrong-binding to a bystander.** Veo sometimes routed the spoken dialogue to a non-persona figure visible in the start frame (husband-bystander, customer, child) when the persona's gender / pose was ambiguous in that frame.
2. **Conflict with persona name in image prompts.** Image prompts deliberately do NOT name the persona (`v553.1 never-describe-persona`); Banana 2 attaches the persona via the upload. If the dialogue cue then says `Dr. Sage says`, Veo has a name without a face binding and may invent features that drift from the persona reference.
3. **Gender-bound voice.** `She` / `He` triggers Veo's gender-stereotyped voice profile early in the prompt — overrides the explicit voice qualifier in some cases.

The "AI generated" qualifier locks the binding to the persona-reference upload, matching the platform's persona-attachment logic in `image_platform.py` at job-emission time. Veo treats it as a role descriptor pointing at the uploaded character image, not as a free-form character it should imagine.

### Applies to

- `code/template_reference.md` v642 dialogue cue placeholder (`The main AI generated character says in a [voice qualifier] voice, "[line]"`)
- `code/template_new_format.md` clip-block skeleton
- `code/veo_generator.py` runtime prompt builder (the `dialogue_block` f-string at the on-camera branch + the voiceover branch's reference to the visible character's lips)
- `wiki/generation/veo-prompting.md` canonical examples
- All future-authored `videos/*.md` and `raw/decoded_*.md` artifacts
- Decode scripts: when reproducing source dialogue, use `The main AI generated character says...` even when the source clearly shows a single human; the rule is uniform across decode and generate to keep the grammar bidirectional.

### Voiceover variant

```
A voiceover with [voice quality] speaks in a [tone] tone, "[exact dialogue]". The main AI generated character's lips DO NOT MOVE.
```

The `voiceover` opener handles the off-screen narration; the second sentence enforces lip-sync suppression on the on-camera persona reference. Subject in the second sentence is still "The main AI generated character" — same persona-binding rationale.

### Action_note prose unaffected

Action_note bullets in the storyboard continue to use "the main character" (no "AI generated" qualifier) for movement description, because action_note is INPUT to Veo's motion path, not the dialogue cue's persona binding. Example:

```
- **action_note:** [Start beat 0-2s] The main character leans slightly forward toward the lens. [Mid-clip beat] On "comment weight loss", the main character's hand gestures openly. [End beat] The main character nods.
```

The dialogue cue then names "The main AI generated character" because that's the SPEAKER attribution. The two phrasings can coexist in one prompt — they target different Veo subsystems.

### Migration check

Before emitting any clip prompt:

- ✅ Dialogue cue subject is `The main AI generated character`
- ✅ No `She` / `He` / `They` as dialogue cue subject
- ✅ No persona name as dialogue cue subject
- ✅ Voiceover scenes use the two-sentence form with the main AI generated character's lips referenced

---

## Dialogue line punctuation (v615)

Scene `- **line:**` entries (and `- **pad:**` per v644) use **commas, periods, and sentence breaks** for natural spoken cadence. The same rule covers all spoken text the persona renders through Veo's TTS. Replacement table for typical em-dash uses: aside / parenthetical → comma pair OR new sentence; trailing emphasis → period + new sentence; restatement → period; list intro → colon OR period; pause-for-breath → period.

Mechanical check before emitting any draft: `grep '^- \*\*line:\*\*'` for `—` returns zero matches.

Em-dashes are still natural in `action_note` prose, image-prompt bodies, frontmatter, corpus annotations (`[corpus: file — section]`), and decoded artifacts in `raw/decoded_*.md` (those preserve verbatim source audio).

→ Full rationale + worked failure modes (v615 / v644 archived deep-dive) in `wiki/meta/decode-negative-rules-archive.md` §"v615 — Em-dash ban in dialogue lines".

---

## Cross-corpus structural survey + mandatory per-scene adaptation map (v614) — every line must lift from a named corpus parent

**Source: 2026-05-06 owner observation** *"also the script doesn0t make any sense... you have plenty of amazing examples and winning case... adapt and innovate those... not just the ones from korella saffron but all, to see how they structure the video and script."*

v613 introduced corpus-grounding (cite ≥2 raw/decoded files; cell honesty NOTE; HOOK from niche hook table; per-line corpus annotation encouraged). v614 closes the remaining gap: **the LLM was still authoring without surveying the WHOLE corpus first**. Owner caught the test video drifting into melodrama ("Your husband sleeps through this — but watch what 2 a.m. does to her body") despite the corpus containing 24 winning videos with very different structural DNA.

### The cross-corpus survey

Before writing a single dialogue line, the LLM must read the dialogue lines of all 24 `raw/decoded_*.md` and `raw/dr_kim_*_decoded.md` files and classify each into one of 5 structural patterns. The survey takes ~30 seconds (1-line-per-file extraction) and changes everything downstream.

| Pattern | Used by (corpus exemplars) | Structure | Line count |
|---|---|---|---|
| **A — BEFORE/AFTER transformation** | dr_kim_back_lump, dr_kim_belly_burn_male, dr_kim_hair_regrowth_male, decoded_back_bump_transformation, decoded_varicose_vein_transformation | L1 problem-now / L2 fixed-N-days-later (with no-surgery-no-gym disclaimer) / L3 [demographic-frequency-benefit] / L4 CTA | 4-6 |
| **B — RECIPE-LED** | decoded_corella_saffron_1to1, decoded_saffron_male, decoded_saffron_vitality, decoded_belly_burn_tea, decoded_bladder_tea, decoded_icelandicwisdom_belly_fat, oldearl_visceralfat | HOOK (claim or symptom-callout) / RECIPE-steps (2-5 lines: warm water, lemon, ginger, honey, capsule) / MECHANISM-line (1 line, concrete benefit) / CTA | 4-15 |
| **C — DIAGNOSTIC / SHOW-PROBLEM-PIVOT** | decoded_corella_saffron_blood_sugar, oldearl_tonsil_healer, decoded_varicose_vein, master_chen_three_things | Repeated-show-problem (3-5 lines) / NEGATION-PIVOT ("not X, not Y, definitely not Z") / mechanism / CTA | 4-17 |
| **D — CULTURAL-AUTHORITY TEMPLATE** | master_salvora trilogy (banana/cabbage/salmon — IDENTICAL 10-line template, swap-INGREDIENT) | "if you think X is only for Y, you are mistaken... cut half... bring to boil... say goodbye to..." | 10 (rigid) |
| **E — PERSONAL-AUTHORITY** | rastajahmeil_fat_melt, master_chen, decoded_meta_papaya_skin | Hook with prankster-lead ("don't drink this too often because your family will think...") / recipe / signature ("my name is X and i am on a mission...") / CTA | 8-16 |

### Universal corpus rules (extracted from all 24)

1. **12-25 words per line.** Conversational, not literary. Test-video melodrama lines like "Your husband sleeps through this — but watch what 2 a.m. does to her body" (14 words) are within range numerically but VIOLATE the corpus tone. Direct symptom-callouts win.
2. **4-17 total lines.** Anything outside this range needs a structural rationale. Most winners are 4-10.
3. **Canonical CTA template.** 12 of 24 corpus videos end verbatim or near-verbatim with: `"comment '<keyword>' and i'll send you my full <protocol>. but follow me first so i can reach you."` Don't reinvent. Lift it.
4. **Mechanism = 1 line, concrete benefit.** Corpus example: *"saffron relaxes blood vessels — more blood means more girth, and within a week she'll feel the difference."* NOT *"saffron is the only ingredient that crosses the blood-brain barrier and resets the hypothalamus directly"* (jargon-academic — corpus never uses this register).
5. **Authority is implicit, not declared.** Corpus voice: *"I've seen people go from constantly tired and drained to feeling lighter, clearer, actually energized — one capsule a day."* CORPORATE VOICE (FORBIDDEN): *"I'm Dr. X, and I help one million women..."*. Practitioner-voice trumps title-pumping.
6. **Recipe steps are SHORT comma-lists or single-action lines.** Corpus example: *"warm water, half a lemon, raw ginger, raw honey, then open a saffron capsule."* NOT *"First — half a fresh lemon, squeezed into warm water."* (over-formatted, breaks the rhythm).
7. **No melodrama.** Corpus uses direct demonstrative language. *"this is what menopause does at 2 a.m. — soaked sheets, racing heart, no sleep."* NOT *"Your husband sleeps through this — but watch what 2 a.m. does to her body."*
8. **Negation-pivot is a Pattern C signature.** *"the best thing for X is not Y, not Z, definitely not W."* Strong dismissal of the wrong solutions before naming the right one.

### The rule (v614)

**v614a — Cross-corpus pre-survey (mandatory before writing)**:

The LLM must, before authoring any dialogue, extract the full set of `- **line:**` entries from all `raw/decoded_*.md` and `raw/dr_kim_*_decoded.md` files (all 24 in the current corpus, more as the corpus grows). This produces a flat list of ~150 corpus dialogue lines. The LLM then classifies each into one of the 5 patterns (A/B/C/D/E) and identifies the 2-4 closest structural matches for the cell being authored.

**v614b — Mandatory `corpus_pattern:` and `adaptation_map:` frontmatter fields**:

Every videos/*.md must declare in its frontmatter:

```yaml
corpus_pattern: B (RECIPE-LED) + C (DIAGNOSTIC-PIVOT) hybrid — HOOK uses Pattern C; Scenes 3-6 use Pattern B; Scene 7 uses Pattern B mechanism-line; Scene 9 uses canonical CTA template
adaptation_map:
  scene_1: "decoded_corella_saffron_blood_sugar_v584.md L1-L4 §HOOK diagnostic-callout pattern + wiki/voiceover-scripts/menopause.md hook 1"
  scene_2: "decoded_corella_saffron_blood_sugar_v584.md L5 §NEGATION-PIVOT (not X, not Y, definitely not Z)"
  scene_3: "decoded_saffron_male_v577.md L2 + decoded_saffron_vitality_v577.md L2 §RECIPE-INTRO"
  ...
  scene_9: "decoded_corella_saffron_blood_sugar_v584.md L17 §CANONICAL-CTA"
```

Every scene maps to a SPECIFIC corpus line citation with the section label. No scene is allowed to be unmapped — if a scene is genuinely novel, map it as `"[novel — testing]"` with rationale.

**v614c — Mandatory per-scene `[corpus: ...]` annotation (was encouraged in v613c, now mandatory)**:

Every scene's `- **action_note:**` MUST begin with a `[corpus: <source-file> L<line> §<section>]` annotation matching the adaptation_map entry for that scene. Mismatch between adaptation_map and the scene's annotation = REJECT.

**v614d — `corpus_compliance_audit:` self-check in frontmatter**:

Each videos/*.md must declare a self-audit section in frontmatter that reports its compliance with the universal corpus rules:

```yaml
corpus_compliance_audit:
  - words_per_line: <range observed in this script's lines, vs corpus norm 12-25>
  - line_count: <count, vs corpus range 4-17>
  - cta_template_canonical: yes/no (lifts the 12-of-24 canonical CTA?)
  - mechanism_concrete_not_clever: yes/no (concrete-benefit chain, NOT jargon-academic reframe?)
  - authority_implicit: yes/no (practitioner-voice, NOT "I'm Dr. X" title-pumping?)
  - melodrama_removed: yes/no (direct symptom-callouts, NOT theatrical reframes?)
```

If any field is `no`, the script must explain WHY in a comment immediately below the audit, OR rewrite to comply.

### Pre-output validation gate

Before emitting any videos/*.md draft:

- ✅ `corpus_pattern:` declared in frontmatter with at least one of A/B/C/D/E?
- ✅ `adaptation_map:` declared with one entry per scene? Each entry cites a specific corpus file + line + section?
- ✅ Every scene's action_note opens with `[corpus: ...]` matching the adaptation_map?
- ✅ `corpus_compliance_audit:` declared with all 6 fields?
- ✅ Words-per-line within 12-25 corpus norm?
- ✅ CTA scene lifts the canonical "comment 'X' / send my full / follow me first" template?
- ✅ Mechanism scenes use concrete-benefit chains, not jargon-academic reframes?

If ANY ❌ → REWRITE before emitting.

### Worked example — menopause-saffron HOOK before/after v614

**Pre-v614 (melodramatic, no corpus parent declared)**:
> Scene 1: "Your husband sleeps through this — but watch what 2 a.m. does to her body."

This sounds plausible in isolation but doesn't match any corpus pattern. The closest corpus opener (decoded_corella_saffron_blood_sugar L1-L4) uses a 4-line REPEATED-SHOWING structure, not a clever-reframe-with-bystander. Test-audience would feel the dissonance even if they couldn't articulate why.

**Post-v614 (corpus-grounded, Pattern C diagnostic-callout)**:
> Scene 1: "This is what menopause does at 2 a.m. — soaked sheets, racing heart, no sleep."
> action_note: `[corpus: decoded_corella_saffron_blood_sugar_v584.md L1-L4 §HOOK diagnostic-callout pattern + wiki/voiceover-scripts/menopause.md hook 1 night-sweats opener] ...`

Same scene composition (HOOK with main character + patient diagnostic). But now the dialogue tone is corpus-aligned (direct demonstrative, no melodrama, no bystander framing) AND the line is auditable — a reviewer can verify the corpus parent and check fidelity.

### Why v614 vs v613 alone

v613 said "cite ≥2 raw/decoded files in Sources" + "per-line corpus annotation encouraged." That was insufficient pressure: under attention load (long prompt body, 4-7 sentence v603 prose, six v606 directives, v610 gender scan, v613 product-binding parity), the LLM dropped corpus-faithfulness and reverted to plausible-but-corporate dialogue. v614 mandates the cross-corpus survey BEFORE writing AND mandates per-scene annotation AFTER writing — closes the loop on both ends.

The cross-corpus survey is the bigger fix. Pre-v614, the LLM would read the niche voiceover-script wiki page (4-5 hooks) and call it done. v614 forces the LLM to read all 24 decoded files' dialogue, see the structural diversity (Pattern A through E), and CHOOSE which pattern matches the cell — making the choice explicit forces corpus-thinking.

### What v614 does NOT change

- v581 binding mechanics, v599 product-presence matrix, v606 compositing, v610 gender-neutrality, v613 product-mention parity — all preserved.
- Niche voiceover-script wiki pages — still cited via v613b. v614 adds the cross-corpus survey on TOP.
- Banana 2 / Veo prompt mechanics — purely authoring discipline.
- Decode-side artifacts — v614 applies to authoring (lift / create), not decode (decode lifts dialogue verbatim from the source video transcription, no adaptation needed).

---

## Product-mention-binding parity + corpus-grounding (v613) — script must come from corpus, every product reference must be bound

**Source: 2026-05-06 owner observation** *"we have to review how we compose a new video... the script doesn't make any sense... you have plenty of examples in raw and clippings folders, base and adapt the script on those ones. sometime when teh product is mentione is not referenced as image. review those rules and make them stronger and reflect it also in the wiki. this is bery important."*

The pre-v613 authoring discipline had two structural gaps:

### Gap 1 — product-mention parity (mechanical)

v599 codified the 3-part product binding (product_image field + binding line + visual description) and the per-scene presence matrix (HOOK NOT visible / RECIPE-early NOT visible / RECIPE-reveal CASCADES / EXPLAIN visible / OUTRO hero-shot). But v599 had no MECHANICAL VALIDATOR forcing parity between two facts:

- **Visual mention** — does the image's prompt body describe the product visibly? ("a Korella saffron bottle on the counter," "the saffron capsule mid-pour")
- **Binding declaration** — is `product_image: <ingredient name>` set on the image?

When these diverged, two failure modes appeared in the menopause-saffron test video:

| Image | Prompt body mention | `product_image:` set? | Failure |
|---|---|---|---|
| Image 1 (HOOK) | "a Korella saffron bottle standing label-forward on the counter behind" | NO | Banana 2 invents a generic supplement (no Korella ref attached) AND violates v599 "HOOK product NOT visible" matrix rule |
| Image 3 (RECIPE setup) | "and a Korella saffron capsule pouch in soft-focus to the right" | NO | Same — invented prop AND v599 "RECIPE-early NOT visible yet" violation |

Both failures stem from the same root: prompt body and binding field drifted out of sync because no validator enforced parity.

### Gap 2 — corpus-grounding (qualitative)

The pre-v613 bundle TASK blocks instructed the LLM to "adapt corpus patterns" but didn't enforce that EACH dialogue line trace back to a SPECIFIC corpus parent. Consequence: the LLM invented dialogue that drifted from the validated patterns in `raw/decoded_*.md` and the niche voiceover-script wiki pages. The user's complaint *"the script doesn't make any sense"* was about exactly this — dialogue that sounds plausible in isolation but doesn't match any corpus voiceover hook, opening rhythm, or claim structure.

The corpus is rich — 16+ decoded competitor videos in `raw/`, niche-specific hook libraries in `wiki/voiceover-scripts/<niche>.md`, the Korella playbook in `raw/Lib Course - Korella.pdf`, the script adaptation reference in `raw/Scripts to adapt_Korella.pdf`. A new video shouldn't reach for novel framing when corpus-validated framings exist for the same niche.

### The rule (v613)

**v613a — Product-mention parity (mechanical, pre-output validator)**:

For every Image N where the prompt body, action_note (in any scene that uses image N), or any clip's `- **line:**` pointed at image N contains a product term (any ingredient name with `type: product` from the Ingredients table, OR brand-name keywords like "Korella," "saffron bottle," "saffron capsule," "Rosabella"), the image MUST have `product_image: <exact-ingredient-name>` set.

**Conversely**, for HOOK images (scenes 1-2) and RECIPE-early images (lemon-pour, ginger-pinch) per the v599 matrix, the prompt body MUST NOT contain any product visual mention. Use a non-product placeholder ("clean cream-tone counter behind," "a small terracotta basil pot"). The product is REVEALED at scene 6 (RECIPE product-cascade) — earlier visibility burns the curiosity loop pre-scene-6.

**v613b — Corpus-grounding (authoring discipline, declared at the top of every videos/*.md)**:

The video frontmatter (or the `## Sources` block immediately under the title) MUST cite at minimum:

1. **2 specific raw/decoded files** that this script adapts from — e.g. `raw/dr_kim_belly_burn_male_decoded.md (clinical-authority HOOK pattern)` + `raw/decoded_corella_saffron_blood_sugar_v584.md (podiatrist + patient active diagnostic)`. Each citation includes the parenthetical PATTERN being borrowed.
2. **The niche voiceover-script wiki page** — `wiki/voiceover-scripts/<niche>.md` — this is the corpus-distilled hook library for the niche. The HOOK line should adapt one of the listed `Opening line` entries from that page's `## Hooks` table.
3. **A "NOTE on cell honesty"** — explicit declaration of whether the cell is corpus-validated (✓ direct adaptation), corpus-adjacent (✓ adapted from neighboring niche), or speculative (⚠ novel territory). Speculative cells should be flagged so the operator knows it's an experiment.

**v613c — Per-line corpus annotation (in action_note, optional but encouraged)**:

For each scene, the action_note can begin with a `[corpus: <source-file> §<section>]` annotation showing which corpus file the dialogue line is paraphrased from. Example:

```
- **action_note:** [corpus: dr_kim_belly_burn_male_decoded.md §HOOK clinical-finding emphasis] Static handheld camera, slight forward push toward the thermometer reading. The main character standing beside the seated patient...
```

When a line is novel (no corpus parent), use `[novel — testing]` instead. This makes the corpus-derivation auditable at review time and keeps scripts from drifting into unfounded territory.

### Pre-output validation gate

Before emitting any videos/*.md draft, the LLM must self-check:

**v613a parity check**:
- For each Image N, list the product terms appearing in: image prompt body, action_notes of scenes pointing to image N, dialogue lines of scenes pointing to image N.
- If ANY product term appears AND `product_image:` is NOT set → REJECT, fix by either (a) setting `product_image:` to the bound ingredient OR (b) removing the product mention if image N falls in HOOK / RECIPE-early per v599 matrix.
- If image N is HOOK (scenes 1-2) or RECIPE-early (lemon-pour, ginger-pinch) per v599 matrix AND prompt body mentions any product → REJECT, replace with non-product anchor language.

**v613b corpus check**:
- Frontmatter or `## Sources` cites ≥2 raw/decoded files? ✓
- Niche voiceover-script wiki page cited? ✓
- Cell honesty note present? ✓
- HOOK line adapts an entry from the niche page's hook table? ✓ (or novel + flagged)

If any ❌ → REWRITE before emitting.

### Worked example — Image 1 + Image 3 fix in the menopause-saffron test video

**Pre-v613 (parity violation)**:

```
### Image 1
- **reference_image:** none
- **Image prompt:**
[...] Bright modern clinical exam room interior with one framed medical certification on the white wall behind, a small American desk-flag stand on the counter, a muscular-anatomy poster on the right edge of frame, and a Korella saffron bottle standing label-forward on the counter behind. [...]
```

The bottle is described visibly but `product_image:` field is absent → Banana 2 invents a generic bottle. Plus this is the HOOK image — v599 matrix says product NOT visible.

**Post-v613 (matrix + parity compliant)**:

```
### Image 1
- **reference_image:** none
- **Image prompt:**
[...] Bright modern clinical exam room interior with one framed medical certification on the white wall behind, a small American desk-flag stand on the counter, a muscular-anatomy poster on the right edge of frame, and a clean cream-tone counter behind (no product visible — HOOK burns the curiosity loop before the product reveal in scene 6). [...]
```

Same scene composition, but the product mention is removed from the prompt body, restoring v599 matrix compliance AND avoiding the binding parity issue.

### Why this matters more than the existing v599 rule

v599 stated the rule clearly but left enforcement to authoring vigilance. Under attention pressure (long prompt body, 4-7 sentence v603 prose, six v606 compositing directives, v610 gender-neutrality scan), the LLM consistently drops the parity check first. v613 codifies the validator as an item in every bundle TASK block ([22]) so the LLM CAN'T emit a draft without scanning for parity violations.

The corpus-grounding half (v613b/c) addresses a different failure: when the LLM "adapts" a script without citing specific corpus parents, it generates plausible-sounding dialogue that drifts from validated voiceover patterns. Forcing each line to trace to a corpus anchor keeps scripts within the corpus's tested-by-virality territory.

### What v613 does NOT change

- v581 binding mechanics (product_image field + binding line + visual description) — same.
- v599 per-scene presence matrix (HOOK NOT visible / RECIPE-early NOT visible / etc.) — same.
- v606 product compositing directives — same six directives apply when product IS visible.
- The niche voiceover-script wiki pages structure — already present, just now mandatory-cited.
- Banana 2 / Veo prompt mechanics — v613 is purely authoring discipline.

---

## Promote-from-images persistence + storyboard mode (v612) — clone-of-promoted-job no longer breaks after redeploy

**Source: 2026-05-06 owner observation** *"the clone of the video i promoted from images job doesnt work.. it doesn include the images and the lines. deeply check it."*

The "promote-to-video" path (`code/image_platform.py:promote_batch_to_video`) takes a completed Banana 2 image batch and creates a video Job + Clip rows from it. Pre-v612 it had two persistence holes that combined to break the clone-of-promoted-job UX.

### Bug 1 — images mirrored locally only, never uploaded to R2

`promote_batch_to_video` copied each chosen variant file from the image-platform variant store to `app_config.uploads_dir / new_job_id / image_NN.png` (LOCAL filesystem) and set `Clip.start_frame = "jobs/{new_job_id}/frames/image_NN.png"` (R2-style key). But the file was **never actually uploaded to R2** at that key.

Knock-on effects on Render's ephemeral filesystem (which wipes on every redeploy):
- `/api/jobs/{job_id}/images/{filename}` 404s — Method 1 (local) fails, Method 2 (R2 lookup at `jobs/{job_id}/frames/{filename}`) finds nothing.
- `/api/jobs/{job_id}/config` returns `images: []` — same lookup chain. The cloneJob frontend reads `data.images` and skips its image-loading block when empty, so the cloned job has no uploaded images.
- The Flow video worker's start_frame fetch fails — the worker reads `Clip.start_frame` (R2 key) and tries to download from R2, but the key doesn't exist.

The standard `/api/jobs` upload path goes through a background task (`main.py` ~line 1936) that uploads frames to R2 via `storage.upload_job_frame(job_id, frame_name, local_path)` and stamps the resulting keys map onto `Job.frames_storage_keys`. Promote was the only Job-creation path that skipped this step.

### Bug 2 — `storyboard_mode` flag missing from promoted job's config

Promote set `assembly_mode: True` in `config_json` but NOT `storyboard_mode: True`. The cloneJob frontend (`code/static/index.html` ~line 10754) checks `cfg.storyboard_mode` to decide whether to:

- Build `sceneBreaks` from `data.scenes`
- Call `setEditorMode('storyboard')` (vs `'auto'`)

When `storyboard_mode` was missing/falsy, even though the response carried full `data.scenes` array, the frontend discarded it and dropped into 'auto' editor mode. The user saw:

- No scene boundaries
- No multi-line scene structure
- No image-line bindings
- An auto-mode editor with possibly-populated dialogue but no way to re-bind it to images

`assembly_mode` and `storyboard_mode` are independent flags: assembly_mode signals "the worker assembles pre-existing clips" (v447 video pipeline behavior); storyboard_mode signals "the editor displays a per-scene UI with image+lines bindings." Promoted jobs are BOTH (assembly-mode for the worker AND storyboard-mode for the editor).

### The fix — v612 inline R2 mirror + flag

In `promote_batch_to_video`:

1. **Initialize R2 storage at function entry** — `is_storage_configured()` + `get_storage()`, gracefully degrades when R2 isn't configured.
2. **Per-image R2 upload** — after `copy2(src, dst)` for each chosen variant, also call `storage.upload_job_frame(new_job_id, dst_filename, dst_path)`. Track filename → R2 key mapping in `frames_storage_keys` dict. Failures log a warning but don't abort the promote (degrades to local-only, which still works on the same server session).
3. **Stamp keys on Job row** — `Job(..., frames_storage_keys=json.dumps(frames_storage_keys) if frames_storage_keys else None)`. The Flow worker (`main.py` ~line 7318) and image-serving endpoint already know to consult this column for R2 fallback.
4. **Add `storyboard_mode: True` to config_dict** — one line, makes cloneJob restore storyboard editor + scene structure correctly.

### Test plan (after deploy)

1. Promote an image batch → produces video Job J1.
2. Verify `Job(J1).frames_storage_keys` is non-null and contains a `{filename: r2_key}` map for every image.
3. Verify `GET /api/jobs/J1/config` returns `images` populated AND `config.storyboard_mode === true`.
4. Trigger Render redeploy (or manually wipe `uploads_dir/J1/`).
5. Re-call `GET /api/jobs/J1/config` — `images` array should still be populated (R2 fallback fires).
6. Click `📋 Clone` on J1 → cloneJob flow should:
   - Fetch images via R2 presigned URLs (no 404s)
   - Restore storyboard mode (scene breaks, image-line bindings)
   - Populate `dialogueInput` with the scene lines

### What v612 does NOT change

- **Job model** — `frames_storage_keys` column existed pre-v612 (`code/models.py:189`); v612 just starts populating it from the promote path.
- **Image-serving endpoint** — pre-existing R2 fallback at `code/main.py:6068` was already correct; the pre-v612 issue was that R2 had nothing to find.
- **Video worker** — pre-existing `frames_storage_keys` consumption path at `code/main.py:7318` was already correct; v612 just makes promoted jobs feed it.
- **Image-platform variant store** — variants stay at `images/<variant_id>.png` under the image-platform R2 prefix. v612 only mirrors COPIES of the chosen variants to the video Job's `jobs/{id}/frames/` prefix at promote time.

### Failure modes still possible after v612

- **R2 not configured** — promote degrades to local-only. Same behavior as pre-v612 on the same server session; first redeploy still wipes everything. Operator-side fix: configure S3_ENDPOINT + credentials.
- **R2 upload fails for some frames** (transient network) — those frames are missing from `frames_storage_keys`; clone after redeploy will be partial. Logged at WARNING. Operator-side fix: re-promote, or manually upload via `_storage_upload_file`.
- **Variant file genuinely missing from R2** — pre-existing v477 R2 rehydration falls through to "permanent loss" 500 error before promote even reaches the upload step. v612 doesn't change that path.

---

## Whisper export — strict matched-word containment (v611) — defense-in-depth final pass

**Source: 2026-05-06 owner observation** *"the whisper export type is still bugging a bit... the final outcome is, i want to maintain only the original lines mentioned in the video, everything else is cut out. you have everything in the code files. check them and find the perfect solution."*

The whisper export pipeline (`code/video_processor.py:detect_speech_segments_whisper`) had accumulated 30+ tuned parameters across v498→v557 to balance "include real speech" vs "cut filler." Each iteration added a guard for a specific failure mode. v611 is the FINAL containment pass — a defense-in-depth check that catches any non-script audio that slipped through the earlier guards.

### Why earlier guards weren't enough

Four leak paths could carry non-script audio into the final export despite v548/v554/v557:

| # | Leak path | Pre-v611 behavior |
|---|---|---|
| (a) | Low-confidence (<0.30) hallucinations | Excluded from `unmatched_words` by `HALLUC_PROB_FLOOR`. Invisible to per-word v554 guard AND to bridger's v548 blocker check. |
| (b) | Clip-final 350ms tail (`EDGE_PAD_END`) | Always applied to the last word of each clip. If the next 350ms is filler/silence/breath, included in segment. |
| (c) | `TAIL_OVERLAP=0.15s` end-pad bleed | Allowed matched-word end-pads to extend 150ms INTO unmatched-word range "for consonant decay." Bleeds in the unmatched word's onset. |
| (d) | Bridger merging across a low-conf hallucination | `BRIDGE_GAP_MAX=0.7s` gaps inside same clip get bridged unless a ≥0.30 conf word sits in the gap. Lower-conf words don't block. |

### What v611 does

After all earlier passes (matching, padding, neighbor-clamp, hallucination guard, grouping, bridging), v611 walks each final speech segment:

1. **Find the matched-word range inside the segment**: locate the first and last matched script words whose timestamps fall inside `(group_start, group_end)`.
2. **End-cap**: scan ALL Whisper words (any confidence — including the <0.30 floor that earlier guards ignored) for the nearest one starting AFTER the last matched word's end and BEFORE the segment's current end. If found, pull segment end back to `unmatched.start - STRICT_END_GUARD (80ms)`. If no unmatched word found in the tail, cap at `last_matched.end + STRICT_FALLBACK_END_PAD (180ms)` instead of the existing 350ms.
3. **Start-cap**: scan for the nearest unmatched word ENDING before the first matched word's start and AFTER the segment's current start. If found, push segment start forward to `unmatched.end + STRICT_START_GUARD (20ms)`. Asymmetric per the v554 carryover: start guard is tight (no pre-onset filler bleed); end guard is moderate (preserves consonant decay tail).
4. **Never extend** — only contract. All earlier preservation logic stays intact for the matched-word interior. v611 cannot add audio to a segment, only remove leak.

### v611 parameters

| Constant | Value | Purpose |
|---|---|---|
| `STRICT_START_GUARD` | 0.02s (20ms) | Buffer between pre-segment unmatched word's end and segment start |
| `STRICT_END_GUARD` | 0.08s (80ms) | Buffer between segment end and post-segment unmatched word's start |
| `STRICT_FALLBACK_END_PAD` | 0.18s (180ms) | Max segment-end extension past last matched word when no unmatched word in tail |
| `STRICT_MIN_END_TAIL` | 0.05s (50ms) | Floor — never trim segment end below `last_matched.end + 50ms` (preserves at least minimal consonant decay) |

### Failure modes addressed

| Pre-v611 leak | v611 fix |
|---|---|
| Low-conf hallucination at clip-final tail (e.g. Whisper transcribes a soft-breath as "uh" with p=0.18, sits 50ms after last script word, 350ms `EDGE_PAD_END` includes its audio) | v611 sees ALL unmatched words. Caps segment end to `unmatched.start - 80ms = ~30ms past last_matched.end`. |
| Filler past last script word, no unmatched word transcribed (silence/ambient) | Fallback cap at `last_matched.end + 180ms` instead of 350ms. Saves 170ms of dead air per clip. |
| Padding overrun absorbed an unmatched word's audio (TAIL_OVERLAP allowed the bleed-in) | v611 final pass re-checks against the unmatched-word range and pulls the edge back to be unmatched-clean. |
| Bridge across low-conf hallucination merged garbage into segment | v611 doesn't unbridge segments (would re-split them), but it caps the segment edges so any garbage past the last matched word gets cut. Mid-segment garbage (between two matched words) survives the bridge — that's an authoring/matcher problem, not a containment problem. |

### What v611 does NOT change

- Earlier matcher / padder / bridger logic — all preserved.
- The matched-word interior (between first and last matched word) — never touched. Consonant decay between adjacent matched words is preserved by intra-pair midpoint clamps.
- Per-clip behavior — v611 operates on already-bridged speech_groups, doesn't see clip boundaries directly.
- Energy-mode silence detection — completely separate code path; v611 only fires when `silence_mode="whisper"`.

### Logging

v611 logs every cap event so the user can verify what was trimmed:

```
[WhisperVAD] ✂ v611 end-cap: 8.453s → 8.227s (filler 'uh' p=0.18 at 8.307s)
[WhisperVAD] ✂ v611 end-cap (fallback): 12.108s → 11.928s (no unmatched in tail, clamp to last-word + 0.18s)
[WhisperVAD] ✂ v611 start-cap: 14.732s → 14.821s (filler 'and' p=0.22 at 14.801s)
```

If v611 ever drops a segment as "unanchored" (no matched words found inside), it logs a warning — that would indicate a bug in the upstream speech_groups construction.

### Reverting / tuning

If a user finds v611 too aggressive, the four constants are at the top of the v611 block. Loosening order (most → least likely to fix over-trim):
1. `STRICT_FALLBACK_END_PAD: 0.18 → 0.30` (give more tail when no filler detected)
2. `STRICT_END_GUARD: 0.08 → 0.04` (closer to filler — preserves more consonant decay)
3. `STRICT_MIN_END_TAIL: 0.05 → 0.10` (always preserve at least 100ms past last matched word)
4. Use only `unmatched_words[≥0.30]` for v611 guard instead of all_unmatched (revert the low-conf-blocker-visibility change)

---

## Gender-neutral main-character references (v610) — never gender the persona in prose

**Source: 2026-05-06 owner directive** *"also when creating a video never assign a gender to the main carchter, always refer as the healer, the main carchter, or anything else that you can think of."*

v602 established that the persona's identity (face, hair, clothing, build) comes from the uploaded reference image — body prose says "the main character" generically rather than the persona's full name. v610 extends that to **gendered pronouns**: prose must NOT use "she / her / hers / he / him / his" to refer to the main character. Identity attributes flow from the upload; prose attributes flow from the prose. When prose says "she lifts her left hand," it's asserting a gender attribute that should come from the upload, and that assertion conflicts with the upload-bound identity model whenever the persona's actual presentation differs from what the prose names. Under attention pressure, Banana 2 may choose either signal — leading to face/body drift between images that all reference the same upload.

The fix is mechanical: drop gendered pronouns when referring to the main character. Use the role descriptor ("the main character," "the healer," "the practitioner," "the host"), the singular "they / their," or pronoun-free constructions ("right hand presses ..." instead of "she presses with her right hand").

### What v610 does NOT change

- **Dialogue** is unaffected. Spoken script lines (`- **line:** ...`) can use any gendered language the persona naturally uses. Dialogue is content, not visual prose.
- **Other characters** in the scene (a patient, a husband bystander, a customer) keep their gendered pronouns. Their identity is described in the prose itself, not bound to an upload, so the gender assertion is the source of truth — there's no upload to drift away from.
- **Persona names** (e.g. "Dr. Amara") in dialogue lines stay verbatim. The persona's name is part of what the persona says about themselves; it isn't a visual claim.
- **The Ingredients table** still names the persona by their canonical role descriptor (`the main character`). No change to v581 / v607 binding behavior.

### Forms

❌ **FORBIDDEN (gendered, pre-v610):**
```
The main character pivots from the patient toward camera, her right hand sweeping in a wide gesture-arc...
She lifts the thermometer away from the patient's temple...
Her left hand steadies the glass at its base while her right squeezes the lemon.
```

✅ **REQUIRED (gender-neutral, v610):**
```
The main character pivots from the patient toward camera, the right hand sweeping in a wide gesture-arc...
The main character lifts the thermometer away from the patient's temple...
The left hand steadies the glass at its base while the right squeezes the lemon.
```

Or with singular-they:
```
The main character pivots from the patient toward camera, their right hand sweeping in a wide gesture-arc.
```

Both forms are acceptable; both eliminate the gender claim from prose.

### Distinguishing persona pronouns from other-character pronouns

When you have a multi-character scene, the persona's pronouns drop; the other characters' pronouns stay. Concrete example from the menopause-saffron HOOK (Image 1):

| Text | Refers to | Treatment |
|---|---|---|
| "the main character's right hand presses ... against the patient's right temple" | persona | "the main character's" — keep, no gender |
| "the patient's right eyebrow lifted in visible surprise" | patient | possessive form `the patient's` — keep, no pronoun needed |
| "her left hand index finger pointing sharply at the reading" | persona | ❌ FORBIDDEN — change to "the left hand index finger" or "the main character's left hand index finger" |
| "the patient (a late-40s woman in a soft beige knit cardigan with dark eye-circles and faint sweat-sheen at her temples)" | patient | "her temples" — keep (patient is described inline; gender claim IS the prose source of truth) |

The mental test: does this pronoun reference an upload-backed ingredient (`type: character`)? If yes → drop the gender. If no → keep.

### Non-persona characters: gender description is REQUIRED

The flip side of the v610 rule. For every non-persona character in the scene (patient, bystander, customer, husband, wife, child, friend), the prose MUST describe their gender, age band, body build, and clothing/role. Their identity is NOT bound to an upload — the prose is the only source of truth.

✅ REQUIRED examples:
- *"a late-40s woman in a soft beige knit cardigan with dark eye-circles"*
- *"an adult male patient seen from behind in a teal hospital gown"*
- *"her husband, a middle-aged man asleep in the bed behind"*
- *"a young female customer at the counter to the right"*

❌ FORBIDDEN (when the character is non-persona):
- *"the patient"* — without gender / age / clothing → Banana 2 hallucinates a generic body
- *"another person beside them"* — vague gender → drift across variants
- *"they sit on the chair"* — when the source video clearly shows a specific gender, the decoder MUST commit to what the camera shows

This applies especially to **decode-side** authoring: when decoding a competitor video, the decoder is OBSERVING what the camera shows. If the patient is a woman, write "a woman." Don't apply persona-style gender-neutrality to non-persona characters — they're a different type of entity.

**The asymmetry rationale**: the main character will be SWAPPED on lift (a Black-female-practitioner upload one day, a Korean-male-practitioner upload another). Other characters are NOT bound to uploads — the prose is the only source of truth for their appearance. Vague non-persona descriptions produce Banana 2 hallucinations across variants. Generic "they" for a clearly-gendered patient = different patients across variant 1 / 2 / 3 / 4 within the SAME image generation.

### Why singular-they is acceptable but role-descriptor is preferred

- **"the main character" / "the healer" / "the practitioner"** — best. Reads naturally, names the role, stays generic. Use this on first reference per scene and after long subjects.
- **"their" / "they"** — acceptable. Cleaner than awkward repeated role-descriptors when the same subject is in three consecutive sentences. Banana 2 tolerates singular-they without confusion.
- **No subject ("right hand presses ...")** — best for action-rich descriptions. The active body part becomes the subject; identity stays in the upload.

Pick whichever reads cleanest in the local sentence. The hard constraint is just: zero gendered pronouns referring to the persona.

### Pre-output validation gate

Before emitting any Image prompt body or action_note, scan for gendered pronouns that refer to the persona:

- ✅ Zero instances of `\bshe\b`, `\bhe\b`, `\bher\b`, `\bhis\b`, `\bhim\b`, `\bhers\b` referring to the main character?
- ✅ Persona references use role descriptor ("the main character," "the healer," "the practitioner") OR singular-they OR pronoun-free body-part subjects?
- ✅ Gendered pronouns referring to the patient, husband bystander, or other prose-described (non-upload) characters are unchanged?

If any persona-pronoun found, REWRITE before emitting.

### Why v610 vs leaving gender implicit in prose

v602 said "use the role descriptor instead of the persona's full name" so the persona could be swapped without rewriting every scene. v610 closes the same hole at the pronoun level: a video designed for one persona shouldn't need a prose rewrite to ship for a different persona presenting differently. Keep prose generic; let the upload carry identity. This makes videos / cells reusable across the persona library — the same prose body can ship with a Korella-female-practitioner upload, a Korella-male-practitioner upload, or a Korella-non-binary-practitioner upload, without re-authoring.

It also removes a class of generation drift: when the prose asserts "she" but the upload's apparent presentation is read by Banana 2 differently (or the upload is updated to a different presentation later), the model has two conflicting identity signals. Drop the prose claim and the conflict cannot occur.

### Worked example — Image 2 from the menopause-saffron HOOK

**Pre-v610 (gendered, persona-locked)**:
> The main character pivots from the patient toward camera, thermometer still aloft in her left hand with display GLOWING red, her right hand sweeping in a gesture-arc...

**Post-v610 (gender-neutral, persona-portable)**:
> The main character pivots from the patient toward camera, thermometer still aloft in the left hand with display GLOWING red, the right hand sweeping in a gesture-arc...

Same scene. Same composition. Zero gender claim. Reusable across persona library.

---

## Concise reference-binding form (v609) — drop the redundant "match X, Y, Z exactly" clause

**Source: 2026-05-06 owner observation** *"`Use the uploaded product reference image for the Korella saffron` — for the images prompt when the product is needed this above is the right format, no this one: `Use the uploaded product reference image for the Rosabella Beetroot bottle — match its label, packaging, color, and proportions exactly.` nano bana match automatically the info, packaging, color and proportions."*

Banana 2 already matches the uploaded reference image's visual attributes (face / hair / clothing for characters; label / packaging / color / proportions for products) **automatically** when the prompt body says `"Use the uploaded [character|product] reference image for [name]"`. The verbose `"— match its label, packaging, color, and proportions exactly"` clause adds nothing. It's redundant noise that dilutes the model's attention from the actual per-image directives (composition, action, lighting, prop position, occlusion).

This rule supersedes the verbose binding form that appeared in v581's first draft and was carried forward through v589.1 / v603 examples by inertia. v609 is purely a prompt-quality cleanup: shorter prompts, sharper attention, no behavioral change in the platform parser (the slot-substitution at `_resolve_flow_prompt_bindings()` still triggers on the literal phrase `"the uploaded character reference image"` / `"the uploaded product reference image"`, regardless of what follows the ingredient name).

### Forms

❌ **FORBIDDEN (verbose, pre-v609):**
```
Use the uploaded character reference image for the main character — match her facial features, identity, hair, and clothing exactly.
Use the uploaded product reference image for the Korella saffron bottle — match its label, packaging, color, and proportions exactly.
```

✅ **REQUIRED (concise, v609):**
```
Use the uploaded character reference image for the main character.
Use the uploaded product reference image for the Korella saffron bottle.
```

### Why concise wins

1. **Banana 2 already does the matching automatically.** Per Google's official Gemini Nano Banana 2 docs, when a generation cites an uploaded reference image as the visual source for a named ingredient, the model preserves the upload's facial geometry, identity markers, hair, clothing, label artwork, color palette, and proportions by default. There is no toggle to "match harder" — the verbose clause does not increase fidelity.
2. **Attention dilution.** Each image prompt has a finite budget of attention the model spreads across directives. Spending a sentence on "match X, Y, Z exactly" — for a behavior the model performs by default — is attention NOT spent on the per-image directive that actually matters (e.g. v600 cartoon-physics, v601 active interaction, v605 prop position, v606 compositing). On dense prompts, the verbose clause measurably erodes those harder-to-enforce rules.
3. **Cleaner human authoring.** The author is forced to think about what's UNIQUE to this image — the action, framing, prop position, lighting — instead of restating boilerplate that's identical across every image of the video.
4. **Cleaner decoded artifacts.** Decode-side, the rule applies symmetrically: when a decoder writes a freshly-decoded `raw/decoded_*.md`, the binding lines should be concise. v609 keeps decode artifacts compact and high-signal.

### What v609 does NOT change

- The 3 binding line types (PERSONA / PRODUCT / CHAIN) and their order — same as v581.
- The platform's slot-substitution behavior at `_resolve_flow_prompt_bindings()` — triggers on the same trigger phrases.
- Whether persona / product / chain bindings appear — same rules as v581 + v607 (persona always; product when `product_image:` is set; chain when `reference_image:` is set).
- The CHAIN line wording from v589.1 (semantic "the prior-scene reference image" form) — kept verbatim.

### Pre-output validation gate

Before emitting any Image prompt body, scan the binding lines:

- ✅ PERSONA line ends with the ingredient name + period? (no "— match her ... exactly" trailer)
- ✅ PRODUCT line (when present) ends with the ingredient name + period? (no "— match its ... exactly" trailer)
- ✅ CHAIN line uses the v589.1 semantic form? (no behavior change here from v589.1)

If any ❌ found, REWRITE before emitting.

### Worked example — Image 2 from the menopause-saffron HOOK

**Pre-v609 (verbose, redundant)**:
> Use the uploaded character reference image for the main character — match her facial features, identity, hair, and skin tone exactly.
> Use the uploaded product reference image for the Korella saffron bottle — match its label, packaging, navy-and-cream wordmark, color, and proportions exactly.

**Post-v609 (concise)**:
> Use the uploaded character reference image for the main character.
> Use the uploaded product reference image for the Korella saffron bottle.

Both forms produce identical persona-fidelity and product-fidelity from Banana 2. The concise form leaves more attention budget for the v600/v601/v605/v606 directives that actually drive the per-image distinctiveness.

---

## Worker file_chooser bypass (v608) — skip 8s of guaranteed-fail file picker calls

**Source: 2026-05-06 owner observation** *"and now that we are changing the worker let's imprve also these warning or mechanism... it works as it is, just avoid these steps that are nto working."* The worker logs from a normal run showed:

```
[node_943] ⚠ File chooser attempt 1 failed: Timeout 4000ms
[node_943] ⚠ File chooser attempt 2 (after reset) failed: Timeout 4000ms
[node_943] ⤴ Sent chain_from_image_3.png (last page-wide input (of 1)) — verifying chip
[node_943] ✓ Recovered via gallery
```

Both file_chooser attempts **always fail** in the current Patchright + Chrome combo. The path that actually works is the set_input_files fallback on the freshly-mounted `<input type="file">`. So the two file_chooser try-blocks waste ~8 seconds per uploaded reference image (4000ms timeout × 2 + dialog reset overhead) on every single upload — and that's the path executed for every chained image, persona ref, and product ref.

### The mechanic — why file_chooser doesn't fire

Playwright's `page.expect_file_chooser()` listens for Chrome's file picker dialog event. Chrome only emits that event when triggered by an `isTrusted=true` user gesture. Patchright synthesizes clicks via CDP, but Chrome flags those as `isTrusted=false` (defense-in-depth against automation), and the file picker is suppressed.

What still works: clicking the upload tile mounts a fresh `<input type="file">` element in Flow's React tree (the React onClick handler runs regardless of isTrusted because it's a JS-level event). We can then call `set_input_files()` directly on that newly-mounted input. The file gets attached without ever opening the OS file picker.

### v608 flow (what the worker now does)

1. Snapshot the page-wide `input[type='file']` count BEFORE clicking.
2. Click the upload button once. **No `expect_file_chooser` wrapper.** No retry. No 4-second timeout.
3. Sleep 0.6-1.0s for Flow to mount the new input.
4. Run the existing strategy chain (dialog-scoped → newly-mounted → portal → last-on-page) to find the input and call `set_input_files()`.
5. Verify the chip attached. If not, fall back to gallery recovery (find the file in the gallery via its filename, click it).

### Time savings

| Path | Before (v607-) | After (v608) |
|---|---|---|
| Successful chip on first try (the common path) | ~10s (4s fc-attempt-1 timeout + reset + 4s fc-attempt-2 timeout + 2-5s set_files sleep + chip-verify) | ~2s (1s click+mount + chip-verify) |
| Per ref image | 8s wasted on guaranteed timeouts | 0s wasted |
| 9-image video × 2 refs/image | 144s wasted on file_chooser timeouts | 0s wasted |

### Code locations

- `code/image_worker.py` `upload_reference_images()` Step 2b — the two `expect_file_chooser` try-blocks were removed; replaced with a single `upload_btn.click()` + brief `time.sleep()`.
- The set_input_files strategy chain (Strategies 1-4) was preserved verbatim. The `if not uploaded:` gate was removed since the strategy chain is now the sole upload path.

### When file_chooser starts working again (forward compatibility)

If a future Patchright update ships an isTrusted=true synthesis, or Flow stops requiring the OS file picker entirely, the v608 path is still correct: `set_input_files()` on the freshly-mounted input is functionally equivalent to `fc_info.value.set_files()`. No regression. We just stop racing against the picker that was never going to fire anyway.

### What v608 does NOT change

- The retry logic when the chip doesn't attach (gallery recovery path) — kept.
- The `MAX_ATTACH_RETRIES` budget per image — kept.
- The `flow_worker.py` `upload_frame()` function (used by clip uploads, not image refs) — kept, because that path's logs aren't reporting the same failure pattern. Apply v608 to flow_worker only if/when the same telemetry shows there.

---

## Worker character force-bind (v607) — every image gets the persona ref attached, mention or no mention

**Source: 2026-05-06 owner observation** *"in this video in on eimage it didn't include teh caracheter image, is it because it's not in the video information, or it should be the worker doing it? poin is: we need the carachter image as reference prompted in flow."*

The user observed that on at least one image, the character (persona) reference upload was missing from Flow's reference slots. The worker received only the chain ref (`chain_from_image_3.png`) and no character ref — so Flow generated an arbitrary face on the next take, breaking persona continuity.

### Root cause

The platform binds the persona-ingredient parent edge ONLY when the body prose **literally contains** the ingredient's name (e.g. "the main character"). The detection lives in `_extract_ingredient_names_in_prompt()` — a substring scanner over the body. If a v602/v603 prompt body drops the literal "the main character" phrase (e.g. a prop-focused close-up that just describes the bottle on the desk), no persona edge gets created at import time. Then when the worker pulls the job, `input_images` arrives without the persona slot, and Flow has no character reference to anchor the face to.

This is a brittle binding pattern. Persona identity is a **video-level invariant** — every image where the character could appear must reference the persona upload, regardless of the per-image phrasing.

### The fix — force-bind characters at import

After the v581 product_image force-bind block in the per-image binding loop (`code/image_platform.py` `import_video()`), add a loop that scans `ingredient_types` for any `character`-typed ingredient and force-adds it to the `mentioned` list. The downstream slot-priority sort (`_slot_priority`) already gives `_is_persona_alias()` names slot 0, so the character always wins the lowest available reference slot in Flow.

```python
# v607: force-bind any character-typed ingredient even when the body
# prose doesn't literally mention it. ...
for _ing_name, _ing_type in ingredient_types.items():
    if _ing_type == "character" and _ing_name in ingredient_nodes:
        if _ing_name not in mentioned:
            mentioned.append(_ing_name)
            log.info(
                f"[import] Image {image_index}: v607 force-bind "
                f"character '{_ing_name}' (not mentioned in body)"
            )
```

### Why force-bind characters (not products)

- **Products**: bind only when the prompt explicitly references them. A close-up on the persona's face shouldn't have the saffron bottle attached. v581 already handles this with the explicit `product_image:` field.
- **Characters**: bind on every image. Persona identity is the through-line of the video. Even if a particular shot is a tight close-up on a prop, Flow's slot manifest still benefits from having the persona reference present (Flow ignores unused slots; nothing breaks). The win: zero risk of an unbound face popping up mid-video.

### What this does NOT do

- Does NOT bypass the 3-parent slot cap. Character gets slot 0 (priority), product slot 1, chain slot 2 — same as before. v607 just guarantees the character edge always exists, not that it always gets a slot. In practice the cap rarely matters: most images have ≤3 binding candidates.
- Does NOT force-add characters that aren't in the Ingredients table. The character must still be declared with `type: character` in the `## Ingredients` block of the video markdown. v607 is "force-bind a declared character"; it's not "invent a character."

### Symptom you're looking at if v607 didn't fire

Look at the worker log for an image that should have a character but doesn't:

```
[API:submit]    Inputs: 1 ref(s)              ← only 1 ref
[node_X]   Image 1/1: chain_from_image_N.png  ← only the chain, no character
```

Compare with v607 active:

```
[API:submit]    Inputs: 2 ref(s)              ← 2 refs
[node_X]   Image 1/1: variant_<persona_id>.png  ← persona attached
[node_X]   Image 2/2: chain_from_image_N.png   ← chain also attached
```

### LLM-author counterpart (still recommended)

Even with v607 force-binding the character at import, body prose should mention "the main character" verbatim somewhere in every image where the character is visible — the prose mention is what gives Flow's slot manifest a textual anchor for the slot. v607 ensures the slot is **bound**; the body prose ensures the slot is **used**. Both layers reinforce each other.

---

## Product compositing / lighting integration (v606) — make the product melt into the scene

**Source: 2026-05-06 owner observation** *"we need to improve the prompting according to nano bana prompting rules to make the product melt into the image and not look like it's photoshopped."* The first generated frame from the menopause-saffron HOOK had the Korella saffron bottle visibly photoshopped-in: oversized (~12-15 inches vs real ~5-inch supplement), self-lit (product-shot lighting on label that didn't match cool-clinical room ambient), floating-flat (no cast shadow on desk, hard edges), color-pop saturated, no foreground occlusion. The bottle read as a separate product render dropped onto the scene, not as an object IN the scene.

Per Google's Gemini Nano Banana 2 official prompting docs (ai.google.dev/gemini-api/docs/image-generation), uploaded reference products integrate into generated scenes ONLY when explicitly prompted to do so. By default, Banana 2 places the upload at product-shot scale with its own lighting — that produces the "photoshopped-in" look every time. v606 codifies six compositing directives that must appear in every Image prompt body where a product is visible.

### The 6 compositing directives

Every Image prompt body where a product is visible (i.e. has `product_image:` field set) must include compositing directives in these six dimensions. Body prose adds a final compositing paragraph BEFORE the v603 closing tag and AFTER the negative-constraint block.

#### [a] Scale anchor — realistic real-world size

❌ FORBIDDEN: "the Korella saffron bottle stands upright on the counter, label-forward to camera" — gives no scale information; Banana 2 places at default product-shot scale (oversized).

✅ REQUIRED: anchor the bottle's size to a scene element.

- "the Korella saffron bottle is shown at realistic supplement-bottle scale, approximately 5 inches tall"
- "the bottle's height is approximately 1/4 of the persona's torso width"
- "the bottle is sized so it would fit naturally in the persona's palm" (when held)
- "the bottle is roughly the same height as the glass tumbler beside it"

#### [b] Lighting integration — match scene light source + color temperature

❌ FORBIDDEN: "label clearly readable", "wordmark squared to lens", "navy-and-cream wordmark visible" without a lighting anchor — these read as product-shot directives.

✅ REQUIRED: explicitly state that the bottle is lit BY the scene's light source(s), at the scene's color temperature.

- "the bottle is lit by the same warm window-soft daylight as the rest of the kitchen — no dedicated product-shot lighting"
- "the bottle's surface picks up the cool-clinical LED ambient of the exam room — slight cool-white highlights on the cap, label colors subtly desaturated to match the muted clinical color palette"
- "the bottle's white plastic cap reflects the warm honey-oak shelving behind in a soft amber tint"
- "the label is in scene-ambient lighting (warm domestic daylight) — readable but not over-illuminated"

#### [c] Cast shadow — explicit shadow on the surface

❌ FORBIDDEN: bottle described without any shadow, or with vague "stands on the counter."

✅ REQUIRED: state cast shadow direction + softness + length, matching the scene's light direction.

- "the bottle's base casts a soft natural cast shadow on the desk surface, falling viewer-right at a 30-degree angle, matching the room's window light from camera-left"
- "the bottle has a subtle cast shadow extending viewer-right approximately 2 inches, soft-edged from the diffuse window light"
- "the bottle's shadow on the marble counter is faint and warm-toned, matching the late-morning daylight"

#### [d] Perspective integration — match scene camera angle

❌ FORBIDDEN: "label-forward to camera" / "wordmark squared to lens" without a perspective anchor — these conflict with the scene's actual camera angle.

✅ REQUIRED: state the bottle's tilt/angle relative to the scene's camera position.

- "the bottle is shot from the same camera angle as the rest of the scene (slightly above desk-eye-level), so the label is angled slightly upward toward the camera, with the cap visible at the top"
- "the bottle's perspective matches the room's vanishing point — slight tilt back at the top because the camera is angled down at the desk surface"
- "the bottle is shown straight-on at chest-height, label visible but with natural perspective foreshortening because the persona's hand grips it slightly off-axis"

If the scene camera is at desk-eye-level, the bottle on the desk should be near-straight-on. If the camera is above, the bottle should be slightly foreshortened. State this explicitly.

#### [e] Surface contact — physical placement, no floating

❌ FORBIDDEN: bottle simply "on the counter" or "in her hand" — generic placement language can produce floating-bottle results where the bottle doesn't appear in physical contact with the surface or hand.

✅ REQUIRED: state the contact point + grip explicitly.

- "the bottle's base sits flush on the wooden desk surface, in clear physical contact, no floating gap"
- "the bottle is gripped firmly in her viewer-left hand, fingers visibly wrapping the cylindrical body, thumb on the cap top, the persona's palm in contact with the bottle's lower third"
- "the bottle's base is in soft contact with the marble counter, with the contact line clearly visible at the bottom edge of the bottle"

#### [f] Natural occlusion — foreground breaks the silhouette

❌ FORBIDDEN: bottle as the dead-center hero element with nothing in front of it — produces a "cut-and-paste" look.

✅ REQUIRED: something in the foreground partially obscures the bottle's silhouette, breaking the cut-out edge.

- "a small portion of the bottle is partially obscured by the persona's hand in the foreground"
- "the bottle is partially behind the wooden cutting board in the foreground, breaking the silhouette so it looks naturally placed in the kitchen workspace"
- "the bottom of the bottle is partially behind the desk edge in the immediate foreground"
- "the persona's gesturing hand on the viewer-left side partially crosses in front of the bottle's lower third"

Even subtle foreground occlusion (a hand grazing the edge of the bottle) breaks the cut-and-paste look that Banana 2 defaults to.

### Compositing paragraph format

Every product-bearing Image prompt body should include a final compositing paragraph BEFORE the v603 closing tag (`"iPhone HDR colors, deep focus."`) and AFTER the main scene description but BEFORE the negative-constraint block:

```
[scene description with persona, props, framing, action]

The bottle integrates naturally with the scene: [a] realistic supplement-bottle scale (~5 inches tall), [b] lit by the same [scene lighting source] as the room with no dedicated product-shot lighting, [c] base [contact-point] with a soft natural cast shadow [direction + length], [d] perspective matching the scene's [camera angle], [e] [grip or surface-contact detail], [f] partially occluded by [foreground element] breaking the silhouette.

iPhone HDR colors, deep focus.

[negative constraints — including v606-specific anti-photoshop ones below]
```

### v606 negative constraints (mandatory addendum to existing negative-constraint block)

Add these to the closing negative-constraint block in every product-bearing image:

- "No dedicated product-shot lighting on the bottle — same ambient lighting as the rest of the scene."
- "No oversized bottle — realistic supplement-bottle scale (~5 inches tall)."
- "No floating bottle — must be in physical contact with the surface or hand."
- "No hard cut-and-paste edges — bottle blends into scene with natural ambient transitions."
- "No color-saturated label — colors match the room's color temperature and may appear slightly desaturated to match scene ambient."
- "No center-stage product hero-shot composition — bottle is integrated into the scene, partially occluded by foreground elements."

### Pre-output validation gate

Before emitting any Image prompt with a `product_image:` field, scan the body prose for ALL six directives:

- ✅ [a] Scale anchor present (realistic-size statement + scene-element anchor)?
- ✅ [b] Lighting integration present (lit by scene's light source, color temperature stated)?
- ✅ [c] Cast shadow present (direction + softness + length stated)?
- ✅ [d] Perspective integration present (bottle angle matches scene camera)?
- ✅ [e] Surface contact / grip explicit (no floating bottle)?
- ✅ [f] Natural occlusion (foreground element partially crosses bottle silhouette)?
- ✅ v606 negative constraints added to closing negative-constraint block?

If any ❌ found, ADD before emitting.

### Why v606 vs leaving compositing implicit

v599 enforced product-presence (3-part binding + bottle visible label-forward). v605 enforced prop-position-grounding (where the bottle is, citing VLM source). Neither addressed COMPOSITING — how the bottle integrates LIGHTING-WISE and PHYSICALLY into the scene. Banana 2's default behavior with an uploaded product reference is to render it at product-shot quality with its own lighting and place it center-frame at oversized scale. Without explicit compositing directives, every product-bearing image looks photoshopped.

The user framing: *"make the product melt into the image and not look like it's photoshopped."* v606 is the rule that operationalizes "melt." Six directives, all six required, mechanical anti-photoshop gate.

### Worked example — Image 2 (HOOK reveal with bottle on counter)

**Pre-v606 (photoshopped look)**:
> The Korella saffron bottle now prominent in the lower-foreground of the desk in front of her, label-forward, navy-and-cream wordmark squared to lens.

**Post-v606 (compositing directives applied)**:
> The Korella saffron bottle is in the lower-foreground of the desk, sized at realistic supplement-bottle scale (~5 inches tall, roughly 1/4 of the persona's torso width). The bottle is lit by the same cool-clinical LED ambient as the exam room — slight cool-white highlights on the white cap, the navy-and-cream label colors subtly desaturated to match the muted clinical palette, no dedicated product-shot lighting. The bottle's base sits flush on the desk surface in clear physical contact, casting a soft natural shadow viewer-right approximately 2 inches at a 30-degree angle from the room's window light source camera-left. The bottle is shown at the same camera angle as the rest of the scene (slightly above desk-eye-level), so the label is angled slightly upward toward camera with the cap visible at the top. The persona's gesturing hand on the viewer-left side partially crosses in front of the bottle's upper third, breaking the silhouette so the bottle reads as naturally placed in the workspace, not as a separate product render. iPhone HDR colors, deep focus.
>
> No dedicated product-shot lighting on the bottle. No oversized bottle. No floating bottle. No hard cut-and-paste edges. No color-saturated label. No center-stage product hero-shot composition.

That's what makes the product melt into the scene.

---

## Decoder anti-template-bias + prop-tracking matrix + prop-as-subject priority (v605)

**Source: 2026-05-06 Gemini decode session** (`raw/decode_prompt_accuracy_gemini_2026-05-06.md`) — same `decoded_healthylifesage_DX5jJgeMj30.md` decode that surfaced v604, but a different failure mode: Image 5 placed the Rosabella bottle ON THE DESK (corpus default) when the actual source video shows Dr. Sage HOLDING the bottle up to camera in his blue-gloved left hand.

Gemini's self-diagnosis:

> *"I encountered a gap in my sparse frame data regarding exactly where the bottle was. Instead of flagging the gap, I fell back on a standard Kaveno corpus template (specifically, the nuri-saffron pattern), where the product sits anchored on the desk while the persona holds the capsule."*

> *"AI models are probability engines. When we lack explicit, high-fidelity data, we default to the most common pattern. Forcing me to cite the exact frame data — and explicitly forbidding me from using fill-in-the-blank templates for props — is the mechanical fix."*

This is **template bias** — the decoder fills VLM-data gaps with the most-statistically-likely corpus pattern. v605 codifies the fixes:

### [a] Anti-template-bias — FLAG GAPS, never fill with corpus prior

When the Stage 4d VLM data has a gap about a prop's position or handling, the decoder MUST:

1. **Flag the gap explicitly** in the decoded artifact:
   ```
   <!-- VLM-GAP: bottle position not visible in dense frames at 105.2s/106.0s/106.8s; mid_state describes only persona torso. -->
   ```
2. **Provide best-effort description sourced from visible frames**, with a confidence annotation:
   ```
   - **prop_position:** UNCLEAR — frames show persona torso facing camera; bottle inferred to be at chest height held by blue-gloved hand based on dialogue cue "the only one I trust" (low confidence; flag for operator review)
   ```
3. **Never silently substitute** a corpus template default ("bottle on desk because that's how nuri-saffron does it"). Corpus priors are EVIDENCE OF WHAT WORKS, not EVIDENCE OF WHAT THIS VIDEO ACTUALLY SHOWS.

The rule:

> The position of the product must be explicitly sourced from the Stage 4d VLM JSON (`start_state`, `mid_state`, `end_state`). If the VLM data does not explicitly state the bottle is "on the desk," do not place it there. If the data is missing, flag the missing data rather than inventing a composition.

This is the **mechanical anti-bias gate** — the decoder must cite the source frame for every prop-position claim, or flag the absence.

### [b] Prop-tracking matrix — explicit prop-position field per product image

Every Image with a `product_image:` field set MUST also have a `prop_position:` field that explicitly answers:

1. Is the product **interacting with environment** (on desk / counter / shelf / floor) — and if so, where?
2. Is the product **interacting with persona** (held in viewer-left hand / viewer-right hand / both hands) — and if so, at what height (chest / chin / waist / above-head) and orientation (label-forward / label-back / vertical / horizontal)?
3. The matrix answer must come from VLM frame data (cite the timestamp / state field), not from corpus prior.

Format:
```
### Image 5
- **frame_anchor:** 106.0s
- **reference_image:** image_4
- **product_image:** the Rosabella Beetroot bottle
- **prop_position:** held in viewer-left hand at chest height, label-forward to camera, wordmark squared to lens (sourced from VLM mid_state at 106.0s + end_state at 107.5s)
- **visual_delta:** Rosabella Beetroot bottle enters frame on viewer-left side, held at chest height by blue-gloved left hand, label-forward; viewer-right hand gestures next to the bottle.
- **Image prompt:**
```

The `prop_position:` field is the explicit prop-tracking declaration. It forces the decoder to answer "where is the prop and how is it being handled?" before authoring the body prose. This breaks the template-bias trap (decoder can no longer skip the question).

### [c] Prop-as-Subject priority for product-reveal scenes

When an image has `product_image:` set (i.e. it's a product-reveal frame), the body prose MUST allocate description weight as:

- **60%** on prop handling — how the product is held, manipulated, positioned, presented (hands relative to product, label orientation, height, lighting on the bottle)
- **40%** on persona pose — eye-contact, body language, facial expression

For non-product-reveal scenes (HOOK, recipe-prep, EXPLAIN with no bottle), the standard v603 prose discipline applies (4-7 sentences, no specific allocation).

The principle: when the product is in the frame, the product IS the subject of the photograph. The persona is the secondary anchor. Pre-v605, decoders led with persona-pose ("Dr. Sage is seated at his desk, eyes locked to camera...") and demoted the product to a background anchor ("The bottle is on the desk in front of him"). v605 inverts this — when the prop is in frame, lead with the prop.

**Pre-v605 (persona-led)**:
> Dr. Sage is seated at his walnut desk, eyes locked to the camera lens, expression warm and authoritative. The Rosabella bottle is on the desk in front of him, label visible.

**Post-v605 (prop-led)**:
> The Rosabella beetroot bottle is held up at chest height in his blue-gloved viewer-left hand, presented directly toward the lens, label-forward, wordmark clearly readable. His viewer-right hand gestures next to the bottle for emphasis. He is seated at his walnut desk with eyes locked to camera, expression warm and authoritative.

The prop-led version names the prop in the first 6 words. Banana 2 prioritizes the subject named first.

### [d] Strict adherence to VLM action_arc JSON for prop placement

This is a downstream pipeline rule that applies when the operator runs the v589 Stage 4d VLM pass: the parser must provide the FULL `action_arc` JSON (start_state / mid_state / end_state) to the LLM context, not just a midpoint summary. The LLM must base its image grammar strictly on the dense-frame VLM descriptions, not on dialogue extrapolation.

For Claude in-session decodes (the v595 default provider), this means the operator/Claude should walk dense frames per shot via the Read tool's PNG support and explicitly cite which frame timestamps were viewed for each `prop_position` claim. The frame audit trail is the anti-bias receipt.

### Pre-output validation gate

Before emitting any decoded artifact with product_image fields, scan for:

- ✅ Every Image with `product_image:` has `prop_position:` field declared
- ✅ Every `prop_position:` cites a VLM frame timestamp or state field as the source
- ✅ For images where VLM data has a gap, an explicit `<!-- VLM-GAP: ... -->` annotation is present (not silently substituted with corpus prior)
- ✅ Body prose for product-reveal images is prop-led (prop named in first sentence)
- ✅ Body prose allocates ~60% to prop handling, ~40% to persona pose for product-reveal images
- ❌ NO references to corpus templates ("nuri-saffron pattern", "standard product-anchor desk shot") as source of prop placement

If ❌ found, FIX before emitting.

### Why v605 vs leaving it implicit

Pre-v605, v599 enforced product-presence (bottle visible + 3-part binding) but not prop-position-grounding. The decoder could honor v599 ("bottle is in the frame, label-forward") while silently inventing the position from corpus prior ("bottle on desk because that's typical"). v605 closes this gap with `prop_position:` field + VLM-citation requirement + prop-as-subject priority.

The user framing: AI models are probability engines that fill data gaps with statistical priors. v605 forces decoders to either CITE source-frame evidence or FLAG GAPS — never substitute a template default.

### Worked example — Image 5 healthylifesage Rosabella

**Pre-v605 (template bias — bottle on desk because nuri-saffron pattern says so)**:
```
### Image 5
- **reference_image:** image_4
- **product_image:** the Rosabella Beetroot bottle
- **Image prompt:**

[persona pose led] Dr. Sage is seated at his walnut desk, eyes locked to camera, expression warm and authoritative. [bottle as background anchor] The Rosabella bottle stands upright on the desk in front of him, label-forward to camera. [generic style] iPhone HDR colors, deep focus.
```

**Post-v605 (VLM-grounded, prop-led, viewer-relative)**:
```
### Image 5
- **frame_anchor:** 106.0s
- **reference_image:** image_4
- **product_image:** the Rosabella Beetroot bottle
- **prop_position:** held in viewer-left hand at chest height, label-forward to camera, wordmark squared to lens, fingers wrapping the cap top (sourced from VLM mid_state at 106.0s — full skin contact between blue nitrile glove and bottle base visible in dense frame)
- **visual_delta:** Rosabella Beetroot bottle enters frame on viewer-left side, held at chest height by blue-gloved left hand, label-forward; viewer-right blue-gloved hand gestures next to the bottle for emphasis.
- **Image prompt:**

Use the uploaded character reference image for the main character.
Use the uploaded product reference image for the Rosabella Beetroot bottle.
Use the prior-scene reference image to preserve the walnut desk, warm wood-paneled office, framed diplomas, lighting, framing, and continuity from the previous scene.

Use image_4 as the exact base frame. Keep everything from image_4 identical. Only change: the Rosabella Beetroot bottle is held up at chest height in his blue-gloved viewer-left hand, presented directly toward the lens, label-forward to camera, navy-and-cream wordmark squared to lens, fingers wrapping the cap top. His viewer-right blue-gloved hand gestures next to the bottle for emphasis. He is seated at his walnut desk with eyes locked to camera, expression warm and authoritative. Shot on iPhone wide-angle lens, handheld, deep focus throughout, vibrant natural HDR daylight. iPhone HDR colors, deep focus.

No lab coat. No stethoscope. No hospital room. No extra products. No recipe ingredients. No background change. Bottle is real-supplement-sized, not oversized, not floating, not redesigned. Bottle is NOT on the desk in this image — it is held in the persona's viewer-left hand.
```

Note the explicit **"Bottle is NOT on the desk in this image"** in the negative-constraint block — this directly counters the template-bias default that would otherwise place it there.

---

## Decode-prompt accuracy + universal prompt-discipline (v604) — frame-locked, viewer-relative, negative-constrained

**Source: 2026-05-06 ChatGPT decode session** (`raw/decode_prompt_accuracy_chat_2026-05-06.md`) — operator audited a freshly-decoded `decoded_healthylifesage_DX5jJgeMj30.md` and found two failure modes:

1. Image 4 and Image 5 should have been **chained** (same clinician, same desk, same diploma-wall office, same camera angle, same shirt/gloves, same framing — only Rosabella bottle enters the frame in Image 5). The decoder separated them because dialogue beat changed (explanation → product reveal). Visual continuity > dialogue grouping.
2. Image prompts described the **idea** of the scene, not the **frame evidence**. That makes generators drift. Prompts need to be frame-locked: same camera, same body crop, same objects, only the visible delta per scene.

v604 codifies the fixes. Decode-side gets two new fields (`frame_anchor`, `visual_delta`); both decode and generate sides get four universal prompt-discipline rules.

### Decode-side new fields

#### `frame_anchor` — timestamp of the source-video key frame this image describes

Every Image block in a decoded artifact (`raw/decoded_*.md`) should declare a `frame_anchor:` field with the source-video timestamp. Format: `frame_anchor: 0.5s` / `frame_anchor: 12.0s` / `frame_anchor: 106.0s`. This locks the image to a single frame from the source, not to a scene-idea description.

```
### Image 1
- **frame_anchor:** 0.5s
- **reference_image:** none
- **Image prompt:**
```
> At 0.5s, he holds a wounded foot upright in the center of frame...

vs. the failed pattern:

> A clinician shows a symptom...

The frame_anchor field forces the decoder to pin the image to a real moment, not to invent a generic scene description.

#### `visual_delta` — only-change description for chained images

For images where `reference_image:` is set (state-evolution chain or same-setup-bottle-enters-frame chain), the body prose should NOT rewrite the entire scene. Instead, declare a `visual_delta:` field that names ONLY the change from the parent image, then the body prose reduces to:

> Use image_K as the exact base frame. Keep everything from image_K identical. Only change: [visual_delta value].

Example from the corpus chat:
```
### Image 5
- **frame_anchor:** 106.0s
- **reference_image:** image_4
- **product_image:** Rosabella Beetroot bottle
- **visual_delta:** Rosabella Beetroot bottle enters the frame on viewer-right side, held at chest height by the clinician's gloved hand, label facing camera. Other gloved hand gestures near the bottle on viewer-left.
- **Image prompt:**
```
> Use image_4 as the exact base frame. Keep the same silver-haired male clinician, same white button-down shirt, same blue nitrile gloves, same seated chest-up framing, same wooden desk edge at the bottom, same warm wood-paneled office, same framed diplomas on the wall, same phone-camera look, same lighting, same camera distance.
>
> Only change: [visual_delta value].
>
> [negative constraints — see rule 3 below]

This is much stronger than rewriting the whole scene. The model gets a clean signal: "preserve everything, change one thing."

### Continuity-chain detection rule (v580 / v590 extension)

When deciding whether to chain Image N to Image N-1, check these visual-continuity criteria:

1. Same person?
2. Same clothes?
3. Same room?
4. Same camera angle?
5. Same prop table / surface?
6. Only object/action changes?

If ALL match → CHAIN it. Even if the dialogue moves to a new point (explanation → product reveal, recipe step → CTA, problem statement → solution).

**The trap to avoid**: trusting dialogue-beat grouping over visual continuity. Decoders frequently treat "talking-head explanation" and "talking-head product reveal" as separate images because the script topic changes — but visually they're the same setup, so the chain saves cost (one Banana 2 generation instead of two, no drift) and improves consistency.

### Universal prompt-discipline rules (apply to BOTH decode and generate)

#### 1. Image prompt = STILL frame only; motion goes ONLY in `action_note`

The `Image prompt:` fenced block describes a STATIC photograph. No motion verbs ("she pivots", "he raises", "hand sweeping in arc"). All motion goes in the scene's `action_note` field.

Mixing motion into image prompts makes generators invent weird poses (the model tries to depict the motion mid-flight and gets confused). Banana 2 generates photographs, not action frames.

❌ FORBIDDEN in Image prompt body: "her right hand is captured mid-action GRIPPING", "PIVOTING from patient toward camera", "frozen at the apex of a wind-up motion"

✅ ALLOWED in Image prompt body: static pose ("stands beside the seated patient", "right hand presses thermometer to right temple") — the verb is "presses" not "is pressing mid-action."

The static-pose phrasing tells the model "this is what's in the photo." The motion-frame phrasing tells the model "depict an in-progress action" which produces blurry / wrong results.

#### 2. Camera lock specificity — concrete anchors, not generic style names

v603 introduced the iPhone-UGC style lock as a baseline. v604 extends it: per-video, the decoded artifact should also lock specific camera anchors that aren't covered by the generic style line.

The generic v603 line (`"Shot on iPhone wide-angle lens, handheld, deep focus throughout, vibrant natural HDR daylight"`) is necessary but not sufficient — different videos need different camera anchors:

- **Vertical or horizontal aspect?** — vertical selfie / horizontal landscape
- **Tripod or handheld?** — fixed tripod / stable handheld / shaky-vlog
- **Framing crop?** — chest-up / head-and-shoulders / full-torso / wide-room
- **Camera height?** — slightly above desk height / at eye level / low-angle / overhead
- **Subject position in frame?** — face centered upper half / lower-third / off-center
- **What's at frame bottom?** — desk edge visible / counter edge / floor
- **Background characteristics?** — warm wood diploma wall / white clinical walls / honey-oak shelving

For the healthylifesage Rosabella decode, the lock is: "vertical selfie-style phone video, stable handheld, chest-up framing, camera slightly above desk height, face centered upper half, desk edge visible at bottom, warm wood diploma wall behind." That's specific enough to prevent room-drift.

#### 3. Negative-constraint discipline

Every Image prompt body should close with explicit DO-NOT statements that prevent generator drift. The negative constraints depend on the niche/persona, but the corpus pattern is to anchor against common drift failures:

> No lab coat. No stethoscope. No hospital room. No extra products. No recipe ingredients. No dramatic cinematic lighting. No background change.

For Korella saffron-vitality videos in T0 kitchen:
> No clinical setting. No lab coat. No medical equipment. No empty kitchen (must have warm honey-oak shelving + ceramic vessels visible).

For T2 clinical exam scenes:
> No domestic kitchen background. No casual clothing on the clinician. No extra patients. No background change between this and the prior scene.

The negative-constraint section should be the LAST paragraph of the Image prompt body, after the v603 closing tag `"iPhone HDR colors, deep focus."` This gives the model a clear separation: positive description → style anchor → negative constraints.

#### 4. Viewer-left / viewer-right convention

Generators frequently confuse "left" and "right" — they may interpret as subject-perspective (the subject's own left/right) instead of frame-perspective (the viewer's left/right looking at the frame). Result: hands reversed, props on wrong side.

Fix: ALWAYS use **viewer-left** and **viewer-right** in body prose:

❌ "her left hand POINTS at the reading"
✅ "her gloved hand on the viewer-left side POINTS at the reading"

❌ "the bottle stands to the left of the glass"
✅ "the bottle stands on the viewer-left side of the glass"

The "viewer-" prefix anchors the perspective to the camera's POV. This is universal — applies to decode prompts, generate prompts, action_notes.

### Pre-output validation gate

Before emitting any decoded artifact OR generate-side videos/*.md, scan for:

- ✅ Decode-side: every Image block has `frame_anchor:` field with timestamp
- ✅ Decode-side: every chained Image has `visual_delta:` field naming only-the-change
- ✅ Continuity-chain check: same person + same clothes + same room + same camera + same surface = CHAIN, even across dialogue-beat boundaries
- ✅ Image prompt body has STATIC pose only — no motion verbs ("captured at", "frozen at", "mid-action")
- ✅ Camera lock specificity beyond the generic v603 style line — concrete anchors per-video
- ✅ Negative-constraint DO-NOT block at end of every Image prompt body
- ❌ NO bare "left" / "right" — replaced with "viewer-left" / "viewer-right"

If any ❌ found, FIX before emitting.

### Worked example — Image 5 of healthylifesage Rosabella

Pre-v604 (general scene prompt):
> The clinician sits at his desk and shows the Rosabella bottle while explaining the supplement. Background is the office. He gestures with his hands.

Post-v604 (frame-locked reconstruction with all four universal rules):

```
### Image 5
- **frame_anchor:** 106.0s
- **reference_image:** image_4
- **product_image:** Rosabella Beetroot bottle
- **visual_delta:** Rosabella Beetroot bottle enters the frame on viewer-right side, held at chest height by the clinician's gloved hand, label facing camera. Other gloved hand gestures near the bottle on viewer-left.
- **Image prompt:**
```
> Use the uploaded character reference image for the main character.
> Use the uploaded product reference image for the Rosabella Beetroot bottle.
> Use the prior-scene reference image to preserve the wood-paneled office, framed diplomas, desk, lighting, framing, and continuity from the previous scene.
>
> Use image_4 as the exact base frame. Keep the same silver-haired male clinician, same white button-down shirt, same blue nitrile gloves, same seated chest-up framing, same wooden desk edge at the bottom, same warm wood-paneled office, same framed diplomas on the wall, same phone-camera look, same lighting, same camera distance. Only change: the Rosabella Beetroot bottle is now held at chest height in his gloved hand on the viewer-right side of the frame, label facing camera, navy-and-cream wordmark squared to lens. His other gloved hand on the viewer-left side gestures near the bottle. iPhone HDR colors, deep focus.
>
> No lab coat. No stethoscope. No hospital room. No extra products. No recipe ingredients. No dramatic cinematic lighting. No background change. Bottle is real-supplement-sized, not oversized, not floating, not redesigned.

### Why v604 vs leaving these implicit

v580 + v589.1 + v590 + v603 set the generic chain-binding contract and the generic style lock, but didn't make decode-side prompts FRAME-LOCKED to specific timestamps and didn't enforce viewer-relative directions or negative constraints. Decoders kept producing "scene-idea" prompts that drifted at generation time. v604 makes frame-locking, viewer-relative direction, negative constraints, and motion-only-in-action_note all explicit gates.

The user framing: *"these decodes I should check: same person / same clothes / same room / same camera angle / same prop table / only object/action changes — when those match, it's probably a chained image even if the script moves to a new point."* v604 codifies that visual-continuity-trumps-dialogue-grouping rule.

---

## Style lock + prose discipline (v603) — corpus iPhone-UGC aesthetic, tight composition

**Source: 2026-05-06 owner observation** *"the compositing of the images and the style is completely off, what the fuck?"* The menopause-saffron prompts produced wrong composition + wrong style. Diagnosis: missing style-lock package, prose too verbose, rule citations leaked into prompt body, cinematography jargon confused Banana 2.

The corpus reference (`videos/nuri-saffron-ed-anatomy-clinic.md`) uses a specific repeated style anchor across every image. v603 codifies this as a hard rule.

### The style lock package (use verbatim, every image)

Every Image prompt body MUST include this exact style anchor as part of the composition description:

```
Shot on iPhone wide-angle lens, handheld, deep focus throughout, vibrant natural HDR daylight
```

And every Image prompt MUST close with this exact tag as the final sentence:

```
iPhone HDR colors, deep focus.
```

This pair — opener style-lock + closing tag — anchors Banana 2 to the corpus UGC aesthetic. Without it, Banana 2 defaults to studio-clean which doesn't match what's already shipping on viral accounts.

### Prose discipline — 4-7 sentences per image prompt body

The corpus reference is tight. Counts in `videos/nuri-saffron-ed-anatomy-clinic.md`:
- Image 1: 5 sentences after binding lines
- Image 2: 4 sentences
- Image 3: 4 sentences
- Image 4: 4 sentences

Target: **4-7 sentences** per Image prompt body. Each sentence carries one of:
1. Setting + style lock + framing distance ("Shot on iPhone... | camera approximately X distance")
2. Subject + props + composition ("the main character is seated at... | foreground props are...")
3. Active gesture + body language
4. Eye-contact + facial expression
5. Closing style tag ("iPhone HDR colors, deep focus.")

What gets CUT from prompt bodies:
- Rule citations: `"per v601 SYMPTOM-DEMO HOOK"`, `"per v585 motion calibration"`, `"per v600 magnitude"` — these are AUTHOR-SIDE notes, not Banana 2 instructions. Move to YAML frontmatter or commentary.
- Cinematography jargon: `"1/500-sec sharpness"`, `"motion-frozen at peak emphasis"`, `"captured at the WIND-UP APEX"` — Banana 2 generates photographs, not action-frames. These confuse it.
- Meta-commentary: `"V601 SYMPTOM-DEMO HOOK — captured at the APPLY moment of an active diagnostic..."` — Banana 2 reads this as competing instructions.
- Excess setting redescription: state the setting once, lock with style anchor, move on.

### Concrete framing distance + crop — required

Every Image prompt must specify:
- **Camera distance** in concrete terms: `"camera approximately one arm's length"`, `"camera approximately 4 feet"`, `"camera approximately 6 feet"`
- **Crop** explicitly: `"head and upper chest filling the upper two-thirds of the frame"`, `"shoulders spanning frame width"`, `"cropped at mid-thigh, NO floor visible, NO feet visible"`

These tight crops are corpus-defining. The "NO floor / NO feet" instruction is universal in nuri — it forces tight headroom.

### Active-gesture + facial-expression discipline

The corpus describes active gesture in 1-2 sentences with concrete verbs + visible state:
- `"Her right arm is RAISED MID-SWEEP at chest height, fingers extended into a flat palm, the bottles in mid-flight — three already SCATTERING off the right edge of the desk"`
- `"Her right hand grips a fresh half-lemon mid-squeeze directly above the glass, golden droplets visibly streaming down into the water, fingers tightening, knuckles whitening"`

Format:
- One sentence on the dominant hand-action verb + visible result
- One sentence on facial expression + eye-contact + brow emphasis

NOT:
- Three paragraphs describing every body angle, weight shift, hip rotation, vein visibility, etc.

### Pre-output validation

Before emitting any Image prompt, scan the body for:
- ✅ Opener style lock present? (`"Shot on iPhone wide-angle lens, handheld, deep focus throughout, vibrant natural HDR daylight"`)
- ✅ Closing tag present? (`"iPhone HDR colors, deep focus."`)
- ✅ Concrete camera distance? (`"camera approximately X"`)
- ✅ Explicit crop? (`"cropped at X, NO floor visible, NO feet visible"`)
- ✅ Body prose 4-7 sentences total (after binding lines)?
- ❌ NO rule citations in body (`"per v585"`, `"per v600"`, `"per v601 SYMPTOM-DEMO"`)?
- ❌ NO cinematography jargon (`"1/500-sec"`, `"motion-frozen"`, `"captured at peak emphasis"`, `"WIND-UP APEX"`)?
- ❌ NO meta-commentary (`"V601 SYMPTOM-DEMO HOOK — captured at the APPLY moment..."`)?

If any ❌ found, REMOVE before emitting.

### Why v603 vs leaving style implicit

Pre-v603, prompts described composition in detail but had no STYLE LOCK. Banana 2 defaulted to studio-clean photography aesthetic. Corpus videos use UGC iPhone-handheld aesthetic — entirely different look. Without v603, every operator's video drifts to a different style and the brand visual identity collapses. v603 makes the style anchor a HARD REQUIREMENT, not an authorial choice.

The user framing: *"we use rules and not a list to pick from."* v603 is a rule (style lock + prose discipline) that any niche applies; the LLM picks scene-specific composition + gesture from niche context.

---

## Persona body-prose generic-reference rule (v602) — identity comes from upload, not from prose

**Source: 2026-05-06 owner observation** *"when we create a new video, we keep the main subject generic, because it's retrieved by the image in the prompt when creating it with nano banana."* The menopause-saffron video had body prose describing the persona as "The Black-female-practitioner persona" with archetype-label + ethnic descriptor baked in. That fights with the v581 upload-binding because Banana 2 reads body prose as INSTRUCTIONS — and a redundant identity description in prose creates identity-drift between scenes (the upload says X, the prose says Y, the model splits the difference and produces Z that matches neither).

The corpus pattern (verified in `videos/nuri-saffron-ed-anatomy-clinic.md` — the canonical reference): body prose refers to the persona as **"the main character"** generically. No archetype labels, no ethnic descriptors, no age ranges, no facial feature descriptors. Identity is upload-authoritative.

### The rule

When the persona is bound to an upload via v581 (persona binding line at top of Image prompt body declaring "Use the uploaded character reference image for the main character — match her facial features, identity, hair, and skin tone exactly"), the BODY PROSE — both inside the fenced Image prompt block AND in scene action_notes — must reference the persona using the **GENERIC ALIAS declared in the v581 binding line** (default: "the main character"; or whatever alias the Ingredients table uses verbatim).

Body prose must NOT redescribe identity-defining attributes that the upload carries.

### Forbidden in body prose (these are upload-authoritative)

- **Ethnic / racial descriptors** — "Black", "Asian", "Caribbean", "Mediterranean", "Hispanic", "European"
- **Age descriptors** — "late-30s", "early-40s", "60s", "mid-50s"
- **Persona-archetype labels** — "Black-female-practitioner", "Asian-elder-herbalist", "modern-clinic-doctor", "Caribbean herbalist", "folk-wisdom elder"
- **Hair color / style identity** — "dark curly hair", "long grey dreadlocks", "salt-and-pepper beard", "olive natural curls"
- **Facial feature identity** — "almond eyes", "Fu Manchu mustache", "olive skin", "high cheekbones"
- **Body type identity** — "tall", "slim", "broad-shouldered", "petite"
- **Permanent-wardrobe identity** — items the persona wears in EVERY scene per the upload (e.g. nuri's stethoscope is part of her persona-identity if always-on)

These are all in the UPLOAD. Banana 2 will read them from the reference image, not from prose. Redescribing creates conflict.

### Allowed in body prose (these are scene-specific)

- **Pose** — "STANDING beside", "seated on exam-chair", "torso angled toward", "body weight forward"
- **Clothing IF non-default for the persona** — "in a crisp white doctor's coat over a navy blouse" (only because the persona doesn't ALWAYS wear this)
- **Facial expression** — "brows raised in clinical-finding emphasis", "mouth open mid-snarl"
- **Body language** — "body weight forward", "shoulders torqued", "hands clenched white-knuckled"
- **Active gesture** — "RIGHT hand presses thermometer to patient's temple", "LEFT hand POINTS at reading"
- **Eye-contact / gaze direction** — "eyes locked to camera", "eye-track to thermometer"
- **Hair STYLING for this scene** — "hair pulled loosely back" (styling, not identity color/texture)
- **Sweat / skin condition for this scene** — "faint sweat-sheen at temples" (scene-state, not identity)

Identity = upload-authoritative; pose / expression / gesture / scene-clothing / scene-styling = prose-driven.

### Multi-character scenes (the bystander / patient exception)

When a scene has additional characters beyond the persona:

- **Bound persona** (with upload via v581) → use "the main character" / declared alias generically
- **Unbound bystander or patient** (no upload) → DESCRIBE with prose since no upload exists, prose is the only identity source. "A late-40s female patient in a soft beige knit cardigan" is FINE if the patient has no upload binding.

For the patient, body prose carries identity. For the persona, the upload carries identity. v602 only applies to UPLOAD-BOUND characters.

### Same rule applies to products

The v581 product binding line ("Use the uploaded product reference image for the [product name verbatim] — match its label, packaging, [wordmark], color, and proportions exactly") makes the product upload authoritative for label, packaging, wordmark, color, proportions.

Body prose for the product:
- ✅ ALLOWED: position ("stands UPRIGHT on the exam-counter", "held at chest-height label-forward to camera", "wordmark squared to lens")
- ❌ FORBIDDEN: re-describing the label color, the wordmark fonts, the bottle shape, the proportions — these are upload-authoritative

### Why this rule (the Banana 2 mechanism)

Per Google's official Gemini Nano Banana 2 prompting docs (ai.google.dev/gemini-api/docs/image-generation), the recommended multi-image prompt format uses semantic descriptors like *"the dress from input 1"*, *"the model from input 2"* — NOT redescription of identity in body prose. Banana 2 expects body prose to describe SCENE COMPOSITION and PHYSICAL ACTION, not character identity. The character identity comes from the upload + the explicit binding line.

When body prose redescribes identity:
- The model receives two competing identity signals (upload + prose)
- Different prose in different scenes produces identity-drift across the image set
- Subtle features (skin tone, eye shape, hair texture) waver between scenes
- Visual consistency across the video collapses

### Pre-output validation gate

Before emitting any image prompt or scene action_note, scan the body prose for:
- Persona-archetype labels (e.g. "Black-female-practitioner persona", "the Caribbean herbalist", "the Asian elder")
- Ethnic / racial descriptors applied to the bound persona
- Age range descriptors applied to the bound persona
- Hair color / texture / facial feature redescription

If found → REPLACE with the generic alias from the v581 binding line ("the main character" or declared alias).

### Worked example — fixing the menopause-saffron video

Pre-v602 body prose: *"The Black-female-practitioner persona is in a crisp white doctor's coat over a navy blouse, stethoscope draped around her neck, hair styled professional, STANDING beside the seated patient..."*

Post-v602 body prose: *"The main character is in a crisp white doctor's coat over a navy blouse, stethoscope draped around her neck, hair styled professional, STANDING beside the seated patient..."*

The archetype label "Black-female-practitioner persona" is removed; everything else stays (clothing for THIS scene + pose + body language). Identity comes from the upload.

### Migration

Pre-v602 markdowns that reference the persona by archetype label in body prose remain functional but produce identity-drift across scenes. Migration is mechanical (find/replace persona-archetype labels with the generic alias).

---

## Healer-patient active-interaction rule (v601) — symptom videos require active clinical demonstration

**Source: 2026-05-06 owner observation** *"we need to show the healer showing the symptoms and interacting with the patient... check what the other decoded are doing when there's a patient with a symptom (example varicose veins) but more in general we need the healer doing something actively to the patient (of course depending on the video, different actions, that's why we use rules and not a list to pick from). when we have symptoms, usually there's the healer, or we have a different type of video content (transformation, which will come later)."*

The first menopause-saffron Scene 7 had the persona seated beside the patient at a desk handing her a drink. That reads as **"two friends,"** not clinical authority. The corpus EXPLAIN pattern when a symptom is involved is **the healer's HANDS DOING SOMETHING TO THE PATIENT** — and that active demonstration is what transfers clinical authority to the viewer.

### The rule (principle, not list)

When a video uses a patient as evidence of a symptom (clinical-authority video type), **the healer must ACTIVELY interact with the symptom-area on the patient's body via clinical-authority hand-actions**. The healer's hands ON or POINTING-AT the patient's symptom-area is what transfers authority. The specific action varies by niche — that's why this is a rule, not a lookup table.

### Corpus evidence (the pattern is universal)

Every corpus video with a patient + symptom uses active healer-on-patient interaction in the EXPLAIN/diagnostic scene:

| Decoded source | Symptom | Active healer-on-patient interaction |
|---|---|---|
| decoded_varicose_vein_transformation | Visible calf veins | Gloved-finger POINT + MOVE-IN closer + hand-opens toward swelling |
| dr_kim_back_lump_decoded | Back lump | Surgical-marker LOWER + PRESS + TRACE curved line + tick mark + LIFT pen |
| dr_kim_belly_burn_male_decoded | Distended belly | RIGHT-index TAP belly + LEFT-hand dismissive flick + return-to-hover |
| decoded_corella_saffron_blood_sugar_v584 | High blood sugar (distended belly) | Podiatrist's RIGHT-index POINT + TAP belly once softly + pull-back-to-hover |
| decoded_healthylifesage_DX7iVuRMzUM | Liver-cause-belly | Gloved-right-hand GESTURE TOWARD liver-area + 3-person blocking with husband-skeptic |
| oldearl_tonsil_healer_decoded | Tonsil stones | Mannequin head-up + flosser tip POSITIONED + LOWERED toward stones |
| dr_kim_hair_regrowth_male_decoded | Bald spot crown | Onion-half SLAM cut-side-down on CROWN + GRIND + juice SPRAY through hair |

Two-shot of healer-and-patient sitting side-by-side without active demonstration **does not appear** in the corpus. It's not a viable trust-transfer composition.

### Decision tree — when v601 applies

```
Is the video about a SYMPTOM (audience has a problem) or a TRANSFORMATION (audience wants a result)?

  → SYMPTOM-DEMO video (patient appears as evidence)
    → v601 APPLIES — to BOTH the HOOK and the EXPLAIN scene.
       The HOOK is the diagnostic-shock moment (healer demonstrating
       the symptom on the patient at peak magnitude). The EXPLAIN is
       the mechanism / resolution callback (can be patient again or
       persona-alone with anatomy poster).

  → RECIPE-FORWARD video (no patient — Korella saffron canonical for
    F→F-about-M with persona alone in T0 kitchen + visible recipe
    prop + dialogue promise)
    → v601 does NOT apply. The recipe demo + product reveal carry
       the authority. The HOOK uses prop-spectacle (banana SLAM,
       saffron-mug + dialogue promise) not patient-demonstration.

  → TRANSFORMATION video (Day-1 / Day-14 same-body before/after)
    → v601 does NOT apply. The body transformation IS the proof.
       No patient-other-than-self needed. (Separate v-rule pending.)

  → RECIPE-ONLY video (no patient, no symptom — pure recipe)
    → v601 does NOT apply. The recipe demo carries the authority.
```

### Video-type decision rule (which HOOK pattern to pick)

The corpus has two distinct HOOK families, and the video-type decision determines which v601 applies:

| HOOK type | When to pick | Corpus example | v601 applies? |
|---|---|---|---|
| **SYMPTOM-DEMO HOOK** | Niche has a body issue the audience can identify with on a patient (visible: belly fat, varicose, back lump, hair loss, tonsil stones; or invisible-via-instrument: menopause/hot-flash, anxiety, insomnia, hormone) | dr_kim_belly_burn_male, decoded_varicose_vein, dr_kim_back_lump, dr_kim_hair_regrowth, decoded_corella_saffron_blood_sugar_v584, decoded_healthylifesage | YES — patient in HOOK + active healer demonstration |
| **RECIPE-FORWARD HOOK** | Niche is vitality/performance/energy where the audience wants a result not a diagnosis (Korella saffron-vitality F→F-about-M canonical pattern, master-chen probiotic, master-salvora costco) | corella_saffron_v578 / v581, saffron_male_v577, saffron_vitality_v577, master_chen_three_things, master_salvora_costco_banana | NO — persona alone with prop + dialogue promise + force-verb spectacle |

**Hybrid niches** (menopause-saffron, hot-flash-vitality, anxiety-saffron) can go either way. The corpus default for Korella saffron F→F-about-M is RECIPE-FORWARD (4-corpus instances). Owner choice determines: if the operator wants clinical-authority register with husband-bystander shame-mediation, use SYMPTOM-DEMO; if recipe-warm-conspiratorial, use RECIPE-FORWARD.

### When v601 applies in the HOOK (SYMPTOM-DEMO video type)

The HOOK is the diagnostic-shock moment. Structure:

- **Scene 1** — PRESENT + APPLY phase. Healer actively demonstrates the symptom on the patient's body via clinical-authority hand-action. Reading climbs, finding lands.
- **Scene 2** — REVEAL phase. Healer LIFTS the instrument away, TURNS to camera with the finding, GESTURES toward the corrective (saffron / anatomy / mechanism), transitions to RECIPE.

The HOOK magnitude (per v600) for SYMPTOM-DEMO videos is NOT cartoon-physics SLAM — it's the **shocking READING** + the **patient's visible reaction** (eyebrow LIFTS, eyes WIDEN, breath catches) + the **healer's clinical-finding emphasis**. The magnitude is in the AUTHORITY of the diagnostic moment, not in physical violence.

For invisible-symptom niches (menopause/hot-flash, anxiety, insomnia), the v600 magnitude expresses through:
- **Display GLOWING red-warning** at unusual reading (99.4°F, 91% O2-sat, 47% sleep-quality, HRV crashing)
- **Patient's visible reaction** captured at peak surprise (eyebrow LIFTS, eyes flick to instrument, breath catches)
- **Healer's clinical-finding emphasis** (brows raised, body angled into the demonstration, gesture-finger pointing AT the reading at peak emphasis)
- **Cascading verbs** in action_note still required: GRAB → RAISE → PRESS → LOCK → GLOW → POINT-AT-READING → TURN → GESTURE-TOWARD-CORRECTIVE (8 verbs)

### Visible vs invisible symptoms — two paths

**When the symptom is EXTERNALLY VISIBLE** (belly fat / visceral, varicose veins, back lump, hair loss, tonsil stones, distended skin, scars, body-volume issues), the active interaction is **direct on the symptom**:

- POINT — gloved-finger pointing AT the visible symptom (varicose calf, bald-spot crown)
- TAP — index-finger TAP on the symptom (distended belly)
- TRACE — surgical-marker TRACING contour around the symptom (back lump)
- MARK — pen-mark documentation on the symptom (back lump tick mark)
- GESTURE-TOWARD — gloved hand opening toward the symptom area (varicose, liver area)
- PRESS — finger-press for palpation demonstration (lump, swelling)
- PALPATE — multi-finger pressure for visible response (joint inflammation)

**When the symptom is NOT EXTERNALLY VISIBLE** (menopause/hot-flash, anxiety, insomnia, brain fog, cognitive decline, internal pain, vitality, sleep, hormone), the healer must **manufacture a clinical demonstration via a wearable evidence instrument** that produces a visible measurement reading the camera can read:

- Digital infrared thermometer pressed to forehead → reading visible (showing elevated for hot-flash / metabolic dysregulation)
- 2 fingertips on patient's wrist for pulse-check → count visibly via lip-movement / wristwatch glance / "1, 2, 3" finger-count
- Fingertip pulse-oximeter clipped on patient's index finger → reading display visible (oxygen sat / heart rate)
- Smartwatch or sleep-tracker held up next to patient's wrist → screen visible with sleep-quality / heart-rate-variability / hot-flash-event log
- Dermatome / heat-strip applied to patient's neck → color-shift visible
- Blood-pressure cuff on patient's arm → reading visible
- Heart-rate monitor / pulse strap → reading visible

The instrument creates an external proxy for the internal symptom. The reading on the screen is the diagnostic moment.

### The 3-part active-interaction structure (apply in EXPLAIN scene)

Every corpus EXPLAIN with healer-on-patient follows this structure (single Veo clip, 3 timed beats):

1. **PRESENT** the instrument or hand-position — healer LIFTS the thermometer / RAISES her hand / GLOVES UP / POSITIONS the surgical pen / READIES the prop. This signals "clinical action incoming."

2. **APPLY** to patient's symptom-area — healer PRESSES / POINTS / TAPS / TRACES / MARKS / PALPATES the symptom-area on the patient's body. This is the active moment. The healer's hand makes contact with the patient or the symptom-area.

3. **REVEAL the finding** — healer LIFTS fingers / TURNS to camera with finding / POINTS at the reading / GESTURES toward the patient's symptom for the reveal moment. The diagnostic conclusion lands on the viewer.

The 3 beats map naturally to v586 action_note grammar: [Start beat 0-2s] PRESENT, [Mid-clip beat 2-4s] APPLY, [End beat 4-6s] REVEAL.

### The compositional rule

The healer remains the **clinical-authority figure** throughout; the patient is the **evidence-provider**, not the subject of explanation. The viewer aligns with the healer (who provides the answer) and identifies with the patient (whose symptom is shared).

Body positioning that supports v601:
- Patient seated on exam-couch / clinic-chair, body turned slightly toward healer, symptom-area exposed or accessible
- Healer standing or leaning beside the patient (NOT seated next to them), body angled toward the symptom-area
- Camera at chest-up two-shot (or three-shot if bystander present per F→F-about-M husband-skeptic pattern)

### Anti-pattern (what fails)

Healer and patient sitting side-by-side at a desk, both facing camera at parallel angles, patient holding the product, healer talking with hands gesturing. This reads as **two friends sharing news**, not as **doctor and patient in clinical demonstration**. Trust-transfer fails. The drink-handover composition is OK in OUTRO scenes 8-9 (the product hero-shot anchors authority via the bottle), but in the EXPLAIN scene the trust-transfer must come from active demonstration.

The varicose-vein decode shows the principle viscerally: the gloved finger POINTING AT the calf vein-cluster is what makes the viewer think *"she sees what's wrong with my legs."* A healer holding a drink next to a patient does not produce that thought.

### Worked example — fixing the menopause-saffron Scene 7

**Pre-v601** (Scene 7 first version): persona seated behind clinic desk, patient seated next to her, patient holding the finished saffron drink at chest height looking down at it, persona gesturing with hand. No active interaction. Reads as "two friends."

**Post-v601** (Scene 7 v601-corrected): persona STANDING beside seated patient on exam-chair, persona's RIGHT hand presses a digital infrared thermometer FIRMLY to the patient's forehead (full skin contact 1 inch from temple), reading display visible "99.4°F" glowing red-warning, persona's LEFT hand points at the reading, patient's eyebrow lifts in visible surprise at the reading, persona turns to camera with clinical finding, then GESTURES toward the Korella saffron bottle on the desk as the corrective.

3-part structure:
- PRESENT (0-2s): persona LIFTS the thermometer from desk, raises it toward patient's forehead
- APPLY (2-4s): persona PRESSES thermometer against patient's temple, reading climbs and locks at 99.4°F glowing red, patient's eyebrow lifts
- REVEAL (4-6s): persona LIFTS thermometer away, TURNS to camera with brows-raised clinical finding, LEFT hand GESTURES toward Korella bottle on desk for the corrective conclusion

Dialogue alignment:
- "Most women blame menopause" — PRESENT moment (instrument-raise sets the diagnostic frame)
- "The real reason is" — APPLY moment (the reading is the proof)
- "your hormones can't absorb overnight" — REVEAL moment (the finding + product gesture)

Now the healer is doing something. The patient is the evidence. The Korella bottle is the corrective.

### Why v601 vs leaving "use a patient" implicit

Pre-v601, the rules said "use a patient when the niche is clinical" but didn't specify the body-position or interaction. LLMs interpreted this as "put a patient in the frame" — and produced the side-by-side seated composition that reads as "two friends." v601 makes ACTIVE INTERACTION explicit + provides the 3-part structure (PRESENT / APPLY / REVEAL) so the LLM has a concrete pattern to instantiate.

The user's framing: *"we use rules and not a list to pick from."* v601 is a rule (active healer-on-patient interaction with 3-part structure) that the LLM applies to ANY niche, deriving the specific action (POINT / TAP / TRACE / MARK / thermometer / pulse-check / pulse-ox / etc.) from the niche context. Not a closed lookup table.

---

## Exaggeration-magnitude discipline (v600) — cartoon-physics or boring

**Source: 2026-05-06 owner observation** *"the pillow would needed to be held higher and sweat running down while is about to smack it or throw it on the desk."* The first soaked-pillow-on-marble hook (v598 Q1-Q8 compliant) was rejected as STILL too weak. Diagnosis: the spectacle was at **realistic** magnitude, not **viral** magnitude. A soaked pillow placed gently on a counter is what a real tired wife does. A drenched pillow held HIGH OVERHEAD with sweat streaming DOWN her forearms while she winds up to SMACK it onto the desk — that's what the corpus does. Real-life = scroll-by. Cartoon-physics = scroll-stop.

The 24-decoded-corpus is built on **magnitude that exceeds reality**:

| Corpus video | What a real person does | What the viral video does |
|---|---|---|
| master_salvora_costco_banana | Pulls a banana off the bunch | RIPS banana → SLAMS bunch → FULL PYRAMID COLLAPSE → neighbors TUMBLE |
| dr_kim_hair_regrowth_male | Cuts an onion in the kitchen | SLAMS onion cut-side DOWN onto CROWN → cut-face FLATTENS → juice SPRAYS 3-4 droplets → GRIND clockwise → juice runs 2-3 streams through hair |
| dr_kim_cockroach_bait | Pours sugar in a glass | LEFT pinched-fingers → VIOLENT VERTICAL SUGAR JET → BLAST into glass → ATOMIZE mist → RICOCHET particles → cockroach ENGULFED |
| master_salvora_costco_salmon | Picks up a salmon | SLAMS salmon on checkout → ICE SPLASHES → CLEAVER swing → scrape into pot |
| master_salvora_costco_cabbage | Selects a cabbage | THRUSTS onto counter-display with FORCE → SLAM down → leaves SCATTER everywhere |
| dr_kim_belly_burn_male | Discusses a patient | DAY1/DAY14 outfit-change CUT — hanging belly to flat abs in editorial weird-action |
| Belly-fat tea pour | Pours tea in a cup | Tea POURED on anatomy torso → yellow fat visibly LIQUEFIES → organs cleanse |

In every case the corpus pattern is: **multiple cascading force-verbs + specific exaggerated quantities + visible effects that exceed physics**.

### The 3-part magnitude test (Q9 — v600 enforcement)

For every HOOK image and HOOK action_note, all 3 must answer YES. (This applies to spectacle-driven scenes too — RECIPE product-cascade, EXPLAIN demonstration. Anywhere the corpus uses force-verbs, magnitude must be cartoon-grade.)

**Q9a. PROP POSITION / SIZE / QUANTITY exaggerated past realism?**

- Real: pillow at counter level. Viral: pillow HELD HIGH OVERHEAD with both arms fully extended.
- Real: one drop of juice. Viral: 3-4 SPRAYING droplets specified.
- Real: a banana off the bunch. Viral: the WHOLE PYRAMID COLLAPSING.
- Real: a single saffron thread. Viral: a CASCADE of saffron threads pouring like a waterfall.
- Real: a salmon on the cutting board. Viral: a salmon SLAMMED on stainless steel with ICE SPLASH ARC.

The corpus rule: **scale up the prop's position/size/quantity by 2-3× past what a real person would do.** Held higher. More streams. More droplets. More leaves scattering. More ice spraying. Specify the quantity and the position explicitly in the prompt body and action_note.

**Q9b. VISIBLE EFFECT PRE-IMPACT?**

The wind-up frame must show MAGNITUDE BEFORE THE IMPACT. The viewer sees the cascading sweat / dripping juice / scattering particles / spraying liquid ALREADY in motion before the climactic moment. This gives 1-2 frames of "oh shit, look at the scale of this" — the scroll-stop micro-decision.

- Real: pillow lifts, then lands on counter. Viral: pillow OVERHEAD with sweat-water STREAMS already POURING DOWN her bare forearms in 3 visible rivulets, hair dripping, camisole already wet — wind-up captures the magnitude.
- Real: hand grinds onion on scalp. Viral: onion held aloft cut-side-down with juice ALREADY DRIPPING in pre-cascade visible streams before contact.
- Real: cabbage placed on counter. Viral: cabbage held high in two hands with leaves ALREADY peeling outward in pre-fall motion before the SLAM.
- Real: saffron sprinkled. Viral: saffron CASCADE held aloft with threads ALREADY in mid-fall streaming downward as a visible curtain before they hit the water.

The wind-up moment is THE moment. Capture it.

**Q9c. CASCADING FORCE-VERBS — 3+ verbs in sequence in the action_note?**

The corpus action_notes describe SEQUENCES of force-verbs, not single verbs:
- RIP → SLAM → COLLAPSE → TUMBLE (4 verbs)
- SLAM → FLATTEN → SPRAY → GRIND → STREAM (5 verbs)
- JET → BLAST → ATOMIZE → RICOCHET → ENGULF (5 verbs)
- SLAM → SPLASH → SWING → SCRAPE (4 verbs)
- SLAM → SCATTER → SETTLE (3 verbs)

A single-verb action_note ("she compresses the saffron threads") = 1 verb = realistic = boring. The Q9c rule: **every spectacle action_note must chain 3+ force-verbs in temporal sequence, with each verb's visible effect specified.**

Verb library by spectacle type:

| Spectacle type | Cascading verb chain template |
|---|---|
| Force-on-prop (banana, cabbage, pillow) | LIFT → SLAM → SCATTER → COLLAPSE → SETTLE |
| Liquid agent-of-change (tea, saffron, juice) | LIFT → POUR → SPRAY → CASCADE → BLEED → DISSOLVE |
| Pressure spectacle (water flosser, sugar jet) | TRIGGER → BLAST → ATOMIZE → SCATTER → ENGULF |
| Body-anatomy demo (belly tap, vein point) | POINT → TRACE → CARVE → MARK → REVEAL |
| Surgical / clinical (marker on lump) | LOWER → PRESS → TRACE → LIFT → ANGLE |
| Wind-up + impact (pillow throw, banana pyramid) | RAISE → WIND-UP → SMACK / THROW → SPLATTER → SPRAY → DRIP |

If your action_note has 1-2 verbs, you're at realistic magnitude. Pull from the verb library and chain 3+ in sequence.

### Worked example — the menopause-saffron HOOK rebuilt at v600 magnitude

**Pre-v600 version** (passed v598 Q1-Q8 but failed v600 Q9 — too realistic):
- Pillowcase placed crumpled half-on-half-off the marble edge
- Persona's RIGHT hand caught mid-release of pillowcase grip
- Single force-verb: SLAM (post-impact frame captured)
- Single visible effect: water droplets in mid-air arc-spraying

**Post-v600 version** (Q9a + Q9b + Q9c all passing):

HOOK image — **wind-up frame at peak magnitude**:
- Persona standing with the soaked white silk pillowcase HELD HIGH OVERHEAD in both hands fully extended above her head, both arms fully visible at full extension
- Sweat-water visibly POURING DOWN her bare forearms in 3-4 distinct rivulets, dripping off her elbows, soaking into her white silk camisole at the shoulders, plastering wet strands of hair to her temples and neck
- Her face is in fury-mode — eyes wide, mouth open mid-snarl, jaw set, eyebrows angled down in determined rage
- Body weight shifted FORWARD onto the front foot, shoulders fully torqued, frozen at the apex of a wind-up SMACK motion (1 frame before the throw)
- The pillowcase itself is comically drenched — not just damp but DRIPPING-WET, visible water sheeting off the silk in vertical cascade-streams from each corner toward the marble counter below
- Foreground on marble (target zone): cream-ceramic bowl of dried red saffron threads positioned directly beneath where the pillowcase is about to SMACK
- Background: husband-silhouette in soft-focus through bedroom doorway, peacefully blanket-covered, dim blue night-light, oblivious — the contrast of HER fury vs HIS peaceful sleep is the taboo direct-address
- Two-temperature lighting: warm amber kitchen + cool blue bedroom doorway = "she is awake in fury, he is asleep in peace"

HOOK action_note — **5-verb cascading chain**:
> Static handheld camera holds at apex of wind-up (mag ~1.5px subtle handheld breathe). The Black-female-practitioner persona in a wet-shouldered white silk camisole + the soaked pillowcase HELD HIGH OVERHEAD in both clenched hands + sweat-water STREAMING DOWN her bare forearms in 3-4 visible rivulets + the cream-ceramic bowl of dried red saffron threads on the marble target-zone below + the husband-silhouette soft-focus through bedroom doorway in dim blue light. [Start beat 0-1s] HARD-CUT to wind-up apex — pillowcase OVERHEAD with sweat-streams POURING DOWN her forearms, eyes WIDE in fury locked to camera, mouth open mid-snarl on "Your husband sleeps through this." [Mid-clip beat 1-3s] On "Saffron is what" she RIPS the pillowcase down in a full-arc DOWNWARD SMACK trajectory, water-spray ARC EXPLODES across the marble, the saffron bowl SHUDDERS, threads SCATTER violently into the spatter, tendrils UNFOLD in gold-amber bloom-arcs through mid-air water droplets. [End beat 3-5s] On "wakes you up" she RIPS a saffron pinch from the wreckage of the spatter, raises it aloft DRIPPING with sweat-and-saffron-water, gold-amber dust trailing in cascade from her fingers as she holds it to camera, eyes still wide in fury, husband still motionless in doorway behind. [Conspiratorial-fury F-to-F-about-M direct-address with husband-as-bystander register]. Ambient: 2 a.m. kitchen quiet, faint distant ceiling-fan hum, sweat-water drumming sound from the pre-impact pillowcase drip, saffron threads compressing with a wet crackle.

Verb chain: RIP → SMACK → EXPLODE → SCATTER → UNFOLD → RIP → RAISE (7 verbs). Magnitude: pillowcase HIGH OVERHEAD + 3-4 sweat-streams already pouring + drenched-not-damp + wind-up apex captured. Cascading effects: water-spray + saffron-scatter + tendril-bloom + dust-cascade.

This is what passes v600.

### When v600 applies

v600 applies to any image/scene where the corpus rule calls for force-verb spectacle:
- **Always** the HOOK (Scenes 1-2)
- **Often** the RECIPE product-reveal scene (saffron CASCADE, capsule POUR, banana SLAM)
- **Sometimes** the EXPLAIN scene if it includes a demonstration (anatomy fat-melt, marker-trace)

v600 does NOT apply to:
- Talking-head closing scenes (CTA / OUTRO) — those need authority-pose, not spectacle
- The Day-1 frame of a Day-1/Day-14 transformation (the "before" is meant to look real-life)

### Why v600 vs leaving it implicit in v598 Q8

v598 Q8 (ii) said "violent-act / spectacle" but that was too soft — LLMs interpreted "violent" as "press-down" or "compress." v600 makes magnitude an explicit gate with 3 sub-tests (position/size/quantity, pre-impact effect, cascading verb chain) so the LLM can't smuggle a realistic gesture into a "violent" label.

---

## Product-image presence discipline + LLM-omission audit (v599)

**Source: 2026-05-06 owner observation** *"chatgpt earlier didn't include the product in the images that needed the product. check the rules, for you and for other LLM. don't miss anything."* v581 documented the explicit-binding contract (product_image field + binding line + product visual in prompt body), but LLMs (ChatGPT, Gemini, Claude API) consistently OMIT one or more of the three required parts when authoring videos/*.md from a bundle. v599 elevates v581's three-part requirement from "implicit per-scene judgment" to "explicit per-scene matrix + pre-output gate."

### v581 product binding has THREE required parts (LLMs skip one or more)

For every image where the product is named in voiceover, visible in frame, or referenced for the platform to bind the product upload, ALL THREE of the following MUST be present (LLMs typically include one, sometimes two, almost never all three):

1. **`product_image:` field** in the image block metadata (above the `Image prompt:` fenced block), with value = product ingredient name verbatim from the Ingredients table:
   ```
   - **product_image:** the Corella saffron bottle
   ```

2. **Product binding line** as the SECOND line at the top of the fenced `Image prompt:` body (after the persona binding line, before the chain binding if present):
   ```
   Use the uploaded product reference image for the Corella saffron bottle — match its label, packaging, navy-and-cream wordmark, color, and proportions exactly.
   ```

3. **Product visual description in the prompt body** — the product MUST be described in the scene composition (where it sits, what the persona does with it, "label-forward to camera, navy-and-cream wordmark clearly readable"):
   > the Corella saffron bottle stands upright on the counter to the left of the glass, label-forward to camera, navy-and-cream wordmark clearly readable

If ANY of the three parts is omitted, Banana 2 either:
- Doesn't bind the product upload (no consistent label/packaging across scenes — defeats the entire upload mechanism)
- Generates a generic bottle that doesn't match the brand's packaging (label drift)
- Generates the scene without the product visible at all (reads as "talking-head over recipe" — no product authority moment)

### Per-scene product-presence matrix (corpus-grounded)

For a typical 9-scene Korella saffron-style video, the product-presence schedule:

| Scene type | Product visible? | product_image field set? | Required | Source video evidence |
|---|---|---|---|---|
| HOOK Scene 1-2 (0-8s) | ❌ NO — use shame-proxy + visual-pun (banana, soaked pillowcase, mannequin tonsil, distended belly). Product appears LATER. | NO | Don't reveal the product yet — burn the curiosity loop first. The HOOK uses the proxy to draw the viewer in. | All 4 corella saffron videos delay the saffron bottle reveal until the recipe-pour or post-recipe EXPLAIN. salvora costco banana never shows product in HOOK. |
| RECIPE Scene 3-5 (build-up: lemon-pour, ginger-pinch, honey-cascade) | ❌ NO ingredient form of the product yet | NO | Building the drink — the product hasn't entered the recipe yet | nuri-saffron-ed-anatomy-clinic Images 3-6 (no product_image field) |
| RECIPE Scene 6 (PRODUCT REVEAL — product cascades into the recipe) | ✅ YES — product capsule/bottle/threads ENTERS the frame as the agent-of-change | YES | This is the climactic agent-of-change moment per v598 Q8. Product CASCADES, POURS, UNFOLDS, etc. + the bottle stands on counter label-forward | nuri-saffron-ed-anatomy-clinic Image 7 (saffron capsule cascade, Korella bottle on counter label-forward) |
| EXPLAIN Scene 7 (8-22s) | ✅ YES — product bottle visible, persona may hold or stand beside | YES | Authority-transfer moment. The brand bottle anchors the credentials | nuri-saffron-ed-anatomy-clinic Image 8 (Korella bottle on desk between persona and patient) |
| OUTRO Scene 8 (Sign-off) | ✅ YES — product bottle held at chest height label-forward, hero-shot | YES | The CTA-prep frame. Persona holds bottle squared to lens | nuri-saffron-ed-anatomy-clinic Image 9 (bottle in right hand, navy-and-cream wordmark squared to lens) |
| OUTRO Scene 9 (Finger-up follow + CTA gesture) | ✅ YES — product bottle in one hand + CTA finger gesture from the other | YES | Final hero-shot + CTA pose | nuri-saffron-ed-anatomy-clinic Image 10 (bottle in left hand, right index pointing to lens) |

**Per-niche variations**:
- **DAY1/14 transformation videos** (back lump, hair regrowth, varicose veins): product reveal can move EARLIER to after Day-14 reveal, or later in the EXPLAIN. Bottle still appears in OUTRO scenes 8-9.
- **Educational / no-branded-product videos** (asian-elder-papaya-skin, master-chen-three-things): drop product_image field entirely, drop the product row from Ingredients table, omit product mentions from prompts. v573 covers this case.
- **Multi-step recipes** (5+ ingredients): the product is typically the LAST ingredient added, so product reveal at scene 6 of 9 (or later) is canonical.

### Ingredients table — required dependency

For any video with `product_image:` set on at least one image, the file MUST contain a `## Ingredients` table at the top (between `## Sources` and `## Storyboard`, per v581 + v573 spec). The table has TWO rows: persona + product.

```
## Ingredients

| # | Type | Name | Reference |
|---|---|---|---|
| 1 | character | the main character | personas/refs/<persona>.png |
| 2 | product | the Corella saffron bottle | products/refs/corella-saffron-bottle.png |
```

The product row's "Name" column value MUST match VERBATIM the value used in `product_image:` fields throughout the file. If the table says "the Corella saffron bottle" and an image's `product_image:` says "Corella saffron" (missing "the" + "bottle"), the platform's binding resolution will fail silently.

LLMs commonly omit the Ingredients table entirely OR mismatch the product name between the table and the image fields. v599 makes this an explicit gate.

### LLM-omission audit (the things ChatGPT/Gemini/Claude consistently skip)

This audit catches everything LLMs commonly omit when authoring videos/*.md from a bundle. v599 enforces all of these as explicit gates in the bundle TASK self-validation block.

| # | Omission | What's missing | Gate enforces |
|---|---|---|---|
| 1 | Product binding (3-part) | `product_image:` field OR binding line OR product visual description | All 3 must be present on every image where product is visible/referenced. v581 contract. |
| 2 | Ingredients table | Entire `## Ingredients` section missing OR product row missing OR name mismatch with `product_image:` field values | Table required if any image uses `product_image:`. Names must match VERBATIM. |
| 3 | Persona pose-to-camera lock | Action_note doesn't specify "eyes locked to lens" / "eyes locked to camera" | Required in every scene's action_note (corpus rule — every viral video has direct-eye-contact lock). |
| 4 | Source / Used-in section | Missing `## Sources` and `## Used in` sections at end of file | Required per CLAUDE.md publishing convention. |
| 5 | YAML frontmatter | Missing `persona:` / `niche:` / `audience:` / `cell:` keys at top of file | Required for indexing. Cell value should reference strategy-mechanisms.md row. |
| 6 | v577 word budget per scene | A `- **line:**` value exceeds 21 words ±2 (≥24 words = must split into multi-line scene) | Word-count check per line. |
| 7 | Universal closer | Final scene's `- **line:**` doesn't end with "follow me first" or close variant | Required final-line check. |
| 8 | DAY1/14 anchor (transformation niches) | Niche is DAY1/14 (back lump, hair regrowth, varicose, sagging skin) but no "$X surgery you didn't pay" anchor in EXPLAIN scene | Required dollar-anchor in EXPLAIN for transformation niches. |

### Bundle TASK gate (v599 enforcement — pre-output validation)

Before emitting the videos/*.md, the LLM MUST walk this 8-item checklist and FIX any item that fails:

```
v599 PRE-OUTPUT PRODUCT-PRESENCE + LLM-OMISSION AUDIT

[A] Ingredients table present (## Ingredients between ## Sources and
    ## Storyboard)? Two rows when product is bound (persona + product).
    Product row's "Name" column value matches VERBATIM the
    product_image: field values used throughout the file?
[B] On every image where the product is visible OR named in voiceover,
    ALL THREE of the v581 binding parts present:
      - product_image: <ingredient-name> field set
      - "Use the uploaded product reference image for <name> — match
        its label, packaging, [color/wordmark], and proportions exactly."
        binding line at top of fenced prompt body (line 2, after
        persona binding line)
      - Product visual described in prompt body ("label-forward to
        camera", "wordmark squared to lens", "stands upright on counter")
[C] Per-scene product-presence matrix respected:
      - HOOK Scenes 1-2: product NOT visible (use shame-proxy)
      - RECIPE early scenes: product NOT visible until product-cascade
        moment
      - RECIPE product-reveal scene: product CASCADES + bottle on
        counter label-forward
      - EXPLAIN: product visible, label readable
      - OUTRO + CTA: product hero-shot + CTA gesture
[D] Every scene's action_note specifies "eyes locked to lens" or
    "eyes locked to camera" (persona-pose-to-camera lock)
[E] Every `- **line:**` value ≤21 words ±2 (split if 24+ words)
[F] Final scene's `- **line:**` ends with "follow me first or I can't
    reach you" or close variant (universal closer)
[G] If niche is DAY1/14 transformation (back lump, hair regrowth,
    varicose, sagging skin): "$X surgery you didn't pay" anchor in
    EXPLAIN scene
[H] File ends with `## Sources` (with citations) + `## Used in`
    (placeholder), and YAML frontmatter at top (persona/niche/
    audience/cell)
```

If ANY of [A]-[H] fail, FIX before emitting. Self-correction here saves a round-trip.

### Why v599 vs leaving v581 implicit

v581 documented the contract (product_image field + binding line + visual description). v599 codifies the per-scene SCHEDULE (which scenes need product, which don't) + the ENFORCEMENT (pre-output gate that catches LLM omissions). The contract was already correct — the LLMs just weren't applying it consistently because the rule was buried in 100+ lines of v581 deep-dive and the per-scene matrix was implicit.

LLMs are pattern-matchers. v599 gives them an explicit pattern to match.

---

## Export-pipeline frame-grid discipline (v597) — eliminates "tweaking frames"

### Corpus-grounded surrogate library (replaces speculative entries — 24-decode evidence)

| Niche | HOOK foreground (corpus-grounded) | HOOK background (corpus-grounded) | Source video |
|---|---|---|---|
| ED / male performance | Single banana ripped from pyramid bunch + bunch SLAM-collapse on display lip | T0-retail Costco produce aisle (yellow Costco signage + fluorescent industrial ceiling + blurred shoppers + plus-sized white-male bystander reaction) | master_salvora_costco_banana |
| ED / saffron-vitality (F→F-about-M) | Fresh ripe banana on marble + amber saffron mug at counter | T0 clean kitchen (warm domestic light + window-soft + marble counter + persona kitchen-anchor pose) DUAL-FLIP to T2 office (diploma + credentials) for OUTRO | corella_saffron_v578 / v581 / saffron_male_v577 / saffron_vitality_v577 (4-corpus F→F-about-M) |
| Belly fat / visceral | Patient's exposed distended belly + clinician's RIGHT-index TAP + LEFT-hand gesture | T2 clinical exam room (bright LED + equipment cart + exam stool + diploma + US flag + anatomy poster) | dr_kim_belly_burn_male / oldearl_visceralfat_clinic / decoded_corella_saffron_blood_sugar_v584 |
| Belly fat / Costco-cabbage variant | Whole purple-red cabbage SLAM onto counter-display + leaves SCATTER | T0-retail Costco produce (signage + fluorescent + cardboard slope-display + shoppers) | master_salvora_costco_cabbage |
| Belly fat / tea-warm-elder variant | Amber tea POUR from glass mug | T0-T1 Caribbean sunroom (bamboo wall + herb jars + Rasta+US flags + honey-oak table + amber light) | rastajahmeil_fat_melt |
| Belly fat / liver-reframe | Patient's exposed belly + gloved-right-hand gesture toward liver area + husband-skeptic bystander | T2 clinical exam room (3-person blocking: clinician + patient + bystander husband) → DUAL-FLIP to T2 office for cinnamon-pour recipe | decoded_healthylifesage_DX7iVuRMzUM |
| Hair loss (male) | Cut onion half SLAM on crown + GRIND clockwise + juice SPRAY 3-4 droplets | T2 clinical exam room (bright LED + diploma + clinical pen visible) → DUAL-FLIP to NYC luxury apartment (corner window + skyline + beige sectional + fiddle-leaf fig) for OUTRO | dr_kim_hair_regrowth_male |
| Tonsil stones | Chrome water-flosser tip + mannequin head mouth-up + flosser LOWERS toward stones | T1 honey-oak farm workbench + barn-board wall + reading-lamp glow + ceramic teapot + dried herb jars | oldearl_tonsil_healer |
| Back lump / sagging skin | Female-patient exposed back + black surgical-marker PRESS + TRACE curved line + tick mark | T2 clinical exam room (bright LED + diploma + equipment cart) → DUAL-FLIP to NYC luxury apartment for OUTRO | dr_kim_back_lump / decoded_back_bump_transformation |
| Varicose veins | Patient's exposed calf with visible vein cluster + gloved-finger POINT + MOVE-IN closer + hand-opens toward swelling | T2 clinical exam room (clinical authority + visible patient-table + cool-white LED) | decoded_varicose_vein_transformation |
| Joint inflammation / brain fog | Whole salmon SLAM on checkout + ice SPLASH + cleaver swing | T0-retail Costco seafood counter (Fresh Catch signage + ice display + fluorescent + shoppers) | master_salvora_costco_salmon |
| Probiotic / gut | Cup steady at chest + downward TILT to read label + ROTATE back + dismissive SHAKE + knot eyebrows | T0 honey-oak kitchen + bright domestic + ceramic teapot visible | master_chen_three_things |
| Pest control / cockroach | Dead cockroach in glass jar + LEFT pinched-fingers + VIOLENT vertical sugar JET + ATOMIZE mist | T0 clean kitchen + window-light + clean countertop (operator persona) | dr_kim_cockroach_bait |
| Anti-aging / skin | Papaya face-mask + seed-jar lift to camera + mask-of-product reveal | T0 Spanish patio (stucco wall + warm window light + botanical) | decoded_meta_AQPaHJENd45_papaya_skin |
| Bladder / nocturia (corpus-evidence) | Tea mug stir + clinician at desk demo | T2 hospital exam room (clinical + IV pole + diploma + US flag) | decoded_bladder_tea_transformation / decoded_belly_burn_tea |
| Big-Soap / chemical conspiracy | Product bottle brand-bashing listicle reveal | T0-retail Walmart parking lot HOOK → Walmart aisle ×4 brands → T1 personal-car-with-windshield-Walmart for TCM CTA | decoded_herbal.health.tips_DX5QQZOhRd1 |
| Ozempic-alternative folk-remedy | Farm prop + folksy demo (warm-elder authority) | T0 farm kitchen (honey-oak + barn-board + ceramic + window with outdoors) | decoded_icelandicwisdom_DX5OIgip1Rq_belly_fat |

**The principle**: never invent a new niche/setting pairing — pull from the corpus first. If the niche is HOOK-HARD (no corpus precedent — hot flash, brain fog, anxiety), pull a setting/persona pairing from a CORPUS-VALIDATED ADJACENT NICHE (e.g. menopause-saffron should reuse the Korella saffron-vitality T0-kitchen-DUAL-FLIP-to-T2-office pattern that has 4 corpus instances, not invent a dim-bedroom).

### Worked example — fixing "When The Heat Hits At Night"

The Ogheci hot-flash output failed because:
- Q1: NO foreground prop (just a person in bed)
- Q2: NO visible motion (sleeping/turning is not a force-verb)
- Q3: NO visual pun (hot flash has no inherent visual)
- Q6: BACKGROUND was a dim bedroom — gives ZERO authority (Ogheci has no persona-archetype anchor; no diploma, no herb jars, no Costco signage, no clinic equipment)
- Q7: Resonates with NO audience (women 40+ with hot flashes don't want to see another woman in another dim bedroom — they live that already; it's not a peer space because it's identity-confirming-suffering, and it's not an authority space)

The corpus-grounded fix: pivot to the Korella saffron-vitality T0-kitchen-DUAL-FLIP pattern (4 corpus instances). HOOK foreground = saffron threads in warm-water bowl + ripe banana on marble. HOOK background = clean domestic kitchen (warm light + window-soft + marble counter + Black-female-practitioner kitchen-anchor pose). OUTRO background = T2 office (diploma + US flag + clean desk). The hot-flash story-beat moves to the dialogue ("when the heat wakes you at 2 a.m., this is what your body is missing..."), but the visual frame is the validated saffron-prop kitchen → office DUAL-FLIP.

---

## Export-pipeline frame-grid discipline (v597) — eliminates "tweaking frames"

**Source: 2026-05-05 user-reported visible artifact** in WhisperVAD-mode exports — "frames that are tweaking" at scattered points in the final video. Three compounding bugs in the export pipeline that all violate the same invariant: **every encoding stage must agree on the frame grid (constant fps, frame-aligned boundaries) or boundary frames get dup/dropped asymmetrically.**

### Bug A — speed-apply path produces VFR output (most impactful)

Location: `code/main.py` `~line 5253` — the `[Export] Speed applied: 1.1×` step.

The ffmpeg command was:

```
ffmpeg -i <output> -filter_complex "[0:v]setpts=(1/speed)*PTS[v];[0:a]atempo=speed[a]"
       -map [v] -map [a] -c:v libx264 -preset ultrafast -crf 18 ...
       <out>
```

**Missing `-r 24 -vsync cfr`.** Same exact bug v560 fixed in `master_align()`:
- `setpts=PTS/N` adjusts presentation timestamps but ffmpeg keeps the original frame count
- Output is VFR — container says X seconds, internal packet timestamps span the original (longer) duration
- Visible at playback as **micro-stutter / "tweaking frames"** because the player's frame-pacing doesn't match the encoded packet timing
- Triggers any time the operator exports with `playback_speed > 1.0`

The v560 fix was applied to `master_align` but missed in the export-speed path. v597 reapplies it.

```python
# Fix:
cmd_speed = [
    "ffmpeg", "-y", "-i", str(output_path),
    "-filter_complex", f"[0:v]setpts={1/speed:.6f}*PTS[v];[0:a]atempo={speed:.3f}[a]",
    "-map", "[v]", "-map", "[a]",
    "-r", "24", "-vsync", "cfr",   # v597 — same fix as v560 in master_align
    ...
]
```

### Bug B — VAD segment extraction missing fps lock

Location: `code/video_processor.py` `apply_vad()` segment-extraction loop.

Each segment was extracted via libx264 without `-r {fps} -vsync cfr`. Source clips are 24fps CFR after the trim step, but absent explicit locking, the encoder can produce VFR if boundaries fall mid-frame. Then the downstream concat step (which DOES use `-vsync cfr`) has to dup/drop boundary frames asymmetrically.

Fix: pass `-r {src_fps} -vsync cfr` on every segment extraction (defense in depth).

### Bug C — VAD segment boundaries are sub-frame

Location: `code/video_processor.py` `apply_vad()` after `merged = []`.

WhisperVAD timestamps come from word-end times (e.g. `segment 1: 0.000s → 4.470s`, `segment 3: 14.800s → 15.255s`). These are **sub-frame at 24fps** (where each frame = 0.04167s). At 4.470s = 107.28 frames; at 15.255s = 366.12 frames — neither aligned.

Without snapping, segment extraction has to decide what to do with a partial boundary frame:
- libx264 + CFR may dup OR drop the partial frame
- The decision varies subtly per segment (depends on rounding inside ffmpeg)
- Concatenating ~16 segments × 2 boundary decisions = ~32 chances for a frame to dup/drop unpredictably
- User sees "tweaking frames" at scattered points in the final video — the dup/drop artifacts

Fix: snap each segment boundary to the nearest frame multiple BEFORE extraction. Round `start` DOWN, round `end` UP. This widens each segment by at most one frame each side (~40ms total at 24fps) — well under the `silence_keep_duration` padding, so no dialogue is lost. Result: every segment has a whole-frame count and concat is glitch-free.

```python
# Fix:
import math
frame_dur = 1.0 / src_fps
snapped = []
for start, end in merged:
    snap_start = math.floor(start / frame_dur) * frame_dur
    snap_end = math.ceil(end / frame_dur) * frame_dur
    snapped.append((max(0.0, snap_start), snap_end))
merged = snapped
```

### Why all three bugs needed fixing

The fixes compound:
- Bug A alone: every export with speed>1.0 has VFR output regardless of segment grid
- Bug B alone: per-segment VFR drift even at 1.0× speed
- Bug C alone: sub-frame boundaries cause asymmetric dup/drop even with CFR locking

Fixing only one leaves residual artifacts. v597 fixes all three so every encoding stage agrees on the frame grid throughout the pipeline.

### What stays unchanged

- `concat_videos()` already had `-r 24 -vsync cfr` (added by v560)
- `master_align()` already had `-r 24 -vsync cfr` (v560)
- `trim_video()` source clips are CFR 24fps from Veo so no fix needed there
- Whisper transcription, motion classification, dense-frame extraction — all unchanged

### Migration

Existing exports made before v597 may have visible frame artifacts. Re-export with the fixed code to eliminate them. No data migration needed; the fix is purely in the encoding pipeline.

### Verification (post-v597)

After v597 ships, exports should have:
- Smooth playback at any `playback_speed` value (no micro-stutter)
- Clean cuts at WhisperVAD segment boundaries (no boundary-frame artifacts)
- Whole-second durations matching the sum of segment durations (no VFR-induced drift)

If artifacts persist after v597, the root cause is upstream of these three fixes (e.g. Veo source-clip fps inconsistency, or DeepFilter audio-resample skew if enabled).

---

## VAD matcher bounded lookahead (v596)

**Source: 2026-05-05 belly-fat-tonic export failure analysis.** The `[WhisperVAD]` script-to-audio in-order matcher was advancing its `wi` pointer arbitrarily far when cross-clip audio bleed caused a late whisper word to fuzzy-match a script word. Earlier valid script words got stranded behind the advanced pointer.

### Concrete failure observed

Belly-fat-tonic clip 2 (script: *"every man over forty hits this wall. metabolism quits. waistline doesn't."*):

```
Whisper bucket (raw heard, with bleed):
[because, it, tells, every, man, wall., metabolism, quits,, w, you, start, doing, this,, you]
                       ^script[0,1]   ^^^^script[6,7,8]^^^^               ^bleed-from-clip-3

Pre-v596 matcher behavior:
- match "every" @ j=3, wi=4
- match "man" @ j=4, wi=5
- skip "over" "forty" "hits" (Whisper drops, OK)
- search "this" from wi=5 to END-of-bucket → finds "this," @ j=12 (clip-3 bleed!)
- accept, wi=13
- search "wall" from wi=13 → NOT FOUND (real "wall" was at j=5, stranded behind wi)
- search "metabolism" → stranded at j=6
- search "quits" → stranded at j=7

Result: 3/11 matched (every, man, this — where "this" is bleed audio, NOT clip 2 script's "this")
```

### The fix

Add a `lookahead_window` parameter (default **6**) to `_match_in_order`. Each script word can only search whisper[wi : wi+6] — NOT whisper[wi : end]. If the script word isn't found within the window, fall through (Whisper drop OR out-of-window bleed) without advancing `wi`.

```python
def _match_in_order(whisper_bucket, script_words,
                    fuzzy_threshold=0.80,
                    short_word_threshold=0.95,
                    lookahead_window=6):    # ← v596 NEW
    ...
    for s_word in script_words:
        ...
        # v596: bounded search
        search_end = min(len(whisper_bucket), wi + lookahead_window)
        for j in range(wi, search_end):
            sim = SequenceMatcher(None, w_clean[j], s_clean).ratio()
            if sim >= threshold:
                kept_indices.append(j)
                wi = j + 1
                break
```

### Post-v596 behavior on the same input

```
- match "every" @ j=3, wi=4
- match "man" @ j=4, wi=5
- skip "over" "forty" "hits" (within wi=[5,11] — none found)
- search "this" within wi=[5,11] → "this," is at j=12, OUT OF WINDOW → skip
- search "wall" within wi=[5,11] → FOUND at j=5! Accept. wi=6.
- search "metabolism" within wi=[6,12] → FOUND at j=6. wi=7.
- search "quits" within wi=[7,13] → FOUND at j=7. wi=8.
- skip "waistline" "doesn't" (Whisper drops, none found in window)

Result: 5/11 matched (every, man, wall, metabolism, quits) — bleed "this" correctly classified as filler
```

### Window sizing rationale

| window | Behavior |
|---|---|
| 1-2 | Too tight; misses normal Whisper drops (1-2 fast function words like "a", "the", "to") |
| **6 (default)** | Absorbs typical Whisper drops while rejecting bleed jumps |
| 8-10 | Lets some bleed through; observed failure mode in clip 2 (j=12 still in window) |
| ∞ (pre-v596) | Original strand-behind bug |

For clips with heavy whisper drops (high music background), increase `lookahead_window` per-call. For clips with severe Veo audio bleed, decrease. Default 6 handles 95%+ of observed cases on the 2026-05 corpus.

### What this fix does NOT address

- **Truncated clips at end of video** (e.g. belly-fat-tonic clip 11: script too long for 7.7s window — Veo cut off mid-word). Matcher correctly stops at audible audio.
- **Whisper genuinely missing words** (e.g. clip 1 "of stubborn" missing). Matcher skips script words it can't find — same behavior pre and post v596.
- **Veo audio hallucinations** (handled by separate logic — refused-bridge detection on unmatched words with 0.4-0.6 confidence).

### Migration

`code/video_processor.py` line 1151 `_match_in_order` already updated. No call-site changes needed — `lookahead_window` defaults to 6. Per-clip overrides possible via the `lookahead_window` parameter.

### Worked impact estimate (belly-fat-tonic export, 2026-05-05)

Pre-fix: 86/116 script words matched (74%)
Post-fix expected: ~95-100/116 (~85-90%) — recovers ~9-14 stranded script words across clips 2, 3, 11. Final-export VAD segments will retain those previously-cut dialogue ranges.

---

## Strict-header platform contract (v593)

**The platform parser (`code/image_platform.py`) uses STRICT regexes — silent on failure.** Bad headers don't error, they're skipped, and you get `Parse error: No scenes found in the markdown` at import.

### Strict header regexes

```python
^###\s+Image\s+(\d+)\s*$    # ### Image N — integer + nothing else
^###\s+Scene\s+(\d+)\s*$    # ### Scene N — integer + nothing else
```

### Required Image block schema

```markdown
### Image N
- **reference_image:** image_M | none      ← optional; defaults to none
- **product_image:** <name>                ← optional (v581 product binding)
- **Image prompt:**
  ```
  <Banana 2 six-block prompt>
  ```
```

The fenced **Image prompt** block is required. The parser only reads the bullet fields above and the FIRST fenced block after `**Image prompt:**`.

### Required Scene block schema

```markdown
### Scene N
- **image:** image_N            ← REQUIRED — references which Image block
- **clip_mode:** fresh|continue|blend     ← optional
- **transition:** cut|null|blend          ← optional (alias: scene_transition)
- **visual register:** <text>             ← optional, em-dash splits at first " — "
- **rhythm tier:** <text>                 ← optional
- **speaker:** on-camera|voiceover|auto   ← optional, normalized via synonym table
- **line:** <dialogue text>               ← REQUIRED, ≥1 per scene
- **action_note:** <single-line prose>    ← optional, attaches to most recent line

# multiple line+action_note pairs allowed in ONE scene block:
- **line:** <second dialogue line>
- **action_note:** <second action_note prose>
```

### action_note must be a single line

The bullet regex captures `(.+?)\s*$` per line — multiline structured forms (bulleted Cinematography/Subject/Action/Context/Style) will NOT parse; the parser sees only the first line. Use **inline prose** with `[Start beat 0-Xs]`, `[Mid-clip beat]`, `[End beat]` markers in one continuous string.

### Speaker synonyms (parser-accepted)

| Canonical | Accepted spellings (case-insensitive, dashes/spaces/underscores ignored) |
|---|---|
| `on-camera` | on-camera, on camera, on_camera, dialogue, speaks, spoken, lip-sync, character, character speaks |
| `voiceover` | voiceover, voice-over, vo, narration, off-screen, narrator, narrated |
| `auto` | auto, detect, default, "" (empty) |

### Block boundaries

Scene/Image blocks end at the next `### Scene N` / `### Image N` / `## <Capital>` heading.

### Pre-import verification (mandatory)

Before pushing a `videos/*.md` to the platform, run:

```bash
python -c "
import re
text = open('videos/<file>.md', encoding='utf-8').read()
imgs = re.findall(r'^###\s+Image\s+(\d+)\s*\$', text, re.MULTILINE)
scns = re.findall(r'^###\s+Scene\s+(\d+)\s*\$', text, re.MULTILINE)
print(f'Images: {len(imgs)}  Scenes: {len(scns)}')
"
```

If either count is 0 (or fewer than expected), check for header suffixes — the most common failure mode.

### Splitting a scene by clip (v577 word budget)

Add a second `- **line:**` + `- **action_note:**` pair within ONE `### Scene N` block — never via `#### Scene Na` h4 sub-headers (h4 is parser-rejected).

### Migration

Pre-v593 markdowns with descriptive header suffixes (e.g. `### Image 1 — HOOK clinical-exam`) silently fail import. Author all new artifacts to v593 strict format from this commit forward.

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
   Use the uploaded character reference image for the main character.
   ```
   Replace "the main character" with whatever persona alias your Ingredients table uses. Replace "her" with the appropriate pronoun for the persona.

2. **Product binding (only when the image binds the product upload — i.e. when the `product_image:` field is set on this image):**
   ```
   Use the uploaded product reference image for the [product ingredient name].
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

---

### v681 — Multi-character cast model + text-card scene type

**What v681 introduces** (decode + lift, plus platform parser/schema/renderer changes):
1. Two new `Type` values in the `## Ingredients` table — `patient` and `extra`.
2. Optional per-scene `- **cast:** name1, name2, ...` bullet declaring presence.
3. Simplified `- **speaker:** <character_name> <on-camera|silent>` (NO `voiceover` in v681 — defer to v682).
4. New `scene_type: text_card` for "2 months later…" black-card transitions, with `caption:`, `bg_color:`, `duration:` bullets.
5. Optional `- **caption:**` on regular scenes — decode capture only; generate ignores per v621.

**Cast type vocabulary**

| Type | Speaks? | Image-bound? | Recurring? | Reference column | Notes |
|---|---|---|---|---|---|
| `character` | yes (persona) | yes | always | REQUIRED | The persona row. Existing v537/v602/v610 binding rules unchanged. |
| `patient` | NO | yes (optional) | yes | OPTIONAL — see below | Recurring named non-speaker (testimonial subject). Two binding modes (v681e). |
| `extra` | NO | NO | one-shot | `—` | Bystander mentioned/shown in a single scene. Identity carried in prose per v669. |
| `product` | n/a | yes | n/a | REQUIRED | Existing product row. Unchanged. |
| `setting` | n/a | yes | n/a | OPTIONAL | Existing recurring-location row. Unchanged. |

**Patient binding modes (v681e):**

| Mode | When | Behavior |
|---|---|---|
| **Upload-backed** | Reference column points to an uploaded reference (e.g. `patients/refs/donna-blonde.png`) | Same as a `character` row — every scene that includes the patient binds the upload via v509. Identity stays consistent because the SAME image is referenced every time. |
| **Anchor-scene** (no upload) | Reference column is `—` / empty | The FIRST scene that mentions the patient by name becomes the anchor — Banana 2 generates the patient's appearance freely (informed by image-prompt prose per v669-style identity descriptors). SUBSEQUENT scenes that mention the patient chain back to that anchor scene's chosen variant via v512 — Flow receives the anchor scene's render as a reference image, locking the patient's appearance across the rest of the video. |

**When to use anchor-scene mode for a patient:**
- Operator doesn't have a clean isolated reference photo for the patient yet
- Patient is fictional (a composite "Donna" character we're inventing for the testimonial)
- Quick decode-to-test cycle where capturing the patient upload is too high-friction
- The patient's appearance can be carried by the first scene's prose alone (v669 identity descriptors: race + age + build + clothing)

**Authoring rule for anchor-scene patients:** the FIRST scene where the patient appears MUST include identity prose in the image_prompt body (race, age, build, hair, clothing) per v669 — this is the only place Banana 2 has to learn what the patient looks like. Subsequent scenes can drop the prose because the v512 chain references the anchor scene's variant.

**Authoring example (upload-backed patient)**

```markdown
## Ingredients

| # | Type      | Name        | Reference                                    |
|---|-----------|-------------|----------------------------------------------|
| 1 | character | the healer  | personas/refs/amish-grandmother.png          |
| 2 | patient   | Donna       | patients/refs/donna-blonde.png               |
| 3 | extra     | husband     | —                                            |
```

**Authoring example (anchor-scene patient — no upload, v681e)**

```markdown
## Ingredients

| # | Type      | Name        | Reference                                    |
|---|-----------|-------------|----------------------------------------------|
| 1 | character | the healer  | personas/refs/amish-grandmother.png          |
| 2 | patient   | Donna       | —                                            |
| 3 | extra     | husband     | —                                            |
```

**How the v512 chain locks Donna's face without an upload:**

```
### Image 1                       — FIRST appearance of Donna
- **cast:** the healer, donna, the husband
- **Image prompt:**
```
…Donna lies on her left side in the foreground. She is a white woman in
her mid-40s, blonde shoulder-length hair parted center, blue eyes, average
build with weight around the midsection, soft round facial features, no
makeup. She wears a soft gray crew-neck sleep tee…
```
                                  ↑ identity prose REQUIRED on first scene
                                    (race + age + build + hair + clothing)
                                    — Banana 2 has no upload to bind, so
                                    prose IS the only signal. v669 rule.

### Image 2                       — second appearance of Donna
- **reference_image:** image_1    ← optional; carries setting (bedroom) too
- **cast:** donna
- **Image prompt:**
```
…The main character stands in left-profile in front of a bedroom dresser…
```
                                  ↑ NO identity prose needed for Donna
                                    — v512 chains image_2 to image_1
                                    via the `cast: donna` lookup, so
                                    Donna's face inherits from image_1's
                                    chosen variant.

# What the platform does at import:
# 1. Image 1 has `cast: donna` AND donna has no upload (Reference: —).
#    → image 1 is registered as the anchor scene for "donna".
# 2. Image 2 has `cast: donna` (donna still has no upload).
#    → platform looks up "donna" in ingredient_nodes (no upload, miss),
#      then in anchor_scenes (hit: image 1) → attaches image 1's chosen
#      variant as a parent edge on image 2.
# 3. Flow receives image 1 as Donna's reference. Donna's face stays
#    identical across image 2/3/5 without any upload.
```

**Two pieces, both required, but they do different jobs:**

| Piece | Where | Job |
|---|---|---|
| `cast: <patient>` bullet | Scene/image YAML | Platform-side: ensures the parent edge attaches the anchor scene's chosen variant. Without this, the platform doesn't know to bind the reference. |
| `Use Image N as the visual reference for the previous scene — preserve <X>, <Y>, <Z> from there.` | First sentence of the image_prompt body | **Banana 2 prompt instruction**: tells the model HOW to use the attached reference. Per Nano Banana 2 official docs (ai.google.dev/gemini-api/docs/image-generation) and v608 — Banana 2 indexes references positionally and does NOT auto-figure-out what each ref is for. Without this line, the attached reference is treated as generic visual context and may be ignored. |

**The `reference_image: image_N` BULLET is OPTIONAL when `cast:` declares the patient** — v512 already attaches the anchor scene as parent edge. Keep `reference_image:` only when:
- You ALSO want the setting/composition to chain (e.g. follow-up scene in the same bedroom)
- You're following a non-cast chain (e.g. continuing a prop or an environmental anchor)

The PROMPT BODY's "Use Image N" line is **REQUIRED** every time you want Banana 2 to actually USE an attached reference — `cast:`-driven and explicit `reference_image:` chains both need it. The platform's `_resolve_flow_prompt_bindings` rewrites N at submission time to Flow's actual slot.

**For the Donna anchor-scene example, Image 2's first body sentence:**

```
Use Image 1 as the visual reference for the previous scene — preserve Donna's
facial features (mid-40s blonde, blue eyes, soft round features) and the
suburban bedroom (dark-wood headboard, beige bedding, dresser) from there.

Shot on iPhone with wide-angle lens, handheld, deep focus, vertical 9:16
framing. Same suburban bedroom as image 1 ...
```

The cast: bullet handles binding; the body's "Use Image 1" line tells Banana 2 what to do with what was bound.

**Slot order — what the platform actually sends to Flow:**

The author writes positional `Image K` references using the markdown's local numbering (`### Image 1`, `### Image 2`, ...). The platform's body-renumber pass (`_resolve_flow_prompt_bindings` in `image_platform.py:1283`) translates those at submission to Flow's actual slot positions.

Slot ordering (v573 priority sort at `image_platform.py:4943`):

| Priority | Slot | Edge type | Markdown source |
|---|---|---|---|
| 0 | Flow Image 1 | persona character upload | `cast: the healer` (or v607 force-bind safety net) |
| 1 | Flow Image 2 | explicit upload (product, variant chain base) | `cast: the [product name]`, variant ingredients |
| 2 | Flow Image 3 | anchor-scene chain (patient, anchor-scene ingredient) | `cast: donna` where Donna has Reference=`—` and was bound on a prior scene; OR `reference_image: image_K` |
| 3+ | (capped at 3 parents) | additional chain references | extra cast members beyond the cap (rare) |

**Renumber pass coverage** (v681e.8):
- ✅ Persona role: `the uploaded character reference image` → `Image 1`
- ✅ Product role: `the uploaded product reference image` → `Image 2`
- ✅ Chain semantic phrases: `the prior-scene reference image`, `the previous scene's reference image` → `Image N` (the chain edge's actual slot)
- ✅ Chain positional `Image K`: rewritten to actual Flow slot — works for both `chain_from_image_*` edges AND v681 anchor-scene patient edges (v681e.8 classifies anchor-scene patient edges as `chain`)
- ❌ Settings or non-chain anchor-scene ingredients: not currently renumbered (rare; if you hit this, write the binding line using the semantic phrase `the prior-scene reference image` instead of a positional number)

**Authoring rule (positional references):**
1. In the chain binding sentence, use the LOCAL markdown image number (`Image 1` for `### Image 1`, etc.).
2. The platform rewrites it to Flow's slot at emission. So if your image's parents are `[the healer (slot 0), Donna anchor=image_1 (slot 1)]`, your body's "Use Image 1 as the visual reference..." gets emitted as "Use Image 2 as the visual reference..." (because Donna's anchor lands at Flow slot 1+1=Image 2 from Banana 2's perspective).
3. Verify in the platform UI: hover the per-image card → "Parents" lists the bound parents in slot order; Flow's "Image 1" = first parent, "Image 2" = second, etc.

#### v681e.9 — generic cast composition matrix (every future video type)

The slot-ordering contract is **deterministic** and works for any cast combination. This matrix enumerates every realistic composition so authors never need to guess what `Use Image N` will become at submission.

**Inputs:** parent edges sorted by `slot_order` (set by the v573 priority sort during `import_scene_table`).
**Output:** Flow receives parents at positions Image 1, Image 2, Image 3 (capped at 3).
**Substitution:** `_resolve_flow_prompt_bindings` rewrites the body's role phrases + positional `Image K` references to match.

| Cast composition | Bound parents (slot order) | Flow Image 1 | Flow Image 2 | Flow Image 3 | Body author writes | Body Banana 2 sees |
|---|---|---|---|---|---|---|
| **Talking-head — persona only** | `[persona]` | persona | — | — | `the uploaded character reference image` | `Image 1` |
| **Listicle — persona + product** | `[persona, product]` | persona | product | — | `the uploaded character reference image` AND `the uploaded product reference image` | `Image 1`, `Image 2` |
| **Recipe-pivot — persona + product + chain** | `[persona, product, chain]` | persona | product | prior scene | `... character ...`, `... product ...`, `the prior-scene reference image` | `Image 1`, `Image 2`, `Image 3` |
| **Multi-character testimonial — persona + patient anchor (v681)** | `[persona, patient-anchor]` | persona | patient anchor scene | — | `the uploaded character reference image` AND `Use Image K as the visual reference for the previous scene` | `Image 1`, `Image 2` |
| **Patient-alone scene — patient anchor only (no persona present)** | `[patient-anchor]` | patient anchor scene | — | — | `Use Image K as the visual reference for the previous scene` | `Image 1` |
| **Patient + product transformation** | `[patient-anchor, product]` | patient anchor scene | product | — | `Use Image K ...` AND `the uploaded product reference image` | `Image 1`, `Image 2` |
| **Variant chain — persona + her variant base** | `[persona, variant-base]` | persona | variant base | — | `... character ...` AND `Use Image K ...` | `Image 1`, `Image 2` |
| **Establishing scene — no parents** | `[]` | — | — | — | (no reference phrases) | (body unchanged) |
| **Text-card scene** | `[]` (no image_node, drawn at video assembly) | — | — | — | n/a — no Banana 2 render | n/a |

**How to read this matrix as an author:**
1. Look at your scene's `cast:` bullet and figure out which parent edges will attach (persona = character ingredient with upload; patient = anchor-scene chain; product = product ingredient with upload).
2. Find the row matching your composition.
3. The "Body author writes" column tells you which phrases to use in the image_prompt body. Use the canonical role phrases (`the uploaded character reference image`, `the uploaded product reference image`, `the prior-scene reference image`) OR positional `Image K` referring to the markdown image number for chain references.
4. The "Body Banana 2 sees" column tells you what the platform actually emits at Flow submission. The `_resolve_flow_prompt_bindings` pass handles the translation.

**Edge classification truth table** (`_classify_edge_for_manifest`):

| `edge.kind` | `edge.role` | `edge.parent.kind` | Classification | Renumbered? |
|---|---|---|---|---|
| `character` | * | * | persona | ✅ role-phrase |
| `product` | * | * | product | ✅ role-phrase |
| (any) | `variant_chain:*` | * | persona | ✅ role-phrase |
| (any) | `chain_from_image_*` | * | chain | ✅ semantic + positional |
| (any) | `subject` (legacy) | * | persona | ✅ role-phrase |
| (any) | `reference` (legacy) | * | chain | ✅ semantic + positional |
| (any) | (persona alias text) | * | persona | ✅ role-phrase |
| (none) | (other) | `generated` | chain (v681e.8) | ✅ semantic + positional |
| (none) | (other) | `upload` / NULL | other | ❌ no rewrite |

**Invariant:** every parent edge that the author intends Banana 2 to use MUST classify as `persona`, `product`, or `chain`. If a `cast:` member produces an edge classified as `other`, the body's `Use Image N` reference will NOT be rewritten to that edge's slot — Banana 2 receives the upload as input but gets no instruction what to do with it.

**Detection in production:** `_resolve_flow_prompt_bindings` emits `[v681e.9/renumber]` log line on every body change OR when `other`-classified edges are present. Search Render logs after import to verify expected behavior:

```
[v681e.9/renumber] node=1234 slots=[(1, 'persona', 'the healer'), (2, 'chain', 'donna')] changed=True
```

If you see `(N, 'other', '...')` in the slot table, the renumber pass skipped that edge — investigate the edge classification (probably needs a kind/role fix at import-time OR a new pattern in `_classify_edge_for_manifest`).

#### v682 — split-rule body references (persona positional, rest descriptive)

**Replaces v581's positional `Image K` for non-persona references.** The renumber pass behavior of v681e.8 / v681e.9 stays as a deprecated fallback for legacy artifacts; new artifacts MUST follow the split rule below.

**Why this rule exists:** the renumber pass works correctly for chain edges, but its `\bImage K\b` regex is aggressive — every `Image K` in body gets rewritten if any chain edge happens to land at flow slot N where md_image_num=K. Authors can't easily predict the rewrite at write-time. The split rule eliminates positional ambiguity for everything except the persona, and uses the description-based prompting pattern that Nano Banana 2's official docs explicitly recommend for "high-fidelity detail preservation" (citation: ai.google.dev/gemini-api/docs/image-generation §"High-fidelity detail preservation").

**HARD RULE:**

| Reference type | Body must use | Why |
|---|---|---|
| **Persona (main character)** | Positional only: `Use Image 1 for the main character (the <persona name>).` | Banana 2 has no prior knowledge of persona's face; description doesn't help — must point to the upload by position. v573 priority sort guarantees persona = slot 0 = Flow Image 1, so `Image 1` is the invariant. |
| **All other references (chain, product, named cast, patient anchor)** | Description ONLY, no `Image K` | Decode/lift produces full identity prose (per v669 — Non-persona character identity prose mandatory on first appearance). Reuse that prose to identify the subject. Banana 2 binds the input to the description visually because the description matches what the input contains. |

**FORBIDDEN in new artifacts:**
- `Use Image 2 ...`, `Use Image 3 ...`, etc. — any non-`Image 1` positional reference for non-persona subjects
- `Use Image K as the visual reference for the previous scene` — replaced by descriptive prose

**Worked example — Donna Image 3 (cast: the healer, donna):**

Slot order at submission: persona (healer) → slot 0 = Flow Image 1; chain (donna anchor=image_1) → slot 1 = Flow Image 2.

Pre-v682 body (deprecated):
```
Use Image 1 as the visual reference to preserve Donna's facial features
(mid-40s blonde, blue eyes, soft round features, average build) from the
previous scene.

Shot on iPhone with wide-angle lens ... Donna sits ... The healer stands ...
```

The `Use Image 1` was rewritten by v681e.8 renumber pass to `Use Image 2` because Donna's chain edge landed at flow slot 2. Worked — but author couldn't predict it.

v682 body:
```
Use Image 1 for the main character (the apothecary healer).

Donna: mid-40s woman, blonde hair pulled back, blue eyes, soft round
features, average build. She wears jeans and a fitted grey sweatshirt.
Preserve her facial features and body identity from the previous bedroom
scene.

Shot on iPhone with wide-angle lens ... Donna sits on a wooden exam bench
in the foreground ... The healer stands beside her ...
```

Persona = `Image 1` literal (slot 0 invariant). Donna = full description, no positional ref. Banana 2 binds Donna's anchor (slot 1, Flow Image 2) by description match — sees a mid-40s blonde woman in the input that matches the body's description, uses her likeness for the rendered output.

**Authoring source for descriptions:**

| Subject | Where description comes from |
|---|---|
| Persona (excluded from rule — uses positional) | n/a |
| Patient (recurring named character with anchor scene) | Identity prose at first appearance (v669) — copy into every later image where patient appears |
| Product | Product card (e.g. `wiki/products/corella-saffron.md`) — full visual description |
| One-shot extra | Identity prose in the image where they appear (one-time, no chain) |
| Setting chain (apothecary kitchen, bedroom, living room) | Setting prose from the anchor image — copy into chained images with "preserve [setting elements] from the prior [scene name] scene" |

**Backward compat:**
- Legacy artifacts using `Use Image K` continue to work — v681e.9 renumber pass still fires
- v681e.9 diagnostic `[v681e.9/renumber]` log line counts legacy fallback occurrences; track migration progress
- Future v683: hard-error on `\bUse Image \d+\b` for non-persona refs after migration completes

**Lint pattern (count remaining v581/v681e.x usage):**
```bash
grep -rEn "Use Image [2-9]" videos/ raw/decoded_*.md | wc -l
```
Image 1 references are exempt (persona-positional). All other `Use Image K` references for K ≥ 2 are migration backlog.

## Storyboard

### Scene 1
- **image:** image_1
- **cast:** the healer, Donna, husband
- **caption:** I felt invisible...
- **speaker:** the healer on-camera
- **clip_mode:** fresh
- **line:** If you're facing the same issue Donna once did,

### Scene 4
- **scene_type:** text_card
- **caption:** 2 months later...
- **bg_color:** black
- **duration:** 1.0s
```

**Speaker attribution**

`<character_name> <mode>`. The character_name MUST match a row in the Ingredients table whose Type is `character` (i.e. the persona). `mode` is one of:

| Mode | Veo audio | Lip-sync | When to use |
|---|---|---|---|
| `on-camera` | ON | ON | Persona speaks the line with visible mouth movement |
| `silent` | (none) | n/a | No dialogue; music or SFX only. `- **line:**` bullet MUST be omitted. |
| (omitted) | ON | ON | Default — equivalent to `<persona> on-camera` |

**Removed in v681**: `voiceover` mode. v681 cannot author videos where the persona's voice plays UNDER b-roll visuals. Such videos require the v682 b-roll-overlay clip-pair construct (see "Out of scope" below).

**Per-scene cast presence**

`- **cast:** name1, name2, ...` is OPTIONAL but encouraged on every scene with 2+ cast members. When present, the platform binds ONLY the named cast members' uploads (skipping the v509 prompt-scan path for that scene). When absent or empty, v509 prompt-scan runs as before — pre-v681 markdown imports unchanged.

For text-card scenes (`scene_type: text_card`), `cast:` is OMITTED.

**v681e.3 — explicit `cast:` is authoritative; v607 force-bind respects it.**

Pre-v681e.3 the v607 character-force-bind path ran UNCONDITIONALLY on every image — even when the operator declared `cast:` and DELIBERATELY omitted the persona (e.g. a Donna-alone BEFORE bedroom scene where the healer is genuinely not present). Result: Flow received the persona upload anyway, Banana 2 tried to fit the persona into the composition as a passive face element, and the rendered scene came out with the healer shoehorned into a frame that was supposed to be Donna alone.

v681e.3 fixes this — when `cast:` is declared on the image:
- INCLUDED character ingredients are bound (v681 fast path adds them to `mentioned` automatically).
- OMITTED character ingredients are RESPECTED — v607 force-bind does NOT add them back.
- Log line `[import] Image N: v681e.3 explicit cast — K char(s) bound: [...]; M char(s) intentionally excluded: [...]` documents what was excluded.

When `cast:` is ABSENT (legacy / pre-v681 imports), v607 force-bind runs as before — every character ingredient with an upload is force-bound regardless of prose mentions.

**Authoring rule (HARD RULE):** the `cast:` bullet MUST list ONLY the characters that are VISUALLY PRESENT in the scene's frame. Not "characters mentioned in dialogue", not "characters relevant to the story" — only "characters whose body / face appears in this composition".

Concrete checklist before writing a `cast:` bullet:
1. Look at the source frame (decode) OR the planned composition (lift). Who is physically in frame?
2. List ONLY those names. Order doesn't matter.
3. If the persona is NOT in this scene's frame, OMIT her — even if she's the speaker via voiceover OR mentioned in another scene's dialogue.
4. If a recurring patient is NOT in this scene's frame, OMIT her — even if her transformation is the video's subject.
5. If a one-shot extra appears in this scene only, INCLUDE them — they have no upload (Reference: `—`) so binding is a no-op anyway, but cast: documents the prose-only character for the audit gate.

Why this matters: per v681e.3, the `cast:` bullet is AUTHORITATIVE. Anyone listed gets their upload (or anchor scene) attached as a parent edge — Flow then receives that reference and Banana 2 will TRY to fit them into the composition. Listing the persona in a Donna-alone bedroom scene results in the persona being shoehorned in as a passive face element. Listing only the characters actually present is the correct authoring discipline.

**Common authoring mistakes:**
- ❌ `cast: the healer, donna, the husband` on a bedroom scene where ONLY Donna and her husband are visible (the healer is a different scene's character) — the healer's upload gets attached and Banana 2 will try to render her in the bedroom. Fix: drop her from cast.
- ❌ `cast: the healer` on a Donna AFTER mirror-selfie scene where only Donna is visible — the healer's upload gets attached. Fix: `cast: donna`.
- ✅ `cast: the healer, donna` on the apothecary examination scene where BOTH are visible.
- ✅ `cast: the healer` on the apothecary talking-head scene where only the healer is visible.

**Text-card scenes**

When `scene_type: text_card`:
- REQUIRED: `caption:`, `bg_color:`
- OPTIONAL: `duration:` (defaults to 1.0s if omitted)
- FORBIDDEN: `image:`, `cast:`, `line:`, `clip_mode:`, `transition:`

The platform skips Nano Banana 2 image generation for these scenes; the video processor renders them via ffmpeg `drawtext` (solid color background + caption text) at export time.

**Captions on regular scenes (decode-side capture)**

Optional `- **caption:**` on a `scene_type: shot` scene records the source's on-screen caption verbatim. The lift bundle's prompt-build path IGNORES this field (v621 caption ban for owned content remains in force). Field exists strictly for analysis / reverse-engineering of the source's caption strategy.

**Decode-side rules**

When the decoder identifies a non-speaking person who appears in 2+ scenes AND has visual continuity (same human, named or unnamed), emit a `patient` row. Set Reference column to a synthetic placeholder filename like `patients/refs/<name>-<hair>.png` (operator captures the actual upload at lift time) OR set it to `—` to defer the upload and rely on v681e anchor-scene mode (Banana 2 generates the patient on the FIRST scene; subsequent scenes chain via v512). When emitting `—`, MAKE SURE the first scene where the patient appears has full identity prose in its image_prompt body (race + age + build + hair + clothing) so Banana 2 has enough signal to render a stable face for the chain.

When a non-speaking person appears in EXACTLY ONE scene AND is referenced incidentally, emit an `extra` row with Reference = `—`. The `extra`'s identity prose lives in the image prompt per v669 (race + age + build + clothing).

When the decoder hears a single voice that does not match any visible mouth movement in the source frames, emit `on-camera` for whatever frames show the speaker AND `silent` scenes for everything else. DO NOT emit `voiceover` — it is reserved for v682. Add a comment in the artifact noting that the source's voiceover-overlay structure was not preserved.

**Out of scope (v682)**

Voiceover b-roll overlay (1 logical scene = 2 generated Veo clips: persona-on-camera audio source + silent b-roll visual overlay) is a separate v-rule deferred until lift-side authoring frequency justifies the platform composer work.

Multi-speaker dialogue (2+ characters BOTH speaking — interviews, sketches) is also deferred. v681 explicitly forbids `patient` or `extra` rows from being a `speaker:` value.
