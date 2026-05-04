# Gemini decode prompt — v589 video reverse-engineering

**Usage:** in Gemini (gemini.google.com or the Gemini API), upload the source MP4 and paste this entire file as the prompt. Gemini natively samples video at 1fps + audio + per-second timestamps.

---

## CRITICAL output-format rules (the platform parser depends on these)

The output is fed verbatim to a strict regex parser. Any deviation breaks the build.

1. **DO NOT wrap the entire output in a code fence.** Do not start with ` ``` ` or ` ```markdown `. The output is plain markdown.
2. **DO NOT use ` ```markdown ` anywhere.** All code fences are bare three-backticks: ` ``` ` (no language tag).
3. **Image headings are EXACTLY `### Image N`** where N is an integer (e.g. `### Image 1`, `### Image 2`). Capital "I" in "Image". The space and integer matter.
4. **Scene headings are EXACTLY `### Scene N`** with the same rules.
5. **References to images use lowercase + underscore: `image_N`** (e.g. `reference_image: image_1`, `image: image_3`). NEVER `Image 1`, `image 1`, `image1`, or `IMAGE_1`.
6. **Every `### Image N` block MUST contain three fields in this order:**
   - `- **reference_image:** image_K`  (or `none` for Image 1 / non-chained images)
   - `- **product_image:** the [product name]`  (ONLY when the image binds the product upload — omit the line entirely otherwise; do NOT write `none`)
   - `- **Image prompt:**` followed by a bare-fence ` ``` ` block containing the prompt body
7. **Every `### Scene N` block MUST contain these fields in this order:**
   - `- **image:** image_K`
   - `- **clip_mode:** fresh | continue | blend`
   - `- **transition:** cut | blend | null`
   - `- **visual register:** <BLOCK_TAG — short descriptor>`
   - `- **rhythm tier:** <descriptor> (Xw)`
   - `- **speaker:** on-camera | voiceover`
   - `- **line:** <verbatim dialogue>`
   - `- **action_note:** <three-beat motion narrative>`
8. **No outer wrapper.** The output starts with `<!--` (the HTML header comment block) and ends with the last clip's negative prompt fence. Nothing before, nothing after.
9. **Image-number references in body prose: capital-I ONLY in the v581 binding line.** The platform's prompt-builder does case-sensitive substitution: `\bImage K\b` (capital I) gets rewritten to Flow's actual slot number. Lowercase `image K` does NOT get rewritten and Banana 2 sees a phantom reference.
   - **OK** (capital, in v581 chain binding line): `Use Image 6 as the visual reference for the previous scene` → platform rewrites `Image 6` → actual Flow slot.
   - **FORBIDDEN** (lowercase, descriptive prose): `Same X interior as image 6`, `the jar from image 5`, `same as image 1`, `continued from image 4`. Banana 2 has only persona + product + chain attached for a given generation; numbered lowercase references break.
   - **Required pattern** in body prose: `as the previous scene` / `from the previous scene` / `same as before` / `the same X` / direct setting description. NEVER `as image K` / `from image K` / `in image K` (lowercase).

---

## Role

You are a video-understanding assistant for a viral-ad reverse-engineering pipeline. Your output drives generative-video re-rendering (Veo 3.1) and image generation (Nano Banana 2). The decoded markdown becomes a **reproduction-ready artifact**: feeding any clip's prompt back to Veo 3.1 should re-render the source clip closely.

## Input

I'm uploading a viral short-form video (typically 30-90s, 9:16). Decode it under the v589 schema below.

## Output structure (literal — copy this skeleton)

The block below is a literal skeleton. Replace `<...>` placeholders with real content. Keep field names, heading levels, and bare-fence syntax exactly as shown.

````
<!--
  DECODED FROM SOURCE: <filename or url>
  Source specs: <duration>s duration, <W>x<H>, <fps>fps, codec=<codec>.

  ═══════════════════════════════════════════════════════════════════
  PIPELINE USED FOR THIS DECODE (v579 + v585 + v586 + v587 + v588 + v589)
  ═══════════════════════════════════════════════════════════════════
  Decoded via Gemini native video understanding (1fps + audio + per-second timestamps).
  Whisper.cpp not run — Gemini transcription is the dialogue source.
  All v586 grammar + v587 sections + v588 dense-frame discipline + v589 absolute-magnitude rule applied.

  ═══════════════════════════════════════════════════════════════════
  STRUCTURE: <N> scenes, <M> clips
  ═══════════════════════════════════════════════════════════════════
  • Scene 1 <BLOCK_TAG>: <one-line summary>
  • Scene 2 <BLOCK_TAG>: <one-line summary>
  ...

  ═══════════════════════════════════════════════════════════════════
  RULE-VARIANT NOTES
  ═══════════════════════════════════════════════════════════════════
  V539 HOOK variant: <force-verb / clinical-markup / diagnostic-press / symptom-curiosity / banana-pun / fat-melt / ...>
  V541 OUTFIT-CHANGE: applied | NOT APPLICABLE — <reason>
  V580 RECIPE STATE-EVOLUTION: applied | partial | NOT APPLICABLE — <reason>
  V573 PRODUCT BINDING: applied (scenes <X, Y, Z>) | NOT APPLICABLE — <reason>
  V585 MOTION CAPTURE: <camera-move classifications observed>
  V589 ABSOLUTE-MAGNITUDE: <which clips are state-evolution + magnitude COMPLETE/PARTIAL>
-->

**Video:** <one-line title>
**Persona:** <archetype>
**Setting:** <Tier 0/1/2 — single or multi>
**Duration:** <X>s
**Structure:** <HOOK N / RECIPE N / EXPLAIN N / AUTHORITY N / PRODUCT N / CTA N>
**Video mode:** storyboard
**Auto-split:** off

---

## Ingredients

| Name (used in prompts) | Type | Description | Source |
|---|---|---|---|
| `the main character` | character | <archetype + ethnicity + age band — never inline-described per v553.1> | External persona upload — Flow slot 0 (Image 1) |
| `the [product name]` | product | <only if branded product visible; omit row if no product> | External product upload — Flow slot 1 (Image 2) |

---

## Images

### Image 1
- **reference_image:** none
- **Image prompt:**
```
Use the uploaded character reference image for the main character — match identity, hair, clothing exactly.

<v586 six-block walk: Subject (pose / eye direction / mouth state / expression) — Composition (frame partition / depth layers / crop / foreshortening / single-vs-two-shot) — Action (current gesture / hand positions / eye tracking) — Location (every prop with EXPLICIT position: "the bottle on the desk lower-left", "the anatomy poster behind at jaw height" — never "in the background") — Style (lighting direction / palette / mood) — Tech (iPhone wide-angle handheld / distance in feet or arm-lengths / deep focus / motion blur if any)>
```

### Image 2
- **reference_image:** image_1
- **Image prompt:**
```
Use the uploaded character reference image for the main character — match identity, hair, clothing exactly.
Use Image 1 as the visual reference for the previous scene — preserve <setting>, <lighting>, <anchor props>, continuity from there.

<v586 six-block walk for this scene's start frame>
```

### Image 3
- **reference_image:** image_2
- **product_image:** the [product name]
- **Image prompt:**
```
Use the uploaded character reference image for the main character — match identity, hair, clothing exactly.
Use the uploaded product reference image for the [product name] — match label, packaging, color, proportions exactly.
Use Image 2 as the visual reference for the previous scene — preserve <setting>, <lighting>, <anchor props>, continuity from there.

<v586 six-block walk; the product is named in the description>
```

---

## Storyboard

### Scene 1
- **image:** image_1
- **clip_mode:** fresh
- **transition:** null
- **visual register:** HOOK — <short descriptor>
- **rhythm tier:** <descriptor> (<X>w)
- **speaker:** on-camera
- **line:** <verbatim dialogue from audio>
- **action_note:** <v586 five-block: Cinematography (camera-move per v585) — Subject (every entity that moves) — Action ([Start beat 0-2s] X / [Mid-clip beat 3-5s] Y / [End beat 5-8s] Z) — Context (anchor-prop carry-over from start frame) — Style & Ambiance ([register tag] + ambient sound cues)>

### Scene 2
- **image:** image_2
- **clip_mode:** fresh
- **transition:** cut
- **visual register:** <BLOCK_TAG> — <descriptor>
- **rhythm tier:** <descriptor> (<X>w)
- **speaker:** on-camera
- **line:** <verbatim dialogue>
- **action_note:** <v586 five-block>

---

## Comprehension

### Structural inventory
- Total: <N> scenes, <M> clips, ~<T>s
- Per-scene block tags:
  - Scene 1: HOOK
  - Scene 2: <TAG>
  - ...

### v-rule inventory
| v-rule | Status | How this video uses it |
|---|---|---|
| v521.1 pin-down | applied | <head/shoulder/crop/distance anchors> |
| v523 reference chaining | applied | <chain pattern> |
| v538 speaker mode | applied | <on-camera / voiceover per scene> |
| v539 HOOK weird-action | applied — variant: <name> | <description> |
| v540 action_note discipline | applied | <motion-only confirmed; three-beat structure> |
| v541 outfit-change | applied / NOT APPLICABLE | <reason> |
| v544 transitions | applied | <fresh+cut default; continue chains in scenes ...> |
| v553.1 persona never inline | applied | <"the main character" used; identity from upload> |
| v573 + v581 product binding | applied / NOT APPLICABLE | <product_image: field on images <list>> |
| v577 line word budget | applied | <line word counts; longest <X>w> |
| v580 recipe state-evolution | applied / partial / NOT APPLICABLE | <each step own image, OR collapsed> |
| v585 motion capture | applied | <camera-move classifications per shot> |
| v586 description grammar parity | applied | <six-block walk per image; five-block per action_note> |
| v587 reproduction-ready artifact | applied | <Comprehension + Veo Final Prompts emitted> |
| v588 dense per-shot frame sampling | applied | <action arcs captured> |
| v589 absolute-magnitude grammar | applied | <COMPLETE/PARTIAL per state-evolution clip> |

### Rhetorical structure
- **HOOK type**: <name + variant>
- **Frame**: <recipe-as-claim / before-after-transformation / authority-stack / curiosity-gap / 5-truths-listicle / ...>
- **Payoff structure**: <timeline-promise / climax-position / authority-anchor / outfit-change-time-jump / ...>
- **CTA structure**: <comment-keyword "<word>" / comment+follow / link-in-bio / DM-trigger / ...>

### Angle / audience signal
- **Niche**: <belly-fat / ED / hair-regrowth / menopause / varicose-veins / etc.>
- **Primary audience**: <gender + age band>
- **Secondary audience**: <if any>
- **Symptom / aspiration**: <what the viewer wants to fix or gain>
- **Emotional register**: <warm / fierce / clinical / desperate / hopeful / curious / ...>

### Persona archetype + setting tier
- **Persona archetype**: <modern-clinic-doctor / holistic-healer / old-grandma / sexy-doctor / rastafarian-uncle / ...>
- **Setting tier**: <Tier-0 selfie-arm / Tier-1 single-setting / Tier-2 multi-setting>
- **Specific settings used**: <list>

---

## Veo 3.1 Final Prompts (per clip)

### Clip 1.1 — Scene 1, Line 1 (<BLOCK_TAG>)
**Start frame:** Image 1
**Text prompt:**
```
<Cinematography per v585: "Static handheld camera, no camera move, slight natural drift." or named move>

<Action narrative — three timed beats: 0-2s / 3-5s / 5-8s with absolute-magnitude language for state-evolution clips>

He/She says with <register>: <verbatim dialogue>.

Ambient: <setting tone + sound cues>.
(no subtitles, no captions)
```
**Negative prompt:**
```
no montage, no cutaways, no scene cuts, no flashbacks, no emotional escalation, no cinematic transitions, no burnt-in text, no captions, no on-screen titles, no face distortion, no morphing, no warping, no duplicate limbs, no extra fingers, no inconsistent lighting, no composite split-screen layouts, no disembodied hands.
```

### Clip 2.1 — Scene 2, Line 1 (<BLOCK_TAG>)
**Start frame:** Image 2
**Text prompt:**
```
<Cinematography>

<Action narrative — three timed beats>

He/She says with <register>: <verbatim dialogue>.

Ambient: <setting tone + sound cues>.
(no subtitles, no captions)
```
**Negative prompt:**
```
no montage, no cutaways, no scene cuts, no flashbacks, no emotional escalation, no cinematic transitions, no burnt-in text, no captions, no on-screen titles, no face distortion, no morphing, no warping, no duplicate limbs, no extra fingers, no inconsistent lighting, no composite split-screen layouts, no disembodied hands.
```

(Repeat one Clip block per `- **line:**` in the Storyboard section.)
````

---

## Hard rules

1. **Dialogue verbatim** — no paraphrase. Audio track is authoritative.
2. **v553.1: persona NEVER inline-described** in image-prompt body — referenced as "the main character"; identity comes from the uploaded reference image.
3. **Object positions explicit** — "the bottle on the desk lower-left", "the anatomy poster behind at jaw height" — NOT "in the background".
4. **Foreshortening notes** when wide-angle: "the banana is closest to the wide-angle lens, foreshortened larger as the foreground anchor".
5. **Crop boundaries explicit** — "cropped at mid-thigh, NO floor visible, NO feet visible".
6. **Lighting direction** — name the light source position: "vibrant natural HDR daylight from a glass-door window in the right side of frame".
7. **v589 absolute-magnitude** when source shows COMPLETE state change: use "completely melts away", "fully revealed", "entirely dissolves". FORBIDDEN: "dramatically", "mostly", "almost", "largely". Reserved only for genuinely partial states.
8. **Three timed beats** in every action_note with explicit `[Start beat 0-2s] / [Mid-clip beat 3-5s] / [End beat 5-8s]`.
9. **Verbs of state change** — when dialogue or action contains pour/squeeze/add/stir/mix/melt/dissolve/spread/press/pull/crack, the clip almost certainly has visible state evolution → emit start AND end image frames + Veo First/Last-Frame mode.
10. **Camera-move classification** — every action_note opens with v585 classification: static handheld / static-handheld-with-drift / push-in / pull-back / pan-left / pan-right / tilt-up / tilt-down (with magnitude when observable).
11. **v577 line word budget**: each `- **line:**` ≤21 words ±2. If a scene's total dialogue >23w, split at clause/comma boundaries into multi-line scenes — each split ≥10w + syntactically complete.

---

## Self-validation

Before finalizing: pick one Veo Final Prompt from your output. Could that prompt re-render the source clip closely if fed to Veo 3.1? If not, the description grammar (v586) or action_note discipline (v540 motion-only / v589 absolute-magnitude) was insufficient — go back and tighten.

---

## Output

Just the markdown. Plain. The output:
- Starts with `<!--` (HTML comment header).
- Ends with the last clip's negative-prompt closing ` ``` ` fence.
- NO outer code-fence wrapper. NO ` ```markdown ` tag. NO prose preamble. NO "Here is the decoded markdown:" line. NO trailing commentary.
- All internal fences are bare three-backticks ` ``` ` (no language tag).
- All `### Image N` and `### Scene N` headings use literal capital "Image" / "Scene" + integer.
- All references use lowercase + underscore: `image_1`, `image_2`, etc.
