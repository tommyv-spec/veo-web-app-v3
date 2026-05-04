# Gemini decode prompt — v587/v588/v589 video reverse-engineering

**Usage:** in Gemini (gemini.google.com or any Gemini-API client), upload the source MP4 and paste this entire file as the prompt. Gemini natively samples video at 1fps + audio + per-second timestamps.

---

## Role

You are a video-understanding assistant for a viral-ad reverse-engineering pipeline. Your output drives generative-video re-rendering (Veo 3.1) and image generation (Nano Banana 2). The decoded markdown becomes a **reproduction-ready artifact**: feeding any clip's prompt back to Veo 3.1 should re-render the source clip closely.

## Input

I'm uploading a viral short-form video (typically 30-90s, 9:16). Decode it under the v589 schema below.

## Output structure

Produce ONE markdown file with these sections in order. NO prose preamble, NO code-fence wrappers.

```
<!-- HTML header with pipeline audit trail (v579 + v585 + v586 + v587 + v588 + v589) -->
<!-- + STRUCTURE summary: N scenes, M clips, total seconds -->
<!-- + RULE-VARIANT NOTES: which v-rules apply, which are NOT APPLICABLE -->

**Video:** <one-line title>
**Persona:** <archetype>
**Setting:** <Tier 0/1/2 — single or multi>
**Duration:** <Xs source / ~Ys at v577 pacing>
**Structure:** <HOOK N / RECIPE N / EXPLAIN N / AUTHORITY N / PRODUCT N / CTA N>

## Ingredients
| Name (used in prompts) | Type | Description | Source |
| `the main character` | character | <archetype + ethnicity + age band — NEVER inline-described in image prompts per v553.1> | External persona upload — Flow slot 0 |
| `the [product name]` | product | <only if branded product visible> | External product upload — Flow slot 1 |

## Images
### Image N
- **reference_image:** image_K | none
- **product_image:** the [product name]   <!-- only on images that bind product upload -->
- **Image prompt:**
` ` `
Use the uploaded character reference image for the main character — match identity, hair, clothing exactly.
Use the uploaded product reference image for [product name] — match label, packaging exactly.   <!-- only when product_image set -->
Use Image K as the visual reference for the previous scene — preserve <setting>, <lighting>, <anchor props>, continuity from there.   <!-- only when reference_image is not none -->

[v586 six-block walk: Subject (pose/eye-direction/mouth/expression) — Composition (frame partition / depth layers / crop / foreshortening / single-vs-two-shot) — Action (current gesture / hand positions / eye tracking) — Location (every prop with EXPLICIT position) — Style (lighting direction / palette / mood) — Tech (camera type / distance / focus depth / motion blur)]
` ` `

## Storyboard
### Scene N
- **image:** image_K
- **clip_mode:** fresh | continue | blend
- **transition:** cut | blend | null
- **visual register:** <HOOK / RECIPE / EXPLAIN / AUTHORITY / PRODUCT / CTA>
- **rhythm tier:** <descriptor> (Xw)
- **speaker:** on-camera | voiceover
- **line:** <verbatim dialogue from audio, ≤21w per v577; split at clause boundaries when scene total >23w>
- **action_note:** [v586 5-block: Cinematography (camera-move classification — static handheld / push-in / pull-back / pan / tilt) — Subject (every entity that moves) — Action ([Start beat 0-2s] X / [Mid-clip beat 3-5s] Y / [End beat 5-8s] Z) — Context (anchor-prop carry-over from start frame) — Style & Ambiance ([register tag] + ambient sound cues)]

## Comprehension
### Structural inventory
- Total: N scenes, M clips, ~Ts
- Per-scene block tags

### v-rule inventory
| v-rule | Status | How this video uses it |
| v539 HOOK weird-action | applied — variant: <force-verb / clinical-markup / diagnostic-press / symptom-curiosity / banana-pun / fat-melt / NEW: ___> | <description> |
| v541 outfit-change | applied / NOT APPLICABLE | <Day-1→Day-14 swap or single-day> |
| v580 recipe state-evolution | applied / partial / NOT APPLICABLE | <each step own image vs collapsed> |
| v585 motion capture | applied | <camera-move classification per shot> |
| v586 description grammar parity | applied | <six-block walk per image, five-block per action_note> |
| v587 reproduction-ready artifact | applied | <Comprehension + Veo Final Prompts both emitted> |
| v588 dense per-shot frame sampling | applied | <start/mid/end + 5+ dense frames when shot >3s OR dialogue contains pour/squeeze/melt/etc.> |
| v589 absolute-magnitude grammar | applied | <COMPLETE/PARTIAL/MINIMAL per state-evolution clip> |

### Rhetorical structure
- HOOK type, Frame, Payoff structure, CTA structure

### Angle / audience signal
- Niche, primary audience (gender + age), secondary, symptom/aspiration, emotional register

### Persona archetype + setting tier

## Veo 3.1 Final Prompts (per clip)

### Clip N.M — Scene N, Line M (<block tag>)
**Mode:** Veo 3.1 First/Last-Frame   <!-- only when has_state_evolution=true -->
**Start frame:** Image N
**End frame:** Image N_end   <!-- only when has_state_evolution=true -->
**Text prompt:**
` ` `
[Cinematography per v585]

[Action narrative — three timed beats: 0-2s / 3-5s / 5-8s with absolute-magnitude language]

He/She says with [register]: <verbatim dialogue>.

Ambient: <setting tone + sound cues>.
(no subtitles, no captions)
` ` `
**Negative prompt:**
` ` `
no montage, no cutaways, no scene cuts, no flashbacks, no emotional escalation, no cinematic transitions, no burnt-in text, no captions, no on-screen titles, no face distortion, no morphing, no warping, no duplicate limbs, no extra fingers, no inconsistent lighting, no composite split-screen layouts, no disembodied hands.
[+ source-specific bans like "no second person in frame" or "no partial fat removal"]
` ` `
```

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

## Self-validation

Before finalizing: pick one Veo Final Prompt from your output. Could that prompt re-render the source clip closely if fed to Veo 3.1? If not, the description grammar (v586) or action_note discipline (v540 motion-only / v589 absolute-magnitude) was insufficient — go back and tighten.

## Output

Just the markdown. No preamble, no commentary, no code fences wrapping the whole thing.
