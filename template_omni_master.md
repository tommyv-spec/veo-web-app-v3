# Google Omni master prompt (v865)

Canonical per-clip render-prompt body for Google Omni Flash. Fill the slot markers from the build's own fields. Deep-dive: code/template_reference.md §v865.

## Both sections are standard (v868)

Every build emits BOTH prompt sections: the Omni section (`## Google Omni Final Prompts (per clip)` — RENDERED by default) AND an anchor-format reference section (inert; the operator can select it to render per video via the Batch overview — batch `prompt_variant`). Deep-dive: code/template_reference.md §v868. Template for the anchor section:

```
## Anchor-Format Prompts (IMMEDIATE ACTION / TERMINAL STATE — reference, selectable)

The same clips in the prior anchor format. Bold `**Clip N.M**` labels (NOT `### Clip`) + a header with no "Final Prompts" token, so the render parser and clip-counter ignore it. The operator can select this set to render per video (batch prompt_variant, v868). Each clip: `**Clip N.M — …**` / `**Start frame:** Image K` / `**Text prompt:**` fenced (IMMEDIATE ACTION: … TERMINAL STATE: … + the v810 dialogue sentence) / `**Prompt B (…):**` fenced.
```

## Locked master (verbatim)

```
Create an 8-second vertical 9:16 realistic UGC video. Quality / Fidelity Lock: Use the exact same lighting, texture, and iPhone image quality as the reference image. Do not sharpen the footage. Do not enhance skin texture. Do not apply any AI beauty filter, skin smoothing, HDR effect, cinematic polish, artificial clarity, or stylized color grade. Keep the video raw, organic, and true to the reference image, with the same warm golden-hour lighting, natural softness, realistic exposure, imperfect smartphone texture, and authentic beach atmosphere. Reference: Use @image1 as the exact identity and scene reference for the older surfer man. Preserve his facial structure, skin tone, curly gray hair, facial lines, sunglasses, tattoos, lean surfer build, age, and overall likeness exactly. Keep him shirtless, wearing olive/green shorts, seated in the same wooden beach chair with relaxed older-surfer energy. Preserve the same rugged beach look and overall realism from the reference. Scene: Sandy golden-hour beach. Keep the environment consistent with @image1: beach sand, soft sunset light, black Jeep behind him, surfboard beside him, and subtle ocean/beach background. The setting should feel rugged, coastal, casual, and authentic. Out in the water, about 300-400 feet from shore, there is a massive ultra-luxury superyacht anchored offshore. It should feel like an absurdly expensive 100-million-dollar yacht: very large, striking, and clearly luxurious, but still realistically distant in the background until the camera pans to it. Camera: Static iPhone perspective at first, vertical 9:16, eye-level medium shot. Match the framing closely to @image1: seated pose, upper body, crossed legs, chair, and beach setting visible. One continuous take. Locked-off phone camera feel during the main line, with only very subtle natural micro-movement. No cuts, no zooms, no whip pans, and no cinematic movement during the first part of the shot. Ending Camera Beat: Immediately after he says the line, the camera naturally pans away from him toward the ocean, like the filmer is following his comment and revealing what he is talking about. The pan should feel like a real iPhone operator casually turning the phone, not a polished cinematic move. The camera lands on a huge ultra-luxury yacht sitting 300-400 feet out from shore in the water. Hold on the yacht briefly at the end so the joke lands. The yacht should look massive, expensive, and unmistakably high-end even at that distance. Performance / Action: The surfer stays seated in the chair, relaxed and casual. He looks toward the camera and delivers the line with a laid-back, matter-of-fact surfer attitude, like he is casually mentioning something ridiculous. His expression should feel amused and cool, not overly acted. After he says the line, he can subtly glance or gesture toward the water as the camera pans away to reveal the yacht. Natural blinking, subtle head movement, and believable mouth movement. The overall performance should feel candid and authentic. Voice: Give him a rough, gravelly older-surfer voice. His voice should sound sun-weathered, raspy, slightly low, lived-in, and a little gritty, while still staying clear and understandable. Delivery should feel calm, casual, amused, and unpolished, not acted or announcer-like. Dialogue: He says exactly, with accurate lip sync and natural pacing: "I gotta get back to my boat." Timing / Performance Beat: He delivers: "I gotta get back to my boat." Right after the line, the camera naturally pans away from him toward the water and reveals the enormous yacht offshore. Audio / Environment Sound: Audio should sound like realistic iPhone-recorded beach audio. Include soft ocean waves in the distance, light coastal breeze, subtle open-air spaciousness, faint sand/beach ambience, and natural outdoor sound. His voice should sound close and real to the phone microphone, not studio-polished. As the camera pans toward the ocean, the ambient beach and water sound should remain consistent and natural. Avoid city noise, traffic, indoor room tone, crowd noise, fake cinematic sound design, or overly clean podcast-style audio. Style: Authentic TikTok/Reels UGC. Raw smartphone footage. Natural skin texture. Slightly imperfect realism. Believable candid beach moment with a funny luxury reveal at the end. Negative Constraints: No text overlays. No captions. No subtitles. No logos. No VFX. No 3D. No cartoon. No beauty filter. No face morphing. No identity drift. No flicker. No jitter. No warped hands. No extra fingers. Do not change his outfit, sunglasses, tattoos, chair, Jeep, surfboard, or overall beach layout. Avoid excessive camera movement except for the natural end pan toward the water to reveal the yacht.
```

## The v865 per-clip block

**Fencing is mandatory.** Both the Prompt A body and the Prompt B body sit inside a triple-backtick fence, exactly like every shipped build. The parser extracts Prompt B with `_extract_fenced_content` only — it has NO unfenced fallback (`code/veo_prompt_overrides.py:396`), so an unfenced Prompt B parses to `None`, the v821 gate hard-fails, and the worker never gets the fallback line. The `**Start frame:**` / `**End frame:**` / `**Text prompt:**` / `**Prompt B …:**` labels stay OUTSIDE the fence; the prose body goes INSIDE it.

**Two literal strings the auditor hard-checks in the `Dialogue:` block — do not reword them away** (`audit_build.py` `c_v810_form`): every dialogue clip body must contain `saying exactly:` AND `stays silent for the rest of the clip`. The block below carries both; keep them verbatim when you fill the slot.

(The example below is wrapped in a 4-backtick outer fence only so the inner 3-backtick fences render literally. In a real build you write plain 3-backtick fences.)

````markdown
### Clip N.M — Scene N, Line M (REGISTER_LABEL)
**Start frame:** Image K
**End frame:** Image K+1
**Text prompt:**
```
Create an 8-second vertical 9:16 realistic UGC video.

Quality / Fidelity Lock: Use the exact same lighting, texture, and iPhone image quality as the attached start-frame image. Do not sharpen the footage. Do not enhance skin texture. Do not apply any AI beauty filter, skin smoothing, HDR effect, cinematic polish, artificial clarity, or stylized color grade. Keep the video raw, organic, and true to the reference image, with the same {{LIGHTING}}, natural softness, realistic exposure, imperfect smartphone texture, and authentic {{SETTING_ATMOSPHERE}}.

Reference: Use the attached start-frame image as the exact identity and scene reference for {{SUBJECT_NAME_AND_ROLE}}. Preserve {{IDENTITY_FEATURES}} exactly. Keep {{WARDROBE_AND_POSITION}}. Preserve the same look and overall realism from the reference.

Scene: {{SETTING_SENTENCE}}. Keep the environment consistent with the start frame: {{PROPS_AND_BACKGROUND}}.

Camera: {{CAMERA_MODE}} iPhone perspective, vertical 9:16, {{SHOT_SIZE}}. Match the framing closely to the start frame: {{WHAT_STAYS_VISIBLE}}. One continuous take. {{MOVEMENT_DISCIPLINE}}.

Ending Camera Beat: {{END_BEAT_OR_NONE}}

Performance / Action: {{ACTION_PROSE}}. Natural blinking, subtle head movement, and believable mouth movement. The performance feels candid and authentic.

Voice: {{VOICE_TEXTURE}}. Delivery is {{DELIVERY_REGISTER}}, unpolished, not acted or announcer-like.

Dialogue: The main AI generated character speaks clearly in a {{REGISTER}} American accent, saying exactly: "{{LINE_LOWERCASE}}" then stops speaking and stays silent for the rest of the clip, holding the final expression.

Audio: {{AUDIO_MODE}}. {{MIC_PROXIMITY}}.

Style: Authentic TikTok/Reels UGC. Raw smartphone footage. Natural skin texture. Slightly imperfect realism.

Negative Constraints: No text overlays. No captions. No subtitles. No logos. No VFX. No 3D. No cartoon. No beauty filter. No face morphing. No identity drift. No flicker. No jitter. No warped hands. No extra fingers. Do not change {{LOCKED_WARDROBE_PROPS_LAYOUT}}. Avoid excessive camera movement{{EXCEPT_END_BEAT}}.
```
**Prompt B (policy fallback — Prompt A with the spoken line reworded, v821):**
```
[Prompt A body byte-identical, EXCEPT the text inside the quotes on the Dialogue line]
```
````

## Fill map

| Omni block | Filled from |
|---|---|
| `{{LIGHTING}}`, `{{SETTING_ATMOSPHERE}}`, `{{PROPS_AND_BACKGROUND}}` | that clip's `### Image K` Image prompt |
| `{{SUBJECT_NAME_AND_ROLE}}`, `{{IDENTITY_FEATURES}}`, `{{WARDROBE_AND_POSITION}}` | the cast row + `### Image K` |
| `{{SETTING_SENTENCE}}` | that clip's `### Image K` Image prompt (scene description sentence) |
| `{{CAMERA_MODE}}`, `{{SHOT_SIZE}}`, `{{WHAT_STAYS_VISIBLE}}` | `### Image K` framing fields |
| `{{MOVEMENT_DISCIPLINE}}` | scene composition; static clips say `Locked-off phone camera feel with only very subtle natural micro-movement. No cuts, no zooms, no whip pans` |
| `{{ACTION_PROSE}}` | Storyboard `- **action_note:**` start + mid beats, brackets stripped |
| `{{END_BEAT_OR_NONE}}` | `- **action_note:**` end beat; `None — the framing holds for the whole clip.` when static |
| `{{REGISTER}}`, `{{VOICE_TEXTURE}}`, `{{DELIVERY_REGISTER}}` | persona voice card |
| `{{LINE_LOWERCASE}}` | `- **line:**` verbatim, lowercase (v693) |
| `{{AUDIO_MODE}}` | `no music, no background noise (clean isolated voice)` |
| `{{MIC_PROXIMITY}}` | `The voice sounds close and real to the phone microphone, not studio-polished` |
| `{{LOCKED_WARDROBE_PROPS_LAYOUT}}` | the wardrobe/prop nouns from `### Image K` that must not drift |
| `{{EXCEPT_END_BEAT}}` | ` except the natural end pan described above` when there is an end beat; empty string when static |

## Intel changelog

Every future operator intel drop appends a dated row here.

- 2026-07-24 — v865 established from operator master prompt. Anchors dropped, negatives kept as prose, both section headers accepted.
