# code/ — Platform engineering scope

Auto-loaded when working in the `code/` submodule. Project-root `CLAUDE.md` covers always-on behavior rules; this file covers platform-specific discipline.

---

## Canonical homes (single source of truth)

| File | Role |
|---|---|
| `code/template_reference.md` | **THE RULES.** Every v-rule deep-dive (v521.1 → latest). 14000+ lines. Always check here BEFORE inventing a new rule. |
| `code/template_new_format.md` | **THE SKELETON.** Output shape, parser-readable grammar. Shared by decode + generate (same `### Image N` / `### Scene N` parser). |

**When adding a new v-rule (vNNN)**:
1. Deep-dive → `code/template_reference.md` (canonical home)
2. Index entry → `wiki/patterns/conventions.md` (one-line row + cross-link)
3. Workflow note → `wiki/meta/decode-grammar-checklist.md` OR `wiki/meta/generate-video-checklist.md`
4. Quickref → root `CLAUDE.md` Critical-Gotcha table IF the rule is critical-or-easy-to-violate
5. Timeline → `wiki/log.md`
6. Skeleton update → `code/template_new_format.md` ONLY if rule changes output shape
7. Bundle scripts → propagate to all 4 (`decode/lift/innovate/create_bundle.sh`) for LLM-in-loop authoring

Deep-dive lives ONCE. Other files reference it.

---

## The 5 video workflows + their bundles

| Workflow | Trigger | Script | Output |
|---|---|---|---|
| **Decode** — observe source video | new mp4 lands in `raw/` | `./code/decode_bundle.sh <mp4>` | `raw/decoded_<id>.md` |
| **Lift (same niche)** — recreate from decoded source | decoded source + same niche | `./code/lift_bundle.sh <decoded.md>` | `videos/<name>.md` |
| **Innovate (cross niche)** — port outside-niche viral | decoded source + different niche | `./code/innovate_bundle.sh <decoded> "<cell-spec>"` | `videos/<name>.md` |
| **Create** — author from 0 | no source, fresh build | `./code/create_bundle.sh` | `videos/<name>.md` |
| **Edit** — modify existing `videos/*.md` | existing artifact needs surgical change | `./code/create_bundle.sh` + edit-mode task prompt | updated `videos/<name>.md` |

**No separate `edit_bundle.sh`** by design — the 17-file create-bundle already loads every rule needed; edit ops reuse the same bundle and route via task-prompt language. Keeps script count at 4.

Edit task-prompt templates (8 scopes) + decision tree + v-rule routing table + per-workflow when-to-use guide live at `wiki/meta/workflows.md`.

---

## Bundle invocation modes

**Mode 1 — LLM has web access**: paste GitHub raw URLs, LLM fetches itself.
**Mode 2 — LLM accepts file attachments**: attach bundle files separately (Claude API, Gemini API w/ Files, NotebookLM, custom GPTs).
**Mode 3 — LLM takes one prompt blob**: use bundle scripts above.

For **Claude in-session** (this Claude Code), equivalent is reading bundle files via Read tool at lift/decode time. Anti-pattern that lift_bundle.md prevents: name-dropping wiki pages from session memory without actually reading them. The bundle discipline forces every lift to re-read canonical sources.

---

## Platform format — videos/*.md (parser is strict)

The platform parser regexes are STRICT and silent on failure — bad headers don't error, they're skipped, and you get `Parse error: No scenes found in the markdown` at import.

**Hard rules** (`code/template_reference.md` has full deep-dive for each):

- `### Scene N` — integer + nothing else (v696 strict; suffixes rejected)
- `### Image N` — integer + OPTIONAL suffix per v718j.1 (`### Image K — Clip C.L START`)
- `- **image:** image_N` mandatory on every shot scene; ABSENT on text_card scenes
- `- **action_note:**` single-line prose with inline `[Start beat]` / `[Mid-clip beat]` / `[End beat]` markers
- `- **Image prompt:**` followed by fenced code block mandatory in every `### Image N` block
- v682d text_card scenes: NO `### Image N` header in `## Images`; ONLY `### Scene N` in `## Storyboard` with `scene_type: text_card`
- v682b on-camera dialogue: persona must be on-camera AND visibly lip-syncing for `- **line:**` bullet
- v693 line: field FULLY LOWERCASE
- v696 5 HARD-FAIL pre-output validation gates
- v698A voiceover-paired clip-pair contract
- v697 explicit force-verb action_arc on every shot scene
- v750/v751 Veo Final Prompts format (Clip N.M headers + bolded fields + NO beat brackets in Veo prompts)
- v718h-C Option C native end-frame interpolation via `- **end_frame_image:** image_K+1`
- v718i + v718i.1 + v718i.2 + v718i.3 platform code (frontend + backend + Flow-path plumbing)
- v718j paired-image identification: START image gets `- **pair_role:** start` ONLY (NO paired_with). END image gets `- **pair_role:** end` + `- **paired_with:** image_K` (back-ref to its START partner). Parser HARD-FAILS if `paired_with` appears on a non-`end` image (`image_platform.py` ~L3434: "START images do not carry paired_with").
- v718d.3 per-axis Pre-Flight Section 6 schema (all 4 axes individually for t=0 + t=end)
- v738 Pre-Flight Checklist + v738.1 + v738.3 normalization-bias countermeasures
- v752 INSTANT REACTION ON CONTACT catalyst pacing
- v580 / v580.2 / v580.3 / v580.4 image inheritance modes

**Pre-import verification** (always run before pushing):
```bash
python -c "import re; t=open('videos/<file>.md',encoding='utf-8').read(); print('Images:', len(re.findall(r'^###\\s+Image\\s+(\\d+)', t, re.MULTILINE)), 'Scenes:', len(re.findall(r'^###\\s+Scene\\s+(\\d+)\\s*\$', t, re.MULTILINE)))"
```

**Reference**: copy `videos/asian-elder-papaya-skin.md` scene-block schema verbatim. Full spec at `wiki/meta/generate-video-checklist.md` §"Platform-format constraints".

---

## Production deploy discipline

`code/` submodule auto-deploys to Render on every push to `main`. Every commit live in 2-3 min. Production = only environment.

**Hard rules:**
- NEVER claim "should work" without evidence — see root `CLAUDE.md` §"Verification before should work claims"
- Add temporary diagnostic log lines on runtime-affecting changes (`main.py` / `video_processor.py` / `image_platform.py` / `static/flow_worker.py`). Remove only after operator-side evidence lands.
- After every push to `code/` main → spawn `caveman:cavecrew-reviewer` on commit set; cheap insurance
- `py_compile` insufficient — `import <module>` before push
- Bumping submodule pointer in wiki repo captures version stamp (`git add code && git commit` from wiki root)

---

## Decode pipeline — Stage 4d VLM is LLM-agnostic (v595)

ANY vision-capable LLM that satisfies Stage 4d input/output contract is valid decode provider.

**Provider priority** (default to #1 inside Claude Code session):
1. **Claude in-session** (free for operator, Read tool's PNG image support) — DEFAULT for any decode run inside Claude Code
2. **LM Studio (local, free)** — vision-capable GGUF + local server at `:1234`
3. **Gemini API** (paid) — `GEMINI_API_KEY` set; native MP4 upload best for motion-heavy videos
4. **OpenAI GPT-4o-vision** (paid) — `OPENAI_API_KEY`
5. **Anthropic Claude API direct** (paid) — `ANTHROPIC_API_KEY`
6. **Ollama local vision model** (free) — `llava` / `llama3.2-vision`
7. **OpenRouter** (paid gateway)
8. **Human-walk template** (always-available fallback)

Stage 4d input/output contract + provider catalog at `wiki/meta/decode-grammar-checklist.md` §"Stage 4d LLM provider catalog (v595)". Unchanged regardless of provider: v578 whisper / v585 Farneback motion / v588 ffmpeg / v594 consolidation.

---

## Image cardinality — universal (v594)

**PySceneDetect shots ≠ images ≠ scenes.** PySceneDetect detects HISTOGRAM CUTS, not compositions.

| Cardinality | What it counts | Where it lives |
|---|---|---|
| PySceneDetect shots (N) | Histogram cuts | `manifest.json` |
| Distinct compositions (M, M ≤ N) | Setups producer actually filmed | `## Images` section — one `### Image M` per composition |
| Dialogue beats / clips (K) | Voiceover units / Veo render units | `## Storyboard` (generate) / `## Veo 3.1 Final Prompts` (decode) |

**Rule**: distinct composition OR state-evolution (recipe / Day1→14 / prop transformed) → new image; otherwise reuse via `- **image:** image_N` pointing to shared image.

**Typical cardinality**: 3-6 images for 8-12 shots/scenes (NOT 1:1). Talking-head + recipe-pivot videos consolidate hard.

Full decision rule + worked examples at `wiki/meta/decode-grammar-checklist.md` + `wiki/meta/generate-video-checklist.md` §"Image cardinality (v594)".

---

## Anti-patterns this discipline avoids

Pre-2026-05-05a anti-pattern: v591/v592/v593/v594/v595 had inline deep-dives in 4-5 wiki files; NONE in `template_reference.md`. Refactored: moved all deep-dives to canonical home, wiki files keep one-row index pointers only.

Pre-2026-05-05b: a third "wrapper" file `code/gemini_decode_prompt.md` existed as LLM invocation entry-point — duplicated rules already in `template_reference.md` + skeleton already in `template_new_format.md`. **Deleted** as redundant.

Pre-2026-05-18 anti-pattern: project-root CLAUDE.md grew to 506 lines / 350KB / 63 v-rule deep-dives DUPLICATING template_reference.md. **Refactored 2026-05-18 late**: lean root + scoped `code/CLAUDE.md` + `wiki/CLAUDE.md`. Deep-dives stay canonical in `template_reference.md`; quickrefs in root `CLAUDE.md` are 1-line + link.
