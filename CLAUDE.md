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

- **v618b ingredient upload binding** — `type=character` / `type=product` mean **"HAS AN UPLOAD"**, not "speaks". Any character/product row with a NON-EMPTY Source cell is read as a declared Reference that must resolve to a real upload, and import HARD-FAILS if it does not (`(no upload)` is a non-empty string and still fails). A speaking non-persona with no upload = `type=extra` + Source `inline` + `speaker: on-camera`. Only the persona + branded product are ever uploaded (v573). Veo corollary: an extra's clip says "The man speaks…", never the v665 "The main AI generated character" (that binds the PERSONA upload → renders the persona's face on the extra).
- **v791.3 struggle-is-the-focus** — on an incident/daily-struggle hook the struggling person is the frame's hero, dead-centre and close, **even when a second person holds the selfie camera**; the camera-holder never takes the foreground.
- **chain depth ≤3 (`feedback_chain-depth-cap-describe-state`)** — a same-setting run of images anchors every frame to the run's FIRST frame (depth-1) and describes the changing state in the prompt; NEVER chain `5→6→7→8→…` frame-to-frame (generation loss compounds each hop). `reference_image: none` only at a real setting boundary. Auditor `reference_chain_depth` computes the hop distance and hard-FAILs past 3 — do not self-declare compliance in prose.

**Run BOTH linters before every import — they check different things:**
```bash
python code/verify_video_format.py videos/<file>.md        # platform IMPORT gates (v698A/v738/v750/v808/role)
python ~/.claude/skills/build-video/audit_build.py videos/<file>.md   # authoring rules + bridges to the above
```
`audit_build.py` now shells out to `verify_video_format.py` (check `platform_import_gates`), so **auditor 0 FAIL implies the parser will accept it** — unless that check reports SKIP (linter not found), in which case run it by hand. Why: 2026-07-17 a build hit 47 PASS / 0 FAIL on the auditor alone and was still rejected at import (v698A Gate 9). `template_reference.md:14555` had already said to run the linter every time; prose did not hold, so it became a check.

**Pre-import verification** (always run before pushing):
```bash
python -c "import re; t=open('videos/<file>.md',encoding='utf-8').read(); print('Images:', len(re.findall(r'^###\\s+Image\\s+(\\d+)', t, re.MULTILINE)), 'Scenes:', len(re.findall(r'^###\\s+Scene\\s+(\\d+)\\s*\$', t, re.MULTILINE)))"
```

**Reference**: copy `videos/asian-elder-papaya-skin.md` scene-block schema verbatim. Full spec at `wiki/meta/generate-video-checklist.md` §"Platform-format constraints".

---

## Production deploy discipline

`code/` submodule auto-deploys to Render on every push to `main`. Every commit live in 2-3 min. Production = only environment.

**Hard rules:**
- Deploy only from a clean `main` checkout with `powershell -File deploy.ps1`. The script refuses branch drift, dirty files, force pushes, and unverified health.
- The installed `pre-push` hook is the last gate for every direct push to main. It fails closed if Python, the remote base, or the protected checker cannot be read.
- **Deploy gate (v898 split, 2026-08-03):** a candidate that does not descend from `origin/main` HARD-FAILS (stale-tree rewind — rebase, never ackable). Line edits on a descendant tree PASS with a printed REPLACEMENT ACCOUNTING (lost line → closest added line; `VANISHED` = nothing similar added) — **whoever deploys reads that accounting in the log; VANISHED lines must each be confirmed intentional.** Only whole-FILE deletions still need `python check_deploy_safety.py --ack` (scoped `.deploy_ack.json`, self-stales on any new commit). The rev-240 "Claude-side deploys must be line-additive" rule is RETIRED. NEVER `push --no-verify`, never contort code to fake zero loss.
- NEVER claim "should work" without evidence — see root `CLAUDE.md` §"Verification before should work claims"
- Add temporary diagnostic log lines on runtime-affecting changes (`main.py` / `video_processor.py` / `image_platform.py` / `static/flow_worker.py`). Remove only after operator-side evidence lands.
- After every push to `code/` main → spawn `caveman:cavecrew-reviewer` on commit set; cheap insurance
- `py_compile` insufficient — `import <module>` before push
- **READ THE PRODUCTION LOG FROM HERE (2026-08-19).** `RENDER_API_KEY` now lives in `~/veo-worker/.env` (outside the repo, never committed) **and** as a persistent Windows user env var, so every shell and session has it. Use **`python code/render_logs.py --text <needle>`** (`--services` lists them; `-n` sets the count; `--service` picks a non-default one). `--text` is Render's own server-side filter, so it searches the whole retained window, not just the tail. **Why it matters:** for a week every `[TEMP]` diagnostic this project shipped had no reader — "did it fire?" was unanswerable and fixes got called done on code reading alone. If a diagnostic exists, check it here before claiming anything.
- **Prefer DB state over logs when you can get it.** A log line proves code ran; a row proves the outcome. `python code/verify_v892_live.py` is the model: it reads the composite plate row and reports whether it has a prompt AND a start frame, with a distinct exit code for "no composite job exists yet" so an untested fix can never be mistaken for a passing one.
- **CHECK `running_jobs` BEFORE YOU PUSH (2026-08-25).** A push to `main` IS the deploy, and a deploy **restarts a server-side render from zero** — the auto-edit work dir falls back to `/tmp` instead of the persistent disk, so every cached stage is discarded. Operator decision 2026-08-25: that bug is **left unfixed** (`c9fd83b` will name the cause on the next server-side render; do NOT file it as solved). So the mitigation is procedural — `curl -s https://veo-web-app-v3.onrender.com/api/health` and confirm `"running_jobs": 0` before pushing. Deploying over someone's hour-long render costs more than your change is worth.
- **How to actually use `verify_deploy.py`** (measured 2026-08-25, correcting a session report that called it useless). It **fails closed** — run from the wiki root with no argument it printed `NOT CONFIRMED` and exited **1**. Two real traps: (1) stdout is **block-buffered when redirected or piped**, so a captured run shows *nothing at all* until the process ends — use `python -u` if you want progress; (2) with no sha it defaults to `HEAD` **of the current repo**, so from the wiki root it waits the full 480s on a wiki commit that can never appear as `render_commit`. **Always pass the `code` sha explicitly**; done that way it prints `DEPLOY CONFIRMED` and exits 0.
- Bumping submodule pointer in wiki repo captures version stamp (`git add code && git commit` from wiki root)

**Local workers (NOT on Render):**
- **ChatGPT image backend** — `python code/static/chatgpt_image_worker.py --watch` claims `model=chatgpt` image jobs (lenient consumer filter; passes prompts Banana 2 / paid API refuse). LOCAL-ONLY, never on Render; session files gitignored. Runbook: `code/static/CHATGPT_IMAGE_WORKER.md`.

---

## Decode pipeline — Stage 4d VLM is LLM-agnostic (v595)

ANY vision-capable LLM that satisfies Stage 4d input/output contract is valid decode provider.

**Provider priority** (operator 2026-08-12: "we don't need to read all the frames, that's why we had gemini implemented" — Gemini is now the DEFAULT decode reader; Claude in-session is the fallback, not the default):
1. **Gemini API** — DEFAULT for every Stage 4d read. `python code/v589_video_understanding.py <mp4> --provider gemini --thinking low` (add `--fps 4` on fast-cut sources). Accepts the decode-reel skill outputs DIRECTLY: auto-finds `hardcut*/clips.tsv` (converted in-loader), `<stem>.json` whisper transcript, computes Farneback motion inline when `motion.json` is absent, and reads `GEMINI_API_KEY` from the Windows USER registry when the shell didn't inherit it. Default `gemini-3.6-flash` (2.5-flash rejects NEW keys); per-call cost logged to `output/gemini_costs.jsonl`, report via `--costs`. **Known network trap:** Surfshark WireGuard MTU blackholes the MP4 upload (small GETs pass, upload dies with `httpx.RemoteProtocolError`) — fix is `netsh interface ipv4 set subinterface "SurfsharkWireGuard" mtu=1280 store=persistent` (elevated), per `surfshark-github-mtu-blackhole`.
2. **Claude in-session** (free for operator, Read tool image support) — FALLBACK when Gemini is unavailable; frames read count toward the coverage gate honestly
4. **OpenAI GPT-4o-vision** (paid) — `OPENAI_API_KEY`
5. **Anthropic Claude API direct** (paid) — `ANTHROPIC_API_KEY`
6. **Ollama local vision model** (free) — `llava` / `llama3.2-vision`
7. **OpenRouter** (paid gateway)
8. **Human-walk template** (always-available fallback)

**Output contract = `stage4d.v2`** (2026-08-11): top-level OBJECT (`schema_version` + `observed_people` + ordered `shots`), mandatory per-shot `forensic_perception` + `action_arc.kinematics`/`.morphology` + `motion_cross_check`; inputs require `motion.json` alongside `shots.json` + `transcript.json`. Validate ANY provider's saved artifact: `python code/v589_video_understanding.py --validate-stage4d <json> --shots <shots.json>`. Deep-dive + full schema: `code/template_reference.md` §v589 Half A + §v595; provider catalog quickref also at `wiki/meta/decode-grammar-checklist.md` §"Stage 4d LLM provider catalog (v595)". Unchanged regardless of provider: v578 whisper / v585 Farneback motion / v588 ffmpeg / v594 consolidation.

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
