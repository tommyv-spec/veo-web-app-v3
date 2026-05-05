# Decode prompt — v595 LLM-agnostic invocation wrapper

**Thin wrapper.** This file is *only* the invocation instructions. The actual decode rules + output skeleton live ONCE in `code/template_new_format.md` (skeleton) + `code/template_reference.md` (rules). Do not duplicate them here.

> **Filename note**: keeps the historical name `gemini_decode_prompt.md` for back-compat, but per v595 this prompt works with **any vision-capable LLM that satisfies the Stage 4d interface contract**. Not Gemini-specific.

---

## Single source of truth (do NOT duplicate)

| File | Role | What it contains |
|---|---|---|
| `code/template_new_format.md` | THE SKELETON | Fill-in scaffold; canonical output shape; parser-readable grammar conventions |
| `code/template_reference.md` | THE BIBLE — single source of truth for v-rules | Every v-rule's full deep-dive (v521.1 → v595); v579 pipeline spec; legacy format history |
| `wiki/meta/decode-grammar-checklist.md` | THE WORKFLOW | Operator-facing process docs; references `template_reference.md` for rule deep-dives |
| `wiki/patterns/conventions.md` | THE INDEX | One row per v-rule; each row links to the deep-dive in `template_reference.md` |
| THIS FILE | THE INVOCATION | How to invoke a decode using an LLM. References the above. |

When v596+ ships, the deep-dive lands ONLY in `template_reference.md`. The other files reference it; this file references it via the operator instructions below.

---

## How to invoke a decode (by provider per v595)

### Provider 1 — Claude in-session (DEFAULT for any decode inside Claude Code)

No API key, no install. Claude has Read access to all files.

```
Operator says: "decode this video: <path>"

Claude:
1. Reads code/template_new_format.md       (the output skeleton)
2. Reads code/template_reference.md        (the v-rule deep-dives)
3. Reads wiki/meta/decode-grammar-checklist.md (the workflow procedure)
4. Runs the v579 pipeline (Stages 1-3 via the existing scripts in _decode_tmp/<prior-decode>/):
   - ffmpeg audio extraction
   - faster-whisper transcription
   - PySceneDetect AdaptiveDetector + Farneback motion classification
   - manifest.json build
   - Stage 4: dense per-shot frame extraction (v588) into _decode_tmp/<source-id>/frames/
5. Performs Stage 4d (v589 Half A) by walking dense frames via the Read tool
   on _decode_tmp/<source-id>/frames/shotNN_<label>_<t>s.png
6. Applies v594 image consolidation: groups shots by composition; emits M images for N shots
7. Authors raw/decoded_<source-id>.md following the template_new_format.md schema
8. Moves artifacts to raw/decode_artifacts/<source-id>/
9. Authors wiki/scripts/<persona>/<source-id>.md summary + bidirectional ## Used in footers
10. Updates wiki/scripts/_index.md count + wiki/log.md entry + wiki/meta/_index.md
```

### Provider 2 — LM Studio (local, free)

Install LM Studio, load a vision-capable GGUF (e.g. `gemma-4-E2B-it-GGUF` with mmproj), enable the local server (Developer tab → Start Server, default port 1234).

```bash
python code/v589_video_understanding.py path/to/source.mp4 --provider lmstudio
```

The script auto-detects via `GET http://localhost:1234/v1/models` + sends frames + transcript via OpenAI-compatible API. Outputs `_decode_tmp/<source-id>/stage4d.json`.

The Claude-in-session operator (or a downstream script) then reads `stage4d.json` + applies v594 consolidation + authors the decoded markdown per `template_new_format.md`.

### Provider 3 — Gemini API (paid, best for motion-heavy / multi-character)

```bash
export GEMINI_API_KEY=<key>
python code/v589_video_understanding.py path/to/source.mp4 --provider gemini
```

Uploads MP4 natively at 1fps + audio + per-second timestamps. Outputs `_decode_tmp/<source-id>/stage4d.json`. Same downstream flow.

### Providers 4-7 — OpenAI / Anthropic API direct / Ollama / OpenRouter

Same pattern with different `--provider` flag. Each provider only needs to satisfy the v595 input/output contract (see `template_reference.md` §"LLM-agnostic Stage 4d decode interface (v595)" for the schema).

```bash
python code/v589_video_understanding.py path/to/source.mp4 --provider openai|anthropic|ollama|openrouter
```

### Provider 8 — Human-walk template (always-available fallback)

```bash
python code/v589_video_understanding.py path/to/source.mp4 --provider template
# writes _decode_tmp/<source-id>/stage4d.json with empty fields + frame paths
# operator (or Claude in-session) fills the JSON; same v589 schema applies
```

### Provider selection rule

```
if operating inside a Claude Code session:
    → Provider 1 (Claude in-session)  # default — no API key, full Read access
elif LM Studio local server is up at localhost:1234:
    → Provider 2 (LM Studio)
elif GEMINI_API_KEY is set:
    → Provider 3 (Gemini API)
elif OPENAI_API_KEY is set:
    → Provider 4 (GPT-4o-vision)
elif ANTHROPIC_API_KEY is set:
    → Provider 5 (Claude API direct)
elif Ollama is running locally:
    → Provider 6 (Ollama vision model)
elif operator has OpenRouter configured:
    → Provider 7 (OpenRouter)
else:
    → Provider 8 (human-walk template)
```

---

## When operating headless with Gemini / GPT-4o / Claude API direct

These providers don't have local file access — they only see the prompt content. To invoke them, the operator must concatenate this wrapper + the relevant template files at invocation time:

```bash
# Build the headless prompt by concatenating the canonical sources
cat code/gemini_decode_prompt.md \
    code/template_new_format.md \
    code/template_reference.md \
    > /tmp/decode_prompt_headless.md

# Then upload the source MP4 + paste /tmp/decode_prompt_headless.md as the prompt
```

The headless LLM then sees:
- (this wrapper's) operator instructions
- (template_new_format.md's) canonical output skeleton
- (template_reference.md's) v-rule deep-dives

The single source of truth stays in the templates; this wrapper just orchestrates the concatenation.

---

## What every decode produces

Per `template_new_format.md` (the skeleton) + `template_reference.md` (the rules):

- `raw/decoded_<source-id>.md` — the v521.1→v595-compliant decoded artifact
- `raw/decode_artifacts/<source-id>/manifest.json` — N shots × dialogue × motion
- `raw/decode_artifacts/<source-id>/transcript.json` — faster-whisper segments
- `raw/decode_artifacts/<source-id>/shots.json` — PySceneDetect AdaptiveDetector
- `raw/decode_artifacts/<source-id>/motion.json` — Farneback per v585
- `raw/decode_artifacts/<source-id>/stage4d.json` — v589 Half A VLM output
- `raw/<source-id>.mp4` — source archived
- `wiki/scripts/<persona>/<source-id>.md` — wiki summary
- `wiki/scripts/<persona>/_index.md` — persona folder (if new persona)
- `wiki/log.md` — decode entry
- `wiki/meta/_index.md` — chronological index entry
- `wiki/scripts/_index.md` — count incremented
- Bidirectional `## Used in` footer on the decoded markdown
- v594 image cardinality applied (M ≤ N — shots cluster into compositions)

---

## What this file is NOT

- NOT the v-rule deep-dives (those live in `code/template_reference.md`)
- NOT the output skeleton (that lives in `code/template_new_format.md`)
- NOT the workflow checklist (that lives in `wiki/meta/decode-grammar-checklist.md`)
- NOT the v-rule index (that lives in `wiki/patterns/conventions.md`)
- NOT a substitute for reading the canonical sources — the LLM (whether Claude in-session or headless) must consult `template_new_format.md` + `template_reference.md` to produce a compliant artifact

---

## Common operator commands

```
"decode this video: <path>"
  → Provider 1 (Claude in-session): Claude reads templates, runs pipeline, walks frames, authors markdown

"recreate this decoded video as a Korella ad"
  → Phase 4-5 of viral-video-pipeline.md: read decoded artifact + apply v590 chain optionality
    + v594 image cardinality + v591 novelty-gate + v592 motion-text-match + risky-vocabulary
    policy-flag pass + psychology-of-conversion authoring → write videos/<name>.md
```

Both flows defer to `template_new_format.md` + `template_reference.md` for the schema and rules.

---

## Updates

When a new v-rule (v596+) lands:
1. Add the deep-dive to `code/template_reference.md` (the bible)
2. Add a row to `wiki/patterns/conventions.md` (the index — one-line summary + cross-link, no duplication)
3. Add a brief workflow note to `wiki/meta/decode-grammar-checklist.md` or `wiki/meta/generate-video-checklist.md` (which side it applies to)
4. Add a quickref paragraph to `CLAUDE.md` (session-start gotcha) IF the rule is critical-or-easy-to-violate
5. **Do NOT update this file** unless the invocation flow itself changes (new provider, new operator command, etc.)

The deduplication architecture: rule deep-dives live ONCE in `template_reference.md`; the index points to them; this wrapper points to the index. When the rule changes, only `template_reference.md` needs editing (+ a one-line index update).
