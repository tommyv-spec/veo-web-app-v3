# ChatGPT image worker — operator runbook

## What it is

A local image-generation backend that drives ChatGPT in a real browser (Patchright) to make images. Use it when Banana 2 or the paid image API **refuse** a prompt — ChatGPT's consumer content filter is more lenient and passes prompts (ED / banana-proxy style) the others reject. It runs **only on the operator's machine** with a logged-in ChatGPT session. It NEVER runs on Render.

Files:
- `static/chatgpt_image_worker.py` — the worker (CLI + `--watch` loop).
- `static/chatgpt_image_backend.py` — the browser drive core.
- `static/chatgpt_job_map.py` — pure platform-job → prompt mapping.

## One-time setup

1. Install Patchright:
   ```bash
   pip install patchright
   ```
2. Seed the session cookies. The login the worker actually uses is a set of **plaintext cookies** injected on every launch, captured by a **netlog capture** step: a real Chrome (your Default profile, already logged into ChatGPT) is launched with `--log-net-log --net-log-capture-mode=IncludeSensitive`, which writes the *decrypted* Cookie headers to a log; those are parsed and saved to `static/.chatgpt_cookies.json` (gitignored). This is **ABE-immune** — it works even though Chrome's App-Bound Encryption makes copied cookie files undecryptable.

   Today this capture is run as a **separate step** (a netlog-capture script), not a worker subcommand. A `--refresh-cookies` subcommand that runs the same capture is planned (see "When the session expires"). Chrome must be fully closed while the capture runs.

   (`python code/static/chatgpt_image_worker.py --login` is a *different*, optional path — it opens the dedicated `.chatgpt_profile` for a manual interactive login and persists profile cookies there. The `--watch`/gen path relies on `.chatgpt_cookies.json`, not the dedicated profile, so the netlog capture above is the one that matters.)

## Serving platform jobs

Run the watcher alongside your local image platform. It claims ONLY `model=="chatgpt"` jobs, generates ONE image per job (variants clamped to 1), writes `variant_1.png` into the job's `output_dir`, and drops a `node_<id>.done.json` marker the platform polls.

```bash
python code/static/chatgpt_image_worker.py --watch
```

The Flow / Banana worker (`code/image_worker.py`) skips `model=="chatgpt"` jobs, so the two workers can run at the same time without stepping on each other.

## Selecting it in the UI

In the image **Model** dropdown pick **"ChatGPT (web · lenient filter)"**. That sets the node's `model="chatgpt"`, which routes the job JSON to `DATA_DIR/_image_jobs/` where `--watch` picks it up.

## Standalone use (no platform)

Single image:
```bash
python code/static/chatgpt_image_worker.py --ref ref.png --prompt "your prompt" --out result.png
```

Batch from a jobs file:
```bash
python code/static/chatgpt_image_worker.py --jobs jobs.json
```

## When the session expires

ChatGPT rotates the session token, so the saved cookies expire (days to weeks). Symptom: jobs fail with `session expired — run --refresh-cookies`.

To refresh **today**: fully close Chrome, then re-run the netlog cookie capture (same step as setup) to rewrite `static/.chatgpt_cookies.json`. The capture relaunches your Default profile briefly, reads the decrypted ChatGPT cookies, and saves them.

> TODO: a dedicated `--refresh-cookies` subcommand that wraps this netlog capture is planned but NOT yet built. Until it lands, run the capture step directly.

## Failure behavior

On timeout, refusal, expired session, or selector drift the worker writes `.done.json` with `status="failed"` and the error. The platform marks the node **failed**. There is **no auto-fallback** — regenerate that node with another backend (Banana).

## Safety

- **LOCAL-ONLY.** Needs your Chrome + logged-in ChatGPT session. NEVER runs on Render.
- Session files (`static/.chatgpt_profile/`, `static/.chatgpt_cookies.json`) are gitignored in `code/.gitignore`. **Never commit them.**
