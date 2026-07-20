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
2. Seed the session cookies. Run the netlog cookie capture (ABE-immune — grabs plaintext cookies from the network log) and log in manually to the dedicated ChatGPT profile when prompted. This writes `static/.chatgpt_cookies.json` (gitignored).
   ```bash
   python code/static/chatgpt_image_worker.py --login
   ```
   The cookies let the worker reuse your logged-in ChatGPT session without re-typing credentials each run.

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

To refresh **today**: re-run the netlog cookie capture (same step as setup) to rewrite `static/.chatgpt_cookies.json`:
```bash
python code/static/chatgpt_image_worker.py --login
```

> TODO: a dedicated `--refresh-cookies` subcommand is planned but NOT yet built. Until it lands, refresh by re-running the netlog capture above.

## Failure behavior

On timeout, refusal, expired session, or selector drift the worker writes `.done.json` with `status="failed"` and the error. The platform marks the node **failed**. There is **no auto-fallback** — regenerate that node with another backend (Banana).

## Safety

- **LOCAL-ONLY.** Needs your Chrome + logged-in ChatGPT session. NEVER runs on Render.
- Session files (`static/.chatgpt_profile/`, `static/.chatgpt_cookies.json`) are gitignored in `code/.gitignore`. **Never commit them.**
