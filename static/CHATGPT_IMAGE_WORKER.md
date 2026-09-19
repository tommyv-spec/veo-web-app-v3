# ChatGPT image worker — operator runbook

## What it is

A local image-generation backend that drives ChatGPT in a real browser
(Patchright). In the production route it is not a fallback or a model selected
for one node. It runs alongside the Banana worker for **every generated base,
chained and reference-dependent node**. A child becomes claimable only after its
parents have chosen variants, so ChatGPT receives the same resolved references
as Banana.

The worker runs only on the operator's machine with a logged-in ChatGPT session.
It never runs on Render.

Files:
- `static/chatgpt_image_worker.py` — the worker (CLI + `--watch` loop).
- `static/chatgpt_http_pull.py` — the production queue client.
- `static/chatgpt_image_backend.py` — the browser drive core.
- `static/chatgpt_job_map.py` — pure platform-job → prompt mapping.
- `static/browser_driver.py` — shared engine switch (same file flow_worker uses).

## Firefox mode (v899)

The worker runs on **Firefox (Camoufox 0.5.4 / FF152 + plain Playwright)** — the
exact stack that fixed the flow worker's reCAPTCHA class — with either:

```bash
python chatgpt_image_worker.py --firefox --api-url ... --api-key ...
# or, parity with the flow worker:
set BROWSER_MODE=firefox
```

Rules (all inherited from `browser_driver.py`, measured on the flow worker):
- **Strictly opt-in.** Unset/typo'd env stays on Chrome — a working Chrome worker
  can never migrate by accident.
- **Headless by default** (a minimized Firefox window cannot be driven at all).
  `set FIREFOX_HEADLESS=0` to see the window.
- **Own profile** — `.chatgpt_profile_firefox[_<email>]`. Chromium and Firefox
  profile formats are mutually unreadable; the Chrome profile is never touched,
  and the per-account cleanup keeps BOTH engines' profiles for the account.
- **First run is ZERO-STEP (v899.1)**: the worker migrates the ChatGPT session
  — AND the Google login behind it, so OpenAI knows the account is
  kaveno.biz@gmail.com — out of its own Chrome profile automatically (cookie
  dump in a child process; a nested driver hangs). Only if no Chrome session
  exists anywhere does it open a visible sign-in window BY ITSELF and drop
  back to headless after the login. No env vars, no extra commands, ever.
- Camoufox auto-installs (`pip install camoufox>=0.5.4` + `camoufox fetch`) if
  missing. The fetch downloads from GitHub — Surfshark's MTU issue blocks it
  (MTU 1280 or VPN off).

## Production contract

- Banana and ChatGPT are separate queue lanes. Each lane owns its own claim,
  status, files and variants.
- Both lanes must save a real, non-manual variant for every generated node.
- Automatic approval checks the current exact variant IDs and refuses stale QC.
- Approval and promotion fail closed when either backend output is missing,
  including on chained and reference-dependent nodes.
- The image workers are shared queue daemons. A pipeline holds a named lease on
  each lane while its image batch needs them, releases the leases after promotion
  or failure, and lets the queue-aware lifecycle sweep stop only proven-idle
  daemons.
- This is different from Flow clip work. Each Flow clip worker is scoped to one
  video job and closes at that job's terminal marker.

The production coordinator starts and leases the shared image lanes through:

```bash
python tools/worker_lifecycle.py ensure image --session <pipeline-session>
python tools/worker_lifecycle.py ensure chatgpt --session <pipeline-session>
```

Do not leave a browser tab open as a handoff. Headless mode is the normal route.

## One-time setup

1. Install Patchright:
   ```bash
   pip install patchright
   ```
2. The installed launcher passes the target account. The worker uses its own
   engine-tagged, per-account profile. It first tries to copy the existing
   account session from Chrome Beta. If no usable session exists, it opens its
   own visible window for a one-time login, verifies the account, persists that
   profile, then returns to headless runs.

## Serving platform jobs

The production path is HTTP pull. The worker asks for the ChatGPT backend lane,
downloads every ordered reference in the job payload, generates one image,
uploads it with `backend=chatgpt`, and reports that lane's status. It does not
claim or overwrite the Banana lane.

```bash
python code/static/chatgpt_image_worker.py --firefox \
  --api-url <platform-url> --api-key <worker-key> \
  --chatgpt-email <account-email>
```

Use the lifecycle command above for normal production instead of typing the
worker key into an interactive shell. `--watch` remains a legacy local-folder
mode; it is not the production dual-backend handoff.

## Selecting variants in the UI

Do not select ChatGPT as an either/or node model. The platform queues the
ChatGPT lane automatically alongside Banana for every generated node. The UI
shows backend-tagged results from both lanes; choosing one result supplies the
resolved reference used by both lanes for the next dependent node.

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

The worker first reuses its verified per-account profile. If that login is no
longer usable and no saved Chrome Beta session can be copied, it opens its own
visible one-time login window. No main Chrome or Firefox profile should be
closed for this repair.

## Failure behavior

On timeout, refusal, expired session or selector drift, the worker reports the
ChatGPT lane as failed. That does not erase Banana's output and does not stop
other jobs, but this job cannot be auto-approved or promoted until the ChatGPT
lane produces a real variant and current QC covers the exact saved variant set.

## Safety

- **LOCAL-ONLY.** Needs the operator's saved ChatGPT session. Never runs on Render.
- Session profiles and cookie files are gitignored in `code/.gitignore`. **Never commit them.**
