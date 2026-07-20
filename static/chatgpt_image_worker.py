#!/usr/bin/env python3
"""
chatgpt_image_worker.py — lean standalone ChatGPT web image-gen automation.

Drives the REAL ChatGPT website by replicating user actions (attach ref image,
type prompt, submit, wait, download). Uses a real Chrome browser via Patchright
so the browser itself mints every sentinel / turnstile / proof-of-work token —
no HTTP-token reversing, no API cost.

WHY web-UI, not the API: the OpenAI Images API costs per call; the consumer web
app is included in the ChatGPT plan. Trade-off: one-at-a-time, human-paced,
selector-fragile. Same risk posture as flow_worker.py.

The browser drive core lives in chatgpt_image_backend.py; this file keeps only
the orchestration (login flow, per-job wiring, batch loop, CLI).

--------------------------------------------------------------------------------
SETUP (once)
--------------------------------------------------------------------------------
  pip install patchright && patchright install chromium
  # (flow_worker already depends on patchright==1.57 via requirements-flow.txt)

  # 1. First run — log in manually. A dedicated profile is used so your daily
  #    Chrome is never touched/corrupted. Log into chatgpt.com, then close it.
  python code/static/chatgpt_image_worker.py --login

  # 2. Run a batch:
  python code/static/chatgpt_image_worker.py --jobs code/static/chatgpt_jobs.example.json

  # single ad-hoc job:
  python code/static/chatgpt_image_worker.py --ref ref.png --prompt "Crea immagine ..." --out out.png

--------------------------------------------------------------------------------
JOBS FILE (JSON) — list of jobs:
[
  {"ref": "path/to/reference.png", "prompt": "Crea immagine ...", "out": "path/to/output.png"},
  {"prompt": "no-reference prompt", "out": "out2.png"}          // ref optional
]
--------------------------------------------------------------------------------
SELECTORS DRIFT: ChatGPT ships UI updates. The locators live in
chatgpt_image_backend.py under SEL with fallbacks. If a step times out, re-pin there.
"""

import argparse
import json

import chatgpt_image_backend as backend
from chatgpt_image_backend import (
    log, jitter, _import_playwright, launch, inject_cookies, dismiss_cookie_banner,
    is_logged_in, generate, SEL, CHATGPT_URL, COOKIES_FILE, PROFILE_DIR, BASE_DIR,
)


def login_flow():
    """Headful manual login. User logs into chatgpt.com, then Ctrl-C / closes."""
    sync_playwright, _ = _import_playwright()
    with sync_playwright() as p:
        ctx, page = launch(p)
        page.goto(CHATGPT_URL, wait_until="domcontentloaded")
        dismiss_cookie_banner(page)
        log("Browser open. Log into ChatGPT manually.")
        log("When the chat composer is visible + working, press ENTER here to save session.")
        try:
            input()
        except (EOFError, KeyboardInterrupt):
            pass
        ok = is_logged_in(page)
        ctx.close()
        log(f"Session saved to {PROFILE_DIR}. logged_in={ok}")


def run_job(page, job, idx, total):
    ref = job.get("ref")
    prompt = job["prompt"]
    out = job["out"]
    log(f"[{idx}/{total}] job -> out={out} ref={ref or '(none)'}")
    ref_paths = [ref] if ref else []
    generate(page, prompt, ref_paths, out)
    log(f"  ✓ saved {out}")


def batch(jobs_file=None, single=None):
    if single:
        jobs = [single]
    else:
        with open(jobs_file, "r", encoding="utf-8") as f:
            jobs = json.load(f)
    if not isinstance(jobs, list) or not jobs:
        log("no jobs to run")
        return

    sync_playwright, _ = _import_playwright()
    ok = 0
    fails = []
    with sync_playwright() as p:
        ctx, page = launch(p)
        try:
            for i, job in enumerate(jobs, 1):
                try:
                    run_job(page, job, i, len(jobs))
                    ok += 1
                except Exception as e:
                    log(f"  ✗ job {i} FAILED: {e}")
                    fails.append((i, str(e)))
                # human gap between jobs
                jitter(4.0, 9.0)
        finally:
            ctx.close()
    log(f"DONE: {ok}/{len(jobs)} ok" + (f", failed={fails}" if fails else ""))


def main():
    ap = argparse.ArgumentParser(description="ChatGPT web image-gen worker")
    ap.add_argument("--login", action="store_true", help="headful manual login, save session")
    ap.add_argument("--jobs", help="path to jobs JSON file")
    ap.add_argument("--ref", help="single job: reference image path")
    ap.add_argument("--prompt", help="single job: prompt text")
    ap.add_argument("--out", help="single job: output image path")
    ap.add_argument("--user-data-dir", help="drive your real Chrome data dir (Chrome must be CLOSED)")
    ap.add_argument("--profile-directory", help="sub-profile to use, e.g. 'Profile 18'")
    args = ap.parse_args()

    if args.user_data_dir:
        backend.USER_DATA_DIR = args.user_data_dir
    if args.profile_directory:
        backend.PROFILE_DIRECTORY = args.profile_directory

    if args.login:
        login_flow()
        return
    if args.jobs:
        batch(jobs_file=args.jobs)
        return
    if args.prompt and args.out:
        batch(single={"ref": args.ref, "prompt": args.prompt, "out": args.out})
        return
    ap.print_help()


if __name__ == "__main__":
    main()
