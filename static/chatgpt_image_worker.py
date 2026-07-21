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
import os

import chatgpt_job_map as jobmap
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


def _is_chatgpt_job(job):
    return (job or {}).get("model") == "chatgpt"

def _scan_pending(watch_dir):
    """chatgpt job files with no sibling .done.json, oldest first."""
    out = []
    for f in sorted(os.listdir(watch_dir)):
        if not f.endswith(".json") or f.endswith(".done.json"):
            continue
        jp = os.path.join(watch_dir, f)
        if os.path.exists(jp.replace(".json", ".done.json")):
            continue
        try:
            with open(jp, encoding="utf-8") as _f:
                job = json.load(_f)
        except Exception:
            continue
        if _is_chatgpt_job(job):
            out.append(jp)
    return out

def _write_done(job_path, payload):
    with open(job_path.replace(".json", ".done.json"), "w", encoding="utf-8") as _f:
        _f.write(json.dumps(payload))

def _process_platform_job(page, job_path, job):
    """Generate ONE image (variants clamped to 1) and write the .done.json."""
    jid = job.get("id") or os.path.basename(job_path).replace(".json", "")
    out_dir = job["output_dir"]
    out_path = os.path.join(out_dir, "variant_1.png")
    try:
        os.makedirs(out_dir, exist_ok=True)
        prompt = jobmap.build_prompt(job)
        refs = jobmap.ref_paths(job)
        generate(page, prompt, refs, out_path)
        _write_done(job_path, jobmap.done_payload(jid, "completed", ["variant_1.png"], None))
        log(f"  ✓ {jid} -> variant_1.png")
    except Exception as e:
        _write_done(job_path, jobmap.done_payload(jid, "failed", [], str(e)))
        log(f"  ✗ {jid} failed: {e}")

def watch_mode(watch_dir, poll_s=5):
    """Poll the platform job folder; process chatgpt jobs one at a time."""
    log(f"watch mode on {watch_dir}")
    sync_playwright, _ = _import_playwright()
    with sync_playwright() as p:
        ctx, page = launch(p)
        try:
            import time as _t
            while True:
                for jp in _scan_pending(watch_dir):
                    try:
                        with open(jp, encoding="utf-8") as _f:
                            job = json.load(_f)
                    except Exception:
                        continue
                    claim = jp.replace(".json", ".claim")
                    try:
                        fd = os.open(claim, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                        os.close(fd)
                    except FileExistsError:
                        continue
                    try:
                        _process_platform_job(page, jp, job)
                    finally:
                        try:
                            os.remove(claim)
                        except OSError:
                            pass
                    jitter(4.0, 9.0)
                _t.sleep(poll_s)
        finally:
            ctx.close()


def main():
    ap = argparse.ArgumentParser(description="ChatGPT web image-gen worker")
    ap.add_argument("--login", action="store_true", help="headful manual login, save session")
    ap.add_argument("--jobs", help="path to jobs JSON file")
    ap.add_argument("--ref", help="single job: reference image path")
    ap.add_argument("--prompt", help="single job: prompt text")
    ap.add_argument("--out", help="single job: output image path")
    ap.add_argument("--user-data-dir", help="drive your real Chrome data dir (Chrome must be CLOSED)")
    ap.add_argument("--profile-directory", help="sub-profile to use, e.g. 'Profile 18'")
    ap.add_argument("--watch", action="store_true", help="poll the platform _image_jobs folder")
    ap.add_argument("--watch-dir", help="override the _image_jobs path")
    args = ap.parse_args()

    if args.user_data_dir:
        backend.USER_DATA_DIR = args.user_data_dir
    if args.profile_directory:
        backend.PROFILE_DIRECTORY = args.profile_directory

    if args.watch:
        wd = args.watch_dir or os.environ.get("IMAGE_JOBS_DIR") or os.path.join(
            os.environ.get("DATA_DIR", os.path.join(BASE_DIR, "..", "..", "data")), "_image_jobs")
        watch_mode(wd)
        return
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
