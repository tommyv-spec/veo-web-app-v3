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


def _logged_in_email(page):
    """The email of the currently logged-in ChatGPT account (via /backend-api/me),
    or None if not logged in / unreadable."""
    try:
        return page.evaluate("""async () => {
            try {
                const r = await fetch('/backend-api/me', {credentials: 'include'});
                if (!r.ok) return null;
                const j = await r.json();
                return (j && (j.email || (j.account && j.account.email))) || null;
            } catch (e) { return null; }
        }""")
    except Exception:
        return None


def ensure_logged_in(page, email=None, timeout_s=600):
    """Universal, simple session: make sure the worker's OWN browser is logged into
    ChatGPT. Navigates to ChatGPT (with a Google login-hint for `email` so the
    account chooser surfaces "Continue as <email>"). If not logged in, the visible
    window is left open for the user to click "Continue as <email>" / sign in ONCE —
    we poll until they do; the session then persists in the worker's own profile.
    NO pulling from the user's main Chrome, NO admin/registry/ABE — works for every
    user on any machine. Returns True once logged in, False on timeout."""
    import time as _t
    page.goto(CHATGPT_URL, wait_until="domcontentloaded", timeout=45000)
    dismiss_cookie_banner(page)

    def _ok():
        """True when logged in AS THE TARGET account (or any account if no email, or
        the account email is unreadable -> best-effort proceed). Returns the current
        email string when it is READABLY WRONG (block + prompt to switch)."""
        if not is_logged_in(page):
            return False
        if not email:
            return True
        cur = _logged_in_email(page)
        if not cur:
            return True  # unreadable -> can't verify, proceed (never hang)
        if cur.strip().lower() == email.strip().lower():
            return True
        return cur  # readable + wrong account -> block

    r = _ok()
    if r is True:
        log(f"ChatGPT: already logged in{f' as {email}' if email else ''}.")
        return True

    who = f" as {email}" if email else ""
    log("=" * 60)
    if isinstance(r, str):
        log(f"  WRONG ACCOUNT: logged in as {r}, but this worker needs {email}.")
        log(f"  In the window: click your profile -> Log out, then Continue{who}.")
    else:
        log(f"  ACTION NEEDED: log into ChatGPT{who} in the Chrome window that opened —")
        log(f"  click \"Continue{who}\" (or sign in).")
    log("  ONE-TIME: the session is saved in the worker's own profile. Waiting 10 min...")
    log("=" * 60)
    deadline = _t.time() + timeout_s
    warned = None
    while _t.time() < deadline:
        r = _ok()
        if r is True:
            log(f"ChatGPT: logged in{f' as {email}' if email else ''} — session saved, continuing.")
            return True
        if isinstance(r, str) and r != warned:
            log(f"  still logged in as {r} — need {email}. Log out + Continue{who}.")
            warned = r
        _t.sleep(3)
    log("ChatGPT: correct-account login not completed in time. Re-run and log in.")
    return False


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

def watch_mode(watch_dir, poll_s=5, email=None):
    """Poll the platform job folder; process chatgpt jobs one at a time."""
    log(f"watch mode on {watch_dir}")
    sync_playwright, _ = _import_playwright()
    with sync_playwright() as p:
        ctx, page = launch(p)
        try:
            if not ensure_logged_in(page, email):
                return
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
    ap.add_argument("--api-url", help="HTTP-pull mode: base URL of the Render platform")
    ap.add_argument("--api-key", help="HTTP-pull mode: worker API key (Bearer)")
    ap.add_argument("--chatgpt-email", help="target ChatGPT account. The worker uses "
                    "a clean per-account profile folder and waits for you to log in "
                    "as this account in its own browser window (persisted + reused).")
    args = ap.parse_args()

    if args.user_data_dir:
        backend.USER_DATA_DIR = args.user_data_dir
    if args.profile_directory:
        backend.PROFILE_DIRECTORY = args.profile_directory

    # PER-ACCOUNT clean folder + AUTO-GRAB. If an email is given: (1) use a clean
    # per-account profile (.chatgpt_profile_<email>) and DELETE old/other folders so
    # a stale session from another account can never be reused; (2) auto-grab the
    # ChatGPT session via netlog (the method that worked first) so NO manual login is
    # needed. If the auto-grab can't get it, the worker falls back to a one-time
    # login in its own window (ensure_logged_in). Either way the account is verified.
    if getattr(args, "chatgpt_email", None):
        import re as _re, glob as _glob, shutil as _shutil
        safe = _re.sub(r"[^A-Za-z0-9._-]", "_", args.chatgpt_email.strip().lower())
        backend.PROFILE_DIR = os.path.join(backend.BASE_DIR, f".chatgpt_profile_{safe}")
        backend.COOKIES_FILE = os.path.join(backend.BASE_DIR, f".chatgpt_cookies_{safe}.json")
        keep = {os.path.abspath(backend.PROFILE_DIR), os.path.abspath(backend.COOKIES_FILE)}
        for _pat in (".chatgpt_profile*", ".chatgpt_cookies*.json"):
            for _d in _glob.glob(os.path.join(backend.BASE_DIR, _pat)):
                if os.path.abspath(_d) in keep:
                    continue
                if os.path.isdir(_d):
                    _shutil.rmtree(_d, ignore_errors=True)
                else:
                    try:
                        os.remove(_d)
                    except OSError:
                        pass
                log(f"deleted stale: {os.path.basename(_d)}")
        log(f"using clean per-account profile: {os.path.basename(backend.PROFILE_DIR)}")
        # AUTO-GRAB the session (netlog — worked in the first run) so no manual login.
        try:
            import chatgpt_session_pull
            if chatgpt_session_pull.pull_chatgpt_cookies_netlog(args.chatgpt_email, backend.COOKIES_FILE):
                log("auto-grabbed ChatGPT session — no manual login needed.")
            else:
                log("could not auto-grab session; will wait for a one-time login in the window.")
        except Exception as _e:
            log(f"auto-grab failed ({_e}); will wait for a one-time login.")

    # SESSION MODEL (universal, simple, no admin/registry/ABE): the worker uses its
    # OWN dedicated Chrome profile and the user logs into ChatGPT ONCE in the visible
    # window (ensure_logged_in). The session persists in the worker's profile and is
    # reused. No pulling from the user's main Chrome — works for every user.
    if args.api_url and args.api_key:
        import socket
        from chatgpt_http_pull import run as http_run
        sync_playwright, _ = _import_playwright()
        with sync_playwright() as p:
            ctx, page = launch(p)
            try:
                if not ensure_logged_in(page, getattr(args, "chatgpt_email", None)):
                    return
                http_run(args.api_url, args.api_key, page, socket.gethostname())
            finally:
                ctx.close()
        return
    if args.watch:
        wd = args.watch_dir or os.environ.get("IMAGE_JOBS_DIR") or os.path.join(
            os.environ.get("DATA_DIR", os.path.join(BASE_DIR, "..", "..", "data")), "_image_jobs")
        watch_mode(wd, email=getattr(args, "chatgpt_email", None))
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
