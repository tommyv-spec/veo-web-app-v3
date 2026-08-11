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
import sys

import chatgpt_job_map as jobmap
import chatgpt_image_backend as backend
from chatgpt_image_backend import (
    log, jitter, _import_playwright, launch, inject_cookies, dismiss_cookie_banner,
    is_logged_in, generate, SEL, CHATGPT_URL, COOKIES_FILE, PROFILE_DIR, BASE_DIR,
)


def _logged_in_email(page):
    """The email of the currently logged-in ChatGPT account, or None.

    /backend-api/me stopped identifying the user (2026-08-10: it returns an
    anonymous ua-* device object with an empty email even on a live session),
    so /api/auth/session — which still carries user.email — is the fallback.
    Only the email ever leaves the page: the session endpoint also holds the
    access token, which must never be logged."""
    try:
        return page.evaluate("""async () => {
            try {
                const r = await fetch('/backend-api/me', {credentials: 'include'});
                if (r.ok) {
                    const j = await r.json();
                    const em = j && (j.email || (j.account && j.account.email));
                    if (em) return em;
                }
            } catch (e) {}
            try {
                const r = await fetch('/api/auth/session', {credentials: 'include'});
                if (r.ok) {
                    const j = await r.json();
                    return (j && j.user && j.user.email) || null;
                }
            } catch (e) {}
            return null;
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
    if backend.FIREFOX_MODE and backend._bd.firefox_headless_enabled():
        # Headless has no window to log into — polling for 10 minutes would just
        # look like a hang. Return fast; launch_logged_in owns the recovery
        # (seed from real Firefox, else open a sign-in window).
        log(f"firefox headless: no saved session{who} — automatic recovery next "
            "(seed from your real Firefox, else open a sign-in window).")
        return False
    log("=" * 60)
    if isinstance(r, str):
        log(f"  WRONG ACCOUNT: logged in as {r}, but this worker needs {email}.")
        log(f"  In the window: click your profile -> Log out, then Continue{who}.")
    else:
        log(f"  ACTION NEEDED: log into ChatGPT{who} in the browser window that opened —")
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


def _seed_firefox_profile(email, log=log):
    """Seed the worker's Firefox profile from a REAL Firefox profile on this
    machine — the SAME method flow_worker uses (firefox_profile_pull /
    build_firefox_golden_from_profile), not a ChatGPT-specific invention.

    Copies durable DATA files only (cookies.sqlite, key4.db, cert9.db,
    logins.json, permissions.sqlite). Never prefs.js / compatibility.ini:
    Firefox REFUSES a profile written by a newer Firefox and exits 0 with no
    error, so a whole-directory copy produces a browser that silently never
    starts (the real Firefox here is 153, Camoufox ships 152).

    Account note: the Google account signed into Firefox is normally NOT the
    ChatGPT worker's email — the operator runs different accounts per service.
    So an exact email match is preferred, and a single unambiguous Firefox
    session is accepted as the source with the address logged. Only a genuinely
    ambiguous machine (several Google sessions, none matching) declines."""
    try:
        import firefox_profile_pull as ffp
    except ImportError:
        log("  firefox_profile_pull.py not in the bundle — cannot seed from Firefox")
        return False
    if email:
        # Exact account only. Seeding a DIFFERENT Google account would sign the
        # worker into ChatGPT as the wrong person: measured 2026-08-03, seeding
        # the machine's only Firefox profile put a "Continue as Kevin
        # (shenkevin480@gmail.com)" one-tap on chatgpt.com while the worker was
        # asked for kaveno.biz@gmail.com. Same rule flow_worker enforces.
        return ffp.build_firefox_golden_from_profile(
            email, golden_folder=backend.PROFILE_DIR, label="CHATGPT", log=log)
    return ffp.build_firefox_golden_from_profile(
        "", golden_folder=backend.PROFILE_DIR, label="CHATGPT", log=log)


def _sign_in_with_google(page, timeout_s=90):
    """chatgpt.com -> Log in -> Continue with Google, using the Google session
    the seeded profile already carries. One-click OAuth, no typing.

    The seeded Firefox profile holds google.com cookies but no chatgpt.com ones
    (the operator is signed into Google in Firefox, not into ChatGPT), so this
    is the step that converts one into the other."""
    import time as _t
    deadline = _t.time() + timeout_s
    try:
        page.goto(CHATGPT_URL, wait_until="domcontentloaded", timeout=45000)
        dismiss_cookie_banner(page)
        _t.sleep(3)
        # Google One Tap ("Sign in to OpenAI with Google — Continue as <name>")
        # renders straight on chatgpt.com when the profile carries a Google
        # session. That is the shortest path: one click, no auth page at all.
        for sel in ("button:has-text('Continue as')", "div:has-text('Continue as') button",
                    "iframe[src*='accounts.google.com']"):
            try:
                loc = page.locator(sel).first
                if loc.count() > 0 and loc.is_visible():
                    if sel.startswith("iframe"):
                        frame = page.frame_locator(sel).locator(
                            "button:has-text('Continue as')").first
                        frame.click(timeout=10000)
                    else:
                        loc.click(timeout=10000)
                    _t.sleep(5)
                    if is_logged_in(page):
                        log("  signed in via Google One Tap")
                        return True
                    break
            except Exception:
                pass
        for sel in ("a[href*='/auth/login']", "button:has-text('Log in')",
                    "a:has-text('Log in')", "button:has-text('Accedi')"):
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible():
                loc.click(timeout=10000)
                break
        else:
            log("  no Log-in control found on chatgpt.com")
            return False
        page.wait_for_load_state("domcontentloaded", timeout=30000)
        _t.sleep(2)
        for sel in ("button:has-text('Continue with Google')",
                    "a:has-text('Continue with Google')",
                    "button:has-text('Continua con Google')",
                    "[data-provider='google']"):
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible():
                loc.click(timeout=15000)
                break
        else:
            log("  no 'Continue with Google' button on the auth page")
            return False
        # Google may show an account chooser, a consent screen, or nothing at
        # all. Poll for the round trip instead of guessing which.
        while _t.time() < deadline:
            _t.sleep(2)
            url = page.url or ""
            if "chatgpt.com" in url and is_logged_in(page):
                log("  signed in via Google")
                return True
            if "accounts.google.com" in url:
                for sel in ("div[data-identifier]", "li[class*='account'] div",
                            "button:has-text('Continue')", "button:has-text('Continua')"):
                    loc = page.locator(sel).first
                    try:
                        if loc.count() > 0 and loc.is_visible():
                            loc.click(timeout=8000)
                            break
                    except Exception:
                        pass
        log("  Google sign-in did not complete in time")
        return False
    except Exception as e:
        log(f"  Google sign-in failed: {str(e).splitlines()[0][:110]}")
        return False


def _bridge_chrome_cookies(ff_ctx, page):
    """Inject the Chrome-Beta account's cookies into the Firefox context.

    Source = the lean golden that chatgpt_session_pull built with flow_worker's
    pull. Both the ChatGPT session-token AND the Google SSO cookies come across,
    so the worker lands logged in, and any later re-login offers the right
    account instead of an empty Google form.

    Proven on the operator's box 2026-08-03: 58 cookies read from the golden —
    __Secure-next-auth.session-token.0/.1 for chatgpt.com plus SID/SAPISID/LSID
    for google.com + accounts.google.com."""
    try:
        import chrome_cookie_bridge as bridge
    except ImportError:
        log("  chrome_cookie_bridge.py not in the bundle — skipping")
        return False
    seen = []
    for src in (getattr(backend, "CHROME_GOLDEN_DIR", None), backend.PROFILE_DIR):
        if not src or not os.path.isdir(src) or src in seen:
            continue
        seen.append(src)
        cookies = bridge.read_cookies(
            src, ("chatgpt.com", "openai.com", "google.com"), log=log)
        if not cookies:
            continue
        try:
            ff_ctx.add_cookies(cookies)
        except Exception as e:
            # One malformed cookie rejects the whole batch; retry per-cookie so
            # a single bad row cannot cost the entire session.
            log(f"  batch inject rejected ({str(e)[:80]}) — injecting one by one")
            ok = 0
            for c in cookies:
                try:
                    ff_ctx.add_cookies([c])
                    ok += 1
                except Exception:
                    pass
            log(f"  injected {ok}/{len(cookies)} cookies individually")
            if not ok:
                continue
        try:
            page.goto(CHATGPT_URL, wait_until="domcontentloaded", timeout=45000)
            dismiss_cookie_banner(page)
        except Exception:
            continue
        if is_logged_in(page):
            log(f"  session bridged from {os.path.basename(src)} — no login needed.")
            return True
        # Cookies are in but ChatGPT still shows logged out: the Google session
        # came across even when the ChatGPT one did not, so one-tap can finish it.
        if _sign_in_with_google(page):
            return True
    return False


def launch_logged_in(p, email=None):
    """(ctx, page) guaranteed logged in, or None after telling the user why.

    Firefox first run, in order of least operator effort:
      1. the saved firefox session (nothing to do — every run after the first)
      2. seed from a real Firefox profile signed into THIS account, then convert
         its Google session into a ChatGPT one (flow_worker's method)
      3. open a visible window AUTOMATICALLY, wait for the one sign-in, then
         drop back to headless — no env var, no re-run, no separate script
    Chrome behaves exactly as before (its window is already visible)."""
    ctx, page = launch(p)
    if ensure_logged_in(page, email):
        return ctx, page
    if not backend.FIREFOX_MODE:
        ctx.close()
        return None

    # 2a. THE FLOW-WORKER METHOD: seed this profile from a real Firefox profile
    # on the machine, then convert its Google session into a ChatGPT one.
    # Seeding REPLACES the profile directory, so it only ever runs here — after
    # the existing profile has been proven not logged in.
    log("firefox: seeding the profile from your real Firefox (flow-worker method)...")
    ctx.close()
    if _seed_firefox_profile(email):
        ctx, page = launch(p)
        if ensure_logged_in(page, email) or (
                _sign_in_with_google(page) and ensure_logged_in(page, email)):
            return ctx, page
        ctx.close()

    # 2b. CHROME-BETA BRIDGE — the path for an account that lives only in
    # Chrome Beta (the normal case here). The session was already collected by
    # chatgpt_session_pull using flow_worker's own pull: it closes the
    # NON-STABLE channel to release the file lock, never touching daily stable
    # Chrome, and copies the durable files into a lean golden. Firefox cannot
    # READ that chromium profile, but the cookies inside it are ordinary
    # cookies — decrypt them from the golden and inject them here.
    log("firefox: bridging the Chrome-Beta session cookies...")
    ctx, page = launch(p)
    if _bridge_chrome_cookies(ctx, page) and ensure_logged_in(page, email):
        return ctx, page
    ctx.close()
    if not backend._bd.firefox_headless_enabled():
        return None      # the window was already visible and login still failed
    log("firefox: opening a visible window for the one-time sign-in...")
    os.environ["FIREFOX_HEADLESS"] = "0"
    try:
        ctx, page = launch(p)
        ok = ensure_logged_in(page, email)
        ctx.close()
    finally:
        os.environ.pop("FIREFOX_HEADLESS", None)
    if not ok:
        return None
    # The persistent profile now HOLDS that session, so this is genuinely
    # one-time: every later run takes branch 1 and never opens a window.
    log("login saved in the firefox profile — dropping back to headless.")
    ctx, page = launch(p)
    if ensure_logged_in(page, email):
        return ctx, page
    ctx.close()
    return None


def login_flow():
    """Headful manual login. User logs into chatgpt.com, then Ctrl-C / closes."""
    if backend.FIREFOX_MODE:
        # --login exists to SHOW a window; headless would show nothing.
        os.environ["FIREFOX_HEADLESS"] = "0"
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
        lp = launch_logged_in(p, email)
        if not lp:
            return
        ctx, page = lp
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
    ap.add_argument("--api-url", help="HTTP-pull mode: base URL of the Render platform")
    ap.add_argument("--api-key", help="HTTP-pull mode: worker API key (Bearer)")
    ap.add_argument("--chatgpt-email", help="target ChatGPT account. The worker uses "
                    "a clean per-account profile folder and waits for you to log in "
                    "as this account in its own browser window (persisted + reused).")
    ap.add_argument("--firefox", action="store_true",
                    help="run on Firefox (Camoufox 0.5.4 + Playwright) — the same "
                    "stack the flow worker ships. Equivalent to BROWSER_MODE=firefox. "
                    "Headless by default; set FIREFOX_HEADLESS=0 for the one-time login.")
    args = ap.parse_args()

    # v899 — engine switch BEFORE any profile-path work, so every profile name
    # below resolves against the right engine. Env BROWSER_MODE=firefox works
    # too (parity with flow_worker); the flag just sets it.
    if args.firefox:
        backend.set_browser_mode("firefox")
    if backend.FIREFOX_MODE:
        log("engine: FIREFOX (Camoufox) — profile "
            f"{os.path.basename(backend.PROFILE_DIR)}")

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
        # v899: engine-tagged per-account profile — a Firefox run must never
        # open the Chrome-format folder (mutually unreadable profile formats).
        _prefix = ".chatgpt_profile_firefox_" if backend.FIREFOX_MODE else ".chatgpt_profile_"
        backend.PROFILE_DIR = os.path.join(backend.BASE_DIR, f"{_prefix}{safe}")
        # No cookie-file injection in this model — the profile itself holds the login.
        # Point COOKIES_FILE at a path that never exists so inject_cookies is a no-op.
        backend.COOKIES_FILE = os.path.join(backend.BASE_DIR, ".chatgpt_cookies_unused")
        # v899: keep BOTH engines' profiles for THIS account. The cleanup exists
        # for cross-ACCOUNT hygiene; without the engine guard, one --firefox run
        # would rmtree the working Chrome login (and a Chrome run the Firefox one).
        # The UNTAGGED defaults are kept too (a run without --chatgpt-email lives
        # there — the operator's installer run deleted a freshly migrated
        # `.chatgpt_profile_firefox` this way). Account hygiene is not weakened:
        # ensure_logged_in verifies the account and blocks a readable mismatch.
        _keep = {
            os.path.abspath(os.path.join(backend.BASE_DIR, n)) for n in (
                f".chatgpt_profile_{safe}", f".chatgpt_profile_firefox_{safe}",
                ".chatgpt_profile", ".chatgpt_profile_firefox",
            )
        }
        for _pat in (".chatgpt_profile*", ".chatgpt_cookies*"):
            for _d in _glob.glob(os.path.join(backend.BASE_DIR, _pat)):
                if os.path.abspath(_d) in _keep:
                    continue  # keep THIS account's profiles (both engines)
                if os.path.isdir(_d):
                    _shutil.rmtree(_d, ignore_errors=True)
                else:
                    try:
                        os.remove(_d)
                    except OSError:
                        pass
                log(f"deleted stale: {os.path.basename(_d)}")
        log(f"using clean per-account profile: {os.path.basename(backend.PROFILE_DIR)}")
        # COPY the session from the account in Chrome BETA (exactly like the video
        # worker) — closes ONLY that one Beta profile's window, never the daily
        # stable Chrome. Beta is ABE-off (v10 cookies) so the golden decrypts.
        # If the account isn't in Beta (or copy fails), fall back to a one-time
        # login in the worker's own window (ensure_logged_in).
        # The Chrome-Beta pull runs in BOTH engines — it is how the account's
        # session is collected, exactly as flow_worker collects it (close the
        # non-stable channel to release the lock, copy the durable files, never
        # touch daily stable Chrome). Only the DESTINATION differs: Chrome mode
        # launches the golden directly; Firefox cannot read a chromium profile,
        # so the golden is kept beside the Firefox profile and its cookies are
        # bridged in (see chrome_cookie_bridge).
        if backend.FIREFOX_MODE:
            backend.CHROME_GOLDEN_DIR = os.path.join(
                backend.BASE_DIR, f".chatgpt_chrome_golden_{safe}")
        _pull_target = (backend.CHROME_GOLDEN_DIR if backend.FIREFOX_MODE
                        else backend.PROFILE_DIR)
        try:
            import chatgpt_session_pull
            if chatgpt_session_pull.pull_chatgpt_session(args.chatgpt_email, _pull_target):
                log("copied ChatGPT session from Chrome Beta — no manual login needed."
                    + (" (firefox: cookies are bridged from it)" if backend.FIREFOX_MODE else ""))
            else:
                log("no Beta session to copy; will wait for a one-time login in the window.")
        except Exception as _e:
            log(f"Beta copy failed ({_e}); will wait for a one-time login.")

    # SESSION MODEL (universal, simple, no admin/registry/ABE): the worker uses its
    # OWN dedicated Chrome profile and the user logs into ChatGPT ONCE in the visible
    # window (ensure_logged_in). The session persists in the worker's profile and is
    # reused. No pulling from the user's main Chrome — works for every user.
    if args.api_url and args.api_key:
        import socket
        from chatgpt_http_pull import run as http_run
        sync_playwright, _ = _import_playwright()
        with sync_playwright() as p:
            lp = launch_logged_in(p, getattr(args, "chatgpt_email", None))
            if not lp:
                return
            ctx, page = lp
            state = {"ctx": ctx}

            def _relaunch():
                """A fresh logged-in page after the browser dies mid-run.
                The saved profile means this is a plain relaunch — no seeding,
                no sign-in — so recovery costs seconds, not a queue."""
                old = state.get("ctx")
                if old is not None:
                    try:
                        old.close()
                    except Exception:
                        pass          # already dead; closing is best-effort
                again = launch_logged_in(p, getattr(args, "chatgpt_email", None))
                if not again:
                    state["ctx"] = None
                    return None
                state["ctx"], new_page = again
                return new_page

            try:
                http_run(args.api_url, args.api_key, page, socket.gethostname(),
                         relaunch=_relaunch)
            finally:
                if state.get("ctx") is not None:
                    try:
                        state["ctx"].close()
                    except Exception:
                        pass
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
