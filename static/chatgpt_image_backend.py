#!/usr/bin/env python3
"""ChatGPT browser drive core (Patchright). Login via netlog-captured cookie
injection (ABE-immune), attach reference images, type prompt, wait for the
generated image (served from estuary/content), download it. Knows nothing about
the platform job format — takes a prompt + ref paths, produces a PNG."""

import base64
import json
import os
import random
import sys
import time

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROFILE_DIR = os.environ.get(
    "CHATGPT_PROFILE_DIR", os.path.join(BASE_DIR, ".chatgpt_profile")
)
# Optional: drive your REAL Chrome user-data dir + a specific sub-profile
# (e.g. the profile already logged into ChatGPT). Chrome MUST be fully closed
# first, or the profile is locked. Set via --user-data-dir / --profile-directory.
USER_DATA_DIR = None          # e.g. C:/Users/tomma/AppData/Local/Google/Chrome/User Data
PROFILE_DIRECTORY = None      # e.g. "Profile 18"
CHROME_CHANNEL = os.environ.get("WORKER_CHROME_CHANNEL", "chrome")
CHATGPT_URL = "https://chatgpt.com/"
# Plaintext cookies captured via netlog (ABE-immune). Injected on every launch so
# login survives App-Bound Encryption (copied v20 cookies never decrypt). Re-run
# the netlog capture to refresh when the session-token expires.
COOKIES_FILE = os.path.join(BASE_DIR, ".chatgpt_cookies.json")

# generation can be slow (thinking model + image render)
GEN_TIMEOUT_S = int(os.environ.get("CHATGPT_GEN_TIMEOUT_S", "180"))
# human jitter between actions (min, max seconds)
JITTER = (float(os.environ.get("CHATGPT_JITTER_MIN", "1.2")),
          float(os.environ.get("CHATGPT_JITTER_MAX", "3.5")))

_IGNORE_DEFAULT_ARGS = ["--enable-automation"]
if sys.platform == "darwin":
    _IGNORE_DEFAULT_ARGS += ["--password-store=basic", "--use-mock-keychain"]

CHROME_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--disable-focus-on-load",
    "--disable-dev-shm-usage",
    "--no-sandbox",
    "--mute-audio",
]

# ---------------------------------------------------------------------------
# SELECTORS (re-pin here when ChatGPT UI drifts)
# ---------------------------------------------------------------------------
SEL = {
    # contenteditable composer
    "composer": "#prompt-textarea",
    # hidden file input the "+" attach uses (set_input_files works without clicking)
    "file_input": "input[type=file]",
    # send button
    "send": "button[data-testid=send-button], button[aria-label*='Send'], button[data-testid='composer-send-button']",
    # a generated image inside the assistant turn. ChatGPT serves gen output from
    # backend-api/estuary/content?id=file-... (primary), sometimes oaiusercontent.
    "gen_img": "img[src*='estuary/content'], img[src*='backend-api/files'], img[src*='oaiusercontent'], img[alt*='Generated']",
    # the "generating / stop" state — while present, still rendering
    "stop_btn": "button[data-testid='stop-button'], button[aria-label*='Stop']",
    # top-right auth button — PRESENT means logged OUT (composer alone is not proof).
    # Locale-agnostic: match the auth href + EN/IT button text.
    "login_btn": "a[href*='/auth/login'], a[href*='/log-in'], "
                 "button:has-text('Log in'), button:has-text('Accedi'), "
                 "button:has-text('Sign up'), button:has-text('Registrati')",
    # cookie-consent banner buttons (overlay blocks clicks until dismissed) — EN/IT
    "cookie_dismiss": "button:has-text('Reject non-essential'), button:has-text('Accept all'), "
                      "button:has-text('Rifiuta opzionali'), button:has-text('Accetta tutto')",
}


def log(msg):
    print(f"[chatgpt-worker] {msg}", flush=True)


def jitter(a=None, b=None):
    lo = a if a is not None else JITTER[0]
    hi = b if b is not None else JITTER[1]
    time.sleep(random.uniform(lo, hi))


def _import_playwright():
    """Prefer Patchright (stealth); fall back to vanilla Playwright."""
    try:
        from patchright.sync_api import sync_playwright  # type: ignore
        log("Patchright ACTIVE (CDP-detection bypass on)")
        return sync_playwright, True
    except ImportError:
        from playwright.sync_api import sync_playwright  # type: ignore
        log("WARNING: patchright not installed — using vanilla Playwright "
            "(higher bot-detection risk). pip install patchright && patchright install chromium")
        return sync_playwright, False


def launch(p):
    args = list(CHROME_ARGS)
    if USER_DATA_DIR:
        # Drive the operator's real Chrome data dir + a specific sub-profile.
        udd = USER_DATA_DIR
        if PROFILE_DIRECTORY:
            args.append(f"--profile-directory={PROFILE_DIRECTORY}")
    else:
        udd = PROFILE_DIR
        os.makedirs(udd, exist_ok=True)
    kwargs = {
        "user_data_dir": udd,
        "channel": CHROME_CHANNEL,
        "ignore_default_args": _IGNORE_DEFAULT_ARGS,
        "headless": False,
        "viewport": {"width": 1280, "height": 900},
        "args": args,
    }
    ctx = p.chromium.launch_persistent_context(**kwargs)
    inject_cookies(ctx)
    page = ctx.pages[0] if ctx.pages else ctx.new_page()
    return ctx, page


def inject_cookies(ctx):
    """Add the netlog-captured plaintext cookies (ABE-immune login). No-op if the
    file is absent — falls back to whatever the profile already holds."""
    if not os.path.exists(COOKIES_FILE):
        return
    try:
        with open(COOKIES_FILE, "r", encoding="utf-8") as f:
            cookies = json.load(f)
        ctx.add_cookies(cookies)
        log(f"injected {len(cookies)} session cookies")
    except Exception as e:
        log(f"cookie inject failed: {e}")


def dismiss_cookie_banner(page):
    """Consent banner overlays the composer and blocks clicks. Dismiss if present."""
    try:
        btn = page.locator(SEL["cookie_dismiss"]).first
        if btn.count() > 0 and btn.is_visible():
            btn.click(timeout=3000)
            jitter(0.4, 0.9)
    except Exception:
        pass


def is_logged_in(page):
    """Logged in = composer present AND no top-right Log-in button.

    NOTE: the composer alone is NOT proof — logged-out ChatGPT shows an
    anonymous composer, but image-gen + file upload require login.
    """
    try:
        page.wait_for_selector(SEL["composer"], timeout=8000)
    except Exception:
        return False
    try:
        return page.locator(SEL["login_btn"]).count() == 0
    except Exception:
        return True


def _wait_generation(page, timeout_s):
    """Wait until a fresh generated image is present AND rendering finished.

    Returns the image element handle, or None on timeout.
    Strategy: count gen imgs BEFORE submit (done by caller via baseline), then
    here poll until a NEW img shows AND the stop-button disappears.
    """
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        # still generating?
        generating = page.locator(SEL["stop_btn"]).count() > 0
        imgs = page.locator(SEL["gen_img"])
        n = imgs.count()
        if n > 0:
            last = imgs.nth(n - 1)
            if not generating:
                # give the src a beat to settle from blob -> final
                jitter(0.8, 1.6)
                return last
        time.sleep(1.5)
    return last  # may be a partial; caller validates


def _download_img(page, img_handle, out_path):
    """Fetch the image bytes in-page (handles blob: + oaiusercontent auth) and save."""
    src = img_handle.get_attribute("src")
    if not src:
        raise RuntimeError("generated img has no src")
    # in-page fetch -> base64 (works for blob: URLs and same-origin authed URLs)
    b64 = page.evaluate(
        """async (url) => {
            const r = await fetch(url);
            const buf = await r.arrayBuffer();
            let bin = '';
            const bytes = new Uint8Array(buf);
            for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
            return btoa(bin);
        }""",
        src,
    )
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "wb") as f:
        f.write(base64.b64decode(b64))
    return src


def generate(page, prompt, ref_paths, out_path, gen_timeout_s=GEN_TIMEOUT_S):
    """One generation. Fresh chat, optional refs, prompt, wait, download to out_path.
    Returns out_path. Raises on timeout / not-logged-in / no image."""
    page.goto(CHATGPT_URL, wait_until="domcontentloaded")
    dismiss_cookie_banner(page)
    if not is_logged_in(page):
        raise RuntimeError("session expired — run --refresh-cookies")
    jitter()
    baseline = page.locator(SEL["gen_img"]).count()
    for rp in ref_paths or []:
        if not os.path.exists(rp):
            raise FileNotFoundError(f"ref image not found: {rp}")
    if ref_paths:
        page.locator(SEL["file_input"]).first.set_input_files(ref_paths)
        jitter(2.0, 4.0)
    comp = page.locator(SEL["composer"]).first
    comp.click(); jitter(0.4, 0.9); comp.fill("")
    comp.type(prompt, delay=6)
    jitter()
    submitted = False
    try:
        send = page.locator(SEL["send"]).first
        if send.count() > 0 and send.is_enabled():
            send.click(); submitted = True
    except Exception:
        pass
    if not submitted:
        comp.press("Enter")
    deadline = time.time() + gen_timeout_s
    img = None
    while time.time() < deadline:
        img = _wait_generation(page, timeout_s=min(15, int(deadline - time.time()) + 1))
        cur = page.locator(SEL["gen_img"]).count()
        if img is not None and cur > baseline and page.locator(SEL["stop_btn"]).count() == 0:
            break
    if img is None or page.locator(SEL["gen_img"]).count() <= baseline:
        raise TimeoutError(f"no new image after {gen_timeout_s}s")
    _download_img(page, img, out_path)
    return out_path
