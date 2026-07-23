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

# generation can be slow (thinking model + image render). Raised 180 -> 300 after
# nodes failed with "no generated image" while the image WAS present in the chat.
GEN_TIMEOUT_S = int(os.environ.get("CHATGPT_GEN_TIMEOUT_S", "300"))
# If a generated image is on screen and UNCHANGED this long but the stop/generating
# indicator never clears, accept it anyway — that combination means the stop button
# is wedged (or its selector drifted), not that the render is still running.
STALL_ACCEPT_S = int(os.environ.get("CHATGPT_STALL_ACCEPT_S", "45"))
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
    # a generated image inside the assistant turn. ChatGPT serves gen OUTPUT from
    # backend-api/estuary/content?id=file-... (primary), sometimes oaiusercontent.
    # DO NOT match backend-api/files — that is the USER-UPLOADED reference image;
    # matching it made the worker capture the attached reference instead of the
    # generation on a slow/still-rendering turn (node 3401 returned the parent).
    "gen_img": "img[src*='estuary/content'], img[src*='oaiusercontent'], img[alt*='Generated']",
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


# ROOT-CAUSE FIX (node 3401/3409 returned the reference): the uploaded REFERENCE
# image and the GENERATED image are BOTH served from backend-api/estuary/content,
# so a src-substring selector cannot tell them apart. The reference appears in the
# DOM (inside the role="user" turn) BEFORE generation starts, and the old baseline/
# count logic captured it during the pre-generation window. Positive-detect the
# generation instead: a gen image is (a) NOT inside a role="user" turn, and (b) has
# alt starting "Generated image" OR is an estuary/oaiusercontent image outside the
# user turn. The uploaded reference (role="user", alt=filename) is excluded, so the
# pre-gen window yields no candidate and the worker waits for the real output.
_GEN_QUERY = r"""
() => {
  const isGen = (im) => {
    const role = im.closest('[data-message-author-role]')
                   ?.getAttribute('data-message-author-role');
    if (role === 'user') return false;                 // uploaded reference lives here
    const alt = im.alt || '';
    if (alt.startsWith('Generated image')) return true;
    const src = im.currentSrc || im.src || '';
    return /estuary\/content|oaiusercontent/.test(src);
  };
  const cands = [...document.querySelectorAll('img')]
    .filter(isGen)
    .map(im => { const r = im.getBoundingClientRect();
                 return { src: im.currentSrc || im.src, area: r.width * r.height }; })
    .filter(c => c.src && c.area > 5000);              // the full render, not a thumb
  cands.sort((a, b) => b.area - a.area);
  return cands.length ? cands[0].src : null;
};
"""


def _find_gen_src(page):
    """Src of the current generated image (NOT the uploaded reference), or None."""
    return page.evaluate(_GEN_QUERY)


def _download_img(page, src, out_path):
    """Fetch the image bytes in-page (same-origin authed URL) and save."""
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
    for rp in ref_paths or []:
        if not os.path.exists(rp):
            raise FileNotFoundError(f"ref image not found: {rp}")
    if ref_paths:
        page.locator(SEL["file_input"]).first.set_input_files(ref_paths)
        jitter(2.0, 4.0)
    comp = page.locator(SEL["composer"]).first
    comp.click(); jitter(0.4, 0.9)
    # Insert the whole prompt in ONE op — char-by-char .type() times out on the
    # ProseMirror editor for the platform's long (2000+ char) prompts. insert_text
    # fires a single insertText event ProseMirror handles instantly.
    try:
        comp.evaluate("el => { el.focus(); }")
        page.keyboard.insert_text(prompt)
    except Exception:
        comp.fill(prompt)
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
    gen_src = None
    ever_cand = False          # did a generation image EVER appear?
    last_cand = None           # last candidate src seen
    cand_since = None          # when the CURRENT candidate src first appeared
    while time.time() < deadline:
        stop_n = page.locator(SEL["stop_btn"]).count()
        cand = _find_gen_src(page)
        if cand:
            ever_cand = True
            if cand != last_cand:
                last_cand, cand_since = cand, time.time()
        else:
            last_cand, cand_since = None, None
        if cand and stop_n == 0:
            jitter(0.8, 1.6)                    # let the src settle (blob -> final)
            gen_src = _find_gen_src(page) or cand
            break
        # Wedge escape: the image is up and has not changed for STALL_ACCEPT_S, yet
        # the stop/generating indicator never cleared. That is a stuck (or drifted)
        # stop-button selector, not an in-progress render — take the image. Nodes
        # were failing with "no generated image" while the picture sat in the chat.
        if cand and cand_since and (time.time() - cand_since) >= STALL_ACCEPT_S:
            log(f"stop indicator still present (count={stop_n}) but gen image stable "
                f"{STALL_ACCEPT_S}s — accepting it")
            gen_src = cand
            break
        time.sleep(1.2)
    if not gen_src:
        # Do NOT fall back to whatever image is on the page — that is how the
        # uploaded reference got uploaded as the "generation". Fail the node.
        # Diagnostics so the NEXT failure says WHICH condition wedged.
        try:
            stop_n = page.locator(SEL["stop_btn"]).count()
        except Exception:
            stop_n = -1
        try:
            inv = page.evaluate(
                "() => { const a=[...document.querySelectorAll('img')];"
                " return {total:a.length, user:a.filter(i=>i.closest('[data-message-author-role]')"
                "?.getAttribute('data-message-author-role')==='user').length,"
                " big:a.filter(i=>{const r=i.getBoundingClientRect(); return r.width*r.height>5000;}).length}; }")
        except Exception as _e:
            inv = f"<inventory failed: {_e}>"
        raise TimeoutError(
            f"no generated image after {gen_timeout_s}s — ChatGPT produced no "
            f"output (refusal or slow render); refusing to upload the reference "
            f"[diag: stop_btn={stop_n} ever_candidate={ever_cand} imgs={inv}]")
    _download_img(page, gen_src, out_path)
    _tone_correct(out_path)
    return out_path


def _tone_correct(out_path):
    """De-yellow the just-generated ChatGPT image IN PLACE, before it is handed to
    the platform. GPT-4o output carries a warm/yellow cast; tone_correct removes it
    (reverse-engineered from gpt-tone.com — per-channel auto-levels). Fail-safe: any
    error leaves the original file untouched. Toggle with TONE_CORRECT_CHATGPT=0."""
    try:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # ensure sibling import
        import tone_correct
        if not tone_correct.is_enabled():
            return
        with open(out_path, "rb") as f:
            raw = f.read()
        ext = os.path.splitext(out_path)[1].lstrip(".").upper() or "PNG"
        fixed = tone_correct.correct_bytes(raw, fmt=ext)
        if fixed and fixed != raw:
            # atomic write — a mid-write failure must not corrupt/lose the download
            tmp = out_path + ".tone.tmp"
            with open(tmp, "wb") as f:
                f.write(fixed)
            os.replace(tmp, out_path)
            log(f"tone-correct applied ({len(raw)}->{len(fixed)} bytes) {os.path.basename(out_path)}")
    except Exception as e:
        log(f"tone-correct skipped ({e})")
