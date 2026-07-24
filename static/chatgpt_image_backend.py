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
# stream_status can read COMPLETE (text stream done) BEFORE the image finishes
# rendering into the message. Keep polling for the image this long after COMPLETE
# before concluding there is none (refusal / usage cap).
POST_COMPLETE_GRACE_S = int(os.environ.get("CHATGPT_POST_COMPLETE_GRACE_S", "90"))
# How many times to click ChatGPT's "Retry" on a transient generation error
# ("Something went wrong") before failing the node.
ERROR_RETRIES = int(os.environ.get("CHATGPT_ERROR_RETRIES", "3"))
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
    # transient generation error bubble ("Something went wrong… Retry") — ChatGPT
    # server-errors mid-render; the Retry button regenerates the SAME turn.
    "retry_btn": "button:has-text('Retry'), button:has-text('Riprova'), "
                 "button[data-testid='regenerate-thread-error-button'], "
                 "button[aria-label*='Retry']",
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
(excl) => {
  const ex = new Set(excl || []);
  const isGen = (im) => {
    const role = im.closest('[data-message-author-role]')
                   ?.getAttribute('data-message-author-role');
    if (role === 'user') return false;                 // uploaded reference lives here
    const src = im.currentSrc || im.src || '';
    if (ex.has(src)) return false;                     // present BEFORE submit = reference
    const alt = im.alt || '';
    if (alt.startsWith('Generated image')) return true;
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


def _find_gen_src(page, exclude=None):
    """Src of the current generated image (NOT the uploaded reference), or None.
    `exclude` = srcs present before submit (the reference preview) — never picked."""
    return page.evaluate(_GEN_QUERY, list(exclude or []))


# Loose fallback: the largest image that is NOT inside a role=user turn and not a
# tiny avatar/icon. Only safe to use when the turn is COMPLETE (render finished),
# because before completion the strict src/alt gate is what keeps us from grabbing
# the still-loading reference. Handles ChatGPT serving a generation from a CDN
# path or with an alt text the strict _GEN_QUERY doesn't recognize (node 3613:
# stream_status=COMPLETE, a big image present, but ever_candidate=False).
_GEN_QUERY_LOOSE = r"""
(excl) => {
  const ex = new Set(excl || []);
  const cands = [...document.querySelectorAll('img')]
    .filter(im => {
      const role = im.closest('[data-message-author-role]')
                     ?.getAttribute('data-message-author-role');
      if (role === 'user') return false;                 // uploaded reference
      const src = im.currentSrc || im.src || '';
      if (!src || src.startsWith('data:')) return false;  // no inline placeholders
      if (ex.has(src)) return false;                      // present BEFORE submit = reference
      if (/\/avatar|profile|favicon|logo|sprite/i.test(src)) return false;
      return true;
    })
    .map(im => { const r = im.getBoundingClientRect();
                 return { src: im.currentSrc || im.src, area: r.width * r.height }; })
    .filter(c => c.area > 40000);                        // a real render, not an icon
  cands.sort((a, b) => b.area - a.area);
  return cands.length ? cands[0].src : null;
};
"""


def _find_gen_src_loose(page, exclude=None):
    """Largest non-user, non-icon image not present before submit. COMPLETE only."""
    try:
        return page.evaluate(_GEN_QUERY_LOOSE, list(exclude or []))
    except Exception:
        return None


def _all_img_srcs(page):
    """Every <img> src currently on the page (for the pre-submit reference snapshot)."""
    try:
        return page.evaluate(
            "() => [...document.querySelectorAll('img')]"
            ".map(i => i.currentSrc || i.src || '').filter(Boolean)")
    except Exception:
        return []


def _looks_like_reference(out_path, ref_paths):
    """True if the downloaded image is pixel-(near)-identical to an uploaded
    reference at the SAME dimensions — i.e. ChatGPT served the input back instead
    of a generation. A real generation, even one that closely follows the
    reference, differs by far more than this (it is a fresh render, not the same
    pixels). Pillow-only; never raises."""
    try:
        from PIL import Image, ImageChops, ImageStat
        a = Image.open(out_path).convert("RGB")
        for rp in ref_paths or []:
            try:
                b = Image.open(rp).convert("RGB")
            except Exception:
                continue
            if a.size != b.size:
                continue
            mad = sum(ImageStat.Stat(ImageChops.difference(a, b)).mean) / 3.0
            if mad < 1.0:
                return True
    except Exception:
        pass
    return False


def _conversation_id(page):
    """The active conversation UUID from the URL (chatgpt.com/c/<uuid>), or None.
    The URL only gains /c/<id> once the turn has actually started server-side."""
    try:
        url = page.url or ""
        if "/c/" not in url:
            return None
        cid = url.rstrip("/").split("/c/")[-1].split("?")[0].strip()
        return cid or None
    except Exception:
        return None


def _stream_status(page, cid):
    """ChatGPT's OWN turn status: GET /backend-api/conversation/<id>/stream_status
    -> {"status": "COMPLETE"|...}. Returns the status string, or None if the
    endpoint is unavailable.

    This is the authoritative "is the turn finished" signal and replaces the
    stop-button heuristic. Measured 2026-07-23: SEL['stop_btn'] matches a
    persistent control — it read 1 before the turn began AND while the finished
    image was already on screen — so gating on it is a race that intermittently
    failed nodes whose image was actually present. stream_status returned a clean
    200 {"status":"COMPLETE"} on the same turn.

    (The full conversation JSON is NOT usable here: /backend-api/conversation/<id>
    returns 404 conversation_inaccessible for these worker chats, and the
    conversations list stays empty because they live under a Project.)
    """
    if not cid:
        return None
    try:
        return page.evaluate(
            """async (cid) => {
                try {
                    const r = await fetch('/backend-api/conversation/' + cid + '/stream_status',
                                          {credentials: 'include'});
                    if (!r.ok) return null;
                    const j = await r.json();
                    return (j && j.status) ? String(j.status) : null;
                } catch (e) { return null; }
            }""",
            cid,
        )
    except Exception:
        return None


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
        # One reload + recheck before failing — the login probe flickers (a
        # navigation/network blip returns a transient False, then the very next
        # node succeeds). Don't fail a node on a one-shot false negative.
        log("login check failed — reloading once before giving up")
        try:
            page.goto(CHATGPT_URL, wait_until="domcontentloaded")
            dismiss_cookie_banner(page)
            time.sleep(2)
        except Exception:
            pass
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
    # Snapshot every image on the page BEFORE submit — these include the uploaded
    # reference preview (served from estuary/content with no user-role wrapper, so
    # the strict detector would otherwise pick it). The generation is a NEW image
    # that appears AFTER submit; anything in this set is never a candidate.
    pre_srcs = set(_all_img_srcs(page))
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
    last_status = None         # last stream_status seen (authoritative turn state)
    stop_n = -1
    complete_since = None      # when stream_status first read COMPLETE
    retries_left = ERROR_RETRIES
    while time.time() < deadline:
        # ChatGPT server-errors mid-generation ("Something went wrong … Retry").
        # Click Retry and reset this attempt instead of waiting out the timeout.
        if gen_src is None and retries_left > 0:
            try:
                rb = page.locator(SEL["retry_btn"]).first
                if rb.count() > 0 and rb.is_visible():
                    retries_left -= 1
                    log(f"ChatGPT error bubble — clicking Retry "
                        f"({ERROR_RETRIES - retries_left}/{ERROR_RETRIES})")
                    rb.click()
                    # reset per-attempt state; give the fresh attempt full time
                    deadline = time.time() + gen_timeout_s
                    last_cand = cand_since = last_status = complete_since = None
                    ever_cand = False
                    time.sleep(3)
                    continue
            except Exception:
                pass
        cand = _find_gen_src(page, pre_srcs)
        if cand:
            ever_cand = True
            if cand != last_cand:
                last_cand, cand_since = cand, time.time()
        else:
            last_cand, cand_since = None, None

        # ChatGPT's own turn status — read every iteration, not just when a
        # candidate is present (node 3613: COMPLETE + a big image on screen but
        # the strict detector never matched, so the loop must still act on it).
        status = _stream_status(page, _conversation_id(page))
        if status:
            last_status = status
        complete = status is not None and status.upper() == "COMPLETE"
        if complete and complete_since is None:
            complete_since = time.time()

        if cand:
            if complete:
                jitter(0.8, 1.6)            # let the src settle (blob -> final)
                gen_src = _find_gen_src(page, pre_srcs) or cand
                break
            if status is None:
                # stream_status unavailable: legacy stop-button gate.
                stop_n = page.locator(SEL["stop_btn"]).count()
                if stop_n == 0:
                    jitter(0.8, 1.6)
                    gen_src = _find_gen_src(page, pre_srcs) or cand
                    break
            # Last resort: image up + unchanged for STALL_ACCEPT_S while nothing
            # ever said "done" — take it rather than fail a node whose picture is
            # sitting in the chat.
            if cand_since and (time.time() - cand_since) >= STALL_ACCEPT_S:
                log(f"gen image stable {STALL_ACCEPT_S}s (stream_status={last_status}, "
                    f"stop={stop_n}) — accepting it")
                gen_src = cand
                break
        elif complete:
            # stream_status COMPLETE means the TEXT stream finished — the image can
            # still render into the message a while later (operator: "sometimes the
            # image takes time to appear in the UI"). So keep polling (strict, then
            # loose) for a generous window after COMPLETE before concluding there is
            # genuinely no image (content refusal / usage cap).
            loose = _find_gen_src_loose(page, pre_srcs)
            if loose:
                log("turn COMPLETE, image appeared — using largest non-user image "
                    "(loose fallback)")
                gen_src = loose
                break
            if complete_since and (time.time() - complete_since) >= POST_COMPLETE_GRACE_S:
                log(f"turn COMPLETE for {POST_COMPLETE_GRACE_S}s with no image — "
                    f"giving up (likely refusal / usage cap)")
                break
        time.sleep(1.5)
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
        # The assistant's last text — when no image is produced this usually SAYS
        # why (rate/usage cap, content refusal). Surfaces cap-vs-refusal-vs-slow.
        try:
            reply = page.evaluate(
                "() => { const t=[...document.querySelectorAll("
                "'[data-message-author-role=assistant]')]; const e=t[t.length-1];"
                " return e ? (e.innerText||'').slice(0,160) : ''; }")
        except Exception:
            reply = ""
        try:
            err_bubble = page.locator(SEL["retry_btn"]).first.count() > 0
        except Exception:
            err_bubble = False
        _cid = _conversation_id(page)
        raise TimeoutError(
            f"no generated image after {gen_timeout_s}s — ChatGPT produced no "
            f"output (refusal / usage cap / slow render); refusing to upload the reference "
            f"[diag: stream_status={_stream_status(page, _cid)} last_seen={last_status} "
            f"conv={_cid} stop_btn={stop_n} ever_candidate={ever_cand} imgs={inv} "
            f"retries_used={ERROR_RETRIES - retries_left} error_bubble={err_bubble} "
            f"reply={reply!r}]")
    _download_img(page, gen_src, out_path)
    # Final safety net: never hand the platform an input image. If what we captured
    # is pixel-(near)-identical to an uploaded reference, ChatGPT echoed the input
    # (refusal / detection slip) — fail the node instead of uploading the reference.
    if _looks_like_reference(out_path, ref_paths):
        try:
            os.remove(out_path)
        except OSError:
            pass
        raise TimeoutError(
            "captured image is identical to an uploaded reference — ChatGPT echoed "
            "the input instead of generating; refusing to upload the reference")
    _tone_correct(out_path, page)
    return out_path


# --- Tone correction ---------------------------------------------------------
# GPT-4o output carries a warm/yellow cast. We remove it before the image is
# handed to the platform. PRIMARY path drives the real gpt-tone.com in a second
# tab of the SAME worker browser — the real logged-in profile passes the site's
# reCAPTCHA/App-Check where a headless scraper gets a 403, and it returns the
# site's EXACT output (our local per-channel-levels approximation did not match
# on every image). FALLBACK is the local tone_correct module so an image is never
# left uncorrected when the site is unreachable/throttled.
TONE_SITE_URL = os.environ.get("CHATGPT_TONE_SITE_URL", "https://gpt-tone.com/")
TONE_SITE_TIMEOUT_S = int(os.environ.get("CHATGPT_TONE_SITE_TIMEOUT_S", "90"))
_TONE_TAB = None


def _use_site() -> bool:
    return os.environ.get("CHATGPT_TONE_USE_SITE", "1").strip().lower() not in ("0", "false", "no", "off")


def _tone_enabled() -> bool:
    try:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        import tone_correct
        return tone_correct.is_enabled()
    except Exception:
        # tone_correct governs the master toggle; if it can't be imported, only
        # the site path is available — respect TONE_CORRECT_CHATGPT directly.
        return os.environ.get("TONE_CORRECT_CHATGPT", "1").strip().lower() not in ("0", "false", "no", "off")


def _get_tone_tab(ctx):
    """A dedicated second tab in the worker's context for gpt-tone.com, reused
    across images (keeps the site's App-Check session warm, fewer token mints)."""
    global _TONE_TAB
    try:
        if _TONE_TAB is not None and not _TONE_TAB.is_closed():
            return _TONE_TAB
    except Exception:
        pass
    _TONE_TAB = ctx.new_page()
    return _TONE_TAB


def _atomic_write(out_path, data):
    tmp = out_path + ".tone.tmp"
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, out_path)


def _tone_correct_via_site(out_path, page):
    """Correct out_path IN PLACE using gpt-tone.com in a second tab. Returns True
    on success, False if the site returned nothing (throttle/block/timeout)."""
    tab = _get_tone_tab(page.context)
    # Fresh load each call: resets their SPA so no stale prior result lingers.
    tab.goto(TONE_SITE_URL, wait_until="networkidle", timeout=45000)
    time.sleep(1.5)
    tab.locator("input[type=file]").first.set_input_files(out_path)
    deadline = time.time() + TONE_SITE_TIMEOUT_S
    src = None
    while time.time() < deadline:
        try:
            srcs = tab.eval_on_selector_all("img", "els=>els.map(e=>(e.currentSrc||e.src||'').slice(0,32))")
        except Exception:
            srcs = []
        if any(s.startswith("data:image/png") for s in srcs):
            src = tab.locator('img[src^="data:image/png"]').first.get_attribute("src")
            break
        time.sleep(1.5)
    if not src or "," not in src:
        return False
    data = base64.b64decode(src.split(",", 1)[1])
    if not data:
        return False
    _atomic_write(out_path, data)
    log(f"tone-correct via gpt-tone.com applied ({len(data)} bytes) {os.path.basename(out_path)}")
    return True


def _tone_correct_local(out_path):
    """Fallback: local per-channel-levels correction (approximation of the site)."""
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import tone_correct
    with open(out_path, "rb") as f:
        raw = f.read()
    ext = os.path.splitext(out_path)[1].lstrip(".").upper() or "PNG"
    fixed = tone_correct.correct_bytes(raw, fmt=ext)
    if fixed and fixed != raw:
        _atomic_write(out_path, fixed)
        log(f"tone-correct (local fallback) applied ({len(raw)}->{len(fixed)} bytes) "
            f"{os.path.basename(out_path)}")


def _tone_correct(out_path, page=None):
    """De-yellow the just-generated image IN PLACE. Site first (exact), local
    fallback. Fail-safe: any error leaves the original file untouched. Master
    toggle TONE_CORRECT_CHATGPT=0; CHATGPT_TONE_USE_SITE=0 forces local-only."""
    if not _tone_enabled():
        return
    if page is not None and _use_site():
        try:
            if _tone_correct_via_site(out_path, page):
                return
            log("tone-correct: gpt-tone.com returned no result — using local fallback")
        except Exception as e:
            log(f"tone-correct: site path failed ({e}) — using local fallback")
    try:
        _tone_correct_local(out_path)
    except Exception as e:
        log(f"tone-correct skipped ({e})")
