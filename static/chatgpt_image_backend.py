#!/usr/bin/env python3
"""ChatGPT browser drive core (Patchright). Login via netlog-captured cookie
injection (ABE-immune), attach reference images, type prompt, wait for the
generated image (served from estuary/content), download it. Knows nothing about
the platform job format — takes a prompt + ref paths, produces a PNG."""

import base64
import json
import os
import random
import re
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
# The worker tab's live stream can go stale: the turn finishes server-side (a
# fresh browser shows the image/error) but the worker's DOM stays "Thinking" /
# stream_status stuck. If no image after this long while NOT COMPLETE, reload the
# conversation page (== opening a fresh browser) to pull the true server state.
STUCK_RELOAD_S = int(os.environ.get("CHATGPT_STUCK_RELOAD_S", "100"))
STUCK_RELOADS = int(os.environ.get("CHATGPT_STUCK_RELOADS", "2"))
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
    # transient generation error bubble — ChatGPT server-errors mid-render; the
    # button regenerates the SAME turn. Seen as "Retry" AND "Try again" ("Image
    # generation failed … Try again"); IT "Riprova".
    "retry_btn": "button:has-text('Try again'), button:has-text('Retry'), "
                 "button:has-text('Riprova'), button:has-text('Prova di nuovo'), "
                 "button[data-testid='regenerate-thread-error-button'], "
                 "button[aria-label*='Retry'], button[aria-label*='Try again']",
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
    # 20s (was 8s): under load the composer can take >8s to attach, which
    # produced frequent false "session expired" flickers on an account that is
    # actually logged in. If the login button is already visible, don't wait.
    try:
        if page.locator(SEL["login_btn"]).count() > 0:
            return False
    except Exception:
        pass
    try:
        page.wait_for_selector(SEL["composer"], timeout=20000)
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


def _attach_reference_files(page, ref_paths, timeout_s=60):
    """Attach references in order and wait until ChatGPT shows every preview."""
    if not ref_paths:
        return
    before = set(_all_img_srcs(page))
    file_input = page.locator(SEL["file_input"]).first
    file_input.set_input_files(ref_paths)
    attached_names = file_input.evaluate(
        "el => Array.from(el.files || []).map(file => file.name)"
    )
    expected_names = [os.path.basename(path) for path in ref_paths]
    if attached_names != expected_names:
        raise RuntimeError(
            "ChatGPT reference attachment order could not be verified: "
            f"expected {expected_names}, got {attached_names}"
        )

    deadline = time.time() + timeout_s
    while time.time() < deadline:
        current = _all_img_srcs(page)
        if sum(1 for src in current if src not in before) >= len(ref_paths):
            return
        time.sleep(0.5)
    raise RuntimeError(
        f"ChatGPT showed fewer than {len(ref_paths)} uploaded reference previews"
    )


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


# ChatGPT's transient image-tool failure, worded as assistant TEXT (no Retry
# button). Seen EN "Something went wrong" and IT "Non sono riuscito a generare
# l'immagine perché si è verificato un errore durante la generazione". Matches
# ERROR wording only — NOT a content refusal ("I can't create that…"), which
# should fail fast rather than burn retries.
_GEN_ERROR_RE = re.compile(
    r"(image generation failed|generazione.*non riuscit|went wrong|"
    r"an error occurred|error (occurred|during|while)|errore|si è verificato|"
    r"problem generating|couldn'?t generate the image|"
    r"non sono riuscit\w+ a genera)", re.I)


def _last_assistant_text(page):
    try:
        return page.evaluate(
            "() => { const t=[...document.querySelectorAll("
            "'[data-message-author-role=assistant]')]; const e=t[t.length-1];"
            " return e ? (e.innerText||'') : ''; }") or ""
    except Exception:
        return ""


def _reply_is_gen_error(page):
    """True if the assistant's last message reads as a transient generation error
    (retryable), not a content refusal."""
    return bool(_GEN_ERROR_RE.search(_last_assistant_text(page)))


def _gen_failed(page):
    """The MATCHED error text when a real generation-ERROR indicator is visible in
    the recent turn ("Image generation failed", "Something went wrong", "si è
    verificato un errore"…), else "". Truthy/falsy, so every existing gate still
    reads as a boolean — but the caller can now LOG what matched.

    Used to gate the Retry-button click so we never click a normal regenerate/
    "Try again" affordance on a healthy turn (that false-click wasted a
    generation). Scans the tail of the page text, not just the button.

    DIAGNOSTIC (node 4495, 2026-08-03): this fired TWICE on a chat that went on to
    produce a good image, so the scan is suspected of matching page text that is
    not this turn's error. The returned snippet is the evidence needed to tighten
    it — do not narrow the regex until a real match string has been logged."""
    try:
        tail = page.evaluate("() => (document.body.innerText || '').slice(-1500)")
    except Exception:
        tail = ""
    m = _GEN_ERROR_RE.search(tail or "")
    if not m:
        return ""
    return " ".join(tail[max(0, m.start() - 60):m.end() + 60].split())


def _resubmit(page, prompt):
    """Re-send the prompt in the same chat — the retry for a text-form generation
    error, where ChatGPT offers no Retry button. Refs already live in the
    conversation, so 'the attached image' still resolves."""
    try:
        comp = page.locator(SEL["composer"]).first
        comp.click()
        try:
            comp.evaluate("el => el.focus()")
            page.keyboard.insert_text(prompt)
        except Exception:
            comp.fill(prompt)
        time.sleep(0.5)
        s = page.locator(SEL["send"]).first
        if s.count() > 0 and s.is_enabled():
            s.click()
        else:
            comp.press("Enter")
        return True
    except Exception as e:
        log(f"resubmit failed: {e}")
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


def _is_provisional_cid(cid):
    """ChatGPT's URL briefly carries a client-side placeholder id ('WEB:<uuid>')
    before the server swaps in the real conversation id. The placeholder is NOT
    navigable: /c/WEB:<uuid> lands on a brand-new empty chat. Node 4655
    (2026-08-07) pinned one — every stale-tab reload then opened a fresh chat and
    the detector stared at an empty conversation while the finished image sat in
    the real one. A placeholder must never be pinned or navigated to."""
    return bool(cid) and str(cid).upper().startswith("WEB:")


def _conv_url(cid):
    """Full chat URL for a conversation id. Logged on every node so the operator
    can open the EXACT chat the worker is watching and see it for themselves."""
    return f"{CHATGPT_URL.rstrip('/')}/c/{cid}" if cid else ""


def _goto_conv(page, cid, timeout_ms=45000):
    """Navigate to the PINNED conversation — used instead of page.reload() so the
    tab can never come back on a different (or brand-new) chat."""
    if not cid or _is_provisional_cid(cid):
        # A placeholder URL opens a NEW chat instead of the pinned one — refuse,
        # so the caller falls back to a plain reload of the current tab.
        return False
    try:
        page.goto(_conv_url(cid), wait_until="domcontentloaded", timeout=timeout_ms)
        return True
    except Exception as e:
        log(f"goto chat {_conv_url(cid)} failed: {e}")
        return False


def _ensure_on_conv(page, cid, why=""):
    """True if the tab is on the pinned conversation, navigating back if it drifted.

    ROOT-CAUSE FIX (node 4495, 2026-08-03): recovery used to act on whatever tab
    was on screen. After a reload + Retry the tab was no longer on the node's
    chat, so `_resubmit` typed into an EMPTY composer and opened a SECOND
    conversation (6a70e062, born 18:39:30Z). The real generation finished in the
    ORIGINAL chat (6a70df41, born 18:34:41Z — 289s earlier, the one the operator
    could still see), which the worker had stopped watching. It then polled the
    wrong chat, found nothing, and failed the node. Every recovery action now runs
    INSIDE the pinned chat, or does not run at all."""
    if not cid:
        return True                       # nothing pinned yet — this tab is it
    cur = _conversation_id(page)
    if cur == cid:
        return True
    log(f"tab drifted off the pinned chat (now {_conv_url(cur) or 'a new/empty chat'})"
        f"{' before ' + why if why else ''} — returning to {_conv_url(cid)}")
    if not _goto_conv(page, cid):
        return False
    time.sleep(3)
    return _conversation_id(page) == cid


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
        _attach_reference_files(page, ref_paths)
        jitter(0.5, 1.0)
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
    reloads_left = STUCK_RELOADS
    attempt_start = time.time()  # reset on submit / retry / reload
    pinned_cid = None            # the chat this node lives in — never leave it
    while time.time() < deadline:
        # Pin the conversation the moment the URL gains /c/<id> (the turn has
        # started server-side) and LOG it, so the operator can open the exact chat
        # the worker is watching instead of hunting for it.
        cid_now = _conversation_id(page)
        if cid_now and not _is_provisional_cid(cid_now):
            if pinned_cid is None or _is_provisional_cid(pinned_cid):
                # Pin only the REAL server-assigned id. The URL's early
                # 'WEB:<uuid>' placeholder is not navigable (node 4655) — wait
                # for the swap, then pin (or re-pin over a placeholder).
                pinned_cid = cid_now
                log(f"chat: {_conv_url(pinned_cid)}")
        elif pinned_cid is None and cid_now:
            pinned_cid = cid_now   # placeholder: keeps the log honest, replaced
            log(f"chat: {_conv_url(pinned_cid)} (provisional id — waiting for the real one)")
        # Stale-tab recovery: the worker's live stream can hang — the turn finishes
        # server-side (a fresh browser shows the image/error) but this tab stays
        # "Thinking" / stream_status stuck non-COMPLETE. Reload the conversation
        # (== opening a fresh browser) to pull the true server state, then re-detect.
        if (gen_src is None and reloads_left > 0 and not complete_since
                and (time.time() - attempt_start) >= STUCK_RELOAD_S):
            reloads_left -= 1
            log(f"tab stuck {STUCK_RELOAD_S}s (status={last_status}, no image) — "
                f"reloading {_conv_url(pinned_cid) or 'the current chat'} "
                f"({STUCK_RELOADS - reloads_left}/{STUCK_RELOADS})")
            # goto the PINNED chat, not reload() — a reload that lands on a
            # redirect is how the tab drifted off the node's conversation.
            if not (pinned_cid and _goto_conv(page, pinned_cid)):
                try:
                    page.reload(wait_until="domcontentloaded", timeout=45000)
                except Exception as _re:
                    log(f"reload failed: {_re}")
            last_cand = cand_since = last_status = complete_since = None
            ever_cand = False
            attempt_start = time.time()
            time.sleep(3)
            continue
        # ChatGPT generation error ("Something went wrong … Retry" / "Image
        # generation failed … Try again" / IT "…si è verificato un errore").
        # GATED on a real error indicator in the DOM — never click a healthy
        # turn's regenerate/"Try again" affordance (that false-click wasted a
        # generation). Click the button if present, else resubmit the prompt.
        err_txt = _gen_failed(page) if (gen_src is None and retries_left > 0) else ""
        if err_txt:
            try:
                retries_left -= 1
                # Recovery runs INSIDE the pinned chat or not at all (node 4495).
                on_conv = _ensure_on_conv(page, pinned_cid, "retry/resubmit")
                log(f"generation error ({ERROR_RETRIES - retries_left}/{ERROR_RETRIES}) "
                    f"in {_conv_url(pinned_cid) or 'the current chat'} "
                    f"— matched: {err_txt!r}")
                rb = page.locator(SEL["retry_btn"]).first
                if rb.count() > 0 and rb.is_visible():
                    log("  clicking Retry/Try-again")
                    rb.click()
                elif on_conv:
                    log("  no button — resubmitting in the same chat")
                    _resubmit(page, prompt)
                    after = _conversation_id(page)
                    if pinned_cid and after and after != pinned_cid:
                        # Canary: a resubmit inside an existing chat must not mint
                        # a new one. If it ever does, say so instead of silently
                        # watching the wrong conversation.
                        log(f"  WARNING: resubmit opened a NEW chat {_conv_url(after)} "
                            f"— watching it instead of {_conv_url(pinned_cid)}")
                        pinned_cid = after
                else:
                    log(f"  cannot reach the pinned chat {_conv_url(pinned_cid)} — "
                        f"NOT resubmitting (that would open a second chat and "
                        f"abandon the one that is generating)")
                deadline = time.time() + gen_timeout_s
                last_cand = cand_since = last_status = complete_since = None
                ever_cand = False
                attempt_start = time.time()
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
        # Ask for the PINNED chat's status — if the tab drifted, the current URL's
        # id belongs to some other conversation and its status means nothing here.
        status = _stream_status(page, pinned_cid or cid_now)
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
            # A text/button generation error is handled at the top of the loop
            # (gated on _gen_failed). Here we only wait out the grace for a
            # genuinely image-less COMPLETE (content refusal / usage cap).
            if complete_since and (time.time() - complete_since) >= POST_COMPLETE_GRACE_S:
                # The tab may be STALE: turn COMPLETE server-side + the image is in
                # the chat (operator confirmed), but this tab never rendered it.
                # Reload the conversation (== a fresh browser) and re-detect before
                # giving up. The non-COMPLETE stuck-reload above can't fire here
                # (it's gated on `not complete_since`), so do it explicitly.
                if reloads_left > 0:
                    reloads_left -= 1
                    log(f"turn COMPLETE {POST_COMPLETE_GRACE_S}s, no image in this tab "
                        f"— reloading {_conv_url(pinned_cid) or 'the current chat'} "
                        f"to pull server state "
                        f"({STUCK_RELOADS - reloads_left}/{STUCK_RELOADS})")
                    if not (pinned_cid and _goto_conv(page, pinned_cid)):
                        try:
                            page.reload(wait_until="domcontentloaded", timeout=45000)
                        except Exception as _re:
                            log(f"reload failed: {_re}")
                    last_cand = cand_since = last_status = complete_since = None
                    ever_cand = False
                    attempt_start = time.time()
                    time.sleep(3)
                    continue
                # Final look INSIDE the pinned chat. Node 4495 gave up while the
                # tab sat on a DIFFERENT conversation than the one generating.
                if pinned_cid and _ensure_on_conv(page, pinned_cid, "the final check"):
                    final = (_find_gen_src(page, pre_srcs)
                             or _find_gen_src_loose(page, pre_srcs))
                    if final:
                        log(f"image WAS in the pinned chat on the final check "
                            f"({_conv_url(pinned_cid)}) — using it")
                        gen_src = final
                        break
                log(f"turn COMPLETE for {POST_COMPLETE_GRACE_S}s with no image "
                    f"(reloads exhausted) — giving up "
                    f"[chat: {_conv_url(pinned_cid) or page.url}] "
                    f"(likely content refusal / usage cap)")
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
            f"[chat: {_conv_url(pinned_cid) or page.url}] "
            f"[diag: stream_status={_stream_status(page, pinned_cid or _cid)} "
            f"last_seen={last_status} "
            f"conv={_cid} pinned={pinned_cid} stop_btn={stop_n} "
            f"ever_candidate={ever_cand} imgs={inv} "
            f"retries_used={ERROR_RETRIES - retries_left} "
            f"reloads_used={STUCK_RELOADS - reloads_left} error_bubble={err_bubble} "
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
    # NOTE: wait_until="domcontentloaded", NOT "networkidle" — gpt-tone.com keeps
    # long-lived connections (recaptcha/analytics) so it never reaches network
    # idle, which was timing out at 45s and forcing the local fallback. Instead we
    # explicitly wait for the file input to be ready.
    tab.goto(TONE_SITE_URL, wait_until="domcontentloaded", timeout=45000)
    tab.locator("input[type=file]").first.wait_for(state="attached", timeout=30000)
    time.sleep(1.0)
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
