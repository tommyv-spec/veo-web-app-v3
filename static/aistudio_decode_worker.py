#!/usr/bin/env python3
"""
aistudio_decode_worker.py — drive Google AI Studio in a real browser to decode a
source video into this repo's `decoded_*.md` grammar.

WHY THE BROWSER AND NOT THE API
  Gemini's API free tier dropped Pro models in April 2026 — Flash/Flash-Lite only.
  A decode ships ~280k tokens of rule canon per run and needs Pro-grade reading,
  so the API lane is paid per decode. AI Studio in a signed-in browser runs on the
  operator's own Google plan, which is the same reason `gemini_video_worker.py`
  exists. Same session model, same risk posture: its own Chrome profile, headful,
  human-paced, no token reversing.

WHAT "SET UP ONCE" MEANS HERE
  Nothing is stored in the AI Studio UI. Every run rebuilds the whole request:
  new chat -> paste system instructions -> attach the 9 canon files -> attach the
  mp4 -> run. So there is no saved Gem/prompt to drift out of sync, and no second
  copy of the rules anywhere. The one manual step is the Google login, once.

  The pack itself is generated from repo canon:
      python tools/build_gemini_decode_pack.py           # build
      python tools/build_gemini_decode_pack.py --check   # is it stale?
  This worker refuses to run on a stale pack unless you pass --allow-stale.

USAGE
  python code/static/aistudio_decode_worker.py --email you@gmail.com --login
  python code/static/aistudio_decode_worker.py --email you@gmail.com \
      --decode raw/videos/mp4/reel.mp4 --slug my-video-slug
  python code/static/aistudio_decode_worker.py --email you@gmail.com --learn

STATUS — READ THIS BEFORE TRUSTING IT
  The session/launch half is inherited from the proven gemini_video_worker lane.
  The AI Studio half drives a UI that Google reshuffles often, so every selector
  here is a LIST of candidates and every step dumps evidence on failure
  (.aistudio_debug/). `--learn` records one operator-driven run — its traffic log
  and element dump are how the selectors get locked from evidence instead of
  guesses. Run --learn once before trusting --decode unattended.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import subprocess
import sys
import time
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import gemini_video_worker as gvw  # session pull, patchright import, chrome args

AISTUDIO_URL = "https://aistudio.google.com/prompts/new_chat"
PACK_DIR = os.environ.get("GEMINI_PACK_DIR", os.path.join(REPO_ROOT, "_deploy_gemini"))
DEBUG_DIR = os.path.join(BASE_DIR, ".aistudio_debug")
SELECTORS_PATH = os.path.join(BASE_DIR, "aistudio_selectors.json")

PROFILE_DIR = os.environ.get("AISTUDIO_PROFILE_DIR", os.path.join(BASE_DIR, ".aistudio_profile"))
CHROME_CHANNEL = os.environ.get("WORKER_CHROME_CHANNEL", "chrome")

RUN_TIMEOUT_S = int(os.environ.get("AISTUDIO_RUN_TIMEOUT_S", "1800"))
UPLOAD_TIMEOUT_S = int(os.environ.get("AISTUDIO_UPLOAD_TIMEOUT_S", "900"))
POLL_EVERY_S = float(os.environ.get("AISTUDIO_POLL_EVERY_S", "5"))
# New chats default to a Flash model; a decode needs Pro-grade reading.
# Pro id as offered by the picker on 2026-08-13; `gemini-3-pro` is NOT a real
# option and `gemini-3-pro-image` is Nano Banana, not a reader.
DEFAULT_MODEL = os.environ.get("AISTUDIO_MODEL", "gemini-3.1-pro-preview")

# Candidate selectors, tried in order. AI Studio renames things; a list survives
# more renames than one string, and a miss dumps the DOM instead of guessing.
SELECTORS = {
    "consent_accept": ["button[aria-label*='Accept terms' i]"],
    "consent_checkbox": ["input[type=checkbox]"],
    "model_card": ["button.model-selector-card", "button[aria-label*='model' i]"],
    "file_input": ["input.file-input", "input[type=file]"],
    "prompt_box": [
        "textarea[aria-label*='prompt' i]",
        "ms-autosize-textarea textarea",
        "textarea[placeholder*='prompt' i]",
        "div[contenteditable=true][role=textbox]",
        "textarea",
    ],
    "run_button": [
        "button[aria-label*='Run' i]",
        "run-button button",
        "button:has-text('Run')",
    ],
    "stop_button": [
        "button[aria-label*='Stop' i]",
        "button:has-text('Stop')",
    ],
    "system_instructions_toggle": [
        "button[aria-label*='System instructions' i]",
        "button:has-text('System instructions')",
        "[data-test-si-toggle]",
    ],
    "system_instructions_box": [
        "textarea[aria-label*='System instructions' i]",
        "ms-system-instructions textarea",
        "textarea[placeholder*='system' i]",
    ],
    "copy_markdown": [
        "button[aria-label*='Copy markdown' i]",
        "button[aria-label*='Copy' i]",
    ],
    "response_turn": [
        "ms-chat-turn:last-of-type",
        "[data-turn-role='model']:last-of-type",
    ],
}

# Response bodies worth capturing — the model's text arrives here before the DOM
# renders it, and reading it off the wire keeps code fences intact.
RESPONSE_URL_HINTS = ("GenerateContent", "alkalimakersuite", "makersuite", "generateContent")


def log(msg):
    print(f"[aistudio] {msg}", flush=True)


def jitter(a=0.6, b=1.6):
    time.sleep(random.uniform(a, b))


def _debug_path(tag, ext="txt"):
    os.makedirs(DEBUG_DIR, exist_ok=True)
    return os.path.join(DEBUG_DIR, f"{tag}_{int(time.time())}.{ext}")


def dump(text, tag):
    p = _debug_path(tag)
    with open(p, "w", encoding="utf-8") as f:
        f.write(text)
    return p


def dump_dom(page, tag):
    """On any selector miss, persist the page so the fix is evidence-driven."""
    try:
        html = page.content()
    except Exception as e:
        return f"(could not read DOM: {e.__class__.__name__})"
    p = dump(html, tag)
    try:
        shot = _debug_path(tag, "png")
        page.screenshot(path=shot, full_page=False)
        return f"{p} + {shot}"
    except Exception:
        return p


# ---------------------------------------------------------------------------
# Selector resolution
# ---------------------------------------------------------------------------

def _load_learned():
    if os.path.exists(SELECTORS_PATH):
        try:
            learned = json.load(open(SELECTORS_PATH, encoding="utf-8"))
            for k, v in learned.items():
                if isinstance(v, str):
                    v = [v]
                SELECTORS[k] = list(v) + [s for s in SELECTORS.get(k, []) if s not in v]
            log(f"loaded learned selectors from {os.path.basename(SELECTORS_PATH)}")
        except (OSError, ValueError) as e:
            log(f"could not read {SELECTORS_PATH}: {e}")


def find(page, key, timeout_ms=8000, required=True):
    """First candidate selector that resolves to a visible element."""
    deadline = time.time() + timeout_ms / 1000.0
    last = None
    while time.time() < deadline:
        for sel in SELECTORS.get(key, []):
            try:
                loc = page.locator(sel).first
                if loc.count() and loc.is_visible():
                    return loc
            except Exception as e:  # selector engine can reject an old candidate
                last = e
        time.sleep(0.4)
    if required:
        where = dump_dom(page, f"miss_{key}")
        raise RuntimeError(
            f"no element for {key!r} (tried {SELECTORS.get(key)}; last error {last}). "
            f"DOM dumped to {where} — add the real selector to {SELECTORS_PATH}")
    return None


# ---------------------------------------------------------------------------
# Browser
# ---------------------------------------------------------------------------

def use_account(email):
    global PROFILE_DIR, CHROME_CHANNEL
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", email.strip().lower())
    PROFILE_DIR = os.path.join(BASE_DIR, f".aistudio_profile_{safe}")
    log(f"using per-account profile: {os.path.basename(PROFILE_DIR)}")
    channel = gvw.pull_session(email, PROFILE_DIR)
    if channel:
        CHROME_CHANNEL = channel
        log(f"session copied — launching with channel={channel}, no manual login needed.")
        return True
    log("no session copied; a one-time login in the worker window will be needed.")
    return False


def launch(p, headless=False):
    os.makedirs(PROFILE_DIR, exist_ok=True)
    ctx = p.chromium.launch_persistent_context(
        user_data_dir=PROFILE_DIR,
        channel=CHROME_CHANNEL,
        ignore_default_args=gvw._IGNORE_DEFAULT_ARGS,
        headless=headless,
        viewport={"width": 1400, "height": 1000},
        args=list(gvw.CHROME_ARGS),
    )
    try:
        ctx.grant_permissions(["clipboard-read", "clipboard-write"],
                              origin="https://aistudio.google.com")
    except Exception as e:
        log(f"clipboard permission not granted ({e.__class__.__name__}); "
            f"the copy-markdown fallback will be unavailable")
    page = ctx.pages[0] if ctx.pages else ctx.new_page()
    page.set_default_timeout(30000)
    return ctx, page


def is_logged_in(page):
    url = page.url or ""
    if "accounts.google.com" in url or "ServiceLogin" in url:
        return False
    try:
        return page.locator(",".join(SELECTORS["prompt_box"])).first.count() > 0
    except Exception:
        return False


def ensure_logged_in(page, timeout_s=600):
    page.goto(AISTUDIO_URL, wait_until="domcontentloaded")
    jitter()
    if is_logged_in(page):
        log("session OK")
        return
    log("  ACTION NEEDED: sign in to aistudio.google.com in the window that opened.")
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        time.sleep(3)
        if is_logged_in(page):
            log("login detected")
            return
        if "accounts.google.com" not in (page.url or ""):
            try:
                page.goto(AISTUDIO_URL, wait_until="domcontentloaded")
            except Exception:
                pass
    raise RuntimeError("timed out waiting for AI Studio login")


# ---------------------------------------------------------------------------
# The pack
# ---------------------------------------------------------------------------

def pack_files():
    mf = os.path.join(PACK_DIR, "MANIFEST.json")
    if not os.path.exists(mf):
        raise SystemExit(
            f"no pack at {PACK_DIR}. Run: python tools/build_gemini_decode_pack.py")
    manifest = json.load(open(mf, encoding="utf-8"))
    sys_path = os.path.join(PACK_DIR, "00_SYSTEM_INSTRUCTIONS.md")
    canon = [
        os.path.join(PACK_DIR, f["name"])
        for f in manifest["files"]
        if f["name"] != "00_SYSTEM_INSTRUCTIONS.md"
    ]
    missing = [p for p in [sys_path] + canon if not os.path.exists(p)]
    if missing:
        raise SystemExit("pack incomplete, rebuild it. missing: " + ", ".join(missing))
    return manifest, sys_path, canon


def pack_is_fresh():
    """Delegates to the builder so there is exactly one staleness rule."""
    r = subprocess.run(
        [sys.executable, os.path.join(REPO_ROOT, "tools", "build_gemini_decode_pack.py"),
         "--check", "--out", PACK_DIR],
        capture_output=True, text=True)
    return r.returncode == 0, (r.stdout or "").strip() + (r.stderr or "").strip()


def system_instruction_text(sys_path):
    text = open(sys_path, encoding="utf-8").read()
    # strip the generated HTML comment header — it is a note to the operator
    return re.sub(r"^<!--.*?-->\s*", "", text, flags=re.S)


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------

def clear_overlay(page, tag, tries=3):
    """A Material dialog leaves a blurred backdrop that swallows every click.

    AI Studio pops several of these (terms, onboarding, 'what's new'), so any
    step that is about to click first makes sure the page is actually clickable,
    and records what the dialog said if it will not go away.
    """
    for attempt in range(tries):
        try:
            if page.locator(".cdk-overlay-backdrop").count() == 0:
                return True
        except Exception:
            return True
        try:
            texts = page.locator(".cdk-overlay-container").first.inner_text()
        except Exception:
            texts = "(unreadable)"
        log(f"overlay blocking {tag} (attempt {attempt + 1}): {texts[:200]!r}")
        # a real button first, Escape second, backdrop click last
        for sel in ("button[aria-label*='copyright acknowledgement' i]",
                    "button:has-text('Acknowledge')",
                    "button[aria-label='Close panel']",
                    "button:has-text('Got it')", "button:has-text('Continue')",
                    "button:has-text('Close')", "button:has-text('Dismiss')",
                    "button:has-text('Accept')", "button[aria-label*='Close' i]"):
            try:
                b = page.locator(sel).first
                if b.count() and b.is_visible():
                    b.click(timeout=3000)
                    break
            except Exception:
                continue
        else:
            try:
                page.keyboard.press("Escape")
            except Exception:
                pass
        time.sleep(1.2)
    ok = page.locator(".cdk-overlay-backdrop").count() == 0
    if not ok:
        where = dump_dom(page, f"overlay_stuck_{tag}")
        raise RuntimeError(f"a dialog is still blocking {tag}. DOM + shot at {where}")
    return ok


def accept_terms(page):
    """First run on a fresh profile shows the Gemini API terms gate, which blocks
    every other control until it is accepted."""
    btn = find(page, "consent_accept", timeout_ms=12000, required=False)
    if btn is None:
        return False
    log("terms-of-service gate present — accepting")
    boxes = page.locator(SELECTORS["consent_checkbox"][0])
    for i in range(boxes.count()):
        box = boxes.nth(i)
        label = (box.get_attribute("aria-label") or "").lower()
        # tick the required consent only; leave the marketing opt-in alone
        if "consent" not in label and "terms" not in label:
            continue
        try:
            # Material hides the real input behind a styled span (opacity 0), so
            # a plain check() fails the visibility test — force it.
            box.check(force=True)
        except Exception as e:
            log(f"  checkbox check() failed ({e.__class__.__name__}); clicking its label")
            try:
                page.locator(f"label[for='{box.get_attribute('id')}']").first.click()
            except Exception:
                pass
    jitter(0.5, 1.0)
    btn.click()
    # The dialog leaves a blurred backdrop that eats every later click, so the
    # accept is only done once that backdrop is gone.
    deadline = time.time() + 30
    while time.time() < deadline:
        if page.locator(".cdk-overlay-backdrop").count() == 0:
            log("  terms accepted, dialog closed")
            jitter(1.0, 2.0)
            return True
        time.sleep(0.5)
    where = dump_dom(page, "consent_stuck")
    raise RuntimeError(
        f"accepted the terms but the dialog backdrop is still up — every later "
        f"click would be swallowed. DOM at {where}")


def _model_id(card):
    """The card shows a display name then the model id ('gemini-3-flash-preview')."""
    lines = [ln.strip() for ln in (card.inner_text() or "").splitlines() if ln.strip()]
    for ln in lines:
        if re.fullmatch(r"[a-z0-9.-]+", ln) and "-" in ln:
            return ln
    return lines[0] if lines else "?"


def select_model(page, want):
    """AI Studio defaults new chats to a Flash model, and the picker holds several
    ids that share a prefix ('gemini-3-pro' vs 'gemini-3-pro-image'), so the match
    is on the EXACT id — a substring match picked the image model on 2026-08-13."""
    clear_overlay(page, "model picker")
    card = find(page, "model_card", timeout_ms=8000, required=False)
    if card is None:
        log("no model selector found; leaving whatever is selected")
        return None
    current = _model_id(card)
    if current == want:
        log(f"model already {current}")
        return current
    card.click()
    jitter(1.0, 2.0)

    # The picker is a lazy carousel: most cards exist in the DOM but report no
    # inner_text until scrolled, so the id is matched on the page text node and
    # the click goes to its nearest button ancestor.
    node = page.get_by_text(want, exact=True).first
    if not node.count():
        where = dump_dom(page, "model_option_miss")
        offered = sorted(set(re.findall(r"gemini-[0-9][a-z0-9.-]*", page.content())))
        raise RuntimeError(
            f"model id {want!r} not in the picker. Offered: {offered}. DOM at {where}")
    picked = node.locator("xpath=ancestor-or-self::button[1]")
    if not picked.count():
        picked = node
    picked.scroll_into_view_if_needed()
    picked.click()
    jitter(1.0, 2.0)
    card = find(page, "model_card", timeout_ms=8000, required=False)
    now = _model_id(card) if card else "?"
    if now != want:
        where = dump_dom(page, "model_not_applied")
        raise RuntimeError(f"asked for {want!r} but the card still reads {now!r}. DOM at {where}")
    log(f"model: {current} -> {now}")
    return now


def disable_grounding(page):
    """Google Search grounding is on by default. A decode must read the video in
    front of it, not the web, so the switch goes off when it is on."""
    # The composer chip is the reliable control; the settings-panel switch only
    # exists while that panel is open.
    for sel in ("button[aria-label*='Remove Grounding with Google Search' i]",
                "button[aria-label='Grounding with Google Search']"):
        chip = page.locator(sel).first
        if not chip.count():
            continue
        cls = chip.get_attribute("class") or ""
        if "aria-label='Grounding" in sel and "checked" not in cls:
            continue
        try:
            chip.click()
            jitter()
            log("grounding with Google Search: off")
            return True
        except Exception as e:
            log(f"could not turn grounding off ({e.__class__.__name__}); continuing")
    return False


def set_system_instructions(page, text):
    clear_overlay(page, "system instructions")
    box = find(page, "system_instructions_box", timeout_ms=3000, required=False)
    if box is None:
        toggle = find(page, "system_instructions_toggle", timeout_ms=8000)
        toggle.click()
        jitter()
        box = find(page, "system_instructions_box", timeout_ms=10000)
    box.click()
    box.fill(text)
    log(f"system instructions set ({len(text)} chars)")
    jitter()
    # the panel opens over the composer — close it or every later click misses
    closer = page.locator("button[aria-label='Close panel']").first
    if closer.count() and closer.is_visible():
        closer.click()
        jitter()


def attach(page, paths):
    """Feed the hidden file input directly — no OS picker, no drag simulation.

    ALL files go in ONE call: set_input_files REPLACES the input's file list, so
    a second call drops the first batch (2026-08-13: the canon vanished and only
    the mp4 reached the model).
    """
    inp = page.locator(SELECTORS["file_input"][0]).first
    if not inp.count():
        where = dump_dom(page, "miss_file_input")
        raise RuntimeError(f"no file input on the page. DOM at {where}")
    total = sum(os.path.getsize(p) for p in paths)
    log(f"attaching {len(paths)} file(s), {total/1e6:.1f} MB")
    inp.set_input_files(paths)
    return total


def wait_for_uploads(page, count, timeout_s=UPLOAD_TIMEOUT_S):
    """Done when nothing still reads as busy AND it has stayed that way.

    A video shows 'Extracting' after the bytes land — submitting during that
    window returns 'Failed to generate content: permission denied'.
    """
    deadline = time.time() + timeout_s
    quiet = 0
    while time.time() < deadline:
        busy = 0
        for sel in ("[role=progressbar]", "mat-progress-bar",
                    "text=/extracting/i", "text=/uploading/i", "text=/processing/i"):
            try:
                busy += page.locator(sel).count()
            except Exception:
                pass
        quiet = quiet + 1 if not busy else 0
        if quiet >= 2:  # two consecutive clean polls, not one lucky frame
            log(f"uploads settled ({count} file(s))")
            jitter(1.5, 2.5)
            return True
        time.sleep(POLL_EVERY_S)
    where = dump_dom(page, "upload_timeout")
    raise RuntimeError(f"uploads still busy after {timeout_s}s. DOM at {where}")


ERROR_PATTERNS = (
    "internal error has occurred",
    "permission denied",
    "failed to generate content",
    "something went wrong",
)


def run_error(page):
    """The visible failure banner, if AI Studio refused or broke on this run."""
    try:
        body = page.locator("body").inner_text().lower()
    except Exception:
        return None
    for pat in ERROR_PATTERNS:
        if pat in body:
            return pat
    return None


class Capture:
    """Records the model's streamed text off the network, so the decode is not
    reconstructed from rendered DOM (which eats code fences and long tables)."""

    def __init__(self, page):
        self.chunks = []
        self.page = page
        page.on("response", self._on_response)

    def _on_response(self, resp):
        try:
            url = resp.url or ""
            if not any(h in url for h in RESPONSE_URL_HINTS):
                return
            body = resp.text()
        except Exception:
            return
        if body:
            self.chunks.append(body)

    def raw(self):
        return "\n".join(self.chunks)


def send(page, prompt):
    clear_overlay(page, 'prompt box')
    box = find(page, "prompt_box", timeout_ms=15000)
    box.click()
    box.fill(prompt)
    jitter()
    btn = find(page, "run_button", timeout_ms=8000, required=False)
    if btn is not None and btn.is_enabled():
        btn.click()
    else:
        page.keyboard.press("Control+Enter")
    log(f"submitted: {prompt!r}")


def wait_for_answer(page, timeout_s=RUN_TIMEOUT_S):
    """Generation is running while a Stop control exists; it ends when it goes."""
    deadline = time.time() + timeout_s
    seen_running = False
    while time.time() < deadline:
        stop = find(page, "stop_button", timeout_ms=1200, required=False)
        if stop is not None:
            seen_running = True
        elif seen_running:
            jitter(1.5, 3.0)
            return True
        time.sleep(POLL_EVERY_S)
    where = dump_dom(page, "answer_timeout")
    raise RuntimeError(f"no finished answer after {timeout_s}s. DOM at {where}")


def _from_network(raw):
    """Pull the model text out of the captured RPC bodies.

    The payloads are JSON-ish arrays of string fragments; the decode is by far
    the longest run of text and always starts at a YAML front-matter fence.
    """
    if not raw:
        return None
    best = None
    for m in re.finditer(r'"((?:[^"\\]|\\.){400,})"', raw):
        s = m.group(1)
        try:
            s = json.loads(f'"{s}"')
        except ValueError:
            s = s.encode().decode("unicode_escape", errors="ignore")
        if best is None or len(s) > len(best):
            best = s
    if best and ("---" in best[:200] or "## " in best):
        return best
    return best


def _from_clipboard(page):
    btn = find(page, "copy_markdown", timeout_ms=4000, required=False)
    if btn is None:
        return None
    try:
        btn.click()
        jitter(0.8, 1.5)
        return page.evaluate("async () => await navigator.clipboard.readText()")
    except Exception as e:
        log(f"clipboard fallback failed: {e.__class__.__name__}")
        return None


def _from_dom(page):
    turn = find(page, "response_turn", timeout_ms=4000, required=False)
    if turn is None:
        return None
    try:
        return turn.inner_text()
    except Exception:
        return None


def extract(page, capture):
    for name, fn in (("network", lambda: _from_network(capture.raw())),
                     ("clipboard", lambda: _from_clipboard(page)),
                     ("dom", lambda: _from_dom(page))):
        text = fn()
        if text and len(text) > 2000:
            log(f"extracted decode via {name} ({len(text)} chars)")
            return text, name
        if text:
            log(f"{name} gave only {len(text)} chars — trying the next path")
    where = dump_dom(page, "extract_failed")
    raw_path = dump(capture.raw()[:2_000_000], "extract_failed_network")
    raise RuntimeError(f"could not extract the decode. DOM at {where}, network at {raw_path}")


def clean(text):
    """Trim chat wrapper so the file starts at the front matter."""
    text = text.replace("\r\n", "\n").strip()
    text = re.sub(r"^```(?:markdown|md)?\n", "", text)
    text = re.sub(r"\n```$", "", text)
    i = text.find("---\n")
    if 0 < i < 400:
        text = text[i:]
    return text.strip() + "\n"


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------

def decode(page, mp4, slug, prompt_extra="", model=DEFAULT_MODEL, out=None):
    manifest, sys_path, canon = pack_files()
    log(f"pack {manifest['pack_sha']} ({manifest['mode']}, built {manifest['built']})")

    page.goto(AISTUDIO_URL, wait_until="domcontentloaded")
    jitter(1.5, 3.0)

    accept_terms(page)
    select_model(page, model)
    disable_grounding(page)
    set_system_instructions(page, system_instruction_text(sys_path))

    files = canon + [mp4]
    attach(page, files)
    clear_overlay(page, "upload")
    wait_for_uploads(page, len(files))

    capture = Capture(page)
    ask = "decode this video"
    if prompt_extra:
        ask += f"\n\n{prompt_extra}"

    for attempt in (1, 2):
        send(page, ask)
        wait_for_answer(page)
        err = run_error(page)
        if not err:
            break
        where = dump_dom(page, f"run_error_{attempt}")
        log(f"AI Studio reported {err!r} (attempt {attempt}). Evidence at {where}")
        if attempt == 2:
            raise RuntimeError(f"AI Studio failed twice with {err!r}. Evidence at {where}")
        jitter(8.0, 14.0)

    text, how = extract(page, capture)
    body = clean(text)

    out = out or os.path.join(REPO_ROOT, "raw", "videos", f"decoded_{slug}.md")
    with open(out, "w", encoding="utf-8") as f:
        f.write(body)
    log(f"wrote {out} ({len(body)} chars, via {how})")
    return out


def verify(path):
    r = subprocess.run(
        [sys.executable, os.path.join(REPO_ROOT, "code", "verify_decode_format.py"), path],
        capture_output=True, text=True, cwd=REPO_ROOT)
    out = (r.stdout or "") + (r.stderr or "")
    log("decode linter:\n" + out.strip())
    return r.returncode == 0


def learn(page, timeout_s=3600):
    """Operator drives one run by hand; the worker records what actually happens.

    This is how selectors get locked from evidence. It logs every AI Studio POST
    and, when the run finishes, dumps the DOM plus the elements it can see, so
    the real selector names go into aistudio_selectors.json.
    """
    os.makedirs(DEBUG_DIR, exist_ok=True)
    traffic = os.path.join(DEBUG_DIR, "learn_traffic.log")

    def on_request(req):
        if req.method != "POST" or "google" not in (req.url or ""):
            return
        with open(traffic, "a", encoding="utf-8") as f:
            f.write(f"\n=== {datetime.now().isoformat()} {req.method} {req.url}\n")
            try:
                f.write((req.post_data or "")[:4000] + "\n")
            except Exception:
                pass

    page.on("request", on_request)
    capture = Capture(page)

    page.goto(AISTUDIO_URL, wait_until="domcontentloaded")
    log("LEARN MODE. Do ONE decode by hand in this window:")
    log("  1. paste _deploy_gemini/00_SYSTEM_INSTRUCTIONS.md into System instructions")
    log("  2. attach the 9 pack files + your mp4")
    log("  3. send 'decode this video' and let it finish")
    log(f"Everything is being recorded to {DEBUG_DIR}. Press Ctrl+C when done.")

    deadline = time.time() + timeout_s
    try:
        while time.time() < deadline:
            time.sleep(5)
    except KeyboardInterrupt:
        pass

    where = dump_dom(page, "learn_dom")
    raw_path = dump(capture.raw()[:5_000_000], "learn_network")
    probe = page.evaluate("""() => {
        const out = [];
        for (const el of document.querySelectorAll('button, textarea, input, [role=textbox]')) {
            out.push({
                tag: el.tagName.toLowerCase(),
                type: el.getAttribute('type') || '',
                aria: el.getAttribute('aria-label') || '',
                placeholder: el.getAttribute('placeholder') || '',
                text: (el.innerText || '').slice(0, 60),
                classes: (el.className || '').toString().slice(0, 120),
            });
        }
        return out;
    }""")
    probe_path = os.path.join(DEBUG_DIR, "learn_elements.json")
    with open(probe_path, "w", encoding="utf-8") as f:
        json.dump(probe, f, indent=2, ensure_ascii=False)
    log(f"recorded: traffic={traffic} dom={where} network={raw_path} elements={probe_path}")
    log(f"Put the real selectors into {SELECTORS_PATH} and re-run --decode.")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--email", help="Google account; copies its session from a NON-STABLE Chrome channel")
    ap.add_argument("--login", action="store_true", help="open the window and stop once signed in")
    ap.add_argument("--learn", action="store_true", help="record one operator-driven run")
    ap.add_argument("--decode", metavar="MP4", help="video file to decode")
    ap.add_argument("--slug", help="output slug -> raw/videos/decoded_<slug>.md")
    ap.add_argument("--out", help="write here instead of raw/videos/decoded_<slug>.md")
    ap.add_argument("--model", default=DEFAULT_MODEL, help="model to select in the picker")
    ap.add_argument("--note", default="", help="extra context appended to the prompt (source URL, operator note)")
    ap.add_argument("--allow-stale", action="store_true", help="run even if the pack no longer matches repo canon")
    ap.add_argument("--headless", action="store_true", help="not recommended; Google flags it")
    args = ap.parse_args()

    if not (args.login or args.learn or args.decode):
        ap.error("pick one of --login / --learn / --decode")

    if args.decode:
        if not args.slug:
            ap.error("--decode needs --slug")
        if not os.path.exists(args.decode):
            ap.error(f"no such file: {args.decode}")
        fresh, msg = pack_is_fresh()
        log(msg)
        if not fresh and not args.allow_stale:
            raise SystemExit(
                "pack is stale — rebuild with tools/build_gemini_decode_pack.py "
                "(or pass --allow-stale to decode against the old rules anyway)")

    _load_learned()
    if args.email:
        use_account(args.email)

    sync_playwright = gvw._import_playwright()
    with sync_playwright() as p:
        ctx, page = launch(p, headless=args.headless)
        try:
            ensure_logged_in(page)
            if args.login:
                log("logged in. Profile is warm for later runs.")
                return 0
            if args.learn:
                accept_terms(page)
                learn(page)
                return 0
            out = decode(page, args.decode, args.slug, args.note, args.model, args.out)
            ok = verify(out)
            return 0 if ok else 2
        finally:
            try:
                ctx.close()
            except Exception:
                pass


if __name__ == "__main__":
    sys.exit(main())
