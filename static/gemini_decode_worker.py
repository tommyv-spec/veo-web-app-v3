#!/usr/bin/env python3
"""
gemini_decode_worker.py — decode a source video into this repo's `decoded_*.md`
grammar by driving gemini.google.com in a real browser.

WHY THIS SURFACE
  AI Studio refuses to generate on an account with no API key/project ("permission
  denied", verified 2026-08-13) and a key would bill per decode. gemini.google.com
  runs on the operator's own Google plan, so a decode costs nothing extra. Session,
  profile and launch come from the proven `gemini_video_worker.py` lane.

WHAT "SET UP ONCE" MEANS
  Nothing is stored in the Gemini UI. Every run rebuilds the whole request: new
  chat -> pick the model -> attach the 9 canon files + the mp4 -> send the routing
  prompt. No Gem, no saved prompt, no second copy of the rules to keep in sync.
  The pack is generated from repo canon:
      python tools/build_gemini_decode_pack.py           # build
      python tools/build_gemini_decode_pack.py --check   # is it stale?

USAGE
  python code/static/gemini_decode_worker.py --email you@gmail.com --login
  python code/static/gemini_decode_worker.py --email you@gmail.com \
      --decode raw/videos/mp4/reel.mp4 --slug my-video-slug

SELECTORS
  Learned from a live probe of the Gemini app on 2026-08-13 (Ultra account):
    composer      div.ql-editor[aria-label='Enter a prompt for Gemini']
    upload menu   button[aria-label*='Upload & tools']
    file input    input.hidden-file-input   (only mounted while that menu is open)
    model picker  button[aria-label*='mode picker']  -> gem-menu-item '3.1 Pro'
  Each is a candidate LIST, and any miss dumps DOM + screenshot to .gemini_debug/.
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

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import gemini_video_worker as gvw  # session pull, patchright, chrome args, login

GEMINI_URL = "https://gemini.google.com/app"
PACK_DIR = os.environ.get("GEMINI_PACK_DIR", os.path.join(REPO_ROOT, "_deploy_gemini"))
DEBUG_DIR = os.path.join(BASE_DIR, ".gemini_debug")
SELECTORS_PATH = os.path.join(BASE_DIR, "gemini_ui_selectors.json")

DEFAULT_MODEL = os.environ.get("GEMINI_DECODE_MODEL", "3.1 Pro")
UPLOAD_TIMEOUT_S = int(os.environ.get("GEMINI_UPLOAD_TIMEOUT_S", "1200"))
ANSWER_TIMEOUT_S = int(os.environ.get("GEMINI_ANSWER_TIMEOUT_S", "2400"))
POLL_EVERY_S = float(os.environ.get("GEMINI_POLL_EVERY_S", "5"))
# Hard cap the Gemini app enforces on one prompt, video included.
MAX_ATTACHMENTS = 10

SELECTORS = {
    "composer": [
        "div.ql-editor[aria-label*='Enter a prompt' i]",
        "rich-textarea div.ql-editor",
        "div[contenteditable=true][role=textbox]",
    ],
    "upload_menu": ["button[aria-label*='Upload & tools' i]", "button[aria-label*='Upload' i]"],
    "file_input": ["input.hidden-file-input", "input[type=file]"],
    "model_picker": ["button[aria-label*='mode picker' i]", "button[aria-label*='model' i]"],
    "send": [
        "button[aria-label*='Send message' i]",
        "button[aria-label*='Send' i]",
        "button.send-button",
    ],
    "stop": ["button[aria-label*='Stop' i]"],
    "model_turn": ["model-response:last-of-type", "message-content:last-of-type"],
    "copy": ["button[aria-label*='Copy' i]", "[data-test-id='copy-button']"],
    "more_options": ["button[aria-label*='More options' i]"],
}


def log(msg):
    print(f"[gemini-decode] {msg}", flush=True)


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
    try:
        p = dump(page.content(), tag)
    except Exception as e:
        return f"(no DOM: {e.__class__.__name__})"
    try:
        shot = _debug_path(tag, "png")
        page.screenshot(path=shot)
        return f"{p} + {shot}"
    except Exception:
        return p


def _load_learned():
    if not os.path.exists(SELECTORS_PATH):
        return
    try:
        learned = json.load(open(SELECTORS_PATH, encoding="utf-8"))
    except (OSError, ValueError) as e:
        log(f"could not read {SELECTORS_PATH}: {e}")
        return
    for k, v in learned.items():
        if k.startswith("_"):
            continue
        v = [v] if isinstance(v, str) else list(v)
        SELECTORS[k] = v + [s for s in SELECTORS.get(k, []) if s not in v]
    log(f"loaded learned selectors from {os.path.basename(SELECTORS_PATH)}")


def find(page, key, timeout_ms=10000, required=True):
    deadline = time.time() + timeout_ms / 1000.0
    last = None
    while time.time() < deadline:
        for sel in SELECTORS.get(key, []):
            try:
                loc = page.locator(sel).first
                if loc.count() and loc.is_visible():
                    return loc
            except Exception as e:
                last = e
        time.sleep(0.4)
    if required:
        where = dump_dom(page, f"miss_{key}")
        raise RuntimeError(
            f"no element for {key!r} (tried {SELECTORS.get(key)}; last error {last}). "
            f"Evidence at {where}")
    return None


# ---------------------------------------------------------------------------
# The pack
# ---------------------------------------------------------------------------

FF_PROFILE_DIR = os.environ.get(
    "GEMINI_FF_PROFILE_DIR", os.path.join(BASE_DIR, ".gemini_ff_profile"))
CAMOUFOX_OS = os.environ.get("CAMOUFOX_OS", "windows")


def firefox_playwright():
    """Plain Playwright, NOT patchright.

    `run_firefox_worker_local.py` measured it: patchright + firefox breaks
    page.evaluate ("Cannot read properties of undefined"), plain playwright +
    firefox works. Camoufox then supplies the stealth Firefox lacks — plain
    Playwright Firefox sets navigator.webdriver=True and Google's OAuth refuses
    it outright.
    """
    from playwright.sync_api import sync_playwright
    return sync_playwright


def launch_firefox(p, headless=False):
    """Camoufox, the same engine the Flow worker runs on.

    Chrome stopped being able to READ uploaded video on 2026-08-13: a 1.4MB clip
    that decoded cleanly an hour earlier came back "CANNOT OPEN", as did every
    other file, while the chip kept appearing normally. That is the same
    engine-side degradation the Flow lane hit, where Firefox works and Chrome
    scores ~0% (`flow-403-is-recaptcha-token-class`).
    """
    from camoufox.sync_api import NewBrowser

    os.makedirs(FF_PROFILE_DIR, exist_ok=True)
    kwargs = {
        "user_data_dir": FF_PROFILE_DIR,
        "headless": headless,
        # keep the fingerprint coherent with this machine — Camoufox otherwise
        # picks at random and has served a macOS UA on this Windows box
        "os": CAMOUFOX_OS,
        "window": (1400, 1000),
        "no_viewport": True,
    }
    try:
        from camoufox.addons import DefaultAddons

        # Camoufox >=0.5 hard-fails the launch when its bundled uBlock is
        # missing, and that download comes from GitHub. Skip it.
        kwargs["exclude_addons"] = [DefaultAddons.UBO]
    except ImportError:
        pass

    ctx = NewBrowser(p, persistent_context=True, **kwargs)
    page = ctx.pages[0] if ctx.pages else ctx.new_page()
    page.set_default_timeout(30000)
    log(f"Camoufox ACTIVE (profile {os.path.basename(FF_PROFILE_DIR)})")
    # DO NOT bridge Chrome's cookies in here. Chrome-bound SID/SAPISID values
    # layered over a freshly pulled Firefox session INVALIDATE it: the identical
    # pull signs in on Pro without the bridge and signs out with it (measured
    # 2026-08-13, twice each way). The Firefox profile pull is the whole seeding
    # story for this lane.
    return ctx, page


def pull_firefox_profile(email):
    """Seed the Camoufox profile from a REAL Firefox profile on this machine.

    The Firefox counterpart of the Chrome session pull, and the same tool the
    Flow worker uses. It copies the durable DATA files only — Camoufox ships
    Firefox 152 and silently refuses a profile written by the machine's 153, so
    a wholesale directory copy produces a browser that never starts.

    Cookie-bridging Chrome's session into Firefox is NOT enough for Google: 37
    bridged cookies still landed on a signed-out page offering Flash-Lite
    (measured 2026-08-13). Google binds the session to the browser, so the
    session has to come from a Firefox that really signed in.
    """
    if not email:
        return False
    try:
        import firefox_profile_pull as ffpull
    except ImportError:
        log("firefox_profile_pull not available — Firefox needs a manual sign-in")
        return False

    # SEED INTO A PROFILE CAMOUFOX HAS NEVER OPENED. Pulling on top of a
    # directory that has already been launched leaves that launch's
    # cookies.sqlite-wal behind, Firefox replays it over the imported database,
    # and the session is silently wiped — the page then renders signed-out on
    # Flash-Lite while the cookie table still LOOKS full. Measured 2026-08-13:
    # same pull, same account, dirty dir signed out / clean dir signed in on
    # Pro. flow_worker never hits this because it restores its golden into a
    # fresh session folder.
    if os.path.isdir(FF_PROFILE_DIR):
        import shutil

        shutil.rmtree(FF_PROFILE_DIR, ignore_errors=True)
        log("cleared the old Camoufox profile so the pulled session is not replayed over")
    try:
        # account_num=1 makes locate_firefox_profile honour ACCOUNT1_FIREFOX_PROFILE
        # BEFORE it tries to match the account by asking Google. That email probe
        # is a documented false-negative (flow-dead-session-reclone-from-golden:
        # "may return None even when a good profile exists"), and it hit us on
        # 2026-08-23: it reported no profile for shenkevin480 while the profile
        # held 39 google.com, 7 gemini.google.com and 5 labs.google cookies - a
        # live session by the cookie test, which is the one that counts. Without
        # this the worker silently drops to the Chrome-cookie bridge and drives a
        # logged-out Gemini for six minutes before failing on a button timeout.
        ok = ffpull.build_firefox_golden_from_profile(
            email, FF_PROFILE_DIR, label="GEMINI-FF", account_num=1, log=log)
    except Exception as e:
        log(f"firefox profile pull failed ({e.__class__.__name__}: {str(e)[:90]})")
        return False
    if ok:
        log(f"Firefox golden seeded for {email} from a real Firefox profile")
    return bool(ok)


def signed_in(page):
    """Positively prove a live session before doing anything else.

    The inherited check passed a SIGNED-OUT page (2026-08-13): it looked for a
    composer, and the logged-out landing page has one. Everything downstream
    then ran against a stranger's Gemini — Flash-Lite, no upload menu, and a
    model picker whose entries are all disabled.

    So check the things only a session has, and the thing only a logged-out page
    has, and require both to agree.
    """
    # DO NOT judge by page text. Camoufox returns ~55 chars of body text for a
    # perfectly signed-in Gemini (the app renders where inner_text does not
    # reach), so a length test calls a working session "signed out" — it did,
    # repeatedly, on 2026-08-13 while the operator was looking at a live window.
    # Judge by controls that only ever exist on one side of the line.
    try:
        if page.locator("button:has-text('Sign in'), a:has-text('Sign in')").first.is_visible():
            return False
    except Exception:
        pass
    for sel in ("button[aria-label*='Upload' i]",          # composer upload menu
                "button[aria-label*='mode picker' i]"):     # model switcher
        try:
            loc = page.locator(sel).first
            if not loc.count():
                continue
            if "mode picker" in sel:
                # the logged-out landing page pins Flash-Lite and offers nothing
                # else; a session shows whatever mode the account can actually use
                label = (loc.get_attribute("aria-label") or "").lower()
                if "flash-lite" in label:
                    continue
            return True
        except Exception:
            continue
    return False


def ensure_session(page, email, firefox=False):
    """Never decode against a signed-out tab. Verify, recover, verify again."""
    page.goto(GEMINI_URL, wait_until="load")
    for _ in range(12):
        time.sleep(2)
        if signed_in(page):
            log("session verified — signed in")
            return True
    if firefox:
        log("signed out — trying the account chooser")
        if firefox_login(page, email) and signed_in(page):
            log("session recovered via the account chooser")
            return True
    where = dump_dom(page, "signed_out")
    raise RuntimeError(
        f"the browser is NOT signed in, so anything it produced would be a "
        f"stranger's Gemini. Evidence at {where}. For Firefox, reseed with "
        f"--reseed-firefox (the pull must land in a profile Camoufox has never "
        f"opened, or the old WAL replays over the session)")


def firefox_pick_account(page, email, tries=3):
    """Resume the transplanted session by PICKING the account, the way
    flow_worker does it.

    A pulled Firefox profile carries the identity but not the live session:
    myaccount.google.com renders an account chooser reading "Kevin Shen …
    Signed out". Nothing is broken — Google just wants an explicit pick before
    it reuses the session. flow_worker (~line 3540) handles exactly this by
    clicking the tile whose text matches the configured email, and that is why
    its Firefox workers never need a manual sign-in.
    """
    for attempt in range(1, tries + 1):
        url = page.url or ""
        if "accounts.google.com" not in url and "accountchooser" not in url:
            return True
        log(f"account chooser (attempt {attempt}) — picking {email or 'the first account'}")
        picked = False
        if email:
            try:
                tile = page.locator(f"*:has-text('{email}')").last
                if tile.count() and tile.is_visible():
                    tile.click()
                    picked = True
            except Exception:
                pass
        if not picked:
            # no email match: take the first listed account rather than stalling
            for sel in ("div[data-identifier]", "li[data-identifier]",
                        "[role='link']", "form li"):
                try:
                    t = page.locator(sel).first
                    if t.count() and t.is_visible():
                        t.click()
                        picked = True
                        break
                except Exception:
                    continue
        if not picked:
            break
        time.sleep(6)
    return "accounts.google.com" not in (page.url or "")


def firefox_login(page, email):
    """Land on Gemini signed in, without a human at the keyboard."""
    page.goto("https://myaccount.google.com/", wait_until="domcontentloaded")
    time.sleep(5)
    firefox_pick_account(page, email)
    page.goto(GEMINI_URL, wait_until="load")
    time.sleep(8)
    firefox_pick_account(page, email)
    try:
        body = page.locator("body").inner_text()
    except Exception:
        body = ""
    ok = len(body) > 800 and "sign in" not in body.lower()[:600]
    log("Gemini session " + ("live" if ok else "still signed out"))
    return ok


def seed_firefox_session(ctx, golden=None):
    """Fallback only: bridge the Chrome account's cookies into Firefox.

    Kept because it costs nothing and occasionally helps a same-account
    handoff, but it does NOT authenticate Google on its own — see
    pull_firefox_profile. The real seeding path is the Firefox profile pull.
    """
    golden = golden or gvw.PROFILE_DIR
    if not os.path.isdir(golden):
        log("no Chrome golden to seed from — Firefox will need a manual sign-in")
        return False
    try:
        import chrome_cookie_bridge as bridge
    except ImportError:
        log("chrome_cookie_bridge unavailable — Firefox needs a manual sign-in")
        return False

    cookies = bridge.read_cookies(golden, domains=("google.com",), log=log)
    if not cookies:
        log("cookie bridge returned nothing — Firefox needs a manual sign-in")
        return False
    try:
        ctx.add_cookies(cookies)
    except Exception as e:
        log(f"could not add cookies to Firefox ({e.__class__.__name__}: {str(e)[:80]})")
        return False
    log(f"seeded Firefox with {len(cookies)} Google cookie(s) from the Chrome golden")
    return True


def pack_files():
    mf = os.path.join(PACK_DIR, "MANIFEST.json")
    if not os.path.exists(mf):
        raise SystemExit(f"no pack at {PACK_DIR}. Run: python tools/build_gemini_decode_pack.py")
    manifest = json.load(open(mf, encoding="utf-8"))
    sys_path = os.path.join(PACK_DIR, "00_SYSTEM_INSTRUCTIONS.md")
    canon = [os.path.join(PACK_DIR, f["name"]) for f in manifest["files"]
             if f["name"] != "00_SYSTEM_INSTRUCTIONS.md"]
    missing = [p for p in [sys_path] + canon if not os.path.exists(p)]
    if missing:
        raise SystemExit("pack incomplete, rebuild it. missing: " + ", ".join(missing))
    return manifest, sys_path, canon


def swap_self_example(canon, mp4):
    """Never hand the model a finished decode OF THE VIDEO IT IS DECODING.

    The pack's worked example is simply the newest decode in raw/videos/. On
    2026-08-13 that happened to be the decode of the very reel under test, and
    69% of the long sentences in the result came back copied verbatim from it —
    the run measured nothing. If the shipped example names this source, another
    decode is attached in its place.
    """
    example = next((p for p in canon if os.path.basename(p).startswith("90_")), None)
    if example is None:
        return canon
    ident = {os.path.basename(os.path.dirname(mp4)),
             os.path.splitext(os.path.basename(mp4))[0]}
    ident = {i for i in ident if len(i) > 4}
    try:
        body = open(example, encoding="utf-8", errors="ignore").read()
    except OSError:
        return canon
    if not any(i in body for i in ident):
        return canon

    log(f"the shipped worked example is a decode of THIS source ({', '.join(sorted(ident))})")
    pool = sorted((os.path.join(REPO_ROOT, "raw", "videos", f)
                   for f in os.listdir(os.path.join(REPO_ROOT, "raw", "videos"))
                   if f.startswith("decoded_") and f.endswith(".md")),
                  key=os.path.getmtime, reverse=True)
    for cand in pool:
        try:
            text = open(cand, encoding="utf-8", errors="ignore").read()
        except OSError:
            continue
        if any(i in text for i in ident):
            continue
        log(f"  swapped in {os.path.basename(cand)} as the worked example")
        return [p if p is not example else cand for p in canon]

    log("  no unrelated decode to swap in — dropping the worked example entirely")
    return [p for p in canon if p is not example]


def pack_is_fresh():
    r = subprocess.run(
        [sys.executable, os.path.join(REPO_ROOT, "tools", "build_gemini_decode_pack.py"),
         "--check", "--out", PACK_DIR], capture_output=True, text=True)
    return r.returncode == 0, ((r.stdout or "") + (r.stderr or "")).strip()


def instructions(sys_path):
    """The routing text. The consumer app has no system-instruction field, so it
    rides at the top of the prompt — it is routing only, no rule text."""
    return re.sub(r"^<!--.*?-->\s*", "", open(sys_path, encoding="utf-8").read(), flags=re.S)


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------

def source_facts(mp4):
    """Duration / size / fps straight from ffprobe.

    The model has to put timestamps on every scene, and it cannot measure the
    file — handing it the measured numbers is evidence, not a rule, and it keeps
    a long source from being summarised as if it were short.
    """
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=width,height,r_frame_rate",
             "-show_entries", "format=duration", "-of", "json", mp4],
            capture_output=True, text=True, timeout=60)
        data = json.loads(r.stdout or "{}")
        st = (data.get("streams") or [{}])[0]
        dur = float((data.get("format") or {}).get("duration", 0) or 0)
        num, _, den = (st.get("r_frame_rate") or "0/1").partition("/")
        fps = float(num) / float(den or 1) if float(den or 1) else 0
    except Exception as e:
        log(f"ffprobe unavailable ({e.__class__.__name__}); sending no source facts")
        return ""
    if not dur:
        return ""
    facts = (f"SOURCE FACTS (measured, use these rather than estimating): "
             f"duration {dur:.2f}s, {st.get('width')}x{st.get('height')}, {fps:.0f} fps.")
    log(facts)
    return facts


def prep_block(mp4, prep_dir=None):
    """Whisper segments + the hardcut clip list, as text the prompt can carry.

    This is what closes the long-form gap. A 133s source has only THREE hard
    cuts, so cut detection alone yields ~4 scenes; the hand decode reaches 12 by
    splitting the 119-second middle take on DIALOGUE beats, which needs whisper's
    timestamps. Gemini has neither instrument, so when the repo pipeline has
    already produced them, they ride along and outrank its own hearing.
    """
    root = prep_dir or os.path.dirname(os.path.abspath(mp4))
    stem = os.path.splitext(os.path.basename(mp4))[0]
    parts = []

    clips = os.path.join(root, "hardcut", "clips.tsv")
    if not os.path.exists(clips):
        hits = [os.path.join(dp, "clips.tsv") for dp, _, fs in os.walk(root) if "clips.tsv" in fs]
        clips = hits[0] if hits else None
    if clips and os.path.exists(clips):
        parts.append("HARD CUTS (PySceneDetect, authoritative — these are the real "
                     "shot boundaries):\n" + open(clips, encoding="utf-8").read().strip())

    tr = os.path.join(root, f"{stem}.json")
    if os.path.exists(tr):
        try:
            segs = (json.load(open(tr, encoding="utf-8")) or {}).get("segments") or []
        except (OSError, ValueError):
            segs = []
        if segs:
            lines = [f"{s.get('start', 0):.2f}-{s.get('end', 0):.2f}  {(s.get('text') or '').strip()}"
                     for s in segs]
            parts.append(
                "SPEECH (whisper, authoritative and verbatim — use these words and "
                f"these timings, do not re-transcribe by ear; {len(segs)} segments):\n"
                + "\n".join(lines))

    if not parts:
        return ""
    block = ("PREP FROM THE REPO PIPELINE — this outranks your own hearing and your own "
             "cut detection. A long single take still splits into several scenes at the "
             "dialogue beats below.\n\n" + "\n\n".join(parts))
    log(f"prep block attached ({len(block)} chars)")
    return block


def new_chat(page):
    page.goto(GEMINI_URL, wait_until="load")
    for _ in range(24):
        time.sleep(2)
        if find(page, "composer", timeout_ms=1500, required=False) is not None:
            jitter(1.0, 2.0)
            return True
    where = dump_dom(page, "app_never_loaded")
    raise RuntimeError(f"the Gemini composer never appeared. Evidence at {where}")


def select_model(page, want):
    """The app defaults to Flash. A decode reads a whole video against 280k tokens
    of rules, so it runs on the Pro / extended-thinking mode instead."""
    btn = find(page, "model_picker", timeout_ms=10000, required=False)
    if btn is None:
        log("no model picker found; leaving the default mode")
        return None
    current = (btn.get_attribute("aria-label") or "").split("currently")[-1].strip()
    # The picker labels the live mode loosely ("Pro") while the menu entry is
    # exact ("3.1 Pro"), so compare BOTH directions before deciding to click.
    if current and (want.lower() in current.lower() or current.lower() in want.lower()):
        log(f"model already {current!r}")
        return current
    btn.click()
    jitter(1.0, 2.0)
    opt = page.locator(f"gem-menu-item:has-text('{want}'), [role=menuitem]:has-text('{want}')").first
    if not opt.count():
        where = dump_dom(page, "model_option_miss")
        raise RuntimeError(f"mode {want!r} not in the picker. Evidence at {where}")
    # An already-active entry renders aria-disabled — clicking it waits forever.
    if (opt.get_attribute("aria-disabled") or "").lower() == "true":
        log(f"model {want!r} is already the active mode (menu entry disabled)")
        page.keyboard.press("Escape")
        jitter()
        return want
    opt.click()
    jitter(1.0, 2.0)
    btn = find(page, "model_picker", timeout_ms=8000, required=False)
    now = (btn.get_attribute("aria-label") or "").split("currently")[-1].strip() if btn else "?"
    log(f"model: {current or '?'} -> {now}")
    return now


def _open_upload_menu(page, menu):
    """Click the upload button, surviving a composer that is still settling.

    The SECOND attach is the one that breaks (2026-08-23). Batch one — the nine
    canon files — clicks fine; batch two, the mp4, dies after 30s on
    "waiting for element to be visible, enabled and stable" with the locator
    already resolved to the right button. By then nine chips have rendered and
    the composer is still reflowing, so Playwright's stability check (same box
    two animation frames running) keeps failing on an element that IS clickable.

    Attaching both batches in one set_input_files call is NOT the fix — the app
    keeps the documents and silently drops the video (see the two-batch rule in
    docs/gemini-browser-decode-setup.md).

    So: settle it into view, try normally, and only then force. The force path is
    logged, because a click that had to bypass actionability is exactly the thing
    a later failure will want to know about.
    """
    try:
        menu.scroll_into_view_if_needed(timeout=5000)
    except Exception:
        pass
    for attempt, force in ((1, False), (2, False), (3, True)):
        try:
            menu.click(timeout=15000, force=force)
            if force:
                log("upload menu opened with force=True (composer never went stable)")
            return
        except Exception as exc:
            if attempt == 3:
                where = dump_dom(page, "upload_menu_unclickable")
                raise RuntimeError(
                    f"could not open the upload menu after 3 tries "
                    f"({exc.__class__.__name__}). Evidence at {where}"
                ) from exc
            jitter(1.5, 2.5)


def attach(page, paths):
    """The file input is only mounted while the upload menu is open, so the menu
    opens first; the files are then set on the input directly, which skips the OS
    picker. All files go in ONE call — set_input_files replaces the list."""
    menu = find(page, "upload_menu", timeout_ms=10000)
    _open_upload_menu(page, menu)
    jitter(1.5, 2.5)
    inp = find(page, "file_input", timeout_ms=10000, required=False)
    if inp is None:  # the input is hidden by design, so visibility is not required
        inp = page.locator(SELECTORS["file_input"][0]).first
        if not inp.count():
            where = dump_dom(page, "miss_file_input")
            raise RuntimeError(f"no file input after opening the upload menu. Evidence at {where}")
    total = sum(os.path.getsize(p) for p in paths)
    log(f"attaching {len(paths)} file(s), {total/1e6:.1f} MB")
    inp.set_input_files(paths)
    jitter(1.0, 2.0)
    page.keyboard.press("Escape")
    return total


def wait_for_uploads(page, count, timeout_s=UPLOAD_TIMEOUT_S):
    """Done when no progress bar is left AND the chip count has stopped moving."""
    deadline = time.time() + timeout_s
    stable, last = 0, -1
    while time.time() < deadline:
        # count only VISIBLE progress bars: the app keeps hidden ones mounted for
        # the whole session, so a plain count never reaches zero.
        busy = 0
        try:
            bars = page.locator("[role=progressbar]")
            for i in range(min(bars.count(), 20)):
                if bars.nth(i).is_visible():
                    busy += 1
        except Exception:
            pass
        try:
            chips = page.locator("[class*='file-preview'], [class*='attachment-']").count()
        except Exception:
            chips = 0
        stable = stable + 1 if (not busy and chips == last) else 0
        last = chips
        if stable >= 3:
            log(f"uploads settled ({count} file(s), {chips} chip nodes)")
            jitter(1.5, 2.5)
            return True
        time.sleep(POLL_EVERY_S)
    where = dump_dom(page, "upload_timeout")
    raise RuntimeError(f"uploads still busy after {timeout_s}s. Evidence at {where}")


def assert_video_attached(page, mp4, timeout_s=240):
    """Prove the mp4 is in the composer before sending anything.

    An attached video shows as a thumbnail chip carrying its filename and
    duration. Without this check a dropped upload only surfaces much later, as a
    decode whose every field reads "not observable" — which looks like a bad read
    rather than a lost file.
    """
    name = os.path.basename(mp4)
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            body = page.locator("body").inner_text()
        except Exception:
            body = ""
        if name in body:
            log(f"video chip present: {name}")
            return True
        # With several attachments the video chip shrinks to a thumbnail whose
        # only text is its running time, so a filename match alone is not enough.
        clock = re.search(r"\b\d{1,2}:\d{2}\b", body)
        if clock:
            log(f"video chip present (duration {clock.group(0)})")
            return True
        time.sleep(POLL_EVERY_S)
    where = dump_dom(page, "video_not_attached")
    raise RuntimeError(
        f"{name} never appeared in the composer — the app kept the other files and "
        f"dropped the video, so there is nothing to decode. Evidence at {where}")


def send(page, prompt):
    box = find(page, "composer", timeout_ms=15000)
    # Do not depend on the composer being CLICKABLE. With ten attachment chips
    # above it the box resolves but never settles as "visible, enabled and
    # stable", and the click waits out its timeout on a page that is perfectly
    # usable (2026-08-13). Focus it directly and fall back to a forced click.
    try:
        box.click(timeout=8000)
    except Exception:
        log("composer not clickable — focusing it directly")
        try:
            box.evaluate("el => el.focus()")
        except Exception:
            box.click(force=True, timeout=8000)
    # Quill rejects a bulk fill on some builds; insert the text through the
    # clipboard so a 4k-char prompt does not get typed character by character.
    page.evaluate("t => navigator.clipboard.writeText(t)", prompt)
    page.keyboard.press("Control+V")
    jitter(1.0, 2.0)
    typed = (box.inner_text() or "").strip()
    if len(typed) < min(200, len(prompt) // 2):
        log(f"paste landed only {len(typed)} chars; typing instead")
        box.fill(prompt)
        jitter()
    btn = find(page, "send", timeout_ms=8000, required=False)
    # Same trap as the composer above, and it bites here too (2026-08-23): with
    # ten chips the send button resolves and is ENABLED but never settles, so a
    # plain click waits out its whole timeout and the run dies with everything
    # already attached. The existing Enter fallback did not cover it, because it
    # only ran when the button was missing or disabled - never when it was simply
    # unstable. Force, then Enter, and say which path was taken.
    if btn is not None and btn.is_enabled():
        try:
            btn.click(timeout=8000)
        except Exception:
            log("send button never went stable - forcing the click")
            try:
                btn.click(force=True, timeout=8000)
            except Exception:
                log("forced send click failed too - pressing Enter")
                page.keyboard.press("Enter")
    else:
        page.keyboard.press("Enter")
    log(f"sent ({len(prompt)} chars)")


def wait_for_answer(page, timeout_s=ANSWER_TIMEOUT_S):
    """Running while a Stop control exists; done once it is gone and the text has
    stopped growing (the app streams, so a single clean poll is not enough)."""
    deadline = time.time() + timeout_s
    seen_running = False
    last_len, stable = -1, 0
    while time.time() < deadline:
        stop = find(page, "stop", timeout_ms=1200, required=False)
        if stop is not None:
            seen_running = True
            stable = 0
        else:
            try:
                cur = len(page.locator("body").inner_text())
            except Exception:
                cur = last_len
            stable = stable + 1 if cur == last_len else 0
            last_len = cur
            if seen_running and stable >= 3:
                log("answer complete")
                jitter(1.5, 2.5)
                return True
        time.sleep(POLL_EVERY_S)
    where = dump_dom(page, "answer_timeout")
    raise RuntimeError(f"no finished answer after {timeout_s}s. Evidence at {where}")


def _from_clipboard(page):
    """The response's Copy control yields real markdown; the DOM does not."""
    for key in ("copy", "more_options"):
        btns = page.locator(",".join(SELECTORS[key]))
        n = btns.count()
        if not n:
            continue
        try:
            btns.nth(n - 1).click()
            jitter(0.8, 1.5)
            if key == "more_options":
                item = page.locator("[role=menuitem]:has-text('Copy')").first
                if not item.count():
                    page.keyboard.press("Escape")
                    continue
                item.click()
                jitter(0.8, 1.5)
            text = page.evaluate("async () => await navigator.clipboard.readText()")
            if text and len(text) > 500:
                return text
        except Exception as e:
            log(f"{key} copy path failed: {e.__class__.__name__}")
    return None


# Rebuild markdown from the rendered answer. The app renders markdown to HTML,
# and innerText throws the markers away — a 78k-char answer came back with zero
# `##` headings and the linter reported every section missing (2026-08-13). This
# walks the response node and puts the markers back.
HTML_TO_MD_JS = r"""(sel) => {
  const nodes = [...document.querySelectorAll(sel)];
  const root = nodes.length ? nodes[nodes.length - 1] : null;
  if (!root) return '';
  const out = [];
  const inline = (el) => {
    let s = '';
    for (const n of el.childNodes) {
      if (n.nodeType === 3) { s += n.textContent; continue; }
      const t = n.tagName ? n.tagName.toLowerCase() : '';
      if (t === 'strong' || t === 'b') s += '**' + inline(n).trim() + '**';
      else if (t === 'em' || t === 'i') s += '*' + inline(n).trim() + '*';
      else if (t === 'code') s += '`' + n.textContent + '`';
      else if (t === 'br') s += '\n';
      else s += inline(n);
    }
    return s;
  };
  const walk = (el, depth) => {
    for (const n of el.children) {
      const t = n.tagName.toLowerCase();
      if (/^h[1-6]$/.test(t)) out.push('\n' + '#'.repeat(+t[1]) + ' ' + inline(n).trim() + '\n');
      else if (t === 'p') out.push(inline(n).trim() + '\n');
      else if (t === 'ul' || t === 'ol') {
        [...n.children].forEach((li, i) => {
          const mark = t === 'ol' ? (i + 1) + '.' : '-';
          out.push('  '.repeat(depth) + mark + ' ' + inline(li).trim());
          walk(li, depth + 1);
        });
        out.push('');
      } else if (t === 'pre') out.push('```\n' + n.textContent.replace(/\n+$/, '') + '\n```\n');
      else if (t === 'table') {
        for (const tr of n.querySelectorAll('tr')) {
          const cells = [...tr.children].map(td => inline(td).trim().replace(/\|/g, '\\|'));
          out.push('| ' + cells.join(' | ') + ' |');
          if (tr.querySelector('th')) out.push('|' + cells.map(() => '---').join('|') + '|');
        }
        out.push('');
      } else if (t === 'hr') out.push('\n---\n');
      else if (t === 'li') continue;
      else walk(n, depth);
    }
  };
  walk(root, 0);
  return out.join('\n');
}"""


def _from_html(page):
    for sel in SELECTORS["model_turn"] + ["message-content", ".markdown", "model-response"]:
        try:
            text = page.evaluate(HTML_TO_MD_JS, sel)
        except Exception:
            continue
        if text and text.strip():
            return text
    return None


def _looks_like_markdown(text):
    """A decode carries its section markers. Plain prose is a failed extraction."""
    return len(re.findall(r"^##\s+\S", text or "", re.M)) >= 3


def _from_dom(page):
    turn = find(page, "model_turn", timeout_ms=4000, required=False)
    if turn is None:
        return None
    try:
        return turn.inner_text()
    except Exception:
        return None


def extract(page):
    """Length alone is not proof: the DOM path once returned 78k chars of prose
    with every `##` stripped, and the linter then reported all seven sections
    missing. Each path must produce something that still reads as markdown.
    """
    best = None
    for name, fn in (("clipboard", lambda: _from_clipboard(page)),
                     ("html", lambda: _from_html(page)),
                     ("dom", lambda: _from_dom(page))):
        text = fn()
        if not text:
            continue
        if len(text) > 2000 and _looks_like_markdown(text):
            log(f"extracted decode via {name} ({len(text)} chars)")
            return text, name
        why = "too short" if len(text) <= 2000 else "no markdown headings left"
        log(f"{name} rejected ({len(text)} chars, {why}) — trying the next path")
        best = best or (text, name)
    where = dump_dom(page, "extract_failed")
    if best:
        dump(best[0], "extract_failed_text")
    # Gemini's own failure reply is not an extraction problem, and reporting it as
    # one sends the next person hunting selectors. Cost 2026-08-23: hours spent on
    # a non-existent extraction bug, when the answer element held exactly
    # "Sorry, something went wrong. Please try your request again." - 77 chars,
    # correctly rejected as too short. Name it instead.
    if best and any(p in best[0].lower() for p in (
            "something went wrong", "please try your request again",
            "try again later", "unable to process")):
        raise RuntimeError(
            f"GEMINI REFUSED THE REQUEST - it answered "
            f"{best[0].strip()[:120]!r}. Nothing to extract, and nothing here is "
            f"broken: the pipeline signed in, attached and sent. This is "
            f"server-side (rate limit / quota / oversized request are the usual "
            f"causes). Wait and retry one video, or try a shorter clip before "
            f"changing any code. Evidence at {where}")
    raise RuntimeError(
        f"no extraction path returned usable markdown. Evidence at {where}")


def sanity_check(body, mp4):
    """Catch a decode of a video the model never received.

    The linter passed a file whose every field read "not observable — video not
    uploaded": one scene, one image, no content, exit 0. Structure was perfect
    and there was nothing in it. These two checks look at substance instead.
    """
    low = body.lower()
    for phrase in ("video not uploaded", "no video was", "video was not provided",
                   "i cannot see the video", "video is not attached",
                   "could not be opened", "unable to open the video",
                   "could not access the video", "video could not be processed"):
        if phrase in low:
            raise RuntimeError(
                f"the answer says it never read the video ({phrase!r}) — the file was "
                f"attached but not opened, so this is scaffolding, not a decode")

    # A phrase list only catches wording it was told about. On 2026-08-13 a 40s
    # 34MB source came back as 72k chars of perfect structure with 47% of its
    # fields reading "not observable — video file could not be opened", and it
    # PASSED the linter, because structure was never the thing that was missing.
    # Measure the emptiness instead of guessing at its phrasing.
    fields = re.findall(r"^- \*\*[a-z_ /-]+:\*\*\s*(.+)$", body, re.M | re.I)
    unusable = [f for f in fields if re.search(r"not observable|unknown|n/?a\b", f, re.I)]
    if fields and len(unusable) / len(fields) > 0.25:
        raise RuntimeError(
            f"{len(unusable)} of {len(fields)} fields ({len(unusable)/len(fields):.0%}) say "
            f"nothing was observable — the model produced the shape of a decode without "
            f"reading the video. Re-run; if it repeats, the source is too long or too "
            f"large for the app to process")
    scenes = len(re.findall(r"^###\s+Scene\s+\d+", body, re.M))
    if not scenes:
        return True

    # Seconds-per-scene is the WRONG yardstick on its own: a 119-second static
    # talking head is legitimately one scene, and warning about it is noise
    # (fired on the EverTide reel, where 3 scenes is the correct read). When the
    # real cut list is on disk, compare against THAT — a decode with fewer scenes
    # than the source has hard cuts has genuinely merged something.
    cuts = hard_cut_count(mp4)
    if cuts:
        if scenes < cuts:
            log(f"WARNING: {scenes} scene(s) but the source has {cuts} hard cuts — "
                f"at least one cut was merged away, check it by hand")
        return True

    facts = source_facts(mp4)
    m = re.search(r"duration ([\d.]+)s", facts)
    if m and float(m.group(1)) / scenes > 45:
        log(f"WARNING: {scenes} scene(s) for {m.group(1)}s of video and no cut list "
            f"on disk to check against — if the source is not a single static take, "
            f"the read is collapsed")
    return True


def hard_cut_count(mp4):
    """How many hard cuts PySceneDetect found, when the prep is on disk."""
    root = os.path.dirname(os.path.abspath(mp4))
    hits = [os.path.join(dp, "clips.tsv") for dp, _, fs in os.walk(root) if "clips.tsv" in fs]
    if not hits:
        return 0
    try:
        rows = [r for r in open(hits[0], encoding="utf-8").read().splitlines() if r.strip()]
    except OSError:
        return 0
    return max(0, len(rows) - 1)  # minus the header


def clean(text):
    """Undo what the app's copy-markdown does to the file on the way out.

    Observed 2026-08-13 on a real decode: bullets came back as `* **image:**`
    where the parser wants `- **image:**`, and the first front-matter key came
    back as a heading (`## shell: talking-head`), which breaks the YAML block.
    """
    text = text.replace("\r\n", "\n").strip()
    text = re.sub(r"^```(?:markdown|md)?\n", "", text)
    text = re.sub(r"\n```$", "", text)
    i = text.find("---\n")
    if 0 < i < 400:
        text = text[i:]

    # front matter: drop heading marks the renderer added to its keys
    if text.startswith("---\n"):
        end = text.find("\n---", 4)
        if end > 0:
            head = re.sub(r"(?m)^#{1,6}\s+(?=\S+:)", "", text[4:end])
            head = re.sub(r"(?m)^\s*$\n", "", head, count=1)
            text = "---\n" + head + text[end:]

    # list markers: the parser matches `- **field:**` only
    text = re.sub(r"(?m)^(\s*)\*(\s+\*\*)", r"\1-\2", text)
    return text.strip() + "\n"


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

def decode(page, mp4, slug, note="", model=DEFAULT_MODEL, out=None, repair_rounds=0,
           prep=None, use_prep=True):
    manifest, sys_path, canon = pack_files()
    log(f"pack {manifest['pack_sha']} ({manifest['mode']}, built {manifest['built']})")

    new_chat(page)
    select_model(page, model)

    docs = swap_self_example(canon, mp4)
    # The app silently keeps the FIRST ten attachments. On 2026-08-13 an eleventh
    # canon file pushed the mp4 out and the model answered "not observable — video
    # not uploaded" for every field, which reads like a bad decode, not a lost
    # upload. Refuse instead of decoding a video that never arrived.
    if len(docs) + 1 > MAX_ATTACHMENTS:
        raise RuntimeError(
            f"{len(docs) + 1} attachments but the app takes {MAX_ATTACHMENTS}; the video "
            f"would be dropped. Rebuild the pack (tools/build_gemini_decode_pack.py "
            f"caps the canon at {MAX_ATTACHMENTS - 1} files).")

    # TWO batches, never one. A single set_input_files carrying documents AND a
    # video loses the video: the app takes the batch, keeps the nine documents and
    # silently drops the odd one out — verified 2026-08-13 against a composer
    # screenshot showing nine doc chips and no video. Sent separately, both stay.
    attach(page, docs)
    wait_for_uploads(page, len(docs))
    attach(page, [mp4])
    wait_for_uploads(page, 1)
    assert_video_attached(page, mp4)

    prompt = instructions(sys_path) + "\n\n---\n\ndecode this video."
    facts = source_facts(mp4)
    if facts:
        prompt += f"\n\n{facts}"
    if use_prep:
        block = prep_block(mp4, prep)
        if block:
            prompt += f"\n\n{block}"
    if note:
        prompt += f"\n\n{note}"
    send(page, prompt)
    wait_for_answer(page)

    text, how = extract(page)
    out = out or os.path.join(REPO_ROOT, "raw", "videos", f"decoded_{slug}.md")
    body = clean(text)
    sanity_check(body, mp4)
    write(out, body, how)

    # The linter knows exactly what is missing, and the chat still holds the
    # video and the rules — so hand the failures back and let it repair in place
    # rather than making the operator re-run the whole decode.
    for round_no in range(1, repair_rounds + 1):
        ok, report = verify(out)
        if ok:
            break
        log(f"repair round {round_no}: asking for a corrected file")
        send(page, "The file you produced fails these checks from our linter:\n\n"
                   f"{report}\n\n"
                   "Re-emit the COMPLETE corrected file, nothing else, starting at "
                   "the YAML front matter. Keep every observation you already made, "
                   "including every front-matter key; fix only what the checks name.")
        wait_for_answer(page)
        body = clean(text := extract(page)[0])
        sanity_check(body, mp4)
        write(out, body, f"repair {round_no}")
    return out


def write(path, body, how):
    with open(path, "w", encoding="utf-8") as f:
        f.write(body)
    log(f"wrote {path} ({len(body)} chars, via {how})")


def verify(path):
    r = subprocess.run(
        [sys.executable, os.path.join(REPO_ROOT, "code", "verify_decode_format.py"), path],
        capture_output=True, text=True, cwd=REPO_ROOT)
    report = ((r.stdout or "") + (r.stderr or "")).strip()
    log("decode linter:\n" + report)
    return r.returncode == 0, report


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--email", help="Google account; its session is copied from a NON-STABLE Chrome channel")
    ap.add_argument("--login", action="store_true", help="open the window and stop once signed in")
    ap.add_argument("--decode", metavar="MP4", help="video file to decode")
    ap.add_argument("--slug", help="output slug -> raw/videos/decoded_<slug>.md")
    ap.add_argument("--out", help="write here instead of raw/videos/decoded_<slug>.md")
    ap.add_argument("--model", default=DEFAULT_MODEL, help="mode-picker entry, e.g. '3.1 Pro'")
    ap.add_argument("--repair-rounds", type=int, default=0,
                    help="patch failures in the same chat instead of fixing the prompt. "
                         "Default 0: read the error, improve the prompt, run again.")
    ap.add_argument("--prep", help="folder holding hardcut/clips.tsv + the whisper json "
                                    "(defaults to the video's own folder)")
    ap.add_argument("--no-prep", action="store_true",
                    help="ignore prep on disk; decode from the video alone")
    ap.add_argument("--note", default="", help="extra context appended to the prompt")
    ap.add_argument("--allow-stale", action="store_true", help="run even if the pack lags repo canon")
    ap.add_argument("--firefox", action="store_true",
                    help="drive Camoufox instead of Chrome. Use when Chrome stops being "
                         "able to READ uploads (chip appears, model says it cannot open the file).")
    ap.add_argument("--no-reseed", action="store_true",
                    help="skip the per-run Firefox profile pull (it is on by default; "
                         "the transplanted session only survives one launch)")
    ap.add_argument("--headless", action="store_true", help="not recommended; Google flags it")
    args = ap.parse_args()

    if not (args.login or args.decode):
        ap.error("pick --login or --decode")
    if args.decode:
        if not args.slug:
            ap.error("--decode needs --slug")
        if not os.path.exists(args.decode):
            ap.error(f"no such file: {args.decode}")
        fresh, msg = pack_is_fresh()
        log(msg)
        if not fresh and not args.allow_stale:
            raise SystemExit("pack is stale — rebuild it, or pass --allow-stale")

    _load_learned()
    if args.email:
        gvw.use_account(args.email)

    if args.firefox:
        global FF_PROFILE_DIR
        if os.environ.get("GEMINI_FF_PROFILE_DIR"):
            FF_PROFILE_DIR = os.environ["GEMINI_FF_PROFILE_DIR"]
        elif args.email:
            # A per-RUN directory. Camoufox leaves state behind that survives an
            # rmtree of the same path (a live process still holding files makes
            # the delete a silent no-op), and a profile that has been launched
            # once no longer accepts the pulled session. A path that never
            # existed before cannot carry that history.
            safe = re.sub(r"[^A-Za-z0-9._-]", "_", args.email.strip().lower())
            FF_PROFILE_DIR = os.path.join(
                BASE_DIR, f".gemini_ff_run_{safe}_{os.getpid()}")
        # Seed from a REAL Firefox profile on this machine, the same way the
        # Flow worker does. Chrome's cookies alone do not authenticate Google.
        #
        # EVERY RUN, not just the first. The transplanted session survives one
        # Camoufox launch and is gone by the next (measured 2026-08-13: clean
        # pull -> signed in on Pro; relaunch the same profile -> signed out on
        # Flash-Lite). The pull costs six file copies, so re-seeding beats
        # decoding against a stranger's Gemini.
        if not args.no_reseed:
            pull_firefox_profile(args.email)
        sync_playwright = firefox_playwright()
    else:
        sync_playwright = gvw._import_playwright()
    with sync_playwright() as p:
        ctx, page = (launch_firefox(p, headless=args.headless) if args.firefox
                     else gvw.launch(p, headless=args.headless))
        try:
            # Prove the session EVERY run. The inherited check passed a
            # signed-out page and everything after it was worthless.
            ensure_session(page, args.email, firefox=args.firefox)
            if args.login:
                log("logged in. Profile is warm for later runs.")
                return 0
            out = decode(page, args.decode, args.slug, args.note, args.model,
                         args.out, args.repair_rounds, args.prep, not args.no_prep)
            return 0 if verify(out)[0] else 2
        finally:
            try:
                ctx.close()
            except Exception:
                pass


if __name__ == "__main__":
    sys.exit(main())
