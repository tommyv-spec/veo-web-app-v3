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
    if want.lower() in current.lower():
        log(f"model already {current!r}")
        return current
    btn.click()
    jitter(1.0, 2.0)
    opt = page.locator(f"gem-menu-item:has-text('{want}'), [role=menuitem]:has-text('{want}')").first
    if not opt.count():
        where = dump_dom(page, "model_option_miss")
        raise RuntimeError(f"mode {want!r} not in the picker. Evidence at {where}")
    opt.click()
    jitter(1.0, 2.0)
    btn = find(page, "model_picker", timeout_ms=8000, required=False)
    now = (btn.get_attribute("aria-label") or "").split("currently")[-1].strip() if btn else "?"
    log(f"model: {current or '?'} -> {now}")
    return now


def attach(page, paths):
    """The file input is only mounted while the upload menu is open, so the menu
    opens first; the files are then set on the input directly, which skips the OS
    picker. All files go in ONE call — set_input_files replaces the list."""
    menu = find(page, "upload_menu", timeout_ms=10000)
    menu.click()
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


def send(page, prompt):
    box = find(page, "composer", timeout_ms=15000)
    box.click()
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
    if btn is not None and btn.is_enabled():
        btn.click()
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


def _from_dom(page):
    turn = find(page, "model_turn", timeout_ms=4000, required=False)
    if turn is None:
        return None
    try:
        return turn.inner_text()
    except Exception:
        return None


def extract(page):
    for name, fn in (("clipboard", lambda: _from_clipboard(page)),
                     ("dom", lambda: _from_dom(page))):
        text = fn()
        if text and len(text) > 2000:
            log(f"extracted decode via {name} ({len(text)} chars)")
            return text, name
        if text:
            log(f"{name} gave only {len(text)} chars — trying the next path")
    where = dump_dom(page, "extract_failed")
    raise RuntimeError(f"could not extract the decode. Evidence at {where}")


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

def decode(page, mp4, slug, note="", model=DEFAULT_MODEL, out=None, repair_rounds=2):
    manifest, sys_path, canon = pack_files()
    log(f"pack {manifest['pack_sha']} ({manifest['mode']}, built {manifest['built']})")

    new_chat(page)
    select_model(page, model)

    files = canon + [mp4]
    attach(page, files)
    wait_for_uploads(page, len(files))

    prompt = instructions(sys_path) + "\n\n---\n\ndecode this video."
    if note:
        prompt += f"\n\n{note}"
    send(page, prompt)
    wait_for_answer(page)

    text, how = extract(page)
    out = out or os.path.join(REPO_ROOT, "raw", "videos", f"decoded_{slug}.md")
    write(out, clean(text), how)

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
        text, how = extract(page)
        write(out, clean(text), f"{how}, repair {round_no}")
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
    ap.add_argument("--repair-rounds", type=int, default=2,
                    help="times to hand linter failures back to the chat for repair")
    ap.add_argument("--note", default="", help="extra context appended to the prompt")
    ap.add_argument("--allow-stale", action="store_true", help="run even if the pack lags repo canon")
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

    sync_playwright = gvw._import_playwright()
    with sync_playwright() as p:
        ctx, page = gvw.launch(p, headless=args.headless)
        try:
            gvw.ensure_logged_in(page)
            if args.login:
                log("logged in. Profile is warm for later runs.")
                return 0
            out = decode(page, args.decode, args.slug, args.note, args.model,
                         args.out, args.repair_rounds)
            return 0 if verify(out)[0] else 2
        finally:
            try:
                ctx.close()
            except Exception:
                pass


if __name__ == "__main__":
    sys.exit(main())
