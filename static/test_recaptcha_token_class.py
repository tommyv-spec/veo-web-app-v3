#!/usr/bin/env python3
"""
Scoped experiment: does the browser ENGINE decide the reCAPTCHA token class Flow gets?

Background (measured 2026-08-07 over 9995 captured generate calls):
every Flow generate carries clientContext.recaptchaContext.token. Two classes appear:

    "0cA..."  (~2530-2560 chars)  real reCAPTCHA Enterprise token -> generation runs
    "HF..."   (~2780-3130 chars)  different class                 -> 403 PERMISSION_DENIED,
                                                                    "reCAPTCHA evaluation failed",
                                                                    PUBLIC_ERROR_UNUSUAL_ACTIVITY

Real-token rate: ~100% until 08-02, then 35% / 39% / 8.7% -> ~0%. The break window
contains a Chrome auto-update to 151.0.7922.75. Every worker-side fix failed, which is
consistent with nothing the worker does selecting the class.

This script tests ONE hypothesis and nothing else: run Flow under a DIFFERENT engine
(Firefox) and see which class it mints. It deliberately does NOT touch flow_worker's
Chrome-only plumbing (window HWND management, chrome_warmup, golden restore, Patchright
stealth), because none of that can be the cause if the class is engine-decided.

It is a MEASUREMENT harness, not a worker. It opens a browser, records traffic, and
tallies. You drive the page by hand: log in, then submit a few generates.

Usage
-----
  # 1. control run FIRST (proves the rig works and pins today's Chrome rate)
  python code/static/test_recaptcha_token_class.py --browser chrome

  # 2. the actual experiment
  python code/static/test_recaptcha_token_class.py --browser firefox

  # 3. tally either/both without launching anything
  python code/static/test_recaptcha_token_class.py --report

Each run uses its OWN profile dir and its OWN jsonl, so nothing here can disturb the
worker's session, its golden, or the production capture at
%LOCALAPPDATA%/Temp/veo_shm/flow_api_capture.jsonl.

The Firefox profile starts EMPTY -> you must sign in to Google once in the window. That
is expected and is not a bug; Chrome cookies cannot be reused by a different engine.

Verdict
-------
  firefox run mostly "0cA"  -> engine decides the class. Fixing the Firefox path in
                               flow_worker.py becomes worth the investment.
  firefox run mostly "HF"   -> engine does NOT decide it. Abandon this lever and fall
                               back to the other open hypothesis (pre-151 Chrome).
"""

import argparse
import json
import os
import sys
import tempfile
import time
from collections import Counter

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# The submit endpoints that carry a recaptcha token.
GENERATE_MARKERS = (
    "batchAsyncGenerateVideoStartImage",
    "batchAsyncGenerateVideoStartAndEndImage",
    "batchAsyncGenerateVideoReferenceImages",
    "batchAsyncGenerateVideo",
    "flowMedia:batchGenerateImages",
    "batchGenerateImages",
)

FLOW_URL = "https://labs.google/fx/tools/flow"


def default_out_path(browser):
    shm = os.path.join(tempfile.gettempdir(), "veo_shm")
    try:
        os.makedirs(shm, exist_ok=True)
    except Exception:
        shm = tempfile.gettempdir()
    return os.path.join(shm, f"recaptcha_token_test_{browser}.jsonl")


def default_profile_dir(browser):
    return os.path.join(BASE_DIR, f".recaptcha_test_profile_{browser}")


def classify(token):
    """Map a raw token to its class. Prefixes are the measured discriminator."""
    if not token:
        return "empty"
    if token.startswith("0cA"):
        return "real"          # accepted -> generation runs
    if token.startswith("HF"):
        return "refused"       # 403 "reCAPTCHA evaluation failed"
    return "other:" + token[:4]


def extract_tokens(body):
    """Pull every recaptcha token out of a generate body.

    The token appears at clientContext.recaptchaContext.token and/or at
    requests[].clientContext.recaptchaContext.token (see flow_api/builders.py
    inject_captcha_token, which fills both).
    """
    tokens = []
    if not body:
        return tokens
    try:
        j = json.loads(body)
    except Exception:
        return tokens

    def _take(ctx):
        if isinstance(ctx, dict):
            rc = ctx.get("recaptchaContext")
            if isinstance(rc, dict):
                t = rc.get("token")
                if isinstance(t, str):
                    tokens.append(t)

    if isinstance(j, dict):
        _take(j.get("clientContext"))
        for r in (j.get("requests") or []):
            if isinstance(r, dict):
                _take(r.get("clientContext"))
    return tokens


def install_listeners(page, out_path, browser, tally):
    """Read-only request/response listeners. Never raise into the page."""

    def _write(row):
        row["ts"] = time.time()
        row["browser"] = browser
        try:
            with open(out_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(row) + "\n")
        except Exception:
            pass

    def _on_request(req):
        try:
            url = getattr(req, "url", "") or ""

            if "recaptcha" in url:
                leaf = url.split("?", 1)[0].rsplit("/", 1)[-1] or "recaptcha"
                print(f"  [mint] recaptcha:{leaf} {getattr(req, 'method', '')}", flush=True)
                _write({"kind": "recaptcha", "leaf": leaf, "url": url,
                        "method": getattr(req, "method", "")})
                return

            if not any(m in url for m in GENERATE_MARKERS):
                return

            try:
                body = req.post_data
            except Exception:
                body = None

            tokens = extract_tokens(body)
            endpoint = url.split("?", 1)[0].rsplit("/", 1)[-1]
            if not tokens:
                print(f"  [submit] {endpoint} — NO TOKEN IN BODY", flush=True)
                _write({"kind": "generate", "endpoint": endpoint, "token_class": "empty"})
                return

            for t in tokens:
                cls = classify(t)
                tally[cls] += 1
                mark = "OK " if cls == "real" else "BAD"
                print(f"  [submit] {endpoint} token={cls} len={len(t)} [{mark}]", flush=True)
                _write({"kind": "generate", "endpoint": endpoint,
                        "token_class": cls, "token_len": len(t),
                        "token_prefix": t[:8]})
        except Exception:
            pass

    def _on_response(resp):
        try:
            url = getattr(resp, "url", "") or ""
            if not any(m in url for m in GENERATE_MARKERS):
                return
            status = getattr(resp, "status", 0)
            endpoint = url.split("?", 1)[0].rsplit("/", 1)[-1]
            tally[f"http_{status}"] += 1
            print(f"  [resp]   {endpoint} HTTP {status}", flush=True)
            _write({"kind": "response", "endpoint": endpoint, "status": status})
        except Exception:
            pass

    page.on("request", _on_request)
    page.on("response", _on_response)


def print_summary(tally, out_path):
    real = tally.get("real", 0)
    refused = tally.get("refused", 0)
    total = real + refused
    print("")
    print("=" * 62)
    print("TOKEN CLASS SUMMARY")
    print("=" * 62)
    if total == 0:
        print("  No generate submits captured. Did you click Generate in the window?")
    else:
        print(f"  real   (0cA, accepted) : {real:4d}  ({100.0 * real / total:.1f}%)")
        print(f"  refused(HF,  403)      : {refused:4d}  ({100.0 * refused / total:.1f}%)")
    for k, v in sorted(tally.items()):
        if k in ("real", "refused"):
            continue
        print(f"  {k:22s} : {v:4d}")
    print("")
    if total:
        if real == 0:
            print("  VERDICT: engine mints ONLY refused tokens. This lever is dead.")
        elif refused == 0:
            print("  VERDICT: engine mints ONLY real tokens. Strong signal — worth pursuing.")
        else:
            print("  VERDICT: mixed. Collect more submits before deciding.")
    print(f"  raw rows -> {out_path}")
    print("=" * 62)


def report(paths):
    """Tally existing jsonl files without launching a browser.

    Understands TWO formats:
      * this script's own rows  — token already classified into token_class
      * flow_worker.py's rows   — raw generate body under body_raw (see
        flow_worker.py _install_flow_api_capture); the token is dug out here.
    That second format is what run_firefox_worker.ps1 produces, so a real
    worker run can be tallied with the same command.
    """
    any_found = False
    for p in paths:
        if not os.path.isfile(p):
            continue
        any_found = True
        tally = Counter()
        with open(p, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                if row.get("kind") == "generate" and row.get("token_class"):
                    tally[row["token_class"]] += 1
                elif row.get("kind") == "response":
                    tally[f"http_{row.get('status')}"] += 1
                elif row.get("body_raw"):
                    # worker-format row — classify the token(s) inside the body
                    for t in extract_tokens(row["body_raw"]):
                        tally[classify(t)] += 1
        print(f"\n### {os.path.basename(p)}")
        print_summary(tally, p)
    if not any_found:
        print("No test capture files found yet. Run a browser session first.")


def launch(browser, profile_dir, out_path, minutes):
    # Patchright is the undetected Playwright fork the worker uses. For the chrome
    # CONTROL run we want the same stack the worker has, so the comparison is fair.
    # For firefox, plain playwright is fine (patchright's patches are chromium-only).
    if browser == "chrome":
        try:
            from patchright.sync_api import sync_playwright
            print("[init] using patchright (matches worker stealth stack)")
        except ImportError:
            from playwright.sync_api import sync_playwright
            print("[init] WARNING: patchright missing — chrome control is NOT worker-equivalent")
    else:
        from playwright.sync_api import sync_playwright

    os.makedirs(profile_dir, exist_ok=True)
    tally = Counter()

    print(f"[init] browser  : {browser}")
    print(f"[init] profile  : {profile_dir}")
    print(f"[init] capture  : {out_path}")
    print("")

    with sync_playwright() as p:
        if browser == "chrome":
            # Mirror flow_worker.py single-account launch (see ~L24079 single_chrome_args)
            # so a chrome control run is comparable to a real worker run.
            args = [
                "--disable-blink-features=AutomationControlled",
                "--disable-focus-on-load",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--force-variation-ids=3300115,3300134,3313321,3328827,3330196,3362821",
                "--disk-cache-size=1",
                "--media-cache-size=1",
                "--mute-audio",
            ]
            ctx = p.chromium.launch_persistent_context(
                user_data_dir=profile_dir,
                channel=os.environ.get("WORKER_CHROME_CHANNEL", "chrome"),
                ignore_default_args=["--enable-automation"],
                headless=False,
                viewport={"width": 1280, "height": 800},
                args=args,
            )
        else:
            ctx = p.firefox.launch_persistent_context(
                user_data_dir=profile_dir,
                headless=False,
                viewport={"width": 1280, "height": 800},
            )

        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        install_listeners(page, out_path, browser, tally)

        # Flow can open work in another tab; instrument those too.
        def _on_page(new_page):
            try:
                install_listeners(new_page, out_path, browser, tally)
                print("  [init] instrumented a new tab", flush=True)
            except Exception:
                pass

        ctx.on("page", _on_page)

        try:
            page.goto(FLOW_URL, timeout=60000)
        except Exception as e:
            print(f"[warn] initial navigation failed ({e}) — navigate by hand", flush=True)

        deadline = time.time() + minutes * 60
        print("-" * 62)
        print("DRIVE THE PAGE BY HAND NOW:")
        print("  1. sign in to Google if the window asks (firefox profile starts empty)")
        print("  2. open or create a project")
        print("  3. submit 3-5 generates")
        print("  4. watch the [submit] lines below — token=real means 0cA")
        print(f"  window closes automatically in {minutes} min, or press Ctrl-C")
        print("-" * 62)

        try:
            while time.time() < deadline:
                if not ctx.pages:
                    print("[init] all tabs closed — stopping", flush=True)
                    break
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[init] interrupted", flush=True)

        try:
            ctx.close()
        except Exception:
            pass

    print_summary(tally, out_path)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--browser", choices=["firefox", "chrome"], default="firefox",
                    help="firefox = the experiment, chrome = the control (default: firefox)")
    ap.add_argument("--profile", default=None, help="profile dir (default: per-browser, isolated)")
    ap.add_argument("--out", default=None, help="jsonl capture path (default: per-browser, isolated)")
    ap.add_argument("--minutes", type=int, default=20, help="how long to keep the window open")
    ap.add_argument("--report", action="store_true",
                    help="tally existing captures and exit — launches nothing")
    a = ap.parse_args()

    if a.report:
        paths = [a.out] if a.out else [default_out_path(b) for b in ("chrome", "firefox")]
        report(paths)
        return 0

    profile = a.profile or default_profile_dir(a.browser)
    out = a.out or default_out_path(a.browser)
    launch(a.browser, profile, out, a.minutes)
    return 0


if __name__ == "__main__":
    sys.exit(main())
