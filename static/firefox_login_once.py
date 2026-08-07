#!/usr/bin/env python3
"""One-time Google sign-in for the Firefox (Camoufox) worker profile.

WHY THIS EXISTS
---------------
The Chrome worker never asks you to sign in: `_maybe_pull_laptop_profile` seeds
its golden from your real Chrome profile, which is already logged in. Firefox
cannot use that — a Chrome profile is unreadable by Firefox, and copying one in
actively corrupts the Firefox profile (that guard is now in flow_worker.py).

So a Firefox worker needs ONE manual sign-in per account. But the worker only
waits about two minutes at the sign-in page before giving up, and when it does
it misreports the cause:

    [STARTUP] Login verified after sign-in        <- false positive
    [STARTUP] Account is NOT ULTRA - cannot use Flow
    "Delete session folders and restart with an ULTRA account."

That advice is wrong and destructive — deleting the session folder throws away
the very sign-in you are trying to create. The account is fine; nobody was at
the keyboard in time.

This script opens the SAME profile directory the worker uses and waits as long
as you need. Sign in, then close the window (or press Ctrl-C). The worker then
starts already logged in, and builds its golden from that session.

USAGE
-----
    python code/static/firefox_login_once.py            # Account1
    python code/static/firefox_login_once.py --account 2

Then start the worker normally:  ~/veo-worker/start_worker.bat
"""

import argparse
import os
import sys
import time

FLOW_URL = "https://labs.google/fx/tools/flow"


def worker_base_dir():
    """Where the installer keeps worker state (matches flow_worker's _BASE)."""
    return os.environ.get("WORKER_BASE_DIR") or os.path.join(
        os.path.expanduser("~"), "veo-worker")


def profile_dir(account):
    """Must match flow_worker's per-engine naming: firefox-session[-N]."""
    suffix = "session" if account == 1 else f"session-{account}"
    return os.path.join(worker_base_dir(), f"firefox-{suffix}")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--account", type=int, default=1,
                    help="Which worker account slot to sign in (default 1)")
    ap.add_argument("--minutes", type=int, default=60,
                    help="How long to keep the window open (default 60)")
    a = ap.parse_args()

    prof = profile_dir(a.account)
    os.makedirs(prof, exist_ok=True)

    try:
        from playwright.sync_api import sync_playwright
        from camoufox.sync_api import NewBrowser
    except ImportError as e:
        print(f"ERROR: missing dependency ({e})", file=sys.stderr)
        print("  pip install camoufox>=0.5.4 playwright", file=sys.stderr)
        print("  python -m camoufox fetch", file=sys.stderr)
        return 1

    kwargs = {"user_data_dir": prof, "headless": False, "os": "windows"}
    # Camoufox >=0.5 hard-fails if its bundled uBlock is missing, and that
    # download comes from GitHub, which a VPN can block. Not wanted on Flow.
    try:
        from camoufox.addons import DefaultAddons
        kwargs["exclude_addons"] = [DefaultAddons.UBO]
    except ImportError:
        pass

    print("=" * 62)
    print(f" FIREFOX SIGN-IN  -  Account{a.account}")
    print("=" * 62)
    print(f"  profile : {prof}")
    print("")
    print("  1. Sign in to Google in the window that opens")
    print("  2. Wait until Flow actually loads (you should see the Flow UI)")
    print("  3. Close the window, or press Ctrl-C here")
    print("")
    print("  Then start the worker: ~/veo-worker/start_worker.bat")
    print("  If it still says 'NOT ULTRA', the sign-in did not stick - re-run this.")
    print("=" * 62)
    print("")

    with sync_playwright() as p:
        ctx = NewBrowser(p, persistent_context=True, **kwargs)
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        try:
            page.goto(FLOW_URL, timeout=60000)
        except Exception as e:
            print(f"[warn] could not open Flow ({e}) - navigate by hand", flush=True)

        deadline = time.time() + a.minutes * 60
        try:
            while time.time() < deadline:
                if not ctx.pages:
                    print("\nWindow closed - sign-in saved to the profile.", flush=True)
                    break
                time.sleep(2)
            else:
                print(f"\n{a.minutes} min elapsed - closing.", flush=True)
        except KeyboardInterrupt:
            print("\nInterrupted - sign-in saved to the profile.", flush=True)

        try:
            ctx.close()
        except Exception:
            pass

    cookies_db = os.path.join(prof, "cookies.sqlite")
    if os.path.isfile(cookies_db) and os.path.getsize(cookies_db) > 0:
        print(f"Profile has a cookie store ({os.path.getsize(cookies_db)} bytes).")
        print("Start the worker now: ~/veo-worker/start_worker.bat")
    else:
        print("WARNING: no cookie store written - the sign-in may not have saved.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
