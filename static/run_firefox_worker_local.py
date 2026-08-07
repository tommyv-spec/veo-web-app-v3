#!/usr/bin/env python3
"""
Local harness: run flow_worker.py on Firefox with a WORKING browser driver.

Why this file exists
--------------------
flow_worker.py imports its driver from Patchright (flow_worker.py:148), the
undetected Playwright fork. Patchright's patches are CHROMIUM-ONLY, and they
break Firefox outright. Measured directly, same script, same page:

    patchright + firefox : page.evaluate -> Error: Cannot read properties of
                                            undefined (reading '_client')
    playwright  + firefox : page.evaluate -> 2   (works)

page.evaluate is load-bearing all through the worker (login detection, button
dumps, DOM polls), so under Patchright the Firefox branch cannot get past the
landing page — it loops "No entry button found" and hard-reloads Flow forever.

This harness therefore does two things, and nothing else:

  1. Points the name `patchright.sync_api` at a shim BEFORE flow_worker is
     imported, so the worker's own `from patchright.sync_api import
     sync_playwright` resolves to a working driver. flow_worker.py is NOT
     edited — that is why this harness exists instead of a patch.

     The shim launches CAMOUFOX, not stock Playwright Firefox. Stock Playwright
     Firefox gets past page.evaluate but cannot log in: Google's OAuth answers
     "Couldn't sign you in - This browser or app may not be secure", because
     navigator.webdriver is True. Camoufox is the Firefox-side equivalent of
     Patchright. Measured on a Camoufox context:
         page.evaluate('1+1')        -> 2
         navigator.webdriver         -> False
         navigator.plugins.length    -> 5

  2. Imports flow_worker as a MODULE and calls main() directly, so the
     `if __name__ == "__main__"` block never runs. That block calls
     check_for_updates(), which re-downloads flow_worker.py from Render and
     os.replace()s it (flow_worker.py:25860) — it would overwrite any local
     experiment before it ever ran.

Losing Patchright's stealth is FINE HERE and is in fact the point: its patches
only ever applied to Chromium, so the Firefox path never had them. Be aware
when reading results though — a "refused" token outcome on Firefox is
ambiguous between "the engine mints refused tokens" and "no stealth", while a
"real" token outcome is unambiguous and is the result worth chasing.

Not for production. The real fix, if Firefox proves out, is a conditional
import in flow_worker.py keyed on BROWSER_MODE, deployed normally.

Usage: launched by run_firefox_worker.ps1, which sets the environment first.
"""

import os
import sys


CAMOUFOX_OS = os.environ.get("CAMOUFOX_OS", "windows")


class _FirefoxShim:
    """Routes the worker's launch_persistent_context call into Camoufox.

    Camoufox IS the reason this harness can log in at all. Plain Playwright
    Firefox sets navigator.webdriver=True, and Google's OAuth refuses it with
    "Couldn't sign you in / This browser or app may not be secure". Measured on
    a Camoufox context: navigator.webdriver -> False, navigator.plugins -> 5.
    """

    def __init__(self, pw):
        self._pw = pw

    def launch_persistent_context(self, **kwargs):
        from camoufox.sync_api import NewBrowser

        # os= keeps the fingerprint coherent with the real machine. Camoufox
        # otherwise picks at random and happily served a macOS UA on this
        # Windows box — a mismatch reCAPTCHA could score against us.
        kwargs.setdefault("os", CAMOUFOX_OS)

        # Camoufox >=0.5 hard-fails the launch if its bundled uBlock Origin is
        # missing (addons.confirm_paths -> InvalidAddonPath), and that download
        # comes from GitHub, which is exactly what the Surfshark MTU issue breaks.
        # An ad blocker is not wanted on Flow anyway — skip it rather than make
        # every launch depend on a flaky fetch.
        try:
            from camoufox.addons import DefaultAddons

            kwargs.setdefault("exclude_addons", [DefaultAddons.UBO])
        except ImportError:
            pass  # older camoufox has no default addons to exclude

        # Window sizing. flow_worker hardcodes viewport 1280x500 on the Firefox
        # branch (flow_worker.py:24160) — far shorter than any real screen, which
        # clips the Flow UI. A fixed `viewport` also pins the page size, so the
        # content would letterbox even if the OS window were resized by hand.
        # Drop it, and size the real window from FIREFOX_WINDOW instead.
        win = os.environ.get("FIREFOX_WINDOW", "").lower().strip()
        if win and "x" in win:
            try:
                w, h = (int(v) for v in win.split("x", 1))
                kwargs.pop("viewport", None)
                kwargs["window"] = (w, h)
                kwargs["no_viewport"] = True
                print(f"[harness] window sized {w}x{h} (viewport unpinned)", flush=True)
            except ValueError:
                print(f"[harness] bad FIREFOX_WINDOW={win!r} — keeping worker default", flush=True)

        try:
            return NewBrowser(self._pw, persistent_context=True, **kwargs)
        except TypeError as e:
            # Camoufox owns screen/viewport as part of the fingerprint and rejects
            # some raw Playwright kwargs. Drop them rather than fail the launch.
            dropped = [k for k in ("viewport", "no_viewport", "ignore_default_args",
                                   "channel", "args") if k in kwargs]
            if not dropped:
                raise
            for k in dropped:
                kwargs.pop(k, None)
            print(f"[harness] camoufox rejected {dropped} ({e}) — retrying without", flush=True)
            return NewBrowser(self._pw, persistent_context=True, **kwargs)


class _PlaywrightProxy:
    """Playwright object with .firefox AND .chromium routed to Camoufox.

    Routing .chromium too is not a shortcut — it is required. flow_worker's
    RECOVERY paths call `p.chromium.launch_persistent_context(...)`
    UNCONDITIONALLY, even in Firefox mode. The `if BROWSER_MODE == "stealth"`
    above line 24839 guards only the user_data_dir assignment, not the launch,
    and several other chromium sites (22883, 23012, 24153, 25229) have no mode
    guard at all.

    Measured 2026-08-07: the worker hit a golden restore mid-job, took that
    path, and died with "Executable doesn't exist" — Chromium is not installed
    in this Firefox-only environment. The browser was lost and the worker went
    zombie ("Target page, context or browser has been closed").

    Safe here because this harness is Firefox-only by construction: main()
    refuses to run unless BROWSER_MODE is a firefox mode, so nothing that
    legitimately wants Chrome ever reaches this proxy.
    """

    def __init__(self, pw):
        self._pw = pw
        shim = _FirefoxShim(pw)
        self.firefox = shim
        self.chromium = shim

    def __getattr__(self, name):
        return getattr(self._pw, name)


def install_camoufox_driver_shim():
    """Point `patchright.sync_api` at a module whose sync_playwright() yields a
    Camoufox-backed Playwright.

    Must run BEFORE flow_worker is imported, and must register the parent
    package too — otherwise `from patchright.sync_api import X` re-imports the
    real package and clobbers the alias.
    """
    import importlib
    import types

    try:
        real_pw = importlib.import_module("playwright.sync_api")
    except ImportError:
        print("[harness] FATAL: playwright is not installed.", flush=True)
        print("[harness]   pip install playwright && python -m playwright install firefox", flush=True)
        raise SystemExit(1)

    try:
        importlib.import_module("camoufox.sync_api")
    except ImportError:
        print("[harness] FATAL: camoufox is not installed.", flush=True)
        print("[harness]   pip install camoufox[geoip] && python -m camoufox fetch", flush=True)
        raise SystemExit(1)

    real_sync_playwright = real_pw.sync_playwright

    class _CM:
        def __enter__(self):
            self._inner = real_sync_playwright()
            return _PlaywrightProxy(self._inner.__enter__())

        def __exit__(self, *exc):
            return self._inner.__exit__(*exc)

        def start(self):
            self._inner = real_sync_playwright()
            return _PlaywrightProxy(self._inner.start())

    shim = types.ModuleType("patchright.sync_api")
    for attr in dir(real_pw):
        if not attr.startswith("_"):
            setattr(shim, attr, getattr(real_pw, attr))
    shim.sync_playwright = lambda: _CM()

    try:
        sys.modules["patchright"] = importlib.import_module("patchright")
    except ImportError:
        pass
    sys.modules["patchright.sync_api"] = shim
    print(f"[harness] driver shim active: firefox -> camoufox (os={CAMOUFOX_OS})", flush=True)


def main():
    run_dir = os.path.dirname(os.path.abspath(__file__))

    mode = os.environ.get("BROWSER_MODE", "")
    if mode == "stealth" or not mode:
        # Refuse to run against Chrome. On Chrome, Patchright IS the stealth and
        # swapping it out would silently degrade a working path.
        print(f"[harness] REFUSING: BROWSER_MODE={mode!r}.", flush=True)
        print("[harness] This harness is Firefox-only — it removes Patchright,", flush=True)
        print("[harness] which is exactly what makes the Chrome path work.", flush=True)
        return 2

    install_camoufox_driver_shim()

    # Import from the run dir so the worker's companion modules resolve.
    if run_dir not in sys.path:
        sys.path.insert(0, run_dir)

    print("[harness] importing flow_worker (auto-update skipped: not __main__)", flush=True)
    import flow_worker

    print(f"[harness] starting main() on BROWSER_MODE={mode}", flush=True)
    flow_worker.main(account_label="FIREFOX")
    return 0


if __name__ == "__main__":
    sys.exit(main())
