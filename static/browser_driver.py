"""Browser driver selection for flow_worker.

Chrome runs on Patchright (its chromium patches ARE the reCAPTCHA stealth).
Firefox must NOT: Patchright's patches are chromium-only and break Firefox's
page.evaluate outright ("Cannot read properties of undefined (reading
'_client')"), which strands the worker on the Flow landing page.

Firefox runs on Camoufox (>=0.5.4 / FF152) driven by plain Playwright.
Measured 2026-08-07: 10/10 real 0cA reCAPTCHA tokens and completed clips,
while Chrome minted ~0% real tokens the same day.
"""
import os

# Firefox is strictly opt-in. Any other value — including unset — stays on
# Chrome, so a missing or typo'd env var can never migrate a working Chrome
# worker. Default-by-config, never default-by-code.
_FIREFOX_MODES = ("firefox", "camoufox")

CHROME_PROCESS_NAMES = ("chrome.exe",)
FIREFOX_PROCESS_NAMES = ("camoufox.exe", "firefox.exe")

# Chrome-only launch kwargs. Camoufox owns the fingerprint and rejects these.
_CHROME_ONLY_KWARGS = ("channel", "ignore_default_args", "args")


def resolve_browser_mode(env=None):
    env = os.environ if env is None else env
    return (env.get("BROWSER_MODE") or "stealth").strip().lower()


def is_firefox_mode(mode):
    return (mode or "").strip().lower() in _FIREFOX_MODES


def browser_process_names(mode):
    """Process names holding a lock on this mode's profile directory.

    Used by the profile-kill path. Matching only chrome.exe on a Firefox run
    means the golden restore's rmtree silently fails against a live Firefox
    (it runs with ignore_errors=True), leaving a half-deleted profile.
    """
    return FIREFOX_PROCESS_NAMES if is_firefox_mode(mode) else CHROME_PROCESS_NAMES


def camoufox_launch_kwargs(kwargs, window=None):
    """Translate flow_worker's launch kwargs into Camoufox's dialect."""
    out = dict(kwargs)

    for k in _CHROME_ONLY_KWARGS:
        out.pop(k, None)

    # Pin the OS or Camoufox randomises it — it served a macOS user-agent on a
    # Windows host, a mismatch reCAPTCHA can score against.
    out.setdefault("os", "windows")

    # Camoufox >=0.5 hard-fails the launch when its bundled uBlock is missing
    # (addons.confirm_paths -> InvalidAddonPath). That download comes from
    # GitHub, which the known Surfshark MTU issue breaks. An ad blocker is not
    # wanted on Flow anyway.
    try:
        from camoufox.addons import DefaultAddons
        out.setdefault("exclude_addons", [DefaultAddons.UBO])
    except ImportError:
        pass

    # flow_worker pins the Firefox branch to viewport 1280x500, shorter than any
    # real screen, which clips the Flow UI. A fixed viewport also pins page size,
    # so content letterboxes even if the OS window is resized.
    if window:
        try:
            w, h = (int(v) for v in str(window).lower().split("x", 1))
        except ValueError:
            return out
        out.pop("viewport", None)
        out["window"] = (w, h)
        out["no_viewport"] = True

    return out


def launch_context(playwright, mode, **kwargs):
    """Launch a persistent context for the given BROWSER_MODE.

    Call this for EVERY launch site, including ones written as
    `p.chromium.launch_persistent_context(...)`. flow_worker's recovery paths
    call chromium unconditionally even in Firefox mode; on 2026-08-07 that
    killed a live Firefox worker after a golden restore with "Executable
    doesn't exist", because Chromium is not installed in a Firefox-only
    environment.
    """
    if not is_firefox_mode(mode):
        return playwright.chromium.launch_persistent_context(**kwargs)

    from camoufox.sync_api import NewBrowser
    cf = camoufox_launch_kwargs(kwargs, window=os.environ.get("FIREFOX_WINDOW"))
    return NewBrowser(playwright, persistent_context=True, **cf)
