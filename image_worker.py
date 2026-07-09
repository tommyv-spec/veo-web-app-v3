#!/usr/bin/env python3
"""
Image Worker — Local Flow UI Automation for Nano Banana Image Generation

Automates Google Flow UI (labs.google/fx/tools/flow) to generate/edit images
via Nano Banana. Same browser automation approach as flow_worker.py but:
- Input:  local image path(s) + prompt
- Output: saved image to local path
- No server, no R2, no API polling — runs locally as a CLI tool

Usage:
    # Text-to-image
    python image_worker.py --prompt "A modern office scene" --output ./output.png

    # Edit/enhance existing image
    python image_worker.py --input ./frame.png --prompt "Enhance: sharpen details" --output ./enhanced.png

    # With settings
    python image_worker.py --input ./frame.png --prompt "Remove watermark" \\
        --aspect-ratio 16:9 --resolution 2K --output ./result.png

    # Batch mode
    python image_worker.py --input-dir ./frames/ --prompt "Enhance all frames" --output-dir ./enhanced/

    # Interactive mode (keep browser open, process multiple jobs)
    python image_worker.py --interactive
"""

import os
import sys
import re
import subprocess
import shutil
import time
import threading
import random
import json
import argparse
import tempfile
import hashlib
from pathlib import Path
from datetime import datetime

# ============================================================
# PATCHRIGHT SETUP (from flow_worker.py)
# ============================================================

def _ensure_patchright():
    """Auto-install patchright if not already installed."""
    try:
        import patchright
        return True
    except ImportError:
        pass
    
    print("[Init] Patchright not found — installing...", flush=True)
    methods = [
        ([sys.executable, "-m", "pip", "install", "patchright"], "pip install"),
        ([sys.executable, "-m", "pip", "install", "--user", "patchright"], "pip install --user"),
    ]
    for pip_cmd in ["pip", "pip3"]:
        p = shutil.which(pip_cmd)
        if p:
            methods.append(([p, "install", "patchright"], f"{pip_cmd} install"))
    
    pip_ok = False
    for cmd, label in methods:
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            if result.returncode == 0:
                print(f"[Init] ✓ {label} succeeded", flush=True)
                pip_ok = True
                break
        except Exception as e:
            pass
    
    if not pip_ok:
        print("[Init] ❌ pip install patchright failed! Install manually.", flush=True)
        return False
    
    # Install browser
    for cmd, label in [
        ([sys.executable, "-m", "patchright", "install", "chromium"], "patchright install chromium"),
    ]:
        try:
            subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        except Exception:
            pass
    
    try:
        import patchright
        return True
    except ImportError:
        return False

_patchright_ok = _ensure_patchright()
if _patchright_ok:
    from patchright.sync_api import sync_playwright
    print("[Init] ✓ Using Patchright (undetected Playwright fork)")
else:
    print("[Init] ⚠ Patchright not available, falling back to Playwright")
    from playwright.sync_api import sync_playwright


# ============================================================
# CONSTANTS
# ============================================================

WORKER_VERSION = "img-v577"  # v791b interleaved download scan + v791c 403/stale-URL re-resolve retry
FLOW_HOME_URL = "https://labs.google/fx/tools/flow"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SESSION_FOLDER = os.environ.get("IMAGE_SESSION_FOLDER",
    os.path.join(BASE_DIR, "image-chrome-session"))
BROWSER_MODE = os.environ.get("BROWSER_MODE", "stealth")


# ============================================================
# LAPTOP-LOGIN PULL (parity with flow_worker.py startup)
# ============================================================
# Same mechanism the video worker uses: if ACCOUNT1_LAPTOP_EMAIL (or
# worker_settings.json {"laptop_email": ...}) names a Google account the
# operator's real Chrome is already logged into, capture that login's LIVE
# cookies and inject them into this worker's fresh (automatable) session — so
# the worker starts already logged in, no manual verification code. Empty email
# => no-op (prior behavior). LAPTOP_PULL_DISABLED=1 turns the whole thing off.
_COOKIE_MARKER = os.path.join(BASE_DIR, ".worker_injected_cookies.json")


def _sync_companion_modules(base_url):
    """Download worker_profile_pull.py + worker_cookie_extract.py next to this
    worker (same as flow_worker's auto-update companion sync). The image
    installer only fetches image_worker.py, so without this the laptop-login
    helpers are absent in production and the pull silently no-ops. Best-effort:
    any failure is logged, never raises. Skipped when no base_url is known."""
    if not base_url:
        return
    import urllib.request as _urllib
    base = base_url.rstrip("/")
    for _comp in ("worker_profile_pull.py", "worker_cookie_extract.py"):
        try:
            _url = f"{base}/api/user-worker/download/{_comp}"
            _req = _urllib.Request(_url, headers={"User-Agent": f"image-worker/{WORKER_VERSION}"})
            with _urllib.urlopen(_req, timeout=15) as _resp:
                _bytes = _resp.read()
            compile(_bytes.decode("utf-8"), f"<{_comp}>", "exec")  # reject corrupt downloads
            _path = os.path.join(BASE_DIR, _comp)
            _tmp = _path + ".tmp"
            with open(_tmp, "wb") as _f:
                _f.write(_bytes)
            os.replace(_tmp, _path)
            print(f"[IMAGE] ✓ Synced {_comp} companion module", flush=True)
        except Exception as _ce:
            print(f"[IMAGE] ⚠ Could not sync {_comp} ({_ce})", flush=True)


# v814 — the retired net-log cookie path's import helper is gone; copy-mode
# imports worker_profile_pull directly inside _maybe_pull_laptop_profile.

# v814 — copy-once guard (keyed per golden folder, one copy per process),
# same as flow_worker._LAPTOP_COPIED_GOLDENS.
_LAPTOP_COPIED_GOLDENS = set()


def _maybe_pull_laptop_profile(session_folder, golden_folder, label="IMAGE"):
    """v814 — COPY-MODE laptop login, EXACTLY like flow_worker's startup: build
    the golden DIRECTLY from the operator's real Chrome profile logged into
    laptop_email (ACCOUNT1_LAPTOP_EMAIL env / worker_settings.json), so the
    worker launches an already-logged-in session with no verification code.
    Copies only the durable file set (build_lean_golden_from_profile), rewrites
    Local State to a single `Default` profile, and reads the profile ONLY.

    Replaces the retired net-log capture+inject (Flow rejected the
    reconstituted session for some accounts AND repeatedly driving the real
    profile signed it out — same reason flow_worker retired it). Requires
    App-Bound Encryption disabled (HKCU policy). Copy ONCE per process.
    Fail-safe: any error logged, never raises; on failure the worker falls
    back to a manual login that run. LAPTOP_PULL_DISABLED=1 turns it off."""
    try:
        # Drop any stale net-log cookie marker from the retired path so the old
        # injection block (kept as a no-op) never fires with dead cookies.
        try:
            if os.path.isfile(_COOKIE_MARKER):
                os.remove(_COOKIE_MARKER)
        except Exception:
            pass
        if os.environ.get("LAPTOP_PULL_DISABLED", "").strip().lower() in ("1", "true", "yes"):
            return
        for _p in (BASE_DIR, os.path.join(BASE_DIR, "static")):
            if os.path.isdir(_p) and _p not in sys.path:
                sys.path.insert(0, _p)
        sys.modules.pop("worker_profile_pull", None)  # fresh-load after companion sync
        from worker_profile_pull import (build_lean_golden_from_profile, locate_profile,
                                         close_laptop_chrome, load_laptop_email as _lle)
        # load_laptop_email reads ACCOUNT1_LAPTOP_EMAIL env first, then the file.
        email = _lle(os.path.join(BASE_DIR, "worker_settings.json"))
        if not email:
            return
        if golden_folder in _LAPTOP_COPIED_GOLDENS:
            print(f"[{label}] laptop copy: golden already built this session — reusing", flush=True)
            return
        loc = locate_profile(email)
        if not loc:
            print(f"[{label}] laptop copy: {email!r} not logged into any Chrome channel", flush=True)
            return
        _ud, _pf, _ch = loc
        # Launch the worker on a SEPARATE Chrome channel from the one the
        # operator's account lives in (copied cookies are user-DPAPI with ABE
        # off, so they decrypt on ANY channel). Record it in the sidecar so
        # every launch site uses the same channel. WORKER_CHROME_CHANNEL
        # overrides. Same discipline as flow_worker (prod 2026-06-27: same-
        # channel runs killed the worker's own browsers).
        _worker_ch = os.environ.get("WORKER_CHROME_CHANNEL", "").strip() or "chrome"
        try:
            with open(os.path.join(BASE_DIR, ".worker_chrome_channel"), "w", encoding="utf-8") as _cf:
                _cf.write(_worker_ch)
        except Exception:
            pass
        print(f"[{label}] laptop copy (copy-mode v814): {email} in {_pf} ({_ch}) — building lean golden", flush=True)
        ch = build_lean_golden_from_profile(
            email, golden_folder, label=label, user_data_dir=_ud,
            close_chrome=lambda _u: close_laptop_chrome(_u, log=lambda m: print(m, flush=True)),
            log=lambda m: print(m, flush=True))
        if ch:
            _LAPTOP_COPIED_GOLDENS.add(golden_folder)
            print(f"[{label}] laptop copy: ✓ lean golden ready (channel={ch})", flush=True)
        else:
            print(f"[{label}] laptop copy: build skipped/failed — manual login needed this run", flush=True)
    except Exception as _pe:
        print(f"[{label}] laptop copy error (continuing): {_pe}", flush=True)


def _worker_chrome_channel():
    """v814 — Chrome channel for the worker browser (parity with flow_worker):
    WORKER_CHROME_CHANNEL env override, else the sidecar written by the
    laptop-profile pull, else stable 'chrome'."""
    ch = os.environ.get("WORKER_CHROME_CHANNEL", "").strip()
    if ch:
        return ch
    try:
        sidecar = os.path.join(BASE_DIR, ".worker_chrome_channel")
        if os.path.isfile(sidecar):
            with open(sidecar, "r", encoding="utf-8") as _f:
                v = _f.read().strip()
            if v:
                return v
    except Exception:
        pass
    return "chrome"


def _inject_laptop_cookies(browser, label="IMAGE"):
    """Inject staged laptop-login cookies into the fresh session so it's already
    logged into Google. Mirrors flow_worker's post-launch injection (host-only
    url retry on the first add_cookies failure). Fail-safe + idempotent."""
    try:
        if not os.path.isfile(_COOKIE_MARKER):
            return
        with open(_COOKIE_MARKER, "r", encoding="utf-8") as _f:
            _cks = json.load(_f)
        if not _cks:
            return
        _ok, _failed = 0, []
        for _ck in _cks:
            try:
                browser.add_cookies([_ck])
                _ok += 1
                continue
            except Exception:
                pass
            # Retry host-only via url ONLY (Playwright rejects url+path together).
            try:
                _h = (_ck.get("domain") or "").lstrip(".") or "accounts.google.com"
                _alt = {"name": _ck["name"], "value": _ck["value"],
                        "url": "https://" + _h, "secure": True, "sameSite": "Lax"}
                browser.add_cookies([_alt])
                _ok += 1
                continue
            except Exception:
                pass
            _failed.append(_ck.get("name", "?"))
        print(f"[{label}] injected {_ok}/{len(_cks)} laptop-login cookies"
              + (f" (failed: {_failed})" if _failed else ""), flush=True)
    except Exception as _ie:
        print(f"[{label}] cookie injection failed (continuing): {_ie}", flush=True)


# Minimal stealth — real Chrome has native plugins/runtime, don't fake them
STEALTH_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', {
    get: () => undefined
});
"""


# ============================================================
# HUMAN-LIKE BEHAVIOR HELPERS (from flow_worker.py)
# ============================================================

_last_mouse_pos = {'x': 640, 'y': 360}

def human_delay(min_sec=0.5, max_sec=1.5):
    """Random delay to simulate human behavior"""
    if random.random() < 0.08:
        delay = random.uniform(max_sec * 1.5, max_sec * 3)
    elif random.random() < 0.15:
        delay = random.uniform(max_sec, max_sec * 1.5)
    else:
        delay = random.uniform(min_sec, max_sec)
    time.sleep(delay)
    return delay


def human_mouse_move_to(page, target_x, target_y, steps=None):
    """Move mouse to target with Bezier curve from current position."""
    global _last_mouse_pos
    try:
        start_x = _last_mouse_pos['x']
        start_y = _last_mouse_pos['y']
        if steps is None:
            dist = ((target_x - start_x)**2 + (target_y - start_y)**2)**0.5
            steps = max(8, min(25, int(dist / 40)))
        ctrl_x = (start_x + target_x) / 2 + random.randint(-80, 80)
        ctrl_y = (start_y + target_y) / 2 + random.randint(-60, 60)
        for i in range(steps + 1):
            t = i / steps
            x = (1-t)**2 * start_x + 2*(1-t)*t * ctrl_x + t**2 * target_x
            y = (1-t)**2 * start_y + 2*(1-t)*t * ctrl_y + t**2 * target_y
            x += random.uniform(-1.2, 1.2)
            y += random.uniform(-1.2, 1.2)
            page.mouse.move(x, y)
            speed = 0.008 + 0.025 * (1 - abs(2*t - 1))
            time.sleep(speed + random.uniform(0, 0.008))
        _last_mouse_pos['x'] = target_x
        _last_mouse_pos['y'] = target_y
    except:
        pass


def human_mouse_move(page):
    """Random mouse movement — like a human idly moving the cursor"""
    try:
        viewport = page.viewport_size
        if not viewport:
            return
        for _ in range(random.randint(2, 3)):
            target_x = random.randint(80, viewport['width'] - 80)
            target_y = random.randint(80, viewport['height'] - 80)
            human_mouse_move_to(page, target_x, target_y, steps=random.randint(5, 12))
            time.sleep(random.uniform(0.05, 0.2))
    except:
        pass


def scroll_randomly(page):
    """Scroll the page a bit like a human would"""
    try:
        for _ in range(random.randint(1, 2)):
            direction = random.choice(['up', 'down'])
            amount = random.randint(30, 100)
            page.mouse.wheel(0, -amount if direction == 'up' else amount)
            time.sleep(random.uniform(0.1, 0.3))
    except:
        pass


def human_click_at(page):
    """Realistic mousedown → hold → mouseup click at current position."""
    page.mouse.down()
    time.sleep(random.uniform(0.05, 0.15))
    page.mouse.up()


def human_click_element(page, selector_or_locator, label="", timeout=10000):
    """Click an element with natural human-like mouse movement."""
    global _last_mouse_pos
    try:
        if isinstance(selector_or_locator, str):
            element = page.locator(selector_or_locator).first
        else:
            element = selector_or_locator
        element.wait_for(state="visible", timeout=timeout)
        # Scroll into view BEFORE reading box — raw page.mouse.down()/up() clicks
        # fixed viewport coords and skips actionability; an off-screen element
        # yields off-screen box coords and the click lands on empty space.
        try:
            element.scroll_into_view_if_needed(timeout=timeout)
        except Exception:
            pass
        box = element.bounding_box()
        if box:
            target_x = box['x'] + box['width'] * random.uniform(0.3, 0.7)
            target_y = box['y'] + box['height'] * random.uniform(0.3, 0.7)
            human_mouse_move_to(page, target_x, target_y)
            time.sleep(random.uniform(0.15, 0.40))
            page.mouse.down()
            time.sleep(random.uniform(0.05, 0.15))
            page.mouse.up()
            _last_mouse_pos['x'] = target_x
            _last_mouse_pos['y'] = target_y
        else:
            element.click(timeout=timeout)
        if label:
            print(f"✓ Clicked: {label}", flush=True)
        time.sleep(random.uniform(0.3, 0.7))
        return True
    except Exception as e:
        if label:
            print(f"❌ Click failed for {label}: {e}", flush=True)
        return False


def human_click_locator(page, locator, label="", timeout=5000):
    """Humanized click on a Playwright locator."""
    try:
        locator.wait_for(state="visible", timeout=timeout)
        box = locator.bounding_box()
        if box:
            target_x = box['x'] + box['width'] * random.uniform(0.3, 0.7)
            target_y = box['y'] + box['height'] * random.uniform(0.3, 0.7)
            human_mouse_move_to(page, target_x, target_y)
            time.sleep(random.uniform(0.12, 0.35))
            human_click_at(page)
        else:
            locator.click(timeout=timeout)
        if label:
            print(f"✓ Clicked: {label}", flush=True)
        time.sleep(random.uniform(0.2, 0.5))
        return True
    except Exception as e:
        if label:
            print(f"⚠️ Click failed for {label}: {e}", flush=True)
        return False


def human_click_for_file_chooser(page, btn_locator):
    """Move mouse to button and click — used for file upload triggers."""
    box = btn_locator.bounding_box()
    if box:
        target_x = box['x'] + box['width'] * random.uniform(0.3, 0.7)
        target_y = box['y'] + box['height'] * random.uniform(0.3, 0.7)
        human_mouse_move_to(page, target_x, target_y)
        time.sleep(random.uniform(0.1, 0.25))
        human_click_at(page)
    else:
        btn_locator.click(timeout=3000)


def human_pre_action(page, action_name=""):
    """Light human-like behavior before an action"""
    human_mouse_move(page)
    human_delay(0.3, 0.8)


# ============================================================
# CHROME SESSION MANAGEMENT (from flow_worker.py)
# ============================================================

def suppress_chrome_signin_dialog(user_data_dir):
    """Write Chrome preferences to suppress sign-in dialogs and force English."""
    prefs_dir = os.path.join(user_data_dir, "Default")
    os.makedirs(prefs_dir, exist_ok=True)
    prefs_file = os.path.join(prefs_dir, "Preferences")
    
    prefs = {}
    if os.path.exists(prefs_file):
        try:
            with open(prefs_file, 'r', encoding='utf-8') as f:
                prefs = json.load(f)
        except Exception:
            prefs = {}
    
    if "signin" not in prefs:
        prefs["signin"] = {}
    prefs["signin"]["allowed"] = False
    prefs["signin"]["allowed_on_next_startup"] = False
    if "browser" not in prefs:
        prefs["browser"] = {}
    prefs["browser"]["signin_intercept_enabled"] = False
    if "intl" not in prefs:
        prefs["intl"] = {}
    prefs["intl"]["accept_languages"] = "en-US,en"
    prefs["intl"]["selected_languages"] = "en-US,en"
    if "translate" not in prefs:
        prefs["translate"] = {}
    prefs["translate"]["enabled"] = False
    
    try:
        with open(prefs_file, 'w', encoding='utf-8') as f:
            json.dump(prefs, f)
    except Exception:
        pass


# ============================================================
# CHROME WINDOW MANAGEMENT
# ============================================================
# On Windows, Chrome pops to the foreground every time we launch or
# navigate. This is disruptive — the user is trying to work in other
# apps and the worker keeps stealing focus. We combat this at three
# levels:
#
#   1. OS-level: use user32.ShowWindow with SW_SHOWMINNOACTIVE to send
#      Chrome to the taskbar immediately after launch. The window is
#      still running (all Playwright calls work), just not visible
#      unless the user clicks the taskbar icon.
#
#   2. Page-level: window.blur() after navigations tells Chrome to
#      release keyboard focus. Doesn't hide the window but prevents
#      focus-stealing on subsequent interactions.
#
#   3. Login exception: when login is needed, we *do* want Chrome
#      visible so the user can complete the Google auth flow. Call
#      restore_chrome_window() before starting the login wait.

def _stash_profile_on_page(page, profile_dir):
    """Attach profile_dir to a Playwright page as _user_data_dir so
    _find_chrome_hwnd can identify the worker's Chrome by owning PID.
    v486: called at every page creation site.
    """
    if page is None:
        return
    try:
        page._user_data_dir = profile_dir
    except Exception:
        pass


def _get_worker_chrome_pids(profile_dir):
    """Return the set of Chrome process PIDs whose commandline contains
    the given profile directory. v486: used by _find_chrome_hwnd for
    unambiguous worker-window identification."""
    import platform as _platform
    if _platform.system() != "Windows" or not profile_dir:
        return set()
    try:
        abs_profile = os.path.abspath(profile_dir)
    except Exception:
        return set()
    pids = set()
    try:
        r = subprocess.run(
            ['wmic', 'process', 'where',
             f'name="chrome.exe" and commandline like "%{abs_profile}%"',
             'get', 'ProcessId', '/format:value'],
            capture_output=True, text=True, timeout=3
        )
        for line in r.stdout.splitlines():
            line = line.strip()
            if line.startswith('ProcessId=') and line[10:].strip().isdigit():
                pids.add(int(line[10:].strip()))
        if pids:
            return pids
    except Exception:
        pass
    try:
        escaped = abs_profile.replace("'", "''")
        ps_cmd = (
            f"Get-CimInstance Win32_Process -Filter \"name='chrome.exe'\""
            f" | Where-Object {{ $_.CommandLine -like '*{escaped}*' }}"
            f" | Select-Object -ExpandProperty ProcessId"
        )
        r2 = subprocess.run(
            ['powershell', '-NoProfile', '-Command', ps_cmd],
            capture_output=True, text=True, timeout=3
        )
        for line in r2.stdout.splitlines():
            line = line.strip()
            if line.isdigit():
                pids.add(int(line))
    except Exception:
        pass
    return pids


def _find_chrome_hwnd(page):
    """Return the HWND of the Chrome window associated with this
    Playwright page, or 0 if not on Windows.

    v486: match by OS process ID, not window title. The previous
    implementation matched any visible Chrome whose title contained
    "Google Flow" / "labs.google" / "Flow". That hit the USER'S
    personal Chrome if they had any tab with "Flow" in the title —
    for example, browsing Kaveno itself, or the Flow studio. The
    worker would cache the user's HWND and minimize their window.
    Identifying the worker's Chrome by the owning process ID is
    unambiguous: the worker's Chrome was launched with
    --user-data-dir=<profile>, so its process commandline contains
    the profile path. The user's personal Chrome doesn't.

    v467: no Playwright API calls in this function — pure ctypes.
    HWND cached on page for repeat calls.
    """
    import platform as _platform
    if _platform.system() != "Windows":
        return 0
    try:
        import ctypes
        from ctypes import wintypes
    except Exception:
        return 0

    # Return cached HWND if still valid.
    cached = getattr(page, "_chrome_hwnd", 0)
    if cached:
        try:
            user32 = ctypes.windll.user32
            user32.IsWindow.argtypes = [wintypes.HWND]
            user32.IsWindow.restype = ctypes.c_bool
            if user32.IsWindow(cached):
                return cached
        except Exception:
            pass
        try:
            page._chrome_hwnd = 0
        except Exception:
            pass

    # Worker-Chrome PIDs for this page's profile.
    profile_dir = getattr(page, "_user_data_dir", None)
    worker_pids = _get_worker_chrome_pids(profile_dir) if profile_dir else set()

    try:
        user32 = ctypes.windll.user32
        user32.EnumWindows.argtypes = [ctypes.WINFUNCTYPE(
            ctypes.c_bool, wintypes.HWND, wintypes.LPARAM), wintypes.LPARAM]
        user32.EnumWindows.restype = ctypes.c_bool
        user32.GetWindowTextLengthW.argtypes = [wintypes.HWND]
        user32.GetWindowTextW.argtypes = [wintypes.HWND, wintypes.LPWSTR, ctypes.c_int]
        user32.IsWindowVisible.argtypes = [wintypes.HWND]
        user32.GetWindowThreadProcessId.argtypes = [
            wintypes.HWND, ctypes.POINTER(wintypes.DWORD)]
        user32.GetWindowThreadProcessId.restype = wintypes.DWORD

        found_hwnd = [0]

        def callback(hwnd, _lparam):
            if not user32.IsWindowVisible(hwnd):
                return True
            length = user32.GetWindowTextLengthW(hwnd)
            if length == 0:
                return True
            # Primary strategy (v486): PID match.
            if worker_pids:
                pid = wintypes.DWORD(0)
                user32.GetWindowThreadProcessId(hwnd, ctypes.byref(pid))
                if pid.value in worker_pids:
                    buff = ctypes.create_unicode_buffer(length + 1)
                    user32.GetWindowTextW(hwnd, buff, length + 1)
                    title = buff.value or ""
                    if "Chrome" in title:
                        found_hwnd[0] = hwnd
                        return False
                return True
            # Fallback: title match (only when profile unknown).
            buff = ctypes.create_unicode_buffer(length + 1)
            user32.GetWindowTextW(hwnd, buff, length + 1)
            title = buff.value or ""
            if "Chrome" in title:
                for marker in ["Google Flow", "labs.google", "Flow"]:
                    if marker in title:
                        found_hwnd[0] = hwnd
                        return False
            return True

        WNDENUMPROC = ctypes.WINFUNCTYPE(ctypes.c_bool, wintypes.HWND, wintypes.LPARAM)
        user32.EnumWindows(WNDENUMPROC(callback), 0)

        if found_hwnd[0]:
            try:
                page._chrome_hwnd = found_hwnd[0]
            except Exception:
                pass
        return found_hwnd[0]
    except Exception:
        return 0


def minimize_chrome_window(page, label=""):
    """Send the Chrome window to the taskbar without activating another
    window. Keeps Chrome running and responsive to automation, just out
    of the user's way.

    Windows-specific; no-op on macOS/Linux (there we rely only on the
    page-level window.blur() which is already portable).
    """
    import platform as _platform
    if _platform.system() != "Windows":
        return
    try:
        import ctypes
        from ctypes import wintypes
        hwnd = _find_chrome_hwnd(page)
        if not hwnd:
            return
        user32 = ctypes.windll.user32
        # SW_SHOWMINNOACTIVE = 7 — minimize but don't activate next window
        SW_SHOWMINNOACTIVE = 7
        user32.ShowWindow.argtypes = [wintypes.HWND, ctypes.c_int]
        user32.ShowWindow.restype = ctypes.c_bool
        user32.ShowWindow(hwnd, SW_SHOWMINNOACTIVE)
    except Exception:
        pass


def restore_chrome_window(page, label=""):
    """Bring Chrome back to the foreground. Called when login is needed
    so the user can complete the Google auth flow without having to hunt
    through the taskbar.
    """
    import platform as _platform
    if _platform.system() != "Windows":
        return
    try:
        import ctypes
        from ctypes import wintypes
        hwnd = _find_chrome_hwnd(page)
        if not hwnd:
            return
        user32 = ctypes.windll.user32
        # SW_RESTORE = 9 — restore window (un-minimize if minimized)
        SW_RESTORE = 9
        user32.ShowWindow.argtypes = [wintypes.HWND, ctypes.c_int]
        user32.ShowWindow.restype = ctypes.c_bool
        user32.ShowWindow(hwnd, SW_RESTORE)
        # Briefly bring to front so user sees it. SetForegroundWindow
        # respects Windows' anti-focus-stealing when called from a
        # process that isn't the foreground one, so this is soft-focus
        # in practice (taskbar button flashes).
        user32.SetForegroundWindow.argtypes = [wintypes.HWND]
        user32.SetForegroundWindow(hwnd)
    except Exception:
        pass


def defocus_chrome(page, label=""):
    """Tell Chrome's DOM to release keyboard focus. Complementary to
    minimize_chrome_window — this works portably across OSes but only
    handles the in-page focus, not the OS window state."""
    try:
        page.evaluate("window.blur()")
    except Exception:
        pass


def kill_chrome_using_profile(profile_dir, label=""):
    """Force-kill any Chrome process holding a lock on the given profile."""
    import platform as _platform
    prefix = f"[{label}] " if label else ""
    abs_profile = os.path.abspath(profile_dir)
    killed = []
    try:
        if _platform.system() == "Windows":
            # v486: full profile path only. The old basename fallback
            # (e.g. "chrome-session") would match ANY Chrome process
            # with that substring in its commandline — including the
            # OTHER worker (video worker also uses chrome-session as
            # a folder name). Cross-worker kills confirmed in the log.
            pids_found = []
            try:
                r = subprocess.run(
                    ['wmic', 'process', 'where',
                     f'name="chrome.exe" and commandline like "%{abs_profile}%"',
                     'get', 'ProcessId', '/format:value'],
                    capture_output=True, text=True, timeout=10)
                for line in r.stdout.splitlines():
                    line = line.strip()
                    if line.startswith('ProcessId=') and line[10:].strip().isdigit():
                        pids_found.append(line[10:].strip())
            except Exception:
                try:
                    escaped = abs_profile.replace("'", "''")
                    ps_cmd = (
                        f"Get-CimInstance Win32_Process -Filter \"name='chrome.exe'\""
                        f" | Where-Object {{ $_.CommandLine -like '*{escaped}*' }}"
                        f" | Select-Object -ExpandProperty ProcessId")
                    r2 = subprocess.run(
                        ['powershell', '-NoProfile', '-Command', ps_cmd],
                        capture_output=True, text=True, timeout=10)
                    for line in r2.stdout.splitlines():
                        if line.strip().isdigit():
                            pids_found.append(line.strip())
                except Exception:
                    pass
            for pid in pids_found:
                subprocess.run(['taskkill', '/F', '/PID', pid], capture_output=True, timeout=5)
                killed.append(pid)
        else:
            try:
                result = subprocess.run(['pgrep', '-f', abs_profile], capture_output=True, text=True, timeout=5)
                for pid in result.stdout.strip().split():
                    if pid:
                        subprocess.run(['kill', '-9', pid], capture_output=True, timeout=5)
                        killed.append(pid)
            except Exception:
                pass
        if killed:
            print(f"{prefix}Killed Chrome pids: {killed}", flush=True)
    except Exception as e:
        print(f"{prefix}Could not kill Chrome: {e}", flush=True)
    
    for lock_file in ['SingletonLock', 'SingletonSocket', 'SingletonCookie']:
        lock_path = os.path.join(profile_dir, lock_file)
        if os.path.exists(lock_path):
            try:
                os.remove(lock_path)
            except Exception:
                pass


def get_golden_folder(session_folder):
    """Derive the golden (baseline) folder path from a session folder path."""
    p = os.path.abspath(session_folder)
    base = os.path.dirname(p)
    name = os.path.basename(p)
    if name == "image-chrome-session":
        return os.path.join(base, "image-chrome-golden")
    m = re.match(r"^image-chrome-session(-\d+)$", name)
    if m:
        return os.path.join(base, f"image-chrome-golden{m.group(1)}")
    new_name = name.replace("session", "golden")
    if new_name != name:
        return os.path.join(base, new_name)
    return os.path.join(base, name + "-golden")


def purge_gpu_caches(session_folder, label=""):
    """Delete GPU/shader caches from a Chrome profile after golden restore."""
    prefix = f"[{label}] " if label else ""
    for d_name in ["Default/GPUCache", "GrShaderCache", "ShaderCache"]:
        d = os.path.join(session_folder, d_name)
        if os.path.exists(d):
            try:
                shutil.rmtree(d)
                print(f"{prefix}✓ Purged: {d_name}", flush=True)
            except Exception:
                pass


def restore_from_golden(session_folder, label="IMAGE"):
    """Restore the image-worker Chrome session profile from the golden snapshot.

    v828 — ported from flow_worker.restore_from_golden (v701g). The old inline
    restore in launch_browser was a single rmtree+copytree: on a Windows file
    lock it printed a warning and shipped a stale/partial profile, so the account
    'unusual activity' block never actually cleared after a golden-restore
    relaunch. Chrome's GPU/singleton subprocess can hold memory-mapped handles
    for several seconds past taskkill, so copytree hits a cookie/cache DB
    mid-copy (WinError 1224 = file with a user-mapped section open, WinError 32 =
    file in use).

    Retry with backoff (0.5s, 2s, 5s); between attempts, force one more pass at
    SingletonLock cleanup + a small sleep so any lingering chrome subprocess
    releases handles. Returns True on success, False if the golden is missing or
    all retries fail. Unit-tested in tests/test_image_worker_golden_restore.py.
    """
    golden_folder = get_golden_folder(session_folder)
    prefix = f"[{label}] " if label else ""

    if not os.path.exists(golden_folder):
        print(f"{prefix}⚠ Golden profile not found at {golden_folder} — cannot restore.", flush=True)
        print(f"{prefix}  Re-run setup_worker.py to create a fresh golden profile.", flush=True)
        return False

    print(f"{prefix}🔄 GOLDEN RESTORE: Restoring session profile from {golden_folder}", flush=True)

    last_err = None
    for _attempt in range(3):
        try:
            if os.path.exists(session_folder):
                shutil.rmtree(session_folder, ignore_errors=True)
            # dirs_exist_ok=True so a locked file left behind by rmtree still gets
            # overwritten from golden rather than failing with FileExistsError.
            shutil.copytree(
                golden_folder, session_folder,
                dirs_exist_ok=True,
                ignore_dangling_symlinks=True,
                ignore=shutil.ignore_patterns(
                    'SingletonLock', 'SingletonSocket', 'SingletonCookie',
                ),
            )
            print(f"{prefix}  ✓ Session profile restored → {session_folder}", flush=True)
            last_err = None
            break
        except Exception as e:
            last_err = e
            err_str = str(e)
            # WinError 1224 = file mapped by another process. WinError 32 = in use.
            if '1224' in err_str or 'WinError 32' in err_str or 'in use' in err_str.lower():
                _wait = (0.5, 2.0, 5.0)[_attempt] if _attempt < 3 else 5.0
                print(
                    f"{prefix}  ⚠ Restore attempt {_attempt+1}/3 hit Windows file-lock; "
                    f"waiting {_wait:.1f}s for handles to release",
                    flush=True,
                )
                for _lock in ('SingletonLock', 'SingletonSocket', 'SingletonCookie'):
                    _lp = os.path.join(session_folder, _lock)
                    if os.path.exists(_lp):
                        try:
                            os.remove(_lp)
                        except Exception:
                            pass
                time.sleep(_wait)
                continue
            # Non-lock error — don't waste time retrying.
            break

    if last_err is not None:
        print(f"{prefix}  ⚠ Failed to restore session profile after retries: {last_err}", flush=True)
        return False

    # Golden may have been built on a different GPU environment — purge caches.
    purge_gpu_caches(session_folder, label=label or "RESTORE")
    print(f"{prefix}✅ Golden restore complete.", flush=True)
    return True


# ============================================================
# CHROME WARMUP (from flow_worker.py)
# ============================================================

def chrome_warmup(page):
    """Warm up Chrome — sync variations seed for valid x-client-data header."""
    try:
        print("[Warmup] Loading Google pages to sync Chrome variations...", flush=True)
        page.goto("https://www.google.com")
        human_delay(3, 5)
        human_mouse_move(page)
        human_delay(1, 2)
        scroll_randomly(page)
        human_delay(1, 2)
        print("[Warmup] ✓ Complete", flush=True)
    except Exception as e:
        err_str = str(e)
        if any(x in err_str for x in ("browser has been closed", "Target page", "context or browser")):
            raise
        print(f"[Warmup] ⚠ Failed (non-fatal): {e}", flush=True)


# ============================================================
# FLOW URL HELPERS (from flow_worker.py)
# ============================================================

def is_flow_url(url):
    url = url.lower()
    return "labs.google/fx" in url and "/tools/flow" in url

def is_flow_home(url):
    return is_flow_url(url) and "/project/" not in url.lower()

def is_flow_project(url):
    return is_flow_url(url) and "/project/" in url.lower()

def is_google_login(url):
    return "accounts.google" in url.lower()

def is_on_flow_not_login(url):
    return is_flow_url(url) and not is_google_login(url)


# ============================================================
# FLOW LOGIN & NAVIGATION (from flow_worker.py)
# ============================================================

def check_and_dismiss_popup(page):
    """Dismiss Flow's popups (Notice, cookies, Chrome sign-in, splash banner)."""
    try:
        # Cookie consent banner
        try:
            cookie_bar = page.locator("#glue-cookie-notification-bar-1, .glue-cookie-notification-bar").first
            if cookie_bar.count() > 0 and cookie_bar.is_visible(timeout=500):
                for btn_text in ["Reject all", "Accept all", "I agree", "OK", "Got it"]:
                    btn = cookie_bar.locator(f"button:has-text('{btn_text}')")
                    if btn.count() > 0 and btn.first.is_visible(timeout=500):
                        btn.first.click(force=True)
                        print(f"✓ Dismissed cookie banner ({btn_text})", flush=True)
                        time.sleep(1)
                        return True
                # Fallback: click any button in the banner
                any_btn = cookie_bar.locator("button").last
                if any_btn.count() > 0:
                    any_btn.click(force=True)
                    print(f"✓ Dismissed cookie banner (fallback)", flush=True)
                    time.sleep(1)
                    return True
        except:
            pass

        # "Meet the new Flow" splash
        try:
            splash = page.locator("text=Meet the new Flow, text=what's new")
            if splash.count() > 0 and splash.first.is_visible(timeout=500):
                close_btn = page.locator("button:has-text('close')").first
                if close_btn.count() > 0 and close_btn.is_visible():
                    close_btn.click(force=True)
                    print(f"✓ Dismissed splash banner", flush=True)
                    time.sleep(1)
                    return True
        except:
            pass
        
        # Chrome sign-in dialogs
        for dismiss_text in ["Use Chrome without an account", "No thanks", "Not now",
                             "Dismiss", "Skip", "Done", "Skip customization"]:
            try:
                btn = page.locator(f"button:has-text('{dismiss_text}')")
                if btn.count() > 0 and btn.first.is_visible(timeout=500):
                    btn.first.click(force=True)
                    print(f"✓ Dismissed Chrome dialog ({dismiss_text})", flush=True)
                    time.sleep(1)
                    return True
            except:
                pass

        # "Continue as X" profile-chooser splash (parity with video worker):
        # prefer "Use Chrome without", else click "Continue as <name>" to proceed.
        try:
            no_btn = page.locator("button:has-text('Use Chrome without')")
            continue_btn = page.locator("button:has-text('Continue as')")
            if no_btn.count() > 0 and no_btn.first.is_visible(timeout=500):
                no_btn.first.click(force=True)
                print(f"✓ Dismissed Chrome sign-in dialog (no account)", flush=True)
                time.sleep(1)
                return True
            elif continue_btn.count() > 0 and continue_btn.first.is_visible(timeout=500):
                continue_btn.first.click(force=True)
                print(f"✓ Clicked Continue as profile", flush=True)
                time.sleep(1)
                return True
        except:
            pass

        # Flow Notice dialog
        try:
            dialog = page.locator("div[role='dialog']")
            if dialog.count() > 0 and dialog.first.is_visible():
                agree_btn = dialog.locator("button:has-text('I agree')")
                if agree_btn.count() > 0 and agree_btn.first.is_visible():
                    agree_btn.first.click(force=True)
                    print(f"✓ Dismissed Notice dialog", flush=True)
                    time.sleep(1)
                    return True
        except:
            pass
        
        # Fallback: any "I agree" button
        for selector in [
            "div[role='dialog'] button:has-text('I agree')",
            "button:has-text('I agree')",
        ]:
            try:
                btn = page.locator(selector)
                if btn.count() > 0 and btn.first.is_visible():
                    btn.first.click(force=True)
                    print(f"✓ Dismissed popup ({selector})", flush=True)
                    time.sleep(1)
                    return True
            except:
                pass
    except:
        pass
    return False


def dismiss_create_with_flow(page, label=""):
    """Click the 'Create with Flow' splash button if present."""
    for sel in ["button:text-matches('Create with.*Flow', 'i')", "a:text-matches('Create with.*Flow', 'i')",
                "button:has-text('Create with Flow')", "a:has-text('Create with Flow')"]:
        try:
            btn = page.locator(sel).first
            if btn.is_visible(timeout=2000):
                try:
                    href = btn.get_attribute("href")
                    target = btn.get_attribute("target")
                    if href and target == "_blank":
                        page.goto(href, wait_until="domcontentloaded", timeout=30000)
                        time.sleep(2)
                        return True
                except Exception:
                    pass
                human_click_element(page, btn, f"Create with Flow")
                time.sleep(2)
                return True
        except Exception:
            pass
    return False


def ensure_logged_into_flow(page, label="IMAGE", timeout_minutes=10):
    """Ensure the page is on Flow and the user is logged in."""
    
    def _get_page_state(p):
        try:
            url = p.url.lower()
        except Exception:
            return 'other'
        
        # Dismiss Chrome browser dialogs
        try:
            for btn_text in ["Use Chrome without an account", "No thanks", "Not now"]:
                btn = p.locator(f"button:has-text('{btn_text}')")
                if btn.count() > 0 and btn.first.is_visible(timeout=500):
                    btn.first.click(force=True)
                    time.sleep(1)
                    break
        except:
            pass
        
        if "accounts.google" in url and ("setsid" in url or "consent" in url):
            return 'google_redirect'
        if "accounts.google" in url:
            return 'google_login'
        if is_flow_url(url):
            if is_flow_project(url):
                return 'flow_logged_in'
            logged_in_selectors = [
                "img[src*='googleusercontent.com']",
                "img[src*='lh3.googleusercontent']",
                "button:has-text('New project')",
                "button:has-text('Nuovo progetto')",
                "button:has-text('Nuevo proyecto')",
                "button:has-text('Nouveau projet')",
                "button:has-text('Neues Projekt')",
                "button:has(i:text('add_2'))",
            ]
            for selector in logged_in_selectors:
                try:
                    if p.locator(selector).first.is_visible(timeout=1500):
                        return 'flow_logged_in'
                except Exception:
                    pass
            try:
                # Regex matches both old "Create with Flow" and new "Create with Google Flow"
                if p.locator("button:text-matches('Create with.*Flow', 'i')").is_visible(timeout=1500):
                    return 'flow_not_logged_in'
            except Exception:
                pass
            time.sleep(2)
            for selector in logged_in_selectors:
                try:
                    if p.locator(selector).first.is_visible(timeout=1500):
                        return 'flow_logged_in'
                except Exception:
                    pass
            return 'flow_not_logged_in'
        return 'other'
    
    def _wait_for_page_settle(p, max_seconds=30):
        for i in range(max_seconds):
            time.sleep(1)
            state = _get_page_state(p)
            if state != 'google_redirect':
                return state
        return _get_page_state(p)
    
    def _wait_for_user_login(p):
        print(f"\n{'='*50}", flush=True)
        print(f"[{label}] GOOGLE LOGIN REQUIRED", flush=True)
        print(f"Please complete login in the browser...", flush=True)
        print(f"{'='*50}\n", flush=True)
        # v457: un-minimize Chrome so the user can actually see the
        # login prompt without hunting through the taskbar. Set the
        # stay_visible flag so the navigation handler doesn't
        # re-minimize us when the login flow navigates around Google
        # auth pages.
        try:
            p._stay_visible = True
        except Exception:
            pass
        try:
            restore_chrome_window(p, label=label)
        except Exception:
            pass
        start_time = time.time()
        max_wait = timeout_minutes * 60
        while True:
            time.sleep(2)
            state = _get_page_state(p)
            if state == 'flow_logged_in':
                print(f"✓ [{label}] Login confirmed!", flush=True)
                time.sleep(3)
                # v457: login done → clear stay-visible flag and go
                # back to minimized so the user's workflow isn't
                # interrupted again on future navigations.
                try:
                    p._stay_visible = False
                except Exception:
                    pass
                try:
                    minimize_chrome_window(p, label=label)
                except Exception:
                    pass
                return
            if state == 'flow_not_logged_in':
                entry_selectors = [
                    "button:text-matches('Create with.*Flow', 'i')", "a:text-matches('Create with.*Flow', 'i')",
                    "button:has-text('Create with Flow')", "a:has-text('Create with Flow')",
                    "button:has-text('Get started')", "a:has-text('Get started')",
                    "button:has-text('Try Flow')", "button:has-text('Sign in')",
                ]
                for sel in entry_selectors:
                    try:
                        btn = p.locator(sel).first
                        if btn.is_visible(timeout=1000):
                            try:
                                href = btn.get_attribute("href")
                                target = btn.get_attribute("target")
                                if href and target == "_blank":
                                    p.goto(href, wait_until="domcontentloaded", timeout=30000)
                                    time.sleep(3)
                                    break
                            except Exception:
                                pass
                            btn.click()
                            time.sleep(3)
                            break
                    except Exception:
                        pass
                continue
            elapsed = time.time() - start_time
            if elapsed > max_wait:
                print(f"⚠️ [{label}] Login timeout!", flush=True)
                return
    
    # Main logic
    for attempt in range(5):
        state = _get_page_state(page)
        if state == 'flow_logged_in':
            if attempt == 0:
                print(f"[{label}] ✓ Already logged in", flush=True)
            check_and_dismiss_popup(page)
            return False
        elif state == 'flow_not_logged_in':
            time.sleep(3)
            recheck = _get_page_state(page)
            if recheck == 'flow_logged_in':
                continue
            entry_selectors = [
                "button:text-matches('Create with.*Flow', 'i')", "a:text-matches('Create with.*Flow', 'i')",
                "button:has-text('Create with Flow')", "a:has-text('Create with Flow')",
                "button:has-text('Get started')", "a:has-text('Get started')",
                "button:has-text('Try Flow')", "button:has-text('Sign in')",
            ]
            for sel in entry_selectors:
                try:
                    btn = page.locator(sel).first
                    if btn.is_visible(timeout=1000):
                        try:
                            href = btn.get_attribute("href")
                            target = btn.get_attribute("target")
                            if href and target == "_blank":
                                page.goto(href, wait_until="domcontentloaded", timeout=30000)
                                time.sleep(3)
                                break
                        except Exception:
                            pass
                        human_click_element(page, btn, f"[{label}] {sel}")
                        break
                except Exception:
                    pass
            state = _wait_for_page_settle(page, max_seconds=15)
            if state == 'google_login':
                _wait_for_user_login(page)
            continue
        elif state == 'google_login':
            _wait_for_user_login(page)
            _wait_for_page_settle(page, max_seconds=15)
            continue
        elif state == 'google_redirect':
            _wait_for_page_settle(page, max_seconds=30)
            continue
        else:
            print(f"[{label}] Not on Flow — navigating...", flush=True)
            try:
                page.goto(FLOW_HOME_URL)
            except Exception:
                pass
            _wait_for_page_settle(page, max_seconds=15)
            continue
    
    check_and_dismiss_popup(page)
    return True


def spa_navigate_to_flow_home(page, label=""):
    """Navigate to Flow homepage without full reload (preserves reCAPTCHA)."""
    if is_flow_home(page.url):
        return True
    
    prefix = f"[{label}]" if label else "[SPA-NAV]"
    print(f"{prefix} Navigating to Flow home...", flush=True)
    
    # Try go_back
    try:
        for _ in range(5):
            page.go_back()
            time.sleep(1)
            if is_flow_home(page.url):
                return True
    except Exception:
        pass
    
    # Try logo click
    for selector in [
        "a[href*='/tools/flow']:not([href*='/project/'])",
        "a:has-text('Flow')",
    ]:
        try:
            link = page.locator(selector).first
            if link.count() > 0 and link.is_visible(timeout=2000):
                human_click_element(page, selector, f"{prefix} Flow home link")
                time.sleep(2)
                if "/project/" not in page.url:
                    return True
        except Exception:
            continue
    
    # Fallback: full navigation
    print(f"{prefix} ⚠ Falling back to page.goto", flush=True)
    try:
        page.goto(FLOW_HOME_URL)
        page.wait_for_load_state("domcontentloaded", timeout=30000)
    except Exception:
        pass
    human_delay(3, 5)
    human_mouse_move(page)
    human_delay(2, 3)
    scroll_randomly(page)
    return True


# ============================================================
# FLOW UI HELPERS (from flow_worker.py)
# ============================================================

def fill_prompt_textarea(page, prompt):
    """Fill the prompt textbox (Slate contenteditable div).

    Returns True on success, False if the textbox couldn't be found or
    the prompt couldn't be entered.
    """
    textbox = page.locator('div[role="textbox"]').first
    if textbox.count() == 0:
        print("⚠ Prompt textbox not found on page", flush=True)
        return False
    try:
        if not textbox.is_visible(timeout=3000):
            print("⚠ Prompt textbox not visible", flush=True)
            return False
    except Exception:
        print("⚠ Prompt textbox visibility check failed", flush=True)
        return False

    try:
        textbox.scroll_into_view_if_needed(timeout=3000)
        time.sleep(0.3)
    except:
        pass
    box = textbox.bounding_box()
    if box:
        human_mouse_move_to(page, box['x'] + box['width']/2, box['y'] + box['height']/2)
        time.sleep(random.uniform(0.1, 0.3))
    try:
        textbox.focus(timeout=2000)
    except:
        pass
    try:
        textbox.click(timeout=3000)
    except:
        if box:
            human_click_at(page)
    time.sleep(random.uniform(0.3, 0.6))

    # Verify focus
    textbox_focused = False
    try:
        textbox_focused = page.evaluate('''() => {
            const tb = document.querySelector('div[role="textbox"]');
            return tb && (document.activeElement === tb || tb.contains(document.activeElement));
        }''')
    except:
        pass
    if not textbox_focused:
        try:
            textbox.click(force=True, timeout=3000)
            time.sleep(0.3)
        except:
            pass

    # Select all + delete
    page.keyboard.press("Control+A")
    time.sleep(random.uniform(0.1, 0.2))
    page.keyboard.press("Backspace")
    time.sleep(random.uniform(0.1, 0.3))

    # Paste via clipboard
    pasted_ok = False
    try:
        escaped = json.dumps(prompt)
        page.evaluate(f"navigator.clipboard.writeText({escaped})")
        time.sleep(random.uniform(0.2, 0.4))
        page.keyboard.press("Control+v")
        time.sleep(random.uniform(0.3, 0.6))
        current_text = textbox.inner_text().strip()
        if len(current_text) >= len(prompt) * 0.8:
            pasted_ok = True
            print("✓ Prompt pasted via clipboard", flush=True)
    except:
        pass

    if not pasted_ok:
        print("⚠ Clipboard paste failed, using insert_text", flush=True)
        page.keyboard.press("Control+A")
        time.sleep(0.1)
        page.keyboard.press("Backspace")
        time.sleep(0.1)
        try:
            page.keyboard.insert_text(prompt)
            time.sleep(0.3)
        except Exception as e:
            print(f"⚠ insert_text also failed: {e}", flush=True)
            return False

    time.sleep(random.uniform(0.3, 0.6))

    # Sanity check: the textbox should contain (most of) the prompt
    try:
        final_text = textbox.inner_text().strip()
        if len(final_text) < len(prompt) * 0.5:
            print(f"⚠ Prompt looks incomplete after paste: {len(final_text)}/{len(prompt)} chars", flush=True)
            return False
    except Exception:
        # If we can't verify, assume it worked (paste message already printed)
        pass

    return True


def clear_prompt_references(page, context=""):
    """No-op. Chip removal via click was unreliable (clicking the chip
    opens a card overlay rather than detaching it). We now handle cross-job
    reference cleanup via page.reload() when reusing a project, which
    clears all transient state. This stub is kept so existing callers
    don't break."""
    return 0


def find_dialog_upload_button(dialog):
    """Find the actual 'Upload image' button inside a Flow frame dialog.

    The dialog contains multiple buttons (date dropdown, upload, 'Recently Used' dropdown).
    The upload element is identified by its <i> icon with text 'upload' or text 'Upload image'.

    NOTE: As of April 2025, Google changed the upload button from <button> to <div>,
    so we check both element types. This function is a direct copy of
    flow_worker.py's find_dialog_upload_button — do not modify without
    syncing the video worker version.

    v532: prioritize the most-specific selector first ("Upload image" text inside
    a div that also contains the 'upload' icon). User-confirmed DOM structure:
        <div class="sc-f4d15a74-11 dzjjJT">
          <div class="sc-f4d15a74-12 gqbjEC">
            <i class="google-symbols">upload</i>
          </div>
          <div>Upload image</div>
        </div>
    The label "Upload image" is unique in the dialog, so matching by it first
    avoids picking up a virtuoso item or other div that happens to have "upload"
    somewhere in its tree.
    """
    # Primary (v532): div that has BOTH the upload icon AND the "Upload image" label.
    # This is the most precise match — even if Flow's class names change, the
    # combination of those two children is the upload tile's signature.
    for selector in [
        "div:has(i:text('upload')):has-text('Upload image')",  # v532: most precise
        "div:has(> div > i:text('upload'))",                   # New: nested div > div > i
        "div:has(i:text('upload')):has-text('Upload')",        # New: div with icon + text
        "button:has(i:text('upload'))",                        # Legacy: button with icon
        "div:has-text('Upload image')",                        # Text-based div match
        "button:has(span:text('Upload image'))",               # Legacy: button with span
    ]:
        try:
            btn = dialog.locator(selector)
            if btn.count() > 0:
                return btn.first
        except Exception:
            continue

    # Last resort
    print("[find_dialog_upload_button] ⚠ Could not find upload button by icon/text, using button.first", flush=True)
    return dialog.locator("button").first


def upload_frame(page, image_path, frame_name="frame"):
    """Upload a frame image. Dialog must already be open."""
    human_pre_action(page, f"upload {frame_name}")
    dialog = page.locator('[role="dialog"]').first
    dialog.wait_for(state="visible", timeout=5000)
    
    upload_btn = find_dialog_upload_button(dialog)
    upload_btn.wait_for(state="visible", timeout=3000)
    
    try:
        # v535: locator.click() instead of human_click_for_file_chooser
        # for the same Patchright/Chrome reason as the main upload path.
        with page.expect_file_chooser(timeout=5000) as fc_info:
            upload_btn.click(timeout=3000)
        time.sleep(random.uniform(2, 5))
        fc_info.value.set_files(image_path)
        print(f"✓ Uploaded {frame_name}", flush=True)
    except Exception as e:
        print(f"⚠ File chooser failed for {frame_name}: {e}, trying fallback...", flush=True)
        time.sleep(random.uniform(2, 4))
        page.locator("input[type='file']").first.set_input_files(image_path)
        print(f"✓ Uploaded {frame_name} (fallback)", flush=True)
    time.sleep(2)


def wait_for_media_popup_to_close(page, context="", timeout=30):
    """Wait for the media gallery popup to close after upload/crop."""
    prefix = f"[{context}] " if context else ""
    popup_selectors = [
        "div[data-testid='virtuoso-item-list']",
        "div.virtuoso-grid-list",
        "div.virtuoso-grid-item",
    ]
    
    def is_popup_visible():
        for selector in popup_selectors:
            try:
                el = page.locator(selector)
                if el.count() > 0 and el.first.is_visible():
                    return True
            except:
                pass
        return False
    
    # Wait for popup to appear
    popup_appeared = False
    for _ in range(16):
        if is_popup_visible():
            popup_appeared = True
            break
        time.sleep(0.5)
    
    if not popup_appeared:
        time.sleep(4)
        return True
    
    # Wait for popup to disappear
    start_time = time.time()
    while time.time() - start_time < timeout:
        if not is_popup_visible():
            time.sleep(0.5)
            return True
        time.sleep(random.uniform(0.3, 0.6))
    
    return False


# ============================================================
# IMAGE-SPECIFIC FUNCTIONS
# ============================================================

# Aspect ratio mapping: CLI value → Flow tab text content
# From the dropdown HTML: each tab has icon + text like "16:9", "4:3", etc.
ASPECT_RATIO_MAP = {
    "16:9": "16:9",
    "4:3":  "4:3",
    "1:1":  "1:1",
    "3:4":  "3:4",
    "9:16": "9:16",
}

# Icon names for each aspect ratio (used as fallback selectors)
ASPECT_RATIO_ICONS = {
    "16:9": "crop_16_9",
    "4:3":  "crop_landscape",
    "1:1":  "crop_square",
    "3:4":  "crop_portrait",
    "9:16": "crop_9_16",
}


def _open_settings_dropdown(page, prefix=""):
    """Open the settings dropdown (shared by select_image_mode + configure).
    
    The trigger button shows: 🍌 Nano Banana 2 | aspect_icon | x1
    Same Radix dropdown as video mode. Must wait for hydration before clicking.
    
    Returns:
        The settings button locator if opened, None if failed.
    """
    settings_btn = None
    
    # Find the settings button — it shows the current variant count (x1-x4)
    # or the model name with Nano Banana
    try:
        # Primary: find by variant text (same as video worker)
        for n in range(1, 5):
            candidate = page.locator(f"button:has-text('x{n}')").first
            try:
                candidate.wait_for(state="visible", timeout=3000)
                settings_btn = candidate
                break
            except:
                continue
        
        # Fallback: find by Nano Banana text
        if settings_btn is None:
            candidate = page.locator("button:has-text('Nano Banana')").first
            try:
                candidate.wait_for(state="visible", timeout=3000)
                settings_btn = candidate
            except:
                pass

        # New Flow composer chip (2026-07 redesign): the per-generation settings
        # trigger is now a menu button reading "Image · [aspect] 1x" (aria-haspopup=
        # menu, number-first variant), so the old "xN"/model-name finders miss it →
        # "Settings button not found". Same fix as the video worker's chip; the
        # menu internals (flow_tab_slider_trigger tabs) are unchanged.
        if settings_btn is None:
            for _newsel in ("button[aria-haspopup='menu']:has-text('Image ·')",
                            "button[aria-haspopup='menu']:has-text('Image')",
                            "button[aria-haspopup='menu']:has-text('Video')"):
                try:
                    _nc = page.locator(_newsel).first
                    _nc.wait_for(state="visible", timeout=3000)
                    settings_btn = _nc
                    break
                except Exception:
                    continue
    except:
        pass

    if settings_btn is None:
        print(f"{prefix}⚠ Settings button not found", flush=True)
        return None
    
    # Wait for Radix hydration (data-state attribute appears)
    print(f"{prefix}⏳ Waiting for settings button hydration...", flush=True)
    hydrated = False
    for _ in range(20):
        try:
            # Dismiss cookie banner that may block the button
            try:
                cookie_bar = page.locator("#glue-cookie-notification-bar-1").first
                if cookie_bar.is_visible(timeout=200):
                    for btn_text in ["Reject all", "Accept all", "I agree", "OK"]:
                        btn = page.locator(f".glue-cookie-notification-bar button:has-text('{btn_text}')").first
                        if btn.is_visible(timeout=200):
                            btn.click(force=True)
                            time.sleep(0.5)
                            break
            except:
                pass
            ds = settings_btn.get_attribute("data-state", timeout=500)
            if ds is not None:
                hydrated = True
                break
        except:
            pass
        time.sleep(0.5)
    
    if not hydrated:
        print(f"{prefix}⚠ Settings button never hydrated", flush=True)
        return None
    
    # Open the dropdown
    state = settings_btn.get_attribute("data-state")
    if state == "open":
        return settings_btn
    
    for click_try in range(5):
        # Re-find button each try (DOM may re-render)
        current_btn = None
        for n in range(1, 5):
            candidate = page.locator(f"button:has-text('x{n}')").first
            try:
                candidate.wait_for(state="visible", timeout=2000)
                current_btn = candidate
                break
            except:
                continue
        if current_btn is None:
            try:
                current_btn = page.locator("button:has-text('Nano Banana')").first
                current_btn.wait_for(state="visible", timeout=2000)
            except:
                current_btn = settings_btn
        
        try:
            current_btn.click(timeout=5000)
            for _ in range(10):
                time.sleep(0.2)
                if current_btn.get_attribute("data-state") == "open":
                    settings_btn = current_btn
                    print(f"{prefix}✓ Settings dropdown opened", flush=True)
                    time.sleep(1.5)  # Wait for Radix portal to mount
                    return settings_btn
        except Exception as e:
            try:
                current_btn.click(force=True, timeout=3000)
                for _ in range(10):
                    time.sleep(0.2)
                    if current_btn.get_attribute("data-state") == "open":
                        settings_btn = current_btn
                        print(f"{prefix}✓ Settings dropdown opened (force)", flush=True)
                        time.sleep(1.5)
                        return settings_btn
            except:
                pass
        time.sleep(0.5)
    
    print(f"{prefix}⚠ Could not open settings dropdown after 5 attempts", flush=True)
    return None


def _close_settings_dropdown(page):
    """Close the settings dropdown by pressing Escape."""
    try:
        page.keyboard.press("Escape")
        time.sleep(0.3)
    except:
        pass


def select_image_mode(page, context=""):
    """
    Switch Flow project to IMAGE generation mode.
    
    Inside the settings dropdown, the first row has two tabs:
      - Image (icon: 'image') — aria-controls contains 'IMAGE'
      - Video (icon: 'videocam') — aria-controls contains 'VIDEO'
    
    Both use class flow_tab_slider_trigger.

    Returns True on success, False on failure (so the caller can branch on
    a boolean). On unrecoverable setup failure (settings dropdown won't
    open), raises Exception.
    """
    prefix = f"[{context}] " if context else ""
    
    settings_btn = _open_settings_dropdown(page, prefix)
    if settings_btn is None:
        raise Exception("Cannot open settings dropdown to select image mode")
    
    try:
        # Check if Image tab is already selected
        image_tab = page.locator(
            "button.flow_tab_slider_trigger:has(i:text('image'))"
        ).first
        image_tab.wait_for(state="visible", timeout=5000)
        
        is_selected = image_tab.get_attribute("aria-selected")
        if is_selected == "true":
            print(f"{prefix}✓ Image mode already selected", flush=True)
            # Don't close — configure_image_settings will use the same open dropdown
            return True

        human_click_element(page, image_tab, f"{prefix}Image tab")
        time.sleep(0.5)
        # Verify
        is_selected = image_tab.get_attribute("aria-selected")
        if is_selected == "true":
            print(f"{prefix}✓ Switched to Image mode", flush=True)
            return True

        print(f"{prefix}⚠ Image tab click may not have registered", flush=True)
        return False
    except Exception as e:
        print(f"{prefix}⚠ Image mode selection failed: {e}", flush=True)
        _close_settings_dropdown(page)
        raise

    # Unreachable — every branch above returns or raises. Defensive only.
    return False


def configure_image_settings(page, aspect_ratio="16:9", resolution="1K",
                              model="nano_banana_2", variants=1, context=""):
    """
    Configure image settings in the Flow settings dropdown.
    
    Dropdown structure (already open from select_image_mode or opened here):
      Row 1: Image | Video  (mode tabs)
      Row 2: 16:9 | 4:3 | 1:1 | 3:4 | 9:16  (aspect ratio tabs)
      Row 3: x1 | x2 | x3 | x4  (variant tabs)
      Model button: 🍌 Nano Banana 2 ▼  (sub-dropdown for model selection)
    
    Args:
        page: Playwright page
        aspect_ratio: one of "16:9", "4:3", "1:1", "3:4", "9:16"
        resolution: "512", "1K", "2K", "4K" (may not be in dropdown — TBD)
        model: "nano_banana_2" or "nano_banana_pro"
        variants: number of variants (1-4)
        context: label for logging
    """
    prefix = f"[{context}] " if context else ""
    settings_applied = {}
    
    # Ensure dropdown is open (may already be from select_image_mode)
    # Check if any flow_tab_slider_trigger is visible (dropdown content)
    dropdown_visible = False
    try:
        tab_check = page.locator("button.flow_tab_slider_trigger:has(i:text('image'))").first
        dropdown_visible = tab_check.is_visible(timeout=1000)
    except:
        pass
    
    if not dropdown_visible:
        settings_btn = _open_settings_dropdown(page, prefix)
        if settings_btn is None:
            raise Exception("Cannot open settings dropdown for image settings")
    
    # --- Image mode (ensure it's selected) ---
    try:
        image_tab = page.locator(
            "button.flow_tab_slider_trigger:has(i:text('image'))"
        ).first
        if image_tab.is_visible(timeout=2000):
            if image_tab.get_attribute("aria-selected") != "true":
                human_click_element(page, image_tab, f"{prefix}Image tab")
                time.sleep(0.5)
            settings_applied['Image'] = True
            print(f"{prefix}✓ Image mode OK", flush=True)
    except:
        settings_applied['Image'] = False
        print(f"{prefix}⚠ Image mode tab missed", flush=True)
    
    # --- Aspect ratio ---
    ar_text = ASPECT_RATIO_MAP.get(aspect_ratio, aspect_ratio)
    ar_icon = ASPECT_RATIO_ICONS.get(aspect_ratio)
    try:
        # Try matching by text content (e.g. "16:9", "9:16")
        ar_tab = page.locator(
            f"button.flow_tab_slider_trigger:text-is('{ar_text}')"
        ).first
        
        # If text-is doesn't work, try icon-based
        if ar_tab.count() == 0 and ar_icon:
            ar_tab = page.locator(
                f"button.flow_tab_slider_trigger:has(i:text('{ar_icon}'))"
            ).first
        
        ar_tab.wait_for(state="visible", timeout=3000)
        if ar_tab.get_attribute("aria-selected") != "true":
            human_click_element(page, ar_tab, f"{prefix}Aspect ratio {ar_text}")
            time.sleep(0.5)
        settings_applied['AspectRatio'] = True
        print(f"{prefix}✓ Aspect ratio {ar_text} OK", flush=True)
    except Exception as e:
        settings_applied['AspectRatio'] = False
        print(f"{prefix}⚠ Aspect ratio {ar_text} failed: {e}", flush=True)
    
    # --- Variants ---
    try:
        target = f"x{variants}"
        # Flow labels the single-variant tab "1x" while multi tabs are
        # "x2"/"x3"/"x4" — match both label orders, plus an aria-controls
        # fallback (the tab's panel id ends in the variant number,
        # e.g. radix-:r15k:-content-1).
        var_tab = page.locator(
            f"button.flow_tab_slider_trigger:text-is('x{variants}'), "
            f"button.flow_tab_slider_trigger:text-is('{variants}x'), "
            f"button.flow_tab_slider_trigger[aria-controls$='-content-{variants}']"
        ).first
        var_tab.wait_for(state="visible", timeout=3000)
        if var_tab.get_attribute("aria-selected") != "true":
            human_click_element(page, var_tab, f"{prefix}Variants {target}")
            time.sleep(0.5)
        settings_applied['Variants'] = True
        print(f"{prefix}✓ Variants {target} OK", flush=True)
    except:
        settings_applied['Variants'] = False
        print(f"{prefix}⚠ Variants tab missed", flush=True)
    
    # --- Model selection ---
    # The model button shows "🍌 Nano Banana 2" with arrow_drop_down
    # Clicking it opens a sub-dropdown to pick NB2 / NB Pro / NB (2.5 Flash)
    try:
        model_btn = page.locator(
            "button:has-text('Nano Banana'):has(i:text('arrow_drop_down')), "
            "button:has-text('Imagen'):has(i:text('arrow_drop_down'))"
        ).first
        model_btn.wait_for(state="visible", timeout=3000)
        
        current_model_text = _normalize_model_label(model_btn.inner_text())

        # Determine target model text
        if model == "nano_banana_pro":
            target_text = "Nano Banana Pro"
        elif model == "nano_banana":
            target_text = "Nano Banana"  # 2.5 Flash (the basic one)
        elif model == "imagen_4":
            target_text = "Imagen 4"
        else:
            target_text = "Nano Banana 2"

        # Check if already correct. v807.1: the menu now lists
        # "Nano Banana 2" AND "Nano Banana 2 Lite" — plain substring
        # matching would accept Lite as NB2 and silently keep the wrong
        # model, so NB2 needs negative guards.
        if model == "nano_banana_pro":
            already = "nano banana pro" in current_model_text
        elif model == "nano_banana":
            already = ("nano banana" in current_model_text
                       and "2" not in current_model_text
                       and "pro" not in current_model_text)
        elif model == "imagen_4":
            already = "imagen" in current_model_text
        else:
            already = ("nano banana 2" in current_model_text
                       and "lite" not in current_model_text
                       and "2.5" not in current_model_text)

        if already:
            settings_applied['Model'] = True
            print(f"{prefix}✓ Model already {target_text}", flush=True)
        else:
            # Open model sub-dropdown
            human_click_locator(page, model_btn, f"{prefix}Model dropdown")
            time.sleep(1)

            # Find and click target model option.
            # v807.1: exact (emoji-stripped) label match FIRST — the old
            # substring selectors match both "Nano Banana 2" and
            # "Nano Banana 2 Lite" and picked whichever came first in
            # the DOM. Substring selectors kept only as fallback.
            model_found = False
            try:
                target_norm = _normalize_model_label(target_text)
                items = page.locator("[role='menuitem'], [role='menuitemradio']")
                for _mi in range(items.count()):
                    it = items.nth(_mi)
                    try:
                        if _normalize_model_label(it.inner_text()) == target_norm \
                                and it.is_visible(timeout=500):
                            human_click_locator(page, it, f"{prefix}{target_text}")
                            model_found = True
                            time.sleep(0.5)
                            break
                    except Exception:
                        continue
            except Exception:
                pass
            if not model_found:
                for sel in [
                    f"[role='menuitem']:has-text('{target_text}')",
                    f"[role='menuitemradio']:has-text('{target_text}')",
                    f"button:has-text('{target_text}')",
                    f"text={target_text}",
                ]:
                    try:
                        opt = page.locator(sel).first
                        if opt.is_visible(timeout=2000):
                            human_click_locator(page, opt, f"{prefix}{target_text}")
                            model_found = True
                            time.sleep(0.5)
                            break
                    except:
                        continue
            
            if model_found:
                settings_applied['Model'] = True
                print(f"{prefix}✓ Model set to {target_text}", flush=True)
            else:
                settings_applied['Model'] = False
                print(f"{prefix}⚠ Could not find {target_text} option", flush=True)
                page.keyboard.press("Escape")
                time.sleep(0.3)
    except Exception as e:
        settings_applied['Model'] = False
        print(f"{prefix}⚠ Model selection failed: {e}", flush=True)
    
    # --- Close dropdown ---
    _close_settings_dropdown(page)
    
    # Report
    print(f"{prefix}Settings: {settings_applied}", flush=True)

    # Consider the configuration a success if we could at least set aspect
    # ratio and variant count. Model selection failure is tolerable — Flow
    # defaults to the currently active model. Resolution isn't in the
    # dropdown at the moment (handled elsewhere or defaults).
    aspect_ok = settings_applied.get('AspectRatio', False)
    variants_ok = settings_applied.get('Variants', True)  # default True if not attempted
    return aspect_ok and variants_ok


def _normalize_model_label(s):
    """Lowercase a Flow model label and collapse emoji/punctuation to
    spaces so '🍌 Nano Banana 2' normalizes to 'nano banana 2'.

    v807.1: the model menu lists 'Nano Banana 2' AND 'Nano Banana 2 Lite';
    menu picks must compare EXACT normalized labels, not substrings, or
    Lite can satisfy an NB2 target.
    """
    import re as _r
    s = _r.sub(r"[^a-z0-9.]+", " ", (s or "").lower())
    return _r.sub(r"\s+", " ", s).strip()


def upload_reference_images(page, image_paths, context="", already_uploaded=None):
    """Upload reference image(s) with gallery-reuse optimization.

    For each image, opens Flow's media dialog and first checks if the
    filename is already in the gallery (from a previous upload). If found,
    selects it. Otherwise, uploads fresh. This saves 10-15s per image on
    repeated reference use (e.g. the same subject across multiple scenes).

    Flow preserves the uploaded filename as the `alt` attribute on each
    gallery thumbnail, so we look up by `img[alt='variant_42.png']`.

    Args:
        page: Playwright page
        image_paths: list of local file paths. The basename of each is
                     used as the gallery lookup key.
        context: label for logging
        already_uploaded: optional set of basenames already uploaded in
                          the current Flow project. If given, files whose
                          basename is in the set are reused from the
                          gallery; on successful fresh upload, the new
                          filename is added to the set (mutating it).
    """
    prefix = f"[{context}] " if context else ""

    if not image_paths:
        print(f"{prefix}No reference images to upload", flush=True)
        return True

    for p in image_paths:
        if not os.path.exists(p):
            raise FileNotFoundError(f"Reference image not found: {p}")

    print(f"{prefix}Processing {len(image_paths)} reference image(s)...", flush=True)

    def _count_attached_chips():
        """Count how many reference-image chips are currently attached to
        the prompt composer. Each attached ref has a visible cancel (×)
        icon button with an associated data-card-open. We match on the
        composer chip structure so we don't catch unrelated cancel icons
        elsewhere in the UI."""
        try:
            chips = page.locator(
                "button[data-card-open]:has(i:text('cancel'))"
            )
            n = chips.count()
            visible = 0
            for i in range(n):
                try:
                    if chips.nth(i).is_visible(timeout=200):
                        visible += 1
                except Exception:
                    pass
            return visible
        except Exception:
            return 0

    MAX_ATTACH_RETRIES = 2  # total attempts = 1 + MAX_ATTACH_RETRIES

    img_idx = 0
    retries_left_for_current = MAX_ATTACH_RETRIES
    while img_idx < len(image_paths):
        img_path = image_paths[img_idx]
        filename = os.path.basename(img_path)
        attempt_num = (MAX_ATTACH_RETRIES - retries_left_for_current) + 1
        if attempt_num == 1:
            print(f"{prefix}  Image {img_idx+1}/{len(image_paths)}: {filename}", flush=True)
        else:
            print(f"{prefix}  ↻ Retry {attempt_num}/{MAX_ATTACH_RETRIES + 1} for {filename}", flush=True)
            # Force fresh-upload path on retry (gallery may have stale state)
            if already_uploaded is not None and filename in already_uploaded:
                already_uploaded.discard(filename)
            # Clean up any stuck dialogs before retrying
            try:
                page.keyboard.press("Escape")
                time.sleep(0.3)
                page.keyboard.press("Escape")
                time.sleep(0.8)
            except Exception:
                pass

        # Snapshot the current number of attached chips BEFORE we start
        # uploading this image. Successful attach = chip count increases
        # by exactly 1. Without this, the verification would pass on the
        # 2nd image just by detecting the 1st image's chip still there.
        chips_before_this = _count_attached_chips()

        # Close any leftover dialog/popover from prior settings configuration
        # — the settings dropdown can leave a stale [role=dialog] behind which
        # confuses the scope of our next search.
        try:
            page.keyboard.press("Escape")
            time.sleep(0.3)
            page.keyboard.press("Escape")
            time.sleep(0.3)
        except Exception:
            pass

        # --- Open the Create/gallery dialog ---
        # EXACT copy of flow_worker.py pattern. The Create button has
        # aria-haspopup="dialog" (from your DOM), which is the same attribute
        # the video worker uses to locate the frame button.
        check_and_dismiss_popup(page)

        frame_selector = 'div[aria-haspopup="dialog"], button[aria-haspopup="dialog"]'

        # Narrow down to the Create button specifically: has an <i>add_2</i>
        # or a <span>Create</span> child. Without this we could match other
        # aria-haspopup=dialog elements (date dropdown, sort dropdown).
        create_btn = page.locator(
            f"{frame_selector}:has(i:text('add_2'))"
        ).first
        try:
            create_btn.wait_for(state="visible", timeout=5000)
        except Exception:
            create_btn = page.locator(
                f"{frame_selector}:has(span:text('Create'))"
            ).first
            create_btn.wait_for(state="visible", timeout=5000)

        # Human-like click — match flow_worker.py pattern: move mouse to
        # bounding box, then use human_click_at (not human_click_element).
        human_pre_action(page, "open gallery dialog")
        time.sleep(random.uniform(0.3, 0.8))

        box = create_btn.bounding_box()
        if box:
            human_mouse_move_to(page, box['x'] + box['width']/2, box['y'] + box['height']/2)
            time.sleep(random.uniform(0.1, 0.3))
            human_click_at(page)
        else:
            create_btn.click(timeout=5000)
        print(f"{prefix}  ✓ Clicked Create (add_2)", flush=True)
        time.sleep(random.uniform(0.8, 1.5))

        # Wait for [role=dialog] to appear — same as flow_worker.upload_frame
        dialog = page.locator('[role="dialog"]').first
        try:
            dialog.wait_for(state="visible", timeout=5000)
        except Exception:
            # Retry the click — Flow sometimes drops the first one
            print(f"{prefix}  Dialog didn't appear, retrying click...", flush=True)
            if box:
                human_mouse_move_to(page, box['x'] + box['width']/2, box['y'] + box['height']/2)
                time.sleep(0.3)
                human_click_at(page)
            else:
                create_btn.click(timeout=5000)
            time.sleep(2)
            dialog.wait_for(state="visible", timeout=5000)

        print(f"{prefix}  ✓ Gallery dialog opened", flush=True)

        # --- Step 2a: Try gallery reuse ---
        # Simple lookup: if an img with the filename as alt text exists and
        # is visible, click its container. Otherwise fall through to upload.
        # Do NOT press Escape to "close dropdowns" — Flow's gallery itself
        # is built with Radix, so [data-radix-popper-content-wrapper] matches
        # the gallery too and Escape would close it.
        selected_from_gallery = False
        expect_in_gallery = already_uploaded is not None and filename in already_uploaded

        if expect_in_gallery:
            print(f"{prefix}  ♻ Looking up {filename} in gallery (seen before)...", flush=True)

        # Fix #1: the virtuoso gallery grid only mounts items near the viewport.
        # A file we uploaded 10 scenes ago may not be in the DOM right now —
        # we have to scroll through the gallery list to find it before giving
        # up. Only relevant when `expect_in_gallery` is True; for a fresh
        # file a single lookup is fine (it won't be there anyway).
        #
        # v529 fix #3: scroll search is more aggressive and instrumented:
        #   - max_scrolls bumped 12 → 20 (covers ~20+ rows of grid items)
        #   - scroll step reduced 400 → 250 (catches items mid-scroll)
        #   - logs progress every 5 scrolls so the operator can see
        #     "we tried, the file genuinely isn't there" vs silent failure
        #   - on miss, dumps the alts that ARE visible so we can see if
        #     the filename was renamed/truncated by Flow
        def _find_gallery_item_scrolling(dialog, target_filename, max_scrolls=20):
            """Scroll through the gallery's virtuoso list searching for an
            img whose alt matches target_filename. Returns the locator if
            found, None otherwise. Scrolls down first, then back up, because
            recently-uploaded items tend to be at the top but the viewport
            may have been scrolled mid-session."""
            # Find the virtuoso scroll container inside the dialog
            try:
                scroll_container = dialog.locator(
                    "div[data-testid='virtuoso-scroller'], "
                    "div.virtuoso-grid-list, "
                    "div[data-testid='virtuoso-item-list']"
                ).first
                if scroll_container.count() == 0:
                    scroll_container = dialog  # fallback: whole dialog
            except Exception:
                scroll_container = dialog

            # Try current viewport first
            try:
                item = dialog.locator(f"img[alt='{target_filename}']").first
                if item.count() > 0 and item.is_visible(timeout=500):
                    return item
            except Exception:
                pass

            # Scroll down through the list, checking after each scroll.
            # v529: smaller steps (250 vs 400) catch mid-scroll items in
            # virtuoso grids where a single big scroll skips rows.
            for step in range(max_scrolls):
                try:
                    scroll_container.evaluate("el => { el.scrollBy(0, 250); }")
                    time.sleep(0.25)
                except Exception:
                    try:
                        page.mouse.wheel(0, 250)
                        time.sleep(0.25)
                    except Exception:
                        break
                try:
                    item = dialog.locator(f"img[alt='{target_filename}']").first
                    if item.count() > 0 and item.is_visible(timeout=500):
                        return item
                except Exception:
                    continue
                # v529: progress log every 5 scrolls so the operator can
                # see we're actively searching, not silently stuck.
                if step > 0 and (step + 1) % 5 == 0:
                    try:
                        visible_count = dialog.locator("img[alt]").count()
                        print(f"{prefix}    ↓ scrolled {step+1}/{max_scrolls} "
                              f"({visible_count} alt-tagged imgs in dialog)",
                              flush=True)
                    except Exception:
                        pass

            # v529: also try scrolling BACK UP past the start (virtuoso
            # may have unmounted items above the original viewport).
            try:
                scroll_container.evaluate("el => { el.scrollTop = 0; }")
                time.sleep(0.3)
            except Exception:
                pass

            # Final check after scroll-reset to top
            try:
                item = dialog.locator(f"img[alt='{target_filename}']").first
                if item.count() > 0 and item.is_visible(timeout=500):
                    return item
            except Exception:
                pass

            # v529 fix #3 diagnostic: dump what alts ARE visible so we
            # can tell whether the file legitimately isn't in the gallery
            # (worth re-uploading) vs whether the alt attribute mismatches
            # (Flow renamed it and we should fix the lookup key).
            try:
                visible_alts = dialog.evaluate(
                    "(d) => Array.from(d.querySelectorAll('img[alt]'))"
                    "  .map(i => i.getAttribute('alt'))"
                    "  .filter(a => a && a !== 'Generated image')"
                    "  .slice(0, 12)"
                )
                if visible_alts:
                    print(f"{prefix}    ⓘ gallery search miss for {target_filename!r} — "
                          f"visible alts: {visible_alts}",
                          flush=True)
            except Exception:
                pass

            return None

        # v807.4 — the ONLY click method that reliably attaches. Operator
        # logs showed a fixed pattern: the tile click fired immediately
        # after the dialog opens NEVER grows the chip ("✓ Reused" then
        # "Chip didn't grow 0 → 0"), while the recovery path's identical
        # click — after a settle + re-find — attaches every time. Flow
        # binds the tile click handlers a beat after render. So: settle,
        # click, VERIFY the chip actually grew, one internal re-click;
        # only report "Reused" on a confirmed chip.
        def _click_tile_and_verify(dialog_loc, item, chips_before):
            expected = chips_before + 1
            for attempt in range(2):
                if attempt > 0:
                    print(f"{prefix}  ↻ tile click didn't attach — settling and re-clicking", flush=True)
                    # Re-acquire dialog + tile; the failed click may have
                    # closed the dialog or re-rendered the grid.
                    try:
                        if not dialog_loc.is_visible(timeout=500):
                            reopen = page.locator(
                                f"{frame_selector}:has(i:text('add_2'))").first
                            reopen.click(timeout=3000)
                            time.sleep(1.0)
                            dialog_loc = page.locator('[role="dialog"]').first
                            dialog_loc.wait_for(state="visible", timeout=3000)
                        item = dialog_loc.locator(f"img[alt='{filename}']").first
                        if item.count() == 0 or not item.is_visible(timeout=1000):
                            return False
                    except Exception:
                        return False
                time.sleep(1.5)  # the settle IS the fix — handlers bind late
                container = item.locator("xpath=..").first
                try:
                    container.scroll_into_view_if_needed(timeout=2000)
                    time.sleep(0.3)
                except Exception:
                    pass
                try:
                    container.click(timeout=3000)
                except Exception:
                    try:
                        item.click(timeout=3000, force=True)
                    except Exception:
                        continue
                try:
                    dialog_loc.wait_for(state="hidden", timeout=5000)
                except Exception:
                    pass
                for _ in range(8):
                    if _count_attached_chips() >= expected:
                        return True
                    time.sleep(1)
            return False

        try:
            if expect_in_gallery:
                # Scroll-search the full gallery — this is the "trust the
                # gallery, not the set" path. Only when we've verified the
                # file genuinely isn't there do we re-upload.
                gallery_item = _find_gallery_item_scrolling(dialog, filename)
                if gallery_item is not None:
                    if _click_tile_and_verify(dialog, gallery_item, chips_before_this):
                        print(f"{prefix}  ✓ Reused from gallery: {filename}", flush=True)
                        selected_from_gallery = True
                    else:
                        print(f"{prefix}  ⚠ Gallery tile wouldn't attach — falling back to upload", flush=True)
            else:
                # Fresh upload path — quick check only. If the alt matches
                # by coincidence we can still reuse; otherwise proceed to
                # upload.
                gallery_item = dialog.locator(f"img[alt='{filename}']").first
                if gallery_item.count() > 0 and gallery_item.is_visible(timeout=1000):
                    if _click_tile_and_verify(dialog, gallery_item, chips_before_this):
                        print(f"{prefix}  ✓ Reused from gallery: {filename}", flush=True)
                        selected_from_gallery = True
        except Exception:
            pass

        if expect_in_gallery and not selected_from_gallery:
            print(f"{prefix}  ⚠ Expected {filename} in gallery but couldn't reuse — will re-upload", flush=True)
            # A failed tile click can leave the dialog closed; the upload
            # step below needs it open. Reopen if necessary.
            try:
                if not dialog.is_visible(timeout=500):
                    page.locator(f"{frame_selector}:has(i:text('add_2'))").first.click(timeout=3000)
                    time.sleep(1.0)
                    dialog = page.locator('[role="dialog"]').first
                    dialog.wait_for(state="visible", timeout=3000)
            except Exception:
                pass

        # --- Step 2b: If not reusing, upload exactly like flow_worker.upload_frame ---
        if not selected_from_gallery:
            upload_btn = find_dialog_upload_button(dialog)
            upload_btn.wait_for(state="visible", timeout=3000)

            uploaded = False

            # v532: snapshot the page-wide file input state BEFORE the click.
            # When file_chooser doesn't fire (Patchright + isTrusted=false on
            # synthesized clicks suppresses Chrome's programmatic file picker
            # trigger), Flow may still mount a NEW <input type="file"> for
            # this dialog instance. Comparing before/after lets us identify
            # exactly which input belongs to THIS click rather than guessing.
            #
            # The pattern: count file inputs now, click upload tile, count
            # again. If a new one appeared, it's the one we want. If count
            # is unchanged, the input was already there and we'll fall back
            # to "last on page" (most recently mounted = freshest).
            try:
                _input_count_before = page.locator("input[type='file']").count()
            except Exception:
                _input_count_before = 0

            # v608: skip the expect_file_chooser path entirely. On Patchright
            # + Chrome, the programmatic file picker trigger is reliably
            # blocked (isTrusted=false on synthesized clicks suppresses
            # Chrome's file picker event). The two prior attempts (4000ms
            # each + a full dialog reset) consistently timed out before
            # falling through to the set_input_files recovery — wasting
            # ~8-10s per upload. Recovery via the newly-mounted input
            # always succeeded.
            #
            # New flow: click the upload tile once (this still mounts a
            # fresh <input type="file"> in Flow's React tree even though
            # the file_chooser event doesn't fire), then go straight to
            # the set_input_files strategy chain below. The chip
            # verification + gallery recovery path catches any chip that
            # didn't auto-attach.
            #
            # If file_chooser semantics change in a future Flow build (the
            # event starts firing again), the strategy chain still works
            # because set_input_files on the freshly-mounted input is
            # equivalent to fc_info.value.set_files. No regression.
            try:
                upload_btn.click(timeout=3000)
                # Brief wait for Flow to mount the file input in response
                # to the click. Empirically <1s is enough; using a small
                # randomized sleep to avoid bot-detection signal patterns
                # while keeping it tight.
                time.sleep(random.uniform(0.6, 1.0))
            except Exception as e_click:
                print(f"{prefix}  ⚠ Upload button click failed: {e_click}", flush=True)
                # Don't raise here — the strategy chain below may still find
                # an existing input under the dialog (rare, but possible).

            # Attempt: set_input_files strategy chain (the path that works).
            # v531: restored the page-wide `input[type='file']` path that
            # v529 incorrectly removed.
            # v532: smarter input picking — use the before/after snapshot
            # taken at line ~1985 to identify the newly-mounted input.
            # When both file_chooser attempts fail on environments where
            # Chrome blocks programmatic file picker triggers (isTrusted=false
            # synthetic clicks), Flow still mounts a fresh <input type="file">
            # element for the click. Diffing against the pre-click count
            # tells us EXACTLY which input is the new one. If somehow no new
            # input appeared, fall back to the LAST input on the page
            # (most recently mounted = freshest), not the first.
            #
            # Fallback chain in priority order:
            #   1. Dialog-scoped input (Flow's "modern" layout)
            #   2. Newly-mounted input (page-wide, count grew vs snapshot)
            #   3. Any input under any open [role='dialog'] (portal mount,
            #      take last as Flow appends modals at body bottom)
            #   4. Last input on the page (coarse, but better than .first)
            # v608: this block always runs (no `if not uploaded` gate). The
            # file_chooser path that used to set `uploaded = True` was removed;
            # the strategy chain below is now the sole upload path.
            try:
                _used_path = None
                # Strategy 1: dialog-scoped (preferred when present)
                _file_input = dialog.locator("input[type='file']").first
                if _file_input.count() > 0:
                    _file_input.set_input_files(img_path)
                    _used_path = "dialog input"
                else:
                    # v532 — Strategy 2: newly-mounted input (count grew).
                    # This is the most precise signal that an input was
                    # mounted in response to OUR click.
                    try:
                        _input_count_after = page.locator("input[type='file']").count()
                    except Exception:
                        _input_count_after = _input_count_before
                    if _input_count_after > _input_count_before:
                        # The new input is at one of the last positions.
                        # Prefer the very last one — Flow appends new
                        # elements to the bottom of the body.
                        page.locator("input[type='file']").nth(_input_count_after - 1).set_input_files(img_path)
                        _used_path = (f"newly-mounted input "
                                      f"({_input_count_before}→{_input_count_after})")
                    else:
                        # v531 Strategy 3: any input under an open dialog.
                        portal_inputs = page.locator("[role='dialog'] input[type='file']")
                        portal_count = portal_inputs.count()
                        if portal_count > 0:
                            portal_inputs.nth(portal_count - 1).set_input_files(img_path)
                            _used_path = f"portal input (last of {portal_count} dialog inputs)"
                        else:
                            # v532 Strategy 4: LAST input on page (was
                            # .first in v531, but .last is empirically
                            # more reliable — most recent mount wins).
                            # Only matters if Flow doesn't grow the count
                            # AND doesn't put input under a dialog —
                            # rare path, kept as safety net.
                            _all_inputs = page.locator("input[type='file']")
                            _all_count = _all_inputs.count()
                            if _all_count > 0:
                                _all_inputs.nth(_all_count - 1).set_input_files(img_path)
                                _used_path = f"last page-wide input (of {_all_count})"
                            else:
                                raise RuntimeError(
                                    "no file input element found anywhere on page")
                print(f"{prefix}  ⤴ Sent {filename} ({_used_path}) — verifying chip", flush=True)
                uploaded = True
            except Exception as e3:
                print(f"{prefix}  ❌ Upload failed: {e3}", flush=True)
                raise

            time.sleep(2)

        # --- Step 2c: Verify image is attached to the prompt ---
        # Flow auto-attaches the uploaded image to the prompt composer
        # and auto-closes the gallery dialog. We verify by checking
        # that the attached-chip count has grown by exactly 1 compared
        # to BEFORE we started uploading this image.
        #
        # This runs for BOTH gallery-reuse AND fresh-upload paths —
        # a gallery click can silently fail to attach just as an upload
        # can. Checking just "is there a chip visible" would falsely
        # pass on the 2nd+ image because the 1st image's chip is still there.

        # Fix #3: wait for the upload dialog to close before we start
        # counting chips. Flow animates the dialog dismiss after a successful
        # upload — a too-early count read during the animation returns the
        # old chip count and we falsely declare failure. Up to 5s to close,
        # then proceed to the count-polling loop regardless.
        try:
            dialog.wait_for(state="hidden", timeout=5000)
        except Exception:
            pass  # dialog may remain open on failure; count-loop will decide

        print(f"{prefix}  ⏳ Verifying image attached to prompt...", flush=True)
        expected_chip_count = chips_before_this + 1
        attached = False
        current_chips = chips_before_this
        for wait_sec in range(15):
            current_chips = _count_attached_chips()
            if current_chips >= expected_chip_count:
                attached = True
                break
            time.sleep(1)

        if attached:
            print(f"{prefix}  ✓ Image attached to prompt: {filename} "
                  f"(chips: {chips_before_this} → {current_chips})", flush=True)
            # Remember this filename so next job can reuse from gallery
            if already_uploaded is not None:
                already_uploaded.add(filename)
            # Advance to the next image and reset retry budget
            img_idx += 1
            retries_left_for_current = MAX_ATTACH_RETRIES
        else:
            # v535 — Gallery recovery before declaring failure.
            # Per user feedback: even when the upload-tile click doesn't
            # auto-attach (Patchright + Chrome blocks isTrusted=false
            # propagation, so React's "select file → attach as chip"
            # callback chain breaks), the file MAY have still landed in
            # the gallery. The gallery upload happens through a different
            # input than the chip-attach handler. Before retrying upload
            # (which would just create duplicate gallery entries), check
            # if the file is sitting in the gallery and click it to
            # attach as a chip directly.
            print(f"{prefix}  🔄 Chip didn't grow ({chips_before_this} → {current_chips}) — "
                  f"checking if {filename} landed in gallery anyway...", flush=True)
            recovered = False
            try:
                # Re-open the dialog if it closed after the upload attempt.
                # Flow auto-closes the dialog on successful chip-attach,
                # but on chip-attach failure it may leave it open or close
                # it anyway. Force-reopen so the gallery is queryable.
                dialog_open_now = False
                try:
                    dialog_open_now = dialog.is_visible(timeout=500)
                except Exception:
                    dialog_open_now = False
                if not dialog_open_now:
                    try:
                        reopen_btn = page.locator(
                            f"{frame_selector}:has(i:text('add_2'))"
                        ).first
                        reopen_btn.click(timeout=3000)
                        time.sleep(1.0)
                        dialog = page.locator('[role="dialog"]').first
                        dialog.wait_for(state="visible", timeout=3000)
                    except Exception as _ro:
                        print(f"{prefix}  ⚠ Could not reopen dialog for gallery check: {_ro}", flush=True)
                # Scroll-search the gallery. The file may have just
                # uploaded — give it a brief moment to mount in the
                # virtuoso list before searching.
                time.sleep(1.5)
                recovered_item = _find_gallery_item_scrolling(dialog, filename)
                if recovered_item is not None:
                    container = recovered_item.locator("xpath=..").first
                    try:
                        container.scroll_into_view_if_needed(timeout=2000)
                        time.sleep(0.3)
                    except Exception:
                        pass
                    try:
                        container.click(timeout=3000)
                    except Exception:
                        try:
                            recovered_item.click(timeout=3000, force=True)
                        except Exception:
                            pass
                    print(f"{prefix}  ✓ Found {filename} in gallery — clicking to attach", flush=True)
                    time.sleep(1.5)
                    # Wait for dialog to close + chip count to grow
                    try:
                        dialog.wait_for(state="hidden", timeout=3000)
                    except Exception:
                        pass
                    for _wait in range(10):
                        current_chips = _count_attached_chips()
                        if current_chips >= expected_chip_count:
                            recovered = True
                            break
                        time.sleep(1)
            except Exception as _rec_err:
                print(f"{prefix}  ⚠ Gallery recovery failed: {_rec_err}", flush=True)

            if recovered:
                print(f"{prefix}  ✓ Recovered via gallery: {filename} "
                      f"(chips: {chips_before_this} → {current_chips})", flush=True)
                if already_uploaded is not None:
                    already_uploaded.add(filename)
                img_idx += 1
                retries_left_for_current = MAX_ATTACH_RETRIES
            else:
                # Attachment AND gallery recovery both failed — the file
                # genuinely isn't there. Original v528 behavior from here on:
                # evict from known-uploaded set, retry from upload step.
                print(f"{prefix}  ❌ Attachment verification failed for {filename} "
                      f"(chips: {chips_before_this} → {current_chips}, expected {expected_chip_count})",
                      flush=True)
                # Evict from the known-uploaded set so future jobs don't try
                # to reuse this file from gallery (it's clearly not there
                # or not clickable).
                if already_uploaded is not None and filename in already_uploaded:
                    already_uploaded.discard(filename)
                if retries_left_for_current > 0:
                    # Stay on the same img_idx; loop will retry
                    retries_left_for_current -= 1
                else:
                    print(f"{prefix}  ❌ All {MAX_ATTACH_RETRIES + 1} attempts exhausted — aborting job", flush=True)
                    return False

        time.sleep(0.5)

    print(f"{prefix}✓ All reference images attached", flush=True)
    return True


# ─────────────────────────────────────────────────────────────────────────────
# v703 — Worker-injected reference manifest (replaces fragile platform-side
# slot-substitution in image_platform.py:_resolve_flow_prompt_bindings).
#
# Problem (pre-v703): platform substituted "the uploaded character/product
# reference image" → "Image N" using DB slot_order, but slot_order vs the
# worker's actual attach order can drift (e.g. chain edge inserted between
# persona + product edges shifted product's flow_image_num). Banana 2 then
# saw "Use Image 3 for the bottle" while the worker had attached the bottle
# at Image 2, so the model bound the wrong reference to the bottle role.
#
# Fix (v703): worker has authoritative attach order at submit time. Build
# the manifest header right BEFORE pasting the prompt, using the same
# input_paths list that was just attached. Manifest is prepended to the
# prompt body; stale "Use Image N for ..." lines from platform substitution
# are stripped first to avoid conflicting numbers. Body's role-descriptor
# phrases ("the prior-scene reference image", "the main character", etc.)
# stay unchanged — manifest at top is authoritative for Banana 2.
# ─────────────────────────────────────────────────────────────────────────────

def _filename_to_ref_display_name(filename):
    """Map a worker-side reference filename to a human-readable display name
    suitable for the manifest header.

    The worker downloads each reference with a slugified filename derived
    from the platform's edge role (see _download_reference_inputs +
    _slugify_role). Examples:
      "the_main_character.png"          → "the main character"
      "the_korella_saffron_bottle.jpg"  → "the korella saffron bottle"
      "chain_from_image_1.png"          → "the prior scene (chain from image_1)"
      "ref_2.png"                       → "reference 2"
    """
    import os as _os
    import re as _re
    stem = _os.path.splitext(_os.path.basename(filename or ""))[0]
    if not stem:
        return "(unknown reference)"
    # v807: staged filenames carry a content-hash suffix ('__a1b2c3d4',
    # optionally '__2' after it for in-job collisions). Strip it so the
    # manifest still reads "the main character", not the hash.
    stem = _re.sub(r"__[0-9a-f]{8}(?:__\d+)?$", "", stem)
    # chain_from_image_K → "the prior scene (chain from image_K)"
    m = _re.match(r"^chain_from_image_(\d+)$", stem)
    if m:
        return f"the prior scene (chain from image_{m.group(1)})"
    # ref_N or variant_N → "reference N"
    m = _re.match(r"^(?:ref|variant)_(\d+)$", stem)
    if m:
        return f"reference {m.group(1)}"
    # default: replace underscores with spaces
    return stem.replace("_", " ")


def _build_reference_manifest(input_paths):
    """Build the v703 manifest header listing each attached reference and
    its actual Image N position, based on the worker's true attach order.

    Returns a string ending with a blank line, ready to prepend to the
    prompt body. Empty if no input_paths.

    Example output:
      Use Image 1 for the main character.
      Use Image 2 for the korella saffron bottle.
      Use Image 3 for the prior scene (chain from image_1).

      (blank line)
    """
    if not input_paths:
        return ""
    lines = []
    for i, p in enumerate(input_paths):
        name = _filename_to_ref_display_name(p)
        lines.append(f"Use Image {i + 1} for {name}.")
    return "\n".join(lines) + "\n\n"


def _strip_stale_reference_lines(prompt):
    """Remove any pre-existing 'Use Image N for X.' lines from the prompt
    body. Platform-side substitution (image_platform.py
    _resolve_flow_prompt_bindings) may have written numbered references
    that no longer match the worker's actual attach order. The v703 manifest
    we prepend is authoritative; stripping prevents conflicting numbers
    from confusing Banana 2.

    Only strips lines that look like the substitution output pattern:
      ^Use Image \\d+ for [^.]+\\.\\s*$
    Body content that mentions "Image N" in other contexts (e.g.
    "Use image_1 as the base frame", "Image quality settings", inline
    references) is preserved.
    """
    import re as _re
    if not prompt:
        return prompt
    # Strip leading "Use Image N for ...." lines (with optional trailing
    # whitespace / newlines after each)
    pattern = _re.compile(
        r"^Use Image \d+ for [^.\n]+\.\s*\n", flags=_re.MULTILINE
    )
    cleaned = pattern.sub("", prompt)
    return cleaned.lstrip("\n")


def upload_reference_images_legacy(page, image_paths, context=""):
    """Original multi-select upload (kept for reference). Not used."""
    prefix = f"[{context}] " if context else ""
    
    if not image_paths:
        print(f"{prefix}No reference images to upload", flush=True)
        return
    
    # Validate all files exist
    for p in image_paths:
        if not os.path.exists(p):
            raise FileNotFoundError(f"Reference image not found: {p}")
    
    print(f"{prefix}Uploading {len(image_paths)} reference image(s)...", flush=True)
    
    # --- Step 1: Click the 'Create' (add_2) button to open the dialog ---
    human_pre_action(page, "open upload dialog")
    
    create_btn = page.locator("button:has(i:text('add_2'))").first
    try:
        create_btn.wait_for(state="visible", timeout=5000)
    except Exception:
        # Fallback: try by aria-label or span text
        create_btn = page.locator("button:has(span:text('Create'))").first
        create_btn.wait_for(state="visible", timeout=5000)
    
    human_click_element(page, create_btn, f"{prefix}Create (add_2) button")
    time.sleep(1.5)
    
    # Wait for dialog to appear
    dialog = page.locator('[role="dialog"]').first
    try:
        dialog.wait_for(state="visible", timeout=5000)
        print(f"{prefix}✓ Upload dialog opened", flush=True)
    except Exception:
        # Sometimes the dialog takes a moment — retry click
        print(f"{prefix}⚠ Dialog didn't appear, retrying click...", flush=True)
        human_click_element(page, create_btn, f"{prefix}Create (retry)")
        time.sleep(2)
        dialog.wait_for(state="visible", timeout=5000)
    
    # --- Step 2: Click the 'upload' button inside the dialog ---
    upload_btn = dialog.locator("button:has(i:text('upload'))").first
    if upload_btn.count() == 0:
        # Fallback selectors
        upload_btn = dialog.locator("button:has(span:text('Upload image'))").first
    if upload_btn.count() == 0:
        upload_btn = find_dialog_upload_button(dialog)
    
    upload_btn.wait_for(state="visible", timeout=3000)
    
    # --- Step 3: Trigger file chooser and select all images ---
    try:
        with page.expect_file_chooser(timeout=8000) as fc_info:
            human_click_for_file_chooser(page, upload_btn)
        
        # Simulate human browsing the file picker
        time.sleep(random.uniform(2, 4))
        
        # Set all files at once (multi-select)
        fc_info.value.set_files(image_paths)
        print(f"{prefix}✓ Selected {len(image_paths)} file(s) in chooser", flush=True)
        
    except Exception as e:
        print(f"{prefix}⚠ File chooser failed: {e}, trying input fallback...", flush=True)
        time.sleep(random.uniform(1, 3))
        # Fallback: find hidden file input and set files directly
        file_input = page.locator("input[type='file']").first
        file_input.set_input_files(image_paths)
        print(f"{prefix}✓ Set {len(image_paths)} file(s) via input fallback", flush=True)
    
    # Wait for upload to process
    print(f"{prefix}⏳ Waiting for upload to process...", flush=True)
    time.sleep(3)
    
    # Wait for any processing indicator to disappear
    # (images may need a moment to appear in the dialog)
    for wait_sec in range(30):
        # Check if images are visible in the dialog (look for img elements or thumbnails)
        try:
            imgs_in_dialog = dialog.locator("img").count()
            if imgs_in_dialog >= len(image_paths):
                print(f"{prefix}✓ All {imgs_in_dialog} image(s) visible in dialog", flush=True)
                break
        except:
            pass
        time.sleep(1)
        if wait_sec % 10 == 9:
            print(f"{prefix}  Still waiting for images to appear... ({wait_sec+1}s)", flush=True)
    
    # Close the dialog if it's still open (images should be attached now)
    # Some flows auto-close; others need Escape or a "Done" button
    try:
        # Check if dialog is still visible
        if dialog.is_visible(timeout=1000):
            # Look for a close/done button
            for close_text in ["Done", "Close", "Save", "Apply"]:
                close_btn = dialog.locator(f"button:has-text('{close_text}')").first
                try:
                    if close_btn.is_visible(timeout=500):
                        human_click_element(page, close_btn, f"{prefix}{close_text}")
                        time.sleep(1)
                        break
                except:
                    continue
            else:
                # No close button found — try Escape
                page.keyboard.press("Escape")
                time.sleep(0.5)
    except:
        pass
    
    time.sleep(1)
    print(f"{prefix}✓ Reference images uploaded", flush=True)


def click_generate_image(page, context="", max_retries=3):
    """
    Click the Generate button (arrow_forward) for image generation.
    
    Same button as video mode. Includes:
    - Wait for button to be enabled (frames may still be processing)
    - Human-like mouse movement before click
    - Retry logic
    """
    prefix = f"[{context}] " if context else ""
    
    for attempt in range(max_retries):
        try:
            # Dismiss popups that might be blocking
            check_and_dismiss_popup(page)
            time.sleep(0.5)
            
            # Check if button is enabled — wait up to 60s
            arrow_btn = page.locator(
                "button:has(i:text('arrow_forward')), i:text('arrow_forward')"
            ).first
            
            if not _is_generate_enabled(page):
                print(f"{prefix}⚠ Generate button disabled — waiting...", flush=True)
                button_ready = False
                for wait_sec in range(60):
                    time.sleep(1)
                    if wait_sec % 10 == 9:
                        print(f"{prefix}  Still waiting... ({wait_sec+1}s)", flush=True)
                        check_and_dismiss_popup(page)
                    if _is_generate_enabled(page):
                        print(f"{prefix}✓ Generate button enabled after {wait_sec+1}s", flush=True)
                        button_ready = True
                        break
                if not button_ready:
                    if attempt < max_retries - 1:
                        time.sleep(3)
                        continue
                    else:
                        raise Exception("Generate button disabled after 60s")
            
            # Human-like pre-generate behavior
            human_mouse_move(page)
            human_delay(1, 2)
            scroll_randomly(page)
            human_delay(0.5, 1)
            
            # Click Generate
            human_click_element(page, arrow_btn, "", timeout=30000)
            print(f"{prefix}✓ Clicked Generate", flush=True)
            time.sleep(1)
            return True
            
        except Exception as e:
            print(f"{prefix}⚠ Generate click failed (attempt {attempt+1}/{max_retries}): {str(e)[:100]}", flush=True)
            if attempt < max_retries - 1:
                check_and_dismiss_popup(page)
                time.sleep(2)
    
    raise Exception(f"Generate button click failed after {max_retries} attempts")


def _is_generate_enabled(page):
    """Check if the Generate (arrow_forward) button is clickable."""
    try:
        btn = page.locator("button:has(i:text('arrow_forward'))").first
        if btn.count() == 0:
            return False
        # Check for disabled attribute or aria-disabled
        disabled = btn.get_attribute("disabled")
        if disabled is not None:
            return False
        aria_disabled = btn.get_attribute("aria-disabled")
        if aria_disabled == "true":
            return False
        # Check if visually enabled (opacity, pointer-events, etc.)
        return btn.is_enabled()
    except:
        return False


def wait_for_image_result(page, timeout=120, context="", baseline_urls=None,
                          get_captured=None):
    """
    Wait for the generated image to appear.

    Detection priority:
      1) `get_captured()` — network listener has already seen the
         `batchGenerateImages` response. Fastest, fires the moment Flow
         returns the JSON (avg ~24s after click). No DOM dependency.
      2) Virtuoso DOM scrape (legacy fallback) — `snapshot_generated_image_urls`
         compared against `baseline_urls`.

    While generating:
      - A tile shows an 'image' icon + percentage text (e.g. "23%", "77%")
      - Percentage is in div.sc-55ebc859-7

    On failure:
      - 'warning' icon appears
      - "Failed" text in div.sc-25d34a31-1

    On success:
      - `batchGenerateImages` JSON response captured (preferred), OR
      - Image appears as <img src="/fx/api/trpc/media.getMediaUrlRedirect?name=UUID">
        with the toolbar mounted (legacy DOM path).

    Args:
        baseline_urls: set of pre-Generate URLs (DOM fallback only).
        get_captured: optional callable returning list[str] of fifeUrls
                      seen by `attach_image_url_listener`.

    Returns:
        True if image generated successfully, False if failed/timeout
    """
    prefix = f"[{context}] " if context else ""
    print(f"{prefix}⏳ Waiting for image generation...", flush=True)

    if baseline_urls is None:
        baseline_urls = set()

    start_time = time.time()
    last_progress = ""
    FAILURE_GRACE_SECONDS = 10  # Don't trust "Failed" detection during warmup
    POLL_INTERVAL = 0.5         # was 2.0 — listener-driven detection wants fast wakeups
    DOM_POLL_EVERY = 4          # poll DOM only every Nth iteration (every 2s wall-clock)
    iteration = 0

    while time.time() - start_time < timeout:
        elapsed = int(time.time() - start_time)
        iteration += 1

        # Check for SUCCESS — listener first (fast path).
        if get_captured is not None:
            try:
                cap = get_captured()
                if cap:
                    print(f"{prefix}✓ Image generated! ({elapsed}s, {len(cap)} URL(s) via network)", flush=True)
                    return True
            except Exception:
                pass

        # DOM fallback — only every DOM_POLL_EVERY ticks (cheap polling
        # for the listener, expensive eval for DOM).
        if iteration % DOM_POLL_EVERY == 0:
            try:
                current_urls = snapshot_generated_image_urls(page)
                new_urls = current_urls - baseline_urls
                if new_urls:
                    print(f"{prefix}✓ Image generated! ({elapsed}s, {len(new_urls)} new tile(s) via DOM)", flush=True)
                    return True
            except:
                pass
        
        # Check for FAILURE — but only *after* a grace period, and only when
        # the Failed badge is visibly attached to a generation tile. Gate
        # this on the DOM-poll interval too (each is_visible call is ~300ms).
        if elapsed >= FAILURE_GRACE_SECONDS and iteration % DOM_POLL_EVERY == 0:
            try:
                failed_badge = page.locator("div.sc-25d34a31-1:has-text('Failed'), div:has-text('Generation failed'):has(i:text('warning'))").first
                if failed_badge.count() > 0:
                    try:
                        if failed_badge.is_visible(timeout=300):
                            print(f"{prefix}❌ Generation failed (Flow showed Failed badge)", flush=True)
                            return False
                    except:
                        pass
            except:
                pass

        # Check PROGRESS — percentage indicator (also DOM-gated)
        if iteration % DOM_POLL_EVERY == 0:
            try:
                progress_el = page.locator("div.sc-55ebc859-7, div.kAxcVK").first
                if progress_el.count() > 0 and progress_el.is_visible(timeout=300):
                    progress_text = progress_el.inner_text().strip()
                    if progress_text and progress_text != last_progress:
                        print(f"{prefix}  Generating: {progress_text} ({elapsed}s)", flush=True)
                        last_progress = progress_text
            except:
                pass

        # Periodic status log
        if elapsed % 15 == 0 and elapsed > 0 and not last_progress:
            print(f"{prefix}  Still waiting... ({elapsed}s)", flush=True)

        time.sleep(POLL_INTERVAL)

    print(f"{prefix}⚠ Timeout after {timeout}s", flush=True)
    return False


def download_generated_image(page, save_path, context=""):
    """
    Download the generated image from Flow UI to a local path.
    
    Two approaches (tries in order):
      A) Click the 'download' button in the toolbar → browser download
      B) Extract <img src="/fx/api/trpc/media.getMediaUrlRedirect?name=UUID">
         and download via HTTP
    
    The toolbar has: download | undo (Reuse) | delete
    Download button: button:has(i:text('download'))
    
    Args:
        page: Playwright page
        save_path: local file path to save the image
    
    Returns:
        True if download succeeded, False otherwise
    """
    prefix = f"[{context}] " if context else ""
    
    # Ensure output directory exists
    out_dir = os.path.dirname(save_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    
    # === Approach A: Click the download button ===
    try:
        download_btn = page.locator("button:has(i:text('download'))").first
        if download_btn.count() > 0 and download_btn.is_visible(timeout=3000):
            print(f"{prefix}Clicking download button...", flush=True)
            
            with page.expect_download(timeout=30000) as download_info:
                human_click_element(page, download_btn, f"{prefix}Download button")
            
            download = download_info.value
            download.save_as(save_path)
            
            if os.path.exists(save_path) and os.path.getsize(save_path) > 0:
                size_kb = os.path.getsize(save_path) / 1024
                print(f"{prefix}✓ Downloaded via button: {save_path} ({size_kb:.0f} KB)", flush=True)
                return True
            else:
                print(f"{prefix}⚠ Download button produced empty file, trying fallback...", flush=True)
    except Exception as e:
        print(f"{prefix}⚠ Download button failed: {e}, trying img src fallback...", flush=True)
    
    # === Approach B: Extract img src and download via HTTP ===
    try:
        import requests as _requests
    except ImportError:
        import urllib.request
        _requests = None
    
    try:
        # Find the generated image element
        gen_img = page.locator("img[src*='media.getMediaUrlRedirect']").first
        if gen_img.count() == 0:
            print(f"{prefix}❌ No generated image found in DOM", flush=True)
            return False
        
        img_src = gen_img.get_attribute("src")
        if not img_src:
            print(f"{prefix}❌ Image element has no src", flush=True)
            return False
        
        # Build full URL (src may be relative)
        if img_src.startswith("/"):
            base_url = page.evaluate("window.location.origin")
            img_url = base_url + img_src
        else:
            img_url = img_src
        
        print(f"{prefix}Downloading from: {img_url[:100]}...", flush=True)
        
        # Get cookies from browser for authenticated download
        cookies = page.context.cookies()
        cookie_header = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
        
        if _requests:
            headers = {
                "Cookie": cookie_header,
                "User-Agent": page.evaluate("navigator.userAgent"),
                "Referer": page.url,
            }
            resp = _requests.get(img_url, headers=headers, timeout=30, stream=True)
            resp.raise_for_status()
            
            with open(save_path, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
        else:
            # urllib fallback
            req = urllib.request.Request(img_url, headers={
                "Cookie": cookie_header,
                "User-Agent": page.evaluate("navigator.userAgent"),
            })
            with urllib.request.urlopen(req, timeout=30) as resp:
                with open(save_path, 'wb') as f:
                    f.write(resp.read())
        
        if os.path.exists(save_path) and os.path.getsize(save_path) > 0:
            size_kb = os.path.getsize(save_path) / 1024
            print(f"{prefix}✓ Downloaded via HTTP: {save_path} ({size_kb:.0f} KB)", flush=True)
            return True
        else:
            print(f"{prefix}❌ Downloaded file is empty", flush=True)
            return False
    
    except Exception as e:
        print(f"{prefix}❌ Image download failed: {e}", flush=True)
        return False


# ============================================================
# CORE JOB PROCESSING
# ============================================================

def process_image_job(page, input_paths, prompt, output_path,
                      aspect_ratio="16:9", resolution="1K",
                      model="nano_banana_2", reuse_project=False):
    """
    Process a single image job: local paths in → Flow UI → local path out.
    
    Args:
        page:          Playwright page (browser tab)
        input_paths:   list of local image paths (empty for text-to-image)
        prompt:        generation/edit prompt
        output_path:   where to save the result
        aspect_ratio:  "16:9", "9:16", "1:1", etc.
        resolution:    "512", "1K", "2K", "4K"
        model:         "nano_banana_2" or "nano_banana_pro"
        reuse_project: if True, don't create a new project (reuse current)
    
    Returns:
        True if successful, False otherwise
    """
    
    job_type = "edit" if input_paths else "generate"
    
    print(f"\n{'='*60}")
    print(f"IMAGE JOB [{job_type.upper()}]")
    print(f"  Prompt: {prompt[:80]}{'...' if len(prompt) > 80 else ''}")
    print(f"  Input:  {[os.path.basename(p) for p in input_paths] if input_paths else 'None (text-to-image)'}")
    print(f"  Output: {output_path}")
    print(f"  Settings: {aspect_ratio} | {resolution} | {model}")
    print(f"{'='*60}\n")
    
    # Validate inputs
    for p in input_paths:
        if not os.path.exists(p):
            print(f"❌ Input file not found: {p}")
            return False
    
    # Ensure output directory exists
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    
    try:
        # --- Step 1: Navigate to Flow → create project ---
        if not reuse_project:
            spa_navigate_to_flow_home(page, "IMAGE")
            human_delay(2, 4)
            ensure_logged_into_flow(page, "IMAGE")
            check_and_dismiss_popup(page)
            
            # Click "New project"
            human_mouse_move(page)
            human_delay(1, 2)
            scroll_randomly(page)
            human_delay(0.5, 1)
            
            dismiss_create_with_flow(page, "IMAGE")
            human_click_element(page,
                "button:has-text('New project'), "
                "button:has-text('Nuovo progetto'), "
                "button:has-text('Nuevo proyecto'), "
                "button:has-text('Nouveau projet'), "
                "button:has-text('Neues Projekt'), "
                "button:has(i:text('add_2'))",
                "New project")
            human_delay(2, 3)
            
            try:
                page.wait_for_url("**/project/**", timeout=30000)
            except:
                for _ in range(15):
                    time.sleep(1)
                    if "/project/" in page.url:
                        break
            
            project_url = page.url
            if "/project/" not in project_url:
                print(f"❌ Failed to create project — URL: {project_url}")
                return False
            print(f"✓ Created project: {project_url}")
            human_delay(1, 2)
            check_and_dismiss_popup(page)
        
        # --- Step 2: Select image mode ---
        select_image_mode(page, "IMAGE")
        human_delay(1, 2)
        
        # --- Step 3: Configure settings ---
        configure_image_settings(page, aspect_ratio, resolution, model, variants=1)
        human_delay(1, 2)

        # --- Step 3.5: optional flow_api (private-API) path ---
        # FLOW_API_MODE=on routes generation through the in-page private API.
        # On any failure, falls through to the DOM path below (Steps 4-8).
        if _flow_api_image_try(page, input_paths, prompt, aspect_ratio, model, output_path):
            return True

        # --- Step 4: Upload input images (if any) ---
        if input_paths:
            upload_reference_images(page, input_paths)
            human_delay(1, 2)
            # v703 — worker-injected reference manifest (see helper docstrings)
            prompt = _build_reference_manifest(input_paths) + _strip_stale_reference_lines(prompt)

        # --- Step 5: Enter prompt ---
        human_mouse_move(page)
        scroll_randomly(page)
        fill_prompt_textarea(page, prompt)
        print(f"✓ Entered prompt: {prompt[:60]}...")
        human_delay(5, 8)
        
        # --- Step 6: Click Generate ---
        click_generate_image(page, "IMAGE")
        print("✓ Clicked Generate")
        
        # --- Step 7: Wait for result ---
        success = wait_for_image_result(page, timeout=120)
        if not success:
            print("❌ Generation timed out or failed")
            return False
        
        # --- Step 8: Download to local path ---
        downloaded = download_generated_image(page, output_path)
        if not downloaded:
            print("❌ Download failed")
            return False
        
        print(f"✓ Image saved to: {output_path}")
        return True
        
    except NotImplementedError as e:
        print(f"\n❌ {e}")
        print("Run with --interactive to manually walk through the UI first.")
        return False
    except Exception as e:
        print(f"❌ Job failed: {e}")
        import traceback
        traceback.print_exc()
        return False


# ============================================================
# BATCH PROCESSING
# ============================================================

def process_batch(page, input_dir, prompt, output_dir, **kwargs):
    """Process all images in a directory."""
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    image_extensions = {'.png', '.jpg', '.jpeg', '.webp', '.bmp'}
    input_files = sorted([
        f for f in input_dir.iterdir()
        if f.suffix.lower() in image_extensions
    ])
    
    if not input_files:
        print(f"❌ No images found in {input_dir}")
        return
    
    print(f"\n{'='*60}")
    print(f"BATCH MODE: {len(input_files)} images")
    print(f"  Input:  {input_dir}")
    print(f"  Output: {output_dir}")
    print(f"  Prompt: {prompt[:60]}...")
    print(f"{'='*60}\n")
    
    succeeded = 0
    failed = 0
    
    for i, input_file in enumerate(input_files):
        output_file = output_dir / f"{input_file.stem}_out{input_file.suffix}"
        print(f"\n--- Image {i+1}/{len(input_files)}: {input_file.name} ---")
        
        reuse = (i > 0)  # Reuse project after first image
        ok = process_image_job(
            page,
            input_paths=[str(input_file)],
            prompt=prompt,
            output_path=str(output_file),
            reuse_project=reuse,
            **kwargs
        )
        
        if ok:
            succeeded += 1
        else:
            failed += 1
    
    print(f"\n{'='*60}")
    print(f"BATCH COMPLETE: {succeeded} succeeded, {failed} failed")
    print(f"{'='*60}")


# ============================================================
# INTERACTIVE MODE
# ============================================================

def interactive_mode(page):
    """Keep browser open and process jobs interactively from stdin."""
    print("\n" + "="*60)
    print("INTERACTIVE MODE")
    print("="*60)
    print("Browser is open and logged into Flow.")
    print("Enter commands:")
    print("  generate <prompt> --output <path>")
    print("  edit <input_path> <prompt> --output <path>")
    print("  quit")
    print("="*60 + "\n")
    
    while True:
        try:
            cmd = input("\n[image_worker] > ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting...")
            break
        
        if not cmd:
            continue
        if cmd.lower() in ('quit', 'exit', 'q'):
            break
        
        # Simple command parsing
        parts = cmd.split()
        if parts[0] == 'generate':
            # generate <prompt text> --output <path>
            if '--output' in parts:
                out_idx = parts.index('--output')
                output_path = parts[out_idx + 1] if out_idx + 1 < len(parts) else "output.png"
                prompt = ' '.join(parts[1:out_idx])
            else:
                prompt = ' '.join(parts[1:])
                output_path = "output.png"
            
            process_image_job(page, [], prompt, output_path)
        
        elif parts[0] == 'edit':
            # edit <input_path> <prompt text> --output <path>
            if len(parts) < 3:
                print("Usage: edit <input_path> <prompt> --output <path>")
                continue
            input_path = parts[1]
            if '--output' in parts:
                out_idx = parts.index('--output')
                output_path = parts[out_idx + 1] if out_idx + 1 < len(parts) else "output.png"
                prompt = ' '.join(parts[2:out_idx])
            else:
                prompt = ' '.join(parts[2:])
                output_path = "output.png"
            
            process_image_job(page, [input_path], prompt, output_path)
        
        else:
            print(f"Unknown command: {parts[0]}")
            print("Commands: generate, edit, quit")


# ============================================================
# MULTI-VARIANT DOWNLOAD
# ============================================================

def _flow_api_capture_enabled():
    """Default ON — continuous read-only network capture (stdout + JSONL).
    Kill-switch: FLOW_API_CAPTURE=off (or 0/false/no)."""
    return os.environ.get("FLOW_API_CAPTURE", "on").strip().lower() not in ("off", "0", "false", "no")


def _flow_api_capture_path():
    return os.environ.get(
        "FLOW_API_CAPTURE_PATH",
        os.path.join(tempfile.gettempdir(), "flow_api_capture.jsonl"),
    )


# ============================================================
# FLOW_API INLINE (self-contained — worker is downloaded as a single file)
# ============================================================
# Mirrors flow_api/ in the repo, inlined here so the standalone-file worker
# doesn't need the flow_api/ package alongside. Source-of-truth lives at
# code/flow_api/; this block is its compiled-in copy. Keep in sync when the
# private API shape changes (HAR-confirmed 2026-05-28 for image side).

_FA_GOOGLE_FLOW_API = "https://aisandbox-pa.googleapis.com"
_FA_GOOGLE_API_KEY = os.environ.get("FLOW_API_KEY", "AIzaSyBtrm0o5ab1c-Ec8ZuLcGt3oJAA5VWt3pY")
_FA_RECAPTCHA_SITE_KEY = os.environ.get("FLOW_RECAPTCHA_SITE_KEY", "6LdsFiUsAAAAAIjVDZcuLhaHiDn5nnHVXVRQGeMV")
_FA_ENDPOINTS = {
    "generate_images": "/v1/projects/{project_id}/flowMedia:batchGenerateImages",
    "upload_image": "/v1/flow/uploadImage",
    "get_media": "/v1/media/{media_id}",
}
_FA_CAPTCHA_IMAGE = "IMAGE_GENERATION"
_FA_CAPTCHA_VIDEO = "VIDEO_GENERATION"
_FA_DEFAULT_IMAGE_ASPECT = "IMAGE_ASPECT_RATIO_PORTRAIT"
_FA_API_COOLDOWN = int(os.environ.get("FLOW_API_COOLDOWN", "10"))
_FA_CAPTCHA_MAX_RETRIES = int(os.environ.get("FLOW_API_CAPTCHA_RETRIES", "10"))
_FA_IMAGE_MODELS = {
    "Nano Banana 2": "NARWHAL",
    "Nano Banana Pro": "GEM_PIX_2",
}
_FA_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)
_FA_UUID_IN_URL_RE = re.compile(r"/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})", re.I)


class _FaError(Exception):
    pass


def _fa_is_uuid(value):
    return bool(value) and bool(_FA_UUID_RE.match(value))


def _fa_uuid_from_url(url):
    m = _FA_UUID_IN_URL_RE.search(url or "")
    return m.group(1) if m else ""


def _fa_is_error(result):
    if not isinstance(result, dict):
        return True
    if result.get("error"):
        return True
    status = result.get("status")
    if isinstance(status, int) and status >= 400:
        return True
    data = result.get("data")
    if isinstance(data, dict) and data.get("error"):
        return True
    return False


def _fa_error_reason(result):
    if not isinstance(result, dict):
        return "non-dict result"
    if result.get("error"):
        return str(result["error"])
    data = result.get("data")
    if isinstance(data, dict) and data.get("error"):
        err = data["error"]
        if isinstance(err, dict):
            parts = []
            msg = err.get("message")
            if msg:
                parts.append(str(msg))
            st = err.get("status")
            if st:
                parts.append(f"status={st}")
            code = err.get("code")
            if code is not None:
                parts.append(f"code={code}")
            # surface field-level details (Google APIs nest these in details[])
            for d in (err.get("details") or [])[:3]:
                if isinstance(d, dict):
                    parts.append(json.dumps(d)[:300])
            return " | ".join(parts) or str(err)[:300]
        return str(err)[:300]
    status = result.get("status")
    if isinstance(status, int) and status >= 400:
        return f"HTTP {status}: {str(result.get('text') or '')[:300]}"
    return ""


def _fa_extract_image_media_id(result):
    data = result.get("data", result) if isinstance(result, dict) else {}
    if not isinstance(data, dict):
        return ""
    media = data.get("media") or []
    if not media or not isinstance(media[0], dict):
        return ""
    item = media[0]
    name = item.get("name", "")
    if _fa_is_uuid(name):
        return name
    gen = (item.get("image") or {}).get("generatedImage") or {}
    val = gen.get("mediaId", "")
    if _fa_is_uuid(val):
        return val
    for f in ("fifeUrl", "imageUri"):
        got = _fa_uuid_from_url(gen.get(f, ""))
        if got:
            return got
    return ""


def _fa_build_url(endpoint_key, **fmt):
    path = _FA_ENDPOINTS[endpoint_key].format(**fmt)
    sep = "&" if "?" in path else "?"
    return f"{_FA_GOOGLE_FLOW_API}{path}{sep}key={_FA_GOOGLE_API_KEY}"


def _fa_client_context(project_id, tier=None):
    # HAR-confirmed 2026-05-28: this account's batchGenerateImages calls send
    # userPaygateTier=null. Default None (-> JSON null). FlowKit's TIER_TWO default
    # caused "Request contains an invalid argument" on accounts where the tier
    # doesn't match — Flow validates the tier server-side.
    cc = {
        "projectId": str(project_id or ""),
        "recaptchaContext": {"applicationType": "RECAPTCHA_APPLICATION_TYPE_WEB", "token": ""},
        "sessionId": f";{int(time.time() * 1000)}",
        "tool": "PINHOLE",
        "userPaygateTier": tier,
    }
    return cc


def _fa_build_upload_image(image_b64, project_id="", file_name="image.jpg", mime="image/jpeg"):
    return {
        "clientContext": {"projectId": str(project_id or ""), "tool": "PINHOLE"},
        "fileName": file_name,
        "imageBytes": image_b64,
        "isHidden": False,
        "isUserUploaded": True,
        "mimeType": mime,
    }


def _fa_build_generate_image(prompt, project_id, image_model_name, aspect=None,
                             seed=None, reference_media_ids=None, base_image_media_id="",
                             tier="PAYGATE_TIER_TWO"):
    aspect = aspect or _FA_DEFAULT_IMAGE_ASPECT
    seed_val = seed if seed is not None else (int(time.time() * 1000) % 1000000)
    request_item = {
        "imageAspectRatio": aspect,
        "imageModelName": image_model_name,
        "seed": seed_val,
        "structuredPrompt": {"parts": [{"text": prompt}]},
    }
    image_inputs = []
    if base_image_media_id:
        image_inputs.append({"name": base_image_media_id, "imageInputType": "IMAGE_INPUT_TYPE_BASE_IMAGE"})
    if reference_media_ids:
        for mid in reference_media_ids:
            image_inputs.append({"name": mid, "imageInputType": "IMAGE_INPUT_TYPE_REFERENCE"})
    if image_inputs:
        request_item["imageInputs"] = image_inputs
    body = {
        "clientContext": _fa_client_context(project_id, tier),
        "requests": [request_item],
    }
    if image_inputs:
        import uuid as _uuid
        body["mediaGenerationContext"] = {"batchId": f"{_uuid.uuid4()}"}
        body["useNewMedia"] = True
    return body


def _fa_inject_captcha_token(body, token):
    cc = body.get("clientContext")
    if isinstance(cc, dict) and isinstance(cc.get("recaptchaContext"), dict):
        cc["recaptchaContext"]["token"] = token
    for req in body.get("requests", []) or []:
        rcc = req.get("clientContext") if isinstance(req, dict) else None
        if isinstance(rcc, dict) and isinstance(rcc.get("recaptchaContext"), dict):
            rcc["recaptchaContext"]["token"] = token
    return body


# ─── In-page primitives (sync Patchright) ────────────────
class _FaTokenStore:
    def __init__(self):
        self.token = ""
        self.captured_at = 0.0

    def set(self, t):
        self.token = t
        self.captured_at = time.time()

    @property
    def age_s(self):
        return time.time() - self.captured_at if self.captured_at else 1e9


# Module-level singleton — the listener attached at worker startup writes here;
# every _FaClient reads from the same store. Critical: listener attaches BEFORE
# the page makes its first authenticated request to Flow (which happens during
# the initial "Navigating to Flow" step), so we don't miss the token by attaching
# too late.
_FA_TOKEN_STORE = _FaTokenStore()


def _fa_attach_global_token_listener(page):
    """Attach the request-sniff listener once per page, bound to the GLOBAL store.
    Idempotent. Called at worker startup (right after Browser launched)."""
    if page is None:
        return _FA_TOKEN_STORE
    try:
        if getattr(page, "_fa_token_listener_installed", False):
            return _FA_TOKEN_STORE
    except Exception:
        return _FA_TOKEN_STORE

    def _on_request(req):
        try:
            auth = (req.headers or {}).get("authorization", "")
            if auth.startswith("Bearer ya29."):
                tok = auth[len("Bearer "):].strip()
                if tok and tok != _FA_TOKEN_STORE.token:
                    _FA_TOKEN_STORE.set(tok)
                    # Inject into page for operator-side console debugging.
                    # Operator's console snippets can read window.__faSniff.bearer
                    # directly — no manual copy from Network tab needed.
                    try:
                        page.evaluate(
                            "(t) => { window.__faSniff = window.__faSniff || {}; window.__faSniff.bearer = t; }",
                            tok,
                        )
                    except Exception:
                        pass
        except Exception:
            pass

    try:
        page.on("request", _on_request)
        try:
            page._fa_token_listener_installed = True
        except Exception:
            pass
        print("[flow_api] global token-capture listener attached", flush=True)
    except Exception as e:
        print(f"[flow_api] failed to attach global token listener: {e}", flush=True)
    return _FA_TOKEN_STORE


def _fa_install_token_capture(page):
    """Compatibility shim: returns the shared global store. Ensures the listener
    is attached if it wasn't already."""
    return _fa_attach_global_token_listener(page)


def _fa_wait_for_token(store, timeout=30.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if store.token and store.age_s < 3000:
            return store.token
        time.sleep(0.5)
    return store.token


_FA_CAPTCHA_JS = """
async ([siteKey, action]) => {
  function waitG(t) {
    return new Promise((res, rej) => {
      const s = Date.now();
      const c = () => {
        if (window.grecaptcha && window.grecaptcha.enterprise && window.grecaptcha.enterprise.execute) return res();
        if (Date.now() - s > t) return rej(new Error('grecaptcha not available'));
        setTimeout(c, 200);
      };
      c();
    });
  }
  await waitG(10000);
  return await window.grecaptcha.enterprise.execute(siteKey, { action });
}
"""

_FA_FETCH_JS = """
async ([url, method, headers, bodyStr]) => {
  const opts = { method, headers, credentials: 'include' };
  if (bodyStr !== null) opts.body = bodyStr;
  let status = 0, ok = false, text = '';
  try {
    const r = await fetch(url, opts);
    status = r.status; ok = r.ok;
    text = await r.text();
  } catch (e) {
    return { status: 0, ok: false, data: null, text: 'fetch failed: ' + (e && e.message) };
  }
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch (e) { data = null; }
  return { status, ok, data, text: data ? '' : (text || '').slice(0, 2000) };
}
"""


def _fa_mint_captcha(page, action):
    return page.evaluate(_FA_CAPTCHA_JS, [_FA_RECAPTCHA_SITE_KEY, action])


def _fa_api_fetch(page, url, method, token, body_obj=None, extra_headers=None):
    headers = {"authorization": f"Bearer {token}"}
    if body_obj is not None:
        headers["content-type"] = "application/json"
    if extra_headers:
        headers.update(extra_headers)
    body_str = json.dumps(body_obj) if body_obj is not None else None
    try:
        return page.evaluate(_FA_FETCH_JS, [url, method, headers, body_str])
    except Exception as e:
        return {"status": 0, "ok": False, "data": None, "text": f"evaluate failed: {e}"}


def _fa_mint_or_empty(page, action):
    try:
        return _fa_mint_captcha(page, action)
    except Exception:
        return ""


# ─── Sync client (minimal: upload + image submit) ────────
class _FaClient:
    def __init__(self, page, project_id="", tier=None):
        self.page = page
        self.project_id = project_id
        self.tier = tier
        self._token_store = _fa_install_token_capture(page)
        self._last_call = 0.0
        # v832 — per-node latency instrumentation. Accumulates seconds spent in
        # each flow_api phase (cooldown / reCAPTCHA mint / fetch) so the pull path
        # can print one [timing] line per node and show where the wait goes.
        # v833 — also bucket each submit outcome (ok / recaptcha / unusual / 5xx /
        # mint_fail / other) so a slow node shows WHY it was slow (e.g. reCAPTCHA
        # retries burning ~12s each), not just how long.
        self._t = self._zero_timings()

    @staticmethod
    def _zero_timings():
        return {"cooldown": 0.0, "mint": 0.0, "mint_n": 0, "fetch": 0.0, "fetch_n": 0,
                "outcomes": {}}

    def reset_timings(self):
        self._t = self._zero_timings()

    def _bump_outcome(self, key):
        self._t["outcomes"][key] = self._t["outcomes"].get(key, 0) + 1

    def timings_summary(self):
        t = self._t
        _oc = " ".join(f"{k}={v}" for k, v in sorted(t["outcomes"].items())) or "none"
        return (f"cooldown={t['cooldown']:.1f}s mint={t['mint']:.1f}s({t['mint_n']}x) "
                f"fetch={t['fetch']:.1f}s({t['fetch_n']}x: {_oc})")

    def _cooldown(self):
        elapsed = time.monotonic() - self._last_call
        if elapsed < _FA_API_COOLDOWN:
            _slept = _FA_API_COOLDOWN - elapsed
            time.sleep(_slept)
            self._t["cooldown"] += _slept   # v832 timing
        self._last_call = time.monotonic()

    def _token(self):
        tok = _fa_wait_for_token(self._token_store, timeout=30)
        if not tok:
            raise _FaError("no bearer token captured (open/refresh a logged-in Flow tab)")
        return tok

    def upload_image(self, image_bytes, file_name="ref.jpg", mime_type="image/jpeg"):
        import base64
        self._cooldown()
        b64 = base64.b64encode(image_bytes).decode("ascii")
        body = _fa_build_upload_image(b64, self.project_id, file_name, mime_type)
        url = _fa_build_url("upload_image")
        res = _fa_api_fetch(self.page, url, "POST", self._token(), body)
        if _fa_is_error(res):
            raise _FaError(f"uploadImage failed: {_fa_error_reason(res)}")
        data = res.get("data") or {}
        media_id = (data.get("media") or {}).get("name", "")
        if not _fa_is_uuid(media_id):
            raise _FaError(f"uploadImage returned non-UUID: {media_id[:40]}")
        return media_id

    def submit_image(self, prompt, image_model_name, reference_media_ids=None,
                     base_image_media_id="", aspect=None, seed=None, cooldown=True):
        if not image_model_name:
            raise _FaError("no imageModelName")
        body = _fa_build_generate_image(
            prompt=prompt, project_id=self.project_id, image_model_name=image_model_name,
            aspect=aspect, seed=seed, reference_media_ids=reference_media_ids,
            base_image_media_id=base_image_media_id, tier=self.tier,
        )
        url = _fa_build_url("generate_images", project_id=self.project_id)
        last = {}
        for attempt in range(_FA_CAPTCHA_MAX_RETRIES):
            if cooldown:
                self._cooldown()
            _m0 = time.monotonic()                                   # v832 timing
            token = _fa_mint_or_empty(self.page, _FA_CAPTCHA_IMAGE)
            self._t["mint"] += time.monotonic() - _m0
            self._t["mint_n"] += 1
            if not token:
                last = {"error": "captcha mint failed"}
                self._bump_outcome("mint_fail")   # v833
                continue
            _fa_inject_captcha_token(body, token)
            _f0 = time.monotonic()                                   # v832 timing
            res = _fa_api_fetch(self.page, url, "POST", self._token(), body)
            self._t["fetch"] += time.monotonic() - _f0
            self._t["fetch_n"] += 1
            if not _fa_is_error(res):
                self._bump_outcome("ok")          # v833
                media_id = _fa_extract_image_media_id(res)
                if not media_id:
                    raise _FaError(f"submit no media_id: {_fa_error_reason(res) or (res.get('text','') or '')[:200]}")
                gen = ((res.get("data") or {}).get("media", [{}])[0].get("image") or {}).get("generatedImage") or {}
                return media_id, gen.get("fifeUrl", gen.get("imageUri", ""))
            reason = _fa_error_reason(res).lower()
            last = res
            if "captcha" in reason or "recaptcha" in reason:
                self._bump_outcome("recaptcha")   # v833
                continue
            # v833 — non-captcha error: bucket it so the timing line shows the mix
            if "permission_denied" in reason or " 403" in reason or "code=403" in reason or "unusual" in reason:
                self._bump_outcome("unusual")
            elif any(s in reason for s in ("500", "502", "503", "504", "internal", "unavailable", "deadline")):
                self._bump_outcome("5xx")
            else:
                self._bump_outcome("other")
            break
        raise _FaError(f"submit_image failed: {_fa_error_reason(last) or 'unknown'}")


def _fa_resolve_image_model_name(label):
    return _FA_IMAGE_MODELS.get(label, "")


# ─── tRPC fetch (labs.google host, session-cookie auth, no Bearer) ───
_FA_TRPC_FETCH_JS = """
async ([url, method, bodyStr]) => {
  const opts = { method, headers: {'content-type': 'application/json', 'accept': '*/*'}, credentials: 'include' };
  if (bodyStr !== null) opts.body = bodyStr;
  let status = 0, ok = false, text = '';
  try {
    const r = await fetch(url, opts);
    status = r.status; ok = r.ok;
    text = await r.text();
  } catch (e) {
    return { status: 0, ok: false, data: null, text: 'fetch failed: ' + (e && e.message) };
  }
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch (e) { data = null; }
  return { status, ok, data, text: data ? '' : (text || '').slice(0, 2000) };
}
"""


def _fa_trpc_fetch(page, url, method, body_obj=None):
    body_str = json.dumps(body_obj) if body_obj is not None else None
    try:
        return page.evaluate(_FA_TRPC_FETCH_JS, [url, method, body_str])
    except Exception as e:
        return {"status": 0, "ok": False, "data": None, "text": f"evaluate failed: {e}"}


def force_agent_off(page, context=""):
    """Force Flow's AGENT mode OFF for a freshly-opened project. Agent mode
    replaces the editor with a chat panel, so the Settings gear vanishes and
    configure_image_settings fails with "Settings button not found". Ported from
    flow_worker.force_agent_off (v839).

    THE real lever is the USER-LEVEL (account-wide) toggle: the per-project
    agentInfo PATCH alone does NOT override the user setting, so every new project
    kept opening in Agent mode. So: (1) user-level updateUserSettings
    isAgentModeToggled=false, (2) per-project agentInfo PATCH DISABLED for the
    current project (belt-and-suspenders), (3) reload so the SPA re-renders the
    editor. Best-effort; never raises. Returns True if attempted."""
    pfx = f"[{context}] " if context else ""
    TRPC = "https://labs.google/fx/api/trpc"
    AISBX = "https://aisandbox-pa.googleapis.com"
    try:
        print(f"{pfx}[agent-off] forcing Agent OFF (user-level + per-project)", flush=True)
        # 1. USER-LEVEL account-wide toggle — THE real lever.
        try:
            _r = _fa_trpc_fetch(
                page, f"{TRPC}/videoFx.updateUserSettings", "POST",
                {"json": {"isAgentModeToggled": False}},
            )
            _ok = isinstance(_r, dict) and not _fa_is_error(_r)
            print(f"{pfx}[agent-off] user-level isAgentModeToggled=false "
                  f"({'ok' if _ok else 'non-blocking: ' + _fa_error_reason(_r)})", flush=True)
        except Exception as _ue:
            print(f"{pfx}[agent-off] user-level toggle raised (non-blocking): {_ue}", flush=True)
        # 2. PER-PROJECT agentInfo PATCH for the current project.
        try:
            _u = page.url or ""
        except Exception:
            _u = ""
        m = re.search(r'/project/([A-Za-z0-9_\-]+)', _u)
        if m:
            project_id = m.group(1)
            token = _FA_TOKEN_STORE.token or ""
            try:
                _fa_api_fetch(
                    page,
                    f"{AISBX}/v1/projects/{project_id}/agentInfo?updateMask=agent_toggle_state",
                    "PATCH", token,
                    {"agentToggleState": "AGENT_TOGGLE_STATE_DISABLED"},
                )
                print(f"{pfx}[agent-off] per-project agent_toggle_state=DISABLED for {project_id[:8]}", flush=True)
            except Exception as _pe:
                print(f"{pfx}[agent-off] per-project PATCH raised (non-blocking): {_pe}", flush=True)
        # 3. reload so the SPA re-fetches user settings + re-renders the editor.
        try:
            page.reload(wait_until="domcontentloaded", timeout=30000)
            try:
                page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                time.sleep(2)
            print(f"{pfx}[agent-off] reload done — editor should show the Settings gear", flush=True)
        except Exception as _re:
            print(f"{pfx}[agent-off] reload failed (non-blocking): {_re}", flush=True)
        return True
    except Exception as e:
        print(f"{pfx}[agent-off] failed (non-blocking): {e}", flush=True)
        return False


def _fa_try_create_new_project_api(page, context=""):
    """Try to create a Flow project via the private tRPC API + fire best-effort
    init calls. Returns the full project URL on success, None on failure (caller
    falls back to the existing DOM 'New project' click).

    Sequence (HAR-confirmed 2026-05-30):
      1. POST https://labs.google/fx/api/trpc/project.createProject
         body: {"json":{"projectTitle":"<auto>","toolName":"PINHOLE"}}
         → result.data.json.result.projectId
      2. page.goto /project/{pid} so URL-based logic sees the project
      3. Fire 6 best-effort init calls (recommendations, credits, agent state, etc.)
         — each wrapped; failures logged but never block.
    """
    pfx = f"[{context}] " if context else ""
    if not _flow_api_mode_enabled():
        return None
    if _fa_api_on_cooldown(page, pfx):
        return None

    try:
        title = datetime.now().strftime("%b %d, %I:%M %p").lstrip("0")
    except Exception:
        title = "Auto Project"

    # Pre-create telemetry replay (HAR steps 1-4)
    try:
        _fa_replay_har_pre_create(page, context=context)
    except Exception:
        pass

    try:
        res = _fa_trpc_fetch(
            page,
            "https://labs.google/fx/api/trpc/project.createProject",
            "POST",
            {"json": {"projectTitle": title, "toolName": "PINHOLE"}},
        )
    except Exception as e:
        print(f"{pfx}[flow_api] createProject raised: {e} — falling back to DOM click", flush=True)
        return None

    if _fa_is_error(res):
        print(f"{pfx}[flow_api] createProject failed: {_fa_error_reason(res)} — falling back to DOM click", flush=True)
        return None

    pid = ""
    try:
        data = res.get("data") or {}
        pid = (
            data.get("result", {})
                .get("data", {})
                .get("json", {})
                .get("result", {})
                .get("projectId") or ""
        )
    except Exception:
        pid = ""
    if not pid:
        print(f"{pfx}[flow_api] createProject returned no projectId — falling back to DOM click", flush=True)
        return None

    # Navigate via SPA (preserves React state — matches DOM "New project" click).
    # Full page.goto re-mounts React and races against hydration → downstream
    # DOM lookups (settings button, variant counter) miss. SPA-nav stays inside
    # the same React tree, hydration is fast.
    project_url = _fa_spa_navigate_to_project(page, pid, context=context)
    if not project_url:
        # SPA-nav failed — full page.goto fallback.
        project_url = f"https://labs.google/fx/tools/flow/project/{pid}"
        try:
            page.goto(project_url, wait_until="domcontentloaded", timeout=30000)
            try:
                actual = page.url or project_url
                if "/project/" in actual:
                    project_url = actual
            except Exception:
                pass
        except Exception as e:
            print(f"{pfx}[flow_api] navigation to {project_url} failed: {e} — falling back to DOM click", flush=True)
            return None

    # Wait for the project SPA to hydrate. Best-effort, log + continue on timeout.
    try:
        hydration_loc = page.locator(
            "button[aria-haspopup='dialog']:has(i:text('add_2'))"
        ).first
        hydration_loc.wait_for(state="visible", timeout=20000)
    except Exception:
        print(f"{pfx}[flow_api] project page hydration probe timed out (20s) — continuing", flush=True)

    print(f"{pfx}[flow_api] ✓ Created project via API: {project_url}", flush=True)

    # Best-effort init — fire each, log on failure, never block.
    _fa_init_project_best_effort(page, pid, context=context)
    return project_url


def _fa_spa_navigate_to_project(page, pid, context=""):
    """Navigate the SPA to /project/{pid} without a full page reload.
    Mirrors what a DOM "New project" click does — React state persists,
    hydration is fast. Returns the actual project URL on success, None on
    failure (caller falls back to full page.goto).

    Tries Next.js router.push() first, then history.pushState + popstate.
    """
    pfx = f"[{context}] " if context else ""
    target_path = f"/fx/tools/flow/project/{pid}"

    # Approach 1: Next.js router.push (preserves all SPA state).
    try:
        result = page.evaluate(
            """
            (target) => {
              try {
                if (window.next && window.next.router && typeof window.next.router.push === 'function') {
                  window.next.router.push(target);
                  return 'next_router';
                }
              } catch (e) {}
              return null;
            }
            """,
            target_path,
        )
        if result == "next_router":
            for _ in range(20):
                time.sleep(0.5)
                cur = page.url or ""
                if pid in cur:
                    print(f"{pfx}[flow_api] SPA-nav via next.router.push", flush=True)
                    return cur
    except Exception:
        pass

    # Approach 2: history.pushState + popstate.
    try:
        page.evaluate(
            """
            (target) => {
              window.history.pushState({}, '', target);
              window.dispatchEvent(new PopStateEvent('popstate', { state: {} }));
            }
            """,
            target_path,
        )
        for _ in range(8):
            time.sleep(0.5)
            cur = page.url or ""
            if pid in cur:
                print(f"{pfx}[flow_api] SPA-nav via history.pushState", flush=True)
                return cur
    except Exception:
        pass

    return None


_FA_EXPERIMENT_IDS = (
    "106070990,106131447,105993823,106238955,105798603,106225453,106259075,"
    "106184493,106151974,105484652,106210719,106243706,106256669,1706538,"
    "106104244,106262194,106001691,105928947,106077941,106281924,119157485,"
    "105746691,1714253,106210711,106297879,106210380,106210378"
)
_FA_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36"


def _fa_session_id():
    return f";{int(time.time() * 1000)}"


def _fa_now_iso():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(time.time() * 1000) % 1000:03d}Z"


def _fa_url_encode(s):
    from urllib.parse import quote
    return quote(s, safe='')


def _fa_replay_har_pre_create(page, context=""):
    """Fire the 4 pre-createProject calls from HAR (telemetry + migration checks).
    All best-effort, in order."""
    pfx = f"[{context}] " if context else ""
    bearer = _FA_TOKEN_STORE.token or ""
    sess_id = _fa_session_id()
    now_iso = _fa_now_iso()

    def be(label, fn):
        try:
            fn()
        except Exception as e:
            print(f"{pfx}[flow_api] pre '{label}' raised (non-blocking): {e}", flush=True)

    be("fetchMigrationStatus(IMAGE_FX)", lambda: _fa_trpc_fetch(
        page,
        "https://labs.google/fx/api/trpc/general.fetchMigrationStatus?input=" +
        _fa_url_encode('{"json":{"tool":"IMAGE_FX"}}'),
        "GET",
    ))
    be("fetchMigrationStatus(BACKBONE)", lambda: _fa_trpc_fetch(
        page,
        "https://labs.google/fx/api/trpc/general.fetchMigrationStatus?input=" +
        _fa_url_encode('{"json":{"tool":"BACKBONE"}}'),
        "GET",
    ))
    be("batchLogFrontendEvents PINHOLE_CREATE_NEW_PROJECT", lambda: _fa_api_fetch(
        page,
        "https://aisandbox-pa.googleapis.com/v1/flow:batchLogFrontendEvents",
        "POST", bearer,
        {"events": [{
            "eventType": "PINHOLE_CREATE_NEW_PROJECT",
            "metadata": {
                "sessionId": sess_id, "createTime": now_iso,
                "additionalParams": {
                    "TOOL_NAME": {"@type": "type.googleapis.com/google.protobuf.StringValue", "value": "PINHOLE"},
                    "G1_PAYGATE_TIER": {"@type": "type.googleapis.com/google.protobuf.StringValue", "value": "PAYGATE_TIER_TIER1P5"},
                    "PINHOLE_PROMPT_BOX_MODE": {"@type": "type.googleapis.com/google.protobuf.StringValue", "value": "TEXT_TO_IMAGE"},
                    "USER_AGENT": {"@type": "type.googleapis.com/google.protobuf.StringValue", "value": _FA_USER_AGENT},
                    "IS_DESKTOP": {"@type": "type.googleapis.com/google.protobuf.StringValue", "value": "true"},
                },
                "experimentIds": _FA_EXPERIMENT_IDS,
            }
        }]},
    ))
    be("submitBatchLog PINHOLE_CREATE_NEW_PROJECT", lambda: _fa_trpc_fetch(
        page, "https://labs.google/fx/api/trpc/general.submitBatchLog", "POST",
        {"json": {"appEvents": [{
            "event": "PINHOLE_CREATE_NEW_PROJECT",
            "eventMetadata": {"sessionId": sess_id},
            "eventProperties": [
                {"key": "TOOL_NAME", "stringValue": "PINHOLE"},
                {"key": "G1_PAYGATE_TIER", "stringValue": "PAYGATE_TIER_TIER1P5"},
                {"key": "PINHOLE_PROMPT_BOX_MODE", "stringValue": "TEXT_TO_IMAGE"},
                {"key": "USER_AGENT", "stringValue": _FA_USER_AGENT},
                {"key": "IS_DESKTOP", "booleanValue": True},
            ],
            "activeExperiments": [],
            "eventTime": now_iso,
        }]}},
    ))


def _fa_init_project_best_effort(page, project_id, context=""):
    """Full HAR replay — 17 post-createProject calls in EXACT HAR order.
    Every call best-effort; failures logged but never block.
    After PATCHes, page.reload() so React re-fetches state + re-renders UI
    (PATCHes alone update backend but React store stays stale)."""
    pfx = f"[{context}] " if context else ""
    print(f"{pfx}[flow_api] HAR replay START project={project_id[:8]}", flush=True)
    _replay_t0 = time.time()

    def best_effort(label, fn):
        try:
            res = fn()
            if isinstance(res, dict) and _fa_is_error(res):
                print(f"{pfx}[flow_api] init '{label}' non-blocking: {_fa_error_reason(res)}", flush=True)
        except Exception as e:
            print(f"{pfx}[flow_api] init '{label}' raised (non-blocking): {e}", flush=True)

    def _bearer():
        return _FA_TOKEN_STORE.token or ""

    sess_id = _fa_session_id()
    now_iso = _fa_now_iso()
    project_url = f"https://labs.google/fx/tools/flow/project/{project_id}"
    AISBX = "https://aisandbox-pa.googleapis.com"
    TRPC = "https://labs.google/fx/api/trpc"

    best_effort("projectInitialData", lambda: _fa_trpc_fetch(
        page,
        f"{TRPC}/flow.projectInitialData?input=" +
        _fa_url_encode(json.dumps({"json": {"projectId": project_id}})),
        "GET",
    ))
    best_effort("batchLogFrontendEvents PAGE_VIEW", lambda: _fa_api_fetch(
        page, f"{AISBX}/v1/flow:batchLogFrontendEvents", "POST", _bearer(),
        {"events": [{
            "eventType": "PAGE_VIEW",
            "metadata": {
                "sessionId": sess_id, "createTime": now_iso,
                "additionalParams": {
                    "URL": {"@type": "type.googleapis.com/google.protobuf.StringValue", "value": project_url},
                    "USER_AGENT": {"@type": "type.googleapis.com/google.protobuf.StringValue", "value": _FA_USER_AGENT},
                    "IS_DESKTOP": {"@type": "type.googleapis.com/google.protobuf.StringValue", "value": "true"},
                },
                "experimentIds": _FA_EXPERIMENT_IDS,
            }
        }]},
    ))
    best_effort("submitBatchLog PAGE_VIEW", lambda: _fa_trpc_fetch(
        page, f"{TRPC}/general.submitBatchLog", "POST",
        {"json": {"appEvents": [{
            "event": "PAGE_VIEW",
            "eventProperties": [
                {"key": "URL", "stringValue": project_url},
                {"key": "USER_AGENT", "stringValue": _FA_USER_AGENT},
                {"key": "IS_DESKTOP", "booleanValue": True},
            ],
            "activeExperiments": [],
            "eventMetadata": {"sessionId": sess_id},
            "eventTime": now_iso,
        }]}},
    ))
    onramp_body = {"onramp": ["FLOW_UPGRADE_BANNER", "FLOW_UPGRADE_BUTTON",
                              "FLOW_MANAGE_AI_CREDITS", "FLOW_VIDEO_TOOLTIP_UPSELL",
                              "FLOW_MODEL_UPGRADE", "FLOW_MANAGE_MEMBERSHIP"]}
    best_effort("fetchUserRecommendations", lambda: _fa_api_fetch(
        page, f"{AISBX}/v1:fetchUserRecommendations", "POST", _bearer(), onramp_body,
    ))
    best_effort("credits", lambda: _fa_api_fetch(
        page, f"{AISBX}/v1/credits?key={_FA_GOOGLE_API_KEY}", "GET", _bearer(),
    ))
    best_effort("flowCreationAgent.sessions GET", lambda: _fa_api_fetch(
        page, f"{AISBX}/v1/flowCreationAgent/sessions?projectId={project_id}", "GET", _bearer(),
    ))
    best_effort("fetchUserPreferences", lambda: _fa_trpc_fetch(
        page,
        f"{TRPC}/general.fetchUserPreferences?input=" +
        _fa_url_encode('{"json":null,"meta":{"values":["undefined"]}}'),
        "GET",
    ))
    best_effort("videoFx.getUserSettings", lambda: _fa_trpc_fetch(
        page,
        f"{TRPC}/videoFx.getUserSettings?input=" +
        _fa_url_encode('{"json":null,"meta":{"values":["undefined"]}}'),
        "GET",
    ))
    best_effort("agentInfo agent_toggle_state=ENABLED", lambda: _fa_api_fetch(
        page,
        f"{AISBX}/v1/projects/{project_id}/agentInfo?updateMask=agent_toggle_state",
        "PATCH", _bearer(),
        {"agentToggleState": "AGENT_TOGGLE_STATE_ENABLED"},
    ))
    best_effort("fetchUserRecommendations#2", lambda: _fa_api_fetch(
        page, f"{AISBX}/v1:fetchUserRecommendations", "POST", _bearer(), onramp_body,
    ))
    best_effort("fetchUserAcknowledgement", lambda: _fa_trpc_fetch(
        page,
        f"{TRPC}/general.fetchUserAcknowledgement?input=" +
        _fa_url_encode('{"json":{"acknowledgementVersion":"FLOW_IMAGE_UPLOAD_TOS"}}'),
        "GET",
    ))
    best_effort("flowAgent.applets", lambda: _fa_api_fetch(
        page, f"{AISBX}/v1/flowAgent/applets", "GET", _bearer(),
    ))
    best_effort("flowAgent.savedSharedApplets", lambda: _fa_api_fetch(
        page, f"{AISBX}/v1/flowAgent/savedSharedApplets", "GET", _bearer(),
    ))
    best_effort("agentInfo chat_panel_open=true", lambda: _fa_api_fetch(
        page,
        f"{AISBX}/v1/projects/{project_id}/agentInfo?updateMask=chat_panel_open",
        "PATCH", _bearer(),
        {"chatPanelOpen": True},
    ))
    best_effort("flowCreationAgent.sessions POST", lambda: _fa_api_fetch(
        page, f"{AISBX}/v1/flowCreationAgent/sessions", "POST", _bearer(),
        {"projectId": f"projects/{project_id}"},
    ))
    best_effort("flowCreationAgent.sessions GET#2", lambda: _fa_api_fetch(
        page, f"{AISBX}/v1/flowCreationAgent/sessions?projectId={project_id}", "GET", _bearer(),
    ))
    best_effort("agentInfo chat_panel_open=false", lambda: _fa_api_fetch(
        page,
        f"{AISBX}/v1/projects/{project_id}/agentInfo?updateMask=chat_panel_open",
        "PATCH", _bearer(),
        {"chatPanelOpen": False},
    ))
    best_effort("agentInfo agent_toggle_state=DISABLED", lambda: _fa_api_fetch(
        page,
        f"{AISBX}/v1/projects/{project_id}/agentInfo?updateMask=agent_toggle_state",
        "PATCH", _bearer(),
        {"agentToggleState": "AGENT_TOGGLE_STATE_DISABLED"},
    ))
    # v839 — USER-LEVEL (account-wide) Agent toggle. THE real lever: the
    # per-project agentInfo PATCH above does NOT override the user setting, so
    # without this every new project kept opening in Agent mode (Settings gear
    # hidden → "Settings button not found"). Parity with flow_worker.force_agent_off.
    best_effort("videoFx.updateUserSettings isAgentModeToggled=false (user-level)", lambda: _fa_trpc_fetch(
        page, f"{TRPC}/videoFx.updateUserSettings", "POST",
        {"json": {"isAgentModeToggled": False}},
    ))

    elapsed = time.time() - _replay_t0
    print(f"{pfx}[flow_api] HAR replay END project={project_id[:8]} ({elapsed:.1f}s)", flush=True)

    # Reload to force React to re-fetch project state + re-render UI in
    # Agent-OFF layout. Without this, PATCHes update backend but React's
    # local store keeps showing stale Agent-ON layout.
    try:
        page.reload(wait_until="domcontentloaded", timeout=30000)
        print(f"{pfx}[flow_api] page.reload() done — UI should reflect Agent-OFF", flush=True)
        try:
            page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            time.sleep(2)
    except Exception as e:
        print(f"{pfx}[flow_api] page.reload() failed (non-blocking): {e}", flush=True)


# ============================================================
# END FLOW_API INLINE
# ============================================================


_IMG_API_ASPECT_MAP = {
    "9:16": "IMAGE_ASPECT_RATIO_PORTRAIT",
    "16:9": "IMAGE_ASPECT_RATIO_LANDSCAPE",
    "1:1":  "IMAGE_ASPECT_RATIO_SQUARE",
    "4:3":  "IMAGE_ASPECT_RATIO_LANDSCAPE_FOUR_THREE",   # v826 (HAR-confirmed)
    "3:4":  "IMAGE_ASPECT_RATIO_PORTRAIT_THREE_FOUR",    # v826 (HAR-confirmed)
}
_IMG_API_MODEL_MAP = {
    "nano_banana_2":   "Nano Banana 2",
    "nano_banana_pro": "Nano Banana Pro",
}


def _flow_api_mode_enabled():
    """Default ON (automatic). Set FLOW_API_MODE=off to disable, falling back to the
    DOM-click path globally. Any value other than the off-set re-enables it."""
    return os.environ.get("FLOW_API_MODE", "on").strip().lower() not in ("off", "0", "false", "no")


def _fa_api_retry_cooldown_s():
    """How long the API path stays paused after a structural failure before
    it is re-tried. v807.2: the old behavior latched the API off for the
    ENTIRE page session — the worker keeps one page alive for hours, so one
    bad moment (stale token, single 404, captcha hiccup at the wrong time)
    demoted the whole run to the slow DOM path permanently. Default 15 min;
    tune with FLOW_API_RETRY_COOLDOWN_S."""
    try:
        return max(60, int(os.environ.get("FLOW_API_RETRY_COOLDOWN_S", "900")))
    except Exception:
        return 900


def _fa_api_on_cooldown(page, pfx=""):
    """True while the API path is paused. Re-arms automatically when the
    cooldown expires. Migrates the legacy permanent-latch boolean if a
    stale one is set on the page."""
    try:
        until = getattr(page, "_flow_api_disabled_until", 0) or 0
        if getattr(page, "_flow_api_disabled_this_session", False) and not until:
            until = time.time() + _fa_api_retry_cooldown_s()
            page._flow_api_disabled_until = until
            page._flow_api_disabled_this_session = False
        if until:
            if time.time() < until:
                return True
            page._flow_api_disabled_until = 0
            print(f"{pfx}[flow_api] cooldown expired — re-arming API path", flush=True)
    except Exception:
        pass
    return False


def _fa_api_start_cooldown(page, reason, pfx=""):
    """v836 — NEUTRALIZED. Operator: never GLOBALLY pause the API. A failing job
    retries the API up to 3x (in _submit_one_job) then uses the DOM path for THAT
    job only; the next job starts fresh on API. This used to set
    page._flow_api_disabled_until (a shared, per-page pause) which demoted every
    later job to the slow DOM path for ~15 min. Kept as a log-only no-op so all
    call sites (the three _latch_off helpers) stay valid without disabling the API."""
    print(f"{pfx}[flow_api] attempt failed (API stays armed, no global pause): {reason}", flush=True)


def _flow_api_image_try(page, input_paths, prompt, aspect_ratio, model, output_path):
    """Try the flow_api (private-API) path for one image generation.

    Returns True on success (output_path written, caller short-circuits the DOM path).
    Returns False on any failure (caller falls through to the existing DOM steps).
    Never raises into the caller.

    Per-page latch: once the API path falls back on a given page, subsequent calls on
    the SAME page skip the API attempt entirely (saves captcha-mint + upload latency
    when something is broken). Cleared by closing/relaunching the page.
    """
    if not _flow_api_mode_enabled():
        return False
    if _fa_api_on_cooldown(page):
        return False

    def _latch_off(reason: str) -> bool:
        _fa_api_start_cooldown(page, reason)
        return False
    project_id = ""
    try:
        m = re.search(r"/project/([0-9a-fA-F-]{36})", page.url or "")
        project_id = m.group(1) if m else ""
    except Exception:
        project_id = ""
    if not project_id:
        return _latch_off("no projectId in URL")

    api_aspect = _IMG_API_ASPECT_MAP.get(aspect_ratio, "IMAGE_ASPECT_RATIO_PORTRAIT")
    model_label = _IMG_API_MODEL_MAP.get((model or "").lower(), "Nano Banana 2")
    image_model = _fa_resolve_image_model_name(model_label)
    if not image_model:
        return _latch_off(f"no imageModelName for '{model_label}'")

    ref_bytes_list = []
    for p in (input_paths or []):
        try:
            with open(p, "rb") as f:
                ref_bytes_list.append(f.read())
        except Exception as e:
            return _latch_off(f"failed to read input {p}: {e}")

    try:
        cli = _FaClient(page, project_id=project_id)
        ref_ids = []
        for i, b in enumerate(ref_bytes_list):
            ref_ids.append(cli.upload_image(b, file_name=f"ref_{i}.jpg"))
        media_id, url = cli.submit_image(
            prompt=prompt,
            image_model_name=image_model,
            reference_media_ids=ref_ids or None,
            aspect=api_aspect,
        )
        result = {"media_id": media_id, "url": url}
    except Exception as e:
        return _latch_off(f"api path raised: {e}")

    if not url:
        return _latch_off("api success but no URL")

    # Download via the page's HTTP request context (carries cookies).
    try:
        resp = page.request.get(url)
        if resp.status != 200:
            return _latch_off(f"download HTTP {resp.status}")
        with open(output_path, "wb") as f:
            f.write(resp.body())
    except Exception as e:
        return _latch_off(f"download failed ({e})")

    print(f"[flow_api] path=api media_id={result.get('media_id')} model={model_label} -> {output_path}", flush=True)
    return True


def _flow_api_image_multi_try(page, input_paths, prompt, aspect_ratio, model,
                              variants, output_dir, context=""):
    """Multi-variant flow_api path for process_image_job_multi.

    Returns a list of saved filenames on success (caller short-circuits the DOM
    Generate-click + URL capture + download). Returns [] on any failure (caller
    falls through to the DOM path); latches API off for the rest of this page's
    life so the next job skips the API attempt entirely.
    """
    pfx = f"[{context}] " if context else ""

    if not _flow_api_mode_enabled():
        return []
    if _fa_api_on_cooldown(page, pfx):
        return []

    def _latch_off(reason: str):
        _fa_api_start_cooldown(page, reason, pfx)
        return []

    project_id = ""
    try:
        m = re.search(r"/project/([0-9a-fA-F-]{36})", page.url or "")
        project_id = m.group(1) if m else ""
    except Exception:
        project_id = ""
    if not project_id:
        return _latch_off("no projectId in URL")

    api_aspect = _IMG_API_ASPECT_MAP.get(aspect_ratio, "IMAGE_ASPECT_RATIO_PORTRAIT")
    model_label = _IMG_API_MODEL_MAP.get((model or "").lower(), "Nano Banana 2")
    image_model = _fa_resolve_image_model_name(model_label)
    if not image_model:
        return _latch_off(f"no imageModelName for '{model_label}'")

    # v703 manifest is normally prepended after Step 3 (upload). The API path
    # uploads via uploadImage instead and skips that DOM step, so we still want
    # the manifest in the prompt for the model. Build it here.
    try:
        v703_manifest = _build_reference_manifest(input_paths) if input_paths else ""
        api_prompt = (v703_manifest + _strip_stale_reference_lines(prompt)) if input_paths else prompt
    except Exception:
        api_prompt = prompt

    ref_bytes_list = []
    for p in (input_paths or []):
        try:
            with open(p, "rb") as f:
                ref_bytes_list.append(f.read())
        except Exception as e:
            return _latch_off(f"failed to read input {p}: {e}")

    try:
        cli = _FaClient(page, project_id=project_id)
        ref_ids = []
        for i, b in enumerate(ref_bytes_list):
            ref_ids.append(cli.upload_image(b, file_name=f"ref_{i}.jpg"))
        results = []
        for v in range(int(variants or 1)):
            mid, mu = cli.submit_image(
                prompt=api_prompt,
                image_model_name=image_model,
                reference_media_ids=ref_ids or None,
                aspect=api_aspect,
                cooldown=(v == 0),
            )
            results.append({"media_id": mid, "url": mu})
    except Exception as e:
        return _latch_off(f"api path raised: {e}")

    urls = [r.get("url") for r in (results or []) if r.get("url")]
    if not urls:
        return _latch_off("api produced no URLs")

    try:
        saved = download_image_urls(page, urls, output_dir, context=context)
    except Exception as e:
        return _latch_off(f"download_image_urls raised: {e}")
    if not saved:
        return _latch_off("download_image_urls returned empty")

    media_ids = [r.get("media_id") for r in (results or [])]
    print(
        f"{pfx}[flow_api] path=api variants={len(saved)}/{variants} "
        f"model={model_label} media_ids={','.join(media_ids[:4])}",
        flush=True,
    )
    return saved


def _is_unusual(reason: str) -> bool:
    """Account/session-level 'unusual activity' block — the signal the golden
    restore triggers on. Covers Flow's reCAPTCHA rejection,
    PUBLIC_ERROR_UNUSUAL_ACTIVITY / PERMISSION_DENIED 403, AND a persistent
    reCAPTCHA-token MINT failure (a flagged account can't mint a token, so
    submit_image raises 'captcha mint failed'). NOT a per-prompt content issue —
    the DOM path hits the SAME block, so falling through to DOM just wastes a
    full UI generation and never clears the block.

    v828 — widened from the original (UNUSUAL_ACTIVITY only, OR RECAPTCHA *and* a
    403 marker). That AND-ed condition missed two real flag manifestations the
    operator hit, so the API golden-restore trigger never fired:
      (1) reCAPTCHA MINT failure -> 'captcha mint failed' (no RECAPTCHA/UNUSUAL
          token in the string) was misrouted by _is_transient to the DOM
          cookie-clear (which does NOT clear Flow's reCAPTCHA block).
      (2) a bare 403 / PERMISSION_DENIED block without the literal word
          'RECAPTCHA' failed the AND.
    Widening is safe against false positives because the submit retry loop +
    zero-capture gate upstream means only a PERSISTENT block (all variants + all
    retries failed with NOTHING captured) reaches _signal_unusual — a one-off
    transient mint hiccup recovers on retry and captures a URL, so this does not
    cause spurious browser relaunches. Module-level (was nested) so it is unit-
    testable — see tests/test_image_worker_unusual_classifier.py.
    """
    r = (reason or "").upper()
    if "UNUSUAL_ACTIVITY" in r:
        return True
    # 403 / permission block — Flow returns PERMISSION_DENIED code=403 on the
    # reCAPTCHA rejection; the literal word 'RECAPTCHA' is not always present.
    if "PERMISSION_DENIED" in r or " 403" in r or "CODE=403" in r or "HTTP 403" in r:
        return True
    # persistent reCAPTCHA-token mint failure = the account can't pass reCAPTCHA
    if "CAPTCHA MINT FAILED" in r or "MINT FAILED" in r:
        return True
    if "RECAPTCHA" in r:
        return True
    return False


def _flow_api_pull_submit_try(page, node_id, node_name, prompt, input_paths, variants,
                              aspect_ratio, model, ctx,
                              listener_state, pending_submissions,
                              captured_urls_by_node,
                              in_flight, out_dir, input_items, original_job):
    """flow_api path for the parallel HTTP-pull `_submit_one_job` entrypoint.

    Fires N batchGenerateImages POSTs via in-page page.evaluate(fetch). Reads the
    fife URL from each response and writes directly to captured_urls_by_node[node_id]
    — bypasses the v624/v627 listener attribution (page.on("request") fires
    unreliably for page.evaluate(fetch); attribution piled up orphan URLs in the
    previous iteration).

    Returns True on success (all N submits fired AND their URLs landed in the
    captured map). Returns False on any failure — caller falls through to the
    DOM path. Latches off for the page session on first failure so subsequent
    jobs skip the API attempt.
    """
    pfx = f"[{ctx}] " if ctx else ""

    if not _flow_api_mode_enabled():
        return False
    if _fa_api_on_cooldown(page, pfx):
        return False

    def _latch_off(reason: str):
        # v836 — _fa_api_start_cooldown is now a log-only no-op (never globally
        # pauses the API); a failing job retries the API 3x then uses the DOM path
        # for THAT job only. Kept routing through it so all latch sites are one.
        _fa_api_start_cooldown(page, reason, pfx)
        return False

    def _fall_back_one(reason: str):
        """Soft fall-back: this clip goes to DOM, but the API path stays armed
        for the NEXT clip. Use for transient errors (HTTP 5xx, network, single
        captcha mint hiccup) where the path itself is fine and a retry next
        job is reasonable."""
        print(f"{pfx}[flow_api] falling back to DOM for this clip (transient): {reason}", flush=True)
        return False

    # _is_unusual is now module-level (v828) so it is unit-testable and the
    # widened classifier is shared — see the def above _flow_api_pull_submit_try.

    def _is_server_5xx(reason: str) -> bool:
        """Transient server-side error (INTERNAL 500 / 502 / 503 / 504 /
        UNAVAILABLE / DEADLINE). Same prompt + same session will very likely
        succeed on a retry, so v818.3 retries the API submit in-place instead of
        falling to the DOM path (operator: 'on error 500 retry via API')."""
        r = (reason or "").upper()
        return any(s in r for s in (
            " 500", " 502", " 503", " 504",
            "CODE=500", "CODE=502", "CODE=503", "CODE=504",
            "INTERNAL", "UNAVAILABLE", "DEADLINE"))

    def _signal_unusual(reason: str):
        """v818 — flag an account-level block on the page so the caller recovers
        the SESSION (cookie-clear → golden restore) instead of falling through
        to the DOM path. Do NOT start the API cooldown: we want the API back the
        moment the session is clean."""
        try:
            page._flow_api_unusual_reason = reason
        except Exception:
            pass
        print(f"{pfx}[flow_api] ⚠ unusual-activity block — recovering session (no DOM fallback): {reason}", flush=True)
        return False

    def _is_transient(reason: str) -> bool:
        """True for failures that should fall back THIS clip but leave the API
        path armed for the NEXT clip — i.e. server-side hiccups OR per-prompt
        rejections (unsafe content, quota) that don't reflect a structural
        problem with this page session."""
        r = (reason or "").upper()
        for needle in (
            # transient server / network
            " 500", " 502", " 503", " 504",
            "INTERNAL", "UNAVAILABLE", "DEADLINE", "TIMEOUT",
            "FETCH FAILED", "EVALUATE FAILED",
            "CAPTCHA MINT FAILED",
            # per-prompt rejections (not structural)
            "UNSAFE_GENERATION",      # content policy — specific to this prompt
            "USER_QUOTA_REACHED",     # daily credits exhausted (other accounts/days fine)
            "MODEL_ACCESS_DENIED",    # tier mismatch — caller should downgrade model
            # auth token rotation — page re-auths naturally, next clip gets fresh token
            " 401", "UNAUTHENTICATED",
            "OAUTH 2 ACCESS TOKEN",
            "AUTHENTICATION CREDENTIALS",
        ):
            if needle in r:
                return True
        return False

    def _maybe_invalidate_token(reason: str):
        """If the failure is a stale-bearer 401, wipe the cached token so the next
        wait_for_token blocks until a freshly-sniffed one arrives."""
        r = (reason or "").upper()
        if " 401" in r or "UNAUTHENTICATED" in r or "OAUTH 2 ACCESS TOKEN" in r:
            try:
                _FA_TOKEN_STORE.token = ""
                _FA_TOKEN_STORE.captured_at = 0.0
                print(f"{pfx}[flow_api] cleared stale bearer; next API call will wait for a fresh one", flush=True)
            except Exception:
                pass

    project_id = ""
    try:
        m = re.search(r"/project/([0-9a-fA-F-]{36})", page.url or "")
        project_id = m.group(1) if m else ""
    except Exception:
        project_id = ""
    if not project_id:
        return _latch_off("no projectId in URL")

    api_aspect = _IMG_API_ASPECT_MAP.get(aspect_ratio, "IMAGE_ASPECT_RATIO_PORTRAIT")
    model_label = _IMG_API_MODEL_MAP.get((model or "").lower(), "Nano Banana 2")
    image_model = _fa_resolve_image_model_name(model_label)
    if not image_model:
        return _latch_off(f"no imageModelName for '{model_label}'")

    # v703 manifest — same shape the DOM path applies after upload_reference_images.
    try:
        if input_paths:
            manifest = _build_reference_manifest(input_paths)
            api_prompt = manifest + _strip_stale_reference_lines(prompt)
            preview = manifest.replace("\n", " | ").strip(" |")
            print(f"{pfx}[flow_api] v703 manifest ({len(input_paths)} ref(s)): {preview}", flush=True)
        else:
            api_prompt = prompt
    except Exception:
        api_prompt = prompt

    # Read reference bytes once.
    ref_bytes_list = []
    for p in (input_paths or []):
        try:
            with open(p, "rb") as f:
                ref_bytes_list.append(f.read())
        except Exception as e:
            return _latch_off(f"failed to read ref {p}: {e}")

    cli = _FaClient(page, project_id=project_id)
    cli.reset_timings()                 # v832 — per-node latency timing
    _t_wall0 = time.monotonic()         # v832 — submit wall-clock start

    # Upload references via private API. uploadImage has no captcha.
    try:
        ref_ids = []
        for i, b in enumerate(ref_bytes_list):
            ref_ids.append(cli.upload_image(b, file_name=f"ref_{i}.jpg"))
        if ref_ids:
            print(f"{pfx}[flow_api] uploaded {len(ref_ids)} ref(s) via API", flush=True)
    except Exception as e:
        reason = f"upload_image raised: {e}"
        _maybe_invalidate_token(reason)
        if _is_unusual(reason):
            return _signal_unusual(reason)
        if _is_transient(reason):
            return _fall_back_one(reason)
        return _latch_off(reason)

    # Register a pending_submissions entry so the legacy DOM path's scanner
    # logic also sees this node (some scanner branches check pending entries).
    try:
        try:
            _proj_url = page.url
        except Exception:
            _proj_url = None
        pending_submissions.append({
            'node_id': node_id,
            'expected_count': int(variants or 1),
            'ts': time.time(),
            'tagged_count': int(variants or 1),  # mark fully accounted-for; we attribute via captured_urls_by_node directly
            'project_url': _proj_url,
        })
        cutoff = time.time() - 60
        pending_submissions[:] = [p for p in pending_submissions if p['ts'] > cutoff]
    except Exception as e:
        return _latch_off(f"pending registration failed: {e}")

    # Fire N submits in-page. Sleep ~1.5s between each to (a) avoid the
    # rapid-fire pattern that tripped PUBLIC_ERROR_UNUSUAL_ACTIVITY in the
    # previous iteration and (b) let captcha tokens be minted cleanly. Total
    # ~6s for x4 — still well faster than the DOM path's ~25s.
    # v818.4 — PER-VARIANT retry. Each of the N variant submits is retried on
    # BOTH a transient server 5xx AND an 'unusual activity' 403 (operator saw a
    # x4 batch where only ONE variant's submit 403'd yet 3 rendered fine — a
    # single variant's block must NOT nuke the whole node). Decision AFTER the
    # loop, based on what actually landed:
    #   - any variant captured  → ship what we got (partial-ok), no restore
    #   - zero captured, all blocked with unusual-activity → real account block
    #     → golden restore (_signal_unusual)
    #   - zero captured, other transient → DOM fall-back; else latch.
    _SUBMIT_API_RETRIES = 3
    captured_fife_urls = []
    _last_fail_reason = None
    _saw_unusual = False
    try:
        for v in range(int(variants or 1)):
            if v > 0:
                time.sleep(1.5)
            for _att in range(_SUBMIT_API_RETRIES + 1):
                try:
                    media_id, fife_url = cli.submit_image(
                        prompt=api_prompt,
                        image_model_name=image_model,
                        reference_media_ids=ref_ids or None,
                        aspect=api_aspect,
                        cooldown=(v == 0 and _att == 0),  # only first call pays the 10s cooldown
                    )
                    if fife_url:
                        captured_fife_urls.append(fife_url)
                    _last_fail_reason = None
                    break  # this variant is done
                except Exception as e:
                    reason = f"submit_image raised: {e}"
                    _maybe_invalidate_token(reason)
                    _unusual = _is_unusual(reason)
                    if _unusual:
                        _saw_unusual = True
                    # retry the SAME variant on a transient 5xx OR a 403 block
                    # (each submit_image mints a fresh reCAPTCHA token).
                    if (_unusual or _is_server_5xx(reason)) and _att < _SUBMIT_API_RETRIES:
                        _w = 2 * (_att + 1)
                        _kind = "unusual-activity" if _unusual else "server error"
                        print(f"{pfx}[flow_api] {_kind} on variant {v + 1}/{variants} "
                              f"({reason[-44:]}) — API retry {_att + 1}/{_SUBMIT_API_RETRIES} in {_w}s", flush=True)
                        time.sleep(_w)
                        continue
                    _last_fail_reason = reason
                    break  # non-retryable, or API retries exhausted for this variant
    except Exception as e:
        _last_fail_reason = f"submit loop raised: {e}"

    # v832 — per-node latency line: shows where the submit wall-time went
    # (cooldown vs reCAPTCHA mint vs fetch), so "the API is slow" becomes a
    # measured breakdown instead of a guess.
    try:
        print(f"{pfx}[timing] node {node_id}: {cli.timings_summary()} "
              f"submit_wall={time.monotonic() - _t_wall0:.1f}s "
              f"captured={len(captured_fife_urls)}/{variants}", flush=True)
    except Exception:
        pass

    # v828 diagnostic (TEMPORARY — remove once operator confirms the classifier
    # catches the real block string). Logs the FULL final reason + how it
    # classified, so any un-caught account-block manifestation is visible in the
    # worker log and we can widen _is_unusual to match it.
    if _last_fail_reason or _saw_unusual:
        try:
            print(f"{pfx}[flow_api][v828-diag] submit outcome: "
                  f"captured={len(captured_fife_urls)}/{variants} "
                  f"reason={(_last_fail_reason or '')!r} "
                  f"is_unusual={_is_unusual(_last_fail_reason or '')} "
                  f"saw_unusual={_saw_unusual}", flush=True)
        except Exception:
            pass

    if captured_fife_urls:
        # At least one variant landed. A single variant's transient 403/500 does
        # NOT block the node — ship what we captured rather than aborting.
        if _last_fail_reason:
            print(f"{pfx}[flow_api] {len(captured_fife_urls)}/{variants} variant(s) captured; "
                  f"some failed transiently ({_last_fail_reason[-44:]}) — shipping what landed", flush=True)
    elif _saw_unusual:
        # EVERY variant blocked with unusual-activity after retries → genuine
        # account block → golden restore.
        return _signal_unusual(_last_fail_reason or "unusual-activity on all variants")
    elif _last_fail_reason is not None:
        if _is_transient(_last_fail_reason):
            return _fall_back_one(_last_fail_reason)
        return _latch_off(_last_fail_reason)
    else:
        return _latch_off("API responses carried no fife URLs (unexpected response shape)")

    # Write URLs DIRECTLY to the scanner's captured pool. Bypasses the v624
    # response listener entirely — page.on("request") fires unreliably for
    # page.evaluate(fetch), so we did our own attribution from the response
    # bodies that submit_image already parsed.
    try:
        bucket = captured_urls_by_node.setdefault(node_id, [])
        for u in captured_fife_urls:
            if u not in bucket:
                bucket.append(u)
    except Exception as e:
        return _latch_off(f"captured_urls_by_node write failed: {e}")

    # Register an InFlightJob so the scanner tracks completion + the platform
    # eventually receives a "done" status. Without this, _submit_one_job's
    # return-True is meaningless to the scanner pipeline — the server times
    # out the claim and re-issues the job (the symptom on f879c59).
    try:
        try:
            prompt_key = _derive_prompt_key(prompt)
        except Exception:
            prompt_key = prompt[:80]
        in_flight[node_id] = InFlightJob(
            node_id=node_id,
            node_name=node_name,
            prompt=prompt,
            prompt_key=prompt_key,
            variants=int(variants or 1),
            output_dir=out_dir,
            input_items=input_items,
            baseline_urls=set(),  # API path: no DOM baseline needed
            tile_ids=[],          # API path: no tile-id capture (attribution by URL pool)
            original_job=original_job,
        )
    except Exception as e:
        return _latch_off(f"InFlightJob registration failed: {e}")

    print(
        f"{pfx}[flow_api] fired {len(captured_fife_urls)}x batchGenerateImages POSTs "
        f"via in-page API; URLs written to scanner pool + InFlightJob registered",
        flush=True,
    )
    return True


def _install_flow_api_capture_image(page):
    """Read-only request listener that records real image submit/upload bodies +
    imageModelName + shape (with/without imageInputs base/reference). Inert unless
    FLOW_API_CAPTURE=1. Mirrors the flow_worker.py capture hook. Never raises into
    Playwright; changes nothing about generation."""
    if page is None or not _flow_api_capture_enabled():
        return
    try:
        if getattr(page, '_flow_api_capture_installed', False):
            return
    except Exception:
        return
    out_path = _flow_api_capture_path()
    _watch = (
        "flowMedia:batchGenerateImages",
        "flow/uploadImage",
        "flow/upsampleImage",
    )

    def _on_request(req):
        try:
            url = getattr(req, 'url', '') or ''
            if 'aisandbox-pa.googleapis.com' not in url:
                return
            if not any(w in url for w in _watch):
                return
            try:
                body = req.post_data
            except Exception:
                body = None
            image_model = ''
            input_shape = ''
            input_count = 0
            if body:
                try:
                    j = json.loads(body)
                    for r in (j.get('requests') or []):
                        if not isinstance(r, dict):
                            continue
                        if r.get('imageModelName'):
                            image_model = r['imageModelName']
                        inputs = r.get('imageInputs') or []
                        if inputs:
                            input_count = len(inputs)
                            types = [i.get('imageInputType', '') for i in inputs if isinstance(i, dict)]
                            input_shape = '+'.join(sorted(set(t.replace('IMAGE_INPUT_TYPE_', '') for t in types)))
                        if image_model:
                            break
                except Exception:
                    pass
            endpoint = url.split('?', 1)[0]
            print(
                f"[flow-api-capture] {endpoint.rsplit('/', 1)[-1]} "
                f"imageModelName={image_model or '-'} inputs={input_count} shape={input_shape or '-'}",
                flush=True,
            )
            try:
                with open(out_path, 'a', encoding='utf-8') as f:
                    f.write(json.dumps({
                        'ts': time.time(),
                        'method': getattr(req, 'method', ''),
                        'url': url,
                        'imageModelName': image_model,
                        'input_shape': input_shape,
                        'input_count': input_count,
                        'body_raw': body,
                    }) + "\n")
            except Exception:
                pass
        except Exception:
            pass

    try:
        page.on('request', _on_request)
        try:
            page._flow_api_capture_installed = True
        except Exception:
            pass
        print(f"[flow-api-capture] (image) enabled -> {out_path}", flush=True)
    except Exception as e:
        print(f"[flow-api-capture] (image) failed to install: {e}", flush=True)


def attach_image_url_listener(page):
    """Attach a network response listener that captures generated image
    URLs directly from `batchGenerateImages` JSON responses.

    Replaces DOM-polling for URL detection. Each Flow image generation
    POSTs to `aisandbox-pa.googleapis.com/.../flowMedia:batchGenerateImages`
    and returns JSON containing `media[].image.generatedImage.fifeUrl` —
    the direct CDN URL we want to download. Catching the response gives
    us URLs roughly when the model finishes (no DOM-render delay) and
    skips Virtuoso list tile-mount race conditions entirely.

    Returns (get_captured, detach):
        get_captured() -> list[str] of fifeUrls in order of arrival.
        detach()        -> remove the response handler.
    """
    captured = []  # list of fifeUrl strings (preserves order)
    seen = set()

    def on_response(response):
        try:
            url = response.url
            if 'batchGenerateImages' not in url:
                return
            if response.status != 200:
                return
            body = response.json()
        except Exception:
            return
        for media in (body.get('media') or []):
            try:
                fife = media['image']['generatedImage']['fifeUrl']
            except (KeyError, TypeError):
                continue
            if fife and fife not in seen:
                seen.add(fife)
                captured.append(fife)

    page.on('response', on_response)

    # flow_api rebuild: read-only capture of real image submit/upload bodies +
    # imageModelName. Inert unless FLOW_API_CAPTURE=1. Never affects generation.
    _install_flow_api_capture_image(page)

    def get_captured():
        return list(captured)

    def detach():
        try:
            page.remove_listener('response', on_response)
        except Exception:
            pass

    return get_captured, detach


def snapshot_generated_image_urls(page, exclude_uploads=True):
    """Return a set of absolute URLs for completed generation tiles in the DOM.

    Flow labels multiple things with <img alt='Generated image'>:
      - Real completed generations (what we want)
      - Uploaded reference images (filtered out by metadata)
      - Pending/loading tiles (may have placeholder imgs)
      - Stale or transitional UI states

    Our filter uses BOTH positive and negative criteria:

      Positive (must match, whitelist):
        1. src contains '/fx/api/trpc/media.getMediaUrlRedirect?name='
           — real completed assets always have this URL
        2. img is wrapped in <a href="/fx/tools/flow/project/.../edit/...">
           — only committed tiles have the edit link

      Negative (must not match, blacklist):
        3. Ancestor virtuoso item does NOT contain 'Uploaded image' text
           — filters out user-uploaded refs that share the alt

    Used as part of delta detection:
      before = snapshot before click Generate
      after  = snapshot after generation
      new = after - before
    """
    urls = set()
    try:
        base = page.evaluate("window.location.origin")
    except Exception:
        base = ""

    try:
        srcs = page.evaluate("""
            (args) => {
                const EXCLUDE_UPLOADS = args.exclude_uploads;
                const REQUIRED_URL_SUBSTR = 'media.getMediaUrlRedirect';
                const out = [];

                const imgs = document.querySelectorAll("img[alt='Generated image']");
                for (const img of imgs) {
                    // 1) Positive: src is a real completed-asset URL
                    if (!img.src) continue;
                    if (!img.src.includes(REQUIRED_URL_SUBSTR)) continue;

                    // 2) Positive: wrapped in an <a href="/edit/"> link
                    //    (committed tiles have this; prompt chips, gallery
                    //    picker thumbs, and loading placeholders don't)
                    const link = img.closest("a[href*='/edit/']");
                    if (!link) continue;

                    // 3) Negative: not inside a virtuoso item marked
                    //    "Uploaded image"
                    if (EXCLUDE_UPLOADS) {
                        let a = img;
                        let inUpload = false;
                        while (a && a !== document.body) {
                            if (a.hasAttribute && a.hasAttribute('data-index')) {
                                const atxt = a.innerText || '';
                                if (atxt.includes('Uploaded image')) {
                                    inUpload = true;
                                }
                                break;
                            }
                            a = a.parentElement;
                        }
                        if (inUpload) continue;
                    }

                    out.push(img.src);
                }
                return Array.from(new Set(out));
            }
        """, {"exclude_uploads": exclude_uploads})

        if isinstance(srcs, list):
            for src in srcs:
                if not src:
                    continue
                full = base + src if src.startswith("/") else src
                urls.add(full)
            return urls
    except Exception as e:
        # JS eval failed — fall back to simple alt-only Playwright lookup
        pass

    # Fallback: simple non-filtered path
    try:
        imgs = page.locator("img[alt='Generated image']")
        n = imgs.count()
        for i in range(n):
            try:
                src = imgs.nth(i).get_attribute("src")
                if not src:
                    continue
                if "media.getMediaUrlRedirect" not in src:
                    continue
                full = base + src if src.startswith("/") else src
                urls.add(full)
            except Exception:
                continue
    except Exception:
        pass
    return urls


def download_image_urls(page, urls, output_dir, context="", max_workers=4):
    """Download a list of image URLs into output_dir as variant_1.png,
    variant_2.png, ... Returns list of saved filenames.

    Two-tier client strategy:
      - Tier A: `httpx.Client(http2=True)` if installed — single TLS
        connection, multiplexed streams (lower TTFB on multi-image batches).
      - Tier B: `requests.Session()` with keep-alive (HTTP/1.1).
    Both use warm browser cookies + Referer/Origin from the live page.

    Downloads run in parallel via ThreadPoolExecutor (default 4 workers).
    Per HAR, each image is ~700 KB / ~210 ms (mostly TTFB); 4-way parallel
    cuts a 6-variant batch from ~1.3s to ~250 ms.

    Falls back to `page.request.get` (Playwright's own HTTP) per-URL if
    both tier-A and tier-B fail for that URL.
    """
    prefix = f"[{context}] " if context else ""
    os.makedirs(output_dir, exist_ok=True)

    if not urls:
        print(f"{prefix}❌ No URLs provided to download", flush=True)
        return []

    # ---- Build a warm-cookie HTTP client (httpx HTTP/2 if available) ----
    try:
        ua = page.evaluate("navigator.userAgent")
    except Exception:
        ua = "Mozilla/5.0"
    headers = {
        "Referer": "https://labs.google/",
        "Origin": "https://labs.google",
        "User-Agent": ua,
        "Accept": "image/png,image/*;q=0.9,*/*;q=0.5",
    }
    try:
        cookies = {ck["name"]: ck["value"] for ck in page.context.cookies()}
    except Exception:
        cookies = {}

    client = None
    client_label = "none"
    try:
        import httpx as _httpx  # type: ignore
        try:
            client = _httpx.Client(http2=True, headers=headers, cookies=cookies,
                                   timeout=60.0, follow_redirects=True)
            client_label = "httpx/h2"
        except Exception:
            # http2 extra not installed — fall back to httpx HTTP/1.1
            client = _httpx.Client(headers=headers, cookies=cookies,
                                   timeout=60.0, follow_redirects=True)
            client_label = "httpx"
    except ImportError:
        try:
            import requests as _requests
            client = _requests.Session()
            client.headers.update(headers)
            client.cookies.update(cookies)
            client_label = "requests"
        except ImportError:
            client = None
            client_label = "playwright-only"

    def _close_client():
        try:
            client.close()
        except Exception:
            pass

    # ---- Per-URL worker ----
    def _fetch_one(idx_url):
        idx, img_url = idx_url
        save_name = f"variant_{idx}.png"
        save_path = os.path.join(output_dir, save_name)

        if client is not None:
            try:
                resp = client.get(img_url, timeout=60)
                status = getattr(resp, "status_code", None)
                if status == 200:
                    body = resp.content if hasattr(resp, "content") else resp.read()
                    with open(save_path, "wb") as f:
                        f.write(body)
                    if os.path.getsize(save_path) > 0:
                        return (idx, save_name, save_path, "client", None)
            except Exception as e:
                client_err = str(e)[:120]
            else:
                client_err = f"status={status}"
        else:
            client_err = "no client"

        # Playwright fallback (page.request shares the browser's session
        # — works even when cookie copy missed something)
        try:
            api_resp = page.request.get(img_url, timeout=60000)
            if api_resp.ok:
                body = api_resp.body()
                with open(save_path, "wb") as f:
                    f.write(body)
                if os.path.getsize(save_path) > 0:
                    return (idx, save_name, save_path, "playwright", None)
                return (idx, save_name, None, "playwright", "empty body")
            return (idx, save_name, None, "playwright", f"status={api_resp.status}")
        except Exception as e:
            return (idx, save_name, None, "playwright", f"{client_err}; pw={str(e)[:120]}")

    # ---- Fan out ----
    print(f"{prefix}Downloading {len(urls)} variant(s) via {client_label} (parallel x{max_workers})...", flush=True)
    saved = []
    try:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            results = list(pool.map(_fetch_one, list(enumerate(urls, start=1))))
    finally:
        _close_client()

    # Preserve original order
    for idx, save_name, save_path, route, err in sorted(results, key=lambda r: r[0]):
        if save_path:
            size_kb = os.path.getsize(save_path) / 1024
            print(f"{prefix}  ✓ {save_name} via {route} ({size_kb:.0f} KB)", flush=True)
            saved.append(save_name)
        else:
            print(f"{prefix}  ❌ {save_name}: {err}", flush=True)

    return saved


def download_all_generated_images(page, output_dir, variants=1, context="", skip_first=0):
    """Deprecated. Use snapshot_generated_image_urls + download_image_urls for
    delta-based detection that works correctly across project reuse.
    Kept as a no-op stub so imports from older code don't break."""
    print(f"[{context}] ⚠ download_all_generated_images is deprecated — use delta-based URL detection", flush=True)
    return []




# v771 — "unusual activity" refresh-and-resume, ported from the video worker
# (flow_worker.py clear_flow_site_data + v758.21). On Flow's "We noticed some
# unusual activity" block: surgically delete labs.google cookies + cache (keep
# Google SSO auth + app route in localStorage), reload so Flow re-signs-in, then
# retry the current image. Bounded + cooldown-deduped: an unbounded refresh loop
# on a genuinely-flagged account worsens throttling (flow-worker-throttle-redo-
# dynamics), so after IMG_UNUSUAL_MAX_STRIKES recoveries we stop and surface an
# actionable error instead of hammering Google.
IMG_UNUSUAL_COOLDOWN = 30        # s — skip a redundant recovery within this window
IMG_UNUSUAL_MAX_STRIKES = 3      # per (worker,label) before giving up
_IMG_UNUSUAL_LAST = {}           # label -> last recovery epoch
_IMG_UNUSUAL_STRIKES = {}        # label -> cumulative recoveries
_IMG_UNUSUAL_LOCK = threading.Lock()


def clear_flow_site_data(page, label=""):
    """Clear labs.google COOKIES + CACHE while PRESERVING localStorage /
    sessionStorage / IndexedDB (keeps the SPA route so reload lands back on the
    project, not the marketing page). The block is cookie-keyed; Google SSO auth
    cookies are never touched, so reload re-signs-in. Ported verbatim from
    flow_worker.clear_flow_site_data (v758.23). Returns True if cookie clear ran."""
    prefix = f"[{label}] " if label else ""
    ok = False
    try:
        ctx = page.context
        cdp = ctx.new_cdp_session(page)
        all_c = cdp.send("Network.getCookies").get("cookies", [])
        removed = 0
        for c in all_c:
            dom = c.get("domain") or ""
            if "labs.google" in dom:
                try:
                    cdp.send("Network.deleteCookies", {
                        "name": c.get("name", ""),
                        "domain": dom,
                        "path": c.get("path", "/"),
                    })
                    removed += 1
                except Exception:
                    pass
        try:
            cdp.detach()
        except Exception:
            pass
        print(f"{prefix}🧹 cookies: removed {removed} labs.google (Google SSO auth untouched)", flush=True)
        ok = True
    except Exception as e:
        print(f"{prefix}⚠ cookie clear failed: {e}", flush=True)
    try:
        if "labs.google" in (page.url or ""):
            page.evaluate("""async () => {
                try {
                    if (window.caches) {
                        const ks = await caches.keys();
                        await Promise.all(ks.map(k => caches.delete(k)));
                    }
                } catch (e) {}
            }""")
            print(f"{prefix}🧹 cacheStorage cleared (localStorage/sessionStorage/IndexedDB preserved)", flush=True)
    except Exception as e:
        print(f"{prefix}⚠ storage clear failed (non-fatal): {e}", flush=True)
    try:
        cdp = page.context.new_cdp_session(page)
        cdp.send("Storage.clearDataForOrigin", {
            "origin": "https://labs.google",
            "storageTypes": "cache_storage,service_workers,file_systems,shader_cache",
        })
        try:
            cdp.detach()
        except Exception:
            pass
        print(f"{prefix}🧹 labs.google cache/service-worker storage cleared via CDP", flush=True)
    except Exception as e:
        print(f"{prefix}⚠ CDP origin clear failed (non-fatal): {e}", flush=True)
    return ok


def page_shows_unusual_activity(page):
    """True if the Flow page currently shows the 'unusual activity' block."""
    try:
        return bool(page.evaluate("""() => {
            const t = ((document.body && document.body.textContent) || '').toLowerCase();
            return t.includes('unusual activity') || t.includes('we noticed some unusual');
        }"""))
    except Exception:
        return False


def recover_unusual_activity(page, label=""):
    """Clear labs.google cookies+cache and reload so Flow re-signs-in. Cooldown-
    deduped + strike-capped. Returns True if the caller should retry the image,
    False if the strike cap is hit (caller should bail with an actionable error)."""
    prefix = f"[{label}] " if label else ""
    now = time.time()
    with _IMG_UNUSUAL_LOCK:
        last = _IMG_UNUSUAL_LAST.get(label, 0)
        if now - last < IMG_UNUSUAL_COOLDOWN:
            print(f"{prefix}↩ unusual-activity recovery within {IMG_UNUSUAL_COOLDOWN}s cooldown — pausing before retry", flush=True)
            _within_cooldown = True
        else:
            _within_cooldown = False
            _IMG_UNUSUAL_STRIKES[label] = _IMG_UNUSUAL_STRIKES.get(label, 0) + 1
            _IMG_UNUSUAL_LAST[label] = now
        strikes = _IMG_UNUSUAL_STRIKES.get(label, 0)
    if _within_cooldown:
        time.sleep(5)
        return True
    if strikes > IMG_UNUSUAL_MAX_STRIKES:
        print(f"{prefix}⛔ unusual-activity persists after {IMG_UNUSUAL_MAX_STRIKES} recoveries — giving up (account likely flagged; retry later)", flush=True)
        return False
    print(f"{prefix}🔥 unusual-activity detected — clearing labs.google cookies + reloading (recovery {strikes}/{IMG_UNUSUAL_MAX_STRIKES})", flush=True)
    try:
        clear_flow_site_data(page, label=label)
    except Exception as e:
        print(f"{prefix}⚠ site-data clear failed: {e}", flush=True)
    try:
        page.reload(wait_until="domcontentloaded", timeout=30000)
        time.sleep(3)
        print(f"{prefix}✓ page reloaded after unusual-activity clear — resuming", flush=True)
    except Exception as e:
        print(f"{prefix}⚠ reload after clear failed: {e}", flush=True)
    return True


def reset_unusual_activity_strikes(label=""):
    """Clear the strike counter after a clean success so a later independent
    block starts fresh (doesn't inherit old strikes)."""
    with _IMG_UNUSUAL_LOCK:
        _IMG_UNUSUAL_STRIKES.pop(label, None)


def process_image_job_multi(page, input_paths, prompt, output_dir,
                             variants=1, aspect_ratio="16:9", resolution="1K",
                             model="nano_banana_2", context="",
                             already_uploaded=None):
    """v771 — bounded unusual-activity retry wrapper around
    _process_image_job_multi_once. On a failed attempt, if the Flow page shows
    the 'unusual activity' block (or the error mentions it), clear cookies +
    reload and retry the SAME image from the top ('restart from where we were').
    Non-unusual failures return unchanged. Strike-capped to avoid throttle churn."""
    for _ua_attempt in range(IMG_UNUSUAL_MAX_STRIKES + 1):
        ok, paths, err = _process_image_job_multi_once(
            page, input_paths, prompt, output_dir,
            variants=variants, aspect_ratio=aspect_ratio, resolution=resolution,
            model=model, context=context, already_uploaded=already_uploaded,
        )
        if ok:
            reset_unusual_activity_strikes(label=context)
            return ok, paths, err
        _is_unusual = page_shows_unusual_activity(page) or (
            isinstance(err, str) and 'unusual activity' in err.lower()
        )
        if not _is_unusual:
            return ok, paths, err
        print(f"[{context}] ⚠ image generation hit 'unusual activity' — attempting refresh-and-resume", flush=True)
        if not recover_unusual_activity(page, label=context):
            return False, [], "unusual activity — account blocked after repeated recoveries, retry later"
    return False, [], "unusual activity persisted after recovery retries"


def _process_image_job_multi_once(page, input_paths, prompt, output_dir,
                             variants=1, aspect_ratio="16:9", resolution="1K",
                             model="nano_banana_2", context="",
                             already_uploaded=None):
    """Multi-variant version of process_image_job. Saves N variants into
    output_dir as variant_<N>.png. Returns (success, output_paths_list,
    error_message).

    Args:
        already_uploaded: optional set of basenames that have already been
            uploaded in the current project. Files whose basename is in
            this set will be reused from the gallery instead of re-uploaded.
            On return (via side effect), newly-uploaded files are added
            to the set.
    """
    prefix = f"[{context}] " if context else ""
    print(f"\n{prefix}{'='*60}")
    print(f"{prefix}IMAGE JOB (multi-variant)")
    print(f"{prefix}{'='*60}")
    print(f"{prefix}Prompt:   {prompt[:80]}{'...' if len(prompt) > 80 else ''}")
    print(f"{prefix}Inputs:   {len(input_paths)} image(s)")
    print(f"{prefix}Outputs:  {output_dir}")
    print(f"{prefix}Settings: {aspect_ratio} / {resolution} / {model} / x{variants}")
    print(f"{prefix}{'='*60}")

    try:
        # 1) Select Image mode
        if not select_image_mode(page, context=context):
            return False, [], "Failed to switch to Image mode"

        # 1b) Clear any reference-image chips attached to the prompt from
        # a previous job in the same project. Reusing projects across jobs
        # means we inherit the previous run's state — detach old chips
        # before this job starts so its own references don't combine.
        clear_prompt_references(page, context=context)

        # 2) Configure settings
        if not configure_image_settings(page, aspect_ratio=aspect_ratio,
                                         resolution=resolution, model=model,
                                         variants=variants, context=context):
            return False, [], "Failed to configure image settings"

        # 2.5) Optional flow_api (private-API) multi-variant path.
        # On any failure: falls through to the DOM Steps 3-8 below unchanged.
        api_saved = _flow_api_image_multi_try(
            page, input_paths, prompt, aspect_ratio, model, variants, output_dir, context=context
        )
        if api_saved:
            print(f"{prefix}✓ [flow_api] saved {len(api_saved)} variant(s) to: {output_dir}", flush=True)
            return True, api_saved, None

        # 3) Upload reference image(s) if provided
        if input_paths:
            if not upload_reference_images(page, input_paths, context=context,
                                           already_uploaded=already_uploaded):
                return False, [], "Failed to upload reference images"
            # v703 — worker-injected reference manifest (see helper docstrings)
            _v703_manifest = _build_reference_manifest(input_paths)
            prompt = _v703_manifest + _strip_stale_reference_lines(prompt)
            print(
                f"[{context}] [v703] manifest prepended ({len(input_paths)} ref(s)): "
                f"{_v703_manifest.replace(chr(10), ' | ').strip(' |')}",
                flush=True,
            )

        # 4) Fill prompt
        if not fill_prompt_textarea(page, prompt):
            return False, [], "Failed to fill prompt"

        # 5) STABILITY WAIT — before taking the pre-Generate baseline,
        # wait for the page to be quiet. If a previous job's generation
        # is still finalizing when we arrive here, its tiles will appear
        # AFTER our snapshot and get misattributed as variants of THIS
        # job. Polling until the URL count is stable across two checks
        # (3s apart, up to 60s total) ensures prior batches have settled.
        print(f"{prefix}Checking page stability (waiting for any in-flight generations to settle)...", flush=True)
        stability_start = time.time()
        stability_deadline = stability_start + 60
        prev_count = -1
        stable_checks = 0
        while time.time() < stability_deadline:
            try:
                snap = snapshot_generated_image_urls(page)
                cur_count = len(snap)
            except Exception:
                cur_count = prev_count  # treat error as unchanged
            if cur_count == prev_count and prev_count >= 0:
                stable_checks += 1
                if stable_checks >= 2:
                    print(f"{prefix}  ✓ Page stable at {cur_count} tile(s)", flush=True)
                    break
            else:
                if prev_count >= 0:
                    print(f"{prefix}  ⏳ Tile count changed: {prev_count} → {cur_count} (prior generation still finalizing)", flush=True)
                stable_checks = 0
            prev_count = cur_count
            time.sleep(3)
        else:
            print(f"{prefix}  ⚠ 60s stability timeout — proceeding anyway", flush=True)

        # Take the baseline now that the page is quiet.
        before_urls = snapshot_generated_image_urls(page)
        print(f"{prefix}Pre-Generate URL snapshot: {len(before_urls)} existing tile(s) (includes uploaded refs)", flush=True)

        # Attach the network response listener BEFORE clicking Generate.
        # It will see the `batchGenerateImages` JSON response the moment
        # Flow's API returns it (~24s after the click) — faster and more
        # reliable than DOM-polling for tile mount.
        get_captured, detach_listener = attach_image_url_listener(page)

        try:
            if not click_generate_image(page, context=context):
                return False, [], "Failed to click Generate"

            # 6) Wait for at least one new variant. The listener fast-paths
            # detection; baseline_urls remains the DOM fallback.
            if not wait_for_image_result(page, timeout=240, context=context,
                                         baseline_urls=before_urls,
                                         get_captured=get_captured):
                return False, [], "Timeout or failure waiting for generation"

            # 7) Collect URLs. Listener-captured URLs are the fife (CDN)
            # URLs we want to download from directly. If for any reason
            # the listener missed (e.g. response body unparseable),
            # fall back to DOM scraping.
            new_urls = []  # ordered list, fife URLs preferred

            print(f"{prefix}Waiting for first variant to appear...", flush=True)
            first_deadline = time.time() + 60
            while time.time() < first_deadline:
                cap = get_captured()
                if cap:
                    new_urls = cap
                    print(f"{prefix}  ✓ First variant URL captured ({len(new_urls)} so far)", flush=True)
                    break
                time.sleep(0.25)
            else:
                # Listener never fired — fall back to DOM
                try:
                    dom_urls = snapshot_generated_image_urls(page) - before_urls
                except Exception:
                    dom_urls = set()
                new_urls = list(dom_urls)
                if new_urls:
                    print(f"{prefix}  ✓ First variant via DOM ({len(new_urls)} so far)", flush=True)

            # Phase 2: wait for remaining variants
            if new_urls and len(new_urls) < variants:
                print(f"{prefix}Waiting up to 30s for remaining variants ({len(new_urls)}/{variants} so far)...", flush=True)
                remaining_deadline = time.time() + 30
                while time.time() < remaining_deadline:
                    cap = get_captured()
                    if cap and len(cap) >= len(new_urls):
                        new_urls = cap
                    if len(new_urls) >= variants:
                        print(f"{prefix}  ✓ All {variants} variants ready", flush=True)
                        break
                    time.sleep(0.25)
                else:
                    # Final DOM sweep before giving up
                    try:
                        dom_urls = snapshot_generated_image_urls(page) - before_urls
                        if len(dom_urls) > len(new_urls):
                            new_urls = list(dom_urls)
                    except Exception:
                        pass
                    print(f"{prefix}  ⏱ 30s elapsed, proceeding with {len(new_urls)}/{variants} variants", flush=True)
        finally:
            detach_listener()

        if not new_urls:
            return False, [], "No new variants produced (all failed or timed out)"

        # Limit to requested count (shouldn't exceed, but safety)
        new_urls_list = list(new_urls)[:variants]

        # 8) Download the new URLs (parallel)
        saved = download_image_urls(page, new_urls_list, output_dir, context=context)
        if not saved:
            return False, [], "No variants could be downloaded"

        print(f"{prefix}✓ Saved {len(saved)} variant(s) to: {output_dir}", flush=True)
        return True, saved, None

    except Exception as e:
        import traceback
        traceback.print_exc()
        return False, [], f"Exception: {e}"


# ============================================================
# WATCH-FOLDER MODE
# ============================================================
# The platform (main FastAPI app) drops a .json job file into the watched
# folder. The worker picks it up, processes it through Flow UI, and writes
# back a .done.json so the platform can update the node.
#
# v509: Job JSON format
# {
#   "id": "node_123",
#   "prompt": "...",
#   "input_images": [
#     {"path": "/abs/path1.png", "role": "the main character", "slot_order": 0},
#     {"path": "/abs/path2.png", "role": "her daughter", "slot_order": 1},
#     ...
#   ],
#   "output_dir": "/abs/path/to/node_123/",
#   "aspect_ratio": "9:16",
#   "resolution": "2K",
#   "model": "nano_banana_2",
#   "variants": 4
# }
#
# Legacy format (still accepted for backwards compat):
#   "input_images": ["/abs/path1.png", "/abs/path2.png"]
#
# Result JSON (.done.json, same name + .done):
# {
#   "id": "node_123",
#   "status": "completed" | "failed",
#   "output_dir": "/abs/path/to/node_123/",
#   "output_paths": ["variant_1.png", "variant_2.png", ...],
#   "error": null
# }

WATCH_POLL_INTERVAL = 2  # seconds


def _slugify_role(role, fallback=""):
    """Turn a role string like 'her daughter (before)' into a basename-safe
    slug like 'her_daughter_before'. Returns fallback (typically the
    original filename stem) if role is empty.

    Used to rename uploaded reference files so Flow's gallery alt-text
    becomes the semantic ingredient name. The prompt's reference to
    'her daughter' then has a strong matching signal against the
    gallery image.
    """
    if not role:
        return fallback
    import re as _re_mod
    slug = _re_mod.sub(r"[^a-zA-Z0-9]+", "_", str(role).strip().lower()).strip("_")
    return slug or fallback


def _content_hash8(path):
    """First 8 hex chars of the file's sha256. Used as a filename suffix so
    the gallery-reuse key carries CONTENT identity, not just the role name.

    v807 root cause: refs were staged as '<roleslug>.png' — the same name
    no matter WHICH image the operator selected on the platform. The
    gallery-reuse lookup matches by alt-text (= upload basename), so when
    the operator swapped the underlying image for a role (new variant,
    different character pick), the worker reused the OLD gallery tile
    under the same name and silently attached the wrong image. Suffixing
    the content hash makes same-content reuse still hit the gallery cache
    while changed content forces a fresh upload under a new name.
    """
    import hashlib
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()[:8]


def _normalize_input_images(input_images):
    """Accept v509 dict format or legacy list-of-strings.

    Returns a list of dicts: [{"path", "role", "slot_order"}, ...]
    in slot order. Items missing role get role="".
    """
    if not input_images:
        return []
    normalized = []
    for idx, item in enumerate(input_images):
        if isinstance(item, str):
            normalized.append({"path": item, "role": "", "slot_order": idx})
        elif isinstance(item, dict):
            normalized.append({
                "path": item.get("path") or item.get("url") or "",
                "role": item.get("role", ""),
                "slot_order": item.get("slot_order", idx),
            })
        else:
            # Unknown item shape — skip
            print(f"[WATCH] ⚠ Skipping malformed input_images item: {item!r}", flush=True)
    normalized.sort(key=lambda d: d.get("slot_order", 0))
    return normalized


def _stage_inputs_with_role_basename(input_dicts, work_dir):
    """For each {path, role, slot_order} input dict, copy the file into
    work_dir under a slugified-role basename (preserving the original
    extension). Returns a list of staged paths in slot order.

    If the role is empty, the file is staged under its original basename.
    If two inputs slugify to the same name, the second gets a "__N" suffix.

    The point of staging: Flow's gallery alt-text comes from the upload
    basename, so naming the staged file 'the_main_character.png' instead
    of 'variant_42.png' lets Nano Banana 2's ingredient-matching bind
    the prompt's 'the main character' phrase to the right gallery image.
    """
    import shutil
    os.makedirs(work_dir, exist_ok=True)
    staged_paths = []
    used_basenames = set()
    for item in input_dicts:
        src = item["path"]
        if not src or not os.path.exists(src):
            print(f"[WATCH] ⚠ Input path missing on disk: {src!r}", flush=True)
            continue
        ext = os.path.splitext(src)[1] or ".png"
        original_stem = os.path.splitext(os.path.basename(src))[0]
        slug = _slugify_role(item.get("role", ""), original_stem)
        # v807: suffix the content hash so a role whose underlying image
        # changed gets a NEW gallery key (fresh upload) instead of a stale
        # gallery-reuse hit under the same role name.
        try:
            slug = f"{slug}__{_content_hash8(src)}"
        except Exception as e:
            print(f"[WATCH] ⚠ Could not hash {src}: {e} — staging without hash", flush=True)
        candidate = f"{slug}{ext}"
        # Disambiguate collisions within this job
        n = 2
        while candidate in used_basenames:
            candidate = f"{slug}__{n}{ext}"
            n += 1
        used_basenames.add(candidate)
        dest = os.path.join(work_dir, candidate)
        try:
            shutil.copyfile(src, dest)
            staged_paths.append(dest)
            role_label = item.get("role") or "(no role)"
            print(f"[WATCH]   ↳ staged {os.path.basename(src)}  →  {candidate}  [{role_label}]", flush=True)
        except Exception as e:
            print(f"[WATCH] ⚠ Failed to stage {src} → {dest}: {e}", flush=True)
            # Fall back to using the original path
            staged_paths.append(src)
    return staged_paths


def _read_job_file(job_path):
    """Read and parse a job JSON file. Returns dict or None on error."""
    try:
        with open(job_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"[WATCH] ❌ Can't read {job_path}: {e}", flush=True)
        return None


def _write_done_file(job_path, result):
    """Write a .done.json file next to the job file."""
    done_path = str(job_path).replace('.json', '.done.json')
    try:
        with open(done_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2)
        print(f"[WATCH] → Wrote {os.path.basename(done_path)}", flush=True)
    except Exception as e:
        print(f"[WATCH] ❌ Can't write {done_path}: {e}", flush=True)


def _process_watch_job(page, job_path, job):
    """Run a single watch-folder job end-to-end."""
    jid = job.get('id', 'unknown')
    prompt = job.get('prompt', '')
    raw_input_images = job.get('input_images') or []
    output_dir = job.get('output_dir')
    if not output_dir:
        result = {"id": jid, "status": "failed", "error": "output_dir missing"}
        _write_done_file(job_path, result)
        return
    variants = int(job.get('variants') or 1)
    aspect_ratio = job.get('aspect_ratio', '16:9')
    resolution = job.get('resolution', '1K')
    model = job.get('model', 'nano_banana_2')

    # v509: normalize to v509 dict format (also accepts legacy list-of-strings)
    input_dicts = _normalize_input_images(raw_input_images)

    # Validate inputs exist on disk before staging
    missing = [d["path"] for d in input_dicts if not os.path.exists(d.get("path", ""))]
    if missing:
        result = {
            "id": jid, "status": "failed",
            "error": f"Input files missing: {missing}",
            "output_dir": output_dir, "output_paths": [],
        }
        _write_done_file(job_path, result)
        return

    # v509: stage inputs into a job-scoped work dir with slugified-role
    # basenames so Flow's gallery alt-text becomes the semantic ingredient
    # name (e.g. 'the_main_character.png' instead of 'variant_42.png').
    if input_dicts:
        stage_dir = os.path.join(output_dir, "_inputs_staged")
        input_paths = _stage_inputs_with_role_basename(input_dicts, stage_dir)
    else:
        input_paths = []

    success, output_paths, error = process_image_job_multi(
        page,
        input_paths=input_paths,
        prompt=prompt,
        output_dir=output_dir,
        variants=variants,
        aspect_ratio=aspect_ratio,
        resolution=resolution,
        model=model,
        context=jid,
    )

    result = {
        "id": jid,
        "status": "completed" if success else "failed",
        "output_dir": output_dir,
        "output_paths": output_paths,
        "error": error,
    }
    _write_done_file(job_path, result)


def watch_folder_mode(page, watch_dir):
    """Poll the watch folder for .json job files. For each new one,
    process it and write a .done.json alongside."""
    os.makedirs(watch_dir, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"WATCH-FOLDER MODE")
    print(f"{'='*60}")
    print(f"Watching: {os.path.abspath(watch_dir)}")
    print(f"Drop .json job files here.")
    print(f"Ctrl+C to stop.")
    print(f"{'='*60}\n", flush=True)

    # Mark ready so the platform knows the worker is up
    ready_file = os.path.join(watch_dir, "_worker_ready")
    try:
        with open(ready_file, 'w') as f:
            f.write(str(int(time.time())))
    except Exception:
        pass

    processed = set()  # paths we've already handled

    try:
        while True:
            # Heartbeat: touch the ready file so the platform can detect
            # whether the worker is still alive (stale if older than ~15s).
            try:
                with open(ready_file, 'w') as f:
                    f.write(str(int(time.time())))
            except Exception:
                pass

            try:
                # Find .json files that don't have a matching .done.json yet
                all_json = [
                    f for f in os.listdir(watch_dir)
                    if f.endswith('.json') and not f.endswith('.done.json')
                ]
                pending = []
                for f in all_json:
                    full = os.path.join(watch_dir, f)
                    done = os.path.join(watch_dir, f.replace('.json', '.done.json'))
                    if full in processed:
                        continue
                    if os.path.exists(done):
                        continue
                    pending.append(full)

                for job_path in pending:
                    job = _read_job_file(job_path)
                    if not job:
                        processed.add(job_path)
                        continue
                    print(f"\n[WATCH] → Picked up: {os.path.basename(job_path)}", flush=True)
                    try:
                        _process_watch_job(page, job_path, job)
                    except Exception as e:
                        import traceback
                        traceback.print_exc()
                        _write_done_file(job_path, {
                            "id": job.get("id", "unknown"),
                            "status": "failed",
                            "error": f"Worker exception: {e}",
                            "output_dir": job.get("output_dir"),
                            "output_paths": [],
                        })
                    processed.add(job_path)

            except Exception as e:
                print(f"[WATCH] Loop error: {e}", flush=True)

            time.sleep(WATCH_POLL_INTERVAL)
    except KeyboardInterrupt:
        print("\n[WATCH] Stopping on user request...", flush=True)


# ============================================================
# HTTP-PULL API MODE (new — mirrors flow_worker.py pattern)
# ============================================================
# Worker polls a remote webapp over HTTP instead of a local folder.
# This is the preferred mode when the webapp runs on Render/Docker
# and the worker runs on the user's Windows machine.

API_POLL_INTERVAL = 3.0  # seconds between polls when idle
API_POLL_BUSY_INTERVAL = 0.5  # brief pause between jobs
API_PATH_PREFIX = "/api/images/worker"


def _api_request(api_url, api_key, method, endpoint, **kwargs):
    """Make a request to the webapp API. Returns parsed JSON or raises.

    v471 (post-deploy): use a module-level requests.Session for connection
    pooling and HTTP keepalive, and drop default timeout from 30s to 10s.
    Rationale: /jobs/pending, /heartbeat, /clips/*/status are all fast
    endpoints that should respond in well under a second. A 30s timeout
    just meant that during Render network stalls, the worker waited 30s
    per attempt before backing off — compounding to 10+ minutes of
    unproductive polling. With 10s, we notice network trouble in a third
    the time and get to backoff sooner. Callers that need longer (file
    downloads, heavy queries) still pass their own timeout.
    """
    import requests
    session = _get_api_session()
    url = f"{api_url.rstrip('/')}{API_PATH_PREFIX}{endpoint}"
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {api_key}"
    timeout = kwargs.pop("timeout", 10)
    resp = session.request(method, url, headers=headers, timeout=timeout, **kwargs)
    resp.raise_for_status()
    if resp.headers.get("content-type", "").startswith("application/json"):
        return resp.json()
    return resp


# Module-level session, lazy-init so existing imports don't break.
_API_SESSION = None
_API_SESSION_LOCK = None
def _get_api_session():
    """Return a shared requests.Session with pooling + retries.

    Shared across all _api_request calls for HTTP keepalive — avoids
    re-doing the TCP + TLS handshake on every poll (~200-500ms saved
    per request). Also enables urllib3's connection pool to reuse a
    warm connection when Render has a brief stall, rather than opening
    a new one.
    """
    global _API_SESSION, _API_SESSION_LOCK
    if _API_SESSION is not None:
        return _API_SESSION
    import threading as _t
    if _API_SESSION_LOCK is None:
        _API_SESSION_LOCK = _t.Lock()
    with _API_SESSION_LOCK:
        if _API_SESSION is not None:
            return _API_SESSION
        import requests
        from requests.adapters import HTTPAdapter
        try:
            from urllib3.util.retry import Retry
            retry = Retry(
                total=2,
                connect=2,
                read=0,  # we handle read errors via the caller's backoff
                backoff_factor=0.5,
                status_forcelist=(502, 503, 504),
                allowed_methods=frozenset(["GET", "POST", "HEAD"]),
                raise_on_status=False,
            )
            adapter = HTTPAdapter(
                max_retries=retry,
                pool_connections=4,
                pool_maxsize=8,
            )
        except Exception:
            adapter = HTTPAdapter(pool_connections=4, pool_maxsize=8)
        s = requests.Session()
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        _API_SESSION = s
        return _API_SESSION


def _api_request_original_signature(api_url, api_key, method, endpoint, **kwargs):
    """(retained as fallback — not used)"""
    import requests
    url = f"{api_url.rstrip('/')}{API_PATH_PREFIX}{endpoint}"
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {api_key}"
    timeout = kwargs.pop("timeout", 30)
    resp = requests.request(method, url, headers=headers, timeout=timeout, **kwargs)
    resp.raise_for_status()
    if resp.headers.get("content-type", "").startswith("application/json"):
        return resp.json()
    return resp


def _download_reference_inputs(api_key, input_images, work_dir):
    """Download each reference image to a local file with a stable name.

    input_images is a list of dicts from the platform:
      [{"url": ..., "filename": "variant_42.png", "role": "subject", "slot_order": 0}, ...]

    Each file is saved under work_dir with the server-provided filename.
    The filename is stable across jobs — Flow UI stores uploaded images in
    its gallery under this exact name (as alt text), so later scenes that
    reference the same variant can skip the upload and select it from the
    gallery instead.

    Returns a tuple (results, missing):
      - results: list of dicts [{"path", "filename", "role"}, ...] in the
        order of input_images, for items that were downloaded successfully
      - missing: list of {"filename", "role", "error"} for items that
        failed to download (e.g. 404 from expired worker-files tokens)

    Accepts legacy input format (plain list of URLs) for backwards compat.
    """
    import requests
    os.makedirs(work_dir, exist_ok=True)
    results = []
    missing = []

    # Backwards compat: accept plain list of URLs too
    if input_images and isinstance(input_images[0], str):
        input_images = [{"url": u, "filename": f"ref_{i+1}.png",
                         "role": "", "slot_order": i}
                        for i, u in enumerate(input_images)]

    for idx, item in enumerate(input_images, start=1):
        url = item["url"]
        original_filename = item.get("filename") or f"ref_{idx}.png"
        role = item.get("role", "")
        slot = role or f"slot {item.get('slot_order', idx-1)}"

        # v509: prefer slugified role as the saved basename so Flow's
        # gallery alt-text becomes the semantic ingredient name. The
        # prompt's reference to "the main character" / "her daughter"
        # then has a strong matching signal against the gallery image.
        ext = os.path.splitext(original_filename)[1] or ".png"
        original_stem = os.path.splitext(original_filename)[0]
        slug = _slugify_role(role, original_stem)
        filename = f"{slug}{ext}"

        # Retry on transient failures. A reference URL points at a parent
        # node's output; while that parent is being regenerated (redo in
        # flight) the URL 404s for a window, then comes back. Observed:
        # node 1746 avatar ref 404'd on first claim, then downloaded fine
        # (6800 KB, same URL) minutes later. Without retry the worker
        # dropped the avatar and silently shipped 4 avatar-less variants.
        # Retry 404 / 5xx / network with backoff; fail-fast on other 4xx.
        backoffs = [2, 5, 10]  # 4 attempts total
        last_err = None
        saved = False
        for attempt in range(len(backoffs) + 1):
            try:
                resp = requests.get(url,
                                    headers={"Authorization": f"Bearer {api_key}"},
                                    timeout=60, stream=True)
                if resp.status_code == 404 or resp.status_code >= 500:
                    if resp.status_code == 404:
                        last_err = "Reference file no longer available (404) — parent node may have been regenerated"
                    else:
                        last_err = f"HTTP {resp.status_code} fetching reference"
                    if attempt < len(backoffs):
                        wait = backoffs[attempt]
                        print(f"  ⚠ {filename} [{slot}]: {last_err} — retry {attempt+1}/{len(backoffs)} in {wait}s", flush=True)
                        time.sleep(wait)
                        continue
                    print(f"  ⚠ {filename} [{slot}]: {last_err}", flush=True)
                    break
                if not resp.ok:
                    # Other 4xx — retrying won't help.
                    last_err = f"HTTP {resp.status_code} fetching reference"
                    print(f"  ⚠ {filename} [{slot}]: {last_err}", flush=True)
                    break

                # v807: download to a temp name first, then rename with the
                # content hash suffix. The hash makes the gallery-reuse key
                # change whenever the underlying image changes (same role,
                # different operator pick) — see _content_hash8.
                tmp_file = os.path.join(work_dir, f".dl_{idx}{ext}")
                try:
                    with open(tmp_file, "wb") as f:
                        for chunk in resp.iter_content(chunk_size=8192):
                            f.write(chunk)
                    filename = f"{slug}__{_content_hash8(tmp_file)}{ext}"
                    local_file = os.path.join(work_dir, filename)
                    os.replace(tmp_file, local_file)
                except Exception:
                    # don't orphan the temp file on hash/rename/write failure
                    try:
                        os.remove(tmp_file)
                    except Exception:
                        pass
                    raise
                size_kb = os.path.getsize(local_file) / 1024
                print(f"  ⬇ {filename}  [{slot}]  ({size_kb:.0f} KB)", flush=True)
                results.append({
                    "path": local_file,
                    "filename": filename,
                    "role": role,
                })
                saved = True
                break
            except requests.exceptions.RequestException as e:
                last_err = f"Network error: {str(e)[:80]}"
                if attempt < len(backoffs):
                    wait = backoffs[attempt]
                    print(f"  ⚠ {filename} [{slot}]: {last_err} — retry {attempt+1}/{len(backoffs)} in {wait}s", flush=True)
                    time.sleep(wait)
                    continue
                print(f"  ⚠ {filename} [{slot}]: {last_err}", flush=True)
                break
            except Exception as e:
                last_err = f"Unexpected error: {str(e)[:80]}"
                print(f"  ⚠ {filename} [{slot}]: {last_err}", flush=True)
                break

        if not saved:
            missing.append({"filename": filename, "role": role,
                            "error": last_err or "unknown download error"})

    return results, missing


def _upload_variants_to_api(api_url, api_key, node_id, variant_paths):
    """POST /worker/jobs/{node_id}/variants with the variant files.

    v450: retries with exponential backoff on transient server drops.

    The webapp endpoint reads all files into memory (FastAPI UploadFile),
    saves them locally, then mirrors each to R2. On Render's free/starter
    tier this can trigger short OOM windows where the container is
    restarted; during that gap requests get RemoteDisconnected. A small
    number of retries bridges those windows automatically rather than
    failing the whole node.

    Retries on: ConnectionError, Timeout, HTTPError with 5xx status.
    Fails fast on: 4xx (client error — retrying won't help), unexpected
    exceptions.

    Backoff schedule: 2s, 5s, 15s between attempts (total up to ~22s of
    waiting across 4 attempts).
    """
    import requests
    url = f"{api_url.rstrip('/')}{API_PATH_PREFIX}/jobs/{node_id}/variants"
    headers = {"Authorization": f"Bearer {api_key}"}

    max_attempts = 4
    backoff_schedule = [2, 5, 15]  # seconds before attempts 2, 3, 4
    last_error = None

    for attempt in range(1, max_attempts + 1):
        # Re-open file handles on each attempt. requests consumes the
        # stream on the previous failed POST, so a reused handle reads
        # zero bytes on retry and the server sees an empty body.
        files = []
        opened = []
        try:
            for p in variant_paths:
                fh = open(p, "rb")
                opened.append(fh)
                files.append(("files", (os.path.basename(p), fh, "image/png")))

            resp = requests.post(url, headers=headers, files=files, timeout=300)

            # 4xx = permanent client error — abort, don't retry
            if 400 <= resp.status_code < 500:
                resp.raise_for_status()  # raises, caught below as HTTPError
            # 5xx handled below by raise_for_status too
            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.HTTPError as e:
            # HTTPError: 4xx = fail fast, 5xx = retry
            if e.response is not None and 400 <= e.response.status_code < 500:
                print(f"[API:http] ⚠ Upload variants returned {e.response.status_code} — no retry", flush=True)
                raise
            last_error = e
            print(f"[API:http] ⚠ Upload variants attempt {attempt}/{max_attempts} got {e.response.status_code if e.response else '?'} — will retry", flush=True)
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            last_error = e
            # Include error class name in log so we can tell drops from timeouts
            print(f"[API:http] ⚠ Upload variants attempt {attempt}/{max_attempts} failed: {type(e).__name__}: {e}", flush=True)
        except Exception as e:
            # Unexpected — don't retry, surface immediately
            print(f"[API:http] ✗ Upload variants attempt {attempt}/{max_attempts} unexpected error: {e}", flush=True)
            raise
        finally:
            for fh in opened:
                try:
                    fh.close()
                except Exception:
                    pass

        # Sleep before the next attempt (unless this was the last one)
        if attempt < max_attempts:
            wait_s = backoff_schedule[attempt - 1]
            print(f"[API:http]    waiting {wait_s}s before retry...", flush=True)
            time.sleep(wait_s)

    # All attempts exhausted
    raise last_error if last_error else RuntimeError(
        f"Upload variants for node {node_id} failed after {max_attempts} attempts"
    )


def _post_status(api_url, api_key, node_id, status, error=None):
    """POST /worker/jobs/{node_id}/status.

    v450: retries with exponential backoff on transient server drops.
    Shorter backoff than variant upload because the payload is a tiny
    JSON — if the server is healthy enough to respond at all, it should
    respond fast. Backoff: 1s, 3s, 7s.

    Same retry class as upload: ConnectionError, Timeout, 5xx. 4xx =
    fail fast. Unlike upload, we don't re-open streams so each attempt
    is self-contained.
    """
    import requests
    url = f"{api_url.rstrip('/')}{API_PATH_PREFIX}/jobs/{node_id}/status"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {"status": status, "error": error}

    max_attempts = 4
    backoff_schedule = [1, 3, 7]
    last_error = None

    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=30)
            if 400 <= resp.status_code < 500:
                resp.raise_for_status()
            resp.raise_for_status()
            return resp.json() if resp.content else {}

        except requests.exceptions.HTTPError as e:
            if e.response is not None and 400 <= e.response.status_code < 500:
                print(f"[API] ⚠ Post status returned {e.response.status_code} — no retry", flush=True)
                raise
            last_error = e
            print(f"[API] ⚠ Post status attempt {attempt}/{max_attempts} got {e.response.status_code if e.response else '?'} — will retry", flush=True)
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            last_error = e
            print(f"[API] ⚠ Post status attempt {attempt}/{max_attempts} failed: {type(e).__name__}: {e}", flush=True)
        except Exception as e:
            print(f"[API] ✗ Post status attempt {attempt}/{max_attempts} unexpected error: {e}", flush=True)
            raise

        if attempt < max_attempts:
            wait_s = backoff_schedule[attempt - 1]
            time.sleep(wait_s)

    raise last_error if last_error else RuntimeError(
        f"Post status for node {node_id} failed after {max_attempts} attempts"
    )


def create_new_flow_project(page, context=""):
    """Navigate to Flow home and create a new project. Each image job runs
    in a fresh project so Flow's Image/Video settings dropdown is available.

    Returns the new project URL on success, None on failure.
    """
    prefix = f"[{context}] " if context else ""

    # v755 — retry the whole land-on-home + click-New-project sequence.
    # Root cause of the prior single-shot failure: after spa_navigate falls
    # back to a hard page.goto, the home SPA isn't fully hydrated when we
    # click "New project". The click either misfires or lands on a non-
    # navigating button, so the URL never becomes a /project/ URL and the
    # caller hard-fails the whole node. Retrying — and on each retry forcing
    # a clean reload to Flow home + confirming the button is visible BEFORE
    # clicking — recovers from that transient state.
    new_btn_selectors = (
        "button:has-text('New project'), "
        "button:has-text('Nuovo progetto'), "
        "button:has-text('Nuevo proyecto'), "
        "button:has-text('Nouveau projet'), "
        "button:has-text('Neues Projekt'), "
        "button:has(i:text('add_2'))"
    )
    MAX_ATTEMPTS = 3
    last_url = ""

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            # Make sure we're actually on Flow home before clicking. First
            # attempt uses the SPA-preserving nav; retries hard-reload home
            # so we never click against a stale/half-hydrated project page.
            if attempt == 1:
                spa_navigate_to_flow_home(page, context or "NEW_PROJECT")
            else:
                print(f"{prefix}↻ Retry {attempt}/{MAX_ATTEMPTS} — reloading Flow home before re-clicking New project (last URL: {last_url})", flush=True)
                try:
                    page.goto(FLOW_HOME_URL, wait_until="domcontentloaded", timeout=30000)
                except Exception:
                    pass
                human_delay(3, 5)
            human_delay(1, 2)

            ensure_logged_into_flow(page, context or "NEW_PROJECT")
            check_and_dismiss_popup(page)

            # Human-like "looking around" before first interaction
            human_mouse_move(page)
            human_delay(1, 2)

            # Click "New project" — matches English/Italian/Spanish/French/German
            # labels as well as the '+' icon button variant
            dismiss_create_with_flow(page, context or "NEW_PROJECT")

            # v755 — confirm the New project button is present + visible before
            # clicking. A click against a not-yet-hydrated home page is the main
            # cause of "clicked but URL never changed to /project/".
            try:
                btn = page.locator(new_btn_selectors).first
                btn.wait_for(state="visible", timeout=15000)
            except Exception:
                last_url = page.url
                print(f"{prefix}⚠ New project button not visible (attempt {attempt}/{MAX_ATTEMPTS}, URL: {last_url}) — retrying", flush=True)
                continue

            print(f"{prefix}Creating new Flow project... (attempt {attempt}/{MAX_ATTEMPTS})", flush=True)
            human_click_element(page, new_btn_selectors, f"{prefix}New project button")
            human_delay(2, 3)

            # Wait for URL to become a project URL
            try:
                page.wait_for_url("**/project/**", timeout=30000)
            except Exception:
                # Fallback: poll for a while
                for _ in range(15):
                    time.sleep(1)
                    if "/project/" in page.url:
                        break

            time.sleep(2)
            project_url = page.url

            if "/project/" in project_url:
                print(f"{prefix}✓ Created project: {project_url}", flush=True)
                human_delay(1, 2)
                check_and_dismiss_popup(page)
                # v839 — the DOM create path had NO Agent-OFF (only the API create
                # path ran the HAR-replay init). Force Agent OFF here too, or the
                # new project opens in Agent mode → Settings gear hidden →
                # "Settings button not found". Matches the video worker.
                force_agent_off(page, context=context)
                return project_url

            last_url = project_url
            print(f"{prefix}⚠ New project click didn't navigate (attempt {attempt}/{MAX_ATTEMPTS}) — current URL: {project_url}", flush=True)

        except Exception as e:
            try:
                last_url = page.url
            except Exception:
                pass
            print(f"{prefix}⚠ Error creating project (attempt {attempt}/{MAX_ATTEMPTS}): {e}", flush=True)
            import traceback
            traceback.print_exc()

    print(f"{prefix}❌ Failed to create project after {MAX_ATTEMPTS} attempts — last URL: {last_url}", flush=True)
    return None


def api_pull_mode(page, api_url, api_key, worker_id=None):
    """Poll the webapp API for pending image jobs and process them.
    Replaces watch_folder_mode for cross-machine deployments."""
    import requests
    if not worker_id:
        import socket
        worker_id = f"image-worker-{socket.gethostname()}"

    print(f"\n{'=' * 60}")
    print(f"HTTP-PULL API MODE")
    print(f"{'=' * 60}")
    print(f"API URL:   {api_url}")
    print(f"Worker ID: {worker_id}")
    print(f"Poll:      every {API_POLL_INTERVAL}s when idle")
    print(f"Ctrl+C to stop.")
    print(f"{'=' * 60}\n", flush=True)

    # Connectivity check first
    try:
        health = _api_request(api_url, api_key, "GET", "/health", timeout=10)
        print(f"[API] ✓ Connected: {health}", flush=True)
    except Exception as e:
        print(f"[API] ❌ Can't reach {api_url}: {e}", flush=True)
        print(f"[API] Make sure the URL is correct and the webapp is running.", flush=True)
        return

    # Release any claims this worker owned from a previous crashed run. Without
    # this, nodes left 'generating' wait for the 10-minute TTL sweep in
    # /jobs/pending before they re-enter the queue.
    try:
        rc = _api_request(api_url, api_key, "POST", "/release-claims",
                          params={"worker_id": worker_id}, timeout=10)
        if rc and rc.get("released"):
            print(f"[API] ↩ Released {rc['released']} stale claim(s) from previous run", flush=True)
    except Exception as e:
        print(f"[API] ⚠ Couldn't release stale claims: {e}", flush=True)

    tmp_root = os.path.join(tempfile.gettempdir(), "image_worker_api")
    os.makedirs(tmp_root, exist_ok=True)

    # Persistent state file — survives worker restarts so we can reuse the
    # Flow project across crashes/reboots. Stored next to the worker script
    # under the user home so it's stable across Python temp cleanups.
    try:
        state_root = os.path.join(os.path.expanduser("~"), "KavenoImageWorker")
    except Exception:
        state_root = tmp_root
    os.makedirs(state_root, exist_ok=True)
    state_file = os.path.join(state_root, "worker_state.json")

    def _save_state():
        """Write current_project_url + current_job_key + uploaded_in_project to disk.

        v541 — Also persists ``projects`` (job_key → project_info), so when
        we revisit an older job key its previously-created Flow project is
        reused instead of orphaned. The legacy top-level keys are kept for
        backward compatibility with older readers; they always mirror the
        currently-active entry in ``projects[current_job_key]``.
        """
        try:
            # Sync the dict before writing so the legacy fields and the
            # dict can never diverge.
            if current_job_key:
                projects_dict[current_job_key] = {
                    "url": current_project_url,
                    "uploaded": sorted(uploaded_in_project),
                    "last_used_at": time.time(),
                }
            with open(state_file, "w", encoding="utf-8") as f:
                json.dump({
                    "current_project_url": current_project_url,
                    "current_job_key": current_job_key,
                    "uploaded_in_project": sorted(uploaded_in_project),
                    "projects": projects_dict,
                }, f, indent=2)
        except Exception as e:
            print(f"[API] ⚠ Couldn't save worker state: {e}", flush=True)

    def _load_state():
        """Read saved state. Returns (project_url, job_key, uploaded_set, projects_dict).

        v541 — Returns a 4-tuple now. The 4th element is a dict of
        all known job_key → {url, uploaded, last_used_at} pairs. Legacy
        state files without that key produce an empty dict and the
        reuse path naturally falls through to create-new on first
        revisit.
        """
        if not os.path.exists(state_file):
            return None, None, set(), {}
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            url = data.get("current_project_url")
            key = data.get("current_job_key")
            uploaded = set(data.get("uploaded_in_project") or [])
            projects = data.get("projects") or {}
            # Defensive normalisation: each entry must have a url key.
            # Older saves with the legacy schema get back-filled from
            # the top-level fields below.
            if not isinstance(projects, dict):
                projects = {}
            if url and key and key not in projects:
                projects[key] = {
                    "url": url,
                    "uploaded": sorted(uploaded),
                    "last_used_at": time.time(),
                }
            return url, key, uploaded, projects
        except Exception as e:
            print(f"[API] ⚠ Couldn't load worker state ({e}), starting fresh", flush=True)
            return None, None, set(), {}

    consecutive_errors = 0

    # Worker-level state: one Flow project per "job" (scene-batch import).
    # When the next claimed job has a different job key, we look up that
    # key in ``projects_dict`` and reuse the existing project if found.
    # Only create a new project when the key is genuinely new.
    current_project_url, current_job_key, uploaded_in_project, projects_dict = _load_state()
    # v541 — cap the dict so revisited jobs are kept but the worker
    # state file doesn't grow without bound. 20 is generous for a
    # human operator; older entries beyond that are evicted by LRU.
    PROJECTS_LRU_CAP = 20
    if current_project_url:
        print(f"[API] ♻ Loaded persisted project: {current_project_url}", flush=True)
        print(f"[API]   Job key: {current_job_key}", flush=True)
        print(f"[API]   Known uploaded files: {len(uploaded_in_project)}", flush=True)
    else:
        print(f"[API] (no persisted project found — will create one on first job)", flush=True)
    if projects_dict:
        print(f"[API] ♻ Loaded {len(projects_dict)} known job → project mapping(s)", flush=True)

    # Background heartbeat thread. Main loop gets blocked for 30-60+ seconds
    # during a job (uploading refs + generating + downloading variants), so
    # inline heartbeats inside the loop aren't frequent enough to keep the
    # UI's "online" indicator stable. This daemon thread pings every 5s
    # independent of whatever the main loop is doing.
    import threading
    heartbeat_stop = threading.Event()

    def _heartbeat_loop():
        while not heartbeat_stop.is_set():
            try:
                _api_request(api_url, api_key, "POST", "/heartbeat",
                             json={}, params={"worker_id": worker_id}, timeout=8)
            except Exception:
                # Swallow silently — main loop's heartbeat will still log
                # real connectivity problems
                pass
            heartbeat_stop.wait(5.0)

    hb_thread = threading.Thread(target=_heartbeat_loop, name="worker-heartbeat", daemon=True)
    hb_thread.start()
    print(f"[API] ♥ Background heartbeat thread started (every 5s)", flush=True)

    try:
        consecutive_timeouts = 0
        while True:
            # Claim next job. Heartbeats are handled by the background
            # thread, so the main loop just focuses on claiming + processing.
            try:
                resp = _api_request(
                    api_url, api_key, "GET", "/jobs/pending",
                    params={"worker_id": worker_id}, timeout=10,
                )
                consecutive_errors = 0
                consecutive_timeouts = 0
                job = resp.get("job") if isinstance(resp, dict) else None
            except requests.exceptions.HTTPError as he:
                status = he.response.status_code if he.response is not None else 0
                if status == 401:
                    print(f"[API] ❌ 401 Unauthorized — wrong API key.", flush=True)
                    print(f"[API] Check LOCAL_WORKER_API_KEY env var on the webapp matches --api-key.", flush=True)
                    return
                print(f"[API] HTTP error {status}: {he}", flush=True)
                consecutive_errors += 1
                time.sleep(min(30, API_POLL_INTERVAL * (2 ** min(consecutive_errors, 5))))
                continue
            except (requests.exceptions.Timeout,
                    requests.exceptions.ReadTimeout,
                    requests.exceptions.ConnectTimeout,
                    requests.exceptions.ConnectionError) as e:
                # v471b: handle timeouts separately from other errors.
                # Consecutive timeouts suggest a network stall — back
                # off more aggressively (up to 60s between polls) so we
                # don't hammer a busy/unavailable server. We still keep
                # polling so the worker auto-recovers without a restart.
                # v550: ConnectionError added — urllib3's "Max retries
                # exceeded ... Caused by ReadTimeoutError" wrapper is
                # NOT a subclass of requests.Timeout. Without this catch
                # those errors fell through to the generic Exception
                # handler and printed a full traceback every poll.
                consecutive_timeouts += 1
                if consecutive_timeouts == 1:
                    print(f"[API] ⏱ Poll timeout — server slow or network stall. Will keep retrying with backoff.", flush=True)
                elif consecutive_timeouts % 5 == 0:
                    backoff = min(60, 5 * (2 ** min(consecutive_timeouts - 1, 4)))
                    print(f"[API] ⏱ Still timing out after {consecutive_timeouts} attempts (next backoff {backoff}s) — server may be down or restarting.", flush=True)
                backoff = min(60, 5 * (2 ** min(consecutive_timeouts - 1, 4)))
                time.sleep(backoff)
                continue
            except Exception as e:
                print(f"[API] Poll error: {e}", flush=True)
                consecutive_errors += 1
                time.sleep(min(30, API_POLL_INTERVAL * (2 ** min(consecutive_errors, 5))))
                continue

            if not job:
                time.sleep(API_POLL_INTERVAL)
                continue

            node_id = job.get("id")
            node_name = job.get("name", "")
            prompt = job.get("prompt", "")
            variants = int(job.get("variants") or 1)
            aspect_ratio = job.get("aspect_ratio", "16:9")
            resolution = job.get("resolution", "1K")
            model = job.get("model", "nano_banana_2")
            # New format (v364+): list of dicts with filename + role + url.
            # Fallback to old flat URL list for older servers.
            input_images = job.get("input_images")
            if input_images is None:
                input_images = job.get("input_image_urls") or []

            # Derive a "job key" from the node name so the worker can keep
            # one Flow project per import batch. Names like "Vinegar — Scene 3"
            # share the prefix "Vinegar —" with their sibling scenes; all
            # children of that import belong to the same job and share the
            # same Flow project. Uploads and unmatched names fall into their
            # own buckets so they don't poison scene-batch projects.
            def _derive_job_key(nm):
                import re
                m = re.match(r"^(.*?)Scene\s+\d+", nm or "", re.IGNORECASE)
                if m:
                    prefix = m.group(1).strip(" -—:\t")
                    # v749 — never collapse empty-prefix scene names to a
                    # shared "(untitled)" bucket. Two unrelated batches
                    # whose names happen to start with bare "Scene N"
                    # would share a Flow project and risk cross-batch
                    # contamination. Fall back to standalone:: keyed by
                    # the full name so each unprefixed node lands in its
                    # own bucket. Wasteful (one Flow project per scene)
                    # but safe. The platform-side v749 fix in
                    # image_platform.py prevents this path from firing
                    # for fresh imports — kept defensive for legacy
                    # nodes already in the DB without a batch label.
                    if not prefix:
                        return f"standalone::{nm}"
                    return f"scene-batch::{prefix}"
                return f"standalone::{nm or 'unnamed'}"

            new_job_key = _derive_job_key(node_name)

            print(f"\n[API] → Claimed job: node {node_id}" + (f" ({node_name})" if node_name else ""), flush=True)
            print(f"       Prompt:  {prompt[:80]}{'...' if len(prompt) > 80 else ''}", flush=True)
            print(f"       Inputs:  {len(input_images)} reference image(s)", flush=True)
            print(f"       Output:  {variants} variant(s) @ {aspect_ratio}/{resolution}/{model}", flush=True)
            print(f"       Job key: {new_job_key}", flush=True)

            # Prepare per-job working directory
            job_work = os.path.join(tmp_root, f"node_{node_id}")
            if os.path.exists(job_work):
                try:
                    shutil.rmtree(job_work)
                except Exception:
                    pass
            os.makedirs(job_work, exist_ok=True)
            out_dir = os.path.join(job_work, "out")
            os.makedirs(out_dir, exist_ok=True)

            try:
                # Download reference inputs — returns (results, missing).
                # If all references fail (e.g. parent variants got regenerated
                # and their worker-files tokens are stale), mark the node
                # failed with a clear reason and move on to the next job.
                input_items = []
                missing_refs = []
                if input_images:
                    input_items, missing_refs = _download_reference_inputs(
                        api_key, input_images, job_work)

                if input_images and not input_items:
                    # All references failed → can't run this job
                    err = f"All {len(input_images)} reference image(s) could not be downloaded"
                    if missing_refs:
                        reasons = {m.get("error", "unknown") for m in missing_refs}
                        err += f": {'; '.join(reasons)}"
                    print(f"[API] ✗ Node {node_id}: {err}", flush=True)
                    _post_status(api_url, api_key, node_id, "failed", error=err)
                    continue

                if missing_refs:
                    print(f"[API] ⚠ Node {node_id}: {len(missing_refs)} reference(s) missing, proceeding with {len(input_items)}", flush=True)

                input_paths = [it["path"] for it in input_items]

                # --- Project reuse: per-job isolation ---
                # We reuse the Flow project only for jobs with the same
                # job key (same import batch). When we see a new job key,
                # we look it up in ``projects_dict`` first — if we've
                # touched that key before in a previous loop iteration
                # we reuse its already-created project, otherwise we
                # spin up a fresh one. This prevents the bug where
                # interleaving Job A → Job B → Job A would orphan the
                # original Job A project and create a third one.
                need_new_project = False
                if current_project_url is None:
                    need_new_project = True
                elif current_job_key != new_job_key:
                    # Persist the about-to-be-displaced job's state into the
                    # dict so a future revisit can find it.
                    if current_job_key:
                        projects_dict[current_job_key] = {
                            "url": current_project_url,
                            "uploaded": sorted(uploaded_in_project),
                            "last_used_at": time.time(),
                        }
                    print(f"[node_{node_id}] Switching from job '{current_job_key}' to '{new_job_key}'", flush=True)

                    # v541 — try to find a previously-created project for
                    # this incoming key.
                    prior = projects_dict.get(new_job_key)
                    if prior and prior.get("url"):
                        prior_url = prior["url"]
                        print(f"[node_{node_id}] ♻ Found prior project for '{new_job_key}' — attempting reuse: {prior_url}", flush=True)
                        try:
                            page.goto(prior_url, wait_until="domcontentloaded", timeout=30000)
                            try:
                                create_btn = page.locator(
                                    "button[aria-haspopup='dialog']:has(i:text('add_2'))"
                                ).first
                                create_btn.wait_for(state="visible", timeout=20000)
                                # Project still alive — adopt it.
                                current_project_url = prior_url
                                current_job_key = new_job_key
                                uploaded_in_project = set(prior.get("uploaded") or [])
                                prior["last_used_at"] = time.time()
                                projects_dict[new_job_key] = prior
                                _save_state()
                                print(f"[node_{node_id}] ✓ Reused prior project for '{new_job_key}'", flush=True)
                            except Exception:
                                # UI didn't hydrate — project may have been
                                # deleted in the Flow account. Fall through to
                                # creating a new one and drop the stale entry.
                                print(f"[node_{node_id}] ⚠ Prior project didn't hydrate — discarding stale entry", flush=True)
                                projects_dict.pop(new_job_key, None)
                                need_new_project = True
                                current_project_url = None
                                uploaded_in_project = set()
                        except Exception as nav_e:
                            print(f"[node_{node_id}] ⚠ Prior project unreachable ({nav_e}) — discarding stale entry", flush=True)
                            projects_dict.pop(new_job_key, None)
                            need_new_project = True
                            current_project_url = None
                            uploaded_in_project = set()
                    else:
                        # First time we've seen this key.
                        need_new_project = True
                        current_project_url = None
                        uploaded_in_project = set()
                    if need_new_project:
                        _save_state()
                else:
                    # Same job — reload the project page to reset transient
                    # UI state between scenes: previous reference chips, open
                    # dialogs, scrolled virtuoso state, cached popovers — all
                    # gone. Flow re-renders the project fresh. Costs ~1-2s.
                    try:
                        print(f"[node_{node_id}] Reloading project for clean state...", flush=True)
                        page.goto(current_project_url, wait_until="domcontentloaded", timeout=30000)
                        # Wait for Flow's editor to hydrate — the Create (add_2)
                        # button appearing is a reliable signal that the UI
                        # is interactive and settings are accessible. Without
                        # this we can race ahead before React has mounted.
                        try:
                            create_btn = page.locator(
                                "button[aria-haspopup='dialog']:has(i:text('add_2'))"
                            ).first
                            create_btn.wait_for(state="visible", timeout=20000)
                            print(f"[node_{node_id}] ✓ Project UI hydrated", flush=True)
                        except Exception:
                            print(f"[node_{node_id}] ⚠ UI didn't hydrate — falling back to 5s sleep", flush=True)
                            time.sleep(5)
                    except Exception as nav_e:
                        print(f"[node_{node_id}] ⚠ Couldn't return to project ({nav_e}), will create new one", flush=True)
                        # The current project is unreachable; drop it from the
                        # dict too so we don't try to reuse a dead URL later.
                        if current_job_key:
                            projects_dict.pop(current_job_key, None)
                        need_new_project = True
                        current_project_url = None
                        uploaded_in_project = set()
                        _save_state()

                if need_new_project:
                    project_url = create_new_flow_project(page, context=f"node_{node_id}")
                    if not project_url:
                        raise RuntimeError("Could not create Flow project for this job")
                    current_project_url = project_url
                    current_job_key = new_job_key
                    uploaded_in_project = set()
                    # v541 — register in the dict and apply LRU eviction.
                    projects_dict[new_job_key] = {
                        "url": project_url,
                        "uploaded": [],
                        "last_used_at": time.time(),
                    }
                    if len(projects_dict) > PROJECTS_LRU_CAP:
                        # Drop the oldest entries to stay under cap.
                        sorted_by_age = sorted(
                            projects_dict.items(),
                            key=lambda kv: (kv[1] or {}).get("last_used_at", 0),
                        )
                        to_drop = len(projects_dict) - PROJECTS_LRU_CAP
                        for evict_key, _ in sorted_by_age[:to_drop]:
                            if evict_key != new_job_key and evict_key != current_job_key:
                                projects_dict.pop(evict_key, None)
                                print(f"[node_{node_id}] LRU evicted job key '{evict_key}'", flush=True)
                    _save_state()
                    print(f"[node_{node_id}] ✓ Created new project for job '{new_job_key}'", flush=True)
                else:
                    print(f"[node_{node_id}] ♻ Reusing project for job '{current_job_key}': {current_project_url}", flush=True)

                # Run Flow UI generation
                success, saved_filenames, error = process_image_job_multi(
                    page,
                    input_paths=input_paths,
                    prompt=prompt,
                    output_dir=out_dir,
                    variants=variants,
                    aspect_ratio=aspect_ratio,
                    resolution=resolution,
                    model=model,
                    context=f"node_{node_id}",
                    already_uploaded=uploaded_in_project,
                )

                if success and saved_filenames:
                    variant_paths = [os.path.join(out_dir, f) for f in saved_filenames]
                    print(f"[API] ⬆ Uploading {len(variant_paths)} variant(s)...", flush=True)
                    _upload_variants_to_api(api_url, api_key, node_id, variant_paths)
                    _post_status(api_url, api_key, node_id, "completed")
                    print(f"[API] ✓ Node {node_id} marked completed", flush=True)
                else:
                    err = error or "Generation failed"
                    _post_status(api_url, api_key, node_id, "failed", error=err)
                    print(f"[API] ✗ Node {node_id} marked failed: {err}", flush=True)
            except Exception as e:
                import traceback
                traceback.print_exc()
                try:
                    _post_status(api_url, api_key, node_id, "failed",
                                 error=f"Worker exception: {e}")
                except Exception:
                    pass
            finally:
                # Persist state after every job so uploaded_in_project
                # (mutated inside upload_reference_images) survives crashes
                _save_state()
                # Clean up per-job temp dir
                try:
                    shutil.rmtree(job_work)
                except Exception:
                    pass

            time.sleep(API_POLL_BUSY_INTERVAL)
    except KeyboardInterrupt:
        print("\n[API] Stopping on user request...", flush=True)
    finally:
        # v516: tell the webapp we're going offline so the UI flips
        # to "● Offline" within ~2s instead of waiting for the 10s
        # heartbeat-stale window. Best-effort with short timeout.
        try:
            rc = _api_request(api_url, api_key, "POST", "/release-claims",
                              params={"worker_id": worker_id, "going_offline": "true"},
                              timeout=3)
            if rc and rc.get("heartbeat_deleted"):
                print("[API] ↩ Notified webapp of graceful shutdown", flush=True)
        except Exception as e:
            print(f"[API] ⚠ Couldn't notify shutdown (UI will flip in ~10s): {e}", flush=True)
        # Signal the background heartbeat thread to stop
        heartbeat_stop.set()


# ============================================================
# PARALLEL MODE — submit/download split (ported from flow_worker.py)
# ============================================================
# PARALLEL MODE — main-thread Playwright + pure-HTTP download worker
# ============================================================
#
# Pattern (mirrors the current video worker design in static/flow_worker.py,
# specifically its _http_download_worker + main-thread scanning model):
#   - Main thread owns the browser page. It both submits new jobs AND scans
#     the gallery for completed tiles. Every Playwright call happens here,
#     avoiding greenlet thread-affinity errors.
#   - Background HTTP thread handles downloads and uploads using plain
#     requests.Session calls. Never touches Playwright.
#   - Two queues connect them: http_queue (main → worker) carries {urls,
#     output_dir, cookies} tuples; done_queue (worker → main) reports
#     completions so the main thread can clean up and free slots.
#   - Parallelism source: while Flow renders job N (15-40s server-side),
#     the main thread submits job N+1 (5-15s local) and scans for completed
#     earlier tiles. At any moment up to `parallel_slots` generations are
#     in flight on Flow's side.

def _derive_prompt_key(full_prompt, max_chars=800):
    """Extract the scene-specific portion of a prompt for tile attribution.

    The global prompt prefix (POSITIVE / NEGATIVE quality rules) is appended
    to every scene's prompt during import. If we matched on the full prompt
    text, every tile in the gallery would match every pending submission
    because they all share the prefix.

    This function strips the prefix. The prefix starts at a line that begins
    with '* POSITIVE' or 'POSITIVE' — everything before is scene-specific.
    If no prefix marker is found, we just cap the raw prompt at max_chars.

    v733 — max_chars bumped 300 → 800. Lift artifacts with v703 manifest +
    v581 binding line + v589.1 chain line + opening Composition phrase
    consumed nearly all of the previous 300-char window, leaving only the
    chain-image-K token as the per-scene disambiguator. Two scenes that
    chained off the same image_K (multi-clip via v698A or sibling scenes
    both chaining off the same anchor) collapsed to identical prompt_keys
    → Strategy 1 longest-match resolved by dict iteration order →
    non-deterministic legacy-fallback attribution. 800 catches per-scene
    Action / Subject body prose that's genuinely unique per scene.
    Callers use substring match which is monotonic in key length —
    strictly safer than the old cap.

    Returns a string suitable for substring matching against tile DOM text.
    """
    if not full_prompt:
        return ""
    # Look for the prefix boundary, in order of specificity
    markers = ["\n* POSITIVE", "\n*POSITIVE", "\nPOSITIVE",
               "* POSITIVE", "*POSITIVE"]
    boundary = -1
    for marker in markers:
        idx = full_prompt.find(marker)
        if idx != -1:
            boundary = idx
            break
    if boundary == -1:
        # No prefix → whole prompt is scene-specific
        key = full_prompt
    else:
        key = full_prompt[:boundary]
    # Strip, cap, and return
    key = key.strip()
    if len(key) > max_chars:
        key = key[:max_chars]
    return key


def scan_gallery_containers(page, max_index=25, context=""):
    """Scroll through and read every gallery container. Ported from
    flow_worker.py's _scan_all_containers with adaptations for image
    generation tiles.

    Flow uses react-virtuoso for the gallery — children of each container
    are only rendered when the container is near the viewport. We must
    scroll to each index individually and read it while it's visible.

    Returns a list of dicts, one per container (or empty list on error):
        {
            data_index: int,
            has_completed_image: bool,  # True only when NO %-indicator
                                         # AND at least one committed tile
            committed_tile_count: int,   # number of completed tiles in this
                                         # container — use this to decide
                                         # whether all N variants are ready
            still_rendering: bool,       # True if any %-indicator present
                                         # (we should skip matching this
                                         # container until it's stable)
            has_failed: bool,
            prompt_text: str,
            tile_image_urls: [str, ...],
            tile_edit_hrefs: [str, ...],
        }
    """
    prefix = f"[{context}] " if context else ""
    results = []

    # Reset scroll to the top of the gallery before scanning. New tiles
    # prepend at data-index=0 — if the previous scan left the viewport
    # scrolled to the bottom, virtuoso won't have index=0 mounted and
    # the scanner will miss the newest completions entirely.
    #
    # v481: scrollTo on window doesn't move Virtuoso's internal scroller.
    # Find the actual Virtuoso scroll container (the element that owns
    # the [data-index] children) and scroll IT to the top. If that
    # element is gone or Virtuoso isn't using the expected structure,
    # fall back to the window scroll + a wheel-up tick.
    try:
        page.evaluate("""() => {
            // Find the scroll parent of any [data-index] element
            const anyItem = document.querySelector('[data-index]');
            if (anyItem) {
                let el = anyItem.parentElement;
                while (el) {
                    const style = window.getComputedStyle(el);
                    const overflowY = style.overflowY;
                    if (el.scrollHeight > el.clientHeight &&
                        (overflowY === 'auto' || overflowY === 'scroll')) {
                        el.scrollTop = 0;
                        return;
                    }
                    el = el.parentElement;
                }
            }
            // Fallback: window scroll + common virtuoso data attr selectors
            window.scrollTo(0, 0);
            const virtuoso = document.querySelector('[data-testid="virtuoso-scroller"], [data-test-id="virtuoso-scroller"]');
            if (virtuoso) virtuoso.scrollTop = 0;
        }""")
        time.sleep(0.2)
        # If no [data-index] is mounted at all, try a wheel-up on the
        # viewport's center to coax Virtuoso into mounting the top.
        try:
            has_idx0 = page.locator("div[data-index='0']").count() > 0
        except Exception:
            has_idx0 = True  # assume yes; don't wheel unnecessarily
        if not has_idx0:
            try:
                page.mouse.wheel(0, -2000)
                time.sleep(0.25)
            except Exception:
                pass
    except Exception:
        pass

    try:
        for idx in range(max_index + 1):
            container = page.locator(f"div[data-index='{idx}']")
            if container.count() == 0:
                # Try ONE wheel tick to force virtuoso to mount the next
                # item. If it still doesn't exist, stop — we've walked past
                # the end of the gallery. Repeated wheel ticks past the end
                # were pinning the page scrolled to the bottom, which then
                # prevented the next scan from seeing data-index=0 until we
                # scrolled back to the top again.
                try:
                    page.mouse.wheel(0, 400)
                    time.sleep(0.25)
                except Exception:
                    pass
                if container.count() == 0:
                    break  # No more containers — stop scanning

            # Scroll into view so virtuoso renders the tile's children
            try:
                container.first.scroll_into_view_if_needed(timeout=2000)
                time.sleep(0.15)
            except Exception:
                pass

            # Read the container's state via JS in a single evaluate call
            info = page.evaluate(f"""
                () => {{
                    const c = document.querySelector("div[data-index='{idx}']");
                    if (!c) return null;
                    const text = c.innerText || '';
                    // Skip containers that represent uploaded reference images.
                    // These show up at data-index=0 (prepended like any new
                    // gallery item) with img[alt='Generated image'] and an
                    // /edit/ link — indistinguishable from real generations
                    // except for the label "Uploaded image" in their inner
                    // text. Without this guard the scanner will "match" a
                    // freshly-uploaded subject reference as if it were the
                    // generation result of the in-flight submission and
                    // download the reference file as the output.
                    if (text.includes('Uploaded image')) return null;
                    // v481: detecting "still rendering" state.
                    // Previously this was `/\\d+\\s*%/.test(text)` on the
                    // whole container innerText. That was way too greedy
                    // — Flow's UI has percentage-like text in many chrome
                    // elements (quality badges, tooltips, menu items)
                    // that don't indicate active generation. The old
                    // check kept completed containers classified as
                    // "rendering" and the scanner skipped them forever.
                    //
                    // New detection: look for percentage text *inside*
                    // an element that actually represents progress state.
                    // Genuine Flow progress indicators:
                    //   - elements with role="progressbar"
                    //   - elements whose class or data attr includes
                    //     "progress", "loading", "generating", "rendering"
                    //   - elements that contain only "NN%" as their text
                    //     (no other content)
                    // Failed tiles show "Failed" text and a warning icon
                    // INSTEAD of a progress indicator, so a "Failed"
                    // label + a % in the same container means that tile
                    // is in a retry/policy-violation state — still count
                    // that as rendering so we don't attribute prematurely.
                    let hasPercentage = false;
                    try {{
                        // Any explicit progress element
                        if (c.querySelector('[role="progressbar"]')) hasPercentage = true;
                        if (!hasPercentage) {{
                            const progEls = c.querySelectorAll(
                                '[class*="progress" i], [class*="loading" i], [class*="generating" i], [class*="rendering" i]'
                            );
                            for (const el of progEls) {{
                                const t = (el.innerText || '').trim();
                                if (/\\d+\\s*%/.test(t)) {{ hasPercentage = true; break; }}
                            }}
                        }}
                        // Fallback: a small element whose ONLY text is "NN%"
                        // is almost certainly a progress indicator
                        if (!hasPercentage) {{
                            const allEls = c.querySelectorAll('div,span');
                            for (const el of allEls) {{
                                const t = (el.innerText || '').trim();
                                if (!t) continue;
                                if (t.length > 10) continue; // too long to be a bare "45%" indicator
                                if (/^\\d+\\s*%$/.test(t)) {{ hasPercentage = true; break; }}
                            }}
                        }}
                    }} catch (_e) {{
                        // If the progress query fails, fall back to the
                        // old broad regex so we err toward skipping
                        // rather than attributing prematurely.
                        hasPercentage = /\\d+\\s*%/.test(text);
                    }}
                    const hasFailedText = (text.includes('Failed') || text.includes('Error'));

                    // Completed image: wrapped in /edit/ link, src has the redirect URL
                    const imgs = c.querySelectorAll("img[alt='Generated image']");
                    const completedImgs = [];
                    const editHrefs = [];
                    for (const img of imgs) {{
                        if (!img.src) continue;
                        if (!img.src.includes('media.getMediaUrlRedirect')) continue;
                        const link = img.closest("a[href*='/edit/']");
                        if (!link) continue;
                        completedImgs.push(img.src);
                        editHrefs.push(link.href);
                    }}
                    const committedCount = completedImgs.length;
                    // A container is "done" only when NO tile is still rendering
                    // AND we have at least one committed tile.
                    const hasCompleted = committedCount > 0 && !hasPercentage;
                    const stillRendering = hasPercentage;
                    const hasFailed = hasFailedText && !hasPercentage && committedCount === 0;
                    // Only care about containers in some interesting state
                    if (!hasCompleted && !hasFailed && !stillRendering) return null;

                    // Extract the longest text element — this holds the prompt.
                    // Anything < 50 chars is likely UI chrome, not the prompt.
                    const candidates = c.querySelectorAll('div, button, span, a, p');
                    let bestPrompt = '';
                    let bestLen = 0;
                    for (const el of candidates) {{
                        const t = (el.innerText || '').trim();
                        if (t.length < 50) continue;
                        if (t.length > bestLen) {{
                            bestPrompt = t;
                            bestLen = t.length;
                        }}
                    }}
                    if (!bestPrompt) bestPrompt = text.substring(0, 1500);

                    return {{
                        data_index: {idx},
                        has_completed_image: hasCompleted,
                        committed_tile_count: committedCount,
                        still_rendering: stillRendering,
                        has_failed: hasFailed,
                        prompt_text: bestPrompt,
                        tile_image_urls: completedImgs,
                        tile_edit_hrefs: editHrefs,
                    }};
                }}
            """)
            if info:
                results.append(info)

    except Exception as e:
        print(f"{prefix}[scan] Container scan error: {e}", flush=True)

    return results


def lookup_tiles_by_id(page, tile_ids):
    """v521: submission-first attribution — given a list of tile UUIDs
    captured at submit time, return their current state in the DOM.

    Returns a dict keyed by tile_id with values:
      {
        'status': 'rendering' | 'ready' | 'failed' | 'not_found',
        'image_url': str | None  (only when status == 'ready')
      }

    Implementation notes:
      - Flow's tile DOM keeps BOTH the loading-skeleton layer AND the
        failure-overlay layer mounted simultaneously, switching between
        them via CSS opacity. So innerText and querySelectorAll('i')
        both return content from BOTH layers regardless of which is
        actually visible to the user. v521.1 (this revision) checks
        computed opacity to ignore the hidden layer:
          * Rendering tile: failure overlay opacity=0, skeleton opacity=1
          * Failed tile: failure overlay opacity=1, skeleton opacity=0
          * Ready tile: <img> with media URL is rendered
      - 'ready' is detected by an <img src> matching the
        getMediaUrlRedirect pattern that Flow uses for completed images.
      - 'failed' is detected by literal "Failed" text inside an element
        whose nearest ancestor with an opacity style has opacity > 0.
      - 'rendering' is the default when neither ready nor failed apply.
      - 'not_found' means the tile isn't currently in the DOM. Tiles can
        temporarily disappear during gallery virtualization. Treat
        not_found as "still rendering" until the submission ages past
        STUCK_TIMEOUT.
    """
    if not tile_ids:
        return {}
    try:
        return page.evaluate("""(ids) => {
            // Walk up to the nearest ancestor with an opacity style and
            // return its computed opacity. Default 1.0 if none found.
            const visibleOpacity = (el) => {
                let cur = el;
                while (cur && cur !== document.body) {
                    const inline = cur.style && cur.style.opacity;
                    if (inline !== undefined && inline !== '') {
                        return parseFloat(inline);
                    }
                    cur = cur.parentElement;
                }
                return 1.0;
            };
            const out = {};
            for (const id of ids) {
                const tile = document.querySelector(`[data-tile-id="${id}"]`);
                if (!tile) {
                    out[id] = {status: 'not_found'};
                    continue;
                }
                // 1. READY — an <img> with a getMediaUrlRedirect URL
                //    inside the tile, AND that <img> is in a visible
                //    layer (opacity > 0).
                const img = tile.querySelector('img[src*="getMediaUrlRedirect"]');
                if (img && img.src) {
                    if (visibleOpacity(img) > 0.01) {
                        out[id] = {status: 'ready', image_url: img.src};
                        continue;
                    }
                }
                // 2. FAILED — find a "Failed" text node inside a VISIBLE
                //    layer (opacity > 0). Both layers (skeleton and
                //    failure overlay) coexist in the DOM, so we have
                //    to check the visibility of each candidate.
                let isFailed = false;
                // Find all elements whose direct text content is "Failed"
                // (the failure-overlay label). Scan the tile subtree.
                const labels = tile.querySelectorAll('div, span');
                for (const el of labels) {
                    const txt = (el.textContent || '').trim();
                    if (txt !== 'Failed') continue;
                    // Confirm visibility — opacity must be > 0
                    if (visibleOpacity(el) > 0.01) {
                        // Also confirm a warning icon is present in the
                        // same visible layer to avoid matching prompt
                        // text that contains the word "Failed"
                        const icons = tile.querySelectorAll('i');
                        for (const i of icons) {
                            const itxt = (i.textContent || '').trim();
                            if ((itxt === 'warning' || itxt === 'error')
                                && visibleOpacity(i) > 0.01) {
                                isFailed = true;
                                break;
                            }
                        }
                        if (isFailed) break;
                    }
                }
                if (isFailed) {
                    out[id] = {status: 'failed'};
                    continue;
                }
                // 3. RENDERING — default when neither ready nor failed
                out[id] = {status: 'rendering'};
            }
            return out;
        }""", tile_ids)
    except Exception as e:
        print(f"[lookup_tiles_by_id] evaluate failed: {e}", flush=True)
        return {tid: {'status': 'not_found'} for tid in tile_ids}


def match_container_to_submission(container, pending_jobs, claimed_uuids=None):
    """Given a scanned container and the list of currently-pending InFlightJob
    objects, return the best-matching job (or None).

    Strategy ladder (from flow_worker.py's _match_container_to_clip):
      1. EXACT: the submission's prompt_key appears as a substring of the
         container's prompt_text. Longest matching key wins.
      2. NORMALIZED: collapse whitespace + lowercase on both sides, then
         substring-match. Handles minor whitespace differences.
      3. SINGLE-PENDING: if only one pending job is left, claim any tile
         that doesn't match a different submission's prompt.

    v671 — `claimed_uuids` (optional set[str]): UUIDs of tile URLs that
    were already attributed to a previous submission. When supplied,
    Strategy 3 refuses to claim a container whose tile UUIDs intersect
    this set — that container is a stale gallery re-scan of an
    already-handled job, NOT a fresh result for the lone pending job.
    Closes the misattribution path that wasted node 1002 on node 1000's
    images (see v671 commit message).

    Returns: the matched InFlightJob or None.
    """
    if not container or not pending_jobs:
        return None
    prompt_text = (container.get("prompt_text") or "")
    if not prompt_text:
        return None

    # Strategy 1: exact substring match, longest key wins
    best_job = None
    best_len = 0
    for job in pending_jobs:
        key = job.prompt_key
        if not key or len(key) < 20:
            continue
        if key in prompt_text and len(key) > best_len:
            best_job = job
            best_len = len(key)
    if best_job:
        return best_job

    # Strategy 2: normalized match
    p_norm = "".join(prompt_text.split()).lower()
    best_job = None
    best_len = 0
    for job in pending_jobs:
        key = job.prompt_key
        if not key or len(key) < 20:
            continue
        k_norm = "".join(key.split()).lower()
        if k_norm in p_norm and len(k_norm) > best_len:
            best_job = job
            best_len = len(k_norm)
    if best_job:
        return best_job

    # Strategy 3: if exactly one pending job left, claim any unmatched tile
    if len(pending_jobs) == 1:
        # v671 — refuse the single-pending catchall when the container's
        # UUIDs are already attributed elsewhere. This is the smoking-gun
        # path for the wrong-set bug: gallery still shows node 1000's
        # tiles, scan misses tile_id resolution, falls into Strategy 3,
        # claims them for the lone pending job (1002). Reject.
        #
        # v732 — extend the guard to cover URLs that existed in this
        # job's baseline at submit time. _claimed_tile_uuids only covers
        # claims made BY CURRENT worker session; stale tiles from prior
        # worker runs or manual gallery use leave _claimed_tile_uuids
        # empty for those UUIDs → v671 guard passes → lone pending job
        # inherits stale tiles. baseline_urls IS captured at every
        # submit (snapshot of gallery URLs at submit time, including
        # pre-existing tiles), so extracting its UUIDs catches any URL
        # that predates this job regardless of which session generated
        # it.
        _UUID_RE_LOCAL = re.compile(
            r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
            re.IGNORECASE,
        )
        container_uuids = set()
        for url in (container.get("tile_image_urls") or []):
            m = _UUID_RE_LOCAL.search(url or "")
            if m:
                container_uuids.add(m.group(0).lower())
        if container_uuids:
            if claimed_uuids and (container_uuids & claimed_uuids):
                # v671 path — already claimed by current session
                return None
            # v732 — additional guard: container UUIDs overlapping the
            # lone pending job's baseline mean the tiles predate this
            # job's submit, so they can't be its result.
            _job_v732 = pending_jobs[0]
            baseline_uuids = set()
            for url in (_job_v732.baseline_urls or set()):
                m = _UUID_RE_LOCAL.search(url or "")
                if m:
                    baseline_uuids.add(m.group(0).lower())
            if baseline_uuids and (container_uuids & baseline_uuids):
                overlap_n = len(container_uuids & baseline_uuids)
                print(f"[match] [v732] ⏭ Strategy 3 rejected: container has {overlap_n}/{len(container_uuids)} UUID(s) in pending job {_job_v732.node_id} baseline — stale gallery state", flush=True)
                return None
        return pending_jobs[0]

    return None


class InFlightJob:
    """A submission that's been fired at Flow but not yet downloaded.
    Only mutated from the main thread. The HTTP worker reports completions
    via `done_queue`, which the main thread drains to remove finished jobs.
    """
    __slots__ = (
        "node_id", "node_name", "prompt", "prompt_key",
        "variants", "submit_time", "output_dir", "input_items",
        "status", "error_message",
        # Stability tracking — used by the scanner to accept a container as
        # done only after seeing the same committed_tile_count across two
        # consecutive scan passes. Handles the case where Flow produces
        # fewer successful variants than requested (partial failures), so
        # we can't gate solely on "committed >= expected".
        "last_committed_count", "last_stable_seen_at",
        # Baseline of tile URLs that existed in the gallery at the moment
        # this job was submitted. Used to filter out pre-existing tiles
        # from attribution — without this, a project with stale tiles
        # (e.g. re-used from a previous worker run) would cause the
        # scanner to immediately "match" old tiles against the new
        # submission's prompt_key and download the wrong images.
        "baseline_urls",
        # v476: diagnostic throttle — last time we dumped a detailed scan
        # dump for this job. Prevents spamming the log when a job stays
        # stuck for minutes.
        "_last_diag_at",
        # v521: per-tile UUIDs captured at submit time from `data-tile-id`
        # attributes inside the new container at data-index="0". This is
        # the primary attribution key — UUID lookup replaces fuzzy prompt
        # matching for tile→submission attribution. Empty list means
        # capture failed (rare race where tiles weren't in DOM yet) and
        # the legacy prompt_key path will run as fallback.
        "tile_ids",
        # v709: stuck-retry bookkeeping. retry_count = how many times this
        # job has been reload+resubmitted after a 90s stall; _original_job
        # = the dict pulled from /jobs/pending so _submit_one_job can be
        # called again verbatim when a stuck job is retried.
        "retry_count",
        "_original_job",
        # v791c: download-retry bookkeeping. dl_retry_count = how many times
        # the HTTP worker reported an auth/expiry (403/401/410) download
        # failure for this job so the main thread re-resolved FRESH signed
        # URLs from the still-present Flow tiles and re-enqueued. Bounded by
        # MAX_DL_RETRIES so a genuinely-broken tile still fails eventually.
        "dl_retry_count",
    )

    def __init__(self, node_id, node_name, prompt, prompt_key, variants,
                 output_dir, input_items, baseline_urls=None, tile_ids=None,
                 retry_count=0, original_job=None):
        self.node_id = node_id
        self.node_name = node_name
        self.prompt = prompt
        self.prompt_key = prompt_key
        self.variants = variants
        self.submit_time = time.time()
        self.output_dir = output_dir
        self.input_items = input_items or []
        self.status = "submitted"  # submitted | completed | failed
        self.error_message = None
        self.last_committed_count = -1  # -1 = never scanned yet
        self.last_stable_seen_at = 0.0
        self.baseline_urls = baseline_urls or set()
        self._last_diag_at = 0.0  # v476
        self.tile_ids = list(tile_ids) if tile_ids else []  # v521
        self.retry_count = retry_count  # v709
        self.dl_retry_count = 0  # v791c — download re-resolve attempts
        self._original_job = original_job  # v709


def api_pull_mode_parallel(page, api_url, api_key, worker_id=None,
                           parallel_slots=2, cross_batch=False):
    """Parallel version of api_pull_mode using a single-threaded main loop
    for all Playwright calls plus a background HTTP thread for downloads
    and uploads.

    This design works around Patchright's greenlet-thread-affinity rule:
    every Playwright call (page.evaluate, page.locator, page.mouse, etc.)
    must be made from the same thread that created the page object. If
    we tried to run a "download thread" that called page.evaluate, we'd
    hit 'Cannot switch to a different thread' — which is exactly what
    killed v438's first incarnation.

    Architecture (mirrors the video worker's current HTTP-download
    design in static/flow_worker.py):

      MAIN THREAD (owns `page`)
        1. Run a "download cycle" — scan the gallery, match completed
           tiles to pending submissions, extract variant URLs into
           http_queue. (Only scan+extract is on Playwright; the actual
           HTTP download happens in the worker thread.)
        2. Drain done_queue — pop finished downloads, clean up their
           temp dirs, remove from in_flight.
        3. If under capacity, poll /jobs/pending for a new job.
           If one returned, submit it (setup + click Generate).
        4. Sleep briefly if nothing happened this iteration.
        5. Repeat.

      HTTP DOWNLOAD THREAD
        1. Pop an item from http_queue (timeout-based loop).
        2. Build a requests.Session with cookies snapshotted at enqueue
           time. Download each variant URL to disk.
        3. Upload the variants to the webapp API.
        4. POST /jobs/{id}/status. Push result into done_queue.
        5. Repeat until stop_flag.

    Parallelism source: while Flow's servers are rendering job N
    (15-40s), the main thread overlaps that wait with submitting
    job N+1 (5-15s of local setup work). With parallel_slots=2,
    at any moment up to 2 generations are in flight on Flow.

    Args:
        parallel_slots: max concurrent in-flight submissions. Default 2.
                        --parallel 1 at the CLI routes to the legacy
                        sequential api_pull_mode instead of here.
    """
    import requests
    import queue as _queue
    if not worker_id:
        import socket
        worker_id = f"image-worker-{socket.gethostname()}"

    print(f"\n{'=' * 60}")
    print(f"HTTP-PULL API MODE (PARALLEL × {parallel_slots})")
    print(f"{'=' * 60}")
    print(f"API URL:   {api_url}")
    print(f"Worker ID: {worker_id}")
    print(f"Slots:     {parallel_slots} concurrent in-flight generations")
    print(f"Poll:      every {API_POLL_INTERVAL}s when idle")
    print(f"Ctrl+C to stop.")
    print(f"{'=' * 60}\n", flush=True)

    # Connectivity check
    try:
        health = _api_request(api_url, api_key, "GET", "/health", timeout=10)
        print(f"[API] ✓ Connected: {health}", flush=True)
    except Exception as e:
        print(f"[API] ❌ Can't reach {api_url}: {e}", flush=True)
        return

    # Release any claims this worker owned from a previous crashed run.
    # See api_pull_mode for rationale; same call, same endpoint.
    try:
        rc = _api_request(api_url, api_key, "POST", "/release-claims",
                          params={"worker_id": worker_id}, timeout=10)
        if rc and rc.get("released"):
            print(f"[API] ↩ Released {rc['released']} stale claim(s) from previous run", flush=True)
    except Exception as e:
        print(f"[API] ⚠ Couldn't release stale claims: {e}", flush=True)

    tmp_root = os.path.join(tempfile.gettempdir(), "image_worker_api")
    os.makedirs(tmp_root, exist_ok=True)

    try:
        state_root = os.path.join(os.path.expanduser("~"), "KavenoImageWorker")
    except Exception:
        state_root = tmp_root
    os.makedirs(state_root, exist_ok=True)
    state_file = os.path.join(state_root, "worker_state.json")

    # --- Shared state ---
    # in_flight is ONLY touched by the main thread. No lock needed.
    in_flight = {}            # node_id → InFlightJob
    http_queue = _queue.Queue()    # main → http worker: {node_id, urls, output_dir, cookies, user_agent, ...}
    done_queue = _queue.Queue()    # http worker → main: {node_id, outcome, error?}

    # v458: URL-attribution bookkeeping. Every time we enqueue tile URLs
    # for a submission, we record them here. Later scans that see those
    # same URLs will NOT re-attribute them to a different submission —
    # which would otherwise happen when the scanner's Strategy 3
    # fallback ("single pending claims any unmatched tile") runs on a
    # gallery that still visually contains tiles from an already-matched
    # submission. Symptom: same 4 URLs downloaded for two different
    # nodes. Set is capped to prevent unbounded growth on long runs.
    #
    # v671: parallel UUID set. Flow serves the SAME image under TWO
    # different URL forms — direct `flow-content.google/image/<uuid>?...`
    # AND redirect `labs.google/fx/api/trpc/media.getMediaUrlRedirect?name=<uuid>`.
    # Pre-v671 the dedup compared full URL strings, so the redirect form
    # of an already-claimed direct URL would slip through and Strategy 3
    # would re-attribute the gallery tile to a still-pending node.
    # Logged smoking gun: node 1000 saved variants with `flow-content.google/image/<UUID>`,
    # node 1002 then saved the SAME 4 UUIDs via `labs.google/.../getMediaUrlRedirect?name=<UUID>`.
    # Fix: extract the UUID from the URL and dedup by UUID. Both forms
    # contain the same UUID; one match = one image regardless of form.
    _claimed_tile_urls = set()
    _claimed_tile_uuids = set()  # v671 — UUID-form-agnostic dedup
    _claimed_tile_urls_insert_order = []  # FIFO for bounded eviction
    _claimed_tile_uuids_insert_order = []  # v671 — parallel FIFO
    _CLAIMED_URLS_CAP = 500

    _UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE)

    def _extract_url_uuid(url):
        """v671 — pull the UUID from a Flow image URL regardless of form.
        Returns None if no UUID is present. Both URL forms put the UUID
        as the longest hex segment in the URL, so a simple regex grab is
        sufficient."""
        if not url:
            return None
        m = _UUID_RE.search(url)
        return m.group(0).lower() if m else None

    def _mark_urls_claimed(urls):
        """Record these URLs as attributed to some submission. FIFO
        eviction once we exceed the cap. v671 — also records UUIDs so
        the dedup works across the direct and redirect URL forms Flow
        uses for the same image."""
        for u in urls:
            if u not in _claimed_tile_urls:
                _claimed_tile_urls.add(u)
                _claimed_tile_urls_insert_order.append(u)
            uid = _extract_url_uuid(u)
            if uid and uid not in _claimed_tile_uuids:
                _claimed_tile_uuids.add(uid)
                _claimed_tile_uuids_insert_order.append(uid)
        while len(_claimed_tile_urls_insert_order) > _CLAIMED_URLS_CAP:
            old = _claimed_tile_urls_insert_order.pop(0)
            _claimed_tile_urls.discard(old)
        while len(_claimed_tile_uuids_insert_order) > _CLAIMED_URLS_CAP:
            old = _claimed_tile_uuids_insert_order.pop(0)
            _claimed_tile_uuids.discard(old)

    # ─────────────────────────────────────────────────────────────
    # v624 NETWORK-LISTENER ATTRIBUTION
    # ─────────────────────────────────────────────────────────────
    # Capture every batchGenerateImages 200 response. The JSON body has
    # media[].image.generatedImage.{fifeUrl, mediaId, prompt} — the EXACT
    # variants generated for THAT specific POST. Matching by prompt text
    # at scan time replaces the data-index=0 tile-id capture, which has
    # a fundamental race: when called right after click_generate_image,
    # data-index=0 may still hold tiles from a previous job (especially
    # in parallel-slot mode), and those tile_ids get attributed to the
    # new submission. The user observed this as "wrong images downloaded
    # — pre-existing project tiles, not the new generation."
    captured_batches = []  # list of {ts, prompt_in_resp, fife_urls, media_ids, consumed, node_id}
    listener_state = {'attached': False, 'current_submitting_node_id': None}
    # v734 — request_to_node bounded by REQUEST_TO_NODE_CAP. Python id()
    # is reusable after GC; long sessions risked id()-collision
    # misattribution if Playwright recycled request objects. Python dicts
    # are insertion-ordered (since 3.7); when size exceeds the cap, drop
    # the 100 oldest entries inside _on_image_request.
    REQUEST_TO_NODE_CAP = 1000
    request_to_node = {}  # id(playwright_request) → node_id (set when request fires, popped on response)
    captured_urls_by_node = {}  # node_id → list[str] (aggregated fife URLs, in order)
    # v628: FIFO fallback queue for POSTs that fire AFTER submit returns.
    # Each entry: {node_id, expected_count, ts, tagged_count}.
    # When _on_image_request fires with no current_submitting_node_id, it
    # FIFO-matches to the oldest pending submission whose tagged_count is
    # still below expected_count. This handles the case where Flow
    # emits N POSTs over 2-5s but our submit-time flag only stays set
    # for ~1-2s — the late-arriving POSTs would otherwise be untagged
    # and force the job onto the brittle prompt-match fallback (which
    # mis-attributes when two scenes share an identical prompt template).
    pending_submissions = []

    def _gc_pending_submission(node_id):
        """v730 — drop pending_submissions entry when a job exits in_flight.
        Pre-v730 entries lingered up to 60s after completion; FIFO fallback
        in _on_image_request could siphon late POSTs from a NEW job to an
        OLD completed job whose tagged_count never reached expected_count.
        The new job's bucket starved → Tier A 90s timeout → legacy fallback
        kicked in → wrong-image risk via Strategy 3 catchall on stale tiles.
        Call this anywhere job.status flips to a terminal value."""
        n = len(pending_submissions)
        pending_submissions[:] = [p for p in pending_submissions if p['node_id'] != node_id]
        if len(pending_submissions) < n:
            print(f"[API:v730] ⟲ GC pending_submissions entry for node {node_id} (was tagged {n - len(pending_submissions)} time(s); {len(pending_submissions)} entries remain)", flush=True)

    def _on_image_request(request):
        """Tag every outgoing batchGenerateImages POST with the node_id of
        the job currently being submitted. Two paths:
          (1) flag-tagged: `current_submitting_node_id` is set → use that
          (2) FIFO fallback: flag is None → match to oldest pending
              submission with tagged_count < expected_count
        """
        try:
            if 'batchGenerateImages' not in request.url:
                return
            if request.method != 'POST':
                return
        except Exception:
            return
        # v734 — bounded prune. Python dicts are insertion-ordered; drop
        # the 100 oldest entries when over cap to keep id()-collision
        # risk negligible over long sessions.
        if len(request_to_node) > REQUEST_TO_NODE_CAP:
            for k in list(request_to_node.keys())[:100]:
                request_to_node.pop(k, None)
            print(f"[API:v734] pruned request_to_node to {len(request_to_node)} entries", flush=True)
        nid = listener_state.get('current_submitting_node_id')
        if nid is not None:
            request_to_node[id(request)] = nid
            # Update pending counter so FIFO fallback knows quota progress
            for p in pending_submissions:
                if p['node_id'] == nid and p['tagged_count'] < p['expected_count']:
                    p['tagged_count'] += 1
                    break
            return
        # Path 2: flag is None, find oldest unfilled pending submission
        # v734 — filter FIFO by project URL. The page-level listener
        # stays attached across navigations; a late POST from project A
        # could in theory FIFO-tag to a job pending on project B.
        # Match request.frame.url to pending entry's project_url when
        # both known; fall back to legacy oldest-unfilled otherwise.
        try:
            _req_url = request.frame.url if request.frame else None
        except Exception:
            _req_url = None
        for p in pending_submissions:
            if p['tagged_count'] >= p['expected_count']:
                continue
            _p_proj = p.get('project_url')
            if _req_url and _p_proj and _p_proj != _req_url:
                continue
            request_to_node[id(request)] = p['node_id']
            p['tagged_count'] += 1
            break

    def _on_image_response(response):
        try:
            if 'batchGenerateImages' not in response.url:
                return
            if response.status != 200:
                return
            body = response.json()
        except Exception:
            return
        media = body.get('media') or []
        if not media:
            return
        # v627: prefer request → node_id tag; falls back to prompt-match.
        try:
            tagged_node_id = request_to_node.pop(id(response.request), None)
        except Exception:
            tagged_node_id = None
        fife_urls = []
        media_ids = []
        prompt_in_resp = ''
        for m in media:
            try:
                gi = m['image']['generatedImage']
                fife = gi.get('fifeUrl')
                if fife:
                    fife_urls.append(fife)
                mid = gi.get('mediaId') or m.get('name')
                if mid:
                    media_ids.append(mid)
                if not prompt_in_resp:
                    prompt_in_resp = gi.get('prompt') or ''
            except (KeyError, TypeError):
                continue
        if not fife_urls:
            return
        if tagged_node_id is not None:
            # Tagged path — guaranteed correct attribution regardless of
            # prompt collisions.
            bucket = captured_urls_by_node.setdefault(tagged_node_id, [])
            for u in fife_urls:
                if u not in bucket:
                    bucket.append(u)
        # Always also store in captured_batches as legacy/fallback path.
        # Prompt-match still works for any response that wasn't tagged
        # (e.g. a request that fired late, after we cleared the state).
        captured_batches.append({
            'ts': time.time(),
            'prompt_in_resp': prompt_in_resp,
            'fife_urls': fife_urls,
            'media_ids': media_ids,
            'consumed': tagged_node_id is not None,  # if tagged, don't double-attribute
            'node_id': tagged_node_id,
        })
        if len(captured_batches) > 200:
            del captured_batches[:50]

    try:
        page.on('request', _on_image_request)
        page.on('response', _on_image_response)
        listener_state['attached'] = True
        print(f"[API] ✓ v624 network-listener attached (batchGenerateImages → fife URL capture)", flush=True)
        print(f"[API] ✓ v627 request-tag attribution enabled (request → node_id mapping survives prompt collisions)", flush=True)
        # v625.1: cross-batch parallelism auto-enables when listener attaches.
        print(f"[API] ✓ v625 cross-batch parallelism ENABLED (auto, listener-backed) — jobs from different batches run concurrently", flush=True)
    except Exception as e:
        print(f"[API] ⚠ Couldn't attach network listener: {e} — v624 attribution disabled, falling back to DOM-only", flush=True)
        if cross_batch:
            print(f"[API] ⚠ Disabling --cross-batch because the network listener didn't attach (need it for cross-batch attribution)", flush=True)
            cross_batch = False

    def _collect_batches_for_prompt(prompt, consume=False):
        """Find ALL unconsumed batches whose response prompt matches
        `prompt`. Returns the list in arrival order WITHOUT consuming
        them by default — caller decides whether to mark consumed once
        enough variants are collected.

        Match passes (each requires a UNIQUE match — strict by design
        because two scenes in the same persona/batch can share the
        first 100+ chars of the prompt header):
          1. Exact full-prompt match (after strip)
          2. Long-prefix match: first 256 chars exact (handles Flow
             truncation/normalization) — only used when the response
             prompt is shorter than the request prompt.

        v626: was `_consume_batch_for_prompt`; renamed + reworked to
        return a LIST so the scan loop can aggregate Flow's per-variant
        POST splits. Flow fires N separate batchGenerateImages POSTs
        when the operator requested N variants (each response carries
        media[0..0] — one variant). Earlier behavior consumed only the
        first matched batch and marked the job completed with 1
        variant out of N.
        """
        if not prompt:
            return []
        target = prompt.strip()
        target_head = target[:256]
        matches = []
        for batch in captured_batches:
            if batch['consumed']:
                continue
            resp = batch['prompt_in_resp'].strip()
            # Pass 1: exact full match
            if resp == target:
                matches.append(batch)
                continue
            # Pass 2: long-prefix match — only when response is a
            # truncation of the request, not vice versa, AND the prefix
            # is long enough to disambiguate similar-header prompts
            # (256 chars catches "Use Image 1 ... Use Image 2 ..."
            # boilerplate plus enough scene-specific text to be unique).
            if resp and len(resp) >= 64 and target_head.startswith(resp[:256]):
                matches.append(batch)
                continue
        if consume:
            for b in matches:
                b['consumed'] = True
        return matches

    # Project-state — only touched by the main thread
    # v541 — `projects` maps every job_key we've ever seen to its
    # Flow project info, so revisited keys reuse their old project
    # rather than creating a new one each time. The legacy
    # current_* fields stay in sync with projects[current_job_key].
    project_state = {
        "current_project_url": None,
        "current_job_key": None,
        "uploaded_in_project": set(),
        "projects": {},  # job_key → {url, uploaded, last_used_at}
    }
    PROJECTS_LRU_CAP_PAR = 20

    stop_flag = threading.Event()
    heartbeat_stop = threading.Event()

    def _save_state():
        try:
            # v541 — keep projects[current_job_key] in sync with the
            # active state before persisting so legacy and dict views
            # never diverge.
            #
            # v723 — Only mirror when current_project_url is non-None.
            # _ensure_project_ready's path-2 (switching jobs, no prior to
            # reuse) clears current_project_url to None and calls
            # _save_state BEFORE updating current_job_key, so a naive
            # mirror would overwrite projects[displaced_key]={url:None},
            # clobbering the URL that line 5752's displacement-persist
            # block correctly stored seconds earlier. Round-trip evidence
            # (2026-05-13): node 1191 created HCC project 7b31aa58; round-5
            # switch back to HCC found prior_url=None and created
            # ca9b3d8f — orphaning 7b31aa58 and the in-flight renders
            # already running there. Same hazard at line 5827. Defensive
            # rule: never persist a None URL onto an existing entry. If
            # we genuinely have no current project, just skip the mirror.
            ck = project_state["current_job_key"]
            cu = project_state["current_project_url"]
            if ck and cu:
                project_state["projects"][ck] = {
                    "url": cu,
                    "uploaded": sorted(project_state["uploaded_in_project"]),
                    "last_used_at": time.time(),
                }
            with open(state_file, "w", encoding="utf-8") as f:
                json.dump({
                    "current_project_url": project_state["current_project_url"],
                    "current_job_key": project_state["current_job_key"],
                    "uploaded_in_project": sorted(project_state["uploaded_in_project"]),
                    "projects": project_state["projects"],
                }, f, indent=2)
        except Exception as e:
            print(f"[API] ⚠ Couldn't save worker state: {e}", flush=True)

    def _load_state():
        if not os.path.exists(state_file):
            return None, None, set(), {}
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            url = data.get("current_project_url")
            key = data.get("current_job_key")
            uploaded = set(data.get("uploaded_in_project") or [])
            projects = data.get("projects") or {}
            if not isinstance(projects, dict):
                projects = {}
            # Back-fill legacy state: if we have a current job but no
            # entry in the dict, register it so reuse works on revisit.
            if url and key and key not in projects:
                projects[key] = {
                    "url": url,
                    "uploaded": sorted(uploaded),
                    "last_used_at": time.time(),
                }
            return url, key, uploaded, projects
        except Exception:
            return None, None, set(), {}

    loaded_url, loaded_key, loaded_uploaded, loaded_projects = _load_state()
    project_state["current_project_url"] = loaded_url
    project_state["current_job_key"] = loaded_key
    project_state["uploaded_in_project"] = loaded_uploaded
    project_state["projects"] = loaded_projects
    if loaded_projects:
        print(f"[API] ♻ Loaded {len(loaded_projects)} known job → project mapping(s)", flush=True)

    # --- Heartbeat thread ---
    def _heartbeat_loop():
        while not heartbeat_stop.is_set():
            try:
                _api_request(api_url, api_key, "POST", "/heartbeat",
                             json={"worker_id": worker_id,
                                   "version": WORKER_VERSION},
                             timeout=10)
            except Exception:
                pass
            heartbeat_stop.wait(timeout=5)

    hb_thread = threading.Thread(target=_heartbeat_loop, name="hb", daemon=True)
    hb_thread.start()

    # ============================================================
    # HTTP DOWNLOAD + UPLOAD WORKER (pure Python, no Playwright)
    # ============================================================
    def _http_worker():
        """Pop items from http_queue, download URLs via requests, upload
        to webapp, post status, push outcome into done_queue.

        No Playwright calls here — that's the whole point. Every task is
        either HTTP or local file I/O, both of which are perfectly happy
        to run on any thread.
        """
        print(f"[API:http] Pure-HTTP download worker started", flush=True)
        while not stop_flag.is_set():
            try:
                item = http_queue.get(timeout=3)
            except _queue.Empty:
                continue
            if item is None:  # Explicit stop sentinel
                break

            node_id = item.get("node_id")
            try:
                # Failed-submission path: nothing to download, just post status
                if item.get("failed"):
                    err = item.get("error") or "Generation failed"
                    try:
                        _post_status(api_url, api_key, node_id, "failed", error=err)
                    except Exception as e:
                        print(f"[API:http] ⚠ Failed to post failed-status for node {node_id}: {e}", flush=True)
                    done_queue.put({"node_id": node_id, "outcome": "failed", "error": err})
                    continue

                urls = item.get("urls") or []
                output_dir = item.get("output_dir")
                cookies = item.get("cookies") or []
                user_agent = item.get("user_agent") or ""

                if not urls or not output_dir:
                    raise RuntimeError("Missing urls/output_dir on queue item")
                os.makedirs(output_dir, exist_ok=True)

                # Build a requests.Session from the snapshotted cookies
                sess = requests.Session()
                sess.headers.update({
                    "Referer": "https://labs.google/",
                    "Origin": "https://labs.google",
                    "User-Agent": user_agent or "Mozilla/5.0",
                    "Accept": "image/png,image/*;q=0.9,*/*;q=0.5",
                })
                for ck in cookies:
                    try:
                        sess.cookies.set(ck.get("name"), ck.get("value"),
                                         domain=ck.get("domain", ""))
                    except Exception:
                        pass

                # Download each URL → variant_N.png
                saved_paths = []
                auth_fail = False  # v791c — saw a 401/403/410 (stale signed URL)
                for idx, url in enumerate(urls, start=1):
                    save_name = f"variant_{idx}.png"
                    save_path = os.path.join(output_dir, save_name)
                    try:
                        print(f"[API:http] node {node_id} variant {idx}: GET {url[:80]}...", flush=True)
                        resp = sess.get(url, timeout=120, allow_redirects=True)
                        if resp.status_code != 200:
                            if resp.status_code in (401, 403, 410):
                                auth_fail = True
                            raise RuntimeError(f"HTTP {resp.status_code}")
                        content = resp.content
                        if not content or len(content) < 100:
                            raise RuntimeError(f"tiny response ({len(content)} bytes)")
                        with open(save_path, "wb") as f:
                            f.write(content)
                        saved_paths.append(save_path)
                        print(f"[API:http]   ✓ saved {save_name} ({len(content)//1024} KB)", flush=True)
                    except Exception as e:
                        print(f"[API:http]   ✗ variant {idx} download failed: {e}", flush=True)

                # v791c — a 403/401/410 on ANY variant means that variant's
                # signed flow-content URL went stale before we fetched it (the
                # image IS still in Flow). Whenever a stale-URL error left us
                # short of the full set (none OR only some saved), don't upload
                # a partial result and don't terminally fail — ask the main
                # thread to re-resolve FRESH signed URLs for ALL variants from
                # the still-present tiles and re-enqueue. _drain_done_queue caps
                # this at MAX_DL_RETRIES so a truly-dead tile still fails.
                if auth_fail and len(saved_paths) < len(urls):
                    print(f"[API:http] ⟳ Node {node_id} {len(saved_paths)}/{len(urls)} variant(s) saved, rest hit auth/expiry (403/401/410) — signed URLs stale, requesting re-resolve", flush=True)
                    done_queue.put({"node_id": node_id, "outcome": "retry_resolve",
                                    "error": "Variant(s) 403/401/410 (stale signed URLs)"})
                    continue

                if not saved_paths:
                    raise RuntimeError("No variants downloaded successfully")

                # Upload + post status
                print(f"[API:http] node {node_id} ⬆ uploading {len(saved_paths)} variants...", flush=True)
                _upload_variants_to_api(api_url, api_key, node_id, saved_paths)
                _post_status(api_url, api_key, node_id, "completed")
                print(f"[API:http] ✓ Node {node_id} completed", flush=True)
                done_queue.put({"node_id": node_id, "outcome": "completed"})

            except Exception as e:
                import traceback
                traceback.print_exc()
                err = f"Download/upload failed: {e}"
                try:
                    _post_status(api_url, api_key, node_id, "failed", error=err)
                except Exception:
                    pass
                done_queue.put({"node_id": node_id, "outcome": "failed", "error": err})

        print(f"[API:http] Pure-HTTP download worker exiting", flush=True)

    http_thread = threading.Thread(target=_http_worker, name="http-worker", daemon=True)
    http_thread.start()

    # ============================================================
    # MAIN LOOP helpers
    # ============================================================
    def _derive_job_key(name):
        import re as _re_local
        m = _re_local.match(r"^(.*?)Scene\s+\d+", name or "", _re_local.IGNORECASE)
        if m:
            p = m.group(1).strip(" -—:\t")
            # v749 — never collapse empty-prefix scene names to a shared
            # "(untitled)" bucket. Two unrelated batches whose names
            # happen to start with bare "Scene N" would share a Flow
            # project and risk cross-batch contamination. Fall back to
            # standalone:: keyed by the full name so each unprefixed
            # node lands in its own bucket. Wasteful (one Flow project
            # per scene) but safe. Platform-side v749 fix in
            # image_platform.py prevents this path from firing for
            # fresh imports — kept defensive for legacy nodes already
            # in the DB without a batch label.
            if not p:
                return f"standalone::{name}"
            return f"scene-batch::{p}"
        return f"standalone::{name or 'unnamed'}"

    def _ensure_project_ready(new_job_key, context=""):
        """Main-thread only. Creates or reuses a Flow project for the given
        job_key. Mutates project_state.

        v541 — Now consults ``project_state["projects"]`` so that when
        a job key is revisited (e.g. Job A → Job B → Job A) we reuse
        the original Job A project instead of creating a third one.

        Cases:
          1. No current project (fresh run or forced new): check the
             projects dict for new_job_key. If present and reachable,
             adopt it. Otherwise create a brand-new one.
          2. Job key changed: persist the displaced job to the dict,
             then check whether new_job_key already exists. Reuse if
             reachable; otherwise create new.
          3. Same job key AND page already at that project URL: do nothing
             (preserves the virtuoso gallery state for in-flight tiles).
          4. Same job key BUT page is NOT at the project URL: navigate back.
        """
        need_new = False
        projects = project_state["projects"]

        if project_state["current_project_url"] is None:
            # No active project — try the dict first.
            prior = projects.get(new_job_key) or {}
            prior_url = prior.get("url")
            if prior_url:
                print(f"[{context}] ♻ Found prior project for '{new_job_key}' — attempting reuse: {prior_url}", flush=True)
                try:
                    page.goto(prior_url, wait_until="domcontentloaded", timeout=30000)
                    try:
                        create_btn = page.locator(
                            "button[aria-haspopup='dialog']:has(i:text('add_2'))"
                        ).first
                        create_btn.wait_for(state="visible", timeout=20000)
                        project_state["current_project_url"] = prior_url
                        project_state["current_job_key"] = new_job_key
                        project_state["uploaded_in_project"] = set(prior.get("uploaded") or [])
                        prior["last_used_at"] = time.time()
                        projects[new_job_key] = prior
                        _save_state()
                        print(f"[{context}] ✓ Reused prior project for '{new_job_key}'", flush=True)
                        return
                    except Exception:
                        print(f"[{context}] ⚠ Prior project didn't hydrate — discarding", flush=True)
                        projects.pop(new_job_key, None)
                except Exception as nav_e:
                    print(f"[{context}] ⚠ Prior project unreachable ({nav_e}) — discarding", flush=True)
                    projects.pop(new_job_key, None)
            need_new = True
        elif project_state["current_job_key"] != new_job_key:
            # Persist the displaced job before switching.
            displaced_key = project_state["current_job_key"]
            if displaced_key:
                projects[displaced_key] = {
                    "url": project_state["current_project_url"],
                    "uploaded": sorted(project_state["uploaded_in_project"]),
                    "last_used_at": time.time(),
                }
            print(f"[{context}] Switching from job '{displaced_key}' to '{new_job_key}'", flush=True)

            # Try to reuse a prior project for the incoming key.
            prior = projects.get(new_job_key) or {}
            prior_url = prior.get("url")
            if prior_url:
                print(f"[{context}] ♻ Found prior project for '{new_job_key}' — attempting reuse: {prior_url}", flush=True)
                try:
                    page.goto(prior_url, wait_until="domcontentloaded", timeout=30000)
                    try:
                        create_btn = page.locator(
                            "button[aria-haspopup='dialog']:has(i:text('add_2'))"
                        ).first
                        create_btn.wait_for(state="visible", timeout=20000)
                        project_state["current_project_url"] = prior_url
                        project_state["current_job_key"] = new_job_key
                        project_state["uploaded_in_project"] = set(prior.get("uploaded") or [])
                        prior["last_used_at"] = time.time()
                        projects[new_job_key] = prior
                        _save_state()
                        print(f"[{context}] ✓ Reused prior project for '{new_job_key}'", flush=True)
                        return
                    except Exception:
                        print(f"[{context}] ⚠ Prior project didn't hydrate — discarding", flush=True)
                        projects.pop(new_job_key, None)
                except Exception as nav_e:
                    print(f"[{context}] ⚠ Prior project unreachable ({nav_e}) — discarding", flush=True)
                    projects.pop(new_job_key, None)

            # No reusable prior project — create a fresh one.
            need_new = True
            project_state["current_project_url"] = None
            project_state["uploaded_in_project"] = set()
            _save_state()
        else:
            # Same job key — check whether we're actually on that project's
            # page right now. On worker startup we'll still be on Flow home;
            # mid-batch we'll already be on the project page.
            try:
                current_url = page.url or ""
            except Exception:
                current_url = ""
            stored_url = project_state["current_project_url"] or ""

            if stored_url and stored_url in current_url:
                # Already on the right project — preserve viewport/virtuoso
                # state so scan_gallery_containers keeps seeing in-flight tiles.
                pass
            elif stored_url:
                # Reusing project from disk state but page isn't there yet.
                # Navigate to it ONCE. Since nothing has been submitted this
                # run, there are no in-flight tiles to preserve — this
                # navigation is safe.
                print(f"[{context}] Navigating to stored project {stored_url}", flush=True)
                try:
                    page.goto(stored_url, wait_until="domcontentloaded", timeout=30000)
                    try:
                        create_btn = page.locator(
                            "button[aria-haspopup='dialog']:has(i:text('add_2'))"
                        ).first
                        create_btn.wait_for(state="visible", timeout=20000)
                    except Exception:
                        time.sleep(5)
                    # v836 — validate we actually LANDED on the project. After a
                    # mid-run ACCOUNT SWITCH (golden restore / relaunch) the stored
                    # project belongs to the OLD account, so goto() silently lands
                    # on home/login/404 with NO exception → later "Settings button
                    # not found". Match the video worker (flow_worker): if we're
                    # not on a /project/ page, drop the stale project + create a
                    # fresh one on the current account.
                    try:
                        _landed = page.url or ""
                    except Exception as _url_e:
                        _landed = ""
                        print(f"[{context}] ⚠ page.url unreadable after stored-project nav ({_url_e})", flush=True)
                    if "/project/" not in _landed:
                        print(f"[{context}] ⚠ Stored project didn't load (account may have switched; landed on {_landed[:60] or '<none>'}) — creating a new one", flush=True)
                        projects.pop(project_state["current_job_key"], None)
                        need_new = True
                        project_state["current_project_url"] = None
                        project_state["uploaded_in_project"] = set()
                        _save_state()
                except Exception as nav_e:
                    print(f"[{context}] ⚠ Stored project unreachable ({nav_e}) — creating a new one", flush=True)
                    # Drop the stale entry from the dict too.
                    projects.pop(project_state["current_job_key"], None)
                    need_new = True
                    project_state["current_project_url"] = None
                    project_state["uploaded_in_project"] = set()
                    _save_state()

        if need_new:
            # Try the flow_api private-API project creation first (HAR-confirmed:
            # trpc/project.createProject + best-effort init). Saves the DOM "New
            # project" click dance + the home-page navigation that precedes it.
            # On any failure, falls back to the DOM path below unchanged.
            project_url = _fa_try_create_new_project_api(page, context=context)
            if not project_url:
                project_url = create_new_flow_project(page, context=context)
            if not project_url:
                raise RuntimeError("Could not create Flow project for this job")
            project_state["current_project_url"] = project_url
            project_state["current_job_key"] = new_job_key
            project_state["uploaded_in_project"] = set()
            # v541 — register and apply LRU eviction.
            projects[new_job_key] = {
                "url": project_url,
                "uploaded": [],
                "last_used_at": time.time(),
            }
            if len(projects) > PROJECTS_LRU_CAP_PAR:
                sorted_by_age = sorted(
                    projects.items(),
                    key=lambda kv: (kv[1] or {}).get("last_used_at", 0),
                )
                to_drop = len(projects) - PROJECTS_LRU_CAP_PAR
                for evict_key, _ in sorted_by_age[:to_drop]:
                    if evict_key != new_job_key:
                        projects.pop(evict_key, None)
                        print(f"[{context}] LRU evicted job key '{evict_key}'", flush=True)
            _save_state()
            print(f"[{context}] ✓ Created new project for job '{new_job_key}'", flush=True)

    def _snapshot_browser_session():
        """Capture cookies + UA from the browser for the HTTP worker.

        Called right before enqueueing each download so the session used
        by the worker has the freshest cookies. Cheap — one cookies()
        call + one page.evaluate() per enqueue.
        """
        try:
            cookies = page.context.cookies()
        except Exception:
            cookies = []
        try:
            ua = page.evaluate("navigator.userAgent")
        except Exception:
            ua = ""
        return cookies, ua

    def _submit_one_job(job):
        """Claim a job and submit it to Flow. Does NOT wait for render.
        Returns True if submission fired successfully, False if the job
        failed before Generate. Main-thread only.
        """
        node_id = job.get("id")
        node_name = job.get("name", "")
        prompt = job.get("prompt", "")
        variants = int(job.get("variants") or 1)
        aspect_ratio = job.get("aspect_ratio", "16:9")
        resolution = job.get("resolution", "1K")
        model = job.get("model", "nano_banana_2")
        input_images = job.get("input_images")
        if input_images is None:
            input_images = job.get("input_image_urls") or []
        new_job_key = _derive_job_key(node_name)
        ctx = f"node_{node_id}"

        # v451: batch-boundary check. If anything is currently in-flight
        # on a different batch, we CAN'T switch projects — the in-flight
        # tiles would be stranded in the old project. Give the claim back
        # and wait for the current batch to finish.
        #
        # v625: when the v624 network listener is attached, this lock is
        # no longer necessary. Attribution is by response-prompt match,
        # not DOM tile-id, so navigating to a different project doesn't
        # strand the in-flight POSTs (Flow's API is project-scoped via
        # URL — already-fired POSTs complete regardless of which project
        # the UI shows). The fife URLs come back through the listener,
        # get matched to the right job, and download via cookies + Referer
        # (no project context needed).
        #
        # v625.1: cross-batch is now AUTO-ENABLED when the listener is
        # attached, since the platform UI launches the worker without
        # flags. The --cross-batch flag is kept as an explicit toggle
        # in case the operator wants to FORCE-enable it even if the
        # listener attach reported a fault, but the default-on path
        # is the listener-attached one.
        active_in_flight = [j for j in in_flight.values()
                            if j.status in ("submitted", "downloading")]
        cross_batch_active = listener_state['attached'] or cross_batch  # retained for log-only context
        # v724 — Cross-PROJECT submits ALWAYS block while in-flight jobs
        # exist on the current project, regardless of cross_batch_active.
        # v625 assumed the v624 network listener handles attribution across
        # project switches — true only when Flow's batchGenerateImages
        # response arrives BEFORE we navigate to the next project. For
        # any render slower than the gap between submit and the next
        # cross-project claim (~10-30s typical), the response is
        # aborted by the navigation, Tier A captured_urls_by_node never
        # populates, tile_id DOM lookup returns not_found (tiles are
        # on the OTHER project's gallery), and at age=90s v709
        # STUCK_RETRY resubmits a fresh copy. Original 4 Banana renders
        # complete server-side and rot in the abandoned gallery —
        # wasted credits + ~5min wall penalty per orphaned job.
        # Surfaced 2026-05-13 from node 1208: pending 196s on HCC page
        # while its tiles rendered on man arm project dee3a7db, then
        # STUCK_RETRY fired and re-rendered the whole job. Reverts v625
        # cross-batch-on-listener default for the cross-PROJECT case
        # only; same-project parallel slots still proceed normally.
        current_batch = project_state.get("current_job_key")
        if active_in_flight and current_batch and current_batch != new_job_key:
            print(f"[API:submit] ⏸ Node {node_id} is batch '{new_job_key}' but {len(active_in_flight)} in-flight on '{current_batch}' — releasing claim, will re-queue (v724 cross-project block)", flush=True)
            # v550: release with retry + extended timeout. The original
            # path used a 10s timeout and gave up on first failure,
            # relying on the 10-min TTL to clear the claim. During
            # transient network stalls (user's log: ReadTimeout
            # cascading on /release call) that means up to 10
            # minutes of work blocked. Retry up to 3 times with
            # progressively longer timeouts; if all fail, fall
            # back to TTL but log it clearly so the operator knows.
            released = False
            last_err = None
            for attempt, t_out in enumerate((10, 20, 30), start=1):
                try:
                    _api_request(api_url, api_key, "POST",
                                 f"/jobs/{node_id}/release",
                                 params={"worker_id": worker_id},
                                 timeout=t_out)
                    released = True
                    if attempt == 1:
                        print(f"[API:submit] ↩ Released node {node_id} back to queue", flush=True)
                    else:
                        print(f"[API:submit] ↩ Released node {node_id} back to queue (retry {attempt})", flush=True)
                    break
                except Exception as e:
                    last_err = e
                    if attempt < 3:
                        # brief backoff between retries — server
                        # might just be momentarily slow.
                        time.sleep(0.5 * attempt)
                    continue
            if not released:
                print(f"[API:submit] ⚠ Release failed after 3 attempts (10-min TTL will clear): {last_err}", flush=True)
            return False

        print(f"\n[API:submit] → Claimed job: node {node_id}" + (f" ({node_name})" if node_name else ""), flush=True)
        print(f"[API:submit]    Prompt: {prompt[:80]}{'...' if len(prompt) > 80 else ''}", flush=True)
        print(f"[API:submit]    Inputs: {len(input_images)} ref(s)", flush=True)
        print(f"[API:submit]    Settings: x{variants} @ {aspect_ratio}/{resolution}/{model}", flush=True)

        job_work = os.path.join(tmp_root, f"node_{node_id}")
        if os.path.exists(job_work):
            try:
                shutil.rmtree(job_work)
            except Exception:
                pass
        os.makedirs(job_work, exist_ok=True)
        out_dir = os.path.join(job_work, "out")
        os.makedirs(out_dir, exist_ok=True)

        try:
            # Download reference inputs (pure HTTP, no Playwright needed)
            input_items, missing_refs = [], []
            if input_images:
                input_items, missing_refs = _download_reference_inputs(
                    api_key, input_images, job_work)
            if input_images and not input_items:
                err = f"All {len(input_images)} reference image(s) could not be downloaded"
                if missing_refs:
                    reasons = {m.get("error", "unknown") for m in missing_refs}
                    err += f": {'; '.join(reasons)}"
                print(f"[API:submit] ✗ Node {node_id}: {err}", flush=True)
                _post_status(api_url, api_key, node_id, "failed", error=err)
                try:
                    shutil.rmtree(job_work)
                except Exception:
                    pass
                return False
            if missing_refs:
                # Fail-hard on partial-missing. Every ref in input_images was
                # deliberately attached by the artifact author (avatar /
                # product / inline character). Proceeding with a subset always
                # produces a degraded render — e.g. an avatar-less Nuri frame —
                # that silently burns 4 variants of wasted spend. After the
                # retry/backoff in _download_reference_inputs already exhausted
                # the transient-404 window, a still-missing ref means redo this
                # node, not ship garbage.
                lost = ", ".join(f"{m.get('filename','?')} [{m.get('role','') or '?'}]"
                                 for m in missing_refs)
                reasons = "; ".join({m.get("error", "unknown") for m in missing_refs})
                err = (f"{len(missing_refs)} of {len(input_images)} reference image(s) "
                       f"could not be downloaded after retries: {lost} ({reasons})")
                print(f"[API:submit] ✗ Node {node_id}: {err} — failing job (no partial-ref render)", flush=True)
                _post_status(api_url, api_key, node_id, "failed", error=err)
                try:
                    shutil.rmtree(job_work)
                except Exception:
                    pass
                return False

            input_paths = [it["path"] for it in input_items]

            # --- All Playwright calls on the main thread; no lock needed ---
            _ensure_project_ready(new_job_key, context=ctx)

            # --- flow_api private-API path (FLOW_API_MODE=on, default) ---
            # Skips mode/settings/DOM-upload/click; uploads refs + fires N
            # batchGenerateImages POSTs in-page (page.evaluate). Reads the fife
            # URL from each response and writes them DIRECTLY into
            # captured_urls_by_node[node_id] — bypasses the v624/v627 listener
            # attribution entirely (page.on('request') fires unreliably for
            # page.evaluate(fetch), so we can't depend on it). The scanner
            # + HTTP downloader pipeline picks the URLs up the same way it
            # would after a DOM-driven click.
            #
            # On any failure: latches off for the page session and falls
            # through to the DOM path below.
            # v836 — up to 3 API attempts per job (operator). Success → ship.
            # An account-block ('unusual activity') → golden restore, NO retry/UI
            # (the DOM hits the same block). Any other API failure → retry the
            # API; after 3 non-unusual fails, fall to the DOM/UI path for THIS job
            # only. The API is never globally disabled (latch neutralized), so the
            # NEXT job starts fresh on API.
            _API_ATTEMPTS_PER_JOB = 3
            for _api_attempt in range(_API_ATTEMPTS_PER_JOB):
                if _flow_api_pull_submit_try(
                    page, node_id, node_name, prompt, input_paths, variants,
                    aspect_ratio, model, ctx,
                    listener_state, pending_submissions, captured_urls_by_node,
                    in_flight, out_dir, input_items, job,
                ):
                    _save_state()
                    print(f"[API:submit] ✓ Node {node_id} submitted via flow_api (in_flight={len([j for j in in_flight.values() if j.status=='submitted'])})", flush=True)
                    return True

                # Account-level 'unusual activity' block → golden restore. Do NOT
                # retry the API or fall to DOM (the DOM hits the same session
                # block). The in-flight node re-queues via /release-claims and
                # retries on the clean session after the restore — not lost.
                _ua_reason = getattr(page, "_flow_api_unusual_reason", "")
                if _ua_reason:
                    try:
                        page._flow_api_unusual_reason = ""
                    except Exception:
                        pass
                    print(f"[{ctx}] 🔁 unusual-activity — restoring directly from golden profile + relaunch "
                          f"(node stays claimed → re-queued by /release-claims → retried after restore)", flush=True)
                    _restore_signal["golden"] = True
                    return False

                # Non-unusual API failure — retry the API, or give up to the UI.
                if _api_attempt < _API_ATTEMPTS_PER_JOB - 1:
                    print(f"[{ctx}] flow_api attempt {_api_attempt + 1}/{_API_ATTEMPTS_PER_JOB} failed — retrying API", flush=True)
                else:
                    print(f"[{ctx}] flow_api failed {_API_ATTEMPTS_PER_JOB}x — using the DOM/UI path for THIS job (API stays on for the next)", flush=True)

            if not select_image_mode(page, context=ctx):
                raise RuntimeError("Failed to switch to Image mode")
            clear_prompt_references(page, context=ctx)
            if not configure_image_settings(page, aspect_ratio=aspect_ratio,
                                             resolution=resolution, model=model,
                                             variants=variants, context=ctx):
                raise RuntimeError("Failed to configure image settings")
            if input_paths:
                if not upload_reference_images(page, input_paths, context=ctx,
                                               already_uploaded=project_state["uploaded_in_project"]):
                    raise RuntimeError("Failed to upload reference images")

            # v703 — Worker-injected reference manifest. After attaching
            # refs in input_paths order, the worker has authoritative
            # knowledge of which file is at which Image N. Build a manifest
            # header from input_paths, strip any stale "Use Image N" lines
            # the platform's substitution path may have written (with
            # potentially mis-matched numbers), and prepend the worker's
            # authoritative header to the prompt body. Banana 2 reads the
            # manifest at top → trusts it → no more "bottle ended up at
            # Image 3 but prompt says Image 2" misbinds.
            if input_paths:
                _v703_manifest = _build_reference_manifest(input_paths)
                _v703_stripped = _strip_stale_reference_lines(prompt)
                prompt = _v703_manifest + _v703_stripped
                _v703_preview = _v703_manifest.replace("\n", " | ").strip(" |")
                print(
                    f"[{ctx}] [v703] manifest prepended ({len(input_paths)} ref(s)): "
                    f"{_v703_preview}",
                    flush=True,
                )

            if not fill_prompt_textarea(page, prompt):
                raise RuntimeError("Failed to fill prompt")

            # Snapshot the set of tile URLs currently in the gallery BEFORE
            # clicking Generate. Anything already here isn't a result of
            # this submission — the scanner uses this baseline to avoid
            # mis-attributing pre-existing tiles (from previous worker
            # runs or earlier jobs in the same project) to the new job.
            #
            # CRITICAL: pass exclude_uploads=False so the baseline includes
            # uploaded-reference tile URLs too. Those refs live in the same
            # virtuoso grid at data-index=0 with alt='Generated image' and
            # an /edit/ link — they're indistinguishable from real generations
            # at the URL level. Including them in the baseline means the
            # scanner's "skip URLs already in baseline" filter will never
            # attribute a ref as a generation output even if the scanner's
            # container-level "Uploaded image" filter misses it.
            try:
                baseline_urls = snapshot_generated_image_urls(
                    page, exclude_uploads=False)
            except Exception as e:
                print(f"[API:submit] ⚠ Node {node_id}: baseline snapshot failed ({e}) — attribution may catch stale tiles", flush=True)
                baseline_urls = set()

            # v458: also fold in all previously-claimed URLs. The gallery
            # snapshot only captures URLs currently visible in the DOM,
            # but Flow's virtuoso unmounts old tiles. Without this, a
            # tile that belonged to a PREVIOUS submission and got unmounted
            # before this submission's baseline was taken could be
            # re-mounted during scan and misattributed to this node.
            baseline_urls = baseline_urls | _claimed_tile_urls

            # v627: tag the upcoming batchGenerateImages POSTs with this
            # node_id so the response listener can attribute them
            # correctly even if two scenes share an identical prompt
            # template (Scene 6 and Scene 9 in the user's batch — both
            # CTA scenes with the same wording — collided under the
            # v626 prompt-match path).
            #
            # v628: also register this submission in pending_submissions so
            # POSTs that fire AFTER the flag is cleared (Flow emits N
            # POSTs over 2-5s; our flag-window is ~1-2s) still get
            # FIFO-matched to the right job. Healthy run: all N POSTs
            # tagged via the flag path. Late-firing run: late POSTs
            # tagged via the FIFO fallback. Either way no fall-through
            # to the brittle prompt-match path.
            listener_state['current_submitting_node_id'] = node_id
            try:
                _v734_proj_url = page.url
            except Exception:
                _v734_proj_url = None
            pending_submissions.append({
                'node_id': node_id,
                'expected_count': variants,
                'ts': time.time(),
                'tagged_count': 0,
                'project_url': _v734_proj_url,  # v734 — per-project FIFO filter
            })
            # Cap pending list — drop entries older than 60s. They
            # represent submissions whose POSTs already fired or were
            # dropped; keeping them risks misattributing future POSTs.
            cutoff = time.time() - 60
            pending_submissions[:] = [p for p in pending_submissions if p['ts'] > cutoff]
            try:
                if not click_generate_image(page, context=ctx):
                    raise RuntimeError("Failed to click Generate")
                _save_state()

                prompt_key = _derive_prompt_key(prompt)
                if len(prompt_key) < 30:
                    print(f"[API:submit] ⚠ Node {node_id}: prompt_key only {len(prompt_key)} chars — attribution may be ambiguous", flush=True)
            except Exception:
                # Restore state before re-raising so downstream submits
                # don't inherit a stale node_id tag
                listener_state['current_submitting_node_id'] = None
                raise

            # v521: capture per-tile UUIDs from the new container at
            # data-index="0". Flow assigns each variant a stable
            # data-tile-id="fe_id_<UUID>" attribute that survives from
            # render-start through completion (and through failure).
            # Capturing these IDs at submit time gives us a deterministic
            # primary key for attribution — replaces fuzzy prompt-text
            # matching, which can collide when two scenes have similar
            # opening sentences. Best-effort with a short retry loop:
            # tiles may not appear in the DOM for a few hundred ms after
            # the click. If capture fails entirely, the InFlightJob keeps
            # tile_ids=[] and falls back to the legacy prompt_key path.
            # v625: when the v624 network listener is attached, attribution
            # is handled by prompt-match against batchGenerateImages JSON
            # responses — tile_ids are no longer needed for the primary
            # path. The only reason to keep capturing them is as a safety
            # net for jobs whose response the listener somehow misses.
            # Drop the budget from 4s × 8 attempts to 1s × 2 attempts: a
            # near-instant best-effort snapshot. If tile_ids appear quickly,
            # great — they're a backup. If they don't, the listener has us
            # covered.
            #
            # When the listener is NOT attached (rare: page.on() failed),
            # fall back to the original 8-attempt loop because tile_id
            # is the only attribution mechanism left.
            tile_ids = []
            tid_attempts = 2 if listener_state['attached'] else 8
            tid_sleep = 0.5
            try:
                for _attempt in range(tid_attempts):
                    tile_ids = page.evaluate("""() => {
                        const c = document.querySelector('[data-index="0"]');
                        if (!c) return [];
                        const ids = new Set();
                        for (const el of c.querySelectorAll('[data-tile-id]')) {
                            const id = el.getAttribute('data-tile-id');
                            if (id && id.startsWith('fe_id_')) ids.add(id);
                        }
                        return Array.from(ids);
                    }""")
                    if tile_ids and len(tile_ids) >= variants:
                        break
                    time.sleep(tid_sleep)
                if tile_ids:
                    print(f"[API:submit] ✓ Node {node_id}: captured {len(tile_ids)} tile_id(s) at data-index=0 (backup; primary is v624 listener)" if listener_state['attached']
                          else f"[API:submit] ✓ Node {node_id}: captured {len(tile_ids)} tile_id(s) at data-index=0", flush=True)
                elif not listener_state['attached']:
                    print(f"[API:submit] ⚠ Node {node_id}: no tile_ids captured — attribution will use prompt_key fallback", flush=True)
            except Exception as e:
                print(f"[API:submit] ⚠ Node {node_id}: tile_id capture failed ({e}) — listener will handle attribution" if listener_state['attached']
                      else f"[API:submit] ⚠ Node {node_id}: tile_id capture failed ({e}) — attribution will use prompt_key fallback", flush=True)
            finally:
                # v628: actively wait up to 5s for all N batchGenerateImages
                # POSTs to fire and be tagged. Flow's frontend can emit POSTs
                # over a 2-5s window; the 1s tile_id capture window above is
                # often too short. Early-exit when tagged_count reaches
                # variants, so healthy runs don't pay the full 5s.
                # Late POSTs that arrive AFTER this wait still get tagged via
                # the FIFO fallback path in _on_image_request — so even if
                # we time out here, attribution remains correct.
                if listener_state['attached']:
                    # v730b — bump 5.0 → 12.0. Flow emits N POSTs over 2-5s
                    # for an N-variant request; the previous 5s window let
                    # the submit thread return before all POSTs were tagged
                    # via the flag-path. Cross-project navigation in the
                    # next main-loop iteration then aborted any POSTs still
                    # queueing in React → captured_urls_by_node stayed
                    # partial → Tier A 90s timeout → legacy fallback. 12s
                    # catches the long tail; early-exit at tagged_count >=
                    # variants so healthy runs still finish in ~2s.
                    _v730b_wait_start = time.time()
                    tag_deadline = _v730b_wait_start + 12.0
                    while time.time() < tag_deadline:
                        tagged_count = sum(1 for v in request_to_node.values() if v == node_id)
                        if tagged_count >= variants:
                            break
                        time.sleep(0.1)
                    _v730b_waited_s = time.time() - _v730b_wait_start
                    _v730b_final_tagged = sum(1 for v in request_to_node.values() if v == node_id)
                    if _v730b_final_tagged >= variants and _v730b_waited_s > 5.0:
                        # Fires only when the new 5-12s window actually did work
                        print(f"[API:submit] [v730b] Node {node_id}: full flag-path tagging took {_v730b_waited_s:.1f}s (pre-v730b 5s window would have lost POST(s) to FIFO/abort)", flush=True)
                # Final tagged-count diagnostic
                tagged_count = sum(1 for v in request_to_node.values() if v == node_id)
                if listener_state['attached']:
                    if tagged_count == 0:
                        print(f"[API:submit] ⚠ Node {node_id}: 0 POSTs tagged via flag-path; FIFO fallback will catch any late POSTs", flush=True)
                    elif tagged_count < variants:
                        print(f"[API:submit] ⓘ Node {node_id}: {tagged_count}/{variants} POSTs tagged via flag-path; FIFO fallback handles the rest", flush=True)
                    else:
                        # Quiet success — the diagnostic only fires on partial-tag conditions
                        pass
                # v627: clear the request-tag flag so subsequent submits
                # don't accidentally tag their POSTs with this job's id.
                listener_state['current_submitting_node_id'] = None

            in_flight[node_id] = InFlightJob(
                node_id=node_id,
                node_name=node_name,
                prompt=prompt,
                prompt_key=prompt_key,
                variants=variants,
                output_dir=out_dir,
                input_items=input_items,
                baseline_urls=baseline_urls,
                tile_ids=tile_ids,
                original_job=job,  # v709 — preserve dict for stuck-retry resubmit
            )
            print(f"[API:submit] ✓ Node {node_id} submitted (in_flight={len([j for j in in_flight.values() if j.status=='submitted'])}, baseline={len(baseline_urls)} urls)", flush=True)
            return True

        except Exception as e:
            import traceback
            traceback.print_exc()
            try:
                _post_status(api_url, api_key, node_id, "failed",
                             error=f"Submit failed: {e}")
            except Exception:
                pass
            try:
                shutil.rmtree(job_work)
            except Exception:
                pass
            return False

    # --- Download-cycle constants ---
    # v709 — stuck-retry chain. After STUCK_RETRY_TIMEOUT seconds with no
    # tile match, the scanner flips the job to status="stuck_retry" so the
    # main loop picks it up next tick, reloads the project page (clears
    # Banana 2 stuck SSE state), and resubmits via _submit_one_job using
    # the preserved original-job dict. After STUCK_MAX_RETRIES exhaustion
    # OR age past STUCK_TIMEOUT, the job is failed for real.
    STUCK_RETRY_TIMEOUT = 90   # seconds — trigger reload+resubmit at this age
    STUCK_TIMEOUT = 300         # seconds — final give-up after retries exhausted
    STUCK_MAX_RETRIES = 2       # max reload+resubmit attempts before failing
    SCAN_INTERVAL = 4    # seconds between scan passes when busy
    IDLE_POLL = API_POLL_INTERVAL  # seconds when nothing in flight

    def _run_download_cycle():
        """Main-thread only. Scan the Flow gallery, attribute completed
        tiles to pending in_flight submissions, and enqueue their URLs
        for the HTTP worker to download+upload. Also handles failed
        tiles (they go straight to http_queue with failed=True so the
        status POST happens on the worker thread).

        v521: primary attribution is now done via per-tile UUIDs captured
        at submit time (data-tile-id="fe_id_<UUID>"). Direct DOM lookup
        by UUID can't collide between submissions, so two scenes with
        similar opening sentences no longer mis-attribute. Submissions
        whose tile_ids capture failed at submit time still go through
        the legacy prompt_key fuzzy-match path below.

        Returns the number of containers processed this cycle.
        """
        # Only care about submissions that are "submitted" (rendering on Flow)
        pending = [j for j in in_flight.values() if j.status == "submitted"]
        if not pending:
            return 0

        # ─────────────────────────────────────────────────────────────
        # v627 NETWORK-LISTENER PATH (highest priority, runs first)
        # ─────────────────────────────────────────────────────────────
        # Two-tier attribution within the listener path:
        #   Tier A (v627): URLs collected via request-tag mapping
        #     (request_to_node). Guaranteed correct — we tagged the
        #     request before it went out, then the response listener
        #     looked up the tag via Playwright's response.request linkage.
        #     Survives prompt collisions (Scene 6 + Scene 9 with same
        #     CTA template — both have identical prompts).
        #   Tier B (v626 prompt-match): only used for jobs that had no
        #     request tagged (rare — request fired before submit set
        #     the flag, or response.request linkage broke). Aggregates
        #     captured_batches by prompt; can only attribute correctly
        #     when prompts are unique.
        # Both tiers wait for variants count to be satisfied or the
        # PARTIAL_TIMEOUT to elapse (90s after submit).
        ids_resolved_jobs = set()
        cookies_v521, user_agent_v521 = None, None
        PARTIAL_TIMEOUT = 90  # seconds after submit before accepting partial result

        def _enqueue_for_job(job, ready_urls, source_label, batch_count=None):
            nonlocal cookies_v521, user_agent_v521
            if cookies_v521 is None:
                try:
                    cookies_v521, user_agent_v521 = _snapshot_browser_session()
                except Exception as _e:
                    print(f"[API:scan] ⚠ session snapshot failed: {_e}", flush=True)
                    cookies_v521, user_agent_v521 = [], ""
            # v681c — call _mark_urls_claimed so the UUID set ALSO gets
            # populated. Pre-v681c this loop only added URL strings to
            # _claimed_tile_urls, leaving _claimed_tile_uuids empty for
            # every job claimed via Tier A (v627 request-tag listener)
            # or Tier B (v626 prompt-match listener). Strategy 3
            # catchall (match_container_to_submission) then saw
            # `claimed_uuids=∅` for the next pending job and accepted
            # stale gallery containers whose URLs were Node N's tiles
            # in redirect form (`labs.google/.../trpc/media.getMediaUrlRedirect?name=<UUID>`).
            # Result: Node N+1 saved Node N's images byte-identical.
            # Reproduced 2026-05-08 with Heavy Legs Transformation CTA 2:
            # Node 1015 → Node 1017 cross-attribution; Node 1019 → Node 1018.
            _mark_urls_claimed(ready_urls)
            http_queue.put({
                'node_id': job.node_id,
                'urls': ready_urls,
                'output_dir': job.output_dir,
                'cookies': cookies_v521,
                'user_agent': user_agent_v521,
            })
            job.status = "completed"
            _gc_pending_submission(job.node_id)  # v730
            ids_resolved_jobs.add(job.node_id)
            need_count = max(1, getattr(job, 'variants', 1) or 1)
            partial = " (partial — timeout reached)" if len(ready_urls) < need_count else ""
            extra = f", aggregated from {batch_count} batch(es)" if batch_count is not None else ""
            print(f"[API:scan] ✓ Node {job.node_id} matched → {len(ready_urls)}/{need_count} variant(s){partial} → enqueue ({source_label}{extra})", flush=True)

        # Tier A: request-tag attribution (v627)
        for job in pending:
            tagged_urls = list(captured_urls_by_node.get(job.node_id, []))
            if not tagged_urls:
                continue
            need_count = max(1, getattr(job, 'variants', 1) or 1)
            age = time.time() - job.submit_time
            if len(tagged_urls) < need_count and age < PARTIAL_TIMEOUT:
                continue  # wait for more responses
            # v731 — baseline-overlap guard. Pre-v731 Tier A trusted the
            # request-tag mapping unconditionally. If a request was
            # mis-tagged (pre-v730a pending_submissions leak, cross-project
            # FIFO drift, or id()-collision after Playwright GC) Tier A
            # would enqueue WRONG fife URLs byte-identical for the wrong
            # job. baseline_urls is the snapshot of gallery URLs at submit
            # time; if any tagged_url is in that set, the URL predates
            # this job's submit and CANNOT belong to it. Drop the bucket
            # and fall through to Tier B / legacy (both have container-
            # level baseline filters at lines ~6646 + ~6714 already).
            tagged_set = set(tagged_urls)
            overlap = tagged_set & (job.baseline_urls or set())
            if overlap:
                print(f"[API:scan] [v731] ⚠ Node {job.node_id}: Tier A bucket has {len(overlap)}/{len(tagged_urls)} URL(s) overlapping baseline — likely mis-tagged. Dropping bucket, falling through to Tier B/legacy.", flush=True)
                captured_urls_by_node.pop(job.node_id, None)
                continue
            # Pop the bucket so we don't double-attribute
            captured_urls_by_node.pop(job.node_id, None)
            _enqueue_for_job(job, tagged_urls, source_label="v627 request-tag listener")

        # Tier B: prompt-match attribution (v626 fallback)
        for job in pending:
            if job.node_id in ids_resolved_jobs:
                continue
            matches = _collect_batches_for_prompt(job.prompt, consume=False)
            if not matches:
                continue
            collected_urls = []
            for b in matches:
                for u in b['fife_urls']:
                    if u and u not in collected_urls:
                        collected_urls.append(u)
            need_count = max(1, getattr(job, 'variants', 1) or 1)
            age = time.time() - job.submit_time
            if len(collected_urls) < need_count and age < PARTIAL_TIMEOUT:
                continue
            for b in matches:
                b['consumed'] = True
            if collected_urls:
                _enqueue_for_job(job, collected_urls, source_label="v626 prompt-match listener", batch_count=len(matches))

        # ─────────────────────────────────────────────────────────────
        # v521 PRIMARY PATH — submission-first DOM tile-ID lookup
        # ─────────────────────────────────────────────────────────────
        # Fallback for jobs whose batchGenerateImages response wasn't
        # captured (listener attach failed, or response body unparseable).
        # For every pending submission that captured tile_ids at submit
        # time, query the DOM for those exact UUIDs. When all tiles are
        # out of 'rendering', queue downloads for ready ones and mark
        # failed ones as failed.
        for job in pending:
            if job.node_id in ids_resolved_jobs:
                continue  # v624 already resolved this one
            if not job.tile_ids:
                continue  # falls through to legacy path
            tile_states = lookup_tiles_by_id(page, job.tile_ids)
            ready = []
            failed = []
            still_rendering = False
            for tid in job.tile_ids:
                state = tile_states.get(tid, {'status': 'not_found'})
                s = state.get('status')
                if s == 'ready':
                    url = state.get('image_url')
                    if url:
                        ready.append(url)
                elif s == 'failed':
                    failed.append(tid)
                elif s == 'rendering':
                    still_rendering = True
                elif s == 'not_found':
                    # Could be virtualized off-screen, or the tile was
                    # deleted. Treat as still-rendering until the job
                    # ages past STUCK_TIMEOUT, at which point the legacy
                    # path will take over and report stuck.
                    still_rendering = True
            # Don't act until we've seen a stable terminal state for ALL
            # tiles. Partial progress (some ready, some still rendering)
            # waits for the next scan pass.
            if still_rendering:
                continue
            # All tiles terminal. If all failed, mark job failed; else
            # enqueue ready URLs.
            if not ready and failed:
                err_msg = f"All {len(failed)} variants failed in Flow"
                print(f"[API:scan] Node {job.node_id} ✗ all {len(failed)} tile(s) failed (ID-attribution)", flush=True)
                http_queue.put({
                    'node_id': job.node_id,
                    'urls': [],
                    'output_dir': job.output_dir,
                    'cookies': [],
                    'user_agent': "",
                    'failed': True,
                    'error': err_msg,
                })
                job.status = "failed"
                job.error_message = err_msg
                _gc_pending_submission(job.node_id)  # v730
                ids_resolved_jobs.add(job.node_id)
                continue
            if ready:
                # Snapshot session cookies once per cycle (lazy — only
                # if at least one job is ready to enqueue).
                if cookies_v521 is None:
                    try:
                        cookies_v521, user_agent_v521 = _snapshot_browser_session()
                    except Exception as _e:
                        print(f"[API:scan] ⚠ session snapshot failed: {_e}", flush=True)
                        cookies_v521, user_agent_v521 = [], ""
                # Mark URLs as claimed BEFORE enqueueing, so the legacy
                # path running below in this same cycle won't try to
                # claim them again from a container-level scan.
                for u in ready:
                    if u:
                        _claimed_tile_urls.add(u)
                http_queue.put({
                    'node_id': job.node_id,
                    'urls': ready,
                    'output_dir': job.output_dir,
                    'cookies': cookies_v521,
                    'user_agent': user_agent_v521,
                })
                # Mark job as completed so the legacy loop below skips
                # it. The HTTP worker will POST the success status.
                job.status = "completed"
                _gc_pending_submission(job.node_id)  # v730
                ids_resolved_jobs.add(job.node_id)
                # Partial-completion log: distinguish full success from
                # mixed (some variants failed in Flow but others made it).
                if failed:
                    print(f"[API:scan] ✓ Node {job.node_id} partial: {len(ready)}/{len(job.tile_ids)} ready, {len(failed)} failed in Flow → enqueue ready (ID-attribution)", flush=True)
                else:
                    print(f"[API:scan] ✓ Node {job.node_id} matched by tile_id → {len(ready)} variant(s) → enqueue (ID-attribution)", flush=True)

        # If every pending submission was resolved by ID, skip the legacy
        # container scan entirely — it's purely a fallback now.
        remaining_pending = [j for j in pending if j.node_id not in ids_resolved_jobs]
        if not remaining_pending:
            return len(ids_resolved_jobs)

        # v529 fix #7: when a job falls through to the legacy fallback
        # because its tile_ids weren't found in the DOM, log it with
        # detail. Helps distinguish "tile_id capture failed at submit"
        # (no tile_ids at all on the job) from "tile_ids captured but
        # the DOM lookup can't find them" (virtualized off-screen, or
        # tile_id renaming bug). Throttled to once per 30s per job.
        #
        # v534 fix: use a module-level dict instead of mutating the job
        # object. InFlightJob has __slots__ that don't include this
        # attribute, so `job._last_id_fallback_diag_at = time.time()`
        # raises AttributeError("no __dict__ for setting new
        # attributes"). The dict approach matches the v529 fix #5
        # pattern (_announced_claimed_sets) — stored on the function
        # itself, naturally cleaned when the function tears down.
        _id_fallback_diag_times = getattr(
            _run_download_cycle, "_id_fallback_diag_times", None)
        if _id_fallback_diag_times is None:
            _id_fallback_diag_times = {}
            _run_download_cycle._id_fallback_diag_times = _id_fallback_diag_times
        for job in remaining_pending:
            age = time.time() - job.submit_time
            if age < 8:
                continue  # too early to flag
            last_id_diag = _id_fallback_diag_times.get(job.node_id, 0)
            if time.time() - last_id_diag < 30:
                continue
            _id_fallback_diag_times[job.node_id] = time.time()
            if not job.tile_ids:
                print(f"[API:scan] ⓘ Node {job.node_id} tile_id capture was empty at submit — using legacy text-match fallback", flush=True)
            else:
                # Tile IDs exist but lookup didn't resolve them — probe
                # current state for the diagnostic log
                try:
                    states = lookup_tiles_by_id(page, job.tile_ids)
                    counts = {'ready': 0, 'rendering': 0, 'failed': 0, 'not_found': 0}
                    for tid in job.tile_ids:
                        s = states.get(tid, {}).get('status', 'not_found')
                        counts[s] = counts.get(s, 0) + 1
                    sample_id = job.tile_ids[0][:24] + '…' if job.tile_ids else 'none'
                    print(f"[API:scan] ⓘ Node {job.node_id} tile_id lookup: "
                          f"{counts} (sample tid={sample_id}) — falling back to legacy text-match", flush=True)
                except Exception as _e:
                    print(f"[API:scan] ⓘ Node {job.node_id} tile_id lookup probe failed: {_e}", flush=True)
        # ─────────────────────────────────────────────────────────────
        # LEGACY FALLBACK PATH — container-first prompt_key match
        # ─────────────────────────────────────────────────────────────
        # Runs only for submissions whose tile_ids couldn't be captured
        # at submit time (rare race condition where tiles weren't in DOM
        # yet when capture ran). Same code as before v521.

        try:
            # Cap the scan to cover pending submissions plus enough buffer
            # for:
            #   - uploaded reference images (each submission adds 1-4 ref
            #     containers that scroll to the top and get filtered null;
            #     they still occupy data-index slots)
            #   - recently-completed tiles from previous submissions that
            #     haven't been scanned yet
            #   - stale completed tiles from earlier worker runs
            # pending*3 + 10 covers the typical case (up to 4 parallel
            # submissions each with a few refs + some stale history) while
            # staying well below the "walk the whole gallery" threshold.
            # Hard cap at 30 to prevent unbounded scanning if things get
            # really hectic.
            max_idx = min(30, len(pending) * 3 + 10)
            containers = scan_gallery_containers(
                page, max_index=max_idx, context="scan")
        except Exception as e:
            print(f"[API:scan] error: {e}", flush=True)
            return 0

        # v481: if the scanner returned 0 containers despite having
        # pending submissions, the gallery DOM has been unmounted or
        # the page navigated away. Diagnostic dump once per minute to
        # avoid spam — includes current URL and some DOM signals.
        if not containers and pending:
            now_empty = time.time()
            last_empty_diag = getattr(_run_download_cycle, "_last_empty_diag", 0)
            if now_empty - last_empty_diag >= 60:
                _run_download_cycle._last_empty_diag = now_empty
                try:
                    dom_info = page.evaluate("""() => ({
                        url: window.location.href,
                        dataIndexCount: document.querySelectorAll('[data-index]').length,
                        virtuosoScrollers: document.querySelectorAll('[data-testid="virtuoso-scroller"]').length,
                        hasAnyImg: document.querySelectorAll("img[alt='Generated image']").length,
                        bodyLen: document.body ? document.body.innerText.length : 0,
                    })""")
                    print(f"[API:scan] ⚠ Scanner saw 0 containers but {len(pending)} pending: {dom_info}", flush=True)
                except Exception as _ee:
                    print(f"[API:scan] ⚠ Scanner saw 0 containers, couldn't probe DOM: {_ee}", flush=True)

        # v476: diagnostic — if any pending submission has been waiting
        # more than 30 seconds, dump what the scanner just saw. This
        # tells us WHY a match isn't happening. Throttled to once every
        # 30 seconds per stuck submission so we don't spam the log.
        now_diag = time.time()
        for job in remaining_pending:
            age = now_diag - job.submit_time
            if age < 30:
                continue
            last_diag = getattr(job, "_last_diag_at", 0)
            if now_diag - last_diag < 30:
                continue
            job._last_diag_at = now_diag
            key_preview = (job.prompt_key or "")[:80].replace("\n", " ")
            print(f"[API:scan] 🔎 Node {job.node_id} pending {int(age)}s (legacy fallback) — scanner saw {len(containers)} container(s). prompt_key[:80]={key_preview!r}", flush=True)
            for c in containers:
                di = c.get("data_index", "?")
                sr = c.get("still_rendering", False)
                cc = c.get("committed_tile_count", 0)
                hf = c.get("has_failed", False)
                urls = c.get("tile_image_urls") or []
                pt_preview = (c.get("prompt_text") or "")[:120].replace("\n", " ")
                claimed = sum(1 for u in urls if u in _claimed_tile_urls)
                in_baseline = sum(1 for u in urls if u in job.baseline_urls)
                # Test Strategy 1 match against THIS pending job
                s1_match = bool(job.prompt_key and len(job.prompt_key) >= 20
                                and job.prompt_key in (c.get("prompt_text") or ""))
                print(f"[API:scan]    idx={di} rendering={sr} committed={cc} failed={hf} urls={len(urls)} claimed={claimed} baseline={in_baseline} s1_match={s1_match} prompt[:120]={pt_preview!r}", flush=True)

        # Snapshot cookies + UA once per cycle — reuse across all enqueues
        cookies, user_agent = None, None

        enqueued = 0
        for c in containers:
            # Skip containers still rendering (any tile showing %) — come back next scan
            if c.get("still_rendering"):
                continue
            if not (c.get("has_completed_image") or c.get("has_failed")):
                continue
            # Re-read pending each iteration; match_container_to_submission
            # can consume a match that we then mark "downloading", so subsequent
            # containers shouldn't see it.
            pending_live = [j for j in in_flight.values() if j.status == "submitted"]
            if not pending_live:
                break
            match = match_container_to_submission(c, pending_live, claimed_uuids=_claimed_tile_uuids)
            if not match:
                continue

            # v458: claimed-URL guard. If any of this container's tiles were
            # already attributed to a previous submission, this is a re-scan
            # of an already-handled container (the matched submission has
            # since moved out of "submitted" state so it's no longer in
            # pending_live, and Strategy 3 catchall misdirected the match
            # to a different pending submission). Skip — the URLs are
            # already downloading for their real owner.
            #
            # v671: ALSO dedup by UUID. Flow serves the same image under
            # TWO URL forms (direct flow-content.google + redirect via
            # labs.google trpc). Pre-v671 the URL-string comparison missed
            # form-aliasing and Strategy 3 mis-attributed the redirect
            # form to a different node. UUID intersection catches it.
            container_urls_set_early = set(c.get("tile_image_urls") or [])
            container_uuids_early = {
                u for u in (_extract_url_uuid(x) for x in container_urls_set_early) if u
            }
            url_overlap = container_urls_set_early & _claimed_tile_urls
            uuid_overlap = container_uuids_early & _claimed_tile_uuids
            if container_urls_set_early and (url_overlap or uuid_overlap):
                # Overlap found — the tile set has been claimed already.
                overlap = url_overlap if url_overlap else uuid_overlap
                overlap_kind = "url" if url_overlap else "uuid"
                # v529 fix #5: dedup the log spam. The same claimed
                # container gets re-skipped on every scan cycle (every
                # 4s) for as long as ANY pending submission is in flight.
                # Across a long-running batch that's hundreds of identical
                # log lines. Memoize which URL sets we've already announced
                # as claimed and only print on first encounter.
                url_fingerprint = frozenset(container_urls_set_early)
                already_announced = getattr(
                    _run_download_cycle, "_announced_claimed_sets", None)
                if already_announced is None:
                    already_announced = set()
                    _run_download_cycle._announced_claimed_sets = already_announced
                if url_fingerprint not in already_announced:
                    already_announced.add(url_fingerprint)
                    print(f"[API:scan] ⏭ Container already claimed ({len(overlap)}/{len(container_urls_set_early)} {overlap_kind} overlap) — skipping to prevent duplicate attribution", flush=True)
                continue

            # Baseline filter: reject if all of the container's tile URLs
            # were already in the gallery when this job was submitted.
            # Without this, pre-existing tiles from previous worker runs
            # (or from earlier jobs in the same project that happened to
            # use the same prompt_key) get mis-attributed to the new
            # submission because the prompt-text match can't distinguish
            # old-vs-new tiles.
            container_urls = set(c.get("tile_image_urls") or [])
            if container_urls and match.baseline_urls:
                new_urls = container_urls - match.baseline_urls
                if not new_urls:
                    # Every URL in this container predates the submission
                    # → stale. Don't attribute it.
                    continue
                # If SOME urls are new and some aren't, only the new ones
                # belong to this submission. This shouldn't normally happen
                # (a single container corresponds to a single Generate
                # click), but being strict is safer.
                if len(new_urls) != len(container_urls):
                    print(f"[API:scan] ⚠ Node {match.node_id}: container has {len(container_urls)} urls, {len(new_urls)} are new — using new only", flush=True)

            # Fully-failed case: no committed tiles, failure indicator present
            committed = int(c.get("committed_tile_count") or 0)
            if c.get("has_failed") and committed == 0:
                # v793: success-sibling guard. Flow splits a partial x4
                # (some variants policy-rejected, some OK) into a Failed
                # container PLUS a committed container — both carry the
                # same prompt. The Failed one often sits at a lower
                # data-index so it's iterated first; marking the node
                # failed + GC here removes it from pending_live BEFORE the
                # committed sibling is reached, so the good tiles get
                # abandoned (operator saw "worker skipped the ones that
                # worked"). The ID-attribution path already does this
                # right (only fails when `not ready and failed`); the
                # legacy path was missing it. Before failing, look ahead
                # in THIS scan for a settled committed container that
                # matches the same node's full prompt_key (800-char scene
                # body per v733 — discriminating per scene). If one
                # exists, skip this failed container so the success
                # container claims the node next iteration.
                key = match.prompt_key or ""
                _success_sibling = False
                if key and len(key) >= 20:
                    k_norm = "".join(key.split()).lower()
                    for other in containers:
                        if other is c or other.get("still_rendering"):
                            continue
                        if int(other.get("committed_tile_count") or 0) <= 0:
                            continue
                        otext = other.get("prompt_text") or ""
                        if key not in otext and k_norm not in "".join(otext.split()).lower():
                            continue
                        o_urls = set(other.get("tile_image_urls") or [])
                        if o_urls and (o_urls & _claimed_tile_urls):
                            continue  # sibling already claimed elsewhere
                        if match.baseline_urls and o_urls and not (o_urls - match.baseline_urls):
                            continue  # sibling is all-baseline (stale), not this job's result
                        _success_sibling = True
                        break
                if _success_sibling:
                    print(f"[API:scan] ⏭ [v793] Node {match.node_id}: failed container skipped — committed success sibling present in same scan", flush=True)
                    continue
                print(f"[API:scan] ✗ Node {match.node_id} — tile marked Failed (0 successes)", flush=True)
                http_queue.put({
                    "node_id": match.node_id,
                    "failed": True,
                    "error": "Flow returned Failed for this generation",
                })
                match.status = "failed"
                _gc_pending_submission(match.node_id)  # v730
                enqueued += 1
                continue

            # From here on we have at least one committed tile. Decide whether
            # the container has actually settled — i.e. Flow is done producing
            # tiles for this submission, even if some slots failed.
            urls = c.get("tile_image_urls") or []
            if not urls:
                continue  # defensive — shouldn't happen if committed > 0

            expected = int(match.variants or 1)
            now = time.time()

            # Stability check — we accept one of two conditions:
            #   (a) committed_count == expected: all variants landed, done
            #   (b) committed_count is unchanged since last scan AND enough
            #       time has passed since we first saw that count
            # Case (b) handles partial successes (e.g. 3 of 4) where we'll
            # never reach `expected`, so waiting forever is wrong.
            STABILITY_WINDOW_S = 8  # must see the same count twice, >=8s apart

            accept = False
            if committed == expected:
                accept = True
            else:
                # Partial state — do we have a stable reading?
                if match.last_committed_count == committed:
                    # Same count as last scan. Enough time elapsed?
                    if (now - match.last_stable_seen_at) >= STABILITY_WINDOW_S:
                        accept = True
                        print(f"[API:scan] ℹ Node {match.node_id} partial: {committed}/{expected} variants (stable for {int(now - match.last_stable_seen_at)}s) — accepting", flush=True)
                else:
                    # Count changed since last scan — reset stability timer
                    match.last_committed_count = committed
                    match.last_stable_seen_at = now
                    continue  # keep waiting

            if not accept:
                continue

            if cookies is None:
                cookies, user_agent = _snapshot_browser_session()

            # Filter to only the new-since-baseline URLs for download
            download_urls = urls
            if match.baseline_urls:
                download_urls = [u for u in urls if u not in match.baseline_urls]
                if not download_urls:
                    # Safety: shouldn't happen (we already checked above)
                    continue

            print(f"[API:scan] ✓ Node {match.node_id} matched → {len(download_urls)} variant(s) → enqueue (legacy fallback)", flush=True)
            http_queue.put({
                "node_id": match.node_id,
                "urls": download_urls,
                "output_dir": match.output_dir,
                "cookies": cookies,
                "user_agent": user_agent,
            })
            match.status = "downloading"
            _gc_pending_submission(match.node_id)  # v730
            # v458: record these URLs as claimed so future scans can't
            # re-attribute them via Strategy 3 catchall when this
            # submission is no longer in `pending`.
            _mark_urls_claimed(download_urls)
            enqueued += 1

        # v709 — Stuck-submission detection with reload+resubmit retry chain
        now = time.time()
        for job in list(in_flight.values()):
            if job.status != "submitted":
                continue
            age = now - job.submit_time
            if age > STUCK_TIMEOUT:
                print(f"[API:scan] ✗ Node {job.node_id} STUCK ({STUCK_TIMEOUT}s, {job.retry_count}/{STUCK_MAX_RETRIES} retries exhausted) — failing", flush=True)
                http_queue.put({
                    "node_id": job.node_id,
                    "failed": True,
                    "error": f"Stuck after {job.retry_count} retries (>{STUCK_TIMEOUT}s)",
                })
                job.status = "failed"
                _gc_pending_submission(job.node_id)  # v730
            elif age > STUCK_RETRY_TIMEOUT and job.retry_count < STUCK_MAX_RETRIES:
                print(f"[API:scan] ⟳ Node {job.node_id} STUCK ({int(age)}s) — queuing reload+resubmit (attempt {job.retry_count + 1}/{STUCK_MAX_RETRIES})", flush=True)
                job.status = "stuck_retry"

        return enqueued

    def _drain_done_queue():
        """Main-thread only. Pop everything the HTTP worker has reported,
        remove those jobs from in_flight, clean up their temp dirs."""
        nonlocal _last_scan_time  # v791c — re-arm path forces an immediate scan
        while True:
            try:
                msg = done_queue.get_nowait()
            except _queue.Empty:
                break
            nid = msg.get("node_id")

            # v791c — re-resolve path: the HTTP worker hit 403/401/410 on every
            # variant (stale signed URLs) but the tiles are still live in Flow.
            # Re-arm the job so the next scan re-resolves FRESH signed URLs from
            # the DOM tiles and re-enqueues. Bounded by MAX_DL_RETRIES.
            if msg.get("outcome") == "retry_resolve":
                MAX_DL_RETRIES = 3
                rjob = in_flight.get(nid)
                if rjob is None:
                    continue  # already gone — nothing to retry
                if rjob.dl_retry_count >= MAX_DL_RETRIES:
                    err = msg.get("error") or "Download failed (stale signed URLs)"
                    print(f"[API:http] ✗ Node {nid} download failed after {rjob.dl_retry_count} re-resolve attempt(s) — failing ({err})", flush=True)
                    try:
                        _post_status(api_url, api_key, nid, "failed", error=err)
                    except Exception:
                        pass
                    in_flight.pop(nid, None)
                    _gc_pending_submission(nid)
                    try:
                        shutil.rmtree(os.path.dirname(rjob.output_dir), ignore_errors=True)
                    except Exception:
                        pass
                else:
                    rjob.dl_retry_count += 1
                    # Drop the stale network-captured URLs so the next scan
                    # SKIPS Tier A and falls through to the DOM tile-ID lookup,
                    # which reads the current (fresh) signed image_url. Cookies
                    # are re-snapshotted each scan cycle, so an expired-cookie
                    # cause is also covered.
                    captured_urls_by_node.pop(nid, None)
                    rjob.status = "submitted"  # back into the scan's pending set
                    # Force the next main-loop tick to scan immediately (don't
                    # wait up to SCAN_DURING_SUBMIT_INTERVAL) so fresh URLs get
                    # re-resolved ASAP — the whole point of the retry.
                    _last_scan_time = 0.0
                    print(f"[API:http] ⟳ Node {nid} re-resolving fresh signed URLs (attempt {rjob.dl_retry_count}/{MAX_DL_RETRIES})", flush=True)
                continue

            job = in_flight.pop(nid, None)
            _gc_pending_submission(nid)  # v730 — defense in depth: also GC at done-queue drain
            if job is not None:
                try:
                    job_work = os.path.dirname(job.output_dir)
                    shutil.rmtree(job_work, ignore_errors=True)
                except Exception:
                    pass

    # ============================================================
    # MAIN LOOP
    # ============================================================
    # Pattern: each iteration does EITHER a submit OR a scan, never both.
    # Previously we ran scan + submit in the same tick, which meant every
    # submission was preceded by a full gallery scroll-through. With many
    # in-flight submissions that gets expensive and pins the viewport at
    # the bottom of the gallery, which in turn hides newly-completed tiles
    # at data-index=0 from the next scan.
    #
    # Priority order each iteration:
    #   1. Drain the done queue (cheap, no Playwright)
    #   2. If under capacity AND a job is available: SUBMIT (no scan this tick)
    #   3. Else if we have in-flight work: SCAN
    #   4. Else: idle sleep
    consecutive_errors = 0
    # v791b — interleave downloads DURING a submit burst. The submit-or-scan
    # priority below skips the scan on any tick that submitted a job. With a
    # high --parallel slot count the worker submits every queued scene before
    # `active` ever hits capacity, so the first scan (and thus the first
    # download) only fires after the WHOLE batch is submitted — images then
    # all land at the very end. This timer forces a download cycle at least
    # every SCAN_DURING_SUBMIT_INTERVAL seconds even mid-burst, so each ready
    # image is enqueued for download as soon as it completes. Kept time-gated
    # (not every tick) to preserve the "don't scan right after every submit"
    # optimization that avoids pinning the gallery viewport at the bottom.
    SCAN_DURING_SUBMIT_INTERVAL = 20.0  # seconds
    _last_scan_time = 0.0
    # v456: cooldown timer. Set when a claim is released due to batch mismatch.
    # Until this time, skip /jobs/pending polling to avoid thrashing the DB
    # in tight claim/release cycles. With the server-side prefer_batch filter
    # this should rarely trigger, but it's defensive.
    _release_cooldown_until = 0.0
    # v818 — golden-restore signal. _submit_one_job sets _restore_signal['golden']
    # when an account-level 'unusual activity' block persists past cheap cookie-
    # clear recovery. The loop then breaks and returns 'RELAUNCH_GOLDEN' so main()
    # can close the browser, restore the golden profile, and relaunch (the profile
    # dir is locked while Chrome runs, so only main() — which owns the browser —
    # can do it).
    _restore_signal = {"golden": False}
    _exit_action = None
    _session_start = time.time()  # v818.2 — for the relaunch-budget reset in main()
    try:
        while not stop_flag.is_set():
            # 1. Harvest completions from the HTTP worker
            _drain_done_queue()

            # v709 — Handle stuck retries (reload+resubmit) before new work.
            # Scanner flips a stalled job to status="stuck_retry" after
            # STUCK_RETRY_TIMEOUT. Here we pop it from in_flight, call
            # _submit_one_job with the preserved original dict — that re-runs
            # _ensure_project_ready which reloads the page (clearing any
            # Banana 2 stuck SSE state), re-attaches refs, re-pastes the
            # prompt, and clicks Generate. retry_count is carried forward
            # onto the new InFlightJob so subsequent stalls still escalate
            # to the final-fail path.
            stuck_jobs = [j for j in in_flight.values() if j.status == "stuck_retry"]
            if stuck_jobs:
                _stuck = stuck_jobs[0]
                _prev_retry = _stuck.retry_count
                _saved_dict = _stuck._original_job
                _stuck_nid = _stuck.node_id
                in_flight.pop(_stuck_nid, None)
                _gc_pending_submission(_stuck_nid)  # v730 — clear stale entry before resubmit
                print(f"[API:retry] ⟳ Node {_stuck_nid} reload+resubmit (attempt {_prev_retry + 1}/{STUCK_MAX_RETRIES})", flush=True)
                try:
                    _ok = _submit_one_job(_saved_dict) if _saved_dict else False
                    if _ok and _stuck_nid in in_flight:
                        in_flight[_stuck_nid].retry_count = _prev_retry + 1
                        in_flight[_stuck_nid]._original_job = _saved_dict
                        print(f"[API:retry] ✓ Node {_stuck_nid} resubmitted (retry {_prev_retry + 1}/{STUCK_MAX_RETRIES})", flush=True)
                    else:
                        print(f"[API:retry] ✗ Node {_stuck_nid} resubmit failed (no original_job or claim released)", flush=True)
                        http_queue.put({"node_id": _stuck_nid, "failed": True,
                                        "error": f"Stuck retry {_prev_retry + 1} resubmit failed"})
                except Exception as _retry_e:
                    print(f"[API:retry] ✗ Node {_stuck_nid} retry exception: {_retry_e}", flush=True)
                    http_queue.put({"node_id": _stuck_nid, "failed": True,
                                    "error": f"Stuck retry exception: {_retry_e}"})
                time.sleep(API_POLL_BUSY_INTERVAL)
                continue

            # 2. Try to submit if we have capacity
            active = sum(1 for j in in_flight.values()
                         if j.status in ("submitted", "downloading"))

            did_work = False
            # Respect cooldown: if we recently released a cross-batch claim,
            # skip polling until the cooldown expires. Scanning still happens
            # normally so in-flight work drains.
            _poll_allowed = time.time() >= _release_cooldown_until
            if active < parallel_slots and _poll_allowed:
                # v456: derive prefer_batch from any in-flight node's name.
                # The server uses this to prioritize same-batch queued nodes
                # over cross-batch ones, avoiding the claim/release thrash
                # that happens when a new batch is imported mid-processing.
                prefer_batch = None
                if in_flight:
                    try:
                        import re as _re_prefer
                        for _j in in_flight.values():
                            if _j.status not in ("submitted", "downloading"):
                                continue
                            _name = _j.node_name or ""
                            _m = _re_prefer.match(r"^(.*?)Scene\s+\d+", _name, _re_prefer.IGNORECASE)
                            if _m:
                                prefer_batch = _m.group(1)  # keep trailing whitespace — node names have it
                                break
                    except Exception:
                        prefer_batch = None

                try:
                    _params = {"worker_id": worker_id}
                    if prefer_batch:
                        _params["prefer_batch"] = prefer_batch
                    # v753 — exclude nodes already in our local in_flight dict so the
                    # server can't re-serve them. Defense against the duplicate-submit
                    # cycle observed 2026-05-20 (same 3 nodes claimed repeatedly while
                    # already mid-render; each re-serve fired another Banana Generate).
                    if in_flight:
                        _excl = ",".join(str(nid) for nid in in_flight.keys())
                        if _excl:
                            _params["exclude"] = _excl
                    resp = _api_request(api_url, api_key, "GET", "/jobs/pending",
                                        params=_params, timeout=10)
                    consecutive_errors = 0
                    _consecutive_timeouts = 0
                    job = resp.get("job") if isinstance(resp, dict) else None
                    # v753 — hard guard. If server still hands us a node we already
                    # have in_flight (older server without exclude support, race during
                    # status flap, or stale-claim-sweep mid-cycle), skip it. We do NOT
                    # call /release because the existing claim is correct and held by
                    # _this_ worker — releasing would un-claim our legitimately
                    # in-progress render. Just drop the duplicate handout.
                    if job and job.get("id") in in_flight:
                        _dup_id = job.get("id")
                        _existing_status = in_flight[_dup_id].status
                        print(f"[API:submit] ⚠ v753 GUARD — server handed back node {_dup_id} (already in_flight, status={_existing_status}). Skipping duplicate submit.", flush=True)
                        job = None
                except requests.exceptions.HTTPError as he:
                    status = he.response.status_code if he.response is not None else 0
                    if status == 401:
                        print(f"[API] ❌ 401 Unauthorized — wrong API key.", flush=True)
                        return
                    print(f"[API] HTTP error {status}: {he}", flush=True)
                    consecutive_errors += 1
                    time.sleep(min(30, API_POLL_INTERVAL * (2 ** min(consecutive_errors, 5))))
                    continue
                except (requests.exceptions.Timeout,
                        requests.exceptions.ReadTimeout,
                        requests.exceptions.ConnectTimeout,
                        requests.exceptions.ConnectionError):
                    # v550: catch ConnectionError too. urllib3 wraps
                    # repeated ReadTimeoutError into ConnectionError
                    # ("Max retries exceeded ... Caused by ReadTimeoutError")
                    # which is NOT a subclass of requests.Timeout — it
                    # used to fall through to the generic Exception
                    # handler below and spam full tracebacks every poll.
                    #
                    # Compact log: print once on first failure, then
                    # only every 5th retry. Exponential backoff caps
                    # at 60s so a long server outage doesn't burn poll
                    # quota.
                    if not hasattr(api_pull_mode_parallel, "_consec_to"):
                        api_pull_mode_parallel._consec_to = 0
                    api_pull_mode_parallel._consec_to += 1
                    n = api_pull_mode_parallel._consec_to
                    if n == 1:
                        print(f"[API] ⏱ Poll timeout — server slow or network stall. Will keep retrying with backoff.", flush=True)
                    elif n % 5 == 0:
                        backoff = min(60, 5 * (2 ** min(n - 1, 4)))
                        print(f"[API] ⏱ Still timing out after {n} attempts (next backoff {backoff}s) — server may be down or restarting.", flush=True)
                    time.sleep(min(60, 5 * (2 ** min(n - 1, 4))))
                    continue
                except Exception as e:
                    print(f"[API] Poll error: {e}", flush=True)
                    consecutive_errors += 1
                    time.sleep(min(30, API_POLL_INTERVAL * (2 ** min(consecutive_errors, 5))))
                    continue

                # Reset timeout counter on any successful poll (including one
                # that returns no job).
                if hasattr(api_pull_mode_parallel, "_consec_to"):
                    api_pull_mode_parallel._consec_to = 0

                if job:
                    # v756 — a single node's submission must NEVER kill the whole
                    # worker. Previously an unhandled Playwright/attachment error
                    # inside _submit_one_job (e.g. a reference-image attach step)
                    # propagated out of this loop → main()'s fatal handler →
                    # browser.close() → process exit, taking the whole batch and
                    # all in-flight work down with it. Now: a per-node error fails
                    # just that node and the loop keeps running. Only a genuine
                    # browser/page death is allowed to propagate (the worker can't
                    # recover in-loop from that).
                    try:
                        submitted_ok = _submit_one_job(job)
                    except KeyboardInterrupt:
                        raise
                    except Exception as _sub_e:
                        _err = str(_sub_e)
                        _nid = job.get("id")
                        if any(x in _err for x in (
                            "browser has been closed", "Target page",
                            "context or browser", "Target closed", "TargetClosed",
                        )):
                            print(f"[API:submit] ❌ Node {_nid} — browser/page closed mid-submit; worker cannot continue in-loop", flush=True)
                            raise
                        import traceback as _tb
                        print(f"[API:submit] ❌ Node {_nid} submission crashed: {_sub_e} — failing this node, worker continues", flush=True)
                        _tb.print_exc()
                        try:
                            _post_status(api_url, api_key, _nid, "failed", error=f"Submission crashed: {_sub_e}")
                        except Exception:
                            pass
                        try:
                            in_flight.pop(_nid, None)
                            _gc_pending_submission(_nid)  # v730
                        except Exception:
                            pass
                        submitted_ok = False
                        _release_cooldown_until = time.time() + 5
                    # If we actually submitted, this tick did work (skip scan).
                    # If we released the claim (batch mismatch), let the main
                    # loop fall through to scan — we want to drain the current
                    # batch fast so the released node can be re-picked up.
                    if submitted_ok:
                        did_work = True
                    else:
                        # v456: we released a cross-batch claim. This shouldn't
                        # normally happen in v456 (the prefer_batch filter
                        # prevents it) but can fire during a race or if the
                        # in-flight set transitioned to empty between our
                        # prefer_batch computation and the server's query.
                        # Pause briefly so we don't immediately re-poll and
                        # thrash the DB.
                        _release_cooldown_until = time.time() + 30

                    # v818 — persistent unusual-activity → break for a golden
                    # restore + relaunch (handled by main()). The finally block
                    # releases every claim this worker holds, so all in-flight
                    # nodes re-queue and are re-claimed cleanly after relaunch.
                    if _restore_signal["golden"]:
                        print("[API] 🔁 Golden-restore requested — draining + stopping loop for browser relaunch", flush=True)
                        _exit_action = "RELAUNCH_GOLDEN"
                        # v818.2 — how long this session ran before the block, so
                        # main() can reset the relaunch budget after a healthy run.
                        try:
                            api_pull_mode_parallel._last_session_secs = time.time() - _session_start
                        except Exception:
                            pass
                        stop_flag.set()
                        break

            # 3. SCAN for completed tiles → enqueue ready images for download.
            # Normally we never scan on a tick that submitted (the submit just
            # churned the page; a scan right after sees transient UI). But
            # during a long submit burst that rule starves scanning entirely,
            # so ALSO scan if it's been > SCAN_DURING_SUBMIT_INTERVAL since the
            # last one — this drips ready images out as they finish instead of
            # all at the end of the batch (v791b).
            if active > 0:
                _due_for_scan = (time.time() - _last_scan_time) >= SCAN_DURING_SUBMIT_INTERVAL
                if not did_work or _due_for_scan:
                    if did_work and _due_for_scan:
                        print(f"[API:scan] [v791b] interleaved scan during submit burst "
                              f"(active={active}, {time.time() - _last_scan_time:.0f}s since last scan)", flush=True)
                    _run_download_cycle()
                    _last_scan_time = time.time()
                    did_work = True

            # 4. Sleep
            if did_work:
                # Short sleep between iterations when busy
                time.sleep(API_POLL_BUSY_INTERVAL)
            else:
                # Nothing to do — longer sleep before re-polling for jobs
                time.sleep(IDLE_POLL)

    except KeyboardInterrupt:
        print("\n[API] Stopping on user request...", flush=True)
    finally:
        stop_flag.set()
        # v516: tell the webapp we're going offline so the UI flips to
        # "● Offline" within the next poll cycle (~2s) instead of waiting
        # for the 10s heartbeat-stale window. Best-effort — short timeout
        # because we're shutting down and don't want to hang on a slow
        # webapp.
        try:
            rc = _api_request(api_url, api_key, "POST", "/release-claims",
                              params={"worker_id": worker_id, "going_offline": "true"},
                              timeout=3)
            if rc and rc.get("heartbeat_deleted"):
                print("[API] ↩ Notified webapp of graceful shutdown", flush=True)
        except Exception as e:
            print(f"[API] ⚠ Couldn't notify shutdown (UI will flip in ~10s): {e}", flush=True)
        # Give the HTTP worker a chance to drain pending downloads
        print("[API] Waiting up to 60s for in-flight HTTP downloads...", flush=True)
        try:
            http_queue.put(None)  # stop sentinel
        except Exception:
            pass
        http_thread.join(timeout=60)
        if http_thread.is_alive():
            print("[API] ⚠ HTTP worker still running at shutdown", flush=True)
        heartbeat_stop.set()

    # v818 — 'RELAUNCH_GOLDEN' tells main() to golden-restore + relaunch the
    # browser and re-enter; None means a normal (KeyboardInterrupt) shutdown.
    return _exit_action




# ============================================================
# BROWSER LAUNCH
# ============================================================

def launch_browser(session_folder=SESSION_FOLDER):
    """Launch Chrome with Patchright, return (playwright, browser, page)."""
    
    # Kill any stale Chrome using this profile
    kill_chrome_using_profile(session_folder, label="IMAGE")
    time.sleep(1)
    
    # Ensure session folder exists
    os.makedirs(session_folder, exist_ok=True)

    # v814 — laptop-login COPY-MODE (exact parity with flow_worker startup):
    # build the golden directly from the operator's real logged-in Chrome
    # profile BEFORE the golden restore below, so the restored session is
    # already signed in. No-op when no email is configured.
    golden = get_golden_folder(session_folder)
    _maybe_pull_laptop_profile(session_folder, golden, label="IMAGE")

    # Restore from golden if available (v828 — robust retry loop, parity with
    # flow_worker: the old single-attempt copytree silently failed on WinError
    # 1224/32 file locks and shipped a stale profile that kept the block alive).
    if os.path.exists(golden):
        restore_from_golden(session_folder, label="IMAGE")
    
    pw = sync_playwright().start()
    
    launch_args = [
        '--disable-blink-features=AutomationControlled',
        '--disable-dev-shm-usage',
        '--no-sandbox',
        '--force-variation-ids=3300115,3300134,3313321,3328827,3330196,3362821',
        '--disk-cache-size=1',
        '--media-cache-size=1',
    ]
    
    browser = pw.chromium.launch_persistent_context(
        user_data_dir=session_folder,
        channel=_worker_chrome_channel(),  # v814 — sidecar/env, parity with flow_worker
        ignore_default_args=['--enable-automation'],
        headless=False,
        viewport={"width": 1280, "height": 720},
        args=launch_args,
    )
    
    page = browser.pages[0] if browser.pages else browser.new_page()
    _stash_profile_on_page(page, session_folder)  # v486
    print("[IMAGE] ✓ Browser launched", flush=True)

    # Inject the staged laptop-login cookies so this fresh session is already
    # logged into Google — no manual verification code (parity with flow_worker).
    _inject_laptop_cookies(browser, "IMAGE")

    # flow_api: attach the bearer-token sniff listener BEFORE any navigation to
    # Flow happens. The page's initial auth-bearing requests (project navigate,
    # login check, settings hydrate) fire shortly after launch; if the listener
    # isn't attached yet they go uncaptured and _FaClient sees an empty store.
    _fa_attach_global_token_listener(page)

    # v457: minimize the Chrome window immediately so it doesn't steal
    # focus when launched. The window stays running in the taskbar and
    # responds to all Playwright calls normally — it's just not in the
    # user's face. We'll only un-minimize if login is needed.
    try:
        # Tiny delay so Chrome has time to actually open the window
        # before we try to find + minimize its HWND.
        time.sleep(0.5)
        minimize_chrome_window(page, label="IMAGE")
        defocus_chrome(page, label="IMAGE")
    except Exception as e:
        print(f"[IMAGE] ⚠ Couldn't minimize Chrome window (non-fatal): {e}", flush=True)

    # v457: attach a navigation-end handler that re-minimizes Chrome
    # after every page.goto(). Every SPA navigation in Flow triggers
    # either Chrome activation or at minimum a focus pulse; this catches
    # all of them without each call site having to remember to defocus.
    #
    # Skippable: set page._stay_visible = True before login flows so
    # the user can actually see the login prompt. _wait_for_user_login
    # manages this flag.
    page._stay_visible = False

    def _on_navigation_end():
        try:
            if getattr(page, "_stay_visible", False):
                return
            # Small delay before minimizing — Chrome's activation
            # sometimes fires slightly after the load event. We give it
            # a moment, then stuff it back to the taskbar.
            import threading as _threading
            def _delayed():
                try:
                    time.sleep(0.3)
                    if not getattr(page, "_stay_visible", False):
                        minimize_chrome_window(page, label="IMAGE")
                except Exception:
                    pass
            _threading.Thread(target=_delayed, daemon=True).start()
        except Exception:
            pass

    try:
        page.on("load", lambda _: _on_navigation_end())
    except Exception:
        pass

    return pw, browser, page


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description='Image Worker — Local Flow UI automation for Nano Banana',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Text-to-image
  python image_worker.py --prompt "A modern office" --output ./output.png

  # Edit/enhance image
  python image_worker.py --input ./frame.png --prompt "Enhance details" --output ./enhanced.png

  # Batch mode
  python image_worker.py --input-dir ./frames/ --prompt "Enhance" --output-dir ./enhanced/

  # Interactive mode
  python image_worker.py --interactive

  # Use custom session folder
  python image_worker.py --session ./my-chrome-session --interactive
        """
    )
    
    # Mode selection
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--interactive', '-i', action='store_true',
        help='Interactive mode — keep browser open for multiple jobs')
    mode_group.add_argument('--input-dir', type=str,
        help='Batch mode — process all images in directory')
    mode_group.add_argument('--watch', type=str,
        help='Watch-folder mode — poll local folder for .json jobs (same-machine)')
    mode_group.add_argument('--api-url', type=str,
        help='HTTP-pull mode — poll a remote webapp for jobs (cross-machine)')

    # API mode options
    parser.add_argument('--api-key', type=str,
        help='Bearer token for --api-url (defaults to LOCAL_WORKER_API_KEY env var)')
    parser.add_argument('--worker-id', type=str,
        help='Identifier sent with API polls (defaults to image-worker-<hostname>)')
    parser.add_argument('--parallel', type=int, default=3,
        help='Max concurrent in-flight generations in API mode (default: 3; '
             'set to 1 for legacy sequential mode). HAR evidence shows Flow '
             'accepts up to ~5 simultaneous batchGenerateImages POSTs.')
    parser.add_argument('--cross-batch', action='store_true',
        help='Allow simultaneous in-flight jobs across DIFFERENT batches '
             '(different Flow projects). Requires v624 network-listener '
             'attribution to be active so jobs are matched by prompt rather '
             'than DOM tile-id. When off (default), cross-batch jobs are '
             'released and re-queued — preserves the legacy single-project '
             '"safe" mode.')
    
    # Single job args
    parser.add_argument('--input', type=str, action='append', default=[],
        help='Input image path(s) for edit/enhance (can specify multiple)')
    parser.add_argument('--prompt', '-p', type=str, default='',
        help='Generation/edit prompt')
    parser.add_argument('--output', '-o', type=str, default='output.png',
        help='Output image path (default: output.png)')
    parser.add_argument('--output-dir', type=str,
        help='Output directory for batch mode')
    
    # Settings
    parser.add_argument('--aspect-ratio', type=str, default='16:9',
        choices=['1:1', '1:4', '1:8', '2:3', '3:2', '3:4', '4:1', '4:3',
                 '4:5', '5:4', '8:1', '9:16', '16:9', '21:9'],
        help='Aspect ratio (default: 16:9)')
    parser.add_argument('--resolution', type=str, default='1K',
        choices=['512', '1K', '2K', '4K'],
        help='Resolution (default: 1K)')
    parser.add_argument('--model', type=str, default='nano_banana_2',
        choices=['nano_banana_2', 'nano_banana_pro'],
        help='Model (default: nano_banana_2)')
    
    # Browser
    parser.add_argument('--session', type=str, default=SESSION_FOLDER,
        help='Chrome session folder path')
    parser.add_argument('--no-warmup', action='store_true',
        help='Skip Chrome warmup (faster but may trigger reCAPTCHA)')
    
    args = parser.parse_args()
    
    # Validate
    if not args.interactive and not args.input_dir and not args.watch and not args.api_url and not args.prompt:
        parser.print_help()
        print("\n❌ Either --prompt, --input-dir, --watch, --api-url, or --interactive is required")
        sys.exit(1)
    
    print("="*60)
    print(f"IMAGE WORKER {WORKER_VERSION}")
    print("="*60)

    # Laptop-login parity with the video worker: sync the helper modules next to
    # this worker (the installer only ships image_worker.py) so the startup pull
    # can read the configured email + reuse the operator's Google login.
    _sync_companion_modules(args.api_url or os.environ.get("WEB_APP_URL") or os.environ.get("APP_URL"))

    # Launch browser
    pw, browser, page = launch_browser(session_folder=args.session)
    
    try:
        # Chrome warmup
        if not args.no_warmup:
            try:
                chrome_warmup(page)
            except Exception:
                print("[IMAGE] Browser died during warmup — relaunching...", flush=True)
                try:
                    browser.close()
                except Exception:
                    pass
                time.sleep(2)
                browser = pw.chromium.launch_persistent_context(
                    user_data_dir=args.session,
                    channel=_worker_chrome_channel(),  # v814
                    ignore_default_args=['--enable-automation'],
                    headless=False,
                    viewport={"width": 1280, "height": 720},
                    args=['--disable-blink-features=AutomationControlled',
                          '--disable-dev-shm-usage', '--no-sandbox'],
                )
                page = browser.pages[0] if browser.pages else browser.new_page()
                _stash_profile_on_page(page, args.session)  # v486
                _inject_laptop_cookies(browser, "IMAGE")  # re-inject after relaunch
        
        # Navigate to Flow and verify login
        print("[IMAGE] Navigating to Flow...", flush=True)
        page.goto(FLOW_HOME_URL)
        human_delay(2, 4)
        human_mouse_move(page)
        human_delay(1, 2)
        scroll_randomly(page)
        human_delay(0.5, 1)
        
        ensure_logged_into_flow(page, "IMAGE")
        print("[IMAGE] ✓ Logged in and ready\n", flush=True)
        
        # Route to mode
        if args.interactive:
            interactive_mode(page)

        elif args.api_url:
            api_key = args.api_key or os.environ.get("LOCAL_WORKER_API_KEY")
            if not api_key:
                print("[IMAGE] ERROR: no worker token. Re-download the installer "
                      "from the website (it bakes your per-account token).", flush=True)
                sys.exit(1)
            if args.parallel >= 2:
                cross_batch_mode = bool(getattr(args, 'cross_batch', False))
                # Cross-batch auto-enables once inside api_pull_mode_parallel
                # whenever the v624 listener attaches, even without the flag.
                # Print the explicit-flag state here for debug clarity; the
                # actual runtime decision is logged once the listener attaches.
                print(f"[IMAGE] Parallel mode — {args.parallel} concurrent slots"
                      f"{' (cross-batch flag set)' if cross_batch_mode else ' (cross-batch auto-on if listener attaches)'}", flush=True)
                # v818 — golden-restore relaunch loop. api_pull_mode_parallel
                # returns 'RELAUNCH_GOLDEN' when an account-level 'unusual
                # activity' block persists past cheap cookie-clear recovery.
                # main() owns the browser, so only here can we close it, restore
                # the golden profile (launch_browser does this), and re-enter the
                # poll loop. Bounded so a hard-flagged account eventually stops
                # instead of relaunch-looping forever.
                MAX_GOLDEN_RELAUNCHES = 4  # v828 — parity with flow_worker MAX_UNUSUAL_GOLDEN_RESTORES
                HEALTHY_SESSION_SECS = 180  # a run this long before a block = healthy → reset budget
                _relaunch_n = 0
                while True:
                    _action = api_pull_mode_parallel(page, args.api_url, api_key,
                                                     worker_id=args.worker_id,
                                                     parallel_slots=args.parallel,
                                                     cross_batch=cross_batch_mode)
                    if _action != "RELAUNCH_GOLDEN":
                        break
                    # v818.2 — a session that ran healthy for a while before the
                    # block shouldn't spend the relaunch budget reserved for
                    # back-to-back blocks. Reset the counter for a productive run.
                    _sess_secs = getattr(api_pull_mode_parallel, "_last_session_secs", 0)
                    if _sess_secs > HEALTHY_SESSION_SECS:
                        _relaunch_n = 0
                    _relaunch_n += 1
                    if _relaunch_n > MAX_GOLDEN_RELAUNCHES:
                        # v828 — account-global block persisted past the restore
                        # budget. Stop cleanly (do NOT keep claiming new jobs into
                        # an active block — that churns the queue and deepens the
                        # flag). The finally-block /release-claims re-queues every
                        # in-flight node → nothing is lost, jobs run when a later
                        # worker starts after the block clears.
                        print(f"[IMAGE] ⛔ Unusual-activity persisted after {MAX_GOLDEN_RELAUNCHES} golden "
                              f"restores — the Google account is rate-limited. Stopping the worker; in-flight "
                              f"jobs are released back to pending and will run when you relaunch after the "
                              f"block clears. Not lost, not failed.", flush=True)
                        break
                    print(f"[IMAGE] 🔁 Golden restore + relaunch {_relaunch_n}/{MAX_GOLDEN_RELAUNCHES} "
                          f"(unusual-activity persisted)...", flush=True)
                    try:
                        browser.close()
                    except Exception:
                        pass
                    try:
                        pw.stop()
                    except Exception:
                        pass
                    time.sleep(3)
                    # launch_browser restores from golden + rebuilds a clean,
                    # signed-in session, then relaunches Chrome.
                    pw, browser, page = launch_browser(session_folder=args.session)
                    try:
                        if not args.no_warmup:
                            chrome_warmup(page)
                    except Exception:
                        pass
                    # v818.1 — a relaunch that can't reach Flow / re-login means
                    # the block hasn't cleared (or the network is down). Stop the
                    # relaunch loop cleanly instead of crashing with an unhandled
                    # traceback; the operator restarts the worker later.
                    try:
                        page.goto(FLOW_HOME_URL)
                        human_delay(2, 4)
                        ensure_logged_into_flow(page, "IMAGE")
                    except Exception as _rl_e:
                        print(f"[IMAGE] ⛔ Relaunch couldn't reach/verify Flow ({_rl_e}) — "
                              f"stopping worker (restart it later when the block clears).", flush=True)
                        break
                    print("[IMAGE] ✓ Relaunched from golden — resuming API poll (API stays primary)", flush=True)
            else:
                print(f"[IMAGE] Sequential mode (legacy — --parallel 1)", flush=True)
                api_pull_mode(page, args.api_url, api_key, worker_id=args.worker_id)

        elif args.watch:
            watch_folder_mode(page, args.watch)

        elif args.input_dir:
            output_dir = args.output_dir or os.path.join(args.input_dir, "output")
            process_batch(page, args.input_dir, args.prompt, output_dir,
                          aspect_ratio=args.aspect_ratio,
                          resolution=args.resolution,
                          model=args.model)
        
        else:
            # Single job
            process_image_job(
                page,
                input_paths=args.input,
                prompt=args.prompt,
                output_path=args.output,
                aspect_ratio=args.aspect_ratio,
                resolution=args.resolution,
                model=args.model,
            )
    
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n[IMAGE] Closing browser...", flush=True)
        try:
            browser.close()
        except Exception:
            pass
        try:
            pw.stop()
        except Exception:
            pass
        print("[IMAGE] Done.")


if __name__ == "__main__":
    main()