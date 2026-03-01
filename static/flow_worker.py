#!/usr/bin/env python3
"""
Local Flow Worker V2 - Parallel Download Architecture

Processes Flow jobs from the Veo web app with:
- Two parallel browser sessions (submit + download)
- Queue-based download system
- Auto-timing for generation wait
- Cache/resume capability
- Stealth mode to avoid bot detection
- Uses Patchright (undetected Playwright fork) to bypass reCAPTCHA CDP detection
- Uses REAL Chrome browser (not Playwright's bundled Chromium)
"""

import os, re
# Build version for auto-update checking (bump this with each deploy)
WORKER_BUILD = "2025.03.01a"
# Display version from parent folder name (for logs)
WORKER_VERSION = os.path.basename(os.path.dirname(os.path.abspath(__file__)))

import subprocess, sys, shutil

def _ensure_patchright():
    """Auto-install patchright if not already installed. REQUIRED for reCAPTCHA bypass."""
    try:
        import patchright
        print("[Init] ✓ Patchright already installed", flush=True)
        return True
    except ImportError:
        pass
    
    print("[Init] ═══════════════════════════════════════════════════", flush=True)
    print("[Init] Patchright not found — installing automatically...", flush=True)
    print("[Init] ═══════════════════════════════════════════════════", flush=True)
    
    # Step 1: pip install patchright (try multiple methods)
    pip_ok = False
    methods = [
        ([sys.executable, "-m", "pip", "install", "patchright"], "pip install"),
        ([sys.executable, "-m", "pip", "install", "--user", "patchright"], "pip install --user"),
    ]
    # Also try pip/pip3 directly from PATH
    for pip_cmd in ["pip", "pip3"]:
        p = shutil.which(pip_cmd)
        if p:
            methods.append(([p, "install", "patchright"], f"{pip_cmd} install"))
    
    for cmd, label in methods:
        try:
            print(f"[Init] Trying: {label}...", flush=True)
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            if result.returncode == 0:
                print(f"[Init] ✓ {label} succeeded", flush=True)
                pip_ok = True
                break
            else:
                print(f"[Init] ✗ {label} failed (rc={result.returncode}): {result.stderr[:200]}", flush=True)
        except Exception as e:
            print(f"[Init] ✗ {label} exception: {e}", flush=True)
    
    if not pip_ok:
        print("[Init] ❌ ALL pip install methods failed!", flush=True)
        print("[Init] ╔══════════════════════════════════════════════════════════╗", flush=True)
        print("[Init] ║  MANUAL INSTALL REQUIRED — run these commands:          ║", flush=True)
        print("[Init] ║  pip install patchright                                 ║", flush=True)
        print("[Init] ║  patchright install chromium                            ║", flush=True)
        print("[Init] ║  Then restart this worker.                              ║", flush=True)
        print("[Init] ╚══════════════════════════════════════════════════════════╝", flush=True)
        return False
    
    # Step 2: Install Chromium browser for Patchright 
    # (channel='chrome' uses system Chrome but Patchright still needs its browser registered)
    print("[Init] Installing browser for Patchright...", flush=True)
    browser_cmds = []
    pr_cmd = shutil.which("patchright")
    if pr_cmd:
        browser_cmds.append(([pr_cmd, "install", "chromium"], "patchright install chromium"))
    browser_cmds.append(([sys.executable, "-m", "patchright", "install", "chromium"], "python -m patchright install chromium"))
    
    for cmd, label in browser_cmds:
        try:
            print(f"[Init] Trying: {label}...", flush=True)
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            if result.returncode == 0:
                print(f"[Init] ✓ {label} succeeded", flush=True)
                break
            else:
                print(f"[Init] ⚠ {label} failed (rc={result.returncode}): {result.stderr[:200]}", flush=True)
        except Exception as e:
            print(f"[Init] ⚠ {label} exception: {e}", flush=True)
    # Browser install failure is non-fatal when using channel='chrome' (system Chrome)
    
    # Verify import works
    try:
        import patchright
        print("[Init] ✓ Patchright verified importable", flush=True)
        return True
    except ImportError:
        # Sometimes after --user install, the path isn't updated in current process
        # Try adding user site-packages to path
        import site
        user_site = site.getusersitepackages()
        if user_site not in sys.path:
            sys.path.insert(0, user_site)
            print(f"[Init] Added {user_site} to sys.path", flush=True)
            try:
                import patchright
                print("[Init] ✓ Patchright verified after path fix", flush=True)
                return True
            except ImportError:
                pass
        
        print("[Init] ❌ Patchright still not importable after install!", flush=True)
        print("[Init] ╔══════════════════════════════════════════════════════════╗", flush=True)
        print("[Init] ║  MANUAL INSTALL REQUIRED — run these commands:          ║", flush=True)
        print("[Init] ║  pip install patchright                                 ║", flush=True)
        print("[Init] ║  patchright install chromium                            ║", flush=True)
        print("[Init] ║  Then restart this worker.                              ║", flush=True)
        print("[Init] ╚══════════════════════════════════════════════════════════╝", flush=True)
        return False

_patchright_ok = _ensure_patchright()

if _patchright_ok:
    from patchright.sync_api import sync_playwright
    print("[Init] ✓ Using Patchright (undetected Playwright fork — CDP detection bypass active)")
else:
    print("[Init] ╔══════════════════════════════════════════════════════════════╗", flush=True)
    print("[Init] ║  ⚠ WARNING: Running WITHOUT Patchright!                    ║", flush=True)
    print("[Init] ║  reCAPTCHA WILL detect automation → 403 errors expected.   ║", flush=True)
    print("[Init] ║  Install manually: pip install patchright                  ║", flush=True)
    print("[Init] ╚══════════════════════════════════════════════════════════════╝", flush=True)
    from playwright.sync_api import sync_playwright
import requests
import time
import random
import os
import json
import tempfile
import shutil
import threading
import queue
from queue import Queue
from datetime import datetime, timedelta


# ============================================================
# STEALTH SCRIPT - Anti reCAPTCHA Enterprise
# ============================================================
# reCAPTCHA Enterprise checks: webdriver flag, CDP artifacts,
# plugin/mimeType arrays, chrome.runtime, permissions API,
# and stack traces for automation frameworks.

# MINIMAL stealth script - matches test_human_like.py which keeps working.
# On real Chrome (channel='chrome'), navigator.plugins and chrome.runtime already
# exist natively. Overwriting them with fakes creates detectable inconsistencies
# that reCAPTCHA Enterprise can flag. Only patch what Playwright actually breaks.
STEALTH_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', {
    get: () => undefined
});
"""

# Full stealth script kept as fallback for bundled Chromium (non-stealth mode)
STEALTH_SCRIPT_FULL = """
// 1. Remove webdriver flag
Object.defineProperty(navigator, 'webdriver', {
    get: () => undefined,
    configurable: true
});

// 1b. Force English language for all web content
Object.defineProperty(navigator, 'language', { get: () => 'en-US', configurable: true });
Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'], configurable: true });

// 2. Fix chrome.runtime (Playwright leaves it missing or broken)
if (!window.chrome) window.chrome = {};
if (!window.chrome.runtime) {
    window.chrome.runtime = {
        connect: function() {},
        sendMessage: function() {},
        onMessage: { addListener: function() {} },
        id: undefined
    };
}

// 3. Fix plugins array (headless/automation has empty plugins)
if (navigator.plugins.length === 0) {
    Object.defineProperty(navigator, 'plugins', {
        get: () => {
            const p = [
                { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
                { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: '' },
                { name: 'Native Client', filename: 'internal-nacl-plugin', description: '' }
            ];
            p.item = (i) => p[i];
            p.namedItem = (n) => p.find(pp => pp.name === n);
            p.refresh = () => {};
            return p;
        },
        configurable: true
    });
}

// 4. Fix permissions API (reCAPTCHA queries notification permission)
const originalQuery = window.navigator.permissions?.query;
if (originalQuery) {
    window.navigator.permissions.query = (parameters) => (
        parameters.name === 'notifications'
            ? Promise.resolve({ state: Notification.permission })
            : originalQuery(parameters)
    );
}

// 5. Prevent Playwright evaluation script detection in stack traces
// reCAPTCHA can inspect Error().stack for __playwright_evaluation_script__
const originalError = Error;
const patchedError = function(...args) {
    const err = new originalError(...args);
    const originalStack = err.stack;
    if (originalStack) {
        Object.defineProperty(err, 'stack', {
            get: () => originalStack.replace(/__playwright_evaluation_script__/g, '<anonymous>'),
            configurable: true
        });
    }
    return err;
};
patchedError.prototype = originalError.prototype;
patchedError.captureStackTrace = originalError.captureStackTrace;
patchedError.stackTraceLimit = originalError.stackTraceLimit;
// Note: We do NOT replace window.Error globally as that itself is detectable.
// Instead, the webdriver removal + chrome.runtime fix handles most detection.
"""


# ============================================================
# HUMAN-LIKE BEHAVIOR HELPERS
# ============================================================

def human_delay(min_sec=0.5, max_sec=1.5):
    """Random delay to simulate human behavior with natural variance"""
    # Add occasional longer pauses (like a human getting distracted or thinking)
    if random.random() < 0.08:  # 8% chance of longer pause
        delay = random.uniform(max_sec * 1.5, max_sec * 3)
    elif random.random() < 0.15:  # 15% chance of slightly longer pause
        delay = random.uniform(max_sec, max_sec * 1.5)
    else:
        delay = random.uniform(min_sec, max_sec)
    time.sleep(delay)
    return delay


def chrome_warmup(page):
    """Warm up Chrome by visiting Google pages first.
    
    This triggers Chrome's VariationsService to sync with clientservices.googleapis.com
    and download the variations seed. Without this, the x-client-data header sent to
    Google properties has only 1 trial ID (looks like fresh/automated install).
    A real Chrome with synced variations has 5-6+ trial IDs.
    
    reCAPTCHA Enterprise checks x-client-data — a short header = low trust score = 403.
    """
    try:
        print("[Warmup] Loading Google pages to sync Chrome variations...", flush=True)
        
        # Intercept requests to capture x-client-data header
        x_client_data = {}
        def capture_header(request):
            xcd = request.headers.get('x-client-data', '')
            if xcd and len(xcd) > len(x_client_data.get('value', '')):
                x_client_data['value'] = xcd
        
        page.on("request", capture_header)
        
        # Visit Google — triggers variations seed download
        page.goto("https://www.google.com")
        human_delay(3, 5)
        
        # Do some human-like browsing (builds interaction history for reCAPTCHA)
        human_mouse_move(page)
        human_delay(1, 2)
        scroll_randomly(page)
        human_delay(1, 2)
        
        # Visit YouTube — another Google property, more time for sync
        page.goto("https://www.youtube.com")
        human_delay(3, 5)
        human_mouse_move(page)
        human_delay(1, 2)
        scroll_randomly(page)
        human_delay(1, 2)
        
        # Remove listener
        page.remove_listener("request", capture_header)
        
        # Report x-client-data status
        xcd = x_client_data.get('value', '')
        if xcd:
            print(f"[Warmup] ✓ x-client-data captured: {len(xcd)} chars ({xcd[:30]}...)", flush=True)
            if len(xcd) < 20:
                print(f"[Warmup] ⚠ SHORT x-client-data — variations may not have synced yet", flush=True)
                # Extra wait for sync
                human_delay(5, 8)
                page.goto("https://www.google.com")
                human_delay(3, 5)
        else:
            print(f"[Warmup] ⚠ No x-client-data header captured", flush=True)
        
        print("[Warmup] ✓ Chrome warmup complete", flush=True)
    except Exception as e:
        print(f"[Warmup] ⚠ Warmup failed (non-fatal): {e}", flush=True)


_last_mouse_pos = {'x': 640, 'y': 360}  # Track mouse position globally

def human_mouse_move_to(page, target_x, target_y, steps=None):
    """Move mouse to target with Bezier curve from CURRENT position (not random).
    
    reCAPTCHA Enterprise tracks continuous mouse trajectory. Starting from a
    random position creates a visible 'teleport' that scores poorly. We track
    the last known position and move smoothly from there.
    """
    global _last_mouse_pos
    try:
        start_x = _last_mouse_pos['x']
        start_y = _last_mouse_pos['y']
        
        if steps is None:
            dist = ((target_x - start_x)**2 + (target_y - start_y)**2)**0.5
            steps = max(8, min(25, int(dist / 40)))
        
        # Bezier control point (creates an arc)
        ctrl_x = (start_x + target_x) / 2 + random.randint(-80, 80)
        ctrl_y = (start_y + target_y) / 2 + random.randint(-60, 60)
        
        for i in range(steps + 1):
            t = i / steps
            # Quadratic Bezier
            x = (1-t)**2 * start_x + 2*(1-t)*t * ctrl_x + t**2 * target_x
            y = (1-t)**2 * start_y + 2*(1-t)*t * ctrl_y + t**2 * target_y
            # Micro-jitter (human hands aren't perfectly steady)
            x += random.uniform(-1.2, 1.2)
            y += random.uniform(-1.2, 1.2)
            page.mouse.move(x, y)
            # Variable speed - slower at start and end (ease-in-out)
            speed = 0.008 + 0.025 * (1 - abs(2*t - 1))
            time.sleep(speed + random.uniform(0, 0.008))
        
        _last_mouse_pos['x'] = target_x
        _last_mouse_pos['y'] = target_y
    except:
        pass


def human_type(page, selector, text, clear_first=True):
    """Type text in a human-like way with random delays between keystrokes"""
    element = page.locator(selector)
    if clear_first:
        element.clear()
    
    for char in text:
        element.type(char, delay=random.randint(30, 120))  # Random delay per keystroke
        
        # Occasional longer pause (like thinking)
        if random.random() < 0.05:  # 5% chance
            time.sleep(random.uniform(0.2, 0.5))
    
    human_delay(0.2, 0.5)


def random_mouse_movement(page):
    """Random mouse movement to appear more human — tracks position"""
    global _last_mouse_pos
    try:
        viewport = page.viewport_size
        if viewport:
            for _ in range(random.randint(2, 4)):
                target_x = random.randint(100, min(viewport['width'] - 100, 800))
                target_y = random.randint(100, min(viewport['height'] - 100, 600))
                human_mouse_move_to(page, target_x, target_y, steps=random.randint(4, 10))
                time.sleep(random.uniform(0.05, 0.15))
    except:
        pass


def human_scroll(page, direction='down', amount=None):
    """Human-like scrolling with variable speed"""
    if amount is None:
        amount = random.randint(150, 400)
    
    if direction == 'up':
        amount = -amount
    
    # Scroll in multiple small increments
    steps = random.randint(3, 6)
    step_amount = amount / steps
    
    for _ in range(steps):
        page.mouse.wheel(0, step_amount)
        time.sleep(random.uniform(0.02, 0.08))
    
    human_delay(0.3, 0.8)


def human_click_at(page):
    """Perform a human-like click at the current mouse position.
    
    Uses mousedown → hold → mouseup instead of synthetic .click().
    reCAPTCHA Enterprise can detect synthetic click events dispatched by
    Playwright's .click(). Real mouse events from down/up are indistinguishable
    from human clicks.
    """
    page.mouse.down()
    time.sleep(random.uniform(0.05, 0.15))  # Human hold duration: 50-150ms
    page.mouse.up()


def human_click_for_file_chooser(page, btn_locator):
    """Move mouse to button and click with human-like behavior.
    
    Used for upload buttons inside dialogs where we need the file chooser to trigger.
    Returns nothing — the caller wraps this in expect_file_chooser.
    """
    box = btn_locator.bounding_box()
    if box:
        target_x = box['x'] + box['width'] * random.uniform(0.3, 0.7)
        target_y = box['y'] + box['height'] * random.uniform(0.3, 0.7)
        human_mouse_move_to(page, target_x, target_y)
        time.sleep(random.uniform(0.1, 0.25))
        human_click_at(page)
    else:
        btn_locator.click(timeout=3000)


def human_click_locator(page, locator, label="", timeout=5000):
    """Humanized click on a Playwright locator: move mouse → hover → mousedown/up.
    
    Lighter than human_click_element — no selector resolution needed.
    Use when you already have a locator reference.
    """
    try:
        locator.wait_for(state="visible", timeout=timeout)
        box = locator.bounding_box()
        if box:
            target_x = box['x'] + box['width'] * random.uniform(0.3, 0.7)
            target_y = box['y'] + box['height'] * random.uniform(0.3, 0.7)
            human_mouse_move_to(page, target_x, target_y)
            time.sleep(random.uniform(0.12, 0.35))  # Hover pause
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


def human_click_element(page, selector_or_locator, label="", timeout=10000):
    """
    Click an element with natural human-like mouse movement and realistic click timing.
    
    Key differences from Playwright's .click():
    1. Moves from current mouse position (not teleport)
    2. Bezier curve path (not straight line)
    3. Brief hover pause before clicking (human visual confirmation)
    4. Separate mousedown → delay → mouseup (real clicks have 50-150ms hold)
    5. Tracks mouse position for next movement
    """
    global _last_mouse_pos
    try:
        # Get the locator
        if isinstance(selector_or_locator, str):
            element = page.locator(selector_or_locator).first
        else:
            element = selector_or_locator
        
        # Wait for element to be visible
        element.wait_for(state="visible", timeout=timeout)
        
        # Get element's bounding box for mouse movement
        box = element.bounding_box()
        
        if box:
            # Calculate target with natural randomness (don't always hit dead center)
            target_x = box['x'] + box['width'] * random.uniform(0.3, 0.7)
            target_y = box['y'] + box['height'] * random.uniform(0.3, 0.7)
            
            # Move mouse with Bezier curve from current position
            human_mouse_move_to(page, target_x, target_y)
            
            # Hover pause — human visual confirmation before clicking (150-400ms)
            time.sleep(random.uniform(0.15, 0.40))
            
            # Realistic click: separate mousedown and mouseup with hold duration
            # Real human clicks hold the button for 50-150ms
            page.mouse.down()
            time.sleep(random.uniform(0.05, 0.15))
            page.mouse.up()
            
            # Update tracked position
            _last_mouse_pos['x'] = target_x
            _last_mouse_pos['y'] = target_y
        else:
            # Fallback to element.click() if no bounding box
            element.click(timeout=timeout)
        
        if label:
            print(f"✓ Clicked: {label}", flush=True)
        
        # Post-click pause (human reaction time)
        time.sleep(random.uniform(0.3, 0.7))
        return True
            
    except Exception as e:
        if label:
            print(f"❌ Click failed for {label}: {e}", flush=True)
        return False


def human_click_selector(page, selector, label="", timeout=10000):
    """Convenience wrapper for human_click_element with a selector string."""
    return human_click_element(page, selector, label, timeout)


def dismiss_create_with_flow(page, label=""):
    """Check for and click the 'Create with Flow' splash button if present.
    
    Google sometimes shows this overlay when starting a new Flow session.
    Must be clicked before the 'New project' button becomes available.
    """
    try:
        btn = page.locator("button:has-text('Create with Flow')").first
        if btn.is_visible(timeout=2000):
            print(f"[{label or 'Flow'}] Found 'Create with Flow' button — clicking...", flush=True)
            human_click_element(page, btn, f"[{label or 'Flow'}] Create with Flow")
            time.sleep(2)
            return True
    except Exception:
        pass  # Not present — normal
    return False


def ensure_logged_into_flow(page, label="Flow", timeout_minutes=10):
    """Ensure the page is on Flow and the user is logged in.
    
    This is the SINGLE entry point for all login/navigation logic.
    It handles every state the page could be in:
    
    States:
      A) Already on Flow + logged in (project page or home with "New project")
         → Return immediately
      B) Already on Flow but NOT logged in (landing page with "Create with Flow")
         → Click "Create with Flow" → wait for Google login → wait for user → return
      C) On Google login/OAuth page
         → Wait for user to complete login → return
      D) Somewhere else (blank, error, etc.)
         → Navigate to Flow → handle result (loops back to A/B/C)
    
    Returns True if login was required, False if already logged in.
    """
    
    def _get_page_state(p):
        """Determine current page state. Returns one of:
        'flow_logged_in', 'flow_not_logged_in', 'google_login', 'google_redirect', 'other'
        """
        try:
            url = p.url.lower()
        except Exception:
            return 'other'
        
        # Quick dismiss of Chrome browser dialogs that block interaction
        try:
            for btn_text in ["Use Chrome without an account", "No thanks", "Not now"]:
                btn = p.locator(f"button:has-text('{btn_text}')")
                if btn.count() > 0 and btn.first.is_visible(timeout=500):
                    btn.first.click(force=True)
                    print(f"[Login] ✓ Dismissed Chrome dialog ({btn_text})", flush=True)
                    time.sleep(1)
                    break
        except:
            pass
        
        # Google redirect in progress (SetSID, OAuth consent, etc.)
        if "accounts.google" in url and ("setsid" in url or "consent" in url):
            return 'google_redirect'
        
        # Google login/signin page
        if "accounts.google" in url:
            return 'google_login'
        
        # On Flow URL (handles locale: /fx/es-419/tools/flow, /fx/tools/flow, etc.)
        if is_flow_url(url):
            # On a project page = definitely logged in
            if is_flow_project(url):
                return 'flow_logged_in'
            
            # Check DOM for login state — multiple indicators
            # New project button text varies by locale: "New project", "Nuevo proyecto", "Dự án mới", etc.
            # So we check multiple selectors, not just English text
            logged_in_selectors = [
                "button:has-text('New project')",          # English
                "button:has-text('new')",                   # Partial match
                "button:has(i:text('add_2'))",             # Icon-based (locale-independent!)
                "button:has-text('Learn more about')",     # "Meet the new Flow" splash = logged in
                "button:has-text('what\\'s new')",          # Part of splash banner
                "text=Meet the new Flow",                   # Splash banner text
                "button.sc-a38764c7-0",                    # New project button class
            ]
            
            for selector in logged_in_selectors:
                try:
                    if p.locator(selector).first.is_visible(timeout=1500):
                        return 'flow_logged_in'
                except Exception:
                    pass
            
            # Check for "Create with Flow" (old splash, means NOT logged in)
            try:
                if p.locator("button:has-text('Create with Flow')").is_visible(timeout=1500):
                    return 'flow_not_logged_in'
            except Exception:
                pass
            
            # Neither found — page might still be loading. Wait and retry once.
            time.sleep(2)
            
            for selector in logged_in_selectors:
                try:
                    if p.locator(selector).first.is_visible(timeout=1500):
                        return 'flow_logged_in'
                except Exception:
                    pass
            
            try:
                if p.locator("button:has-text('Create with Flow')").is_visible(timeout=1500):
                    return 'flow_not_logged_in'
            except Exception:
                pass
            
            # Still nothing — could be a project page without buttons visible
            # or a slow-loading page. Return 'other' to trigger re-navigation.
            return 'other'
        
        return 'other'
    
    def _wait_for_page_settle(p, max_seconds=30):
        """Wait for redirects to settle. Returns the final state."""
        for i in range(max_seconds):
            time.sleep(1)
            state = _get_page_state(p)
            if state != 'google_redirect':
                return state
            if i % 5 == 4:
                try:
                    print(f"[{label}] Still redirecting... ({p.url[:80]})", flush=True)
                except Exception:
                    pass
        return _get_page_state(p)
    
    def _navigate_to_flow(p):
        """Navigate to Flow, handling redirect chains."""
        try:
            p.goto(FLOW_HOME_URL)
        except Exception:
            pass  # Redirect interruptions expected
        return _wait_for_page_settle(p)
    
    def _wait_for_user_login(p):
        """Wait for user to complete Google login."""
        print(f"\n{'='*50}", flush=True)
        print(f"[{label}] GOOGLE LOGIN REQUIRED", flush=True)
        print(f"Please complete login in the browser...", flush=True)
        print(f"{'='*50}\n", flush=True)
        
        start_time = time.time()
        max_wait = timeout_minutes * 60
        last_url = ""
        
        while True:
            time.sleep(2)
            state = _get_page_state(p)
            
            if state in ('flow_logged_in', 'flow_not_logged_in'):
                # Reached Flow — if not logged in, the outer loop will handle it
                print(f"✓ [{label}] Login completed! (reached Flow)", flush=True)
                time.sleep(3)
                return
            
            # Log URL changes
            try:
                url = p.url.lower()
                if url != last_url:
                    print(f"[{label}] URL: {url[:80]}...", flush=True)
                    last_url = url
            except Exception:
                pass
            
            elapsed = time.time() - start_time
            if elapsed > max_wait:
                print(f"⚠️ [{label}] Login timeout after {timeout_minutes} minutes!", flush=True)
                return
            if int(elapsed) % 30 == 0 and int(elapsed) > 0:
                print(f"[{label}] Still waiting for login... ({int(elapsed)}s)", flush=True)
    
    # ── Main logic: loop until logged into Flow ──
    max_attempts = 5
    for attempt in range(max_attempts):
        state = _get_page_state(page)
        
        if state == 'flow_logged_in':
            if attempt == 0:
                print(f"[{label}] ✓ Already logged in on Flow", flush=True)
            check_and_dismiss_popup(page)
            # Export storage state for download browser
            try:
                context = page.context
                storage_file = os.path.join(BASE_DIR, ".submit_storage_state.json")
                context.storage_state(path=storage_file)
            except Exception:
                pass
            return False
        
        elif state == 'flow_not_logged_in':
            print(f"[{label}] On Flow but not logged in — clicking 'Create with Flow'...", flush=True)
            try:
                btn = page.locator("button:has-text('Create with Flow')").first
                human_click_element(page, btn, f"[{label}] Create with Flow")
            except Exception as e:
                print(f"[{label}] ⚠ Could not click Create with Flow: {e}", flush=True)
            # Wait for redirect to settle
            state = _wait_for_page_settle(page, max_seconds=15)
            if state == 'google_login':
                _wait_for_user_login(page)
            elif state == 'flow_logged_in':
                continue  # Will be caught at top of loop
            # Loop back to re-check state
            continue
        
        elif state == 'google_login':
            _wait_for_user_login(page)
            # After login, Google redirects to Flow — wait for settle
            _wait_for_page_settle(page, max_seconds=15)
            continue
        
        elif state == 'google_redirect':
            state = _wait_for_page_settle(page, max_seconds=30)
            continue
        
        else:  # 'other'
            print(f"[{label}] Not on Flow — navigating...", flush=True)
            state = _navigate_to_flow(page)
            continue
    
    # Final check
    check_and_dismiss_popup(page)
    try:
        context = page.context
        storage_file = os.path.join(BASE_DIR, ".submit_storage_state.json")
        context.storage_state(path=storage_file)
    except Exception:
        pass
    return True


def safe_goto_flow(page, label="", timeout=60000):
    """Navigate to Flow if not already there. Handles redirects gracefully.
    Returns True if on Flow, False if login needed.
    """
    # Skip if already on Flow
    try:
        url = page.url.lower()
        if is_on_flow_not_login(url):
            return True
    except Exception:
        pass
    
    # Navigate
    try:
        page.goto(FLOW_HOME_URL, timeout=timeout, wait_until="commit")
    except Exception:
        pass
    
    # Wait for settle
    for _ in range(15):
        time.sleep(1)
        try:
            url = page.url.lower()
            if is_on_flow_not_login(url):
                return True
            if is_google_login(url) and "setsid" not in url:
                return False
        except Exception:
            continue
    return True


def spa_navigate_to_flow_home(page, label=""):
    """Navigate to Flow homepage WITHOUT full page reload.
    
    Uses SPA (client-side) navigation to preserve the reCAPTCHA Enterprise session.
    Full page.goto() kills the reCAPTCHA session, forcing re-evaluation which often
    results in low scores (597-char tokens) → 403 PERMISSION_DENIED.
    
    Approach (in priority order):
    1. Click a link/logo that navigates to Flow home within the SPA
    2. Use Next.js router.push() for client-side navigation
    3. Use history.pushState + dispatchEvent for SPA navigation
    4. FALLBACK: page.goto (full reload) — only if all else fails
    """
    current_url = page.url
    prefix = f"[{label}]" if label else "[SPA-NAV]"
    
    # Already on homepage?
    if is_flow_home(current_url):
        print(f"{prefix} Already on Flow homepage", flush=True)
        return True
    
    print(f"{prefix} SPA-navigating to Flow home (preserving reCAPTCHA session)...", flush=True)
    
    # === Approach 1: Browser back button (zero CDP footprint) ===
    try:
        for _ in range(5):
            page.go_back()
            time.sleep(1)
            if is_flow_home(page.url):
                print(f"{prefix} ✓ SPA navigation via go_back()", flush=True)
                return True
    except Exception as e:
        print(f"{prefix} go_back approach failed: {e}", flush=True)
    
    # === Approach 2: Click the Flow logo/title link (human click, no evaluate) ===
    try:
        logo_selectors = [
            "a[href*='/tools/flow']:not([href*='/project/'])",  # Matches any locale
            "a[href='/fx/tools/flow']",                          # No locale (exact)
            "header a[href*='flow']",
            "nav a[href*='flow']",
            ".logo a",
            "a:has-text('Flow')",
        ]
        for selector in logo_selectors:
            try:
                link = page.locator(selector).first
                if link.count() > 0 and link.is_visible(timeout=2000):
                    human_click_element(page, selector, f"{prefix} Flow home link")
                    time.sleep(2)
                    if "/project/" not in page.url:
                        print(f"{prefix} ✓ SPA navigation via link click", flush=True)
                        return True
            except Exception:
                continue
    except Exception as e:
        print(f"{prefix} Link click approach failed: {e}", flush=True)
    
    # === Approach 3: Next.js router push (single evaluate — only as fallback) ===
    try:
        result = page.evaluate(r"""() => {
            if (window.next && window.next.router) {
                window.next.router.push('/fx/tools/flow');
                return 'next_router';
            }
            if (window.history && window.history.pushState) {
                window.history.pushState({}, '', '/fx/tools/flow');
                window.dispatchEvent(new PopStateEvent('popstate', {state: {}}));
                return 'history_push';
            }
            return null;
        }""")
        if result:
            time.sleep(2)
            if "/project/" not in page.url:
                print(f"{prefix} ✓ SPA navigation via {result}", flush=True)
                return True
    except Exception as e:
        print(f"{prefix} Router push approach failed: {e}", flush=True)
    
    # === FALLBACK: Full page.goto (kills reCAPTCHA but at least works) ===
    print(f"{prefix} ⚠ All SPA approaches failed, falling back to page.goto (will reset reCAPTCHA)", flush=True)
    try:
        page.goto(FLOW_HOME_URL)
        page.wait_for_load_state("domcontentloaded", timeout=30000)
    except Exception as e:
        print(f"{prefix} ⚠ Even page.goto failed: {e}", flush=True)
    
    # After a full reload, add extra interaction time for reCAPTCHA
    print(f"{prefix} Adding extended interaction time for reCAPTCHA recovery...", flush=True)
    human_delay(3, 5)
    human_mouse_move(page)
    human_delay(2, 3)
    scroll_randomly(page)
    human_delay(2, 3)
    human_mouse_move(page)
    human_delay(2, 3)
    scroll_randomly(page)
    human_delay(3, 5)
    human_mouse_move(page)
    human_delay(2, 3)
    
    return True


# ============================================================
# CONFIGURATION
# ============================================================

# FFMPEG Configuration (for continue mode frame extraction)
# Set this to your ffmpeg.exe path on Windows
from pathlib import Path
FFMPEG_BIN = os.environ.get("FFMPEG_BIN") or r"C:\ffmpeg\ffmpeg-2025-07-23-git-829680f96a-essentials_build\bin\ffmpeg.exe"
if os.path.exists(FFMPEG_BIN):
    os.environ["FFMPEG_BIN"] = FFMPEG_BIN
    os.environ["ImageIO_FFMPEG_EXE"] = FFMPEG_BIN
    os.environ["PATH"] = str(Path(FFMPEG_BIN).parent) + os.pathsep + os.environ.get("PATH", "")
    print(f"[Config] FFMPEG configured: {FFMPEG_BIN}", flush=True)
else:
    print(f"[Config] FFMPEG not found at {FFMPEG_BIN}, will try system PATH", flush=True)

WEB_APP_URL = os.environ.get("WEB_APP_URL", "https://veo-web-app-v3.onrender.com")

# Worker mode: "admin" uses shared LOCAL_WORKER_API_KEY, "user" uses personal token
WORKER_MODE = os.environ.get("WORKER_MODE", "admin")
if WORKER_MODE == "user":
    API_KEY = os.environ.get("USER_WORKER_TOKEN", "")
    API_PATH_PREFIX = "/api/user-worker"
    if not API_KEY:
        print("ERROR: USER_WORKER_TOKEN is required in user mode!")
        print("   Get your token from the web app: Settings -> My Worker")
        import sys
        sys.exit(1)
    # Auto-configure defaults for user mode (can be overridden by .env)
    os.environ.setdefault("PROXY_TYPE", "none")
    os.environ.setdefault("BROWSER_MODE", "stealth")
    multi = os.environ.get("MULTI_ACCOUNT", "false")
    print(f"USER MODE: Processing only your jobs (multi-account: {multi})")
    print(f"   Token: {API_KEY[:8]}...{API_KEY[-4:]}")
else:
    API_KEY = os.environ.get("LOCAL_WORKER_API_KEY", "local-worker-secret-key-12345")
    API_PATH_PREFIX = "/api/local-worker"

# Worker identification (for multi-worker setups)
# Each worker instance should have a unique ID to prevent job conflicts
import socket
DEFAULT_WORKER_ID = f"worker-{socket.gethostname()}-{os.getpid()}"
WORKER_ID = os.environ.get("WORKER_ID", DEFAULT_WORKER_ID)

# Base directory for the worker
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Single account config (legacy)
SESSION_FOLDER = os.environ.get("SESSION_FOLDER", "./flow_session_chrome")
DOWNLOAD_SESSION_FOLDER = os.environ.get("DOWNLOAD_SESSION_FOLDER", "./flow_session_chrome_download")
CACHE_FILE = "./worker_cache.json"


def suppress_chrome_signin_dialog(user_data_dir):
    """Write Chrome preferences to suppress the 'Sign in to Chrome?' dialog
    and force English UI language. Must be called BEFORE launching the browser.
    """
    import json as _json
    
    # ---- 1. Default/Preferences (per-profile settings) ----
    prefs_dir = os.path.join(user_data_dir, "Default")
    os.makedirs(prefs_dir, exist_ok=True)
    prefs_file = os.path.join(prefs_dir, "Preferences")
    
    prefs = {}
    if os.path.exists(prefs_file):
        try:
            with open(prefs_file, 'r', encoding='utf-8') as f:
                prefs = _json.load(f)
        except Exception:
            prefs = {}
    
    # Suppress sign-in prompts
    if "signin" not in prefs:
        prefs["signin"] = {}
    prefs["signin"]["allowed"] = False
    prefs["signin"]["allowed_on_next_startup"] = False
    
    if "browser" not in prefs:
        prefs["browser"] = {}
    prefs["browser"]["signin_intercept_enabled"] = False
    
    if "profile" not in prefs:
        prefs["profile"] = {}
    prefs["profile"]["default_content_setting_values"] = prefs.get("profile", {}).get("default_content_setting_values", {})
    
    # Force English language in profile
    if "intl" not in prefs:
        prefs["intl"] = {}
    prefs["intl"]["accept_languages"] = "es-419,es,en,vi"
    prefs["intl"]["selected_languages"] = "es-419,es,en,vi"
    
    # Disable translate popups
    if "translate" not in prefs:
        prefs["translate"] = {}
    prefs["translate"]["enabled"] = False
    
    try:
        with open(prefs_file, 'w', encoding='utf-8') as f:
            _json.dump(prefs, f)
    except Exception as e:
        print(f"[Config] Warning: Could not write Chrome prefs: {e}", flush=True)
    
    # ---- 2. Local State (browser-wide settings — controls Chrome UI language) ----
    local_state_file = os.path.join(user_data_dir, "Local State")
    local_state = {}
    if os.path.exists(local_state_file):
        try:
            with open(local_state_file, 'r', encoding='utf-8') as f:
                local_state = _json.load(f)
        except Exception:
            local_state = {}
    
    # This is the key setting that controls Chrome's UI language
    if "intl" not in local_state:
        local_state["intl"] = {}
    local_state["intl"]["app_locale"] = "en-US"
    local_state["intl"]["accept_languages"] = "es-419,es,en,vi"
    
    # Also set application_locale which some Chrome versions use
    if "browser" not in local_state:
        local_state["browser"] = {}
    local_state["browser"]["application_locale"] = "en-US"
    
    try:
        with open(local_state_file, 'w', encoding='utf-8') as f:
            _json.dump(local_state, f)
    except Exception as e:
        print(f"[Config] Warning: Could not write Local State: {e}", flush=True)
    
    # ---- 3. Also set Preferences in any other profile dirs (Profile 1, etc.) ----
    for entry in os.listdir(user_data_dir):
        profile_dir = os.path.join(user_data_dir, entry)
        if os.path.isdir(profile_dir) and entry.startswith("Profile"):
            pf = os.path.join(profile_dir, "Preferences")
            pp = {}
            if os.path.exists(pf):
                try:
                    with open(pf, 'r', encoding='utf-8') as f:
                        pp = _json.load(f)
                except Exception:
                    pp = {}
            if "intl" not in pp:
                pp["intl"] = {}
            pp["intl"]["accept_languages"] = "es-419,es,en,vi"
            pp["intl"]["selected_languages"] = "es-419,es,en,vi"
            if "translate" not in pp:
                pp["translate"] = {}
            pp["translate"]["enabled"] = False
            try:
                with open(pf, 'w', encoding='utf-8') as f:
                    _json.dump(pp, f)
            except Exception:
                pass

# Multi-account configuration
# Each account has: name, session_folder, download_session_folder, proxy (optional)
# Proxy format: "http://user:pass@host:port" or "http://host:port"

# ============================================================
# PROXY CONFIGURATION
# ============================================================
# Set PROXY_TYPE to choose which proxies to use:
#   "residential" = Bright Data residential IPs (recommended, looks like real users)
#   "datacenter"  = Static datacenter IPs (cheaper, but Google may flag)
#   "none"        = No proxies (NOT recommended - accounts will share your IP)
# Default changed to "none" — datacenter IPs have poor reputation with reCAPTCHA Enterprise
# and were causing 403s after a few clips. test_human_like.py works because it uses no proxy.
# Set PROXY_TYPE=residential or PROXY_TYPE=datacenter via env var to re-enable.
PROXY_TYPE = os.environ.get("PROXY_TYPE", "none")

RESIDENTIAL_PROXIES = {
    1: "http://brd-customer-hl_e36317ba-zone-residential_proxy1:k4m80km88ols@brd.superproxy.io:33335",
    2: "http://brd-customer-hl_e36317ba-zone-residential_proxy2:17bp9gayqt8a@brd.superproxy.io:33335",
    3: "http://brd-customer-hl_e36317ba-zone-residential_proxy3:b2wcez0e6jzw@brd.superproxy.io:33335",
    4: "http://brd-customer-hl_e36317ba-zone-residential_proxy4:ttylrdu9a74c@brd.superproxy.io:33335",
}

DATACENTER_PROXIES = {
    1: "http://tvinat01:R8KVZpfh@194.50.189.21:29842",    # Rome
    2: "http://tvinat01:R8KVZpfh@52.128.4.126:29842",     # San Jose
    3: "http://tvinat01:R8KVZpfh@91.245.189.176:29842",   # Milan
    4: "http://tvinat01:R8KVZpfh@23.227.76.122:29842",    # New York
}

def _get_proxy(account_num):
    """Get proxy URL for an account based on PROXY_TYPE setting."""
    env_override = os.environ.get(f"ACCOUNT{account_num}_PROXY")
    if env_override:
        return env_override
    if PROXY_TYPE == "residential":
        return RESIDENTIAL_PROXIES.get(account_num)
    elif PROXY_TYPE == "datacenter":
        return DATACENTER_PROXIES.get(account_num)
    return None

ACCOUNTS = [
    {
        "name": "Account1",
        "session_folder": os.environ.get("ACCOUNT1_SESSION", "./flow_session_account1"),
        "download_folder": os.environ.get("ACCOUNT1_DOWNLOAD", "./flow_download_account1"),
        "proxy": _get_proxy(1),
        "enabled": os.environ.get("ACCOUNT1_ENABLED", "true").lower() == "true",
    },
    {
        "name": "Account2",
        "session_folder": os.environ.get("ACCOUNT2_SESSION", "./flow_session_account2"),
        "download_folder": os.environ.get("ACCOUNT2_DOWNLOAD", "./flow_download_account2"),
        "proxy": _get_proxy(2),
        "enabled": os.environ.get("ACCOUNT2_ENABLED", "true").lower() == "true",
    },
    {
        "name": "Account3",
        "session_folder": os.environ.get("ACCOUNT3_SESSION", "./flow_session_account3"),
        "download_folder": os.environ.get("ACCOUNT3_DOWNLOAD", "./flow_download_account3"),
        "proxy": _get_proxy(3),
        "enabled": os.environ.get("ACCOUNT3_ENABLED", "false").lower() == "true",
    },
    {
        "name": "Account4",
        "session_folder": os.environ.get("ACCOUNT4_SESSION", "./flow_session_account4"),
        "download_folder": os.environ.get("ACCOUNT4_DOWNLOAD", "./flow_download_account4"),
        "proxy": _get_proxy(4),
        "enabled": os.environ.get("ACCOUNT4_ENABLED", "false").lower() == "true",
    },
]

# Enable multi-account mode via env var (auto-detected if Account2 is enabled)
MULTI_ACCOUNT_MODE = os.environ.get("MULTI_ACCOUNT_MODE", "false").lower() == "true"

# Browser mode: 'stealth' uses real Chrome, 'playwright' uses bundled browsers
BROWSER_MODE = os.environ.get("BROWSER_MODE", "stealth")


def parse_proxy_url(proxy_url):
    """Parse proxy URL into Playwright proxy config dict.
    Handles: http://host:port, http://user:pass@host:port, socks5://user:pass@host:port
    """
    if not proxy_url:
        return None
    from urllib.parse import urlparse
    parsed = urlparse(proxy_url)
    server = f"{parsed.scheme}://{parsed.hostname}"
    if parsed.port:
        server += f":{parsed.port}"
    config = {"server": server}
    if parsed.username:
        config["username"] = parsed.username
    if parsed.password:
        config["password"] = parsed.password
    return config


def create_proxy_auth_extension(proxy_url, ext_dir):
    """Create a tiny Chrome extension that auto-fills proxy auth credentials.
    
    Chrome doesn't auto-authenticate proxy connections even when Playwright
    provides credentials. This extension intercepts 407 challenges and 
    provides username/password automatically - no popup dialog.
    """
    if not proxy_url:
        return None
    from urllib.parse import urlparse
    parsed = urlparse(proxy_url)
    if not parsed.username or not parsed.password:
        return None
    
    os.makedirs(ext_dir, exist_ok=True)
    
    manifest = {
        "version": "1.0.0",
        "manifest_version": 2,
        "name": "Proxy Auto Auth",
        "permissions": ["proxy", "webRequest", "webRequestBlocking", "<all_urls>"],
        "background": {"scripts": ["background.js"]}
    }
    
    background_js = (
        'chrome.webRequest.onAuthRequired.addListener(\n'
        '    function(details) {\n'
        '        return {\n'
        '            authCredentials: {\n'
        '                username: "%s",\n'
        '                password: "%s"\n'
        '            }\n'
        '        };\n'
        '    },\n'
        '    {urls: ["<all_urls>"]},\n'
        '    ["blocking"]\n'
        ');\n'
    ) % (parsed.username, parsed.password)
    
    with open(os.path.join(ext_dir, 'manifest.json'), 'w') as f:
        json.dump(manifest, f, indent=2)
    with open(os.path.join(ext_dir, 'background.js'), 'w') as f:
        f.write(background_js)
    
    return ext_dir


# ============================================================
# ACCOUNT DETECTION AND VALIDATION
# ============================================================

def is_account_ready(account_config):
    """
    Check if an account is ready to use.
    An account is ready if both session folders exist and contain Chrome data.
    """
    session_folder = account_config.get('session_folder', '')
    download_folder = account_config.get('download_folder', '')
    
    # Check if both folders exist
    if not os.path.exists(session_folder):
        return False, f"Session folder missing: {session_folder}"
    
    if not os.path.exists(download_folder):
        return False, f"Download folder missing: {download_folder}"
    
    # Check for Chrome profile data (indicates authenticated session)
    # Chrome creates a 'Default' folder with various data files
    session_default = os.path.join(session_folder, 'Default')
    download_default = os.path.join(download_folder, 'Default')
    
    if not os.path.exists(session_default):
        return False, f"No Chrome profile in session folder"
    
    if not os.path.exists(download_default):
        return False, f"No Chrome profile in download folder"
    
    return True, "Ready"


def detect_ready_accounts():
    """
    Detect which accounts have valid session folders ready.
    Returns list of (account_index, account_config, is_ready, status_message) tuples.
    """
    results = []
    for i, account in enumerate(ACCOUNTS):
        is_ready, message = is_account_ready(account)
        results.append((i + 1, account, is_ready, message))  # 1-indexed for user display
    return results


def get_ready_accounts():
    """Return only the accounts that are ready to use."""
    ready = []
    for i, account in enumerate(ACCOUNTS):
        is_ready, _ = is_account_ready(account)
        if is_ready:
            ready.append((i + 1, account))  # 1-indexed
    return ready


def list_accounts():
    """Print status of all accounts."""
    print("\n" + "=" * 60)
    print("ACCOUNT STATUS")
    print("=" * 60)
    
    results = detect_ready_accounts()
    ready_count = 0
    
    for idx, account, is_ready, message in results:
        status = "✓ READY" if is_ready else "✗ NOT READY"
        if is_ready:
            ready_count += 1
        
        print(f"\n  Account {idx}: {account['name']}")
        print(f"    Status:   {status}")
        print(f"    Session:  {account['session_folder']}")
        print(f"    Download: {account.get('download_folder', 'N/A')}")
        if not is_ready:
            print(f"    Reason:   {message}")
    
    print(f"\n" + "-" * 60)
    print(f"Total: {ready_count}/{len(ACCOUNTS)} accounts ready")
    print("=" * 60)
    
    return ready_count


def parse_account_selection(selection_str):
    """
    Parse account selection string like "1,2,3" or "1-3" or "1,3,4".
    Returns list of account indices (1-indexed).
    """
    indices = set()
    parts = selection_str.split(',')
    
    for part in parts:
        part = part.strip()
        if '-' in part:
            # Range like "1-3"
            try:
                start, end = part.split('-')
                for i in range(int(start), int(end) + 1):
                    indices.add(i)
            except ValueError:
                print(f"Invalid range: {part}")
                return None
        else:
            # Single number
            try:
                indices.add(int(part))
            except ValueError:
                print(f"Invalid account number: {part}")
                return None
    
    return sorted(list(indices))


def select_accounts_by_indices(indices):
    """
    Select accounts by their indices (1-indexed).
    Returns list of account configs with 'enabled' set appropriately.
    """
    selected = []
    for idx in indices:
        if 1 <= idx <= len(ACCOUNTS):
            account = ACCOUNTS[idx - 1].copy()
            account['enabled'] = True
            selected.append(account)
        else:
            print(f"⚠ Account {idx} does not exist (valid: 1-{len(ACCOUNTS)})")
    return selected


def validate_selected_accounts(selected_accounts):
    """
    Validate that selected accounts are ready.
    Returns (valid_accounts, errors).
    """
    valid = []
    errors = []
    
    for account in selected_accounts:
        is_ready, message = is_account_ready(account)
        if is_ready:
            valid.append(account)
        else:
            errors.append(f"{account['name']}: {message}")
    
    return valid, errors

FLOW_HOME_URL = "https://labs.google/fx/tools/flow"


def is_flow_url(url):
    """Check if URL is any Flow page (with or without locale segment).
    
    Matches:
      https://labs.google/fx/tools/flow
      https://labs.google/fx/es-419/tools/flow
      https://labs.google/fx/en/tools/flow/project/abc-123
    """
    url = url.lower()
    return "labs.google/fx" in url and "/tools/flow" in url


def is_flow_home(url):
    """Check if URL is Flow homepage (not a project page)."""
    return is_flow_url(url) and "/project/" not in url.lower()


def is_flow_project(url):
    """Check if URL is a Flow project page."""
    return is_flow_url(url) and "/project/" in url.lower()


def is_google_login(url):
    """Check if URL is a Google login/auth page."""
    return "accounts.google" in url.lower()


def is_on_flow_not_login(url):
    """Check if URL is on Flow (not Google login). Used to detect post-login state."""
    return is_flow_url(url) and not is_google_login(url)


POLL_INTERVAL = 5       # Seconds between status polls (used in download phase)
MAX_POLL_TIME = 120     # Max seconds to poll before giving up
MAX_GENERATION_RETRIES = 2   # Max retries per clip
CLIP_READY_WAIT = 70    # Seconds to wait after submission before clip is ready for download
FAILURE_CHECK_DELAY = 1 # Brief pause before failure check (check itself polls for up to 8s)
GENERATION_WAIT = 90    # Seconds to wait for generation before download

# ============================================================
# CLIP CHAIN ANALYSIS (for parallel distribution)
# ============================================================

def analyze_clip_chains(clips):
    """
    Analyze clips to find independent chains based on frame dependencies.
    
    A clip is "independent" if it has new frames (doesn't reuse previous clip's frames).
    Dependent clips (has_new_frames=False) must stay with their parent.
    
    Args:
        clips: List of clip dicts with start_frame_key and end_frame_key
    
    Returns:
        List of chains, where each chain is a list of clip indices.
        Example: [[0], [1, 2], [3]] means:
          - Chain 0: clip 0 (independent)
          - Chain 1: clips 1 and 2 (2 depends on 1)
          - Chain 2: clip 3 (independent)
    """
    if not clips:
        return []
    
    chains = []
    current_chain = []
    prev_start_key = None
    prev_end_key = None
    
    for i, clip in enumerate(clips):
        start_key = clip.get('start_frame_key')
        end_key = clip.get('end_frame_key')
        
        # Check if this clip has new frames (independent of previous)
        has_new_start = (start_key != prev_start_key) if prev_start_key else True
        has_new_end = (end_key != prev_end_key) if prev_end_key else (end_key is not None)
        has_new_frames = has_new_start or has_new_end
        
        if i == 0:
            # First clip always starts a new chain
            current_chain = [i]
        elif has_new_frames:
            # New frames = independent = start a new chain
            if current_chain:
                chains.append(current_chain)
            current_chain = [i]
        else:
            # Reuses previous frames = dependent = add to current chain
            current_chain.append(i)
        
        # Update previous frame keys
        prev_start_key = start_key
        prev_end_key = end_key
    
    # Don't forget the last chain
    if current_chain:
        chains.append(current_chain)
    
    return chains


def assign_chains_to_accounts(chains, available_accounts):
    """
    Distribute chains across accounts using round-robin.
    
    Args:
        chains: List of chains (each chain is list of clip indices)
        available_accounts: List of account names
    
    Returns:
        Dict of account_name -> list of clip indices assigned to that account
    """
    if not chains or not available_accounts:
        return {}
    
    assignments = {acc: [] for acc in available_accounts}
    
    for i, chain in enumerate(chains):
        account = available_accounts[i % len(available_accounts)]
        assignments[account].extend(chain)
    
    return assignments


def get_idle_account(account_status, exclude=None):
    """
    Find an idle account for failover.
    
    Args:
        account_status: Dict of account_name -> status ('idle', 'busy', 'offline')
        exclude: Account name to exclude (the one that failed)
    
    Returns:
        Account name if found, None otherwise
    """
    for account, status in account_status.items():
        if account != exclude and status == 'idle':
            return account
    return None


# ============================================================
# ACCOUNT HEALTH TRACKER & FAILOVER ROUTER
# ============================================================

class AccountHealthTracker:
    """
    Tracks health/failure state of all accounts.
    
    Monitors:
    - Recent failure count per account (rolling window)
    - Time since last failure (cooldown)
    - Whether an account is currently busy
    - Whether an account is "hot" (recently failed reCAPTCHA, needs cooldown)
    
    Thread-safe — used by multiple AccountWorkers and the main dispatcher.
    """
    
    # After this many consecutive failures, mark account as hot
    HOT_THRESHOLD = 2
    # Cooldown period (seconds) after an account is marked hot
    COOLDOWN_SECONDS = 300  # 5 minutes
    
    def __init__(self):
        self._lock = threading.Lock()
        # account_name -> { 
        #   'consecutive_failures': int,
        #   'last_failure_time': datetime or None,
        #   'total_failures': int,
        #   'total_successes': int,
        #   'is_busy': bool,
        #   'current_job_id': str or None,
        # }
        self._accounts = {}
    
    def register_account(self, account_name):
        """Register an account for tracking"""
        with self._lock:
            if account_name not in self._accounts:
                self._accounts[account_name] = {
                    'consecutive_failures': 0,
                    'last_failure_time': None,
                    'total_failures': 0,
                    'total_successes': 0,
                    'is_busy': False,
                    'current_job_id': None,
                }
    
    def record_failure(self, account_name, job_id=None):
        """Record a clip submission failure for an account"""
        with self._lock:
            if account_name not in self._accounts:
                self.register_account(account_name)
            acc = self._accounts[account_name]
            acc['consecutive_failures'] += 1
            acc['last_failure_time'] = datetime.now()
            acc['total_failures'] += 1
            
            is_hot = acc['consecutive_failures'] >= self.HOT_THRESHOLD
            if is_hot:
                print(f"[HealthTracker] 🔥 {account_name} marked HOT ({acc['consecutive_failures']} consecutive failures, cooldown {self.COOLDOWN_SECONDS}s)", flush=True)
            else:
                print(f"[HealthTracker] ⚠️ {account_name} failure #{acc['consecutive_failures']} (threshold: {self.HOT_THRESHOLD})", flush=True)
    
    def record_success(self, account_name):
        """Record a successful clip submission — resets consecutive failure count"""
        with self._lock:
            if account_name not in self._accounts:
                self.register_account(account_name)
            acc = self._accounts[account_name]
            if acc['consecutive_failures'] > 0:
                print(f"[HealthTracker] ✓ {account_name} recovered (was at {acc['consecutive_failures']} consecutive failures)", flush=True)
            acc['consecutive_failures'] = 0
            acc['total_successes'] += 1
    
    def set_busy(self, account_name, job_id=None):
        """Mark an account as busy processing a job"""
        with self._lock:
            if account_name not in self._accounts:
                self.register_account(account_name)
            self._accounts[account_name]['is_busy'] = True
            self._accounts[account_name]['current_job_id'] = job_id
    
    def set_idle(self, account_name):
        """Mark an account as idle (finished processing)"""
        with self._lock:
            if account_name not in self._accounts:
                self.register_account(account_name)
            self._accounts[account_name]['is_busy'] = False
            self._accounts[account_name]['current_job_id'] = None
    
    def is_hot(self, account_name):
        """Check if an account is 'hot' (recently failed, needs cooldown)"""
        with self._lock:
            acc = self._accounts.get(account_name)
            if not acc:
                return False
            consec = acc['consecutive_failures']
            if consec < self.HOT_THRESHOLD:
                return False
            # Check if cooldown has elapsed
            if acc['last_failure_time']:
                elapsed = (datetime.now() - acc['last_failure_time']).total_seconds()
                if elapsed >= self.COOLDOWN_SECONDS:
                    # Cooldown complete — reset
                    acc['consecutive_failures'] = 0
                    print(f"[HealthTracker] ✓ {account_name} cooldown complete, resetting", flush=True)
                    return False
                print(f"[HealthTracker] {account_name} IS hot: consec={consec}, elapsed={elapsed:.0f}s/{self.COOLDOWN_SECONDS}s", flush=True)
            return True
    
    def is_busy(self, account_name):
        """Check if an account is busy"""
        with self._lock:
            acc = self._accounts.get(account_name)
            return acc['is_busy'] if acc else False
    
    def get_best_account(self, exclude=None, exclude_list=None):
        """
        Find the best account for failover.
        
        Priority:
        1. Idle + not hot (no recent failures)
        2. Idle + hot but cooldown elapsed
        3. None (all busy or hot)
        
        Among candidates, prefer the one with longest time since last failure.
        
        Args:
            exclude: Single account name to exclude
            exclude_list: List of account names to exclude
        
        Returns:
            account_name or None
        """
        excludes = set()
        if exclude:
            excludes.add(exclude)
        if exclude_list:
            excludes.update(exclude_list)
        
        with self._lock:
            candidates = []
            for name, acc in self._accounts.items():
                if name in excludes:
                    continue
                if acc['is_busy']:
                    continue
                
                # Check hot status (with cooldown check)
                is_hot = False
                if acc['consecutive_failures'] >= self.HOT_THRESHOLD:
                    if acc['last_failure_time']:
                        elapsed = (datetime.now() - acc['last_failure_time']).total_seconds()
                        if elapsed < self.COOLDOWN_SECONDS:
                            is_hot = True
                        else:
                            # Cooldown done
                            acc['consecutive_failures'] = 0
                
                if is_hot:
                    continue
                
                # Score: time since last failure (longer = better), or infinity if never failed
                if acc['last_failure_time']:
                    time_since_fail = (datetime.now() - acc['last_failure_time']).total_seconds()
                else:
                    time_since_fail = float('inf')
                
                candidates.append((name, time_since_fail))
            
            if not candidates:
                return None
            
            # Sort by time since last failure (longest first)
            candidates.sort(key=lambda x: x[1], reverse=True)
            return candidates[0][0]
    
    def get_status_summary(self):
        """Get a human-readable summary of all account health"""
        with self._lock:
            lines = []
            for name, acc in self._accounts.items():
                status = "BUSY" if acc['is_busy'] else "idle"
                hot = " 🔥HOT" if acc['consecutive_failures'] >= self.HOT_THRESHOLD else ""
                fails = acc['consecutive_failures']
                total = f"{acc['total_successes']}✓/{acc['total_failures']}✗"
                cooldown = ""
                if acc['last_failure_time'] and acc['consecutive_failures'] >= self.HOT_THRESHOLD:
                    elapsed = (datetime.now() - acc['last_failure_time']).total_seconds()
                    remaining = max(0, self.COOLDOWN_SECONDS - elapsed)
                    cooldown = f" (cooldown: {int(remaining)}s)"
                lines.append(f"  {name}: {status}{hot} streak:{fails} total:{total}{cooldown}")
            return "\n".join(lines) if lines else "  (no accounts registered)"


# Global instance — shared across all threads
account_health = AccountHealthTracker()


class FailoverRouter:
    """
    Centralized failover routing that works with any number of accounts.
    
    Instead of hardcoded pairwise failover (Account1 → Account2), this provides:
    1. Same-account retry in new project (cheap, fast, handles project-level flags)
    2. Cross-account failover to healthiest idle account
    3. Standby account activation as last resort
    
    Thread-safe — called from multiple AccountWorker threads.
    """
    
    MAX_SAME_ACCOUNT_RETRIES = 2  # Try new project on same account up to N times
    
    def __init__(self, account_job_queues, account_download_queues, swap_request_queue=None):
        """
        Args:
            account_job_queues: Dict of account_name -> job Queue
            account_download_queues: Dict of account_name -> download Queue  
            swap_request_queue: Queue for standby manager (None if no standbys)
        """
        self._lock = threading.Lock()
        self.account_job_queues = account_job_queues
        self.account_download_queues = account_download_queues
        self.swap_request_queue = swap_request_queue
        # Track retry attempts: (job_id, account_name) -> retry_count
        self._retry_counts = {}
    
    def get_retry_count(self, job_id, account_name):
        """Get how many same-account retries have been done for this job on this account"""
        with self._lock:
            return self._retry_counts.get((job_id, account_name), 0)
    
    def increment_retry(self, job_id, account_name):
        """Increment same-account retry count"""
        with self._lock:
            key = (job_id, account_name)
            self._retry_counts[key] = self._retry_counts.get(key, 0) + 1
            return self._retry_counts[key]
    
    def should_retry_same_account(self, job_id, account_name):
        """
        Check if we should retry on the same account (new project) before failing over.
        
        Returns True if retry count < MAX_SAME_ACCOUNT_RETRIES and the account isn't hot.
        """
        count = self.get_retry_count(job_id, account_name)
        is_hot_val = account_health.is_hot(account_name)
        print(f"[FailoverRouter] should_retry_same_account({account_name}): count={count}/{self.MAX_SAME_ACCOUNT_RETRIES}, is_hot={is_hot_val}", flush=True)
        if count >= self.MAX_SAME_ACCOUNT_RETRIES:
            return False
        if is_hot_val:
            return False
        return True
    
    def route_failover(self, failed_account, failover_data, download_queue=None,
                       download_queued=False, job_id=None):
        """
        Route a failover to the best available account.
        
        Strategy:
        1. Find healthiest idle active account (not the failed one, not hot)
        2. If no active account available, try standby manager
        3. If no standby either, mark clips as permanently failed
        
        Args:
            failed_account: Name of the account that failed
            failover_data: Dict with job info, remaining clips, etc.
            download_queue: The failed account's download queue (for cleanup)
            download_queued: Whether download was already started
            job_id: Job ID for logging
            
        Returns:
            'routed' if successfully handed off
            'no_target' if no account available (clips marked failed)
        """
        job_id_short = (job_id or failover_data.get('job_id', 'unknown'))[:8]
        remaining_clips = failover_data.get('remaining_clips', [])
        
        # Record the failure
        account_health.record_failure(failed_account, job_id)
        
        # Step 1: Try to find a healthy idle account
        target = account_health.get_best_account(exclude=failed_account)
        
        if target and target in self.account_job_queues:
            print(f"\n{'='*50}", flush=True)
            print(f"🔄 FAILOVER: {failed_account} → {target}", flush=True)
            print(f"   Job: {job_id_short}...", flush=True)
            print(f"   Remaining clips: {len(remaining_clips)}", flush=True)
            print(f"   Reason: routing to healthiest idle account", flush=True)
            print(f"{'='*50}\n", flush=True)
            
            # Route to the target account's job queue
            self.account_job_queues[target].put(failover_data)
            print(f"[{failed_account}] ✓ Handed off to {target}", flush=True)
            return 'routed'
        
        # Step 1b: All active accounts are busy — queue for ANY non-hot account
        # (they'll pick it up when they finish their current job)
        with self._lock:
            for name, q in self.account_job_queues.items():
                if name == failed_account:
                    continue
                if not account_health.is_hot(name):
                    print(f"\n{'='*50}", flush=True)
                    print(f"🔄 FAILOVER QUEUED: {failed_account} → {name} (busy, will process when idle)", flush=True)
                    print(f"   Job: {job_id_short}...", flush=True)
                    print(f"   Remaining clips: {len(remaining_clips)}", flush=True)
                    print(f"{'='*50}\n", flush=True)
                    
                    q.put(failover_data)
                    print(f"[{failed_account}] ✓ Queued for {name} (currently busy)", flush=True)
                    return 'routed'
            
            # Even the failed account itself can take it (if not hot) — it's about to become idle
            if not account_health.is_hot(failed_account) and failed_account in self.account_job_queues:
                print(f"\n{'='*50}", flush=True)
                print(f"🔄 SELF-RETRY QUEUED: {failed_account} will retry when current job finishes", flush=True)
                print(f"   Job: {job_id_short}...", flush=True)
                print(f"   Remaining clips: {len(remaining_clips)}", flush=True)
                print(f"{'='*50}\n", flush=True)
                
                self.account_job_queues[failed_account].put(failover_data)
                print(f"[{failed_account}] ✓ Queued for self-retry", flush=True)
                return 'routed'
        
        # Step 2: Try standby manager
        if self.swap_request_queue is not None:
            print(f"\n{'='*50}", flush=True)
            print(f"🔄 FAILOVER TO STANDBY: {failed_account} → standby pool", flush=True)
            print(f"   Job: {job_id_short}...", flush=True)
            print(f"   Remaining clips: {len(remaining_clips)}", flush=True)
            print(f"   Reason: no healthy active accounts available", flush=True)
            print(f"{'='*50}\n", flush=True)
            
            self.swap_request_queue.put({
                'type': 'failover_swap',
                'failed_account': failed_account,
                'failover_data': failover_data,
            })
            print(f"[{failed_account}] ✓ Handed off to STANDBY pool", flush=True)
            return 'routed'
        
        # Step 3: No accounts available at all
        print(f"\n{'='*50}", flush=True)
        print(f"❌ FAILOVER FAILED: No accounts available for {job_id_short}...", flush=True)
        print(f"   {failed_account} failed, no idle active accounts, no standby accounts", flush=True)
        print(f"   Marking {len(remaining_clips)} clips as permanently failed", flush=True)
        print(f"\nAccount health:\n{account_health.get_status_summary()}", flush=True)
        print(f"{'='*50}\n", flush=True)
        
        for clip in remaining_clips:
            clip_id = clip.get('id')
            if clip_id:
                update_clip_status(clip_id, 'failed',
                    error_message="All accounts failed or busy, no failover target available")
        
        return 'no_target'


# Global instance — set up in main_multi_account
failover_router = None

# ============================================================
# CACHE FUNCTIONS
# ============================================================

def load_cache():
    """Load job cache from file"""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                cache = json.load(f)
                print(f"✓ Loaded cache: {len(cache.get('jobs', {}))} jobs")
                return cache
        except Exception as e:
            print(f"⚠ Could not load cache: {e}")
    return {'jobs': {}}


def save_cache(cache):
    """Save job cache to file"""
    try:
        with open(CACHE_FILE, 'w') as f:
            json.dump(cache, f, indent=2, default=str)
    except Exception as e:
        print(f"⚠ Could not save cache: {e}")


def mark_job_started(cache, job_id, project_url, clips):
    """Mark a job as started"""
    cache['jobs'][job_id] = {
        'project_url': project_url,
        'started_at': datetime.now().isoformat(),
        'clips_submitted': [],
        'clips_downloaded': [],
        'status': 'started'
    }
    save_cache(cache)


def mark_clip_submitted(cache, job_id, clip_index):
    """Mark a clip as submitted"""
    if job_id in cache['jobs']:
        if clip_index not in cache['jobs'][job_id]['clips_submitted']:
            cache['jobs'][job_id]['clips_submitted'].append(clip_index)
        save_cache(cache)


def mark_job_submitted(cache, job_id):
    """Mark all clips submitted"""
    if job_id in cache['jobs']:
        cache['jobs'][job_id]['status'] = 'submitted'
        cache['jobs'][job_id]['submitted_at'] = datetime.now().isoformat()
        save_cache(cache)


def mark_clip_downloaded(cache, job_id, clip_index):
    """Mark a clip as downloaded"""
    if job_id in cache['jobs']:
        if clip_index not in cache['jobs'][job_id]['clips_downloaded']:
            cache['jobs'][job_id]['clips_downloaded'].append(clip_index)
        save_cache(cache)


def mark_job_completed(cache, job_id):
    """Mark job as fully completed"""
    if job_id in cache['jobs']:
        cache['jobs'][job_id]['status'] = 'completed'
        cache['jobs'][job_id]['completed_at'] = datetime.now().isoformat()
        save_cache(cache)


def get_cached_job(cache, job_id):
    """Get cached job info"""
    return cache.get('jobs', {}).get(job_id)


def is_job_completed(cache, job_id):
    """Check if job is fully completed"""
    job = cache.get('jobs', {}).get(job_id)
    return job and job.get('status') == 'completed'


# ============================================================
# HUMAN-LIKE BEHAVIOR FUNCTIONS
# ============================================================
# These functions add randomness to avoid bot detection

def human_delay(min_sec=0.5, max_sec=1.5):
    """Random delay to simulate human behavior"""
    delay = random.uniform(min_sec, max_sec)
    time.sleep(delay)
    return delay


def human_mouse_move(page):
    """Random mouse movement with Bezier curves — like a human idly moving the cursor"""
    try:
        viewport = page.viewport_size
        if not viewport:
            return
        
        # 2-3 random movements with curves
        moves = random.randint(2, 3)
        for _ in range(moves):
            target_x = random.randint(80, viewport['width'] - 80)
            target_y = random.randint(80, viewport['height'] - 80)
            human_mouse_move_to(page, target_x, target_y, steps=random.randint(5, 12))
            time.sleep(random.uniform(0.05, 0.2))
    except:
        pass  # Ignore errors - this is just for anti-bot


def scroll_randomly(page):
    """Scroll the page a bit like a human would"""
    try:
        for _ in range(random.randint(1, 2)):
            direction = random.choice(['up', 'down'])
            amount = random.randint(30, 100)
            if direction == 'up':
                page.mouse.wheel(0, -amount)
            else:
                page.mouse.wheel(0, amount)
            time.sleep(random.uniform(0.1, 0.3))
    except:
        pass  # Ignore errors


def human_pre_action(page, action_name=""):
    """Light human-like behavior before an action - matches test_human_like.py simplicity"""
    # Just move mouse a bit and small delay (like test_human_like does before clicks)
    human_mouse_move(page)
    human_delay(0.3, 0.8)


def human_look_around(page):
    """
    Simulate human looking around a page before interacting.
    Includes mouse movement, scrolling, and natural pauses as if reading/scanning.
    """
    try:
        # Look around with mouse (scanning the page)
        human_mouse_move(page)
        human_delay(1.0, 2.5)
        
        # Scroll down a bit (reading content)
        scroll_randomly(page)
        human_delay(0.8, 1.5)
        
        # Maybe move mouse again (like hovering over something interesting)
        if random.random() < 0.6:
            human_mouse_move(page)
            human_delay(0.5, 1.2)
        
        # Scroll back up occasionally
        if random.random() < 0.4:
            scroll_randomly(page)
            human_delay(0.3, 0.8)
    except:
        pass


def human_pre_generate_wait(page, context=""):
    """
    Natural wait after entering prompt, before clicking Generate.
    Simulates a human reviewing their prompt and frames before submitting.
    Replaces rigid time.sleep(10) with natural behavior.
    """
    # Base wait (like re-reading the prompt)
    wait_time = random.uniform(6, 12)
    
    # Break it into segments with mouse movement
    segment1 = wait_time * random.uniform(0.3, 0.5)
    time.sleep(segment1)
    
    # Move mouse like reviewing the frames
    try:
        human_mouse_move(page)
    except:
        pass
    
    segment2 = wait_time * random.uniform(0.2, 0.4)
    time.sleep(segment2)
    
    # Maybe a small scroll (reviewing what's on screen)
    if random.random() < 0.3:
        try:
            scroll_randomly(page)
        except:
            pass
    
    # Final pause before clicking generate
    time.sleep(random.uniform(0.5, 2.0))


class HumanPacer:
    """Controls timing between generations with realistic human patterns."""

    def __init__(self, account_name="", max_clips_per_session=20,
                 min_delay=8, max_delay=23):
        self.account_name = account_name
        self.max_clips_per_session = max_clips_per_session
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.session_start = datetime.now()
        self.clips_this_session = 0
        self._next_break_at = random.randint(3, 5)
        self._breaks_taken = 0

    def wait_between_clips(self, page, clip_number=0, total_clips=1,
                           failure_monitor=None, job_id=None):
        self.clips_this_session += 1
        delayed_failures = []
        delay = self._calculate_delay(clip_number, total_clips)
        is_break = self.clips_this_session >= self._next_break_at
        if is_break:
            break_duration = random.uniform(15, 45)
            delay += break_duration
            self._next_break_at = self.clips_this_session + random.randint(3, 5)
            self._breaks_taken += 1
            print(f"[{self.account_name}] Taking a natural break ({break_duration:.0f}s) "
                  f"after {self.clips_this_session} clips...", flush=True)
        print(f"[{self.account_name}] Waiting {delay:.0f}s before next clip "
              f"(clip {clip_number + 1}/{total_clips})...", flush=True)
        wait_start = time.time()
        activities_done = 0
        while time.time() - wait_start < delay:
            remaining = delay - (time.time() - wait_start)
            if remaining <= 0:
                break
            self._do_random_activity(page, remaining, is_break)
            activities_done += 1
            if failure_monitor:
                try:
                    failures = failure_monitor.do_periodic_check(page, self.account_name)
                    if failures:
                        delayed_failures.extend(failures)
                except Exception:
                    pass
            gap = random.uniform(2, 6)
            if time.time() - wait_start + gap < delay:
                time.sleep(gap)
        actual_wait = time.time() - wait_start
        print(f"[{self.account_name}] Done waiting {actual_wait:.0f}s "
              f"({activities_done} activities)", flush=True)
        return delayed_failures

    def should_take_session_break(self):
        return self.clips_this_session >= self.max_clips_per_session

    def do_session_break(self, page):
        cooldown = random.uniform(300, 600)
        print(f"[{self.account_name}] Session cap reached ({self.clips_this_session} clips). "
              f"Cooling down for {cooldown:.0f}s...", flush=True)
        wait_start = time.time()
        while time.time() - wait_start < cooldown:
            if cooldown - (time.time() - wait_start) <= 30:
                break
            self._hp_idle(page)
            time.sleep(random.uniform(15, 45))
        self.clips_this_session = 0
        self.session_start = datetime.now()
        self._next_break_at = random.randint(3, 5)
        self._breaks_taken = 0

    def _calculate_delay(self, clip_number, total_clips):
        base = random.uniform(self.min_delay, self.max_delay)
        fatigue = 1.0 + (self.clips_this_session * random.uniform(0.02, 0.06))
        fatigue = min(fatigue, 1.8)
        if clip_number == 0:
            base *= random.uniform(0.6, 0.8)
        if clip_number == total_clips - 2:
            base *= random.uniform(0.7, 0.9)
        if random.random() < 0.10:
            base *= random.uniform(1.8, 3.0)
            print(f"[{self.account_name}] Got distracted...", flush=True)
        delay = base * fatigue
        noise = random.gauss(0, delay * 0.1)
        return max(self.min_delay * 0.7, delay + noise)

    def _do_random_activity(self, page, max_duration, is_break=False):
        if max_duration < 3:
            return
        if is_break:
            activities = [(self._hp_idle, 0.40), (self._hp_review, 0.20),
                          (self._hp_scroll, 0.15), (self._hp_wander, 0.15),
                          (self._hp_wait, 0.10)]
        else:
            activities = [(self._hp_wander, 0.30), (self._hp_scroll, 0.25),
                          (self._hp_review, 0.20), (self._hp_idle, 0.15),
                          (self._hp_wait, 0.10)]
        r = random.random()
        cumulative = 0
        chosen = self._hp_wait
        for activity, weight in activities:
            cumulative += weight
            if r <= cumulative:
                chosen = activity
                break
        try:
            chosen(page, max_duration)
        except Exception:
            time.sleep(random.uniform(1, 3))

    def _hp_wander(self, page, max_duration=10):
        duration = min(random.uniform(3, 8), max_duration)
        start = time.time()
        try:
            vp = page.viewport_size or {'width': 1280, 'height': 720}
            x, y = random.randint(200, vp['width'] - 200), random.randint(150, vp['height'] - 150)
            while time.time() - start < duration:
                tx = max(50, min(vp['width'] - 50, x + random.gauss(0, 150)))
                ty = max(50, min(vp['height'] - 50, y + random.gauss(0, 100)))
                steps = random.randint(5, 15)
                for s in range(steps):
                    t = (s + 1) / steps; t = t * t * (3 - 2 * t)
                    page.mouse.move(x + (tx - x) * t, y + (ty - y) * t)
                    time.sleep(random.uniform(0.01, 0.04))
                x, y = tx, ty
                if random.random() < 0.3:
                    time.sleep(random.uniform(0.5, 2.0))
        except Exception:
            pass

    def _hp_scroll(self, page, max_duration=10):
        start = time.time()
        try:
            amt = random.randint(200, 600)
            steps = random.randint(4, 8)
            for _ in range(steps):
                page.mouse.wheel(0, amt / steps); time.sleep(random.uniform(0.05, 0.15))
            time.sleep(random.uniform(1, 3))
            if random.random() < 0.7:
                for _ in range(steps):
                    page.mouse.wheel(0, -amt / steps); time.sleep(random.uniform(0.05, 0.15))
        except Exception:
            pass
        remaining = min(random.uniform(3, 10), max_duration) - (time.time() - start)
        if remaining > 0:
            time.sleep(min(remaining, random.uniform(0.5, 2)))

    def _hp_review(self, page, max_duration=15):
        """Simplified review - just look at the page without interacting with specific elements.
        scroll_into_view_if_needed and bounding_box calls on video elements accumulate
        Playwright interaction traces that reCAPTCHA Enterprise monitors."""
        duration = min(random.uniform(4, 12), max_duration)
        start = time.time()
        try:
            # Just wander the mouse around like looking at things
            vp = page.viewport_size or {'width': 1280, 'height': 720}
            x = random.randint(200, vp['width'] - 200)
            y = random.randint(150, vp['height'] - 150)
            page.mouse.move(x, y)
            time.sleep(random.uniform(2, 5))
        except Exception:
            pass
        remaining = duration - (time.time() - start)
        if remaining > 0:
            time.sleep(min(remaining, random.uniform(0.5, 2)))

    def _hp_idle(self, page, max_duration=20):
        duration = min(random.uniform(8, 20), max_duration)
        start = time.time()
        try:
            while time.time() - start < duration:
                vp = page.viewport_size or {'width': 1280, 'height': 720}
                page.mouse.move(random.randint(300, vp['width'] - 300),
                                random.randint(200, vp['height'] - 200))
                time.sleep(random.uniform(3, 8))
        except Exception:
            time.sleep(random.uniform(2, 5))

    def _hp_wait(self, page=None, max_duration=10):
        time.sleep(min(random.uniform(3, 10), max_duration))


def wait_for_end_frame_button(page, timeout=30):
    """
    Wait for END frame button to be available.
    After uploading START frame, the UI needs time to show the second button.
    
    Args:
        page: Playwright page
        timeout: Max seconds to wait
        
    Returns:
        Locator for the END frame button (the .last one)
    """
    selector = "div.sc-8f31d1ba-1, button.sc-d02e9a37-1"
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            # New UI: look for distinct "Final" / "End" button
            count = page.locator(selector).count()
            if count >= 1:
                print(f"✓ END frame button available ({count} buttons found)")
                return page.locator(selector).last
        except:
            pass
        time.sleep(0.5)
    
    # Timeout - return .last anyway and let it fail with a clear message
    print(f"⚠️ Timeout waiting for END frame button (only {page.locator(selector).count()} button(s) found)")
    return page.locator(selector).last


def ensure_batch_view_mode(page, context=""):
    """
    Ensure Batch view mode is selected in the gear/settings dropdown (top bar).
    
    This is a SEPARATE dropdown from the bottom bar settings (select_frames_to_video_mode).
    The gear button has icon 'settings_2' and opens a popup with View Mode (Grid/Batch),
    grid size, and other display options.
    The Batch tab uses icon 'campaign_all' with class flow_tab_slider_trigger.
    """
    prefix = f"{context} " if context else ""
    
    try:
        # Find the gear settings button (icon: settings_2)
        gear_btn = page.locator("button:has(i:text('settings_2'))").first
        
        if gear_btn.count() == 0 or not gear_btn.is_visible(timeout=3000):
            print(f"{prefix}⚠ Gear settings button (settings_2) not found", flush=True)
            return False
        
        # Open the gear dropdown
        state = gear_btn.get_attribute("data-state")
        if state != "open":
            human_click_locator(page, gear_btn, f"{prefix}Opened gear settings dropdown")
            time.sleep(0.8)
        
        # Find and click the Batch tab (icon: campaign_all)
        batch_tab = page.locator(
            "button.flow_tab_slider_trigger:has(i:text('campaign_all'))"
        ).first
        
        if batch_tab.count() > 0 and batch_tab.is_visible(timeout=3000):
            is_selected = batch_tab.get_attribute("aria-selected")
            if is_selected != "true":
                human_click_locator(page, batch_tab, f"{prefix}Selected Batch view mode")
                time.sleep(0.5)
            else:
                print(f"{prefix}✓ Batch view mode already selected", flush=True)
        else:
            print(f"{prefix}⚠ Batch tab (campaign_all) not found in gear dropdown", flush=True)
        
        # Close the gear dropdown
        try:
            page.keyboard.press("Escape")
            time.sleep(0.3)
        except Exception:
            pass
        # Verify closed
        try:
            state = gear_btn.get_attribute("data-state")
            if state == "open":
                page.mouse.click(100, 100)
                time.sleep(0.3)
        except Exception:
            pass
        
        return True
        
    except Exception as e:
        print(f"{prefix}⚠ Batch view mode configuration failed: {e}", flush=True)
        try:
            page.keyboard.press("Escape")
        except Exception:
            pass
        return False


def select_frames_to_video_mode(page, context="", **kwargs):
    """
    Ensure all project settings are correct: Video + Frames + Portrait + Lower Priority model + Variants count.
    
    The new Google Flow UI (Feb 2025+) has a settings summary button in the bottom bar
    (class sc-46973129-1). Clicking it opens a dropdown menu containing tab rows for:
    - Image/Video mode
    - Ingredients/Frames mode  
    - Landscape/Portrait orientation
    - x1/x2/x3/x4 variants
    - Model selector (Veo 3.1 dropdown)
    
    All tabs use class "flow_tab_slider_trigger" with aria-selected state.
    We need to open this dropdown, verify/set each setting, then close it.
    """
    prefix = f"{context} " if context else ""
    
    try:
        # ---- Step 0: Open the settings dropdown ----
        # The summary button has class sc-46973129-1 and shows current config
        settings_btn = page.locator("button.sc-46973129-1").first
        dropdown_open = False
        
        if settings_btn.count() > 0 and settings_btn.is_visible(timeout=3000):
            # Check if dropdown is already open
            state = settings_btn.get_attribute("data-state")
            if state != "open":
                human_click_element(page, settings_btn, f"{prefix}Settings dropdown")
                time.sleep(0.8)
                print(f"{prefix}✓ Opened settings dropdown", flush=True)
            else:
                print(f"{prefix}✓ Settings dropdown already open", flush=True)
            dropdown_open = True
        else:
            print(f"{prefix}⚠ Settings button (sc-46973129-1) not found — tabs may be visible directly", flush=True)
        
        # ---- Step 1: Select "Video" tab (icon: videocam) ----
        video_tab = page.locator(
            "button.flow_tab_slider_trigger:has(i:text('videocam'))"
        ).first
        if video_tab.count() > 0 and video_tab.is_visible(timeout=3000):
            is_selected = video_tab.get_attribute("aria-selected")
            if is_selected != "true":
                human_click_element(page, video_tab, f"{prefix}Video tab")
                print(f"{prefix}✓ Selected Video tab", flush=True)
                time.sleep(0.5)
            else:
                print(f"{prefix}✓ Video tab already selected", flush=True)
        else:
            print(f"{prefix}⚠ Video tab not found", flush=True)
        
        # ---- Step 2: Select "Frames" tab (icon: crop_free) ----
        frames_tab = page.locator(
            "button.flow_tab_slider_trigger:has(i:text('crop_free'))"
        ).first
        if frames_tab.count() > 0 and frames_tab.is_visible(timeout=3000):
            is_selected = frames_tab.get_attribute("aria-selected")
            if is_selected != "true":
                human_click_element(page, frames_tab, f"{prefix}Frames tab")
                print(f"{prefix}✓ Selected Frames tab", flush=True)
                time.sleep(0.5)
            else:
                print(f"{prefix}✓ Frames tab already selected", flush=True)
        else:
            print(f"{prefix}⚠ Frames tab not found", flush=True)
        
        # ---- Step 3: Select Portrait/Vertical orientation (icon: crop_9_16) ----
        portrait_tab = page.locator(
            "button.flow_tab_slider_trigger:has(i:text('crop_9_16'))"
        ).first
        if portrait_tab.count() > 0 and portrait_tab.is_visible(timeout=2000):
            is_selected = portrait_tab.get_attribute("aria-selected")
            if is_selected != "true":
                human_click_element(page, portrait_tab, f"{prefix}Portrait tab")
                print(f"{prefix}✓ Selected Portrait orientation", flush=True)
                time.sleep(0.5)
            else:
                print(f"{prefix}✓ Portrait orientation already selected", flush=True)
        else:
            print(f"{prefix}⚠ Portrait tab not found", flush=True)
        
        # ---- Step 4: Ensure Lower Priority model ----
        # Model button shows current model with icon (volume_up for audio models, etc.)
        # Button text contains model name like "Veo 3.1 - Fast [Lower Priority]"
        # Class names change frequently — use content-based selectors
        model_btn = page.locator(
            "button:has(span:text('Veo')), "
            "button:has(div:text('Veo')), "
            "button:has(i:text('volume_up')):has(span:text('Veo')), "
            "button.sc-a0dcecfb-3:has(span:text('Veo')), "
            "button.sc-a0dcecfb-1:has(i:text('arrow_drop_down'))"
        ).first
        if model_btn.count() > 0 and model_btn.is_visible(timeout=2000):
            model_text = model_btn.inner_text().lower()
            if "lower priority" not in model_text:
                print(f"{prefix}Model is '{model_btn.inner_text().strip()}' — switching to Lower Priority...", flush=True)
                human_click_locator(page, model_btn, f"{prefix}Model dropdown")
                time.sleep(1)
                
                # Click the Lower Priority option in the model dropdown
                lp_option = page.locator(
                    "[role='menuitem']:has-text('Lower Priority'), "
                    "[role='menuitemradio']:has-text('Lower Priority'), "
                    "div[role='option']:has-text('Lower Priority'), "
                    "label:has-text('Lower Priority'), "
                    "button:has(span:text('Lower Priority'))"
                ).first
                if lp_option.is_visible(timeout=3000):
                    human_click_locator(page, lp_option, f"{prefix}Selected Lower Priority model")
                    time.sleep(0.5)
                else:
                    # Try clicking by text content directly
                    lp_text = page.locator("text=Lower Priority").first
                    if lp_text.is_visible(timeout=2000):
                        human_click_locator(page, lp_text, f"{prefix}Selected Lower Priority model (text)")
                        time.sleep(0.5)
                    else:
                        print(f"{prefix}⚠ Could not find Lower Priority option", flush=True)
                        page.keyboard.press("Escape")
                        time.sleep(0.3)
            else:
                print(f"{prefix}✓ Lower Priority model already selected", flush=True)
        else:
            print(f"{prefix}⚠ Model button not found", flush=True)
        
        # ---- Step 4.5: Set variants count (x1/x2/x3/x4) ----
        # The tab row with x1, x2, x3, x4 controls how many variants Flow generates per clip
        # Each is a flow_tab_slider_trigger button with text content "x1", "x2", etc.
        variants_count = kwargs.get('variants_count', 2)
        target_variant_text = f"x{variants_count}"
        
        variant_tab = page.locator(
            f"button.flow_tab_slider_trigger:text-is('{target_variant_text}')"
        ).first
        if variant_tab.count() > 0 and variant_tab.is_visible(timeout=2000):
            is_selected = variant_tab.get_attribute("aria-selected")
            if is_selected != "true":
                human_click_element(page, variant_tab, f"{prefix}Variants {target_variant_text} tab")
                print(f"{prefix}✓ Selected {target_variant_text} variants", flush=True)
                time.sleep(0.5)
            else:
                print(f"{prefix}✓ {target_variant_text} variants already selected", flush=True)
        else:
            print(f"{prefix}⚠ Variants tab '{target_variant_text}' not found", flush=True)
        
        # ---- Step 5: Close the settings dropdown ----
        if dropdown_open:
            # Click outside or press Escape to close
            try:
                page.keyboard.press("Escape")
                time.sleep(0.3)
            except Exception:
                pass
            # Verify it closed
            try:
                state = settings_btn.get_attribute("data-state")
                if state == "open":
                    # Click outside the dropdown to close it
                    page.mouse.click(100, 100)
                    time.sleep(0.3)
            except Exception:
                pass
        
        print(f"{prefix}✓ All settings verified: Video + Frames + Portrait + Lower Priority", flush=True)
        return True
        
    except Exception as e:
        print(f"{prefix}⚠️ Settings configuration failed: {e}", flush=True)
        # Try to close any open dropdowns
        try:
            page.keyboard.press("Escape")
        except Exception:
            pass
        # Fallback: try old dropdown approach in case UI reverts
        try:
            mode_dropdown = page.locator(
                'button:has(span:text("Image to Video")), '
                'button:has(span:text("Text to Video")), '
                'button:has(span:text("Frames to Video"))'
            ).first
            mode_dropdown.click(timeout=5000)
            time.sleep(1)
            frames_option = page.locator(
                'li:has-text("Frames to Video"), '
                'div[role="option"]:has-text("Frames to Video")'
            ).first
            frames_option.click(timeout=5000)
            print(f"{prefix}✓ Selected Frames to Video (legacy dropdown fallback)", flush=True)
            time.sleep(2)
            return True
        except Exception as e2:
            print(f"{prefix}❌ Settings configuration failed completely: {e2}", flush=True)
            return False


# ============================================================
# API FUNCTIONS
# ============================================================

def api_request(method, endpoint, data=None):
    """Make API request to web app"""
    url = f"{WEB_APP_URL}{API_PATH_PREFIX}{endpoint}"
    headers = {"Authorization": f"Bearer {API_KEY}"}
    
    try:
        if method == "GET":
            response = requests.get(url, headers=headers, timeout=30)
        elif method == "POST":
            response = requests.post(url, headers=headers, json=data, timeout=30)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"[API] Error {response.status_code}: {response.text[:100]}")
            return None
    except Exception as e:
        print(f"[API] Request failed: {e}")
        return None


def get_pending_job(exclude_ids=None):
    """Get next pending job from API and claim it for this worker.
    
    Args:
        exclude_ids: Set of job IDs to exclude (already being processed)
    """
    url = f"/jobs/pending?worker_id={WORKER_ID}"
    if exclude_ids:
        url += f"&exclude={','.join(exclude_ids)}"
    result = api_request("GET", url)
    if result and result.get("job"):
        job = result["job"]
        claimed_by = job.get("claimed_by")
        if claimed_by:
            print(f"[API] Job {job['id'][:8]}... claimed by {claimed_by}")
        return job
    return None


def get_redo_clips():
    """Get clips that need regeneration and claim them for this worker"""
    result = api_request("GET", f"/clips/redo-pending?worker_id={WORKER_ID}")
    if result and result.get("clips"):
        clips = result["clips"]
        for clip in clips:
            claimed_by = clip.get("claimed_by")
            if claimed_by:
                print(f"[API] Clip {clip['id']} (redo) claimed by {claimed_by}")
        return clips
    return []


def update_job_status(job_id, status, error_message=None):
    """Update job status via API"""
    data = {"status": status, "error_message": error_message}
    result = api_request("POST", f"/jobs/{job_id}/status", data)
    if result:
        print(f"[API] Job {job_id[:8]}... status → {status}")
    return result


def update_clip_status(clip_id, status, output_url=None, error_message=None):
    """Update clip status via API"""
    data = {
        "status": status,
        "output_url": output_url,
        "error_message": error_message
    }
    result = api_request("POST", f"/clips/{clip_id}/status", data)
    if result:
        print(f"[API] Clip {clip_id} status → {status}")
    return result


def download_frame(url, local_path, r2_fallback_url=None):
    """Download frame from web app proxy or R2
    
    Args:
        url: Primary URL (web app proxy)
        local_path: Where to save locally
        r2_fallback_url: Optional direct R2 URL to try if proxy fails
    """
    # Check if file already exists locally
    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
        print(f"[Download] {os.path.basename(local_path)} already exists locally")
        return local_path
    
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    headers = {"Authorization": f"Bearer {API_KEY}"}
    
    try:
        response = requests.get(url, headers=headers, timeout=120)
        response.raise_for_status()
        
        with open(local_path, 'wb') as f:
            f.write(response.content)
        
        filename = os.path.basename(local_path)
        print(f"[Download] {filename} ({len(response.content)} bytes)")
        return local_path
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 409:
            # 409 Conflict - frame was already processed by server
            print(f"[Download] 409 Conflict - frame already processed on server")
            
            # Try to get R2 URL from error response
            try:
                error_data = e.response.json() if e.response.content else {}
                r2_url = error_data.get('url') or error_data.get('r2_url') or r2_fallback_url
                
                if r2_url:
                    print(f"[Download] Trying R2 URL...")
                    r2_response = requests.get(r2_url, timeout=120)
                    r2_response.raise_for_status()
                    
                    with open(local_path, 'wb') as f:
                        f.write(r2_response.content)
                    
                    filename = os.path.basename(local_path)
                    print(f"[Download] {filename} ({len(r2_response.content)} bytes) [from R2]")
                    return local_path
            except Exception as inner_e:
                print(f"[Download] R2 fallback failed: {inner_e}")
            
            # If we can't get the file but it exists on server, return None
            # The upload_frame function will need to handle this
            print(f"[Download] ⚠️ Frame exists on server but can't download - will skip upload if possible")
            return None
        else:
            print(f"[Download] Failed: {e}")
            raise
    except Exception as e:
        print(f"[Download] Failed: {e}")
        raise


def upload_video(local_path, job_id, clip_index, attempt=1, variant=1):
    """Upload video via web app proxy with variant support
    
    Naming convention: clip_{clip_index}_{attempt}.{variant}.mp4
    - clip_0_1.1.mp4 = clip 0, attempt 1, variant 1 (main)
    - clip_0_1.2.mp4 = clip 0, attempt 1, variant 2
    - clip_0_2.1.mp4 = clip 0, attempt 2 (redo), variant 1
    """
    url = f"{WEB_APP_URL}{API_PATH_PREFIX}/jobs/{job_id}/upload-video/{clip_index}"
    headers = {"Authorization": f"Bearer {API_KEY}"}
    
    # Filename includes attempt and variant
    filename = f'clip_{clip_index}_{attempt}.{variant}.mp4'
    
    with open(local_path, 'rb') as f:
        files = {'file': (filename, f, 'video/mp4')}
        response = requests.post(url, headers=headers, files=files, timeout=300)
    
    response.raise_for_status()
    result = response.json()
    
    print(f"[Upload] Clip {clip_index} ({attempt}.{variant}) → R2 ({os.path.getsize(local_path)} bytes)")
    return result.get('url')


# ============================================================
# CONTINUE MODE: FRAME EXTRACTION AND ENHANCEMENT
# ============================================================

def extract_frame_from_video(video_path, output_path=None, frame_offset=-8):
    """Extract a frame from video using ffmpeg.
    
    Args:
        video_path: Path to the video file
        output_path: Where to save the frame (optional, auto-generated if not provided)
        frame_offset: Frames from end (negative) or start (positive). Default -8 = 8 frames before end.
    
    Returns:
        Path to extracted frame, or None if extraction failed
    
    Environment variables:
        FFMPEG_BIN or ImageIO_FFMPEG_EXE: Path to ffmpeg executable
        FFPROBE_BIN: Path to ffprobe executable (auto-derived from ffmpeg if not set)
    """
    import subprocess
    from pathlib import Path
    
    if not os.path.exists(video_path):
        print(f"[ExtractFrame] Video not found: {video_path}", flush=True)
        return None
    
    if output_path is None:
        base = os.path.splitext(video_path)[0]
        output_path = f"{base}_lastframe.jpg"
    
    try:
        # Use same ffmpeg/ffprobe config as worker.py
        # Check environment variables for custom paths (important for Windows)
        ffmpeg_exe = os.environ.get("FFMPEG_BIN") or os.environ.get("ImageIO_FFMPEG_EXE") or "ffmpeg"
        ffprobe_exe = os.environ.get("FFPROBE_BIN", "ffprobe")
        
        # If we have a custom ffmpeg path but not ffprobe, derive ffprobe from ffmpeg path
        if ffmpeg_exe not in ("ffmpeg", None) and ffprobe_exe == "ffprobe":
            ffmpeg_path = Path(ffmpeg_exe)
            if ffmpeg_path.exists():
                # ffprobe should be in same directory
                probe_name = "ffprobe.exe" if os.name == 'nt' else "ffprobe"
                derived_probe = ffmpeg_path.parent / probe_name
                if derived_probe.exists():
                    ffprobe_exe = str(derived_probe)
        
        print(f"[ExtractFrame] Using ffprobe: {ffprobe_exe}", flush=True)
        print(f"[ExtractFrame] Using ffmpeg: {ffmpeg_exe}", flush=True)
        
        # Get video info using ffprobe
        ffprobe_cmd = [
            ffprobe_exe, '-v', 'error', '-select_streams', 'v:0',
            '-show_entries', 'stream=duration,r_frame_rate',
            '-of', 'csv=p=0', video_path
        ]
        
        probe_result = subprocess.run(ffprobe_cmd, capture_output=True, text=True, timeout=30)
        if probe_result.returncode != 0:
            print(f"[ExtractFrame] ffprobe failed: {probe_result.stderr}", flush=True)
            return None
        
        # Parse duration and fps
        parts = probe_result.stdout.strip().split(',')
        if len(parts) < 2:
            print(f"[ExtractFrame] Could not parse ffprobe output: {probe_result.stdout}", flush=True)
            return None
        
        fps_str = parts[0]
        duration_str = parts[1] if len(parts) > 1 else "8"
        
        # Calculate fps from fraction (e.g., "30000/1001" or "30/1")
        if '/' in fps_str:
            num, den = fps_str.split('/')
            fps = float(num) / float(den)
        else:
            fps = float(fps_str) if fps_str else 30.0
        
        duration = float(duration_str) if duration_str else 8.0
        
        # Calculate timestamp for frame_offset from end
        frames_from_end = abs(frame_offset)
        seconds_from_end = frames_from_end / fps
        timestamp = max(0, duration - seconds_from_end)
        
        print(f"[ExtractFrame] Extracting frame at {timestamp:.3f}s (fps={fps:.2f}, duration={duration:.2f}s)", flush=True)
        
        # Extract frame using ffmpeg
        extract_cmd = [
            ffmpeg_exe, '-y', '-ss', str(timestamp), '-i', video_path,
            '-frames:v', '1', '-q:v', '2', output_path
        ]
        
        extract_result = subprocess.run(extract_cmd, capture_output=True, text=True, timeout=30)
        
        if extract_result.returncode == 0 and os.path.exists(output_path):
            print(f"[ExtractFrame] ✓ Extracted frame to {os.path.basename(output_path)}", flush=True)
            return output_path
        else:
            print(f"[ExtractFrame] ffmpeg failed: {extract_result.stderr}", flush=True)
            return None
            
    except subprocess.TimeoutExpired:
        print(f"[ExtractFrame] Timeout extracting frame", flush=True)
        return None
    except Exception as e:
        print(f"[ExtractFrame] Error: {e}", flush=True)
        return None


def enhance_frame_via_api(frame_path, original_frame_key=None, job_id=None):
    """Enhance a frame using Nano Banana Pro via the API endpoint.
    
    Args:
        frame_path: Path to the extracted frame
        original_frame_key: R2 key of original scene image for facial consistency
        job_id: Job ID for context
    
    Returns:
        Path to enhanced frame, or original frame_path if enhancement failed/unavailable
    """
    import base64
    
    if not os.path.exists(frame_path):
        print(f"[EnhanceFrame] Frame not found: {frame_path}", flush=True)
        return frame_path
    
    try:
        # Read and encode the frame
        with open(frame_path, 'rb') as f:
            frame_bytes = f.read()
        frame_base64 = base64.b64encode(frame_bytes).decode('utf-8')
        
        # Call the API endpoint
        url = f"{WEB_APP_URL}{API_PATH_PREFIX}/enhance-frame"
        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "frame_base64": frame_base64,
            "original_frame_key": original_frame_key,
            "job_id": job_id or ""
        }
        
        print(f"[EnhanceFrame] Calling Nano Banana Pro API...", flush=True)
        response = requests.post(url, headers=headers, json=payload, timeout=120)
        
        if response.status_code != 200:
            print(f"[EnhanceFrame] API returned {response.status_code}, using original frame", flush=True)
            return frame_path
        
        result = response.json()
        
        if result.get('success') and result.get('enhanced'):
            # Decode and save enhanced frame
            enhanced_base64 = result.get('frame_base64')
            enhanced_bytes = base64.b64decode(enhanced_base64)
            
            enhanced_path = frame_path.replace('.jpg', '_enhanced.jpg').replace('.png', '_enhanced.png')
            if enhanced_path == frame_path:
                enhanced_path = frame_path + '_enhanced.jpg'
            
            with open(enhanced_path, 'wb') as f:
                f.write(enhanced_bytes)
            
            print(f"[EnhanceFrame] ✓ Frame enhanced via Nano Banana Pro", flush=True)
            return enhanced_path
        else:
            if result.get('error'):
                print(f"[EnhanceFrame] API error: {result.get('error')}, using original frame", flush=True)
            else:
                print(f"[EnhanceFrame] Enhancement not available, using original frame", flush=True)
            return frame_path
            
    except requests.exceptions.Timeout:
        print(f"[EnhanceFrame] API timeout, using original frame", flush=True)
        return frame_path
    except Exception as e:
        print(f"[EnhanceFrame] Error calling API: {e}, using original frame", flush=True)
        return frame_path


def analyze_continue_mode_chains(clips):
    """Analyze clips to identify continue-mode chains.
    
    A chain is a sequence of clips in the same scene where all but the first
    have clip_mode='continue'. These clips must be processed sequentially.
    
    Args:
        clips: List of clip dicts with clip_mode and scene_index
    
    Returns:
        List of chains, where each chain is a list of clip indices
        Example: [[0, 1, 2], [3], [4, 5]] means clips 0-2 are a chain, 3 is standalone, 4-5 is a chain
    """
    if not clips:
        return []
    
    chains = []
    current_chain = [0]  # Start with first clip
    
    for i in range(1, len(clips)):
        clip = clips[i]
        prev_clip = clips[i - 1]
        
        clip_mode = clip.get('clip_mode', 'blend')
        scene_index = clip.get('scene_index', 0)
        prev_scene_index = prev_clip.get('scene_index', 0)
        
        # Continue mode in same scene = part of chain
        if clip_mode == 'continue' and scene_index == prev_scene_index:
            current_chain.append(i)
        else:
            # Start new chain
            chains.append(current_chain)
            current_chain = [i]
    
    # Don't forget the last chain
    chains.append(current_chain)
    
    # Log the analysis
    print(f"[ContinueMode] Analyzed {len(clips)} clips into {len(chains)} chain(s):", flush=True)
    for chain_idx, chain in enumerate(chains):
        if len(chain) > 1:
            modes = [clips[i].get('clip_mode', 'blend') for i in chain]
            print(f"  Chain {chain_idx}: clips {chain} (modes: {modes}) - SEQUENTIAL", flush=True)
        else:
            print(f"  Chain {chain_idx}: clip {chain[0]} - PARALLEL OK", flush=True)
    
    return chains


def get_clip_approval_status(clip_id):
    """Poll API for clip approval status.
    
    Args:
        clip_id: Database ID of the clip
    
    Returns:
        Dict with approval_status, selected_variant, output_url, has_video, status
        or None if request failed
    """
    try:
        url = f"{WEB_APP_URL}{API_PATH_PREFIX}/clips/{clip_id}/approval-status"
        headers = {"Authorization": f"Bearer {API_KEY}"}
        
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"[ContinueMode] API returned {response.status_code} for clip {clip_id}", flush=True)
            return None
    except Exception as e:
        print(f"[ContinueMode] Error getting approval status for clip {clip_id}: {e}", flush=True)
        return None


def find_downloaded_video(temp_dir, clip_index, variant=None):
    """Find downloaded video file on disk.
    
    Args:
        temp_dir: Temporary directory where videos are saved
        clip_index: Clip index to look for
        variant: Specific variant to look for (e.g., 1 for 1.1, 2 for 1.2). If None, returns first found.
    
    Returns:
        Path to video file if found, None otherwise
    """
    import glob
    
    if variant:
        # Look for specific variant: clip_0_1.1.mp4
        pattern = os.path.join(temp_dir, f"clip_{clip_index}_*.{variant}.mp4")
    else:
        # Look for any variant of this clip
        pattern = os.path.join(temp_dir, f"clip_{clip_index}_*.mp4")
    
    matches = glob.glob(pattern)
    
    if matches:
        # Return first match (or specific variant if requested)
        return matches[0]
    
    return None


def wait_for_clip_approval(clip_id, clip_index, temp_dir, timeout=600):
    """Wait for a clip to be approved by the user.
    
    Args:
        clip_id: Database ID of the clip
        clip_index: Index of the clip (for logging)
        temp_dir: Directory where videos are downloaded
        timeout: Maximum wait time in seconds (default 10 minutes)
    
    Returns:
        Dict with:
            - success: Whether approval was received
            - video_path: Path to the approved variant's video
            - selected_variant: Which variant was approved
        or None if timeout/failure
    """
    start_time = datetime.now()
    poll_interval = 10  # Check every 10 seconds
    last_status = None
    
    print(f"[ContinueMode] Waiting for clip {clip_index} approval (timeout: {timeout}s)...", flush=True)
    
    while (datetime.now() - start_time).total_seconds() < timeout:
        elapsed = int((datetime.now() - start_time).total_seconds())
        
        # Get approval status from API
        status = get_clip_approval_status(clip_id)
        
        if status:
            approval_status = status.get('approval_status', 'pending_review')
            has_video = status.get('has_video', False)
            selected_variant = status.get('selected_variant', 1)
            clip_status = status.get('status', '')
            
            # Log status changes
            if approval_status != last_status:
                print(f"[ContinueMode] Clip {clip_index}: status={approval_status}, has_video={has_video}, variant={selected_variant}", flush=True)
                last_status = approval_status
            
            # Check if approved
            if approval_status == 'approved':
                print(f"[ContinueMode] ✓ Clip {clip_index} APPROVED! (variant {selected_variant})", flush=True)
                
                # Find the video file for the approved variant
                video_path = find_downloaded_video(temp_dir, clip_index, variant=selected_variant)
                
                if video_path and os.path.exists(video_path):
                    return {
                        'success': True,
                        'video_path': video_path,
                        'selected_variant': selected_variant
                    }
                else:
                    # Try to find any video if specific variant not found
                    video_path = find_downloaded_video(temp_dir, clip_index)
                    if video_path and os.path.exists(video_path):
                        print(f"[ContinueMode] Using first available video (couldn't find variant {selected_variant})", flush=True)
                        return {
                            'success': True,
                            'video_path': video_path,
                            'selected_variant': selected_variant
                        }
                    else:
                        print(f"[ContinueMode] WARNING: Approved but video not found on disk", flush=True)
            
            # Check if rejected or max attempts
            elif approval_status in ('rejected', 'max_attempts'):
                print(f"[ContinueMode] Clip {clip_index} is {approval_status} - will use original frame", flush=True)
                return {
                    'success': False,
                    'video_path': None,
                    'selected_variant': None,
                    'reason': approval_status
                }
        
        # Log progress periodically
        if elapsed % 30 == 0:
            print(f"[ContinueMode] Still waiting for clip {clip_index} approval... ({elapsed}s)", flush=True)
        
        time.sleep(poll_interval)
    
    print(f"[ContinueMode] Timeout waiting for clip {clip_index} approval after {timeout}s", flush=True)
    return {
        'success': False,
        'video_path': None,
        'selected_variant': None,
        'reason': 'timeout'
    }


# ============================================================
# BROWSER HELPERS
# ============================================================

def ensure_videos_tab_selected(page):
    """Ensure the 'Videos' view is selected in the project sidebar.
    
    New UI (Feb 2025+): Left sidebar with icon buttons (dashboard, image, videocam, drive_folder_upload)
    Old UI: Radio buttons for Images/Videos tabs
    """
    try:
        # New UI: sidebar button with videocam icon
        videos_sidebar = page.locator("button:has(i:text('videocam')):not(.flow_tab_slider_trigger), button:has-text('View videos')").first
        if videos_sidebar.count() > 0 and videos_sidebar.is_visible(timeout=3000):
            videos_sidebar.click(timeout=5000)
            human_delay(0.5, 1)
            print("✓ Clicked Videos view (sidebar)")
            return
        
        # Old UI: radio buttons
        images_tab = page.locator("button[role='radio']:has(i:text('image')), button[role='radio']:has-text('Images')").first
        if images_tab.count() > 0 and images_tab.is_visible(timeout=3000):
            images_tab.click(timeout=5000)
            human_delay(0.5, 1)
            print("✓ Clicked Images tab")
        
        videos_tab = page.locator("button[role='radio']:has(i:text('videocam')), button[role='radio']:has-text('Videos')").first
        if videos_tab.count() > 0 and videos_tab.is_visible(timeout=3000):
            videos_tab.click(timeout=5000)
            human_delay(0.5, 1)
            print("✓ Clicked Videos tab")
        else:
            print("  (Videos tab not found, continuing...)")
    except Exception as e:
        print(f"  (Tab selection: {e})")


def check_and_dismiss_popup(page):
    """Dismiss Flow's popups if present (Notice, I agree, Chrome sign-in/sync, splash banner, etc.)"""
    try:
        # ── "Meet the new Flow" splash banner ──
        # New UI shows a large banner with X close button on first visit
        try:
            close_btn = page.locator("button:has-text('close')").first
            # Only dismiss if "Meet the new Flow" or "what's new" text is visible
            splash = page.locator("text=Meet the new Flow, text=what's new")
            if splash.count() > 0 and splash.first.is_visible(timeout=500):
                if close_btn.count() > 0 and close_btn.is_visible():
                    close_btn.click(force=True)
                    print(f"✓ Dismissed 'Meet the new Flow' splash banner", flush=True)
                    time.sleep(1)
                    return True
        except:
            pass
        
        # ── Chrome browser-level dialogs ──
        # These appear as overlays after Google login
        
        # 1. "Sign in to Chrome?" dialog → click "Use Chrome without an account"
        try:
            no_account_btn = page.locator("button:has-text('Use Chrome without an account')")
            if no_account_btn.count() > 0 and no_account_btn.first.is_visible():
                no_account_btn.first.click(force=True)
                print(f"✓ Dismissed Chrome sign-in dialog", flush=True)
                time.sleep(1)
                return True
        except:
            pass
        
        # 2. "Turn on sync?" / "Sync is paused" → click "No thanks" / "Dismiss"
        try:
            for dismiss_text in ["No thanks", "No, thanks", "Dismiss", "Not now", "Skip"]:
                btn = page.locator(f"button:has-text('{dismiss_text}')")
                if btn.count() > 0 and btn.first.is_visible():
                    btn.first.click(force=True)
                    print(f"✓ Dismissed Chrome sync dialog ({dismiss_text})", flush=True)
                    time.sleep(1)
                    return True
        except:
            pass
        
        # 3. "Continue as X" with "Use Chrome without" → click "Use Chrome without"
        #    "Continue as X" alone → click it (user already picked this profile)
        try:
            continue_btn = page.locator("button:has-text('Continue as')")
            no_btn = page.locator("button:has-text('Use Chrome without')")
            if no_btn.count() > 0 and no_btn.first.is_visible():
                no_btn.first.click(force=True)
                print(f"✓ Dismissed Chrome sign-in dialog (no account)", flush=True)
                time.sleep(1)
                return True
            elif continue_btn.count() > 0 and continue_btn.first.is_visible():
                continue_btn.first.click(force=True)
                print(f"✓ Clicked Continue as profile", flush=True)
                time.sleep(1)
                return True
        except:
            pass
        
        # 4. "Customize your Chrome profile" → click "Done" or "Skip"
        try:
            for done_text in ["Done", "Skip customization"]:
                btn = page.locator(f"button:has-text('{done_text}')")
                if btn.count() > 0 and btn.first.is_visible():
                    btn.first.click(force=True)
                    print(f"✓ Dismissed Chrome profile setup ({done_text})", flush=True)
                    time.sleep(1)
                    return True
        except:
            pass
        
        # ── Flow-specific dialogs ──
        try:
            dialog = page.locator("div[role='dialog']")
            if dialog.count() > 0 and dialog.first.is_visible():
                # Check if this is the Notice dialog (has h2 with "Notice" text)
                notice_header = dialog.locator("h2:has-text('Notice')")
                if notice_header.count() > 0:
                    # Find and click the "I agree" button within this dialog
                    agree_btn = dialog.locator("button:has-text('I agree')")
                    if agree_btn.count() > 0 and agree_btn.first.is_visible():
                        agree_btn.first.click(force=True)
                        print(f"✓ Dismissed Notice dialog (I agree)", flush=True)
                        time.sleep(1)
                        return True
        except:
            pass
        
        # Fallback: try various selectors for the I agree button
        selectors = [
            "button.sc-e17e280e-5.kcVnNc",  # Specific class from screenshot
            "button.sc-e17e280e-5",  # Partial class match
            "div[role='dialog'] button:has-text('I agree')",
            "button:has-text('I agree')",
            "text=I agree",
            "button:text('I agree')",
        ]
        
        for selector in selectors:
            try:
                btn = page.locator(selector)
                if btn.count() > 0:
                    try:
                        if btn.first.is_visible():
                            btn.first.click(force=True)
                            print(f"✓ Dismissed popup (selector: {selector})", flush=True)
                            time.sleep(1)
                            return True
                    except:
                        pass
            except:
                continue
                
        # Last resort: look for Notice text and then find agree button nearby
        try:
            notice = page.locator("text=Notice")
            if notice.count() > 0 and notice.first.is_visible():
                agree_btn = page.locator("button:has-text('I agree')")
                if agree_btn.count() > 0:
                    agree_btn.first.click(force=True)
                    print(f"✓ Dismissed Notice popup", flush=True)
                    time.sleep(1)
                    return True
        except:
            pass
            
    except:
        pass
    return False


def wait_and_dismiss_popup(page, timeout=5):
    """Wait for popup to appear and dismiss it"""
    for _ in range(timeout):
        if check_and_dismiss_popup(page):
            return True
        time.sleep(1)
    return False


def ensure_vertical_orientation(page, label=""):
    """Ensure orientation is set to Vertical (portrait) in the new tab UI."""
    prefix = f"[{label}] " if label else ""
    try:
        vert_tab = page.locator(
            "button.flow_tab_slider_trigger:has-text('crop_9_16'), "
            "button.flow_tab_slider_trigger:has-text('Vertical')"
        ).first
        if vert_tab.count() > 0 and vert_tab.is_visible(timeout=2000):
            is_selected = vert_tab.get_attribute("aria-selected")
            if is_selected != "true":
                human_click_element(page, vert_tab, f"{prefix}Vertical tab")
                print(f"{prefix}✓ Selected Vertical orientation", flush=True)
                time.sleep(0.5)
        # Also try localized version (crop_9_16 icon)
        else:
            vert_tab = page.locator("button.flow_tab_slider_trigger:has-text('crop_9_16')").first
            if vert_tab.count() > 0 and vert_tab.is_visible(timeout=1000):
                is_selected = vert_tab.get_attribute("aria-selected")
                if is_selected != "true":
                    vert_tab.click(timeout=2000)
                    print(f"{prefix}✓ Selected Vertical orientation (icon)", flush=True)
    except Exception as e:
        print(f"{prefix}⚠ Orientation check failed: {e}", flush=True)


def ensure_lower_priority_model(page, label=""):
    """Ensure the model is set to 'Lower Priority' (free tier) before generating.
    
    Checks the model button in the bottom bar. If it doesn't say 'Lower Priority',
    opens the settings panel and selects the Lower Priority option.
    """
    prefix = f"[{label}] " if label else ""
    
    try:
        # Find the model button — content-based selectors (class names change frequently)
        model_btn = page.locator(
            "button:has(span:text('Veo')), "
            "button:has(div:text('Veo')), "
            "button:has(i:text('volume_up')):has(span:text('Veo')), "
            "button.sc-a0dcecfb-3:has(span:text('Veo')), "
            "button.sc-a0dcecfb-1:has(i:text('arrow_drop_down'))"
        ).first
        if not model_btn.is_visible(timeout=2000):
            print(f"{prefix}⚠ Model button not found — skipping model check", flush=True)
            return
        
        # Read current model text
        model_text = model_btn.inner_text().lower()
        
        if "lower priority" in model_text:
            return  # Already correct
        
        print(f"{prefix}Model is '{model_btn.inner_text().strip()}' — switching to Lower Priority...", flush=True)
        
        # Click the model button to open settings
        model_btn.click(timeout=3000)
        time.sleep(1)
        
        # Find and click the "Lower Priority" option in the dropdown/list
        lp_option = page.locator(
            "[role='menuitem']:has-text('Lower Priority'), "
            "[role='menuitemradio']:has-text('Lower Priority'), "
            "div[role='option']:has-text('Lower Priority'), "
            "label:has-text('Lower Priority'), "
            "div[role='radio']:has-text('Lower Priority'), "
            "button:has-text('Lower Priority')"
        ).first
        
        if lp_option.is_visible(timeout=3000):
            lp_option.click(timeout=3000)
            print(f"{prefix}✓ Selected Lower Priority model", flush=True)
            time.sleep(1)
        else:
            # Try clicking any element with "Lower Priority" text
            lp_text = page.locator("text=Lower Priority").first
            if lp_text.is_visible(timeout=2000):
                lp_text.click(timeout=3000)
                print(f"{prefix}✓ Selected Lower Priority model (text fallback)", flush=True)
                time.sleep(1)
            else:
                print(f"{prefix}⚠ Could not find Lower Priority option", flush=True)
        
        # Close the settings panel by clicking outside or pressing Escape
        try:
            page.keyboard.press("Escape")
            time.sleep(0.5)
        except Exception:
            pass
            
    except Exception as e:
        print(f"{prefix}⚠ Model check failed: {e}", flush=True)


def click_generate_button(page, context_name="", max_retries=3):
    """
    Click the arrow_forward (Generate) button with retry logic.
    
    MATCHES test_human_like.py: uses dispatch_event('click') on the <i> icon element.
    
    If the button is disabled or click fails, this raises an exception.
    The caller (click_generate_with_crash_handler) handles recovery by
    refreshing the project and re-uploading frames.
    
    Args:
        page: Playwright page
        context_name: For logging (e.g., "Account1" or "Clip 5")
        max_retries: Maximum number of retry attempts
        
    Returns:
        True if click succeeded
        
    Raises:
        Exception if all retries fail
    """
    prefix = f"[{context_name}] " if context_name else ""
    
    for attempt in range(max_retries):
        try:
            # First dismiss any popups that might be blocking
            check_and_dismiss_popup(page)
            time.sleep(0.5)
            
            # Ensure model is set to Lower Priority (free tier)
            if attempt == 0:
                ensure_lower_priority_model(page, context_name)
            
            # Check if button is actually enabled before clicking
            if not is_generate_button_enabled(page):
                print(f"{prefix}⚠️ Generate button is DISABLED (attempt {attempt + 1}/{max_retries})", flush=True)
                if attempt < max_retries - 1:
                    time.sleep(3)
                    continue
                else:
                    raise Exception("Generate button is disabled - frames may not have loaded")
            
            # Pre-generate look-around (matching test_human_like.py [9/10])
            human_mouse_move(page)
            human_delay(1, 2)
            scroll_randomly(page)
            human_delay(0.5, 1)
            
            # Click Generate — use human_click for natural mouse movement + real click events
            # This is THE click that triggers the API call with reCAPTCHA token
            arrow_btn = page.locator("button:has(i:text('arrow_forward')), i:text('arrow_forward')").first
            human_click_element(page, arrow_btn, "", timeout=30000)
            
            if prefix:
                print(f"{prefix}✓ Clicked Generate button", flush=True)
            
            time.sleep(1)
            return True
            
        except Exception as e:
            error_msg = str(e)
            print(f"{prefix}⚠️ Generate button click failed (attempt {attempt + 1}/{max_retries}): {error_msg[:100]}", flush=True)
            
            if attempt < max_retries - 1:
                check_and_dismiss_popup(page)
                time.sleep(2)
    
    # All retries failed
    raise Exception(f"Generate button click failed after {max_retries} attempts")


def click_generate_with_crash_handler(page, account_name, clip_index, clips, clip_submit_times, 
                                       download_queued, download_queue, job_id,
                                       start_frame=None, end_frame=None, prompt=None,
                                       clip_mode=None, start_frame_key=None, end_frame_key=None):
    """
    Wrapper around click_generate_button that handles crashes gracefully.
    
    If the click fails (e.g. Generate button disabled because frames didn't load
    after media gallery popup timeout):
    1. First attempt: refresh the project page, re-upload frames, re-enter prompt, retry Generate
    2. If rebuild also fails: notify download thread, mark unsubmitted clips as failed, raise
    
    Args:
        page, account_name, clip_index, clips, clip_submit_times, download_queued, download_queue, job_id:
            Standard submission context
        start_frame: Local path to start frame image (for rebuild)
        end_frame: Local path to end frame image (for rebuild)
        prompt: Prompt text (for rebuild)
        clip_mode: 'blend' or 'continue' (for rebuild - determines if end frame is needed)
        start_frame_key: R2 key for start frame (for scene transition detection)
        end_frame_key: R2 key for end frame (for scene transition detection)
    """
    MAX_REBUILD_ATTEMPTS = 2
    
    for rebuild_attempt in range(MAX_REBUILD_ATTEMPTS + 1):
        try:
            click_generate_button(page, account_name)
            return True
        except Exception as e:
            error_msg = str(e)
            is_disabled = "disabled" in error_msg.lower()
            
            # If button is disabled and we have frame data to rebuild with, try rebuild
            if is_disabled and rebuild_attempt < MAX_REBUILD_ATTEMPTS and start_frame and prompt:
                print(f"\n[{account_name}] 🔄 Generate button disabled — rebuilding project (attempt {rebuild_attempt + 1}/{MAX_REBUILD_ATTEMPTS})", flush=True)
                
                try:
                    # Refresh the project page
                    page.reload(timeout=30000)
                    page.wait_for_load_state("domcontentloaded", timeout=15000)
                    time.sleep(3)
                    check_and_dismiss_popup(page)
                    
                    # Select Frames to Video mode
                    select_frames_to_video_mode(page, f"[{account_name}-Rebuild]")
                    ensure_batch_view_mode(page, f"[{account_name}-Rebuild]")
                    
                    # Re-upload START frame
                    if start_frame and os.path.exists(start_frame):
                        check_and_dismiss_popup(page)
                        human_click_element(page, "div.sc-8f31d1ba-1, button.sc-d02e9a37-1", "START frame button (rebuild)")
                        print(f"[{account_name}] ✓ Clicked Add START frame button (rebuild)", flush=True)
                        time.sleep(1)  # Wait for gallery to open
                        upload_frame(page, start_frame, "START frame")
                    
                    # Re-upload END frame (unless continue mode with same image)
                    is_scene_transition = (end_frame_key and start_frame_key and end_frame_key != start_frame_key)
                    skip_end = (clip_mode == 'continue' and not is_scene_transition)
                    
                    if end_frame and os.path.exists(end_frame) and not skip_end:
                        check_and_dismiss_popup(page)
                        end_frame_btn = page.locator("div.sc-8f31d1ba-1, button.sc-d02e9a37-1").last
                        human_click_element(page, end_frame_btn, "END frame button (rebuild)")
                        time.sleep(1)  # Wait for gallery to open
                        upload_frame(page, end_frame, "END frame")
                    
                    # Re-enter prompt
                    fill_prompt_textarea(page, prompt)
                    print(f"[{account_name}] ✓ Re-entered prompt (rebuild)", flush=True)
                    human_pre_generate_wait(page)
                    
                    # Loop back to try click_generate_button again
                    print(f"[{account_name}] ✓ Rebuild complete, retrying Generate...", flush=True)
                    time.sleep(1)
                    continue
                    
                except Exception as rebuild_err:
                    print(f"[{account_name}] ❌ Rebuild failed: {rebuild_err}", flush=True)
                    # Fall through to crash handling below
            
            # Either not a disabled-button error, or rebuild attempts exhausted, or no frame data
            print(f"\n{'='*50}", flush=True)
            print(f"[{account_name}] ❌ SUBMISSION CRASHED at clip {clip_index}!", flush=True)
            print(f"[{account_name}] Error: {e}", flush=True)
            print(f"[{account_name}] Submitted clips before crash: {list(clip_submit_times.keys())}", flush=True)
            print(f"{'='*50}\n", flush=True)
            
            # Notify download thread to only expect already-submitted clips
            if download_queued:
                submitted_clip_indices = set(clip_submit_times.keys())
                if submitted_clip_indices:
                    print(f"[{account_name}] Limiting download to {len(submitted_clip_indices)} submitted clips: {sorted(submitted_clip_indices)}", flush=True)
                    download_queue.put({
                        'type': 'limit_clips',
                        'job_id': job_id,
                        'allowed_clips': submitted_clip_indices
                    })
                else:
                    print(f"[{account_name}] No clips were submitted - cancelling download", flush=True)
                    download_queue.put({
                        'type': 'cancel',
                        'job_id': job_id
                    })
                
                download_queue.put({
                    'type': 'shutdown_after_complete',
                    'job_id': job_id
                })
            
            # Mark unsubmitted clips as failed
            for clip in clips:
                clip_idx = clip['clip_index']
                if clip_idx not in clip_submit_times:
                    update_clip_status(clip['id'], 'failed', error_message=f"Submission crashed: {str(e)[:100]}")
            
            raise



def is_generate_button_enabled(page):
    """
    Check if the Generate button (arrow_forward) is enabled/clickable.
    Uses pure Playwright locators — no page.evaluate() to avoid reCAPTCHA CDP detection.
    """
    try:
        # Check if the arrow_forward icon exists and is visible
        arrow_btn = page.locator("i:text('arrow_forward')").first
        if arrow_btn.count() == 0:
            return False
        
        if not arrow_btn.is_visible():
            return False
        
        # Check parent button for disabled state using Playwright locators
        # Walk up from the icon to find the button ancestor
        btn = page.locator("button:has(i:text('arrow_forward'))").first
        if btn.count() == 0:
            return False
        
        # Check disabled attribute
        if btn.is_disabled():
            return False
        
        # Check aria-disabled
        aria_disabled = btn.get_attribute("aria-disabled")
        if aria_disabled == "true":
            return False
        
        # Check if button has disabled-looking classes
        btn_class = btn.get_attribute("class") or ""
        if "disabled" in btn_class.lower() or "Mui-disabled" in btn_class:
            return False
        
        return True
    except Exception as e:
        print(f"[GenerateCheck] Error checking button state: {e}", flush=True)
        return False


def click_reuse_and_generate(page, prompt, clip_num, account_name="", max_retries=3, wait_timeout=60):
    """
    Click reuse button, fill prompt, and click Generate with retry logic.
    
    Sometimes the reuse button click doesn't properly load the frames into
    the dialog, leaving the Generate button disabled. This function:
    1. Clicks reuse button
    2. Fills prompt
    3. Checks if Generate button is enabled
    4. If not, waits up to wait_timeout seconds
    5. If still not enabled, refreshes page and retries
    
    Args:
        page: Playwright page
        prompt: The prompt text to fill
        clip_num: Clip number for logging (1-indexed display)
        account_name: Account name for logging
        max_retries: Maximum retry attempts
        wait_timeout: How long to wait for Generate button to become enabled
        
    Returns:
        True if successful, raises Exception on failure
    """
    prefix = f"[{account_name}] " if account_name else ""
    
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                print(f"{prefix}Retry {attempt + 1}/{max_retries} for clip {clip_num} reuse...", flush=True)
                # Refresh page before retry
                print(f"{prefix}Refreshing page for reuse retry...", flush=True)
                page.reload(timeout=30000)
                page.wait_for_load_state("domcontentloaded", timeout=15000)
                time.sleep(3)
                check_and_dismiss_popup(page)
                
                # Human-like behavior after refresh
                human_pre_action(page, "reuse prompt")
            
            # Step 1: Click reuse button
            # New UI (Feb 2025+): button with class 'reuse-prompt-bu', icon 'redo', text 'Reuse text prompt'
            # Old UI: hidden <span>Reuse prompt</span> and <i>wrap_text</i> icon
            reuse_clicked = False
            reuse_selectors = [
                "button.reuse-prompt-button, button.reuse-prompt-bu",
                "button:has(i:text('redo')):has-text('Reuse')",
                "button:has-text('Reuse text prompt')",
                "button:has(span:text('Reuse prompt'))",
                "button:has(i:text('wrap_text'))",
                "i:text('wrap_text')",
            ]
            for sel in reuse_selectors:
                try:
                    loc = page.locator(sel).first
                    if loc.count() > 0 and loc.is_visible(timeout=1000):
                        human_click_locator(page, loc, "Reuse prompt button")
                        reuse_clicked = True
                        break
                except:
                    continue
            if not reuse_clicked:
                raise Exception(f"Could not click reuse button for clip {clip_num}")
            time.sleep(2)
            
            # Step 2: Fill the prompt (with human pacing)
            human_delay(1, 2)
            human_mouse_move(page)
            fill_prompt_textarea(page, prompt)
            print(f"✓ Clip {clip_num}: Entered prompt: {prompt[:50]}...", flush=True)
            
            # Post-prompt wait (matching test_human_like.py 10s wait)
            time.sleep(random.uniform(8, 12))
            
            # Step 3: Check if Generate button is enabled
            # If not enabled after 10s, re-click reuse button (up to 3 re-clicks)
            # If still not enabled after all re-clicks, do full page refresh retry
            generate_enabled = False
            max_reuse_reclicks = 3
            
            for reuse_attempt in range(max_reuse_reclicks + 1):
                start_wait = time.time()
                # Wait 10s per reuse attempt (first attempt gets the full check)
                reuse_wait = 10 if reuse_attempt == 0 else 8
                
                while (time.time() - start_wait) < reuse_wait:
                    if is_generate_button_enabled(page):
                        generate_enabled = True
                        break
                    time.sleep(2)
                
                if generate_enabled:
                    break
                
                if reuse_attempt < max_reuse_reclicks:
                    print(f"{prefix}⚠️ Generate button not enabled after {reuse_wait}s - re-clicking reuse button (attempt {reuse_attempt + 1}/{max_reuse_reclicks})", flush=True)
                    
                    # Re-click the reuse button
                    try:
                        # Try new UI selector first, then old
                        reuse_btn = page.locator("button.reuse-prompt-button, button.reuse-prompt-bu, button:has(i:text('redo')):has-text('Reuse'), button:has(span:text('Reuse prompt')), button:has(i:text('wrap_text'))")
                        if reuse_btn.count() > 0:
                            reuse_btn.first.click(force=True)
                            time.sleep(1)
                        else:
                            # Fallback to icon selector
                            icon = page.locator("i:text('redo'), i:text('wrap_text')")
                            if icon.count() > 0:
                                icon.first.click(force=True)
                        time.sleep(2)
                        
                        # CRITICAL: Re-fill the prompt after re-clicking reuse.
                        # The reuse button reloads the PREVIOUS clip's prompt into the textarea,
                        # overwriting our unique prompt. We must re-apply it.
                        fill_prompt_textarea(page, prompt)
                        time.sleep(1)
                        
                    except Exception as reclick_err:
                        print(f"{prefix}⚠️ Reuse re-click failed: {reclick_err}", flush=True)
            
            if not generate_enabled:
                print(f"{prefix}⚠️ Generate button not enabled after {wait_timeout}s for clip {clip_num}", flush=True)
                if attempt < max_retries - 1:
                    continue  # Retry
                else:
                    raise Exception(f"Generate button never became enabled for clip {clip_num}")
            
            # Step 4: Click Generate button (uses dispatch_event internally now)
            click_generate_button(page, account_name)
            print(f"✓ Clip {clip_num}: Generation started", flush=True)
            
            return True
            
        except Exception as e:
            error_msg = str(e)
            print(f"{prefix}⚠️ Reuse+Generate failed for clip {clip_num} (attempt {attempt + 1}/{max_retries}): {error_msg[:100]}", flush=True)
            
            if attempt >= max_retries - 1:
                raise Exception(f"Reuse+Generate failed for clip {clip_num} after {max_retries} attempts: {error_msg}")
    
    return False


def wait_for_login_if_needed(page, browser_name="Browser", timeout_minutes=10):
    """Wait for Google login if required, then dismiss any popups"""
    login_indicators = ["accounts.google.com", "identifier", "signin"]
    
    current_url = page.url.lower()
    
    def is_on_flow_page(url):
        """Check if URL is the Flow app (not Google auth).
        Simple URL check only — no DOM inspection to avoid slowdowns and false triggers.
        """
        return is_on_flow_not_login(url)
    
    current_url = page.url.lower()
    
    is_login_page = any(indicator in current_url for indicator in login_indicators)
    
    # If on Flow URL, do a quick check if actually logged in
    if is_on_flow_page(current_url):
        # Only check for unauthenticated landing page at startup (not during job processing)
        # If we can see "New project" or "Generate" or a project URL — we're logged in
        if "/project/" in current_url:
            # Already on a project page = definitely logged in
            check_and_dismiss_popup(page)
            return False
        try:
            # Quick check for logged-in indicators
            new_proj = page.locator("button:has-text('New project'), button:has(i:text('add_2')), button.sc-a38764c7-0").first
            if new_proj.is_visible(timeout=1000):
                check_and_dismiss_popup(page)
                return False
        except Exception:
            pass
        # Check if it's the unauthenticated landing page
        try:
            create_btn = page.locator("button:has-text('Create with Flow')")
            if create_btn.is_visible(timeout=1000):
                print(f"[{browser_name}] On Flow landing page but not logged in — triggering login...", flush=True)
                human_click_element(page, create_btn, f"[{browser_name}] Create with Flow")
                # Wait for redirect to Google login
                for _rw in range(15):
                    time.sleep(1)
                    try:
                        current_url = page.url.lower()
                        if "accounts.google" in current_url:
                            break
                    except Exception:
                        pass
                current_url = page.url.lower()
                is_login_page = True
            else:
                # No "Create with Flow" and no "New project" — could be loading, assume logged in
                check_and_dismiss_popup(page)
                return False
        except Exception:
            check_and_dismiss_popup(page)
            return False
    
    try:
        sign_in_text = page.locator("text=Sign in").count() > 0
        email_input = page.locator("input[type='email']").count() > 0
        is_login_page = is_login_page or (sign_in_text and email_input)
    except:
        pass
    
    if is_login_page:
        print(f"\n{'='*50}")
        print(f"[{browser_name}] GOOGLE LOGIN REQUIRED")
        print(f"Please complete login in the browser...")
        print(f"{'='*50}\n")
        
        start_time = time.time()
        max_wait = timeout_minutes * 60
        last_url = ""
        
        while True:
            time.sleep(2)
            
            try:
                current_url = page.url.lower()
            except Exception as e:
                print(f"[{browser_name}] Error getting URL: {e}", flush=True)
                continue
            
            # Log URL changes to help debug
            if current_url != last_url:
                print(f"[{browser_name}] URL changed to: {current_url[:80]}...", flush=True)
                last_url = current_url
            
            # Check if we're now on Flow (success!)
            if is_on_flow_page(current_url):
                print(f"✓ [{browser_name}] Login completed! (reached Flow)")
                time.sleep(3)
                break
            
            # Check if we're no longer on login page (but NOT on a redirect like SetSID)
            still_on_login = any(indicator in current_url for indicator in login_indicators)
            if not still_on_login and "accounts.google" not in current_url:
                print(f"✓ [{browser_name}] Login completed! (left login page)")
                time.sleep(3)
                break
            
            # Timeout check
            elapsed = time.time() - start_time
            if elapsed > max_wait:
                print(f"⚠️ [{browser_name}] Login timeout after {timeout_minutes} minutes!", flush=True)
                print(f"[{browser_name}] Current URL: {current_url}", flush=True)
                print(f"[{browser_name}] Continuing anyway...", flush=True)
                break
            
            # Progress indicator every 30 seconds
            if int(elapsed) % 30 == 0 and int(elapsed) > 0:
                print(f"[{browser_name}] Still waiting for login... ({int(elapsed)}s)", flush=True)
        
        wait_and_dismiss_popup(page, timeout=3)
        
        # Export storage state so download browser can reuse login
        try:
            context = page.context
            storage_state_file = os.path.join(BASE_DIR, f".storage_state_{browser_name.replace('-','_').lower()}.json")
            context.storage_state(path=storage_state_file)
            # Also save as the generic one for single-account mode
            generic_path = os.path.join(BASE_DIR, ".submit_storage_state.json")
            import shutil
            shutil.copy2(storage_state_file, generic_path)
        except Exception:
            pass  # Non-critical, download browser will just ask for login
        
        return True
    
    check_and_dismiss_popup(page)
    
    # Export storage state even if no login was needed
    try:
        context = page.context
        storage_state_file = os.path.join(BASE_DIR, f".storage_state_{browser_name.replace('-','_').lower()}.json")
        context.storage_state(path=storage_state_file)
        generic_path = os.path.join(BASE_DIR, ".submit_storage_state.json")
        import shutil
        shutil.copy2(storage_state_file, generic_path)
    except Exception:
        pass
    
    return False


def build_flow_prompt(
    dialogue_line: str,
    language: str = "English",
    voice_profile: str = None,
    duration: float = 8.0,
    facial_expression: str = None,
    body_language: str = None,
    delivery_style: str = None,
    emotion: str = None,
    redo_feedback: str = None,
) -> str:
    """
    Build prompt for Flow backend using same structure as veo_generator.py build_prompt().
    
    This ensures consistency between API and Flow backends.
    
    KEY PRINCIPLES (from veo_generator.py):
    1. NO VISUAL REDESCRIPTION: The image locks appearance
    2. RAW/DOCUMENTARY STYLE: Not "cinematic" - prevents AI glossy look
    3. STATIC CAMERA: For talking heads, locked-off camera preserves lip-sync
    4. VOICE PROFILE: Extract and pass voice traits correctly
    5. "Character says:" syntax for Veo lip-sync engine
    """
    # Clean dialogue
    dialogue_line = dialogue_line.strip().strip('"').strip("'")
    
    # Defaults
    if facial_expression is None:
        facial_expression = "natural engaged expression"
    if body_language is None:
        body_language = "natural posture"
    if delivery_style is None:
        delivery_style = "natural conversational"
    if emotion is None:
        emotion = "neutral"
    
    # Calculate timing
    speech_end_time = duration - 1.0
    
    # Extract voice traits from voice profile if provided
    voice_texture = ""
    voice_tone = ""
    voice_accent = ""
    voice_signature = ""
    
    if voice_profile:
        lines = voice_profile.split('\n')
        for line in lines:
            line_lower = line.lower().strip()
            line_clean = line.strip()
            
            # Extract texture (raspy, smooth, gravelly, etc.)
            if 'texture:' in line_lower:
                voice_texture = line_clean.split(':', 1)[1].strip() if ':' in line_clean else ""
            elif 'quality:' in line_lower and not voice_texture:
                voice_texture = line_clean.split(':', 1)[1].strip() if ':' in line_clean else ""
            
            # Extract tone
            if 'tone:' in line_lower:
                voice_tone = line_clean.split(':', 1)[1].strip() if ':' in line_clean else ""
            
            # Extract accent
            if 'accent:' in line_lower:
                accent_val = line_clean.split(':', 1)[1].strip() if ':' in line_clean else ""
                if accent_val and 'none' not in accent_val.lower() and 'neutral' not in accent_val.lower():
                    voice_accent = accent_val
            
            # Extract signature traits
            if 'signature' in line_lower and 'trait' in line_lower:
                voice_signature = line_clean.split(':', 1)[1].strip() if ':' in line_clean else ""
    
    # Build consolidated voice instruction
    voice_parts = []
    if voice_texture:
        voice_parts.append(voice_texture)
    if voice_tone:
        voice_parts.append(voice_tone)
    if voice_signature:
        voice_parts.append(voice_signature)
    if voice_accent:
        voice_parts.append(f"accent: {voice_accent}")
    
    short_voice = ", ".join(voice_parts) if voice_parts else "natural voice"
    
    # Build prompt following Veo 3.1 official structure (same as veo_generator.py)
    if voice_profile:
        final_prompt = f"""=== VOICE PROFILE ===
{voice_profile}
===

Medium shot, static locked-off camera, sharp focus on subject.

The subject in the frame speaks directly to camera with {facial_expression}, {body_language}.

The character says in {language}, "{dialogue_line}"

Voice: {short_voice}. {delivery_style}, {emotion} emotion.

Ambient noise: Complete silence, professional recording booth, no room ambiance.

Style: Raw realistic footage, natural lighting, photorealistic. Speech timing: 0s to {speech_end_time:.1f}s, then silence.

No subtitles, no text overlays, no captions, no watermarks. No background music, no laughter, no applause, no crowd sounds, no ambient noise. No morphing, no face distortion, no jerky movements. Only the speaker's isolated voice.

(no subtitles)"""
    else:
        # Simpler format without voice profile section
        final_prompt = f"""Medium shot, static locked-off camera, sharp focus on subject.

The subject in the frame speaks directly to camera with {facial_expression}, {body_language}.

The character says in {language}, "{dialogue_line}"

Voice: {short_voice}. {delivery_style}, {emotion} emotion.

Ambient noise: Complete silence, professional recording booth, no room ambiance.

Style: Raw realistic footage, natural lighting, photorealistic. Speech timing: 0s to {speech_end_time:.1f}s, then silence.

No subtitles, no text overlays, no captions, no watermarks. No background music, no laughter, no applause, no crowd sounds, no ambient noise. No morphing, no face distortion, no jerky movements. Only the speaker's isolated voice.

(no subtitles)"""

    # Add redo feedback at the top if present (same as veo_generator.py)
    if redo_feedback:
        final_prompt = f"""=== PRIORITY ===
{redo_feedback}
===

{final_prompt}"""
    
    return final_prompt


def get_prompt(dialogue, language="English", duration=8.0, voice_profile=None):
    """
    Generate video prompt from dialogue.
    
    This is a simplified wrapper around build_flow_prompt() for backward compatibility.
    The full build_flow_prompt() is used when more context is available.
    """
    return build_flow_prompt(
        dialogue_line=dialogue,
        language=language,
        voice_profile=voice_profile,
        duration=duration,
    )


# ============================================================
# DOWNLOAD WORKER (Parallel Thread)
# ============================================================

def get_tile_count_at_index0(page):
    """Kept for backward compat — returns 0."""
    return 0


def check_recent_clip_failure(page, data_index=1, clip_num=0, old_tile_ids=None):
    """
    Check if the most recently submitted clip has failed.
    
    Uses a robust tile detection approach:
    - Deduplicates tiles by data-tile-id (outer + inner divs share same ID)
    - Uses textContent (not innerText) to see through opacity:0 layers
    - 'videocam' in textContent = tile is generating (earliest signal)
    - percentage (digit+%) in textContent = tile is generating
    - <video> element = tile is completed
    - 'refresh' button = tile is TRULY failed (not just starting up)
    - 'undo' button WITHOUT videocam/% = tile starting up, not yet failed
    """
    print(f"[FailCheck] Checking clip {clip_num} for immediate failure...", flush=True)
    
    # Shared JS for tile analysis — used for both initial check and recheck
    TILE_CHECK_JS = r"""
        () => {
            const container = document.querySelector("div[data-index='0']");
            if (!container) return {tiles: 0, hasVideo: false, hasGenerating: false, failedCount: 0, allFailed: false};
            
            // Deduplicate tiles by data-tile-id (outer wrapper + inner div share same ID)
            const allTileEls = container.querySelectorAll("[data-tile-id]");
            const seen = new Set();
            const tiles = [];
            allTileEls.forEach(t => {
                const id = t.getAttribute("data-tile-id");
                if (!seen.has(id)) { seen.add(id); tiles.push(t); }
            });
            
            let hasVideo = false;
            let hasGenerating = false;
            let failedCount = 0;
            
            tiles.forEach(t => {
                // Check for completed video
                if (t.querySelector("video")) {
                    hasVideo = true;
                    return;
                }
                
                const text = t.textContent || '';
                
                // "videocam" = generating (appears before percentage)
                // percentage = generating
                if (text.includes('videocam') || /\d+%/.test(text)) {
                    hasGenerating = true;
                    return;
                }
                
                // Truly failed = has a refresh (Retry) button
                // Starting up tiles have "undo" (Reuse Prompt) but NOT "refresh"
                const hasRefresh = t.querySelector("i") && 
                    Array.from(t.querySelectorAll("i")).some(i => i.textContent.trim() === 'refresh');
                
                if (hasRefresh) {
                    failedCount++;
                    return;
                }
                
                // Has error/warning text but no refresh button = still starting up, not failed
            });
            
            return {
                tiles: tiles.length,
                hasVideo: hasVideo,
                hasGenerating: hasGenerating,
                failedCount: failedCount,
                allFailed: tiles.length > 0 && failedCount === tiles.length && !hasGenerating && !hasVideo
            };
        }
    """
    
    try:
        # Wait 10 seconds for the new clip to render
        time.sleep(10)
        
        result = page.evaluate(TILE_CHECK_JS)
        
        tiles = result.get('tiles', 0)
        has_video = result.get('hasVideo', False)
        has_generating = result.get('hasGenerating', False)
        failed_count = result.get('failedCount', 0)
        all_failed = result.get('allFailed', False)
        
        print(f"[FailCheck] data-index=0: {tiles} tiles, {failed_count} failed, generating={has_generating}, video={has_video}", flush=True)
        
        # Click Retry on truly failed tiles (ones with 'refresh' button)
        if failed_count > 0:
            print(f"[FailCheck] ⚠️ {failed_count} tile(s) truly failed — clicking Retry on each...", flush=True)
            try:
                container = page.locator("div[data-index='0']").first
                # Get deduplicated tiles via Playwright
                all_raw = container.locator("[data-tile-id]").all()
                seen_ids = set()
                unique_tiles = []
                for t in all_raw:
                    try:
                        tid = t.get_attribute("data-tile-id")
                        if tid and tid not in seen_ids:
                            seen_ids.add(tid)
                            unique_tiles.append(t)
                    except:
                        pass
                
                retried = 0
                for tile_idx, tile in enumerate(unique_tiles):
                    try:
                        # Only retry tiles that have the 'refresh' icon (truly failed)
                        retry_btn = tile.locator("button:has(i:text('refresh'))").first
                        if retry_btn.count() > 0 and retry_btn.is_visible(timeout=2000):
                            human_click_element(page, retry_btn, f"Retry button tile {tile_idx + 1}")
                            retried += 1
                            print(f"[FailCheck] ✓ Clicked Retry on tile {tile_idx + 1}", flush=True)
                            time.sleep(1)
                    except Exception as retry_err:
                        print(f"[FailCheck] Could not retry tile {tile_idx + 1}: {retry_err}", flush=True)
                print(f"[FailCheck] Retried {retried}/{failed_count} failed tiles", flush=True)
            except Exception as e:
                print(f"[FailCheck] Error clicking Retry buttons: {e}", flush=True)
        
        if has_generating:
            print(f"[FailCheck] ✓ Clip generating (has percentage)", flush=True)
            return False
        
        if has_video:
            print(f"[FailCheck] ✓ Clip has video", flush=True)
            return False
        
        if all_failed and tiles > 0:
            # All tiles truly failed — retries were clicked above.
            # Wait and re-check in case retries take effect.
            print(f"[FailCheck] All tiles truly failed — waiting 10s for retries to take effect...", flush=True)
            time.sleep(10)
            
            recheck = page.evaluate(TILE_CHECK_JS)
            
            rc_generating = recheck.get('hasGenerating', False)
            rc_video = recheck.get('hasVideo', False)
            rc_failed = recheck.get('failedCount', 0)
            rc_tiles = recheck.get('tiles', 0)
            print(f"[FailCheck] Re-check: {rc_tiles} tiles, {rc_failed} failed, generating={rc_generating}, video={rc_video}", flush=True)
            
            if rc_generating:
                print(f"[FailCheck] ✓ Clip generating after retry (has percentage)", flush=True)
                return False
            
            if rc_video:
                print(f"[FailCheck] ✓ Clip has video after retry", flush=True)
                return False
            
            if rc_failed == rc_tiles and rc_tiles > 0:
                print(f"[FailCheck] ⚠️ ALL {rc_tiles} tiles still failed after retry + 10s wait", flush=True)
                return True
            
            print(f"[FailCheck] ✓ Some tiles recovered after retry", flush=True)
            return False
        
        # No clear signal — assume OK
        print(f"[FailCheck] ✓ No clear failure detected", flush=True)
        return False
        
    except Exception as e:
        print(f"[FailCheck] Error: {e}", flush=True)
        return False


class ExtendedFailureMonitor:
    """
    Tracks clips that passed the immediate 3s failure check but need continued monitoring.
    
    After a clip passes the initial check, we continue monitoring it for up to 60 seconds
    to catch delayed failures that happen after submission but before download.
    
    Uses dialogue-based matching (same as download worker) to accurately identify which
    clip failed, regardless of how Flow reorders containers.
    """
    
    def __init__(self, monitoring_duration=60):
        """
        Args:
            monitoring_duration: How long (seconds) to monitor each clip after submission
        """
        self.monitoring_duration = monitoring_duration
        self.monitored_clips = {}  # clip_index -> {'submit_time': datetime, 'dialogue': str}
    
    def add_clip(self, clip_index, submit_time, dialogue_text="", prompt=""):
        """Add a clip to the monitoring list after it passes immediate failure check.
        
        Args:
            clip_index: The clip's index
            submit_time: When the clip was submitted
            dialogue_text: The clip's raw dialogue text (fallback)
            prompt: The full prompt sent to Flow (preferred for matching)
        """
        # Extract dialogue from prompt (same as download worker) for reliable matching
        dialogue = _extract_dialogue_from_prompt(prompt) if prompt else ""
        if not dialogue:
            dialogue = dialogue_text.strip().strip('"').strip("'") if dialogue_text else ""
        
        self.monitored_clips[clip_index] = {
            'submit_time': submit_time,
            'dialogue': dialogue,
        }
        print(f"[FailMonitor] Clip {clip_index} added to extended monitoring (60s window)", flush=True)
    
    def remove_clip(self, clip_index):
        """Remove a clip from monitoring (e.g., when it's confirmed failed or safe)."""
        if clip_index in self.monitored_clips:
            del self.monitored_clips[clip_index]
    
    def get_clips_to_check(self):
        """Get list of clips that are still within monitoring window."""
        now = datetime.now()
        clips_to_check = []
        expired = []
        
        for clip_idx, data in self.monitored_clips.items():
            elapsed = (now - data['submit_time']).total_seconds()
            if elapsed < self.monitoring_duration:
                clips_to_check.append(clip_idx)
            else:
                expired.append(clip_idx)
        
        # Remove expired clips (they're now considered safe)
        for clip_idx in expired:
            print(f"[FailMonitor] Clip {clip_idx} passed 60s monitoring window - considered safe", flush=True)
            del self.monitored_clips[clip_idx]
        
        return clips_to_check
    
    def check_for_failures(self, page, account_name=""):
        """
        Check all monitored clips for failures using dialogue-based matching.
        
        Args:
            page: Playwright page (must be on the project page)
            account_name: For logging
        
        Returns:
            List of clip indices that have failed
        """
        clips_to_check = self.get_clips_to_check()
        if not clips_to_check:
            return []
        
        failed_clips = []
        
        try:
            # Use Playwright locators instead of page.evaluate to avoid
            # reCAPTCHA stack trace accumulation during between-clip waits
            failed_containers_data = []
            container = page.locator("div[data-index='1']")
            if container.count() > 0:
                fail_text = container.locator("text=Failed")
                error_text = container.locator("text=Error")
                if fail_text.count() > 0 or error_text.count() > 0:
                    # Found a failure - try to get prompt text for matching
                    prompt_text = ""
                    try:
                        # Try prompt button
                        prompt_btn = container.locator("button[class*='sc-20145656-8']").first
                        if prompt_btn.count() > 0:
                            prompt_text = prompt_btn.inner_text(timeout=2000)
                    except:
                        pass
                    
                    if not prompt_text:
                        try:
                            # Try textarea
                            textarea = container.locator("textarea").first
                            if textarea.count() > 0:
                                prompt_text = textarea.input_value(timeout=2000)
                        except:
                            pass
                    
                    failed_containers_data.append({
                        'dataIndex': 1,
                        'promptText': prompt_text[:500] if prompt_text else ''
                    })
            
            if failed_containers_data and len(failed_containers_data) > 0:
                print(f"[FailMonitor] ⚠️ Detected {len(failed_containers_data)} failed container(s)", flush=True)
                
                # Build dialogue map for monitored clips
                dialogue_to_clip = {}
                for clip_idx in clips_to_check:
                    clip_data = self.monitored_clips.get(clip_idx, {})
                    dialogue = clip_data.get('dialogue', '')
                    if dialogue and len(dialogue) > 10:
                        dialogue_to_clip[dialogue] = clip_idx
                
                # Match each failed container to a clip by dialogue
                for container_data in failed_containers_data:
                    data_index = container_data.get('dataIndex')
                    prompt_text = container_data.get('promptText', '')
                    
                    matched_clip = None
                    best_match_len = 0
                    p_norm = ''.join(prompt_text.split()).lower()
                    for dialogue, clip_idx in dialogue_to_clip.items():
                        # Skip clips already marked as failed
                        if clip_idx in failed_clips:
                            continue
                        # Check if dialogue appears in the prompt (exact or normalized)
                        # Use longest match to avoid substring false positives
                        if dialogue in prompt_text:
                            if matched_clip is None or len(dialogue) > best_match_len:
                                matched_clip = clip_idx
                                best_match_len = len(dialogue)
                        d_norm = ''.join(dialogue.split()).lower()
                        if len(d_norm) > 15 and d_norm in p_norm:
                            if matched_clip is None or len(d_norm) > best_match_len:
                                matched_clip = clip_idx
                                best_match_len = len(d_norm)
                    
                    if matched_clip is not None:
                        print(f"[FailMonitor] ⚠️ Clip {matched_clip} FAILED (matched by dialogue at data-index={data_index})", flush=True)
                        failed_clips.append(matched_clip)
                    else:
                        print(f"[FailMonitor] ⚠️ Failed container at data-index={data_index} - no dialogue match", flush=True)
                
                # If we found failures but couldn't match any, try fallback
                if failed_containers_data and not failed_clips and clips_to_check:
                    # Assume most recently submitted clip failed
                    most_recent = max(clips_to_check)
                    print(f"[FailMonitor] ⚠️ Unmapped failure detected - assuming clip {most_recent} failed", flush=True)
                    failed_clips.append(most_recent)
        
        except Exception as e:
            print(f"[FailMonitor] Error during check: {e}", flush=True)
        
        # Remove failed clips from monitoring
        for clip_idx in failed_clips:
            self.remove_clip(clip_idx)
        
        return failed_clips
    
    def has_clips_to_monitor(self):
        """Check if there are any clips still being monitored."""
        # First clean up expired clips
        self.get_clips_to_check()
        return len(self.monitored_clips) > 0
    
    def do_periodic_check(self, page, account_name=""):
        """
        Disabled — failure detection is handled by check_recent_clip_failure (immediate)
        and the download browser timeout (eventual). ExtendedFailureMonitor caused false
        positives by detecting old "Failed"/"Error" text without checking for percentage.
        """
        return []


def quick_failure_check(page, clips_data, clip_project_map, main_project_url):
    """
    Quick scan for any failed clips and rebuild them in NEW projects.
    
    Args:
        page: Playwright page
        clips_data: List of clip data dicts
        clip_project_map: Dict mapping clip_index -> project_url (modified in place)
        main_project_url: URL of the main project
    
    Returns:
        Number of clips that were moved to retry projects
    """
    failures_rebuilt = 0
    check_and_dismiss_popup(page)
    
    # STEP 1: Scroll through ALL containers to ensure they're rendered in DOM
    # This is critical - elements outside viewport may not be detected
    num_clips = len(clips_data)
    max_data_index = num_clips + 10  # Check more in case of date headers
    
    print(f"[QuickCheck] Scrolling through containers to detect all failures...", flush=True)
    
    # PASS 1: Start at TOP where newest clips are (idx=1, 2, 3)
    page.keyboard.press("Home")  # Scroll to top without evaluate
    time.sleep(0.5)
    
    # Scroll through each data-index container (starting from idx=1, which is at top)
    for idx in range(1, max_data_index):
        try:
            container = page.locator(f"div[data-index='{idx}']")
            if container.count() > 0:
                container.first.scroll_into_view_if_needed(timeout=2000)
                time.sleep(0.2)
        except:
            pass  # Container might not exist, that's fine
    
    # PASS 2: Go back to top and scroll again (catches new failures at top)
    page.keyboard.press("Home")  # Scroll to top without evaluate
    time.sleep(0.3)
    
    for idx in range(1, max_data_index):
        try:
            container = page.locator(f"div[data-index='{idx}']")
            if container.count() > 0:
                container.first.scroll_into_view_if_needed(timeout=1000)
                time.sleep(0.15)
        except:
            pass
    
    # Small pause after scrolling to let everything settle
    time.sleep(1)
    check_and_dismiss_popup(page)
    
    # STEP 2: Now detect ALL failures using JavaScript — extract prompt text for matching
    try:
        # Find failed containers AND their prompt text for dialogue matching
        debug_info = page.evaluate(r"""
            () => {
                const failures = [];
                const allElements = document.querySelectorAll('*');
                for (const el of allElements) {
                    if (((el.innerText === 'Failed Generation' || el.innerText === 'Failed' || el.innerText === 'Error')) && el.children.length === 0) {
                        // Walk up to find the data-index container
                        let parent = el.parentElement;
                        let attempts = 0;
                        let foundIndex = null;
                        let container = null;
                        while (parent && attempts < 30) {
                            if (parent.dataset && parent.dataset.index !== undefined) {
                                foundIndex = parseInt(parent.dataset.index);
                                container = parent;
                                break;
                            }
                            parent = parent.parentElement;
                            attempts++;
                        }
                        // Skip if the container has a percentage (still generating)
                        if (container) {
                            const containerText = container.innerText || '';
                            if (/\d+%/.test(containerText)) continue;
                        }
                        // Extract prompt text from the container for dialogue matching
                        let promptText = '';
                        if (container) {
                            // Try the prompt display area
                            const promptEl = container.querySelector('[class*="prompt"], [class*="sc-21e778e8"], textarea');
                            if (promptEl) {
                                promptText = promptEl.innerText || promptEl.value || '';
                            }
                            // Fallback: get all text from container
                            if (!promptText || promptText.length < 20) {
                                promptText = container.innerText || '';
                            }
                        }
                        failures.push({
                            foundIndex: foundIndex,
                            attemptsNeeded: attempts,
                            parentTag: el.parentElement ? el.parentElement.tagName : 'none',
                            promptText: promptText.substring(0, 500)
                        });
                    }
                }
                return failures;
            }
        """)
        
        print(f"[QuickCheck] DEBUG: Raw failures found: {debug_info}", flush=True)
        
        # Extract the data indices (filtering out nulls)
        failed_indices_raw = [f['foundIndex'] for f in debug_info if f['foundIndex'] is not None]
        failed_indices = list(set(failed_indices_raw))  # Deduplicate
        
        # Log any failures that couldn't find their data-index
        orphan_failures = [f for f in debug_info if f['foundIndex'] is None]
        if orphan_failures:
            print(f"[QuickCheck] ⚠️ WARNING: {len(orphan_failures)} failure(s) couldn't find data-index parent!", flush=True)
        
        if not failed_indices or len(failed_indices) == 0:
            if orphan_failures:
                print(f"[QuickCheck] All {len(orphan_failures)} failures are orphans (no data-index container)", flush=True)
            return 0
        
        # Debug: show both raw count and unique containers
        if len(debug_info) != len(failed_indices):
            print(f"[QuickCheck] ⚠️ Found {len(debug_info)} 'Failed' texts → {len(failed_indices)} unique containers", flush=True)
            if len(debug_info) > len(failed_indices) and not orphan_failures:
                print(f"[QuickCheck] Note: Some containers have multiple failed variants", flush=True)
        
        print(f"[QuickCheck] ⚠️ Found {len(failed_indices)} failure(s) at data-index: {sorted(failed_indices)}", flush=True)
        
        # STEP 3: Match failed containers to clips using DIALOGUE MATCHING (not position)
        # Position-based mapping (len - 1 - index) is unreliable because Flow has date headers,
        # generating spinners, and indices shift after failures.
        
        # Build dialogue lookup from clips_data
        dialogue_to_clip = {}
        for cd in clips_data:
            prompt = cd.get('prompt', '')
            dialogue = cd.get('dialogue_text', '').strip().strip('"').strip("'")
            clip_idx = cd.get('clip_index')
            # Use dialogue from prompt if available
            extracted = _extract_dialogue_from_prompt(prompt) if prompt else ''
            key = extracted if extracted else dialogue
            if key and len(key) > 10:
                dialogue_to_clip[key] = cd
        
        clips_to_retry = []
        matched_clip_indices = set()
        
        for data_index in failed_indices:
            # Get prompt text from the failure info
            prompt_text = ''
            for f in debug_info:
                if f.get('foundIndex') == data_index:
                    prompt_text = f.get('promptText', '')
                    break
            
            # Try dialogue matching — find ALL matches, pick LONGEST
            matched_clip = None
            if prompt_text:
                p_norm = ''.join(prompt_text.split()).lower()
                best_match = None
                best_len = 0
                for dialogue_key, clip_data in dialogue_to_clip.items():
                    ci = clip_data.get('clip_index')
                    if ci in matched_clip_indices:
                        continue  # Already matched
                    if dialogue_key in prompt_text and len(dialogue_key) > best_len:
                        best_match = clip_data
                        best_len = len(dialogue_key)
                    d_norm = ''.join(dialogue_key.split()).lower()
                    if len(d_norm) > 15 and d_norm in p_norm and len(d_norm) > best_len:
                        best_match = clip_data
                        best_len = len(d_norm)
                matched_clip = best_match
            
            # Fallback: position-based (less reliable but better than nothing)
            if matched_clip is None:
                positional_idx = len(clips_data) - 1 - data_index
                if 0 <= positional_idx < len(clips_data):
                    fallback = clips_data[positional_idx]
                    if fallback.get('clip_index') not in matched_clip_indices:
                        matched_clip = fallback
                        print(f"[QuickCheck] ⚠️ Using position fallback for data-index={data_index} → clip {fallback.get('clip_index')}", flush=True)
            
            if matched_clip:
                actual_clip_index = matched_clip.get('clip_index')
                matched_clip_indices.add(actual_clip_index)
                
                # Check if already retried
                if actual_clip_index in clip_project_map and clip_project_map[actual_clip_index] != main_project_url:
                    print(f"[QuickCheck] Clip {actual_clip_index} already in retry project, skipping", flush=True)
                    continue
                
                print(f"[QuickCheck] Matched data-index={data_index} → clip {actual_clip_index} (by dialogue)", flush=True)
                clips_to_retry.append({
                    'clip_data': matched_clip,
                    'actual_clip_index': actual_clip_index,
                    'data_index': data_index
                })
            else:
                print(f"[QuickCheck] ⚠️ Could not match data-index={data_index} to any clip", flush=True)
        
        if not clips_to_retry:
            print(f"[QuickCheck] All failed clips already have retry projects", flush=True)
            return 0
        
        print(f"[QuickCheck] Will create retry projects for {len(clips_to_retry)} clip(s): {[c['actual_clip_index'] for c in clips_to_retry]}", flush=True)
        
        # STEP 4: Create retry projects for each failed clip
        for retry_info in clips_to_retry:
            clip_data = retry_info['clip_data']
            actual_clip_index = retry_info['actual_clip_index']
            data_index = retry_info['data_index']
            
            print(f"\n[QuickCheck] Creating retry project for clip {actual_clip_index} (data-index={data_index})", flush=True)
            
            # Create new project for this failed clip
            new_project_url, retry_success = _create_retry_project_for_clip(page, clip_data, max_retries=2)
            
            if new_project_url and retry_success:
                clip_project_map[actual_clip_index] = new_project_url
                failures_rebuilt += 1
                print(f"[QuickCheck] ✓ Clip {actual_clip_index} moved to: {new_project_url}", flush=True)
                
                # Navigate back to main project to continue with next retry
                print(f"[QuickCheck] Returning to main project...", flush=True)
                page.goto(main_project_url, timeout=30000)
                time.sleep(3)
                check_and_dismiss_popup(page)
            else:
                print(f"[QuickCheck] ❌ Failed to create retry project for clip {actual_clip_index}", flush=True)
        
        print(f"\n[QuickCheck] Completed: {failures_rebuilt}/{len(clips_to_retry)} retry projects created", flush=True)
        
    except Exception as e:
        print(f"[QuickCheck] Error: {e}", flush=True)
        import traceback
        traceback.print_exc()
    
    return failures_rebuilt


def _rebuild_failed_clip(page, clip_data):
    """Rebuild a failed clip with frames and prompt. Delegates to rebuild_clip."""
    start_frame = clip_data.get('start_frame')
    end_frame = clip_data.get('end_frame')
    prompt = clip_data.get('prompt')
    clip_index = clip_data.get('clip_index')
    
    print(f"[Rebuild] Rebuilding clip {clip_index}...", flush=True)
    return rebuild_clip(page, start_frame, end_frame, prompt, is_first_clip=True, context="[Rebuild]")


def _create_retry_project_for_clip(page, clip_data, max_retries=2):
    """
    Create a NEW project and submit a single failed clip to it.
    Will retry up to max_retries times if the generation fails immediately.
    
    Returns:
        Tuple of (project_url, success) where:
        - project_url: URL of the retry project (or None if all retries failed)
        - success: True if generation started successfully, False if it failed
    """
    clip_index = clip_data.get('clip_index')
    prompt = clip_data.get('prompt', '')
    start_frame = clip_data.get('start_frame')
    end_frame = clip_data.get('end_frame')
    
    # Verify frame files exist — re-download if missing
    if start_frame and not os.path.exists(start_frame):
        print(f"[RetryProject] ⚠️ Start frame missing: {start_frame}", flush=True)
        start_frame_url = clip_data.get('start_frame_url')
        if start_frame_url:
            start_frame = download_frame(start_frame_url, start_frame)
            print(f"[RetryProject] {'✓ Re-downloaded' if start_frame else '✗ Failed to re-download'} start frame", flush=True)
        else:
            start_frame = None
    
    if end_frame and not os.path.exists(end_frame):
        print(f"[RetryProject] ⚠️ End frame missing: {end_frame}", flush=True)
        end_frame_url = clip_data.get('end_frame_url')
        if end_frame_url:
            end_frame = download_frame(end_frame_url, end_frame)
            print(f"[RetryProject] {'✓ Re-downloaded' if end_frame else '✗ Failed to re-download'} end frame", flush=True)
        else:
            end_frame = None
    
    if not start_frame and not end_frame:
        print(f"[RetryProject] ❌ No frames available for clip {clip_index}!", flush=True)
        return (None, False)
    
    for retry_attempt in range(max_retries):
        try:
            attempt_label = f"(attempt {retry_attempt + 1}/{max_retries})" if max_retries > 1 else ""
            print(f"\n[RetryProject] Creating new project for failed clip {clip_index} {attempt_label}...", flush=True)
            
            # Navigate back to Flow home to create new project (SPA — preserve reCAPTCHA)
            spa_navigate_to_flow_home(page, "RetryProject")
            human_delay(2, 4)  # Match main flow post-navigation wait
            
            ensure_logged_into_flow(page, "RETRY")
            check_and_dismiss_popup(page)
            
            # Human-like "looking around" before first interaction (match main flow exactly)
            human_mouse_move(page)
            human_delay(1, 2)
            scroll_randomly(page)
            human_delay(0.5, 1)
            
            # Click "New project" button
            dismiss_create_with_flow(page, "RetryProject")
            human_click_element(page, "button:has-text('New project'), button:has(i:text('add_2')), button.sc-a38764c7-0", "[RetryProject] New project button")
            human_delay(2, 3)  # Match main flow post-click wait
            
            # Wait for project URL
            try:
                page.wait_for_url("**/project/**", timeout=30000)
            except:
                print("[RetryProject] wait_for_url timed out, polling...", flush=True)
                for _ in range(15):
                    time.sleep(1)
                    if "/project/" in page.url:
                        break
            
            time.sleep(2)
            new_project_url = page.url
            
            if "/project/" not in new_project_url:
                print(f"[RetryProject] ❌ Failed to create project - URL: {new_project_url}", flush=True)
                continue  # Try again
            
            print(f"[RetryProject] ✓ Created retry project: {new_project_url}", flush=True)
            human_delay(1, 2)  # Match main flow post-creation wait
            check_and_dismiss_popup(page)
            ensure_videos_tab_selected(page)
            
            # Submit clip using the same method as the main flow
            pre_generate_tile_count = get_tile_count_at_index0(page)
            if not rebuild_clip(page, start_frame, end_frame, prompt, is_first_clip=True, context="[RetryProject]"):
                print(f"[RetryProject] ❌ Failed to submit clip {clip_index}", flush=True)
                continue  # Try again with a new project
            
            # Check for immediate failure
            if check_recent_clip_failure(page, data_index=1, clip_num=clip_index, old_tile_ids=pre_generate_tile_count):
                print(f"[RetryProject] ⚠️ Clip {clip_index} failed again in retry project!", flush=True)
                if retry_attempt < max_retries - 1:
                    print(f"[RetryProject] Will try again...", flush=True)
                    continue  # Try again with a new project
                else:
                    print(f"[RetryProject] ❌ Clip {clip_index} failed after {max_retries} retry attempts", flush=True)
                    return (new_project_url, False)  # Return URL but mark as failed
            
            # Success!
            print(f"[RetryProject] ✓ Clip {clip_index} retry successful!", flush=True)
            return (new_project_url, True)
            
        except Exception as e:
            print(f"[RetryProject] ❌ Error creating retry project: {e}", flush=True)
            import traceback
            traceback.print_exc()
            if retry_attempt < max_retries - 1:
                print(f"[RetryProject] Will try again...", flush=True)
                continue
    
    return (None, False)  # All retries failed


def _extract_dialogue_from_prompt(prompt: str) -> str:
    """Extract the unique dialogue line from a prompt for clip matching.
    
    Handles both prompt formats:
    1. build_flow_prompt format: 'The character says in English, "dialogue here"'
    2. JSON prompt format: '"character_line": "dialogue here"'
    
    Returns the dialogue string, or empty string if not found.
    """
    if not prompt:
        return ""
    
    # Format 1: The character says in {language}, "..."
    import re
    match = re.search(r'The character says in \w+,\s*"(.+?)"', prompt)
    if match:
        return match.group(1).strip()
    
    # Format 2: "character_line": "..."
    match = re.search(r'"character_line"\s*:\s*"(.+?)"', prompt)
    if match:
        return match.group(1).strip()
    
    return ""


class DownloadWorker(threading.Thread):
    """Background thread that downloads completed clips - browser opens on first job"""
    
    def __init__(self, download_queue, cache, session_folder=None, account_name="DOWNLOAD", proxy=None, submit_session_folder=None):
        super().__init__(daemon=True)
        self.download_queue = download_queue
        self.cache = cache
        self.session_folder = session_folder or DOWNLOAD_SESSION_FOLDER
        self.submit_session_folder = submit_session_folder  # Main browser session to sync from
        self.account_name = account_name
        self.proxy = proxy
        self.stop_flag = threading.Event()
        self.ready_flag = threading.Event()
        self.browser = None
        self.page = None
        self.browser_started = False
        self.cancelled_jobs = set()  # Jobs cancelled due to failover
        self.limited_clips = {}  # job_id -> set of allowed clip indices (for partial failover)
        self.shutdown_after_job = False  # If True, shutdown after completing current job (failover)
    
    def _sync_session_cookies(self):
        """Ensure download browser profile has login state from the submit browser.
        
        The profile should have been copied at startup (while the submit Chrome was closed),
        but if that was skipped (e.g., browser crashed before login, user closed browser early),
        we copy it here before launching the download browser.
        
        NOTE: The submit browser IS running at this point, so file locks may exist.
        We copy cautiously, skipping locked files (SingletonLock, etc.).
        """
        import shutil
        
        prefs_file = os.path.join(self.session_folder, "Default", "Preferences")
        cookies_file = os.path.join(self.session_folder, "Default", "Cookies")
        
        # Check if we have a valid profile with cookies
        has_profile = os.path.exists(prefs_file)
        has_cookies = os.path.exists(cookies_file) and os.path.getsize(cookies_file) > 0
        
        if has_profile and has_cookies:
            print(f"[{self.account_name}] ✓ Download profile exists with cookies (synced from submit browser)")
            return
        
        # Profile is missing or incomplete — try to copy from submit browser
        submit_folder = self.submit_session_folder
        if not submit_folder or not os.path.exists(submit_folder):
            print(f"[{self.account_name}] ⚠ No download profile and no submit folder to copy from — you may need to log in manually")
            return
        
        submit_cookies = os.path.join(submit_folder, "Default", "Cookies")
        if not os.path.exists(submit_cookies):
            print(f"[{self.account_name}] ⚠ Submit browser has no cookies yet — download browser may need manual login")
            return
        
        print(f"[{self.account_name}] ⚠ Download profile missing/incomplete — copying from submit browser...")
        
        try:
            # Remove stale download profile if it exists
            if os.path.exists(self.session_folder):
                try:
                    shutil.rmtree(self.session_folder)
                except Exception as e:
                    print(f"[{self.account_name}] ⚠ Could not remove old download profile: {e}")
            
            # Copy the submit browser profile
            # The submit browser IS running, so we skip lock files and handle errors gracefully
            def ignore_locks(dir, files):
                return [f for f in files if f in ('SingletonLock', 'SingletonSocket', 'SingletonCookie', 
                                                   'lockfile', 'LOCK', 'lock')]
            
            shutil.copytree(submit_folder, self.session_folder,
                           ignore_dangling_symlinks=True,
                           ignore=ignore_locks)
            print(f"[{self.account_name}] ✓ Profile copied from submit browser → download session")
        except Exception as e:
            print(f"[{self.account_name}] ⚠ Profile copy failed: {e}")
            print(f"[{self.account_name}] Download browser may need manual login")
    
    def _start_browser(self, p):
        """Lazy-load browser on first download job"""
        if self.browser_started:
            return
        
        print(f"\n[{self.account_name}] 🚀 Starting download browser (first job received)...")
        print(f"[{self.account_name}] Session folder: {self.session_folder}")
        
        # Sync login cookies from the submit browser session to the download session
        # This ensures the download browser is always logged in
        self._sync_session_cookies()
        
        try:
            proxy_config = parse_proxy_url(self.proxy) if self.proxy else None
            if proxy_config:
                print(f"[{self.account_name}] Using proxy: {proxy_config['server']}", flush=True)
            
            dl_chrome_args = [
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--force-variation-ids=3300115,3300134,3313321,3328827,3330196,3362821',
            ]
            
            # Only add proxy-related flags when proxy is configured
            if proxy_config:
                dl_chrome_args.append('--ignore-certificate-errors')
                # Create proxy auth extension for automatic credential handling
                ext_dir = os.path.join(BASE_DIR, f".proxy_auth_ext_{self.account_name}")
                auth_ext = create_proxy_auth_extension(self.proxy, ext_dir)
                if auth_ext:
                    dl_chrome_args.extend([
                        f'--disable-extensions-except={auth_ext}',
                        f'--load-extension={auth_ext}',
                    ])
            
            if BROWSER_MODE == "stealth":
                print(f"[{self.account_name}] Launching Chrome...")
                dl_launch_kwargs = {
                    'user_data_dir': self.session_folder,
                    'channel': 'chrome',
                    'ignore_default_args': ['--enable-automation'],
                    'headless': False,
                    'viewport': {"width": 1280, "height": 720},
                    'args': dl_chrome_args,
                }
                if proxy_config:
                    dl_launch_kwargs['proxy'] = proxy_config
                self.browser = p.chromium.launch_persistent_context(**dl_launch_kwargs)
            else:
                print(f"[{self.account_name}] Launching Chromium...")
                dl_launch_kwargs = {
                    'user_data_dir': self.session_folder,
                    'headless': False,
                    'viewport': {"width": 1280, "height": 720},
                    'args': dl_chrome_args,
                }
                if proxy_config:
                    dl_launch_kwargs['proxy'] = proxy_config
                self.browser = p.chromium.launch_persistent_context(**dl_launch_kwargs)
            
            # Match test_human_like.py
            self.page = self.browser.pages[0] if self.browser.pages else self.browser.new_page()
            # Note: Patchright handles stealth (webdriver, CDP) natively — no init script needed
            
            # Warm up Chrome — sync variations seed
            chrome_warmup(self.page)
            
            # Match test_human_like.py startup
            print(f"[{self.account_name}] Navigating to Flow...", flush=True)
            self.page.goto(FLOW_HOME_URL)
            human_delay(2, 4)
            
            human_mouse_move(self.page)
            human_delay(1, 2)
            
            # Check if login needed
            current_url = self.page.url.lower()
            if "accounts.google" in current_url:
                print(f"[{self.account_name}] Login page detected, waiting...", flush=True)
                for _v in range(120):
                    time.sleep(1)
                    if is_on_flow_not_login(self.page.url):
                        print(f"[{self.account_name}] ✓ Login complete", flush=True)
                        human_delay(2, 4)
                        break
                else:
                    print(f"[{self.account_name}] ⚠ Login timeout", flush=True)
            else:
                print(f"[{self.account_name}] ✓ Already logged in", flush=True)
            
            check_and_dismiss_popup(self.page)
            
            self.browser_started = True
            print(f"[{self.account_name}] ✓ Download browser ready!")
            
        except Exception as e:
            print(f"[{self.account_name}] ❌ Error starting browser: {e}", flush=True)
            import traceback
            traceback.print_exc()
            raise  # Re-raise to let the job be handled
    
    def _drain_cancel_messages(self):
        """Check queue for any pending cancel/limit messages and process them.
        
        This allows cancel/limit messages to be processed even while we're in the middle of
        downloading, preventing race conditions where cancel arrives after job was dequeued.
        """
        drained = 0
        while True:
            try:
                msg = self.download_queue.get_nowait()
                if msg is None:
                    # Stop signal - put it back
                    self.download_queue.put(None)
                    break
                elif msg.get('type') == 'cancel':
                    cancel_job_id = msg.get('job_id', '')
                    self.cancelled_jobs.add(cancel_job_id)
                    print(f"[{self.account_name}] ⚠️ Job {cancel_job_id[:8]}... CANCELLED (failover)", flush=True)
                    drained += 1
                    self.download_queue.task_done()
                elif msg.get('type') == 'limit_clips':
                    limit_job_id = msg.get('job_id', '')
                    allowed_clips = msg.get('allowed_clips', set())
                    self.limited_clips[limit_job_id] = allowed_clips
                    print(f"[{self.account_name}] ⚠️ Job {limit_job_id[:8]}... LIMITED to clips {sorted(allowed_clips)} (failover)", flush=True)
                    drained += 1
                    self.download_queue.task_done()
                elif msg.get('type') == 'shutdown_after_complete':
                    self.shutdown_after_job = True
                    print(f"[{self.account_name}] ⚠️ Will shutdown after completing current job (failover)", flush=True)
                    drained += 1
                    self.download_queue.task_done()
                else:
                    # Regular job - put it back
                    self.download_queue.put(msg)
                    break
            except queue.Empty:
                break
        return drained
    
    def run(self):
        print(f"[{self.account_name}] Download worker started (browser will open on first job)")
        
        # Signal ready immediately - browser will start lazily
        self.ready_flag.set()
        
        with sync_playwright() as p:
            while not self.stop_flag.is_set():
                try:
                    job_data = self.download_queue.get(timeout=5)
                    
                    if job_data is None:
                        print(f"[{self.account_name}] Received stop signal")
                        break
                    
                    # Handle cancellation messages (from failover)
                    if job_data.get('type') == 'cancel':
                        cancelled_job_id = job_data['job_id']
                        self.cancelled_jobs.add(cancelled_job_id)
                        print(f"[{self.account_name}] ⚠️ Job {cancelled_job_id[:8]}... CANCELLED (failover to other account)", flush=True)
                        self.download_queue.task_done()
                        continue
                    
                    # Handle limit_clips messages (partial failover - some clips handed off)
                    if job_data.get('type') == 'limit_clips':
                        limit_job_id = job_data['job_id']
                        allowed_clips = job_data.get('allowed_clips', set())
                        self.limited_clips[limit_job_id] = allowed_clips
                        print(f"[{self.account_name}] ⚠️ Job {limit_job_id[:8]}... LIMITED to clips {sorted(allowed_clips)} (failover)", flush=True)
                        self.download_queue.task_done()
                        continue
                    
                    # Handle shutdown_after_complete messages (failover - shutdown after current job)
                    if job_data.get('type') == 'shutdown_after_complete':
                        self.shutdown_after_job = True
                        print(f"[{self.account_name}] ⚠️ Will shutdown after completing current job (failover)", flush=True)
                        self.download_queue.task_done()
                        continue
                    
                    # Check if this job was already cancelled
                    job_id = job_data.get('job_id', '')
                    if job_id in self.cancelled_jobs:
                        print(f"[{self.account_name}] ⏭️ Skipping cancelled job {job_id[:8]}...", flush=True)
                        self.cancelled_jobs.discard(job_id)  # Clean up
                        self.download_queue.task_done()
                        continue
                    
                    # Start browser on first job
                    if not self.browser_started:
                        self._start_browser(p)
                    
                    self.process_download(job_data)
                    self.download_queue.task_done()
                    
                    # Check if we should shutdown after this job (failover occurred)
                    if self.shutdown_after_job:
                        print(f"[{self.account_name}] ✓ Job complete, shutting down (failover mode)", flush=True)
                        break
                    
                except queue.Empty:
                    continue
                except Exception as e:
                    print(f"[{self.account_name}] Error: {e}", flush=True)
                    import traceback
                    traceback.print_exc()
                    try:
                        self.download_queue.task_done()
                    except:
                        pass
            
            if self.browser:
                print(f"[{self.account_name}] Closing browser...", flush=True)
                self.browser.close()
    
    def process_download(self, job_data):
        """Download all clips for a job (or single clip for redo), handling multiple projects.
        
        IMPORTANT: clip_project_map is shared by reference and gets updated by the submission
        thread when clips fail and retry projects are created. We must re-check it dynamically
        rather than grouping clips once at the start.
        """
        job_id = job_data['job_id']
        project_url = job_data['project_url']
        clips = job_data['clips']
        temp_dir = job_data['temp_dir']
        is_redo = job_data.get('is_redo', False)
        clip_project_map = job_data.get('clip_project_map', {})
        clip_submit_times = job_data.get('clip_submit_times', {})  # Per-clip submission times
        clips_data = job_data.get('clips_data', [])  # For failure handling
        permanently_failed_clips = job_data.get('permanently_failed_clips', set())  # Clips that failed permanently
        downloaded_videos = job_data.get('downloaded_videos', {})  # Shared dict for continue mode - maps clip_index to video path
        num_clips = job_data.get('num_clips', len(clips))
        
        print(f"\n[{self.account_name}] {'='*40}", flush=True)
        if is_redo:
            print(f"[{self.account_name}] REDO Clip {clips[0]['clip_index']}", flush=True)
        else:
            print(f"[{self.account_name}] JOB {job_id[:8]}...", flush=True)
        print(f"[{self.account_name}] Main Project: {project_url}", flush=True)
        print(f"[{self.account_name}] Clips: {len(clips)}", flush=True)
        if clip_submit_times:
            print(f"[{self.account_name}] Submit times tracked for {len(clip_submit_times)} clips", flush=True)
        
        print(f"\n[{self.account_name}] Clip data received:", flush=True)
        for c in clips:
            print(f"  Clip {c.get('clip_index')}: dialogue_text='{c.get('dialogue_text', '')[:60]}...'", flush=True)
        
        print(f"[{self.account_name}] {'='*40}", flush=True)
        
        total_downloaded = 0
        downloaded_clip_indices = set()
        failed_in_main = set()  # Clips that failed in main project, waiting for retry
        
        try:
            # STEP 1: Navigate to main project and start downloading
            print(f"\n[{self.account_name}] === MAIN PROJECT ===", flush=True)
            
            # Download from main project, but skip clips that have retry URLs or permanently failed
            main_downloaded, main_failed = self._download_from_project_dynamic(
                project_url, clips, job_id, temp_dir,
                clip_submit_times=clip_submit_times, 
                clips_data=clips_data, 
                is_redo=is_redo,
                clip_project_map=clip_project_map,
                downloaded_clip_indices=downloaded_clip_indices,
                permanently_failed_clips=permanently_failed_clips,
                downloaded_videos=downloaded_videos
            )
            total_downloaded += main_downloaded
            failed_in_main = main_failed
            
            # STEP 2: Check clip_project_map for any clips that need to be downloaded from retry projects
            # This map gets updated by the submission thread as retries happen
            retry_project_clips = {}
            for clip in clips:
                clip_idx = clip.get('clip_index')
                if clip_idx in downloaded_clip_indices:
                    continue  # Already downloaded
                clip_project = clip_project_map.get(clip_idx, project_url)
                if clip_project != project_url:
                    # This clip has a retry project
                    if clip_project not in retry_project_clips:
                        retry_project_clips[clip_project] = []
                    retry_project_clips[clip_project].append(clip)
            
            if retry_project_clips:
                print(f"\n[{self.account_name}] Found {len(retry_project_clips)} retry project(s) to download from", flush=True)
                
            # STEP 3: Download from each retry project
            for retry_url, retry_clips in retry_project_clips.items():
                print(f"\n[{self.account_name}] === RETRY PROJECT ({len(retry_clips)} clips) ===", flush=True)
                print(f"[{self.account_name}] Navigating to: {retry_url}", flush=True)
                
                # For retry projects, use simplified download logic
                # Each retry project has exactly the clips we expect - no need for complex matching
                # _download_retry_project handles the wait timing internally
                downloaded = self._download_retry_project(
                    retry_url, retry_clips, job_id, temp_dir,
                    downloaded_clip_indices=downloaded_clip_indices,
                    clip_submit_times=clip_submit_times
                )
                total_downloaded += downloaded
            
            # STEP 4: Re-check clip_project_map for NEW retry projects that appeared during downloads
            # (The submission thread may have created more retry projects while we were downloading)
            max_recheck_rounds = 15  # Each round waits 30s = up to 7.5 min total patience
            recheck_round = 0
            while recheck_round < max_recheck_rounds:
                new_retry_project_clips = {}
                for clip in clips:
                    clip_idx = clip.get('clip_index')
                    if clip_idx in downloaded_clip_indices:
                        continue
                    if clip_idx in permanently_failed_clips:
                        continue
                    clip_project = clip_project_map.get(clip_idx, project_url)
                    if clip_project != project_url and clip_project not in retry_project_clips:
                        if clip_project not in new_retry_project_clips:
                            new_retry_project_clips[clip_project] = []
                        new_retry_project_clips[clip_project].append(clip)
                
                if not new_retry_project_clips:
                    # Check if there are still clips pending that haven't been assigned to retry projects yet
                    pending_clips = [c for c in clips 
                                    if c.get('clip_index') not in downloaded_clip_indices 
                                    and c.get('clip_index') not in permanently_failed_clips]
                    if pending_clips:
                        # Some clips are still pending — wait a bit for the submission thread to finish retries
                        pending_indices = [c.get('clip_index') for c in pending_clips]
                        print(f"[{self.account_name}] ⏳ {len(pending_clips)} clip(s) still pending: {pending_indices}, waiting 30s for retry projects... (round {recheck_round + 1}/{max_recheck_rounds})", flush=True)
                        time.sleep(30)
                        recheck_round += 1
                        continue  # Re-check after waiting
                    break  # All clips accounted for
                
                print(f"\n[{self.account_name}] Found {len(new_retry_project_clips)} NEW retry project(s) (recheck round {recheck_round + 1})", flush=True)
                retry_project_clips.update(new_retry_project_clips)
                
                for retry_url, retry_clips in new_retry_project_clips.items():
                    print(f"\n[{self.account_name}] === RETRY PROJECT ({len(retry_clips)} clips) ===", flush=True)
                    print(f"[{self.account_name}] Navigating to: {retry_url}", flush=True)
                    downloaded = self._download_retry_project(
                        retry_url, retry_clips, job_id, temp_dir,
                        downloaded_clip_indices=downloaded_clip_indices,
                        clip_submit_times=clip_submit_times
                    )
                    total_downloaded += downloaded
                    if downloaded > 0:
                        # Reset counter — we made progress, give more time for remaining retries
                        recheck_round = 0
                    else:
                        recheck_round += 1
            
            num_failed = len(permanently_failed_clips) if permanently_failed_clips else 0
            print(f"\n[{self.account_name}] ✓ Downloaded {total_downloaded}/{len(clips)} clips total (failed: {num_failed})", flush=True)
            
            # Clean up any limited_clips entry for this job
            self.limited_clips.pop(job_id, None)
            
            if not is_redo:
                if total_downloaded + num_failed >= len(clips):
                    update_job_status(job_id, 'completed')
                    mark_job_completed(self.cache, job_id)
                else:
                    missing = len(clips) - total_downloaded - num_failed
                    print(f"[{self.account_name}] ⚠️ {missing} clip(s) not downloaded — marking job as completed anyway to avoid stuck state", flush=True)
                    update_job_status(job_id, 'completed')
                    mark_job_completed(self.cache, job_id)
        
        except Exception as e:
            print(f"[{self.account_name}] ❌ ERROR in download process: {e}", flush=True)
            import traceback
            traceback.print_exc()
        
        finally:
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except:
                pass
    
    def _download_retry_project(self, project_url, clips, job_id, temp_dir, downloaded_clip_indices=None, clip_submit_times=None):
        """Download clips from a retry project.
        
        Simplified logic for retry projects:
        - Each retry project contains exactly the clips we expect
        - No need for complex dialogue matching or position calculation
        - Just find videos and download them, assigning to the expected clips
        
        Args:
            project_url: URL of the retry project
            clips: List of clips expected in this project
            job_id: Job ID
            temp_dir: Temp directory
            downloaded_clip_indices: Set of already downloaded clip indices (modified in place)
            clip_submit_times: Dict of clip_index -> submission datetime (for timing)
        
        Returns:
            Number of clips downloaded
        """
        if downloaded_clip_indices is None:
            downloaded_clip_indices = set()
        if clip_submit_times is None:
            clip_submit_times = {}
        
        downloaded_count = 0
        
        try:
            # First, wait for clips to be ready (60s since submission)
            for clip in clips:
                clip_idx = clip.get('clip_index')
                submit_time = clip_submit_times.get(clip_idx)
                if submit_time:
                    elapsed = (datetime.now() - submit_time).total_seconds()
                    if elapsed < CLIP_READY_WAIT:
                        wait_time = CLIP_READY_WAIT - elapsed
                        print(f"[{self.account_name}] Waiting {wait_time:.0f}s for retry clip {clip_idx} to be ready...", flush=True)
                        time.sleep(wait_time)
            
            # Navigate to retry project
            max_nav_retries = 3
            for nav_attempt in range(max_nav_retries):
                try:
                    print(f"[{self.account_name}] Navigation attempt {nav_attempt + 1}/{max_nav_retries}...", flush=True)
                    self.page.goto(project_url, timeout=60000)
                    self.page.wait_for_load_state("domcontentloaded", timeout=30000)
                    time.sleep(5)
                    
                    current_url = self.page.url.lower()
                    if "accounts.google" in current_url or "signin" in current_url:
                        print(f"[{self.account_name}] Redirected to Google login, waiting...", flush=True)
                        ensure_logged_into_flow(self.page, "DOWNLOAD")
                        continue
                    
                    print(f"[{self.account_name}] Navigation successful!", flush=True)
                    
                    # CRITICAL: Verify we're on the correct project URL
                    actual_url = self.page.url
                    if "/project/" in project_url:
                        expected_project_id = project_url.split("/project/")[-1].split("?")[0].split("/")[0]
                        actual_project_id = actual_url.split("/project/")[-1].split("?")[0].split("/")[0] if "/project/" in actual_url else ""
                        
                        if expected_project_id != actual_project_id:
                            print(f"[{self.account_name}] ⚠️ URL MISMATCH! Expected: {expected_project_id}, Got: {actual_project_id}", flush=True)
                            print(f"[{self.account_name}]   Force-navigating to correct project...", flush=True)
                            self.page.goto(project_url, timeout=60000)
                            self.page.wait_for_load_state("domcontentloaded", timeout=30000)
                            time.sleep(3)
                    
                    break
                    
                except Exception as nav_error:
                    if nav_attempt < max_nav_retries - 1:
                        print(f"[{self.account_name}] Navigation error, retrying: {nav_error}", flush=True)
                        time.sleep(3)
                        continue
                    else:
                        raise
            
            check_and_dismiss_popup(self.page)
            ensure_videos_tab_selected(self.page)
            
            # Wait for videos to appear (with extended timeout for retry projects)
            print(f"[{self.account_name}] Waiting for videos in retry project...", flush=True)
            videos_found = False
            max_wait_attempts = 60  # Up to 2 minutes of waiting
            for wait_attempt in range(max_wait_attempts):
                video_count = self.page.locator("video").count()
                if video_count > 0:
                    print(f"[{self.account_name}] Found {video_count} video(s) after {wait_attempt * 2}s", flush=True)
                    videos_found = True
                    break
                
                # Check for failure
                try:
                    fail_count = self.page.evaluate(r"""
                        () => {
                            let count = 0;
                            const containers = document.querySelectorAll('[data-index]');
                            for (const c of containers) {
                                const text = c.innerText || '';
                                if (/\d+%/.test(text)) continue;
                                c.querySelectorAll('*').forEach(el => {
                                    if (((el.innerText === 'Failed Generation' || el.innerText === 'Failed' || el.innerText === 'Error')) && el.children.length === 0) count++;
                                });
                            }
                            return count;
                        }
                    """)
                    if fail_count > 0:
                        print(f"[{self.account_name}] ⚠️ Found {fail_count} failed generation(s) in retry project", flush=True)
                        # Mark clips as failed
                        for clip in clips:
                            clip_idx = clip.get('clip_index')
                            clip_id = clip.get('id')
                            if clip_id:
                                update_clip_status(clip_id, 'failed', error_message="Retry generation also failed")
                        return 0
                except:
                    pass
                
                # Refresh page periodically to check for new videos
                if wait_attempt > 0 and wait_attempt % 10 == 0:
                    print(f"[{self.account_name}] Still waiting for video... ({wait_attempt * 2}s elapsed)", flush=True)
                    try:
                        self.page.reload(timeout=30000)
                        time.sleep(3)
                        check_and_dismiss_popup(self.page)
                        ensure_videos_tab_selected(self.page)
                        ensure_batch_view_mode(self.page, f"[{self.account_name}-RetryRefresh]")
                    except:
                        pass
                
                time.sleep(2)
            
            if not videos_found:
                print(f"[{self.account_name}] ⚠️ No videos found in retry project after {max_wait_attempts * 2}s", flush=True)
                return 0
            
            # For each clip in this retry project, download using download_single_clip logic
            # Since retry projects typically have 1 clip each, this works well
            for clip in clips:
                clip_idx = clip.get('clip_index')
                
                if clip_idx in downloaded_clip_indices:
                    print(f"[{self.account_name}] Clip {clip_idx} already downloaded, skipping", flush=True)
                    continue
                
                print(f"[{self.account_name}] Downloading retry clip {clip_idx}...", flush=True)
                
                # Use download_single_clip which finds the first available video
                result = self.download_single_clip(clip, job_id, temp_dir, data_index=0)
                
                if result > 0:
                    downloaded_clip_indices.add(clip_idx)
                    downloaded_count += 1
                    print(f"[{self.account_name}] ✓ Retry clip {clip_idx} downloaded!", flush=True)
                else:
                    print(f"[{self.account_name}] ❌ Failed to download retry clip {clip_idx}", flush=True)
                    # Mark as failed
                    clip_id = clip.get('id')
                    if clip_id:
                        update_clip_status(clip_id, 'failed', error_message="Could not download from retry project")
            
            return downloaded_count
            
        except Exception as e:
            print(f"[{self.account_name}] Error downloading from retry project: {e}", flush=True)
            import traceback
            traceback.print_exc()
            return 0
    
    # ================================================================
    # REFACTORED DOWNLOAD SYSTEM
    # 3 clean phases per cycle: SCAN → MATCH → DOWNLOAD
    # Then refresh and repeat until all clips handled.
    # ================================================================

    def _scan_all_containers(self, max_index=25):
        """Phase 1: SCAN — Scroll to each container and read it while visible.
        
        CRITICAL: Flow uses virtual scrolling. Container children (video, textarea,
        buttons) are only rendered when the container is in/near the viewport.
        A single JS call from the top sees containers but their children are unloaded.
        We MUST scroll to each index individually and read it while it's visible.
        
        Returns a list of dicts, each with:
            data_index, has_video, has_failed, prompt_text
        """
        results = []
        consecutive_missing = 0
        
        try:
            for idx in range(max_index + 1):
                # Check if container exists in DOM at all
                container = self.page.locator(f"div[data-index='{idx}']")
                if container.count() == 0:
                    # Try scrolling down to force virtual scroll to render it
                    self.page.mouse.wheel(0, 400)
                    time.sleep(0.3)
                    if container.count() == 0:
                        consecutive_missing += 1
                        if consecutive_missing >= 3:
                            break  # No more containers
                        continue
                
                consecutive_missing = 0
                
                # SCROLL into view so Flow renders the children
                try:
                    container.first.scroll_into_view_if_needed(timeout=2000)
                    time.sleep(0.2)  # Brief pause for render
                except:
                    pass
                
                # NOW read the container's content via JS while it's visible
                # ALSO extract video src URLs while the container is rendered
                info = self.page.evaluate(f"""
                    () => {{
                        const c = document.querySelector("div[data-index='{idx}']");
                        if (!c) return null;
                        const text = c.innerText || '';
                        const hasVideo = c.querySelector('video') !== null;
                        const hasPercentage = /\\d+%/.test(text);
                        const hasFailedText = (text.includes('Failed Generation') || text.includes('Failed') || text.includes('Error'));
                        const hasFailed = hasFailedText && !hasPercentage && !hasVideo;
                        if (!hasVideo && !hasFailed) return null;
                        let promptText = '';
                        let promptSource = 'none';
                        
                        // Robust prompt extraction: find longest text element containing dialogue
                        const candidates = c.querySelectorAll('div, button, span, a, p');
                        let bestPrompt = '';
                        let bestLen = 0;
                        let bestSrc = 'none';
                        for (const el of candidates) {{
                            const t = el.innerText || '';
                            if (t.length < 100) continue;
                            const hasSaysIn = t.includes('says in');
                            const score = hasSaysIn ? t.length + 100000 : t.length;
                            if (score > bestLen) {{
                                bestPrompt = t;
                                bestLen = score;
                                bestSrc = hasSaysIn ? 'dialogue-div' : 'long-text';
                            }}
                        }}
                        if (bestPrompt) {{
                            promptText = bestPrompt;
                            promptSource = bestSrc;
                        }}
                        if (!promptText) {{ promptText = text.substring(0, 1500); promptSource = 'fallback-text'; }}
                        
                        // Extract video src URLs for direct HTTP download
                        const videoUrls = [];
                        const videos = c.querySelectorAll('video');
                        for (const v of videos) {{
                            let url = v.src || '';
                            if (!url) {{
                                const source = v.querySelector('source');
                                if (source) url = source.src || '';
                            }}
                            videoUrls.push(url);
                        }}
                        
                        return {{dataIndex: {idx}, hasVideo: hasVideo, hasFailed: hasFailed, promptText: promptText, promptSource: promptSource, videoUrls: videoUrls}};
                    }}
                """)
                
                if info:
                    results.append(info)
            
        except Exception as e:
            print(f"[{self.account_name}] Scan error: {e}", flush=True)
        
        return results

    def _match_container_to_clip(self, container_info, dialogue_to_clip, pending_clip_indices, downloaded_clip_indices, clip_submit_times=None, num_submitted=0):
        """Phase 2: MATCH — Pure Python, no DOM access.
        
        Matches a scanned container to a pending clip using dialogue.
        
        Strategies (in order):
        1. EXACT DIALOGUE: Full dialogue string found in prompt text (longest wins)
        2. NORMALIZED DIALOGUE: Collapse whitespace, compare (longest wins)
        3. SINGLE PENDING: If only one clip left, assign to any video container
        
        SAFETY: Containers with unreliable prompt sources (fallback-text, none)
        are SKIPPED entirely — they'll be rescanned after refresh when the 
        prompt div renders properly.
        
        Returns the matched clip dict or None.
        """
        prompt_text = container_info.get('promptText', '')
        has_video = container_info.get('hasVideo', False)
        prompt_source = container_info.get('promptSource', 'none')
        data_index = container_info.get('dataIndex', -1)
        
        # SAFETY: Never match on unreliable prompt sources.
        # fallback-text contains garbage like "warning Failed undo Reuse Prompt..."
        # Wait for next refresh when the prompt div will render properly.
        if prompt_source in ('fallback-text', 'none') or not prompt_text:
            return None
        
        # Strategy 1: EXACT FULL dialogue match
        # The dialogue is the complete sentence from 'The character says in X, "..."'
        # It MUST appear exactly in the prompt text. Longest match wins (handles
        # the theoretical case where one dialogue is a substring of another).
        best_match = None
        best_len = 0
        for dialogue, clip in dialogue_to_clip.items():
            ci = clip.get('clip_index')
            if ci not in pending_clip_indices:
                continue
            if not dialogue:
                continue
            if dialogue in prompt_text and len(dialogue) > best_len:
                best_match = clip
                best_len = len(dialogue)
        if best_match:
            return best_match
        
        # Strategy 2: Normalized match — collapse ALL whitespace, lowercase
        # Handles minor whitespace differences between stored dialogue and DOM text
        p_norm = ''.join(prompt_text.split()).lower()
        best_match = None
        best_len = 0
        for dialogue, clip in dialogue_to_clip.items():
            ci = clip.get('clip_index')
            if ci not in pending_clip_indices:
                continue
            if not dialogue or len(dialogue) < 20:
                continue
            d_norm = ''.join(dialogue.split()).lower()
            if d_norm in p_norm and len(d_norm) > best_len:
                best_match = clip
                best_len = len(d_norm)
        if best_match:
            return best_match
        
        # Strategy 3: If only ONE clip is pending, assign to any video container
        # that doesn't match a downloaded clip's dialogue AND doesn't match
        # any other known clip's dialogue (e.g. a clip still generating/not yet pending)
        if len(pending_clip_indices) == 1 and has_video:
            # Safety: make sure this container isn't from an already-downloaded clip
            for dialogue, clip in dialogue_to_clip.items():
                if clip.get('clip_index') in downloaded_clip_indices and dialogue:
                    d_norm = ''.join(dialogue.split()).lower()
                    if len(d_norm) > 20 and d_norm in p_norm:
                        return None  # Belongs to an already-downloaded clip
            # Safety: make sure this container isn't from a DIFFERENT clip
            # that's still generating (not in pending yet due to CLIP_READY_WAIT)
            only_ci = next(iter(pending_clip_indices))
            for dialogue, clip in dialogue_to_clip.items():
                ci = clip.get('clip_index')
                if ci == only_ci or ci in downloaded_clip_indices:
                    continue  # Skip the pending clip itself and already-downloaded ones
                # This is another clip (generating, not ready, etc.)
                if dialogue:
                    d_norm = ''.join(dialogue.split()).lower()
                    if len(d_norm) > 20 and d_norm in p_norm:
                        return None  # Container belongs to another clip, not ours
            for dialogue, clip in dialogue_to_clip.items():
                if clip.get('clip_index') == only_ci:
                    return clip
        
        return None

    def _navigate_to_project(self, project_url, max_retries=3):
        """Shared navigation logic with URL verification and login handling.
        
        Returns True if navigation succeeded, False otherwise.
        """
        for nav_attempt in range(max_retries):
            try:
                print(f"[{self.account_name}] Navigation attempt {nav_attempt + 1}/{max_retries}...", flush=True)
                self.page.goto(project_url, timeout=60000)
                self.page.wait_for_load_state("domcontentloaded", timeout=30000)
                time.sleep(5)
                
                current_url = self.page.url.lower()
                if "accounts.google" in current_url or "signin" in current_url:
                    print(f"[{self.account_name}] Redirected to Google login, waiting...", flush=True)
                    ensure_logged_into_flow(self.page, "DOWNLOAD")
                    continue
                
                print(f"[{self.account_name}] Navigation successful!", flush=True)
                
                # Verify correct project URL (Flow sometimes redirects to last-visited project)
                if "/project/" in project_url:
                    actual_url = self.page.url
                    expected_pid = project_url.split("/project/")[-1].split("?")[0].split("/")[0]
                    actual_pid = actual_url.split("/project/")[-1].split("?")[0].split("/")[0] if "/project/" in actual_url else ""
                    
                    if expected_pid != actual_pid:
                        print(f"[{self.account_name}] ⚠️ URL MISMATCH! Expected: {expected_pid}, Got: {actual_pid}", flush=True)
                        self.page.goto(project_url, timeout=60000)
                        self.page.wait_for_load_state("domcontentloaded", timeout=30000)
                        time.sleep(3)
                        
                        final_url = self.page.url
                        final_pid = final_url.split("/project/")[-1].split("?")[0].split("/")[0] if "/project/" in final_url else ""
                        if expected_pid != final_pid:
                            raise Exception(f"Failed to navigate to correct project. Expected {expected_pid}, got {final_pid}")
                        print(f"[{self.account_name}] ✓ Now on correct project: {final_pid}", flush=True)
                
                return True
                
            except Exception as nav_error:
                if nav_attempt < max_retries - 1:
                    print(f"[{self.account_name}] Navigation error, retrying: {nav_error}", flush=True)
                    time.sleep(3)
                    continue
                else:
                    print(f"[{self.account_name}] Navigation failed after {max_retries} attempts: {nav_error}", flush=True)
                    return False
        return False

    def _refresh_and_verify(self, project_url=None):
        """Refresh page and verify we're still on the correct project.
        
        Returns True if refresh succeeded.
        """
        try:
            self.page.reload(timeout=30000)
            time.sleep(3)
            check_and_dismiss_popup(self.page)
            ensure_videos_tab_selected(self.page)
            ensure_batch_view_mode(self.page, f"[{self.account_name}-Refresh]")
            
            # Verify project URL after reload
            if project_url and "/project/" in project_url:
                actual_url = self.page.url
                expected_pid = project_url.split("/project/")[-1].split("?")[0].split("/")[0]
                actual_pid = actual_url.split("/project/")[-1].split("?")[0].split("/")[0] if "/project/" in actual_url else ""
                
                if expected_pid != actual_pid:
                    print(f"[{self.account_name}] ⚠️ URL MISMATCH after reload! Force-navigating...", flush=True)
                    self.page.goto(project_url, timeout=60000)
                    self.page.wait_for_load_state("domcontentloaded", timeout=30000)
                    time.sleep(3)
            
            # Wait for video elements (up to 10s)
            try:
                self.page.wait_for_selector("video", timeout=10000)
            except:
                pass  # OK — maybe no videos are ready yet
            
            return True
        except Exception as e:
            print(f"[{self.account_name}] Refresh error: {e}", flush=True)
            return False

    def _download_from_project_dynamic(self, project_url, clips, job_id, temp_dir,
                                        clip_submit_times=None, clips_data=None, is_redo=False,
                                        clip_project_map=None, downloaded_clip_indices=None,
                                        permanently_failed_clips=None, downloaded_videos=None):
        """Download clips from a project using the clean Scan→Match→Download cycle.
        
        Handles:
        - Per-clip timing (wait 60s after submission)
        - Failure detection (before refresh, since Flow clears failure UI on refresh)
        - Multi-clip batch download per refresh cycle
        - Stuck clip retry
        - Dynamic clip_project_map changes (clips moved to retry projects)
        
        Returns:
            Tuple of (num_downloaded, set_of_failed_clip_indices)
        """
        if downloaded_clip_indices is None:
            downloaded_clip_indices = set()
        if clip_project_map is None:
            clip_project_map = {}
        if permanently_failed_clips is None:
            permanently_failed_clips = set()
        if clip_submit_times is None:
            clip_submit_times = {}
        
        failed_clips = set()
        
        try:
            # Navigate to project
            if not self._navigate_to_project(project_url):
                return 0, failed_clips
            
            check_and_dismiss_popup(self.page)
            ensure_videos_tab_selected(self.page)
            ensure_batch_view_mode(self.page, f"[{self.account_name}-Download]")
            clips_for_this_project = []
            for clip in clips:
                clip_idx = clip.get('clip_index')
                if clip_idx in downloaded_clip_indices:
                    continue
                if clip_idx in permanently_failed_clips:
                    continue
                clip_project = clip_project_map.get(clip_idx, project_url)
                if clip_project != project_url:
                    continue
                clips_for_this_project.append(clip)
            
            if not clips_for_this_project:
                print(f"[{self.account_name}] No clips to download from this project", flush=True)
                return 0, failed_clips
            
            print(f"[{self.account_name}] Downloading {len(clips_for_this_project)} clips from this project", flush=True)
            
            # Special case: single redo clip — simple path
            if is_redo and len(clips_for_this_project) == 1:
                clip = clips_for_this_project[0]
                clip_idx = clip.get('clip_index')
                submit_time = clip_submit_times.get(clip_idx) if clip_submit_times else None
                if submit_time:
                    elapsed = (datetime.now() - submit_time).total_seconds()
                    if elapsed < CLIP_READY_WAIT:
                        wait_remaining = CLIP_READY_WAIT - elapsed
                        print(f"[{self.account_name}] Waiting {wait_remaining:.0f}s for redo clip...", flush=True)
                        time.sleep(wait_remaining)
                    self._refresh_and_verify(project_url)
                
                result = self.download_single_clip(clip, job_id, temp_dir, data_index=0)
                if result > 0:
                    downloaded_clip_indices.add(clip_idx)
                return result, failed_clips
            
            # ============================================================
            # MAIN DOWNLOAD LOOP: Scan → Match → Download (all ready) → Refresh → Repeat
            # ============================================================
            downloaded_count = self._download_loop(
                clips=clips_for_this_project,
                all_clips=clips,
                job_id=job_id,
                temp_dir=temp_dir,
                clip_submit_times=clip_submit_times,
                clips_data=clips_data,
                clip_project_map=clip_project_map,
                downloaded_clip_indices=downloaded_clip_indices,
                failed_clips=failed_clips,
                permanently_failed_clips=permanently_failed_clips,
                downloaded_videos=downloaded_videos,
                project_url=project_url,
            )
            return downloaded_count, failed_clips
            
        except Exception as e:
            print(f"[{self.account_name}] Error in dynamic download: {e}", flush=True)
            import traceback
            traceback.print_exc()
            return 0, failed_clips

    def _download_loop(self, clips, all_clips, job_id, temp_dir,
                        clip_submit_times, clips_data, clip_project_map,
                        downloaded_clip_indices, failed_clips,
                        permanently_failed_clips, downloaded_videos,
                        project_url):
        """Core download loop: Scan → Match → Download ALL ready → Refresh → Repeat.
        
        This is the single unified download method that replaces:
        - _download_with_timing_dynamic
        - _download_with_timing
        - download_all_clips
        - the deep scan section
        
        Returns number of clips downloaded.
        """
        num_clips = len(clips)
        downloaded_count = 0
        refreshed_for_clips = set()       # Clips we've already triggered an initial refresh for
        last_refresh_times = {}            # clip_idx → datetime of last refresh
        downloaded_urls = set()            # Track video URLs already claimed by a clip (prevent cross-clip dupes)
        
        # Build dialogue mapping for matching containers to clips.
        # We extract unique dialogue snippets from each clip's ACTUAL PROMPT
        # (the same text that Flow stores and shows in the prompt button).
        # This ensures we match against exactly what appears in the DOM.
        dialogue_to_clip = {}
        for clip in clips:
            # Primary: extract dialogue from the full prompt (what Flow actually has)
            prompt = clip.get('prompt', '')
            dialogue_key = _extract_dialogue_from_prompt(prompt)
            
            # Fallback: use raw dialogue_text
            if not dialogue_key:
                dialogue_key = clip.get('dialogue_text', '').strip().strip('"').strip("'")
            
            if dialogue_key:
                if dialogue_key in dialogue_to_clip:
                    existing_ci = dialogue_to_clip[dialogue_key].get('clip_index')
                    new_ci = clip.get('clip_index')
                    print(f"[{self.account_name}] ⚠️ DUPLICATE DIALOGUE: clip {new_ci} has same dialogue as clip {existing_ci} — matching may be unreliable", flush=True)
                dialogue_to_clip[dialogue_key] = clip
        
        print(f"\n[{self.account_name}] ═══ Download loop: {num_clips} clips ═══", flush=True)
        print(f"[{self.account_name}] Wait {CLIP_READY_WAIT}s per clip after submission", flush=True)
        for dk, dc in dialogue_to_clip.items():
            print(f"[{self.account_name}]   dialogue_key[{dc.get('clip_index')}]: '{dk[:60]}'", flush=True)
        
        max_poll_time = max(300, num_clips * 60 + 120)
        STUCK_TIMEOUT = 300  # 5 minutes before considering a clip stuck
        MAX_STUCK_RETRIES = 3  # Only retry this many stuck clips before giving up
        MAX_EMPTY_CYCLES = 8  # Give up after N scan cycles with zero downloads
        start_time = datetime.now()
        empty_cycles = 0  # Consecutive scan cycles with 0 downloads
        
        # Check for cancellation support
        allowed_clips_for_job = self.limited_clips.get(job_id)
        
        while downloaded_count + len(failed_clips) < num_clips:
            elapsed = (datetime.now() - start_time).total_seconds()
            
            # Dynamic timeout: max_poll_time counts from the LAST clip submission,
            # not from when the download loop started. This prevents timeout while
            # the submit thread is still submitting clips.
            last_submit = max(clip_submit_times.values()) if clip_submit_times else start_time
            time_since_last_submit = (datetime.now() - last_submit).total_seconds()
            all_submitted = len(clip_submit_times) >= num_clips - len(permanently_failed_clips)
            
            # Only enforce timeout after all clips have been submitted (or after a very long absolute cap)
            if all_submitted and time_since_last_submit > max_poll_time:
                print(f"[{self.account_name}] ⚠️ Max poll time exceeded ({time_since_last_submit:.0f}s since last submit, cap={max_poll_time}s)", flush=True)
                break
            elif elapsed > max_poll_time * 3:
                # Absolute safety cap: 3x the normal timeout from loop start
                print(f"[{self.account_name}] ⚠️ Absolute poll time exceeded ({elapsed:.0f}s)", flush=True)
                break
            
            # Check for cancellation (failover)
            self._drain_cancel_messages()
            if job_id in self.cancelled_jobs:
                print(f"[{self.account_name}] ⚠️ Job cancelled (failover), aborting download", flush=True)
                self.cancelled_jobs.discard(job_id)
                break
            
            allowed_clips_for_job = self.limited_clips.get(job_id)
            
            # ── Build the set of PENDING clips (ready for download) ──
            pending = set()
            not_ready_yet = []
            for clip in clips:
                ci = clip.get('clip_index')
                if ci in downloaded_clip_indices or ci in failed_clips:
                    continue
                if ci in permanently_failed_clips:
                    failed_clips.add(ci)
                    continue
                if allowed_clips_for_job is not None and ci not in allowed_clips_for_job:
                    failed_clips.add(ci)  # Handed off to another account
                    continue
                # Check if moved to retry project
                clip_project = clip_project_map.get(ci, project_url)
                if clip_project != project_url:
                    failed_clips.add(ci)  # Will be downloaded from retry project
                    continue
                
                submit_time = clip_submit_times.get(ci)
                if not submit_time:
                    not_ready_yet.append(ci)
                    continue
                
                elapsed_since_submit = (datetime.now() - submit_time).total_seconds()
                if elapsed_since_submit >= CLIP_READY_WAIT:
                    pending.add(ci)
                else:
                    not_ready_yet.append(ci)
            
            if not pending and not not_ready_yet:
                break  # All clips handled
            
            if not pending:
                # No clips ready yet — sleep and retry
                time.sleep(5)
                continue
            
            # ── Decide whether to REFRESH ──
            should_refresh = False
            refresh_reason = ""
            
            # Initial refresh: a clip just reached 60s for the first time
            new_ready = pending - refreshed_for_clips
            if new_ready:
                should_refresh = True
                refresh_reason = f"Clips {sorted(new_ready)} reached {CLIP_READY_WAIT}s"
                refreshed_for_clips.update(new_ready)
            
            # Re-refresh: a clip has been pending for 30s+ since last refresh without download
            if not should_refresh:
                for ci in pending:
                    if ci in downloaded_clip_indices:
                        continue
                    last_ref = last_refresh_times.get(ci, start_time)
                    since_ref = (datetime.now() - last_ref).total_seconds()
                    if since_ref >= 30:
                        should_refresh = True
                        refresh_reason = f"Re-refresh for clip {ci} ({since_ref:.0f}s since last refresh)"
                        break
            
            if should_refresh:
                # ── REFRESH ──
                # Note: failure detection is handled by the submit browser.
                # The download browser only looks for completed videos.
                print(f"[{self.account_name}] {refresh_reason} — refreshing page...", flush=True)
                self._refresh_and_verify(project_url)
                for ci in pending:
                    last_refresh_times[ci] = datetime.now()
            
            # ── Progress log (only when status changes) ──
            submitted = len([c for c in clips if c.get('clip_index') in clip_submit_times])
            status_key = f"{submitted}:{len(pending)}:{downloaded_count}:{len(failed_clips)}"
            if not hasattr(self, '_last_status') or self._last_status != status_key:
                print(f"[{self.account_name}] Status: {submitted}/{num_clips} submitted, "
                      f"{len(pending)} pending, {downloaded_count} downloaded, "
                      f"{len(failed_clips)} failed/moved", flush=True)
                self._last_status = status_key
            
            check_and_dismiss_popup(self.page)
            
            if not pending:
                time.sleep(5)
                continue
            
            # ══════════════════════════════════════════════════
            # CHECK idx 0, 1, 2, 3, 4, 5 — match by dialogue, download pending
            # New UI (Feb 2025+): No date headers — clips start at idx 0.
            # Old UI: idx 0 = date header, clips at idx 1+.
            # We scan idx 0-5 to handle both layouts.
            # Newest clip is at lowest idx, older ones higher.
            # ══════════════════════════════════════════════════
            downloaded_this_cycle = 0
            
            for idx in range(6):
                if not pending:
                    break
                
                # Scroll to container and read it
                container = self.page.locator(f"div[data-index='{idx}']")
                if container.count() == 0:
                    # Try scrolling down to render it
                    for _ in range(3):
                        self.page.mouse.wheel(0, 500)
                        time.sleep(0.3)
                        if container.count() > 0:
                            break
                    if container.count() == 0:
                        continue
                
                try:
                    container.first.scroll_into_view_if_needed(timeout=2000)
                    time.sleep(0.2)
                except:
                    pass
                
                # Read container via JS
                cinfo = self.page.evaluate(f"""
                    () => {{
                        const c = document.querySelector("div[data-index='{idx}']");
                        if (!c) return null;
                        const text = c.innerText || '';
                        const hasVideo = c.querySelector('video') !== null;
                        const hasPercentage = /\\d+%/.test(text);
                        const hasFailedText = (text.includes('Failed Generation') || text.includes('Failed') || text.includes('Error'));
                        const hasFailed = hasFailedText && !hasPercentage && !hasVideo;
                        let promptText = '';
                        let promptSource = 'none';
                        
                        // Robust prompt extraction: scan ALL leaf-ish elements for the longest
                        // text that looks like a prompt (contains 'says in' which is in every
                        // prompt we generate). This works regardless of CSS class changes.
                        // Check divs, buttons, spans, a elements — anything that might hold prompt text.
                        const candidates = c.querySelectorAll('div, button, span, a, p');
                        let bestPrompt = '';
                        let bestLen = 0;
                        let bestSource = 'none';
                        for (const el of candidates) {{
                            // Use innerText to get visible text only
                            const t = el.innerText || '';
                            if (t.length < 100) continue;
                            // Prefer elements containing 'says in' (our dialogue pattern)
                            const hasSaysIn = t.includes('says in');
                            const score = hasSaysIn ? t.length + 100000 : t.length;
                            if (score > bestLen) {{
                                bestPrompt = t;
                                bestLen = score;
                                bestSource = hasSaysIn ? 'dialogue-div' : 'long-text';
                            }}
                        }}
                        if (bestPrompt) {{
                            promptText = bestPrompt;
                            promptSource = bestSource;
                        }}
                        
                        // Fallback: container text (unreliable — may contain tile UI garbage)
                        if (!promptText) {{ promptText = text.substring(0, 1500); promptSource = 'fallback-text'; }}
                        
                        const videoUrls = [];
                        if (hasVideo) {{
                            const videos = c.querySelectorAll('video');
                            for (const v of videos) {{
                                let url = v.src || '';
                                if (!url) {{
                                    const source = v.querySelector('source');
                                    if (source) url = source.src || '';
                                }}
                                videoUrls.push(url);
                            }}
                        }}
                        
                        return {{
                            dataIndex: {idx}, hasVideo: hasVideo, hasFailed: hasFailed, 
                            promptText: promptText, promptSource: promptSource, videoUrls: videoUrls
                        }};
                    }}
                """)
                
                if not cinfo:
                    continue
                
                hv = cinfo.get('hasVideo')
                hf = cinfo.get('hasFailed')
                pt = cinfo.get('promptText', '')
                ps = cinfo.get('promptSource', '?')
                
                # Only log scan details on first cycle or when state changes
                scan_key = f"{idx}:{hv}:{hf}"
                if not hasattr(self, '_last_scan_state'):
                    self._last_scan_state = {}
                if scan_key not in self._last_scan_state or self._last_scan_state[scan_key] != (hv, hf):
                    # Extract dialogue snippet from prompt for debug
                    says_pos = pt.find('says in')
                    dialogue_snip = pt[says_pos+15:says_pos+75].strip() if says_pos >= 0 else pt[:60]
                    print(f"[{self.account_name}] SCAN idx={idx}: vid={hv} fail={hf} prompt_len={len(pt)} src={ps} dialogue='{dialogue_snip}'", flush=True)
                    self._last_scan_state[scan_key] = (hv, hf)
                
                if not hv and not hf:
                    continue
                
                # MATCH — find which pending clip this container belongs to
                num_submitted = len(clip_submit_times) if clip_submit_times else 0
                matched = self._match_container_to_clip(cinfo, dialogue_to_clip, pending, downloaded_clip_indices, clip_submit_times=clip_submit_times, num_submitted=num_submitted)
                if not matched:
                    # Log unmatched only once per idx
                    if not hasattr(self, '_logged_unmatched'):
                        self._logged_unmatched = set()
                    says_idx = pt.find('says in')
                    unmatched_key = f"{idx}:{pt[:50]}"
                    if says_idx >= 0 and unmatched_key not in self._logged_unmatched:
                        print(f"[{self.account_name}] idx={idx} dialogue: '{pt[says_idx+15:says_idx+75]}'", flush=True)
                        self._logged_unmatched.add(unmatched_key)
                    continue
                
                ci = matched.get('clip_index')
                
                # Skip failed containers — retry is handled by submit browser
                if hf and not hv:
                    continue
                
                # Handle video ready → DOWNLOAD
                if hv:
                    video_check = container.locator("video")
                    if video_check.count() == 0:
                        print(f"[{self.account_name}] Video at idx={idx} vanished, skipping", flush=True)
                        continue
                    
                    print(f"[{self.account_name}] ✓ Clip {ci} ready at idx={idx}, downloading...", flush=True)
                    # Log what dialogue was matched for debugging
                    matched_dialogue = matched.get('dialogue_text', '')[:80]
                    prompt_snippet = cinfo.get('promptText', '')[:80]
                    print(f"[{self.account_name}]   matched_dialogue: '{matched_dialogue}'", flush=True)
                    print(f"[{self.account_name}]   container_prompt: '{prompt_snippet}'", flush=True)
                    try:
                        scan_urls = cinfo.get('videoUrls', [])
                        result = self._download_clip_variants(container, matched, job_id, temp_dir, downloaded_videos, pre_extracted_urls=scan_urls, downloaded_urls=downloaded_urls)
                        if result:
                            downloaded_clip_indices.add(ci)
                            pending.discard(ci)
                            downloaded_count += 1
                            downloaded_this_cycle += 1
                            print(f"[{self.account_name}] ✓ Clip {ci} downloaded! ({downloaded_count}/{num_clips})", flush=True)
                        
                        try:
                            self.page.keyboard.press("Escape")
                            time.sleep(0.3)
                        except:
                            pass
                        
                    except Exception as e:
                        print(f"[{self.account_name}] Error downloading clip {ci}: {e}", flush=True)
                        try:
                            self.page.keyboard.press("Escape")
                        except:
                            pass
            
            if downloaded_this_cycle > 0:
                print(f"[{self.account_name}] Downloaded {downloaded_this_cycle} clip(s) this cycle", flush=True)
                empty_cycles = 0  # Reset on any progress
            else:
                empty_cycles += 1
                if empty_cycles >= MAX_EMPTY_CYCLES and len(pending) > 0:
                    print(f"[{self.account_name}] ⚠️ {empty_cycles} consecutive empty scan cycles with {len(pending)} pending clips — giving up on main project downloads", flush=True)
                    break
            
            time.sleep(5)
        
        # ══════════════════════════════════════════════════════
        # DEEP SCAN: Final check — refresh + scan for any missed clips
        # ══════════════════════════════════════════════════════
        missing = []
        for clip in clips:
            ci = clip.get('clip_index')
            if ci not in downloaded_clip_indices and ci not in failed_clips:
                if allowed_clips_for_job is None or ci in allowed_clips_for_job:
                    if clip_submit_times.get(ci):
                        missing.append(clip)
        
        if missing:
            missing_indices = [c.get('clip_index') for c in missing]
            print(f"\n[{self.account_name}] ═══ DEEP SCAN: {len(missing)} clip(s) missing: {missing_indices} ═══", flush=True)
            
            missing_pending = set(missing_indices)
            
            for attempt in range(3):
                if not missing_pending:
                    break
                
                print(f"[{self.account_name}] Deep scan attempt {attempt + 1}/3...", flush=True)
                self._refresh_and_verify(project_url)
                time.sleep(2)
                
                containers = self._scan_all_containers(max_index=num_clips + 5)
                
                for cinfo in containers:
                    if not missing_pending:
                        break
                    
                    matched = self._match_container_to_clip(cinfo, dialogue_to_clip, missing_pending, downloaded_clip_indices, clip_submit_times=clip_submit_times, num_submitted=len(clip_submit_times) if clip_submit_times else 0)
                    if not matched:
                        continue
                    
                    ci = matched.get('clip_index')
                    
                    if cinfo.get('hasFailed'):
                        print(f"[{self.account_name}] Deep scan: Clip {ci} FAILED", flush=True)
                        failed_clips.add(ci)
                        missing_pending.discard(ci)
                        update_clip_status(matched.get('id'), 'failed', error_message="Generation failed (deep scan)")
                    elif cinfo.get('hasVideo'):
                        print(f"[{self.account_name}] Deep scan: Found clip {ci} at idx={cinfo['dataIndex']}!", flush=True)
                        container = self.page.locator(f"div[data-index='{cinfo['dataIndex']}']")
                        if container.count() == 0:
                            print(f"[{self.account_name}] Deep scan: Container disappeared, skipping", flush=True)
                            continue
                        try:
                            container.first.scroll_into_view_if_needed(timeout=2000)
                            time.sleep(0.5)
                        except:
                            pass
                        try:
                            deep_scan_urls = cinfo.get('videoUrls', [])
                            result = self._download_clip_variants(container, matched, job_id, temp_dir, downloaded_videos, pre_extracted_urls=deep_scan_urls, downloaded_urls=downloaded_urls)
                            if result:
                                downloaded_clip_indices.add(ci)
                                missing_pending.discard(ci)
                                downloaded_count += 1
                                print(f"[{self.account_name}] Deep scan: Clip {ci} downloaded!", flush=True)
                        except Exception as e:
                            print(f"[{self.account_name}] Deep scan download error for clip {ci}: {e}", flush=True)
                
                if missing_pending:
                    time.sleep(10)
            
            if not missing_pending:
                print(f"[{self.account_name}] Deep scan: All clips found!", flush=True)
            else:
                print(f"[{self.account_name}] Deep scan complete, still missing: {sorted(missing_pending)}", flush=True)
        
        # ══════════════════════════════════════════════════════
        # STUCK CLIP RETRY: clips submitted but never became ready/failed
        # ══════════════════════════════════════════════════════
        stuck_clips = []
        for clip in clips:
            ci = clip.get('clip_index')
            if ci in downloaded_clip_indices or ci in failed_clips:
                continue
            if allowed_clips_for_job is not None and ci not in allowed_clips_for_job:
                continue
            submit_time = clip_submit_times.get(ci)
            if submit_time and (datetime.now() - submit_time).total_seconds() > STUCK_TIMEOUT:
                stuck_clips.append(clip)
                print(f"[{self.account_name}] ⚠️ Clip {ci} stuck ({(datetime.now() - submit_time).total_seconds():.0f}s since submit)", flush=True)
        
        if stuck_clips:
            print(f"\n[{self.account_name}] ═══ RETRY: {len(stuck_clips)} stuck clip(s) ═══", flush=True)
            
            retried_this_round = 0
            for stuck_clip in stuck_clips:
                ci = stuck_clip.get('clip_index')
                clip_id = stuck_clip.get('id')
                
                # Cap retries: after MAX_STUCK_RETRIES, fail remaining immediately
                if retried_this_round >= MAX_STUCK_RETRIES:
                    print(f"[{self.account_name}] Clip {ci} — skipping retry (hit {MAX_STUCK_RETRIES} retry cap), marking FAILED", flush=True)
                    failed_clips.add(ci)
                    if clip_id:
                        update_clip_status(clip_id, 'failed', error_message="Generation timed out — please retry manually from UI")
                    continue
                
                # Only retry once
                if not hasattr(self, '_retried_clips'):
                    self._retried_clips = set()
                
                if ci in self._retried_clips:
                    print(f"[{self.account_name}] Clip {ci} already retried — marking FAILED", flush=True)
                    failed_clips.add(ci)
                    if clip_id:
                        update_clip_status(clip_id, 'failed', error_message="Timed out after retry — please retry manually from UI")
                    continue
                
                self._retried_clips.add(ci)
                retried_this_round += 1
                print(f"[{self.account_name}] Retrying clip {ci} ({retried_this_round}/{MAX_STUCK_RETRIES})...", flush=True)
                
                try:
                    clip_data = None
                    if clips_data:
                        for cd in clips_data:
                            if cd.get('clip_index') == ci:
                                clip_data = cd
                                break
                    
                    if not clip_data:
                        print(f"[{self.account_name}] ❌ No clip data for clip {ci}", flush=True)
                        failed_clips.add(ci)
                        if clip_id:
                            update_clip_status(clip_id, 'failed', error_message="Retry failed — no clip data")
                        continue
                    
                    retry_result = self._retry_single_clip(clip_data, job_id, temp_dir, downloaded_videos, dialogue_to_clip, downloaded_urls=downloaded_urls)
                    if retry_result:
                        downloaded_clip_indices.add(ci)
                        downloaded_count += 1
                        print(f"[{self.account_name}] ✓ Clip {ci} retry successful!", flush=True)
                    else:
                        print(f"[{self.account_name}] ❌ Clip {ci} retry failed", flush=True)
                        failed_clips.add(ci)
                        if clip_id:
                            update_clip_status(clip_id, 'failed', error_message="Generation timed out — please retry manually from UI")
                
                except Exception as e:
                    print(f"[{self.account_name}] ❌ Error retrying clip {ci}: {e}", flush=True)
                    failed_clips.add(ci)
                    if clip_id:
                        update_clip_status(clip_id, 'failed', error_message=f"Retry error: {str(e)[:100]}")
        
        return downloaded_count

    # ================================================================
    # LEGACY METHOD STUBS (kept for backward compatibility)
    # These redirect to the new unified _download_loop via _download_from_project_dynamic
    # ================================================================

    def _download_from_project(self, project_url, clips, job_id, temp_dir, clip_submit_times=None, clips_data=None, is_redo=False):
        """Legacy wrapper — redirects to _download_from_project_dynamic.
        
        Args:
            project_url: URL of the Flow project
            clips: List of clips to download
            job_id: Job ID
            temp_dir: Temp directory for downloads
            clip_submit_times: Dict mapping clip_index -> datetime of submission
            clips_data: List of clip data dicts for failure handling
            is_redo: Whether this is a redo download
        """
        # Legacy redirect — uses the new unified download path
        result, _ = self._download_from_project_dynamic(
            project_url, clips, job_id, temp_dir,
            clip_submit_times=clip_submit_times,
            clips_data=clips_data,
            is_redo=is_redo,
        )
        return result
    
    def _retry_single_clip(self, clip_data, job_id, temp_dir, downloaded_videos, dialogue_to_clip, downloaded_urls=None):
        """
        Retry a single stuck clip by creating a new project and resubmitting.
        
        Args:
            clip_data: Dict with clip info (prompt, start_frame, end_frame, etc.)
            job_id: Job ID
            temp_dir: Temp directory for downloads
            downloaded_videos: Shared dict for continue mode
            dialogue_to_clip: Dialogue mapping for download matching
        
        Returns:
            True if clip was successfully downloaded, False otherwise
        """
        clip_index = clip_data.get('clip_index')
        prompt = clip_data.get('prompt', '')
        start_frame_path = clip_data.get('start_frame')
        end_frame_path = clip_data.get('end_frame')
        
        # Verify frame files exist — re-download if missing
        if start_frame_path and not os.path.exists(start_frame_path):
            print(f"[{self.account_name}-RETRY] ⚠️ Start frame missing: {start_frame_path}", flush=True)
            url = clip_data.get('start_frame_url')
            if url:
                start_frame_path = download_frame(url, start_frame_path)
        
        if end_frame_path and not os.path.exists(end_frame_path):
            print(f"[{self.account_name}-RETRY] ⚠️ End frame missing: {end_frame_path}", flush=True)
            url = clip_data.get('end_frame_url')
            if url:
                end_frame_path = download_frame(url, end_frame_path)
        
        RETRY_TIMEOUT = 180  # 3 minutes for retry attempt
        
        try:
            print(f"[{self.account_name}-RETRY] Creating new project for clip {clip_index}...", flush=True)
            
            # Navigate to Flow home (SPA — preserve reCAPTCHA)
            spa_navigate_to_flow_home(self.page, f"{self.account_name}-RETRY")
            human_delay(2, 4)  # Match main flow post-navigation wait
            
            ensure_logged_into_flow(self.page, f"{self.account_name}-RETRY")
            check_and_dismiss_popup(self.page)
            
            # Human-like "looking around" (match main flow exactly)
            human_mouse_move(self.page)
            human_delay(1, 2)
            scroll_randomly(self.page)
            human_delay(0.5, 1)
            
            # Click "New project" button
            dismiss_create_with_flow(self.page, f"{self.account_name}-RETRY")
            human_click_element(self.page, "button:has-text('New project'), button:has(i:text('add_2')), button.sc-a38764c7-0", f"[{self.account_name}-RETRY] New project button")
            human_delay(2, 3)  # Match main flow post-click wait
            
            # Wait for project URL (match main flow: wait_for_url + fallback poll)
            try:
                self.page.wait_for_url("**/project/**", timeout=30000)
            except:
                print(f"[{self.account_name}-RETRY] wait_for_url timed out, polling...", flush=True)
                for _ in range(15):
                    time.sleep(1)
                    if "/project/" in self.page.url:
                        break
            
            time.sleep(2)
            
            if "/project/" not in self.page.url:
                print(f"[{self.account_name}-RETRY] ❌ Failed to create project", flush=True)
                return False
            
            retry_project_url = self.page.url
            print(f"[{self.account_name}-RETRY] ✓ Created retry project: {retry_project_url}", flush=True)
            human_delay(1, 2)  # Match main flow post-creation wait
            check_and_dismiss_popup(self.page)
            ensure_videos_tab_selected(self.page)
            
            # Submit clip using the same method as the main flow
            s_path = start_frame_path if (start_frame_path and os.path.exists(start_frame_path)) else None
            e_path = end_frame_path if (end_frame_path and os.path.exists(end_frame_path)) else None
            pre_generate_tile_count = get_tile_count_at_index0(self.page)
            if not rebuild_clip(self.page, s_path, e_path, prompt, is_first_clip=True, context=f"[{self.account_name}-RETRY]"):
                print(f"[{self.account_name}-RETRY] ❌ Failed to submit clip", flush=True)
                return False
            
            submit_time = datetime.now()
            
            # Check for immediate failure
            if check_recent_clip_failure(self.page, data_index=1, clip_num=clip_index, old_tile_ids=pre_generate_tile_count):
                print(f"[{self.account_name}-RETRY] ❌ Clip {clip_index} failed immediately in retry", flush=True)
                return False
            
            print(f"[{self.account_name}-RETRY] Waiting for generation (up to {RETRY_TIMEOUT}s)...", flush=True)
            
            # Wait for generation to complete
            start_wait = datetime.now()
            while (datetime.now() - start_wait).total_seconds() < RETRY_TIMEOUT:
                time.sleep(10)
                
                # Refresh and check for video or failure
                self.page.reload(timeout=30000)
                time.sleep(3)
                check_and_dismiss_popup(self.page)
                ensure_videos_tab_selected(self.page)
                ensure_batch_view_mode(self.page, f"[{self.account_name}-RegenRefresh]")
                
                # Check for video at data-index 1
                container = self.page.locator("div[data-index='1']")
                if container.count() > 0:
                    # Check for failure
                    has_failed = self.page.evaluate(r"""
                        () => {
                            const container = document.querySelector("div[data-index='1']");
                            if (!container) return false;
                            const text = container.innerText || '';
                            const hasPercentage = /\d+%/.test(text);
                            if (hasPercentage) return false;
                            return (text.includes('Failed Generation') || text.includes('Failed') || text.includes('Error'));
                        }
                    """)
                    
                    if has_failed:
                        print(f"[{self.account_name}-RETRY] ❌ Clip {clip_index} failed during retry", flush=True)
                        return False
                    
                    # Check for video
                    video = container.locator("video")
                    if video.count() > 0:
                        print(f"[{self.account_name}-RETRY] ✓ Video ready, downloading...", flush=True)
                        
                        # Create a clip dict for download
                        retry_clip = {
                            'clip_index': clip_index,
                            'id': clip_data.get('id'),
                            'generation_attempt': clip_data.get('generation_attempt', 1),
                        }
                        
                        # Download the clip
                        result = self._download_clip_variants(container, retry_clip, job_id, temp_dir, downloaded_videos, downloaded_urls=downloaded_urls)
                        return result
                
                elapsed = (datetime.now() - start_wait).total_seconds()
                print(f"[{self.account_name}-RETRY] Still generating... ({elapsed:.0f}s)", flush=True)
            
            # Timeout
            print(f"[{self.account_name}-RETRY] ❌ Retry timed out after {RETRY_TIMEOUT}s", flush=True)
            return False
            
        except Exception as e:
            print(f"[{self.account_name}-RETRY] ❌ Error during retry: {e}", flush=True)
            import traceback
            traceback.print_exc()
            return False
    
    def _download_clip_variants(self, container, clip, job_id, temp_dir, downloaded_videos=None, pre_extracted_urls=None, downloaded_urls=None):
        """Download all variants of a clip from a container.
        
        APPROACH: Three download strategies tried in order:
        1. Pre-extracted URLs from scan phase (fastest — no DOM interaction needed)
        2. Extract signed GCS URLs from <video src> attributes (reliable — direct HTTP)
        3. UI-click fallback: hover → download icon → "Original size (720p)" (last resort)
        
        Flow serves videos as signed GCS URLs:
            https://storage.googleapis.com/ai-sandbox-videofx/video/{uuid}?GoogleAccessId=...&Signature=...
        These are valid ~24h and downloadable with simple HTTP GET.
        
        Falls back through strategies if earlier ones fail.
        
        Returns True if successful, False otherwise.
        """
        clip_index = clip.get('clip_index')
        clip_id = clip.get('id')
        attempt = clip.get('generation_attempt', 1)
        print(f"[{self.account_name}] _download_clip_variants: clip_index={clip_index}, clip_id={clip_id}, attempt={attempt}", flush=True)
        
        # CRITICAL: Scroll container into view before checking children
        try:
            container.first.scroll_into_view_if_needed(timeout=3000)
            time.sleep(1)
        except Exception as scroll_err:
            print(f"[{self.account_name}] Warning: Could not scroll clip {clip_index} into view: {scroll_err}", flush=True)
        
        time.sleep(1)
        
        # Find all video elements in this container
        videos = container.locator("video")
        video_count = videos.count()
        
        # If we have pre-extracted URLs from scan phase, use count from there
        if pre_extracted_urls and len(pre_extracted_urls) > 0:
            video_count = max(video_count, len(pre_extracted_urls))
        
        if video_count == 0:
            print(f"[{self.account_name}] No videos found for clip {clip_index}", flush=True)
            return False
        
        print(f"[{self.account_name}] Found {video_count} variant(s) for clip {clip_index}", flush=True)
        
        variants_downloaded = []
        
        for v_idx in range(video_count):
            variant_num = v_idx + 1
            variant_name = f"{attempt}.{variant_num}"
            
            try:
                # ─── STRATEGY 1: Use pre-extracted URL from scan phase ───
                video_url = None
                video_elem = None  # Initialize to avoid 'not defined' in UI fallback
                
                if pre_extracted_urls and v_idx < len(pre_extracted_urls):
                    url = pre_extracted_urls[v_idx]
                    if url and (url.startswith("http") or url.startswith("/")) and "blob:" not in url:
                        video_url = url
                        print(f"[{self.account_name}] Using pre-extracted URL for variant {variant_name}: {url[:120]}", flush=True)
                
                # ─── STRATEGY 2: Extract video src URL from DOM ───
                if v_idx < videos.count():
                    video_elem = videos.nth(v_idx)
                    if not video_url:
                        try:
                            video_url = video_elem.get_attribute("src")
                            # Filter out blob: URLs — they can't be downloaded via HTTP
                            if video_url and video_url.startswith("blob:"):
                                print(f"[{self.account_name}] Video src is blob URL, will try UI fallback", flush=True)
                                video_url = None
                        except:
                            pass
                        
                        # Also try source child element
                        if not video_url:
                            try:
                                source_elem = video_elem.locator("source")
                                if source_elem.count() > 0:
                                    video_url = source_elem.first.get_attribute("src")
                                    if video_url and video_url.startswith("blob:"):
                                        video_url = None
                            except:
                                pass
                
                output_path = os.path.join(temp_dir, f"clip_{clip_index}_{attempt}.{variant_num}.mp4")
                downloaded_via_url = False
                
                # Resolve relative URLs (new Flow UI uses /fx/api/trpc/... instead of absolute GCS URLs)
                if video_url and video_url.startswith("/"):
                    from urllib.parse import urlparse
                    parsed = urlparse(self.page.url)
                    video_url = f"{parsed.scheme}://{parsed.netloc}{video_url}"
                    print(f"[{self.account_name}] Resolved relative URL for variant {variant_name}", flush=True)
                
                if video_url and video_url.startswith("http"):
                    # Dedup: skip URLs already downloaded for another clip
                    # For Flow redirect URLs, the unique part is the ?name=UUID parameter
                    # For GCS signed URLs, the path itself is unique
                    url_key = video_url  # Default: use full URL
                    if 'name=' in video_url:
                        # Extract the name parameter (UUID) as the dedup key
                        import re as _re
                        name_match = _re.search(r'[?&]name=([^&]+)', video_url)
                        if name_match:
                            url_key = name_match.group(1)
                    else:
                        url_key = video_url.split("?")[0]  # Strip signatures for GCS URLs
                    
                    if downloaded_urls is not None and url_key in downloaded_urls:
                        print(f"[{self.account_name}] ⚠️ Skipping variant {variant_name} — URL already used by another clip: {url_key[-40:]}", flush=True)
                        continue
                    
                    print(f"[{self.account_name}] Downloading clip {clip_index} variant {variant_name} via URL: {video_url[:120]}", flush=True)
                    try:
                        import requests as req
                        # Get browser cookies for authenticated download
                        cookies = self.page.context.cookies()
                        cookie_dict = {c['name']: c['value'] for c in cookies if 'google' in c.get('domain', '') or 'flow' in c.get('domain', '') or 'aisandbox' in c.get('domain', '')}
                        session = req.Session()
                        for name, value in cookie_dict.items():
                            session.cookies.set(name, value)
                        
                        resp = session.get(video_url, timeout=120, stream=True)
                        resp.raise_for_status()
                        
                        with open(output_path, 'wb') as f:
                            for chunk in resp.iter_content(chunk_size=8192):
                                f.write(chunk)
                        
                        file_size = os.path.getsize(output_path)
                        if file_size > 10000:  # Sanity check: video should be > 10KB
                            downloaded_via_url = True
                            # Register this URL so no other clip can reuse it
                            if downloaded_urls is not None:
                                downloaded_urls.add(url_key)
                            print(f"[{self.account_name}] ✓ URL download OK ({file_size:,} bytes)", flush=True)
                        else:
                            print(f"[{self.account_name}] ⚠️ URL download too small ({file_size} bytes), trying UI fallback", flush=True)
                            os.remove(output_path)
                    except Exception as url_err:
                        print(f"[{self.account_name}] URL download failed: {url_err}, trying UI fallback", flush=True)
                        if os.path.exists(output_path):
                            os.remove(output_path)
                
                # ─── FALLBACK: UI-click download ───
                if not downloaded_via_url:
                    print(f"[{self.account_name}] Using UI fallback for clip {clip_index} variant {variant_name}...", flush=True)
                    try:
                        if video_elem is None:
                            raise Exception("No video element available for UI fallback")
                        
                        # Dismiss any open menus
                        self.page.keyboard.press("Escape")
                        time.sleep(0.5)
                        
                        # Hover video to reveal controls
                        video_elem.hover(force=True)
                        time.sleep(1)
                        
                        # Find download button near this video
                        variant_containers = container.locator("div.sc-d90fd836-2.dLxTam")
                        download_btn = None
                        
                        if v_idx < variant_containers.count():
                            vc = variant_containers.nth(v_idx)
                            download_btn = vc.locator("button[aria-label='download']")
                            if download_btn.count() == 0:
                                download_btn = vc.locator("button:has(i:text('download'))")
                        
                        if not download_btn or download_btn.count() == 0:
                            all_btns = container.locator("button[aria-label='download']")
                            if all_btns.count() == 0:
                                all_btns = container.locator("button:has(i:text('download'))")
                            if v_idx < all_btns.count():
                                download_btn = all_btns.nth(v_idx)
                        
                        if download_btn and download_btn.count() > 0:
                            download_btn.first.click(force=True)
                            time.sleep(1)
                            
                            with self.page.expect_download(timeout=60000) as download_info:
                                self.page.click("text=Original size (720p)")
                            
                            download = download_info.value
                            download.save_as(output_path)
                            print(f"[{self.account_name}] ✓ UI download OK", flush=True)
                        else:
                            print(f"[{self.account_name}] No download button for variant {variant_name}", flush=True)
                            continue
                    except Exception as ui_err:
                        print(f"[{self.account_name}] UI download failed: {ui_err}", flush=True)
                        continue
                
                # ─── Upload to R2 ───
                if os.path.exists(output_path) and os.path.getsize(output_path) > 10000:
                    output_url = upload_video(output_path, job_id, clip_index, attempt=attempt, variant=variant_num)
                    
                    if output_url:
                        print(f"[{self.account_name}] ✓ Clip {clip_index} variant {variant_name} uploaded!", flush=True)
                        variants_downloaded.append(variant_name)
                        if variant_num == 1 and downloaded_videos is not None:
                            downloaded_videos[clip_index] = output_path
                    else:
                        print(f"[{self.account_name}] ⚠️ Upload failed for variant {variant_name}", flush=True)
                
                time.sleep(1)
                
            except Exception as e:
                print(f"[{self.account_name}] Error downloading variant {variant_name}: {e}", flush=True)
        
        if variants_downloaded:
            update_clip_status(clip_id, 'completed')
            print(f"[{self.account_name}] ✓ Clip {clip_index} all variants done!", flush=True)
            return True
        else:
            return False

    def _regenerate_clip_and_download(self, clip, job_id, temp_dir, attempt):
        """Regenerate a clip in a new Flow project and attempt to download it.
        
        Called when the original project has no video (silent failure).
        
        Args:
            clip: Clip data dict
            job_id: Job ID
            temp_dir: Temp directory with frame files
            attempt: Generation attempt number
        
        Returns:
            1 if successful, 0 if failed
        """
        clip_index = clip.get('clip_index', 0)
        clip_id = clip.get('id')
        
        print(f"\n[{self.account_name}] {'='*50}", flush=True)
        print(f"[{self.account_name}] REGENERATING CLIP {clip_index} IN NEW PROJECT", flush=True)
        print(f"[{self.account_name}] {'='*50}", flush=True)
        
        # Mark as generating so frontend shows "in progress"
        update_clip_status(clip_id, 'generating')
        
        try:
            # Get frame paths from temp_dir
            start_frame = os.path.join(temp_dir, f"start_{clip_index}.png")
            end_frame = os.path.join(temp_dir, f"end_{clip_index}.png")
            
            # Check if frames exist — re-download if missing
            has_start = os.path.exists(start_frame)
            has_end = os.path.exists(end_frame)
            
            if not has_start:
                url = clip.get('start_frame_url')
                if url:
                    print(f"[{self.account_name}] Re-downloading start frame...", flush=True)
                    result = download_frame(url, start_frame)
                    has_start = result is not None
            
            if not has_end:
                url = clip.get('end_frame_url')
                if url:
                    print(f"[{self.account_name}] Re-downloading end frame...", flush=True)
                    result = download_frame(url, end_frame)
                    has_end = result is not None
            
            print(f"[{self.account_name}] Start frame: {'✓' if has_start else '✗'} {start_frame}", flush=True)
            print(f"[{self.account_name}] End frame: {'✓' if has_end else '✗'} {end_frame}", flush=True)
            
            if not has_start:
                print(f"[{self.account_name}] ❌ Cannot regenerate - no start frame available", flush=True)
                update_clip_status(clip_id, 'failed', error_message="Cannot regenerate - no start frame")
                return 0
            
            # Get prompt from clip data
            prompt = clip.get('prompt', '')
            if not prompt:
                # Build prompt from dialogue if no prompt stored
                dialogue = clip.get('dialogue_text', '')
                language = clip.get('language', 'English')
                voice_profile = clip.get('voice_profile', '')
                duration = float(clip.get('duration', '8'))
                prompt = build_flow_prompt(
                    dialogue_line=dialogue,
                    language=language,
                    voice_profile=voice_profile,
                    duration=duration,
                )
            
            print(f"[{self.account_name}] Prompt: {prompt[:80]}...", flush=True)
            
            # Navigate to Flow home (SPA — preserve reCAPTCHA)
            print(f"[{self.account_name}] Navigating to Flow home...", flush=True)
            spa_navigate_to_flow_home(self.page, self.account_name)
            human_delay(2, 4)  # Match main flow post-navigation wait
            
            ensure_logged_into_flow(self.page, self.account_name)
            check_and_dismiss_popup(self.page)
            
            # Human-like "looking around" (match main flow exactly)
            human_mouse_move(self.page)
            human_delay(1, 2)
            scroll_randomly(self.page)
            human_delay(0.5, 1)
            
            # Create new project
            dismiss_create_with_flow(self.page, self.account_name)
            human_click_element(self.page, "button:has-text('New project'), button:has(i:text('add_2')), button.sc-a38764c7-0", f"[{self.account_name}] New project button")
            human_delay(2, 3)  # Match main flow post-click wait
            
            try:
                self.page.wait_for_url("**/project/**", timeout=30000)
            except:
                print(f"[{self.account_name}] wait_for_url timed out, polling...", flush=True)
                for _ in range(15):
                    time.sleep(1)
                    if "/project/" in self.page.url:
                        break
            
            time.sleep(2)
            new_project_url = self.page.url
            
            if "/project/" not in new_project_url:
                print(f"[{self.account_name}] ❌ Failed to create project - URL: {new_project_url}", flush=True)
                update_clip_status(clip_id, 'failed', error_message="Failed to create retry project")
                return 0
            
            print(f"[{self.account_name}] ✓ Created retry project: {new_project_url}", flush=True)
            human_delay(1, 2)  # Match main flow post-creation wait
            check_and_dismiss_popup(self.page)
            ensure_videos_tab_selected(self.page)
            
            # Submit clip using the same method as the main flow
            s_path = start_frame if has_start else None
            e_path = end_frame if has_end else None
            pre_generate_tile_count = get_tile_count_at_index0(self.page)
            if not rebuild_clip(self.page, s_path, e_path, prompt, is_first_clip=True, context=f"[{self.account_name}]"):
                print(f"[{self.account_name}] ❌ Failed to submit clip in retry project", flush=True)
                update_clip_status(clip_id, 'failed', error_message="Failed to submit clip in retry project")
                return 0
            
            # Check for immediate failure
            if check_recent_clip_failure(self.page, data_index=1, clip_num=clip_index, old_tile_ids=pre_generate_tile_count):
                print(f"[{self.account_name}] ⚠️ Retry generation also failed immediately!", flush=True)
                update_clip_status(clip_id, 'failed', error_message="Retry generation also failed")
                return 0
            
            # Wait for generation (CLIP_READY_WAIT seconds)
            print(f"[{self.account_name}] Waiting {CLIP_READY_WAIT}s for generation...", flush=True)
            time.sleep(CLIP_READY_WAIT)
            
            # Refresh page
            print(f"[{self.account_name}] Refreshing page...", flush=True)
            self.page.reload(timeout=30000)
            time.sleep(5)
            check_and_dismiss_popup(self.page)
            
            # Try to download (with is_retry_attempt=True to prevent infinite loop)
            print(f"[{self.account_name}] Attempting to download from retry project...", flush=True)
            result = self.download_single_clip(clip, job_id, temp_dir, data_index=0, is_retry_attempt=True)
            
            return result
            
        except Exception as e:
            print(f"[{self.account_name}] ❌ Error during regeneration: {e}", flush=True)
            import traceback
            traceback.print_exc()
            update_clip_status(clip_id, 'failed', error_message=f"Regeneration error: {str(e)}")
            return 0

    def download_single_clip(self, clip, job_id, temp_dir, data_index=0, is_retry_attempt=False):
        """Download a single clip (for redo) with all variants - finds first video container in project
        
        If no video is found after retries, will attempt to regenerate the clip in a new project
        (unless is_retry_attempt=True, which prevents infinite loops).
        """
        print(f"[{self.account_name}] Looking for redo clip (finding first video)...", flush=True)
        
        # Get generation attempt for naming
        attempt = clip.get('generation_attempt', 1)
        
        # Retry loop - sometimes the page needs more time to render videos
        max_retries = 5  # Increased from 3
        actual_index = None
        found_generating = False  # Track if we saw "Generating" status
        found_failed = False  # Track if we saw "Failed Generation" status
        
        for retry in range(max_retries):
            if retry > 0:
                wait_time = 10 if retry < 3 else 15  # Longer waits on later retries
                print(f"[{self.account_name}] Retry {retry}/{max_retries-1} - waiting {wait_time}s and refreshing...", flush=True)
                time.sleep(wait_time)
                try:
                    self.page.reload(timeout=30000)
                    time.sleep(5)  # Wait for page to settle after refresh
                    check_and_dismiss_popup(self.page)
                    ensure_videos_tab_selected(self.page)
                    ensure_batch_view_mode(self.page, f"[{self.account_name}-RedoRefresh]")
                except Exception as e:
                    print(f"[{self.account_name}] Refresh error: {e}", flush=True)
            
            # Wait a moment for video elements to render
            time.sleep(2)
            
            # Reset status flags for this retry
            found_generating = False
            found_failed = False
            
            # Find the first data-index container that has a video (skip date headers)
            for idx in range(15):
                container = self.page.locator(f"div[data-index='{idx}']")
                if container.count() > 0:
                    video = container.locator("video")
                    if video.count() > 0:
                        actual_index = idx
                        print(f"[{self.account_name}] Found video at data-index={idx}", flush=True)
                        break
                    else:
                        # No video yet — check if it's generating or failed
                        # New UI: no date headers, but container might be generating
                        container_text = ""
                        try:
                            container_text = container.inner_text(timeout=1000)
                        except:
                            pass
                        
                        if not container_text or len(container_text.strip()) < 10:
                            print(f"[{self.account_name}] Skipping data-index={idx} (empty/header)", flush=True)
                        else:
                            # Has clip section but no video - check status
                            # Check for "Generating" text
                            generating = container.locator("text=Generating")
                            if generating.count() > 0:
                                found_generating = True
                                print(f"[{self.account_name}] data-index={idx} still generating...", flush=True)
                            
                            # Check for "Failed" or "Failed Generation" text
                            try:
                                has_failed = self.page.evaluate(f"""
                                    () => {{
                                        const container = document.querySelector("div[data-index='{idx}']");
                                        if (!container) return false;
                                        const text = container.innerText || '';
                                        const hasPercentage = /\\d+%/.test(text);
                                        if (hasPercentage) return false;
                                        return (text.includes('Failed Generation') || text.includes('Failed') || text.includes('Error'));
                                    }}
                                """)
                                if has_failed:
                                    found_failed = True
                                    print(f"[{self.account_name}] data-index={idx} shows FAILED!", flush=True)
                            except:
                                pass
            
            if actual_index is not None:
                break
            
            # If we found "Failed", no point waiting more
            if found_failed:
                print(f"[{self.account_name}] Generation failed, stopping retries", flush=True)
                break
            
            # If still generating, extend retries
            if found_generating and retry >= max_retries - 2:
                print(f"[{self.account_name}] Still generating, extending retries...", flush=True)
                max_retries = min(max_retries + 2, 10)  # Extend but cap at 10
                
            if retry == 0:
                # First failure - try scrolling
                print(f"[{self.account_name}] No video found in first 15 containers, trying scroll...", flush=True)
                self.page.mouse.wheel(0, -300)
                time.sleep(2)
                for idx in range(15):
                    container = self.page.locator(f"div[data-index='{idx}']")
                    if container.count() > 0 and container.locator("video").count() > 0:
                        actual_index = idx
                        print(f"[{self.account_name}] Found video at data-index={idx} after scroll", flush=True)
                        break
                
                if actual_index is not None:
                    break
        
        if actual_index is None:
            print(f"[{self.account_name}] ❌ No video found after {max_retries} attempts", flush=True)
            
            # If this is already a retry attempt, mark as failed
            if is_retry_attempt:
                print(f"[{self.account_name}] ❌ Retry generation also failed - marking clip as failed", flush=True)
                update_clip_status(clip['id'], 'failed', error_message="No video generated after retry - Flow generation failed")
                return 0
            
            # Try to regenerate the clip in a new project
            print(f"[{self.account_name}] 🔄 Attempting to regenerate clip in new project...", flush=True)
            
            retry_result = self._regenerate_clip_and_download(clip, job_id, temp_dir, attempt)
            return retry_result
        
        container = self.page.locator(f"div[data-index='{actual_index}']")
        
        # CRITICAL: Scroll container into view before interacting
        try:
            container.first.scroll_into_view_if_needed(timeout=5000)
            time.sleep(2)
        except:
            self.page.mouse.wheel(0, 200)
            time.sleep(1)
        
        # Find ALL video elements in this container
        videos = container.locator("video")
        video_count = videos.count()
        
        if video_count == 0:
            print(f"[{self.account_name}] No video elements found in container", flush=True)
            return 0
        
        print(f"[{self.account_name}] Found {video_count} variant(s) for redo clip", flush=True)
        
        main_output_url = None
        variants_downloaded = 0
        
        for v_idx in range(video_count):
            variant_num = v_idx + 1
            variant_name = f"{attempt}.{variant_num}"
            output_path = os.path.join(temp_dir, f"clip_{clip['clip_index']}_{attempt}.{variant_num}.mp4")
            downloaded_via_url = False
            
            try:
                video_elem = videos.nth(v_idx)
                
                # ─── STRATEGY 1: Extract video src URL and download via HTTP (primary) ───
                video_url = None
                try:
                    video_url = video_elem.get_attribute("src")
                    if video_url and video_url.startswith("blob:"):
                        video_url = None
                except:
                    pass
                
                if not video_url:
                    try:
                        source_elem = video_elem.locator("source")
                        if source_elem.count() > 0:
                            video_url = source_elem.first.get_attribute("src")
                            if video_url and video_url.startswith("blob:"):
                                video_url = None
                    except:
                        pass
                
                if video_url and video_url.startswith("http"):
                    pass  # Already absolute
                elif video_url and video_url.startswith("/"):
                    # Relative URL — prepend origin from current page
                    from urllib.parse import urlparse
                    parsed = urlparse(self.page.url)
                    video_url = f"{parsed.scheme}://{parsed.netloc}{video_url}"
                    print(f"[{self.account_name}] Resolved relative URL to: {video_url[:80]}...", flush=True)
                
                if video_url and video_url.startswith("http"):
                    print(f"[{self.account_name}] Downloading redo variant {variant_name} via URL: {video_url[:120]}", flush=True)
                    try:
                        import requests as req_lib
                        # Get browser cookies for authenticated download
                        cookies = self.page.context.cookies()
                        cookie_dict = {c['name']: c['value'] for c in cookies if 'google' in c.get('domain', '') or 'flow' in c.get('domain', '')}
                        session = req_lib.Session()
                        for name, value in cookie_dict.items():
                            session.cookies.set(name, value)
                        resp = session.get(video_url, timeout=120, stream=True, allow_redirects=True)
                        resp.raise_for_status()
                        with open(output_path, 'wb') as f:
                            for chunk in resp.iter_content(chunk_size=8192):
                                f.write(chunk)
                        
                        file_size = os.path.getsize(output_path)
                        if file_size > 10000:
                            downloaded_via_url = True
                            print(f"[{self.account_name}] ✓ URL download OK ({file_size:,} bytes)", flush=True)
                        else:
                            print(f"[{self.account_name}] ⚠️ URL download too small ({file_size} bytes), trying UI fallback", flush=True)
                            os.remove(output_path)
                    except Exception as url_err:
                        print(f"[{self.account_name}] URL download failed: {url_err}, trying UI fallback", flush=True)
                        if os.path.exists(output_path):
                            os.remove(output_path)
                
                # ─── STRATEGY 2: UI-click fallback (hover → download → 720p) ───
                if not downloaded_via_url:
                    print(f"[{self.account_name}] Using UI fallback for redo variant {variant_name}...", flush=True)
                    try:
                        self.page.keyboard.press("Escape")
                        time.sleep(0.5)
                        
                        # Scroll video into view and hover
                        video_elem.scroll_into_view_if_needed(timeout=3000)
                        time.sleep(0.5)
                        video_elem.hover(force=True)
                        time.sleep(1)
                        
                        # Find download button
                        variant_containers = container.locator("div.sc-d90fd836-2.dLxTam")
                        download_btn = None
                        
                        if v_idx < variant_containers.count():
                            vc = variant_containers.nth(v_idx)
                            download_btn = vc.locator("button[aria-label='download']")
                            if download_btn.count() == 0:
                                download_btn = vc.locator("button:has(i:text('download'))")
                        
                        if not download_btn or download_btn.count() == 0:
                            all_btns = container.locator("button[aria-label='download']")
                            if all_btns.count() == 0:
                                all_btns = container.locator("button:has(i:text('download'))")
                            if v_idx < all_btns.count():
                                download_btn = all_btns.nth(v_idx)
                        
                        if download_btn and download_btn.count() > 0:
                            download_btn.first.click(force=True)
                            time.sleep(1)
                            
                            with self.page.expect_download(timeout=60000) as download_info:
                                self.page.click("text=Original size (720p)")
                            
                            download = download_info.value
                            download.save_as(output_path)
                            print(f"[{self.account_name}] ✓ UI download OK", flush=True)
                        else:
                            print(f"[{self.account_name}] No download button for variant {variant_name}", flush=True)
                            continue
                    except Exception as ui_err:
                        print(f"[{self.account_name}] UI download failed: {ui_err}", flush=True)
                        continue
                
                # ─── Upload to R2 ───
                if os.path.exists(output_path) and os.path.getsize(output_path) > 10000:
                    output_url = upload_video(output_path, job_id, clip['clip_index'], attempt=attempt, variant=variant_num)
                    
                    if variant_num == 1:
                        main_output_url = output_url
                    
                    variants_downloaded += 1
                    print(f"[{self.account_name}] ✓ Redo variant {variant_name} uploaded!", flush=True)
                
                time.sleep(1)
                
            except Exception as ve:
                print(f"[{self.account_name}] Error downloading variant {variant_name}: {ve}", flush=True)
        
        # Update clip status with main variant URL
        if main_output_url:
            update_clip_status(clip['id'], 'completed', output_url=main_output_url)
            print(f"[{self.account_name}] ✓ Redo clip all variants done! ({variants_downloaded} variants)", flush=True)
            return 1
        elif variants_downloaded > 0:
            update_clip_status(clip['id'], 'completed')
            return 1
        else:
            print(f"[{self.account_name}] No variants downloaded", flush=True)
            return 0
    
    def stop(self):
        self.stop_flag.set()
        self.download_queue.put(None)


def click_frame_and_upload(page, image_path, is_end_frame=False, context=""):
    """Click a frame button, open dialog, upload file. Used for individual frame uploads."""
    prefix = f"{context} " if context else ""
    frame_name = "END frame" if is_end_frame else "START frame"
    
    check_and_dismiss_popup(page)
    
    frame_btns = page.locator('div.sc-8f31d1ba-1[aria-haspopup="dialog"]')
    if frame_btns.count() == 0:
        frame_btns = page.locator('div.sc-8f31d1ba-1')
    
    btn_count = frame_btns.count()
    idx = 1 if is_end_frame else 0
    
    if idx >= btn_count:
        time.sleep(2)
        frame_btns = page.locator('div.sc-8f31d1ba-1[aria-haspopup="dialog"]')
        btn_count = frame_btns.count()
    
    # If requested index not available but buttons exist, use the available one
    if idx >= btn_count and btn_count > 0:
        print(f"{prefix}⚠️ {frame_name} not at idx {idx}, using idx 0 ({btn_count} button(s))", flush=True)
        idx = 0
    elif btn_count == 0:
        print(f"{prefix}⚠️ No frame buttons available for {frame_name}", flush=True)
        return
    
    frame_btns.nth(idx).wait_for(state="visible", timeout=5000)
    human_click_locator(page, frame_btns.nth(idx), f"{prefix}{frame_name} button (idx {idx})")
    print(f"{prefix}\u2713 Clicked {frame_name} button (idx {idx}/{btn_count})", flush=True)
    time.sleep(1)
    upload_frame(page, image_path, frame_name)


def click_frame_and_upload_with_policy_check(page, image_path, is_end_frame=False, context=""):
    """Click a frame button, upload file, and wait for uploadImage policy check.
    
    Returns:
        (True, None) if upload succeeded
        (False, 'policy') if rejected by policy (should blacklist)
        (False, 'no_buttons') if no frame buttons available (should NOT blacklist)
    """
    prefix = f"{context} " if context else ""
    frame_name = "END frame" if is_end_frame else "START frame"
    which = 'end' if is_end_frame else 'start'
    
    check_and_dismiss_popup(page)
    
    frame_selector = 'div.sc-8f31d1ba-1[aria-haspopup="dialog"]'
    frame_btns = page.locator(frame_selector)
    if frame_btns.count() == 0:
        frame_btns = page.locator('div.sc-8f31d1ba-1')
    
    btn_count = frame_btns.count()
    idx = 1 if is_end_frame else 0
    
    if idx >= btn_count:
        time.sleep(2)
        frame_btns = page.locator(frame_selector)
        btn_count = frame_btns.count()
    
    # If requested index not available but buttons exist, use the available one
    # This handles: after START upload consumed its button, END is now at idx 0
    if idx >= btn_count and btn_count > 0:
        print(f"{prefix}⚠️ {frame_name} button not at idx {idx}, using idx 0 (only {btn_count} button(s))", flush=True)
        idx = 0
    elif btn_count == 0:
        print(f"{prefix}⚠️ No frame buttons available for {frame_name} (not a policy rejection)", flush=True)
        return (False, 'no_buttons')
    
    frame_btns.nth(idx).wait_for(state="visible", timeout=5000)
    human_click_locator(page, frame_btns.nth(idx), f"{prefix}{frame_name} button (idx {idx})")
    print(f"{prefix}✓ Clicked {frame_name} button (idx {idx}/{btn_count})", flush=True)
    time.sleep(1)
    
    # Start network monitor BEFORE upload
    monitor = FramePolicyMonitor(page)
    monitor.start()
    
    # Upload the frame
    upload_frame(page, image_path, frame_name)
    
    # Wait for uploadImage response (up to 35s)
    print(f"{prefix}Waiting for {frame_name} policy check...", flush=True)
    btn_gone = False
    for w in range(35):
        time.sleep(1)
        if w % random.randint(3, 6) == 0:
            try:
                human_mouse_move(page)
            except:
                pass
        
        if monitor.is_rejected():
            monitor.stop()
            print(f"{prefix}⚠️ {frame_name} REJECTED by policy (network)!", flush=True)
            try:
                page.keyboard.press("Escape")
                time.sleep(0.5)
            except:
                pass
            return (False, 'policy')
        
        if not btn_gone:
            remaining = page.locator(frame_selector).count()
            if remaining < btn_count:
                print(f"{prefix}✓ {frame_name} button gone ({w+1}s), waiting for uploadImage response...", flush=True)
                btn_gone = True
        
        if monitor.is_resolved() and not monitor.is_rejected():
            print(f"{prefix}✓ {frame_name} passed policy check ({w+1}s)", flush=True)
            break
    
    monitor.stop()
    return (True, None)


def upload_both_frames(page, start_image, end_image, context=""):
    """Upload START and END frames with human-like behavior.
    
    Correct flow (confirmed via browser console testing):
    1. Click frame div -> dialog opens with upload button and gallery
    2. Click the upload button INSIDE the dialog -> native file picker opens
    3. Pick file -> image uploads and gets assigned to THAT frame
    4. Frame button (div.sc-8f31d1ba-1) DISAPPEARS from DOM when upload completes
    5. Only remaining button is the other frame
    
    We upload START first (nth(0)), wait for its button to disappear,
    then the only remaining button is END -> click it -> upload.
    """
    prefix = f"{context} " if context else ""
    
    frame_selector = 'div.sc-8f31d1ba-1[aria-haspopup="dialog"]'
    
    # --- Upload START frame ---
    if start_image:
        check_and_dismiss_popup(page)
        btn_count = page.locator(frame_selector).count()
        print(f"{prefix}Frame buttons: {btn_count}", flush=True)
        
        # Human: look around before clicking
        human_look_around(page)
        time.sleep(random.uniform(0.3, 0.8))
        
        # Human: move mouse toward the frame button, then click
        start_btn = page.locator(frame_selector).first
        box = start_btn.bounding_box()
        if box:
            human_mouse_move_to(page, box['x'] + box['width']/2, box['y'] + box['height']/2)
            time.sleep(random.uniform(0.1, 0.3))
        human_click_at(page) if box else start_btn.click(timeout=5000)
        print(f"{prefix}\u2713 Clicked START frame button", flush=True)
        time.sleep(random.uniform(0.8, 1.5))
        
        # Click the upload button INSIDE the dialog
        dialog = page.locator('[role="dialog"]').first
        dialog.wait_for(state="visible", timeout=5000)
        upload_btn = find_dialog_upload_button(dialog)
        upload_btn.wait_for(state="visible", timeout=3000)
        
        # Human: small pause before clicking upload
        time.sleep(random.uniform(0.3, 0.7))
        
        # Use file chooser triggered by the upload button
        try:
            with page.expect_file_chooser(timeout=5000) as fc_info:
                human_click_for_file_chooser(page, upload_btn)
            # Simulate human browsing the file picker (2-5 seconds)
            time.sleep(random.uniform(2, 5))
            fc_info.value.set_files(start_image)
        except:
            # Fallback: try set_input_files
            time.sleep(random.uniform(2, 4))
            page.locator("input[type='file']").first.set_input_files(start_image)
        print(f"{prefix}\u2713 START frame file set", flush=True)
        
        # Wait for START button to disappear from DOM (upload complete)
        print(f"{prefix}Waiting for START upload...", flush=True)
        for w in range(25):
            time.sleep(1)
            # Occasional mouse movement while waiting (human fidgeting)
            if w % random.randint(3, 6) == 0:
                try:
                    human_mouse_move(page)
                except:
                    pass
            remaining = page.locator(frame_selector).count()
            if remaining < btn_count:
                print(f"{prefix}\u2713 START uploaded ({w+1}s), {remaining} button(s) remaining", flush=True)
                break
        else:
            print(f"{prefix}\u26a0 START not confirmed after 25s", flush=True)
        
        # Human: pause after upload completes, like looking at the result
        time.sleep(random.uniform(1.0, 2.5))
        human_look_around(page)
    
    # --- Upload END frame ---
    if end_image:
        check_and_dismiss_popup(page)
        remaining = page.locator(frame_selector).count()
        print(f"{prefix}Frame buttons remaining: {remaining}", flush=True)
        
        if remaining == 0:
            print(f"{prefix}\u26a0 No frame buttons remaining for END frame", flush=True)
            return
        
        # Human: move mouse around before clicking END frame
        time.sleep(random.uniform(0.5, 1.2))
        
        # The only remaining button IS the END frame
        end_btn = page.locator(frame_selector).first
        box = end_btn.bounding_box()
        if box:
            human_mouse_move_to(page, box['x'] + box['width']/2, box['y'] + box['height']/2)
            time.sleep(random.uniform(0.1, 0.3))
        human_click_at(page) if box else end_btn.click(timeout=5000)
        print(f"{prefix}\u2713 Clicked END frame button", flush=True)
        time.sleep(random.uniform(0.8, 1.5))
        
        # Click the upload button INSIDE the dialog
        dialog = page.locator('[role="dialog"]').first
        dialog.wait_for(state="visible", timeout=5000)
        upload_btn = find_dialog_upload_button(dialog)
        upload_btn.wait_for(state="visible", timeout=3000)
        
        # Human: small pause before clicking upload
        time.sleep(random.uniform(0.3, 0.7))
        
        try:
            with page.expect_file_chooser(timeout=5000) as fc_info:
                human_click_for_file_chooser(page, upload_btn)
            # Simulate human browsing the file picker (2-5 seconds)
            time.sleep(random.uniform(2, 5))
            fc_info.value.set_files(end_image)
        except:
            time.sleep(random.uniform(2, 4))
            page.locator("input[type='file']").first.set_input_files(end_image)
        print(f"{prefix}\u2713 END frame file set", flush=True)
        
        # Wait for END upload
        print(f"{prefix}Waiting for END upload...", flush=True)
        for w in range(25):
            time.sleep(1)
            # Occasional mouse movement while waiting (human fidgeting)
            if w % random.randint(3, 6) == 0:
                try:
                    human_mouse_move(page)
                except:
                    pass
            remaining = page.locator(frame_selector).count()
            if remaining == 0:
                print(f"{prefix}\u2713 END uploaded ({w+1}s), 0 buttons remaining", flush=True)
                break
        else:
            print(f"{prefix}\u26a0 END not confirmed after 25s", flush=True)
        
        # Human: pause after second upload
        time.sleep(random.uniform(0.8, 1.5))
    
    print(f"{prefix}\u2713 Both frames uploaded", flush=True)


class FramePolicyMonitor:
    """Monitor uploadImage network responses for policy rejections.
    
    Intercepts responses to the uploadImage endpoint and checks for
    PUBLIC_ERROR_PROMINENT_PEOPLE_UPLOAD in the response body.
    
    Also tracks successful uploads so callers can distinguish between
    "still waiting" vs "upload succeeded" vs "upload rejected".
    
    The uploadImage request can take 20-30 seconds to return.
    """
    
    def __init__(self, page):
        self.page = page
        self.rejected = False
        self.succeeded = False
        self.error_reason = None
        self._handler = None
    
    def _on_response(self, response):
        try:
            url = response.url
            if 'uploadImage' not in url and 'uploadimage' not in url.lower():
                return
            
            status = response.status
            if status >= 400:
                try:
                    body = response.text()
                    if 'PROMINENT_PEOPLE' in body or 'prominent_people' in body.lower():
                        self.rejected = True
                        self.error_reason = 'PUBLIC_ERROR_PROMINENT_PEOPLE_UPLOAD'
                        print(f"[PolicyMonitor] ⚠️ uploadImage REJECTED: {self.error_reason}", flush=True)
                    else:
                        print(f"[PolicyMonitor] uploadImage returned {status} (not policy)", flush=True)
                except:
                    pass
            elif status == 200:
                self.succeeded = True
                print(f"[PolicyMonitor] ✓ uploadImage succeeded (200)", flush=True)
        except:
            pass
    
    def start(self):
        self.rejected = False
        self.succeeded = False
        self.error_reason = None
        self._handler = self._on_response
        self.page.on("response", self._handler)
    
    def is_rejected(self):
        """Returns True if frame was rejected by policy."""
        return self.rejected
    
    def is_resolved(self):
        """Returns True if uploadImage has responded (success or rejection)."""
        return self.rejected or self.succeeded
    
    def check(self):
        """Legacy — returns True if rejected."""
        return self.rejected
    
    def stop(self):
        if self._handler:
            try:
                self.page.remove_listener("response", self._handler)
            except:
                pass
            self._handler = None


def build_image_pool(clips):
    """Build a pool of unique images from all clips' frame keys.
    
    Returns:
        dict: {frame_key: {'local_path': str, 'url': str}} for each unique image
        list: ordered list of unique frame_keys
    """
    pool = {}
    ordered_keys = []
    
    for clip in clips:
        for prefix in ['start', 'end']:
            key = clip.get(f'{prefix}_frame_key')
            if key and key not in pool:
                pool[key] = {
                    'local_path': clip.get(f'{prefix}_frame_local'),
                    'url': clip.get(f'{prefix}_frame_url'),
                }
                ordered_keys.append(key)
    
    return pool, ordered_keys


def get_next_available_image(current_key, ordered_keys, blacklisted_keys):
    """Find the next non-blacklisted image from the pool.
    
    Args:
        current_key: The rejected frame key
        ordered_keys: Ordered list of all unique frame keys
        blacklisted_keys: Set of blacklisted frame keys
        
    Returns:
        Next available frame_key, or None if all blacklisted
    """
    if not ordered_keys:
        return None
    
    try:
        current_idx = ordered_keys.index(current_key)
    except ValueError:
        current_idx = 0
    
    for offset in range(1, len(ordered_keys) + 1):
        candidate_idx = (current_idx + offset) % len(ordered_keys)
        candidate = ordered_keys[candidate_idx]
        if candidate not in blacklisted_keys:
            return candidate
    
    return None  # All images blacklisted


def reassign_clip_frames(clips, clip_start_idx, blacklisted_keys, image_pool, ordered_keys):
    """Reassign frames for current clip and cascade to all subsequent clips.
    
    Maintains the end→start chain: each clip's end frame = next clip's start frame.
    Skips blacklisted images.
    
    Args:
        clips: List of all clip dicts
        clip_start_idx: Index in clips list to start reassigning from
        blacklisted_keys: Set of blacklisted frame keys
        image_pool: {frame_key: {'local_path': str, 'url': str}}
        ordered_keys: Ordered list of unique frame keys
        
    Returns:
        Number of clips that couldn't be assigned (all images blacklisted)
    """
    available_keys = [k for k in ordered_keys if k not in blacklisted_keys]
    
    if not available_keys:
        print(f"[FrameReassign] ❌ ALL images blacklisted! Cannot reassign.", flush=True)
        return len(clips) - clip_start_idx
    
    failed_count = 0
    
    for i in range(clip_start_idx, len(clips)):
        clip = clips[i]
        old_start = clip.get('start_frame_key')
        old_end = clip.get('end_frame_key')
        
        # For the first reassigned clip, pick based on what's available
        if i == clip_start_idx:
            # Start frame: use first available that's different from blacklisted
            new_start = old_start if old_start not in blacklisted_keys else available_keys[0]
        else:
            # Start frame = previous clip's end frame (maintain chain)
            prev_end = clips[i-1].get('end_frame_key')
            new_start = prev_end if prev_end not in blacklisted_keys else available_keys[0]
        
        # End frame: pick a different available image if possible
        new_end = old_end
        if new_end in blacklisted_keys:
            # Find an available key different from start if possible
            for k in available_keys:
                if k != new_start:
                    new_end = k
                    break
            else:
                # Only one image available, use it for both
                new_end = available_keys[0]
        
        if new_start in blacklisted_keys or new_end in blacklisted_keys:
            failed_count += 1
            continue
        
        # Update clip data
        if new_start != old_start:
            clip['start_frame_key'] = new_start
            clip['start_frame_local'] = image_pool[new_start]['local_path']
            clip['start_frame_url'] = image_pool[new_start]['url']
            print(f"[FrameReassign] Clip {clip['clip_index']}: start {os.path.basename(str(old_start))} → {os.path.basename(str(new_start))}", flush=True)
        
        if new_end != old_end:
            clip['end_frame_key'] = new_end
            clip['end_frame_local'] = image_pool[new_end]['local_path']
            clip['end_frame_url'] = image_pool[new_end]['url']
            print(f"[FrameReassign] Clip {clip['clip_index']}: end {os.path.basename(str(old_end))} → {os.path.basename(str(new_end))}", flush=True)
    
    return failed_count



def upload_both_frames_with_policy_check(page, start_image, end_image, context=""):
    """Upload both frames with network-level policy error detection.
    
    Intercepts uploadImage API responses for PUBLIC_ERROR_PROMINENT_PEOPLE_UPLOAD.
    
    Returns:
        (True, None) if both uploaded successfully
        (False, 'start') if start frame rejected
        (False, 'end') if end frame rejected
    """
    prefix = f"{context} " if context else ""
    
    frame_selector = 'div.sc-8f31d1ba-1[aria-haspopup="dialog"]'
    monitor = FramePolicyMonitor(page)
    
    # --- Upload START frame ---
    if start_image:
        check_and_dismiss_popup(page)
        btn_count = page.locator(frame_selector).count()
        print(f"{prefix}Frame buttons: {btn_count}", flush=True)
        
        human_look_around(page)
        time.sleep(random.uniform(0.3, 0.8))
        
        start_btn = page.locator(frame_selector).first
        box = start_btn.bounding_box()
        if box:
            human_mouse_move_to(page, box['x'] + box['width']/2, box['y'] + box['height']/2)
            time.sleep(random.uniform(0.1, 0.3))
        human_click_at(page) if box else start_btn.click(timeout=5000)
        print(f"{prefix}\u2713 Clicked START frame button", flush=True)
        time.sleep(random.uniform(0.8, 1.5))
        
        dialog = page.locator('[role="dialog"]').first
        dialog.wait_for(state="visible", timeout=5000)
        upload_btn = find_dialog_upload_button(dialog)
        upload_btn.wait_for(state="visible", timeout=3000)
        time.sleep(random.uniform(0.3, 0.7))
        
        # Start monitoring BEFORE the upload
        monitor.start()
        
        try:
            with page.expect_file_chooser(timeout=5000) as fc_info:
                human_click_for_file_chooser(page, upload_btn)
            time.sleep(random.uniform(2, 5))
            fc_info.value.set_files(start_image)
        except:
            time.sleep(random.uniform(2, 4))
            page.locator("input[type='file']").first.set_input_files(start_image)
        print(f"{prefix}\u2713 START frame file set", flush=True)
        
        # Wait for upload to complete or policy rejection
        print(f"{prefix}Waiting for START upload + policy check...", flush=True)
        upload_confirmed = False
        for w in range(35):
            time.sleep(1)
            if w % random.randint(3, 6) == 0:
                try:
                    human_mouse_move(page)
                except:
                    pass
            
            # Check network monitor — uploadImage takes up to ~30s
            if monitor.is_rejected():
                monitor.stop()
                print(f"{prefix}⚠️ START frame REJECTED by policy (network)!", flush=True)
                try:
                    page.keyboard.press("Escape")
                    time.sleep(0.5)
                except:
                    pass
                return (False, 'start')
            
            # Track button disappearance (visual confirmation)
            if not upload_confirmed:
                remaining = page.locator(frame_selector).count()
                if remaining < btn_count:
                    print(f"{prefix}✓ START button gone ({w+1}s), waiting for uploadImage response...", flush=True)
                    upload_confirmed = True
            
            # uploadImage returned 200 — we're good
            if monitor.is_resolved() and not monitor.is_rejected():
                print(f"{prefix}✓ START frame passed policy check ({w+1}s)", flush=True)
                break
        
        monitor.stop()
        
        if not upload_confirmed:
            print(f"{prefix}⚠️ START not confirmed after 35s", flush=True)
        
        time.sleep(random.uniform(1.0, 2.5))
        human_look_around(page)
    
    # --- Upload END frame ---
    if end_image:
        check_and_dismiss_popup(page)
        remaining = page.locator(frame_selector).count()
        print(f"{prefix}Frame buttons remaining: {remaining}", flush=True)
        
        if remaining == 0:
            print(f"{prefix}\u26a0\ufe0f No frame buttons remaining for END frame", flush=True)
            return (True, None)
        
        time.sleep(random.uniform(0.5, 1.2))
        
        # The only remaining button IS the END frame (START was consumed)
        end_btn = page.locator(frame_selector).first
        box = end_btn.bounding_box()
        if box:
            human_mouse_move_to(page, box['x'] + box['width']/2, box['y'] + box['height']/2)
            time.sleep(random.uniform(0.1, 0.3))
        human_click_at(page) if box else end_btn.click(timeout=5000)
        print(f"{prefix}\u2713 Clicked END frame button", flush=True)
        time.sleep(random.uniform(0.8, 1.5))
        
        dialog = page.locator('[role="dialog"]').first
        # Retry dialog wait — sometimes first click doesn't register after slow START upload
        for _dialog_attempt in range(3):
            try:
                dialog.wait_for(state="visible", timeout=5000)
                break
            except:
                print(f"{prefix}⚠️ END dialog not visible (attempt {_dialog_attempt+1}/3), retrying click...", flush=True)
                check_and_dismiss_popup(page)
                time.sleep(1)
                remaining = page.locator(frame_selector).count()
                if remaining == 0:
                    print(f"{prefix}⚠️ No frame buttons left for END — skipping", flush=True)
                    return (True, None)
                end_btn = page.locator(frame_selector).first
                human_click_locator(page, end_btn, "END frame retry")
                time.sleep(random.uniform(1.0, 2.0))
        upload_btn = find_dialog_upload_button(dialog)
        upload_btn.wait_for(state="visible", timeout=3000)
        time.sleep(random.uniform(0.3, 0.7))
        
        # Start monitoring BEFORE the upload
        monitor.start()
        
        try:
            with page.expect_file_chooser(timeout=5000) as fc_info:
                human_click_for_file_chooser(page, upload_btn)
            time.sleep(random.uniform(2, 5))
            fc_info.value.set_files(end_image)
        except:
            time.sleep(random.uniform(2, 4))
            page.locator("input[type='file']").first.set_input_files(end_image)
        print(f"{prefix}\u2713 END frame file set", flush=True)
        
        # Wait for END upload to complete or policy rejection
        print(f"{prefix}Waiting for END upload + policy check...", flush=True)
        expected_remaining = remaining - 1
        upload_confirmed = False
        for w in range(35):
            time.sleep(1)
            if w % random.randint(3, 6) == 0:
                try:
                    human_mouse_move(page)
                except:
                    pass
            
            # Check network monitor — uploadImage takes up to ~30s
            if monitor.is_rejected():
                monitor.stop()
                print(f"{prefix}⚠️ END frame REJECTED by policy (network)!", flush=True)
                try:
                    page.keyboard.press("Escape")
                    time.sleep(0.5)
                except:
                    pass
                return (False, 'end')
            
            # Track button disappearance (visual confirmation)
            if not upload_confirmed:
                now_remaining = page.locator(frame_selector).count()
                if now_remaining <= expected_remaining:
                    print(f"{prefix}✓ END button gone ({w+1}s), waiting for uploadImage response...", flush=True)
                    upload_confirmed = True
            
            # uploadImage returned 200 — we're good
            if monitor.is_resolved() and not monitor.is_rejected():
                print(f"{prefix}✓ END frame passed policy check ({w+1}s)", flush=True)
                break
        
        monitor.stop()
        
        if not upload_confirmed:
            print(f"{prefix}⚠️ END not confirmed after 35s", flush=True)
        
        time.sleep(random.uniform(0.5, 1.5))
    
    print(f"{prefix}✓ Both frames uploaded", flush=True)
    return (True, None)


def find_dialog_upload_button(dialog):
    """Find the actual 'Upload image' button inside a Flow frame dialog.
    
    The dialog contains multiple buttons (date dropdown, upload, 'Recently Used' dropdown).
    The upload button is identified by its <i> icon with text 'upload'.
    
    Dialog structure (as of Feb 2025):
      - button: date dropdown (e.g. "Feb 28 - 04:51")
      - input: search field ("Search for Assets")
      - button: upload icon (<i>upload</i>) ← THIS is the one we need
      - button: "Recently Used" dropdown
      - gallery: virtuoso list with previously uploaded images
    """
    # Primary: find by icon text
    btn = dialog.locator("button:has(i:text('upload'))")
    if btn.count() > 0:
        return btn.first
    
    # Fallback: find by span text
    btn = dialog.locator("button:has(span:text('Upload image'))")
    if btn.count() > 0:
        return btn.first
    
    # Fallback: button next to search input
    btn = dialog.locator("input[placeholder*='Search'] ~ button, input[placeholder*='Asset'] ~ button")
    if btn.count() > 0:
        return btn.first
    
    # Last resort
    print("[find_dialog_upload_button] ⚠️ Could not find upload button by icon/text, using button.first", flush=True)
    return dialog.locator("button").first


def upload_frame(page, image_path, frame_name="frame"):
    """Upload a frame image. Dialog is already open.
    Click the upload button inside the dialog to trigger file picker bound to that frame.
    
    Dialog structure (as of Feb 2025):
      - button: date dropdown ("Feb 28 - 04:51") ← NOT this one
      - input: search field
      - button: upload icon (i:text('upload')) ← THIS is the upload button
      - button: "Recently Used" dropdown
      - gallery: previously uploaded images
    """
    human_pre_action(page, f"upload {frame_name}")
    
    dialog = page.locator('[role="dialog"]').first
    dialog.wait_for(state="visible", timeout=5000)
    
    # Find the actual upload button (not the date dropdown or other buttons)
    upload_btn = find_dialog_upload_button(dialog)
    upload_btn.wait_for(state="visible", timeout=3000)
    
    try:
        with page.expect_file_chooser(timeout=5000) as fc_info:
            human_click_for_file_chooser(page, upload_btn)
        # Simulate human browsing the file picker (2-5 seconds)
        time.sleep(random.uniform(2, 5))
        fc_info.value.set_files(image_path)
        print(f"\u2713 Uploaded {frame_name} (file chooser)", flush=True)
    except Exception as e:
        print(f"[upload_frame] ⚠️ File chooser failed for {frame_name}: {e}, trying set_input_files fallback...", flush=True)
        time.sleep(random.uniform(2, 4))
        page.locator("input[type='file']").first.set_input_files(image_path)
        print(f"\u2713 Uploaded {frame_name} (input fallback)", flush=True)
    time.sleep(2)


def wait_for_media_popup_to_close(page, context="", timeout=30):
    """
    Wait for the media gallery popup to disappear after uploading/cropping a frame.
    
    After clicking 'Crop and Save', a media gallery popup appears showing
    previously uploaded assets as a virtuoso grid. The first item is the Upload
    button, followed by previously uploaded images. We must wait for this popup
    to close automatically before clicking other buttons (like the END frame button).
    
    CRITICAL: This function first waits for the popup to APPEAR (up to 8s),
    then waits for it to DISAPPEAR. Previously it only checked for disappearance,
    so if the selectors didn't match or the popup hadn't rendered yet, it would 
    return instantly (0.0s) while the popup was still open.
    
    Args:
        page: Playwright page
        context: Optional context string for logging (e.g., "START frame")
        timeout: Maximum seconds to wait for popup to close
    
    Returns:
        True if popup closed, False if timeout
    """
    prefix = f"[{context}] " if context else ""
    
    # Selectors for the media gallery popup - from actual HTML inspection
    # The popup is a virtuoso-grid-list containing upload button + previously uploaded assets
    popup_selectors = [
        "div[data-testid='virtuoso-item-list']",  # Most reliable - data-testid attribute
        "div.virtuoso-grid-list",                   # Virtuoso virtual grid class
        "div.virtuoso-grid-item",                   # Individual grid items
        "button.sc-fbea20b2-0",                     # The Upload button inside the gallery
    ]
    
    def is_popup_visible():
        """Check if any popup selector is visible."""
        for selector in popup_selectors:
            try:
                el = page.locator(selector)
                if el.count() > 0 and el.first.is_visible():
                    return True
            except:
                pass
        return False
    
    # Phase 1: Wait for popup to APPEAR (up to 8 seconds)
    # After Crop and Save, the popup may take a moment to render
    print(f"{prefix}Waiting for media gallery popup to appear...", flush=True)
    popup_appeared = False
    appear_start = time.time()
    appear_timeout = 8
    
    while time.time() - appear_start < appear_timeout:
        if is_popup_visible():
            popup_appeared = True
            elapsed = time.time() - appear_start
            print(f"{prefix}✓ Media gallery popup detected (after {elapsed:.1f}s)", flush=True)
            break
        time.sleep(0.3)
    
    if not popup_appeared:
        # Popup never appeared - selectors may not match or it opened and closed very fast
        # Safety fallback: wait a fixed duration to let UI settle
        print(f"{prefix}⚠️ Media gallery popup not detected by selectors, waiting 4s as safety buffer...", flush=True)
        time.sleep(4)
        return True
    
    # Phase 2: Wait for popup to DISAPPEAR
    print(f"{prefix}Waiting for media gallery popup to close...", flush=True)
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        if not is_popup_visible():
            elapsed = time.time() - start_time
            print(f"{prefix}✓ Media gallery popup closed (after {elapsed:.1f}s)", flush=True)
            time.sleep(0.5)  # Small buffer after popup closes
            return True
        
        # Human-like polling interval
        time.sleep(random.uniform(0.3, 0.6))
    
    print(f"{prefix}⚠️ Media gallery popup didn't close within {timeout}s, proceeding anyway", flush=True)
    return False


def fill_prompt_textarea(page, prompt):
    """Fill the prompt textbox (new UI uses Slate contenteditable div with role='textbox')
    
    Slate editor doesn't respond to textContent/innerText changes.
    Must use keyboard-level input or execCommand to update Slate's internal state.
    """
    # New UI (Feb 2025+): contenteditable div with role="textbox" (Slate editor)
    # Old UI: textarea#PINHOLE_TEXT_AREA_ELEMENT_ID
    
    textbox = page.locator('div[role="textbox"]').first
    if textbox.count() > 0 and textbox.is_visible(timeout=3000):
        # Scroll textbox into view (it may be off-screen after Reuse loads a long prompt)
        try:
            textbox.scroll_into_view_if_needed(timeout=3000)
            time.sleep(0.3)
        except:
            pass
        
        # Human: move mouse to textbox before clicking
        box = textbox.bounding_box()
        if box:
            human_mouse_move_to(page, box['x'] + box['width']/2, box['y'] + box['height']/2)
            time.sleep(random.uniform(0.1, 0.3))
        
        # Click to focus — use textbox.focus() first, then humanized click
        # textbox.focus() guarantees the element gets focus even if mouse click misses
        try:
            textbox.focus(timeout=2000)
        except:
            pass
        human_click_at(page) if box else textbox.click(timeout=3000)
        time.sleep(random.uniform(0.3, 0.6))
        
        # CRITICAL: Verify the textbox is focused before Ctrl+A
        # If focus failed, Ctrl+A selects the entire page and Backspace destroys the UI
        textbox_focused = False
        try:
            textbox_focused = page.evaluate('''() => {
                const tb = document.querySelector('div[role="textbox"]');
                return tb && (document.activeElement === tb || tb.contains(document.activeElement));
            }''')
        except:
            pass
        
        if not textbox_focused:
            # Focus failed — force it with direct click (not humanized)
            print("⚠ Textbox not focused after humanized click — using direct click", flush=True)
            try:
                textbox.click(timeout=3000)
                time.sleep(0.3)
                textbox_focused = page.evaluate('''() => {
                    const tb = document.querySelector('div[role="textbox"]');
                    return tb && (document.activeElement === tb || tb.contains(document.activeElement));
                }''')
            except:
                pass
        
        if not textbox_focused:
            # Still not focused — last resort: JS focus
            print("⚠ Textbox STILL not focused — forcing JS focus", flush=True)
            try:
                page.evaluate('document.querySelector(\'div[role="textbox"]\').focus()')
                time.sleep(0.3)
            except:
                pass
        
        # Select all existing text and delete it
        page.keyboard.press("Control+A")
        time.sleep(random.uniform(0.1, 0.2))
        page.keyboard.press("Backspace")
        time.sleep(random.uniform(0.1, 0.3))
        
        # Copy prompt to clipboard via JS, then Ctrl+V paste
        # This generates real clipboard events (paste, beforeinput, input)
        # which reCAPTCHA Enterprise monitors — insert_text generates zero key events
        # Use keyboard.insert_text first (fast, no evaluate), then Ctrl+A to select and
        # verify. If clipboard works natively, prefer that.
        try:
            # Try native clipboard API via the browser context (no page.evaluate needed)
            page.context.grant_permissions(["clipboard-read", "clipboard-write"], origin=page.url)
        except:
            pass  # Permissions may already be granted or not supported
        
        # Type the prompt using insert_text (generates TextInput events, no CDP evaluate)
        page.keyboard.insert_text(prompt)
        time.sleep(random.uniform(0.3, 0.6))
        
        # Verify text was pasted (Slate can be finicky with clipboard)
        try:
            current_text = textbox.inner_text().strip()
            if len(current_text) < len(prompt) * 0.5:
                # Paste didn't work — fallback to insert_text
                print("⚠ Clipboard paste incomplete, using insert_text fallback", flush=True)
                page.keyboard.press("Control+A")
                time.sleep(0.1)
                page.keyboard.press("Backspace")
                time.sleep(0.1)
                page.keyboard.insert_text(prompt)
                time.sleep(0.3)
        except:
            pass
        
        time.sleep(random.uniform(0.3, 0.6))
        return
    
    # Fallback: old textarea
    escaped = json.dumps(prompt)
    page.evaluate(f'''() => {{
        const textarea = document.querySelector("#PINHOLE_TEXT_AREA_ELEMENT_ID");
        if (!textarea) return;
        
        const nativeTextAreaValueSetter = Object.getOwnPropertyDescriptor(
            window.HTMLTextAreaElement.prototype, 'value'
        ).set;
        nativeTextAreaValueSetter.call(textarea, {escaped});
        const event = new Event('input', {{ bubbles: true, cancelable: true }});
        textarea.dispatchEvent(event);
        const changeEvent = new Event('change', {{ bubbles: true, cancelable: true }});
        textarea.dispatchEvent(changeEvent);
    }}''')


def poll_clip_status(page, data_index=None, max_time=MAX_POLL_TIME):
    """Poll a clip's status until it completes or fails.
    
    If data_index is None, scans through indices 0-5 to find the generating/completed clip.
    """
    polls = max_time // POLL_INTERVAL
    
    for poll in range(polls):
        time.sleep(POLL_INTERVAL)
        
        # Scan through multiple data indices to find the clip
        indices_to_check = [data_index] if data_index is not None else [0, 1, 2, 3, 4, 5]
        
        for idx in indices_to_check:
            container = page.locator(f"div[data-index='{idx}']")
            
            if container.count() == 0:
                continue
            
            # Check if this container has video content or is generating
            # New UI: no date headers, all containers are clips
            # Old UI: date headers had no video section (div.sc-d90fd836-2.dLxTam)
            has_video_or_content = container.locator("video").count() > 0
            if not has_video_or_content:
                # Check for generating/failed text (container has content but no video yet)
                container_text = ""
                try:
                    container_text = container.inner_text(timeout=1000)
                except:
                    pass
                if not container_text or len(container_text.strip()) < 10:
                    if poll % 4 == 0 and idx == 0:
                        print(f"[Poll {poll+1}] Skipping data-index={idx} (empty/header)", flush=True)
                    continue
            
            # Found a clip section, check its status
            try:
                # Use JavaScript to check for "Failed" in this container
                has_failed = page.evaluate(f"""
                    () => {{
                        const container = document.querySelector("div[data-index='{idx}']");
                        if (!container) return false;
                        const fullText = container.innerText || '';
                        if (/\\d+%/.test(fullText)) return false;
                        const elements = container.querySelectorAll('*');
                        for (const el of elements) {{
                            if (((el.innerText === 'Failed Generation' || el.innerText === 'Failed' || el.innerText === 'Error')) && el.children.length === 0) {{
                                return true;
                            }}
                        }}
                        return false;
                    }}
                """)
                if has_failed:
                    print(f"[Poll {poll+1}] ⚠️ FAILED GENERATION detected at data-index={idx}!", flush=True)
                    return "failed"
            except:
                pass
            
            try:
                video = container.locator("video")
                if video.count() > 0:
                    print(f"[Poll {poll+1}] ✓ Video ready at data-index={idx}!", flush=True)
                    return "completed"
            except:
                pass
            
            # Found a clip section that's still generating
            if poll % 4 == 0:
                print(f"[Poll {poll+1}] Still generating at data-index={idx}...", flush=True)
            break  # Found the generating clip, stop scanning indices
    
    print(f"[Poll] Timeout after {max_time}s", flush=True)
    return "timeout"


def rebuild_clip(page, start_frame_path, end_frame_path, prompt, is_first_clip=False, context="[Flow]"):
    """Rebuild a clip from scratch with frames and prompt.
    
    Uses the same upload + wait pattern as the main submission flow:
    - upload_both_frames_with_policy_check (waits for uploadImage API response ~25-30s)
    - click_frame_and_upload_with_policy_check for single frames
    - Waits for Generate button to be enabled before clicking
    """
    try:
        check_and_dismiss_popup(page)
        
        # Human-like behavior before mode selection (anti-bot)
        human_pre_action(page, "mode selection")
        
        # Select Frames to Video mode using proper method
        select_frames_to_video_mode(page, context)
        ensure_batch_view_mode(page, context)
        
        # Upload frames — use policy-check version that waits for uploadImage response
        s_path = start_frame_path if start_frame_path and os.path.exists(start_frame_path) else None
        e_path = end_frame_path if end_frame_path and os.path.exists(end_frame_path) else None
        
        if not s_path and start_frame_path:
            print(f"{context} ⚠️ Start frame not found: {start_frame_path}", flush=True)
        if not e_path and end_frame_path:
            print(f"{context} ⚠️ End frame not found: {end_frame_path}", flush=True)
        
        if s_path and e_path:
            upload_ok, rejected_which = upload_both_frames_with_policy_check(page, s_path, e_path, context=context)
            if not upload_ok:
                print(f"{context} ⚠️ Frame upload failed (rejected: {rejected_which})", flush=True)
                return False
        elif s_path:
            result, reason = click_frame_and_upload_with_policy_check(page, s_path, is_end_frame=False, context=context)
            if not result and reason == 'policy':
                print(f"{context} ⚠️ START frame rejected by policy", flush=True)
                return False
        elif e_path:
            result, reason = click_frame_and_upload_with_policy_check(page, e_path, is_end_frame=True, context=context)
            if not result and reason == 'policy':
                print(f"{context} ⚠️ END frame rejected by policy", flush=True)
                return False
        else:
            print(f"{context} ⚠️ NO FRAMES TO UPLOAD! start={start_frame_path}, end={end_frame_path}", flush=True)
        
        # Enter prompt
        fill_prompt_textarea(page, prompt)
        print(f"{context} Entered prompt: {prompt[:50]}...", flush=True)
        human_pre_generate_wait(page)
        
        # Human-like pre-generate behavior (match main flow exactly)
        human_mouse_move(page)
        human_delay(1, 2)
        scroll_randomly(page)
        human_delay(0.5, 1)
        
        # Wait for Generate button to be enabled (frames must finish processing)
        for wait_sec in range(60):
            if is_generate_button_enabled(page):
                break
            if wait_sec == 0:
                print(f"{context} Waiting for Generate button to be enabled...", flush=True)
            time.sleep(1)
        else:
            print(f"{context} ⚠️ Generate button not enabled after 60s", flush=True)
        
        # Click Generate button
        human_delay(0.5, 1.5)
        human_click_element(page, page.locator("button:has(i:text('arrow_forward'))").first, "Generate button", timeout=30000)
        print(f"{context} ✓ Clicked Generate", flush=True)
        human_delay(3, 6)
        
        return True
        
    except Exception as e:
        print(f"{context} Error rebuilding clip: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return False


def submit_and_poll(page, start_frame_path, end_frame_path, prompt, clip_num,
                    is_first_clip=False, max_retries=MAX_GENERATION_RETRIES):
    """Submit a clip and poll until success."""
    for attempt in range(max_retries + 1):
        if attempt > 0:
            print(f"\n[Flow] === RETRY {attempt}/{max_retries} for clip {clip_num} ===", flush=True)
        
        if not rebuild_clip(page, start_frame_path, end_frame_path, prompt,
                           is_first_clip=(is_first_clip and attempt == 0)):
            print(f"[Flow] Failed to submit clip {clip_num}", flush=True)
            time.sleep(2)
            continue
        
        status = poll_clip_status(page)  # Let it scan for the generating clip
        
        if status == "completed":
            print(f"[Flow] ✓ Clip {clip_num} completed!", flush=True)
            return True
        elif status == "failed":
            if attempt < max_retries:
                print(f"[Flow] Clip {clip_num} failed, will rebuild and retry...", flush=True)
                time.sleep(2)
                continue
            else:
                print(f"[Flow] ⚠️ Clip {clip_num} failed after {max_retries} retries!", flush=True)
                return False
        else:
            print(f"[Flow] Clip {clip_num} timed out - assuming still generating", flush=True)
            return True
    
    return False


# ============================================================
# REDO PROCESSING  
# ============================================================

def process_redo_clip(page, clip, download_queue, cache):
    """Process a single clip redo in a NEW Flow project (not the main project)."""
    clip_id = clip['id']
    job_id = clip['job_id']
    
    # Immediately mark as generating so frontend shows "in progress" instead of "failed"
    update_clip_status(clip_id, 'generating')
    
    dialogue = clip.get('dialogue_text', '').strip().strip('"').strip("'")
    
    # Get additional context for prompt building (from API response)
    language = clip.get('language', 'English')
    duration = float(clip.get('duration', '8'))
    voice_profile = clip.get('voice_profile', '')
    redo_reason = clip.get('redo_reason', '')
    
    # ALWAYS rebuild prompt for redo to ensure new dialogue is used
    # (User may have edited dialogue in the redo dialog - same behavior as API jobs)
    prompt = build_flow_prompt(
        dialogue_line=dialogue,
        language=language,
        voice_profile=voice_profile,
        duration=duration,
        redo_feedback=redo_reason if redo_reason else None,
    )
    
    attempt = clip.get('generation_attempt', 1)
    clip_index = clip.get('clip_index', 0)
    start_frame_url = clip.get('start_frame_url')
    end_frame_url = clip.get('end_frame_url')
    
    print(f"\n{'='*60}", flush=True)
    print(f"REDO: Clip {clip_index} (Attempt {attempt})", flush=True)
    print(f"Job: {job_id[:8]}...", flush=True)
    print(f"Creating NEW project for this redo...", flush=True)
    print(f"{'='*60}", flush=True)
    
    temp_dir = tempfile.mkdtemp(prefix=f"flow_redo_{clip_id}_")
    
    start_frame_local = None
    end_frame_local = None
    
    if start_frame_url:
        start_frame_local = download_frame(start_frame_url, os.path.join(temp_dir, f"start_{clip_index}.png"))
        print(f"[REDO] {'✓' if start_frame_local else '✗'} Start frame", flush=True)
    
    if end_frame_url:
        end_frame_local = download_frame(end_frame_url, os.path.join(temp_dir, f"end_{clip_index}.png"))
        print(f"[REDO] {'✓' if end_frame_local else '✗'} End frame", flush=True)
    
    # Navigate to Flow home to create NEW project
    print(f"[REDO] Navigating to Flow homepage...", flush=True)
    spa_navigate_to_flow_home(page, "REDO")
    human_delay(2, 4)  # Match main flow post-navigation wait
    
    ensure_logged_into_flow(page, "REDO")
    check_and_dismiss_popup(page)
    
    # Human-like "looking around" (match main flow exactly)
    human_mouse_move(page)
    human_delay(1, 2)
    scroll_randomly(page)
    human_delay(0.5, 1)
    
    # Save current URL so we can verify a NEW project is created
    url_before_create = page.url
    print(f"[REDO] URL before create: {url_before_create}", flush=True)
    
    # Create new project
    dismiss_create_with_flow(page, "REDO")
    
    # Click New project button (match main flow: human_click_element)
    try:
        human_click_element(page, "button:has-text('New project'), button:has(i:text('add_2')), button.sc-a38764c7-0", "[REDO] New project button")
    except Exception as e:
        print(f"[REDO] ❌ Could not click New project button: {e}", flush=True)
        update_clip_status(clip_id, 'failed', error_message="Failed to click New project button for redo")
        shutil.rmtree(temp_dir, ignore_errors=True)
        return False
    
    human_delay(2, 3)  # Match main flow post-click wait
    
    # Wait for new project URL
    try:
        page.wait_for_url("**/project/**", timeout=30000)
    except:
        print("[REDO] wait_for_url timed out, polling...", flush=True)
        for _ in range(15):
            time.sleep(1)
            if "/project/" in page.url:
                break
    
    time.sleep(2)
    redo_project_url = page.url
    
    if "/project/" not in redo_project_url:
        print(f"[REDO] ❌ Failed to create project - URL: {redo_project_url}", flush=True)
        update_clip_status(clip_id, 'failed', error_message="Failed to create redo project")
        shutil.rmtree(temp_dir, ignore_errors=True)
        return False
    
    # CRITICAL: Verify this is actually a NEW project, not the old one
    if redo_project_url == url_before_create:
        print(f"[REDO] ❌ Project URL didn't change - still on old project!", flush=True)
        print(f"[REDO] Old: {url_before_create}", flush=True)
        print(f"[REDO] New: {redo_project_url}", flush=True)
        update_clip_status(clip_id, 'failed', error_message="Redo project creation failed - reused old project")
        shutil.rmtree(temp_dir, ignore_errors=True)
        return False
    
    print(f"[REDO] ✓ Created NEW redo project: {redo_project_url}", flush=True)
    human_delay(1, 2)  # Match main flow post-creation wait
    check_and_dismiss_popup(page)
    ensure_videos_tab_selected(page)
    
    update_clip_status(clip_id, 'generating')
    
    # Submit clip to new project (just submit, don't poll - download browser will handle detection)
    pre_generate_tile_count = get_tile_count_at_index0(page)
    if not rebuild_clip(page, start_frame_local, end_frame_local, prompt, is_first_clip=True):
        print(f"[REDO] ❌ Failed to submit clip {clip_index}", flush=True)
        update_clip_status(clip_id, 'failed', error_message="Failed to submit clip")
        shutil.rmtree(temp_dir, ignore_errors=True)
        return False
    
    # Record submission time
    submit_time = datetime.now()
    print(f"[REDO] Clip {clip_index} submitted at {submit_time.strftime('%H:%M:%S')}", flush=True)
    
    # Check for immediate failure
    immediate_failure = check_recent_clip_failure(page, data_index=1, clip_num=clip_index, old_tile_ids=pre_generate_tile_count)
    
    if immediate_failure:
        print(f"[REDO] ⚠️ Clip {clip_index} failed immediately!", flush=True)
        # Mark as failed immediately - don't waste time waiting for download
        update_clip_status(clip_id, 'failed', error_message="Flow generation failed immediately")
        shutil.rmtree(temp_dir, ignore_errors=True)
        print(f"[REDO] ❌ Clip {clip_index} marked as failed - user can request another redo", flush=True)
        return False
    
    # Queue for download - download browser will wait for clip to be ready
    download_queue.put({
        'job_id': job_id,
        'project_url': redo_project_url,  # Use the NEW project URL
        'clips': [clip],
        'clip_submit_times': {clip_index: submit_time},  # Per-clip submission time
        'submitted_at': submit_time,
        'temp_dir': temp_dir,
        'is_redo': True
    })
    
    print(f"✓ Redo queued for download (will be ready after {CLIP_READY_WAIT}s)", flush=True)
    return True


# ============================================================
# SUBMISSION LOGIC
# ============================================================

def process_job_submission_with_failover(page, job, cache, download_queue, account_name,
                                          failover_queue=None, all_download_queues=None,
                                          clips_to_process=None, is_failover=False, failed_account=None,
                                          is_parallel=False, is_parallel_primary=False,
                                          is_failover_to_standby=False):
    """
    Submit clips for a job with cross-account failover support.
    
    On first clip failure:
    - If failover_queue is available: hand off ALL remaining clips to other account
    - If no failover_queue (already failed over): use old retry logic or mark as failed
    
    Args:
        page: Playwright page
        job: Job data from API
        cache: Local cache
        download_queue: Queue for this account's download worker
        account_name: Name of this account (e.g., "Account1")
        failover_queue: Queue to send job to other account (None if already failed over)
        all_download_queues: Dict of account_name -> download_queue for routing
        clips_to_process: Specific clips to process (for failover jobs)
        is_failover: True if this is a failover from another account
        failed_account: Name of account that failed (for logging)
        is_parallel: True if this is a parallel execution (multiple accounts simultaneously)
        is_parallel_primary: True if this account is responsible for job status updates
        is_failover_to_standby: True if failover should go to standby manager (not another active account)
    
    Returns:
        project_url if successful, None if failed and handed off
    """
    job_id = job['id']
    all_clips = job['clips']
    
    # Determine which clips to process
    if clips_to_process is not None:
        clips = clips_to_process
    else:
        clips = all_clips
    
    # IMMEDIATELY mark as processing to prevent duplicate pickup
    # Only primary account (or non-parallel single account) updates job status
    if not is_failover and not is_parallel:
        update_job_status(job_id, 'processing')
    elif is_parallel and is_parallel_primary:
        update_job_status(job_id, 'processing')
    
    print(f"\n{'='*60}")
    if is_parallel:
        print(f"🚀 PARALLEL {'PRIMARY' if is_parallel_primary else 'SECONDARY'}: {job_id[:8]}...")
        print(f"   Clips: {[c.get('clip_index') for c in clips]}")
    elif is_failover:
        print(f"🔄 FAILOVER JOB: {job_id[:8]}...")
        print(f"   Taking over from: {failed_account}")
    else:
        print(f"PROCESSING JOB: {job_id[:8]}...")
    print(f"Account: {account_name}")
    print(f"Clips: {len(clips)}")
    print(f"{'='*60}")
    
    # Initialize human-like timing controller
    human_pacer = HumanPacer(account_name=account_name)
    
    # For failover jobs, always create a NEW project
    if is_failover:
        spa_navigate_to_flow_home(page, account_name)
        time.sleep(3)
        
        ensure_logged_into_flow(page, account_name)
        check_and_dismiss_popup(page)
        
        # Human-like "looking around" before first interaction (anti-bot)
        human_look_around(page)
        
        dismiss_create_with_flow(page, account_name)
        human_click_element(page, "button:has-text('New project'), button:has(i:text('add_2')), button.sc-a38764c7-0", "New project button")
        
        try:
            page.wait_for_url("**/project/**", timeout=30000)
        except:
            print("[Flow] wait_for_url timed out, polling...")
            for _ in range(15):
                time.sleep(1)
                if "/project/" in page.url:
                    break
        
        time.sleep(2)
        project_url = page.url
        print(f"✓ Created NEW project for failover: {project_url}")
        
        if "/project/" not in project_url:
            raise Exception(f"Failed to create project - URL: {project_url}")
        
        time.sleep(3)
        check_and_dismiss_popup(page)
        ensure_videos_tab_selected(page)
        clips_done = []
    else:
        # Non-failover: check cache for existing project
        cached_job = get_cached_job(cache, job_id)
        
        if cached_job and cached_job.get('project_url'):
            project_url = cached_job['project_url']
            clips_done = cached_job.get('clips_submitted', [])
            print(f"✓ Resuming from cache: {project_url}")
            print(f"  Clips done: {clips_done}")
            
            page.goto(project_url, timeout=60000)
            page.wait_for_load_state("domcontentloaded", timeout=30000)
            time.sleep(5)
            ensure_logged_into_flow(page, account_name)
        else:
            spa_navigate_to_flow_home(page, account_name)
            time.sleep(3)
            
            ensure_logged_into_flow(page, account_name)
            check_and_dismiss_popup(page)
            
            # Human-like "looking around" before first interaction (anti-bot)
            human_look_around(page)
            
            dismiss_create_with_flow(page, account_name)
            human_click_element(page, "button:has-text('New project'), button:has(i:text('add_2')), button.sc-a38764c7-0", "New project button")
            
            try:
                page.wait_for_url("**/project/**", timeout=30000)
            except:
                print("[Flow] wait_for_url timed out, polling...")
                for _ in range(15):
                    time.sleep(1)
                    if "/project/" in page.url:
                        break
            
            time.sleep(2)
            project_url = page.url
            print(f"✓ Project URL: {project_url}")
            
            if "/project/" not in project_url:
                raise Exception(f"Failed to create project - URL: {project_url}")
            
            print(f"✓ Created project: {project_url}")
            
            time.sleep(3)
            check_and_dismiss_popup(page)
            
            clips_done = []
            mark_job_started(cache, job_id, project_url, clips)
    
    # Update project URL in API
    api_request("POST", f"/jobs/{job_id}/status", {"flow_project_url": project_url})
    
    temp_dir = tempfile.mkdtemp(prefix=f"flow_job_{job_id[:8]}_")
    frames_dir = os.path.join(temp_dir, "frames")
    os.makedirs(frames_dir, exist_ok=True)
    
    # Download frames
    for clip in clips:
        if clip.get('start_frame_url'):
            clip['start_frame_local'] = download_frame(
                clip['start_frame_url'],
                os.path.join(frames_dir, f"start_{clip['clip_index']}.png")
            )
        if clip.get('end_frame_url'):
            clip['end_frame_local'] = download_frame(
                clip['end_frame_url'],
                os.path.join(frames_dir, f"end_{clip['clip_index']}.png")
            )
    
    # Get job context for prompt building
    job_language = job.get('language', 'English')
    job_duration = float(job.get('duration', '8'))
    job_voice_profile = job.get('voice_profile', '')
    
    clips_data = []
    for clip in clips:
        prompt = clip.get('prompt')
        if not prompt:
            prompt = build_flow_prompt(
                dialogue_line=clip.get('dialogue_text', ''),
                language=job_language,
                voice_profile=job_voice_profile,
                duration=job_duration,
            )
        clips_data.append({
            'clip_index': clip['clip_index'],
            'id': clip.get('id'),
            'start_frame': clip.get('start_frame_local'),
            'end_frame': clip.get('end_frame_local'),
            'start_frame_url': clip.get('start_frame_url'),
            'end_frame_url': clip.get('end_frame_url'),
            'start_frame_key': clip.get('start_frame_key'),
            'end_frame_key': clip.get('end_frame_key'),
            'prompt': prompt,
            'dialogue_text': clip.get('dialogue_text', ''),
        })
    
    # Initialize clip_project_map - all clips start in main project
    clip_project_map = {clip['clip_index']: project_url for clip in clips}
    print(f"[{account_name}] Initialized clip_project_map with {len(clip_project_map)} clips", flush=True)
    
    # Track submission times per clip for download timing
    clip_submit_times = {}
    
    # Track permanently failed clips
    permanently_failed_clips = set()
    
    # Track downloaded videos for continue mode (clip_index -> video_path)
    # This dict is shared with the download worker thread
    downloaded_videos = {}
    
    # Analyze continue mode chains
    continue_chains = analyze_continue_mode_chains(clips)
    has_continue_mode = any(len(chain) > 1 for chain in continue_chains)
    if has_continue_mode:
        print(f"[ContinueMode] Job has continue mode clips - will handle sequentially within chains", flush=True)
    
    # Extended failure monitoring - continues checking clips after immediate 3s check
    failure_monitor = ExtendedFailureMonitor(monitoring_duration=60)
    
    prev_start_frame_key = None
    prev_end_frame_key = None
    
    download_queued = False
    
    for i, clip in enumerate(clips):
        clip_index = clip['clip_index']
        
        if clip_index in clips_done:
            print(f"\n--- Clip {i+1}/{len(clips)} SKIPPED (cached) ---")
            prev_start_frame_key = clip.get('start_frame_key')
            prev_end_frame_key = clip.get('end_frame_key')
            continue
        
        print(f"\n--- Clip {i+1}/{len(clips)} ---")
        
        start_frame = clip.get('start_frame_local')
        end_frame = clip.get('end_frame_local')
        start_frame_key = clip.get('start_frame_key')
        end_frame_key = clip.get('end_frame_key')
        
        # Get clip mode and scene info
        clip_mode = clip.get('clip_mode', 'blend')
        scene_index = clip.get('scene_index', 0)
        prev_scene_index = clips[i-1].get('scene_index', 0) if i > 0 else -1
        continue_frame_extracted = False  # Flag to track if we successfully extracted a frame
        
        # ============================================================
        # CONTINUE MODE HANDLING
        # If this is a continue-mode clip that's not the first in its chain,
        # we need to:
        # 1. Wait for the previous clip's video to be downloaded
        # 2. Wait for user APPROVAL of the previous clip
        # 3. Extract frame from the APPROVED variant
        # 4. Enhance and use that frame
        # ============================================================
        if clip_mode == 'continue' and i > 0 and scene_index == prev_scene_index:
            prev_clip = clips[i-1]
            prev_clip_index = prev_clip.get('clip_index')
            prev_clip_id = prev_clip.get('id')
            
            print(f"[ContinueMode] Clip {clip_index} requires frame from approved clip {prev_clip_index}", flush=True)
            
            # Step 1: Wait for video to exist on disk (max 5 minutes)
            max_video_wait = 300
            video_wait_start = datetime.now()
            prev_video_path = None
            
            while (datetime.now() - video_wait_start).total_seconds() < max_video_wait:
                # Check for any video file for this clip
                prev_video_path = find_downloaded_video(temp_dir, prev_clip_index)
                
                if prev_video_path and os.path.exists(prev_video_path):
                    print(f"[ContinueMode] Previous clip video downloaded: {os.path.basename(prev_video_path)}", flush=True)
                    break
                
                elapsed = int((datetime.now() - video_wait_start).total_seconds())
                if elapsed % 30 == 0:
                    print(f"[ContinueMode] Waiting for clip {prev_clip_index} video download... ({elapsed}s)", flush=True)
                time.sleep(10)
            
            if not prev_video_path or not os.path.exists(prev_video_path):
                print(f"[ContinueMode] WARNING: Timeout waiting for video download, using original start frame", flush=True)
            else:
                # Step 2: Wait for user approval (max 10 minutes)
                approval_result = wait_for_clip_approval(prev_clip_id, prev_clip_index, temp_dir, timeout=600)
                
                if approval_result and approval_result.get('success'):
                    # Use the approved variant's video
                    approved_video = approval_result.get('video_path')
                    selected_variant = approval_result.get('selected_variant', 1)
                    
                    # If a different variant was selected, find that video
                    if selected_variant != 1:
                        specific_video = find_downloaded_video(temp_dir, prev_clip_index, variant=selected_variant)
                        if specific_video and os.path.exists(specific_video):
                            approved_video = specific_video
                            print(f"[ContinueMode] Using approved variant {selected_variant}: {os.path.basename(approved_video)}", flush=True)
                    
                    if approved_video and os.path.exists(approved_video):
                        # Step 3: Extract frame from approved video
                        extracted_frame = extract_frame_from_video(approved_video, frame_offset=-8)
                        
                        if extracted_frame:
                            # Step 4: Enhance frame via Nano Banana API
                            original_frame_key = clip.get('start_frame_key')  # Original scene image for facial consistency
                            enhanced_frame = enhance_frame_via_api(extracted_frame, original_frame_key, job_id)
                            
                            # Use enhanced frame as start frame
                            start_frame = enhanced_frame
                            print(f"[ContinueMode] ✓ Using enhanced frame from approved clip {prev_clip_index} (variant {selected_variant})", flush=True)
                            
                            # Mark that we successfully extracted a frame
                            continue_frame_extracted = True
                            has_new_start = True  # Force new start since we have a new frame
                        else:
                            print(f"[ContinueMode] WARNING: Frame extraction failed, using original start frame", flush=True)
                    else:
                        print(f"[ContinueMode] WARNING: Approved video not found, using original start frame", flush=True)
                else:
                    # Approval failed, timed out, or clip was rejected
                    reason = approval_result.get('reason', 'unknown') if approval_result else 'error'
                    print(f"[ContinueMode] WARNING: Approval not received ({reason}), using original start frame", flush=True)
        
        prompt = clip.get('prompt')
        if not prompt:
            prompt = build_flow_prompt(
                dialogue_line=clip.get('dialogue_text', ''),
                language=job_language,
                voice_profile=job_voice_profile,
                duration=job_duration,
            )
        
        # Calculate if frames are new (unless already set by continue mode handling above)
        # In continue mode with extracted frame, has_new_start was already set to True
        if not continue_frame_extracted:
            has_new_start = (start_frame_key != prev_start_frame_key) if prev_start_frame_key else True
        # else: has_new_start was already set in continue mode block above
        
        has_new_end = (end_frame_key != prev_end_frame_key) if prev_end_frame_key else (end_frame_key is not None)
        
        # If this clip has an end frame but the previous clip didn't (or vice versa),
        # the frame slot layout changes in Flow's UI — must re-upload start frame
        prev_had_end = prev_end_frame_key is not None
        curr_has_end = end_frame_key is not None
        if curr_has_end != prev_had_end:
            has_new_start = True
            has_new_end = curr_has_end
        
        has_new_frames = has_new_start or has_new_end
        
        print(f"  start_frame_key: {start_frame_key}")
        print(f"  end_frame_key: {end_frame_key}")
        print(f"  prev_start: {prev_start_frame_key}, prev_end: {prev_end_frame_key}")
        print(f"  has_new_start={has_new_start}, has_new_end={has_new_end}, has_new_frames={has_new_frames}")
        print(f"  clip_mode={clip_mode}, scene_index={scene_index}")
        
        if i == 0:
            # First clip - needs frames and full setup
            check_and_dismiss_popup(page)
            
            # Select Frames to Video mode using proper method
            select_frames_to_video_mode(page)
            ensure_batch_view_mode(page)
            
            # Upload START frame
            if start_frame:
                check_and_dismiss_popup(page)
                human_click_element(page, "div.sc-8f31d1ba-1, button.sc-d02e9a37-1", "START frame button")
                print("✓ Clicked Add START frame button")
                time.sleep(1)  # Wait for gallery to open
                upload_frame(page, start_frame, "START frame")
            
            # Upload END frame
            # Skip ONLY if: continue mode AND same image (no scene transition)
            # If end_frame_key differs from start_frame_key, it's a scene transition - MUST use end frame
            is_scene_transition = (end_frame_key and start_frame_key and end_frame_key != start_frame_key)
            skip_end_frame = (clip_mode == 'continue' and not is_scene_transition)
            
            if end_frame and not skip_end_frame:
                check_and_dismiss_popup(page)
                # Single click - popup closed after START frame upload
                end_frame_btn = page.locator("div.sc-8f31d1ba-1, button.sc-d02e9a37-1").last
                human_click_element(page, end_frame_btn, "END frame button")
                time.sleep(1)  # Wait for gallery to open
                upload_frame(page, end_frame, "END frame")
                if is_scene_transition:
                    print(f"  (scene transition: {start_frame_key} → {end_frame_key})")
            elif skip_end_frame:
                print("✓ Skipping END frame (continue mode within same scene - no interpolation target)")
            
            # Enter prompt
            fill_prompt_textarea(page, prompt)
            print(f"✓ Entered prompt: {prompt[:50]}...")
            human_pre_generate_wait(page)
            
            # Click Generate (with retry logic and crash handling)
            time.sleep(1)
            click_generate_with_crash_handler(page, account_name, clip_index, clips, 
                                              clip_submit_times, download_queued, download_queue, job_id,
                                              start_frame=start_frame, end_frame=end_frame, prompt=prompt,
                                              clip_mode=clip_mode, start_frame_key=start_frame_key, end_frame_key=end_frame_key)
            print(f"✓ Started generation for clip {i+1}")
            time.sleep(5)
            
        elif has_new_frames:
            # Subsequent clip with NEW frames
            check_and_dismiss_popup(page)
            
            # Upload START frame first if it changed
            if has_new_start and start_frame:
                check_and_dismiss_popup(page)
                human_click_element(page, "div.sc-8f31d1ba-1, button.sc-d02e9a37-1", "START frame button")
                print("✓ Clicked Add START frame button")
                time.sleep(1)  # Wait for gallery to open
                upload_frame(page, start_frame, "START frame")
            
            # Then upload END frame if it changed
            # Skip ONLY if: continue mode AND same image (no scene transition)
            is_scene_transition = (end_frame_key and start_frame_key and end_frame_key != start_frame_key)
            skip_end_frame = (clip_mode == 'continue' and not is_scene_transition)
            
            if has_new_end and end_frame and not skip_end_frame:
                check_and_dismiss_popup(page)
                # Single click - popup closed after START frame upload
                end_frame_btn = page.locator("div.sc-8f31d1ba-1, button.sc-d02e9a37-1").last
                human_click_element(page, end_frame_btn, "END frame button")
                time.sleep(1)  # Wait for gallery to open
                upload_frame(page, end_frame, "END frame")
                if is_scene_transition:
                    print(f"  (scene transition: {start_frame_key} → {end_frame_key})")
            elif skip_end_frame:
                print("✓ Skipping END frame (continue mode within same scene - no interpolation target)")
            
            fill_prompt_textarea(page, prompt)
            print(f"✓ Entered prompt: {prompt[:50]}...")
            human_pre_generate_wait(page)
            
            time.sleep(1)
            click_generate_with_crash_handler(page, account_name, clip_index, clips, 
                                              clip_submit_times, download_queued, download_queue, job_id,
                                              start_frame=start_frame, end_frame=end_frame, prompt=prompt,
                                              clip_mode=clip_mode, start_frame_key=start_frame_key, end_frame_key=end_frame_key)
            print(f"✓ Started generation for clip {i+1}")
            time.sleep(5)
            
        else:
            # No new frames - reuse
            print(f"\n--- Clip {i+1}/{len(clips)} (reuse frames) ---")
            
            # Use new robust reuse function that waits for Generate button to be enabled
            # and retries with page refresh if it gets stuck
            try:
                click_reuse_and_generate(page, prompt, i+1, account_name, max_retries=3, wait_timeout=60)
            except Exception as e:
                # Handle as crash - notify download thread and mark failures
                print(f"\n{'='*50}", flush=True)
                print(f"[{account_name}] ❌ REUSE+GENERATE FAILED at clip {clip_index}!", flush=True)
                print(f"[{account_name}] Error: {e}", flush=True)
                print(f"{'='*50}\n", flush=True)
                
                # Notify download thread to only expect already-submitted clips
                if download_queued:
                    submitted_clip_indices = set(clip_submit_times.keys())
                    if submitted_clip_indices:
                        print(f"[{account_name}] Limiting download to {len(submitted_clip_indices)} submitted clips: {sorted(submitted_clip_indices)}", flush=True)
                        download_queue.put({
                            'type': 'limit_clips',
                            'job_id': job_id,
                            'allowed_clips': submitted_clip_indices
                        })
                    else:
                        print(f"[{account_name}] No clips were submitted - cancelling download", flush=True)
                        download_queue.put({
                            'type': 'cancel',
                            'job_id': job_id
                        })
                
                # Mark this and remaining clips as failed
                for remaining_clip in clips[i:]:
                    remaining_idx = remaining_clip['clip_index']
                    if remaining_idx not in clip_submit_times:
                        update_clip_status(remaining_clip['id'], 'failed', error_message=f"Reuse button stuck: {str(e)[:100]}")
                
                # Signal early termination
                return None
            
            time.sleep(3)
        
        update_clip_status(clip['id'], 'generating')
        mark_clip_submitted(cache, job_id, clip_index)
        
        # Record submission time
        clip_submit_times[clip_index] = datetime.now()
        print(f"[{account_name}] Clip {clip_index} submitted at {clip_submit_times[clip_index].strftime('%H:%M:%S')}", flush=True)
        
        # Wait 3 seconds then check for immediate failure
        # IMPORTANT: We do this BEFORE queuing for download on first clip
        time.sleep(FAILURE_CHECK_DELAY)
        clip_failed = check_recent_clip_failure(page, data_index=1, clip_num=i)
        
        if clip_failed:
            print(f"[{account_name}] ⚠️ Clip {clip_index} FAILED immediately!", flush=True)
            
            # ============================================================
            # SMART FAILOVER STRATEGY:
            # 1. Try same-account retry in new project (fast, handles project-level flags)
            # 2. If same-account retries exhausted, use FailoverRouter for cross-account
            # 3. FailoverRouter picks healthiest idle account, or standby, or marks failed
            # ============================================================
            
            remaining_clips = clips[i:]  # Include the failed clip
            
            # Step 1: Same-account retry?
            can_retry_same = False
            if failover_router is not None:
                can_retry_same = failover_router.should_retry_same_account(job_id, account_name)
                if not can_retry_same:
                    retry_count = failover_router.get_retry_count(job_id, account_name)
                    is_hot = account_health.is_hot(account_name)
                    print(f"[{account_name}] Same-account retry SKIPPED: retry_count={retry_count}/{failover_router.MAX_SAME_ACCOUNT_RETRIES}, is_hot={is_hot}", flush=True)
                else:
                    print(f"[{account_name}] Same-account retry APPROVED", flush=True)
            else:
                print(f"[{account_name}] failover_router is None — same-account retry not available", flush=True)
            
            if can_retry_same:
                retry_num = failover_router.increment_retry(job_id, account_name)
                print(f"\n{'='*50}", flush=True)
                print(f"🔁 SAME-ACCOUNT RETRY #{retry_num}/{failover_router.MAX_SAME_ACCOUNT_RETRIES}", flush=True)
                print(f"   Account: {account_name}", flush=True)
                print(f"   Failed clip: {clip_index}", flush=True)
                print(f"   Creating new project on same account...", flush=True)
                print(f"{'='*50}\n", flush=True)
                
                # Record failure in health tracker (but don't hand off yet)
                account_health.record_failure(account_name, job_id)
                
                # If download was already queued, limit it to clips before the failure
                if download_queued:
                    successfully_submitted = [c.get('clip_index') for c in clips[:i]]
                    if successfully_submitted:
                        print(f"[{account_name}] Limiting download to clips {successfully_submitted} (before failure)...", flush=True)
                        download_queue.put({
                            'type': 'limit_clips',
                            'job_id': job_id,
                            'allowed_clips': set(successfully_submitted)
                        })
                    else:
                        print(f"[{account_name}] Cancelling download job (no successful clips)...", flush=True)
                        download_queue.put({
                            'type': 'cancel',
                            'job_id': job_id
                        })
                
                # Navigate to Flow home and create a NEW project
                try:
                    spa_navigate_to_flow_home(page, account_name)
                    time.sleep(3)
                    
                    ensure_logged_into_flow(page, account_name)
                    check_and_dismiss_popup(page)
                    human_look_around(page)
                    
                    dismiss_create_with_flow(page, account_name)
                    human_click_element(page, "button:has-text('New project'), button:has(i:text('add_2')), button.sc-a38764c7-0", "New project (retry)")
                    
                    try:
                        page.wait_for_url("**/project/**", timeout=30000)
                    except:
                        for _ in range(15):
                            time.sleep(1)
                            if "/project/" in page.url:
                                break
                    
                    time.sleep(2)
                    new_project_url = page.url
                    
                    if "/project/" not in new_project_url:
                        raise Exception(f"Failed to create retry project - URL: {new_project_url}")
                    
                    print(f"[{account_name}] ✓ Created retry project: {new_project_url}", flush=True)
                    time.sleep(3)
                    check_and_dismiss_popup(page)
                    ensure_videos_tab_selected(page)
                    
                    # Update project URL and reset state for remaining clips
                    project_url = new_project_url
                    clip_project_map = {c['clip_index']: project_url for c in remaining_clips}
                    api_request("POST", f"/jobs/{job_id}/status", {"flow_project_url": project_url})
                    
                    # Reset download state — need fresh download for new project
                    download_queued = False
                    
                    # Re-process remaining clips from scratch in the new project
                    # by recursively calling ourselves with the remaining clips
                    print(f"[{account_name}] Re-submitting {len(remaining_clips)} clips in new project...", flush=True)
                    
                    result = process_job_submission_with_failover(
                        page=page,
                        job=job,
                        cache=cache,
                        download_queue=download_queue,
                        account_name=account_name,
                        failover_queue=failover_queue,
                        all_download_queues=all_download_queues,
                        clips_to_process=remaining_clips,
                        is_failover=True,  # Force new project setup
                        failed_account=account_name,
                        is_parallel=is_parallel,
                        is_parallel_primary=is_parallel_primary,
                        is_failover_to_standby=is_failover_to_standby,
                    )
                    return result
                    
                except Exception as retry_err:
                    print(f"[{account_name}] ❌ Same-account retry failed: {retry_err}", flush=True)
                    # Fall through to cross-account failover below
            
            # Step 2: Cross-account failover via FailoverRouter
            if failover_router is not None:
                # Clean up download state
                if download_queued:
                    successfully_submitted = [c.get('clip_index') for c in clips[:i]]
                    if successfully_submitted:
                        print(f"[{account_name}] Limiting download to clips {successfully_submitted} (before failure)...", flush=True)
                        download_queue.put({
                            'type': 'limit_clips',
                            'job_id': job_id,
                            'allowed_clips': set(successfully_submitted)
                        })
                    else:
                        print(f"[{account_name}] Cancelling download job (no successful clips)...", flush=True)
                        download_queue.put({
                            'type': 'cancel',
                            'job_id': job_id
                        })
                    
                    download_queue.put({
                        'type': 'shutdown_after_complete',
                        'job_id': job_id
                    })
                    print(f"[{account_name}] Download worker will shutdown after completing current job", flush=True)
                
                # Build failover data
                failover_data = {
                    'type': 'failover',
                    'job_id': job_id,
                    'original_job': job,
                    'remaining_clips': remaining_clips,
                    'failed_clip_index': clip_index,
                    'failed_account': account_name,
                    'clips_data': clips_data[i:],
                    'all_download_queues': all_download_queues,
                }
                
                result = failover_router.route_failover(
                    failed_account=account_name,
                    failover_data=failover_data,
                    download_queue=download_queue,
                    download_queued=download_queued,
                    job_id=job_id,
                )
                
                if result == 'routed':
                    return None  # Successfully handed off
                # else: no target available, clips already marked failed by router
            
            elif failover_queue is not None:
                # Legacy fallback: direct failover queue (old behavior)
                if download_queued:
                    successfully_submitted = [c.get('clip_index') for c in clips[:i]]
                    if successfully_submitted:
                        download_queue.put({
                            'type': 'limit_clips',
                            'job_id': job_id,
                            'allowed_clips': set(successfully_submitted)
                        })
                    else:
                        download_queue.put({
                            'type': 'cancel',
                            'job_id': job_id
                        })
                    download_queue.put({
                        'type': 'shutdown_after_complete',
                        'job_id': job_id
                    })
                
                failover_data = {
                    'type': 'failover',
                    'job_id': job_id,
                    'original_job': job,
                    'remaining_clips': remaining_clips,
                    'failed_clip_index': clip_index,
                    'failed_account': account_name,
                    'clips_data': clips_data[i:],
                    'all_download_queues': all_download_queues,
                }
                
                if is_failover_to_standby:
                    failover_queue.put({
                        'type': 'failover_swap',
                        'failed_account': account_name,
                        'failover_data': failover_data,
                    })
                else:
                    failover_queue.put(failover_data)
                
                print(f"[{account_name}] ✓ Handed off via legacy failover", flush=True)
                return None
            else:
                # No failover available at all
                print(f"[{account_name}] ⚠️ No failover available, clip {clip_index} will be marked as failed", flush=True)
                permanently_failed_clips.add(clip_index)
                update_clip_status(clip['id'], 'failed', error_message="Generation failed, no failover available")
        else:
            # Clip passed immediate check - add to extended monitoring
            # Record success in health tracker (resets consecutive failure count)
            account_health.record_success(account_name)
            # Reset same-account retry counter on success — later clips should get fresh retry budget
            if failover_router is not None:
                with failover_router._lock:
                    key = (job_id, account_name)
                    if key in failover_router._retry_counts:
                        failover_router._retry_counts[key] = 0
            # Pass prompt for accurate dialogue extraction (same approach as download worker)
            dialogue = clip.get('dialogue_text', '')
            prompt = clip.get('prompt', '')
            failure_monitor.add_clip(clip_index, clip_submit_times[clip_index], dialogue_text=dialogue, prompt=prompt)
        
        # Queue for download after FIRST clip passes failure check
        # This ensures we don't start downloading from a failed project
        if not download_queued and not clip_failed:
            download_queue.put({
                'job_id': job_id,
                'project_url': project_url,
                'clips': clips,
                'clips_data': clips_data,
                'clip_project_map': clip_project_map,
                'clip_submit_times': clip_submit_times,
                'permanently_failed_clips': permanently_failed_clips,
                'downloaded_videos': downloaded_videos,  # Shared dict for continue mode
                'num_clips': len(clips),
                'submitted_at': datetime.now(),
                'temp_dir': temp_dir,
                'account_name': account_name,  # Track which account handles this
            })
            print(f"[{account_name}] ✓ Queued for download (parallel mode)", flush=True)
            download_queued = True
        
        if i < len(clips) - 1:
            # Human-like wait with micro-activities and failure monitoring
            delayed_failures = human_pacer.wait_between_clips(
                page, clip_number=i, total_clips=len(clips),
                failure_monitor=failure_monitor
            )
            if delayed_failures:
                    # Trigger failover for the failed clip(s) and all remaining clips
                    failed_clip_idx = delayed_failures[0]  # Handle first failure
                    # Find the position of the failed clip in our clips list
                    failed_pos = next((idx for idx, c in enumerate(clips) if c['clip_index'] == failed_clip_idx), None)
                    if failed_pos is not None:
                        remaining_clips = [clips[failed_pos]] + clips[i+1:]  # Failed clip + not-yet-submitted
                    else:
                        remaining_clips = clips[i+1:]  # Just not-yet-submitted
                    
                    print(f"\n{'='*50}", flush=True)
                    print(f"🔄 DELAYED FAILOVER TRIGGERED!", flush=True)
                    print(f"   Failed clip: {failed_clip_idx}", flush=True)
                    print(f"   Remaining clips: {len(remaining_clips)}", flush=True)
                    print(f"{'='*50}\n", flush=True)
                    
                    # Limit download to exclude the failed clip
                    if download_queued:
                        successfully_submitted = [c.get('clip_index') for c in clips[:i+1] if c.get('clip_index') != failed_clip_idx]
                        if successfully_submitted:
                            print(f"[{account_name}] Limiting download to clips {successfully_submitted} (excluding delayed failure)...", flush=True)
                            download_queue.put({
                                'type': 'limit_clips',
                                'job_id': job_id,
                                'allowed_clips': set(successfully_submitted)
                            })
                        
                        download_queue.put({
                            'type': 'shutdown_after_complete',
                            'job_id': job_id
                        })
                    
                    # Build failover data
                    failover_data = {
                        'type': 'failover',
                        'job_id': job_id,
                        'original_job': job,
                        'remaining_clips': remaining_clips,
                        'failed_clip_index': failed_clip_idx,
                        'failed_account': account_name,
                        'clips_data': [cd for cd in clips_data if cd['clip_index'] in [c['clip_index'] for c in remaining_clips]],
                        'all_download_queues': all_download_queues,
                    }
                    
                    # Use FailoverRouter if available, else legacy
                    if failover_router is not None:
                        result = failover_router.route_failover(
                            failed_account=account_name,
                            failover_data=failover_data,
                            job_id=job_id,
                        )
                        if result == 'routed':
                            return None
                    elif failover_queue is not None:
                        if is_failover_to_standby:
                            failover_queue.put({
                                'type': 'failover_swap',
                                'failed_account': account_name,
                                'failover_data': failover_data,
                            })
                        else:
                            failover_queue.put(failover_data)
                        return None
                    else:
                        # No failover available - handle ALL delayed failures
                        for failed_clip_idx in delayed_failures:
                            print(f"[{account_name}] ⚠️ DELAYED FAILURE detected for clip {failed_clip_idx}!", flush=True)
                            permanently_failed_clips.add(failed_clip_idx)
                            for c in clips:
                                if c['clip_index'] == failed_clip_idx:
                                    update_clip_status(c['id'], 'failed', error_message="Delayed generation failure")
                                    break
        
        prev_start_frame_key = start_frame_key
        prev_end_frame_key = end_frame_key
    
    # FINAL SWEEP: Check for any delayed failures before considering job complete
    # Wait a bit and do one final check on all monitored clips
    if failure_monitor.has_clips_to_monitor():
        print(f"[{account_name}] Doing final failure sweep (checking monitored clips)...", flush=True)
        time.sleep(3)  # Give a moment for any failures to show
        
        final_failures = failure_monitor.do_periodic_check(page, account_name)
        if final_failures:
            print(f"[{account_name}] ⚠️ FINAL SWEEP found {len(final_failures)} delayed failure(s): {final_failures}", flush=True)
            
            for failed_clip_idx in final_failures:
                if failover_queue is not None:
                    # Create failover for just this clip
                    failed_clip = next((c for c in clips if c['clip_index'] == failed_clip_idx), None)
                    if failed_clip:
                        print(f"\n{'='*50}")
                        print(f"🔄 FINAL SWEEP FAILOVER!")
                        print(f"   Failed clip: {failed_clip_idx}")
                        print(f"   Handing off to {'STANDBY' if is_failover_to_standby else 'other'} account...")
                        print(f"{'='*50}\n")
                        
                        # Limit download to exclude this clip
                        if download_queued:
                            allowed = [c.get('clip_index') for c in clips if c.get('clip_index') != failed_clip_idx and c.get('clip_index') not in permanently_failed_clips]
                            if allowed:
                                print(f"[{account_name}] Limiting download to clips {allowed}...", flush=True)
                                download_queue.put({
                                    'type': 'limit_clips',
                                    'job_id': job_id,
                                    'allowed_clips': set(allowed)
                                })
                        
                        # Build failover data
                        failover_data = {
                            'type': 'failover',
                            'job_id': job_id,
                            'original_job': job,
                            'remaining_clips': [failed_clip],
                            'failed_clip_index': failed_clip_idx,
                            'failed_account': account_name,
                            'clips_data': [cd for cd in clips_data if cd['clip_index'] == failed_clip_idx],
                            'all_download_queues': all_download_queues,
                        }
                        
                        # Send to standby manager or directly to another account
                        if is_failover_to_standby:
                            failover_queue.put({
                                'type': 'failover_swap',
                                'failed_account': account_name,
                                'failover_data': failover_data,
                            })
                        else:
                            failover_queue.put(failover_data)
                else:
                    # No failover - mark as permanently failed
                    permanently_failed_clips.add(failed_clip_idx)
                    for c in clips:
                        if c['clip_index'] == failed_clip_idx:
                            update_clip_status(c['id'], 'failed', error_message="Delayed generation failure (final sweep)")
                            break
    
    mark_job_submitted(cache, job_id)
    
    successful_submissions = len(clip_submit_times) - len(permanently_failed_clips)
    
    print(f"\n{'='*50}")
    print(f"ALL CLIPS SUBMITTED!")
    print(f"Account: {account_name}")
    print(f"Project: {project_url}")
    print(f"Successful: {successful_submissions}/{len(clips)}")
    if permanently_failed_clips:
        print(f"Permanently failed: {len(permanently_failed_clips)}")
    print(f"{'='*50}")
    
    return project_url


def process_job_submission(page, job, cache, download_queue):
    """Submit all clips for a job"""
    job_id = job['id']
    clips = job['clips']
    
    # IMMEDIATELY mark as processing to prevent duplicate pickup
    update_job_status(job_id, 'processing')
    
    print(f"\n{'='*60}")
    print(f"PROCESSING JOB: {job_id[:8]}...")
    print(f"Clips: {len(clips)}")
    print(f"{'='*60}")
    
    cached_job = get_cached_job(cache, job_id)
    
    if cached_job and cached_job.get('project_url'):
        project_url = cached_job['project_url']
        clips_done = cached_job.get('clips_submitted', [])
        print(f"✓ Resuming from cache: {project_url}")
        print(f"  Clips done: {clips_done}")
        
        page.goto(project_url, timeout=60000)
        page.wait_for_load_state("domcontentloaded", timeout=30000)
        time.sleep(5)
        ensure_logged_into_flow(page, "SUBMIT")
    else:
        # --- Match test_human_like.py flow exactly ---
        
        # [2/10] Navigate to Flow (SPA — preserve reCAPTCHA session)
        print(f"[SUBMIT] Navigating to Flow homepage...", flush=True)
        spa_navigate_to_flow_home(page, "SUBMIT")
        human_delay(2, 4)  # [page load] wait like test_human_like.py
        
        # Check if login is required
        ensure_logged_into_flow(page, "SUBMIT")
        check_and_dismiss_popup(page)
        
        # [3/10] Looking around the page (matching test_human_like.py)
        human_mouse_move(page)
        human_delay(1, 2)
        scroll_randomly(page)
        human_delay(0.5, 1)
        
        # [4/10] Click "New project" button
        dismiss_create_with_flow(page, "SUBMIT")
        human_click_element(page, "button:has-text('New project'), button:has(i:text('add_2')), button.sc-a38764c7-0", "New project button")
        human_delay(2, 3)  # [project creation] wait like test_human_like.py
        
        # Wait for URL to contain /project/
        try:
            page.wait_for_url("**/project/**", timeout=30000)
        except:
            # Fallback: poll for URL change
            print("[Flow] wait_for_url timed out, polling...")
            for _ in range(15):
                time.sleep(1)
                if "/project/" in page.url:
                    break
        
        time.sleep(2)
        project_url = page.url
        print(f"✓ Project URL: {project_url}")
        
        # Verify we got a valid project URL
        if "/project/" not in project_url:
            raise Exception(f"Failed to create project - URL: {project_url}")
        
        print(f"✓ Created project: {project_url}")
        human_delay(1, 2)  # Post-creation wait like test_human_like.py
        
        # Wait for project page to load
        check_and_dismiss_popup(page)
        ensure_videos_tab_selected(page)
        
        clips_done = []
        mark_job_started(cache, job_id, project_url, clips)
    
    # Update project URL in API (status already set to 'processing' at start)
    api_request("POST", f"/jobs/{job_id}/status", {"flow_project_url": project_url})
    
    temp_dir = tempfile.mkdtemp(prefix=f"flow_job_{job_id[:8]}_")
    frames_dir = os.path.join(temp_dir, "frames")
    os.makedirs(frames_dir, exist_ok=True)
    
    for clip in clips:
        clip_idx = clip['clip_index']
        
        # Try to download start frame
        if clip.get('start_frame_url'):
            local_path = os.path.join(frames_dir, f"start_{clip_idx}.png")
            # Try proxy URL first, fall back to R2 URL if available
            r2_fallback = clip.get('start_frame_r2_url') or clip.get('start_frame_key')
            if r2_fallback and not r2_fallback.startswith('http'):
                # It's a key, not a URL - construct URL (this may need adjustment based on your R2 setup)
                r2_fallback = None  # Can't construct URL from key alone
            
            result = download_frame(clip['start_frame_url'], local_path, r2_fallback)
            if result:
                clip['start_frame_local'] = result
            else:
                # Frame exists on server but we can't download it
                # Check if we have it cached locally from a previous run
                print(f"[Download] ⚠️ Can't download start frame for clip {clip_idx} - checking cache...")
                clip['start_frame_local'] = None
        
        # Try to download end frame  
        if clip.get('end_frame_url'):
            local_path = os.path.join(frames_dir, f"end_{clip_idx}.png")
            r2_fallback = clip.get('end_frame_r2_url') or clip.get('end_frame_key')
            if r2_fallback and not r2_fallback.startswith('http'):
                r2_fallback = None
            
            result = download_frame(clip['end_frame_url'], local_path, r2_fallback)
            if result:
                clip['end_frame_local'] = result
            else:
                print(f"[Download] ⚠️ Can't download end frame for clip {clip_idx} - checking cache...")
                clip['end_frame_local'] = None
    
    # Check if we have any clips without frames - this is a problem
    clips_without_frames = [c for c in clips if not c.get('start_frame_local') and c.get('start_frame_url')]
    if clips_without_frames:
        print(f"[Warning] {len(clips_without_frames)} clip(s) missing local frames - job may have been partially processed before")
        print(f"[Warning] Consider clearing the job from the server and resubmitting")
    
    # Get job context for prompt building
    job_language = job.get('language', 'English')
    job_duration = float(job.get('duration', '8'))
    job_voice_profile = job.get('voice_profile', '')
    
    clips_data = []
    for clip in clips:
        # Use API prompt if available, otherwise build with job context
        prompt = clip.get('prompt')
        if not prompt:
            prompt = build_flow_prompt(
                dialogue_line=clip.get('dialogue_text', ''),
                language=job_language,
                voice_profile=job_voice_profile,
                duration=job_duration,
            )
        clips_data.append({
            'clip_index': clip['clip_index'],
            'id': clip.get('id'),  # Include clip ID for failure status updates
            'start_frame': clip.get('start_frame_local'),
            'end_frame': clip.get('end_frame_local'),
            'start_frame_url': clip.get('start_frame_url'),
            'end_frame_url': clip.get('end_frame_url'),
            'start_frame_key': clip.get('start_frame_key'),
            'end_frame_key': clip.get('end_frame_key'),
            'prompt': prompt,
            'dialogue_text': clip.get('dialogue_text', ''),
        })
    
    # Initialize clip_project_map - all clips start in main project
    clip_project_map = {clip['clip_index']: project_url for clip in clips}
    print(f"[Flow] Initialized clip_project_map with {len(clip_project_map)} clips in main project", flush=True)
    
    # Build image pool for celebrity/policy filter handling
    image_pool, ordered_image_keys = build_image_pool(clips)
    blacklisted_images = set()
    print(f"[Flow] Image pool: {len(ordered_image_keys)} unique images: {[os.path.basename(k) for k in ordered_image_keys]}", flush=True)
    
    # Track submission times per clip for download timing
    clip_submit_times = {}
    
    # Track clips that need retry (failed immediately)
    clips_needing_retry = []
    
    # Track downloaded videos for continue mode (clip_index -> video_path)
    # This dict is shared with the download worker thread
    downloaded_videos = {}
    
    prev_start_frame_key = None
    prev_end_frame_key = None
    
    # Shared set for permanently failed clips - passed to download queue and updated during retries
    permanently_failed_clips = set()
    
    for i, clip in enumerate(clips):
        clip_index = clip['clip_index']
        
        if clip_index in clips_done:
            print(f"\n--- Clip {i+1}/{len(clips)} SKIPPED (cached) ---")
            prev_start_frame_key = clip.get('start_frame_key')
            prev_end_frame_key = clip.get('end_frame_key')
            continue
        
        print(f"\n--- Clip {i+1}/{len(clips)} ---")
        
        start_frame = clip.get('start_frame_local')
        end_frame = clip.get('end_frame_local')
        start_frame_key = clip.get('start_frame_key')
        end_frame_key = clip.get('end_frame_key')
        
        # Use API prompt if available, otherwise build with job context
        prompt = clip.get('prompt')
        if not prompt:
            prompt = build_flow_prompt(
                dialogue_line=clip.get('dialogue_text', ''),
                language=job_language,
                voice_profile=job_voice_profile,
                duration=job_duration,
            )
        
        has_new_start = (start_frame_key != prev_start_frame_key) if prev_start_frame_key else True
        has_new_end = (end_frame_key != prev_end_frame_key) if prev_end_frame_key else (end_frame_key is not None)
        
        # If this clip has an end frame but the previous clip didn't (or vice versa),
        # the frame slot layout changes in Flow's UI — must re-upload start frame
        prev_had_end = prev_end_frame_key is not None
        curr_has_end = end_frame_key is not None
        if curr_has_end != prev_had_end:
            has_new_start = True
            has_new_end = curr_has_end
        
        has_new_frames = has_new_start or has_new_end
        
        # Pre-check: if current frame is already blacklisted, reassign before trying upload
        if blacklisted_images:
            needs_reassign = False
            if start_frame_key in blacklisted_images:
                print(f"[Flow] Clip {i+1}: start_frame {os.path.basename(start_frame_key)} already blacklisted, reassigning...", flush=True)
                needs_reassign = True
            if end_frame_key in blacklisted_images:
                print(f"[Flow] Clip {i+1}: end_frame {os.path.basename(end_frame_key)} already blacklisted, reassigning...", flush=True)
                needs_reassign = True
            if needs_reassign:
                failed_count = reassign_clip_frames(clips, i, blacklisted_images, image_pool, ordered_image_keys)
                # Update local vars after reassignment
                start_frame = clip.get('start_frame_local')
                end_frame = clip.get('end_frame_local')
                start_frame_key = clip.get('start_frame_key')
                end_frame_key = clip.get('end_frame_key')
                # Recompute has_new_frames with updated keys
                has_new_start = (start_frame_key != prev_start_frame_key) if prev_start_frame_key else True
                has_new_end = (end_frame_key != prev_end_frame_key) if prev_end_frame_key else (end_frame_key is not None)
                has_new_frames = has_new_start or has_new_end
                if start_frame_key in blacklisted_images or end_frame_key in blacklisted_images:
                    print(f"[Flow] ❌ Clip {i+1}: all images blacklisted after reassign", flush=True)
                    update_clip_status(clip['id'], 'failed', error_message="All images blocked by content policy")
                    permanently_failed_clips.add(clip_index)
                    prev_start_frame_key = start_frame_key
                    prev_end_frame_key = end_frame_key
                    continue
        
        print(f"  start_frame_key: {start_frame_key}")
        print(f"  end_frame_key: {end_frame_key}")
        print(f"  prev_start: {prev_start_frame_key}, prev_end: {prev_end_frame_key}")
        print(f"  has_new_start={has_new_start}, has_new_end={has_new_end}, has_new_frames={has_new_frames}")
        
        if i == 0:
            # First clip - needs frames and full setup
            # --- Match test_human_like.py steps [5] through [10] ---
            check_and_dismiss_popup(page)
            
            # [5/10] Selecting mode (with mouse move + delay like test)
            human_mouse_move(page)
            human_delay(0.3, 0.6)
            variants_count = job.get('flow_variants_count', 2)
            print(f"[SUBMIT] Flow variants count from job config: {variants_count}", flush=True)
            select_frames_to_video_mode(page, variants_count=variants_count)
            ensure_batch_view_mode(page)
            human_delay(1, 2)
            
            # [6/10] Upload START frame (with mouse move + scroll before)
            human_mouse_move(page)
            scroll_randomly(page)
            human_delay(0.5, 1)
            
            if start_frame and end_frame:
                s_sz = os.path.getsize(start_frame) if os.path.exists(start_frame) else 0
                e_sz = os.path.getsize(end_frame) if os.path.exists(end_frame) else 0
                print(f"[Flow] Clip 1 frames: start={os.path.basename(start_frame)}({s_sz}), end={os.path.basename(end_frame)}({e_sz})", flush=True)
                upload_ok, rejected_which = upload_both_frames_with_policy_check(page, start_frame, end_frame, context="")
                
                if not upload_ok:
                    # Frame rejected by policy — blacklist and reassign
                    rejected_key = start_frame_key if rejected_which == 'start' else end_frame_key
                    blacklisted_images.add(rejected_key)
                    print(f"[Flow] ⚠️ Blacklisted image: {os.path.basename(rejected_key)}", flush=True)
                    
                    failed_count = reassign_clip_frames(clips, i, blacklisted_images, image_pool, ordered_image_keys)
                    if failed_count > 0:
                        print(f"[Flow] ❌ {failed_count} clips cannot be assigned — all images blacklisted", flush=True)
                    
                    # Update local vars from reassigned clip
                    start_frame = clip.get('start_frame_local')
                    end_frame = clip.get('end_frame_local')
                    start_frame_key = clip.get('start_frame_key')
                    end_frame_key = clip.get('end_frame_key')
                    
                    if start_frame_key in blacklisted_images or end_frame_key in blacklisted_images:
                        print(f"[Flow] ❌ Clip {i+1} cannot be generated — all images blacklisted", flush=True)
                        update_clip_status(clip['id'], 'failed', error_message="All images blocked by content policy")
                        permanently_failed_clips.add(clip_index)
                        prev_start_frame_key = start_frame_key
                        prev_end_frame_key = end_frame_key
                        continue
                    
                    # Retry upload — only upload the frame that was replaced
                    if rejected_which == 'start':
                        # START was rejected — try remaining images in a loop
                        retry_success = False
                        for retry_attempt in range(len(ordered_image_keys)):
                            print(f"[Flow] Retrying clip {i+1} (attempt {retry_attempt+1}): START={os.path.basename(start_frame)} + END={os.path.basename(end_frame)}", flush=True)
                            upload_ok2, rejected2 = upload_both_frames_with_policy_check(page, start_frame, end_frame, context="")
                            if upload_ok2:
                                retry_success = True
                                break
                            rejected_key2 = start_frame_key if rejected2 == 'start' else end_frame_key
                            blacklisted_images.add(rejected_key2)
                            print(f"[Flow] ⚠️ Blacklisted: {os.path.basename(rejected_key2)}. Trying next...", flush=True)
                            reassign_clip_frames(clips, i, blacklisted_images, image_pool, ordered_image_keys)
                            start_frame = clip.get('start_frame_local')
                            end_frame = clip.get('end_frame_local')
                            start_frame_key = clip.get('start_frame_key')
                            end_frame_key = clip.get('end_frame_key')
                            if start_frame_key in blacklisted_images or end_frame_key in blacklisted_images:
                                print(f"[Flow] ❌ No more images to try", flush=True)
                                break
                        
                        if not retry_success:
                            update_clip_status(clip['id'], 'failed', error_message="All images blocked by content policy")
                            permanently_failed_clips.add(clip_index)
                            prev_start_frame_key = start_frame_key
                            prev_end_frame_key = end_frame_key
                            continue
                    else:
                        # END was rejected, START already accepted — try remaining images in a loop
                        retry_success = False
                        for retry_attempt in range(len(ordered_image_keys)):
                            print(f"[Flow] Retrying clip {i+1} END (attempt {retry_attempt+1}): uploading END={os.path.basename(end_frame)}", flush=True)
                            result2, reason2 = click_frame_and_upload_with_policy_check(page, end_frame, is_end_frame=True, context="")
                            if result2:
                                retry_success = True
                                break
                            if reason2 == 'policy':
                                blacklisted_images.add(end_frame_key)
                                print(f"[Flow] ⚠️ Blacklisted: {os.path.basename(end_frame_key)}. Trying next...", flush=True)
                                reassign_clip_frames(clips, i, blacklisted_images, image_pool, ordered_image_keys)
                                end_frame = clip.get('end_frame_local')
                                end_frame_key = clip.get('end_frame_key')
                                if end_frame_key in blacklisted_images:
                                    print(f"[Flow] ❌ No more images to try for END frame", flush=True)
                                    break
                            else:
                                print(f"[Flow] ❌ END retry failed (no buttons).", flush=True)
                                break
                        
                        if not retry_success:
                            update_clip_status(clip['id'], 'failed', error_message="All images blocked by content policy")
                            permanently_failed_clips.add(clip_index)
                            prev_start_frame_key = start_frame_key
                            prev_end_frame_key = end_frame_key
                            continue
            elif start_frame:
                result, reason = click_frame_and_upload_with_policy_check(page, start_frame, is_end_frame=False, context="")
                if not result:
                    if reason == 'no_buttons':
                        print(f"[Flow] No START buttons — frame already loaded, continuing", flush=True)
                    else:
                        blacklisted_images.add(start_frame_key)
                        print(f"[Flow] ⚠️ Blacklisted START image: {os.path.basename(start_frame_key)}", flush=True)
                        retry_success = False
                        for retry_attempt in range(len(ordered_image_keys)):
                            reassign_clip_frames(clips, i, blacklisted_images, image_pool, ordered_image_keys)
                            start_frame = clip.get('start_frame_local')
                            start_frame_key = clip.get('start_frame_key')
                            if start_frame_key in blacklisted_images:
                                print(f"[Flow] ❌ No more images to try for START", flush=True)
                                break
                            print(f"[Flow] Retrying START (attempt {retry_attempt+1}): {os.path.basename(start_frame_key)}", flush=True)
                            result2, reason2 = click_frame_and_upload_with_policy_check(page, start_frame, is_end_frame=False, context="")
                            if result2:
                                retry_success = True
                                break
                            if reason2 == 'policy':
                                blacklisted_images.add(start_frame_key)
                                print(f"[Flow] ⚠️ Blacklisted: {os.path.basename(start_frame_key)}. Trying next...", flush=True)
                            else:
                                break
                        if not retry_success:
                            update_clip_status(clip['id'], 'failed', error_message="All images blocked by content policy")
                            permanently_failed_clips.add(clip_index)
                            prev_start_frame_key = start_frame_key
                            prev_end_frame_key = end_frame_key
                            continue
            elif end_frame:
                result, reason = click_frame_and_upload_with_policy_check(page, end_frame, is_end_frame=True, context="")
                if not result:
                    if reason == 'no_buttons':
                        print(f"[Flow] No END buttons — frame already loaded, continuing", flush=True)
                    else:
                        blacklisted_images.add(end_frame_key)
                        print(f"[Flow] ⚠️ Blacklisted END image: {os.path.basename(end_frame_key)}", flush=True)
                        retry_success = False
                        for retry_attempt in range(len(ordered_image_keys)):
                            reassign_clip_frames(clips, i, blacklisted_images, image_pool, ordered_image_keys)
                            end_frame = clip.get('end_frame_local')
                            end_frame_key = clip.get('end_frame_key')
                            if end_frame_key in blacklisted_images:
                                print(f"[Flow] ❌ No more images to try for END", flush=True)
                                break
                            print(f"[Flow] Retrying END (attempt {retry_attempt+1}): {os.path.basename(end_frame_key)}", flush=True)
                            result2, reason2 = click_frame_and_upload_with_policy_check(page, end_frame, is_end_frame=True, context="")
                            if result2:
                                retry_success = True
                                break
                            if reason2 == 'policy':
                                blacklisted_images.add(end_frame_key)
                                print(f"[Flow] ⚠️ Blacklisted: {os.path.basename(end_frame_key)}. Trying next...", flush=True)
                            else:
                                break
                        if not retry_success:
                            update_clip_status(clip['id'], 'failed', error_message="All images blocked by content policy")
                            permanently_failed_clips.add(clip_index)
                            prev_start_frame_key = start_frame_key
                            prev_end_frame_key = end_frame_key
                            continue
            # [8/10] Enter prompt (with mouse move + scroll before, like test)
            human_delay(1, 2)
            human_mouse_move(page)
            scroll_randomly(page)
            
            fill_prompt_textarea(page, prompt)
            print(f"✓ Entered prompt: {prompt[:50]}...")
            
            # Wait 8-12s after prompt (test_human_like.py waits 10s)
            print(f"  ⏳ Waiting after prompt entry...", flush=True)
            time.sleep(random.uniform(8, 12))
            
            # [9/10] Pre-generate look-around (matching test)
            human_mouse_move(page)
            human_delay(1, 2)
            scroll_randomly(page)
            human_delay(0.5, 1)
            
            # [10/10] Click Generate — wait for button to be enabled first
            for _w in range(30):
                if is_generate_button_enabled(page):
                    break
                if _w == 0:
                    print("  Waiting for Generate button to be enabled...", flush=True)
                time.sleep(1)
            human_delay(0.5, 1.5)
            human_click_element(page, page.locator("button:has(i:text('arrow_forward'))").first, "Generate button", timeout=30000)
            print("✓ Clicked Generate for clip 1", flush=True)
            human_delay(3, 6)
            
        elif has_new_frames:
            print(f"[Flow] Clip {i+1}: New frames detected, uploading...")
            
            # Look around before upload (like test_human_like.py between steps)
            human_mouse_move(page)
            scroll_randomly(page)
            human_delay(0.5, 1)
            
            # Upload new frames
            s_img = start_frame if (has_new_start and start_frame) else None
            e_img = end_frame if (has_new_end and end_frame) else None
            s_size = os.path.getsize(s_img) if s_img and os.path.exists(s_img) else 0
            e_size = os.path.getsize(e_img) if e_img and os.path.exists(e_img) else 0
            print(f"[Flow] Clip {i+1} frames: start={os.path.basename(s_img) if s_img else 'None'}({s_size}), end={os.path.basename(e_img) if e_img else 'None'}({e_size})", flush=True)
            if s_img and e_img:
                upload_ok, rejected_which = upload_both_frames_with_policy_check(page, s_img, e_img, context=f"Clip {i+1}")
                
                if not upload_ok:
                    rejected_key = start_frame_key if rejected_which == 'start' else end_frame_key
                    blacklisted_images.add(rejected_key)
                    print(f"[Flow] ⚠️ Blacklisted image: {os.path.basename(rejected_key)}", flush=True)
                    
                    failed_count = reassign_clip_frames(clips, i, blacklisted_images, image_pool, ordered_image_keys)
                    if failed_count > 0:
                        print(f"[Flow] ❌ {failed_count} clips cannot be assigned — all images blacklisted", flush=True)
                    
                    start_frame = clip.get('start_frame_local')
                    end_frame = clip.get('end_frame_local')
                    start_frame_key = clip.get('start_frame_key')
                    end_frame_key = clip.get('end_frame_key')
                    s_img = start_frame
                    e_img = end_frame
                    
                    if start_frame_key in blacklisted_images or end_frame_key in blacklisted_images:
                        print(f"[Flow] ❌ Clip {i+1} cannot be generated — all images blacklisted", flush=True)
                        update_clip_status(clip['id'], 'failed', error_message="All images blocked by content policy")
                        permanently_failed_clips.add(clip_index)
                        prev_start_frame_key = start_frame_key
                        prev_end_frame_key = end_frame_key
                        continue
                    
                    if rejected_which == 'start':
                        retry_success = False
                        for retry_attempt in range(len(ordered_image_keys)):
                            print(f"[Flow] Retrying clip {i+1} (attempt {retry_attempt+1}): START + END", flush=True)
                            upload_ok2, rejected2 = upload_both_frames_with_policy_check(page, s_img, e_img, context=f"Clip {i+1}")
                            if upload_ok2:
                                retry_success = True
                                break
                            rejected_key2 = start_frame_key if rejected2 == 'start' else end_frame_key
                            blacklisted_images.add(rejected_key2)
                            print(f"[Flow] ⚠️ Blacklisted: {os.path.basename(rejected_key2)}. Trying next...", flush=True)
                            reassign_clip_frames(clips, i, blacklisted_images, image_pool, ordered_image_keys)
                            start_frame = clip.get('start_frame_local')
                            end_frame = clip.get('end_frame_local')
                            start_frame_key = clip.get('start_frame_key')
                            end_frame_key = clip.get('end_frame_key')
                            s_img = start_frame
                            e_img = end_frame
                            if start_frame_key in blacklisted_images or end_frame_key in blacklisted_images:
                                print(f"[Flow] ❌ No more images to try", flush=True)
                                break
                        
                        if not retry_success:
                            update_clip_status(clip['id'], 'failed', error_message="All images blocked by content policy")
                            permanently_failed_clips.add(clip_index)
                            prev_start_frame_key = start_frame_key
                            prev_end_frame_key = end_frame_key
                            continue
                    else:
                        # END was rejected, START already accepted — try remaining images
                        retry_success = False
                        for retry_attempt in range(len(ordered_image_keys)):
                            print(f"[Flow] Retrying clip {i+1} END (attempt {retry_attempt+1}): END={os.path.basename(e_img)}", flush=True)
                            result2, reason2 = click_frame_and_upload_with_policy_check(page, e_img, is_end_frame=True, context=f"Clip {i+1}")
                            if result2:
                                retry_success = True
                                break
                            if reason2 == 'policy':
                                blacklisted_images.add(end_frame_key)
                                print(f"[Flow] ⚠️ Blacklisted: {os.path.basename(end_frame_key)}. Trying next...", flush=True)
                                reassign_clip_frames(clips, i, blacklisted_images, image_pool, ordered_image_keys)
                                end_frame = clip.get('end_frame_local')
                                end_frame_key = clip.get('end_frame_key')
                                e_img = end_frame
                                if end_frame_key in blacklisted_images:
                                    print(f"[Flow] ❌ No more images to try for END frame", flush=True)
                                    break
                            else:
                                print(f"[Flow] ❌ END retry failed (no buttons).", flush=True)
                                break
                        
                        if not retry_success:
                            update_clip_status(clip['id'], 'failed', error_message="All images blocked by content policy")
                            permanently_failed_clips.add(clip_index)
                            prev_start_frame_key = start_frame_key
                            prev_end_frame_key = end_frame_key
                            continue
            elif s_img:
                result, reason = click_frame_and_upload_with_policy_check(page, s_img, is_end_frame=False, context=f"Clip {i+1}")
                if not result:
                    if reason == 'no_buttons':
                        print(f"[Flow] No START buttons — frame already loaded, continuing", flush=True)
                    else:
                        blacklisted_images.add(start_frame_key)
                        print(f"[Flow] ⚠️ Blacklisted START image: {os.path.basename(start_frame_key)}", flush=True)
                        retry_success = False
                        for retry_attempt in range(len(ordered_image_keys)):
                            reassign_clip_frames(clips, i, blacklisted_images, image_pool, ordered_image_keys)
                            start_frame = clip.get('start_frame_local')
                            start_frame_key = clip.get('start_frame_key')
                            s_img = start_frame
                            if start_frame_key in blacklisted_images:
                                print(f"[Flow] ❌ No more images to try for START", flush=True)
                                break
                            print(f"[Flow] Retrying START (attempt {retry_attempt+1}): {os.path.basename(start_frame_key)}", flush=True)
                            result2, reason2 = click_frame_and_upload_with_policy_check(page, s_img, is_end_frame=False, context=f"Clip {i+1}")
                            if result2:
                                retry_success = True
                                break
                            if reason2 == 'policy':
                                blacklisted_images.add(start_frame_key)
                                print(f"[Flow] ⚠️ Blacklisted: {os.path.basename(start_frame_key)}. Trying next...", flush=True)
                            else:
                                break
                        if not retry_success:
                            update_clip_status(clip['id'], 'failed', error_message="All images blocked by content policy")
                            permanently_failed_clips.add(clip_index)
                            prev_start_frame_key = start_frame_key
                            prev_end_frame_key = end_frame_key
                            continue
            elif e_img:
                result, reason = click_frame_and_upload_with_policy_check(page, e_img, is_end_frame=True, context=f"Clip {i+1}")
                if not result:
                    if reason == 'no_buttons':
                        print(f"[Flow] No END buttons — frame already loaded, continuing", flush=True)
                    else:
                        blacklisted_images.add(end_frame_key)
                        print(f"[Flow] ⚠️ Blacklisted END image: {os.path.basename(end_frame_key)}", flush=True)
                        retry_success = False
                        for retry_attempt in range(len(ordered_image_keys)):
                            reassign_clip_frames(clips, i, blacklisted_images, image_pool, ordered_image_keys)
                            end_frame = clip.get('end_frame_local')
                            end_frame_key = clip.get('end_frame_key')
                            e_img = end_frame
                            if end_frame_key in blacklisted_images:
                                print(f"[Flow] ❌ No more images to try for END", flush=True)
                                break
                            print(f"[Flow] Retrying END (attempt {retry_attempt+1}): {os.path.basename(end_frame_key)}", flush=True)
                            result2, reason2 = click_frame_and_upload_with_policy_check(page, e_img, is_end_frame=True, context=f"Clip {i+1}")
                            if result2:
                                retry_success = True
                                break
                            if reason2 == 'policy':
                                blacklisted_images.add(end_frame_key)
                                print(f"[Flow] ⚠️ Blacklisted: {os.path.basename(end_frame_key)}. Trying next...", flush=True)
                            else:
                                break
                        if not retry_success:
                            update_clip_status(clip['id'], 'failed', error_message="All images blocked by content policy")
                            permanently_failed_clips.add(clip_index)
                            prev_start_frame_key = start_frame_key
                            prev_end_frame_key = end_frame_key
                            continue
            # Pre-prompt look-around
            human_delay(1, 2)
            human_mouse_move(page)
            scroll_randomly(page)
            
            # Enter prompt
            fill_prompt_textarea(page, prompt)
            print(f"✓ Clip {i+1}: Entered prompt: {prompt[:50]}...")
            
            # Post-prompt wait (8-12s like test)
            print(f"  ⏳ Waiting after prompt entry...", flush=True)
            time.sleep(random.uniform(8, 12))
            
            # Pre-generate look-around
            human_mouse_move(page)
            human_delay(1, 2)
            scroll_randomly(page)
            human_delay(0.5, 1)
            
            # Click Generate — wait for button to be enabled first
            for _w in range(30):
                if is_generate_button_enabled(page):
                    break
                if _w == 0:
                    print(f"  Waiting for Generate button to be enabled...", flush=True)
                time.sleep(1)
            human_delay(0.5, 1.5)
            human_click_element(page, page.locator("button:has(i:text('arrow_forward'))").first, "Generate button", timeout=30000)
            print(f"✓ Clicked Generate for clip {i+1}", flush=True)
            human_delay(3, 6)
            
        else:
            # No new frames - just reuse and change prompt
            print(f"\n--- Clip {i+1}/{len(clips)} (reuse frames) ---")
            
            # Human-like behavior before reuse prompt click
            human_pre_action(page, "reuse prompt")
            
            # Use new robust reuse function that waits for Generate button to be enabled
            # and retries with page refresh if it gets stuck
            try:
                click_reuse_and_generate(page, prompt, i+1, "Flow", max_retries=3, wait_timeout=60)
            except Exception as e:
                print(f"[Flow] ❌ REUSE+GENERATE FAILED at clip {clip_index}: {e}", flush=True)
                # Track for retry
                clips_needing_retry.append({
                    'clip_index': clip_index,
                    'clip_data': clips_data[i] if i < len(clips_data) else None
                })
                continue  # Skip to next clip
            
            time.sleep(3)
        
        update_clip_status(clip['id'], 'generating')
        mark_clip_submitted(cache, job_id, clip_index)
        
        # Record submission time for download timing
        clip_submit_times[clip_index] = datetime.now()
        print(f"[Flow] Clip {clip_index} submitted at {clip_submit_times[clip_index].strftime('%H:%M:%S')}", flush=True)
        
        # Wait 3 seconds then check for immediate failure
        time.sleep(FAILURE_CHECK_DELAY)
        clip_failed = check_recent_clip_failure(page, data_index=1, clip_num=i)
        if clip_failed:
            print(f"[Flow] ⚠️ Clip {clip_index} failed immediately, will retry after all clips submitted...", flush=True)
            # Track this clip for retry - we'll create retry projects after all submissions
            clips_needing_retry.append({
                'clip_index': clip_index,
                'clip_data': clips_data[i] if i < len(clips_data) else None
            })
        
        # Queue for download after FIRST clip that passes failure check
        # This ensures we don't start the download browser for a failed project
        if not clip_failed and not hasattr(process_job_submission, '_download_queued'):
            download_queue.put({
                'job_id': job_id,
                'project_url': project_url,
                'clips': clips,
                'clips_data': clips_data,
                'clip_project_map': clip_project_map,
                'clip_submit_times': clip_submit_times,
                'permanently_failed_clips': permanently_failed_clips,
                'downloaded_videos': downloaded_videos,
                'num_clips': len(clips),
                'submitted_at': datetime.now(),
                'temp_dir': temp_dir
            })
            print(f"[Flow] ✓ Queued for download after clip {clip_index} passed check (parallel mode)", flush=True)
            process_job_submission._download_queued = True
        
        if i < len(clips) - 1:
            # Human-like wait with micro-activities
            if not hasattr(process_job_submission, '_pacer'):
                process_job_submission._pacer = HumanPacer(account_name="Flow")
            process_job_submission._pacer.wait_between_clips(
                page, clip_number=i, total_clips=len(clips)
            )
        
        prev_start_frame_key = start_frame_key
        prev_end_frame_key = end_frame_key
    
    mark_job_submitted(cache, job_id)
    
    # Count successful submissions (not failed)
    successful_submissions = len(clip_submit_times) - len(clips_needing_retry)
    
    print(f"\n{'='*50}")
    print(f"ALL CLIPS ATTEMPTED!")
    print(f"Project: {project_url}")
    print(f"Successful: {successful_submissions}/{len(clips)}")
    print(f"Failed (need retry): {len(clips_needing_retry)}")
    print(f"{'='*50}")
    
    # NOTE: permanently_failed_clips is defined before the loop and shared with download queue
    
    # Handle any clips that failed immediately - create retry projects for them
    if clips_needing_retry:
        print(f"\n[Flow] Creating retry projects for {len(clips_needing_retry)} failed clip(s)...", flush=True)
        
        for retry_info in clips_needing_retry:
            clip_index = retry_info['clip_index']
            clip_data = retry_info['clip_data']
            
            if not clip_data:
                print(f"[Flow] ⚠️ No clip data for clip {clip_index}, skipping retry", flush=True)
                permanently_failed_clips.add(clip_index)
                continue
            
            print(f"\n[Flow] Creating retry project for clip {clip_index}...", flush=True)
            
            # Create new project for this failed clip (with retry logic built-in)
            retry_project_url, retry_success = _create_retry_project_for_clip(page, clip_data, max_retries=2)
            
            if retry_project_url and retry_success:
                # Update the clip_project_map so download worker knows where to find this clip
                clip_project_map[clip_index] = retry_project_url
                # Update submission time for the retry
                clip_submit_times[clip_index] = datetime.now()
                print(f"[Flow] ✓ Clip {clip_index} retry submitted in: {retry_project_url}", flush=True)
                
                # Navigate back to main project for next retry
                if clips_needing_retry.index(retry_info) < len(clips_needing_retry) - 1:
                    print(f"[Flow] Returning to main project...", flush=True)
                    page.goto(project_url, timeout=30000)
                    time.sleep(3)
                    check_and_dismiss_popup(page)
            else:
                print(f"[Flow] ❌ Clip {clip_index} failed permanently after all retry attempts", flush=True)
                permanently_failed_clips.add(clip_index)
                # Mark clip as failed via API
                clip_id = clip_data.get('id') or clip_data.get('clip_id')
                if clip_id:
                    update_clip_status(clip_id, 'failed', error_message="Generation failed after multiple retries")
        
        # Summary
        retry_success_count = len(clips_needing_retry) - len(permanently_failed_clips)
        print(f"\n[Flow] Retry summary:", flush=True)
        print(f"  Retried successfully: {retry_success_count}", flush=True)
        print(f"  Permanently failed: {len(permanently_failed_clips)}", flush=True)
        
        if permanently_failed_clips:
            print(f"  Failed clip indices: {sorted(permanently_failed_clips)}", flush=True)
        
        print(f"\n[Flow] Updated clip_project_map:", flush=True)
        for idx, url in clip_project_map.items():
            if idx in permanently_failed_clips:
                marker = " (PERMANENTLY FAILED)"
            elif url != project_url:
                marker = " (RETRY)"
            else:
                marker = ""
            print(f"  Clip {idx}: {url[:60]}...{marker}", flush=True)
    
    # NOTE: Download was already queued after first clip (parallel mode)
    # clip_project_map has been updated with retry URLs - download worker will see these updates
    # If download was never queued (all initial clips failed), queue it now after retries
    if not hasattr(process_job_submission, '_download_queued'):
        # All initial clips failed, but retries may have succeeded
        any_success = len(permanently_failed_clips) < len(clips)
        if any_success:
            download_queue.put({
                'job_id': job_id,
                'project_url': project_url,
                'clips': clips,
                'clips_data': clips_data,
                'clip_project_map': clip_project_map,
                'clip_submit_times': clip_submit_times,
                'permanently_failed_clips': permanently_failed_clips,
                'downloaded_videos': downloaded_videos,
                'num_clips': len(clips),
                'submitted_at': datetime.now(),
                'temp_dir': temp_dir
            })
            print(f"[Flow] ✓ Queued for download after retries (delayed start)", flush=True)
            process_job_submission._download_queued = True
    
    # Reset the flag for next job
    if hasattr(process_job_submission, '_download_queued'):
        del process_job_submission._download_queued
    
    return project_url


# ============================================================
# MAIN LOOP
# ============================================================

def check_api_connection():
    """Check if API is reachable"""
    try:
        response = requests.get(f"{WEB_APP_URL}{API_PATH_PREFIX}/health", timeout=10)
        return response.status_code == 200
    except:
        return False


# ============================================================
# MULTI-ACCOUNT WORKER
# ============================================================

# ============================================================
# STANDBY ACCOUNT MANAGER
# ============================================================

class StandbyAccountManager(threading.Thread):
    """
    Manages standby accounts and coordinates account swapping when active accounts fail.
    
    When an active account experiences failures, instead of routing to another active account,
    this manager:
    1. Shuts down the failed account's browser
    2. Pops a fresh account from the standby queue
    3. Starts new AccountWorker and DownloadWorker for it
    4. Routes the failed clips to the new account
    
    This keeps the other active account working uninterrupted on its own clips.
    """
    
    def __init__(self, standby_accounts, swap_request_queue, cache, 
                 active_workers, active_download_workers,
                 account_job_queues, account_download_queues):
        super().__init__(daemon=True)
        self.standby_queue = Queue()
        for acc in standby_accounts:
            self.standby_queue.put(acc)
        self.swap_request_queue = swap_request_queue
        self.cache = cache
        self.active_workers = active_workers  # List of AccountWorker
        self.active_download_workers = active_download_workers  # List of DownloadWorker
        self.account_job_queues = account_job_queues  # account_name -> Queue
        self.account_download_queues = account_download_queues  # account_name -> Queue
        self.lock = threading.Lock()
        
        # Build name -> worker mappings for quick lookup
        self.account_workers_by_name = {}  # account_name -> AccountWorker
        self.download_workers_by_name = {}  # account_name -> DownloadWorker
        for worker in active_workers:
            self.account_workers_by_name[worker.name] = worker
        for worker in active_download_workers:
            # Download worker names are like "Account1-DOWNLOAD"
            base_name = worker.account_name.replace("-DOWNLOAD", "")
            self.download_workers_by_name[base_name] = worker
        
    def get_standby_count(self):
        """Return number of standby accounts available"""
        return self.standby_queue.qsize()
    
    def has_standby(self):
        """Check if there are standby accounts available"""
        return not self.standby_queue.empty()
    
    def run(self):
        """Main loop - process swap requests"""
        print(f"[StandbyManager] Started with {self.standby_queue.qsize()} standby account(s)", flush=True)
        
        while True:
            try:
                # Wait for swap request
                request = self.swap_request_queue.get()
                
                if request.get('type') == 'shutdown':
                    print("[StandbyManager] Shutdown requested", flush=True)
                    break
                
                if request.get('type') == 'failover_swap':
                    self._handle_failover_swap(request)
                    
            except Exception as e:
                print(f"[StandbyManager] Error: {e}", flush=True)
                import traceback
                traceback.print_exc()
                time.sleep(1)
    
    def _handle_failover_swap(self, request):
        """Handle a failover swap request - start new account for failed clips"""
        failed_account = request.get('failed_account')
        failover_data = request.get('failover_data')
        job_id = failover_data.get('job_id', 'unknown')[:8]
        
        print(f"\n[StandbyManager] {'='*50}", flush=True)
        print(f"[StandbyManager] SWAP REQUEST from {failed_account}", flush=True)
        print(f"[StandbyManager] Job: {job_id}...", flush=True)
        print(f"[StandbyManager] Standby accounts available: {self.standby_queue.qsize()}", flush=True)
        
        # Check if we have standby accounts
        if self.standby_queue.empty():
            print(f"[StandbyManager] ❌ No standby accounts available!", flush=True)
            print(f"[StandbyManager] Marking clips as permanently failed", flush=True)
            # Mark clips as failed since no account can take over
            remaining_clips = failover_data.get('remaining_clips', [])
            for clip in remaining_clips:
                clip_id = clip.get('id')
                if clip_id:
                    update_clip_status(clip_id, 'failed', 
                        error_message="No standby accounts available for failover")
            print(f"[StandbyManager] {'='*50}\n", flush=True)
            return
        
        # Pop standby account
        new_account = self.standby_queue.get()
        new_account_name = new_account['name']
        
        print(f"[StandbyManager] ✓ Activating standby account: {new_account_name}", flush=True)
        
        # Shutdown the failed account's SUBMISSION browser (download can continue)
        if failed_account in self.account_workers_by_name:
            failed_worker = self.account_workers_by_name[failed_account]
            print(f"[StandbyManager] 🛑 Shutting down {failed_account} submission browser...", flush=True)
            failed_worker.request_shutdown()
            # Remove from tracking
            del self.account_workers_by_name[failed_account]
            if failed_worker in self.active_workers:
                self.active_workers.remove(failed_worker)
        
        with self.lock:
            # Create download queue for new account
            download_queue = Queue()
            self.account_download_queues[new_account_name] = download_queue
            
            # Create job queue for new account
            job_queue = Queue()
            self.account_job_queues[new_account_name] = job_queue
            
            # Create download worker for new account
            download_folder = new_account.get('download_folder', f"./flow_download_{new_account_name.lower()}")
            os.makedirs(download_folder, exist_ok=True)
            
            download_worker = DownloadWorker(
                download_queue,
                self.cache,
                session_folder=download_folder,
                account_name=f"{new_account_name}-DOWNLOAD",
                proxy=new_account.get('proxy'),
                submit_session_folder=new_account.get('session_folder'),
            )
            download_worker.start()
            self.active_download_workers.append(download_worker)
            self.download_workers_by_name[new_account_name] = download_worker
            
            # Create account worker for new account
            # New account's failover goes to swap manager (not another active account)
            account_worker = AccountWorker(
                new_account,
                download_queue,
                self.cache,
                job_queue,
                failover_queue=self.swap_request_queue,  # Route failovers to swap manager
                all_download_queues=self.account_download_queues,
                is_failover_to_standby=True,  # Flag to use swap manager for failover
            )
            account_worker.start()
            self.active_workers.append(account_worker)
            self.account_workers_by_name[new_account_name] = account_worker
            
            # Register with health tracker and failover router
            account_health.register_account(new_account_name)
            if failover_router is not None:
                failover_router.account_job_queues[new_account_name] = job_queue
                failover_router.account_download_queues[new_account_name] = download_queue
            
            print(f"[StandbyManager] ✓ {new_account_name} browser starting...", flush=True)
            
            # Wait for the new account to be ready
            if account_worker.ready_flag.wait(timeout=120):
                print(f"[StandbyManager] ✓ {new_account_name} ready!", flush=True)
                
                # Route the failover job to the new account
                job_queue.put({
                    'type': 'failover',
                    'job_id': failover_data['job_id'],
                    'original_job': failover_data['original_job'],
                    'remaining_clips': failover_data['remaining_clips'],
                    'failed_clip_index': failover_data['failed_clip_index'],
                    'failed_account': failed_account,
                    'clips_data': failover_data.get('clips_data', []),
                    'all_download_queues': self.account_download_queues,
                })
                
                print(f"[StandbyManager] ✓ Failover job routed to {new_account_name}", flush=True)
            else:
                print(f"[StandbyManager] ❌ {new_account_name} failed to start in time!", flush=True)
                # Mark clips as failed
                for clip in failover_data.get('remaining_clips', []):
                    clip_id = clip.get('id')
                    if clip_id:
                        update_clip_status(clip_id, 'failed', 
                            error_message="Standby account failed to start")
        
        print(f"[StandbyManager] Remaining standby accounts: {self.standby_queue.qsize()}", flush=True)
        print(f"[StandbyManager] {'='*50}\n", flush=True)


class AccountWorker(threading.Thread):
    """Worker thread for a single account - handles job submission"""
    
    def __init__(self, account_config, download_queue, cache, job_queue, 
                 failover_queue=None, all_download_queues=None, account_name_to_index=None,
                 is_failover_to_standby=False):
        super().__init__(daemon=True)
        self.account = account_config
        self.name = account_config['name']
        self.session_folder = account_config['session_folder']
        self.proxy = account_config.get('proxy')
        self.download_queue = download_queue
        self.cache = cache
        self.job_queue = job_queue  # Shared queue for jobs to process
        self.failover_queue = failover_queue  # Queue to send failed jobs to other account OR swap manager
        self.all_download_queues = all_download_queues or {}  # account_name -> download_queue
        self.account_name_to_index = account_name_to_index or {}  # account_name -> index
        self.is_failover_to_standby = is_failover_to_standby  # If True, failover goes to swap manager
        self.ready_flag = threading.Event()
        self.shutdown_event = threading.Event()  # Signal to gracefully shutdown
        self.page = None
        self.browser = None
    
    def request_shutdown(self):
        """Request the worker to shutdown gracefully"""
        print(f"[{self.name}] Shutdown requested", flush=True)
        self.shutdown_event.set()
    
    def run(self):
        """Main worker loop"""
        print(f"[{self.name}] Starting browser...", flush=True)
        
        with sync_playwright() as p:
            # Build launch args - match test_human_like.py which keeps working
            # Only the bare minimum flags; extras create detectable fingerprint
            launch_args = [
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--force-variation-ids=3300115,3300134,3313321,3328827,3330196,3362821',
            ]
            
            # Add proxy if configured
            proxy_config = parse_proxy_url(self.proxy)
            if proxy_config:
                print(f"[{self.name}] Using proxy: {proxy_config['server']}", flush=True)
                # Only add cert-error bypass when routing through a proxy
                launch_args.append('--ignore-certificate-errors')
                
                # Create proxy auth extension ONLY when proxy needs credentials
                ext_dir = os.path.join(BASE_DIR, f".proxy_auth_ext_{self.name}")
                auth_ext = create_proxy_auth_extension(self.proxy, ext_dir)
                if auth_ext:
                    launch_args.extend([
                        f'--disable-extensions-except={auth_ext}',
                        f'--load-extension={auth_ext}',
                    ])
            else:
                print(f"[{self.name}] No proxy - direct connection (matching test_human_like.py)", flush=True)
            
            # Select stealth script based on browser mode
            # Real Chrome (channel='chrome') has native plugins/runtime - don't fake them
            # Stealth handled by Patchright natively (no init script needed)
            
            if BROWSER_MODE == "stealth":
                acct_launch_kwargs = {
                    'user_data_dir': self.session_folder,
                    'channel': 'chrome',
                    'ignore_default_args': ['--enable-automation'],
                    'headless': False,
                    'viewport': {"width": 1280, "height": 720},
                    'args': launch_args,
                }
                if proxy_config:
                    acct_launch_kwargs['proxy'] = proxy_config
                self.browser = p.chromium.launch_persistent_context(**acct_launch_kwargs)
            else:
                acct_launch_kwargs = {
                    'user_data_dir': self.session_folder,
                    'headless': False,
                    'viewport': {"width": 1280, "height": 500},
                }
                if proxy_config:
                    acct_launch_kwargs['proxy'] = proxy_config
                self.browser = p.firefox.launch_persistent_context(**acct_launch_kwargs)
            
            # Match test_human_like.py
            self.page = self.browser.pages[0] if self.browser.pages else self.browser.new_page()
            # Note: Patchright handles stealth natively — no init script needed
            
            print(f"[{self.name}] ✓ Browser started", flush=True)
            
            # Warm up Chrome — sync variations seed
            chrome_warmup(self.page)
            
            # Match test_human_like.py startup - navigate, look around
            print(f"[{self.name}] Navigating to Flow...", flush=True)
            self.page.goto(FLOW_HOME_URL)
            human_delay(2, 4)
            
            # Look around like a human
            human_mouse_move(self.page)
            human_delay(1, 2)
            scroll_randomly(self.page)
            human_delay(0.5, 1)
            
            # Check if login needed
            current_url = self.page.url.lower()
            if "accounts.google" in current_url:
                print(f"[{self.name}] Login page detected, waiting...", flush=True)
                for _v in range(120):
                    time.sleep(1)
                    if is_on_flow_not_login(self.page.url):
                        print(f"[{self.name}] ✓ Login complete", flush=True)
                        human_delay(2, 4)
                        break
                else:
                    print(f"[{self.name}] ⚠ Login timeout", flush=True)
            else:
                print(f"[{self.name}] ✓ Already logged in", flush=True)
            
            check_and_dismiss_popup(self.page)
            
            self.ready_flag.set()
            print(f"[{self.name}] ✓ Ready for jobs", flush=True)
            
            # Main loop - process jobs from queue
            while not self.shutdown_event.is_set():
                try:
                    # Check for redo clips first (with short timeout)
                    try:
                        job = self.job_queue.get(timeout=1)
                        
                        if job.get('type') == 'redo':
                            clip = job['clip']
                            print(f"[{self.name}] Processing redo for clip {clip['clip_index']}", flush=True)
                            account_health.set_busy(self.name, job_id=f"redo-{clip.get('id', 'unknown')}")
                            process_redo_clip(self.page, clip, self.download_queue, self.cache)
                            account_health.set_idle(self.name)
                        elif job.get('type') == 'failover':
                            # This job failed on another account, we're taking over
                            print(f"[{self.name}] 🔄 FAILOVER: Taking over job {job['job_id'][:8]}... from {job['failed_account']}", flush=True)
                            account_health.set_busy(self.name, job_id=job.get('job_id'))
                            self._process_failover_job(job)
                            account_health.set_idle(self.name)
                        elif job.get('type') == 'parallel_primary':
                            # Parallel mode - we're the primary account
                            print(f"[{self.name}] 🚀 PARALLEL PRIMARY: Processing job {job['job']['id'][:8]}...", flush=True)
                            account_health.set_busy(self.name, job_id=job['job']['id'])
                            self._process_parallel_job(job, is_primary=True)
                            account_health.set_idle(self.name)
                        elif job.get('type') == 'parallel_secondary':
                            # Parallel mode - we're a secondary account
                            print(f"[{self.name}] 🚀 PARALLEL SECONDARY: Processing job {job['job']['id'][:8]}...", flush=True)
                            account_health.set_busy(self.name, job_id=job['job']['id'])
                            self._process_parallel_job(job, is_primary=False)
                            account_health.set_idle(self.name)
                        elif job.get('type') == 'shutdown':
                            # Shutdown request received
                            print(f"[{self.name}] Received shutdown command", flush=True)
                            break
                        else:
                            # Regular job - process with failover support
                            print(f"[{self.name}] Processing job {job['id'][:8]}...", flush=True)
                            account_health.set_busy(self.name, job_id=job.get('id'))
                            self._process_job_with_failover(job)
                            account_health.set_idle(self.name)
                        
                        self.job_queue.task_done()
                        
                    except queue.Empty:
                        # No jobs in queue, check for shutdown
                        if self.shutdown_event.is_set():
                            break
                        time.sleep(0.5)
                        
                except Exception as e:
                    print(f"[{self.name}] Error: {e}", flush=True)
                    import traceback
                    traceback.print_exc()
                    # Safety: ensure account is marked idle even after error
                    account_health.set_idle(self.name)
                    time.sleep(5)
            
            # Graceful shutdown
            print(f"[{self.name}] 🛑 Shutting down browser...", flush=True)
            try:
                self.browser.close()
            except:
                pass
            print(f"[{self.name}] ✓ Browser closed", flush=True)
    
    def _process_job_with_failover(self, job):
        """Process a job, handing off to other account on first failure"""
        result = process_job_submission_with_failover(
            page=self.page,
            job=job,
            cache=self.cache,
            download_queue=self.download_queue,
            account_name=self.name,
            failover_queue=self.failover_queue,
            all_download_queues=self.all_download_queues,
            is_failover_to_standby=self.is_failover_to_standby
        )
        return result
    
    def _process_failover_job(self, failover_data):
        """Process a job that failed on another account"""
        job = failover_data['original_job']
        remaining_clips = failover_data['remaining_clips']
        failed_clip_index = failover_data['failed_clip_index']
        failed_account = failover_data['failed_account']
        
        print(f"[{self.name}] Failover job: {len(remaining_clips)} clips to process (failed at clip {failed_clip_index})", flush=True)
        
        # Process the remaining clips in a NEW project
        # Note: Even failover jobs can trigger another failover if standby accounts exist
        result = process_job_submission_with_failover(
            page=self.page,
            job=job,
            cache=self.cache,
            download_queue=self.download_queue,
            account_name=self.name,
            failover_queue=self.failover_queue,  # Allow chained failover to next standby
            all_download_queues=self.all_download_queues,
            clips_to_process=remaining_clips,
            is_failover=True,
            failed_account=failed_account,
            is_failover_to_standby=self.is_failover_to_standby
        )
        return result
    
    def _process_parallel_job(self, parallel_data, is_primary=False):
        """Process a portion of a job in parallel mode.
        
        Args:
            parallel_data: Dict with job info and clip assignments
            is_primary: If True, this account updates job status
        """
        job = parallel_data['job']
        clip_indices = parallel_data['clip_indices']
        total_clips = parallel_data['total_clips']
        
        job_id = job['id']
        all_clips = job['clips']
        
        # Filter clips for this account
        my_clips = [all_clips[i] for i in clip_indices]
        
        print(f"\n{'='*60}", flush=True)
        print(f"🚀 PARALLEL {'PRIMARY' if is_primary else 'SECONDARY'}: {job_id[:8]}...", flush=True)
        print(f"Account: {self.name}", flush=True)
        print(f"My clips: {clip_indices} ({len(my_clips)}/{total_clips} total)", flush=True)
        print(f"{'='*60}", flush=True)
        
        # Primary account marks job as processing
        if is_primary:
            update_job_status(job_id, 'processing')
        
        # Always create a NEW project for parallel jobs (each account has its own)
        result = process_job_submission_with_failover(
            page=self.page,
            job=job,
            cache=self.cache,
            download_queue=self.download_queue,
            account_name=self.name,
            failover_queue=self.failover_queue,  # Allow failover within parallel
            all_download_queues=self.all_download_queues,
            clips_to_process=my_clips,
            is_failover=True,  # Force new project creation
            failed_account=None,  # Not a real failover, just parallel
            is_parallel=True,  # New flag to indicate parallel mode
            is_parallel_primary=is_primary,
            is_failover_to_standby=self.is_failover_to_standby,
        )
        
        return result


def main_multi_account(accounts_override=None):
    """Multi-account mode - runs multiple submission browsers in parallel
    
    Args:
        accounts_override: Optional list of account configs to use instead of ACCOUNTS
    """
    print("=" * 60)
    print(f"LOCAL FLOW WORKER {WORKER_VERSION} - MULTI-ACCOUNT MODE (build {WORKER_BUILD})")
    print("=" * 60)
    print(f"Worker ID: {WORKER_ID}")
    
    # Get enabled accounts (use override if provided)
    if accounts_override is not None:
        enabled_accounts = accounts_override
    else:
        enabled_accounts = [a for a in ACCOUNTS if a.get('enabled', True)]
    
    if not enabled_accounts:
        print("❌ No accounts enabled! Check ACCOUNTS configuration.")
        return
    
    # Split accounts into active and standby
    # First 2 accounts are active, rest are standby
    MAX_ACTIVE_ACCOUNTS = 2
    active_accounts = enabled_accounts[:MAX_ACTIVE_ACCOUNTS]
    standby_accounts = enabled_accounts[MAX_ACTIVE_ACCOUNTS:]
    
    print(f"\nAccounts: {len(enabled_accounts)} total")
    print(f"  Active:  {len(active_accounts)} ({[a['name'] for a in active_accounts]})")
    print(f"  Standby: {len(standby_accounts)} ({[a['name'] for a in standby_accounts]})")
    print(f"Total browsers: {len(active_accounts) * 2} ({len(active_accounts)} submit + {len(active_accounts)} download)")
    
    for acc in active_accounts:
        acc_name = acc['name']
        download_folder = acc.get('download_folder', f"./flow_download_{acc_name.lower()}")
        print(f"\n  {acc_name} [ACTIVE]:")
        print(f"    Submit:   {acc['session_folder']}")
        print(f"    Download: {download_folder}")
        if acc.get('proxy'):
            print(f"    Proxy: {acc['proxy']}")
    
    for acc in standby_accounts:
        print(f"\n  {acc['name']} [STANDBY]:")
        print(f"    Session:  {acc['session_folder']}")
    
    print(f"\n  ⚠️  Each account needs login in BOTH submit AND download browsers!")
    print(f"      (Download browser opens when first job is ready)")
    print(f"      (Standby accounts start automatically when needed)")
    print(f"\nWeb app: {WEB_APP_URL}")
    print(f"Clip ready wait: {CLIP_READY_WAIT}s (per clip)")
    print(f"Failure check: {FAILURE_CHECK_DELAY}s (after submission)")
    print("=" * 60)
    
    print("\nChecking API connection...")
    if check_api_connection():
        print("✓ API connection OK")
    else:
        print(f"\n⚠ Cannot reach API at {WEB_APP_URL}")
        response = input("Continue anyway? (y/n): ")
        if response.lower() != 'y':
            return
    
    cache = load_cache()
    
    # Each account gets its own download queue and download worker
    # Jobs from Account1 go to Account1's download worker, etc.
    account_download_queues = {}  # account_name -> Queue
    download_workers = []
    
    # Start download workers ONLY for active accounts initially
    print("\nPreparing download workers for active accounts...")
    for account in active_accounts:
        account_name = account['name']
        # Use SEPARATE folder for download browser (can't share with submit)
        download_folder = account.get('download_folder', f"./flow_download_{account_name.lower()}")
        
        # Auto-create the folder if it doesn't exist
        os.makedirs(download_folder, exist_ok=True)
        
        # Create download queue for this account
        download_queue = Queue()
        account_download_queues[account_name] = download_queue
        
        # Create download worker for this account
        download_worker = DownloadWorker(
            download_queue, 
            cache, 
            session_folder=download_folder,
            account_name=f"{account_name}-DOWNLOAD",
            proxy=account.get('proxy'),
            submit_session_folder=account.get('session_folder'),
        )
        download_worker.start()
        download_workers.append(download_worker)
    
    # Download workers are lazy-loaded - they'll start their browsers on first job
    print(f"✓ {len(download_workers)} download workers ready (browsers will open on first job)")
    
    # Create per-account job queues
    account_job_queues = {}  # account_name -> Queue
    for account in active_accounts:
        account_job_queues[account['name']] = Queue()
    
    # Create swap request queue for standby coordination
    swap_request_queue = Queue()
    
    # Determine if we use standby failover
    use_standby_failover = len(standby_accounts) > 0
    
    # Initialize account health tracking for all accounts
    for account in enabled_accounts:
        account_health.register_account(account['name'])
    
    # Initialize the global FailoverRouter
    global failover_router
    failover_router = FailoverRouter(
        account_job_queues=account_job_queues,
        account_download_queues=account_download_queues,
        swap_request_queue=swap_request_queue if use_standby_failover else None,
    )
    
    # Start account workers (submission browsers) for active accounts only
    account_workers = []
    for i, account in enumerate(active_accounts):
        account_name = account['name']
        # Pass this account's download queue to its submission worker
        download_queue = account_download_queues[account_name]
        
        # Determine failover mechanism
        if use_standby_failover:
            # Failovers go to standby manager
            failover_queue = swap_request_queue
            is_failover_to_standby = True
        elif len(active_accounts) > 1:
            # Fallback: failover to other active account (no standby available)
            other_index = (i + 1) % len(active_accounts)
            other_account_name = active_accounts[other_index]['name']
            failover_queue = account_job_queues[other_account_name]
            is_failover_to_standby = False
        else:
            failover_queue = None  # Single account mode - no failover
            is_failover_to_standby = False
        
        worker = AccountWorker(
            account, 
            download_queue, 
            cache, 
            account_job_queues[account_name],  # This account's job queue
            failover_queue=failover_queue,
            all_download_queues=account_download_queues,
            is_failover_to_standby=is_failover_to_standby,
        )
        worker.start()
        account_workers.append(worker)
    
    # Start StandbyAccountManager if we have standby accounts
    standby_manager = None
    if standby_accounts:
        standby_manager = StandbyAccountManager(
            standby_accounts=standby_accounts,
            swap_request_queue=swap_request_queue,
            cache=cache,
            active_workers=account_workers,
            active_download_workers=download_workers,
            account_job_queues=account_job_queues,
            account_download_queues=account_download_queues,
        )
        standby_manager.start()
        print(f"✓ Standby manager started with {len(standby_accounts)} standby account(s)")
    
    # Wait for all account workers to be ready
    print("\nWaiting for account browsers to start...")
    for worker in account_workers:
        worker.ready_flag.wait(timeout=60)
    print(f"✓ All {len(account_workers)} active account workers ready")
    
    print("\n" + "=" * 50)
    print("MULTI-ACCOUNT WORKER READY - Polling for jobs...")
    print(f"Failover strategy:")
    print(f"  1. Same-account retry: up to {FailoverRouter.MAX_SAME_ACCOUNT_RETRIES}x in new project")
    print(f"  2. Cross-account: route to healthiest idle account")
    if use_standby_failover:
        print(f"  3. Standby pool: {len(standby_accounts)} account(s) on standby")
    else:
        print(f"  3. Standby pool: NONE")
    print(f"  Hot threshold: {AccountHealthTracker.HOT_THRESHOLD} consecutive failures")
    print(f"  Cooldown: {AccountHealthTracker.COOLDOWN_SECONDS}s")
    print("=" * 50)
    
    # Track which account handles which job (round-robin among ACTIVE accounts)
    account_index = 0
    
    # Track jobs we've already queued to prevent duplicates
    queued_job_ids = set()
    queued_redo_keys = set()  # Track by (clip_id, attempt) to allow re-redos
    
    try:
        while True:
            # Check for redo clips
            redo_clips = get_redo_clips()
            if redo_clips:
                new_redo_clips = []
                for c in redo_clips:
                    # Use (clip_id, attempt) as key so the same clip can be retried
                    redo_key = (c.get('id'), c.get('generation_attempt', 1))
                    if redo_key not in queued_redo_keys:
                        new_redo_clips.append(c)
                
                if new_redo_clips:
                    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Found {len(new_redo_clips)} NEW clip(s) needing redo")
                    for clip in new_redo_clips:
                        clip_id = clip.get('id')
                        attempt = clip.get('generation_attempt', 1)
                        redo_key = (clip_id, attempt)
                        queued_redo_keys.add(redo_key)
                        # Distribute redos round-robin to ACTIVE account job queues
                        target_account = active_accounts[account_index]['name']
                        account_job_queues[target_account].put({'type': 'redo', 'clip': clip})
                        print(f"  → Clip {clip.get('clip_index')} (attempt {attempt}) assigned to {target_account}")
                        account_index = (account_index + 1) % len(active_accounts)
            
            # Check for new jobs (get one at a time, excluding already-queued jobs)
            job = get_pending_job(exclude_ids=queued_job_ids)
            if job:
                job_id = job.get('id')
                if job_id not in queued_job_ids:
                    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Found NEW pending job: {job_id[:8]}...")
                    queued_job_ids.add(job_id)
                    
                    clips = job.get('clips', [])
                    num_accounts = len(active_accounts)
                    
                    # Analyze clip chains for parallel distribution
                    # Use continue-mode chain analysis to ensure continue-mode clips
                    # stay on the same account (they need sequential processing with approval)
                    chains = analyze_continue_mode_chains(clips)
                    
                    # Check which accounts are idle (not busy AND empty queue)
                    idle_accounts = []
                    for acc in active_accounts:
                        acc_name = acc['name']
                        is_busy = account_health.is_busy(acc_name)
                        queue_size = account_job_queues[acc_name].qsize()
                        if not is_busy and queue_size == 0:
                            idle_accounts.append(acc_name)
                    
                    busy_count = num_accounts - len(idle_accounts)
                    print(f"  📋 Account status: {len(idle_accounts)} idle, {busy_count} busy")
                    
                    # Decision: split across accounts ONLY if multiple accounts are idle
                    # If only 1 idle (or none), assign whole job to one account to keep
                    # the other accounts free for the NEXT job
                    can_split = len(chains) > 1 and len(idle_accounts) > 1
                    
                    if can_split:
                        print(f"  📊 Found {len(chains)} independent chains: {[c for c in chains]}")
                        
                        # Only use idle accounts for splitting
                        assignments = assign_chains_to_accounts(chains, idle_accounts)
                        
                        # Filter out empty assignments
                        active_assignments = {acc: clips_list for acc, clips_list in assignments.items() if clips_list}
                        
                        if len(active_assignments) > 1:
                            # Parallel execution - send to multiple idle accounts
                            print(f"  🚀 PARALLEL MODE: Splitting across {len(active_assignments)} idle accounts")
                            
                            primary_account = None
                            for acc_name, clip_indices in active_assignments.items():
                                if primary_account is None:
                                    # First account handles job status and is "primary"
                                    primary_account = acc_name
                                    account_job_queues[acc_name].put({
                                        'type': 'parallel_primary',
                                        'job': job,
                                        'clip_indices': clip_indices,
                                        'total_clips': len(clips),
                                        'all_assignments': active_assignments,
                                    })
                                    print(f"    → {acc_name}: clips {clip_indices} (PRIMARY)")
                                else:
                                    # Secondary accounts just process their clips
                                    account_job_queues[acc_name].put({
                                        'type': 'parallel_secondary',
                                        'job': job,
                                        'clip_indices': clip_indices,
                                        'total_clips': len(clips),
                                        'primary_account': primary_account,
                                    })
                                    print(f"    → {acc_name}: clips {clip_indices}")
                        else:
                            # Only one account has work - use single account mode
                            target_account = idle_accounts[0]
                            account_job_queues[target_account].put(job)
                            print(f"  → Assigned to {target_account} (single chain)")
                    else:
                        # Single account mode: pick the best account
                        # Prefer idle accounts, then fall back to shortest queue
                        if idle_accounts:
                            target_account = idle_accounts[0]
                            print(f"  → Assigned to {target_account} (idle)")
                        else:
                            # All busy - find account with shortest queue
                            best_account = None
                            best_qsize = float('inf')
                            for acc in active_accounts:
                                acc_name = acc['name']
                                qsize = account_job_queues[acc_name].qsize()
                                if qsize < best_qsize:
                                    best_qsize = qsize
                                    best_account = acc_name
                            target_account = best_account or active_accounts[account_index]['name']
                            print(f"  → Assigned to {target_account} (all busy, shortest queue: {best_qsize})")
                            account_index = (account_index + 1) % len(active_accounts)
                        
                        account_job_queues[target_account].put(job)
                # else: job already queued, skip
            
            # Only print "no jobs" if we didn't find anything new
            if not redo_clips and not job:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] No pending jobs or redos...", flush=True)
            
            time.sleep(POLL_INTERVAL)
            
    except KeyboardInterrupt:
        print("\n\n⚠ Shutting down...")
    finally:
        # Proper cleanup for all workers and browsers
        print("Stopping account workers...")
        for worker in account_workers:
            try:
                worker.request_shutdown()
            except:
                pass
        
        print("Stopping download workers...")
        for download_worker in download_workers:
            try:
                download_worker.stop()
            except:
                pass
        
        print("Waiting for download workers to finish...")
        for download_worker in download_workers:
            try:
                download_worker.join(timeout=10)
            except:
                pass
        
        print("Waiting for account workers to finish...")
        for worker in account_workers:
            try:
                worker.join(timeout=10)
            except:
                pass
        
        print("✓ All workers stopped")


def main():
    print("=" * 60)
    print(f"LOCAL FLOW WORKER {WORKER_VERSION} - Stealth Edition (build {WORKER_BUILD})")
    print("=" * 60)
    print(f"Worker ID: {WORKER_ID}")
    print(f"Browser mode: {BROWSER_MODE.upper()}")
    print(f"Session folder: {SESSION_FOLDER}")
    print(f"Download session: {DOWNLOAD_SESSION_FOLDER}")
    print(f"Web app: {WEB_APP_URL}")
    print(f"Clip ready wait: {CLIP_READY_WAIT}s (per clip)")
    print(f"Failure check: {FAILURE_CHECK_DELAY}s (after submission)")
    print(f"Poll interval: {POLL_INTERVAL}s")
    # Patchright diagnostic
    try:
        import patchright
        print(f"\n✅ PATCHRIGHT ACTIVE (v{getattr(patchright, '__version__', '?')})")
        print("   CDP detection bypass: ENABLED")
        print("   --enable-automation: REMOVED by Patchright")
    except ImportError:
        print("\n⚠️  PATCHRIGHT NOT INSTALLED — using regular Playwright")
        print("   CDP detection bypass: DISABLED (reCAPTCHA will detect automation)")
        print("   --enable-automation: BLOCKED via ignore_default_args")
        print("   ➡ Install with: pip install patchright && patchright install chromium")
    if BROWSER_MODE == "stealth":
        print("\n🔒 STEALTH MODE: Using your real Chrome browser")
        print("   This helps avoid Google's bot detection!")
    print("=" * 60)
    
    print("\nChecking API connection...")
    if check_api_connection():
        print("✓ API connection OK")
    else:
        print(f"\n⚠ Cannot reach API at {WEB_APP_URL}")
        response = input("Continue anyway? (y/n): ")
        if response.lower() != 'y':
            return
    
    cache = load_cache()
    
    download_queue = Queue()
    single_proxy = ACCOUNTS[0].get('proxy') if ACCOUNTS else None
    download_worker = DownloadWorker(download_queue, cache, proxy=single_proxy, submit_session_folder=SESSION_FOLDER)
    download_worker.start()
    
    print("\nWaiting for download browser to start...")
    download_worker.ready_flag.wait(timeout=60)
    print("✓ Download worker ready")
    
    with sync_playwright() as p:
        # Use REAL Chrome browser for better stealth
        # channel='chrome' uses your installed Chrome, not Playwright's bundled Chromium
        print(f"\n[Browser] Mode: {BROWSER_MODE}")
        
        single_proxy_config = parse_proxy_url(single_proxy)
        if single_proxy_config:
            print(f"[Browser] Using proxy: {single_proxy_config['server']}")
        
        single_chrome_args = [
            '--disable-blink-features=AutomationControlled',
            '--disable-dev-shm-usage',
            '--no-sandbox',
            # Force Chrome variation IDs so x-client-data header looks like a real browser
            # Without this, Playwright Chrome only has 1 trial ID (CKmXywE=, 10 chars)
            # Real Chrome has 5-6+ (46+ chars). reCAPTCHA Enterprise uses this to score sessions.
            '--force-variation-ids=3300115,3300134,3313321,3328827,3330196,3362821',
        ]
        
        # Only add proxy-related flags when proxy is configured
        if single_proxy_config:
            single_chrome_args.append('--ignore-certificate-errors')
            # Create proxy auth extension for automatic credential handling
            single_ext_dir = os.path.join(BASE_DIR, ".proxy_auth_ext_single")
            single_auth_ext = create_proxy_auth_extension(single_proxy, single_ext_dir)
            if single_auth_ext:
                single_chrome_args.extend([
                    f'--disable-extensions-except={single_auth_ext}',
                    f'--load-extension={single_auth_ext}',
                ])
        
        # Select stealth script - minimal for real Chrome, full for bundled Chromium
        # Stealth handled by Patchright natively (no init script needed)
        
        if BROWSER_MODE == "stealth":
            print("[Browser] Launching REAL Chrome (stealth mode)...")
            # Match test_human_like.py launch exactly
            launch_kwargs = {
                'user_data_dir': SESSION_FOLDER,
                'channel': 'chrome',
                    'ignore_default_args': ['--enable-automation'],
                'headless': False,
                'viewport': {"width": 1280, "height": 720},
                'args': single_chrome_args,
            }
            # Only pass proxy if actually configured (test_human_like.py doesn't pass proxy)
            if single_proxy_config:
                launch_kwargs['proxy'] = single_proxy_config
            browser = p.chromium.launch_persistent_context(**launch_kwargs)
            browser_name = "Chrome"
        else:
            print("[Browser] Launching Firefox (playwright mode)...")
            launch_kwargs = {
                'user_data_dir': SESSION_FOLDER,
                'headless': False,
                'viewport': {"width": 1280, "height": 500},
            }
            if single_proxy_config:
                launch_kwargs['proxy'] = single_proxy_config
            browser = p.firefox.launch_persistent_context(**launch_kwargs)
            browser_name = "Firefox"
        
        # Match test_human_like.py: browser.pages[0] if browser.pages else browser.new_page()
        page = browser.pages[0] if browser.pages else browser.new_page()
        
        # Note: Patchright handles stealth (webdriver, CDP, Runtime.enable) natively
        
        print(f"✓ {browser_name} browser started")
        
        # Warm up Chrome — sync variations seed for valid x-client-data header
        chrome_warmup(page)
        
        # === Match test_human_like.py startup exactly ===
        print("[STARTUP] Navigating to Flow...", flush=True)
        page.goto(FLOW_HOME_URL)
        human_delay(2, 4)
        
        # Look around
        human_mouse_move(page)
        human_delay(1, 2)
        scroll_randomly(page)
        human_delay(0.5, 1)
        
        # ── Wait for user to be fully logged in ──
        # ensure_logged_into_flow handles ALL states:
        #   - Already logged in → returns immediately  
        #   - On Google login page → waits for user to complete (up to 10 min)
        #   - On Flow landing page → clicks "Create with Flow" → waits for login
        # It checks the DOM for "New project" button, not just the URL.
        print("[STARTUP] Verifying login status...", flush=True)
        login_was_required = ensure_logged_into_flow(page, "STARTUP", timeout_minutes=10)
        if login_was_required:
            print("[STARTUP] ✓ Login verified after sign-in", flush=True)
        else:
            print("[STARTUP] ✓ Already logged in and verified", flush=True)
        
        # Dismiss any popups
        check_and_dismiss_popup(page)
        
        # ── Sync login to download browser profile ──
        # Copy the submit browser's profile to the download folder while Chrome is closed.
        # This ensures the download browser starts fully logged in without needing manual login.
        import shutil
        download_folder = DOWNLOAD_SESSION_FOLDER
        submit_folder = SESSION_FOLDER
        
        # Always sync if login was required (fresh session) or download folder is missing/stale
        needs_sync = login_was_required or not os.path.exists(download_folder)
        if not needs_sync:
            # Check if submit folder has newer login data
            submit_prefs = os.path.join(submit_folder, "Default", "Preferences")
            dl_prefs = os.path.join(download_folder, "Default", "Preferences")
            if os.path.exists(submit_prefs):
                submit_mtime = os.path.getmtime(submit_prefs)
                dl_mtime = os.path.getmtime(dl_prefs) if os.path.exists(dl_prefs) else 0
                if submit_mtime > dl_mtime:
                    needs_sync = True
        
        if needs_sync:
            print("\n[Sync] Syncing login profile to download browser...")
            print("[Sync] Closing submit browser temporarily...")
            browser.close()
            time.sleep(2)
            
            # Copy profile while Chrome is NOT running (no locked files)
            if os.path.exists(download_folder):
                try:
                    shutil.rmtree(download_folder)
                except Exception as e:
                    print(f"[Sync] ⚠ Could not remove old download profile: {e}")
            
            try:
                shutil.copytree(submit_folder, download_folder, 
                               ignore_dangling_symlinks=True,
                               ignore=shutil.ignore_patterns('SingletonLock', 'SingletonSocket', 'SingletonCookie'))
                print(f"[Sync] ✓ Profile copied to {download_folder}")
            except Exception as e:
                print(f"[Sync] ⚠ Profile copy failed: {e}")
            
            # Suppress Chrome sign-in dialog in both profiles
            
            # Relaunch submit browser
            print("[Sync] Relaunching submit browser...")
            if BROWSER_MODE == "stealth":
                relaunch_kwargs = {
                    'user_data_dir': SESSION_FOLDER,
                    'channel': 'chrome',
                    'ignore_default_args': ['--enable-automation'],
                    'headless': False,
                    'viewport': {"width": 1280, "height": 720},
                    'args': single_chrome_args,
                }
                if single_proxy_config:
                    relaunch_kwargs['proxy'] = single_proxy_config
                browser = p.chromium.launch_persistent_context(**relaunch_kwargs)
            else:
                relaunch_kwargs = {
                    'user_data_dir': SESSION_FOLDER,
                    'headless': False,
                    'viewport': {"width": 1280, "height": 500},
                }
                if single_proxy_config:
                    relaunch_kwargs['proxy'] = single_proxy_config
                browser = p.firefox.launch_persistent_context(**relaunch_kwargs)
            
            page = browser.pages[0] if browser.pages else browser.new_page()
            # Note: Patchright handles stealth natively
            
            # Warm up Chrome — sync variations seed
            chrome_warmup(page)
            
            # Match test_human_like.py startup
            print("[Sync] Navigating to Flow...", flush=True)
            page.goto(FLOW_HOME_URL)
            human_delay(2, 4)
            human_mouse_move(page)
            human_delay(1, 2)
            scroll_randomly(page)
            human_delay(0.5, 1)
            check_and_dismiss_popup(page)
            print("[Sync] ✓ Submit browser relaunched and ready!")
        
        print("\n" + "=" * 50)
        print("WORKER READY - Polling for jobs...")
        print("=" * 50)
        
        try:
            while True:
                redo_clips = get_redo_clips()
                
                if redo_clips:
                    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Found {len(redo_clips)} clip(s) needing redo")
                    
                    for clip in redo_clips:
                        try:
                            process_redo_clip(page, clip, download_queue, cache)
                        except Exception as e:
                            print(f"\n✗ Error processing redo: {e}")
                            import traceback
                            traceback.print_exc()
                            update_clip_status(clip['id'], 'failed', error_message=str(e))
                    
                    time.sleep(5)
                    continue
                
                job = get_pending_job()
                
                if job:
                    job_id = job['id']
                    
                    if is_job_completed(cache, job_id):
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] Job {job_id[:8]}... already completed (cached)")
                        time.sleep(POLL_INTERVAL)
                        continue
                    
                    try:
                        process_job_submission(page, job, cache, download_queue)
                    except Exception as e:
                        print(f"\n✗ Error processing job: {e}")
                        import traceback
                        traceback.print_exc()
                        update_job_status(job_id, 'failed', str(e))
                else:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] No pending jobs or redos...")
                
                time.sleep(POLL_INTERVAL)
                
        except KeyboardInterrupt:
            print("\n\nShutting down...")
        finally:
            print("\nWaiting for pending downloads...")
            download_queue.join()
            
            download_worker.stop()
            download_worker.join(timeout=10)
            
            print("Closing browser...")
            browser.close()
    
    print("\n✓ Worker stopped")


def show_help():
    """Show command line help"""
    print("""
Local Flow Worker V7 - Multi-Account Edition
=============================================

Uses your REAL Chrome browser to avoid bot detection!
Supports multiple Google accounts for parallel processing.

QUICK START:
  python local_flow_worker.py --auto            Auto-detect and use ALL ready accounts
  python local_flow_worker.py --accounts 1,2,3  Use specific accounts (by number)
  python local_flow_worker.py --list            Show status of all accounts

Commands:
  --auto              Auto-detect ready accounts and start with all of them
  --accounts 1,2,3    Use specific accounts (comma-separated, e.g., 1,2 or 1,3,4)
  --accounts 1-4      Use account range (e.g., 1-4 means accounts 1,2,3,4)
  --count N           Use first N ready accounts
  --list              List all accounts and their ready status (dry run)
  
  --single            Force single-account mode (legacy)
  --multi / -m        Run multi-account mode with enabled accounts (legacy)
  --clear-cache       Clear all cache and start fresh
  --show-cache        Show cache status
  --recover           Re-queue stuck/failed downloads
  --help              Show this help

Examples:
  # Start with all 4 ready accounts
  python local_flow_worker.py --auto
  
  # Start with only accounts 1 and 3
  python local_flow_worker.py --accounts 1,3
  
  # Start with accounts 2, 3, and 4
  python local_flow_worker.py --accounts 2-4
  
  # Start with first 2 ready accounts
  python local_flow_worker.py --count 2
  
  # Check which accounts are ready
  python local_flow_worker.py --list

Account Folder Structure:
  Each account needs TWO folders with Chrome profile data:
    ./flow_session_account1/Default/   (submit browser)
    ./flow_download_account1/Default/  (download browser)
  
  Run the setup script to create these folders and log in.

Tips to Avoid Detection:
  - Use different IPs/proxies for each account
  - Don't run too many jobs in quick succession
  - Let the browser warm up naturally before processing
  - Keep the browser windows visible (don't minimize)
  - If Google shows captchas, solve them manually
""")


def recover_stuck_jobs(cache):
    """Find and re-queue stuck jobs for download"""
    print("\n" + "=" * 50)
    print("RECOVERING STUCK JOBS")
    print("=" * 50)
    
    jobs = cache.get('jobs', {})
    recovered = 0
    
    for job_id, job_data in jobs.items():
        status = job_data.get('status')
        project_url = job_data.get('project_url')
        clips = job_data.get('clips', [])
        
        if status == 'submitted' and project_url and clips:
            downloaded_clips = job_data.get('clips_downloaded', [])
            total_clips = len(clips)
            
            if len(downloaded_clips) < total_clips:
                print(f"\n  Job {job_id[:8]}...")
                print(f"    Project: {project_url}")
                print(f"    Downloaded: {len(downloaded_clips)}/{total_clips}")
                print(f"    Status: NEEDS RECOVERY")
                recovered += 1
    
    if recovered == 0:
        print("\n  No stuck jobs found!")
    else:
        print(f"\n  Found {recovered} job(s) needing recovery")
        print("\n  To recover, run the worker normally and it will re-attempt downloads")
        
        response = input("\n  Clear download status to retry? (y/n): ")
        if response.lower() == 'y':
            for job_id, job_data in jobs.items():
                status = job_data.get('status')
                if status == 'submitted':
                    job_data['clips_downloaded'] = []
            save_cache(cache)
            print("  ✓ Download status cleared - run worker to retry")


if __name__ == "__main__":
    import sys
    import argparse
    
    # ── AUTO-UPDATE CHECK ──
    def check_for_updates():
        """Check if a newer version is available and auto-update if so."""
        try:
            import requests as _req
            resp = _req.get(f"{WEB_APP_URL}/api/user-worker/version", timeout=5)
            if resp.ok:
                remote_version = resp.json().get("version", "")
                if remote_version and remote_version != WORKER_BUILD:
                    print(f"\n🔄 Update available: {WORKER_BUILD} → {remote_version}")
                    print(f"   Downloading new version...", flush=True)
                    dl = _req.get(f"{WEB_APP_URL}/api/user-worker/download/flow_worker.py", timeout=30)
                    if dl.ok:
                        import pathlib
                        my_path = pathlib.Path(__file__).resolve()
                        my_path.write_bytes(dl.content)
                        print(f"   ✓ Updated! Restarting...\n", flush=True)
                        os.execv(sys.executable, [sys.executable] + sys.argv)
                    else:
                        print(f"   ✗ Download failed (HTTP {dl.status_code})", flush=True)
                else:
                    print(f"✓ Worker up to date ({WORKER_BUILD})", flush=True)
        except Exception as e:
            print(f"⚠ Update check skipped: {e}", flush=True)
    
    check_for_updates()
    
    # Create argument parser
    parser = argparse.ArgumentParser(
        description='Local Flow Worker - Multi-Account Video Generation',
        add_help=False  # We have custom help
    )
    
    # Account selection (mutually exclusive)
    account_group = parser.add_mutually_exclusive_group()
    account_group.add_argument('--auto', action='store_true',
        help='Auto-detect and use all ready accounts')
    account_group.add_argument('--accounts', '-a', type=str,
        help='Specific accounts to use (e.g., 1,2,3 or 1-4)')
    account_group.add_argument('--count', '-n', type=int,
        help='Use first N ready accounts')
    account_group.add_argument('--list', '-l', action='store_true',
        help='List all accounts and their status')
    account_group.add_argument('--single', action='store_true',
        help='Force single-account mode')
    account_group.add_argument('--multi', '-m', action='store_true',
        help='Run with enabled accounts (legacy)')
    
    # Utility commands
    parser.add_argument('--clear-cache', action='store_true',
        help='Clear all cache and start fresh')
    parser.add_argument('--show-cache', action='store_true',
        help='Show cache status')
    parser.add_argument('--recover', action='store_true',
        help='Re-queue stuck/failed downloads')
    parser.add_argument('--help', '-h', action='store_true',
        help='Show help message')
    
    args = parser.parse_args()
    
    # Handle help
    if args.help:
        show_help()
        sys.exit(0)
    
    # Handle utility commands
    if args.clear_cache:
        if os.path.exists(CACHE_FILE):
            os.remove(CACHE_FILE)
            print("✓ Cache cleared")
        else:
            print("No cache file to clear")
        sys.exit(0)
    
    if args.show_cache:
        cache = load_cache()
        print("\n=== CACHE STATUS ===")
        jobs = cache.get('jobs', {})
        
        if not jobs:
            print("No jobs in cache")
        else:
            for job_id, job_data in jobs.items():
                status = job_data.get('status', 'unknown')
                project_url = job_data.get('project_url', 'N/A')
                clips_submitted = job_data.get('clips_submitted', [])
                clips_downloaded = job_data.get('clips_downloaded', [])
                total_clips = len(job_data.get('clips', []))
                
                print(f"\nJob {job_id[:8]}... [{status.upper()}]")
                print(f"  Project: {project_url}")
                print(f"  Submitted: {len(clips_submitted)}/{total_clips}")
                print(f"  Downloaded: {len(clips_downloaded)}/{total_clips}")
        sys.exit(0)
    
    if args.recover:
        cache = load_cache()
        recover_stuck_jobs(cache)
        sys.exit(0)
    
    # Handle account selection
    if args.list:
        # Just list accounts and exit
        list_accounts()
        sys.exit(0)
    
    if args.auto:
        # Auto-detect all ready accounts
        ready_accounts = get_ready_accounts()
        if not ready_accounts:
            print("❌ No ready accounts found!")
            print("Run --list to see account status")
            sys.exit(1)
        
        accounts_to_use = [acc for idx, acc in ready_accounts]
        print(f"✓ Auto-detected {len(accounts_to_use)} ready account(s): {[acc['name'] for acc in accounts_to_use]}")
        main_multi_account(accounts_override=accounts_to_use)
    
    elif args.accounts:
        # Parse specific account selection
        indices = parse_account_selection(args.accounts)
        if indices is None:
            print("❌ Invalid account selection")
            sys.exit(1)
        
        selected = select_accounts_by_indices(indices)
        if not selected:
            print("❌ No valid accounts selected")
            sys.exit(1)
        
        # Validate selected accounts are ready
        valid, errors = validate_selected_accounts(selected)
        if errors:
            print("⚠ Some accounts are not ready:")
            for err in errors:
                print(f"  - {err}")
        
        if not valid:
            print("❌ No ready accounts in selection")
            sys.exit(1)
        
        print(f"✓ Starting with {len(valid)} account(s): {[acc['name'] for acc in valid]}")
        main_multi_account(accounts_override=valid)
    
    elif args.count:
        # Use first N ready accounts
        ready_accounts = get_ready_accounts()
        if not ready_accounts:
            print("❌ No ready accounts found!")
            sys.exit(1)
        
        n = min(args.count, len(ready_accounts))
        accounts_to_use = [acc for idx, acc in ready_accounts[:n]]
        print(f"✓ Using first {n} ready account(s): {[acc['name'] for acc in accounts_to_use]}")
        main_multi_account(accounts_override=accounts_to_use)
    
    elif args.single:
        # Force single-account mode
        print("Running in SINGLE account mode (--single flag)")
        main()
    
    elif args.multi:
        # Legacy multi-account mode
        main_multi_account()
    
    else:
        # No arguments - show quick help and use auto mode
        print("=" * 60)
        print(f"LOCAL FLOW WORKER {WORKER_VERSION} - Account Auto-Detection (build {WORKER_BUILD})")
        print("=" * 60)
        
        ready_accounts = get_ready_accounts()
        
        if not ready_accounts:
            print("\n❌ No ready accounts found!")
            print("\nRun with --list to see account status")
            print("Run with --help for usage information")
            sys.exit(1)
        
        # Show what's available
        print(f"\nDetected {len(ready_accounts)} ready account(s):")
        for idx, acc in ready_accounts:
            print(f"  {idx}. {acc['name']}")
        
        print(f"\n→ Starting with all {len(ready_accounts)} accounts...")
        print("  (Use --accounts 1,2 to select specific accounts)")
        print()
        
        accounts_to_use = [acc for idx, acc in ready_accounts]
        main_multi_account(accounts_override=accounts_to_use)