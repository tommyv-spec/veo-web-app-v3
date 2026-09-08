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
import re
import subprocess
import threading
import time

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


def firefox_headless_enabled(env=None):
    """Headless is the DEFAULT for Firefox mode. Set FIREFOX_HEADLESS=0 to show it.

    A minimized Firefox window cannot be driven at all. Measured 2026-08-09 on
    Camoufox: click on a visible window succeeds in 0.1s; minimize the same
    window and the identical click times out. Firefox stops compositing when
    minimized and Playwright's click does hit-testing that needs live layout.

    That is not a corner case — it cost a live run ~19 minutes stuck retrying
    one settings click ("performing click action" then timeout, 15 attempts),
    which only recovered when the window came back to the foreground. Firefox
    prefs do not help: window_occlusion_tracking.enabled=false,
    dom.suspend_inactive.enabled=false and the timer-throttling prefs were all
    tried and the minimized click still timed out.

    Headless has no window to minimize, so the failure cannot happen, and the
    operator gets their desktop back. navigator.webdriver stays False.
    """
    env = os.environ if env is None else env
    raw = (env.get("FIREFOX_HEADLESS") or "").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False
    return True


def camoufox_launch_kwargs(kwargs, window=None, env=None):
    """Translate flow_worker's launch kwargs into Camoufox's dialect."""
    out = dict(kwargs)

    for k in _CHROME_ONLY_KWARGS:
        out.pop(k, None)

    # flow_worker hardcodes headless=False (it was written for a Chrome window
    # the operator watches). See firefox_headless_enabled for why that is the
    # wrong default here.
    out["headless"] = firefox_headless_enabled(env)

    # Silence. The Chrome path passes --mute-audio; Firefox has no such flag, so
    # mute at the pref level. Flow autoplays every generated clip, and a headless
    # browser still routes audio to the speakers — the window is invisible but the
    # noise is not. media.volume_scale scales ALL output to zero at the platform
    # layer, so it cannot be re-enabled by page JS the way muting a video element
    # can. autoplay.default=5 (BLOCKED_ALL) stops the playback starting at all.
    prefs = dict(out.get("firefox_user_prefs") or {})
    prefs.setdefault("media.volume_scale", "0.0")
    prefs.setdefault("media.autoplay.default", 5)
    prefs.setdefault("media.autoplay.blocking_policy", 2)
    out["firefox_user_prefs"] = prefs

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


# ─────────────────────────── the profile-lock gate ───────────────────────────
#
# A browser profile has exactly one owner. When an orphan Camoufox is still
# holding one, the next launch does NOT fail fast: measured 2026-09-08 on a
# scratch profile, it spends 125 seconds and then raises TargetClosedError, and
# on a desktop it also puts the modal "Camoufox is already running, but is not
# responding" on the operator's screen. Headless does not save you — the
# camoufox processes measured that day carried -headless and the dialog came up
# anyway. Downstream the worker then reads as "no Google account" when the
# profile was signed in the whole time: the browser simply never opened.
#
# The cleanup was already proven in tools/launch_workers.py (clear_flow_browser)
# and docs/handoff-archive/2026-08.md:3949 recorded the missing half on
# 2026-08-17 — "wants a startup sweep of the lock when no live process holds the
# profile". It stayed open because the cleanup lived in ONE launcher, so a
# direct `python flow_worker.py`, an fg_launch_*.ps1 helper or the .bat each
# walked past it. It lives HERE now: launch_context is the one function every
# worker and every tool goes through, so no route can skip it.
#
# Two rules carried over from `camoufox-orphan-holds-profile-lock`, both load
# bearing:
#   * scope by the PROFILE PATH, never by process name — the operator runs
#     three camoufox trees at once and a name sweep takes down live lanes;
#   * only a parent's command line carries the profile; -contentproc children
#     are reached through their parent pid.


class ProfileLockedError(RuntimeError):
    """Another browser holds this profile. Launching would hang, not fail."""


_PROFILE_ARG_RE = re.compile(
    r'(?:^|\s)(?:-{1,2}profile|--user-data-dir)(?:=|\s+)(?:"([^"]*)"|(\S+))')


def profile_arg_from_cmdline(cmdline):
    """The profile directory a browser command line names, or None.

    Handles Firefox/Camoufox (`-profile <dir>`) and chromium
    (`--user-data-dir=<dir>`). Returns None for -contentproc children, which
    carry no profile at all — that is why they are found through their parent.
    """
    m = _PROFILE_ARG_RE.search(cmdline or "")
    if not m:
        return None
    return m.group(1) if m.group(1) is not None else m.group(2)


def _norm(path):
    return os.path.normcase(os.path.abspath(str(path).strip().strip('"'))).rstrip("\\/")


class ProcessQueryError(RuntimeError):
    """The process list could not be read. NEVER silently read as 'nothing found'."""


def _query_processes():
    """Live processes as [{pid, ppid, name, cmd}], or raise ProcessQueryError.

    It raises rather than returning [] on purpose. An empty list means "nothing
    holds this profile" and would send the caller straight into the launch that
    hangs; a query that quietly returns less than reality is how this class of
    bug hides (repo CLAUDE.md §9, "log-and-continue = failing open").

    `strict=False` is load bearing. Windows command lines contain raw control
    characters, ConvertTo-Json emits them unescaped, and strict json.loads then
    rejects the WHOLE 270 KB payload over one byte — measured on this box
    2026-09-08, "Invalid control character at line 1 column 100853". With the
    default strict parse this function returned zero rows on a machine running
    two camoufox trees.
    """
    import json
    try:
        if os.name == "nt":
            ps = ("Get-CimInstance Win32_Process | Select-Object ProcessId,"
                  "ParentProcessId,Name,CommandLine | ConvertTo-Json -Compress")
            done = subprocess.run(
                ["powershell", "-NoProfile", "-NonInteractive", "-Command", ps],
                capture_output=True, text=True, encoding="utf-8", errors="replace",
                timeout=90)
            out = (done.stdout or "").strip()
            if not out:
                raise ProcessQueryError(
                    f"powershell returned no process list (rc={done.returncode}, "
                    f"stderr={(done.stderr or '').strip()[:200]!r})")
            rows = json.loads(out, strict=False)
            rows = [rows] if isinstance(rows, dict) else rows
            return [{"pid": int(r.get("ProcessId") or 0),
                     "ppid": int(r.get("ParentProcessId") or 0),
                     "name": (r.get("Name") or ""),
                     "cmd": (r.get("CommandLine") or "")} for r in rows]
        out = subprocess.run(["ps", "-eo", "pid=,ppid=,comm=,args="],
                             capture_output=True, text=True, encoding="utf-8",
                             errors="replace", timeout=30).stdout
        rows = []
        for line in out.splitlines():
            parts = line.split(None, 3)
            if len(parts) == 4 and parts[0].isdigit():
                rows.append({"pid": int(parts[0]), "ppid": int(parts[1]),
                             "name": parts[2], "cmd": parts[3]})
        if not rows:
            raise ProcessQueryError("ps returned no usable rows")
        return rows
    except ProcessQueryError:
        raise
    except Exception as exc:
        raise ProcessQueryError(f"{type(exc).__name__}: {exc}") from exc


def profile_holders(profile_dir, rows=None):
    """Processes holding THIS profile — children first, then their parents.

    Exact path match, never a substring: "…/firefox-session" must not match
    "…/firefox-session-2", or clearing one lane kills another account's browser
    (the v684 trap, re-proven by the mutation check in the tests).
    """
    rows = _query_processes() if rows is None else rows
    target = _norm(profile_dir)
    parents = []
    for r in rows:
        arg = profile_arg_from_cmdline(r.get("cmd") or "")
        if arg and _norm(arg) == target:
            parents.append(r)
    parent_pids = {r["pid"] for r in parents}
    kids = [r for r in rows
            if r.get("ppid") in parent_pids and r["pid"] not in parent_pids]
    return kids + parents


def lock_is_held(profile_dir):
    """True/False on Windows, None where the question cannot be answered honestly.

    The probe is a delete attempt, which is exact and needs no process list:
    Windows refuses to unlink parent.lock while a browser holds it (measured
    2026-09-08 — PermissionError "used by another process" while held, deleted
    after close). A stale lock left by a hard kill is swept by the same call.

    On posix an unlink SUCCEEDS even while Firefox holds it, so probing there
    would destroy a live profile's lock and then report it free. Posix returns
    None and the caller falls back to the process list.
    """
    if os.name != "nt":
        return None
    lock = os.path.join(str(profile_dir), "parent.lock")
    if not os.path.exists(lock):
        return False
    try:
        os.remove(lock)
        return False
    except OSError:
        return True


def _kill_pid(pid):
    try:
        if os.name == "nt":
            return subprocess.run(["taskkill", "/PID", str(pid), "/F"],
                                  capture_output=True, text=True,
                                  timeout=20).returncode == 0
        import signal
        os.kill(int(pid), signal.SIGKILL)
        return True
    except Exception:
        return False


def _live_owner(holders, rows, levels=5):
    """The live worker process a holder hangs off, or None if it is an orphan.

    An orphan is ours to clear. A holder whose python worker is still alive is
    a lane that already has an owner — killing its browser mid-render is the
    "one owner per job" collision, so that case refuses instead.
    """
    by_pid = {r["pid"]: r for r in rows}
    for h in holders:
        seen, cur = set(), by_pid.get(h.get("ppid"))
        for _ in range(levels):
            if cur is None or cur["pid"] in seen:
                break
            seen.add(cur["pid"])
            if os.path.basename((cur.get("name") or "").lower()).startswith("python"):
                return cur
            cur = by_pid.get(cur.get("ppid"))
    return None


_OPEN_PROFILES = {}
_OPEN_PROFILES_LOCK = threading.Lock()


def profile_open_in_this_process(profile_dir):
    """True while THIS process still holds an open context on that profile.

    os.getpid() alone cannot tell "my own browser that nobody closed" from "my
    SIBLING's live browser": the multi-account coordinator runs several
    AccountWorkers as threads of ONE python process, so both wear the same pid.
    Killing on a pid match would take a sibling's browser out mid-render.

    An open context is registered here and deregistered when it closes, so a
    profile a live context still owns is refused, while one whose context was
    closed (or never came up) is ours to clear.
    """
    with _OPEN_PROFILES_LOCK:
        return _norm(profile_dir) in _OPEN_PROFILES


def _register_open_profile(profile_dir, ctx):
    key = _norm(profile_dir)
    with _OPEN_PROFILES_LOCK:
        _OPEN_PROFILES[key] = _OPEN_PROFILES.get(key, 0) + 1

    def _drop(*_a):
        with _OPEN_PROFILES_LOCK:
            left = _OPEN_PROFILES.get(key, 0) - 1
            if left > 0:
                _OPEN_PROFILES[key] = left
            else:
                _OPEN_PROFILES.pop(key, None)
    try:
        ctx.on("close", _drop)
    except Exception:
        # No close event to hang off: forget it rather than pin the profile
        # forever, which would refuse every later relaunch in this process.
        _drop()


def profile_lock_gate_enabled(env=None):
    env = os.environ if env is None else env
    return (env.get("PROFILE_LOCK_GATE") or "").strip().lower() not in ("0", "false", "no", "off")


def ensure_profile_unlocked(profile_dir, log=print, settle_s=1.0):
    """Free the profile, or refuse the launch. Never launch into a held lock.

    Returns True when the profile is free. Raises ProfileLockedError when it is
    not — a loud one-second failure naming the pid, instead of two silent
    minutes ending in a misread error.
    """
    if not profile_lock_gate_enabled():
        return True
    if lock_is_held(profile_dir) is False:
        return True

    try:
        rows = _query_processes()
    except ProcessQueryError as exc:
        raise ProfileLockedError(
            f"{profile_dir} is locked and the process list could not be read "
            f"({exc}). Refusing to launch blind — that is the 125-second hang.") from exc
    holders = profile_holders(profile_dir, rows=rows)
    if not holders:
        if lock_is_held(profile_dir) is None:
            return True          # posix, nothing visible: nothing to act on
        raise ProfileLockedError(
            f"{profile_dir} is locked but no process on this machine admits to "
            f"holding it. Close any browser using that profile and relaunch.")

    owner = _live_owner(holders, rows)
    if owner is not None and owner["pid"] != os.getpid():
        raise ProfileLockedError(
            f"{profile_dir} is in use by a LIVE worker (pid {owner['pid']}, "
            f"{owner.get('name')}). That lane already has an owner — stop it "
            f"with `python tools/worker_lifecycle.py sweep` rather than racing it.")
    if owner is not None and profile_open_in_this_process(profile_dir):
        # Same pid, but a context in this process still has it open — the
        # multi-account coordinator's sibling worker, not our own leftover.
        raise ProfileLockedError(
            f"{profile_dir} is already open by another worker in THIS process "
            f"(pid {owner['pid']}). Two workers must not share one profile; "
            f"killing it here would take a live render out mid-flight.")

    log(f"[profile-lock] {len(holders)} orphan browser process(es) still hold "
        f"{profile_dir} — clearing (scoped to this profile only)")
    for h in holders:                       # children first, then their parents
        log(f"[profile-lock]   kill {h['pid']} ({h.get('name')})"
            f"{'' if _kill_pid(h['pid']) else ' — FAILED'}")

    for _ in range(4):
        if settle_s:
            time.sleep(settle_s)
        held = lock_is_held(profile_dir)
        if held is False:
            log(f"[profile-lock] {profile_dir} is free — launching")
            return True
        if held is None:
            # posix: no honest lock probe, so the process list is the verdict.
            # `not None` is truthy, so testing the probe alone declared victory
            # before the killed processes had even gone.
            if not profile_holders(profile_dir):
                log(f"[profile-lock] {profile_dir} has no holder left — launching")
                return True
    raise ProfileLockedError(
        f"{profile_dir} is still locked after clearing "
        f"{[h['pid'] for h in holders]}. Launching now would hang on the "
        f"'Camoufox is already running' dialog, so this launch stops here.")


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

    cf = camoufox_launch_kwargs(kwargs, window=os.environ.get("FIREFOX_WINDOW"))
    return new_firefox_browser(playwright, **cf)


def new_firefox_browser(playwright, **kwargs):
    """THE one door a Camoufox persistent context is created through.

    Everything that opens a Firefox profile comes here — the workers via
    launch_context, and the tools that need their OWN launch kwargs directly —
    so the profile-lock gate cannot be skipped by adding another launch site.
    Before this existed the sweep lived in one launcher and five call sites
    walked past it; `tests/test_profile_lock_gate.py::NoBypasses` now fails the
    build if a new one appears.

    It deliberately does NOT apply camoufox_launch_kwargs. That translation is
    flow_worker's (it forces headless from the environment, pins the window and
    strips Chrome-only keys), and callers like gemini_decode_worker and
    amazon_range_capture pass their own tuned kwargs including an explicit
    `headless` that a translation would silently overrule.
    """
    profile = kwargs.get("user_data_dir")
    if profile:
        ensure_profile_unlocked(profile)
    from camoufox.sync_api import NewBrowser
    ctx = NewBrowser(playwright, persistent_context=True, **kwargs)
    if profile:
        _register_open_profile(profile, ctx)
    return ctx
