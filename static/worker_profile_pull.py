"""Pull the laptop's already-trusted Chrome profile into the worker golden.

Self-contained, stdlib only, importable in isolation (no playwright / no
flow_worker side effects). flow_worker.py calls pull_profile_from_laptop()
at slot-1 startup when ACCOUNT1_LAPTOP_EMAIL / worker_settings.json sets an
email. Empty email => no-op.

WHY copy Local State too: Chrome cookie encryption key lives in
Local State -> os_crypt.encrypted_key (DPAPI-wrapped to the Windows user).
Copying only the profile folder leaves cookies encrypted with the laptop's
key while the golden has a different key -> decryption fails -> trust lost.
Same Windows user + same machine required.
"""
import json
import os
import re as _re
import shutil
import subprocess
import sys
import time


def _profile_account_emails(user_data_dir, folder):
    """All Google account emails in a profile, from its Preferences
    account_info — includes SECONDARY accounts added via 'add another account'
    that Local State's info_cache (primary only) does not list."""
    pref = os.path.join(user_data_dir, folder, "Preferences")
    try:
        with open(pref, "r", encoding="utf-8") as f:
            accs = json.load(f).get("account_info", []) or []
    except (OSError, ValueError):
        return []
    return [str(a.get("email", "")).strip().lower() for a in accs if a.get("email")]


def find_profile_dir_for_email(user_data_dir, email):
    """Return the profile folder name (e.g. 'Default', 'Profile 65') logged into
    `email`, or None. Checks Local State info_cache (primary account) first, then
    each profile's Preferences account_info (catches secondary accounts that
    info_cache omits). Case-insensitive."""
    target = (email or "").strip().lower()
    if not target:
        return None
    # 1. Local State info_cache — the synced/primary account per profile.
    try:
        with open(os.path.join(user_data_dir, "Local State"), "r", encoding="utf-8") as f:
            cache = json.load(f).get("profile", {}).get("info_cache", {})
    except (OSError, ValueError):
        cache = {}
    for folder, info in cache.items():
        if str(info.get("user_name", "")).strip().lower() == target:
            return folder
    # 2. Per-profile Preferences account_info — catches secondary accounts.
    if os.path.isdir(user_data_dir):
        for entry in sorted(os.listdir(user_data_dir)):
            if os.path.isdir(os.path.join(user_data_dir, entry)):
                if target in _profile_account_emails(user_data_dir, entry):
                    return entry
    return None


def list_profile_emails(user_data_dir):
    """List the emails present in info_cache (for diagnostics on no-match)."""
    try:
        with open(os.path.join(user_data_dir, "Local State"), "r", encoding="utf-8") as f:
            state = json.load(f)
    except (OSError, ValueError):
        return []
    cache = state.get("profile", {}).get("info_cache", {})
    return [info.get("user_name", "") for info in cache.values() if info.get("user_name")]


def find_logged_in_profile(user_data_dir):
    """Pick the laptop Chrome profile most likely to hold the Google login when
    no email is given. First prefer a profile flagged signed-in in Local State
    (Chrome sync on); otherwise fall back to the 'Default' profile folder on
    disk — its Cookies hold the Google session even when Chrome sync is OFF
    (logged into Gmail in a tab but not into the browser). None if nothing."""
    try:
        with open(os.path.join(user_data_dir, "Local State"), "r", encoding="utf-8") as f:
            cache = json.load(f).get("profile", {}).get("info_cache", {})
    except (OSError, ValueError):
        cache = {}
    signed_in = [folder for folder, info in cache.items()
                 if str(info.get("user_name", "")).strip()]
    if "Default" in signed_in:
        return "Default"
    if signed_in:
        return sorted(signed_in)[0]
    # No Chrome-sync sign-in info -> fall back to a real profile folder on disk.
    if os.path.isdir(os.path.join(user_data_dir, "Default")):
        return "Default"
    if os.path.isdir(user_data_dir):
        for entry in sorted(os.listdir(user_data_dir)):
            if entry.startswith("Profile") and os.path.isdir(os.path.join(user_data_dir, entry)):
                return entry
    return None


def load_laptop_email(settings_path, env=None):
    """Email from ACCOUNT1_LAPTOP_EMAIL env (override) else
    worker_settings.json {"laptop_email": ...}. '' if neither."""
    env = os.environ if env is None else env
    override = (env.get("ACCOUNT1_LAPTOP_EMAIL") or "").strip()
    if override:
        return override
    try:
        with open(settings_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return str(data.get("laptop_email", "")).strip()
    except (OSError, ValueError):
        return ""


def resolve_laptop_user_data_dir(env=None):
    r"""LAPTOP_CHROME_USER_DATA override, else
    %LOCALAPPDATA%\Google\Chrome\User Data, else None."""
    env = os.environ if env is None else env
    override = env.get("LAPTOP_CHROME_USER_DATA")
    if override:
        return override
    local_appdata = env.get("LOCALAPPDATA")
    if not local_appdata:
        return None
    return os.path.join(local_appdata, "Google", "Chrome", "User Data")


def _channel_for_user_data_dir(user_data_dir):
    """Map a Chrome User Data path to its Playwright launch channel so the
    worker can open the pulled profile with the SAME Chrome channel it came
    from (a Beta profile should be opened by Chrome Beta)."""
    u = (user_data_dir or "").lower()
    if "chrome beta" in u:
        return "chrome-beta"
    if "chrome dev" in u:
        return "chrome-dev"
    if "chrome sxs" in u:
        return "chrome-canary"
    if "chromium" in u:
        return "chromium"
    return "chrome"


def resolve_laptop_user_data_dirs(env=None):
    r"""All Chrome-family User Data dirs to search for the account, in order:
    stable, Beta, Dev, Canary (SxS), Chromium. The Google account may live in
    any channel — Beta has its own %LOCALAPPDATA%\Google\Chrome Beta\User Data.
    LAPTOP_CHROME_USER_DATA override returns just that one."""
    env = os.environ if env is None else env
    override = env.get("LAPTOP_CHROME_USER_DATA")
    if override:
        return [override]
    dirs = []
    local_appdata = env.get("LOCALAPPDATA")
    if local_appdata:
        for parts in (("Google", "Chrome"), ("Google", "Chrome Beta"),
                      ("Google", "Chrome Dev"), ("Google", "Chrome SxS"),
                      ("Chromium",)):
            dirs.append(os.path.join(local_appdata, *parts, "User Data"))
    return dirs


def locate_profile(email):
    """Find (user_data_dir, profile_folder, channel) for `email` across all
    Chrome channels (stable/Beta/Dev/Canary) + secondary accounts. If no email,
    falls back to the signed-in/Default profile. None if nothing found."""
    for ud in resolve_laptop_user_data_dirs():
        if not ud or not os.path.isdir(ud) or not os.path.isfile(os.path.join(ud, "Local State")):
            continue
        pf = find_profile_dir_for_email(ud, email) if email else find_logged_in_profile(ud)
        if pf:
            return ud, pf, _channel_for_user_data_dir(ud)
    return None


def _parse_user_data_dir_from_cmdline(cmdline):
    """Extract --user-data-dir value from a Chrome commandline, or None."""
    if not cmdline:
        return None
    m = _re.search(r'--user-data-dir=("([^"]*)"|(\S+))', cmdline)
    if m:
        return os.path.abspath(m.group(2) if m.group(2) is not None else m.group(3))
    m = _re.search(r'--user-data-dir\s+(?:"([^"]*)"|(\S+))', cmdline)
    if m:
        return os.path.abspath(m.group(1) if m.group(1) is not None else m.group(2))
    return None


def _parse_profile_directory_from_cmdline(cmdline):
    """Extract the --profile-directory value from a Chrome commandline, or None.
    Chrome quotes values containing spaces ('Profile 1' -> "Profile 1")."""
    if not cmdline:
        return None
    m = _re.search(r'--profile-directory=("([^"]*)"|(\S+))', cmdline)
    if m:
        return (m.group(2) if m.group(2) is not None else m.group(3))
    m = _re.search(r'--profile-directory\s+(?:"([^"]*)"|(\S+))', cmdline)
    if m:
        return (m.group(1) if m.group(1) is not None else m.group(2))
    return None


def _chrome_proc_uses_dir(cmdline, target_user_data_dir):
    """True if this Chrome process belongs to target_user_data_dir (explicit
    flag match) OR uses the default profile (no flag) -> which is the laptop
    User Data. Worker slots always pass an explicit different dir -> False."""
    udd = _parse_user_data_dir_from_cmdline(cmdline)
    if udd is None:
        return True
    return udd.rstrip("\\/").lower() == os.path.abspath(target_user_data_dir).rstrip("\\/").lower()


def close_laptop_chrome(user_data_dir, profile_folder=None, log=print):
    """v819.2 — NO LONGER closes any Chrome window/process. The cookie DB lock is
    now released per-file DURING the copy via the Windows Restart Manager (see
    _lean_copy2 / _rm_force_unlock), which force-restarts ONLY the shared Network
    Service that holds Network\\Cookies — Chrome respawns it instantly, so nothing
    the operator sees closes. Operator: 'we just need to close the associated
    [locker], not the whole chrome.'

    Kept as a near no-op so injected close_chrome hooks and older callers stay
    valid. Non-Windows unchanged (the copy path handles locks; nothing to do)."""
    log("close chrome: skipped (v819.2 — per-file cookie-lock release during copy; "
        "Chrome stays open)")
    return


def pull_profile_from_laptop(email, golden_folder, label="",
                             user_data_dir=None, close_chrome=None, log=print):
    """Rebuild `golden_folder` from the laptop Chrome profile logged into
    `email`. Returns True on success, False on any skip (golden left as-is).
    Never raises. `close_chrome` (callable) is invoked to unlock cookie DBs
    before copying; pass a targeted closer in production, a stub in tests."""
    tag = f"[{label}] " if label else ""

    # Search every Chrome channel (stable, Beta, Dev, Canary, Chromium) — the
    # account may live in any of them (e.g. Chrome Beta has its own User Data).
    candidates = [user_data_dir] if user_data_dir else resolve_laptop_user_data_dirs()

    user_data_dir, profile_folder = None, None
    for ud in candidates:
        if not ud or not os.path.isdir(ud) or not os.path.isfile(os.path.join(ud, "Local State")):
            continue
        pf = find_profile_dir_for_email(ud, email) if email else find_logged_in_profile(ud)
        if pf:
            user_data_dir, profile_folder = ud, pf
            break

    if not profile_folder:
        avail = []
        for ud in candidates:
            try:
                avail += list_profile_emails(ud)
            except Exception:
                pass
        if email:
            # Email given => use exactly that account; never pull a different one.
            log(f"{tag}laptop pull: account {email!r} is NOT logged into any Chrome "
                f"channel (stable/Beta/Dev/Canary). Log into it in Chrome first, or "
                f"use one of: {avail}")
        else:
            log(f"{tag}laptop pull: no signed-in Chrome profile found in any channel.")
        return False

    src_profile = os.path.join(user_data_dir, profile_folder)
    if not os.path.isdir(src_profile):
        log(f"{tag}laptop pull: profile folder missing {src_profile}")
        return False
    log(f"{tag}laptop pull: using profile {profile_folder!r} from {user_data_dir}")

    # Unlock cookie/login SQLite DBs by closing ONLY this profile's Chrome tree
    # (v819 — profile-scoped, not the whole channel).
    try:
        if close_chrome is not None:
            close_chrome(user_data_dir, profile_folder)
        else:
            close_laptop_chrome(user_data_dir, profile_folder=profile_folder, log=log)
    except Exception as e:
        log(f"{tag}laptop pull: close chrome failed: {e}")
        return False

    # Build into a temp dir, then atomic swap -- never wipe the live golden first.
    tmp = golden_folder + ".pull_tmp"
    ignore = shutil.ignore_patterns("SingletonLock", "SingletonSocket", "SingletonCookie")
    try:
        if os.path.exists(tmp):
            shutil.rmtree(tmp, ignore_errors=True)
        os.makedirs(tmp, exist_ok=True)
        shutil.copytree(src_profile, os.path.join(tmp, "Default"),
                        ignore=ignore, ignore_dangling_symlinks=True,
                        copy_function=_lean_copy2)
        shutil.copy2(os.path.join(user_data_dir, "Local State"),
                     os.path.join(tmp, "Local State"))
    except Exception as e:
        log(f"{tag}laptop pull: copy failed: {e}")
        shutil.rmtree(tmp, ignore_errors=True)
        return False

    # Swap: rename the old golden aside, move the temp in, then drop the backup.
    # If the move fails, restore the old golden -- never leave the worker with
    # no golden at all (all paths share one parent dir => same volume rename).
    backup = golden_folder + ".old"
    try:
        if os.path.exists(backup):
            shutil.rmtree(backup, ignore_errors=True)
        if os.path.exists(golden_folder):
            os.rename(golden_folder, backup)
        os.rename(tmp, golden_folder)
    except Exception as e:
        log(f"{tag}laptop pull: swap failed: {e}")
        if not os.path.exists(golden_folder) and os.path.exists(backup):
            try:
                os.rename(backup, golden_folder)
            except Exception:
                pass
        shutil.rmtree(tmp, ignore_errors=True)
        return False
    shutil.rmtree(backup, ignore_errors=True)

    channel = _channel_for_user_data_dir(user_data_dir)
    log(f"{tag}pulled laptop profile for {email or '(auto)'} "
        f"({profile_folder} -> golden, channel={channel})")
    return channel


# ── Lean copy-mode (the proven ABE-off path) ───────────────────────────────
# Copy ONLY the golden's durable file set. Excluded entries are caches Chrome
# rebuilds, session-restore files (else it reopens the operator's tabs), and
# lock/log/journal scratch. Everything kept carries the durable login: cookies,
# Login Data, Preferences, Accounts, IndexedDB (Flow app session), Local Storage,
# Network bound-session tokens, Web Data.
_LEAN_EXCLUDE_DIRS = {
    "Cache", "Code Cache", "GPUCache", "GrShaderCache", "ShaderCache",
    "DawnGraphiteCache", "DawnWebGPUCache", "GPUPersistentCache",
    "component_crx_cache", "extensions_crx_cache", "Crashpad", "blob_storage",
    "Safe Browsing", "Safe Browsing Network", "AutofillAiModelCache",
    "Sessions", "JumpListIconsMostVisited", "JumpListIconsRecentClosed",
    "VideoDecodeStats",
}
_LEAN_EXCLUDE_FILES = {
    "SingletonLock", "SingletonSocket", "SingletonCookie",
    "LOCK", "LOG", "LOG.old",
    "Current Tabs", "Current Session", "Last Tabs", "Last Session",
}


def _lean_ignore(dirpath, names):
    """shutil.copytree ignore callback for the lean golden copy."""
    drop = set()
    for n in names:
        if n in _LEAN_EXCLUDE_FILES or n.endswith("-journal"):
            drop.add(n)
            continue
        if n in _LEAN_EXCLUDE_DIRS and os.path.isdir(os.path.join(dirpath, n)):
            drop.add(n)
    return drop


# ── v819.2: copy a Chrome-locked cookie DB WITHOUT closing any window ─────────
# Chrome exclusively locks Network\Cookies, so a plain copy fails (WinError 32).
# The Windows Restart Manager can shut down ONLY the process holding a lock on
# THAT specific file — for the cookie store that is the shared Network Service
# (a `chrome.exe --type=utility` child), which Chrome respawns instantly. So no
# window closes: we just read the file in the sub-second gap before it re-locks.
# We NEVER RM a file locked by the MAIN browser process (a locker with no
# `--type=`), so we can't accidentally close the operator's Chrome.

_RM_SAFE_FILES = {"cookies"}  # only the Network Service holds this


def _pid_cmdline(pid):
    """Best-effort command line for a PID (Windows). '' on failure."""
    if sys.platform != "win32":
        return ""
    try:
        ps = (f"(Get-CimInstance Win32_Process -Filter \"ProcessId={int(pid)}\")"
              ".CommandLine")
        return (subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command", ps],
            capture_output=True, text=True, timeout=10).stdout or "").strip()
    except Exception:
        return ""


def _rm_lockers(path):
    """Return the list of PIDs holding a lock on `path` via the Restart Manager,
    or None on failure. Empty list = not locked."""
    if sys.platform != "win32":
        return None
    try:
        from ctypes import (windll, byref, create_unicode_buffer, c_wchar_p, cast,
                             Structure, sizeof, c_uint, c_int)
        from ctypes.wintypes import DWORD, WCHAR, FILETIME, BOOL
    except Exception:
        return None

    class RM_UNIQUE_PROCESS(Structure):
        _fields_ = [("dwProcessId", DWORD), ("ProcessStartTime", FILETIME)]

    class RM_PROCESS_INFO(Structure):
        _fields_ = [("Process", RM_UNIQUE_PROCESS), ("strAppName", WCHAR * 256),
                    ("strServiceShortName", WCHAR * 64), ("ApplicationType", c_int),
                    ("AppStatus", c_uint), ("TSSessionId", DWORD), ("bRestartable", BOOL)]

    ERROR_SUCCESS, ERROR_MORE_DATA = 0, 234
    try:
        rm = windll.LoadLibrary("Rstrtmgr")
    except Exception:
        return None
    sh = DWORD(0)
    key = (WCHAR * 256)()
    if rm.RmStartSession(byref(sh), 0, key) != ERROR_SUCCESS:
        return None
    try:
        buf = create_unicode_buffer(path)
        arr = (c_wchar_p * 1)(cast(buf, c_wchar_p))
        if rm.RmRegisterResources(sh, 1, arr, 0, None, 0, None) != ERROR_SUCCESS:
            return None
        need = DWORD(0)
        got = DWORD(0)
        rea = DWORD(0)
        r = rm.RmGetList(sh, byref(need), byref(got), None, byref(rea))
        if r not in (ERROR_SUCCESS, ERROR_MORE_DATA):
            return None
        if not need.value:
            return []
        infos = (RM_PROCESS_INFO * need.value)()
        got = DWORD(need.value)
        if rm.RmGetList(sh, byref(need), byref(got), infos, byref(rea)) != ERROR_SUCCESS:
            return None
        return [int(infos[i].Process.dwProcessId) for i in range(got.value)]
    finally:
        rm.RmEndSession(sh)


def _rm_force_unlock(path, log=print):
    """Force-release the lock on `path` by shutting down its lockers — but ONLY
    if every locker is a Chrome child process (`--type=`), never the main
    browser. Returns True if a shutdown ran. Safe no-op on any failure."""
    if sys.platform != "win32":
        return False
    pids = _rm_lockers(path)
    if not pids:  # None (failure) or [] (not locked)
        return False
    # Guard: refuse to shut down the MAIN browser process (no --type=) so we can
    # never close the operator's Chrome window. The cookie store's locker is the
    # Network Service (--type=utility); if anything else holds it, bail.
    for pid in pids:
        cl = _pid_cmdline(pid).lower()
        if "chrome.exe" in cl and "--type=" not in cl:
            log(f"  cookie lock held by a main Chrome process (pid {pid}) — "
                f"NOT force-closing (would close the window); skipping")
            return False
    try:
        from ctypes import windll, byref, create_unicode_buffer, WINFUNCTYPE, c_wchar_p, cast
        from ctypes.wintypes import DWORD, WCHAR, UINT
        ERROR_SUCCESS, RmForceShutdown = 0, 1
        rm = windll.LoadLibrary("Rstrtmgr")

        @WINFUNCTYPE(None, UINT)
        def _cb(_p):
            return None

        sh = DWORD(0)
        key = (WCHAR * 256)()
        if rm.RmStartSession(byref(sh), 0, key) != ERROR_SUCCESS:
            return False
        try:
            buf = create_unicode_buffer(path)
            arr = (c_wchar_p * 1)(cast(buf, c_wchar_p))
            if rm.RmRegisterResources(sh, 1, arr, 0, None, 0, None) != ERROR_SUCCESS:
                return False
            return rm.RmShutdown(sh, RmForceShutdown, _cb) == ERROR_SUCCESS
        finally:
            rm.RmEndSession(sh)
    except Exception as e:
        try:
            log(f"  RM unlock error on {os.path.basename(path)}: {e}")
        except Exception:
            pass
        return False


def _read_unlocked_bytes(path, timeout=2.0):
    """Grab a file's bytes in a tight no-sleep loop right after its lock is
    released, before Chrome re-locks it. Returns bytes or None."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            with open(path, "rb") as f:
                return f.read()
        except (PermissionError, OSError):
            continue  # still locked — keep hammering (no sleep = win the race)
    return None


def _lean_copy2(src, dst):
    """shutil.copytree copy_function that survives Chrome's cookie-DB lock
    WITHOUT closing any window. Normal copy first; on a lock, the cookie store is
    read via the Restart Manager (Network Service only) + tight read; any other
    locked file is skipped (not needed for Flow auth). NEVER raises on a lock, so
    a single locked file can't abort the whole profile copy."""
    try:
        shutil.copy2(src, dst)
        return
    except (PermissionError, OSError) as e:
        if getattr(e, "winerror", None) not in (32, 33):  # not a sharing/lock error
            raise
    base = os.path.basename(src).lower()
    if base in _RM_SAFE_FILES and _rm_force_unlock(src):
        data = _read_unlocked_bytes(src)
        if data is not None:
            try:
                with open(dst, "wb") as f:
                    f.write(data)
                return
            except Exception:
                pass
    # Couldn't get it, or not safe to unlock → skip this file (best-effort).
    return


def build_lean_golden_from_profile(email, golden_folder, label="",
                                   user_data_dir=None, close_chrome=None, log=print):
    """COPY-MODE (App-Bound Encryption must be OFF). Build `golden_folder` as a
    clean single-profile Chrome user-data-dir from the real laptop profile logged
    into `email`, copying ONLY the golden's durable file set (see _LEAN_EXCLUDE_*).
    Rewrites Local State so the lone profile is `Default`=email and patches
    Preferences for a clean exit (no tab restore). Atomic swap; never raises.
    Returns the launch channel string on success, False on any skip.

    The real profile is READ ONLY (never automated/killed beyond one flush-close),
    so it cannot be signed out. Replaces the retired net-log capture+inject."""
    tag = f"[{label}] " if label else ""

    candidates = [user_data_dir] if user_data_dir else resolve_laptop_user_data_dirs()
    user_data_dir, profile_folder = None, None
    for ud in candidates:
        if not ud or not os.path.isdir(ud) or not os.path.isfile(os.path.join(ud, "Local State")):
            continue
        pf = find_profile_dir_for_email(ud, email) if email else find_logged_in_profile(ud)
        if pf:
            user_data_dir, profile_folder = ud, pf
            break
    if not profile_folder:
        log(f"{tag}lean golden: {email!r} not logged into any Chrome channel")
        return False
    src_profile = os.path.join(user_data_dir, profile_folder)
    if not os.path.isdir(src_profile):
        log(f"{tag}lean golden: profile folder missing {src_profile}")
        return False
    log(f"{tag}lean golden: copying profile {profile_folder!r} from {user_data_dir}")

    # Close ONLY this profile's Chrome so its cookie/login DBs are unlocked +
    # flushed (v819 — profile-scoped, not the whole channel).
    try:
        if close_chrome is not None:
            close_chrome(user_data_dir, profile_folder)
        else:
            close_laptop_chrome(user_data_dir, profile_folder=profile_folder, log=log)
    except Exception as e:
        log(f"{tag}lean golden: close chrome failed: {e}")
        return False

    tmp = golden_folder + ".lean_tmp"
    try:
        if os.path.exists(tmp):
            shutil.rmtree(tmp, ignore_errors=True)
        os.makedirs(tmp, exist_ok=True)
        # 1. profile -> Default (lean)
        shutil.copytree(src_profile, os.path.join(tmp, "Default"),
                        ignore=_lean_ignore, ignore_dangling_symlinks=True,
                        copy_function=_lean_copy2)
        # 2. Local State (DPAPI cookie key) -> golden root, single Default profile
        with open(os.path.join(user_data_dir, "Local State"), "r", encoding="utf-8") as f:
            ls = json.load(f)
        prof = ls.setdefault("profile", {})
        entry = (prof.get("info_cache", {}) or {}).get(profile_folder, {})
        prof["info_cache"] = {"Default": entry}
        prof["last_used"] = "Default"
        prof["last_active_profiles"] = ["Default"]
        with open(os.path.join(tmp, "Local State"), "w", encoding="utf-8") as f:
            json.dump(ls, f)
        # 3. Preferences: clean exit so Chrome does not reopen the operator's tabs
        _pref = os.path.join(tmp, "Default", "Preferences")
        try:
            with open(_pref, "r", encoding="utf-8") as f:
                pr = json.load(f)
            pr.setdefault("profile", {})["exit_type"] = "Normal"
            pr["profile"]["exited_cleanly"] = True
            pr.setdefault("session", {})["restore_on_startup"] = 5
            with open(_pref, "w", encoding="utf-8") as f:
                json.dump(pr, f)
        except (OSError, ValueError):
            pass  # missing/unreadable -> Chrome rebuilds it
    except Exception as e:
        log(f"{tag}lean golden: copy failed: {e}")
        shutil.rmtree(tmp, ignore_errors=True)
        return False

    # Atomic swap -- never leave the worker with no golden (same volume rename).
    backup = golden_folder + ".old"
    try:
        if os.path.exists(backup):
            shutil.rmtree(backup, ignore_errors=True)
        if os.path.exists(golden_folder):
            os.rename(golden_folder, backup)
        os.rename(tmp, golden_folder)
    except Exception as e:
        log(f"{tag}lean golden: swap failed: {e}")
        if not os.path.exists(golden_folder) and os.path.exists(backup):
            try:
                os.rename(backup, golden_folder)
            except Exception:
                pass
        shutil.rmtree(tmp, ignore_errors=True)
        return False
    shutil.rmtree(backup, ignore_errors=True)

    channel = _channel_for_user_data_dir(user_data_dir)
    log(f"{tag}lean golden built for {email or '(auto)'} "
        f"({profile_folder} -> golden/Default, channel={channel})")
    return channel
