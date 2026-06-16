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


def _chrome_proc_uses_dir(cmdline, target_user_data_dir):
    """True if this Chrome process belongs to target_user_data_dir (explicit
    flag match) OR uses the default profile (no flag) -> which is the laptop
    User Data. Worker slots always pass an explicit different dir -> False."""
    udd = _parse_user_data_dir_from_cmdline(cmdline)
    if udd is None:
        return True
    return udd.rstrip("\\/").lower() == os.path.abspath(target_user_data_dir).rstrip("\\/").lower()


def close_laptop_chrome(user_data_dir, log=print):
    """Close ONLY the Chrome channel that owns user_data_dir (e.g. Chrome Beta),
    sparing the operator's OTHER Chrome (stable stays open). Matches processes
    whose command line contains the channel's app folder, e.g. '\\Chrome Beta\\'.
    Windows-only via PowerShell CIM (`wmic` is removed on Win11). On enumerate
    failure it closes nothing (safer than killing the wrong Chrome). Sleeps after
    so the cookie DB file lock is released before we read it."""
    if sys.platform != "win32":
        try:
            subprocess.run(["pkill", "-f", "chrome"], capture_output=True)
            time.sleep(1.0)
        except Exception as e:
            log(f"close chrome: pkill failed: {e}")
        return
    # ...\Google\Chrome Beta\User Data -> folder "Chrome Beta" -> needle "\chrome beta\"
    folder = os.path.basename(os.path.dirname(os.path.normpath(user_data_dir)))
    needle = (os.sep + folder + os.sep).lower()
    killed = 0
    try:
        ps = ("Get-CimInstance Win32_Process -Filter \"name='chrome.exe'\" | "
              "ForEach-Object { \"$($_.ProcessId)`t$($_.CommandLine)\" }")
        out = subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command", ps],
            capture_output=True, text=True, timeout=30).stdout
        for row in out.splitlines():
            row = row.strip()
            if not row:
                continue
            pid, _, cmd = row.partition("\t")
            pid = pid.strip()
            if pid and needle in (cmd or "").lower():
                try:
                    subprocess.run(["taskkill", "/F", "/PID", pid], capture_output=True)
                    killed += 1
                except Exception:
                    pass
        log(f"close chrome: closed {killed} '{folder}' process(es); other Chrome left running")
    except Exception as e:
        log(f"close chrome: enumerate failed ({e}); closed nothing (safe)")
    time.sleep(2.0)  # let Windows release the cookie DB file lock


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

    # Unlock cookie/login SQLite DBs by closing the laptop Chrome that owns this
    # profile (the channel we matched).
    try:
        if close_chrome is not None:
            close_chrome(user_data_dir)
        else:
            close_laptop_chrome(user_data_dir, log=log)
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
                        ignore=ignore, ignore_dangling_symlinks=True)
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
