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


def find_profile_dir_for_email(user_data_dir, email):
    """Return the profile folder name (e.g. 'Default', 'Profile 3') logged
    into `email`, or None. Match is case-insensitive on info_cache.user_name."""
    local_state_path = os.path.join(user_data_dir, "Local State")
    try:
        with open(local_state_path, "r", encoding="utf-8") as f:
            state = json.load(f)
    except (OSError, ValueError):
        return None
    cache = state.get("profile", {}).get("info_cache", {})
    target = (email or "").strip().lower()
    if not target:
        return None
    for folder, info in cache.items():
        if str(info.get("user_name", "")).strip().lower() == target:
            return folder
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
    """Force-close Chrome processes belonging to the laptop User Data (and
    default-profile Chrome). Leaves worker-slot Chrome (explicit other
    --user-data-dir) running. Windows-first; best-effort elsewhere."""
    if sys.platform != "win32":
        try:
            subprocess.run(["pkill", "-f", "chrome"], capture_output=True)
        except Exception as e:
            log(f"laptop pull: pkill failed: {e}")
        return
    try:
        out = subprocess.run(
            ["wmic", "process", "where", "name='chrome.exe'",
             "get", "ProcessId,CommandLine", "/format:list"],
            capture_output=True, text=True).stdout
    except Exception as e:
        log(f"laptop pull: wmic enumerate failed: {e}")
        return
    pid, cmd = None, None
    killed = 0
    for line in out.splitlines():
        line = line.strip()
        if line.startswith("CommandLine="):
            cmd = line[len("CommandLine="):]
        elif line.startswith("ProcessId="):
            pid = line[len("ProcessId="):].strip()
            # Require a real CommandLine for this record; a None/empty cmd means
            # we could not read it (access denied / record boundary) -> skip,
            # never kill on a stale or missing commandline.
            if pid and cmd and _chrome_proc_uses_dir(cmd, user_data_dir):
                try:
                    subprocess.run(["taskkill", "/F", "/PID", pid], capture_output=True)
                    killed += 1
                except Exception:
                    pass
            pid, cmd = None, None
    log(f"laptop pull: closed {killed} laptop Chrome process(es)")


def pull_profile_from_laptop(email, golden_folder, label="",
                             user_data_dir=None, close_chrome=None, log=print):
    """Rebuild `golden_folder` from the laptop Chrome profile logged into
    `email`. Returns True on success, False on any skip (golden left as-is).
    Never raises. `close_chrome` (callable) is invoked to unlock cookie DBs
    before copying; pass a targeted closer in production, a stub in tests."""
    tag = f"[{label}] " if label else ""
    if not email:
        return False

    user_data_dir = user_data_dir or resolve_laptop_user_data_dir()
    if not user_data_dir or not os.path.isdir(user_data_dir):
        log(f"{tag}laptop pull: Chrome User Data not found ({user_data_dir})")
        return False
    if not os.path.isfile(os.path.join(user_data_dir, "Local State")):
        log(f"{tag}laptop pull: Local State missing in {user_data_dir}")
        return False

    profile_folder = find_profile_dir_for_email(user_data_dir, email)
    if not profile_folder:
        log(f"{tag}laptop pull: email {email!r} not found. Available: {list_profile_emails(user_data_dir)}")
        return False
    src_profile = os.path.join(user_data_dir, profile_folder)
    if not os.path.isdir(src_profile):
        log(f"{tag}laptop pull: profile folder missing {src_profile}")
        return False

    # Unlock cookie/login SQLite DBs by closing the laptop's Chrome.
    if close_chrome is not None:
        try:
            close_chrome()
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

    log(f"{tag}pulled laptop profile for {email} ({profile_folder} -> golden)")
    return True
