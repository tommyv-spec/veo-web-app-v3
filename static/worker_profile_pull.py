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


def _chrome_user_data_dirs_for_platform(env):
    r"""Ordered Chrome-family profile roots for THIS OS (stable, Beta, Dev,
    Canary, Chromium). The "user data dir" = the folder that DIRECTLY holds
    `Local State` + the profile subfolders (Default, Profile 1, ...):
      Windows  %LOCALAPPDATA%\Google\Chrome\User Data
      macOS    ~/Library/Application Support/Google/Chrome   (NO "User Data")
      Linux    ~/.config/google-chrome                       (NO "User Data")
    """
    dirs = []
    if sys.platform == "win32":
        local_appdata = env.get("LOCALAPPDATA")
        if local_appdata:
            for parts in (("Google", "Chrome"), ("Google", "Chrome Beta"),
                          ("Google", "Chrome Dev"), ("Google", "Chrome SxS"),
                          ("Chromium",)):
                dirs.append(os.path.join(local_appdata, *parts, "User Data"))
    elif sys.platform == "darwin":
        home = env.get("HOME") or os.path.expanduser("~")
        base = os.path.join(home, "Library", "Application Support")
        for parts in (("Google", "Chrome"), ("Google", "Chrome Beta"),
                      ("Google", "Chrome Dev"), ("Google", "Chrome Canary"),
                      ("Chromium",)):
            dirs.append(os.path.join(base, *parts))
    else:  # linux / other posix
        home = env.get("HOME") or os.path.expanduser("~")
        base = os.path.join(home, ".config")
        for name in ("google-chrome", "google-chrome-beta",
                     "google-chrome-unstable", "chromium"):
            dirs.append(os.path.join(base, name))
    return dirs


def resolve_laptop_user_data_dir(env=None):
    r"""LAPTOP_CHROME_USER_DATA override, else the OS-default STABLE Chrome
    user-data dir (Win %LOCALAPPDATA%\Google\Chrome\User Data / macOS
    ~/Library/Application Support/Google/Chrome / Linux ~/.config/google-chrome),
    else None."""
    env = os.environ if env is None else env
    override = env.get("LAPTOP_CHROME_USER_DATA")
    if override:
        return override
    dirs = _chrome_user_data_dirs_for_platform(env)
    return dirs[0] if dirs else None


def _channel_for_user_data_dir(user_data_dir):
    """Map a Chrome User Data path to its Playwright launch channel so the
    worker can open the pulled profile with the SAME Chrome channel it came
    from (a Beta profile should be opened by Chrome Beta). Handles Win
    'Chrome SxS', macOS 'Chrome Canary', and Linux hyphen names."""
    u = (user_data_dir or "").lower()
    if "chrome beta" in u or "google-chrome-beta" in u:
        return "chrome-beta"
    if "chrome dev" in u or "google-chrome-unstable" in u:
        return "chrome-dev"
    if "chrome sxs" in u or "chrome canary" in u:
        return "chrome-canary"
    if "chromium" in u:
        return "chromium"
    return "chrome"


def resolve_laptop_user_data_dirs(env=None):
    r"""All Chrome-family user-data dirs to search for the account, in order:
    stable, Beta, Dev, Canary, Chromium — OS-aware (see
    _chrome_user_data_dirs_for_platform). The Google account may live in any
    channel. LAPTOP_CHROME_USER_DATA override returns just that one."""
    env = os.environ if env is None else env
    override = env.get("LAPTOP_CHROME_USER_DATA")
    if override:
        return [override]
    return _chrome_user_data_dirs_for_platform(env)


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


# Channels that are NOT the operator's daily stable Chrome. Same set the
# chatgpt worker's session pull uses (chatgpt_session_pull._NON_STABLE).
_NON_STABLE_TAGS = ("chrome beta", "chrome dev", "chrome sxs", "chrome canary",
                    "chromium", "google-chrome-beta", "google-chrome-unstable")


def locate_profile_beta_first(email):
    """v892 — like locate_profile, but scan NON-STABLE channels (Beta/Dev/
    Canary/Chromium) FIRST, exactly like the chatgpt worker's session pull.

    Why prefer Beta: copying from a non-stable channel closes only THAT
    channel's window — the operator's daily stable Chrome is never touched —
    and non-stable channels run with App-Bound Encryption off (v10 cookies),
    so the copied golden decrypts reliably. Falls back to the normal
    stable-first scan when the account is not in any non-stable channel, so
    existing stable-only setups keep working unchanged."""
    for ud in resolve_laptop_user_data_dirs():
        if not ud or not os.path.isdir(ud) or not os.path.isfile(os.path.join(ud, "Local State")):
            continue
        if not any(tag in ud.lower() for tag in _NON_STABLE_TAGS):
            continue
        pf = find_profile_dir_for_email(ud, email) if email else find_logged_in_profile(ud)
        if pf:
            return ud, pf, _channel_for_user_data_dir(ud)
    return locate_profile(email)


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


# ── v824 per-profile release (no whole-channel kill) ───────────────────────
# A loaded Chrome profile's per-profile SQLite DBs (Cookies / Login Data /
# Web Data) are locked by the SHARED processes (one Network Service + one main
# browser per user-data-dir). When Chrome UNLOADS a profile (its last window
# closes and no keep-alive pins it), those shared procs release THAT profile's
# file handles in-process — the other profiles + the rest of the channel keep
# running. So if the target profile is already unloaded we copy it live with
# ZERO kills. Windows Restart Manager tells us exactly who (if anyone) holds
# the profile's DBs. Verified 2026-07-06: copied Profile 61 while Profile 75
# stayed locked/loaded in the same Beta channel, no process killed.
def _profile_db_paths(user_data_dir, profile_folder):
    """The per-profile files whose lock proves the profile is still loaded."""
    base = os.path.join(user_data_dir, profile_folder)
    return [os.path.join(base, "Network", "Cookies"),
            os.path.join(base, "Login Data"),
            os.path.join(base, "Web Data")]


def _profile_lock_holders(paths):
    """PIDs holding any of `paths` open, via Windows Restart Manager. Empty set
    when nothing holds them (profile unloaded => safe to copy). Returns None on
    non-Windows / RM unavailable (caller then falls back to channel close)."""
    if sys.platform != "win32":
        return None
    paths = [p for p in paths if os.path.isfile(p)]
    if not paths:
        return set()  # no DBs on disk yet => nothing locked
    try:
        import ctypes
        import ctypes.wintypes as w
        rm = ctypes.windll.rstrtmgr

        class _RUP(ctypes.Structure):
            _fields_ = [("dwProcessId", w.DWORD), ("t", w.FILETIME)]

        class _RPI(ctypes.Structure):
            _fields_ = [("Process", _RUP), ("app", w.WCHAR * 256),
                        ("svc", w.WCHAR * 64), ("at", ctypes.c_int),
                        ("st", w.ULONG), ("ts", w.DWORD), ("r", w.BOOL)]

        sess = w.DWORD(0)
        key = (ctypes.c_wchar * 65)()
        if rm.RmStartSession(ctypes.byref(sess), 0, key) != 0:
            return None
        try:
            _MORE_DATA = 234  # ERROR_MORE_DATA: expected on the sizing call
            arr = (w.LPCWSTR * len(paths))(*paths)
            if rm.RmRegisterResources(sess, len(paths), arr, 0, None, 0, None) != 0:
                return None
            need = w.UINT(0)
            n = w.UINT(0)
            rb = w.DWORD(0)
            # sizing call: 0 => no holders, ERROR_MORE_DATA => `need` holders exist
            rc = rm.RmGetList(sess, ctypes.byref(need), ctypes.byref(n), None, ctypes.byref(rb))
            if rc not in (0, _MORE_DATA):
                return None
            if need.value == 0:
                return set()
            buf = (_RPI * need.value)()
            n = w.UINT(need.value)
            # data call must succeed (0) before we read buf, else it is garbage
            if rm.RmGetList(sess, ctypes.byref(need), ctypes.byref(n), buf, ctypes.byref(rb)) != 0:
                return None
            return {buf[i].Process.dwProcessId for i in range(n.value)}
        finally:
            rm.RmEndSession(sess)
    except Exception:
        return None


def _wait_profile_unlocked(user_data_dir, profile_folder, wait_s=8, log=print):
    """Poll the target profile's DBs; return True as soon as they are unlocked
    (profile unloaded -> copyable with no kill). False if still locked after
    wait_s, or if RM is unavailable (caller falls back to channel close)."""
    paths = _profile_db_paths(user_data_dir, profile_folder)
    deadline = wait_s
    waited = 0.0
    while True:
        holders = _profile_lock_holders(paths)
        if holders is None:
            return False  # RM unavailable -> let caller channel-close
        if not holders:
            return True   # unloaded -> safe to copy, nothing killed
        if waited >= deadline:
            log(f"  {profile_folder!r} still loaded (held by {sorted(holders)}) "
                f"after {int(waited)}s")
            return False
        time.sleep(1.0)
        waited += 1.0


def _profile_display_name(user_data_dir, profile_folder):
    """The Chrome profile's toolbar display name (info_cache[folder].name), used
    to match its avatar button in the UI Automation tree. '' if unknown."""
    try:
        with open(os.path.join(user_data_dir, "Local State"), "r", encoding="utf-8") as f:
            ic = json.load(f).get("profile", {}).get("info_cache", {})
        return str(ic.get(profile_folder, {}).get("name", "")).strip()
    except (OSError, ValueError):
        return ""


# PowerShell UI-Automation: close ONLY the windows whose profile-avatar button
# name matches the target profile. Depth-limited walk of the browser FRAME
# (skips the web Document subtree) so it is fast + never hangs on a heavy page.
# Verified 2026-07-06: closing just Profile 61's ("Kevin") window unloaded it in
# ~4s while Profile 75 + the rest of Beta kept running.
_UIA_CLOSE_PS = r'''
$ErrorActionPreference='SilentlyContinue'
Add-Type -AssemblyName UIAutomationClient,UIAutomationTypes | Out-Null
$sig=@"
using System;using System.Runtime.InteropServices;using System.Text;using System.Collections.Generic;
public class WN{
 [DllImport("user32.dll")]public static extern bool EnumWindows(EnumWindowsProc cb,IntPtr p);
 [DllImport("user32.dll")]public static extern bool IsWindowVisible(IntPtr h);
 [DllImport("user32.dll",CharSet=CharSet.Unicode)]public static extern int GetClassName(IntPtr h,StringBuilder s,int m);
 [DllImport("user32.dll")]public static extern int GetWindowThreadProcessId(IntPtr h,out int pid);
 [DllImport("user32.dll")]public static extern bool PostMessage(IntPtr h,uint m,IntPtr w,IntPtr l);
 public delegate bool EnumWindowsProc(IntPtr h,IntPtr p);
 public static List<IntPtr> Wins(){var r=new List<IntPtr>();EnumWindows((h,p)=>{if(IsWindowVisible(h)){var sb=new StringBuilder(64);GetClassName(h,sb,64);if(sb.ToString().StartsWith("Chrome_WidgetWin")){r.Add(h);}}return true;},IntPtr.Zero);return r;}
}
"@
Add-Type -TypeDefinition $sig
$target=$env:TGT_NAME
# escape any *, ?, [ ] in the profile name so -like treats it literally (not a glob)
$tesc=[System.Management.Automation.WildcardPattern]::Escape($target)
$needle=$env:TGT_NEEDLE
$dry=($env:TGT_DRYRUN -eq '1')
# pids whose cmdline belongs to the target channel
$pids=@{}
Get-CimInstance Win32_Process -Filter "name='chrome.exe'" | ForEach-Object { if($_.CommandLine -and $_.CommandLine.ToLower().Contains($needle)){$pids[[int]$_.ProcessId]=$true} }
$walker=[System.Windows.Automation.TreeWalker]::ControlViewWalker
$AE=[System.Windows.Automation.AutomationElement]
$btnType=[System.Windows.Automation.ControlType]::Button
$docType=[System.Windows.Automation.ControlType]::Document
$closed=0
foreach($h in [WN]::Wins()){
  # NB: $pid is a read-only automatic var in PowerShell -> use $wpid.
  $wpid=0;[WN]::GetWindowThreadProcessId($h,[ref]$wpid)|Out-Null
  if(-not $pids.ContainsKey($wpid)){continue}
  $match=$false
  try{
    $root=$AE::FromHandle($h); if($root -eq $null){continue}
    # depth-limited BFS over the frame; skip Document (web page) subtrees
    $q=New-Object System.Collections.Queue
    $q.Enqueue(@($root,0))
    while($q.Count -gt 0 -and -not $match){
      $it=$q.Dequeue(); $el=$it[0]; $d=$it[1]
      if($d -gt 10){continue}
      $c=$walker.GetFirstChild($el)
      while($c -ne $null){
        $ct=$c.Current.ControlType
        if($ct -ne $docType){
          if($ct -eq $btnType){
            $nm=$c.Current.Name
            if($nm -eq $target -or $nm -like "$tesc*"){$match=$true;break}
          }
          $q.Enqueue(@($c,$d+1))
        }
        $c=$walker.GetNextSibling($c)
      }
    }
  }catch{ continue }   # transient UIA/RPC fault on one window -> skip it
  if($match){
    $hv=[int64]$h
    if($dry){ Write-Output "WOULD_CLOSE $hv" }
    else{ [WN]::PostMessage($h,0x0010,[IntPtr]::Zero,[IntPtr]::Zero)|Out-Null; Write-Output "CLOSED $hv"; $closed++ }
  }
}
Write-Output "DONE closed=$closed dry=$dry target=$target"
'''


def _profiles_sharing_name(user_data_dir, name):
    """Profile folders whose info_cache display name == `name` (case-insensitive).
    Used to detect an ambiguous avatar name (two profiles labelled the same), where
    a UIA close-by-name would close BOTH profiles' windows."""
    try:
        with open(os.path.join(user_data_dir, "Local State"), "r", encoding="utf-8") as f:
            ic = json.load(f).get("profile", {}).get("info_cache", {})
    except (OSError, ValueError):
        return []
    t = (name or "").strip().lower()
    return [folder for folder, info in ic.items()
            if str(info.get("name", "")).strip().lower() == t]


def _close_target_profile_windows(user_data_dir, profile_folder, log=print, dry_run=False):
    """Gracefully WM_CLOSE only the target profile's Chrome windows (matched by
    avatar-button name via UIA), sparing every other profile + the channel.
    Windows-only. Returns the count closed (0 on any failure -> caller falls back
    to channel close). Returns -1 when the avatar name is AMBIGUOUS (shared by
    another profile) -> the caller must NOT close (would nuke the sibling) and must
    NOT channel-fall-back. dry_run just reports which windows WOULD close."""
    if sys.platform != "win32":
        return 0
    name = _profile_display_name(user_data_dir, profile_folder)
    if not name:
        log(f"  no display name for {profile_folder!r} -> cannot target its window")
        return 0
    # Ambiguity guard: a close-by-name would close EVERY window whose avatar shows
    # this name. That only harms a sibling profile if a same-named sibling is
    # actually OPEN. So refuse ONLY when a same-named sibling is currently loaded
    # (has lock-holders); if the siblings are closed, targeting by name is safe
    # (only this profile has windows).
    others = [p for p in _profiles_sharing_name(user_data_dir, name) if p != profile_folder]
    loaded_others = [p for p in others
                     if _profile_lock_holders(_profile_db_paths(user_data_dir, p))]
    if loaded_others:
        log(f"  AMBIGUOUS profile name {name!r}: also used by OPEN profile(s) "
            f"{loaded_others} — closing by name would also close them. Rename the "
            f"target profile to a unique name, or close {loaded_others} first. "
            f"NOT closing anything.")
        return -1
    if others:
        log(f"  note: name {name!r} also used by {others}, but they are not open -> "
            f"safe to close only {profile_folder!r}'s window(s).")
    folder = os.path.basename(os.path.dirname(os.path.normpath(user_data_dir)))
    needle = (os.sep + folder + os.sep).lower()
    try:
        import base64
        enc = base64.b64encode(_UIA_CLOSE_PS.encode("utf-16-le")).decode("ascii")
        env = dict(os.environ, TGT_NAME=name, TGT_NEEDLE=needle,
                   TGT_DRYRUN=("1" if dry_run else "0"))
        res = subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-EncodedCommand", enc],
            capture_output=True, text=True, timeout=60, env=env)
        out = res.stdout or ""
        if res.returncode != 0 and "DONE" not in out:
            # PS crashed before finishing -> trust nothing it printed; channel-close
            log(f"  uia close: powershell rc={res.returncode}; "
                f"treating as no windows closed (will fall back)")
            return 0
        closed = 0
        for row in out.splitlines():
            row = row.strip()
            if row.startswith("CLOSED") or row.startswith("WOULD_CLOSE"):
                log(f"  {'would close' if dry_run else 'closed'} {profile_folder!r} "
                    f"window {row.split()[-1]} (avatar={name!r})")
                closed += 1
            elif row.startswith("DONE"):
                log(f"  uia close: {row}")
        return closed
    except Exception as e:
        log(f"  uia close failed ({e}); will fall back to channel close")
        return 0


def _release_target_profile(user_data_dir, profile_folder, close_chrome, log,
                            allow_channel_close=True):
    """Unlock the target profile for copying with the LEAST disruption:
    1. If it is already unloaded -> copy live, kill NOTHING (common case: the
       account is not actively open in a window).
    2. Else gracefully close ONLY that profile's windows (UIA avatar match) and
       wait for it to unload -> still spares every other profile + the channel.
    3. Else (UIA unavailable, profile pinned by a keep-alive, or won't unload):
       if allow_channel_close, close the whole Chrome channel (previous behavior);
       otherwise RAISE (a primary/daily profile with background apps never unloads
       on window-close, and channel-closing it would kill the operator's whole
       Chrome — refuse instead so the caller can tell them to use a dedicated,
       closable profile).
    Raises RuntimeError when it cannot unload without a channel kill and
    allow_channel_close is False; propagates close_chrome failures otherwise."""
    if _wait_profile_unlocked(user_data_dir, profile_folder, wait_s=8, log=log):
        log(f"  {profile_folder!r} already unloaded -> copy without channel close "
            f"(nothing killed)")
        return
    # 2. targeted graceful close of just this profile's windows
    n = _close_target_profile_windows(user_data_dir, profile_folder, log=log)
    if n == -1:
        # Ambiguous avatar name -> closing by name would nuke the sibling profile,
        # and channel-closing would kill the operator's whole Chrome. Refuse both;
        # fail the pull with a clear, actionable message.
        raise RuntimeError(
            f"profile {profile_folder!r} shares its Chrome avatar name with another "
            f"profile — cannot close only its window. Rename this profile to a "
            f"unique name in Chrome, then retry.")
    if n and _wait_profile_unlocked(user_data_dir, profile_folder, wait_s=12, log=log):
        log(f"  {profile_folder!r} unloaded after closing its {n} window(s) -> "
            f"copy without channel close (channel spared)")
        return
    # 3. last resort
    if not allow_channel_close:
        raise RuntimeError(
            f"profile {profile_folder!r} would not unload after closing its "
            f"window(s) — it is likely the PRIMARY/daily profile (background apps "
            f"keep it loaded). Refusing to channel-close (that would kill ALL "
            f"Chrome). Use a DEDICATED Chrome profile for this account and keep it "
            f"closed, so it can be copied without killing your browser.")
    log(f"  {profile_folder!r} still loaded -> closing channel (fallback)")
    if close_chrome is not None:
        close_chrome(user_data_dir)
    else:
        close_laptop_chrome(user_data_dir, log=log)


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

    # Unlock the target profile's cookie/login SQLite DBs. v824: if the profile
    # is already unloaded, copy live with no kill; else fall back to closing the
    # channel that owns it.
    try:
        _release_target_profile(user_data_dir, profile_folder, close_chrome, log=log)
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


# next-auth OAuth-HANDSHAKE cookie name fragments. These are single-use for ONE
# sign-in flow; a stale one carried into the golden makes Flow's next-auth
# callback replay a dead handshake -> "unusual activity". We KEEP
# __Secure-next-auth.session-token (the durable login) + Google SSO and drop
# only these. (The decided cookie set, from worker_cookie_extract analysis.)
_HANDSHAKE_SUBSTR = ("csrf-token", "callback-url", ".state", "pkce", "nonce")


# Google/Flow auth cookies whose presence in the golden proves the login copied.
_AUTH_COOKIE_NAMES = ("sid", "hsid", "ssid", "apisid", "sapisid", "lsid",
                      "__secure-1psid", "__secure-3psid", "__secure-1psidts",
                      "__secure-next-auth.session-token")


def _diag_cookie_readiness(cookies_db, log=print, tag=""):
    """Read-only sanity report on the freshly-built golden Cookies DB (NO
    decrypt): how many rows, how many are Google/Flow auth cookies, and the
    encryption scheme byte (v10/v11 = OS-keyring-encrypted; empty = plaintext).
    Lets us tell 'cookies never copied' from 'copied but undecryptable at launch'
    without leaking any secret value."""
    if not cookies_db or not os.path.isfile(cookies_db):
        log(f"{tag}cookie-diag: no Cookies DB at {cookies_db}")
        return
    try:
        import sqlite3
        con = sqlite3.connect(f"file:{cookies_db}?mode=ro&immutable=1", uri=True, timeout=5)
        try:
            rows = con.execute(
                "SELECT name, length(encrypted_value), "
                "hex(substr(encrypted_value,1,3)) FROM cookies").fetchall()
        finally:
            con.close()
        total = len(rows)
        auth = [(n, ln, hx) for (n, ln, hx) in rows
                if (n or "").lower() in _AUTH_COOKIE_NAMES]
        schemes = {}
        for (_n, _ln, hx) in auth:
            try:
                pfx = bytes.fromhex(hx or "").decode("ascii", "replace")[:3]
            except ValueError:
                pfx = "?"
            schemes[pfx] = schemes.get(pfx, 0) + 1
        names = sorted({(n or "").lower() for (n, _l, _h) in auth})
        log(f"{tag}cookie-diag: {total} cookies total, {len(auth)} auth "
            f"(schemes={schemes or '{}'}, names={names or '[]'})")
    except Exception as e:
        log(f"{tag}cookie-diag: read failed: {e}")


def _prune_handshake_cookies(cookies_db, log=print):
    """Delete stale next-auth handshake cookies from a copied golden Cookies DB
    (keeps the session token + Google SSO). Returns count deleted, -1 on error.

    Operates on the GOLDEN's copied Cookies DB only — never the real profile."""
    if not cookies_db or not os.path.isfile(cookies_db):
        return -1
    try:
        import sqlite3
        con = sqlite3.connect(cookies_db, timeout=5)
        try:
            rows = con.execute("SELECT rowid, name FROM cookies").fetchall()
            kill = [rid for (rid, name) in rows
                    if "next-auth" in (name or "").lower()
                    and any(s in (name or "").lower() for s in _HANDSHAKE_SUBSTR)]
            for rid in kill:
                con.execute("DELETE FROM cookies WHERE rowid=?", (rid,))
            con.commit()
            return len(kill)
        finally:
            con.close()
    except Exception as e:
        try:
            log(f"  prune handshake cookies failed: {e}")
        except Exception:
            pass
        return -1


def build_lean_golden_from_profile(email, golden_folder, label="",
                                   user_data_dir=None, close_chrome=None, log=print,
                                   allow_channel_close=True):
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
        # DIAG (cross-platform profile-pull): list what we searched so a Mac/Linux
        # miss is debuggable (empty list => platform paths unresolved).
        _searched = [
            f"{d} [{'ok' if (d and os.path.isdir(d) and os.path.isfile(os.path.join(d, 'Local State'))) else 'missing'}]"
            for d in candidates
        ]
        log(f"{tag}lean golden: {email!r} not logged into any Chrome channel "
            f"(platform={sys.platform}, searched={_searched or '[]'})")
        return False
    src_profile = os.path.join(user_data_dir, profile_folder)
    if not os.path.isdir(src_profile):
        log(f"{tag}lean golden: profile folder missing {src_profile}")
        return False
    log(f"{tag}lean golden: copying profile {profile_folder!r} from {user_data_dir}")

    # v824: unlock the target profile with least disruption — copy live if it is
    # already unloaded (no kill), else fall back to closing this channel's Chrome.
    try:
        _release_target_profile(user_data_dir, profile_folder, close_chrome, log=log,
                                allow_channel_close=allow_channel_close)
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
                        ignore=_lean_ignore, ignore_dangling_symlinks=True)
        # 2. Local State (DPAPI cookie key) -> golden root, single Default profile
        with open(os.path.join(user_data_dir, "Local State"), "r", encoding="utf-8") as f:
            ls = json.load(f)
        prof = ls.setdefault("profile", {})
        entry = (prof.get("info_cache", {}) or {}).get(profile_folder, {})
        prof["info_cache"] = {"Default": entry}
        prof["last_used"] = "Default"
        prof["last_active_profiles"] = ["Default"]
        # v903 — carry a REAL Chrome variations seed into the golden.
        #
        # flow_worker's own chrome_warmup docstring states the rule: "reCAPTCHA
        # Enterprise checks x-client-data — a short header = low trust score =
        # 403". Chrome only sends x-client-data when it has an applied
        # variations seed, which lives in Local State.variations_compressed_seed.
        #
        # Measured 2026-08-06: the golden had variations_seed_signature and
        # _date but NO variations_compressed_seed, because the profile is pulled
        # from Chrome BETA and that install has never synced one (stable's seed
        # is 61292 chars, Beta's is absent). The copy faithfully preserved the
        # absence. Result: worker Chrome sent NO x-client-data ("[Warmup] no
        # x-client-data header captured"), reCAPTCHA scored the session
        # untrusted, and EVERY generate returned 403 "reCAPTCHA evaluation
        # failed" / PUBLIC_ERROR_UNUSUAL_ACTIVITY — which the worker then
        # misread as an account block and golden-restored forever.
        #
        # The seed is per Chrome install, not per account, and the worker
        # launches on stable `chrome`, so stable's seed is the correct one.
        # Copy the whole variations_* set together: the signature signs the
        # seed, so a mismatched pair is worse than none.
        try:
            _seed = (ls.get("variations_compressed_seed") or "")
            if not _seed:
                _VAR_KEYS = ("variations_compressed_seed", "variations_seed_signature",
                             "variations_seed_date", "variations_last_fetch_time",
                             "variations_country", "variations_permanent_consistency_country")
                _donor = None
                for _cand in resolve_laptop_user_data_dirs():
                    if not _cand or _cand == user_data_dir:
                        continue
                    _cls = os.path.join(_cand, "Local State")
                    if not os.path.isfile(_cls):
                        continue
                    try:
                        with open(_cls, "r", encoding="utf-8", errors="ignore") as _f:
                            _d = json.load(_f)
                    except (OSError, ValueError):
                        continue
                    if _d.get("variations_compressed_seed"):
                        _donor = (_cand, _d)
                        break
                if _donor:
                    _dpath, _d = _donor
                    for _k in _VAR_KEYS:
                        ls.pop(_k, None)
                        if _k in _d:
                            ls[_k] = _d[_k]
                    log(f"{tag}lean golden: v903 borrowed variations seed "
                        f"({len(ls.get('variations_compressed_seed') or '')} chars) from {_dpath} "
                        f"— without it Chrome sends no x-client-data and reCAPTCHA 403s every generate")
                else:
                    log(f"{tag}lean golden: v903 WARNING no Chrome install has a variations seed; "
                        f"x-client-data will be empty and Flow may 403 every generate")
        except Exception as _ve:
            log(f"{tag}lean golden: v903 variations-seed copy skipped: {_ve}")
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
        # 4. Drop the stale next-auth HANDSHAKE cookies (csrf-token / callback-url
        #    / .state / pkce / nonce). They are single-use for ONE sign-in; a
        #    stale one carried into the golden makes Flow's next-auth callback
        #    replay an old handshake -> "unusual activity" 403. Keep the
        #    __Secure-next-auth.session-token + Google SSO so the golden stays
        #    logged in (reinstated from the reverted v819.4, isolated — cookie
        #    prune only, none of the v819 Restart-Manager copy rework).
        _pruned = _prune_handshake_cookies(
            os.path.join(tmp, "Default", "Network", "Cookies"), log=log)
        if _pruned > 0:
            log(f"{tag}lean golden: dropped {_pruned} stale next-auth handshake cookie(s) "
                f"(kept session-token + Google SSO)")
        # DIAG: prove the auth cookies actually landed in the golden (read-only,
        # no decrypt). If these are present + v10 but Flow still says logged-out,
        # the blocker is decryption at launch (mac keychain), not the copy.
        _diag_cookie_readiness(
            os.path.join(tmp, "Default", "Network", "Cookies"), log=log, tag=tag)
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
