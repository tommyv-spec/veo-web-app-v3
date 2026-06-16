"""Capture a logged-in Google session from a real Chrome profile and hand it to
the worker's OWN fresh, automatable profile.

WHY netlog (not copy / not decrypt): Chrome 127+ app-bound encryption means the
on-disk cookies cannot be copied to another folder (they decrypt to nothing),
and the elevation service that would decrypt them is not registered on a no-admin
install. Chrome 136+ also blocks debugging on the real profile, so the worker
cannot drive it directly. BUT a normally-launched (no-debug) Chrome IS logged in
and, with --log-net-log + --net-log-capture-mode=IncludeSensitive, it writes its
own live request traffic — including the decrypted Cookie headers — to a file.
We launch the real profile no-debug, capture that file, parse the auth cookies,
and inject them into the worker's own debuggable session. No copy, no decrypt,
no debug on the real profile.

Windows-only. Stdlib only.
"""
import json
import os
import re as _re
import subprocess
import time


_CHROME_EXE_SUB = {
    "chrome": r"Google\Chrome\Application\chrome.exe",
    "chrome-beta": r"Google\Chrome Beta\Application\chrome.exe",
    "chrome-dev": r"Google\Chrome Dev\Application\chrome.exe",
    "chrome-canary": r"Google\Chrome SxS\Application\chrome.exe",
    "chromium": r"Chromium\Application\chrome.exe",
}


def _chrome_exe(channel):
    sub = _CHROME_EXE_SUB.get(channel, _CHROME_EXE_SUB["chrome"])
    bases = [os.environ.get("ProgramFiles", r"C:\Program Files"),
             os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)"),
             os.environ.get("LOCALAPPDATA", "")]
    for base in bases:
        if not base:
            continue
        p = os.path.join(base, sub)
        if os.path.isfile(p):
            return p
    return None


def _close_channel_chrome(user_data_dir, log):
    """Close ONLY the Chrome channel that owns user_data_dir (e.g. Chrome Beta),
    sparing the operator's other Chrome. Matches the channel app folder."""
    folder = os.path.basename(os.path.dirname(os.path.normpath(user_data_dir)))
    needle = (os.sep + folder + os.sep).lower()
    try:
        ps = ("Get-CimInstance Win32_Process -Filter \"name='chrome.exe'\" | "
              "ForEach-Object { \"$($_.ProcessId)`t$($_.CommandLine)\" }")
        out = subprocess.run(["powershell", "-NoProfile", "-NonInteractive", "-Command", ps],
                             capture_output=True, text=True, timeout=30).stdout
        killed = 0
        for row in out.splitlines():
            row = row.strip()
            if not row:
                continue
            pid, _, cmd = row.partition("\t")
            pid = pid.strip()
            if pid and needle in (cmd or "").lower():
                subprocess.run(["taskkill", "/F", "/PID", pid], capture_output=True)
                killed += 1
        log(f"cookie-capture: closed {killed} '{folder}' process(es); other Chrome left running")
    except Exception as e:
        log(f"cookie-capture: close failed ({e})")
    time.sleep(2.0)


def _registrable_domain(host):
    """'.google.com' for *.google.com, '.labs.google' for *.labs.google, else
    a leading-dot last-two-labels guess."""
    h = host.lower().strip(".")
    if h.endswith("google.com") or h == "google.com":
        return ".google.com"
    if h.endswith("labs.google") or h == "labs.google":
        return ".labs.google"
    parts = h.split(".")
    return "." + ".".join(parts[-2:]) if len(parts) >= 2 else h


def _parse_netlog_cookies(path, log):
    """Parse a Chrome netlog for Cookie request headers sent to *google* hosts.
    Returns Playwright add_cookies() dicts. Robust to large files (regex scan)."""
    try:
        raw = open(path, "r", encoding="utf-8", errors="ignore").read()
    except OSError as e:
        log(f"cookie-capture: cannot read netlog: {e}")
        return []
    # netlog request-header events carry a "headers" array of "name: value"
    # strings. Pull each Cookie header plus the :authority/host in the same
    # array. Work on the raw text: find every "cookie: ..." and the nearest
    # preceding authority/host marker.
    by_name = {}  # name -> (value, host)
    # Each headers array is a JSON list of quoted strings; scan for cookie lines
    # together with an authority within a small window before them.
    for m in _re.finditer(r'"cookie:\s*([^"]+)"', raw, _re.IGNORECASE):
        cookie_str = m.group(1)
        window = raw[max(0, m.start() - 2000):m.start()]
        am = None
        for am in _re.finditer(r'":authority:\s*([^"]+)"|"host:\s*([^"]+)"', window, _re.IGNORECASE):
            pass  # take the LAST (closest) authority/host before the cookie
        host = ""
        if am:
            host = (am.group(1) or am.group(2) or "").strip()
        if "google" not in host.lower():
            # still record under a generic google domain if host unknown
            if host and "google" not in host.lower():
                continue
        for pair in cookie_str.split(";"):
            pair = pair.strip()
            if "=" not in pair:
                continue
            name, _, value = pair.partition("=")
            name = name.strip()
            if name and name not in by_name:
                by_name[name] = (value.strip(), host or "google.com")
    cookies = []
    for name, (value, host) in by_name.items():
        if name.startswith("__Host-"):
            # __Host- cookies MUST be host-only -> url form ONLY (Playwright
            # rejects url + path together: "should have either url or path").
            cookies.append({"name": name, "value": value,
                            "url": "https://" + (host or "accounts.google.com"),
                            "secure": True, "sameSite": "Lax"})
        else:
            cookies.append({"name": name, "value": value,
                            "domain": _registrable_domain(host), "path": "/",
                            "secure": True, "sameSite": "None"})
    log(f"cookie-capture: parsed {len(cookies)} distinct cookies from netlog")
    return cookies


def extract_cookies(user_data_dir, profile_dir, channel, log=print):
    """Launch the real profile no-debug with net-logging, let it authenticate to
    Flow, then parse the live auth cookies. Returns Playwright add_cookies dicts.
    Closes the channel's Chrome first (and after). [] on failure."""
    exe = _chrome_exe(channel)
    if not exe:
        log(f"cookie-capture: chrome exe for channel {channel!r} not found")
        return []
    _close_channel_chrome(user_data_dir, log)
    nl = os.path.join(os.environ.get("TEMP", os.environ.get("TMP", ".")), "wrk_netlog.json")
    try:
        if os.path.exists(nl):
            os.remove(nl)
    except OSError:
        pass
    try:
        proc = subprocess.Popen([
            exe, f"--user-data-dir={user_data_dir}", f"--profile-directory={profile_dir}",
            f"--log-net-log={nl}", "--net-log-capture-mode=IncludeSensitive",
            "--no-first-run", "--no-default-browser-check", "--start-minimized",
            "--disable-search-engine-choice-screen", "--ash-no-nudges",
            "https://labs.google/fx/tools/flow",
        ])
    except Exception as e:
        log(f"cookie-capture: launch failed: {e}")
        return []
    # Let it load + send authenticated requests, then close so the netlog flushes.
    time.sleep(14)
    _close_channel_chrome(user_data_dir, log)
    if not os.path.isfile(nl):
        log("cookie-capture: netlog not written")
        return []
    cookies = _parse_netlog_cookies(nl, log)
    try:
        os.remove(nl)
    except OSError:
        pass
    return cookies
