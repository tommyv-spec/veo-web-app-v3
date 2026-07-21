"""Pull a logged-in ChatGPT session from the operator's real Chrome profile and
write it as plaintext Playwright cookies for the ChatGPT image worker to inject.

WHY netlog (same as the Flow worker): Chrome 127+ app-bound encryption means the
on-disk cookies cannot be copied to another folder, and Chrome 136+ blocks
debugging on the real profile. BUT a normally-launched (no-debug) Chrome IS
logged in and, with --log-net-log + --net-log-capture-mode=IncludeSensitive,
writes its own live request traffic — including the decrypted Cookie headers —
to a file. We launch the real profile no-debug, open chatgpt.com, capture that
file, parse the auth cookies, and write them for the worker's own session.

NON-DISRUPTIVE: closes ONLY the Chrome channel that owns the target profile
(reusing worker_cookie_extract._close_channel_chrome) — the operator's daily
Chrome keeps running. NEVER a blanket `taskkill /IM chrome.exe`.

REUSES the Flow-worker machinery:
- worker_profile_pull.locate_profile(email) -> (user_data_dir, folder, channel)
- worker_cookie_extract._chrome_exe(channel)
- worker_cookie_extract._close_channel_chrome(user_data_dir, log)

Windows-only. Stdlib only.
"""
import json
import os
import re as _re
import subprocess
import time

import worker_profile_pull
import worker_cookie_extract


# next-auth OAuth-HANDSHAKE cookie name fragments. Single-use for ONE sign-in
# flow; a stale one collides with a fresh handshake. Keep the SESSION token,
# drop the handshake cookies.
_HANDSHAKE_SUBSTR = ("csrf-token", "callback-url", ".state", "pkce", "nonce")


def _chatgpt_host(host):
    """True if `host` belongs to ChatGPT / OpenAI (the hosts we keep)."""
    h = (host or "").lower()
    return "chatgpt.com" in h or "openai.com" in h


def chatgpt_cookie_from_pair(name, value, host):
    """Build ONE Playwright add_cookies() dict for a ChatGPT/OpenAI cookie.

    __Host- cookies MUST be host-only -> url form ONLY (Playwright rejects url +
    domain/path together). Everyone else gets a leading-dot registrable domain
    (.chatgpt.com or .openai.com per host)."""
    h = (host or "").lower()
    is_openai = "openai.com" in h and "chatgpt.com" not in h
    if name.startswith("__Host-"):
        base = "https://openai.com" if is_openai else "https://chatgpt.com"
        return {"name": name, "value": value, "url": base,
                "secure": True, "sameSite": "Lax"}
    domain = ".openai.com" if is_openai else ".chatgpt.com"
    return {"name": name, "value": value, "domain": domain, "path": "/",
            "secure": True, "sameSite": "None"}


def filter_chatgpt_cookies(rows):
    """rows = iterable of (name, value, host). Returns deduped Playwright cookie
    dicts, keeping ONLY chatgpt/openai hosts, dropping stale next-auth handshake
    cookies (csrf-token/callback-url/.state/pkce/nonce) but KEEPING the
    session-token + oai-* auth cookies. First value per name wins."""
    seen = {}
    for name, value, host in rows:
        if not name or name in seen:
            continue
        if not _chatgpt_host(host):
            continue
        nl = name.lower()
        if any(s in nl for s in _HANDSHAKE_SUBSTR):
            continue
        seen[name] = chatgpt_cookie_from_pair(name, value, host)
    return list(seen.values())


def has_session_token(cookies):
    """True if any cookie dict carries the durable next-auth session token."""
    for c in cookies:
        if "session-token" in (c.get("name", "") or "").lower():
            return True
    return False


def _parse_chatgpt_netlog(path, log=print):
    """Parse a Chrome netlog for Cookie request headers sent to chatgpt.com /
    openai.com hosts. Returns Playwright add_cookies() dicts. Adapted from
    worker_cookie_extract._parse_netlog_cookies (regex scan, robust to size)."""
    try:
        raw = open(path, "r", encoding="utf-8", errors="ignore").read()
    except OSError as e:
        log(f"chatgpt-capture: cannot read netlog: {e}")
        return []
    rows = []  # (name, value, host)
    seen = set()
    for m in _re.finditer(r'"cookie:\s*([^"]+)"', raw, _re.IGNORECASE):
        cookie_str = m.group(1)
        window = raw[max(0, m.start() - 2000):m.start()]
        am = None
        for am in _re.finditer(r'":authority:\s*([^"]+)"|"host:\s*([^"]+)"', window, _re.IGNORECASE):
            pass  # take the LAST (closest) authority/host before the cookie
        host = ""
        if am:
            host = (am.group(1) or am.group(2) or "").strip()
        if not _chatgpt_host(host):
            continue
        for pair in cookie_str.split(";"):
            pair = pair.strip()
            if "=" not in pair:
                continue
            name, _, value = pair.partition("=")
            name = name.strip()
            if name and name not in seen:
                seen.add(name)
                rows.append((name, value.strip(), host))
    cookies = filter_chatgpt_cookies(rows)
    log(f"chatgpt-capture: parsed {len(cookies)} distinct cookies from netlog")
    return cookies


def _capture_chatgpt_cookies(user_data_dir, profile_dir, channel, log=print):
    """Launch the real profile no-debug with net-logging, open ChatGPT so it
    sends authenticated requests, then parse the live auth cookies. Mirrors
    worker_cookie_extract.extract_cookies EXACTLY but for chatgpt.com. Closes the
    channel's Chrome first (and after) via the non-disruptive channel close. []
    on failure."""
    exe = worker_cookie_extract._chrome_exe(channel)
    if not exe:
        log(f"chatgpt-capture: chrome exe for channel {channel!r} not found")
        return []
    worker_cookie_extract._close_channel_chrome(user_data_dir, log)
    nl = os.path.join(os.environ.get("TEMP", os.environ.get("TMP", ".")), "chatgpt_netlog.json")
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
            # backend-api/me forces an authenticated request that carries the
            # session-token cookie; the root loads the app session cookies too.
            "https://chatgpt.com/backend-api/me",
            "https://chatgpt.com/",
        ])
    except Exception as e:
        log(f"chatgpt-capture: launch failed: {e}")
        return []
    # Let it load + send authenticated requests, then close so the netlog flushes.
    time.sleep(14)
    worker_cookie_extract._close_channel_chrome(user_data_dir, log)
    if not os.path.isfile(nl):
        log("chatgpt-capture: netlog not written")
        return []
    cookies = _parse_chatgpt_netlog(nl, log)
    try:
        os.remove(nl)
    except OSError:
        pass
    return cookies


def pull_chatgpt_cookies(email, out_json_path, log=print):
    """Pull the ChatGPT session from the Chrome profile logged into `email` and
    write Playwright cookies to `out_json_path`. Returns True if a session-token
    cookie was captured, else False. Non-disruptive: closes ONLY the owning
    Chrome channel. Never raises for the expected miss cases."""
    located = worker_profile_pull.locate_profile(email)
    if not located:
        log(f"chatgpt-pull: account {email!r} is NOT logged into any Chrome channel "
            f"(stable/Beta/Dev/Canary). Log into chatgpt.com in Chrome first.")
        return False
    user_data_dir, profile_folder, channel = located
    log(f"chatgpt-pull: using profile {profile_folder!r} from {user_data_dir} "
        f"(channel={channel})")

    cookies = _capture_chatgpt_cookies(user_data_dir, profile_folder, channel, log=log)
    try:
        with open(out_json_path, "w", encoding="utf-8") as f:
            json.dump(cookies, f)
    except OSError as e:
        log(f"chatgpt-pull: cannot write {out_json_path}: {e}")
        return False

    if has_session_token(cookies):
        log(f"chatgpt-pull: captured {len(cookies)} cookies incl. session-token -> {out_json_path}")
        return True
    log(f"chatgpt-pull: NO session-token captured ({len(cookies)} cookies written). "
        f"Existing cookies may still be valid.")
    return False
