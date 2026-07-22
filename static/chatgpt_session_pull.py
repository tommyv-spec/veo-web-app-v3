"""Auto-grab a logged-in ChatGPT session for a given account email, so the worker
comes up logged in WITHOUT a manual login. This is the netlog method that worked
in the first successful run.

HOW (netlog — ABE-immune, works v10 AND v20 cookies): launch the operator's real
Chrome profile for `email` no-debug with --log-net-log + IncludeSensitive so Chrome
writes its own DECRYPTED Cookie headers to a log; parse the chatgpt.com auth
cookies; write them as plaintext Playwright cookies the worker injects. No file
copy (dodges App-Bound Encryption), no admin/registry.

Cost: netlog must relaunch the profile, so it closes the Chrome CHANNEL that owns
it (worker_cookie_extract._close_channel_chrome) — closes only that channel,
sparing other Chrome channels. Falls back to a manual in-worker login when this
can't get the session (see chatgpt_image_worker.ensure_logged_in).

Windows-focused; stdlib only.
"""
import os
import re as _re
import subprocess
import time

import worker_profile_pull
import worker_cookie_extract

_HANDSHAKE = ("csrf-token", "callback-url", ".state", "pkce", "nonce")


def _cookie_dict(name, value, host):
    is_openai = "openai.com" in host.lower() and "chatgpt.com" not in host.lower()
    if name.startswith("__Host-"):
        base = "https://openai.com" if is_openai else "https://chatgpt.com"
        return {"name": name, "value": value, "url": base, "secure": True, "sameSite": "Lax"}
    domain = ".openai.com" if is_openai else ".chatgpt.com"
    return {"name": name, "value": value, "domain": domain, "path": "/",
            "secure": True, "sameSite": "None"}


def _parse_chatgpt_netlog(path, log=print):
    try:
        raw = open(path, "r", encoding="utf-8", errors="ignore").read()
    except OSError as e:
        log(f"chatgpt-netlog: cannot read netlog: {e}")
        return []
    seen, out = set(), []
    for m in _re.finditer(r'"cookie:\s*([^"]+)"', raw, _re.IGNORECASE):
        window = raw[max(0, m.start() - 2000):m.start()]
        host = ""
        for am in _re.finditer(r'":authority:\s*([^"]+)"|"host:\s*([^"]+)"', window, _re.IGNORECASE):
            host = (am.group(1) or am.group(2) or "").strip()
        if "chatgpt.com" not in host.lower() and "openai.com" not in host.lower():
            continue
        for pair in m.group(1).split(";"):
            pair = pair.strip()
            if "=" not in pair:
                continue
            n, _, v = pair.partition("=")
            n = n.strip()
            nl = n.lower()
            if not n or n in seen or any(s in nl for s in _HANDSHAKE):
                continue
            seen.add(n)
            out.append(_cookie_dict(n, v.strip(), host))
    log(f"chatgpt-netlog: parsed {len(out)} cookies")
    return out


def pull_chatgpt_cookies_netlog(email, out_json_path, log=print):
    """Capture the ChatGPT session for `email` via netlog, write Playwright cookies
    to out_json_path. Returns True iff a session-token was captured. Closes only the
    channel that owns the profile. Never raises for expected misses."""
    import json
    located = worker_profile_pull.locate_profile(email)
    if not located:
        log(f"chatgpt-netlog: {email!r} not logged into any Chrome channel.")
        return False
    user_data_dir, profile_folder, channel = located
    exe = worker_cookie_extract._chrome_exe(channel)
    if not exe:
        log(f"chatgpt-netlog: chrome exe for channel {channel!r} not found.")
        return False
    log(f"chatgpt-netlog: capturing {email} from {profile_folder!r} (channel={channel})")
    worker_cookie_extract._close_channel_chrome(user_data_dir, log)
    nl = os.path.join(os.environ.get("TEMP", "."), "chatgpt_netlog.json")
    try:
        if os.path.exists(nl):
            os.remove(nl)
    except OSError:
        pass
    try:
        subprocess.Popen([
            exe, f"--user-data-dir={user_data_dir}", f"--profile-directory={profile_folder}",
            f"--log-net-log={nl}", "--net-log-capture-mode=IncludeSensitive",
            "--no-first-run", "--no-default-browser-check", "--start-minimized",
            "--disable-search-engine-choice-screen",
            "https://chatgpt.com/backend-api/me", "https://chatgpt.com/",
        ])
    except Exception as e:
        log(f"chatgpt-netlog: launch failed: {e}")
        return False
    time.sleep(15)
    worker_cookie_extract._close_channel_chrome(user_data_dir, log)
    cookies = _parse_chatgpt_netlog(nl, log) if os.path.isfile(nl) else []
    try:
        os.remove(nl)
    except OSError:
        pass
    try:
        with open(out_json_path, "w", encoding="utf-8") as f:
            json.dump(cookies, f)
    except OSError as e:
        log(f"chatgpt-netlog: cannot write {out_json_path}: {e}")
        return False
    has_tok = any("session-token" in c.get("name", "").lower() for c in cookies)
    log(f"chatgpt-netlog: {'captured session-token' if has_tok else 'NO session-token'} "
        f"({len(cookies)} cookies) -> {out_json_path}")
    return has_tok
