"""Acquire a logged-in ChatGPT session for the worker by COPYING the operator's
Chrome profile — EXACTLY like the video worker, and ONLY from a non-stable Chrome
channel (Chrome Beta) so it NEVER touches the operator's daily Chrome.

Why this works (proven 2026-07-22 with kaveno.biz in Beta):
- The ChatGPT account lives in Chrome BETA (a separate channel/user-data-dir from
  the daily stable Chrome). Copying/closing Beta never disturbs daily browsing.
- Beta has App-Bound Encryption OFF (cookies are scheme v10), so the copied golden
  decrypts the session-token on the same machine — no netlog, no cookie export.
- `build_lean_golden_from_profile` closes ONLY the target profile's window (UIA
  avatar match; the collision guard refuses if a same-named sibling is open) and
  copies live when the profile is already unloaded. The worker launches the golden
  already logged in.

If the account is NOT in a non-stable channel, this returns False WITHOUT touching
stable Chrome (the worker then falls back to a one-time login in its own window).
Windows-focused; stdlib only.
"""

import worker_profile_pull as _wpp

# Chrome channels the worker MAY copy from — everything EXCEPT stable, so the
# operator's daily Chrome is never closed/copied.
_NON_STABLE = ("chrome beta", "chrome dev", "chrome sxs", "chrome canary", "chromium")


def _non_stable_profile(email):
    """(user_data_dir, profile_folder) for `email` in a NON-STABLE Chrome channel
    (Beta/Dev/Canary/Chromium), or (None, None). Never returns the stable channel."""
    for ud in _wpp.resolve_laptop_user_data_dirs():
        if not ud:
            continue
        low = ud.lower()
        if any(tag in low for tag in _NON_STABLE):
            folder = _wpp.find_profile_dir_for_email(ud, email)
            if folder:
                return ud, folder
    return None, None


def pull_chatgpt_session(email, golden_folder, log=print):
    """Build a golden copy of the Chrome-BETA profile logged into `email` that the
    worker launches directly. Returns the launch channel string on success, False
    on skip. NEVER touches the daily stable Chrome — only a non-stable channel."""
    ud, folder = _non_stable_profile(email)
    if not ud:
        log(f"chatgpt-pull: {email!r} is not logged in under a non-stable Chrome "
            f"channel. Log ChatGPT into CHROME BETA (a separate channel from your "
            f"daily Chrome) so the worker can copy it without touching your daily "
            f"browsing — then re-run.")
        return False
    log(f"chatgpt-pull: copying {email} from {folder!r} in {ud} (non-stable channel)")
    # allow_channel_close=True is safe here: close_chrome only ever closes the
    # NON-STABLE channel (Beta), never the operator's daily stable Chrome.
    return _wpp.build_lean_golden_from_profile(
        email, golden_folder=golden_folder, label="CHATGPT", user_data_dir=ud,
        close_chrome=lambda _u: _wpp.close_laptop_chrome(ud, log=log),
        log=log, allow_channel_close=True)
