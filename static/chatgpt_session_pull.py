"""Seed the ChatGPT worker's profile with the operator's logged-in ChatGPT
session by copying the auth files LIVE — Chrome stays open, NO window is closed.

WHY live copy (not the close+copytree golden, not netlog):
- The ChatGPT auth cookies (`__Secure-next-auth.session-token`) are Chrome scheme
  v10 = DPAPI-encrypted with the Windows user's key (NOT app-bound v20). So a copy
  on the SAME Windows user decrypts fine — no netlog needed.
- The Cookies DB is locked while Chrome runs, but SQLite's online-backup API reads
  a consistent snapshot of a live, in-use database. So we never close any window
  and never hit the "two profiles share an avatar name" close-ambiguity.
- Local State carries the os_crypt key (DPAPI-wrapped) that decrypts the v10
  cookies; copied verbatim so the golden decrypts them.

Resolves the target profile from an operator-given email via the Flow worker's
`worker_profile_pull.locate_profile`. Windows-focused; stdlib only.
"""
import os
import shutil
import sqlite3

import worker_profile_pull


def _sqlite_backup(src_db, dst_db, log=print):
    """Consistent snapshot of a live (locked) SQLite DB via the online-backup API.
    Returns True on success."""
    try:
        os.makedirs(os.path.dirname(dst_db), exist_ok=True)
        # read-only source; backup() reads committed rows even while Chrome writes.
        scon = sqlite3.connect(f"file:{src_db}?mode=ro", uri=True, timeout=10)
        try:
            dcon = sqlite3.connect(dst_db, timeout=10)
            try:
                scon.backup(dcon)
            finally:
                dcon.close()
        finally:
            scon.close()
        return True
    except Exception as e:
        log(f"chatgpt-pull: sqlite backup failed for {os.path.basename(src_db)}: {e}")
        return False


def _has_session_token(cookies_db, log=print):
    """Read-only check that the copied Cookies DB carries the ChatGPT session token."""
    try:
        con = sqlite3.connect(f"file:{cookies_db}?mode=ro&immutable=1", uri=True, timeout=5)
        try:
            n = con.execute(
                "SELECT COUNT(*) FROM cookies WHERE host_key LIKE '%chatgpt.com%' "
                "AND name LIKE '%session-token%'").fetchone()[0]
        finally:
            con.close()
        return n > 0
    except Exception as e:
        log(f"chatgpt-pull: session-token check failed: {e}")
        return False


def pull_chatgpt_session(email, golden_folder, log=print):
    """Copy the ChatGPT session from the Chrome profile logged into `email` into
    `golden_folder` (a user-data-dir the worker launches, using its Default
    profile). LIVE copy — Chrome stays open, nothing is closed. Returns the launch
    channel string ('chrome') on success, False on skip/failure.

    Copies: Default/Network/Cookies (SQLite online-backup), Local State (DPAPI
    key), Default/Preferences (best-effort). The v10 session-token decrypts on the
    same Windows user."""
    located = worker_profile_pull.locate_profile(email)
    if not located:
        log(f"chatgpt-pull: account {email!r} is NOT logged into any Chrome channel. "
            f"Log into chatgpt.com in Chrome first.")
        return False
    user_data_dir, profile_folder, channel = located
    src_profile = os.path.join(user_data_dir, profile_folder)
    log(f"chatgpt-pull: LIVE-copying session from {profile_folder!r} "
        f"({user_data_dir}) — no window closed")

    dst_default = os.path.join(golden_folder, "Default")
    os.makedirs(os.path.join(dst_default, "Network"), exist_ok=True)

    # 1. Cookies — the auth cookies (v10 session-token). Online-backup the live DB.
    src_cookies = os.path.join(src_profile, "Network", "Cookies")
    dst_cookies = os.path.join(dst_default, "Network", "Cookies")
    if not _sqlite_backup(src_cookies, dst_cookies, log=log):
        return False

    # 2. Local State — carries the DPAPI-wrapped os_crypt key that decrypts v10.
    try:
        shutil.copy2(os.path.join(user_data_dir, "Local State"),
                     os.path.join(golden_folder, "Local State"))
    except OSError as e:
        log(f"chatgpt-pull: cannot copy Local State: {e}")
        return False

    # 3. Preferences (best-effort — clean exit, no tab restore). Non-fatal.
    for fname in ("Preferences", "Secure Preferences"):
        try:
            shutil.copy2(os.path.join(src_profile, fname),
                         os.path.join(dst_default, fname))
        except OSError:
            pass

    if not _has_session_token(dst_cookies, log=log):
        log(f"chatgpt-pull: NO ChatGPT session-token in the copied cookies — "
            f"is {email!r} logged into chatgpt.com in that profile?")
        return False

    log(f"chatgpt-pull: session copied for {email} (channel={channel}); "
        f"golden ready at {golden_folder}")
    return channel or "chrome"
