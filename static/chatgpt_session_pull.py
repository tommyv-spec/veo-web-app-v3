"""Seed the ChatGPT image worker's session by COPYING the operator's real Chrome
profile (logged into the ChatGPT account) into a "golden" folder the worker
launches directly.

WHY copy-mode (same as the video + image workers): Chrome 127+ EXCLUSIVELY LOCKS
the Cookies DB of a loaded profile — it cannot be read live (verified: sqlite
read-only, immutable, raw copy, all fail with lock/permission errors while the
profile is open). So the profile must be UNLOADED to copy its cookies. Netlog
(the other option) works while loaded but closes the whole channel. Copy-mode via
worker_profile_pull.build_lean_golden_from_profile does the least-disruptive
thing: if the profile is already unloaded it copies live (no close); else it
closes ONLY that profile's window (targeted UIA close — never a whole-channel
kill; refuses if a SAME-NAMED sibling profile is open, to avoid nuking it).

The ChatGPT session-token is Chrome scheme v10 (DPAPI, per-Windows-user), so the
copied golden decrypts it on the same machine. Windows-focused; stdlib only.
"""


def pull_chatgpt_session(email, golden_folder, log=print):
    """Build a golden copy of the Chrome profile logged into `email` (the ChatGPT
    account) that the worker launches directly. Reuses the video/image worker's
    build_lean_golden_from_profile: targeted per-profile close (never a
    whole-channel taskkill), or a live copy when the profile is already unloaded.
    Returns the launch channel string on success, False on skip."""
    import worker_profile_pull
    # Close ONLY the target profile's window (targeted UIA), NEVER the whole
    # channel — allow_channel_close=False. If the profile is already unloaded it
    # copies live with no close at all; if it has a window it closes just that one
    # and copies once it unloads; if it will not unload (primary/background-pinned)
    # it FAILS cleanly instead of killing the channel. Use a profile that unloads
    # (non-primary, no background apps) — same as the video worker's accounts.
    return worker_profile_pull.build_lean_golden_from_profile(
        email, golden_folder=golden_folder, label="CHATGPT",
        close_chrome=lambda _u: worker_profile_pull.close_laptop_chrome(_u, log=log),
        log=log, allow_channel_close=False)
