"""Seed the ChatGPT image worker's session by COPYING the operator's real Chrome
profile (logged into the ChatGPT account) into a clean "golden" folder the worker
launches directly.

WHY copy-mode (same as the video + image workers): repeatedly driving the real
profile no-debug to scrape a net-log signed the account out AND some accounts
rejected the reconstituted cookie session. The copy path instead reads the real
profile ONCE (a single targeted UIA per-profile close via
worker_profile_pull._release_target_profile — NEVER a whole-channel taskkill),
copies the durable profile files into the golden folder, and the worker launches
that golden already logged in. No net-log, no whole-channel kill.

Requires App-Bound Encryption OFF on that profile (HKCU policy) — same as the
video worker's copy-mode. Windows-only. Stdlib only.

REUSES the video/image-worker machinery:
- worker_profile_pull.build_lean_golden_from_profile(email, golden_folder, ...)
- worker_profile_pull.close_laptop_chrome(user_data_dir, log)  (fallback closer)
"""


def pull_chatgpt_session(email, golden_folder, log=print):
    """Build a golden copy of the Chrome profile logged into `email` (the ChatGPT
    account) that the worker launches directly. Reuses the video/image worker's
    build_lean_golden_from_profile -> targeted UIA per-profile close (never a
    whole-channel taskkill). Returns the launch channel string on success, False
    on skip. Requires App-Bound Encryption OFF on that profile (same as the video
    worker's copy-mode)."""
    import worker_profile_pull
    return worker_profile_pull.build_lean_golden_from_profile(
        email, golden_folder=golden_folder, label="CHATGPT",
        close_chrome=lambda _u: worker_profile_pull.close_laptop_chrome(_u, log=log),
        log=log)
