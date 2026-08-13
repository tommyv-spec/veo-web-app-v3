"""Firefox-mode helpers in image_worker.

These cover the pure logic only — engine selection, profile isolation and the
cookie bridge. The browser launch itself is proven by a live render, not here.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from image_worker import (  # noqa: E402
    session_folder_for_mode,
    chromium_golden_folder,
    get_golden_folder,
    _bridge_golden_cookies_if_firefox,
    should_restore_golden,
)


def test_chrome_session_folder_is_unchanged():
    assert session_folder_for_mode("stealth", "/w/image-chrome-session") == "/w/image-chrome-session"


def test_unset_mode_stays_on_chrome():
    # Default-by-config, never default-by-code: anything that is not an explicit
    # firefox value must keep the existing Chrome profile.
    for mode in ("", None, "chrome", "typo"):
        assert session_folder_for_mode(mode, "/w/image-chrome-session") == "/w/image-chrome-session"


def test_firefox_gets_its_own_session_folder():
    got = session_folder_for_mode("firefox", "/w/image-chrome-session")
    assert got == os.path.join("/w", "image-firefox-session")
    assert session_folder_for_mode("camoufox", "/w/image-chrome-session") == got


def test_firefox_session_never_collides_with_chrome():
    chrome = session_folder_for_mode("stealth", "/w/image-chrome-session")
    firefox = session_folder_for_mode("firefox", "/w/image-chrome-session")
    assert chrome != firefox


def test_chromium_golden_is_the_same_for_both_engines():
    """The Firefox profile is SEEDED from the chromium golden, so the golden
    path must not move when the engine changes."""
    from_chrome = chromium_golden_folder("/w/image-chrome-session")
    from_firefox = chromium_golden_folder("/w/image-firefox-session")
    assert from_chrome == from_firefox
    assert from_chrome.endswith("image-chrome-golden")


def test_numbered_chrome_sessions_keep_their_numbered_golden():
    assert chromium_golden_folder("/w/image-chrome-session-2").endswith("image-chrome-golden-2")


def test_get_golden_folder_untouched_for_chrome():
    assert get_golden_folder("/w/image-chrome-session").endswith("image-chrome-golden")


def test_bridge_is_skipped_on_chrome():
    calls = []

    def reader(*a, **k):
        calls.append(a)
        return []

    assert _bridge_golden_cookies_if_firefox(
        mode="stealth", ctx=None, golden="/w/image-chrome-golden", reader=reader) is False
    assert calls == []


def test_bridge_injects_google_cookies_on_firefox():
    added = {}

    class Ctx:
        def add_cookies(self, cookies):
            added["cookies"] = cookies

    ok = _bridge_golden_cookies_if_firefox(
        mode="firefox", ctx=Ctx(), golden="/w/image-chrome-golden",
        reader=lambda d, domains, log=None: [{"name": "SID", "domain": ".google.com"}],
        log=lambda m: None)
    assert ok is True
    assert added["cookies"][0]["name"] == "SID"


def test_bridge_reports_failure_when_golden_has_no_cookies():
    ok = _bridge_golden_cookies_if_firefox(
        mode="firefox", ctx=object(), golden="/w/image-chrome-golden",
        reader=lambda d, domains, log=None: [], log=lambda m: None)
    assert ok is False


def test_bridge_falls_back_to_one_by_one_when_batch_rejected():
    """One malformed cookie rejects the whole batch; a single bad row must not
    cost the entire session."""
    accepted = []

    class PickyCtx:
        def __init__(self):
            self.first = True

        def add_cookies(self, cookies):
            if self.first and len(cookies) > 1:
                self.first = False
                raise ValueError("invalid cookie in batch")
            accepted.extend(cookies)

    ok = _bridge_golden_cookies_if_firefox(
        mode="firefox", ctx=PickyCtx(), golden="/w/image-chrome-golden",
        reader=lambda d, domains, log=None: [
            {"name": "SID", "domain": ".google.com"},
            {"name": "HSID", "domain": ".google.com"},
        ],
        log=lambda m: None)
    assert ok is True
    assert [c["name"] for c in accepted] == ["SID", "HSID"]


# ---------------------------------------------------------------------------
# The golden is a LOGIN-recovery artifact, not a startup ritual.
#
# Measured 2026-08-13, same minute, same account: a golden-restored profile
# lands on Flow's MARKETING shell with no New-project control and cannot mint
# the app (/project returns a 161-char empty shell), while the live session
# profile lands straight in the app. The lean golden copies durable files only,
# so the app entitlement that lives in site storage is not in it. Restoring it
# over a working session therefore DESTROYS the only profile that can reach
# Flow — which is exactly what stalled every run that day.
# ---------------------------------------------------------------------------

def _make_profile(tmp_path, name, with_cookies=True):
    prof = tmp_path / name
    (prof / "Default" / "Network").mkdir(parents=True)
    if with_cookies:
        (prof / "Default" / "Network" / "Cookies").write_bytes(b"SQLite format 3\x00")
    return prof


def test_healthy_session_is_never_clobbered_by_the_golden(tmp_path):
    session = _make_profile(tmp_path, "image-chrome-session")
    golden = _make_profile(tmp_path, "image-chrome-golden")
    assert should_restore_golden(str(session), str(golden)) is False


def test_missing_session_restores_from_golden(tmp_path):
    golden = _make_profile(tmp_path, "image-chrome-golden")
    assert should_restore_golden(str(tmp_path / "nope"), str(golden)) is True


def test_signed_out_session_restores_from_golden(tmp_path):
    """No cookie DB = no login. That is what the golden is for."""
    session = _make_profile(tmp_path, "image-chrome-session", with_cookies=False)
    golden = _make_profile(tmp_path, "image-chrome-golden")
    assert should_restore_golden(str(session), str(golden)) is True


def test_no_golden_means_nothing_to_restore(tmp_path):
    session = _make_profile(tmp_path, "image-chrome-session")
    assert should_restore_golden(str(session), str(tmp_path / "absent")) is False


def test_force_overrides_a_healthy_session(tmp_path):
    session = _make_profile(tmp_path, "image-chrome-session")
    golden = _make_profile(tmp_path, "image-chrome-golden")
    assert should_restore_golden(str(session), str(golden), force=True) is True
