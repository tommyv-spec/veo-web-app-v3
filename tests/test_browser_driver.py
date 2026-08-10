"""Driver selection for BROWSER_MODE. No browser is launched here."""
import os, sys

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static"))

import browser_driver as bd


def test_stealth_is_chromium():
    assert bd.is_firefox_mode("stealth") is False


def test_firefox_mode_detected():
    assert bd.is_firefox_mode("firefox") is True
    assert bd.is_firefox_mode("camoufox") is True
    assert bd.is_firefox_mode("FIREFOX") is True


def test_unset_mode_defaults_to_chromium():
    # Anything that is not an explicit firefox opt-in must stay on Chrome.
    # Getting this backwards would silently move every Chrome worker.
    assert bd.is_firefox_mode("") is False
    assert bd.is_firefox_mode(None) is False
    assert bd.is_firefox_mode("playwright") is False


def test_resolve_browser_mode_defaults_to_stealth():
    assert bd.resolve_browser_mode({}) == "stealth"
    assert bd.resolve_browser_mode({"BROWSER_MODE": "firefox"}) == "firefox"
    assert bd.resolve_browser_mode({"BROWSER_MODE": " FIREFOX "}) == "firefox"


def test_process_names_by_mode():
    assert "chrome.exe" in bd.browser_process_names("stealth")
    ff = bd.browser_process_names("firefox")
    assert "camoufox.exe" in ff and "firefox.exe" in ff
    assert "chrome.exe" not in ff


def test_camoufox_kwargs_pin_windows_os():
    out = bd.camoufox_launch_kwargs({"user_data_dir": "X"})
    assert out["os"] == "windows"


def test_camoufox_kwargs_drop_chrome_only_keys():
    out = bd.camoufox_launch_kwargs({
        "user_data_dir": "X",
        "channel": "chrome",
        "ignore_default_args": ["--enable-automation"],
        "args": ["--no-sandbox"],
    })
    for k in ("channel", "ignore_default_args", "args"):
        assert k not in out
    assert out["user_data_dir"] == "X"


def test_camoufox_kwargs_window_unpins_viewport():
    out = bd.camoufox_launch_kwargs(
        {"user_data_dir": "X", "viewport": {"width": 1280, "height": 500}},
        window="1280x752")
    assert out["window"] == (1280, 752)
    assert out["no_viewport"] is True
    assert "viewport" not in out


def test_camoufox_kwargs_bad_window_keeps_default():
    out = bd.camoufox_launch_kwargs({"user_data_dir": "X"}, window="not-a-size")
    assert "window" not in out


def test_firefox_defaults_to_headless():
    # A minimized Firefox window cannot be clicked at all (measured: visible
    # click 0.1s, same click minimized times out). Headless has no window to
    # minimize. flow_worker hardcodes headless=False, so the override must win.
    assert bd.firefox_headless_enabled({}) is True
    out = bd.camoufox_launch_kwargs({"user_data_dir": "X", "headless": False}, env={})
    assert out["headless"] is True


def test_firefox_headless_opt_out():
    for val in ("0", "false", "no", "off"):
        assert bd.firefox_headless_enabled({"FIREFOX_HEADLESS": val}) is False
    out = bd.camoufox_launch_kwargs({"user_data_dir": "X", "headless": False},
                                    env={"FIREFOX_HEADLESS": "0"})
    assert out["headless"] is False


def test_firefox_is_muted():
    # Chrome passes --mute-audio; Firefox has no such flag. A headless browser
    # still routes audio to the speakers, and Flow autoplays every clip.
    out = bd.camoufox_launch_kwargs({"user_data_dir": "X"}, env={})
    prefs = out["firefox_user_prefs"]
    assert prefs["media.volume_scale"] == "0.0"
    assert prefs["media.autoplay.default"] == 5


def test_firefox_mute_does_not_clobber_caller_prefs():
    out = bd.camoufox_launch_kwargs(
        {"user_data_dir": "X", "firefox_user_prefs": {"custom.pref": 1}}, env={})
    prefs = out["firefox_user_prefs"]
    assert prefs["custom.pref"] == 1
    assert prefs["media.volume_scale"] == "0.0"


def test_firefox_mode_never_targets_chrome_processes():
    # The golden restore kills by process name then rmtree's the profile with
    # ignore_errors=True. Wrong names => live Firefox keeps its lock => the
    # profile is half-deleted and nothing reports a failure.
    names = bd.browser_process_names("firefox")
    assert "chrome.exe" not in names
    assert any(n.startswith("camoufox") for n in names)
