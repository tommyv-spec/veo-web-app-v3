"""Unit tests for worker_profile_pull (laptop-profile pull into worker golden).

Run: cd code && python -m pytest test_worker_profile_pull.py -v
No real Chrome needed (Chrome close is injected).
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "static"))

import worker_profile_pull as wpp


def _write_local_state(user_data_dir, info_cache, os_crypt=None):
    os.makedirs(user_data_dir, exist_ok=True)
    state = {"profile": {"info_cache": info_cache}}
    if os_crypt is not None:
        state["os_crypt"] = os_crypt
    with open(os.path.join(user_data_dir, "Local State"), "w", encoding="utf-8") as f:
        json.dump(state, f)


# --- Task 1: lookup ---------------------------------------------------------

def test_find_profile_dir_for_email_matches_case_insensitive(tmp_path):
    ud = str(tmp_path / "User Data")
    _write_local_state(ud, {
        "Default": {"user_name": "someone@gmail.com"},
        "Profile 3": {"user_name": "Me@Gmail.com"},
    })
    assert wpp.find_profile_dir_for_email(ud, "me@gmail.com") == "Profile 3"


def test_find_profile_dir_for_email_no_match_returns_none(tmp_path):
    ud = str(tmp_path / "User Data")
    _write_local_state(ud, {"Default": {"user_name": "a@gmail.com"}})
    assert wpp.find_profile_dir_for_email(ud, "nobody@gmail.com") is None


def test_list_profile_emails(tmp_path):
    ud = str(tmp_path / "User Data")
    _write_local_state(ud, {
        "Default": {"user_name": "a@gmail.com"},
        "Profile 1": {"user_name": "b@gmail.com"},
        "Profile 2": {},
    })
    assert sorted(wpp.list_profile_emails(ud)) == ["a@gmail.com", "b@gmail.com"]


# --- Task 2: config ---------------------------------------------------------

def test_load_laptop_email_from_file(tmp_path):
    p = tmp_path / "worker_settings.json"
    p.write_text(json.dumps({"laptop_email": "  me@gmail.com  "}), encoding="utf-8")
    assert wpp.load_laptop_email(str(p), env={}) == "me@gmail.com"


def test_load_laptop_email_env_overrides_file(tmp_path):
    p = tmp_path / "worker_settings.json"
    p.write_text(json.dumps({"laptop_email": "file@gmail.com"}), encoding="utf-8")
    assert wpp.load_laptop_email(str(p), env={"ACCOUNT1_LAPTOP_EMAIL": "env@gmail.com"}) == "env@gmail.com"


def test_load_laptop_email_missing_file_returns_empty(tmp_path):
    assert wpp.load_laptop_email(str(tmp_path / "nope.json"), env={}) == ""


def test_resolve_user_data_dir_override(tmp_path):
    assert wpp.resolve_laptop_user_data_dir(env={"LAPTOP_CHROME_USER_DATA": "X:/ud"}) == "X:/ud"


def test_resolve_user_data_dir_from_localappdata():
    got = wpp.resolve_laptop_user_data_dir(env={"LOCALAPPDATA": r"C:\Users\me\AppData\Local"})
    assert got == os.path.join(r"C:\Users\me\AppData\Local", "Google", "Chrome", "User Data")


def test_resolve_user_data_dir_none_when_no_env():
    assert wpp.resolve_laptop_user_data_dir(env={}) is None


# --- Task 3: the pull -------------------------------------------------------

def _fake_laptop(tmp_path, email="me@gmail.com", folder="Profile 3"):
    ud = tmp_path / "User Data"
    (ud / folder / "Network").mkdir(parents=True)
    (ud / folder / "Network" / "Cookies").write_bytes(b"COOKIEDB")
    (ud / folder / "Preferences").write_text("{}", encoding="utf-8")
    with open(ud / "Local State", "w", encoding="utf-8") as f:
        json.dump({
            "profile": {"info_cache": {folder: {"user_name": email}}},
            "os_crypt": {"encrypted_key": "LAPTOPKEY"},
        }, f)
    return str(ud)


def test_pull_builds_golden_with_default_and_local_state(tmp_path):
    ud = _fake_laptop(tmp_path)
    golden = str(tmp_path / "chrome-golden")
    calls = []
    ok = wpp.pull_profile_from_laptop(
        "me@gmail.com", golden, label="Account1",
        user_data_dir=ud, close_chrome=lambda: calls.append("closed"),
        log=lambda m: None,
    )
    assert ok is True
    assert calls == ["closed"]
    assert os.path.isfile(os.path.join(golden, "Default", "Network", "Cookies"))
    with open(os.path.join(golden, "Local State"), encoding="utf-8") as f:
        assert json.load(f)["os_crypt"]["encrypted_key"] == "LAPTOPKEY"
    assert not os.path.exists(golden + ".pull_tmp")
    assert not os.path.exists(golden + ".old")


def test_pull_empty_email_is_noop(tmp_path):
    golden = str(tmp_path / "chrome-golden")
    assert wpp.pull_profile_from_laptop("", golden, user_data_dir="x", log=lambda m: None) is False
    assert not os.path.exists(golden)


def test_pull_email_not_found_fails_keeps_golden(tmp_path):
    # Email given but NOT logged into this Chrome -> fail clearly, never pull a
    # different account. Existing golden untouched.
    ud = _fake_laptop(tmp_path, email="other@gmail.com", folder="Default")
    golden = str(tmp_path / "chrome-golden")
    os.makedirs(os.path.join(golden, "Default"))
    open(os.path.join(golden, "Default", "marker"), "w").close()
    ok = wpp.pull_profile_from_laptop("me@gmail.com", golden, user_data_dir=ud,
                                      close_chrome=lambda: None, log=lambda m: None)
    assert ok is False
    assert os.path.isfile(os.path.join(golden, "Default", "marker"))


def test_pull_email_not_found_no_signin_keeps_golden(tmp_path):
    # Email not present AND nothing signed in -> keep the existing golden.
    ud = str(tmp_path / "User Data")
    _write_local_state(ud, {"Default": {}})
    golden = str(tmp_path / "chrome-golden")
    os.makedirs(os.path.join(golden, "Default"))
    open(os.path.join(golden, "Default", "marker"), "w").close()
    ok = wpp.pull_profile_from_laptop("me@gmail.com", golden, user_data_dir=ud, log=lambda m: None)
    assert ok is False
    assert os.path.isfile(os.path.join(golden, "Default", "marker"))


def test_pull_missing_user_data_dir_returns_false(tmp_path):
    golden = str(tmp_path / "chrome-golden")
    ok = wpp.pull_profile_from_laptop("me@gmail.com", golden,
                                      user_data_dir=str(tmp_path / "nope"), log=lambda m: None)
    assert ok is False


def test_find_logged_in_profile_prefers_default(tmp_path):
    ud = str(tmp_path / "User Data")
    _write_local_state(ud, {
        "Default": {"user_name": "a@gmail.com"},
        "Profile 1": {"user_name": "b@gmail.com"},
    })
    assert wpp.find_logged_in_profile(ud) == "Default"


def test_find_logged_in_profile_first_when_no_default(tmp_path):
    ud = str(tmp_path / "User Data")
    _write_local_state(ud, {"Default": {}, "Profile 2": {"user_name": "b@gmail.com"}})
    assert wpp.find_logged_in_profile(ud) == "Profile 2"


def test_find_logged_in_profile_none_when_no_signin_and_no_folders(tmp_path):
    ud = str(tmp_path / "User Data")
    _write_local_state(ud, {"Default": {}, "Profile 1": {}})
    assert wpp.find_logged_in_profile(ud) is None


def test_find_logged_in_profile_disk_fallback_when_sync_off(tmp_path):
    # Sync off (no user_name) but the Default folder exists on disk -> use it.
    ud = tmp_path / "User Data"
    (ud / "Default").mkdir(parents=True)
    _write_local_state(str(ud), {})
    assert wpp.find_logged_in_profile(str(ud)) == "Default"


def test_pull_auto_detect_no_email(tmp_path):
    # No email -> auto-detect the signed-in Default profile and pull it.
    ud = _fake_laptop(tmp_path, email="me@gmail.com", folder="Default")
    golden = str(tmp_path / "chrome-golden")
    ok = wpp.pull_profile_from_laptop("", golden, user_data_dir=ud,
                                      close_chrome=lambda: None, log=lambda m: None)
    assert ok is True
    assert os.path.isfile(os.path.join(golden, "Default", "Network", "Cookies"))


def test_pull_no_email_no_signin_returns_false(tmp_path):
    ud = str(tmp_path / "User Data")
    _write_local_state(ud, {"Default": {}})
    golden = str(tmp_path / "chrome-golden")
    ok = wpp.pull_profile_from_laptop("", golden, user_data_dir=ud,
                                      close_chrome=lambda: None, log=lambda m: None)
    assert ok is False


# --- Task 4: targeted close -------------------------------------------------

def test_parse_user_data_dir_forms():
    f = wpp._parse_user_data_dir_from_cmdline
    assert f(r'chrome.exe --user-data-dir=C:\veo\chrome-session x') == os.path.abspath(r'C:\veo\chrome-session')
    assert f(r'chrome.exe --user-data-dir="C:\path with space\ud"') == os.path.abspath(r'C:\path with space\ud')
    assert f('chrome.exe --no-sandbox') is None


def test_chrome_proc_targets_laptop_ud_only():
    target = r'C:\Users\me\AppData\Local\Google\Chrome\User Data'
    g = wpp._chrome_proc_uses_dir
    assert g('chrome.exe --no-sandbox', target) is True
    # Chrome quotes paths containing spaces (default User Data path has one)
    assert g(rf'chrome.exe --user-data-dir="{target}"', target) is True
    assert g(r'chrome.exe --user-data-dir=C:\veo\chrome-session-2', target) is False
