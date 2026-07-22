import sys, os, json
import pytest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "static"))
import worker_profile_pull as wpp


def _local_state(tmp_path, names):
    ud = str(tmp_path)
    ls = {"profile": {"info_cache": {folder: {"name": nm} for folder, nm in names.items()}}}
    with open(os.path.join(ud, "Local State"), "w", encoding="utf-8") as f:
        f.write(json.dumps(ls))
    return ud


def test_profiles_sharing_name_finds_collision(tmp_path):
    ud = _local_state(tmp_path, {"Default": "Tommaso", "Profile 21": "Tommaso", "Profile 1": "Online"})
    assert set(wpp._profiles_sharing_name(ud, "Tommaso")) == {"Default", "Profile 21"}


def test_profiles_sharing_name_unique(tmp_path):
    ud = _local_state(tmp_path, {"Default": "Tommaso", "Profile 1": "Online"})
    assert wpp._profiles_sharing_name(ud, "Online") == ["Profile 1"]


def test_profiles_sharing_name_case_insensitive(tmp_path):
    ud = _local_state(tmp_path, {"Default": "Tommaso", "Profile 21": "tommaso"})
    assert set(wpp._profiles_sharing_name(ud, "TOMMASO")) == {"Default", "Profile 21"}


@pytest.mark.skipif(sys.platform != "win32", reason="UIA close is Windows-only")
def test_close_refuses_ambiguous_name(tmp_path, monkeypatch):
    ud = _local_state(tmp_path, {"Default": "Tommaso", "Profile 21": "Tommaso"})
    monkeypatch.setattr(wpp, "_profile_display_name", lambda u, f: "Tommaso")
    # ambiguous -> returns -1 sentinel WITHOUT closing anything (no PowerShell run)
    assert wpp._close_target_profile_windows(ud, "Default") == -1
