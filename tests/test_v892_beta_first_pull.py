"""v892 — Beta-first laptop-profile lookup for the image worker.

The account in Chrome Beta must win over the same account in stable Chrome
(so the copy never touches the operator's daily browser); when the account
is only in stable, the old stable-first behavior must still work.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "static"))

import worker_profile_pull as wpp


def _mk_ud(tmp_path, name):
    ud = tmp_path / name / "User Data"
    ud.mkdir(parents=True)
    (ud / "Local State").write_text("{}", encoding="utf-8")
    return str(ud)


def test_beta_wins_over_stable(tmp_path, monkeypatch):
    stable = _mk_ud(tmp_path, "Chrome")
    beta = _mk_ud(tmp_path, "Chrome Beta")
    monkeypatch.setattr(wpp, "resolve_laptop_user_data_dirs", lambda env=None: [stable, beta])
    monkeypatch.setattr(wpp, "find_profile_dir_for_email", lambda ud, email: "Profile 7")
    ud, pf, ch = wpp.locate_profile_beta_first("a@b.com")
    assert ud == beta
    assert pf == "Profile 7"


def test_stable_fallback_when_not_in_beta(tmp_path, monkeypatch):
    stable = _mk_ud(tmp_path, "Chrome")
    beta = _mk_ud(tmp_path, "Chrome Beta")
    monkeypatch.setattr(wpp, "resolve_laptop_user_data_dirs", lambda env=None: [stable, beta])
    # account only present in the stable dir
    monkeypatch.setattr(wpp, "find_profile_dir_for_email",
                        lambda ud, email: "Default" if ud == stable else None)
    ud, pf, ch = wpp.locate_profile_beta_first("a@b.com")
    assert ud == stable
    assert pf == "Default"


def test_none_when_nowhere(tmp_path, monkeypatch):
    stable = _mk_ud(tmp_path, "Chrome")
    monkeypatch.setattr(wpp, "resolve_laptop_user_data_dirs", lambda env=None: [stable])
    monkeypatch.setattr(wpp, "find_profile_dir_for_email", lambda ud, email: None)
    assert wpp.locate_profile_beta_first("a@b.com") is None
