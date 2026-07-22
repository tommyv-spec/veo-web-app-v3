import sys, os, sqlite3
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "static"))
import chatgpt_session_pull as sp
import worker_profile_pull as wpp


def _make_cookies(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE cookies (host_key TEXT, name TEXT, encrypted_value BLOB)")
    for h, n in rows:
        con.execute("INSERT INTO cookies VALUES (?,?,?)", (h, n, b"x"))
    con.commit(); con.close()


def test_sqlite_backup_and_session_token(tmp_path):
    src = os.path.join(tmp_path, "src", "Cookies")
    dst = os.path.join(tmp_path, "dst", "Cookies")
    _make_cookies(src, [("chatgpt.com", "__Secure-next-auth.session-token.0"), ("x", "y")])
    assert sp._sqlite_backup(src, dst) is True
    assert sp._has_session_token(dst) is True


def test_has_session_token_false_without_token(tmp_path):
    db = os.path.join(tmp_path, "Cookies")
    _make_cookies(db, [("chatgpt.com", "oai-did")])
    assert sp._has_session_token(db) is False


def test_pull_returns_false_when_no_profile(monkeypatch):
    monkeypatch.setattr(wpp, "locate_profile", lambda e: None)
    assert sp.pull_chatgpt_session("x@y.com", "/tmp/g_none") is False


def test_pull_copies_live_and_verifies(tmp_path, monkeypatch):
    ud = os.path.join(tmp_path, "ud")
    _make_cookies(os.path.join(ud, "Default", "Network", "Cookies"),
                  [("chatgpt.com", "__Secure-next-auth.session-token.0")])
    with open(os.path.join(ud, "Local State"), "w") as f:
        f.write("{}")
    monkeypatch.setattr(wpp, "locate_profile", lambda e: (ud, "Default", "chrome"))
    gold = os.path.join(tmp_path, "gold")
    assert sp.pull_chatgpt_session("x@y.com", gold) == "chrome"
    assert os.path.exists(os.path.join(gold, "Default", "Network", "Cookies"))
    assert os.path.exists(os.path.join(gold, "Local State"))


def test_pull_false_when_no_session_token_in_source(tmp_path, monkeypatch):
    ud = os.path.join(tmp_path, "ud2")
    _make_cookies(os.path.join(ud, "Default", "Network", "Cookies"),
                  [("chatgpt.com", "oai-did")])  # no session-token
    with open(os.path.join(ud, "Local State"), "w") as f:
        f.write("{}")
    monkeypatch.setattr(wpp, "locate_profile", lambda e: (ud, "Default", "chrome"))
    assert sp.pull_chatgpt_session("x@y.com", os.path.join(tmp_path, "gold2")) is False
