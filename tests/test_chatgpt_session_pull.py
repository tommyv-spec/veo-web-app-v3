import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "static"))
import chatgpt_session_pull as sp

def test_normal_cookie_dict():
    c = sp.chatgpt_cookie_from_pair("__Secure-next-auth.session-token.0", "abc", "chatgpt.com")
    assert c["name"] == "__Secure-next-auth.session-token.0"
    assert c["value"] == "abc"
    assert c["domain"] == ".chatgpt.com"
    assert c["secure"] is True

def test_host_cookie_uses_url_not_domain():
    c = sp.chatgpt_cookie_from_pair("__Host-next-auth.csrf-token", "x", "chatgpt.com")
    assert c.get("url", "").startswith("https://chatgpt.com")
    assert "domain" not in c

def test_filter_keeps_session_token_drops_handshake():
    rows = [("__Secure-next-auth.session-token.0", "v", "chatgpt.com"),
            ("__Host-next-auth.csrf-token", "v", "chatgpt.com"),
            ("_ga", "v", "google.com")]
    out = sp.filter_chatgpt_cookies(rows)
    names = {c["name"] for c in out}
    assert "__Secure-next-auth.session-token.0" in names   # kept
    assert "__Host-next-auth.csrf-token" not in names       # handshake dropped
    assert "_ga" not in names                               # non-chatgpt host dropped

def test_has_session_token():
    assert sp.has_session_token([{"name": "__Secure-next-auth.session-token.0"}]) is True
    assert sp.has_session_token([{"name": "oai-did"}]) is False
