import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "static"))
import chatgpt_session_pull as sp

def test_pull_delegates_to_lean_golden(monkeypatch):
    calls = {}
    import worker_profile_pull as wpp
    def fake_build(email, golden_folder, label="", close_chrome=None, log=print, **kw):
        calls["email"] = email; calls["golden"] = golden_folder; calls["label"] = label
        return "chrome"
    monkeypatch.setattr(wpp, "build_lean_golden_from_profile", fake_build)
    out = sp.pull_chatgpt_session("me@x.com", "/tmp/gold")
    assert out == "chrome"
    assert calls["email"] == "me@x.com" and calls["golden"] == "/tmp/gold" and calls["label"] == "CHATGPT"
