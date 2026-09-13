"""v996 -- a job is not stopped for a proactive golden restore that v967 will skip.

The proactive restore exists to put a clean-login profile snapshot back every
6 clips (reCAPTCHA hygiene). v967 refuses to copy a golden over a live session
that is newer than it (Google rotates the token on every sign-in, so the older
golden is a corpse) -- which is every restore after the first login. On
2026-09-13 that made each proactive restore a mid-job stop, a browser relaunch,
`[v967] SKIPPING golden restore`, and a resume that killed the rest of the run.
Twice. Now the mid-job stop asks v967's question first, resets the clip
counter and keeps going. The reactive restore and FORCE_GOLDEN_RESTORE are
untouched.
"""
import ast
import os
import pathlib
import time
import types

_STATIC = pathlib.Path(__file__).parent / "static"
_PATH = pathlib.Path(os.environ.get("FLOW_WORKER_PATH") or (_STATIC / "flow_worker.py"))


def _tree():
    return ast.parse(_PATH.read_text(encoding="utf-8", errors="replace"))


def _fn(tree, name):
    return next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == name)


def _load(name, **ns):
    """Compile ONE module-level function out of the worker and return it --
    the worker itself cannot be imported in a test (35k lines of side effects)."""
    mod = ast.Module(body=[_fn(_tree(), name)], type_ignores=[])
    ast.fix_missing_locations(mod)
    space = {"os": os, **ns}
    exec(compile(mod, f"<{name}>", "exec"), space)
    return space[name]


def test_the_comparison_lives_in_one_function_and_restore_uses_it():
    tree = _tree()
    _fn(tree, "_v967_session_lead_seconds")
    rf = _fn(tree, "restore_from_golden")
    calls = [c for c in ast.walk(rf) if isinstance(c, ast.Call)
             and isinstance(c.func, ast.Name) and c.func.id == "_v967_session_lead_seconds"]
    assert len(calls) == 1, "restore_from_golden must ask the shared comparison exactly once"
    assert not any(isinstance(c, ast.Call) and isinstance(c.func, ast.Attribute) and c.func.attr == "getmtime"
                   for c in ast.walk(rf)), "the inline mtime comparison must be gone"


def test_session_lead_seconds_behaviour(tmp_path, monkeypatch):
    lead = _load("_v967_session_lead_seconds")
    s, g = tmp_path / "s", tmp_path / "g"
    s.mkdir(); g.mkdir()
    (s / "cookies.sqlite").write_bytes(b"x")
    (g / "cookies.sqlite").write_bytes(b"x")
    now = time.time()
    os.utime(s / "cookies.sqlite", (now, now))
    os.utime(g / "cookies.sqlite", (now - 100, now - 100))
    monkeypatch.delenv("FORCE_GOLDEN_RESTORE", raising=False)
    assert 99 < lead(str(s), str(g)) < 101          # live newer -> positive lead
    os.utime(g / "cookies.sqlite", (now + 50, now + 50))
    assert lead(str(s), str(g)) < 0                  # golden newer -> negative
    monkeypatch.setenv("FORCE_GOLDEN_RESTORE", "1")
    assert lead(str(s), str(g)) is None              # forced -> no opinion
    monkeypatch.delenv("FORCE_GOLDEN_RESTORE")
    (g / "cookies.sqlite").unlink()
    assert lead(str(s), str(g)) is None              # missing DB -> no opinion


def test_restore_would_be_skipped_behaviour(tmp_path, monkeypatch):
    lead = _load("_v967_session_lead_seconds")
    gold = tmp_path / "golden"
    would_skip = _load("_v996_restore_would_be_skipped",
                       _v967_session_lead_seconds=lead, get_golden_folder=lambda s: str(gold))
    monkeypatch.delenv("FORCE_GOLDEN_RESTORE", raising=False)
    sess = tmp_path / "session"
    sess.mkdir()
    (sess / "cookies.sqlite").write_bytes(b"x")
    page = types.SimpleNamespace(_user_data_dir=str(sess))
    assert would_skip(types.SimpleNamespace()) is False   # profile unknown -> old behaviour
    assert would_skip(page) is True                        # no golden -> nothing to restore
    gold.mkdir()
    (gold / "cookies.sqlite").write_bytes(b"x")
    now = time.time()
    os.utime(sess / "cookies.sqlite", (now, now))
    os.utime(gold / "cookies.sqlite", (now - 3600, now - 3600))
    assert would_skip(page) is True                        # live newer -> v967 skips -> do not stop
    os.utime(gold / "cookies.sqlite", (now + 60, now + 60))
    assert would_skip(page) is False                       # golden newer -> restore runs -> stop
    monkeypatch.setenv("FORCE_GOLDEN_RESTORE", "1")
    os.utime(gold / "cookies.sqlite", (now - 3600, now - 3600))
    assert would_skip(page) is False                       # forced -> restore runs -> stop


def test_mid_job_stop_asks_first_and_resets_only_the_counter():
    tree = _tree()
    guards = [n for n in ast.walk(tree) if isinstance(n, ast.If)
              and ast.unparse(n.test) == "_needs_proactive_restore and _v996_restore_would_be_skipped(page)"]
    assert len(guards) == 1
    body = ast.unparse(guards[0])
    assert "account_health.reset_restore_counter(account_name)" in body
    assert "_needs_proactive_restore = False" in body
    cls = next(n for n in ast.walk(tree) if isinstance(n, ast.ClassDef) and n.name == "AccountHealthTracker")
    m = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == "reset_restore_counter")
    s = ast.unparse(m)
    assert "acc['clips_since_restore'] = 0" in s
    assert "consecutive_failures" not in s, "only the clip counter may be cleared"


def test_the_old_stop_still_follows_when_a_restore_would_run():
    plain = [n for n in ast.walk(_tree()) if isinstance(n, ast.If)
             and ast.unparse(n.test) == "_needs_proactive_restore"]
    # two sites read the flag: the print-and-save-state one right after the
    # check, and the raise at the end of the clip iteration; both must survive
    assert len(plain) == 2, "the real proactive stop must survive for a restore that would run"
