"""Non-numeric clip_id on the worker clip-status routes must 404, not 500.

Production has been receiving POST /api/local-worker/clips/d/status (clip_id
literally "d") every few hours since at least 2026-08-21. Clip.id is an
Integer column, so the value reached Postgres and raised
DataError: invalid input syntax for type integer: "d" — a 500 with a full
traceback — instead of the 404 the semantics imply (no such clip exists).
A 404 also matters client-side: workers drop a status update on 404, so a
poisoned retry/replay entry clears itself instead of hammering forever.

Layer 1 unit-tests the pure helper; layer 2 source-grep-asserts both status
endpoints (legacy local-worker + user-worker twin) actually use it, since
main.py is not importable in this test environment.
"""
import os
import re

_HERE = os.path.dirname(os.path.abspath(__file__))
_MAIN = os.path.join(os.path.dirname(_HERE), "main.py")


def _load_helper():
    """Extract and exec just the pure helper from main.py source."""
    src = open(_MAIN, encoding="utf-8").read()
    m = re.search(r"^def _clip_id_as_int\(.*?(?=^\S)", src, re.MULTILINE | re.DOTALL)
    assert m, "_clip_id_as_int not defined in main.py"
    ns = {}
    exec(m.group(0), ns)
    return ns["_clip_id_as_int"]


# ---- Layer 1: pure helper behaviour --------------------------------------
def test_numeric_ids_pass_through_as_int():
    f = _load_helper()
    assert f("14661") == 14661
    assert f(" 7 ") == 7
    assert f(5097) == 5097


def test_the_production_value_d_is_rejected():
    f = _load_helper()
    assert f("d") is None


def test_other_garbage_is_rejected():
    f = _load_helper()
    assert f("") is None
    assert f(None) is None
    assert f("de7f9331-9a15-4fc5-8cff-e2c5a53a8fef") is None
    assert f("-1") is None
    assert f("1.5") is None


# ---- Layer 2: both endpoints wired ---------------------------------------
def test_both_status_endpoints_use_the_guard():
    src = open(_MAIN, encoding="utf-8").read()
    for fn in ("local_worker_update_clip_status", "user_worker_update_clip_status"):
        body_start = src.index(f"async def {fn}(")
        body = src[body_start:body_start + 3000]
        assert "_clip_id_as_int(" in body, f"{fn} does not guard non-numeric clip_id"
        assert "[clip-status-guard]" in body, f"{fn} missing the caller-fingerprint log"


def test_guard_logs_before_touching_the_db():
    """The whole point is evidence: the guard must log client + payload
    fingerprint (the print carries request info) before raising 404."""
    src = open(_MAIN, encoding="utf-8").read()
    assert src.count("[clip-status-guard]") >= 2
