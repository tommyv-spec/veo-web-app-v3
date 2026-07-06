"""v822 — local-folder watcher never-miss hardening.

Layer 1: pure helpers in local_transcribe (importable standalone).
Layer 2: source-grep-assert endpoint + frontend markers (this codebase has
been bitten by missing-name regressions py_compile does not catch).
"""
import os
from datetime import datetime, timedelta

import importlib.util

_HERE = os.path.dirname(os.path.abspath(__file__))
_CODE = os.path.dirname(_HERE)
_LT = os.path.join(_CODE, "local_transcribe.py")
_MAIN = os.path.join(_CODE, "main.py")
_INDEX = os.path.join(_CODE, "static", "index.html")


def _load_lt():
    spec = importlib.util.spec_from_file_location("lt_v822_test", _LT)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


# ---- Layer 1: should_reprocess ------------------------------------------
def test_failed_always_reprocesses():
    lt = _load_lt()
    assert lt.should_reprocess("failed", datetime.utcnow()) is True


def test_done_never_reprocesses():
    lt = _load_lt()
    old = datetime.utcnow() - timedelta(hours=5)
    assert lt.should_reprocess("done", old) is False


def test_fresh_pending_not_reprocessed():
    lt = _load_lt()
    fresh = datetime.utcnow() - timedelta(seconds=30)
    assert lt.should_reprocess("pending", fresh) is False
    assert lt.should_reprocess("running", fresh) is False


def test_stuck_pending_reprocessed():
    lt = _load_lt()
    stuck = datetime.utcnow() - timedelta(minutes=11)
    assert lt.should_reprocess("pending", stuck) is True
    assert lt.should_reprocess("running", stuck) is True


def test_pending_without_created_at_reprocessed():
    lt = _load_lt()
    assert lt.should_reprocess("pending", None) is True


# ---- Layer 2: endpoint + wiring symbols -----------------------------------
def test_upload_endpoint_uses_should_reprocess():
    src = open(_MAIN, encoding="utf-8").read()
    assert "should_reprocess(" in src
    assert "/api/local-videos/rematch" in src


def test_rematch_helper_defined_and_scoped():
    src = open(_LT, encoding="utf-8").read()
    assert "def rematch_unmatched(user_id, db" in src
    assert 'transcription_status == "done"' in src
    assert "matched_job_id == None" in src


# ---- Layer 2: frontend markers --------------------------------------------
def test_frontend_stability_gate_and_cache():
    src = open(_INDEX, encoding="utf-8").read()
    assert "_localPendingStability" in src
    assert "_localStatCache" in src
    assert "_localMissingStreak" in src
    assert "_localDeleteEligible" in src


def test_frontend_timeout_and_rematch_wiring():
    src = open(_INDEX, encoding="utf-8").read()
    assert "function _fetchT(" in src
    assert '"/api/local-videos/rematch"' in src
    assert "_localRowRetryable" in src
    assert "visibilitychange" in src


def test_frontend_poll_path_noninteractive_permission():
    src = open(_INDEX, encoding="utf-8").read()
    assert "_verifyDirPermission(_localDirHandle, false)" in src
    assert "_localPermissionLost" in src


def test_frontend_dead_upload_helper_removed():
    src = open(_INDEX, encoding="utf-8").read()
    assert "_uploadLocalFile" not in src


def test_frontend_review_fixes_v822_1():
    """Post-review fixes: UTC parse (naive isoformat read as local time broke
    stuck-row retry east of UTC), retry cap, scan-generation guard."""
    src = open(_INDEX, encoding="utf-8").read()
    assert "function _localParseUtc(" in src
    assert "LOCAL_MAX_RETRY_ATTEMPTS" in src
    assert "_localRetryAttempts" in src
    assert "_localScanGen" in src
    assert "gen !== _localScanGen" in src


# ---- v822.3: sweep DoS fix (bulk dialogue + bounded sweep) ----------------
class _FakeQuery:
    def __init__(self, rows):
        self._rows = list(rows)
    def filter(self, *a, **k):
        return self
    def order_by(self, *a, **k):
        return self
    def limit(self, n):
        self._rows = self._rows[:n]
        return self
    def all(self):
        return self._rows
    def first(self):
        return self._rows[0] if self._rows else None


class _FakeDB:
    """Returns the SAME canned rows for every query() — enough for the
    grouping + cooldown/empty-pool guards, which never depend on WHICH cols."""
    def __init__(self, rows):
        self._rows = rows
    def query(self, *cols):
        return _FakeQuery(self._rows)


def test_bulk_dialogue_map_groups_and_coalesces():
    lt = _load_lt()
    rows = [
        ("j1", "hello", None),   # dialogue_text
        ("j1", "", "world"),     # voiceover_line preferred
        ("j2", "solo", None),
    ]
    m = lt._bulk_dialogue_map(_FakeDB(rows), ["j1", "j2"])
    assert m["j1"] == "hello world"
    assert m["j2"] == "solo"


def test_bulk_dialogue_map_empty_ids_no_query():
    lt = _load_lt()
    # empty id list short-circuits BEFORE touching db (pass a db that would
    # raise if queried).
    class _Boom:
        def query(self, *a):
            raise AssertionError("must not query on empty ids")
    assert lt._bulk_dialogue_map(_Boom(), []) == {}


def test_sweep_cooldown_blocks_second_immediate_call():
    lt = _load_lt()
    uid = "cooldown-user-xyz"
    db = _FakeDB([])  # empty candidate pool -> first call returns checked 0
    first = lt.rematch_unmatched(uid, db)
    assert first.get("checked") == 0
    second = lt.rematch_unmatched(uid, db)
    assert second.get("skipped") == "cooldown"


def test_sweep_guard_constants_present():
    lt = _load_lt()
    assert lt._SWEEP_BUDGET_S > 0
    assert lt._SWEEP_MAX_VIDEOS > 0
    assert lt._SWEEP_MAX_AGE_H > 0
    assert lt._SWEEP_COOLDOWN_S > 0


def test_no_per_job_n_plus_one_in_sweep_source():
    """The old per-job _full_dialogue N+1 (Clip query inside the candidate
    loop) must be gone; the bulk map + advance helper must exist."""
    src = open(_LT, encoding="utf-8").read()
    assert "def _bulk_dialogue_map(" in src
    assert "def _advance_job_to_published(" in src
    assert "def _full_dialogue(job):" not in src  # the N+1 shape is deleted
    assert "dialogue_map=dialogue_map" in src     # sweep shares one map
