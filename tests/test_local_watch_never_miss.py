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
