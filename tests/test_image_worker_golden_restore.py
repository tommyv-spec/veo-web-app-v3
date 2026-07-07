# tests/test_image_worker_golden_restore.py
#
# v828: image_worker.restore_from_golden must survive the Windows file-lock
# (WinError 1224 / 32) that Chrome holds for a few seconds past taskkill. The
# old inline restore in launch_browser was a single rmtree+copytree that printed
# a warning and shipped a stale/partial profile on a lock, so the account block
# never actually cleared after a golden-restore relaunch. Ported from
# flow_worker.restore_from_golden (v701g): 3-attempt retry + backoff + Singleton
# lock cleanup.
#
# Direct import works under pytest (stdout is captured, so the module-level
# unicode banner print does not hit a cp1252 console). Run from code/:
#   PYTHONUTF8=1 python -m pytest tests/test_image_worker_golden_restore.py -v

import os
import shutil

import image_worker


def _make_golden(tmp_path):
    """Fake golden profile: a couple of files + a Singleton lock that must be
    skipped by ignore_patterns. Named so get_golden_folder derives it from the
    session path (image-chrome-session -> image-chrome-golden)."""
    golden = tmp_path / "image-chrome-golden"
    (golden / "Default").mkdir(parents=True)
    (golden / "Default" / "Cookies").write_text("cookie-data")
    (golden / "Local State").write_text("state-data")
    (golden / "SingletonLock").write_text("lock")
    return golden


def test_missing_golden_returns_false(tmp_path):
    session = tmp_path / "image-chrome-session"
    assert image_worker.restore_from_golden(str(session), label="TEST") is False


def test_happy_path_copies_golden_and_skips_singletons(tmp_path):
    _make_golden(tmp_path)
    session = tmp_path / "image-chrome-session"
    ok = image_worker.restore_from_golden(str(session), label="TEST")
    assert ok is True
    assert (session / "Default" / "Cookies").read_text() == "cookie-data"
    assert (session / "Local State").read_text() == "state-data"
    assert not (session / "SingletonLock").exists()


def test_retries_on_winerror_lock_then_succeeds(tmp_path, monkeypatch):
    _make_golden(tmp_path)
    session = tmp_path / "image-chrome-session"

    real_copytree = shutil.copytree
    # Count only the forced FAILURES (lock hits). copytree recurses through the
    # patched name for subdirs, so a raw call-count is nondeterministic — but the
    # number of raised WinErrors before the first success is exactly the retry
    # count we care about.
    fails = {"n": 0}

    def flaky_copytree(src, dst, *a, **k):
        if fails["n"] < 2:
            fails["n"] += 1
            raise OSError("[WinError 1224] The requested operation cannot be "
                          "performed on a file with a user-mapped section open")
        return real_copytree(src, dst, *a, **k)

    monkeypatch.setattr(image_worker.shutil, "copytree", flaky_copytree)
    monkeypatch.setattr(image_worker.time, "sleep", lambda *a, **k: None)

    ok = image_worker.restore_from_golden(str(session), label="TEST")
    assert ok is True
    assert fails["n"] == 2  # hit the lock twice, retried, succeeded on the third attempt
    assert (session / "Local State").read_text() == "state-data"
