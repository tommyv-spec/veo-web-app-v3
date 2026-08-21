import sys, pathlib
from datetime import datetime, timedelta
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from autoedit_queue import can_queue, is_claimable, next_state_on_fail, STALE_AFTER


def test_one_live_run_per_job():
    assert can_queue(["done", "failed"])
    assert not can_queue(["done", "running"])


def test_no_history_is_queueable():
    # A job that has never had an auto-edit run must be queueable.
    assert can_queue([])


def test_stale_claim_is_reclaimable():
    now = datetime.utcnow()
    assert is_claimable("queued", None, now)
    assert not is_claimable("running", now, now)
    assert is_claimable("running", now - STALE_AFTER - timedelta(seconds=1), now)
    assert not is_claimable("done", None, now)


def test_claimed_with_no_heartbeat_is_reclaimable():
    # A worker that claimed the row and died before its first heartbeat
    # write must not strand the row forever. No heartbeat == stale.
    now = datetime.utcnow()
    assert is_claimable("claimed", None, now)
    assert is_claimable("running", None, now)


def test_fail_requeues_until_cap():
    assert next_state_on_fail(1) == "queued"
    assert next_state_on_fail(3) == "failed"


def test_fail_past_cap_stays_failed():
    assert next_state_on_fail(4) == "failed"
