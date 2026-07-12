"""v850 — durable export queue. Pure state helpers."""
from datetime import datetime, timedelta

import export_queue as eq


def test_running_with_fresh_heartbeat_is_not_stale():
    now = datetime(2026, 7, 12, 12, 0, 0)
    hb = now - timedelta(seconds=30)
    assert eq.is_stale("running", hb, now) is False


def test_running_with_old_heartbeat_is_stale():
    now = datetime(2026, 7, 12, 12, 0, 0)
    hb = now - timedelta(seconds=200)
    assert eq.is_stale("running", hb, now) is True


def test_running_with_no_heartbeat_is_stale():
    now = datetime(2026, 7, 12, 12, 0, 0)
    assert eq.is_stale("running", None, now) is True


def test_queued_with_no_heartbeat_is_stale():
    # A queued run left behind by a dead container must be reclaimed.
    now = datetime(2026, 7, 12, 12, 0, 0)
    assert eq.is_stale("queued", None, now) is True


def test_terminal_states_are_never_stale():
    now = datetime(2026, 7, 12, 12, 0, 0)
    assert eq.is_stale("done", None, now) is False
    assert eq.is_stale("failed", None, now) is False


def test_next_state_retries_below_cap():
    assert eq.next_state_after_reclaim(0) == "queued"
    assert eq.next_state_after_reclaim(2) == "queued"


def test_next_state_gives_up_at_cap():
    assert eq.next_state_after_reclaim(3) == "failed"
    assert eq.next_state_after_reclaim(9) == "failed"
