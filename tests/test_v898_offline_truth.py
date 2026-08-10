"""v898 — a closed worker must read OFFLINE.

Operator 2026-08-03: "i closed the worker but in the platform it still shows
it's online, we need to improve this."

Two defects fed that:

1. v897 let ANY claim newer than the 10-minute stale-claim sweep window override
   a dead heartbeat. Kill a worker mid-node and its claim stays "fresh" for ten
   minutes, so the light stayed green that whole time.
2. The ChatGPT worker never called release-claims with going_offline=true, so
   even a polite Ctrl+C left the heartbeat row in place (the server has deleted
   it on that flag since v516 — nobody sent the flag). That half is covered by
   the worker-side test; this file covers the server rule.

The BUSY window stays at 10 minutes on purpose: a worker that is beating
normally must still read "working" deep into a multi-minute render.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from image_platform import (  # noqa: E402
    WORKER_CLAIM_LIVENESS_SECONDS,
    _apply_busy_liveness,
)


def _light(online):
    return {"online": online, "worker_id": "chatgpt-BOX", "age": None, "busy": False}


def test_killed_worker_with_a_stale_claim_reads_offline():
    """The reported bug: heartbeat dead, claim 5 minutes old (inside the 10-min
    sweep window, outside the liveness window) -> must be OFFLINE."""
    light = _light(False)
    _apply_busy_liveness(light, fresh_claims=1, live_claims=0)
    assert light["online"] is False, "a killed worker must not read online"
    assert light["busy"] is False


def test_working_worker_still_reads_online_and_busy():
    """Beating normally, mid-render: green and flagged busy."""
    light = _light(True)
    _apply_busy_liveness(light, fresh_claims=1, live_claims=0)
    assert light["online"] is True
    assert light["busy"] is True, "a beating worker mid-render must read as working"


def test_recent_claim_still_covers_a_short_beat_outage():
    """v897's real purpose survives: a few failed beats plus a claim taken
    seconds ago still keeps the lane green and marks it busy."""
    light = _light(False)
    _apply_busy_liveness(light, fresh_claims=1, live_claims=1)
    assert light["online"] is True
    assert light["busy"] is True


def test_idle_online_worker_is_not_busy():
    light = _light(True)
    _apply_busy_liveness(light, fresh_claims=0, live_claims=0)
    assert light["online"] is True
    assert light["busy"] is False


def test_no_claims_and_no_heartbeat_is_offline():
    light = _light(False)
    _apply_busy_liveness(light, fresh_claims=0, live_claims=0)
    assert light["online"] is False
    assert light["busy"] is False


def test_pre_v898_callers_keep_old_behaviour():
    """Two-arg calls must behave exactly as they did under v897."""
    light = _light(False)
    _apply_busy_liveness(light, 1)
    assert light["online"] is True and light["busy"] is True


def test_liveness_window_is_far_shorter_than_the_sweep_window():
    assert 0 < WORKER_CLAIM_LIVENESS_SECONDS < 600, (
        "the liveness window must be well under the 10-minute stale-claim sweep "
        "window — reusing the sweep window is exactly what caused the bug")
