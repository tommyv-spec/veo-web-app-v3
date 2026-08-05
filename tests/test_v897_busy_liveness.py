"""v897 — a BUSY worker must not read as offline.

The 2026-08-05 case: the ChatGPT worker sends its heartbeat inline in the poll
loop, then blocks for minutes inside generate(). Its heartbeat row ages past
WORKER_HEARTBEAT_STALE_SECONDS and the platform light flips red while the
worker is demonstrably rendering. `_lane_stalled` made it worse: that check
only fires when the light is GREEN, so a busy worker showed as plain offline
and never as working.

A claim taken inside the 10-minute stale-claim window proves the process is
alive right now, so it keeps the lane green and marks it BUSY.
"""
from datetime import datetime, timedelta

import image_platform as ip


NOW = datetime(2026, 8, 5, 12, 0, 0)


class Row:
    def __init__(self, worker_id, age_s, now=NOW):
        self.worker_id = worker_id
        self.last_heartbeat_at = now - timedelta(seconds=age_s)


def _lights(rows):
    rows = sorted(rows, key=lambda r: r.last_heartbeat_at, reverse=True)
    return ip._split_worker_lights(rows, NOW)


def test_stale_beat_plus_fresh_claim_reads_online_and_busy():
    """The exact operator scenario: mid-generation, beat 5 min old."""
    _flow, cg = _lights([Row("chatgpt-BOOK", 300)])
    assert cg["online"] is False  # heartbeat alone says dead
    ip._apply_busy_liveness(cg, fresh_claims=1)
    assert cg["online"] is True, "a worker holding a fresh claim is alive"
    assert cg["busy"] is True, "and it must be reported as BUSY, not idle-green"


def test_stale_beat_and_no_claim_stays_offline():
    """A genuinely dead worker must still read offline — no false green."""
    _flow, cg = _lights([Row("chatgpt-BOOK", 300)])
    ip._apply_busy_liveness(cg, fresh_claims=0)
    assert cg["online"] is False
    assert cg["busy"] is False


def test_fresh_beat_no_claim_is_online_but_idle():
    _flow, cg = _lights([Row("chatgpt-BOOK", 3)])
    ip._apply_busy_liveness(cg, fresh_claims=0)
    assert cg["online"] is True
    assert cg["busy"] is False, "idle green, nothing claimed"


def test_fresh_beat_with_claim_is_online_and_busy():
    _flow, cg = _lights([Row("chatgpt-BOOK", 3)])
    ip._apply_busy_liveness(cg, fresh_claims=2)
    assert cg["online"] is True
    assert cg["busy"] is True


def test_lanes_stay_independent_under_busy_liveness():
    """v891a's core guarantee must survive: a busy chatgpt worker must never
    turn the flow light green."""
    flow, cg = _lights([Row("chatgpt-BOOK", 300)])
    ip._apply_busy_liveness(cg, fresh_claims=1)
    ip._apply_busy_liveness(flow, fresh_claims=0)
    assert cg["online"] is True and cg["busy"] is True
    assert flow["online"] is False and flow["busy"] is False


def test_busy_lane_is_not_reported_stalled():
    """_lane_stalled keys off the corrected light; a busy lane with a fresh
    claim must not be called stalled (generating_fresh > 0)."""
    assert ip._lane_stalled(True, queued=3, oldest_queued_age_s=99999,
                            generating_fresh=1) is False
    # ...but a green light with an old queue and NO fresh claim still stalls.
    assert ip._lane_stalled(True, queued=3, oldest_queued_age_s=99999,
                            generating_fresh=0) is True
