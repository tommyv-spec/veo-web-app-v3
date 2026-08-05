"""v891 — worker-status truthfulness.

a) The flow light must NEVER take a chatgpt heartbeat row (the 2026-08-05
   false-"Online": only the chatgpt worker was beating, flow worker dead,
   platform showed "Your image worker: ● Online").
b) Stalled = light green but the lane's queue not draining.
"""
from datetime import datetime, timedelta

import image_platform as ip


class Row:
    def __init__(self, worker_id, age_s, now):
        self.worker_id = worker_id
        self.last_heartbeat_at = now - timedelta(seconds=age_s)


NOW = datetime(2026, 8, 5, 12, 0, 0)


def _lights(rows):
    rows = sorted(rows, key=lambda r: r.last_heartbeat_at, reverse=True)
    return ip._split_worker_lights(rows, NOW)


def test_chatgpt_row_never_feeds_flow_light():
    # The exact operator scenario: only a chatgpt worker beating.
    flow, cg = _lights([Row("chatgpt-mypc", 3, NOW)])
    assert flow["online"] is False
    assert flow["worker_id"] is None
    assert cg["online"] is True
    assert cg["worker_id"] == "chatgpt-mypc"


def test_flow_row_never_feeds_chatgpt_light():
    flow, cg = _lights([Row("worker-abc", 3, NOW)])
    assert flow["online"] is True
    assert cg["online"] is False


def test_both_kinds_each_take_their_own_freshest():
    flow, cg = _lights([
        Row("chatgpt-mypc", 2, NOW),
        Row("worker-abc", 5, NOW),
        Row("worker-old", 90, NOW),
    ])
    assert flow["worker_id"] == "worker-abc"
    assert flow["online"] is True
    assert cg["worker_id"] == "chatgpt-mypc"


def test_stale_flow_row_reports_age_but_offline():
    flow, _cg = _lights([Row("worker-abc", 60, NOW)])
    assert flow["online"] is False
    assert flow["worker_id"] == "worker-abc"
    assert flow["age"] == 60.0


def test_default_worker_id_counts_as_flow():
    # Parallel-mode worker beats without a worker_id param → row "default".
    flow, cg = _lights([Row("default", 3, NOW)])
    assert flow["online"] is True
    assert cg["online"] is False


def test_stalled_requires_online_and_old_queue_and_no_fresh_claim():
    S = ip.IMAGE_QUEUE_STALL_SECONDS
    assert ip._lane_stalled(True, 3, S + 10, 0) is True
    # offline lane is just offline, not stalled
    assert ip._lane_stalled(False, 3, S + 10, 0) is False
    # empty queue can't stall
    assert ip._lane_stalled(True, 0, None, 0) is False
    # queue younger than the window — worker may simply not have gotten to it
    assert ip._lane_stalled(True, 3, S - 10, 0) is False
    # a fresh claim means the worker is genuinely busy, not wedged
    assert ip._lane_stalled(True, 3, S + 10, 1) is False
