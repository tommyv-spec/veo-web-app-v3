"""My Worker online dot must track the FLOW worker, not the shared token.

UserWorkerToken.last_seen is refreshed by ANY authenticated call on that token,
and the operator runs image_worker.py and chatgpt_image_worker.py on the SAME
token. So a stopped Flow worker kept showing Online while an image worker polled
(observed 2026-08-11). Liveness is now keyed on the worker_id heartbeat, which
only flow_worker sends.
"""
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import main


def _reset(user="u1"):
    main._FLOW_WORKER_BEATS.pop(user, None)
    return user


def test_unknown_worker_returns_none_not_offline():
    # A worker too old to send worker_id must NOT be reported offline — the
    # caller falls back to token.last_seen. Reading unknown as offline would
    # show a working worker as down.
    u = _reset()
    assert main.flow_worker_online(u) is None


def test_fresh_heartbeat_is_online():
    u = _reset()
    main._FLOW_WORKER_BEATS[u] = ("worker-abc", datetime.utcnow())
    assert main.flow_worker_online(u) is True


def test_stale_heartbeat_is_offline():
    u = _reset()
    main._FLOW_WORKER_BEATS[u] = ("worker-abc", datetime.utcnow() - timedelta(seconds=40))
    assert main.flow_worker_online(u) is False


def test_boundary_just_inside_and_outside_window():
    u = _reset()
    now = datetime.utcnow()
    main._FLOW_WORKER_BEATS[u] = ("w", now - timedelta(seconds=14))
    assert main.flow_worker_online(u, now=now) is True
    main._FLOW_WORKER_BEATS[u] = ("w", now - timedelta(seconds=16))
    assert main.flow_worker_online(u, now=now) is False


def test_clean_shutdown_clears_the_beat():
    # going_offline pops the entry; the user then reads as unknown, and the
    # backdated token.last_seen makes the fallback report offline too.
    u = _reset()
    main._FLOW_WORKER_BEATS[u] = ("worker-abc", datetime.utcnow())
    main._FLOW_WORKER_BEATS.pop(u, None)
    assert main.flow_worker_online(u) is None


def test_other_users_are_independent():
    a, b = _reset("ua"), _reset("ub")
    main._FLOW_WORKER_BEATS[a] = ("w-a", datetime.utcnow())
    assert main.flow_worker_online(a) is True
    assert main.flow_worker_online(b) is None
