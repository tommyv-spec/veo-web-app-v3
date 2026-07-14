"""v850 — durable export queue: pure state helpers.

The export state machine lives here (no DB, no FastAPI imports) so it is
unit-testable without booting main.py. main.py owns the DB rows and the
asyncio runner; this module owns the RULES.

States: queued -> running -> done | failed

A run is RECLAIMABLE when its heartbeat is stale: the container that owned
it died (Render deploy / OOM / crash) without finishing. The runner
heartbeats every HEARTBEAT_INTERVAL_S; anything older than STALE_AFTER_S is
considered orphaned.
"""
import os
from datetime import datetime
from typing import Optional

STATE_QUEUED = "queued"
STATE_RUNNING = "running"
STATE_DONE = "done"
STATE_FAILED = "failed"

ACTIVE_STATES = (STATE_QUEUED, STATE_RUNNING)
TERMINAL_STATES = (STATE_DONE, STATE_FAILED)

HEARTBEAT_INTERVAL_S = 30
# 6 missed ticks, not 3. The heartbeat is written from the event loop while
# ffmpeg saturates Render's single CPU, so under export load a few ticks WILL
# land late. A false-positive reclaim is the expensive failure: it double-runs
# a 15-minute export (two ffmpeg jobs fighting for the same CPU, two uploads).
# The wide window costs the common path nothing — a graceful deploy NULLs the
# heartbeat on shutdown, so the next container reclaims instantly instead of
# waiting this out. Only a hard kill (OOM/SIGKILL) pays the delay.
STALE_AFTER_S = 180
MAX_ATTEMPTS = 3

# v855 — how many exports may RUN AT ONCE on this container.
#
# DEFAULT 1. Do not raise it without measuring peak RSS on the real box.
# Render standard = 2 GB RAM / 1 CPU (render.yaml:11-12), and a SINGLE export
# already lives at that ceiling: v691d, v701w, v701x, v701y, v701z and v692b are
# every one of them a fix for ONE export OOMing. Two exports at once means two
# ffmpeg chains and two Whisper models on one CPU — and on one CPU that is not
# even FASTER than running them back to back, just riskier. The box also runs 5
# video-gen workers (MAX_JOB_WORKERS=5).
#
# Queueing is the feature; parallelism is not.
MAX_CONCURRENT = int(os.environ.get("EXPORT_MAX_CONCURRENT", "1"))

# How often the dispatcher looks for queued work. A 2s pickup delay is invisible
# next to a 10-minute export.
DISPATCH_INTERVAL_S = 2


def slots_free(running_now: int, max_concurrent: int = MAX_CONCURRENT) -> int:
    """How many more exports this container may start right now."""
    return max(0, max_concurrent - running_now)


def is_stale(state: str, heartbeat_at: Optional[datetime], now: datetime,
             stale_after_s: int = STALE_AFTER_S) -> bool:
    """True when an active run has no live owner and should be reclaimed.

    A NULL heartbeat means either (a) never started, or (b) a graceful
    shutdown deliberately cleared it so the next boot picks it up at once.
    Both are reclaimable.
    """
    if state not in ACTIVE_STATES:
        return False
    if heartbeat_at is None:
        return True
    return (now - heartbeat_at).total_seconds() > stale_after_s


def next_state_after_reclaim(attempts: int, max_attempts: int = MAX_ATTEMPTS) -> str:
    """Where a reclaimed run goes: back in the queue, or give up.

    `attempts` is how many times the run has ALREADY been started.
    """
    return STATE_QUEUED if attempts < max_attempts else STATE_FAILED
