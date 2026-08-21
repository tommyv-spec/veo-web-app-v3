"""Pure decisions for the auto-edit queue. The endpoints apply them to rows.

Mirrors export_queue.py: no DB, no FastAPI imports, so this is testable
without booting main.py. States: queued -> claimed -> running -> done | failed.
"""
from datetime import datetime, timedelta

STALE_AFTER = timedelta(minutes=5)
MAX_ATTEMPTS = 3


def can_queue(existing_states: list) -> bool:
    """One live run per job: refuse while a queued/claimed/running one exists."""
    return not any(s in ("queued", "claimed", "running") for s in existing_states)


def is_claimable(state: str, heartbeat_at, now: datetime) -> bool:
    """True when a worker may claim this row.

    A `claimed`/`running` row with NO heartbeat is treated as stale, same as
    a NULL heartbeat on `queued`. Without this, a worker that claimed a row
    and died before its first heartbeat write would leave the row stuck
    forever — nobody could ever reclaim it, since `heartbeat_at is not None`
    would always be False.
    """
    if state == "queued":
        return True
    if state in ("claimed", "running"):
        if heartbeat_at is None:
            return True
        return now - heartbeat_at > STALE_AFTER
    return False


def next_state_on_fail(attempts: int) -> str:
    return "queued" if attempts < MAX_ATTEMPTS else "failed"
