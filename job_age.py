"""Age cap for worker job claiming.

Workers claim by STATUS only, so a job that was queued months ago and never
finished stays claimable forever. A 2026-08-07 Firefox test run picked up
exactly that: ancient redo clips, re-rendering them at full Flow cost.

Kept out of main.py so it is unit-testable without starting the app.
"""
import os
from datetime import datetime, timedelta

DEFAULT_MAX_JOB_AGE_DAYS = 7


def max_job_age_days(env=None):
    """Claim window in days. 0 disables the cap (deliberate backfill).

    Any unparseable or negative value falls back to the default rather than
    disabling the cap — a typo must never silently reopen the whole backlog.
    """
    env = os.environ if env is None else env
    raw = (env.get("WORKER_MAX_JOB_AGE_DAYS") or "").strip()
    if not raw:
        return DEFAULT_MAX_JOB_AGE_DAYS
    try:
        val = int(raw)
    except ValueError:
        return DEFAULT_MAX_JOB_AGE_DAYS
    if val < 0:
        return DEFAULT_MAX_JOB_AGE_DAYS
    return val


def job_age_cutoff(env=None, now=None):
    """Oldest Job.created_at a worker may claim, or None when uncapped."""
    days = max_job_age_days(env)
    if days == 0:
        return None
    return (now or datetime.utcnow()) - timedelta(days=days)
