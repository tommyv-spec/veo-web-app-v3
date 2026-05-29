"""Post-render lifecycle helpers (2026-05-29).

Spec: docs/superpowers/specs/2026-05-29-video-lifecycle-tracker-design.md §3.

Kept in a separate module so helpers are importable in tests without loading
the full FastAPI/SQLAlchemy stack from main.py.
"""
from config import LifecycleStage

_LIFECYCLE_STAGE_TO_TIMESTAMP_FIELD = {
    LifecycleStage.AWAITING_APPROVAL.value:  "approval_at",
    LifecycleStage.AWAITING_EXPORT.value:    "export_at",
    LifecycleStage.AWAITING_FINISHING.value: "finishing_at",
    LifecycleStage.PUBLISHED.value:          "published_at",
}


def apply_lifecycle_change(job, stage, notes, now, clear=False):
    """Pure mutation of a Job's lifecycle fields. No DB I/O.

    Args:
        job: a Job (or duck-typed stand-in) with the 7 lifecycle attributes.
        stage: target LifecycleStage value (str), or None (no stage change)
               unless clear=True.
        notes: new notes value, or None to skip notes update.
               Pass an empty string to clear notes.
        now: datetime to record as the stage-entry timestamp.
        clear: when True with stage=None, blanks lifecycle_stage.

    Behavior:
        - If stage is set, validates against LifecycleStage. On first entry to
          a stage (existing timestamp is None), records `now`. Re-entry to a
          previously visited stage preserves the original timestamp (COALESCE).
        - Notes are overwritten only when `notes is not None`. Pass "" to clear.
        - Stage timestamps are NEVER cleared (they form an audit trail).

    Raises:
        ValueError: when stage is not a valid LifecycleStage value.
    """
    if stage is not None:
        if stage not in _LIFECYCLE_STAGE_TO_TIMESTAMP_FIELD:
            raise ValueError(f"invalid lifecycle stage: {stage!r}")
        job.lifecycle_stage = stage
        ts_field = _LIFECYCLE_STAGE_TO_TIMESTAMP_FIELD[stage]
        if getattr(job, ts_field) is None:
            setattr(job, ts_field, now)
    elif clear:
        job.lifecycle_stage = None

    if notes is not None:
        job.notes = notes


def compute_stuck_days(job, now):
    """How many full days the Job has sat in its current lifecycle stage.

    Returns None when the Job has no lifecycle stage or when the current
    stage's entry timestamp is missing (defensive).
    """
    if not job.lifecycle_stage:
        return None
    ts_field = _LIFECYCLE_STAGE_TO_TIMESTAMP_FIELD.get(job.lifecycle_stage)
    if ts_field is None:
        return None
    ts = getattr(job, ts_field, None)
    if ts is None:
        return None
    return (now - ts).days


def _maybe_auto_enter_lifecycle(job, now):
    """Idempotent T1 trigger — moves a freshly-completed Job into the lifecycle.

    has_export=True implies all clips approved + export-final ran (the export
    endpoint enforces that). Such jobs skip approval+export stages and land
    directly in awaiting_finishing (the first manual stage — captions/touches
    in CapCut). Jobs without has_export sit in awaiting_approval.
    """
    if job.lifecycle_stage:
        return
    if job.status != "completed":
        return
    if job.has_export:
        job.lifecycle_stage = LifecycleStage.AWAITING_FINISHING.value
        if job.approval_at is None:
            job.approval_at = now
        if job.export_at is None:
            job.export_at = now
        if job.finishing_at is None:
            job.finishing_at = now
    else:
        job.lifecycle_stage = LifecycleStage.AWAITING_APPROVAL.value
        if job.approval_at is None:
            job.approval_at = now


def apply_jobs_filters(query, user_id, status, since_days, lifecycle, archived):
    """Apply standard Jobs-list filters to a SQLAlchemy query. Returns query.

    Test-friendly: _FakeQuery in tests counts .filter() calls without needing
    real SQLAlchemy column expressions. Real callers pass a db.query(Job) and
    the expressions evaluate normally.

    Filter logic:
    - user_id: always applied (1 filter call).
    - status: applied when truthy (1 filter call).
    - since_days: applied when > 0 (1 filter call).
    - lifecycle "any": jobs with any stage set (1 filter call).
    - lifecycle "null": jobs with no stage (1 filter call).
    - lifecycle <value>: exact stage match (1 filter call).
    - archived: always applied (1 filter call).
    """
    from datetime import datetime, timedelta
    from models import Job

    query = query.filter(Job.user_id == user_id)
    if status:
        query = query.filter(Job.status == status)
    if since_days and since_days > 0:
        cutoff = datetime.utcnow() - timedelta(days=since_days)
        query = query.filter(Job.created_at >= cutoff)
    if lifecycle == "any":
        query = query.filter(Job.lifecycle_stage.isnot(None))
    elif lifecycle == "null":
        query = query.filter(Job.lifecycle_stage.is_(None))
    elif lifecycle:
        query = query.filter(Job.lifecycle_stage == lifecycle)
    query = query.filter(Job.archived == bool(archived))
    return query
