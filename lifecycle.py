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
        - Notes are merge-updated only when `notes is not None`.
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
