# tests/test_auto_finish_trigger.py
#
# v947 — the auto-finish trigger hangs off approve_clip, and approve_clip has
# ALREADY committed the approval by the time it runs. That ordering is what
# makes these two failures possible, and both were real:
#
#   1. A trigger that throws mid-transaction leaves the session deactivated.
#      The `return ApprovalResponse(clip_id=clip.id, ...)` below it then touches
#      an attribute the commit expired, SQLAlchemy re-loads it, and the dead
#      transaction turns that into PendingRollbackError — a 500 on an approval
#      that actually succeeded and is already durable. The except must roll the
#      TRIGGER's transaction back (the approval's is long gone).
#
#   2. Re-clicking approve on an already-approved clip re-fired the trigger.
#      _queue_export_run's idempotent join only covers queued/running, so once
#      the first export finished, the second click queued a whole new one.
#
# Both are checked against a real sqlite session and the real approve_clip, not
# a mock of it — the bugs live in the interaction with the session, which is
# exactly what a mocked session would paper over.

import asyncio
import json
import sys
import pathlib
import types

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import image_platform  # noqa: F401 — registers image_nodes for Clip's FK
from models import Job, Clip, JobLog, ExportRun
import main
from models import ClipStatus


def _session():
    eng = create_engine("sqlite:///:memory:")
    Job.__table__.metadata.create_all(eng)
    return sessionmaker(bind=eng)()


def _job(db, spec, job_id="job-auto-1", user_id="u1"):
    db.add(Job(
        id=job_id, user_id=user_id,
        config_json="{}", dialogue_json="[]", images_dir="", output_dir="",
        finishing_spec=json.dumps(spec) if spec is not None else None,
    ))
    db.commit()
    return job_id


def _clip(db, job_id, index, approval=None):
    c = Clip(
        job_id=job_id, clip_index=index,
        dialogue_id=f"d{index}", dialogue_text=f"line {index}",
        status=ClipStatus.COMPLETED.value, approval_status=approval,
    )
    db.add(c)
    db.commit()
    return c.id


def _approve(db, clip_id, user_id="u1"):
    return asyncio.run(main.approve_clip(
        clip_id=clip_id, db=db, current_user=types.SimpleNamespace(id=user_id),
    ))


ON = {"auto_finish": "on"}


def test_trigger_failure_does_not_500_the_approval(monkeypatch):
    """The reproduced critical: a throwing trigger must not poison the response."""
    db = _session()
    job_id = _job(db, ON)
    _clip(db, job_id, 0, approval="approved")
    last = _clip(db, job_id, 1)

    real_add_job_log = main.add_job_log

    def _boom(session, jid, message, level="INFO", source=None):
        if source != "auto_finish":
            return real_add_job_log(session, jid, message, level, source)
        # A genuine DB failure inside the trigger, mid-transaction — a JobLog
        # row missing its NOT NULL message. This is what deactivates the
        # session; raising a bare Python error would not reproduce the bug.
        session.add(JobLog(job_id=jid, message=None))
        session.commit()

    monkeypatch.setattr(main, "add_job_log", _boom)

    result = _approve(db, last)          # must NOT raise PendingRollbackError

    assert result.status == "approved"
    assert result.clip_id == last
    # The approval itself is durable, and the export row committed BEFORE the
    # log blew up survives — losing the log line is the accepted trade.
    assert db.query(Clip).filter(Clip.id == last).one().approval_status == "approved"
    assert db.query(ExportRun).filter(ExportRun.job_id == job_id).count() == 1
    # The session is usable again, which is the whole point of the rollback.
    assert db.query(Job).filter(Job.id == job_id).one().id == job_id


def test_reclicking_approve_does_not_queue_a_second_export():
    """A no-op re-click must not start another export once the first is DONE."""
    db = _session()
    job_id = _job(db, ON)
    last = _clip(db, job_id, 0)

    _approve(db, last)
    assert db.query(ExportRun).filter(ExportRun.job_id == job_id).count() == 1

    # First export finishes, so the idempotent join can no longer catch a
    # re-fire — only the was_approved guard can.
    run = db.query(ExportRun).filter(ExportRun.job_id == job_id).one()
    run.state = "done"
    db.commit()

    _approve(db, last)
    assert db.query(ExportRun).filter(ExportRun.job_id == job_id).count() == 1


@pytest.mark.parametrize("reset_to", ["rejected", "pending_review", None])
def test_redo_after_reject_still_fires(reset_to):
    """The guard keys on the PRIOR status, so a real redo is not suppressed.

    All three values a reset really produces are checked: production writes
    "rejected" (reject) and "pending_review" (redo); None is the falsy edge a
    future `if clip.approval_status:` "simplification" would break silently.
    """
    db = _session()
    job_id = _job(db, ON)
    last = _clip(db, job_id, 0)

    _approve(db, last)
    run = db.query(ExportRun).filter(ExportRun.job_id == job_id).one()
    run.state = "done"
    db.commit()

    # A redo/reject resets the clip's approval away from "approved".
    clip = db.query(Clip).filter(Clip.id == last).one()
    clip.approval_status = reset_to
    db.commit()

    _approve(db, last)
    assert db.query(ExportRun).filter(ExportRun.job_id == job_id).count() == 2


@pytest.mark.parametrize("spec", [None, {"auto_finish": "off"}, {}])
def test_no_declaration_queues_nothing(spec):
    db = _session()
    job_id = _job(db, spec)
    last = _clip(db, job_id, 0)

    _approve(db, last)
    assert db.query(ExportRun).filter(ExportRun.job_id == job_id).count() == 0


def test_unapproved_sibling_holds_the_export():
    """Approving clip 0 of 2 must not fire — the trigger waits for the last."""
    db = _session()
    job_id = _job(db, ON)
    first = _clip(db, job_id, 0)
    _clip(db, job_id, 1)

    _approve(db, first)
    assert db.query(ExportRun).filter(ExportRun.job_id == job_id).count() == 0
