"""Real-sqlite contract tests for the atomic QC approval endpoint.

These tests need the application's FastAPI dependency set.  They intentionally
exercise the endpoint coroutine with a real SQLAlchemy session, like the
auto-finish trigger tests, so a failed validation can prove that no clip row
was changed.
"""
import asyncio
import json
import pathlib
import sys
from types import SimpleNamespace

import pytest

pytest.importorskip("fastapi")
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import image_platform  # noqa: F401
import main
from models import Clip, ClipStatus, Job


HASH = "a" * 64


def _session():
    engine = create_engine("sqlite:///:memory:")
    Job.__table__.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def _clip(db, job_id, index, *, rejected=False):
    filename = f"clip-{index}.mp4"
    qc = {
        "version": 1, "checker": "v939", "scored_at": f"score-{index}",
        "operator_state_at_scoring": "pending_review", "selected_at_scoring": 1,
        "recommended_attempt": 1, "verdict": "PASS",
        "takes": [{"attempt": 1, "filename": filename, "file_sha256": HASH,
                   "line_variant": "A", "line": f"line {index}",
                   "verdict": "PASS", "hard": [], "warnings": []}],
    }
    row = Clip(
        job_id=job_id, clip_index=index, dialogue_id=f"d{index}",
        dialogue_text=f"line {index}", status=ClipStatus.COMPLETED.value,
        approval_status="rejected" if rejected else "pending_review",
        output_filename=filename, selected_variant=1,
        versions_json=json.dumps([{"attempt": 1, "filename": filename}]),
        qc_json=json.dumps(qc),
    )
    db.add(row)
    db.commit()
    return row.id, qc


def _setup():
    db = _session()
    job_id = "batch-job"
    db.add(Job(id=job_id, user_id="u1", config_json="{}", dialogue_json="[]",
               images_dir="", output_dir=""))
    db.commit()
    a = _clip(db, job_id, 0)
    b = _clip(db, job_id, 1)
    claims = [main.AutoApproveClipClaim(clip_id=a[0], scored_at=a[1]["scored_at"],
                                        selected_variant=1, filename="clip-0.mp4",
                                        file_sha256=HASH),
              main.AutoApproveClipClaim(clip_id=b[0], scored_at=b[1]["scored_at"],
                                        selected_variant=1, filename="clip-1.mp4",
                                        file_sha256=HASH)]
    return db, job_id, claims, (a[0], b[0])


def _call(db, job_id, claims):
    return asyncio.run(main.auto_approve_clips(
        job_id=job_id,
        request=main.AutoApproveClipsRequest(claims=claims),
        db=db,
        current_user=SimpleNamespace(id="u1"),
    ))


def test_batch_approves_two_clips_in_one_mutation():
    db, job_id, claims, ids = _setup()
    real_commit = db.commit
    commits = []
    def counted_commit():
        commits.append(1)
        return real_commit()
    db.commit = counted_commit
    result = _call(db, job_id, claims)
    assert result["approved_clip_ids"] == list(ids)
    assert {c.approval_status for c in db.query(Clip).all()} == {"approved"}
    assert len(commits) == 1


@pytest.mark.parametrize("mutation", [
    lambda claims: claims[:-1],
    lambda claims: [claims[0].model_copy(update={"scored_at": "old"}), claims[1]],
    lambda claims: [claims[0].model_copy(update={"selected_variant": 2}), claims[1]],
    lambda claims: [claims[0].model_copy(update={"filename": "other.mp4"}), claims[1]],
])
def test_batch_validation_conflicts_change_no_clip(mutation):
    db, job_id, claims, _ = _setup()
    with pytest.raises(main.HTTPException) as exc:
        _call(db, job_id, mutation(claims))
    assert exc.value.status_code == 409
    assert {c.approval_status for c in db.query(Clip).all()} == {"pending_review"}


def test_rejected_clip_conflicts_without_mutation():
    db, job_id, claims, _ = _setup()
    row = db.query(Clip).filter(Clip.clip_index == 1).one()
    row.approval_status = "rejected"
    db.commit()
    with pytest.raises(main.HTTPException) as exc:
        _call(db, job_id, claims)
    assert exc.value.status_code == 409
    assert db.query(Clip).filter(Clip.clip_index == 0).one().approval_status == "pending_review"


def test_failed_shared_decision_conflicts_without_mutation(monkeypatch):
    db, job_id, claims, _ = _setup()
    monkeypatch.setattr(main.clip_qc, "auto_approval_decision",
                        lambda clip: {"action": "review", "reason": "test failure"})
    with pytest.raises(main.HTTPException) as exc:
        _call(db, job_id, claims)
    assert exc.value.status_code == 409
    assert {c.approval_status for c in db.query(Clip).all()} == {"pending_review"}


@pytest.mark.parametrize("field,value", [
    ("selected_variant", None),
    ("scene_type", "text_card"),
])
def test_missing_selection_and_text_cards_conflict_without_mutation(field, value):
    db, job_id, claims, _ = _setup()
    row = db.query(Clip).filter(Clip.clip_index == 0).one()
    setattr(row, field, value)
    db.commit()
    with pytest.raises(main.HTTPException) as exc:
        _call(db, job_id, claims)
    assert exc.value.status_code == 409
    assert {c.approval_status for c in db.query(Clip).all()} == {"pending_review"}
