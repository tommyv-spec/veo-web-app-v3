"""v858 — resolve_job_by_filename resolves a legacy folder file by its export
basename, against a REAL session (in-memory SQLite + the real models), because
the whole point is a DB equality lookup on Job.export_basename.

The operator's real folder is full of pre-v856 files that have NO job id in the
name, only the export basename (final_export_<ts>_<hash>). We store that basename
on the Job row at mint + backfill, so those files resolve to their job by name.
"""
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import main  # noqa: F401 — registers every model on Base so create_all resolves FKs
from local_transcribe import resolve_job_by_filename
from models import Base, User, Job

_BASENAME = "final_export_20260713_002341_026904"
# The operator's actual file: label, odd spacing, doubled extension.
_REAL_NAME = "Posted- 0714 (6) -  final_export_20260713_002341_026904.mp.mp4"


def _db(export_basename=_BASENAME, user_id="u1"):
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    db = sessionmaker(bind=engine)()
    db.add(User(id="u1", email="operator@example.com"))
    db.add(Job(
        id="6e52de72-1111-4000-8000-000000000001", user_id=user_id,
        config_json="{}", dialogue_json="[]", images_dir="/img", output_dir="/out",
        status="completed", export_basename=export_basename,
    ))
    db.commit()
    return db


def test_legacy_filename_resolves_by_stored_basename():
    db = _db()
    job = resolve_job_by_filename(db, _REAL_NAME, "u1")
    assert job is not None
    assert job.id == "6e52de72-1111-4000-8000-000000000001"


def test_basename_lookup_is_scoped_to_the_owner():
    """A job belonging to somebody else must never be reachable by filename."""
    db = _db(user_id="someone-else")
    assert resolve_job_by_filename(db, _REAL_NAME, "u1") is None


def test_no_matching_basename_falls_through_to_none():
    db = _db(export_basename="final_export_20260101_000000_ffffff")
    assert resolve_job_by_filename(db, _REAL_NAME, "u1") is None


def test_renamed_file_with_no_token_is_none():
    db = _db()
    assert resolve_job_by_filename(db, "my saffron reel FINAL.mp4", "u1") is None
