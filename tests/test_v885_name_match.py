"""v885 — resolve_job_by_filename resolves a folder file by the VIDEO NAME.

The operator names local-folder files after the build (the import batch title,
e.g. "nuri-korella-ed-carshow-...-v1"), not only with the minted
final_export_<ts>_<hash> token. Batch names are unique per user, so a filename
that token-contains one identifies its job — via
ImageJobBatch.promoted_video_job_id, against a REAL session (in-memory SQLite +
the real models), because the whole point is the DB lookup.

Pure containment rules (token-bounded, normalized) are tested here too, on
instagram_match directly.
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import main  # noqa: F401 — registers every model on Base so create_all resolves FKs
import instagram_match as m
from local_transcribe import resolve_job_by_filename
from models import Base, User, Job
from image_platform import ImageJobBatch

_NAME = "nuri-korella-ed-carshow-armgrab-two-men-jealousy-man64-healer-handoff-johnson-korella-saffron-selling-v1"
_JOB = "6e52de72-1111-4000-8000-000000000001"


def _db():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    db = sessionmaker(bind=engine)()
    db.add(User(id="u1", email="operator@example.com"))
    db.add(User(id="u2", email="other@example.com"))
    db.commit()
    return db


def _add_job(db, job_id=_JOB, user_id="u1", **kw):
    db.add(Job(
        id=job_id, user_id=user_id,
        config_json="{}", dialogue_json="[]", images_dir="/img", output_dir="/out",
        status="completed", **kw,
    ))
    db.commit()


def _add_batch(db, name, job_id, user_id="u1", batch_id=None):
    db.add(ImageJobBatch(
        id=batch_id or f"batch-{abs(hash((name, job_id))) % 10**8}",
        user_id=user_id, name=name, promoted_video_job_id=job_id,
    ))
    db.commit()


# ---------------------------------------------------------------- pure rules

def test_tokens_normalize_case_spaces_hyphens():
    assert m.name_tokens("Nuri KORELLA — ed_carshow-v1.mp4") == \
        ["nuri", "korella", "ed", "carshow", "v1", "mp4"]


def test_containment_survives_decoration_and_respacing():
    assert m.filename_contains_name(
        "Posted 0801 (2) - nuri korella ed carshow v1.mp.mp4",
        "nuri-korella-ed-carshow-v1",
    )


def test_v1_name_does_not_fire_on_a_v10_file():
    assert not m.filename_contains_name("nuri-korella-ed-carshow-v10.mp4",
                                        "nuri-korella-ed-carshow-v1")


def test_token_must_match_whole_not_substring():
    assert not m.filename_contains_name("nuri-korella-ed-carshowcase-v1.mp4",
                                        "nuri-korella-ed-carshow-v1")


def test_short_generic_name_never_claims_a_file():
    assert not m.filename_contains_name("my final.mp4", "final")
    assert not m.filename_contains_name("a b final.mp4", "a b")  # < 8 chars signal


def test_containment_survives_junk_input():
    assert not m.filename_contains_name(None, _NAME)
    assert not m.filename_contains_name("x.mp4", None)
    assert m.name_tokens(123) == []


# ------------------------------------------------------------- DB resolution

def test_file_named_with_the_video_name_resolves():
    db = _db()
    _add_job(db)
    _add_batch(db, _NAME, _JOB)
    job = resolve_job_by_filename(db, f"Posted- 0801 (3) - {_NAME}.mp4", "u1")
    assert job is not None and job.id == _JOB


def test_respaced_name_resolves_too():
    db = _db()
    _add_job(db)
    _add_batch(db, "nuri-korella-ed-carshow-armgrab-v1", _JOB)
    job = resolve_job_by_filename(
        db, "nuri korella ed carshow armgrab V1 (edit).mp4", "u1")
    assert job is not None and job.id == _JOB


def test_name_lookup_is_scoped_to_the_owner():
    db = _db()
    _add_job(db, user_id="u2")
    _add_batch(db, _NAME, _JOB, user_id="u2")
    assert resolve_job_by_filename(db, f"{_NAME}.mp4", "u1") is None


def test_longer_contained_name_beats_its_prefix_sibling():
    db = _db()
    _add_job(db, job_id="aaaaaaaa-1111-4000-8000-000000000001")
    _add_job(db, job_id="bbbbbbbb-1111-4000-8000-000000000002")
    _add_batch(db, "nuri-korella-ed-carshow", "aaaaaaaa-1111-4000-8000-000000000001")
    _add_batch(db, "nuri-korella-ed-carshow-final-cut", "bbbbbbbb-1111-4000-8000-000000000002")
    job = resolve_job_by_filename(db, "nuri-korella-ed-carshow-final-cut.mp4", "u1")
    assert job is not None and job.id == "bbbbbbbb-1111-4000-8000-000000000002"


def test_equal_length_double_match_is_ambiguous_none():
    db = _db()
    _add_job(db, job_id="aaaaaaaa-1111-4000-8000-000000000001")
    _add_job(db, job_id="bbbbbbbb-1111-4000-8000-000000000002")
    _add_batch(db, "carshow armgrab", "aaaaaaaa-1111-4000-8000-000000000001")
    _add_batch(db, "armgrab carshow", "bbbbbbbb-1111-4000-8000-000000000002")
    assert resolve_job_by_filename(
        db, "carshow armgrab carshow.mp4", "u1") is None


def test_unpromoted_batch_never_resolves():
    db = _db()
    _add_job(db)
    db.add(ImageJobBatch(id="batch-x", user_id="u1", name=_NAME,
                         promoted_video_job_id=None))
    db.commit()
    assert resolve_job_by_filename(db, f"{_NAME}.mp4", "u1") is None


def test_renamed_file_with_no_known_name_is_none():
    db = _db()
    _add_job(db)
    _add_batch(db, _NAME, _JOB)
    assert resolve_job_by_filename(db, "my saffron reel FINAL.mp4", "u1") is None


def test_export_stamp_still_wins_before_the_name():
    """Keys 1-3 run first: a minted token in the name beats the batch title."""
    db = _db()
    _add_job(db, export_basename="final_export_20260713_002341_026904")
    _add_job(db, job_id="bbbbbbbb-1111-4000-8000-000000000002")
    _add_batch(db, _NAME, "bbbbbbbb-1111-4000-8000-000000000002")
    job = resolve_job_by_filename(
        db, f"{_NAME} - final_export_20260713_002341_026904.mp4", "u1")
    assert job is not None and job.id == _JOB
