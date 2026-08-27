# tests/test_update_finishing.py
#
# v947 — the import->promote path only carries `## Finishing` onto jobs that
# were promoted AFTER their build declared it. Every job promoted before that
# has no way to pick the declaration up short of a full re-import, which throws
# away the operator's approved clips. `POST /api/jobs/{id}/finishing` is the
# bridge: re-parse a build markdown and write the spec onto the existing job.
#
# Three things have to hold, and each is one test below:
#   1. A good section lands on the job as JSON the rest of v947 can read back.
#   2. A bad section 400s here exactly as it would die at import (fail-closed),
#      and leaves the stored spec ALONE — a typo must never silently wipe a
#      working declaration.
#   3. An ABSENT section CLEARS the stored spec. Re-import semantics: deleting
#      the section from the build has to mean "stop auto-finishing", not "keep
#      finishing the old way forever".
# Plus ownership, since this endpoint writes.

import asyncio
import json
import sys
import pathlib
import types

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import image_platform  # noqa: F401 — registers image_nodes for the FKs
from models import Job
import main


GOOD_MD = """# a build

## Finishing

- **captions:** none
- **auto_finish:** on
- **export_remove_silence:** true

## Storyboard
"""

BAD_MD = """# a build

## Finishing

- **captions:** none
- **export_zzz:** 1
"""

NO_SECTION_MD = "# a build with nothing to say about finishing\n\n### Scene 1\n"


def _session():
    eng = create_engine("sqlite:///:memory:")
    Job.__table__.metadata.create_all(eng)
    return sessionmaker(bind=eng)()


def _job(db, spec=None, job_id="job-fin-1", user_id="u1"):
    db.add(Job(
        id=job_id, user_id=user_id,
        config_json="{}", dialogue_json="[]", images_dir="", output_dir="",
        finishing_spec=json.dumps(spec) if spec is not None else None,
    ))
    db.commit()
    return job_id


def _update(db, job_id, markdown, user_id="u1"):
    return asyncio.run(main.update_job_finishing(
        job_id=job_id,
        req=main.FinishingUpdate(markdown=markdown),
        db=db,
        current_user=types.SimpleNamespace(id=user_id),
    ))


def _stored(db, job_id):
    return db.query(Job).filter(Job.id == job_id).one().finishing_spec


def test_valid_section_lands_on_the_job():
    db = _session()
    job_id = _job(db)

    resp = _update(db, job_id, GOOD_MD)

    stored = json.loads(_stored(db, job_id))
    assert stored["auto_finish"] == "on"
    assert stored["captions"] == "none"
    assert stored["export"] == {"remove_silence": True}
    # The response echoes exactly what was stored, so the CLI can print it
    # without a second round-trip.
    assert resp["job_id"] == job_id
    assert resp["finishing_spec"] == stored
    assert resp["finishing_spec"]["auto_finish"] == "on"


def test_bad_section_400s_and_leaves_the_stored_spec_alone():
    db = _session()
    job_id = _job(db, {"auto_finish": "on"})

    with pytest.raises(HTTPException) as exc:
        _update(db, job_id, BAD_MD)

    assert exc.value.status_code == 400
    assert "export_zzz" in str(exc.value.detail)
    # Fail-closed means the OLD declaration survives an attempted bad push.
    assert json.loads(_stored(db, job_id)) == {"auto_finish": "on"}


def test_absent_section_clears_a_previously_set_spec():
    db = _session()
    job_id = _job(db, {"auto_finish": "on", "captions": "none"})

    resp = _update(db, job_id, NO_SECTION_MD)

    assert _stored(db, job_id) is None
    assert resp["finishing_spec"] is None


def test_another_users_job_is_refused():
    db = _session()
    job_id = _job(db, {"auto_finish": "on"}, user_id="someone-else")

    with pytest.raises(HTTPException) as exc:
        _update(db, job_id, GOOD_MD, user_id="u1")

    # get_user_job: 404 when the job does not exist, 403 when it is not yours.
    assert exc.value.status_code == 403
    assert json.loads(_stored(db, job_id)) == {"auto_finish": "on"}
