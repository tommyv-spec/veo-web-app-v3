"""v857.1 — the operator's pick MOVES the link, it does not clone it.

main.py match_video wrote `v.matched_job_id` / `job.instagram_video_id` and never
cleared the OTHER reel already holding that job. There is no unique constraint
(models.py), so two reels ended up pointing at one job — and that phantom holder
poisons find_job_incumbent (a `.first()` with no order_by picks whichever row the
DB hands back). The popover now tells the operator "already linked to X — picking
this moves the link". This is the test that it is true.

Run against a REAL session (in-memory SQLite + the real models), because the bug
was in the writes: a fake that ignores filters cannot prove a row was released.
"""
import asyncio
import math
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import audio_fingerprint as _afp
import main
from models import Base, User, Job, InstagramAccount, InstagramVideo

_JOB = "c9f0e6c9-1111-4000-8000-000000000001"


def _fp(seed, n=120, phase=0.0):
    env = [
        abs(math.sin((i + phase) * (0.07 + 0.013 * seed))
            + 0.35 * math.cos((i + phase) * 0.31 * (seed + 1)))
        for i in range(n)
    ]
    norm = math.sqrt(sum(x * x for x in env)) or 1.0
    return _afp.encode_fingerprint([x / norm for x in env])


def _seed(b_dur, b_fp):
    """Job J, held by reel A. Reel B is unlinked and about to be picked."""
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    db = sessionmaker(bind=engine)()

    user = User(id="u1", email="operator@example.com")
    job = Job(
        id=_JOB, user_id="u1", config_json="{}", dialogue_json="[]",
        images_dir="/img", output_dir="/out", status="completed",
        has_export=True, lifecycle_stage="published", published_via="ig_match",
        published_at=datetime.utcnow(), completed_at=datetime.utcnow(),
        instagram_url="https://instagram.com/reel/A", instagram_video_id=1,
    )
    acc = InstagramAccount(id=1, user_id="u1", handle="operator",
                           api_key_encrypted="x")
    reel_a = InstagramVideo(
        id=1, account_id=1, shortcode="REEL_A", url="https://instagram.com/reel/A",
        duration_s=17.274, audio_fp=_fp(4, phase=0.4), matched_job_id=_JOB,
        matched_at=datetime.utcnow(), match_source="evidence",
        transcription_status="done",
    )
    reel_b = InstagramVideo(
        id=2, account_id=1, shortcode="REEL_B", url="https://instagram.com/reel/B",
        duration_s=b_dur, audio_fp=b_fp, transcription_status="done",
    )
    db.add_all([user, job, acc, reel_a, reel_b])
    db.commit()
    return db, user, job, reel_a, reel_b


def _match(db, user, video_id, job_id):
    req = main.MatchInstagramVideoRequest(job_id=job_id)
    return asyncio.run(main.match_video(video_id, req, db=db, current_user=user))


def test_picking_a_different_reel_moves_the_link_off_the_previous_holder():
    """Reel B is a DIFFERENT video (0.637s away, another waveform). Picking it
    for job J must RELEASE reel A — one job, one video."""
    db, user, job, reel_a, reel_b = _seed(b_dur=16.637, b_fp=_fp(11))

    _match(db, user, 2, _JOB)

    db.refresh(reel_a)
    db.refresh(reel_b)
    db.refresh(job)
    assert reel_a.matched_job_id is None          # the previous holder let go
    assert reel_a.matched_at is None
    assert reel_a.match_source is None
    assert reel_b.matched_job_id == _JOB         # and the pick holds it
    assert job.instagram_video_id == reel_b.id
    assert job.instagram_url == reel_b.url
    # Exactly ONE reel points at the job — the phantom holder is gone.
    holders = db.query(InstagramVideo).filter(
        InstagramVideo.matched_job_id == _JOB).all()
    assert [v.shortcode for v in holders] == ["REEL_B"]


def test_the_operators_pick_is_stamped_manual_so_the_matcher_cannot_undo_it():
    db, user, job, reel_a, reel_b = _seed(b_dur=16.637, b_fp=_fp(11))
    _match(db, user, 2, _JOB)
    db.refresh(reel_b)
    assert reel_b.match_source == "manual"


def test_a_repost_keeps_both_reels_linked():
    """One export, posted twice. Reel B is the SAME video as reel A — both
    legitimately hold the job, so picking B must not unlink A."""
    db, user, job, reel_a, reel_b = _seed(b_dur=17.274, b_fp=_fp(4, phase=0.45))

    _match(db, user, 2, _JOB)

    db.refresh(reel_a)
    db.refresh(reel_b)
    db.refresh(job)
    assert reel_a.matched_job_id == _JOB         # left alone
    assert reel_b.matched_job_id == _JOB
    # Job.instagram_video_id holds ONE reel, so it names the most recent pick.
    assert job.instagram_video_id == reel_b.id


def test_the_publish_survives_the_move():
    """Releasing the old holder reverts the publish IT caused; writing the new
    link re-publishes in the same transaction. The job must not be left parked
    outside `published` after an operator repair."""
    db, user, job, reel_a, reel_b = _seed(b_dur=16.637, b_fp=_fp(11))

    _match(db, user, 2, _JOB)

    db.refresh(job)
    assert job.lifecycle_stage == "published"
    assert job.published_via == "ig_match"
    assert job.published_at is not None
