"""Render an exact SUBSET of a job's clips on FIRST generation.

The whole point of this feature is that it CANNOT touch a clip it was not
given, and CANNOT end the job it only saw four clips of. So these tests are
behavioural wherever they can be: the server ones run the real endpoints
against a real (in-memory sqlite) database, and the release ones actually kill
a process four different ways and read back what it sent.

Spec: docs/superpowers/plans/2026-09-11-exact-clip-first-generation.md
Numbered comments map each test back to the spec's §5 list.
"""

import ast
import asyncio
import json
import os
import subprocess
import sys
import textwrap
from datetime import datetime, timedelta
from pathlib import Path

import pytest

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "static" / "flow_worker.py"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import image_platform  # noqa: E402,F401 — registers image_nodes so sqlite can build the FKs
import models  # noqa: E402
import main  # noqa: E402

from sqlalchemy import create_engine, event  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402


# ──────────────────────────────────────────────────────────────────────────
# database fixture — the real models, on real sqlite
# ──────────────────────────────────────────────────────────────────────────

USER = "user-under-test"


@pytest.fixture()
def engine():
    eng = create_engine("sqlite://")
    models.Base.metadata.create_all(eng)
    return eng


@pytest.fixture()
def db(engine):
    session = sessionmaker(bind=engine)()
    try:
        yield session
    finally:
        session.close()


def make_job(db, job_id="job-under-test", n_clips=5, status="pending",
             user_id=USER, claimed_by=None):
    now = datetime.utcnow()
    db.add(models.Job(
        id=job_id, user_id=user_id, backend="flow", status=status,
        config_json="{}", dialogue_json="{}", api_keys_json="{}",
        images_dir="images", output_dir="output",
        total_clips=n_clips, completed_clips=0,
        claimed_by_worker=claimed_by,
        claimed_at=now if claimed_by else None,
        created_at=now, updated_at=now,
    ))
    clips = []
    for i in range(n_clips):
        c = models.Clip(job_id=job_id, clip_index=i, dialogue_id=f"d{i}",
                        dialogue_text=f"line {i}", status="pending",
                        prompt_text=f"prompt {i}")
        db.add(c)
        clips.append(c)
    db.commit()
    return [c.id for c in clips]


class FakeRequest:
    base_url = "https://test.invalid/"


LANES = ["local-worker", "user-worker"]


def call_pending(lane, db, worker_id="worker-A", clip_ids=None, exclude=None):
    """Run the real pending endpoint for one lane."""
    if lane == "local-worker":
        coro = main.local_worker_get_pending_job(
            request=FakeRequest(), worker_id=worker_id, exclude=exclude,
            arms=None, clip_ids=clip_ids, db=db, authorized=True)
    else:
        coro = main.user_worker_get_pending_job(
            request=FakeRequest(), worker_id=worker_id, exclude=exclude,
            arms=None, clip_ids=clip_ids, db=db, user_id=USER)
    return asyncio.run(coro)


def call_release(lane, db, job_id, worker_id):
    payload = main.ScopedReleaseRequest(worker_id=worker_id)
    if lane == "local-worker":
        coro = main.local_worker_scoped_release_job(
            job_id=job_id, payload=payload, db=db, authorized=True)
    else:
        coro = main.user_worker_scoped_release_job(
            job_id=job_id, payload=payload, db=db, user_id=USER)
    return asyncio.run(coro)


# ──────────────────────────────────────────────────────────────────────────
# spec test 1 + 8 + 9 — the subset, and only the subset, on BOTH routes
# ──────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("lane", LANES)
def test_scoped_poll_returns_only_the_allowlisted_clips(lane, db):
    ids = make_job(db, n_clips=5)
    wanted = [ids[1], ids[3]]

    result = call_pending(lane, db, clip_ids=",".join(map(str, wanted)))

    job = result["job"]
    assert job is not None, result
    assert [c["id"] for c in job["clips"]] == wanted
    # spec test 9 — the count is the JOB's, not the filtered array's
    assert job["authoritative_total_clips"] == 5
    assert job["scope_firstgen"] is True
    assert job["scoped_clip_ids"] == wanted

    # the untouched clips are still exactly as they were
    others = db.query(models.Clip).filter(
        models.Clip.job_id == "job-under-test",
        ~models.Clip.id.in_(wanted)).all()
    assert len(others) == 3
    assert all(c.status == "pending" for c in others)
    assert all(c.claimed_by_worker is None for c in others)


@pytest.mark.parametrize("lane", LANES)
def test_scoped_poll_claims_the_job_the_clips_belong_to(lane, db):
    ids = make_job(db, n_clips=5)
    call_pending(lane, db, clip_ids=str(ids[0]))
    job = db.query(models.Job).filter(models.Job.id == "job-under-test").first()
    assert job.claimed_by_worker == "worker-A"
    assert job.claimed_at is not None


@pytest.mark.parametrize("lane", LANES)
def test_scoped_poll_never_takes_the_queue_head(lane, db):
    """The job is an OUTPUT of the clip list. A much older job sits at the head
    of the queue; asking for a clip of the newer job must not hand it over."""
    make_job(db, job_id="head-of-queue", n_clips=3)
    head = db.query(models.Job).filter(models.Job.id == "head-of-queue").first()
    head.created_at = datetime.utcnow() - timedelta(hours=2)
    db.commit()
    ids = make_job(db, job_id="job-under-test", n_clips=4)

    result = call_pending(lane, db, clip_ids=str(ids[2]))
    assert result["job"]["id"] == "job-under-test"
    assert [c["id"] for c in result["job"]["clips"]] == [ids[2]]
    assert db.query(models.Job).filter(
        models.Job.id == "head-of-queue").first().claimed_by_worker is None


# ──────────────────────────────────────────────────────────────────────────
# spec tests 3, 5, 6 — named failures, no queue-head fallback, fail CLOSED
# ──────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("lane", LANES)
def test_unknown_clip_id_is_refused_before_any_claim(lane, db):
    ids = make_job(db, n_clips=3)
    result = call_pending(lane, db, clip_ids=f"{ids[0]},999999")

    assert result["job"] is None
    assert result["scope_error"] == "UNKNOWN_CLIP_ID"
    assert result["scope_error_detail"]["missing_clip_ids"] == [999999]
    # nothing was claimed on the way to the refusal
    assert db.query(models.Job).filter(
        models.Job.id == "job-under-test").first().claimed_by_worker is None


@pytest.mark.parametrize("lane", LANES)
def test_clips_spanning_two_jobs_are_refused_naming_both(lane, db):
    a = make_job(db, job_id="job-aaa", n_clips=2)
    b = make_job(db, job_id="job-bbb", n_clips=2)

    result = call_pending(lane, db, clip_ids=f"{a[0]},{b[0]}")

    assert result["job"] is None
    assert result["scope_error"] == "CLIPS_SPAN_MULTIPLE_JOBS"
    assert sorted(result["scope_error_detail"]["job_ids"]) == ["job-aaa", "job-bbb"]
    for jid in ("job-aaa", "job-bbb"):
        assert db.query(models.Job).filter(
            models.Job.id == jid).first().claimed_by_worker is None


@pytest.mark.parametrize("lane", LANES)
def test_job_already_claimed_by_another_worker_is_refused(lane, db):
    ids = make_job(db, n_clips=3, claimed_by="some-other-worker")
    result = call_pending(lane, db, clip_ids=str(ids[0]))

    assert result["job"] is None
    assert result["scope_error"] == "JOB_NOT_CLAIMABLE"
    assert db.query(models.Job).filter(
        models.Job.id == "job-under-test").first().claimed_by_worker == "some-other-worker"


@pytest.mark.parametrize("lane", LANES)
def test_job_in_a_non_claimable_status_is_refused(lane, db):
    ids = make_job(db, n_clips=3, status="processing")
    result = call_pending(lane, db, clip_ids=str(ids[0]))
    assert result["job"] is None
    assert result["scope_error"] == "JOB_NOT_CLAIMABLE"
    assert result["scope_error_detail"]["job_status"] == "processing"


@pytest.mark.parametrize("lane", LANES)
def test_a_scoped_claim_lost_to_a_racing_worker_returns_CLAIM_RACE_LOST(lane, engine, db):
    """A rival claims the row between the eligibility read and the conditional
    UPDATE. Injected through a real SQL hook on the same connection, so the
    conditional update genuinely matches zero rows."""
    ids = make_job(db, n_clips=3)
    fired = {"n": 0}

    @event.listens_for(engine, "before_cursor_execute")
    def steal(conn, cursor, statement, parameters, context, executemany):
        s = " ".join(statement.split()).lower()
        if s.startswith("update jobs set") and "claimed_by_worker" in s and not fired["n"]:
            fired["n"] = 1
            cursor.execute(
                "UPDATE jobs SET claimed_by_worker='rival-worker' WHERE id='job-under-test'")

    try:
        result = call_pending(lane, db, clip_ids=str(ids[0]))
    finally:
        event.remove(engine, "before_cursor_execute", steal)

    assert fired["n"] == 1, "the race was never injected — the test proves nothing"
    assert result["job"] is None
    assert result["scope_error"] == "CLAIM_RACE_LOST"


@pytest.mark.parametrize("lane", LANES)
def test_a_scoped_claim_with_no_worker_id_is_refused(lane, db):
    ids = make_job(db, n_clips=3)
    result = call_pending(lane, db, worker_id=None, clip_ids=str(ids[0]))
    assert result["job"] is None
    assert result["scope_error"] == "SCOPED_CLAIM_REQUIRES_WORKER_ID"


@pytest.mark.parametrize("lane", LANES)
def test_malformed_clip_ids_are_rejected_not_widened(lane, db):
    make_job(db, n_clips=3)
    with pytest.raises(main.HTTPException) as exc:
        call_pending(lane, db, clip_ids="12,not-a-number")
    assert exc.value.status_code == 422


# ──────────────────────────────────────────────────────────────────────────
# spec test 7 — the subset finishing does NOT end the job
# ──────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("lane", LANES)
def test_finishing_the_subset_leaves_the_other_clips_and_the_job_alone(lane, db):
    ids = make_job(db, n_clips=10)
    wanted = [ids[0], ids[1], ids[2], ids[3]]

    result = call_pending(lane, db, clip_ids=",".join(map(str, wanted)))
    assert result["job"]["authoritative_total_clips"] == 10

    # the four finish
    for cid in wanted:
        db.query(models.Clip).filter(models.Clip.id == cid).update({"status": "completed"})
    db.commit()

    # the run ends and hands the parent back
    released = call_release(lane, db, "job-under-test", "worker-A")
    assert released["released"] is True

    job = db.query(models.Job).filter(models.Job.id == "job-under-test").first()
    db.refresh(job)
    assert job.status == "pending", "the parent must be claimable again"
    assert job.claimed_by_worker is None
    assert job.claimed_at is None

    others = db.query(models.Clip).filter(
        models.Clip.job_id == "job-under-test",
        ~models.Clip.id.in_(wanted)).all()
    assert len(others) == 6
    assert all(c.status == "pending" for c in others), \
        "the clips this run never saw kept their statuses"

    # and the job really is claimable: an ordinary unscoped poll picks it up
    fresh = call_pending(lane, db, worker_id="worker-B")
    assert fresh["job"]["id"] == "job-under-test"
    assert len(fresh["job"]["clips"]) == 10


# ──────────────────────────────────────────────────────────────────────────
# spec test 10 (server half) — release is conditional on the claim being ours
# ──────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("lane", LANES)
def test_a_claim_race_loser_cannot_release_the_winners_job(lane, db):
    ids = make_job(db, n_clips=4)
    call_pending(lane, db, worker_id="winner", clip_ids=str(ids[0]))

    answer = call_release(lane, db, "job-under-test", "loser")

    assert answer["released"] is False
    assert answer["reason"] == "CLAIM_NOT_OURS"
    job = db.query(models.Job).filter(models.Job.id == "job-under-test").first()
    db.refresh(job)
    assert job.claimed_by_worker == "winner"


@pytest.mark.parametrize("lane", LANES)
def test_release_works_from_processing_which_is_where_a_live_run_sits(lane, db):
    ids = make_job(db, n_clips=4)
    call_pending(lane, db, worker_id="worker-A", clip_ids=str(ids[0]))
    # the worker marks the job processing the moment it starts it
    db.query(models.Job).filter(models.Job.id == "job-under-test").update(
        {"status": "processing"})
    db.commit()

    assert call_release(lane, db, "job-under-test", "worker-A")["released"] is True
    job = db.query(models.Job).filter(models.Job.id == "job-under-test").first()
    db.refresh(job)
    assert job.status == "pending"
    assert job.claimed_by_worker is None


@pytest.mark.parametrize("lane", LANES)
def test_release_with_no_worker_id_changes_nothing(lane, db):
    make_job(db, n_clips=2)
    answer = call_release(lane, db, "job-under-test", None)
    assert answer["released"] is False
    job = db.query(models.Job).filter(models.Job.id == "job-under-test").first()
    assert job.status == "pending"


def test_user_lane_release_cannot_reach_another_users_job(db):
    make_job(db, n_clips=2, user_id="somebody-else", claimed_by="worker-A")
    answer = call_release("user-worker", db, "job-under-test", "worker-A")
    assert answer["released"] is False
    job = db.query(models.Job).filter(models.Job.id == "job-under-test").first()
    assert job.claimed_by_worker == "worker-A"


def test_user_lane_scoped_claim_cannot_reach_another_users_clips(db):
    ids = make_job(db, n_clips=3, user_id="somebody-else")
    result = call_pending("user-worker", db, clip_ids=str(ids[0]))
    assert result["job"] is None
    assert result["scope_error"] == "UNKNOWN_CLIP_ID"


# ──────────────────────────────────────────────────────────────────────────
# the unscoped poll is untouched (server half of spec test 4)
# ──────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("lane", LANES)
def test_an_unscoped_poll_still_returns_the_whole_job(lane, db, monkeypatch):
    make_job(db, n_clips=6)

    def explode(*a, **k):
        raise AssertionError("the scoped path ran for a poll with no clip_ids")

    monkeypatch.setattr(main, "_scoped_firstgen_claim", explode)
    result = call_pending(lane, db)

    assert result["job"]["id"] == "job-under-test"
    assert len(result["job"]["clips"]) == 6
    assert result["job"]["authoritative_total_clips"] == 6
    assert result["job"]["scope_firstgen"] is False
    assert result["job"]["scoped_clip_ids"] is None


@pytest.mark.parametrize("lane", LANES)
def test_the_look_without_claiming_branch_still_works(lane, db):
    """FLOW_ONLY_JOB_IDS walks the queue with no worker_id; that branch sits in
    the same if/elif chain the scoped one was added to."""
    make_job(db, n_clips=3)
    result = call_pending(lane, db, worker_id=None)
    assert result["job"]["id"] == "job-under-test"
    assert db.query(models.Job).filter(
        models.Job.id == "job-under-test").first().claimed_by_worker is None


# ──────────────────────────────────────────────────────────────────────────
# both routes really are mirrored
# ──────────────────────────────────────────────────────────────────────────

def test_both_pending_routes_call_the_same_two_scope_helpers():
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    local = source.split("async def local_worker_get_pending_job(", 1)[1].split(
        "\ndef _parse_worker_clip_ids", 1)[0]
    user = source.split("async def user_worker_get_pending_job(", 1)[1].split(
        '\n@app.get("/api/user-worker/clips/redo-pending")', 1)[0]
    for endpoint in (local, user):
        assert "allowed_clip_ids = _parse_worker_clip_ids(clip_ids)" in endpoint
        assert "_scoped_firstgen_claim(" in endpoint
        assert "return _scope_err" in endpoint
        assert '"authoritative_total_clips": _scope_total' in endpoint
        # the scoped resolution happens BEFORE the queue is ever claimed from
        assert endpoint.index("_scoped_firstgen_claim(") < endpoint.index(
            "job.claimed_by_worker = worker_id")
        # and the queue branch is skipped outright on a scoped poll
        assert "if allowed_clip_ids is not None:\n        pass" in endpoint


def test_both_lanes_expose_a_scoped_release_route():
    source = (ROOT / "main.py").read_text(encoding="utf-8")
    assert '@app.post("/api/local-worker/jobs/{job_id}/scoped-release")' in source
    assert '@app.post("/api/user-worker/jobs/{job_id}/scoped-release")' in source


# ──────────────────────────────────────────────────────────────────────────
# worker side
# ──────────────────────────────────────────────────────────────────────────

def _worker_source():
    return WORKER.read_text(encoding="utf-8")


def _worker_ns(names, **globals_dict):
    """Compile the named top-level worker functions into one namespace."""
    tree = ast.parse(_worker_source())
    wanted = [n for n in tree.body
              if isinstance(n, ast.FunctionDef) and n.name in set(names)]
    missing = set(names) - {n.name for n in wanted}
    assert not missing, f"function(s) not found in flow_worker.py: {missing}"
    ns = dict(globals_dict)
    exec(compile(ast.Module(body=wanted, type_ignores=[]), str(WORKER), "exec"), ns)
    return ns


# --- the new opt-in itself ---------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    (None, False), ("", False), ("0", False), ("false", False), ("OFF", False),
    ("1", True), ("true", True), ("Yes", True), ("on", True),
])
def test_firstgen_flag_parses(raw, expected):
    parse = _worker_ns(["_parse_flow_clip_scope_firstgen"])["_parse_flow_clip_scope_firstgen"]
    assert parse(raw, (1, 2)) is expected


@pytest.mark.parametrize("raw", ["maybe", "2", "y e s"])
def test_firstgen_flag_fails_closed_on_junk(raw):
    parse = _worker_ns(["_parse_flow_clip_scope_firstgen"])["_parse_flow_clip_scope_firstgen"]
    with pytest.raises(RuntimeError):
        parse(raw, (1, 2))


def test_firstgen_without_an_allowlist_is_refused():
    """Otherwise it is just an unscoped worker with an extra env var set."""
    parse = _worker_ns(["_parse_flow_clip_scope_firstgen"])["_parse_flow_clip_scope_firstgen"]
    with pytest.raises(RuntimeError):
        parse("1", ())


# --- spec test 4: firstgen OFF behaves exactly as today ----------------------

def test_firstgen_off_leaves_get_pending_job_returning_None():
    calls = []

    def api_request(method, url, data=None):
        calls.append(url)
        raise AssertionError("a clip-scoped run with firstgen OFF must not poll for jobs")

    ns = _worker_ns(
        ["get_pending_job"],
        FLOW_RESUME_JOB_ID=None,
        FLOW_ONLY_CLIP_IDS=(14908, 14909),
        FLOW_CLIP_SCOPE_FIRSTGEN=False,
        FLOW_ONLY_JOB_IDS=frozenset(),
        api_request=api_request,
        WORKER_ID="w",
        _worker_arms_q=lambda: "arms=x",
        _get_scoped_firstgen_job=lambda **k: pytest.fail(
            "the firstgen path ran with FLOW_CLIP_SCOPE_FIRSTGEN off"),
    )
    assert ns["get_pending_job"]() is None
    assert calls == []


def test_firstgen_off_is_the_default_in_the_shipped_module(monkeypatch):
    """No env var → the flag is False, so every existing invocation is unchanged."""
    parse = _worker_ns(["_parse_flow_clip_scope_firstgen"])["_parse_flow_clip_scope_firstgen"]
    monkeypatch.delenv("FLOW_CLIP_SCOPE_FIRSTGEN", raising=False)
    assert parse(os.environ.get("FLOW_CLIP_SCOPE_FIRSTGEN"), (14908,)) is False


def test_the_redo_path_still_carries_its_own_server_filter():
    """Spec test 4's other half: the redo scoping is untouched."""
    redo = _worker_source().split("def get_redo_clips(", 1)[1].split(
        "\ndef clip_done_in_platform", 1)[0]
    assert "clip_ids={_url_quote(_flow_only_clip_ids_q())}" in redo
    assert 'clip.get("id") in allowed' in redo
    assert "BLOCKED" in redo


# --- the scoped claim, worker side ------------------------------------------

def _firstgen_ns(response, clip_ids=(11, 12), released=None):
    """A namespace whose api_request answers the scoped poll with `response`."""
    released = released if released is not None else []

    def api_request(method, url, data=None):
        if method == "POST" and "scoped-release" in url:
            released.append(url)
            return {"released": True}
        return response

    ns = _worker_ns(
        ["_get_scoped_firstgen_job", "_scoped_firstgen_note_claim",
         "_scoped_firstgen_release_parent_jobs"],
        FLOW_ONLY_CLIP_IDS=tuple(clip_ids),
        WORKER_ID="worker-A",
        api_request=api_request,
        is_job_aborted=lambda _j: True,
        mark_job_aborted=lambda _j: None,
        _worker_arms_q=lambda: "arms=veo",
        _url_quote=lambda s: s,
        _flow_only_clip_ids_q=lambda: ",".join(map(str, clip_ids)),
        _SCOPED_FIRSTGEN_JOB_TAKEN=[None],
        _SCOPED_FIRSTGEN_CLAIMED_JOBS=set(),
        _SCOPED_FIRSTGEN_RELEASE_LOCK=__import__("threading").Lock(),
        _SCOPED_FIRSTGEN_FATAL_ERRORS=(
            "UNKNOWN_CLIP_ID", "CLIPS_SPAN_MULTIPLE_JOBS",
            "SCOPED_CLAIM_REQUIRES_WORKER_ID", "SCOPE_FILTER_FAILED"),
        _SCOPED_FIRSTGEN_UNCLAIMABLE_LIMIT=3,
        _scoped_firstgen_unclaimable_polls=[0],
    )
    ns["_released"] = released
    return ns


def _ok_response(clip_ids=(11, 12), total=80, extra_clips=()):
    clips = [{"id": cid, "clip_index": i} for i, cid in enumerate(clip_ids)]
    clips.extend(extra_clips)
    return {"job": {"id": "0d456c24-aaaa", "clips": clips,
                    "authoritative_total_clips": total},
            "aborted_jobs": []}


def test_scoped_firstgen_job_keeps_only_the_allowlist_and_the_real_total():
    ns = _firstgen_ns(_ok_response())
    job = ns["_get_scoped_firstgen_job"]()
    assert [c["id"] for c in job["clips"]] == [11, 12]
    assert job["_authoritative_total_clips"] == 80
    assert job["_scope_firstgen"] is True


# --- spec test 2: a server-side regression is BLOCKED and logged -------------

def test_an_unlisted_clip_from_the_server_is_blocked_and_logged(capsys):
    ns = _firstgen_ns(_ok_response(extra_clips=[{"id": 14907, "clip_index": 9}]))
    job = ns["_get_scoped_firstgen_job"]()
    assert [c["id"] for c in job["clips"]] == [11, 12]
    out = capsys.readouterr().out
    assert "BLOCKED 1 unlisted clip(s)" in out
    assert "14907" in out


# --- spec test 6: the filter failing refuses the job, it does not run --------

@pytest.mark.parametrize("response", [
    {"job": {"id": "j", "clips": [], "authoritative_total_clips": 80}},
    {"job": {"id": "j", "clips": "not a list", "authoritative_total_clips": 80}},
    {"job": {"id": "j", "clips": [{"clip_index": 0}], "authoritative_total_clips": 80}},
    {"job": {"id": "j", "clips": [{"id": 11, "clip_index": 0}],
             "authoritative_total_clips": 80}},          # only one of the two
    {"job": {"id": "j", "clips": [{"id": 11}, {"id": 12}]}},   # no total at all
    {"job": {"id": "j", "clips": [{"id": 11}, {"id": 12}],
             "authoritative_total_clips": 1}},           # total smaller than the subset
    {"job": {"id": "j", "clips": [{"id": 11}, {"id": 12}],
             "authoritative_total_clips": "eighty"}},
])
def test_a_filter_that_cannot_be_applied_refuses_and_releases_the_job(response):
    released = []
    ns = _firstgen_ns(response, released=released)
    with pytest.raises(SystemExit) as exc:
        ns["_get_scoped_firstgen_job"]()
    assert exc.value.code == 1
    assert released, "a refused job must be handed back, not silently held"


@pytest.mark.parametrize("code", [
    "UNKNOWN_CLIP_ID", "CLIPS_SPAN_MULTIPLE_JOBS",
    "SCOPED_CLAIM_REQUIRES_WORKER_ID", "SCOPE_FILTER_FAILED"])
def test_an_unsatisfiable_scope_stops_the_run(code):
    ns = _firstgen_ns({"job": None, "scope_error": code,
                       "scope_error_message": "no", "aborted_jobs": []})
    with pytest.raises(SystemExit):
        ns["_get_scoped_firstgen_job"]()


def test_a_transient_refusal_waits_then_gives_up_bounded():
    ns = _firstgen_ns({"job": None, "scope_error": "JOB_NOT_CLAIMABLE",
                       "scope_error_message": "busy", "aborted_jobs": []})
    assert ns["_get_scoped_firstgen_job"]() is None
    assert ns["_get_scoped_firstgen_job"]() is None
    with pytest.raises(SystemExit):
        ns["_get_scoped_firstgen_job"]()      # limit is 3 in this namespace


def test_a_network_failure_is_not_treated_as_a_job():
    ns = _firstgen_ns(None)
    assert ns["_get_scoped_firstgen_job"]() is None


def test_the_scoped_run_claims_one_job_once():
    ns = _firstgen_ns(_ok_response())
    assert ns["_get_scoped_firstgen_job"]()["id"] == "0d456c24-aaaa"
    assert ns["_get_scoped_firstgen_job"]() is None, \
        "a scoped run must not re-claim and re-submit the same job in a loop"


def test_the_surviving_clip_ids_are_printed_before_any_render(capsys):
    ns = _firstgen_ns(_ok_response())
    ns["_get_scoped_firstgen_job"]()
    out = capsys.readouterr().out
    assert "rendering clip ids [11, 12]" in out
    assert "of 80 clip(s) in the job" in out
    assert "the other 78 are not touched" in out


# --- spec test 7 (worker half): the job can never be marked completed --------

def test_update_job_status_refuses_completed_on_a_scoped_run():
    sent = []
    ns = _worker_ns(
        ["update_job_status"],
        _scoped_firstgen_active=lambda: True,
        FLOW_ONLY_CLIP_IDS=(11, 12, 13, 14),
        api_request_ex=lambda m, e, d=None: (sent.append((m, e, d)), ({"ok": True}, 200))[1],
        mark_job_aborted=lambda _j: None,
        time=__import__("time"),
    )
    assert ns["update_job_status"]("0d456c24-aaaa", "completed") is None
    assert sent == [], "not one request may leave for a 'completed' under a scope"


def test_update_job_status_still_sends_every_other_status_on_a_scoped_run():
    sent = []
    ns = _worker_ns(
        ["update_job_status"],
        _scoped_firstgen_active=lambda: True,
        FLOW_ONLY_CLIP_IDS=(11, 12),
        api_request_ex=lambda m, e, d=None: (sent.append((m, e, d)), ({"ok": True}, 200))[1],
        mark_job_aborted=lambda _j: None,
        time=__import__("time"),
    )
    assert ns["update_job_status"]("job-1", "processing") == {"ok": True}
    assert len(sent) == 1


def test_update_job_status_completes_normally_when_no_scope_is_active():
    sent = []
    ns = _worker_ns(
        ["update_job_status"],
        _scoped_firstgen_active=lambda: False,
        FLOW_ONLY_CLIP_IDS=(),
        api_request_ex=lambda m, e, d=None: (sent.append((m, e, d)), ({"ok": True}, 200))[1],
        mark_job_aborted=lambda _j: None,
        time=__import__("time"),
    )
    assert ns["update_job_status"]("job-1", "completed") == {"ok": True}
    assert sent[0][2]["status"] == "completed"


def test_the_local_cache_is_not_marked_completed_either():
    """The main loop reads this cache and SKIPS a job it calls completed, so a
    four-clip proof run writing it would strand the rest on this machine."""
    ns = _worker_ns(
        ["mark_job_completed"],
        _scoped_firstgen_active=lambda: True,
        FLOW_ONLY_CLIP_IDS=(11, 12),
        save_cache=lambda _c: pytest.fail("the cache must not be written"),
        datetime=datetime,
    )
    cache = {"jobs": {"job-1": {"status": "processing"}}}
    ns["mark_job_completed"](cache, "job-1")
    assert cache["jobs"]["job-1"]["status"] == "processing"


# --- the authoritative count is what the completion arithmetic reads ---------

def test_the_authoritative_total_beats_the_filtered_length():
    total = _worker_ns(["_job_authoritative_total_clips"])["_job_authoritative_total_clips"]
    four = [{"id": i} for i in range(4)]
    assert total({"_authoritative_total_clips": 80}, four) == 80
    assert total({"authoritative_total_clips": 80}, four) == 80
    assert total({}, four) == 4, "with no scope the job's own list is the total"
    assert total({"authoritative_total_clips": None}, four) == 4
    assert total({"authoritative_total_clips": "x"}, four) == 4


def test_the_parallel_completion_check_uses_the_authoritative_total():
    source = _worker_source()
    block = source.split("completed = sum(1 for c in all_clips", 1)[1][:600]
    assert "_job_authoritative_total_clips(job, clips)" in block
    assert "completed >= _total" in block
    assert "completed >= len(clips)" not in source


# ──────────────────────────────────────────────────────────────────────────
# spec test 10 (worker half) — all four exit paths really release
# ──────────────────────────────────────────────────────────────────────────

_RELEASE_FUNCS = [
    "_scoped_firstgen_note_claim",
    "_scoped_firstgen_release_parent_jobs",
    "_scoped_firstgen_signal_handler",
    "_install_scoped_firstgen_release_hooks",
]


def _release_harness(record_path, ending):
    """A runnable script carrying the REAL release functions from the worker."""
    tree = ast.parse(_worker_source())
    bodies = []
    for name in _RELEASE_FUNCS:
        node = next(n for n in tree.body
                    if isinstance(n, ast.FunctionDef) and n.name == name)
        bodies.append(ast.get_source_segment(_worker_source(), node))
    return textwrap.dedent(f"""
        import json, signal, sys, threading
        WORKER_ID = "worker-A"
        RECORD = r"{record_path}"

        def api_request(method, endpoint, data=None):
            with open(RECORD, "a", encoding="utf-8") as fh:
                fh.write(json.dumps({{"method": method, "endpoint": endpoint,
                                      "data": data}}) + "\\n")
            return {{"released": True}}

        _SCOPED_FIRSTGEN_CLAIMED_JOBS = set()
        _SCOPED_FIRSTGEN_RELEASE_LOCK = threading.Lock()

    """) + "\n\n".join(bodies) + textwrap.dedent(f"""

        _install_scoped_firstgen_release_hooks()
        _scoped_firstgen_note_claim("0d456c24-aaaa")
        {ending}
    """)


@pytest.mark.parametrize("path,ending,expect_code", [
    # the subset finished — the run exits cleanly, exactly like the scoped
    # auto-exit in get_redo_clips does
    ("success", "raise SystemExit(0)", 0),
    # a clip failed and ended the run
    ("clip-failure", "sys.exit(1)", 1),
    # an unhandled worker exception
    ("worker-exception", "raise RuntimeError('boom')", 1),
    # Ctrl+C
    ("sigint", "signal.raise_signal(signal.SIGINT)", 128 + 2),
])
def test_the_parent_job_is_released_on_every_terminal_and_abort_path(
        tmp_path, path, ending, expect_code):
    record = tmp_path / f"{path}.jsonl"
    script = tmp_path / f"{path}.py"
    script.write_text(_release_harness(str(record).replace("\\", "\\\\"), ending),
                      encoding="utf-8")

    proc = subprocess.run([sys.executable, str(script)], capture_output=True,
                          text=True, encoding="utf-8", errors="replace", timeout=120)

    assert record.exists(), (f"nothing was released on the {path} path\n"
                             f"stdout: {proc.stdout}\nstderr: {proc.stderr}")
    calls = [json.loads(line) for line in record.read_text(encoding="utf-8").splitlines()]
    assert len(calls) == 1, f"release must be idempotent, got {calls}"
    assert calls[0]["method"] == "POST"
    assert calls[0]["endpoint"] == "/jobs/0d456c24-aaaa/scoped-release"
    assert calls[0]["data"] == {"worker_id": "worker-A"}
    assert proc.returncode == expect_code, proc.stderr


def test_release_hooks_are_only_installed_for_a_scoped_firstgen_run():
    source = _worker_source()
    tail = source.split("def _install_scoped_firstgen_release_hooks(", 1)[1]
    assert "if FLOW_CLIP_SCOPE_FIRSTGEN:\n    _install_scoped_firstgen_release_hooks()" in tail
