"""v850 — durable export queue. Pure state helpers + main.py wiring.

main.py is too heavy to import-and-introspect reliably in a unit test, and this
repo has been bitten by missing-name regressions py_compile does not catch — so
the wiring half asserts on the SOURCE (same approach as
tests/test_local_watch_never_miss.py).
"""
import os
import re
from datetime import datetime, timedelta

import export_queue as eq

_HERE = os.path.dirname(os.path.abspath(__file__))
_CODE = os.path.dirname(_HERE)
_MAIN = os.path.join(_CODE, "main.py")
_INDEX = os.path.join(_CODE, "static", "index.html")


def _main_src():
    return open(_MAIN, encoding="utf-8").read()


def _index_src():
    return open(_INDEX, encoding="utf-8").read()


def test_running_with_fresh_heartbeat_is_not_stale():
    now = datetime(2026, 7, 12, 12, 0, 0)
    hb = now - timedelta(seconds=30)
    assert eq.is_stale("running", hb, now) is False


def test_running_with_old_heartbeat_is_stale():
    now = datetime(2026, 7, 12, 12, 0, 0)
    hb = now - timedelta(seconds=200)
    assert eq.is_stale("running", hb, now) is True


def test_heartbeat_late_under_ffmpeg_load_is_not_reclaimed():
    """The killer false positive. ffmpeg saturates Render's single CPU, so the
    event-loop heartbeat can miss several 30s ticks while the export is
    perfectly alive. Reclaiming there would run a SECOND 15-minute export.
    A 120s-old heartbeat (4 missed ticks) must still count as alive."""
    now = datetime(2026, 7, 12, 12, 0, 0)
    assert eq.is_stale("running", now - timedelta(seconds=120), now) is False
    assert eq.STALE_AFTER_S >= 6 * eq.HEARTBEAT_INTERVAL_S


def test_running_with_no_heartbeat_is_stale():
    now = datetime(2026, 7, 12, 12, 0, 0)
    assert eq.is_stale("running", None, now) is True


def test_queued_with_no_heartbeat_is_stale():
    # A queued run left behind by a dead container must be reclaimed.
    now = datetime(2026, 7, 12, 12, 0, 0)
    assert eq.is_stale("queued", None, now) is True


def test_terminal_states_are_never_stale():
    now = datetime(2026, 7, 12, 12, 0, 0)
    assert eq.is_stale("done", None, now) is False
    assert eq.is_stale("failed", None, now) is False


def test_next_state_retries_below_cap():
    assert eq.next_state_after_reclaim(0) == "queued"
    assert eq.next_state_after_reclaim(2) == "queued"


def test_next_state_gives_up_at_cap():
    assert eq.next_state_after_reclaim(3) == "failed"
    assert eq.next_state_after_reclaim(9) == "failed"


# ---- Model ---------------------------------------------------------------
def test_export_run_table_and_columns_exist():
    from models import ExportRun
    cols = set(ExportRun.__table__.columns.keys())
    for c in ("id", "job_id", "user_id", "state", "settings_json",
              "result_json", "error", "attempts", "heartbeat_at",
              "created_at", "started_at", "finished_at"):
        assert c in cols, f"missing column {c}"


def test_job_has_cascading_exports_relationship():
    # Without cascade delete-orphan the FK blocks job deletion.
    from models import Job
    rel = Job.__mapper__.relationships["exports"]
    assert "delete-orphan" in rel.cascade


def test_export_run_to_dict_round_trips_result():
    import json
    from models import ExportRun
    run = ExportRun(
        id="e1", job_id="j1", state="done",
        settings_json="{}", result_json=json.dumps({"filename": "x.mp4"}),
        attempts=1,
    )
    d = run.to_dict()
    assert d["state"] == "done"
    assert d["attempts"] == 1
    assert d["result"] == {"filename": "x.mp4"}


# ---- main.py wiring (source asserts) --------------------------------------
def test_runner_symbols_exist_in_main():
    src = _main_src()
    for sym in ("async def _do_export_final(",
                "async def _export_runner(",
                "def _spawn_export_runner(",
                "def _claim_export_run(",
                "def _sweep_stale_exports(",
                "async def _export_sweeper(",
                "def _requeue_local_exports_on_shutdown("):
        assert sym in src, f"missing {sym}"


def test_post_does_not_await_the_export_work():
    """The whole point of v850: the 5-15 min job must NOT run inside the HTTP
    request. Only _export_runner may await it."""
    src = _main_src()
    assert src.count("await _do_export_final(") == 1
    body = src.split("async def _export_runner(", 1)[1].split("\ndef _sweep_stale_exports(", 1)[0]
    assert "await _do_export_final(" in body


def test_both_routes_registered_and_202():
    src = _main_src()
    assert '@app.post("/api/jobs/{job_id}/export-final")' in src
    assert '@app.get("/api/jobs/{job_id}/export-status")' in src
    assert "status_code=202" in src


def test_claim_is_compare_and_swap():
    """Two claimants must never both win. The UPDATE is guarded by a state
    predicate and the win is decided by rowcount."""
    src = _main_src()
    assert "UPDATE export_runs SET state='running'" in src
    assert "WHERE id=:id AND state=:queued" in src
    assert "rowcount != 1" in src


def test_claim_predicate_is_queued_only():
    """The CAS must only ever claim a QUEUED row.

    Nothing hands the runner a 'running' row (POST inserts queued, sweep and
    shutdown handover re-queue). Allowing 'running' in the WHERE would let a
    stray second spawn win the claim on an already-running export and start a
    SECOND 15-minute ffmpeg job. This is the backstop — never loosen it.
    """
    src = _main_src()
    claim = src.split("def _claim_export_run(", 1)[1].split("\nasync def ", 1)[0]
    assert "UPDATE export_runs" in claim
    where = claim.split("WHERE id=:id", 1)[1].split('"', 1)[0]
    assert "running" not in where, f"claim WHERE must not accept 'running': {where!r}"
    assert "state=:queued" in where
    assert '"queued": _eq.STATE_QUEUED' in claim, "queued state must be bound from export_queue"
    # and nowhere else in main.py may a claim-style UPDATE allow 'running'
    assert "state IN ('queued','running')" not in src


def test_finish_export_run_uses_a_fresh_session():
    """The runner holds ONE session for 15 minutes. If Postgres dropped it
    mid-export, the terminal write must NOT go on that corpse — the row would
    stay 'running' with a dead owner and the sweeper would re-run an export that
    genuinely failed. _finish_export_run opens its own short session."""
    src = _main_src()
    assert "def _finish_export_run(" in src
    fn = src.split("def _finish_export_run(", 1)[1].split("\nasync def ", 1)[0]
    assert "with get_db() as _db:" in fn, "_finish_export_run must open its OWN session"
    assert "_db.commit()" in fn
    assert "row.finished_at" in fn
    assert "json.dumps(result)" in fn
    assert "[:2000]" in fn, "error text must be truncated"


def test_runner_writes_both_terminal_states_via_finish_helper():
    src = _main_src()
    body = src.split("async def _export_runner(", 1)[1].split("\ndef _sweep_stale_exports(", 1)[0]
    assert re.search(r"await asyncio\.to_thread\(\s*_finish_export_run,\s*export_id,\s*_eq\.STATE_DONE",
                     body), "success path must finish on a fresh session"
    assert re.search(r"await asyncio\.to_thread\(\s*_finish_export_run,\s*export_id,\s*_eq\.STATE_FAILED",
                     body), "failure path must finish on a fresh session"
    # the long-lived session must never write the outcome itself
    assert "row.state = _eq.STATE_DONE" not in body
    assert "row.state = _eq.STATE_FAILED" not in body


def test_runner_error_text_prefers_httpexception_detail():
    """_do_export_final was a route handler and still raises HTTPException. It
    subclasses Exception so the runner catches it, but its str() is not what the
    user should read — .detail is."""
    src = _main_src()
    body = src.split("async def _export_runner(", 1)[1].split("\ndef _sweep_stale_exports(", 1)[0]
    assert 'getattr(e, "detail", None) or str(e)' in body


def test_spawn_registers_id_before_creating_task():
    """The sweeper skips ids in _LOCAL_EXPORT_IDS. If the task were created
    first, a sweeper tick in the gap could call the run orphaned and start a
    second copy of the same ffmpeg job."""
    src = _main_src()
    fn = src.split("def _spawn_export_runner(", 1)[1].split("\ndef ", 1)[0]
    add_i = fn.index("_LOCAL_EXPORT_IDS.add(export_id)")
    task_i = fn.index("asyncio.create_task(_export_runner(export_id))")
    assert add_i < task_i


def test_lifespan_wires_boot_sweep_and_shutdown_handover():
    src = _main_src()
    lifespan = src.split("async def lifespan(", 1)[1].split("\napp = FastAPI(", 1)[0]
    assert "_asyncio.to_thread(_sweep_stale_exports)" in lifespan       # boot resume
    assert "_asyncio.create_task(_export_sweeper())" in lifespan        # periodic
    assert "_export_sweeper_task.cancel()" in lifespan
    assert "_asyncio.to_thread(_requeue_local_exports_on_shutdown)" in lifespan
    # the handover must happen BEFORE the worker is torn down
    assert (lifespan.index("_requeue_local_exports_on_shutdown")
            < lifespan.index("worker.stop()"))


def test_settings_serialized_with_pydantic_v2_api():
    """pydantic 2.x: .json() is deprecated. The queued row must still carry the
    full ExportSettings payload or the runner rebuilds the wrong settings."""
    src = _main_src()
    assert "settings_json=settings.model_dump_json()" in src


# ---- static/index.html wiring (source asserts) ----------------------------
def test_frontend_polls_export_status():
    """The backend POST now answers 202 with only {export_id, state}. If the
    frontend ever goes back to awaiting the POST body, nothing errors — a 202
    passes response.ok — it just silently yields result.filename === undefined.
    So the poll call is the guard."""
    src = _index_src()
    assert "/export-status?export_id=" in src, "frontend never polls /export-status"
    assert "[Export/v850]" in src, "v850 marker missing — frontend may be the pre-v850 copy"


def test_frontend_has_no_dead_v701v_machinery():
    """v701v polled R2 for the output file and could not tell 'still working'
    apart from 'the container died'. It is fully replaced by the run-state poll;
    leftovers would race the new path."""
    src = _index_src()
    assert "_fetchErrored" not in src, "dead v701v _fetchErrored still present"
    assert "_preExportFilenames" not in src, "dead v701v _preExportFilenames still present"


def test_frontend_fails_fast_on_a_4xx_status_poll():
    """A 404 (run gone) or 401/403 (session gone) is PERMANENT. The old
    `if (!_r.ok) continue;` kept polling for the full 30-minute cap and then
    reported a misleading generic timeout. 4xx must throw at once; 5xx must keep
    polling (that IS the container being replaced — the point of v850)."""
    src = _index_src()
    assert "_r.status >= 400 && _r.status < 500" in src, "no 4xx fast-fail branch on the status poll"
    poll = src.split("/export-status?export_id=", 1)[1].split("_lastAttempt = _s.attempts", 1)[0]
    assert "throw new Error(" in poll, "4xx branch must throw, not continue"
    assert "if (!_r.ok) continue;" in poll, "5xx must still keep polling"
    assert "malformed status payload" in src, "a 200 with unparseable JSON must be logged distinctly"


def test_frontend_reads_the_run_result_payload():
    """ExportRun.to_dict() puts the old synchronous endpoint's body under
    `result`. The success card + the voice-clone chain both read that payload,
    so the frontend must unwrap it (not use the status envelope directly)."""
    src = _index_src()
    assert "_run.result" in src, "frontend does not read the run's result payload"
