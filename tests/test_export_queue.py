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


def _main_src():
    return open(_MAIN, encoding="utf-8").read()


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
    assert "WHERE id=:id AND state IN ('queued','running')" in src
    assert "rowcount != 1" in src


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
