"""v850 — durable export queue. Pure state helpers."""
from datetime import datetime, timedelta

import export_queue as eq


def test_running_with_fresh_heartbeat_is_not_stale():
    now = datetime(2026, 7, 12, 12, 0, 0)
    hb = now - timedelta(seconds=30)
    assert eq.is_stale("running", hb, now) is False


def test_running_with_old_heartbeat_is_stale():
    now = datetime(2026, 7, 12, 12, 0, 0)
    hb = now - timedelta(seconds=200)
    assert eq.is_stale("running", hb, now) is True


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
