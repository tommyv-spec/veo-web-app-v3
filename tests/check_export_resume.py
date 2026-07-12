"""v850 — proves the sweeper reclaims an export a dead container left behind.

Simulates: container A claims + starts an export, then dies (heartbeat goes
stale). Container B boots and sweeps. The run must go back to 'queued' and be
re-fired — exactly the Render-deploy-mid-export case.

Run from code/:  python tests/check_export_resume.py
"""
import os
import sys
import uuid
import json
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Scratch DB — MUST be set before models/main import (models reads it at init).
_DB_FILE = os.path.abspath("./_v850_check.db")
os.environ["DATABASE_URL"] = "sqlite:///./_v850_check.db"

import export_queue as eq          # noqa: E402
import image_platform              # noqa: E402,F401  (registers image_nodes; the ExportRun FK chain needs it)
import models                      # noqa: E402
from models import Job, ExportRun, init_db, get_db  # noqa: E402

import main                        # noqa: E402


def _fail(msg):
    print(f"\nFAIL: {msg}")
    _cleanup()
    sys.exit(1)


def _check(cond, msg):
    if not cond:
        _fail(msg)
    print(f"  ok  {msg}")


def _cleanup():
    try:
        models.engine.dispose()
    except Exception:
        pass
    for suffix in ("", "-journal", "-wal", "-shm"):
        p = _DB_FILE + suffix
        if os.path.exists(p):
            try:
                os.remove(p)
            except OSError:
                pass


# ---------------------------------------------------------------------------
# Setup: real tables, real rows.
# ---------------------------------------------------------------------------
_cleanup()
init_db()

JOB_ID = str(uuid.uuid4())
with get_db() as db:
    db.add(Job(id=JOB_ID, status="completed",
               config_json="{}", dialogue_json="[]",
               images_dir="/tmp/_v850/images", output_dir="/tmp/_v850/out"))
    db.commit()

NOW = datetime.utcnow()


def _mk(state, heartbeat_at, attempts, label):
    rid = str(uuid.uuid4())
    with get_db() as db:
        db.add(ExportRun(
            id=rid, job_id=JOB_ID, state=state,
            settings_json=json.dumps({"label": label}),
            attempts=attempts, heartbeat_at=heartbeat_at,
            created_at=NOW, started_at=NOW if state == "running" else None,
        ))
        db.commit()
    return rid


def _state_of(rid):
    with get_db() as db:
        r = db.query(ExportRun).filter(ExportRun.id == rid).first()
        return (r.state, r.heartbeat_at, r.error)


# The 4 scenarios, planted as real DB rows.
DEAD_HARD = _mk("running", NOW - timedelta(seconds=300), 1, "container A SIGKILLed")
HANDED_OVER = _mk("queued", None, 1, "container A graceful shutdown handover")
BURNED_OUT = _mk("running", NOW - timedelta(seconds=300), eq.MAX_ATTEMPTS, "already retried 3x")
ALIVE_SLOW = _mk("running", NOW - timedelta(seconds=60), 1, "live container, slow under ffmpeg")

# Container B boots. It owns nothing yet, and we intercept the re-fire instead
# of really launching a 15-min ffmpeg run (no event loop here anyway).
main._LOCAL_EXPORT_IDS.clear()
FIRED = []
main._spawn_export_runner = lambda export_id: FIRED.append(export_id)

print("v850 deploy-resume check — container B boots and sweeps\n")

n = main._sweep_stale_exports()

print(f"sweep re-fired {n} run(s)\n")

# --- 1. hard-killed container: stale heartbeat -> re-queued + re-fired ------
print("1. running run, heartbeat 300s old (container A was SIGKILLed / OOMed)")
_check(eq.is_stale("running", NOW - timedelta(seconds=300), NOW) is True,
       "is_stale() -> True")
st, hb, _ = _state_of(DEAD_HARD)
_check(st == eq.STATE_QUEUED, f"DB row reclaimed to 'queued' (got '{st}')")
_check(hb is None, "heartbeat cleared so the new owner starts fresh")
_check(DEAD_HARD in FIRED, "runner re-fired -> the export actually re-runs")

# --- 2. graceful handover: NULL heartbeat -> reclaimed immediately ----------
print("\n2. queued run, heartbeat_at=None (container A shut down gracefully)")
_check(eq.is_stale("queued", None, NOW) is True,
       "is_stale() -> True with no waiting for the 180s stale window")
st, _, _ = _state_of(HANDED_OVER)
_check(st == eq.STATE_QUEUED, f"stays 'queued' (got '{st}')")
_check(HANDED_OVER in FIRED, "runner re-fired on the boot sweep")

# --- 3. attempt cap: give up, do not retry forever --------------------------
print(f"\n3. run that already burned MAX_ATTEMPTS ({eq.MAX_ATTEMPTS})")
_check(eq.next_state_after_reclaim(eq.MAX_ATTEMPTS) == eq.STATE_FAILED,
       "next_state_after_reclaim() -> 'failed'")
st, _, err = _state_of(BURNED_OUT)
_check(st == eq.STATE_FAILED, f"DB row marked 'failed' (got '{st}')")
_check(bool(err), "a human-readable error is written for the UI")
_check(BURNED_OUT not in FIRED, "NOT re-fired — no infinite retry loop")

# --- 4. false-positive guard: a live-but-slow run must not be touched -------
print("\n4. running run, heartbeat 60s old (live container, ffmpeg is just slow)")
_check(eq.is_stale("running", NOW - timedelta(seconds=60), NOW) is False,
       "is_stale() -> False")
st, hb, _ = _state_of(ALIVE_SLOW)
_check(st == eq.STATE_RUNNING, f"DB row untouched, still 'running' (got '{st}')")
_check(hb is not None, "heartbeat left intact")
_check(ALIVE_SLOW not in FIRED,
       "NOT re-fired — reclaiming it would double-run a 15-min ffmpeg job")

# --- sweep return value ----------------------------------------------------
print("\n5. sweep bookkeeping")
_check(n == 2, f"sweep re-fired exactly the 2 orphans (got {n})")
_check(sorted(FIRED) == sorted([DEAD_HARD, HANDED_OVER]),
       "the re-fired ids are exactly the two orphaned runs")

# --- 6. a genuinely-failed run is FINAL (FIX-2 property) --------------------
# The runner's terminal write goes through _finish_export_run on a FRESH
# session, precisely so the outcome lands even when the export's own 15-minute
# session is dead. Prove the write lands AND that the sweeper then leaves the
# row alone: if a failure could not be recorded, the row would stay 'running'
# with a dead owner and the sweeper would re-run an export that really failed —
# burning all MAX_ATTEMPTS on a doomed job.
print("\n6. a run that genuinely FAILED is written on a fresh session and never reclaimed")
REALLY_FAILED = _mk("running", NOW - timedelta(seconds=300), 1, "export raised mid-ffmpeg")

ok = main._finish_export_run(
    REALLY_FAILED, eq.STATE_FAILED, None,
    "Export failed: ffmpeg exited with code 1 (no such file: clip_003.mp4)",
)
_check(ok is True, "_finish_export_run() reported the write landed")

st, hb, err = _state_of(REALLY_FAILED)
_check(st == eq.STATE_FAILED, f"DB row is 'failed' (got '{st}')")
_check("ffmpeg exited with code 1" in (err or ""),
       f"the error the UI shows is human-readable: {err!r}")
with get_db() as db:
    _row = db.query(ExportRun).filter(ExportRun.id == REALLY_FAILED).first()
    _check(_row.finished_at is not None, "finished_at stamped")

# heartbeat is 300s stale — the ONLY thing keeping the sweeper off it is the
# terminal state.
_check(eq.is_stale(st, hb, NOW) is False,
       "is_stale() -> False on a terminal run even with a 300s-old heartbeat")
FIRED.clear()
main._sweep_stale_exports()
_check(REALLY_FAILED not in FIRED,
       "NOT re-fired by the sweeper — a genuinely-failed export is never retried "
       "into the ground (the other two rows are still queued, so they do fire again)")
st2, _, _ = _state_of(REALLY_FAILED)
_check(st2 == eq.STATE_FAILED, f"still 'failed' after the sweep (got '{st2}')")

_cleanup()
print(
    "\nPASS — v850 deploy-resume proven against real DB rows: "
    "_sweep_stale_exports re-queues + re-fires a hard-killed run (stale heartbeat) "
    "and a gracefully-handed-over run (NULL heartbeat), FAILS a run past "
    f"MAX_ATTEMPTS={eq.MAX_ATTEMPTS} instead of retrying forever, leaves a "
    "live-but-slow run (60s heartbeat) completely alone, and a run finished via "
    "_finish_export_run() on a FRESH session lands as 'failed' with a readable "
    "error that the sweeper never touches again."
)
