# Deploy-Survivable Work: Durable Export Queue (v850) + Worker Upload Retry (v851)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** No in-flight work dies just because `code/` auto-deployed. Two independent paths lose work today on a Render restart:
- **v850 (Tasks 1-8)** — a final-video export runs inside the HTTP request; SIGTERM kills ffmpeg mid-run and the browser eventually reports failure. Fix: persist it, run it detached, re-run it on the next container.
- **v851 (Task 0)** — the image worker's variant upload dies with `ChunkedEncodingError` and gets ZERO retries, so 4 already-rendered Flow variants are thrown away and the node is marked failed. Fix: retry on the right exception family, with a backoff that outlasts a deploy.

Both were reported by the operator on 2026-07-12 ("an export shouldn't fail if a new version is deployed in the meantime" / "also this one happened during a deployment").

**v850 goal:** A final-video export survives a Render deploy / restart / OOM — it is persisted as a DB row, runs detached from the HTTP request, and is automatically re-run by the next container instead of dying half-way through ffmpeg.

**Architecture:** Today `POST /api/jobs/{job_id}/export-final` ([main.py:8512-9795](../../../main.py#L8512-L9795)) does the entire 5-15 min export *inside* the request via `await asyncio.to_thread(process_export, ...)`. Nothing is persisted, so a SIGTERM (Render deploy) kills the ffmpeg work and no `final_export_*.mp4` ever reaches R2; the frontend's v701v poll ([static/index.html:14567-14595](../../../static/index.html#L14567-L14595)) then waits out its 15 min cap and throws *"Export polling exceeded the time cap"*.

New shape:
1. A new `export_runs` table holds the request (settings JSON + state machine `queued → running → done|failed` + attempts + heartbeat).
2. The endpoint becomes thin: validate, insert the row, spawn a **detached** `asyncio` task, return `202 {export_id}`. The existing 1280-line export body is renamed to `_do_export_final(...)` and called by the runner — its logic is untouched.
3. The runner heartbeats every 30s. A sweeper (on boot + every 60s) reclaims any run whose heartbeat is stale and re-fires it. Graceful shutdown pre-marks in-flight runs `queued` so the next boot resumes instantly.
4. The frontend polls `GET /api/jobs/{job_id}/export-status` instead of waiting on the POST body, and shows "re-running after deploy" when the attempt counter bumps.

Re-run is **from scratch** (export is deterministic and idempotent — a new `final_export_<ts>_<hash>.mp4` filename is minted per attempt). No partial-resume complexity.

**Tech Stack:** FastAPI, SQLAlchemy (Postgres in prod, SQLite locally), asyncio, vanilla JS frontend. Tests: pytest, run from `code/` (`python -m pytest tests/...`). No conftest — tests import modules directly or grep source (established pattern: [tests/test_local_watch_never_miss.py:1-23](../../../tests/test_local_watch_never_miss.py#L1-L23)).

**Single-instance assumption:** the Render web service runs one instance. The heartbeat + stale-reclaim design is nevertheless multi-instance safe (a run is only reclaimed when its heartbeat is stale AND it is not in the local in-flight set).

---

### Task 0 (v851): image-worker upload survives a deploy

Ship this first — it is one file, no DB, and it stops the operator losing rendered images on every deploy.

**The bug, from the 2026-07-12 log:**
```
[API:http] node 2791 ⬆ uploading 4 variants...
[API:http] ✗ Upload variants attempt 1/4 unexpected error: Response ended prematurely
requests.exceptions.ChunkedEncodingError: Response ended prematurely
```
`ChunkedEncodingError` subclasses `RequestException`, **not** `ConnectionError`. The retry catch at [image_worker.py:6317-6321](../../../image_worker.py#L6317-L6321) only lists `ConnectionError` / `Timeout`, so the error fell through to the bare `except Exception` at [6322](../../../image_worker.py#L6322), which **re-raises without retrying** — note the log says `attempt 1/4` and attempts 2-4 never ran. [image_worker.py:8176-8184](../../../image_worker.py#L8176-L8184) then posted `status=failed`, discarding 4 good Flow renders that were already on disk.

Second defect: even with the right catch, the backoff `[2, 5, 15]` ([6286](../../../image_worker.py#L6286)) totals ~22s. A Render redeploy is unreachable for 60-180s, so all 4 attempts burn before the new container serves. `_post_status` ([6387-6427](../../../image_worker.py#L6387-L6427)) has both defects too.

**Files:**
- Modify: `code/image_worker.py` — add `is_retryable_api_error()` + `API_RETRY_BACKOFF`; use them in `_upload_variants_to_api` and `_post_status`; gate the final upload failure on `_api_wait_for_health`.
- Test: `code/tests/test_image_worker_upload_retry.py`

- [ ] **Step 1: Write the failing test**

Create `code/tests/test_image_worker_upload_retry.py`:

```python
# tests/test_image_worker_upload_retry.py
#
# v851: a Render deploy mid-upload used to throw away 4 already-rendered Flow
# variants. requests raised ChunkedEncodingError ("Response ended
# prematurely") — which is NOT a ConnectionError — so the retry catch missed
# it, the bare `except Exception` re-raised on attempt 1/4, and the node was
# posted as failed.
#
# Run from code/:  PYTHONUTF8=1 python -m pytest tests/test_image_worker_upload_retry.py -v

import requests

import image_worker


def test_chunked_encoding_error_is_retryable():
    # THE 2026-07-12 BUG. This is the whole point of v851.
    assert image_worker.is_retryable_api_error(
        requests.exceptions.ChunkedEncodingError("Response ended prematurely")
    ) is True


def test_connection_error_and_timeout_stay_retryable():
    assert image_worker.is_retryable_api_error(requests.exceptions.ConnectionError()) is True
    assert image_worker.is_retryable_api_error(requests.exceptions.Timeout()) is True


def test_5xx_is_retryable():
    resp = requests.Response()
    resp.status_code = 502
    err = requests.exceptions.HTTPError(response=resp)
    assert image_worker.is_retryable_api_error(err) is True


def test_4xx_is_not_retryable():
    # A client error will fail identically forever — don't burn 6 attempts.
    resp = requests.Response()
    resp.status_code = 422
    err = requests.exceptions.HTTPError(response=resp)
    assert image_worker.is_retryable_api_error(err) is False


def test_non_requests_error_is_not_retryable():
    assert image_worker.is_retryable_api_error(ValueError("bug in our code")) is False


def test_backoff_outlasts_a_render_deploy():
    # A redeploy leaves the platform unreachable for 60-180s. The old
    # [2, 5, 15] schedule gave up after ~22s.
    assert sum(image_worker.API_RETRY_BACKOFF) >= 180


def test_upload_retries_a_chunked_drop_then_succeeds(monkeypatch, tmp_path):
    png = tmp_path / "variant_1.png"
    png.write_bytes(b"\x89PNG fake")
    calls = {"n": 0}

    class _OK:
        status_code = 200
        def raise_for_status(self): pass
        def json(self): return {"ok": True}

    def flaky_post(url, **kw):
        calls["n"] += 1
        if calls["n"] == 1:
            raise requests.exceptions.ChunkedEncodingError("Response ended prematurely")
        return _OK()

    monkeypatch.setattr(image_worker.time, "sleep", lambda *a, **k: None)
    monkeypatch.setattr(requests, "post", flaky_post)

    out = image_worker._upload_variants_to_api("http://x", "k", 2791, [str(png)])
    assert out == {"ok": True}
    assert calls["n"] == 2  # retried the chunked drop instead of giving up
```

- [ ] **Step 2: Run test to verify it fails**

Run (from `code/`): `PYTHONUTF8=1 python -m pytest tests/test_image_worker_upload_retry.py -v`
Expected: FAIL — `AttributeError: module 'image_worker' has no attribute 'is_retryable_api_error'`

- [ ] **Step 3: Write the classifier + backoff**

In `code/image_worker.py`, add immediately ABOVE `def _upload_variants_to_api(` ([6262](../../../image_worker.py#L6262)):

```python
# ============================================================================
# v851 — API retry classification
# ----------------------------------------------------------------------------
# 2026-07-12: a deploy landed mid-upload and requests raised
#   ChunkedEncodingError: Response ended prematurely
# ChunkedEncodingError subclasses RequestException, NOT ConnectionError — so
# the old `except (ConnectionError, Timeout)` catch missed it, the bare
# `except Exception` re-raised on attempt 1 of 4, and _http_worker posted the
# node as FAILED. Four already-rendered Flow variants, sitting finished on
# disk, were thrown away because a new version shipped.
#
# Classify by FAMILY, not by a hand-listed tuple: every requests transport
# error is transient except a 4xx, which will fail identically forever.
#
# The backoff must also outlast a deploy. The old [2, 5, 15] gave up after
# ~22s; Render is unreachable for 60-180s while the new container boots.
# ============================================================================
API_RETRY_BACKOFF = [5, 15, 30, 60, 90]   # 5 waits => 6 attempts, ~200s total
API_MAX_ATTEMPTS = len(API_RETRY_BACKOFF) + 1


def is_retryable_api_error(exc) -> bool:
    """True when re-sending the same request could plausibly succeed."""
    import requests as _rq

    if isinstance(exc, _rq.exceptions.HTTPError):
        resp = getattr(exc, "response", None)
        if resp is not None and 400 <= resp.status_code < 500:
            return False   # client error — permanent
        return True        # 5xx / unknown — server-side, worth retrying
    # ConnectionError, Timeout, ChunkedEncodingError, ContentDecodingError,
    # RemoteDisconnected-wrapped-in-RequestException, ... all transport-level.
    return isinstance(exc, _rq.exceptions.RequestException)
```

- [ ] **Step 4: Rewrite the two retry loops around it**

Replace the body of `_upload_variants_to_api` ([6262-6342](../../../image_worker.py#L6262-L6342)) with:

```python
def _upload_variants_to_api(api_url, api_key, node_id, variant_paths):
    """POST /worker/jobs/{node_id}/variants with the variant files.

    v450: retries with exponential backoff on transient server drops.
    v851: retry on the whole requests transport family (ChunkedEncodingError
    was silently NOT retried — see is_retryable_api_error) and with a backoff
    that outlasts a Render deploy window.
    """
    import requests
    url = f"{api_url.rstrip('/')}{API_PATH_PREFIX}/jobs/{node_id}/variants"
    headers = {"Authorization": f"Bearer {api_key}"}

    last_error = None

    for attempt in range(1, API_MAX_ATTEMPTS + 1):
        # Re-open file handles on each attempt. requests consumes the stream
        # on the previous failed POST, so a reused handle reads zero bytes on
        # retry and the server sees an empty body.
        files = []
        opened = []
        try:
            for p in variant_paths:
                fh = open(p, "rb")
                opened.append(fh)
                files.append(("files", (os.path.basename(p), fh, "image/png")))

            resp = requests.post(url, headers=headers, files=files, timeout=300)
            resp.raise_for_status()
            return resp.json()

        except Exception as e:
            if not is_retryable_api_error(e):
                print(f"[API:http] ✗ Upload variants: permanent error, no retry: "
                      f"{type(e).__name__}: {e}", flush=True)
                raise
            last_error = e
            print(f"[API:http] ⚠ Upload variants attempt {attempt}/{API_MAX_ATTEMPTS} "
                  f"failed: {type(e).__name__}: {e}", flush=True)
        finally:
            for fh in opened:
                try:
                    fh.close()
                except Exception:
                    pass

        if attempt < API_MAX_ATTEMPTS:
            wait_s = API_RETRY_BACKOFF[attempt - 1]
            print(f"[API:http]    waiting {wait_s}s before retry "
                  f"(platform may be redeploying)...", flush=True)
            time.sleep(wait_s)

    raise last_error if last_error else RuntimeError(
        f"Upload variants for node {node_id} failed after {API_MAX_ATTEMPTS} attempts"
    )
```

Replace the retry loop inside `_post_status` ([6407-6427+](../../../image_worker.py#L6407)) the same way — drop its local `max_attempts = 4` / `backoff_schedule = [1, 3, 7]` and its `except` ladder, and use `API_MAX_ATTEMPTS` / `API_RETRY_BACKOFF` / `is_retryable_api_error(e)` exactly as above (keep its `requests.post(url, headers=headers, json=payload, timeout=30)` call and its `return resp.json() if resp.content else {}`). Update its docstring: the schedule is now shared with the upload path, because a status POST that lands mid-deploy fails for the same reason.

- [ ] **Step 5: Don't discard finished renders when the platform is down**

The upload can still exhaust ~200s of retries if the deploy is slow. Rather than throwing away renders that are sitting complete on disk, wait for the platform to come back (`_api_wait_for_health` already rides out a deploy — v845) and try once more.

In `code/image_worker.py`, add right after `is_retryable_api_error`:

```python
def _upload_variants_with_health_gate(api_url, api_key, node_id, saved_paths):
    """v851 — last line of defence for finished renders.

    If the upload exhausts its retries (a long deploy / outage), the variants
    are still complete on local disk. Throwing them away costs a full Flow
    re-render. Wait for the platform to answer /health again (v845 already
    rides out a deploy window) and upload once more.
    """
    try:
        return _upload_variants_to_api(api_url, api_key, node_id, saved_paths)
    except Exception as e:
        if not is_retryable_api_error(e):
            raise
        print(f"[API:http] ⏳ Node {node_id}: upload still failing ({type(e).__name__}). "
              f"{len(saved_paths)} finished variant(s) are on disk — waiting for the "
              f"platform to come back rather than binning them.", flush=True)
        if not _api_wait_for_health(api_url, api_key):
            raise
        print(f"[API:http] ✓ Platform healthy again — re-uploading node {node_id}", flush=True)
        return _upload_variants_to_api(api_url, api_key, node_id, saved_paths)
```

Then in `_http_worker`, change the call at [8171](../../../image_worker.py#L8171):

```python
                _upload_variants_with_health_gate(api_url, api_key, node_id, saved_paths)
```

- [ ] **Step 6: Run the tests**

Run (from `code/`):
```bash
PYTHONUTF8=1 python -m pytest tests/test_image_worker_upload_retry.py tests/test_image_worker_health_retry.py -v
python -c "import image_worker; print('import ok')"
```
Expected: all PASS; `import ok`.

- [ ] **Step 7: Commit**

```bash
git add code/image_worker.py code/tests/test_image_worker_upload_retry.py
git commit -m "v851: retry ChunkedEncodingError on variant upload; backoff outlasts a deploy"
```

- [ ] **Step 8: Get the worker fix onto the operator's machine**

The worker runs from `C:\Users\tomma\KavenoImageWorker\image_worker.py`, not from `code/`. Per the deploy path: push to `main`, Render serves the new `image_worker.py`, and the worker pulls it **on restart**. Tell the operator to restart the image worker after the deploy, and confirm the fix is live by grepping its startup banner / the file for `v851`.

Evidence required before calling it fixed: a real deploy while a node is uploading, with the worker log showing
`⚠ Upload variants attempt 1/6 failed: ChunkedEncodingError` → `waiting 5s before retry (platform may be redeploying)...` → `✓ Node NNNN completed`.

---

### Task 1: `export_queue.py` — pure state helpers

Pure functions with no DB/FastAPI import, so they are unit-testable standalone (main.py is too heavy to import in a test).

**Files:**
- Create: `code/export_queue.py`
- Test: `code/tests/test_export_queue.py`

- [ ] **Step 1: Write the failing test**

Create `code/tests/test_export_queue.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run (from `code/`): `python -m pytest tests/test_export_queue.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'export_queue'`

- [ ] **Step 3: Write minimal implementation**

Create `code/export_queue.py`:

```python
"""v850 — durable export queue: pure state helpers.

The export state machine lives here (no DB, no FastAPI imports) so it is
unit-testable without booting main.py. main.py owns the DB rows and the
asyncio runner; this module owns the RULES.

States: queued -> running -> done | failed

A run is RECLAIMABLE when its heartbeat is stale: the container that owned
it died (Render deploy / OOM / crash) without finishing. The runner
heartbeats every HEARTBEAT_INTERVAL_S; anything older than STALE_AFTER_S is
considered orphaned.
"""
from datetime import datetime
from typing import Optional

STATE_QUEUED = "queued"
STATE_RUNNING = "running"
STATE_DONE = "done"
STATE_FAILED = "failed"

ACTIVE_STATES = (STATE_QUEUED, STATE_RUNNING)
TERMINAL_STATES = (STATE_DONE, STATE_FAILED)

HEARTBEAT_INTERVAL_S = 30
STALE_AFTER_S = 90
MAX_ATTEMPTS = 3


def is_stale(state: str, heartbeat_at: Optional[datetime], now: datetime,
             stale_after_s: int = STALE_AFTER_S) -> bool:
    """True when an active run has no live owner and should be reclaimed.

    A NULL heartbeat means either (a) never started, or (b) a graceful
    shutdown deliberately cleared it so the next boot picks it up at once.
    Both are reclaimable.
    """
    if state not in ACTIVE_STATES:
        return False
    if heartbeat_at is None:
        return True
    return (now - heartbeat_at).total_seconds() > stale_after_s


def next_state_after_reclaim(attempts: int, max_attempts: int = MAX_ATTEMPTS) -> str:
    """Where a reclaimed run goes: back in the queue, or give up.

    `attempts` is how many times the run has ALREADY been started.
    """
    return STATE_QUEUED if attempts < max_attempts else STATE_FAILED
```

- [ ] **Step 4: Run test to verify it passes**

Run (from `code/`): `python -m pytest tests/test_export_queue.py -v`
Expected: PASS — 7 passed

- [ ] **Step 5: Commit**

```bash
git add code/export_queue.py code/tests/test_export_queue.py
git commit -m "v850: export queue state helpers (is_stale / next_state_after_reclaim)"
```

---

### Task 2: `ExportRun` model + cascade

**Files:**
- Modify: `code/models.py` (add class after `Job`'s `to_dict`, before `class JobLog` at [models.py:481](../../../models.py#L481); add relationship inside `Job` at [models.py:251-255](../../../models.py#L251-L255))
- Test: `code/tests/test_export_queue.py` (append)

No SQL migration is needed for a NEW table — `Base.metadata.create_all(bind=engine)` at [models.py:980](../../../models.py#L980) creates it on both Postgres and SQLite. The `_run_migrations_postgresql` list is only for ALTERs on existing tables.

- [ ] **Step 1: Write the failing test**

Append to `code/tests/test_export_queue.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run (from `code/`): `python -m pytest tests/test_export_queue.py -v`
Expected: FAIL — `ImportError: cannot import name 'ExportRun' from 'models'`

- [ ] **Step 3: Write minimal implementation**

In `code/models.py`, inside `class Job`, add to the relationships block (currently [models.py:251-255](../../../models.py#L251-L255)):

```python
    # Relationships
    user = relationship("User", back_populates="jobs")
    clips = relationship("Clip", back_populates="job", cascade="all, delete-orphan")
    logs = relationship("JobLog", back_populates="job", cascade="all, delete-orphan")
    blacklist = relationship("BlacklistEntry", back_populates="job", cascade="all, delete-orphan")
    # v850 — durable export queue. delete-orphan so deleting a job doesn't
    # trip the export_runs FK.
    exports = relationship("ExportRun", back_populates="job", cascade="all, delete-orphan")
```

Then add the new class immediately before `class JobLog(Base):` ([models.py:481](../../../models.py#L481)):

```python
class ExportRun(Base):
    """v850 — one durable final-video export request.

    Pre-v850 the export ran inside the POST /export-final request. A Render
    deploy (SIGTERM) killed the ffmpeg work mid-flight: no file ever landed
    in R2 and the browser sat there until its poll cap expired. The request
    now lands here first, a detached task runs it, and a stale-heartbeat
    sweep re-runs anything a dead container left behind.

    State machine lives in export_queue.py (pure, unit-tested).
    """
    __tablename__ = "export_runs"

    id = Column(String(36), primary_key=True)
    job_id = Column(String(36), ForeignKey("jobs.id"), nullable=False, index=True)
    user_id = Column(String(36), nullable=True)

    state = Column(String(16), default="queued", nullable=False)  # queued|running|done|failed
    settings_json = Column(Text, nullable=False)   # the ExportSettings payload, verbatim
    result_json = Column(Text, nullable=True)      # the success payload the endpoint used to return
    error = Column(Text, nullable=True)
    attempts = Column(Integer, default=0, nullable=False)

    # Liveness. The owning container ticks this every 30s; a stale value
    # means that container is gone and the run is up for reclaim.
    heartbeat_at = Column(DateTime, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)

    job = relationship("Job", back_populates="exports")

    def to_dict(self) -> Dict[str, Any]:
        import json as _json
        result = None
        if self.result_json:
            try:
                result = _json.loads(self.result_json)
            except Exception:
                result = None
        return {
            "export_id": self.id,
            "job_id": self.job_id,
            "state": self.state,
            "attempts": self.attempts,
            "error": self.error,
            "result": result,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "heartbeat_at": self.heartbeat_at.isoformat() if self.heartbeat_at else None,
        }
```

- [ ] **Step 4: Run test to verify it passes**

Run (from `code/`):
```bash
python -m pytest tests/test_export_queue.py -v
python -c "import models; print('ExportRun' in models.Base.metadata.tables and 'export_runs' in models.Base.metadata.tables or list(models.Base.metadata.tables)[:0]); print('export_runs' in models.Base.metadata.tables)"
```
Expected: PASS — 10 passed; second command prints `True`

- [ ] **Step 5: Commit**

```bash
git add code/models.py code/tests/test_export_queue.py
git commit -m "v850: ExportRun table + Job.exports cascade"
```

---

### Task 3: Rename the export body to `_do_export_final`

Pure refactor — zero behavior change. The 1280-line body stays byte-identical; only the signature and the decorator move.

**Files:**
- Modify: `code/main.py:8512-8518` (signature) and `code/main.py:9782-9795` (return / except tail is unchanged, but the function is no longer a route)

- [ ] **Step 1: Replace the decorator + signature**

Current ([main.py:8512-8518](../../../main.py#L8512-L8518)):

```python
@app.post("/api/jobs/{job_id}/export-final")
async def export_final_video(
    job_id: str,
    settings: ExportSettings,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
```

Replace with (note: NO decorator, NO `Depends` defaults — the runner passes both in):

```python
async def _do_export_final(
    job_id: str,
    settings: ExportSettings,
    db: DBSession,
    current_user: User,
) -> dict:
    """v850 — the export WORK. Was the body of POST /export-final until the
    durable queue landed. Unchanged logic; it just no longer runs inside the
    HTTP request (a Render deploy used to kill it mid-ffmpeg). Called by
    _export_runner() with its own long-lived DB session.
    """
```

Leave every line from the old `from video_processor import ...` (8529) through the `raise HTTPException(status_code=500, ...)` tail (9795) exactly as-is.

- [ ] **Step 2: Verify the module still imports**

Run (from `code/`): `python -c "import main; print('ok')"`
Expected: `ok` (py_compile is NOT sufficient — see `code/CLAUDE.md` deploy discipline)

- [ ] **Step 3: Verify no route is registered for export-final right now**

Run (from `code/`):
```bash
python -c "import main; print([r.path for r in main.app.routes if 'export' in r.path])"
```
Expected: the list does NOT contain `/api/jobs/{job_id}/export-final` (Task 4 re-adds it).

- [ ] **Step 4: Commit**

```bash
git add code/main.py
git commit -m "v850: rename export body to _do_export_final (no behavior change)"
```

---

### Task 4: Runner, heartbeat, sweeper, thin endpoint, status endpoint

**Files:**
- Modify: `code/main.py` — insert the runner block immediately BEFORE `async def _do_export_final` (i.e. just above line 8512); the two routes go immediately AFTER the `_do_export_final` tail (after the old line 9795, before `@app.get("/api/vad-available")`).
- Test: `code/tests/test_export_queue.py` (append source-grep asserts)

- [ ] **Step 1: Write the failing test**

Append to `code/tests/test_export_queue.py`:

```python
# ---- Wiring (source-grep: main.py is too heavy to import-and-introspect
# reliably in CI, and this repo has been bitten by missing-name regressions
# py_compile does not catch — same pattern as tests/test_local_watch_never_miss.py)
import os

_HERE = os.path.dirname(os.path.abspath(__file__))
_CODE = os.path.dirname(_HERE)
_MAIN = os.path.join(_CODE, "main.py")
_INDEX = os.path.join(_CODE, "static", "index.html")


def _main_src():
    with open(_MAIN, encoding="utf-8") as f:
        return f.read()


def test_export_work_is_detached_from_the_request():
    src = _main_src()
    assert "async def _do_export_final(" in src
    assert "async def _export_runner(" in src
    assert "def _spawn_export_runner(" in src
    # the POST must NOT await the work
    assert "await _do_export_final(" not in src


def test_endpoint_returns_202_and_status_route_exists():
    src = _main_src()
    assert '@app.post("/api/jobs/{job_id}/export-final")' in src
    assert '@app.get("/api/jobs/{job_id}/export-status")' in src
    assert "status_code=202" in src


def test_sweeper_and_shutdown_requeue_are_wired():
    src = _main_src()
    assert "async def _export_sweeper(" in src
    assert "_sweep_stale_exports(" in src
    assert "[Export/v850]" in src          # diagnostic log marker
    assert "_requeue_local_exports_on_shutdown(" in src


def test_frontend_polls_export_status():
    with open(_INDEX, encoding="utf-8") as f:
        src = f.read()
    assert "/export-status" in src
    assert "Export/v850" in src
```

- [ ] **Step 2: Run test to verify it fails**

Run (from `code/`): `python -m pytest tests/test_export_queue.py -v`
Expected: FAIL — `assert 'async def _export_runner(' in src`

- [ ] **Step 3: Write the runner block**

Insert into `code/main.py` immediately above `async def _do_export_final(`:

```python
# ============================================================================
# v850 — DURABLE EXPORT QUEUE
# ----------------------------------------------------------------------------
# Pre-v850 the whole 5-15 min export ran inside POST /export-final. Render
# auto-deploys on every push to main; the SIGTERM killed ffmpeg mid-run, the
# mp4 never reached R2, and the browser's v701v poll timed out with
# "Export polling exceeded the time cap". Nothing on the server remembered
# the export had been asked for.
#
# Now: the POST persists an ExportRun row and returns 202. A detached task
# does the work and heartbeats every 30s. A sweeper (on boot + every 60s)
# re-runs any ExportRun whose heartbeat went stale — which is exactly what a
# deploy/OOM/crash leaves behind. Re-run is from scratch: the export is
# deterministic and mints a fresh final_export_<ts>_<hash>.mp4, so a partial
# file from the dead container can never be mistaken for the result.
# ============================================================================
import export_queue as _eq
from fastapi.responses import JSONResponse  # v850 — main.py's response imports
                                            # (L133) bring in FileResponse /
                                            # StreamingResponse / HTMLResponse /
                                            # RedirectResponse but NOT this one.

# Export ids this container is actively running. The sweeper never touches an
# id in here, so a slow-but-alive export is never double-started.
_LOCAL_EXPORT_IDS: set = set()
_EXPORT_TASKS: set = set()  # strong refs; asyncio only holds weak ones


def _spawn_export_runner(export_id: str) -> None:
    """Fire the runner detached from any HTTP request."""
    task = asyncio.create_task(_export_runner(export_id))
    _EXPORT_TASKS.add(task)
    task.add_done_callback(_EXPORT_TASKS.discard)


async def _export_heartbeat(export_id: str):
    """Tick heartbeat_at so the sweeper knows this container is still alive."""
    from models import get_db, ExportRun
    while True:
        await asyncio.sleep(_eq.HEARTBEAT_INTERVAL_S)
        try:
            def _tick():
                with get_db() as hdb:
                    run = hdb.query(ExportRun).filter(ExportRun.id == export_id).first()
                    if run and run.state == _eq.STATE_RUNNING:
                        run.heartbeat_at = datetime.utcnow()
                        hdb.commit()
            await asyncio.to_thread(_tick)
        except Exception as e:
            print(f"[Export/v850] heartbeat {export_id[:8]} failed (non-fatal): {e}", flush=True)


async def _export_runner(export_id: str):
    """Run one ExportRun to completion. Owns its own DB session — the
    request-scoped session died with the POST that queued it."""
    from models import get_db, ExportRun, User

    _LOCAL_EXPORT_IDS.add(export_id)
    hb_task = None
    try:
        with get_db() as db:
            run = db.query(ExportRun).filter(ExportRun.id == export_id).first()
            if not run:
                print(f"[Export/v850] run {export_id[:8]} vanished; nothing to do", flush=True)
                return
            if run.state in _eq.TERMINAL_STATES:
                print(f"[Export/v850] run {export_id[:8]} already {run.state}; skip", flush=True)
                return

            run.state = _eq.STATE_RUNNING
            run.attempts = (run.attempts or 0) + 1
            run.started_at = datetime.utcnow()
            run.heartbeat_at = datetime.utcnow()
            db.commit()

            job_id = run.job_id
            attempt = run.attempts
            settings = ExportSettings(**json.loads(run.settings_json))
            user = db.query(User).filter(User.id == run.user_id).first()
            print(
                f"[Export/v850] START run={export_id[:8]} job={job_id[:8]} "
                f"attempt={attempt}/{_eq.MAX_ATTEMPTS}",
                flush=True,
            )

            hb_task = asyncio.create_task(_export_heartbeat(export_id))

            try:
                result = await _do_export_final(job_id, settings, db, user)
                run = db.query(ExportRun).filter(ExportRun.id == export_id).first()
                run.state = _eq.STATE_DONE
                run.result_json = json.dumps(result)
                run.finished_at = datetime.utcnow()
                db.commit()
                print(f"[Export/v850] DONE run={export_id[:8]} → {result.get('filename')}", flush=True)
            except Exception as e:
                import traceback
                traceback.print_exc()
                db.rollback()
                run = db.query(ExportRun).filter(ExportRun.id == export_id).first()
                if run:
                    run.state = _eq.STATE_FAILED
                    run.error = str(e)[:2000]
                    run.finished_at = datetime.utcnow()
                    db.commit()
                print(f"[Export/v850] FAILED run={export_id[:8]}: {e}", flush=True)
    finally:
        if hb_task:
            hb_task.cancel()
        _LOCAL_EXPORT_IDS.discard(export_id)


def _sweep_stale_exports() -> int:
    """Reclaim exports a dead container left behind. Sync — call via to_thread.

    An ExportRun is orphaned when it is still queued/running, this container
    is not the one running it, and its heartbeat is stale. That is exactly the
    state a Render deploy leaves. Returns how many runs were re-fired.
    """
    from models import get_db, ExportRun

    to_fire = []
    with get_db() as db:
        candidates = db.query(ExportRun).filter(
            ExportRun.state.in_(list(_eq.ACTIVE_STATES))
        ).all()
        now = datetime.utcnow()
        for run in candidates:
            if run.id in _LOCAL_EXPORT_IDS:
                continue  # alive, right here
            if not _eq.is_stale(run.state, run.heartbeat_at, now):
                continue
            nxt = _eq.next_state_after_reclaim(run.attempts or 0)
            if nxt == _eq.STATE_FAILED:
                run.state = _eq.STATE_FAILED
                run.error = (
                    f"Export gave up after {run.attempts} attempts "
                    f"(each one was killed before it finished — check the Render logs)."
                )
                run.finished_at = now
                print(
                    f"[Export/v850] GIVE UP run={run.id[:8]} after {run.attempts} attempts",
                    flush=True,
                )
            else:
                run.state = _eq.STATE_QUEUED
                run.heartbeat_at = None
                to_fire.append(run.id)
                print(
                    f"[Export/v850] RECLAIM run={run.id[:8]} job={run.job_id[:8]} "
                    f"(attempts so far={run.attempts}) — a restart killed it; re-running",
                    flush=True,
                )
        db.commit()

    for export_id in to_fire:
        _spawn_export_runner(export_id)
    return len(to_fire)


async def _export_sweeper():
    """Periodic reclaim. Covers the hard-kill path (OOM / SIGKILL) where the
    shutdown hook never ran."""
    while True:
        await asyncio.sleep(60)
        try:
            n = await asyncio.to_thread(_sweep_stale_exports)
            if n:
                print(f"[Export/v850] sweeper re-fired {n} orphaned export(s)", flush=True)
        except Exception as e:
            print(f"[Export/v850] sweeper error (non-fatal): {e}", flush=True)


def _requeue_local_exports_on_shutdown() -> int:
    """Graceful-shutdown hook (the Render deploy path). Flip our in-flight runs
    back to queued with a NULL heartbeat so the NEXT container picks them up
    immediately instead of waiting out the 90s stale window."""
    from models import get_db, ExportRun

    if not _LOCAL_EXPORT_IDS:
        return 0
    ids = list(_LOCAL_EXPORT_IDS)
    with get_db() as db:
        runs = db.query(ExportRun).filter(
            ExportRun.id.in_(ids),
            ExportRun.state == _eq.STATE_RUNNING,
        ).all()
        for run in runs:
            run.state = _eq.STATE_QUEUED
            run.heartbeat_at = None
        db.commit()
        print(
            f"[Export/v850] shutdown: requeued {len(runs)} in-flight export(s) "
            f"for the next container",
            flush=True,
        )
        return len(runs)
```

- [ ] **Step 4: Write the two routes**

Insert into `code/main.py` immediately AFTER the `_do_export_final` tail (the `raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")` line) and BEFORE `@app.get("/api/vad-available")`:

```python
@app.post("/api/jobs/{job_id}/export-final")
async def export_final_video(
    job_id: str,
    settings: ExportSettings,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """v850 — QUEUE an export. Returns immediately (202); the work runs
    detached so a Render deploy can't kill it. Poll /export-status."""
    from models import ExportRun

    job = get_user_job(db, job_id, current_user)  # 404/403 if not the caller's

    # Idempotent: a second click (or the browser retrying after a dropped
    # connection) joins the export already in flight instead of starting a
    # duplicate 15-minute ffmpeg run.
    existing = db.query(ExportRun).filter(
        ExportRun.job_id == job_id,
        ExportRun.state.in_(list(_eq.ACTIVE_STATES)),
    ).order_by(ExportRun.created_at.desc()).first()
    if existing:
        print(
            f"[Export/v850] job={job_id[:8]} already has run={existing.id[:8]} "
            f"({existing.state}); joining it",
            flush=True,
        )
        return JSONResponse(status_code=202, content=existing.to_dict())

    run = ExportRun(
        id=str(uuid.uuid4()),
        job_id=job_id,
        user_id=current_user.id,
        state=_eq.STATE_QUEUED,
        settings_json=settings.json(),
        attempts=0,
        created_at=datetime.utcnow(),
    )
    db.add(run)
    db.commit()
    db.refresh(run)

    print(f"[Export/v850] QUEUED run={run.id[:8]} job={job_id[:8]}", flush=True)
    _spawn_export_runner(run.id)
    return JSONResponse(status_code=202, content=run.to_dict())


@app.get("/api/jobs/{job_id}/export-status")
async def export_status(
    job_id: str,
    export_id: str = None,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """v850 — poll target for the frontend. Returns the named run, or the most
    recent one for this job. `result` carries the exact payload the old
    synchronous endpoint used to return (filename / download_url / stats /
    audio / support_tracks)."""
    from models import ExportRun

    get_user_job(db, job_id, current_user)

    q = db.query(ExportRun).filter(ExportRun.job_id == job_id)
    if export_id:
        q = q.filter(ExportRun.id == export_id)
    run = q.order_by(ExportRun.created_at.desc()).first()
    if not run:
        raise HTTPException(status_code=404, detail="No export has been requested for this job")
    return run.to_dict()
```

`settings.json()` is Pydantic v1 (`ExportSettings` is a `BaseModel`). If the repo is on Pydantic v2, use `settings.model_dump_json()` — check with `python -c "import pydantic; print(pydantic.VERSION)"` from `code/` and use the matching call.

- [ ] **Step 5: Wire the sweeper into startup and the requeue into shutdown**

In `lifespan`, inside `_run_deferred_startup()`, add right after the stuck-job recovery block (after [main.py:585](../../../main.py#L585), before the v564 NOTE comment):

```python
        # v850 — re-run exports killed by the deploy that just replaced the
        # previous container. Runs once at boot; _export_sweeper then keeps
        # checking every 60s for the hard-kill (OOM/SIGKILL) path.
        try:
            _n_exports = await _asyncio.to_thread(_sweep_stale_exports)
            if _n_exports:
                print(f"[Deferred][Export/v850] re-fired {_n_exports} export(s) orphaned by the last restart", flush=True)
            else:
                print("[Deferred][Export/v850] no orphaned exports to re-run", flush=True)
        except Exception as _ex_e:
            print(f"[Deferred][Export/v850] orphan sweep failed: {_ex_e}", flush=True)
```

Next to `_purge_task = _asyncio.create_task(_purge_old_logs())` ([main.py:635](../../../main.py#L635)), add:

```python
    _export_sweeper_task = _asyncio.create_task(_export_sweeper())
    print("[App][Export/v850] export sweeper started (60s tick)", flush=True)
```

In the shutdown section ([main.py:644-652](../../../main.py#L644-L652)), before `worker.stop()`:

```python
    # Shutdown
    _purge_task.cancel()
    _export_sweeper_task.cancel()
    # v850 — Render deploy path. Hand our in-flight exports to the next
    # container before this one dies, so the operator's export doesn't fail
    # just because a new version shipped mid-run.
    try:
        await _asyncio.to_thread(_requeue_local_exports_on_shutdown)
    except Exception as _rq_e:
        print(f"[Export/v850] shutdown requeue failed: {_rq_e}", flush=True)
```

- [ ] **Step 6: Run tests + import check**

Run (from `code/`):
```bash
python -m pytest tests/test_export_queue.py -v
python -c "import main; print([r.path for r in main.app.routes if 'export' in r.path])"
```
Expected: the frontend test still FAILS (Task 5 fixes it); all other tests PASS. Second command lists both `/api/jobs/{job_id}/export-final` and `/api/jobs/{job_id}/export-status`.

- [ ] **Step 7: Commit**

```bash
git add code/main.py code/tests/test_export_queue.py
git commit -m "v850: durable export queue - detached runner, heartbeat, stale sweep, 202 endpoint"
```

---

### Task 5: Frontend polls export-status

**Files:**
- Modify: `code/static/index.html:14522-14624` — replace the v701v pre-snapshot + POST-await + list-outputs poll with a POST-then-poll-export-status loop.

The downstream code is unchanged: it reads `result.filename`, `result.download_url`, `result.stats` and the voice-clone chain at [index.html:14626-14650](../../../static/index.html#L14626-L14650). `export-status`'s `result` field is the identical payload, so nothing after this block needs touching.

- [ ] **Step 1: Replace the block**

Delete everything from the `// v701v — Snapshot pre-export R2 file list...` comment ([index.html:14522](../../../static/index.html#L14522)) through the closing brace of the `if (!response.ok) { ... }` guard ([index.html:14624](../../../static/index.html#L14624)), i.e. up to (but not including) `const result = await response.json();`. Put this in its place:

```javascript
                // v850 — the export is now a DURABLE SERVER-SIDE JOB. The POST
                // returns 202 immediately with an export_id; the work runs
                // detached from the request. If Render redeploys mid-export
                // (auto-deploy fires on every push to main) the next container
                // re-runs it — attempts bumps and we just keep polling. The old
                // v701v R2-file poll is gone: it could not tell "still working"
                // apart from "the container died", so a deploy always ended in
                // "Export polling exceeded the time cap".
                const _queueRes = await fetch(`${API}/jobs/${jobId}/export-final`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(settings),
                });
                if (!_queueRes.ok) {
                    let _qErr = 'Export could not be queued';
                    try { _qErr = (await _queueRes.json()).detail || _qErr; }
                    catch (e) { _qErr = `HTTP ${_queueRes.status}: ${_queueRes.statusText}`; }
                    throw new Error(_qErr);
                }
                const _queued = await _queueRes.json();
                const _exportId = _queued.export_id;
                console.log(`[Export/v850] queued export_id=${_exportId} state=${_queued.state}`);

                const _statusEl = document.getElementById('exportStatusText');
                const _pollStart = Date.now();
                const _pollMaxMs = 1800000;   // 30 min — a v698A dual-output run plus one deploy-restart
                let _lastAttempt = 0;
                let _run = null;

                while (Date.now() - _pollStart < _pollMaxMs) {
                    await new Promise(r => setTimeout(r, 3000));
                    let _s;
                    try {
                        const _r = await fetch(`${API}/jobs/${jobId}/export-status?export_id=${_exportId}`);
                        if (!_r.ok) continue;             // server mid-restart — keep polling
                        _s = await _r.json();
                    } catch (_pollErr) {
                        // Container is being replaced right now. NOT a failure.
                        console.log('[Export/v850] poll tick error (server restarting?):', _pollErr);
                        if (_statusEl) _statusEl.textContent = '⏳ Server restarting — your export is saved and will resume...';
                        continue;
                    }

                    if (_s.attempts > _lastAttempt && _lastAttempt > 0) {
                        console.log(`[Export/v850] export restarted by the server (attempt ${_s.attempts})`);
                    }
                    _lastAttempt = _s.attempts;

                    if (_s.state === 'done')   { _run = _s; break; }
                    if (_s.state === 'failed') { throw new Error(_s.error || 'Export failed'); }

                    const _elapsed = ((Date.now() - _pollStart) / 1000).toFixed(0);
                    if (_statusEl) {
                        _statusEl.textContent = _s.state === 'queued'
                            ? `⏳ Export queued — a deploy restarted it, re-running (attempt ${_s.attempts + 1})...`
                            : `🎬 Processing (${_elapsed}s, attempt ${_s.attempts})...`;
                    }
                }

                if (!_run) {
                    throw new Error('Export is still running after 30 minutes. It is saved on the server — reopen this job in a moment to pick up the result.');
                }

                // Same payload the old synchronous endpoint returned.
                const response = new Response(JSON.stringify(_run.result || {}), {
                    status: 200, headers: { 'Content-Type': 'application/json' },
                });
```

Delete the now-dead `const controller = new AbortController();` / `exportTimeout` / `timeoutId` lines at [index.html:14517-14520](../../../static/index.html#L14517-L14520) — nothing aborts the POST any more (it returns in milliseconds). Keep the server warm-up guard above them; it still helps on a cold start.

- [ ] **Step 2: Check no stale references survive**

Run (from repo root):
```bash
grep -n "_fetchErrored\|_preExportFilenames\|timeoutId\|clearTimeout" "code/static/index.html" | sed -n '1,20p'
```
Expected: no hits inside the export function (hits elsewhere in the file are other features and are fine — verify each hit's line number is outside 14400-14700).

- [ ] **Step 3: Run tests**

Run (from `code/`): `python -m pytest tests/test_export_queue.py -v`
Expected: PASS — all tests including `test_frontend_polls_export_status`

- [ ] **Step 4: Commit MY HUNKS ONLY (shared working tree — read this)**

⚠ Another session has ~87 UNCOMMITTED lines in `static/index.html` (a v833 per-image download button, in the image-gallery code — nowhere near the export block). A plain `git add static/index.html` would sweep that unfinished work into this commit and push it to production.

Do NOT `git add` the file, do NOT `git stash`, do NOT `git checkout --` it (all of those risk destroying the other session's in-flight work). Build the commit from a temp index blob instead — the working tree is never touched:

```bash
cd "c:/Users/tomma/Documents/Videos Obsidian 2/code"
SCRATCH="$TMPDIR/v850"; mkdir -p "$SCRATCH"

# 1. The pristine HEAD version (no foreign lines, no mine)
git show HEAD:static/index.html > "$SCRATCH/base.html"

# 2. Apply the SAME export-block edit to that pristine copy (write a small
#    python script that does the exact same string replacement you made in the
#    working file — do not hand-retype it).
python "$SCRATCH/apply_export_edit.py" "$SCRATCH/base.html"

# 3. Commit that blob directly — working tree (which still holds BOTH your
#    edit and the other session's v833 lines) is left alone.
BLOB=$(git hash-object -w "$SCRATCH/base.html")
git update-index --cacheinfo 100644,$BLOB,static/index.html
git commit -m "v850: frontend polls export-status; a deploy mid-export no longer fails the export"
```

Verify afterwards — the commit must contain ONLY the export change, and the foreign v833 lines must still be sitting uncommitted in the working tree:

```bash
git show --stat HEAD
git show HEAD -- static/index.html | grep -c "v833"   # expect 0
git diff --stat -- static/index.html                  # expect the ~87 foreign lines still here
git diff -- static/index.html | grep -c "export-status"  # expect 0 (yours is committed)
```

---

### Task 6: Local end-to-end proof (before pushing to prod)

Production is the only environment (`code/CLAUDE.md` deploy discipline) — so prove the resume path locally against SQLite first. NEVER claim this works off a code read (root `CLAUDE.md` §2).

**Files:**
- Create: `code/tests/check_export_resume.py` (a `check_*.py` script, matching the existing convention in `code/tests/`)

- [ ] **Step 1: Write the check script**

Create `code/tests/check_export_resume.py`:

```python
"""v850 — proves the sweeper reclaims an export a dead container left behind.

Simulates: container A queues + starts an export, then dies (heartbeat goes
stale). Container B boots and sweeps. The run must go back to 'queued' and be
re-fired — that is exactly the Render-deploy-mid-export case.

Run from code/:  python tests/check_export_resume.py
"""
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ["DATABASE_URL"] = "sqlite:///./_v850_check.db"

from models import init_db, get_db, Job, ExportRun, User  # noqa: E402
import export_queue as eq  # noqa: E402

init_db()

with get_db() as db:
    db.query(ExportRun).delete()
    db.commit()
    user = db.query(User).first()
    job = Job(
        id="job-v850", user_id=user.id if user else None,
        config_json="{}", dialogue_json="[]",
        images_dir="/tmp/i", output_dir="/tmp/o",
    )
    db.merge(job)
    run = ExportRun(
        id="run-v850", job_id="job-v850", state="running",
        settings_json="{}", attempts=1,
        heartbeat_at=datetime.utcnow() - timedelta(seconds=300),  # container died 5 min ago
        created_at=datetime.utcnow(),
    )
    db.add(run)
    db.commit()

# The sweep decision, without booting FastAPI.
with get_db() as db:
    run = db.query(ExportRun).filter(ExportRun.id == "run-v850").first()
    now = datetime.utcnow()
    stale = eq.is_stale(run.state, run.heartbeat_at, now)
    nxt = eq.next_state_after_reclaim(run.attempts)
    print(f"state={run.state} attempts={run.attempts} heartbeat_age="
          f"{(now - run.heartbeat_at).total_seconds():.0f}s")
    print(f"is_stale={stale}  next_state={nxt}")
    assert stale is True, "FAIL: dead container's run was not detected as stale"
    assert nxt == "queued", "FAIL: run should be re-queued, not failed"
    print("PASS — a deploy-killed export gets re-queued and re-run.")
```

- [ ] **Step 2: Run it**

Run (from `code/`): `python tests/check_export_resume.py`
Expected final line: `PASS — a deploy-killed export gets re-queued and re-run.`

- [ ] **Step 3: Clean up the scratch DB**

```bash
rm -f code/_v850_check.db
```

- [ ] **Step 4: Full import + test sweep**

Run (from `code/`):
```bash
python -c "import main, models, export_queue; print('imports ok')"
python -m pytest tests/test_export_queue.py tests/test_export_active_line.py -v
```
Expected: `imports ok`, all tests pass.

- [ ] **Step 5: Commit**

```bash
git add code/tests/check_export_resume.py
git commit -m "v850: local check - deploy-killed export is reclaimed and re-queued"
```

---

### Task 7: Ship + capture production evidence

`code/` auto-deploys to Render on push to `main`. Root `CLAUDE.md` §2: no "should work" claim without evidence.

- [ ] **Step 1: Push**

```bash
cd code && git push origin main
```

- [ ] **Step 2: Confirm the boot log**

Ask the operator for the Render log after the deploy. Required lines:
```
[App][Export/v850] export sweeper started (60s tick)
[Deferred][Export/v850] no orphaned exports to re-run
```

- [ ] **Step 3: Prove the resume path in production**

Ask the operator to: start an export on a real job, then push a trivial commit (or hit Manual Deploy) while it is running. Required evidence:
- old container: `[Export/v850] shutdown: requeued 1 in-flight export(s) for the next container`
- new container: `[Export/v850] RECLAIM run=… — a restart killed it; re-running` then `[Export/v850] START run=… attempt=2/3`
- eventually: `[Export/v850] DONE run=… → final_export_….mp4`
- browser: the status text shows the "deploy restarted it, re-running" message and then the normal success card. No "Export polling exceeded the time cap".

Do NOT mark this task done until those lines land. Invoke `superpowers:verification-before-completion` before claiming it works.

- [ ] **Step 4: Post-push review**

Spawn `caveman:cavecrew-reviewer` on the v850 commit set (root `CLAUDE.md` §3).

- [ ] **Step 5: Bump the submodule pointer in the wiki repo**

```bash
cd .. && git add code && git commit -m "build: bump code to v850 (durable export queue)"
```

---

### Task 8: Document the rule

Per `code/CLAUDE.md` "When adding a new v-rule".

**Files:**
- Modify: `code/template_reference.md` (deep-dive — canonical home)
- Modify: `wiki/patterns/conventions.md` (one-row index entry + cross-link)
- Modify: `wiki/log.md` (timeline entry)

- [ ] **Step 1a: Deep-dive `§v851 — worker upload survives a deploy` in `code/template_reference.md`**

Cover: `ChunkedEncodingError` is a `RequestException`, not a `ConnectionError` — the old hand-listed catch tuple missed it entirely, so the very first mid-response drop re-raised through the bare `except Exception` and the node was posted failed with 4 finished renders on disk. The rule: **classify retryability by exception FAMILY, never by a hand-listed tuple**, and size retry backoff against the platform's real recovery window (a Render deploy = 60-180s unreachable, so ~22s of backoff is theatre). Note the health-gate fallback for finished-but-unuploadable renders.

- [ ] **Step 1b: Deep-dive `§v850 — Durable export queue` in `code/template_reference.md`**

Append a section covering: the pre-v850 failure (export ran inside the HTTP request; Render's deploy SIGTERM killed ffmpeg; no file in R2; browser's v701v poll timed out), the state machine (`queued → running → done|failed`, heartbeat every 30s, stale after 90s, 3 attempts), why re-run is from-scratch rather than resume (deterministic export, fresh filename per attempt, no partial-file ambiguity), the graceful-shutdown requeue vs the periodic sweeper (deploy vs OOM), and the idempotency guard (a second POST joins the in-flight run).

- [ ] **Step 2: Index rows in `wiki/patterns/conventions.md`**

Two rows:
- `v850 | durable export queue — export survives a deploy/restart; ExportRun row + detached runner + stale-heartbeat re-run | code/template_reference.md §v850`
- `v851 | worker API retry by exception family (ChunkedEncodingError WAS being dropped) + backoff that outlasts a deploy | code/template_reference.md §v851`

- [ ] **Step 3: Timeline entry in `wiki/log.md`**

One line, dated 2026-07-12, covering both fixes + the operator reports that triggered them ("an export shouldn't fail if a new version is deployed in the meantime" / "also this one happened during a deployment").

- [ ] **Step 4: Refresh gbrain**

```bash
gbrain import wiki
```

- [ ] **Step 5: Commit**

```bash
git add code/template_reference.md wiki/patterns/conventions.md wiki/log.md
git commit -m "rules: v850 durable export queue"
```

---

## Verification summary

| Claim | Evidence required |
|---|---|
| A mid-response drop is retried (v851) | `PYTHONUTF8=1 python -m pytest tests/test_image_worker_upload_retry.py -v` green — incl. `test_chunked_encoding_error_is_retryable` |
| Worker rides out a real deploy (v851) | worker log: `⚠ Upload variants attempt 1/6 failed: ChunkedEncodingError` → `waiting 5s before retry` → `✓ Node NNNN completed` |
| State machine is right | `python -m pytest tests/test_export_queue.py -v` all green |
| Work is detached from the request | source-grep test: `await _do_export_final(` appears nowhere; endpoint returns `status_code=202` |
| A dead container's export is reclaimed | `python tests/check_export_resume.py` prints PASS |
| Nothing broke on import | `python -c "import main"` (py_compile is not enough) |
| It actually works in prod | Render logs: `shutdown: requeued` on the old container → `RECLAIM` + `START … attempt=2/3` → `DONE` on the new one |
