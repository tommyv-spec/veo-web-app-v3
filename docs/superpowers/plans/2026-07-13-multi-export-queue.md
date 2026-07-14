# Multi-Export Queue (v853) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The operator can hit Export on several jobs back-to-back. Every one is accepted instantly, they run **one at a time** on the server, all of them survive a deploy, and the operator can close the tab and come back to finished exports.

**Architecture:** v850 already made a single export durable (an `ExportRun` row + a detached runner + heartbeat + stale-sweep reclaim). This adds the three things that make *several* safe:

1. **A dispatcher owns the only spawn path**, honouring a concurrency cap (default **1**). Nothing else may call `_spawn_export_runner`.
2. **Disk hygiene** — exports currently leak 50-150 MB each onto a 1 GB disk with no cleanup anywhere. Queueing several makes a full disk a matter of when, not if.
3. **A frontend that can hold more than one export** — today a single global `isExporting` flag blocks the second one outright.

**Tech Stack:** FastAPI, SQLAlchemy (Postgres prod / SQLite local), asyncio, vanilla JS. Tests: pytest from `code/`.

---

## Why the cap is 1 (do not raise it without measuring)

`render.yaml:11-12` — **2 GB RAM, 1 CPU**, a single gunicorn worker (`Dockerfile:71-80`; *"1 worker avoids duplicate memory (Whisper model ~300MB) which causes OOM"*).

A **single** export already sits at that ceiling. Every one of these is a shipped fix for a **one-export** OOM:

| Incident | Evidence |
|---|---|
| A second Whisper-small over the concat OOM-killed the box | `video_processor.py:5579-5591` — *"Render OOM-kills at the 2GB ceiling (verified 2026-05-11 with the donut-glaze v698A dual-output export)"* |
| Two parallel trim workers each loading Whisper-small → ~500 MB peak | `video_processor.py:5210-5216` (v691d) |
| Overlapping Whisper loads (speaker + master-audio) | `main.py:9502-9509` — *"That sequencing — not overlapping Whisper loads — was what triggered the v701r → OOM regression"* |
| 11× tiny-model load/unload churn → climbing RSS | `video_processor.py:5288-5293` (v701y) |
| 233 s runaway concat + `capture_output=True` speed pass | `main.py:9862-9868` (v692b) |

The box also runs **5 concurrent video-gen workers** (`MAX_JOB_WORKERS=5`, `render.yaml:108-109`), the image-platform watcher every 2 s, and video streaming to the browser.

**Two exports at once means two ffmpeg chains and two Whisper models on 1 CPU / 2 GB.** The code already treats that as a failure state, not a feature — `export_queue.py:29-30` and `main.py:8604-8606` both describe a double-run as the expensive accident the CAS exists to prevent. On 1 CPU, running two exports concurrently is also **not faster** than running them back to back.

`EXPORT_MAX_CONCURRENT` is an env var so it *can* be raised — but only after someone measures peak RSS during a real export on the Standard plan. Default stays 1.

---

## Failure modes considered (and where each is handled)

| # | Failure | Consequence | Handled by |
|---|---|---|---|
| 1 | N queued exports all spawn at once | 2 GB OOM, container killed, every in-flight export dies | Task 1 — dispatcher + cap; `_spawn_export_runner` becomes private to it |
| 2 | Deploy orphans N runs; boot sweep re-fires **all N** (`main.py:8827-8830` today) | OOM → restart → sweep → OOM: **crashloop** until `MAX_ATTEMPTS=3` burns every export | Task 1 — the sweeper only ever sets `state=queued`; it never spawns |
| 3 | Slot accounting counts DB `running` rows | A row left `running` by a dead container holds a slot **forever** → queue stalls permanently | Task 1 — slots count `_LOCAL_EXPORT_IDS` (this container's live tasks), never a DB count |
| 4 | The dispatcher task dies on an unhandled exception | Nothing ever runs again; every export sits `queued` silently | Task 1 — per-iteration try/except, loop never exits; the 60 s sweeper double-nudges it |
| 5 | Deploy overlap: old + new container both dispatch | Briefly 2 exports on one box | Accepted + logged. The old container is inside its SIGTERM grace and its ffmpeg is seconds from death. Documented, not defended against — defending needs a DB-level global lock, which then reintroduces #3 |
| 6 | Disk fills (1 GB, ~50-150 MB leaked per export, **no cleanup anywhere today**) | Every write on the box fails — exports, image platform, SQLite. Not just an export bug | Task 3 — delete source clips after trim on all paths, prune old artifacts, and refuse to start an export under a free-space floor |
| 7 | Export fails → its `output_dir` is never cleaned (`_do_export_final` only re-raises, `main.py:10159-10163`) | Leaks compound fastest exactly when things go wrong | Task 3 |
| 8 | Operator queues 10 by mistake | 10 × ~10 min = the box is busy for hours with no way out | Task 2 — cancel a `queued` run (not a `running` one) |
| 9 | Job deleted while its export is queued | `ExportRun` cascade-deletes; dispatcher picks a vanished id | Task 1 — dispatcher tolerates a missing row (already does: the runner returns if the row is gone) |
| 10 | Two exports finish at once in the browser | Job A's completion rips away job B's banner (`index.html:14743` removes `#floatingExportBanner` by id) | Task 4 — per-export DOM keyed by `export_id` |
| 11 | Page reload mid-export | `_exportId` is a local `const` (`index.html:14561`) — lost. The export finishes on the server and the user never sees it | Task 4 — persist active `{jobId, exportId}` to `localStorage`, reattach on load |
| 12 | Voice-clone chain collides across exports | `window._vcErrorMsg` (`index.html:14678`) and `window._masterAudioFilename` (`13873`) are single globals | Task 4 — scope per export |
| 13 | Two export modals stacked (pre-existing latent bug) | Duplicate DOM ids; the second modal's widgets are inert and the confirm handler reads the **first** modal's values | Task 4 — remove any existing modal before inserting |
| 14 | An export is queued for a job with no approved clips | The 400 (`main.py:8936` *"No valid clip files found"*) now surfaces late, as a `failed` run instead of an instant error | Task 2 — validate at enqueue time so it fails fast at the click |
| 15 | Heartbeat starves under load → false stale → double-run | Two ffmpeg on 1 CPU | Unchanged by this work, and **improved**: with a cap of 1 the CPU is no more contended than today. `STALE_AFTER_S=180` (6 missed ticks) stands |
| 16 | Same job exported twice | Duplicate 15-min ffmpeg | Already handled — per-job idempotent join (`main.py:10182-10189`). Task 2 must preserve it |
| 17 | A poisoned export retries forever | Wasted hours | Already handled — `MAX_ATTEMPTS=3` then `failed` |

**Explicitly out of scope:** raising the cap above 1 (needs a real RSS measurement first); a card-level export button in the kanban (operator chose modal-only); cancelling a *running* export (would need to kill the ffmpeg subprocess tree — a separate feature).

---

### Task 1: The dispatcher — one spawn path, one slot

**Files:**
- Modify: `code/export_queue.py` (add the cap + a pure `slots_free` helper)
- Modify: `code/main.py` (`_spawn_export_runner` → private; new `_export_dispatcher`; POST no longer spawns; `_sweep_stale_exports` no longer spawns)
- Test: `code/tests/test_export_queue.py`

- [ ] **Step 1: Write the failing test**

Append to `code/tests/test_export_queue.py`:

```python
# ---- v853 dispatcher ------------------------------------------------------
def test_max_concurrent_defaults_to_one():
    import export_queue as eq
    assert eq.MAX_CONCURRENT == 1


def test_slots_free_counts_down_from_the_cap():
    import export_queue as eq
    assert eq.slots_free(0, max_concurrent=1) == 1
    assert eq.slots_free(1, max_concurrent=1) == 0
    assert eq.slots_free(2, max_concurrent=1) == 0   # never negative
    assert eq.slots_free(1, max_concurrent=3) == 2


def test_only_the_dispatcher_spawns_runners():
    """v853: an unbounded spawn is how a deploy OOM-crashloops the box.

    The POST used to spawn on the request and the sweeper used to spawn every
    orphan it found (`for _rid in to_fire: _spawn_export_runner(_rid)`), so N
    queued exports became N simultaneous ffmpeg+Whisper runs on a 1-CPU/2GB
    box. Now the dispatcher is the ONLY caller.
    """
    src = _main_src()
    # the sweeper must only re-queue, never fire
    assert "for _rid in to_fire:" not in src
    # exactly one call site, inside the dispatcher
    assert src.count("_spawn_export_runner(") == 2   # the def + the dispatcher's one call
    assert "async def _export_dispatcher(" in src
    assert "_export_dispatcher_task" in src


def test_dispatcher_counts_local_tasks_not_db_rows():
    """A row left 'running' by a DEAD container must not hold a slot forever —
    that would stall the queue permanently. Slots are counted from this
    container's live asyncio tasks."""
    src = _main_src()
    assert "len(_LOCAL_EXPORT_IDS)" in src
```

- [ ] **Step 2: Run it, watch it fail**

`python -m pytest tests/test_export_queue.py -v` → expect failures on `MAX_CONCURRENT` / `slots_free` / `_export_dispatcher`.

- [ ] **Step 3: Add the cap + pure helper to `export_queue.py`**

```python
import os

# v853 — how many exports may run AT ONCE on this container.
#
# DEFAULT 1, and do not raise it without measuring peak RSS on the real box.
# Render standard = 2 GB RAM / 1 CPU (render.yaml:11-12), and a SINGLE export
# already lives at that ceiling: v691d, v701w, v701x, v701y, v701z and v692b
# are all fixes for ONE export OOMing. Two exports = two ffmpeg chains + two
# Whisper models on one CPU. It is also not FASTER on 1 CPU — just riskier.
# The box additionally runs 5 video-gen workers (MAX_JOB_WORKERS=5).
MAX_CONCURRENT = int(os.environ.get("EXPORT_MAX_CONCURRENT", "1"))


def slots_free(running_now: int, max_concurrent: int = MAX_CONCURRENT) -> int:
    """How many more exports this container may start right now."""
    return max(0, max_concurrent - running_now)
```

- [ ] **Step 4: Rewrite the spawn path in `main.py`**

Rename `_spawn_export_runner` to make its single-caller contract loud, and add the dispatcher above it:

```python
# ============================================================================
# v853 — THE DISPATCHER OWNS THE ONLY SPAWN PATH
# ----------------------------------------------------------------------------
# Pre-v853 two places fired runners directly: the POST (on the request) and the
# sweeper (`for _rid in to_fire: _spawn_export_runner(_rid)`). Neither had a
# cap. So N queued exports for N jobs became N simultaneous ffmpeg+Whisper runs
# on a 2 GB / 1 CPU box — and a deploy that orphaned N runs made the NEXT
# container fire all N on boot, OOM, restart, and do it again: a crashloop that
# only ended when MAX_ATTEMPTS burned every export.
#
# Now: the POST only INSERTS a queued row. The sweeper only RE-QUEUES. This
# loop is the one place a runner is ever spawned, and it will not start an
# export while another is running.
# ============================================================================
async def _export_dispatcher():
    """Start queued exports, oldest first, up to the concurrency cap."""
    while True:
        try:
            # Slots are counted from THIS container's live tasks, never from a
            # DB count of state='running'. A row stranded in 'running' by a
            # dead container would otherwise hold a slot forever and stall the
            # queue for good; the sweeper is what rescues those rows.
            free = _eq.slots_free(len(_LOCAL_EXPORT_IDS))
            if free > 0:
                next_ids = await asyncio.to_thread(_next_queued_export_ids, free)
                for _rid in next_ids:
                    print(f"[Export/v853] DISPATCH run={_rid[:8]} "
                          f"({len(_LOCAL_EXPORT_IDS)}/{_eq.MAX_CONCURRENT} slots in use)",
                          flush=True)
                    _spawn_export_runner(_rid)
        except Exception as e:
            # NEVER let the dispatcher die — if it does, every export sits
            # queued forever and nothing tells anyone.
            print(f"[Export/v853] dispatcher tick failed (non-fatal): {e}", flush=True)
        await asyncio.sleep(_eq.DISPATCH_INTERVAL_S)
```

Add `_next_queued_export_ids(limit)` (sync, called via `to_thread`): opens its own session, returns the ids of the oldest `state='queued'` runs by `created_at`, skipping any id already in `_LOCAL_EXPORT_IDS`, limited to `limit`. It only READS — the CAS claim inside `_export_runner` is still what actually takes the row, so a dispatcher tick that races the sweeper is harmless.

Add `DISPATCH_INTERVAL_S = 2` to `export_queue.py` (a 2 s pickup delay is invisible next to a 10-minute export, and it keeps the loop cheap).

**Then remove the two old spawn sites:**
- `POST /export-final` (`main.py` ~L10205): delete the `_spawn_export_runner(run.id)` call. The row is inserted `queued`; the dispatcher picks it up within 2 s. Keep the 202 response exactly as-is.
- `_sweep_stale_exports` (`main.py` ~L8827-8830): delete the `for _rid in to_fire: _spawn_export_runner(_rid)` loop. Reclaiming means setting `state=queued` — that is all. Keep returning the count for the log line, and reword it (it no longer "re-fires", it "re-queues").

- [ ] **Step 5: Wire the dispatcher into `lifespan`**

Next to `_export_sweeper_task` (`main.py` ~L653):

```python
    _export_dispatcher_task = _asyncio.create_task(_export_dispatcher())
    print(f"[App][Export/v853] export dispatcher started "
          f"(max {_eq.MAX_CONCURRENT} concurrent, {_eq.DISPATCH_INTERVAL_S}s tick)", flush=True)
```
Cancel it in the shutdown block alongside `_export_sweeper_task`.

- [ ] **Step 6: Verify**

```bash
python -m pytest tests/test_export_queue.py -v
python -c "import main; print('import ok')"
python -c "import main; print([r.path for r in main.app.routes if 'export' in r.path])"
```
Both `/api/jobs/{job_id}/export-final` and `/api/jobs/{job_id}/export-status` must still be registered.

- [ ] **Step 7: Extend `tests/check_export_resume.py` with the fan-out case**

Add a case proving the deploy-crashloop is gone: seed **4** orphaned runs (stale heartbeats), run `_sweep_stale_exports`, and assert it spawned **zero** runners and left all 4 `queued`. Then run one dispatcher tick with the cap at 1 and assert exactly **one** was spawned. Run it; paste the output.

- [ ] **Step 8: Commit**

```bash
git add export_queue.py main.py tests/test_export_queue.py tests/check_export_resume.py
git commit -m "v853: dispatcher owns the only spawn path; cap concurrent exports (default 1)"
```

---

### Task 2: Queue position, cancel, and fail-fast validation

**Files:**
- Modify: `code/main.py` (extend `ExportRun.to_dict` payload via the status route; new cancel route; enqueue-time validation)
- Modify: `code/models.py` (nothing structural — `to_dict` already carries `state`/`attempts`)
- Test: `code/tests/test_export_queue.py`

- [ ] **Step 1: Write the failing test**

```python
def test_status_payload_carries_queue_position():
    src = _main_src()
    assert "queue_position" in src


def test_cancel_route_exists_and_only_kills_queued_runs():
    """A running export cannot be cancelled — that needs killing the ffmpeg
    subprocess tree, which is a separate feature. A QUEUED one must be
    cancellable, or one mis-click blocks the line for 10 minutes."""
    src = _main_src()
    assert '@app.delete("/api/jobs/{job_id}/export-status/{export_id}")' in src
    assert "cannot cancel an export that is already running" in src.lower()


def test_enqueue_validates_clips_before_accepting():
    """Without this the 'No valid clip files found' 400 fires inside the runner
    minutes later, surfacing as a mysterious failed run instead of an instant
    error at the click."""
    src = _main_src()
    assert "_export_precheck(" in src
```

- [ ] **Step 2: Run, watch it fail**

- [ ] **Step 3: Implement**

**Queue position** — in the `/export-status` route, when `state == 'queued'`, compute `queue_position` = 1 + the count of `queued` runs with an earlier `created_at`; when `running`, position 0. Add it to the returned dict (build on top of `run.to_dict()`; do not change the model's `to_dict`, other callers rely on its shape).

**Cancel** — `DELETE /api/jobs/{job_id}/export-status/{export_id}`: ownership-checked via `get_user_job`. If the run is `queued` → set `state='failed'`, `error='Cancelled by the operator before it started'`, `finished_at=utcnow()`, return the run. If it is `running` → `409` with the message *"Cannot cancel an export that is already running"*. If terminal → return it unchanged (idempotent).

**Fail-fast validation** — extract the "is this job actually exportable?" check that today lives inside `_do_export_final` (the approved-clip lookup that ends in `HTTPException(400, "No valid clip files found")`, `main.py` ~L8931-8936) into `_export_precheck(db, job_id) -> None`, raising the same 400. Call it from the POST **before** inserting the row, and leave the in-runner check as-is (it is the real gate; the precheck is only there to fail at the click). Do not duplicate the clip-collection logic — the precheck may be a cheap count query, not the full `clip_info` build.

- [ ] **Step 4: Verify + commit**

```bash
python -m pytest tests/test_export_queue.py -v
python -c "import main; print('import ok')"
git add main.py tests/test_export_queue.py
git commit -m "v853: queue position, cancel a queued export, fail fast at enqueue"
```

---

### Task 3: Disk hygiene — the 1 GB disk will fill

**This is not optional.** `/app/data` is a **1 GB persistent disk** (`render.yaml:129-132`) shared by exports, the image platform, and the torch/HF caches. Every export leaves 50-150 MB on it permanently and **nothing ever cleans up** — no failure-path cleanup, no periodic sweeper (verified: the only `rmtree` of a job dir is on explicit job deletion, `main.py:1894`, `3746-3748`). Today you export one at a time and notice. Queue ten and a full disk takes the whole box down.

**Files:**
- Modify: `code/video_processor.py` (source-clip cleanup on all paths)
- Modify: `code/main.py` (artifact prune + a free-space floor before an export starts)
- Test: `code/tests/test_export_disk.py` (new)

- [ ] **Step 1: Write the failing test**

Create `code/tests/test_export_disk.py` covering the pure helpers you are about to add:
- `enough_disk_free(free_bytes, needed_bytes)` → False under the floor, True above it.
- `stale_export_artifacts(files, keep_newest, now)` → given a list of `(name, mtime)` for one job's output dir, returns the ones safe to delete: everything matching the known generated-artifact patterns (`final_export_*.mp4`, `final_export_*.mp3`, `final_broll_*.mp4`, `support_track_*.mp4`, `speaker_master_audio*`, `support_master.mp3`) **except the newest `keep_newest` exports and their siblings**. It must NEVER return a source clip (`clip_*.mp4`) that a running export might still need, and never a file newer than a safety age.

Assert the obvious traps: an empty list returns empty; `keep_newest=2` keeps two; a file that does not match any known pattern is never deleted.

- [ ] **Step 2: Run, watch it fail**

- [ ] **Step 3: Implement**

**(a) Source clips are deleted on every path.** `video_processor.py:5217-5222` unlinks downloaded clips after trim — but only on the `needs_trimming` branch and only when the path starts with `/app/data/outputs`. Make the unlink unconditional for clips under the outputs dir (they are re-downloadable from R2 at any time — `main.py:9184-9190` does exactly that), and put it in a `finally` so a failed export cleans up too.

**(b) Prune old artifacts per job.** A sync helper `prune_export_artifacts(job_id, keep_newest=2) -> int` using the pure `stale_export_artifacts`, called from `_export_runner` **after** a successful R2 upload (the files live in R2; the local copies are a cache). Log what it removed. Never delete the export that just completed.

**(c) Refuse to start under a free-space floor.** In `_export_runner`, before calling `_do_export_final`: `shutil.disk_usage(outputs_dir)`; if free space is under the floor (start at **250 MB**), first run the prune; if it is *still* under, fail the run with a clear error (*"Not enough disk space to export (X MB free). Old exports were pruned and it is still short — free space on the Render disk."*) rather than letting ffmpeg die halfway and leave a corrupt file. A clear failure beats a mysterious one.

- [ ] **Step 4: Verify**

```bash
python -m pytest tests/test_export_disk.py -v
python -c "import main, video_processor; print('import ok')"
```

- [ ] **Step 5: Commit**

```bash
git add video_processor.py main.py tests/test_export_disk.py
git commit -m "v853: disk hygiene - always clean source clips, prune old artifacts, refuse to export under a free-space floor"
```

---

### Task 4: Frontend — hold more than one export

Today the second export is blocked outright by one global flag (`index.html:13423` `let isExporting = false;` → `alert('Export already in progress')` at L13426-13429). Behind it, every export DOM id is a singleton and `doExport` **destroys any existing banner** before creating its own (`index.html:14400-14401`).

**Files:**
- Modify: `code/static/index.html`
- Test: `code/tests/test_export_queue.py` (frontend source-grep guards)

- [ ] **Step 1: Write the failing test**

```python
def test_frontend_allows_more_than_one_export():
    src = _index_src()
    assert "isExporting" not in src           # the global blocker is gone
    assert "Export already in progress" not in src


def test_frontend_banner_is_keyed_per_export():
    """One singleton #floatingExportBanner means job A's completion rips away
    job B's banner (the old code removed it by id)."""
    src = _index_src()
    assert "exportBanner_${_exportId}" in src or "data-export-id" in src


def test_frontend_persists_active_exports_for_reload():
    """_exportId was a local const — a reload lost it, the export finished on
    the server, and the operator never saw it."""
    src = _index_src()
    assert "activeExports" in src
    assert "localStorage" in src
```

- [ ] **Step 2: Run, watch it fail**

- [ ] **Step 3: Implement**

1. **Delete the global lock.** Remove `isExporting` (L13423), the `alert` guard (L13426-13429), and the `finally { isExporting = false; }` (L14900-14902). Replace the guard with a **per-job** one: if this job already has an active export in `activeExports`, re-attach to it (the backend already returns the in-flight run — the 202 idempotent join at `main.py:10182-10189`) instead of alerting.

2. **Per-export banner.** `doExport` must stop removing `#floatingExportBanner` (L14400-14401). Instead, render each export as a **row inside one persistent stack container** (`#exportBannerStack`), each row carrying `data-export-id`. Each row owns its own status text, progress bar and timer nodes, addressed *within that row* — never by a global `getElementById`. A completing export removes **only its own row**; the stack disappears when empty. The row shows the queue position while `state === 'queued'` ("Queued — 2nd in line"), the elapsed time while `running`, and the attempt counter when it is more than 1.

3. **Survive a reload.** Persist `{jobId, exportId}` per active export into `localStorage['activeExports']` right after the 202 lands; remove the entry when the run reaches `done` / `failed`. On page load, read the list and re-attach a poll loop + banner row for each. This is what makes "close the tab and come back" real.

4. **Scope the per-export globals.** `window._vcErrorMsg` (L14678/14685) and `window._masterAudioFilename` (L13873) must become per-export values (carry them on the export's own state object), or two concurrent exports overwrite each other's voice-clone error.

5. **Fix the stacked-modal bug** (pre-existing): `exportFinalVideo` inserts `#exportSettingsModal` (L13776) without removing an existing one, so two open modals share duplicate ids and the confirm handler reads the **first** modal's values. Remove any existing `#exportSettingsModal` before inserting.

6. **Cancel button** on a `queued` row → `DELETE /api/jobs/{jobId}/export-status/{exportId}` (Task 2), then drop the row.

7. **Keep the completion guard.** The success card still only renders when `selectedJobId === jobId` (L14761) — that behaviour is correct; an export for a job you are not looking at should not inject a card into the open job's panel. The banner row is how you see the others.

- [ ] **Step 4: Verify**

```bash
python -m pytest tests/test_export_queue.py -v
```
And prove the JS parses — extract the inline `<script>` blocks to temp `.js` files and `node --check` each. A stray brace breaks the whole UI and there is no build step.

- [ ] **Step 5: Commit**

```bash
git add static/index.html tests/test_export_queue.py
git commit -m "v853: queue several exports from the UI - per-export banner rows, reload-safe, cancel a queued one"
```

---

### Task 5: Prove it, then ship

- [ ] **Step 1: Full regression sweep**

```bash
python -m pytest tests/test_export_queue.py tests/test_export_disk.py tests/test_export_active_line.py tests/test_clip_new_columns.py tests/test_image_worker_upload_retry.py -v
python tests/check_export_resume.py
python -c "import main, models, export_queue, video_processor; print('imports ok')"
python -c "import main; print([r.path for r in main.app.routes if 'export' in r.path])"
```

- [ ] **Step 2: Push** (auto-deploys to Render)

⚠ SHARED TREE: another session commits and pushes in `code/`. Never leave `main` broken between commits — a foreign push can ship it at any moment.

- [ ] **Step 3: Production evidence — do NOT claim this works without it**

Ask the operator to queue **three** exports back-to-back and send the logs. Required:
- `[App][Export/v853] export dispatcher started (max 1 concurrent, 2s tick)`
- three `[Export/v850] QUEUED run=…` lines, then **one** `[Export/v853] DISPATCH run=… (0/1 slots in use)`
- the second `DISPATCH` only appears **after** the first run's `DONE` — never two `START` lines without a `DONE` between them
- the browser shows three banner rows: one running, two "Queued — Nth in line"
- a reload mid-run reattaches all three rows

Then the deploy case: queue three, hit Manual Deploy mid-run. Required: the old container requeues, the new one re-queues the orphans **without** firing them all, and the dispatcher restarts them **one at a time**.

Invoke `superpowers:verification-before-completion` before claiming done.

- [ ] **Step 4: Post-push review** — spawn `caveman:cavecrew-reviewer` on the v853 commit set.

---

### Task 6: Document

- [ ] **Step 1:** `code/template_reference.md` → `§v853` deep-dive: why the cap is 1 (cite the OOM history), the dispatcher as the sole spawn path, the sweeper-never-spawns invariant, slots counted from local tasks not DB rows, and the disk floor.
- [ ] **Step 2:** `wiki/patterns/conventions.md` → one row.
- [ ] **Step 3:** `wiki/log.md` → timeline entry, dated 2026-07-13, quoting the operator ("i want to be able to export multiple jobs at the same time").
- [ ] **Step 4:** `gbrain import wiki`.
- [ ] **Step 5:** Commit (code repo + wiki repo separately; do not bump the submodule pointer).

---

## Verification summary

| Claim | Evidence required |
|---|---|
| Only the dispatcher spawns | source test: `_spawn_export_runner(` appears exactly twice (def + dispatcher); no `for _rid in to_fire:` |
| A deploy can't OOM-crashloop | `check_export_resume.py`: 4 orphans → sweep spawns 0, all stay `queued`; one dispatcher tick starts exactly 1 |
| The queue can't stall forever | slots counted from `_LOCAL_EXPORT_IDS`, not a DB `running` count; dispatcher survives an exception |
| The disk can't silently fill | `test_export_disk.py` green; prune runs after upload; export refuses to start under the floor |
| Several exports work in the UI | `node --check` passes; three rows render; a reload reattaches |
| It actually works in prod | three queued exports serialise (one `DISPATCH` at a time), and a mid-run deploy resumes them one at a time |
