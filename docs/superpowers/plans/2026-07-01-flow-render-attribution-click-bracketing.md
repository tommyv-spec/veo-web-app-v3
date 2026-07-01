# Flow Render Attribution (Click-Bracketing) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bind each rendered Flow clip to the right job/clip by the time bracket between its own Generate click and the next click, eliminating the timeout-race + DOM `data-index` misattribution — while keeping the entire existing v700/v729/DOM stack as an untouched fallback.

**Architecture:** A new browser-free module `flow_attribution.py` holds all pure attribution state + math (click log, render ledger, bracket resolution) and is unit-tested in isolation. `flow_worker.py` feeds it two things it already receives (submit responses, status-poll bodies), stamps every Generate click, and — when the module attributes a render to a clip — writes the render id into the SAME `_PRIMARY_MEDIA_BINDINGS` map every existing consumer already reads. New path runs first; the old window/late-bind/DOM path is unchanged and only matters when the new path is silent. A kill-switch env flag reverts to pure-legacy instantly.

**Tech Stack:** Python 3.13, `pytest` for the pure module, Patchright (browser) untouched by tests.

**Design spec:** `code/docs/superpowers/specs/2026-07-01-flow-render-attribution-stability-design.md`

---

## File Structure

- **Create** `code/static/flow_attribution.py` — pure attribution: `RenderAttributor` (click log, ledger, bracket resolve, reconcile). No browser/network imports.
- **Create** `code/tests/test_flow_attribution.py` — pytest unit tests for the module.
- **Modify** `code/static/flow_worker.py` — instantiate one module-level `RenderAttributor`; stamp every Generate click; feed submit responses + status polls; write bindings when the module attributes; reconcile at job end; read the kill-switch flag.

Every task is TDD where the code is pure. Wiring tasks (into `flow_worker.py`) are verified by `import flow_worker` + the live-golden harness, since they touch browser paths.

**Convention for `flow_worker.py` edits:** anchor by function name + the existing unique call string shown in each task (line numbers drift as edits land). Always finish a wiring task with `PYTHONIOENCODING=utf-8 PYTHONUTF8=1 python -c "import flow_worker"` before commit (py_compile is insufficient per repo rule).

---

## Task 1: Pure module skeleton + click log

**Files:**
- Create: `code/static/flow_attribution.py`
- Test: `code/tests/test_flow_attribution.py`

- [ ] **Step 1: Write the failing test**

```python
# code/tests/test_flow_attribution.py
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "static"))
from flow_attribution import RenderAttributor


def test_stamp_click_records_ordered_entries_per_account():
    a = RenderAttributor()
    a.stamp_click("Account1", job_id="J", clip_index=0, clip_id="c0", now=100.0)
    a.stamp_click("Account1", job_id="J", clip_index=1, clip_id="c1", now=160.0)
    a.stamp_click("Account2", job_id="J", clip_index=0, clip_id="d0", now=110.0)
    log1 = a.click_log_for("Account1")
    assert [e["clip_index"] for e in log1] == [0, 1]
    assert [e["click_at"] for e in log1] == [100.0, 160.0]
    assert a.click_log_for("Account2")[0]["clip_id"] == "d0"
    assert a.click_log_for("Account1") is not a._click_log["Account1"]  # returns a copy
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd code && PYTHONUTF8=1 python -m pytest tests/test_flow_attribution.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'flow_attribution'`

- [ ] **Step 3: Write minimal implementation**

```python
# code/static/flow_attribution.py
"""Pure (browser-free) render attribution for the Flow worker.

Clip N owns exactly the renders that appear between its own Generate click and
the next Generate click on the SAME account (submits are sequential per account).
This module holds that state (click log + render ledger) and the bracket math.
flow_worker.py feeds it submit responses + status-poll bodies and writes the
resolved (render_id -> clip) into its existing _PRIMARY_MEDIA_BINDINGS map.

No imports from flow_worker — keep this unit-testable without booting Patchright.
"""
import threading


class RenderAttributor:
    def __init__(self, enabled=True):
        self.enabled = enabled
        self._click_log = {}   # account -> list[{click_at, job_id, clip_index, clip_id}]
        self._ledger = {}      # render_id -> {account, captured_at, create_time, status, batch_id, workflow_id, project_id}
        self._lock = threading.RLock()

    def stamp_click(self, account, job_id, clip_index, clip_id, now):
        """Record a Generate click. `now` = local wall-clock (time.time()) at click."""
        if not account:
            return
        entry = {"click_at": float(now), "job_id": job_id,
                 "clip_index": clip_index, "clip_id": clip_id}
        with self._lock:
            self._click_log.setdefault(account, []).append(entry)
            self._click_log[account].sort(key=lambda e: e["click_at"])

    def click_log_for(self, account):
        with self._lock:
            return [dict(e) for e in self._click_log.get(account, [])]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd code && PYTHONUTF8=1 python -m pytest tests/test_flow_attribution.py -q`
Expected: PASS (1 passed)

- [ ] **Step 5: Commit**

```bash
git add code/static/flow_attribution.py code/tests/test_flow_attribution.py
git commit -m "feat(attribution): pure module skeleton + per-account click log"
```

---

## Task 2: Bracket resolution

**Files:**
- Modify: `code/static/flow_attribution.py`
- Test: `code/tests/test_flow_attribution.py`

- [ ] **Step 1: Write the failing test**

```python
def test_bracket_for_returns_owning_click_entry():
    a = RenderAttributor()
    a.stamp_click("A", "J", 0, "c0", now=100.0)
    a.stamp_click("A", "J", 1, "c1", now=160.0)
    a.stamp_click("A", "J", 2, "c2", now=220.0)
    # inside clip 0's bracket [100,160)
    assert a.bracket_for("A", 130.0)["clip_index"] == 0
    # exactly on a boundary belongs to the later bracket (>= start)
    assert a.bracket_for("A", 160.0)["clip_index"] == 1
    # after the last click -> open-ended last bracket
    assert a.bracket_for("A", 999.0)["clip_index"] == 2
    # before the first click -> None (no owner)
    assert a.bracket_for("A", 50.0) is None
    # unknown account -> None
    assert a.bracket_for("ZZ", 130.0) is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd code && PYTHONUTF8=1 python -m pytest tests/test_flow_attribution.py::test_bracket_for_returns_owning_click_entry -q`
Expected: FAIL — `AttributeError: 'RenderAttributor' object has no attribute 'bracket_for'`

- [ ] **Step 3: Write minimal implementation**

Add to `RenderAttributor`:

```python
    def bracket_for(self, account, when):
        """The click-log entry whose bracket [click_at, next_click_at) contains
        `when`. Last entry's bracket is open-ended. Returns a copy or None if
        `when` precedes the first click / the account is unknown."""
        when = float(when)
        with self._lock:
            log = self._click_log.get(account) or []
            owner = None
            for e in log:  # sorted ascending by click_at
                if e["click_at"] <= when:
                    owner = e
                else:
                    break
            return dict(owner) if owner else None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd code && PYTHONUTF8=1 python -m pytest tests/test_flow_attribution.py -q`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add code/static/flow_attribution.py code/tests/test_flow_attribution.py
git commit -m "feat(attribution): click-bracket resolution"
```

---

## Task 3: Observe a render → attribute to its bracket clip

**Files:**
- Modify: `code/static/flow_attribution.py`
- Test: `code/tests/test_flow_attribution.py`

- [ ] **Step 1: Write the failing test**

```python
def test_observe_render_attributes_by_captured_at_bracket():
    a = RenderAttributor()
    a.stamp_click("A", "J", 0, "c0", now=100.0)
    a.stamp_click("A", "J", 1, "c1", now=160.0)
    # render captured at 135 -> clip 0's bracket
    b = a.observe_render("RID1", account="A", captured_at=135.0)
    assert b == {"job_id": "J", "clip_index": 0, "clip_id": "c0"}
    # a second variant of the same clip, captured later but still < next click
    b2 = a.observe_render("RID2", account="A", captured_at=155.0)
    assert b2["clip_index"] == 0
    # render for clip 1
    b3 = a.observe_render("RID3", account="A", captured_at=170.0)
    assert b3["clip_index"] == 1
    # ledger recorded all three with account + status default
    assert set(a.renders_for_clip("J", 0)) == {"rid1", "rid2"}
    assert a.renders_for_clip("J", 1) == ["rid3"]


def test_observe_render_uses_create_time_when_no_captured_at():
    a = RenderAttributor()
    a.stamp_click("A", "J", 0, "c0", now=100.0)
    a.stamp_click("A", "J", 1, "c1", now=160.0)
    # status-poll-only render: no captured_at, fall back to create_time epoch
    b = a.observe_render("RID9", account="A", create_time=150.0)
    assert b["clip_index"] == 0


def test_observe_render_returns_none_when_disabled_or_unbracketed():
    a = RenderAttributor(enabled=False)
    a.stamp_click("A", "J", 0, "c0", now=100.0)
    assert a.observe_render("RID", account="A", captured_at=130.0) is None  # disabled
    a2 = RenderAttributor()
    a2.stamp_click("A", "J", 0, "c0", now=100.0)
    assert a2.observe_render("RID", account="A", captured_at=50.0) is None  # pre-first-click
    # but the ledger still recorded it (for reconcile/backstop), even if unbound
    assert "rid" in a2._ledger
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd code && PYTHONUTF8=1 python -m pytest tests/test_flow_attribution.py -q`
Expected: FAIL — `AttributeError: … 'observe_render'`

- [ ] **Step 3: Write minimal implementation**

Add to `RenderAttributor`:

```python
    def observe_render(self, render_id, account, captured_at=None, create_time=None,
                       status=None, batch_id=None, workflow_id=None, project_id=None):
        """Record a render into the ledger and attribute it to the clip whose
        bracket contains it. Returns {job_id, clip_index, clip_id} if attributed,
        else None. Always records the ledger row (used by reconcile) even when
        unattributed or disabled. Idempotent per render_id (later status updates
        upsert; the binding, once found, is stable)."""
        if not render_id:
            return None
        rid = render_id.lower()
        when = captured_at if captured_at is not None else create_time
        with self._lock:
            row = self._ledger.get(rid, {})
            row.update({
                "account": account or row.get("account"),
                "captured_at": captured_at if captured_at is not None else row.get("captured_at"),
                "create_time": create_time if create_time is not None else row.get("create_time"),
                "status": status if status is not None else row.get("status"),
                "batch_id": batch_id if batch_id is not None else row.get("batch_id"),
                "workflow_id": workflow_id if workflow_id is not None else row.get("workflow_id"),
                "project_id": project_id if project_id is not None else row.get("project_id"),
            })
            self._ledger[rid] = row
            if not self.enabled or when is None:
                return None
            owner = self.bracket_for(account, when)
            if not owner:
                return None
            binding = {"job_id": owner["job_id"], "clip_index": owner["clip_index"],
                       "clip_id": owner["clip_id"]}
            row["bound"] = binding
            return dict(binding)

    def renders_for_clip(self, job_id, clip_index):
        """All render ids the ledger has attributed to this (job, clip)."""
        with self._lock:
            out = []
            for rid, row in self._ledger.items():
                b = row.get("bound")
                if b and b["job_id"] == job_id and b["clip_index"] == clip_index:
                    out.append(rid)
            return out
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd code && PYTHONUTF8=1 python -m pytest tests/test_flow_attribution.py -q`
Expected: PASS (5 passed)

- [ ] **Step 5: Commit**

```bash
git add code/static/flow_attribution.py code/tests/test_flow_attribution.py
git commit -m "feat(attribution): observe render + bracket attribution + ledger"
```

---

## Task 4: Status upsert + reconcile snapshot

**Files:**
- Modify: `code/static/flow_attribution.py`
- Test: `code/tests/test_flow_attribution.py`

- [ ] **Step 1: Write the failing test**

```python
def test_reconcile_reports_per_clip_status():
    a = RenderAttributor()
    a.stamp_click("A", "J", 0, "c0", now=100.0)
    a.stamp_click("A", "J", 1, "c1", now=160.0)
    a.observe_render("RID0", account="A", captured_at=130.0, status="MEDIA_GENERATION_STATUS_SCHEDULED")
    a.observe_render("RID1", account="A", captured_at=170.0, status="MEDIA_GENERATION_STATUS_SCHEDULED")
    # later status poll flips clip 0 -> SUCCESSFUL, clip 1 -> FAILED
    a.observe_render("RID0", account="A", status="MEDIA_GENERATION_STATUS_SUCCESSFUL")
    a.observe_render("RID1", account="A", status="MEDIA_GENERATION_STATUS_FAILED")
    rec = a.reconcile("J", [0, 1])
    assert rec[0]["state"] == "successful" and rec[0]["render_ids"] == ["rid0"]
    assert rec[1]["state"] == "failed"


def test_reconcile_flags_clip_with_no_render():
    a = RenderAttributor()
    a.stamp_click("A", "J", 0, "c0", now=100.0)
    rec = a.reconcile("J", [0, 1])
    assert rec[1]["state"] == "missing" and rec[1]["render_ids"] == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd code && PYTHONUTF8=1 python -m pytest tests/test_flow_attribution.py -q`
Expected: FAIL — `AttributeError: … 'reconcile'`

- [ ] **Step 3: Write minimal implementation**

Add to `RenderAttributor`:

```python
    # Flow mediaGenerationStatus -> coarse state
    _SUCCESS = "MEDIA_GENERATION_STATUS_SUCCESSFUL"
    _FAILED = "MEDIA_GENERATION_STATUS_FAILED"

    def reconcile(self, job_id, clip_indices):
        """Per-clip final view for the end-of-job safety net. For each clip index:
        {state: successful|failed|pending|missing, render_ids: [...]}. 'missing' =
        no render ever attributed (the new path saw nothing — caller falls back to
        the legacy path / redo)."""
        with self._lock:
            out = {}
            for ci in clip_indices:
                rids = [rid for rid, row in self._ledger.items()
                        if (row.get("bound") or {}).get("job_id") == job_id
                        and (row.get("bound") or {}).get("clip_index") == ci]
                if not rids:
                    out[ci] = {"state": "missing", "render_ids": []}
                    continue
                statuses = [self._ledger[r].get("status") for r in rids]
                if any(s == self._SUCCESS for s in statuses):
                    state = "successful"
                elif all(s == self._FAILED for s in statuses):
                    state = "failed"
                else:
                    state = "pending"
                out[ci] = {"state": state, "render_ids": rids}
            return out
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd code && PYTHONUTF8=1 python -m pytest tests/test_flow_attribution.py -q`
Expected: PASS (7 passed)

- [ ] **Step 5: Commit**

```bash
git add code/static/flow_attribution.py code/tests/test_flow_attribution.py
git commit -m "feat(attribution): status upsert + per-clip reconcile"
```

---

## Task 5: Purge on re-submit (redo/restore) + kill switch honored

**Files:**
- Modify: `code/static/flow_attribution.py`
- Test: `code/tests/test_flow_attribution.py`

- [ ] **Step 1: Write the failing test**

```python
def test_redo_click_opens_new_bracket_and_old_binding_is_dropped():
    a = RenderAttributor()
    a.stamp_click("A", "J", 0, "c0", now=100.0)
    a.stamp_click("A", "J", 1, "c1", now=160.0)
    a.observe_render("OLD", account="A", captured_at=130.0)      # clip 0, first attempt
    assert a.renders_for_clip("J", 0) == ["old"]
    # redo of clip 0 much later: purge its prior bindings, stamp a new click
    a.purge_clip("J", 0)
    a.stamp_click("A", "J", 0, "c0", now=300.0)
    assert a.renders_for_clip("J", 0) == []                       # old dropped
    a.observe_render("NEW", account="A", captured_at=305.0)       # redo render
    assert a.renders_for_clip("J", 0) == ["new"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd code && PYTHONUTF8=1 python -m pytest tests/test_flow_attribution.py -q`
Expected: FAIL — `AttributeError: … 'purge_clip'`

- [ ] **Step 3: Write minimal implementation**

Add to `RenderAttributor`:

```python
    def purge_clip(self, job_id, clip_index):
        """Drop bindings for a clip before it is re-submitted (redo / golden-restore
        resume), mirroring flow_worker's v700i purge so a fresh render wins. Ledger
        rows stay (for history) but lose their `bound` tag."""
        with self._lock:
            for row in self._ledger.values():
                b = row.get("bound")
                if b and b["job_id"] == job_id and b["clip_index"] == clip_index:
                    row.pop("bound", None)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd code && PYTHONUTF8=1 python -m pytest tests/test_flow_attribution.py -q`
Expected: PASS (8 passed)

- [ ] **Step 5: Commit**

```bash
git add code/static/flow_attribution.py code/tests/test_flow_attribution.py
git commit -m "feat(attribution): purge_clip for redo/restore re-submit"
```

---

## Task 6: Instantiate the attributor in flow_worker + kill switch

**Files:**
- Modify: `code/static/flow_worker.py` (near the other module-level attribution globals, around `_PRIMARY_MEDIA_BINDINGS` definition)

- [ ] **Step 1: Add the import + singleton**

Find the line defining `_PRIMARY_MEDIA_BINDINGS = {}` (currently ~L291). Immediately AFTER the `_PRIMARY_MEDIA_LOCK` line that follows it, add:

```python
# v800 — click-bracket attribution (see docs/.../2026-07-01-flow-render-attribution-*).
# Primary, windowless render->clip binding by the time bracket between consecutive
# Generate clicks per account. Writes into _PRIMARY_MEDIA_BINDINGS (below) so every
# existing consumer is unchanged; the legacy window/late-bind/DOM path stays as the
# fallback. Kill switch: FLOW_BRACKET_ATTRIBUTION=off -> pure legacy behaviour.
from flow_attribution import RenderAttributor
_BRACKET_ATTR_ENABLED = os.environ.get("FLOW_BRACKET_ATTRIBUTION", "on").strip().lower() != "off"
_RENDER_ATTRIBUTOR = RenderAttributor(enabled=_BRACKET_ATTR_ENABLED)
```

- [ ] **Step 2: Verify import**

Run: `cd code/static && PYTHONIOENCODING=utf-8 PYTHONUTF8=1 python -c "import flow_worker as f; print('enabled=', f._RENDER_ATTRIBUTOR.enabled)"`
Expected: prints `import` banners then `enabled= True`

- [ ] **Step 3: Commit**

```bash
git add code/static/flow_worker.py
git commit -m "feat(attribution): wire RenderAttributor singleton + kill switch into flow_worker"
```

---

## Task 7: Stamp every Generate click (incl. all redos)

**Files:**
- Modify: `code/static/flow_worker.py`

A helper that reads the click's account + clip and stamps the attributor. Placed so it fires on every submit path.

- [ ] **Step 1: Add the stamp helper**

After the `_RENDER_ATTRIBUTOR` definition (Task 6), add:

```python
def _stamp_generate_click(account_label, job_id, clip_index, clip_id=None):
    """Record a Generate click for click-bracket attribution. Safe/no-op if the
    attributor is disabled. Call at EVERY generate-click site (main, failover,
    policy-retry, redo, reuse, restore-resume)."""
    try:
        _RENDER_ATTRIBUTOR.stamp_click(account_label, job_id, clip_index, clip_id, time.time())
    except Exception as _e:
        print(f"[v800] stamp_click failed (non-fatal): {_e}", flush=True)
```

- [ ] **Step 2: Stamp inside the shared bind helper (covers main + policy paths)**

In `_bind_pending_submits_for_page` (def ~L1674), add a stamp at the TOP of the function body (before it calls `_bind_pending_submits`). Insert right after the docstring / first line:

```python
    _stamp_generate_click(account_label, job_id, clip_index, clip_id)
```

Note: confirm the parameter name for the account in this function's signature (`account_label`). If it differs, use the actual name. This single insertion covers call sites L5966, L6207, L16698, L18447, L19185, L19393.

- [ ] **Step 3: Stamp the redo path explicitly**

`process_redo_clip` (def ~L15193) submits via `rebuild_clip` and does NOT necessarily route through `_bind_pending_submits_for_page`. Right AFTER the point where the redo is known to be submitting (immediately before or after the `update_clip_status(clip_id, 'generating')` call near the top of `process_redo_clip`), add:

```python
    _stamp_generate_click("Flow", job_id, clip.get('clip_index'), clip_id)
```

Use the account label the redo runs under. If `process_redo_clip` has an `account_name` in scope, use it; otherwise `"Flow"` matches the single-account user-worker label used elsewhere in this function's logs. Verify by reading the function's existing `print(f"[REDO]...` lines for the label in use.

- [ ] **Step 4: Purge on re-submit**

Wherever `_bind_pending_submits` already purges (v700i, inside `_bind_pending_submits` at ~L1727) OR at the top of `process_redo_clip`, add a matching attributor purge so a redo drops the stale bracket binding:

```python
    _RENDER_ATTRIBUTOR.purge_clip(job_id, clip_index)
```

Place it next to the existing v700i purge log line inside `_bind_pending_submits` (use the `job_id` / `clip_index` already in scope there), AND at the top of `process_redo_clip` (use `clip.get('clip_index')`).

- [ ] **Step 5: Verify import + no crash on a dry construct**

Run: `cd code/static && PYTHONIOENCODING=utf-8 PYTHONUTF8=1 python -c "import flow_worker as f; f._stamp_generate_click('Account1','J',0,'c0'); print(f._RENDER_ATTRIBUTOR.click_log_for('Account1'))"`
Expected: prints `[{'click_at': <float>, 'job_id': 'J', 'clip_index': 0, 'clip_id': 'c0'}]`

- [ ] **Step 6: Commit**

```bash
git add code/static/flow_worker.py
git commit -m "feat(attribution): stamp every Generate click incl redo + purge on resubmit"
```

---

## Task 8: Feed submit responses into the ledger + write bindings

**Files:**
- Modify: `code/static/flow_worker.py`

The submit-response drain (`_bind_pending_submits`, ~L1748-1778) already iterates `_extract_media_bindings(data)` and writes `_PRIMARY_MEDIA_BINDINGS`. Add a parallel feed into the attributor using the response's local `captured_at`, and write any bracket-resolved binding into the SAME map (idempotent with the legacy write).

- [ ] **Step 1: Feed + bracket-write inside the drain loop**

Inside `_bind_pending_submits`, in the loop `for _b in _extract_media_bindings(data):` (after `media_id = _b['media_id']` and the existing `_PRIMARY_MEDIA_BINDINGS[media_id] = {...}` assignment ~L1768), add:

```python
                        _attr_binding = _RENDER_ATTRIBUTOR.observe_render(
                            media_id, account=account_label,
                            captured_at=cap_at or None,
                            batch_id=_b.get('batch_id'),
                            workflow_id=_b.get('workflow_name'),
                        )
                        if _attr_binding and media_id not in _PRIMARY_MEDIA_BINDINGS:
                            # bracket resolved a binding the legacy drain didn't set
                            _PRIMARY_MEDIA_BINDINGS[media_id] = {
                                'job_id': _attr_binding['job_id'],
                                'clip_index': _attr_binding['clip_index'],
                                'clip_id': _attr_binding['clip_id'],
                                'batch_id': _b.get('batch_id'),
                                'workflow_id': _b.get('workflow_name'),
                                'submit_time': cap_at or time.time(),
                                'account': account_label,
                                'via': 'bracket',
                            }
                            print(f"[v800] bracket-bind clip {_attr_binding['clip_index']} "
                                  f"← render {media_id[:8]} (captured_at bracket)", flush=True)
```

Note: this runs INSIDE the existing `with _PRIMARY_MEDIA_LOCK:` block (the drain loop holds it), so the map write is safe. `observe_render` takes its own lock — no deadlock (different lock objects, no nested acquisition of the same lock).

- [ ] **Step 2: Verify import**

Run: `cd code/static && PYTHONIOENCODING=utf-8 PYTHONUTF8=1 python -c "import flow_worker"`
Expected: import banners, no traceback.

- [ ] **Step 3: Commit**

```bash
git add code/static/flow_worker.py
git commit -m "feat(attribution): feed submit responses to ledger + bracket-write bindings"
```

---

## Task 9: Feed status polls into the ledger (backstop + status)

**Files:**
- Modify: `code/static/flow_worker.py`

`_scan_failure_reason` (def ~L1427) already parses `batchCheckAsyncVideoGenerationStatus` bodies. Add a pass that upserts every `media[]` entry (uuid + status + createTime) into the attributor, so (a) dropped-submit-response renders still get attributed by create_time, and (b) reconcile has fresh statuses.

- [ ] **Step 1: Add a media-list extractor helper**

Near `_failed_media_ids_in_status` (def ~L355), add:

```python
def _iter_status_media(data):
    """Yield (uuid, status, create_time_epoch, project_id) for each media entry in
    a batchCheckAsyncVideoGenerationStatus body. create_time parsed from ISO
    'createTime' to epoch seconds (None if absent/unparseable)."""
    import datetime as _dt
    def _epoch(s):
        if not s:
            return None
        try:
            return _dt.datetime.fromisoformat(str(s).replace('Z', '+00:00')).timestamp()
        except Exception:
            return None
    def _walk(node, proj):
        if isinstance(node, dict):
            proj = node.get('projectId', proj)
            if 'media' in node and isinstance(node['media'], list):
                for m in node['media']:
                    if not isinstance(m, dict):
                        continue
                    uuid = m.get('name')
                    meta = m.get('mediaMetadata') or {}
                    st = ((meta.get('mediaStatus') or {}).get('mediaGenerationStatus'))
                    ct = _epoch(meta.get('createTime'))
                    if uuid:
                        yield (uuid, st, ct, m.get('projectId', proj))
            for v in node.values():
                yield from _walk(v, proj)
        elif isinstance(node, list):
            for v in node:
                yield from _walk(v, proj)
    yield from _walk(data, None)
```

- [ ] **Step 2: Call it from `_scan_failure_reason`**

Inside `_scan_failure_reason`, after the existing terminal-reason recording block (after the `for _r in _VIDEO_POLICY_TERMINAL_REASONS:` loop ~L1489), add:

```python
    # v800 — feed the status poll into the click-bracket attributor: upsert status
    # for known renders + attribute dropped-response renders by create_time.
    try:
        for _uuid, _st, _ct, _proj in _iter_status_media(data):
            _ab = _RENDER_ATTRIBUTOR.observe_render(
                _uuid, account=(buf_key.split('acct:')[-1] if buf_key else None),
                create_time=_ct, status=_st, project_id=_proj)
            if _ab and _uuid.lower() not in _PRIMARY_MEDIA_BINDINGS:
                with _PRIMARY_MEDIA_LOCK:
                    _PRIMARY_MEDIA_BINDINGS[_uuid.lower()] = {
                        'job_id': _ab['job_id'], 'clip_index': _ab['clip_index'],
                        'clip_id': _ab['clip_id'], 'submit_time': time.time(),
                        'account': (buf_key.split('acct:')[-1] if buf_key else None),
                        'via': 'bracket-status'}
                print(f"[v800] bracket-bind (status) clip {_ab['clip_index']} ← render {_uuid[:8]}", flush=True)
    except Exception as _e:
        print(f"[v800] status feed failed (non-fatal): {_e}", flush=True)
```

Note: confirm `_scan_failure_reason`'s parameter that carries the account key is named `buf_key` (per its call `_scan_failure_reason(resp, url, buf_key)` ~L1518). If different, use the actual name.

- [ ] **Step 3: Verify import + extractor on the captured sample**

Run:
```bash
cd code/static && PYTHONIOENCODING=utf-8 PYTHONUTF8=1 python -c "
import flow_worker as f, json
sample={'media':[{'name':'abc','projectId':'P','mediaMetadata':{'createTime':'2026-07-01T17:28:23.7Z','mediaStatus':{'mediaGenerationStatus':'MEDIA_GENERATION_STATUS_SUCCESSFUL'}}}]}
print(list(f._iter_status_media(sample)))
"
```
Expected: `[('abc', 'MEDIA_GENERATION_STATUS_SUCCESSFUL', <float epoch>, 'P')]`

- [ ] **Step 4: Commit**

```bash
git add code/static/flow_worker.py
git commit -m "feat(attribution): feed status polls to ledger (backstop + status)"
```

---

## Task 10: End-of-job reconcile safety net

**Files:**
- Modify: `code/static/flow_worker.py`

Add a reconcile pass at the end of `process_job_submission` (and the failover variant) that logs any clip the new path never attributed (so the operator sees coverage) and, for a `failed`/`missing` clip that the legacy path also left un-downloaded, ensures it routes to redo/fail via the existing helpers.

- [ ] **Step 1: Add the reconcile helper**

After `_stamp_generate_click`, add:

```python
def _reconcile_job_attribution(job_id, clips, http_enqueued_clips=None):
    """v800 safety net — log the attributor's final per-clip view at job end. This
    is observational + a redo trigger for clips the new AND legacy paths both
    missed. Never overrides a clip already enqueued/downloaded."""
    try:
        idxs = [c.get('clip_index') for c in clips if c.get('clip_index') is not None]
        rec = _RENDER_ATTRIBUTOR.reconcile(job_id, idxs)
        for ci, info in sorted(rec.items()):
            already = http_enqueued_clips and ci in http_enqueued_clips
            print(f"[v800-reconcile] clip {ci}: {info['state']} "
                  f"renders={[r[:8] for r in info['render_ids']]} "
                  f"{'(already handled)' if already else ''}", flush=True)
        return rec
    except Exception as _e:
        print(f"[v800-reconcile] failed (non-fatal): {_e}", flush=True)
        return {}
```

- [ ] **Step 2: Call it at job end**

In `process_job_submission` (def ~L17274), find the block that prints `ALL CLIPS ATTEMPTED!` (near the end of the submission loop). Immediately after that print, add:

```python
        _reconcile_job_attribution(job_id, clips, http_enqueued_clips)
```

Confirm `clips`, `job_id`, and `http_enqueued_clips` are in scope at that point (they are — used throughout the function). Do the same in `process_job_submission_with_failover` (def ~L15819) at its equivalent end-of-submission point; if `http_enqueued_clips` is not a variable there, pass `None`.

- [ ] **Step 3: Verify import**

Run: `cd code/static && PYTHONIOENCODING=utf-8 PYTHONUTF8=1 python -c "import flow_worker as f; print(callable(f._reconcile_job_attribution))"`
Expected: import banners then `True`

- [ ] **Step 4: Commit**

```bash
git add code/static/flow_worker.py
git commit -m "feat(attribution): end-of-job reconcile safety-net log + redo trigger"
```

---

## Task 11: Live-golden end-to-end verification

**Files:**
- Modify: `code/../scratchpad/e2e_video_worker.py` (reuse the harness; add a bracket-attribution assertion) — or a new `scratchpad/e2e_attribution.py`.

This task is manual/operator-driven (needs the live golden + a real generate). It confirms the wiring against production reality per the repo's "verification before done" rule.

- [ ] **Step 1: Drive a 1-clip generate with the attributor active**

Reuse `e2e_video_worker.py` (single session, copies the live golden). Before `rebuild_clip`, call `fw._stamp_generate_click("e2e", "JOBTEST", 0, "c0")` right after the Generate click is issued (or rely on the wired `_bind_pending_submits_for_page` stamp if the harness drives the full path). After generation, print `fw._RENDER_ATTRIBUTOR.renders_for_clip("JOBTEST", 0)`.

- [ ] **Step 2: Run + capture evidence**

Run: `cd scratchpad && PYTHONIOENCODING=utf-8 PYTHONUTF8=1 python e2e_video_worker.py 2>&1 | grep -E "v800|bracket-bind|renders_for_clip|VERDICT"`
Expected: at least one `[v800] bracket-bind clip 0 ← render <id>` line and the clip's render id listed. VERDICT PASS (video rendered).

- [ ] **Step 3: Confirm legacy fallback still works with kill switch off**

Run the same with `FLOW_BRACKET_ATTRIBUTION=off` prefixed. Expected: NO `[v800] bracket-bind` lines, video still renders + downloads (pure-legacy path), VERDICT PASS.

- [ ] **Step 4: Record evidence + commit any harness change**

```bash
git add scratchpad/e2e_video_worker.py
git commit -m "test(attribution): live-golden bracket-bind assertion + kill-switch check"
```

---

## Task 12: Deploy, review, verify in production

**Files:** none (process)

- [ ] **Step 1: Full unit run**

Run: `cd code && PYTHONUTF8=1 python -m pytest tests/test_flow_attribution.py -q`
Expected: all pass (8+).

- [ ] **Step 2: Import gate**

Run: `cd code/static && PYTHONIOENCODING=utf-8 PYTHONUTF8=1 python -c "import flow_worker"`
Expected: no traceback.

- [ ] **Step 3: Push + bump submodule**

```bash
cd code && git push origin main
cd .. && git add code && git commit -m "bump code submodule -> v800 click-bracket attribution"
```

- [ ] **Step 4: Spawn reviewer**

Spawn `caveman:cavecrew-reviewer` on the commit range (per repo rule: after every push to code main).

- [ ] **Step 5: Operator-side production evidence**

After deploy lands (2-3 min on Render), the operator runs a real multi-clip job (with at least one redo). Confirm in logs: `[v800] bracket-bind` for each clip, `[v800-reconcile]` shows all clips `successful`, and no `legacy tile-position attribution` WARN drove a download. Only THEN claim the stability fix landed. If a clip binds via legacy/DOM, capture that log line and open a follow-up — do not claim done.

---

## Self-Review

**Spec coverage:**
- Component 1 click log → Task 1. Component 2 ledger → Task 3 (+ status feed Task 9). Component 3 bracket bind → Tasks 2-3, wired Tasks 8-9. Component 4 old stack as fallback → additive design; no legacy code modified (only added-after writes guarded by `media_id not in _PRIMARY_MEDIA_BINDINGS`). Component 5 every-path incl redo → Task 7 (shared helper + explicit redo stamp + purge). Component 6 reconcile → Tasks 4 + 10. Component 7 kill switch → Task 6. Testing → Tasks 1-5 (unit), 11 (live), 12 (prod). All covered.

**Placeholder scan:** No TBD/TODO. Every code step shows complete code. The two "confirm the parameter name" notes (Tasks 7/9) are verification instructions, not missing code — the insertion code is fully given; the reader confirms one identifier against the live signature.

**Type consistency:** `stamp_click(account, job_id, clip_index, clip_id, now)`, `observe_render(render_id, account, captured_at, create_time, status, batch_id, workflow_id, project_id) -> {job_id,clip_index,clip_id}|None`, `bracket_for(account, when)`, `renders_for_clip(job_id, clip_index)`, `reconcile(job_id, clip_indices) -> {ci:{state,render_ids}}`, `purge_clip(job_id, clip_index)`, `click_log_for(account)`. Names used consistently across Tasks 6-11. Binding dict keys (`job_id`/`clip_index`/`clip_id`) match `_PRIMARY_MEDIA_BINDINGS` existing shape.

**Fallback safety:** every new write is guarded `if … not in _PRIMARY_MEDIA_BINDINGS`, so the new path never overwrites a legacy binding — worst case it adds a binding the legacy path would also have added. Kill switch neutralizes all writes (attributor returns None when disabled).
