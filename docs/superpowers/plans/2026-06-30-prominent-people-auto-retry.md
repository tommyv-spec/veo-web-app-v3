# Prominent-People Image Auto-Retry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When a clip's start_frame is rejected by Flow for prominent-people/celebrity, the platform automatically retries with a substitute image per an account-level setting (A = next image, B = previous image, C = all other batch images), marks the card with what happened, and shows the originally-intended image.

**Architecture:** Reuse the existing replace-image swap path (`Clip.start_frame` → new key → `FLOW_REDO_QUEUED` → worker redo poll). Add (1) a per-account settings store, (2) a distinct error code so prominent-people is separable from generic content-policy, (3) a server-side auto-substitute trigger inside the worker `policy-violation` report, (4) per-clip audit state, (5) frontend settings UI + card marking. Substitutes are existing job frame keys (already in R2), so no re-upload.

**Tech Stack:** FastAPI + SQLAlchemy (`main.py`, `models.py`, `image_platform.py`), Flow worker (`static/flow_worker.py`), vanilla JS frontend (`static/index.html`). Production auto-deploys to Render on push to `main` — verification discipline per project `CLAUDE.md` §2 applies (diagnostic log + operator evidence, not "should work").

---

## Decisions locked (all confirmed by operator)

- **Scope:** prominent-people / celebrity ONLY. Other rejections (generic content-policy, safety) keep the existing manual-upload card.
- **Default mode:** C (all other batch images) when the job came from an image job; falls back to A (next) when there is no batch.
- **A / B = single substitute.** One auto-attempt (the immediate next / previous image); if that substitute is also rejected → manual card.
- **C = FULL SWEEP (confirmed: "try all the images").** Try every other image in the batch, ONE render at a time, until one passes or all are exhausted → manual card. Not single-shot.

### Double-check findings (verified firsthand in `main.py`)

1. **Attempt cap does NOT limit the sweep.** `_swap_clip_start_frame` (and the existing `replace-image` it's factored from, L4877-4892) sets `FLOW_REDO_QUEUED` + `approval_status='pending_review'` but does **not** bump `generation_attempt`. The 3-attempt `max_attempts` gate (L6156) lives only in the *manual user-redo* path (L6031-6196). So image-substitution retries are uncapped by attempts — C can sweep all N images even when N > 3. Resetting `approval_status` to `pending_review` in the swap also clears any prior `max_attempts` flag.
2. **The sweep is event-driven, not a blocking loop.** Each render the worker attempts on a substitute that is *also* rejected fires another `policy-violation` report → `_auto_image_retry` runs again → picks the next untried frame → swaps → `FLOW_REDO_QUEUED`. The `tried` list persists in `Clip.auto_image_retry_json` across reports, so no frame is reused and the loop terminates when `pick_substitute` returns `None` (all tried). One Veo render per substitute.
3. **Credit cost is the operator's explicit choice.** A full C sweep where every image trips prominent-people will burn one Veo render per image before yielding to the manual card. Acceptable per "try all the images"; the `[v815]` diagnostic logs each substitution so the cost is visible.
4. **No donor-clobber.** The prominent-people branch (Task 5 Step 3) returns BEFORE the generic `CONTENT_POLICY_VIOLATION` + v701e preemptive sibling cascade, so swapping to an image shared by other clips never marks those siblings; each clip sweeps independently.

---

## File Structure

| File | Change | Responsibility |
|---|---|---|
| `models.py` | Modify (`User`, `Clip`) | Add `User.settings_json` (TEXT) + `Clip.auto_image_retry_json` (TEXT). Migration helpers. |
| `main.py` | Modify | Settings GET/PUT endpoints; `PolicyViolationRequest` gains `error_reason`; distinct `PROMINENT_PEOPLE_FILTER` code; auto-retry trigger in BOTH worker policy-violation endpoints; shared `_swap_clip_start_frame` helper; candidate + picker helpers; expose audit + setting in serialization. |
| `static/flow_worker.py` | Modify | Pass the prominent-people reason through `report_policy_violation`. |
| `static/index.html` | Modify | Settings UI (4 options: off / next / prev / batch); card marking "image rejected → used image X" + original-intended thumbnail. |
| `tests/test_auto_image_retry.py` | Create | Unit tests for pure helpers (candidate ordering, substitute picker, setting parse). |

New constants (define once in `main.py`, near `IMAGE_ATTRIBUTABLE_ERROR_CODES` ~L4387):
```python
# v815 — prominent-people / celebrity codes that trigger image auto-retry.
PROMINENT_PEOPLE_ERROR_CODES = frozenset({
    "PROMINENT_PEOPLE_FILTER",   # v815 — distinct code stamped when the worker
                                 # reports a PROMINENT_PEOPLE upload rejection
    "CELEBRITY_FILTER",
    "CELEBRITY_RAI_FILTER",
})
```
`PROMINENT_PEOPLE_FILTER` must be added to `IMAGE_ATTRIBUTABLE_ERROR_CODES` too (so the manual replace-image card still works when auto-retry is off or exhausted).

Auto-retry audit shape (`Clip.auto_image_retry_json`, JSON string):
```json
{
  "original_frame": "jobs/<jid>/frames/image_03.png",
  "used_frame":     "jobs/<jid>/frames/image_04.png",
  "tried":          ["jobs/<jid>/frames/image_03.png"],
  "count":          1,
  "mode":           "next"
}
```
`original_frame` = the FIRST rejected image (what the build intended). `used_frame` = the substitute currently rendering. `tried` = every key already rejected (so a C sweep never reuses one). `count` = auto-substitutions performed. `mode` = strategy used (for the card copy).

Account setting shape (`User.settings_json`, JSON string):
```json
{ "auto_image_retry": { "mode": "batch" } }
```
`mode` ∈ `"off" | "next" | "prev" | "batch"`. Absent/unparseable → default `"batch"`.

---

### Task 1: Settings store — `User.settings_json` column + migration

**Files:**
- Modify: `models.py` (`User` model + the migration runner that adds missing columns)

- [ ] **Step 1: Add the column to the User model**

In `models.py`, in the `User` class (~L30-64), add after the existing columns:
```python
    # v815 — per-account settings JSON blob. Currently holds
    # {"auto_image_retry": {"mode": "off|next|prev|batch"}}. Nullable;
    # absent = defaults applied in code.
    settings_json = Column(Text, nullable=True)
```

- [ ] **Step 2: Add the SQLite/Postgres migration**

Find the column-add migration block (the same pattern used for `image_nodes.batch_id`, e.g. `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`). Add:
```python
        ("users", "settings_json",
         "ALTER TABLE users ADD COLUMN IF NOT EXISTS settings_json TEXT"),
```
(Match the exact tuple/format the surrounding migration list uses; for the SQLite variant omit `IF NOT EXISTS` if that's the established pattern.)

- [ ] **Step 3: Verify the model imports**

Run: `cd code && python -c "import models; print('models OK')"`
Expected: `models OK` (no ImportError, no SQLAlchemy mapper error).

- [ ] **Step 4: Commit**

```bash
git add models.py
git commit -m "v815: add User.settings_json for per-account settings"
```

---

### Task 2: Settings read/write endpoints

**Files:**
- Modify: `main.py` (new endpoints, near other `/api/user` or auth routes)
- Test: `tests/test_auto_image_retry.py`

- [ ] **Step 1: Write the failing test for setting parse/default**

Create `tests/test_auto_image_retry.py`:
```python
from main import parse_auto_image_retry_mode

def test_default_mode_is_batch_when_absent():
    assert parse_auto_image_retry_mode(None) == "batch"
    assert parse_auto_image_retry_mode("") == "batch"
    assert parse_auto_image_retry_mode("not json") == "batch"
    assert parse_auto_image_retry_mode('{}') == "batch"

def test_explicit_mode_is_returned():
    assert parse_auto_image_retry_mode('{"auto_image_retry":{"mode":"next"}}') == "next"
    assert parse_auto_image_retry_mode('{"auto_image_retry":{"mode":"off"}}') == "off"

def test_invalid_mode_falls_back_to_batch():
    assert parse_auto_image_retry_mode('{"auto_image_retry":{"mode":"bogus"}}') == "batch"
```

- [ ] **Step 2: Run it, verify it fails**

Run: `cd code && python -m pytest tests/test_auto_image_retry.py -v`
Expected: FAIL — `ImportError: cannot import name 'parse_auto_image_retry_mode'`.

- [ ] **Step 3: Implement the parser + endpoints**

In `main.py` add the pure helper (near the v815 constants):
```python
import json as _json807

_VALID_RETRY_MODES = {"off", "next", "prev", "batch"}

def parse_auto_image_retry_mode(settings_json: str | None) -> str:
    """v815 — extract the account's auto-image-retry mode. Defaults to
    'batch' (mode C) when missing/invalid. Never raises."""
    if not settings_json:
        return "batch"
    try:
        data = _json807.loads(settings_json)
        mode = (data.get("auto_image_retry") or {}).get("mode")
    except Exception:
        return "batch"
    return mode if mode in _VALID_RETRY_MODES else "batch"
```
Add the endpoints:
```python
@app.get("/api/user/settings")
async def get_user_settings(current_user: User = Depends(get_current_user)):
    return {"auto_image_retry": {"mode": parse_auto_image_retry_mode(current_user.settings_json)}}

class UserSettingsRequest(BaseModel):
    auto_image_retry_mode: str  # off | next | prev | batch

@app.put("/api/user/settings")
async def put_user_settings(
    req: UserSettingsRequest,
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    if req.auto_image_retry_mode not in _VALID_RETRY_MODES:
        raise HTTPException(400, f"invalid mode {req.auto_image_retry_mode!r}")
    try:
        data = _json807.loads(current_user.settings_json) if current_user.settings_json else {}
    except Exception:
        data = {}
    data.setdefault("auto_image_retry", {})["mode"] = req.auto_image_retry_mode
    current_user.settings_json = _json807.dumps(data)
    db.commit()
    return {"ok": True, "auto_image_retry": {"mode": req.auto_image_retry_mode}}
```

- [ ] **Step 4: Run the test, verify it passes**

Run: `cd code && python -m pytest tests/test_auto_image_retry.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add main.py tests/test_auto_image_retry.py
git commit -m "v815: account settings endpoints + auto-retry mode parser"
```

---

### Task 3: `Clip.auto_image_retry_json` column + migration

**Files:**
- Modify: `models.py` (`Clip` model + migration + `to_dict`)

- [ ] **Step 1: Add the column to Clip**

In `models.py` Clip class (~L277-410), after `replacement_start_frame`:
```python
    # v815 — auto-image-retry audit. JSON: {original_frame, used_frame,
    # tried:[...], count, mode}. Set when prominent-people auto-retry swaps
    # the start_frame. Drives the "image rejected -> used image X" card mark.
    auto_image_retry_json = Column(Text, nullable=True)
```

- [ ] **Step 2: Add the migration tuple**

```python
        ("clips", "auto_image_retry_json",
         "ALTER TABLE clips ADD COLUMN IF NOT EXISTS auto_image_retry_json TEXT"),
```

- [ ] **Step 3: Expose it in Clip.to_dict (or wherever clips serialize for the frontend)**

Find the Clip serialization used by the clips list endpoint (search for `replacement_start_frame` in the dict the frontend consumes — likely `Clip.to_dict` in `models.py` and/or an assembler in `main.py`). Add:
```python
            "auto_image_retry": (
                json.loads(self.auto_image_retry_json) if self.auto_image_retry_json else None
            ),
```
(If serialization happens in `main.py` not `to_dict`, add it there instead — grep for where `replacement_start_frame` is put into the clip payload and mirror it.)

- [ ] **Step 4: Verify import**

Run: `cd code && python -c "import models; print('models OK')"`
Expected: `models OK`.

- [ ] **Step 5: Commit**

```bash
git add models.py
git commit -m "v815: add Clip.auto_image_retry_json audit column + serialization"
```

---

### Task 4: Candidate resolution + substitute picker (pure helpers, TDD)

**Files:**
- Modify: `main.py` (helpers)
- Test: `tests/test_auto_image_retry.py`

Candidate universe = the ordered list of DISTINCT `start_frame` keys across the job's clips, ordered by `clip_index`. These keys already live under `jobs/{job_id}/frames/` (materialized at prepare time, so for an image-job promotion they ARE the batch's scene images). No batch query required; works for non-image-job jobs too.

- [ ] **Step 1: Write failing tests**

Append to `tests/test_auto_image_retry.py`:
```python
from main import order_distinct_frames, pick_substitute

def test_order_distinct_frames_dedupes_preserving_clip_order():
    clips = [
        {"clip_index": 0, "start_frame": "a"},
        {"clip_index": 1, "start_frame": "b"},
        {"clip_index": 2, "start_frame": "a"},   # reused
        {"clip_index": 3, "start_frame": "c"},
    ]
    assert order_distinct_frames(clips) == ["a", "b", "c"]

def test_pick_next_returns_following_frame():
    frames = ["a", "b", "c"]
    assert pick_substitute("next", frames, original="a", tried=["a"]) == "b"

def test_pick_prev_returns_preceding_frame():
    frames = ["a", "b", "c"]
    assert pick_substitute("prev", frames, original="b", tried=["b"]) == "a"

def test_pick_next_at_end_returns_none():
    frames = ["a", "b"]
    assert pick_substitute("next", frames, original="b", tried=["b"]) is None

def test_pick_prev_at_start_returns_none():
    frames = ["a", "b"]
    assert pick_substitute("prev", frames, original="a", tried=["a"]) is None

def test_pick_batch_returns_first_untried_other():
    frames = ["a", "b", "c"]
    assert pick_substitute("batch", frames, original="a", tried=["a"]) == "b"
    assert pick_substitute("batch", frames, original="a", tried=["a", "b"]) == "c"
    assert pick_substitute("batch", frames, original="a", tried=["a", "b", "c"]) is None

def test_off_returns_none():
    assert pick_substitute("off", ["a", "b"], original="a", tried=["a"]) is None
```

- [ ] **Step 2: Run, verify fail**

Run: `cd code && python -m pytest tests/test_auto_image_retry.py -v`
Expected: FAIL — `ImportError` for `order_distinct_frames` / `pick_substitute`.

- [ ] **Step 3: Implement the helpers**

In `main.py`:
```python
def order_distinct_frames(clips: list) -> list:
    """v815 — distinct start_frame keys in clip_index order. Accepts dicts
    or Clip objects (duck-typed on .get / attribute)."""
    def _idx(c): return c["clip_index"] if isinstance(c, dict) else c.clip_index
    def _sf(c):  return c["start_frame"] if isinstance(c, dict) else c.start_frame
    seen, out = set(), []
    for c in sorted(clips, key=_idx):
        sf = _sf(c)
        if sf and sf not in seen:
            seen.add(sf); out.append(sf)
    return out

def pick_substitute(mode: str, frames: list, original: str, tried: list) -> str | None:
    """v815 — choose the next substitute frame for the given mode.
    A/B are single-step neighbors; batch (C) returns the first untried OTHER
    frame (caller loops it as a bounded sweep). Returns None when no
    candidate is available (auto-retry then yields to the manual card)."""
    if mode == "off" or not frames or original not in frames:
        return None
    i = frames.index(original)
    if mode == "next":
        cand = frames[i + 1] if i + 1 < len(frames) else None
        return cand if cand and cand not in tried else None
    if mode == "prev":
        cand = frames[i - 1] if i - 1 >= 0 else None
        return cand if cand and cand not in tried else None
    if mode == "batch":
        for cand in frames:
            if cand != original and cand not in tried:
                return cand
        return None
    return None
```

- [ ] **Step 4: Run, verify pass**

Run: `cd code && python -m pytest tests/test_auto_image_retry.py -v`
Expected: all passed.

- [ ] **Step 5: Commit**

```bash
git add main.py tests/test_auto_image_retry.py
git commit -m "v815: candidate-frame ordering + substitute picker helpers"
```

---

### Task 5: Shared start_frame swap helper + auto-retry trigger

**Files:**
- Modify: `main.py` (refactor `replace_clip_image` swap into a helper; add `_auto_image_retry` and call it from BOTH worker policy-violation endpoints)

- [ ] **Step 1: Extract the swap into a reusable helper**

In `main.py`, factor the `Clip.start_frame` swap + status reset (currently inline in `replace_clip_image` ~L4877-4891) into:
```python
def _swap_clip_start_frame(clip, new_key: str):
    """v815 — point a clip at a new start_frame R2 key and re-queue it for
    the worker redo poll. Shared by manual replace-image + auto-retry."""
    clip.start_frame = new_key
    clip.error_code = None
    clip.error_message = None
    clip.status = ClipStatus.FLOW_REDO_QUEUED.value
    clip.approval_status = "pending_review"
    clip.claimed_by_worker = None
    clip.claimed_at = None
```
Then have `replace_clip_image` call `_swap_clip_start_frame(clip, new_key)` after preserving the audit (`clip.replacement_start_frame = previous_rejected`). Keep existing behavior identical for the manual path.

- [ ] **Step 2: Implement the auto-retry function**

In `main.py`:
```python
def _auto_image_retry(db, clip, rejected_key: str) -> dict | None:
    """v815 — attempt a prominent-people auto-substitution. Returns a dict
    {used_frame, mode, count} when a substitute was applied (clip is now
    FLOW_REDO_QUEUED), or None to fall through to the manual replace card.

    Gating:
      - account setting mode (off disables);
      - candidate availability (A/B single neighbor; C first untried other);
      - per-clip 'tried' history so a rejected key is never reused.
    """
    user = db.query(User).filter(User.id == clip_owner_user_id(db, clip)).first()
    mode = parse_auto_image_retry_mode(user.settings_json if user else None)
    if mode == "off":
        return None

    # Load / seed audit state.
    try:
        audit = _json807.loads(clip.auto_image_retry_json) if clip.auto_image_retry_json else {}
    except Exception:
        audit = {}
    original = audit.get("original_frame") or rejected_key
    tried = list(audit.get("tried") or [])
    if rejected_key and rejected_key not in tried:
        tried.append(rejected_key)

    # A/B are single-shot: if we already substituted once, yield to manual.
    if mode in ("next", "prev") and audit.get("count", 0) >= 1:
        _persist_retry_audit(clip, original, audit.get("used_frame"), tried, audit.get("count", 0), mode)
        return None

    # Resolve candidates from the job's distinct frames in clip order.
    job_clips = db.query(Clip).filter(Clip.job_id == clip.job_id).all()
    frames = order_distinct_frames(job_clips)

    # Mode C with no other batch image (e.g. single-image job / not from an
    # image job) -> fall back to A (next) per operator decision.
    eff_mode = mode
    if mode == "batch" and len([f for f in frames if f != original]) == 0:
        eff_mode = "next"

    cand = pick_substitute(eff_mode, frames, original, tried)
    if not cand:
        _persist_retry_audit(clip, original, audit.get("used_frame"), tried, audit.get("count", 0), mode)
        return None  # exhausted -> manual card

    new_count = audit.get("count", 0) + 1
    _swap_clip_start_frame(clip, cand)
    _persist_retry_audit(clip, original, cand, tried, new_count, mode)
    db.commit()
    print(f"[v815] auto-image-retry clip {clip.id} mode={mode} eff={eff_mode} "
          f"original={original} -> used={cand} count={new_count}", flush=True)
    return {"used_frame": cand, "mode": mode, "count": new_count}

def _persist_retry_audit(clip, original, used, tried, count, mode):
    clip.auto_image_retry_json = _json807.dumps({
        "original_frame": original, "used_frame": used,
        "tried": tried, "count": count, "mode": mode,
    })
```
Add a tiny owner resolver (clip → job → user_id):
```python
def clip_owner_user_id(db, clip):
    job = db.query(Job).filter(Job.id == clip.job_id).first()
    return job.user_id if job else None
```
(If `Job` has no `user_id`, find the existing job→user linkage and use it; grep `Job.user_id` first.)

- [ ] **Step 3: Wire it into BOTH worker policy-violation endpoints**

In `local_worker_report_policy_violation` (~L10787) AND `user_worker_report_policy_violation` (~L12783) — operator runs USER MODE, but mirror both (memory: worker-endpoint-divergence). After the request parses `rejected_key`, BEFORE stamping the manual `CONTENT_POLICY_VIOLATION` failure, branch on the new reason:
```python
        is_prominent = (request.error_reason or "").upper().find("PROMINENT") >= 0 \
                       or (request.error_reason or "").upper().find("CELEBRITY") >= 0
        if is_prominent:
            # Stamp the distinct code first so the manual card / gating is correct
            # if auto-retry declines or is exhausted.
            clip.error_code = "PROMINENT_PEOPLE_FILTER"
            clip.error_message = request.detail or "Rejected (prominent people). Auto-retry in progress."
            if rejected_key:
                clip.replacement_start_frame = rejected_key
            db.commit()
            applied = _auto_image_retry(db, clip, rejected_key)
            if applied:
                return {"ok": True, "clip_id": clip_id, "auto_retry": applied}
            # else: fall through, clip keeps PROMINENT_PEOPLE_FILTER + FAILED for manual card
            clip.status = ClipStatus.FAILED.value
            db.commit()
            return {"ok": True, "clip_id": clip_id, "auto_retry": None,
                    "rejected_image_key": rejected_key or None}
        # ... existing generic CONTENT_POLICY_VIOLATION path unchanged below ...
```
Add `error_reason: Optional[str] = None` to `PolicyViolationRequest` (~L4789).

Add `"PROMINENT_PEOPLE_FILTER"` to `IMAGE_ATTRIBUTABLE_ERROR_CODES` (L4387) so the manual replace-image card still works when auto-retry is off/exhausted.

- [ ] **Step 4: Import-check**

Run: `cd code && python -c "import main; print('main OK')"`
Expected: `main OK` (if a pre-existing unrelated import error appears, confirm via the `git stash` comparison technique that it predates this change).

- [ ] **Step 5: Commit**

```bash
git add main.py
git commit -m "v815: prominent-people auto-substitute trigger in worker policy-violation"
```

---

### Task 6: Worker threads the prominent-people reason through

**Files:**
- Modify: `static/flow_worker.py` (`report_policy_violation` ~L7305 + the `FramePolicyMonitor` PROMINENT_PEOPLE detection ~L13356)

- [ ] **Step 1: Pass the reason into the report payload**

In `report_policy_violation(clip_id, ...)` (~L7305-7354), add an `error_reason` argument and include it in the POST body:
```python
    payload = {"rejected_image_key": rejected_image_key, "detail": detail,
               "error_reason": error_reason}
```
At the `FramePolicyMonitor.on_response_complete` site (~L13356) where `error_reason='PUBLIC_ERROR_PROMINENT_PEOPLE_UPLOAD'` is set, pass that value through to `report_policy_violation(..., error_reason=error_reason)`. For the celebrity/RAI path (if it also calls report_policy_violation), pass its reason too.

- [ ] **Step 2: Verify the worker module parses**

Run: `cd code && python -m py_compile static/flow_worker.py && echo OK`
Expected: `OK`.

- [ ] **Step 3: Commit**

```bash
git add static/flow_worker.py
git commit -m "v815: worker forwards prominent-people reason to policy-violation"
```

---

### Task 7: Frontend — account settings UI

**Files:**
- Modify: `static/index.html` (settings panel + load/save wiring)

- [ ] **Step 1: Add the settings control**

Add a settings section (near the existing account/header controls — grep for an existing settings or account menu; if none, add a small gear button in the top bar that opens a modal). Render 4 radios bound to a save handler:
```html
<div class="setting-row">
  <label>Auto-retry when an image is rejected for prominent people</label>
  <select id="autoImageRetryMode" onchange="saveAutoImageRetryMode(this.value)">
    <option value="batch">All other images in the batch (default)</option>
    <option value="next">Next image</option>
    <option value="prev">Previous image</option>
    <option value="off">Off — let me upload manually</option>
  </select>
</div>
```

- [ ] **Step 2: Load + save handlers**

```javascript
async function loadAutoImageRetryMode() {
  try {
    const r = await fetch(`${API}/user/settings`);
    if (!r.ok) return;
    const j = await r.json();
    const sel = document.getElementById('autoImageRetryMode');
    if (sel && j.auto_image_retry?.mode) sel.value = j.auto_image_retry.mode;
  } catch (_) {}
}
async function saveAutoImageRetryMode(mode) {
  try {
    const r = await fetch(`${API}/user/settings`, {
      method: 'PUT', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({auto_image_retry_mode: mode}),
    });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    showToast('auto-retry setting saved', '#10b981');
  } catch (e) { showToast('save failed: ' + e.message, '#ef4444'); }
}
```
Call `loadAutoImageRetryMode()` on page init (next to other initial fetches).

- [ ] **Step 3: Commit**

```bash
git add static/index.html
git commit -m "v815: account settings UI for image auto-retry mode"
```

---

### Task 8: Frontend — card marking (what happened + original intended image)

**Files:**
- Modify: `static/index.html` (`renderClip` / `patchClipUI` — wherever a clip card renders)

- [ ] **Step 1: Render the audit badge on any clip that was auto-substituted**

In the clip-card render path, when `clip.auto_image_retry` is present, show a persistent note (regardless of the clip's current status — it survives into the rendered/approved card so the operator always sees what happened):
```javascript
function autoRetryBadgeHtml(clip, jid) {
  const a = clip.auto_image_retry;
  if (!a || !a.used_frame || a.used_frame === a.original_frame) return '';
  const origBase = (a.original_frame || '').split('/').pop();
  const usedBase = (a.used_frame || '').split('/').pop();
  const origThumb = origBase
    ? `${API}/jobs/${jid}/images/${encodeURIComponent(origBase)}` : '';
  return `<div style="margin-top:6px;font-size:11px;color:#fbbf24;background:rgba(245,158,11,0.12);padding:6px 8px;border-radius:6px;border:1px solid rgba(245,158,11,0.35);">
      ⚠ image rejected (prominent people) — auto-used <strong>${usedBase}</strong> (mode: ${a.mode})
      ${origThumb ? `<div style="margin-top:4px;opacity:0.85;">originally intended:<br><img src="${origThumb}" style="max-width:80px;border-radius:4px;margin-top:2px;"></div>` : ''}
    </div>`;
}
```
Insert `${autoRetryBadgeHtml(clip, jid)}` into the clip card body in `renderClip` (and add the same DOM update in `patchClipUI` so polling keeps it visible).

- [ ] **Step 2: Manual eyeball check after deploy**

There is no DOM unit test harness here; verification is operator-side (Task 9).

- [ ] **Step 3: Commit**

```bash
git add static/index.html
git commit -m "v815: clip card marks auto-substituted image + shows original intended"
```

---

### Task 9: Diagnostics, deploy, operator verification

**Files:** none (process)

- [ ] **Step 1: Confirm the diagnostic log lines are present**

`[v815] auto-image-retry clip ...` (Task 5). This is the runtime evidence line per `CLAUDE.md` §2.

- [ ] **Step 2: Run the unit suite once more**

Run: `cd code && python -m pytest tests/test_auto_image_retry.py -v`
Expected: all passed.

- [ ] **Step 3: Push + bump submodule pointer**

```bash
cd code && git push origin main
cd .. && git add code && git commit -m "bump code pointer: v815 prominent-people auto-retry"
```

- [ ] **Step 4: Spawn reviewer**

Spawn `caveman:cavecrew-reviewer` on the v815 commit range.

- [ ] **Step 5: Operator evidence (do NOT claim done before this)**

Operator triggers a build with a known prominent-people-tripping frame:
- mode = batch: card shows "auto-used <X> (mode: batch)" + original thumbnail; clip re-renders; log shows `[v815] auto-image-retry ... mode=batch`.
- mode = next/prev: single substitute; if it also trips, card flips to the manual replace-image card.
- mode = off: behaves exactly as today (manual card immediately).
Capture the log line + a screenshot. Only then mark the feature complete.

---

## Self-Review

**Spec coverage:**
- "3 possibilities selected from settings" → Task 1/2 (store) + Task 7 (UI, 4 options incl. off).
- a = next image → `pick_substitute("next", ...)` Task 4; default-fallback for C-no-batch Task 5.
- b = previous image → `pick_substitute("prev", ...)` Task 4.
- c = all other batch images, default, image-job only → `pick_substitute("batch", ...)` bounded sweep Task 4/5; default in Task 2 parser; no-batch fallback to A in Task 5.
- "retry automatically" → Task 5/6 server-side trigger via worker reason.
- "show original intended image (a/b)" → `original_frame` audit + thumbnail Task 8 (applies to all modes, not just a/b — superset, fine).
- "mark card: image rejected so used image X" → Task 8 badge.
- prominent-people scope → `PROMINENT_PEOPLE_ERROR_CODES` + worker reason threading Task 5/6.

**Placeholder scan:** core helpers, schema, endpoints, trigger all have concrete code. Mechanical mirror sites (second worker endpoint, exact migration tuple format, settings-panel anchor) are described as "match the existing pattern at <location>" because the surrounding format must be copied verbatim — the executing agent reads that pattern in-file.

**Type consistency:** `parse_auto_image_retry_mode`, `order_distinct_frames`, `pick_substitute`, `_swap_clip_start_frame`, `_auto_image_retry`, `_persist_retry_audit`, `clip_owner_user_id` — names used consistently across Tasks 2/4/5. Audit JSON keys (`original_frame`/`used_frame`/`tried`/`count`/`mode`) consistent across Tasks 3/5/8. Setting key `auto_image_retry.mode` consistent Tasks 2/7.

**Open items:** none. C-sweep-vs-single-shot resolved (C = full sweep, A/B single-shot). Attempt-cap, loop-termination, credit-cost, and donor-clobber all double-checked against `main.py` (see "Double-check findings").

**Sweep mechanics consistency check:** Task 4 `pick_substitute("batch", ...)` returns the first untried OTHER frame (looped per worker report); Task 5 `_auto_image_retry` applies the single-shot gate ONLY to `next`/`prev` (`mode in ("next","prev") and count >= 1 → None`), never to `batch` — so batch keeps sweeping while A/B stop after one. `tried` is persisted every call (including on the yield-to-manual paths) so the history survives across worker reports. Consistent.
