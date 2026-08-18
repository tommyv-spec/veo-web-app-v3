# v931 Unlimited Clip Redos Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the 3-attempt cap on clip redos — the operator can redo any clip an unlimited number of times.

**Architecture:** The cap lives in ONE authoritative place: the `/api/clips/{clip_id}/redo` endpoint in `main.py` (blocks at `generation_attempt >= 3` and flags the clip `approval_status='max_attempts'`). Everything else is derived display: `attempts_remaining = 3 - generation_attempt` is computed in 9 places in `main.py` and rendered/gated in ~15 places in `static/index.html` (buttons disable at `<= 0`). The fix: delete the endpoint cap, make `attempts_remaining` a constant positive sentinel (999) so every frontend guard permanently passes, strip the visible counters from the UI so "999" is never shown, and un-strand legacy clips already flagged `max_attempts` via a deferred-startup normalization (same pattern as the existing user_id backfill).

**What this does NOT touch (deliberate):** `MAX_AUTO_REDO_CYCLES = 2` and `POLICY_FAIL_ATTEMPT = 3` in `static/flow_worker.py`. Those are automatic-loop safety valves (a clip that hard-fails or policy-fails repeatedly stops being *auto*-requeued so the worker can't burn the account in a loop). They do not limit operator-initiated redos — after an auto give-up the clip lands `failed` and the operator can now redo it manually forever.

**Tech Stack:** FastAPI + SQLAlchemy (main.py), vanilla JS single-file frontend (static/index.html).

---

### Task 1: Backend — remove the cap in `main.py`

**Files:**
- Modify: `code/main.py` (all line numbers pre-change)

- [ ] **Step 1: Add the sentinel constant** near the `ClipResponse` model (above line 354, `class ClipResponse(BaseModel):`):

```python
# v931 — redos are unlimited (operator 2026-08-18). attempts_remaining stays
# in the API as a positive sentinel so older frontends' "disable at <= 0"
# guards never fire. 999 is never displayed (v931 UI strips the counters).
UNLIMITED_ATTEMPTS_REMAINING = 999
```

- [ ] **Step 2: Model default** — line 369: `attempts_remaining: int = 2` → `attempts_remaining: int = UNLIMITED_ATTEMPTS_REMAINING` (constant must be defined before the class; it is, per Step 1 placement).

- [ ] **Step 3: Replace all 9 computed values** with the constant:
  - L5754: `attempts_remaining=3 - (c.generation_attempt or 1),` → `attempts_remaining=UNLIMITED_ATTEMPTS_REMAINING,`
  - L5852: same as 5754
  - L5944: `attempts_remaining=3 - clip.generation_attempt` → `attempts_remaining=UNLIMITED_ATTEMPTS_REMAINING`
  - L5972: same as 5944
  - L7394: `attempts_remaining=3 - (clip.generation_attempt or 1),` → constant
  - L7433: same as 5944 (inside redo endpoint's already-queued response)
  - L7647: `attempts_remaining=3 - new_attempt` → constant
  - L7804: `"attempts_remaining": 3 - clip.generation_attempt,` → `"attempts_remaining": UNLIMITED_ATTEMPTS_REMAINING,`
  - L7835: same as 7804

- [ ] **Step 4: Delete the cap block** in the redo endpoint (L7446–7457) and replace with a legacy-flag clear:

```python
    # v931 — redos are unlimited. The old 3-attempt cap ('max_attempts' flag +
    # MAX_ATTEMPTS_REACHED 400) is gone. Clear the legacy flag so clips capped
    # under the old rule become reviewable again after this redo.
    if clip.approval_status == "max_attempts":
        clip.approval_status = "pending_review"
```

- [ ] **Step 5: Update the endpoint docstring** (L7410–7419): replace the three attempt bullets with:

```
    - Attempt 1 → 2: Uses same logged parameters
    - Attempt 3+: Uses fresh parameters (no log)
    - v931: attempts are unlimited (the old cap at 3 is removed)
```

- [ ] **Step 6: Remove the approve block** (L5896–5897):

```python
    if clip.approval_status == "max_attempts":
        raise HTTPException(status_code=400, detail="Clip has reached max attempts - contact support")
```
Delete both lines (legacy flagged clips become approvable; new clips never get the flag).

- [ ] **Step 7: Startup normalization for stranded legacy clips.** In `_run_deferred_startup()` (Phase 2, after the user_id backfill block ~L543), add:

```python
        # v931 — one-time normalization: clips stranded 'max_attempts' by the
        # retired 3-attempt cap become reviewable again. Idempotent.
        try:
            from models import Clip as _Clip, get_db as _get_db2
            def _clear_max_attempts():
                with _get_db2() as _db:
                    _n = _db.query(_Clip).filter(_Clip.approval_status == "max_attempts").update(
                        {"approval_status": "pending_review"}, synchronize_session=False
                    )
                    if _n:
                        _db.commit()
                    return _n
            _nm = await _asyncio.to_thread(_clear_max_attempts)
            if _nm:
                print(f"[Deferred][v931] Cleared legacy max_attempts flag on {_nm} clip(s) — redos are unlimited now", flush=True)
        except Exception as _v931e:
            print(f"[Deferred][v931] max_attempts normalization failed: {_v931e}", flush=True)
```

- [ ] **Step 8: Verify import** — `cd code && python -c "import main"` must exit 0.

### Task 2: Frontend — strip the counters in `static/index.html`

The guards (`<=0` disabled, `>0` ternaries, `maxed` branches) can stay — with `attempts_remaining` always 999 they permanently pass / never fire, and startup normalization removes the `max_attempts` status that feeds `maxed`. Only the VISIBLE numbers change (nobody should see "↻999").

**Files:**
- Modify: `code/static/index.html`

- [ ] **Step 1: Replace all `↻${...attempts_remaining}` button labels** (bare count):
  - L11762, L11773, L12036: `>↻${clip.attempts_remaining}</button>` / `>↻${c.attempts_remaining}</button>` → `>↻</button>`
  - L12301: `title="redo ${sideLabel}">↻${clip.attempts_remaining}</button>` → `title="redo ${sideLabel}">↻</button>`

- [ ] **Step 2: Replace all `↻ Redo (${...attempts_remaining})` labels**:
  - L11760, L11770, L11772, L12006: `↻ Redo (${clip.attempts_remaining})` / `(${c.attempts_remaining})` → `↻ Redo`

- [ ] **Step 3: Failed-clip retry label** — L12034: `↻ Retry (${c.attempts_remaining} left)` → `↻ Retry`

- [ ] **Step 4: redoModal** (L13488–13525):
  - Delete L13489: `if(rem<=0)return alert('No redos left');`
  - L13520: `↻ Regenerate (${rem-1} left)` → `↻ Regenerate`

- [ ] **Step 5: Sweep for leftovers** — grep `index.html` for `attempts_remaining}` inside visible text (patterns `(${` + `left)` + `Redo (`) and for any other rendered counter this plan missed (L12097–12106, L12864, L13202, L13458 were flagged in recon — inspect each; if the value only feeds `redoModal(...)` args or `<=0`/`>0` guards, leave it; if it is rendered as text, strip the number the same way).

### Task 3: Worker log cosmetics — `worker.py`

**Files:**
- Modify: `code/worker.py`

- [ ] **Step 1:** L1314: `(attempt {clip.generation_attempt}/3)` → `(attempt {clip.generation_attempt})`
- [ ] **Step 2:** L1750: `(attempt {clip.generation_attempt}/3)` → `(attempt {clip.generation_attempt})`
- [ ] **Step 3:** `python -c "import worker"` — wait, worker.py may start threads on import; use `python -m py_compile worker.py` PLUS check the module has no import-time side effects by reading its tail guard. If `if __name__ == "__main__":` guards execution, `python -c "import worker"` is safe and preferred (repo rule: py_compile insufficient).

### Task 4: Verification + commit

- [ ] **Step 1: Imports** — from `code/`: `python -c "import main"` and `python -c "import worker"` both exit 0.
- [ ] **Step 2: Existing redo tests still pass** — `python -m pytest test_auto_redo_cap.py test_redo_inflight.py test_redo_status_precheck.py -q` (the auto-redo cap tests must still pass — that mechanism is deliberately unchanged).
- [ ] **Step 3: No stale "of 3" promises** — `grep -n "3 - clip.generation_attempt\|3 - (c\|3 - new_attempt\|MAX_ATTEMPTS_REACHED" main.py` returns nothing.
- [ ] **Step 4: Commit ONLY these paths** (shared tree — another session has dirty files):

```bash
git add main.py static/index.html worker.py docs/superpowers/plans/2026-08-18-v931-unlimited-redos.md
git commit -F <msg-file> -- main.py static/index.html worker.py docs/superpowers/plans/2026-08-18-v931-unlimited-redos.md
```

Commit subject: `v931: clip redos are unlimited — the 3-attempt cap is gone`

- [ ] **Step 5: HANDOFF.md** — append a rev entry in the wiki root HANDOFF.md describing v931 (cap removed; auto-redo safety valves untouched; deploy pending operator).

**Deploy note:** `code/` auto-deploys on push to `origin/main`. Push is a separate deliberate step (deploy.ps1 from clean main per deploy discipline) — report ready-to-deploy rather than pushing unasked.
