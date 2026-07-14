# Image-Worker Golden-Restore Parity Implementation Plan

> **STATUS 2026-07-07: SHIPPED (local commits, pending deploy+verify).** Root cause turned out DEEPER than this plan's original premise — the recovery path was never *firing* on the API block, so hardening the restore alone would have fixed a path that never ran. See "Actual root cause" below. Delivered as v828 on branch `v821-reworded-promptb`:
> - `a96f18f` — the trigger fix (the real root cause): widened `_is_unusual`.
> - `0c17d33` — the restore-robustness + give-up parity (this plan's original Tasks 1-3).

## Actual root cause (found via systematic-debugging, superseding the premise below)

The image worker's API golden-restore trigger depends **entirely** on one classifier, `_is_unusual(reason)`. On an account flag, `cli.submit_image` raises, and only if `_is_unusual(reason)` is True does the chain fire (`_signal_unusual` → `page._flow_api_unusual_reason` → `_restore_signal["golden"]` → `RELAUNCH_GOLDEN` → relaunch). The **old classifier** (`UNUSUAL_ACTIVITY` only, OR `RECAPTCHA` **and** a 403 marker) missed the two real flag manifestations:
1. **reCAPTCHA mint failure** — a flagged account can't mint a token → `submit_image failed: captcha mint failed`. That string has no `UNUSUAL_ACTIVITY` and no `RECAPTCHA` (it's `CAPTCHA`), so it was misrouted by `_is_transient` to the DOM cookie-clear (which does **not** clear Flow's reCAPTCHA block) → **golden restore never triggered.**
2. **bare 403 / PERMISSION_DENIED** without the literal word `RECAPTCHA` → failed the AND.

Fix: move `_is_unusual` to module level (unit-testable) and widen it to catch persistent captcha-mint-failure + bare 403/PERMISSION_DENIED, kept safe from false positives by the upstream retry + zero-capture gate. Added a temporary `[v828-diag]` log of the full reason string to confirm/extend coverage on the next real block. THEN the restore-robustness + give-up parity below make the recovery succeed end-to-end once it fires.

**Original goal (still valid, delivered as the 2nd commit):** Make the image worker recover from an "unusual activity" account block as robustly as the video worker — port the proven retry-hardened golden restore, and make the give-up path surface an actionable reason — without losing in-flight work.

**Architecture:** The image worker (`code/image_worker.py`) already mirrors the video worker's recovery *shape*: on an account-level "unusual activity" block it signals `main()`, closes Chrome, sleeps 3s, and relaunches from the golden profile via `launch_browser()`; the `/release-claims` call re-queues every in-flight node so nothing is lost. Two gaps vs the video worker (`code/static/flow_worker.py`): (1) the golden copy in `launch_browser()` was a **single-attempt** `rmtree`+`copytree` that silently fails on a Windows file lock (`WinError 1224`/`32`) — leaving a stale/partial profile so the block never clears; (2) when the relaunch budget is exhausted the worker stopped with only a console print, no operator-facing reason. Fixed by a robust `restore_from_golden()` (the video worker's retry loop, reusing the image worker's existing `get_golden_folder()` + `purge_gpu_caches()`), wiring `launch_browser()` to it, and improving the give-up message + budget parity. Deliberately does NOT port the video worker's per-*job* restore cap, because the image block is account-*global* (a per-job cap would churn the whole queue during a block — see Task 3 rationale).

**Tech Stack:** Python 3, Patchright/Playwright sync API, pytest, Windows Chrome profile management. Deploys to Render on push to `code/` `main` (this branch is NOT main — see verification/deploy note).

---

## Pre-flight (read before starting)

- **Branch discipline (memory `project_concurrent-sessions-shared-tree`):** concurrent sessions share one git tree. Before ANY commit run `git branch --show-current` and confirm it's the intended branch. The `code/` submodule has its own git — commit inside `code/`.
- **Deploy discipline (`code/CLAUDE.md`):** every push to `code/` `main` is live in 2-3 min; production is the only environment. `py_compile` is insufficient — actually `import image_worker` before pushing. Add a temporary diagnostic log line on the runtime path; remove only after operator-side evidence lands.
- **Scope:** two files touched — `code/image_worker.py` (source) and `code/tests/test_image_worker_golden_restore.py` (new test). No changes to `flow_worker.py` (it is the reference, already correct).

---

## File Structure

- **Modify** `code/image_worker.py`
  - Add function `restore_from_golden(session_folder, label="IMAGE")` right after `purge_gpu_caches()` (currently ends line 836). Responsibility: robust golden→session copy with the WinError retry/backoff/Singleton-cleanup loop.
  - Replace the naive inline restore block in `launch_browser()` (lines 9471-9482) with a single call to `restore_from_golden(session_folder, label="IMAGE")`.
  - Improve the relaunch-budget-exhausted branch in `main()` (lines 9750-9753): clearer operator-facing message + bump `MAX_GOLDEN_RELAUNCHES` 3→4 for parity with the video worker's `MAX_UNUSUAL_GOLDEN_RESTORES = 4`.
- **Create** `code/tests/test_image_worker_golden_restore.py`
  - Responsibility: unit-test `restore_from_golden()` — happy path, missing-golden, and the WinError-lock retry path (mocked `shutil.copytree`). No browser needed.

---

## Task 1: Add robust `restore_from_golden()` to the image worker

**Files:**
- Modify: `code/image_worker.py` (add function after `purge_gpu_caches`, which ends at line 836)
- Test: `code/tests/test_image_worker_golden_restore.py`

- [ ] **Step 0: Confirm the function does not already exist**

Run: `grep -n "def restore_from_golden" code/image_worker.py`
Expected: no output (function absent — the restore is currently inline in `launch_browser`). If it prints a line, STOP and reconcile before continuing.

- [ ] **Step 1: Write the failing test**

Create `code/tests/test_image_worker_golden_restore.py`:

```python
import os
import shutil
import pytest
import image_worker


def _make_golden(tmp_path):
    """Create a fake golden profile dir with a couple of files + a Singleton lock."""
    golden = tmp_path / "image-chrome-golden"
    (golden / "Default").mkdir(parents=True)
    (golden / "Default" / "Cookies").write_text("cookie-data")
    (golden / "Local State").write_text("state-data")
    (golden / "SingletonLock").write_text("lock")  # must be skipped by ignore_patterns
    return golden


def test_missing_golden_returns_false(tmp_path):
    session = tmp_path / "image-chrome-session"
    # get_golden_folder derives <base>/image-chrome-golden from the session path;
    # we never created it, so restore must decline.
    assert image_worker.restore_from_golden(str(session), label="TEST") is False


def test_happy_path_copies_golden_and_skips_singletons(tmp_path):
    _make_golden(tmp_path)
    session = tmp_path / "image-chrome-session"
    ok = image_worker.restore_from_golden(str(session), label="TEST")
    assert ok is True
    assert (session / "Default" / "Cookies").read_text() == "cookie-data"
    assert (session / "Local State").read_text() == "state-data"
    # Singleton* files are excluded by ignore_patterns
    assert not (session / "SingletonLock").exists()


def test_retries_on_winerror_lock_then_succeeds(tmp_path, monkeypatch):
    _make_golden(tmp_path)
    session = tmp_path / "image-chrome-session"

    real_copytree = shutil.copytree
    calls = {"n": 0}

    def flaky_copytree(src, dst, *a, **k):
        calls["n"] += 1
        if calls["n"] < 3:
            raise OSError("[WinError 1224] The requested operation cannot be "
                          "performed on a file with a user-mapped section open")
        return real_copytree(src, dst, *a, **k)

    monkeypatch.setattr(image_worker.shutil, "copytree", flaky_copytree)
    monkeypatch.setattr(image_worker.time, "sleep", lambda *_a, **_k: None)  # no real backoff wait

    ok = image_worker.restore_from_golden(str(session), label="TEST")
    assert ok is True
    assert calls["n"] == 3  # failed twice on the lock, succeeded on the third attempt
    assert (session / "Local State").read_text() == "state-data"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd code && python -m pytest tests/test_image_worker_golden_restore.py -v`
Expected: FAIL — `AttributeError: module 'image_worker' has no attribute 'restore_from_golden'` (or collection error) on all three tests.

- [ ] **Step 3: Add the implementation**

In `code/image_worker.py`, insert this function immediately after `purge_gpu_caches()` (after line 836, before the `# ===...CHROME WARMUP` comment block at line 839):

```python
def restore_from_golden(session_folder, label="IMAGE"):
    """Restore the image-worker Chrome session profile from the golden snapshot.

    Ported from flow_worker.restore_from_golden (v701g). The naive single
    copytree that used to live inline in launch_browser silently failed on a
    Windows file lock (WinError 1224 / 32) — Chrome's GPU/singleton subprocess
    can hold memory-mapped handles for several seconds past taskkill, so
    copytree hit a cookie/cache DB mid-copy, gave up, and shipped a
    partially-restored profile. Session then has stale fragments mixed with
    golden state, reCAPTCHA flags it as suspicious, the unusual-activity block
    never actually clears, and the worker relaunch-loops to its cap.

    Retry with backoff (0.5s, 2s, 5s); between attempts, force one more pass at
    SingletonLock cleanup + a small sleep so any lingering chrome subprocess
    releases handles.

    Returns True on success, False if the golden is missing or all retries fail.
    """
    golden_folder = get_golden_folder(session_folder)
    prefix = f"[{label}] " if label else ""

    if not os.path.exists(golden_folder):
        print(f"{prefix}⚠ Golden profile not found at {golden_folder} — cannot restore.", flush=True)
        print(f"{prefix}  Re-run setup_worker.py to create a fresh golden profile.", flush=True)
        return False

    print(f"{prefix}🔄 GOLDEN RESTORE: Restoring session profile from {golden_folder}", flush=True)

    last_err = None
    for _attempt in range(3):
        try:
            if os.path.exists(session_folder):
                shutil.rmtree(session_folder, ignore_errors=True)
            # dirs_exist_ok=True so a locked file left behind by rmtree still gets
            # overwritten from golden rather than failing with FileExistsError.
            shutil.copytree(
                golden_folder, session_folder,
                dirs_exist_ok=True,
                ignore_dangling_symlinks=True,
                ignore=shutil.ignore_patterns(
                    'SingletonLock', 'SingletonSocket', 'SingletonCookie',
                ),
            )
            print(f"{prefix}  ✓ Session profile restored → {session_folder}", flush=True)
            last_err = None
            break
        except Exception as e:
            last_err = e
            err_str = str(e)
            # WinError 1224 = file mapped by another process. WinError 32 = in use.
            if '1224' in err_str or 'WinError 32' in err_str or 'in use' in err_str.lower():
                _wait = (0.5, 2.0, 5.0)[_attempt] if _attempt < 3 else 5.0
                print(
                    f"{prefix}  ⚠ Restore attempt {_attempt+1}/3 hit Windows file-lock; "
                    f"waiting {_wait:.1f}s for handles to release",
                    flush=True,
                )
                for _lock in ('SingletonLock', 'SingletonSocket', 'SingletonCookie'):
                    _lp = os.path.join(session_folder, _lock)
                    if os.path.exists(_lp):
                        try:
                            os.remove(_lp)
                        except Exception:
                            pass
                time.sleep(_wait)
                continue
            # Non-lock error — don't waste time retrying.
            break

    if last_err is not None:
        print(f"{prefix}  ⚠ Failed to restore session profile after retries: {last_err}", flush=True)
        return False

    # Golden may have been built on a different GPU environment — purge caches.
    purge_gpu_caches(session_folder, label=label or "RESTORE")
    print(f"{prefix}✅ Golden restore complete.", flush=True)
    return True
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd code && python -m pytest tests/test_image_worker_golden_restore.py -v`
Expected: PASS — all three tests green.

- [ ] **Step 5: Commit**

```bash
cd code
git branch --show-current   # confirm intended branch first
git add image_worker.py tests/test_image_worker_golden_restore.py
git commit -m "fix(image-worker): robust restore_from_golden with WinError retry loop

Port flow_worker's retry-hardened golden restore into image_worker (reuses
existing get_golden_folder + purge_gpu_caches). Single-attempt inline copy
silently failed on WinError 1224/32 file locks, shipping a stale profile so
the unusual-activity block never cleared."
```

---

## Task 2: Wire `launch_browser()` to the robust restore

**Files:**
- Modify: `code/image_worker.py:9471-9482` (the naive inline restore block inside `launch_browser`)

- [ ] **Step 1: Replace the inline block with a call to the new function**

In `code/image_worker.py`, find this exact block (lines 9470-9482):

```python
    # Restore from golden if available
    if os.path.exists(golden):
        print(f"[IMAGE] Restoring session from golden: {golden}", flush=True)
        try:
            if os.path.exists(session_folder):
                shutil.rmtree(session_folder, ignore_errors=True)
            shutil.copytree(golden, session_folder,
                ignore_dangling_symlinks=True,
                ignore=shutil.ignore_patterns('SingletonLock', 'SingletonSocket', 'SingletonCookie'))
            purge_gpu_caches(session_folder, label="IMAGE")
            print("[IMAGE] ✓ Session restored from golden", flush=True)
        except Exception as e:
            print(f"[IMAGE] ⚠ Golden restore failed: {e}", flush=True)
```

Replace it with:

```python
    # Restore from golden if available (v819 — robust retry loop, parity with
    # flow_worker: the old single-attempt copytree silently failed on WinError
    # 1224/32 file locks and shipped a stale profile that kept the block alive).
    if os.path.exists(golden):
        restore_from_golden(session_folder, label="IMAGE")
```

Note: `restore_from_golden` re-derives `golden` internally via `get_golden_folder(session_folder)`, which returns the same path already computed into the local `golden` at line 9467 — so the `if os.path.exists(golden)` guard and the internal check agree. Leaving the guard avoids the "Golden profile not found" log on the normal no-golden path.

- [ ] **Step 2: Verify the module still imports**

Run: `cd code && python -c "import image_worker; print('import OK')"`
Expected: `import OK` (py_compile is insufficient per `code/CLAUDE.md` — this catches missing-name regressions).

- [ ] **Step 3: Re-run the restore tests (regression guard)**

Run: `cd code && python -m pytest tests/test_image_worker_golden_restore.py -v`
Expected: PASS — the function is unchanged; this confirms the edit above didn't break the import path the tests use.

- [ ] **Step 4: Commit**

```bash
cd code
git branch --show-current
git add image_worker.py
git commit -m "fix(image-worker): launch_browser uses robust restore_from_golden

Replace the naive inline rmtree+copytree in launch_browser with a call to the
retry-hardened restore_from_golden added in the prior commit."
```

---

## Task 3: Actionable give-up + budget parity in `main()`

**Rationale — why NOT a per-job cap (read before editing):** The video worker caps golden restores *per job* (`_UNUSUAL_GOLDEN_RESTORES[job_id]`, max 4) and fails that one job with an actionable message while the worker keeps running. That works because a video "job" runs its own session sequence. The image worker's block is **account-global** (`code/image_worker.py:8243-8253` — one node's API call trips a block that hits every in-flight node). Porting a per-job cap would make the worker keep claiming *new* jobs during an active account block, each of which re-hits the block and burns a restore — churning the whole queue and deepening the flag. The image worker's existing per-*worker* circuit-breaker (`MAX_GOLDEN_RELAUNCHES`, stop after N) is the correct unit for an account-global block, and its `finally` `/release-claims` (line 9424) already re-queues every in-flight node so **no work is lost** — the jobs go back to `pending` and run when a later worker starts after the block clears. So this task keeps the per-worker breaker and only (a) surfaces an actionable reason on give-up and (b) bumps the budget to match the video worker's 4. If the operator wants strict per-job semantics despite the churn cost, that is a separate, larger change — flag it, do not silently add it.

**Files:**
- Modify: `code/image_worker.py:9733` (`MAX_GOLDEN_RELAUNCHES`) and `9750-9753` (give-up branch)

- [ ] **Step 1: Bump the relaunch budget to parity with the video worker**

In `code/image_worker.py`, find (line 9733):

```python
                MAX_GOLDEN_RELAUNCHES = 3
```

Replace with:

```python
                MAX_GOLDEN_RELAUNCHES = 4  # v819 — parity with flow_worker MAX_UNUSUAL_GOLDEN_RESTORES
```

- [ ] **Step 2: Make the give-up branch actionable**

In `code/image_worker.py`, find (lines 9750-9753):

```python
                    if _relaunch_n > MAX_GOLDEN_RELAUNCHES:
                        print(f"[IMAGE] ⛔ Golden restore requested {_relaunch_n}x — account likely flagged; "
                              f"stopping worker (relaunch it later when the block clears).", flush=True)
                        break
```

Replace with:

```python
                    if _relaunch_n > MAX_GOLDEN_RELAUNCHES:
                        # v819 — account-global block persisted past the restore
                        # budget. Stop cleanly (do NOT keep claiming new jobs into
                        # an active block). The finally-block /release-claims below
                        # re-queues every in-flight node → nothing is lost, jobs run
                        # when a later worker starts after the block clears.
                        print(f"[IMAGE] ⛔ Unusual-activity persisted after {MAX_GOLDEN_RELAUNCHES} golden "
                              f"restores — the Google account is rate-limited. Stopping the worker; in-flight "
                              f"jobs are released back to pending and will run when you relaunch after the "
                              f"block clears (usually a while). Not lost, not failed.", flush=True)
                        break
```

- [ ] **Step 3: Add a temporary diagnostic log line on the restore path (deploy discipline)**

Per `code/CLAUDE.md`, runtime changes to a worker need a temporary diagnostic line so operator-side evidence can confirm the new path fired. In `restore_from_golden` (Task 1), the existing prints already serve this — confirm the success path prints `✅ Golden restore complete.` and the retry path prints `⚠ Restore attempt N/3 hit Windows file-lock`. No extra line needed; note in the commit that these prints ARE the diagnostic and stay until evidence lands. (If the operator wants a distinct `[v819]` tag to grep, add `print("[v819] restore_from_golden entered", flush=True)` as the first line of the function and remove it in a follow-up once evidence is captured.)

- [ ] **Step 4: Verify import + tests**

Run: `cd code && python -c "import image_worker; print('import OK')" && python -m pytest tests/test_image_worker_golden_restore.py -v`
Expected: `import OK` then all tests PASS.

- [ ] **Step 5: Commit**

```bash
cd code
git branch --show-current
git add image_worker.py
git commit -m "fix(image-worker): actionable give-up message + restore budget parity

Bump MAX_GOLDEN_RELAUNCHES 3->4 (parity with flow_worker). On budget-exhaust,
print an actionable 'account rate-limited, jobs released to pending, relaunch
later' message instead of a bare stop line. Keeps the per-worker circuit-breaker
(correct for an account-global block) + the /release-claims re-queue that
preserves in-flight work — deliberately NOT a per-job cap (would churn the queue
during a block; see plan Task 3 rationale)."
```

---

## Task 4: Runtime verification gate (before declaring done)

**No "should work" claims without evidence** (root `CLAUDE.md` §2). The unit tests prove the restore logic; they do NOT prove the end-to-end recovery on a real block. Confirm with operator-side evidence.

- [ ] **Step 1: Push and let Render deploy**

```bash
cd code
git branch --show-current   # confirm main (or the deploy branch) before push
git push
```
Wait 2-3 min for Render to redeploy the worker code (workers download the new code on restart per memory `flow-worker-deploy-path`).

- [ ] **Step 2: Restart the image worker so it pulls the new code**

Operator action: restart the image worker process (it downloads updated code on restart).

- [ ] **Step 3: Capture evidence on the next real unusual-activity hit**

Ask the operator to watch the image-worker console for these lines when a block occurs:
- `🔄 GOLDEN RESTORE: Restoring session profile from ...`
- either `✓ Session profile restored →` (clean) or `⚠ Restore attempt N/3 hit Windows file-lock` followed by a later `✓ Session profile restored →` (the retry loop doing its job — this is the exact case the old code silently failed).
- `✅ Golden restore complete.`
- worker resumes claiming jobs (no `⛔` unless the block genuinely persisted 4×).

Do NOT claim the fix works until the operator confirms at least one restore log sequence completing with `✅ Golden restore complete.` and the worker resuming.

- [ ] **Step 4: Spawn a reviewer on the commit set (cheap insurance)**

Per `code/CLAUDE.md`: after pushing to `code/`, spawn `caveman:cavecrew-reviewer` on the three commits from Tasks 1-3.

- [ ] **Step 5: Log the change**

Add a one-line entry to `wiki/log.md` (from the wiki repo root) noting the image-worker golden-restore hardening (v819) and bump the submodule pointer:
```bash
# from wiki repo root
git add code && git commit -m "bump code: image-worker golden-restore parity (v819)"
```

---

## Self-Review

**Spec coverage:**
- "Do the same as video worker" on unusual-activity → Task 1 ports the video worker's exact retry-hardened `restore_from_golden`; Task 2 wires it into the recovery path; Task 3 brings the budget + give-up messaging to parity. ✓
- "All the solutions to not lose the ongoing process" → documented that `/release-claims` (line 9424) already re-queues in-flight nodes; Task 3 preserves it and its rationale block makes explicit that no work is lost on give-up. ✓
- The account-global vs per-job divergence is surfaced (Task 3 rationale) rather than silently copied — matches §5 scope discipline: a structural semantic change (per-job cap) is flagged for explicit operator decision, not assumed. ✓

**Placeholder scan:** No TBD/TODO/"add error handling"/"similar to Task N" — every code step carries verbatim code. ✓

**Type/name consistency:** `restore_from_golden(session_folder, label="IMAGE")` signature is defined once (Task 1) and called with the same signature in Task 2. `get_golden_folder` / `purge_gpu_caches` are the existing image_worker names (verified at lines 810 / 826). `MAX_GOLDEN_RELAUNCHES` is the existing name (line 9733). ✓

---

## Execution Handoff

Plan complete. Two execution options:
1. **Subagent-Driven (recommended)** — fresh subagent per task, review between tasks.
2. **Inline Execution** — execute in this session with checkpoints.
