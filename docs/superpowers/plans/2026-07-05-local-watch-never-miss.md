# v822 — Local-folder watcher never-miss hardening

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close every known path where a video dropped into the watched local folder can fail to reach a matched job.

**Architecture:** Frontend watcher (`code/static/index.html`, the Local-folder watcher block ~L4722-5142) gets a stability gate, a hash cache, fetch timeouts, permission surfacing, delete debounce and folder-switch protection. Backend (`code/main.py` + `code/local_transcribe.py`) gets a reprocess path for failed/stuck rows and a re-match sweep endpoint the frontend calls each poll.

**Tech Stack:** Vanilla JS (File System Access API), FastAPI, SQLAlchemy, pytest (source-grep-assert convention per `tests/test_status_records_variant.py`).

---

## The 9 miss-paths this closes (audit 2026-07-05, files read fresh)

| # | Miss path | Where today | Fix |
|---|---|---|---|
| 1 | Export writes file over ~seconds; scan hashes a PARTIAL file, uploads it, partial may auto-match (>=0.70 on truncated audio). Full file gets a different hash, uploads, but its job is already `published` → full file = "done — no match" forever | `index.html` `_scanAndUpload` reads any file it sees | Stability gate: upload only when (size, lastModified) unchanged across 2 consecutive scans |
| 2 | `transcription_status='failed'` row (transient ffmpeg/whisper error) is returned as-is by the idempotent upload endpoint; frontend marks hash seen → NEVER retried | `main.py` upload endpoint L4246-4252 | Reprocess path: failed OR stuck pending/running (>10 min) rows re-run `transcribe_local` on re-upload; frontend excludes those rows from the seen-set |
| 3 | Match runs ONCE at upload vs `awaiting_finishing` only. Video uploaded before its job reaches Finishing = "done — no match" forever | `local_transcribe.py` `_maybe_auto_match` | `POST /api/local-videos/rematch` sweep, called by the frontend after every scan |
| 4 | Hung upload fetch (no timeout) leaves `_localPollInFlight=true` forever → all future polls silently skipped until reload | `index.html` L4907-4920 | AbortController timeouts (180s upload / 20s other) + 5-min in-flight watchdog |
| 5 | Permission lost → scan silently returns; `requestPermission` from a timer (no user gesture) throws; status still says "watching" | `index.html` L4875-4879 | Non-interactive check in the poll path; RED status + `⚠` title prefix on loss |
| 6 | Transient `getFile` lock (AV scan, OneDrive) → hash missing ONE scan → DELETE fires → matched job reverted, churn | `index.html` delete pass L4926-4947 | Delete debounce: hash must be missing 2 consecutive scans |
| 7 | Switching watched folders: every previously-seen hash is "missing" → mass DELETE/revert of recent matches | same delete pass | Delete-eligible set: only hashes actually observed in THIS folder this session can trigger DELETE |
| 8 | Every scan re-reads + re-hashes EVERY file (multi-GB folder = minutes per scan, big partial-file window) | `_scanAndUpload` loop | Hash cache keyed `name|size|mtime` — unchanged files never re-read |
| 9 | >500MB and unreadable files skipped with console.warn only — operator never sees | `_scanAndUpload` | `skipped` list rendered in the panel status area |

Not fixable in-browser (documented, surfaced): tab closed / discarded = no polling. Mitigations shipped: `visibilitychange` → immediate scan, "last scan Xs ago" in status, existing PWA-install hint.

---

### Task 1: Branch

**Files:** none (git only)

- [ ] **Step 1: Verify base + branch**

```bash
cd code
git fetch origin
git log --oneline -1 origin/main
git log --oneline -1 HEAD
```

If `origin/main` already contains `553c8b3` (v821 HEAD), branch off `origin/main`; otherwise branch off current HEAD (v821 branch — do NOT merge v821 into main as a side effect of this work):

```bash
git checkout -b v822-local-watch-never-miss
```

---

### Task 2: Server — `should_reprocess` helper (TDD)

**Files:**
- Modify: `code/local_transcribe.py` (top-level, after `_AUTO_MATCH_THRESHOLD`)
- Test: `code/tests/test_local_watch_never_miss.py` (new)

- [ ] **Step 1: Write the failing tests**

```python
"""v822 — local-folder watcher never-miss hardening.

Layer 1: pure helpers in local_transcribe (importable standalone).
Layer 2: source-grep-assert endpoint + frontend markers (this codebase has
been bitten by missing-name regressions py_compile does not catch).
"""
import os
from datetime import datetime, timedelta

import importlib.util

_HERE = os.path.dirname(os.path.abspath(__file__))
_CODE = os.path.dirname(_HERE)
_LT = os.path.join(_CODE, "local_transcribe.py")
_MAIN = os.path.join(_CODE, "main.py")
_INDEX = os.path.join(_CODE, "static", "index.html")


def _load_lt():
    spec = importlib.util.spec_from_file_location("lt_v822_test", _LT)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


# ---- Layer 1: should_reprocess ------------------------------------------
def test_failed_always_reprocesses():
    lt = _load_lt()
    assert lt.should_reprocess("failed", datetime.utcnow()) is True


def test_done_never_reprocesses():
    lt = _load_lt()
    old = datetime.utcnow() - timedelta(hours=5)
    assert lt.should_reprocess("done", old) is False


def test_fresh_pending_not_reprocessed():
    lt = _load_lt()
    fresh = datetime.utcnow() - timedelta(seconds=30)
    assert lt.should_reprocess("pending", fresh) is False
    assert lt.should_reprocess("running", fresh) is False


def test_stuck_pending_reprocessed():
    lt = _load_lt()
    stuck = datetime.utcnow() - timedelta(minutes=11)
    assert lt.should_reprocess("pending", stuck) is True
    assert lt.should_reprocess("running", stuck) is True


def test_pending_without_created_at_reprocessed():
    lt = _load_lt()
    assert lt.should_reprocess("pending", None) is True
```

- [ ] **Step 2: Run to verify failure**

Run: `cd code && python -m pytest tests/test_local_watch_never_miss.py -v`
Expected: FAIL — `AttributeError: module ... has no attribute 'should_reprocess'`

NOTE: `local_transcribe.py` imports `instagram_transcribe` at module top; if that import drags heavy deps that are absent locally, load via the same `importlib` pattern anyway (it works on the deploy image); locally run inside the project venv that already imports `main.py`.

- [ ] **Step 3: Implement helper in `local_transcribe.py`** (after `_AUTO_MATCH_THRESHOLD = ...`):

```python
# v822: pending/running rows older than this are considered stuck (dyno
# restart mid-transcribe) and get re-run when the browser re-uploads.
_STUCK_AFTER_S = 600


def should_reprocess(status, created_at, now=None):
    """True when an existing LocalVideo row should be re-run on re-upload.

    failed -> always (transient ffmpeg/whisper errors were permanent misses).
    pending/running -> only when older than _STUCK_AFTER_S; a live request
    may still be transcribing.  done -> never.
    """
    if status == "failed":
        return True
    if status in ("pending", "running"):
        if not created_at:
            return True
        now = now or datetime.utcnow()
        return (now - created_at).total_seconds() > _STUCK_AFTER_S
    return False
```

- [ ] **Step 4: Run tests — expect the 5 Layer-1 tests PASS**

- [ ] **Step 5: Commit**

```bash
git add local_transcribe.py tests/test_local_watch_never_miss.py
git commit -m "v822: should_reprocess helper - failed/stuck local rows retryable"
```

---

### Task 3: Server — upload reprocess path + rematch sweep

**Files:**
- Modify: `code/main.py` (upload endpoint ~L4244-4252; new endpoint after the DELETE one ~L4322)
- Modify: `code/local_transcribe.py` (new `rematch_unmatched`)
- Test: extend `code/tests/test_local_watch_never_miss.py`

- [ ] **Step 1: Add failing source-grep tests**

```python
# ---- Layer 2: endpoint + wiring symbols -----------------------------------
def test_upload_endpoint_uses_should_reprocess():
    src = open(_MAIN, encoding="utf-8").read()
    assert "should_reprocess(" in src
    assert "/api/local-videos/rematch" in src


def test_rematch_helper_defined_and_scoped():
    src = open(_LT, encoding="utf-8").read()
    assert "def rematch_unmatched(user_id, db" in src
    assert 'transcription_status == "done"' in src
    assert "matched_job_id == None" in src
```

Run: expect FAIL on both.

- [ ] **Step 2: `rematch_unmatched` in `local_transcribe.py`** (below `_maybe_auto_match`):

```python
def rematch_unmatched(user_id, db: Session) -> dict:
    """Re-score every done-but-unmatched LocalVideo for this user against the
    CURRENT awaiting_finishing pool.  Called by the browser after each scan.
    Closes the race: video uploaded before its job reached Finishing used to
    stay 'done - no match' forever (match ran once, at upload time).
    """
    from models import LocalVideo, Job
    pool = (
        db.query(Job.id)
        .filter(Job.user_id == user_id, Job.lifecycle_stage == "awaiting_finishing")
        .first()
    )
    if not pool:
        return {"checked": 0, "matched": 0}
    vids = (
        db.query(LocalVideo)
        .filter(
            LocalVideo.user_id == user_id,
            LocalVideo.transcription_status == "done",
            LocalVideo.matched_job_id == None,  # noqa: E711
        )
        .all()
    )
    matched = 0
    for v in vids:
        _maybe_auto_match(v, db)
        if v.matched_job_id:
            matched += 1
    if vids:
        # v822 diagnostic - remove after operator-side evidence lands.
        print(f"[local] rematch sweep user={str(user_id)[:8]} checked={len(vids)} matched={matched}", flush=True)
    return {"checked": len(vids), "matched": matched}
```

- [ ] **Step 3: Upload endpoint reprocess path in `main.py`** — replace the `if existing: return existing.to_dict()` block:

```python
    existing = (
        db.query(LocalVideo)
        .filter_by(user_id=current_user.id, file_hash=file_hash)
        .first()
    )
    if existing:
        from local_transcribe import should_reprocess
        if should_reprocess(existing.transcription_status, existing.created_at):
            # v822: failed or stuck row - re-run the pipeline with the fresh
            # bytes instead of returning the dead row (was a permanent miss).
            blob = await file.read()
            if not blob or len(blob) < 1024:
                raise HTTPException(400, detail=f"file too small ({len(blob)}B)")
            if len(blob) > 500 * 1024 * 1024:
                raise HTTPException(413, detail="file > 500MB")
            print(f"[local] v822 reprocess hash={file_hash[:8]} (was {existing.transcription_status})", flush=True)
            existing.file_name = file_name
            existing.size_bytes = len(blob)
            existing.transcription_status = "pending"
            existing.transcription_error = None
            existing.transcription = None
            db.commit()
            transcribe_local(existing, blob, db)
            db.refresh(existing)
        return existing.to_dict()
```

- [ ] **Step 4: Rematch endpoint in `main.py`** (after the DELETE endpoint):

```python
@app.post("/api/local-videos/rematch")
async def rematch_local_videos(
    db: DBSession = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """v822: sweep done-but-unmatched local videos against the current
    awaiting_finishing pool.  Browser calls this once per poll cycle."""
    from local_transcribe import rematch_unmatched
    return rematch_unmatched(current_user.id, db)
```

- [ ] **Step 5: Run tests (all green) + import check**

```bash
cd code && python -m pytest tests/test_local_watch_never_miss.py -v
python -c "import main"   # py_compile insufficient per code/CLAUDE.md
```

- [ ] **Step 6: Commit**

```bash
git add main.py local_transcribe.py tests/test_local_watch_never_miss.py
git commit -m "v822: reprocess failed/stuck local rows + rematch sweep endpoint"
```

---

### Task 4: Frontend — seen-set retry policy + rematch call

**Files:**
- Modify: `code/static/index.html` (`_refreshSeenHashesFromServer` L4828-4838; `_scanAndUpload` tail)

- [ ] **Step 1: Replace `_refreshSeenHashesFromServer`**

```js
        const LOCAL_RETRY_STUCK_AFTER_MS = 10 * 60 * 1000;
        function _localRowRetryable(v) {
          // Mirror of server-side should_reprocess (v822): failed and stuck
          // pending/running rows are NOT "seen" - the next scan re-uploads
          // them and the server re-runs the pipeline.
          if (v.transcription_status === "failed") return true;
          if (v.transcription_status === "pending" || v.transcription_status === "running") {
            const t = v.created_at ? Date.parse(v.created_at) : 0;
            return !t || (Date.now() - t > LOCAL_RETRY_STUCK_AFTER_MS);
          }
          return false;
        }

        async function _refreshSeenHashesFromServer() {
          // Server is the source of truth for "already processed". Rows that
          // failed or got stuck are EXCLUDED so they retry (v822).
          try {
            const r = await _fetchT("/api/local-videos", { credentials: "include" });
            if (!r.ok) return;
            const arr = await r.json();
            _localSeenHashes = new Set(arr.filter(v => !_localRowRetryable(v)).map(v => v.file_hash));
          } catch (e) { /* no-op */ }
        }
```

(`_fetchT` arrives in Task 6; if implementing Task 4 first keep `fetch` and switch in Task 6.)

- [ ] **Step 2: Call the rematch sweep at the end of `_scanAndUpload`** — inside the `try`, after the delete pass, before the `if (newOnes.length || deletedJobIds.length)` block:

```js
            // v822: server-side re-match sweep - catches videos uploaded
            // before their job reached Finishing ("done - no match" race).
            let rematched = 0;
            try {
              const rr = await _fetchT("/api/local-videos/rematch", {
                method: "POST", credentials: "include",
              });
              if (rr.ok) {
                const d = await rr.json();
                rematched = d.matched || 0;
                if (rematched) console.log(`[local] rematch sweep matched ${rematched} video(s)`);
              }
            } catch (e) { /* sweep is best-effort */ }
```

and widen the refresh condition:

```js
            if (newOnes.length || deletedJobIds.length || rematched) {
```

- [ ] **Step 3: Commit**

```bash
git add static/index.html
git commit -m "v822: watcher retries failed/stuck rows + calls rematch sweep each poll"
```

---

### Task 5: Frontend — stability gate, hash cache, delete debounce, folder-switch guard, skip surfacing

**Files:**
- Modify: `code/static/index.html` (`_scanAndUpload` L4872-4962, state vars L4734-4737)

- [ ] **Step 1: Add state** (next to `_localSeenHashes`):

```js
        let _localStatCache = new Map();        // "name|size|mtime" -> sha256 (skip re-reads)
        let _localPendingStability = new Map(); // name -> "size|mtime" from last scan (settling files)
        let _localMissingStreak = new Map();    // hash -> consecutive scans missing
        let _localDeleteEligible = new Set();   // hashes OBSERVED in this folder this session
        let _localLastSkipped = [];             // [{name, reason}] from last scan
        let _localLastScanAt = 0;
        let _localScanStartedAt = 0;
        let _localPermissionLost = false;
```

- [ ] **Step 2: Replace the file loop inside `_scanAndUpload`**

```js
            const newOnes = [];
            const currentScanHashes = new Set();
            const skipped = [];
            let fileCount = 0;
            for await (const entry of _localDirHandle.values()) {
              if (entry.kind !== "file") continue;
              if (!_localIsVideo(entry.name)) continue;
              fileCount++;
              let file;
              try { file = await entry.getFile(); }
              catch (e) { skipped.push({ name: entry.name, reason: "unreadable (locked?)" }); continue; }
              if (!file || file.size < 1024) { if (file) skipped.push({ name: entry.name, reason: "<1KB (still writing?)" }); continue; }
              if (file.size > 500 * 1024 * 1024) { skipped.push({ name: entry.name, reason: ">500MB" }); continue; }

              const statKey = `${entry.name}|${file.size}|${file.lastModified}`;
              const cachedHash = _localStatCache.get(statKey);
              if (cachedHash) {
                // Unchanged since a previous scan - no re-read, no re-hash.
                currentScanHashes.add(cachedHash);
                _localDeleteEligible.add(cachedHash);
                _localMissingStreak.delete(cachedHash);
                _localPendingStability.delete(entry.name);
                continue;
              }

              // v822 stability gate: a file must show the SAME size+mtime on
              // two consecutive scans before we read it. Kills the
              // partial-file-upload -> wrong-match -> full-file-orphaned chain.
              const sig = `${file.size}|${file.lastModified}`;
              if (_localPendingStability.get(entry.name) !== sig) {
                _localPendingStability.set(entry.name, sig);
                skipped.push({ name: entry.name, reason: "settling (new/changed - next scan)" });
                continue;
              }
              _localPendingStability.delete(entry.name);

              const buf = await file.arrayBuffer();
              const hash = await _sha256Hex(buf);
              _localStatCache.set(statKey, hash);
              currentScanHashes.add(hash);
              _localDeleteEligible.add(hash);
              _localMissingStreak.delete(hash);
              if (_localSeenHashes.has(hash)) continue;

              const fd = new FormData();
              fd.append("file", new Blob([buf], { type: file.type || "video/mp4" }), file.name);
              fd.append("file_hash", hash);
              try {
                const r = await _fetchT("/api/local-videos/upload", {
                  method: "POST", credentials: "include", body: fd,
                }, 180000);
                if (r.ok) {
                  const data = await r.json();
                  _localSeenHashes.add(hash);
                  newOnes.push(data);
                } else {
                  console.warn("[local] upload failed:", r.status, await r.text());
                }
              } catch (e) {
                console.warn("[local] upload exception:", e);
              }
            }
            _localLastSkipped = skipped;
```

- [ ] **Step 3: Replace the delete pass**

```js
            // Delete pass (v822): debounced 2 scans + only hashes actually
            // observed in THIS folder (folder-switch must not mass-revert).
            const deletedJobIds = [];
            if (fileCount > 0 || _localSeenHashes.size === 0) {
              for (const seenHash of Array.from(_localSeenHashes)) {
                if (currentScanHashes.has(seenHash)) { _localMissingStreak.delete(seenHash); continue; }
                if (!_localDeleteEligible.has(seenHash)) continue; // never observed here
                const streak = (_localMissingStreak.get(seenHash) || 0) + 1;
                _localMissingStreak.set(seenHash, streak);
                if (streak < 2) continue; // transient lock / AV scan blip
                try {
                  const r = await _fetchT(`/api/local-videos/by-hash/${seenHash}`, {
                    method: "DELETE", credentials: "include",
                  });
                  if (r.ok) {
                    const data = await r.json().catch(() => ({}));
                    _localSeenHashes.delete(seenHash);
                    _localDeleteEligible.delete(seenHash);
                    _localMissingStreak.delete(seenHash);
                    if (data.reverted_job_id) deletedJobIds.push(data.reverted_job_id);
                  } else if (r.status === 404) {
                    _localSeenHashes.delete(seenHash);
                    _localMissingStreak.delete(seenHash);
                  }
                } catch (e) {
                  console.warn("[local] delete exception:", e);
                }
              }
            } else {
              console.warn(`[local] scan returned 0 files but seen-set has ${_localSeenHashes.size} - skipping delete pass (likely permission blip)`);
            }
```

- [ ] **Step 4: Reset per-folder state in `localPickFolder` + `localUnpickFolder`** — after `_localDirHandle = handle;` (and in unpick after clearing the handle):

```js
            _localStatCache = new Map();
            _localPendingStability = new Map();
            _localMissingStreak = new Map();
            _localDeleteEligible = new Set();
```

- [ ] **Step 5: Commit**

```bash
git add static/index.html
git commit -m "v822: stability gate + hash cache + delete debounce + folder-switch guard"
```

---

### Task 6: Frontend — timeouts, watchdog, permission surfacing, visibility rescan, last-scan display

**Files:**
- Modify: `code/static/index.html`

- [ ] **Step 1: `_fetchT` helper** (above `_idbOpen`):

```js
        function _fetchT(url, opts = {}, timeoutMs = 20000) {
          // v822: every watcher fetch gets a timeout - one hung request used
          // to leave _localPollInFlight=true forever (watcher silently dead).
          const ctl = new AbortController();
          const t = setTimeout(() => ctl.abort(), timeoutMs);
          return fetch(url, { ...opts, signal: ctl.signal }).finally(() => clearTimeout(t));
        }
```

Switch remaining watcher `fetch(` calls (`refreshLocalPanel` list load, manual ✕ delete, `_uploadLocalFile` if kept) to `_fetchT`.

- [ ] **Step 2: Harden `_scanAndUpload` entry**

```js
        async function _scanAndUpload() {
          if (_localPollInFlight) {
            // v822 watchdog: a scan stuck >5min means a hung await slipped
            // through - clear the flag so polling resumes.
            if (_localScanStartedAt && Date.now() - _localScanStartedAt > 5 * 60 * 1000) {
              console.warn("[local] v822 watchdog: clearing stuck in-flight flag");
              _localPollInFlight = false;
            } else {
              return;
            }
          }
          if (!_localDirHandle) return;
          let ok = false;
          try { ok = await _verifyDirPermission(_localDirHandle, false); } catch (e) { ok = false; }
          if (!ok) {
            if (!_localPermissionLost) {
              _localPermissionLost = true;
              document.title = "⚠ " + document.title.replace(/^⚠ /, "");
              refreshLocalPanel();
            }
            return;
          }
          if (_localPermissionLost) {
            _localPermissionLost = false;
            document.title = document.title.replace(/^⚠ /, "");
            refreshLocalPanel();
          }
          _localPollInFlight = true;
          _localScanStartedAt = Date.now();
          try {
```

and in the `finally`:

```js
          } finally {
            _localPollInFlight = false;
            _localLastScanAt = Date.now();
          }
```

NOTE: the poll path uses `interactive=false` — `requestPermission` from a timer has no user gesture and throws. The CLICK paths (Pick button, Force rescan) keep `interactive=true`.

- [ ] **Step 3: Status line in `refreshLocalPanel`** — replace the `watching` branch:

```js
            if (_localDirHandle && _localPermissionLost) {
              statusEl.innerHTML = `<strong style="color:#f66;">PERMISSION LOST</strong> - watching <strong>${escapeHtml(_localDirHandle.name)}</strong> stopped. Click <strong>Pick local folder</strong> to re-grant.`;
              statusEl.style.color = "#f66";
              if (unpickBtn) unpickBtn.style.display = "";
            } else if (_localDirHandle && _localPollTimer) {
              const persists = _localIsInstalledPWA() ? " (persists across sessions)" : "";
              const age = _localLastScanAt ? Math.round((Date.now() - _localLastScanAt) / 1000) : null;
              const ageTxt = age == null ? "" : (age > 90 ? ` — <span style="color:#fc8;">last scan ${age}s ago</span>` : ` — last scan ${age}s ago`);
              const skipTxt = _localLastSkipped.length
                ? ` — <span style="color:#fc8;" title="${escapeHtml(_localLastSkipped.map(s => `${s.name}: ${s.reason}`).join("\n"))}">${_localLastSkipped.length} skipped</span>`
                : "";
              statusEl.innerHTML = `watching <strong>${escapeHtml(_localDirHandle.name)}</strong> — polling every ${Math.round(LOCAL_POLL_INTERVAL_MS / 1000)}s${persists}${ageTxt}${skipTxt}`;
              statusEl.style.color = "#9b9";
              if (unpickBtn) unpickBtn.style.display = "";
            } else if (_localDirHandle) {
```

- [ ] **Step 4: Visibility rescan** (after the Force-rescan listener):

```js
        // v822: background tabs get throttled - scan immediately on return.
        document.addEventListener("visibilitychange", () => {
          if (document.visibilityState === "visible" && _localPollTimer) {
            _scanAndUpload();
          }
        });
```

- [ ] **Step 5: Commit**

```bash
git add static/index.html
git commit -m "v822: fetch timeouts + watchdog + permission surfacing + visibility rescan"
```

---

### Task 7: Marker tests + full verification

**Files:**
- Test: extend `code/tests/test_local_watch_never_miss.py`

- [ ] **Step 1: Frontend marker tests**

```python
# ---- Layer 2: frontend markers --------------------------------------------
def test_frontend_stability_gate_and_cache():
    src = open(_INDEX, encoding="utf-8").read()
    assert "_localPendingStability" in src
    assert "_localStatCache" in src
    assert "_localMissingStreak" in src
    assert "_localDeleteEligible" in src


def test_frontend_timeout_and_rematch_wiring():
    src = open(_INDEX, encoding="utf-8").read()
    assert "function _fetchT(" in src
    assert '"/api/local-videos/rematch"' in src
    assert "_localRowRetryable" in src
    assert "visibilitychange" in src
```

- [ ] **Step 2: Full run**

```bash
cd code && python -m pytest tests/test_local_watch_never_miss.py -v && python -c "import main; import local_transcribe"
```

Expected: all PASS, imports clean.

- [ ] **Step 3: Commit**

```bash
git add tests/test_local_watch_never_miss.py
git commit -m "v822: marker tests for watcher hardening"
```

---

### Task 8: Docs (v-rule checklist per code/CLAUDE.md)

**Files:**
- Modify: `code/template_reference.md` (append §v822 deep-dive: the 9 miss-paths table + fixes)
- Modify: `wiki/patterns/conventions.md` (one-line index row)
- Modify: `wiki/log.md` (timeline entry)
- Skeleton/bundles: NO (no output-shape change). Root CLAUDE.md quickref: NO (platform-internal, not an authoring rule).

- [ ] **Step 1: Write the three doc entries; commit code + wiki separately**

---

### Task 9: Ship

- [ ] **Step 1: Push branch** (`git push -u origin v822-local-watch-never-miss`). Merge to `main` = deploy decision — surface to operator with the operator-side verification checklist below. Do NOT merge v821 into main as a side effect.
- [ ] **Step 2: After merge lands on Render, operator-side evidence** (per §2 no-"should work"):
  1. Drop a fresh export into the watched folder mid-write → panel shows "settling" skip, then upload on the NEXT scan (single hash, no partial row).
  2. Kill a transcription (unplug net mid-upload) → row shows failed → next scan re-uploads → row goes done. Render log: `[local] v822 reprocess hash=…`.
  3. Export a final cut BEFORE moving its job to Finishing → "done — no match" → move job to Finishing → within one poll the card flips to MATCH. Render log: `[local] rematch sweep … matched=1`.
  4. Revoke folder permission (site settings) → status turns RED + `⚠` title.
  5. Delete a matched file → nothing on first scan, revert on second (~60s).
- [ ] **Step 3: Remove the two v822 diagnostic prints in a follow-up commit after evidence lands.**
- [ ] **Step 4: Spawn `caveman:cavecrew-reviewer` on the commit set (per code/CLAUDE.md).**
