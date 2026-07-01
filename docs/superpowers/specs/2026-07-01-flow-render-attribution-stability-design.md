# Flow render attribution — stability design (click-bracketing)

Date: 2026-07-01
Status: approved design, pre-plan
Scope: `code/static/flow_worker.py` only (worker attribution path). No platform/API/UI change.

---

## Problem

The worker's #1 instability is **renders not caught / misattributed**: a clip generates in Flow but the worker fails to download it, downloads the WRONG clip's video, false-fails a good clip, or redoes needlessly.

Almost every attribution v-rule (v700, v700i, v700j, v729, v739, v774, v792) is a patch on ONE root cause.

## Root cause — timing-coupled attribution

Today, to learn a clip's render ID (`primaryMediaId`), the worker must **catch the ephemeral submit response inside a time window**:

1. Worker clicks Generate for clip N.
2. A Playwright response listener buffers `batchAsyncGenerateVideo…` responses per account (`_install_submit_response_listener`, L1494; buffer `_SUBMIT_RESPONSE_BUFFERS`, L293).
3. `_bind_pending_submits` (L1708) drains that buffer within `drain_timeout` (40s at the main sites, L16701/L18450) and writes `render_id → (job, clip_index)` into `_PRIMARY_MEDIA_BINDINGS` (L291).
4. If the response does not arrive in the window → **WARN → fall back to DOM `data-index` tile-position attribution** (L1798-1803) + stamp a 90s late-bind slot (`_V729_LATE_BIND_*`, L331/L1817).

Two fragilities:

- **The window race.** Flow response latency is ~35–59s (see comment L16701); the 40s window is borderline and the 2nd variant lands ~59s post-click, so binding routinely misses.
- **The DOM fallback misattributes.** `data-index` tiles reindex as new clips are added to the same project, so position-based matching grabs the wrong tile → wrong download, false hard-fail, useless redo.

The authoritative, DOM-free, render-ID-keyed source `batchCheckAsyncVideoGenerationStatus` (lists every media in the project with `name`=uuid, `mediaGenerationStatus`, `createTime`, workflow/batch) is **already received** by the worker but is used **only for failure reasons** (`_scan_failure_reason`, L1427) — never for attribution.

## Key invariant this design relies on

**Submits are sequential per account** — one clip's Generate is clicked, awaited, then the next (comment L1819: "sequential submits inside one account"). Therefore clip N owns exactly the renders that appear **between its own Generate click and the next click on that account**. That bracket is a stable identity that needs no fixed timeout and never consults the DOM.

---

## Design — Direction B: windowless bracket bind (primary) + keep the old stack (fallback)

Additive. Nothing existing is deleted. The new path is a new, earlier, windowless **writer** into the SAME `_PRIMARY_MEDIA_BINDINGS` map, so every downstream consumer (`bound_media_ids_for_clip` L1353, `captured_urls_for_clip` L1363, v774 recovery, the HTTP download) works **unchanged**.

### Component 1 — click log (new)

`_ACCOUNT_CLICK_LOG: dict[account] -> list[{click_at, job_id, clip_index, clip_id}]` (+ lock), append-only, ordered by `click_at` (local wall-clock at the moment Generate is clicked).

`_stamp_generate_click(account, job_id, clip_index, clip_id)` appends an entry. Consecutive entries define brackets: clip at index k owns `[click_at[k], click_at[k+1])`; the last entry's bracket is open-ended until job end / reconcile.

Redo is just another click at a later time: a redo of clip 2 appends a new entry; its render (captured after the redo click) binds to that entry. v700i already purges the clip's stale binding at re-submit (L1727), so the fresh render wins.

### Component 2 — render ledger (new)

`_RENDER_LEDGER: dict[render_id] -> {workflow_id, batch_id, create_time, status, project_id, account, captured_at}` (+ lock).

Fed passively from traffic the worker already receives — **no new network calls**:
- **submit responses** — extend the existing listener drain / `_extract_media_bindings` (L1307) to also record each render into the ledger with its **local `captured_at`** (skew-free) plus server `create_time`/`batch`/`workflow`.
- **status polls** — extend `_scan_failure_reason` (L1427, already parsing `batchCheckAsyncVideoGenerationStatus`) to upsert each `media[].name` with its `mediaGenerationStatus` + `createTime`. This is the backstop source when a submit response was dropped entirely.

### Component 3 — bracket bind (new, windowless, primary)

`bracket_bind_render(render_id)`: look up the render's `captured_at` (preferred, local, skew-free) — or server `create_time` for status-poll-only renders — find the click-log bracket on the same account that contains it, and write `_PRIMARY_MEDIA_BINDINGS[render_id] = {job, clip_index, clip_id, …}` for that bracket's clip. Idempotent; re-binding the same render to the same clip is a no-op.

Invoked:
- whenever a render is added/updated in the ledger (so a render that shows up 59s late still auto-attributes to its bracket — this **replaces the role of v729 late-bind** with a cleaner, uncapped rule);
- once at submit time (best-effort immediate bind);
- once at download/decision time before the fallback runs (a final attempt).

Because it writes the existing map, `bound_media_ids_for_clip` / `captured_urls_for_clip` already read it — **no consumer change**.

### Component 4 — old stack kept as fallback (unchanged)

`_bind_pending_submits` window-bind, v729 late-bind, v774 mediaId-recovery, and the DOM `data-index` fallback stay **verbatim**. They run exactly as today and only matter when bracket bind produced nothing for a clip (dropped submit response AND no ledger entry). Layer order at any decision point is therefore: **bracket binding (new) → window/late-bind binding (old) → DOM data-index (old, last resort)**. If the new path is ever wrong or silent, the old path still resolves the clip.

### Component 5 — every path, incl. all redos

`_stamp_generate_click` must fire at every Generate click. Integration points (each verified during planning):
- main submit `process_job_submission` (L17274) and `…_with_failover` (L15819) — at/next to the existing `_bind_pending_submits_for_page` calls (L16698, L18447);
- policy-retry resubmits (L5966, L6207, L19185, L19393);
- **redo** `process_redo_clip` (L15193) → `rebuild_clip` (L15043, generate click L15143) — this path today may not call `_bind_pending_submits_for_page`; add the stamp here explicitly (this is the redo guarantee the operator asked for);
- `click_reuse_and_generate` (L8699) and `click_generate_button` (L8410);
- golden-restore self-resume resubmits.

Cleanest placement: stamp inside the shared `_bind_pending_submits_for_page` (covers all its call sites) AND add one explicit stamp in the redo generate path that bypasses it. Planning task 1 audits each site and lists exactly where the stamp is missing.

### Component 6 — end-of-job reconcile (safety net)

Before a job (or redo job) is marked done, one pass reads the latest ledger (fed by the status poll) for the project and, per clip, confirms its bracket render reached `SUCCESSFUL` **and** was downloaded; otherwise route `FAILED` / redo via the existing paths. Catches anything all layers missed. Same code for main and redo jobs.

### Component 7 — kill switch

Env flag `FLOW_BRACKET_ATTRIBUTION` (default `on`). When `off`, `_stamp_generate_click` still records (cheap) but bracket bind does not write bindings — behaviour is identical to today (pure legacy). Lets the operator revert instantly in production without a code change if the new path ever misbehaves.

---

## Why this is stabler

- Attribution is bounded by a **real event (the next click)**, not an arbitrary 40s/90s timeout → no window race.
- It never consults the drifting DOM `data-index` on the happy path → the misattribution source is bypassed.
- It reuses the render-ID→URL capture that already works and writes the existing binding map → tiny consumer blast radius.
- It is **additive** — old machinery is untouched and remains the fallback, so worst case is "no worse than today."

## Risks + mitigations

- **Clock skew** (server `createTime` vs local click time): prefer the submit response's **local `captured_at`** for bracketing; use server `createTime` only for status-poll-only (dropped-response) renders, where the old fallback also backs it up.
- **Parallel accounts**: click log + ledger are keyed by account; brackets never cross accounts.
- **Bracket boundary jitter** (response captured just after the next click): the same render also carries `workflow_id`/`batch_id`; if a render's `captured_at` is within a small epsilon of a boundary, prefer the bracket whose submit response `batch_id` matches. (Planning decides whether this epsilon guard is needed or if `captured_at` ordering alone suffices.)
- **Regression**: kill switch returns to pure-legacy instantly.

## Testing / verification

Reuse the live-golden harness (`scratchpad/e2e_video_worker.py`) driving the shipped path:
1. Multi-clip job (≥3 clips, x2 variants) → assert each clip binds via bracket first (`[bracket] clip N ← render <id> (captured_at in bracket)`), not via window/DOM.
2. Force a **redo** of one clip → assert the redo click opens a new bracket and the redo render binds to it, old binding purged.
3. Simulate a **dropped submit response** (skip the listener for one clip) → assert the status-poll ledger + reconcile still resolve it, and that the DOM fallback is only reached when both bracket and ledger are empty.
4. Kill switch `off` → assert behaviour byte-identical to legacy (no bracket writes).

Per repo rule: `import flow_worker` (not just `py_compile`) before push; add a temporary diagnostic log line on each new write path; spawn `caveman:cavecrew-reviewer` on the commit set; verify against operator-side production evidence before claiming fixed.

## Out of scope

Session eviction / 403 throttling / UI-finder fragility (separate failure classes). Content-side rejects (PROMINENT_PEOPLE / RAI). No platform, API, or UI changes.
