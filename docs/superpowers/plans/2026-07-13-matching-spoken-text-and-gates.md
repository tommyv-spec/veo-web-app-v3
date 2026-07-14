# Matching Accuracy Phase 1 — Spoken Text, Time Constraint, Confidence Gate

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make video→job matching compare the reel against the words *actually spoken* in the render, drop jobs that could not have produced the reel, and refuse to present an ambiguous match as a confident one.

**Architecture:** All matching decisions become pure, DB-free functions in `instagram_match.py` (stdlib only — matches the existing test style, which loads the module with `importlib` and never touches a DB). One canonical DB builder in `local_transcribe.py` feeds all three consumers (IG suggestions, drive watcher, local-folder watcher). `main.py` loses its dead duplicate builder and gains a pool-level time filter.

**Tech Stack:** Python 3, SQLAlchemy, FastAPI, pytest. No new dependencies.

---

## Background — why the matcher is wrong today

Three defects, all verified in the code:

1. **The matcher compares against words that were never spoken.** `Clip.rendered_prompt_variant` (`models.py:395`) records whether the downloaded render came from Prompt A or the Prompt-B policy fallback; `Clip.dialogue_text_b` (`models.py:393`) holds *"the reworded line spoken if Prompt B rendered"*. Neither builder reads either column. Every B-fallback clip therefore contributes the wrong words, dragging the correct job's score down and letting a near-duplicate twin win on shared boilerplate.

2. **Clips that never made the final cut still contribute text.** The export is "all approved clips" (`main.py:9062`), but the builder filters only on `clip_role != 'audio_pair'` — rejected / failed / skipped clips still pollute the job's text.

3. **No time constraint.** A job created *after* a reel was posted cannot be that reel's source, but nothing excludes it.

**The b-roll pairing contract** (documented at `main.py:3827-3839`), which the reconstruction must honour:
- `single` → spoken words in `dialogue_text`
- `visual_pair` → **silent** b-roll visual; `dialogue_text` is empty; the spoken words live in `voiceover_line`
- `audio_pair` → the twin that **actually renders the speech**; its `dialogue_text` duplicates the sibling's `voiceover_line`

Consequence: a Prompt-B rewording on an **audio_pair** makes the visual twin's `voiceover_line` stale. The rebuild must reach *through* the pair to the audio twin. A Prompt-B rewording on a **visual_pair** is irrelevant — that render is silent.

**Why `created_at`, not `export_at`, for the time constraint:** `export_at` was backfilled as `COALESCE(completed_at, NOW())` (`models.py:1168`), so on legacy rows it can be a *migration* timestamp — later than reels that were posted long before. Filtering on it would drop correct old jobs. `Job.created_at` (`models.py:166`) is set at row insert and never backfilled, so it is trustworthy.

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `instagram_match.py` | Pure matching rules — stdlib only, DB-free, unit-testable | **Modify**: add `spoken_line`, `reconstruct_dialogue`, `job_predates_post`, `match_verdict` |
| `local_transcribe.py` | The ONE canonical DB→dialogue builder + local watcher | **Modify**: `_bulk_dialogue_map` becomes B-aware + final-cut-aware |
| `main.py` | IG suggestions + drive watcher endpoints | **Modify**: delete dead `_job_full_dialogue`; add time filter; return verdict |
| `static/index.html` | Suggestion popover | **Modify**: surface score + verdict, warn on ambiguous |
| `test_instagram_match.py` | Pure-rule tests | **Modify**: add cases for the 4 new functions |
| `tests/test_local_watch_never_miss.py` | Builder tests | **Modify**: fake rows gain the new columns |

---

### Task 1: Spoken-line + dialogue reconstruction (pure rules)

**Files:**
- Modify: `instagram_match.py` (append after `_normalize`, before `_phrase_boost`)
- Test: `test_instagram_match.py`

- [ ] **Step 1: Write the failing tests**

Append to `test_instagram_match.py`:

```python
# ---- v823: spoken-text reconstruction (Prompt B + final cut) --------------

def _clip(**kw):
    """A Clip row as the bulk builder hands it to the pure rules."""
    base = {
        "id": 1, "clip_index": 0, "clip_role": None, "paired_clip_id": None,
        "dialogue_text": "", "dialogue_text_b": None,
        "rendered_prompt_variant": "A", "voiceover_line": None,
        "approval_status": "approved",
    }
    base.update(kw)
    return base


def test_spoken_line_variant_a_uses_dialogue_text():
    m = _load()
    c = _clip(dialogue_text="your soldier wont wake up", dialogue_text_b="a reworded line")
    assert m.spoken_line(c) == "your soldier wont wake up"


def test_spoken_line_variant_b_uses_reworded_line():
    m = _load()
    c = _clip(dialogue_text="the banned wording",
              dialogue_text_b="the reworded line that was actually said",
              rendered_prompt_variant="B")
    assert m.spoken_line(c) == "the reworded line that was actually said"


def test_spoken_line_variant_b_without_b_text_falls_back():
    m = _load()
    c = _clip(dialogue_text="original line", dialogue_text_b=None,
              rendered_prompt_variant="B")
    assert m.spoken_line(c) == "original line"


def test_reconstruct_uses_audio_twin_not_stale_voiceover_line():
    """The audio_pair renders the speech. When IT fell back to Prompt B, the
    visual twin's voiceover_line is stale and must NOT be used."""
    m = _load()
    clips = [
        _clip(id=10, clip_index=0, clip_role="visual_pair",
              dialogue_text="", voiceover_line="the stale original line"),
        _clip(id=11, clip_index=100000, clip_role="audio_pair", paired_clip_id=10,
              dialogue_text="the stale original line",
              dialogue_text_b="the reworded line actually spoken",
              rendered_prompt_variant="B"),
    ]
    assert m.reconstruct_dialogue(clips) == "the reworded line actually spoken"


def test_reconstruct_visual_pair_without_twin_falls_back_to_voiceover_line():
    m = _load()
    clips = [_clip(id=10, clip_role="visual_pair", dialogue_text="",
                   voiceover_line="spoken over the b-roll")]
    assert m.reconstruct_dialogue(clips) == "spoken over the b-roll"


def test_reconstruct_drops_clips_not_in_the_final_cut():
    m = _load()
    clips = [
        _clip(id=1, clip_index=0, dialogue_text="kept line", approval_status="approved"),
        _clip(id=2, clip_index=1, dialogue_text="rejected line", approval_status="rejected"),
        _clip(id=3, clip_index=2, dialogue_text="pending line", approval_status="pending_review"),
    ]
    assert m.reconstruct_dialogue(clips) == "kept line"


def test_reconstruct_never_blanks_a_job_with_no_approved_clips():
    """A legacy job with nothing marked approved must still produce text —
    blank text would silently drop it from the candidate pool entirely."""
    m = _load()
    clips = [
        _clip(id=1, clip_index=0, dialogue_text="first line", approval_status="pending_review"),
        _clip(id=2, clip_index=1, dialogue_text="second line", approval_status="pending_review"),
    ]
    assert m.reconstruct_dialogue(clips) == "first line second line"


def test_reconstruct_orders_by_clip_index():
    m = _load()
    clips = [
        _clip(id=2, clip_index=1, dialogue_text="second"),
        _clip(id=1, clip_index=0, dialogue_text="first"),
    ]
    assert m.reconstruct_dialogue(clips) == "first second"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd code && python -m pytest test_instagram_match.py -v -k "spoken or reconstruct"`
Expected: FAIL — `AttributeError: module 'instagram_match' has no attribute 'spoken_line'`

- [ ] **Step 3: Implement the rules**

In `instagram_match.py`, insert after `_normalize()` (around line 19):

```python
# ============================================================================
# v823 — SPOKEN-TEXT RECONSTRUCTION.
#
# The matcher must compare the reel's transcript against the words that were
# actually SAID in the render we downloaded — not against the script we first
# wrote. Two things make those diverge:
#
#   1. Prompt B (v805/v821 policy fallback). When the primary prompt trips a
#      generation-policy block, the worker re-renders with Prompt B, which
#      speaks a REWORDED line. `rendered_prompt_variant` says which prompt
#      produced the downloaded render; `dialogue_text_b` holds the reworded
#      words. Reading dialogue_text alone compares against words nobody spoke.
#
#   2. The b-roll clip pair (v698A). `visual_pair` is a SILENT b-roll visual;
#      the speech is rendered by its `audio_pair` twin, whose dialogue_text
#      duplicates the visual's voiceover_line. So when the AUDIO twin fell back
#      to Prompt B, the visual's voiceover_line is stale — the rebuild has to
#      reach through the pair. (A Prompt-B fallback on the visual twin is
#      irrelevant: that render is silent.)
#
# Only clips that made the FINAL CUT count — the export is "all approved clips".
# ============================================================================

FINAL_CUT_APPROVAL = "approved"


def spoken_line(clip):
    """The words actually heard in the render downloaded for this clip.

    `clip` is a plain dict (kept DB-free so these rules stay unit-testable).
    """
    if ((clip.get("rendered_prompt_variant") or "A").upper() == "B"):
        reworded = (clip.get("dialogue_text_b") or "").strip()
        if reworded:
            return reworded
    return ((clip.get("voiceover_line") or clip.get("dialogue_text")) or "").strip()


def reconstruct_dialogue(clips, final_cut_only=True):
    """Concatenate a job's SPOKEN words, in clip_index order.

    clips: list of dicts with keys id, clip_index, clip_role, paired_clip_id,
           dialogue_text, dialogue_text_b, rendered_prompt_variant,
           voiceover_line, approval_status.
    """
    # The audio twin owns the speech for its visual partner.
    audio_by_visual = {}
    for c in clips:
        if (c.get("clip_role") or "") == "audio_pair" and c.get("paired_clip_id"):
            audio_by_visual[c["paired_clip_id"]] = spoken_line(c)

    def _emit(pool):
        parts = []
        for c in sorted(pool, key=lambda x: (x.get("clip_index") or 0)):
            role = c.get("clip_role") or "single"
            if role == "audio_pair":
                continue  # emitted via its visual twin; counting both double-counts
            if role == "visual_pair":
                text = (
                    audio_by_visual.get(c.get("id"))
                    or (c.get("voiceover_line") or "").strip()
                    or (c.get("dialogue_text") or "").strip()
                )
            else:
                text = spoken_line(c)
            if text:
                parts.append(text)
        return " ".join(parts).strip()

    if final_cut_only:
        kept = [
            c for c in clips
            if (c.get("clip_role") or "") == "audio_pair"  # lookup source, never filtered
            or (c.get("approval_status") or "") == FINAL_CUT_APPROVAL
        ]
        text = _emit(kept)
        if text:
            return text
        # Fall through: a job with nothing marked approved (legacy rows) must
        # not reconstruct to BLANK — that would drop it from the candidate pool
        # entirely, which is strictly worse than matching on slightly noisy text.
    return _emit(clips)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd code && python -m pytest test_instagram_match.py -v -k "spoken or reconstruct"`
Expected: PASS — 8 passed

- [ ] **Step 5: Commit**

```bash
cd code
git add instagram_match.py test_instagram_match.py
git commit -m "feat(match): reconstruct the words actually SPOKEN (Prompt B + final cut)"
```

---

### Task 2: Time constraint + confidence verdict (pure rules)

**Files:**
- Modify: `instagram_match.py` (append at end of file)
- Test: `test_instagram_match.py`

- [ ] **Step 1: Write the failing tests**

Append to `test_instagram_match.py`:

```python
# ---- v823: time constraint + confidence verdict ---------------------------
import datetime as _dt


def test_job_created_after_the_reel_was_posted_is_impossible():
    m = _load()
    posted = _dt.datetime(2026, 6, 1, 12, 0, 0)
    created_after = _dt.datetime(2026, 6, 10, 12, 0, 0)   # 9 days AFTER the post
    assert m.job_predates_post(created_after, posted) is False


def test_job_created_before_the_reel_is_eligible():
    m = _load()
    posted = _dt.datetime(2026, 6, 10, 12, 0, 0)
    created_before = _dt.datetime(2026, 6, 1, 12, 0, 0)
    assert m.job_predates_post(created_before, posted) is True


def test_job_created_just_after_post_survives_on_clock_skew_slack():
    m = _load()
    posted = _dt.datetime(2026, 6, 10, 12, 0, 0)
    created = _dt.datetime(2026, 6, 10, 20, 0, 0)  # 8h later — within 1-day slack
    assert m.job_predates_post(created, posted) is True


def test_unknown_timestamps_never_exclude():
    m = _load()
    assert m.job_predates_post(None, _dt.datetime(2026, 6, 1)) is True
    assert m.job_predates_post(_dt.datetime(2026, 6, 1), None) is True


def test_verdict_confident_when_top_is_high_and_clear():
    m = _load()
    ranked = [{"job_id": "a", "score": 0.80}, {"job_id": "b", "score": 0.40}]
    v = m.match_verdict(ranked, high=0.50, margin=0.12)
    assert v["verdict"] == "confident"


def test_verdict_ambiguous_when_twins_are_neck_and_neck():
    m = _load()
    ranked = [{"job_id": "a", "score": 0.80}, {"job_id": "b", "score": 0.78}]
    v = m.match_verdict(ranked, high=0.50, margin=0.12)
    assert v["verdict"] == "ambiguous"


def test_verdict_weak_when_nothing_scores_well():
    m = _load()
    ranked = [{"job_id": "a", "score": 0.20}, {"job_id": "b", "score": 0.05}]
    v = m.match_verdict(ranked, high=0.50, margin=0.12)
    assert v["verdict"] == "weak"


def test_verdict_none_on_empty_ranking():
    m = _load()
    assert m.match_verdict([], high=0.5, margin=0.12)["verdict"] == "none"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd code && python -m pytest test_instagram_match.py -v -k "job_created or unknown_timestamps or verdict"`
Expected: FAIL — `AttributeError: module 'instagram_match' has no attribute 'job_predates_post'`

- [ ] **Step 3: Implement the rules**

Add `from datetime import timedelta` to the imports at the top of `instagram_match.py`, then append at the end of the file:

```python
# ============================================================================
# v823 — HARD TIME CONSTRAINT.
#
# A reel was rendered and exported BEFORE it was posted, so a job created AFTER
# the reel went live cannot possibly be its source. This is a hard fact, not a
# similarity score — it separates near-duplicate twins that the WORDS cannot,
# because twins are typically built days apart.
#
# We gate on Job.created_at, NOT Job.export_at: export_at was backfilled as
# COALESCE(completed_at, NOW()) (models.py:1168), so on legacy rows it can be a
# MIGRATION timestamp — later than reels posted long before — and filtering on
# it would drop the correct old job. created_at is written at row insert and
# never backfilled.
# ============================================================================

JOB_CREATED_SLACK_DAYS = 1.0


def job_predates_post(job_created_at, posted_at, slack_days=JOB_CREATED_SLACK_DAYS):
    """False only when the job was created AFTER the reel was posted (+slack).

    Unknown timestamps never exclude — an absent posted_at must not silently
    empty the candidate pool.
    """
    if job_created_at is None or posted_at is None:
        return True
    return job_created_at <= posted_at + timedelta(days=slack_days)


def match_verdict(ranked, high, margin):
    """Classify a ranking so the UI can refuse to present a guess as a fact.

    confident — top clears `high` AND clearly beats the runner-up.
    ambiguous — top is strong but a twin sits within `margin`: the words cannot
                tell them apart, so a human must.
    weak      — nothing scored well enough to trust.
    """
    if not ranked:
        return {"verdict": "none", "top": 0.0, "margin": 0.0}
    top = ranked[0]["score"]
    second = ranked[1]["score"] if len(ranked) > 1 else 0.0
    gap = round(top - second, 4)
    if top < high:
        verdict = "weak"
    elif gap < margin:
        verdict = "ambiguous"
    else:
        verdict = "confident"
    return {"verdict": verdict, "top": top, "margin": gap}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd code && python -m pytest test_instagram_match.py -v`
Expected: PASS — all tests (including the 4 pre-existing `score`/`best_matches` ones)

- [ ] **Step 5: Commit**

```bash
cd code
git add instagram_match.py test_instagram_match.py
git commit -m "feat(match): hard time constraint + confidence verdict"
```

---

### Task 3: Make the ONE canonical DB builder B-aware

**Files:**
- Modify: `local_transcribe.py:87-115` (`_bulk_dialogue_map`)
- Modify: `tests/test_local_watch_never_miss.py:136-145`

Note the builder now needs `audio_pair` rows (as the lookup source for their visual twins), so the SQL `clip_role != 'audio_pair'` filter is **removed** — the exclusion moves into `reconstruct_dialogue`, which drops them from the OUTPUT while still reading them. Rows are unpacked **positionally** so the fake-row tuples in the tests keep working.

- [ ] **Step 1: Update the failing test**

Replace `test_bulk_dialogue_map_groups_and_coalesces` in `tests/test_local_watch_never_miss.py` with:

```python
def _row(job_id, clip_id=1, clip_index=0, role=None, paired=None,
         dt="", dtb=None, variant="A", vo=None, approval="approved"):
    """Positional row tuple matching the _bulk_dialogue_map query column order."""
    return (clip_id, job_id, clip_index, role, paired, dt, dtb, variant, vo, approval)


def test_bulk_dialogue_map_groups_and_coalesces():
    lt = _load_lt()
    rows = [
        _row("j1", clip_id=1, clip_index=0, dt="hello"),
        _row("j1", clip_id=2, clip_index=1, dt="", vo="world"),   # voiceover_line preferred
        _row("j2", clip_id=3, clip_index=0, dt="solo"),
    ]
    m = lt._bulk_dialogue_map(_FakeDB(rows), ["j1", "j2"])
    assert m["j1"] == "hello world"
    assert m["j2"] == "solo"


def test_bulk_dialogue_map_uses_prompt_b_reworded_line():
    """v823: a clip that rendered via Prompt B speaks dialogue_text_b."""
    lt = _load_lt()
    rows = [
        _row("j1", clip_id=1, clip_index=0, dt="the banned wording",
             dtb="the reworded line actually spoken", variant="B"),
    ]
    m = lt._bulk_dialogue_map(_FakeDB(rows), ["j1"])
    assert m["j1"] == "the reworded line actually spoken"


def test_bulk_dialogue_map_excludes_rejected_clips():
    """v823: only clips that made the final cut are in the exported video."""
    lt = _load_lt()
    rows = [
        _row("j1", clip_id=1, clip_index=0, dt="kept", approval="approved"),
        _row("j1", clip_id=2, clip_index=1, dt="rejected", approval="rejected"),
    ]
    m = lt._bulk_dialogue_map(_FakeDB(rows), ["j1"])
    assert m["j1"] == "kept"
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd code && python -m pytest tests/test_local_watch_never_miss.py -v -k bulk_dialogue`
Expected: FAIL — the old builder unpacks 3-tuples, so the 10-tuple rows raise `ValueError: too many values to unpack`

- [ ] **Step 3: Rewrite the builder**

Replace `_bulk_dialogue_map` in `local_transcribe.py` (lines 87-115) with:

```python
def _bulk_dialogue_map(db, job_ids) -> dict:
    """{job_id: the words actually SPOKEN in the render} for MANY jobs, ONE query.

    THE canonical dialogue builder — feeds the IG suggestions endpoint, the
    drive watcher, and the local-folder watcher. The reconstruction rules live
    in instagram_match.reconstruct_dialogue (pure + unit-tested); this function
    is only the DB glue.

    v823: audio_pair rows are now FETCHED (they are the lookup source for their
    visual twin's spoken words — a Prompt-B fallback on the audio twin makes the
    visual's voiceover_line stale). reconstruct_dialogue drops them from the
    OUTPUT, so nothing is double-counted.

    v822.3: replaces the old per-job `_full_dialogue` N+1. At pool=226 the
    N+1 fired 226 Clip queries PER video, and the v822 rematch sweep ran that
    for ~33 unmatched videos every 30s (~7,500 queries + 7,500 char-level
    SequenceMatchers) synchronously on the web worker → gunicorn WORKER
    TIMEOUT → SIGABRT → killed in-flight DB connections → SSL SYSCALL EOF
    across the whole platform (prod incident 2026-07-06).
    """
    from collections import defaultdict
    from models import Clip
    import instagram_match as _ig_match
    job_ids = list(job_ids)
    if not job_ids:
        return {}
    rows = (
        db.query(
            Clip.id, Clip.job_id, Clip.clip_index, Clip.clip_role, Clip.paired_clip_id,
            Clip.dialogue_text, Clip.dialogue_text_b, Clip.rendered_prompt_variant,
            Clip.voiceover_line, Clip.approval_status,
        )
        .filter(Clip.job_id.in_(job_ids))
        .order_by(Clip.job_id, Clip.clip_index.asc())
        .all()
    )
    by_job = defaultdict(list)
    for (cid, jid, cidx, role, paired, dt, dtb, variant, vo, approval) in rows:
        by_job[jid].append({
            "id": cid,
            "clip_index": cidx,
            "clip_role": role,
            "paired_clip_id": paired,
            "dialogue_text": dt,
            "dialogue_text_b": dtb,
            "rendered_prompt_variant": variant,
            "voiceover_line": vo,
            "approval_status": approval,
        })
    return {jid: _ig_match.reconstruct_dialogue(clips) for jid, clips in by_job.items()}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd code && python -m pytest tests/test_local_watch_never_miss.py -v`
Expected: PASS — all tests, including the 3 bulk_dialogue ones

- [ ] **Step 5: Delete the dead duplicate**

`_job_full_dialogue` (`main.py:3824-3848`) has **zero callers** — confirm, then delete the whole function.

Run: `cd code && grep -rn "_job_full_dialogue" . --include=*.py`
Expected: only the definition line. If any caller appears, replace it with
`_bulk_dialogue_map(db, [job_id]).get(job_id, "")` instead of deleting.

- [ ] **Step 6: Verify the app still imports**

Run: `cd code && python -c "import main, local_transcribe, instagram_match; print('imports OK')"`
Expected: `imports OK`

- [ ] **Step 7: Commit**

```bash
cd code
git add local_transcribe.py main.py tests/test_local_watch_never_miss.py
git commit -m "feat(match): one canonical B-aware dialogue builder; drop dead duplicate"
```

---

### Task 4: Apply the time constraint to the IG candidate pool

**Files:**
- Modify: `main.py` — `suggest_matches` (the `candidates` query, ~line 4190)

- [ ] **Step 1: Filter the pool after loading it**

In `suggest_matches`, immediately after the `candidates = (...).all()` block and BEFORE `dmap = _bulk_dialogue_map(...)`, insert:

```python
    # v823 — a job created AFTER the reel was posted cannot be its source. This
    # is a hard fact, and it separates near-duplicate twins (same script, built
    # days apart) that the WORDS alone cannot tell apart. Applied in Python, not
    # SQL, so an absent posted_at can never silently empty the pool.
    _before = len(candidates)
    candidates = [
        j for j in candidates
        if _ig_match.job_predates_post(j.created_at, v.posted_at)
    ]
    if _before != len(candidates):
        print(f"[ig-suggest] video={video_id} time-filter dropped "
              f"{_before - len(candidates)} job(s) created after posted_at={v.posted_at}",
              flush=True)
```

- [ ] **Step 2: Verify the module imports and the route still resolves**

Run:
```bash
cd code && python -c "
import main
r = [x for x in main.app.routes if 'suggestions' in getattr(x, 'path', '')]
print([ (sorted(x.methods), x.path) for x in r ])
"
```
Expected: `[(['GET'], '/api/instagram/videos/{video_id}/suggestions')]`

- [ ] **Step 3: Commit**

```bash
cd code
git add main.py
git commit -m "feat(match): drop jobs created after the reel was posted"
```

---

### Task 5: Return the confidence verdict from the suggestions endpoint

**Files:**
- Modify: `main.py` — `suggest_matches` return block (~line 4205-4218)

- [ ] **Step 1: Replace the return block**

Replace the ranking + return tail of `suggest_matches` (from `ranked = _ig_match.rank_tfidf(...)` through `return top`) with:

```python
    from local_transcribe import _bulk_dialogue_map, _MATCH_IDF_POWER, _MATCH_HIGH, _MATCH_MARGIN
    ranked_all = _ig_match.rank_tfidf(v.transcription or "", pairs, idf_power=_MATCH_IDF_POWER)
    # v823 — the verdict is computed on the FULL ranking, before truncation:
    # the runner-up that makes a match ambiguous must be seen even if we only
    # SHOW five. Previously this endpoint returned top-5 "regardless of score",
    # so a 0.02 guess looked exactly like a 0.95 certainty in the popover.
    verdict = _ig_match.match_verdict(ranked_all, _MATCH_HIGH, _MATCH_MARGIN)
    ranked = ranked_all[:5]
    print(f"[ig-suggest] video={video_id} pool={len(candidates)} "
          f"verdict={verdict['verdict']} top={verdict['top']:.3f} margin={verdict['margin']:.3f} "
          f"top5={[(r['job_id'][:8], r['score']) for r in ranked]}", flush=True)
    top = []
    for r in ranked:
        clip = db.query(Clip).filter(Clip.job_id == r["job_id"], Clip.clip_index == 0).first()
        slug = (clip.dialogue_text or "")[:80] if clip and clip.dialogue_text else r["job_id"][:8]
        top.append({"job_id": r["job_id"], "score": r["score"], "slug": slug})
    return {"verdict": verdict["verdict"], "top": verdict["top"],
            "margin": verdict["margin"], "suggestions": top}
```

Also delete the now-duplicated `from local_transcribe import _bulk_dialogue_map, _MATCH_IDF_POWER` line that preceded `dmap = ...`, and move `dmap`/`pairs` above this block if they are not already there.

**Breaking change:** the endpoint returned a bare JSON *array*; it now returns an *object*. The only caller is the popover in `static/index.html` (Task 6), updated in the same commit.

- [ ] **Step 2: Verify import**

Run: `cd code && python -c "import main; print('import OK')"`
Expected: `import OK`

- [ ] **Step 3: Commit (with Task 6 — do not ship the API change alone)**

Hold this commit until Task 6 is done; the frontend would break on an array→object change.

---

### Task 6: Surface the verdict in the popover

**Files:**
- Modify: `static/index.html:5595-5611` (the `.ig-match-btn` handler)

- [ ] **Step 1: Replace the fetch + popover render**

Replace lines 5595-5611 (from `const r = await fetch(...suggestions...)` through the `.join("");`) with:

```javascript
            const r = await fetch(`/api/instagram/videos/${videoId}/suggestions`, { credentials: "include" });
            const data = r.ok ? await r.json() : {};
            const suggestions = data.suggestions || [];
            if (!suggestions.length) {
              alert("No finishing-lane jobs left to match against. Either every candidate is already linked to another IG video, or none has reached that stage yet.");
              return;
            }
            const existing = document.querySelector(".ig-suggestion-popover");
            if (existing) existing.remove();
            const pop = document.createElement("div");
            pop.className = "ig-suggestion-popover";
            pop.style.cssText = "position:absolute;background:var(--bg-elevated);border:1px solid var(--border);border-radius:6px;padding:6px;z-index:1000;min-width:280px;box-shadow:0 4px 12px rgba(0,0,0,.4);";
            // v823 — say out loud how much to trust this. "ambiguous" means two
            // near-duplicate scripts scored within noise of each other: the words
            // genuinely cannot separate them, so the operator must.
            const BANNER = {
              ambiguous: ["#f59e0b", "⚠ Too close to call — two builds score nearly the same. Check before picking."],
              weak:      ["#ef4444", "⚠ Weak match — nothing scored well. The right job may not be in the pool."],
              confident: ["#10b981", "✓ Clear winner."],
            }[data.verdict];
            const banner = BANNER
              ? `<div style="font-size:10px;color:${BANNER[0]};padding:3px 4px 5px;line-height:1.3;">${BANNER[1]}</div>`
              : "";
            pop.innerHTML = banner + suggestions.map((s, i) =>
              `<div class="ig-suggestion-row" data-job-id="${escapeHtml(s.job_id)}" data-video-id="${videoId}"
                    style="padding:3px 4px;cursor:pointer;${i === 0 && data.verdict === "confident" ? "font-weight:600;" : ""}">
                <span>${escapeHtml((s.slug || s.job_id).slice(0, 50))}</span>
                <span style="float:right;color:#888;">${Math.round(s.score * 100)}%</span>
              </div>`
            ).join("");
```

- [ ] **Step 2: Verify the changed block is syntactically valid**

Run:
```bash
cd code && python - <<'PY'
import re
src = open('static/index.html', encoding='utf-8').read()
assert 'data.suggestions || []' in src, 'popover not reading the new object shape'
assert 'Too close to call' in src, 'ambiguity banner missing'
print('frontend markers OK')
PY
```
Expected: `frontend markers OK`

- [ ] **Step 3: Commit API + frontend together**

```bash
cd code
git add main.py static/index.html
git commit -m "feat(match): confidence gate — never present an ambiguous guess as a fact"
```

---

### Task 7: Full test run + deploy

- [ ] **Step 1: Run the whole suite**

Run: `cd code && python -m pytest test_instagram_match.py tests/test_local_watch_never_miss.py -v`
Expected: PASS, no failures

- [ ] **Step 2: Import the real app one last time**

Run: `cd code && python -c "import main, local_transcribe, instagram_match; print('imports OK')"`
Expected: `imports OK` (py_compile is NOT sufficient — see `code/CLAUDE.md`)

- [ ] **Step 3: Push (auto-deploys to Render)**

```bash
cd code && git push origin main
cd .. && git add code && git commit -m "build: bump code (matching phase 1)"
```

- [ ] **Step 4: Verify against production**

After the deploy is green, click **Match?** on an Instagram video and read the Render log:

```
[ig-suggest] video=<id> pool=<n> verdict=<confident|ambiguous|weak> top=0.NNN margin=0.NNN top5=[...]
```

Confirm: (a) `pool` is smaller than before (the time filter dropped impossible jobs), (b) a video whose job used Prompt B now ranks its correct job at #1. Report the actual log line — do NOT claim success without it.

---

## Self-Review

**Spec coverage:**
- Tier 1 (spoken text: `dialogue_text_b` when variant B, final-cut clips only) → Tasks 1, 3 ✅
- Tier 2 (time constraint) → Tasks 2, 4 ✅
- Tier 3 (confidence gate on the IG path) → Tasks 2, 5, 6 ✅
- Local-folder watcher covered → Task 3 (it consumes `_bulk_dialogue_map`, so it inherits Tier 1 automatically; its `auto_pick` margin gate already exists) ✅
- Drive watcher covered → Task 3 (same shared builder, `main.py:4643`) ✅
- Tier 4 (duration + audio fingerprint) → **separate plan**, `2026-07-13-matching-media-identity.md`

**Known gap, deliberate:** the local + drive watchers do NOT get the time constraint in this plan — their pool is `lifecycle_stage == 'awaiting_finishing'` and their input file has no `posted_at` (a local file has no post time). The equivalent constraint for them is a duration check, which is Phase 2.
