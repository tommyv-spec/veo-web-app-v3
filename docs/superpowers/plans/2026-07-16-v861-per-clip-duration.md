# v861 — Per-clip duration from word count Implementation Plan

> **Renumbered v857 → v861 (2026-07-16).** v857 was already the one-job-one-video gate (live in `main.py` / `drive_transcribe.py` / `instagram_match.py`, undocumented in `template_reference.md` — which is how the collision slipped through). v858 (image-regenerate), v859 (multi-reference chain), and v860 (rolling-deploy lock-hang guard) are also taken; v861 was verified free across both repos' code, docs, and full commit history. **Lesson: the v-number space lives in COMMIT HISTORY and CODE COMMENTS, not just `template_reference.md` — check all three before claiming a number.** Commits `a1cb294`, `7429630`, `c04ece9`, `4566ed3`, `c7dad6b` predate the renumber and still say v857 in their messages; history was deliberately not rewritten because a concurrent session's commit is interleaved in this branch.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every clip renders at its own duration (4/6/8/10s) picked from the spoken line's word count, declared explicitly in `videos/*.md` and honored by both render paths — instead of one job-level duration with a last-clip-only guess.

**Architecture:** One helper module owns the bucket math. The markdown parser reads a new per-line `- **clip_duration_s:**` bullet; `prepare_batch_for_video` resolves the final bucket (explicit > v667 anchor > word-count auto > NULL) into the ALREADY-EXISTING but currently-dead `clips.veo_render_duration_s` column. The two render paths read that column: `worker.py` (Veo API) passes it as `override_duration` per clip and clamps 10→8 because the Veo API only accepts 4/6/8; `flow_worker.py` sets `page._duration` per clip before each `select_frames_to_video_mode` call, which already knows how to click a 4s/6s/8s/10s tab. Legacy builds without the field keep importing (auto-compute fallback); the `/build` auditor is what makes the field mandatory going forward.

**Tech Stack:** Python 3 · FastAPI (`main.py`) · SQLAlchemy (`models.py`) · pytest (`code/tests/`) · Playwright (`static/flow_worker.py`) · Google Veo 3.1 API (`veo_generator.py`)

---

## Locked decisions (operator, 2026-07-16)

| Decision | Value |
|---|---|
| Bucket table | Operator numbers LITERAL as upper bounds |
| v831 line cap | 25 → **28 words** (forward-only; shipped builds untouched) |
| Markdown field | **Explicit + mandatory** at authoring (auditor hard-FAIL). Parser auto-computes as fallback so 180 legacy builds still import. |
| Render paths | Flow **and** Veo API. The 1080p/interpolation 8s pin (`main.py:2006-2010`) stays as-is. |

**The bucket table (v861):**

| words `W` | duration |
|---|---|
| `W <= 11` | 4s |
| `12 <= W <= 16` | 6s |
| `17 <= W <= 24` | 8s |
| `25 <= W <= 28` | 10s |
| `W > 28` | 10s + log warning (auditor FAILs the build first) |

Implied speech rate 2.67–3.0 words/sec; least-squares fit of the operator's 4 points = 2.8 w/s.

**Verified constraint:** the Veo API accepts `durationSeconds` of 4, 6, or 8 only ([ai.google.dev/gemini-api/docs/veo](https://ai.google.dev/gemini-api/docs/veo)). 10s exists only in Flow's 2026-07 composer (`static/flow_worker.py:7532-7552`, commit `1297f4d`). The API path MUST clamp 10→8 and log it.

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `code/clip_duration.py` | **NEW.** Sole owner of the v861 bucket math + the allowed-values constant. Pure functions, no imports from the app. | Create |
| `code/tests/test_v861_clip_duration.py` | **NEW.** Unit tests for the bucket table + precedence resolver. | Create |
| `code/image_platform.py` | Markdown parser + `prepare_batch_for_video` resolver. | Modify |
| `code/main.py` | PATCH validator (allow 10) + Flow clips payload field. | Modify |
| `code/worker.py` | Veo API path: per-clip `override_duration` + 10→8 clamp. | Modify |
| `code/static/flow_worker.py` | Flow path: per-clip `page._duration` + per-clip prompt duration. | Modify |
| `code/template_reference.md` | §v861 canonical deep-dive + v831 cap amendment. | Modify |
| `code/template_new_format.md` | Skeleton gains the field. | Modify |
| `~/.claude/skills/build-video/audit_build.py` | New mandatory check + v831 cap 25→28. | Modify |
| `wiki/patterns/conventions.md` | One-row index entry. | Modify |
| `wiki/log.md` | Timeline entry. | Modify |

`static/flow_worker.py` is shipped standalone to the operator's machine and CANNOT import `code/clip_duration.py`. It gets the number from the API payload instead — the math is never duplicated.

---

### Task 1: The bucket-math module

**Files:**
- Create: `code/clip_duration.py`
- Test: `code/tests/test_v861_clip_duration.py`

> **Amended after code review (2026-07-16).** The code blocks below are the FIRST DRAFT. Quality review found the module validated its front door (`explicit`) but left the side doors open, and the front-door check itself coerced before validating. The shipped module differs on five points — read `code/clip_duration.py` for the truth:
> 1. `explicit` is validated BEFORE coercion (`6.7` raises instead of silently becoming `6`; bools rejected).
> 2. `anchor_bucket` passes the same gate as `explicit` (was trusted blindly — a Task-3 wiring slip onto the adjacent `target_duration_s` float would have gone silent).
> 3. `clamp_for_veo_api` → **renamed `veo_api_duration_s`** and made honest: folds only 10→8, passes 4/6/8 through as `int`, raises on anything else. The old catch-all `return 8` sent below-range input (`2`, `-5`) UP to the longest bucket.
> 4. `LINE_WORD_CAP` **deleted** — nothing read it, and it duplicated a cap enforced in `audit_build.py` (outside this repo, cannot import). Exactly the drift this module exists to prevent. Cap now lives in the auditor + §v861 prose.
> 5. Tests extended to cover the above; the `"a b c" * 20` fixture (41 words, missing space — not the 60 intended) replaced with `" ".join(["w"] * 60)`.

- [ ] **Step 1: Write the failing test**

Create `code/tests/test_v861_clip_duration.py`:

```python
"""v861 — per-clip duration bucket math."""
import pytest

from clip_duration import (
    ALLOWED_CLIP_DURATIONS_S,
    VEO_API_DURATIONS_S,
    clamp_for_veo_api,
    pick_clip_duration_s,
    resolve_clip_duration_s,
)


@pytest.mark.parametrize("words,expected", [
    (0, 4), (1, 4), (11, 4),          # <=11 -> 4s
    (12, 6), (16, 6),                  # 12-16 -> 6s
    (17, 8), (24, 8),                  # 17-24 -> 8s
    (25, 10), (28, 10),                # 25-28 -> 10s
    (29, 10), (60, 10),                # >28 -> 10s (auditor FAILs the build)
])
def test_pick_clip_duration_s_buckets(words, expected):
    assert pick_clip_duration_s(words) == expected


def test_operator_anchor_points():
    """The 4 points the operator specified on 2026-07-16."""
    assert pick_clip_duration_s(11) == 4    # "less than 12 words is 4 seconds"
    assert pick_clip_duration_s(16) == 6    # "16 is 6 seconds"
    assert pick_clip_duration_s(24) == 8    # "24 is 8 seconds"
    assert pick_clip_duration_s(28) == 10   # "around 28 words we 10 seconds"


def test_allowed_values():
    assert ALLOWED_CLIP_DURATIONS_S == (4, 6, 8, 10)
    assert VEO_API_DURATIONS_S == (4, 6, 8)


def test_clamp_for_veo_api():
    assert clamp_for_veo_api(4) == 4
    assert clamp_for_veo_api(6) == 6
    assert clamp_for_veo_api(8) == 8
    assert clamp_for_veo_api(10) == 8   # Veo API has no 10s bucket
    assert clamp_for_veo_api(None) is None


def test_resolve_precedence_explicit_wins():
    assert resolve_clip_duration_s(explicit=6, anchor_bucket=8, line_text="a b c" * 20) == 6


def test_resolve_precedence_anchor_beats_wordcount():
    assert resolve_clip_duration_s(explicit=None, anchor_bucket=8, line_text="two words") == 8


def test_resolve_precedence_wordcount_when_no_anchor():
    # 18 words -> 8s bucket
    assert resolve_clip_duration_s(
        explicit=None, anchor_bucket=None,
        line_text=" ".join(["w"] * 18)) == 8


def test_resolve_returns_none_for_silent_scene():
    """No explicit, no anchor, no words -> NULL -> job default duration applies."""
    assert resolve_clip_duration_s(explicit=None, anchor_bucket=None, line_text="") is None
    assert resolve_clip_duration_s(explicit=None, anchor_bucket=None, line_text=None) is None


def test_resolve_rejects_bad_explicit():
    with pytest.raises(ValueError, match="not in"):
        resolve_clip_duration_s(explicit=7, anchor_bucket=None, line_text="hello")
```

- [ ] **Step 2: Run test to verify it fails**

Run from `code/`: `python -m pytest tests/test_v861_clip_duration.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'clip_duration'`

- [ ] **Step 3: Write minimal implementation**

Create `code/clip_duration.py`:

```python
"""v861 — per-clip render duration picked from the spoken line's word count.

SINGLE SOURCE OF TRUTH for the bucket math. Imported by image_platform.py
(resolve at import time) and main.py (validate the PATCH). worker.py and
static/flow_worker.py do NOT import this — they read the resolved integer off
the Clip row / API payload, so the table lives in exactly one place.

Operator table (2026-07-16), literal upper bounds:

    words <= 11   -> 4s
    12..16        -> 6s
    17..24        -> 8s
    25..28        -> 10s
    > 28          -> 10s + caller logs a warning (v831 caps lines at 28 words;
                     the /build auditor FAILs before a build gets this far)

Implied speech rate 2.67-3.0 words/sec (least-squares fit of the operator's
four points = 2.8 w/s). Full deep-dive: template_reference.md §v861.
"""
from typing import Optional

# Everything the platform can ask for. Flow's 2026-07 composer has a
# 4s/6s/8s/10s tablist (static/flow_worker.py select_frames_to_video_mode).
ALLOWED_CLIP_DURATIONS_S = (4, 6, 8, 10)

# What the Veo API itself accepts on durationSeconds — 10s does NOT exist here.
# https://ai.google.dev/gemini-api/docs/veo
VEO_API_DURATIONS_S = (4, 6, 8)

# (max_words, duration_s) — first row whose max_words the count fits under wins.
_BUCKETS = ((11, 4), (16, 6), (24, 8), (28, 10))

# v831 (amended 2026-07-16) — a spoken line over this many words must be split
# into two clips. Was 25; raised to 28 so the 10s bucket is reachable.
LINE_WORD_CAP = 28


def pick_clip_duration_s(word_count: int) -> int:
    """Map a word count to its v861 duration bucket. Never returns None."""
    for max_words, duration in _BUCKETS:
        if word_count <= max_words:
            return duration
    return 10  # over the cap — biggest bucket; caller should warn


def clamp_for_veo_api(duration_s: Optional[int]) -> Optional[int]:
    """Veo API has no 10s bucket — fold 10 down to 8. None passes through."""
    if duration_s is None:
        return None
    if duration_s in VEO_API_DURATIONS_S:
        return duration_s
    return 8


def resolve_clip_duration_s(
    explicit: Optional[int],
    anchor_bucket: Optional[int],
    line_text: Optional[str],
) -> Optional[int]:
    """Final per-clip duration. Precedence, highest first:

    1. ``explicit``      — the scene's `- **clip_duration_s:**` bullet (v861)
    2. ``anchor_bucket`` — the v667 frame-anchor-derived bucket (transformation
                           montages; already ceil'd to [4,6,8] by the caller)
    3. word count of ``line_text`` — the v861 table
    4. None              — no line, no anchor: the job-level duration applies

    Raises ValueError on an explicit value outside ALLOWED_CLIP_DURATIONS_S.
    """
    if explicit is not None:
        if int(explicit) not in ALLOWED_CLIP_DURATIONS_S:
            raise ValueError(
                "clip_duration_s %r not in %r (v861)"
                % (explicit, list(ALLOWED_CLIP_DURATIONS_S))
            )
        return int(explicit)
    if anchor_bucket is not None:
        return int(anchor_bucket)
    words = len((line_text or "").split())
    if words == 0:
        return None
    return pick_clip_duration_s(words)
```

- [ ] **Step 4: Run test to verify it passes**

Run from `code/`: `python -m pytest tests/test_v861_clip_duration.py -v`
Expected: PASS — 17 passed

- [ ] **Step 5: Commit**

```bash
git add code/clip_duration.py code/tests/test_v861_clip_duration.py
git commit -m "feat(v861): clip duration bucket math from line word count

Buckets: <=11w->4s, 12-16->6s, 17-24->8s, 25-28->10s. Operator table
2026-07-16, literal upper bounds. Veo API clamps 10->8 (API has no 10s
bucket); Flow's composer has a real 10s tab.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Parse the `- **clip_duration_s:**` bullet

**Files:**
- Modify: `code/image_platform.py:4123-4155` (bullet loop), `:4196-4215` (scene dict)
- Test: `code/tests/test_v861_clip_duration.py`

The bullet is PER LINE, attaching to the closest preceding `- **line:**` — same rule the v644 `pad` bullet already uses. A scene with two lines can hold two different durations.

- [ ] **Step 1: Write the failing test**

Append to `code/tests/test_v861_clip_duration.py`:

```python
from image_platform import _parse_scene_blocks_new

_MD_ONE = """### Scene 1
- **image:** image_1
- **clip_mode:** fresh
- **line:** your soldier will not wake up in the morning anymore
- **clip_duration_s:** 6
- **action_note:** she lifts the banana. [Start beat]
"""

_MD_TWO_LINES = """### Scene 1
- **image:** image_1
- **line:** first line here
- **clip_duration_s:** 4
- **line:** second line is quite a lot longer than the first one right here
- **clip_duration_s:** 8
"""

_MD_NONE = """### Scene 1
- **image:** image_1
- **line:** no duration bullet on this one
"""


def test_parse_clip_duration_single_line():
    scenes = _parse_scene_blocks_new(_MD_ONE, {1})
    assert scenes[0]["clip_durations"] == [6]


def test_parse_clip_duration_attaches_per_line():
    scenes = _parse_scene_blocks_new(_MD_TWO_LINES, {1})
    assert scenes[0]["lines"] == [
        "first line here",
        "second line is quite a lot longer than the first one right here",
    ]
    assert scenes[0]["clip_durations"] == [4, 8]


def test_parse_clip_duration_absent_is_none():
    scenes = _parse_scene_blocks_new(_MD_NONE, {1})
    assert scenes[0]["clip_durations"] == [None]


def test_parse_clip_duration_rejects_bad_value():
    md = _MD_ONE.replace("- **clip_duration_s:** 6", "- **clip_duration_s:** 7")
    with pytest.raises(ValueError, match="clip_duration_s"):
        _parse_scene_blocks_new(md, {1})
```

- [ ] **Step 2: Run test to verify it fails**

Run from `code/`: `python -m pytest tests/test_v861_clip_duration.py -k parse -v`
Expected: FAIL — `KeyError: 'clip_durations'`

- [ ] **Step 3: Add the module import**

At the top of `code/image_platform.py`, alongside the other local imports, add:

```python
from clip_duration import ALLOWED_CLIP_DURATIONS_S
```

- [ ] **Step 4: Extend the bullet regex + parallel array**

In `code/image_platform.py`, replace the bullet-pattern block at `:4123-4142`:

```python
        bullet_pattern = _re.compile(
            r"^\s*[-*]\s*\*\*(line|action_note|pad)\s*:\*\*\s*(.+?)\s*$",
            flags=_re.MULTILINE | _re.IGNORECASE,
        )
        lines_list: List[str] = []
        action_notes: List[Optional[str]] = []
        pads: List[Optional[str]] = []  # v644 parallel array
```

with:

```python
        # v861 — `clip_duration_s` joins the per-line bullet set. Like v644's
        # `pad` it attaches to the closest preceding `line`, so a two-line
        # scene can render its clips at two different durations.
        bullet_pattern = _re.compile(
            r"^\s*[-*]\s*\*\*(line|action_note|pad|clip_duration_s)\s*:\*\*\s*(.+?)\s*$",
            flags=_re.MULTILINE | _re.IGNORECASE,
        )
        lines_list: List[str] = []
        action_notes: List[Optional[str]] = []
        pads: List[Optional[str]] = []  # v644 parallel array
        clip_durations: List[Optional[int]] = []  # v861 parallel array
```

Then in the same loop, replace the `if key == "line":` branch and add a new branch. Replace `:4139-4155`:

```python
            if key == "line":
                lines_list.append(value)
                action_notes.append(None)
                pads.append(None)
            elif key == "action_note":
                if lines_list:
                    # Attach to most recent line
                    action_notes[-1] = value
                else:
                    # v786 — scene-level note on a no-lines (silent /
                    # text_card) scene; kept, not malformed.
                    dangling_action_note = value
            elif key == "pad":
                # v644 — attach pad to most recent line
                if lines_list:
                    pads[-1] = value
                # else: pad before any line — ignore, likely malformed
```

with:

```python
            if key == "line":
                lines_list.append(value)
                action_notes.append(None)
                pads.append(None)
                clip_durations.append(None)  # v861
            elif key == "action_note":
                if lines_list:
                    # Attach to most recent line
                    action_notes[-1] = value
                else:
                    # v786 — scene-level note on a no-lines (silent /
                    # text_card) scene; kept, not malformed.
                    dangling_action_note = value
            elif key == "pad":
                # v644 — attach pad to most recent line
                if lines_list:
                    pads[-1] = value
                # else: pad before any line — ignore, likely malformed
            elif key == "clip_duration_s":
                # v861 — attach the render-duration bucket to most recent line.
                m_dur = _re.match(r"\d+", value)
                if not m_dur:
                    raise ValueError(
                        f"Scene {scene_index}: clip_duration_s {value!r} is not "
                        f"a number (expected one of "
                        f"{list(ALLOWED_CLIP_DURATIONS_S)} — see "
                        f"template_reference.md §v861)"
                    )
                dur_val = int(m_dur.group())
                if dur_val not in ALLOWED_CLIP_DURATIONS_S:
                    raise ValueError(
                        f"Scene {scene_index}: clip_duration_s {dur_val} not in "
                        f"{list(ALLOWED_CLIP_DURATIONS_S)} (v861). Pick the bucket "
                        f"the line's word count lands in: <=11w=4s, 12-16w=6s, "
                        f"17-24w=8s, 25-28w=10s."
                    )
                if lines_list:
                    clip_durations[-1] = dur_val
                else:
                    print(
                        f"[v861/parse] scene_{scene_index} clip_duration_s="
                        f"{dur_val} appears before any `- **line:**` bullet — "
                        f"ignored (malformed)",
                        flush=True,
                    )
```

- [ ] **Step 5: Emit it on the scene dict**

In `code/image_platform.py`, in the `scenes.append({...})` block, add one entry right after the `"pads": pads,` line:

```python
            "clip_durations": clip_durations,  # v861 — parallel to lines; int (4|6|8|10) or None
```

- [ ] **Step 6: Run tests to verify they pass**

Run from `code/`: `python -m pytest tests/test_v861_clip_duration.py -v`
Expected: PASS — all parse tests green

- [ ] **Step 7: Verify no legacy build regressed**

Run from the repo root — every existing build must still parse:

```bash
python - <<'PY'
import sys, glob
sys.path.insert(0, "code")
from image_platform import _parse_scene_blocks_new, _parse_image_blocks_new
import re
bad = []
for p in sorted(glob.glob("videos/*.md")):
    t = open(p, encoding="utf-8").read()
    known = {int(m) for m in re.findall(r"^###\s+Image\s+(\d+)", t, re.M)}
    try:
        _parse_scene_blocks_new(t, known)
    except Exception as e:
        bad.append((p, str(e)[:90]))
print("parsed:", len(glob.glob("videos/*.md")), "failed:", len(bad))
for p, e in bad[:10]:
    print(" FAIL", p, "->", e)
PY
```

Expected: `failed: 0`. Any failure here is a regression from this task — fix before committing.

- [ ] **Step 8: Commit**

```bash
git add code/image_platform.py code/tests/test_v861_clip_duration.py
git commit -m "feat(v861): parse per-line clip_duration_s bullet

Attaches to the closest preceding line, same rule as v644 pad. Rejects any
value outside 4/6/8/10 at import with a message naming the bucket table.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Resolve the duration in `prepare_batch_for_video`

**Files:**
- Modify: `code/image_platform.py:7436-7442` (resolver), `:7584-7626` (silent flat row), `:7632-7662` (per-line flat row)
- Test: `code/tests/test_v861_clip_duration.py`

Precedence: explicit bullet > v667 anchor bucket > word count > NULL. `_ceil_to_veo_bucket` stays on `(4, 6, 8)` — v667's anchor-trim behavior is NOT touched by this rule.

- [ ] **Step 1: Write the failing test**

Append to `code/tests/test_v861_clip_duration.py`:

```python
def test_resolver_is_the_one_used_by_prepare():
    """Guard: prepare_batch_for_video must call the shared resolver, not
    re-implement the table. Fails loudly if someone forks the math."""
    import inspect
    import image_platform
    src = inspect.getsource(image_platform.prepare_batch_for_video)
    assert "resolve_clip_duration_s" in src, (
        "prepare_batch_for_video must resolve v861 durations via "
        "clip_duration.resolve_clip_duration_s")
```

- [ ] **Step 2: Run test to verify it fails**

Run from `code/`: `python -m pytest tests/test_v861_clip_duration.py -k resolver_is_the_one -v`
Expected: FAIL — `AssertionError: prepare_batch_for_video must resolve v861 durations`

- [ ] **Step 3: Widen the import**

In `code/image_platform.py`, change the Task-2 import line to:

```python
from clip_duration import ALLOWED_CLIP_DURATIONS_S, resolve_clip_duration_s
```

- [ ] **Step 4: Keep the anchor bucket separate from the final pick**

In `code/image_platform.py`, replace `:7436-7442`:

```python
        target_duration_s: Optional[float] = None
        veo_render_duration_s: Optional[int] = None
        if this_anchor is not None:
            nxt = _next_anchor_after(scene["scene_index"])
            if nxt is not None and nxt > this_anchor:
                target_duration_s = round(nxt - this_anchor, 3)
                veo_render_duration_s = _ceil_to_veo_bucket(target_duration_s)
```

with:

```python
        target_duration_s: Optional[float] = None
        # v667 anchor-derived bucket — the trim duration for transformation
        # montages. v861 treats this as the SECOND-priority input; an explicit
        # `- **clip_duration_s:**` bullet outranks it.
        anchor_bucket: Optional[int] = None
        if this_anchor is not None:
            nxt = _next_anchor_after(scene["scene_index"])
            if nxt is not None and nxt > this_anchor:
                target_duration_s = round(nxt - this_anchor, 3)
                anchor_bucket = _ceil_to_veo_bucket(target_duration_s)

        # v861 — per-line explicit durations parsed off the scene block.
        # Parallel to `lines`; entries are int (4|6|8|10) or None.
        scene_clip_durations: List[Optional[int]] = scene.get("clip_durations") or []
```

- [ ] **Step 5: Resolve on the silent flat row**

Silent scenes have no lines, so the word-count input is empty and the anchor wins (existing v667 behavior). In `code/image_platform.py`, replace the silent flat row's duration entry at `:7602`:

```python
                "veo_render_duration_s": veo_render_duration_s if scene_is_silent else None,
```

with:

```python
                # v861 — silent scenes have no spoken line, so the pick comes
                # from an explicit bullet if the author set one, else the v667
                # anchor bucket, else NULL (job-level duration applies).
                "veo_render_duration_s": resolve_clip_duration_s(
                    explicit=(scene_clip_durations[0] if scene_clip_durations else None),
                    anchor_bucket=anchor_bucket,
                    line_text=None,
                ) if scene_is_silent else None,
```

- [ ] **Step 6: Resolve on the per-line flat row**

In `code/image_platform.py`, replace the per-line flat row's duration entry at `:7662`:

```python
                "veo_render_duration_s": veo_render_duration_s,
```

with:

```python
                # v861 — per-line pick: explicit bullet > v667 anchor bucket >
                # word count of THIS line > NULL. Each Clip row is 1:1 with a
                # dialogue line, so each carries its own render duration.
                "veo_render_duration_s": _v861_line_duration,
```

and immediately after the `for i_in_scene, (line_text, note, vp, pad) in enumerate(...)` loop header at `:7632-7634`, insert the resolve + log:

```python
            _v861_explicit = (
                scene_clip_durations[i_in_scene]
                if i_in_scene < len(scene_clip_durations) else None
            )
            _v861_line_duration = resolve_clip_duration_s(
                explicit=_v861_explicit,
                anchor_bucket=anchor_bucket,
                line_text=line_text,
            )
            _v861_words = len((line_text or "").split())
            if _v861_explicit is None and _v861_line_duration is not None:
                print(
                    f"[v861/auto] scene_{scene['scene_index']} line {i_in_scene}: "
                    f"{_v861_words}w → {_v861_line_duration}s "
                    f"(no clip_duration_s bullet — auto-picked; declare it per v861)",
                    flush=True,
                )
            elif _v861_explicit is not None:
                _v861_auto = resolve_clip_duration_s(
                    explicit=None, anchor_bucket=None, line_text=line_text)
                _flag = "" if _v861_auto in (None, _v861_explicit) else \
                    f" ⚠ word count suggests {_v861_auto}s"
                print(
                    f"[v861/explicit] scene_{scene['scene_index']} line {i_in_scene}: "
                    f"{_v861_words}w → {_v861_line_duration}s (declared){_flag}",
                    flush=True,
                )
            if _v861_words > 28:
                print(
                    f"[v861/warn] scene_{scene['scene_index']} line {i_in_scene}: "
                    f"{_v861_words}w exceeds the 28-word cap (v831 amended) — "
                    f"split into two clips",
                    flush=True,
                )
```

- [ ] **Step 7: Run tests to verify they pass**

Run from `code/`: `python -m pytest tests/test_v861_clip_duration.py -v`
Expected: PASS — all green

- [ ] **Step 8: Verify the module actually imports (py_compile is not enough)**

Run from `code/`:

```bash
python -c "import image_platform; print('image_platform OK')"
```

Expected: `image_platform OK`

- [ ] **Step 9: Commit**

```bash
git add code/image_platform.py code/tests/test_v861_clip_duration.py
git commit -m "feat(v861): resolve per-clip duration at prepare time

Precedence: explicit clip_duration_s > v667 anchor bucket > line word count >
NULL (job default). Writes the already-existing clips.veo_render_duration_s
column, which nothing read until now. Diagnostic log per clip.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Let the PATCH endpoint accept 10

**Files:**
- Modify: `code/main.py:5980-5988`

`main.py:5982` currently rejects anything not in `(4, 6, 8)`, so a 10s clip cannot be corrected by hand.

- [ ] **Step 1: Add the import**

In `code/main.py`, next to the other local imports, add:

```python
from clip_duration import ALLOWED_CLIP_DURATIONS_S
```

- [ ] **Step 2: Widen the validator**

In `code/main.py`, replace `:5980-5988`:

```python
    # ─── veo_render_duration_s ───────────────────────────────────────────
    if req.veo_render_duration_s is not None:
        if int(req.veo_render_duration_s) not in (4, 6, 8):
```

with:

```python
    # ─── veo_render_duration_s ───────────────────────────────────────────
    # v861 — 10s joined the set (Flow's 2026-07 composer). The Veo API path
    # clamps 10→8 at render time; Flow renders a real 10s clip.
    if req.veo_render_duration_s is not None:
        if int(req.veo_render_duration_s) not in ALLOWED_CLIP_DURATIONS_S:
```

Leave the error message body below it intact — it already interpolates the value; update only the allowed-set text it prints so it reads `(4, 6, 8, 10)`.

- [ ] **Step 3: Verify the module imports**

Run from `code/`: `python -c "import main; print('main OK')"`
Expected: `main OK`

- [ ] **Step 4: Commit**

```bash
git add code/main.py
git commit -m "feat(v861): PATCH accepts veo_render_duration_s=10

Flow's composer has a 10s tab; the validator pinned 4/6/8 and blocked hand
corrections on 10s clips.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Ship the number to the Flow worker

**Files:**
- Modify: `code/main.py:12784-12800` (`clips_data` payload)

`static/flow_worker.py` reads its clips off this payload. Without this field it cannot know the per-clip duration.

- [ ] **Step 1: Add the field to the payload**

In `code/main.py`, in the `clip_data = {` dict, add right after the `"scene_index": clip.scene_index or 0,` line:

```python
            # v861 — per-clip render duration (4|6|8|10). NULL → the worker
            # falls back to the job-level duration (legacy / manual jobs).
            "veo_render_duration_s": clip.veo_render_duration_s,
```

- [ ] **Step 2: Verify the module imports**

Run from `code/`: `python -c "import main; print('main OK')"`
Expected: `main OK`

- [ ] **Step 3: Commit**

```bash
git add code/main.py
git commit -m "feat(v861): send per-clip veo_render_duration_s to the flow worker

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: Veo API path — per-clip duration, 10→8 clamp

**Files:**
- Modify: `code/worker.py:3448-3475`

Today only the LAST clip gets a word-count pick, using a hardcoded 2.5 words/sec and buckets 4/6/8. v861 replaces the whole block: EVERY clip reads its own resolved bucket off the Clip row. The Veo API has no 10s bucket, so 10 folds to 8 with a log line.

- [ ] **Step 1: Add the import**

In `code/worker.py`, next to the other local imports, add:

```python
from clip_duration import pick_clip_duration_s, veo_api_duration_s
```

(`veo_api_duration_s` was named `clamp_for_veo_api` in the first draft of Task 1; renamed during code review because it folds a dialect difference rather than clamping. It raises `ValueError` on anything outside 4/6/8/10.)

- [ ] **Step 2: Replace the last-clip-only block**

In `code/worker.py`, replace `:3448-3475` (from `# Calculate dynamic duration for LAST CLIP` through the `print(f"[Worker] LAST CLIP: ...")` line) with:

```python
                # v861 — per-clip render duration. Every clip (not just the
                # last) renders at the bucket its line's word count lands in.
                # The bucket was resolved at import time from the markdown's
                # `- **clip_duration_s:**` bullet (explicit) or the word count
                # (auto) and stored on the Clip row. NULL → legacy/manual job
                # with no per-clip pick: fall back to the job-level duration
                # by leaving override_duration as None.
                #
                # The Veo API accepts durationSeconds of 4/6/8 ONLY
                # (ai.google.dev/gemini-api/docs/veo) — a 10s pick folds to 8
                # here. Flow's composer renders a real 10s clip; this path
                # cannot.
                override_duration = None
                with get_db() as db:
                    _dur_clip = db.query(Clip).filter(
                        Clip.job_id == job_id,
                        Clip.clip_index == clip_index
                    ).first()
                    _picked = _dur_clip.veo_render_duration_s if _dur_clip else None

                if _picked is None and dialogue_text:
                    # Legacy job imported before v861 — no stored pick. Derive
                    # from the word count so behavior matches a fresh import.
                    _picked = pick_clip_duration_s(len(dialogue_text.split()))
                    _picked_src = "auto (legacy row, no stored pick)"
                else:
                    _picked_src = "clips.veo_render_duration_s"

                if _picked is not None:
                    # v861 — veo_api_duration_s RAISES on anything outside
                    # 4/6/8/10. That is reachable in production, not theory:
                    # clips.veo_render_duration_s is a bare INTEGER with no
                    # CHECK constraint (image_platform.py:173) and main.py:2186
                    # writes it straight from the client payload unvalidated.
                    # A bad row must fail THIS CLIP, never the whole job.
                    try:
                        _clamped = veo_api_duration_s(_picked)
                    except ValueError as _dur_err:
                        print(
                            f"[v861/worker] clip {clip_index}: bad stored duration "
                            f"{_picked!r} — {_dur_err}. Falling back to the "
                            f"job-level duration for this clip.",
                            flush=True,
                        )
                        _clamped = None

                if _picked is not None and _clamped is not None:
                    override_duration = str(_clamped)
                    _note = "" if _clamped == _picked else \
                        f" (folded from {_picked}s — the Veo API has no {_picked}s bucket)"
                    print(
                        f"[v861/worker] clip {clip_index}: "
                        f"{len((dialogue_text or '').split())} words → "
                        f"{_clamped}s duration via {_picked_src}{_note}",
                        flush=True,
                    )
```

- [ ] **Step 3: Verify the module imports**

Run from `code/`: `python -c "import worker; print('worker OK')"`
Expected: `worker OK`

- [ ] **Step 4: Confirm the dead last-clip logic is gone**

Run from `code/`:

```bash
grep -n "LAST CLIP\|wps_map" worker.py
```

Expected: no output. Any hit means the old 2.5-wps block survived and will fight the new one.

- [ ] **Step 5: Commit**

```bash
git add code/worker.py
git commit -m "feat(v861): Veo API path renders every clip at its own duration

Replaces the last-clip-only 2.5-wps guess with the per-clip bucket resolved at
import. Clamps 10->8: the Veo API's durationSeconds accepts 4/6/8 only.
Diagnostic log per clip (remove after operator evidence lands).

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: Flow path — per-clip duration tab

**Files:**
- Modify: `code/static/flow_worker.py:17698-17701`, `:18798-18820`, `:17275-17296`

`select_frames_to_video_mode` already clicks a 4s/6s/8s/10s tab from `page._duration` (`:7532-7552`) and is already called once per clip inside both submission loops. The only change: set `page._duration` from THIS clip before the call, instead of once per job. `build_flow_prompt(duration=...)` also drives the speech-timing block, so it takes the per-clip value too.

- [ ] **Step 1: Per-clip duration in `process_job_submission_with_failover`**

In `code/static/flow_worker.py`, replace `:17698-17701`:

```python
            variants_count = job.get('flow_variants_count', 2)
            try:
                select_frames_to_video_mode(page, variants_count=variants_count)
```

with:

```python
            variants_count = job.get('flow_variants_count', 2)
            # v861 — per-clip duration. The API resolved this clip's bucket
            # from its line's word count; page._duration is what
            # select_frames_to_video_mode clicks in the 4s/6s/8s/10s tablist.
            # NULL (legacy/manual job) → keep the job-level duration.
            _clip_dur = clip.get('veo_render_duration_s')
            try:
                page._duration = str(_clip_dur) if _clip_dur else job.get('duration', '8')
                print(f"[v861/flow] clip {clip_index+1}: duration tab → {page._duration}s "
                      f"({'per-clip' if _clip_dur else 'job default'})", flush=True)
            except Exception:
                pass
            try:
                select_frames_to_video_mode(page, variants_count=variants_count)
```

- [ ] **Step 2: Per-clip duration in `process_job_submission`**

`process_job_submission` (`:18478`) is the non-failover twin of Step 1's function and has its own clip loop (from `:18808`) with its own `select_frames_to_video_mode(page, variants_count=...)` call. Insert the same guard immediately before that call:

```python
            # v861 — per-clip duration. The API resolved this clip's bucket
            # from its line's word count; page._duration is what
            # select_frames_to_video_mode clicks in the 4s/6s/8s/10s tablist.
            # NULL (legacy/manual job) → keep the job-level duration.
            _clip_dur = clip.get('veo_render_duration_s')
            try:
                page._duration = str(_clip_dur) if _clip_dur else job.get('duration', '8')
                print(f"[v861/flow] clip {clip_index+1}: duration tab → {page._duration}s "
                      f"({'per-clip' if _clip_dur else 'job default'})", flush=True)
            except Exception:
                pass
```

If this loop's clip counter is named something other than `clip_index`, use whatever name the surrounding loop already uses in its own log lines — do not introduce a new variable.

Do NOT touch the `select_frames_to_video_mode` calls at `:16033` (rebuild), `:17455` / `:18962` (crash/resume recovery) — those re-apply settings outside a per-clip context, and the job-level `page._duration` set at `:17072` / `:18511` is the right value there. `:16477` (redo) is covered by Step 3.

- [ ] **Step 3: Per-clip duration on the redo path**

In `code/static/flow_worker.py`, in `process_redo_clip` (`:16162`), immediately before the `select_frames_to_video_mode(page, context="REDO", variants_count=variants)` call at `:16477`, insert:

```python
            # v861 — a redo re-renders ONE clip; use that clip's own duration.
            _redo_dur = clip.get('veo_render_duration_s')
            if _redo_dur:
                try:
                    page._duration = str(_redo_dur)
                    print(f"[v861/flow] REDO clip {clip.get('clip_index')}: "
                          f"duration tab → {_redo_dur}s", flush=True)
                except Exception:
                    pass
```

- [ ] **Step 4: Per-clip duration into the prompt builder**

`build_flow_prompt(duration=...)` sets the speech-timing window inside the prompt, so a clip rendering at 6s must not be handed an 8s timing block. In `code/static/flow_worker.py`, inside the `for clip in clips:` prompt-build loop at `:17284-17296`, replace:

```python
                duration=job_duration,
```

with:

```python
                # v861 — this clip's own duration drives the speech-timing
                # window in the prompt, not the job-level default.
                duration=float(clip.get('veo_render_duration_s') or job_duration),
```

Apply the identical replacement inside the matching prompt-build loop in `process_job_submission` at `:18808-18820`.

- [ ] **Step 5: Verify the file parses**

Run from `code/`:

```bash
python -c "import ast; ast.parse(open('static/flow_worker.py',encoding='utf-8').read()); print('flow_worker parses OK')"
```

Expected: `flow_worker parses OK`

(`import flow_worker` is not usable here — it is a standalone operator-machine script with Playwright-only imports.)

- [ ] **Step 6: Confirm every submit site was covered**

Run from `code/`:

```bash
grep -n "_duration = \|veo_render_duration_s" static/flow_worker.py
```

Expected: `page._duration` is set in `process_job_submission_with_failover`, `process_job_submission`, and `process_redo_clip` — 3 per-clip sites plus the original job-level line at `:17072` / `:18511` (kept as the fallback default).

- [ ] **Step 7: Commit**

```bash
git add code/static/flow_worker.py
git commit -m "feat(v861): flow worker clicks the duration tab per clip

page._duration was set once per job; now each clip sets its own resolved
bucket before select_frames_to_video_mode, and its own duration feeds
build_flow_prompt's speech-timing window. Redo path included. NULL falls back
to the job-level duration.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 8: Auditor — make the field mandatory, raise the word cap to 28

**Files:**
- Modify: `~/.claude/skills/build-video/audit_build.py:957-967` (word cap), `:1109-1162` (check registry)

The parser auto-computes so legacy builds import. The auditor is what makes the field mandatory for NEW builds.

- [ ] **Step 1: Raise the v831 cap to 28**

In `audit_build.py`, replace `:957-967`:

```python
def c_line_word_cap(B):
    """v831 - no spoken line over 25 words; split into 2 clips instead (operator 2026-07-11)."""
    over = []
    for ln, v in B.line_fields:
        n = len(v.split())
        if n > 25:
            over.append((ln, n))
    if over:
        return FAIL, "line(s) over the 25-word cap (v831 - split into 2 clips): " + ", ".join(
            "L%d=%dw" % x for x in over)
    return PASS, "all spoken lines <= 25 words (v831)"
```

with:

```python
def c_line_word_cap(B):
    """v831 - no spoken line over the word cap; split into 2 clips instead.

    Cap was 25 (operator 2026-07-11); raised to 28 on 2026-07-16 so v861's 10s
    bucket (25-28 words) is reachable. Forward-only - shipped builds untouched.
    """
    over = []
    for ln, v in B.line_fields:
        n = len(v.split())
        if n > 28:
            over.append((ln, n))
    if over:
        return FAIL, "line(s) over the 28-word cap (v831 - split into 2 clips): " + ", ".join(
            "L%d=%dw" % x for x in over)
    return PASS, "all spoken lines <= 28 words (v831, amended for v861)"
```

- [ ] **Step 2: Add the v861 check**

In `audit_build.py`, immediately before the `# Check registry` banner comment at `:1105`, add:

```python
def c_v861_clip_duration(B):
    """v861 - every spoken line declares `- **clip_duration_s:**` and the value
    matches the bucket its word count lands in.

    Table (operator 2026-07-16): <=11w=4s, 12-16w=6s, 17-24w=8s, 25-28w=10s.
    """
    buckets = ((11, 4), (16, 6), (24, 8), (28, 10))

    def pick(n):
        for max_w, d in buckets:
            if n <= max_w:
                return d
        return 10

    if not B.line_fields:
        return SKIP, "no spoken lines (silent build)"

    # Pair each `- **line:**` with the `- **clip_duration_s:**` that follows it
    # before the next line/scene header - same attach rule as the platform
    # parser (image_platform.py: bullet_pattern loop).
    declared = {}   # line lineno -> (duration lineno, value)
    cur = None
    for ln, t in B.numbered:
        if re.match(r"^\s*[-*]\s*\*\*line\s*:\*\*", t, re.I):
            cur = ln
        elif re.match(r"^###\s+(Scene|Image|Clip)\s", t):
            cur = None
        else:
            m = re.match(r"^\s*[-*]\s*\*\*clip_duration_s\s*:\*\*\s*(\d+)", t, re.I)
            if m and cur is not None:
                declared[cur] = (ln, int(m.group(1)))

    missing, wrong, bad_val = [], [], []
    for ln, v in B.line_fields:
        n = len(v.split())
        want = pick(n)
        if ln not in declared:
            missing.append((ln, n, want))
            continue
        _dln, got = declared[ln]
        if got not in (4, 6, 8, 10):
            bad_val.append((ln, got))
        elif got != want:
            wrong.append((ln, n, got, want))

    if bad_val:
        return FAIL, "v861 clip_duration_s not in 4/6/8/10: " + ", ".join(
            "L%d=%s" % x for x in bad_val)
    if missing:
        return FAIL, ("v861 line(s) missing `- **clip_duration_s:**` (mandatory - "
                      "<=11w=4s, 12-16w=6s, 17-24w=8s, 25-28w=10s): " + ", ".join(
                          "L%d(%dw needs %ds)" % x for x in missing[:8]))
    if wrong:
        return FAIL, ("v861 clip_duration_s does not match the word count: " + ", ".join(
            "L%d %dw declared %ds needs %ds" % x for x in wrong[:8]))
    return PASS, "all %d spoken line(s) declare a clip_duration_s matching the v861 table" % len(
        B.line_fields)
```

- [ ] **Step 3: Register the check**

In `audit_build.py`, in the `CHECKS` list, add immediately after the `("line_word_cap", ...)` row:

```python
    ("v861_clip_duration", "v861 clip_duration_s on every line, matching the word-count bucket", c_v861_clip_duration),
```

- [ ] **Step 4: Verify the auditor runs and the new check fires**

Run from the repo root against a build that predates v861 — it MUST fail the new check and nothing else new:

```bash
python ~/.claude/skills/build-video/audit_build.py videos/nuri-korella-ed-5signs-bloodflow-walmart-banana-mic-insult-growth-v6.md
```

Expected: a `FAIL` row for `v861_clip_duration` naming the missing lines, and `line_word_cap` reporting the 28-word cap.

- [ ] **Step 5: Commit**

```bash
git add ~/.claude/skills/build-video/audit_build.py
git commit -m "feat(v861): auditor requires clip_duration_s per line; v831 cap 25->28

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

(If the skills directory is not a git repo, skip the commit and note the change in the final report.)

---

### Task 9: Docs — canonical deep-dive, skeleton, index, timeline

**Files:**
- Modify: `code/template_reference.md` (append §v861; amend §v831)
- Modify: `code/template_new_format.md`
- Modify: `wiki/patterns/conventions.md`
- Modify: `wiki/log.md`

Per `code/CLAUDE.md`, the deep-dive lives ONCE in `template_reference.md`; everything else points at it.

- [ ] **Step 1: Append the v861 deep-dive**

Append to `code/template_reference.md`:

```markdown
---

## v861 — Per-clip render duration from the line's word count

**Where it came from**: operator 2026-07-16 — *"we need to adapt the video markdown and the platform to also use the clip duration setting. so if we have around 28 words we 10 seconds, less than 12 words is 4 seconds and 16 is 6 seconds and 24 is 8 seconds."*

**The problem it fixes**: duration was JOB-level. Every clip rendered at the job's single setting (default 8s) no matter how many words its line held. Only the LAST clip got a word-count pick, off a hardcoded 2.5 words/sec (`worker.py`, now replaced). Short lines wasted seconds of dead air; long lines got cut off mid-sentence.

**The table** (literal upper bounds — the operator's four points):

| words `W` | duration |
|---|---|
| `W <= 11` | 4s |
| `12 <= W <= 16` | 6s |
| `17 <= W <= 24` | 8s |
| `25 <= W <= 28` | 10s |
| `W > 28` | v831 violation — split into two clips |

Implied speech rate 2.67-3.0 words/sec; least-squares fit of the four points = 2.8 w/s. Compare v577's 158 wpm (2.63 w/s) budget — v861 is the same ballpark, stated as buckets.

**The markdown field** — one per spoken line, attaching to the closest preceding `- **line:**` (the same attach rule as v644's `pad`):

```
### Scene 3
- **image:** image_2
- **clip_mode:** fresh
- **transition:** cut
- **line:** your soldier will not wake up in the morning anymore
- **clip_duration_s:** 6
- **action_note:** she lifts the banana to the lens. [Start beat]
```

Legal values: `4` | `6` | `8` | `10`. Anything else HARD-FAILS at import.

**Mandatory at authoring.** The `/build` auditor (`audit_build.py` check `v861_clip_duration`) FAILs a build when a line has no `clip_duration_s`, or when the declared value does not match the line's word-count bucket. The platform parser is deliberately more forgiving — it auto-computes when the bullet is absent and logs `[v861/auto]`, so the ~180 pre-v861 builds still import. Forward-only.

**Resolution precedence** (`clip_duration.resolve_clip_duration_s`):

1. explicit `- **clip_duration_s:**` bullet (v861)
2. the v667 frame-anchor-derived bucket (transformation montages; `_ceil_to_veo_bucket` over [4,6,8] — UNCHANGED by this rule)
3. the line's word count via the table above
4. `None` — no line, no anchor → the job-level duration applies (legacy + manual UI jobs)

The resolved integer lands on `clips.veo_render_duration_s` — a column that existed since v667 but that NO render path read until v861.

**The 10s asymmetry (important).** The Veo API accepts `durationSeconds` of **4, 6, or 8 only** (https://ai.google.dev/gemini-api/docs/veo). 10s exists **only** in Flow's 2026-07 composer, whose settings menu carries a 4s/6s/8s/10s tablist. Therefore:

- **Flow path** (`static/flow_worker.py`) — renders a real 10s clip. `page._duration` is set per clip before each `select_frames_to_video_mode` call, which clicks the matching tab.
- **Veo API path** (`worker.py`) — folds 10 → 8 and logs the clamp. A 25-28 word line on the API path WILL be tight.

A build that leans on the 10s bucket should render on Flow.

**Related rule change**: v831's spoken-line cap moved 25 → **28 words** on the same date, so the 10s bucket is reachable. Forward-only; shipped builds are not retro-edited.

**Touched**: this deep-dive (canonical), `code/clip_duration.py` (NEW — the only home of the math), `code/tests/test_v861_clip_duration.py`, `code/image_platform.py` (parser + prepare-time resolver), `code/main.py` (PATCH validator + Flow payload), `code/worker.py` (Veo API path), `code/static/flow_worker.py` (Flow path), `code/template_new_format.md` (skeleton), `~/.claude/skills/build-video/audit_build.py` (`v861_clip_duration` check + v831 cap), `wiki/patterns/conventions.md` (index row), `wiki/log.md`.
```

- [ ] **Step 2: Amend the v831 deep-dive**

In `code/template_reference.md`, in the `## v831` section, immediately under the `**The rule**:` sentence that names 25 words, insert:

```markdown
> **Amended 2026-07-16 (v861)**: the cap is now **28 words**, not 25 — v861's 10s bucket covers 25-28 words and was unreachable under the old cap. Everything else about v831 is unchanged: split at a natural sentence boundary, both halves reuse the SAME start image. Forward-only; shipped builds are not retro-edited.
```

- [ ] **Step 3: Add the field to the skeleton**

In `code/template_new_format.md`, in the `### Scene N` block spec, add the field under the `- **line:**` entry:

```markdown
- **clip_duration_s:** 6      # v861 MANDATORY — 4|6|8|10, matches the line's word count
                              # <=11w=4s · 12-16w=6s · 17-24w=8s · 25-28w=10s
                              # attaches to the line above it (same rule as `pad`)
```

- [ ] **Step 4: Add the conventions index row**

In `wiki/patterns/conventions.md`, add a row in v-number order:

```markdown
| v861 | Per-clip render duration from the line's word count — mandatory `- **clip_duration_s:**` bullet (4/6/8/10); <=11w=4s, 12-16w=6s, 17-24w=8s, 25-28w=10s. Flow renders 10s; the Veo API clamps 10→8. Raises v831's line cap 25→28. | [template_reference.md §v861](../../code/template_reference.md) |
```

- [ ] **Step 5: Add the timeline entry**

Prepend to the current section of `wiki/log.md`:

```markdown
- **2026-07-16 — v861 per-clip duration.** Duration was one job-level setting; only the last clip got a word-count guess (hardcoded 2.5 wps). Now every clip declares `- **clip_duration_s:**` (4/6/8/10) matching its line's word count — <=11w=4s, 12-16w=6s, 17-24w=8s, 25-28w=10s (operator table, ~2.8 words/sec). Math lives in the new `code/clip_duration.py`; the resolved value fills `clips.veo_render_duration_s` (a column live since v667 that no render path read). Flow clicks a real 10s tab per clip; the Veo API has no 10s bucket so it clamps 10→8. v831's line cap moved 25→28 to make the 10s bucket reachable. Auditor check `v861_clip_duration` makes the field mandatory; the parser auto-computes for the ~180 legacy builds. Forward-only.
```

- [ ] **Step 6: Commit**

```bash
git add code/template_reference.md code/template_new_format.md
git commit -m "docs(v861): canonical deep-dive + skeleton field; v831 cap 25->28

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

Then from the wiki root:

```bash
git add wiki/patterns/conventions.md wiki/log.md
git commit -m "docs(v861): conventions index row + log entry

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 10: Full verification before deploy

**Files:** none modified — this task is evidence-gathering. Per root `CLAUDE.md` §2, no "should work" claim ships without it.

- [ ] **Step 1: Unit tests green**

Run from `code/`: `python -m pytest tests/test_v861_clip_duration.py -v`
Expected: all PASS

- [ ] **Step 2: Existing test suite not regressed**

Run from `code/`: `python -m pytest tests/ -q`
Expected: no NEW failures vs. the pre-change baseline. Capture the baseline first with `git stash` if unsure.

- [ ] **Step 3: Every runtime module imports (py_compile is not enough)**

Run from `code/`:

```bash
python -c "import clip_duration, image_platform, main, worker; print('all modules import OK')"
python -c "import ast; ast.parse(open('static/flow_worker.py',encoding='utf-8').read()); print('flow_worker parses OK')"
```

Expected: `all modules import OK` then `flow_worker parses OK`

- [ ] **Step 4: All 180 legacy builds still parse**

Re-run the Task 2 Step 7 loop from the repo root.
Expected: `failed: 0`

- [ ] **Step 5: End-to-end bucket check on a real build**

Run from the repo root — prints what each line WOULD render at:

```bash
python - <<'PY'
import sys, re
sys.path.insert(0, "code")
from clip_duration import pick_clip_duration_s
p = "videos/nuri-korella-ed-5signs-bloodflow-walmart-banana-mic-insult-growth-v6.md"
t = open(p, encoding="utf-8").read()
tot = 0
for i, ln in enumerate(re.findall(r"^\s*[-*]\s*\*\*line:\*\*\s*(.+)$", t, re.M), 1):
    n = len(ln.split())
    d = pick_clip_duration_s(n)
    tot += d
    print(f"  line {i:2d}: {n:2d}w -> {d:2d}s   {ln[:52]}")
print(f"total render seconds: {tot}")
PY
```

Expected: every line maps to 4/6/8/10 and the totals look sane for the build's runtime.

- [ ] **Step 6: Push and watch the deploy**

```bash
git push origin main
```

Render auto-deploys in 2-3 min. Then, per `code/CLAUDE.md`, spawn `caveman:cavecrew-reviewer` on the commit set.

- [ ] **Step 7: Capture operator-side evidence — DO NOT claim success before this**

The diagnostic log lines added in Tasks 3, 6, and 7 are the evidence. On the next real render, the operator's logs must show:

- `[v861/auto]` or `[v861/explicit]` per clip at import
- `[v861/worker] clip N: Xw → Ys duration via clips.veo_render_duration_s` (API path), OR
- `[v861/flow] clip N: duration tab → Ys (per-clip)` (Flow path)

Confirm the rendered clip lengths match with `ffprobe` on the downloaded clips:

```bash
for f in <downloaded-clips>/*.mp4; do
  printf "%s " "$f"
  ffprobe -v error -show_entries format=duration -of csv=p=0 "$f"
done
```

Expected: clip durations track the picked buckets (Flow variants may run slightly long before trim — compare against the PICK, not the trimmed export).

Only AFTER this evidence lands: report success, and open a follow-up to strip the temporary `[v861/*]` diagnostic prints.

---

## Known gaps (deliberate, not oversights)

- **Manual UI jobs** (dialogue typed in the form, no markdown import) get `veo_render_duration_s = NULL` and keep the job-level duration. Operator scope was "the video markdown and the platform"; extending the auto-pick to manual jobs is a separate ask.
- **The 1080p / interpolation 8s pin** (`main.py:2006-2010`) is untouched per the operator's decision. A 1080p job with a 4s clip pick will still be validated against the job-level 8s rule. If a real job trips this, raise it — do not silently relax it.
- **`_ceil_to_veo_bucket` stays on `(4, 6, 8)`.** The v667 anchor path is not a v861 concern; widening it to 10 would change transformation-montage trim behavior with no ask.
