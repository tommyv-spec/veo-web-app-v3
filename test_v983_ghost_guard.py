"""A clip with a bound media id was submitted. It must never be called a ghost.

Measured 2026-09-13, and it destroyed finished work:

    [API] Clip 14911 status -> completed
    [Flow] ⚠ Clip 5 was never submitted (ghost) — queuing for redo resubmission

14911 was submitted, rendered, downloaded and uploaded by the worker itself, and
the orphan sweep then reset it to `flow_redo_queued` — discarding a delivered
clip and queueing a second paid render. All five clips in that batch went the
same way.

The sweep decides "never submitted" from in-process bookkeeping
(`clips_done | clip_submit_times | permanently_failed | ghost_clips`). Anything
that drops a clip out of those sets makes a delivered clip look orphaned. The
worker already holds proof of submission — the media ids bound out of the clip's
own submit response — and simply was not consulting it here.
"""
import ast
import pathlib

_STATIC = pathlib.Path(__file__).parent / "static"


def _src():
    return (_STATIC / "flow_worker.py").read_text(encoding="utf-8", errors="replace")


def test_the_orphan_sweep_consults_the_bound_media_ids():
    """By AST: the guard must sit in the same function as the ghost verdict,
    not merely somewhere in the file."""
    tree = ast.parse(_src())

    owner = None
    for fn in [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]:
        for s in ast.walk(fn):
            if (isinstance(s, ast.Constant) and isinstance(s.value, str)
                    and "was never submitted (ghost)" in s.value):
                owner = fn
                break
        if owner:
            break
    assert owner is not None, "could not find the function that declares a ghost"

    called = {n.func.id for n in ast.walk(owner)
              if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
    assert "bound_media_ids_for_clip" in called, (
        "the ghost verdict must consult proof of submission")


def test_the_guard_runs_before_the_orphan_list_is_built():
    """A guard after the decision guards nothing."""
    src = _src()
    guard = src.index("if bound_media_ids_for_clip(job_id, _c['clip_index']):")
    decide = src.index("_orphaned = [c for c in clips if c['clip_index'] not in _submitted_or_done]")
    assert guard < decide, "the bound-id guard must widen the set before the sweep reads it"


def test_the_guard_cannot_raise_into_the_sweep():
    """A lookup failure must not abort the sweep or, worse, let every clip
    through as a ghost."""
    src = _src()
    start = src.index("for _c in clips:\n        try:\n            if bound_media_ids_for_clip")
    window = src[start:start + 400]
    assert "except Exception" in window, "the guard must be exception-safe"
