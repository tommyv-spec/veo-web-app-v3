"""v854 — the sweeper could never actually start an orphaned export.

THE BUG (found in production 2026-07-13, an export sat queued for 4 hours):

`_sweep_stale_exports` is sync and BOTH callers run it via `asyncio.to_thread`
— i.e. on a worker thread with no event loop. It ended with:

    for _rid in to_fire:
        _spawn_export_runner(_rid)     # -> asyncio.create_task(...)

`asyncio.create_task()` raises `RuntimeError: no running event loop` when there
is no loop in the CALLING thread. So every sweep blew up there. Worse,
`_spawn_export_runner` added the id to `_LOCAL_EXPORT_IDS` BEFORE calling
create_task, so the id stayed in the set with no task behind it — and the
sweeper skips any id in that set. The run became permanently unreclaimable:
state=queued, attempts=1 (attempts only increments on a successful claim),
heartbeat=NULL, forever, silently.

Both sweep paths (boot + the 60s loop) were dead from day one. Only the POST
path worked, because that one runs on the event loop.

The v850 resume proof did not catch this because it MONKEYPATCHED
`_spawn_export_runner` to a recorder — the stub is exactly what hid the bug.
These tests use the real function.

Run from code/:  PYTHONUTF8=1 python -m pytest tests/test_export_spawn_from_thread.py -v
"""
import asyncio
import os

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_CODE = os.path.dirname(_HERE)
_MAIN = os.path.join(_CODE, "main.py")


def _main_src():
    return open(_MAIN, encoding="utf-8").read()


# ---- Layer 1: source-level invariants -------------------------------------

def test_sweep_does_not_spawn_it_returns_ids():
    """The sweep runs on a thread. It must never call create_task."""
    src = _main_src()
    sweep = src.split("def _sweep_stale_exports(")[1].split("\nasync def _export_sweeper(")[0]
    assert "_spawn_export_runner(" not in sweep, (
        "_sweep_stale_exports runs via asyncio.to_thread (no event loop) — "
        "spawning there raises RuntimeError and poisons _LOCAL_EXPORT_IDS"
    )
    assert "return to_fire" in sweep


def test_no_sweep_caller_spawns_at_all_any_more():
    """v855 superseded the v854 fix: the sweep callers no longer spawn EITHER.

    v854 moved the spawn from the thread onto the loop, which fixed the crash.
    v855 removed it entirely — the sweep only re-queues, and the dispatcher
    starts runs one at a time under the concurrency cap. That kills the same bug
    class permanently (nothing on a thread can spawn) AND stops N orphans
    becoming N simultaneous ffmpeg runs after a deploy.

    The load-bearing invariant is unchanged and still tested below:
    _spawn_export_runner must never be reachable from a worker thread.
    """
    src = _main_src()
    sweeper = src.split("async def _export_sweeper(")[1].split("\ndef ")[0]
    assert "await asyncio.to_thread(_sweep_stale_exports)" in sweeper
    assert "_spawn_export_runner(" not in sweeper

    # the boot sweep, inside lifespan, likewise only re-queues
    boot = src.split("_orphan_ids = await _asyncio.to_thread(_sweep_stale_exports)")[1][:400]
    assert "_spawn_export_runner(" not in boot


def test_spawn_unregisters_the_id_when_the_task_cannot_be_created():
    src = _main_src()
    spawn = src.split("def _spawn_export_runner(")[1].split("\ndef ")[0]
    assert "except RuntimeError" in spawn
    assert "_LOCAL_EXPORT_IDS.discard(export_id)" in spawn


def test_a_dead_runner_task_cannot_leak_its_id():
    """The runner's own `finally` does not run if the coroutine dies BEFORE its
    try block. The done-callback is the belt that keeps the set honest."""
    src = _main_src()
    spawn = src.split("def _spawn_export_runner(")[1].split("\ndef _claim_export_run(")[0]
    assert "add_done_callback" in spawn
    assert "_t.exception()" in spawn      # a swallowed task exception is how this hid


def test_a_stuck_queued_run_gets_picked_up_without_a_re_click():
    """v854 added a rescue-spawn on the POST, because a queued run with no live
    runner was otherwise stranded and re-clicking Export just re-attached to the
    dead row.

    v855 makes that unnecessary: the dispatcher polls for queued work every
    DISPATCH_INTERVAL_S and starts ANY queued run, including one a dead
    container left behind. No click required.
    """
    src = _main_src()
    disp = src.split("async def _export_dispatcher(")[1].split("\ndef _claim_export_run(")[0]
    assert "_next_queued_export_ids" in disp
    assert "_spawn_export_runner(_rid)" in disp


# ---- Layer 2: the actual failure, reproduced ------------------------------

def test_create_task_from_a_worker_thread_raises():
    """This is the exception that killed every sweep. Pinning it so nobody
    'simplifies' the spawn back onto a thread."""
    async def _noop():
        return None

    async def _main():
        def _from_thread():
            try:
                asyncio.create_task(_noop())
                return None
            except RuntimeError as e:
                return str(e)

        return await asyncio.to_thread(_from_thread)

    err = asyncio.run(_main())
    assert err is not None, "create_task from a to_thread worker must raise"
    assert "no running event loop" in err.lower()


def test_spawn_from_a_thread_raises_and_leaves_the_set_clean():
    """The real _spawn_export_runner, called the broken way. It must raise —
    and, critically, must NOT leave the id behind in _LOCAL_EXPORT_IDS, because
    an id stranded there makes the run unreclaimable forever."""
    import main

    async def _main():
        def _from_thread():
            try:
                main._spawn_export_runner("v854-test-id")
                return "NO RAISE"
            except RuntimeError:
                return "raised"

        return await asyncio.to_thread(_from_thread)

    outcome = asyncio.run(_main())
    assert outcome == "raised"
    assert "v854-test-id" not in main._LOCAL_EXPORT_IDS, (
        "id leaked into _LOCAL_EXPORT_IDS — the sweeper will skip this run forever"
    )
