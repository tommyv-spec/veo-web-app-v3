"""v855 — several exports may be QUEUED, but only MAX_CONCURRENT may RUN.

The operator could not queue a second export at all: the frontend's global
`isExporting` flag alerted "Export already in progress". Removing that alert on
its own would have been dangerous, because the backend had NO cap — the POST
spawned a runner on the request and the sweeper spawned every orphan it found.
N queued exports for N jobs would have become N simultaneous ffmpeg+Whisper runs
on a 2 GB / 1 CPU box, and a deploy that orphaned N runs would have fired all N
at boot: OOM, restart, repeat.

So the cap ships first. The dispatcher is now the ONLY thing that spawns a
runner, and it never exceeds MAX_CONCURRENT (default 1).

Run from code/:  PYTHONUTF8=1 python -m pytest tests/test_export_dispatcher.py -v
"""
import os

import export_queue as eq

_HERE = os.path.dirname(os.path.abspath(__file__))
_CODE = os.path.dirname(_HERE)
_MAIN = os.path.join(_CODE, "main.py")
_INDEX = os.path.join(_CODE, "static", "index.html")


def _main_src():
    return open(_MAIN, encoding="utf-8").read()


def _index_src():
    return open(_INDEX, encoding="utf-8").read()


# ---- the cap --------------------------------------------------------------

def test_default_cap_is_one():
    """2 GB / 1 CPU, and a SINGLE export already sits at that ceiling (v691d,
    v701w/x/y/z, v692b are all one-export OOM fixes). Two at once is not faster
    on one CPU — just riskier."""
    assert eq.MAX_CONCURRENT == 1


def test_slots_free_never_goes_negative():
    assert eq.slots_free(0, max_concurrent=1) == 1
    assert eq.slots_free(1, max_concurrent=1) == 0
    assert eq.slots_free(2, max_concurrent=1) == 0
    assert eq.slots_free(1, max_concurrent=3) == 2


# ---- one spawn path -------------------------------------------------------

def test_the_dispatcher_is_the_only_spawner():
    src = _main_src()
    # def + the dispatcher's single call. Nothing else may spawn.
    assert src.count("_spawn_export_runner(") == 2, (
        "someone added another spawn site — the cap only holds if the "
        "dispatcher is the sole spawn path"
    )
    assert "async def _export_dispatcher(" in src
    assert "_export_dispatcher_task" in src


def test_the_post_does_not_spawn():
    """It only inserts a queued row; the dispatcher picks it up in ~2s."""
    src = _main_src()
    post = src.split('@app.post("/api/jobs/{job_id}/export-final")')[1].split("\n@app.")[0]
    assert "_spawn_export_runner(" not in post


def test_the_sweeper_does_not_spawn():
    """It only re-queues. Firing every orphan at once is how a deploy
    OOM-crashloops the box."""
    src = _main_src()
    sweeper = src.split("async def _export_sweeper(")[1].split("\ndef ")[0]
    assert "_spawn_export_runner(" not in sweeper


def test_slots_are_counted_from_live_tasks_not_db_rows():
    """A row left 'running' by a DEAD container must never hold a slot — that
    would stall the queue permanently."""
    src = _main_src()
    disp = src.split("async def _export_dispatcher(")[1].split("\ndef _claim_export_run(")[0]
    assert "len(_LOCAL_EXPORT_IDS)" in disp
    assert "slots_free" in disp


def test_dispatcher_survives_a_bad_tick():
    """If this loop dies, every export sits queued forever and nothing says so."""
    src = _main_src()
    disp = src.split("async def _export_dispatcher(")[1].split("\ndef _claim_export_run(")[0]
    assert "except Exception" in disp
    assert "while True:" in disp


# ---- the queue read -------------------------------------------------------

def test_queue_read_is_fifo_and_skips_local_runs():
    src = _main_src()
    q = src.split("def _next_queued_export_ids(")[1].split("\nasync def _export_dispatcher(")[0]
    assert "created_at.asc()" in q          # oldest first
    assert "not in _LOCAL_EXPORT_IDS" in q  # never double-start a live run
    assert "STATE_QUEUED" in q


# ---- the frontend: the cap is useless if the UI won't let you queue -------
# The backend queues N exports and runs them one at a time, but the operator
# could still only ever start ONE: a global `isExporting` flag alerted
# "Export already in progress" on the second job. These are source greps —
# index.html has no build step and no JS test harness.

def test_the_global_export_lock_is_gone():
    src = _index_src()
    assert "isExporting" not in src, "the global export lock is back — only one export can be queued again"
    assert "Export already in progress" not in src, "the second-export alert is back"


def test_the_banner_is_a_stack_with_one_row_per_export():
    """A singleton #floatingExportBanner meant export B's status text overwrote
    export A's — and A's completion removed B's banner."""
    src = _index_src()
    assert "exportBannerStack" in src
    assert "exportRow_" in src or "data-export-id" in src
    assert "floatingExportBanner" not in src, "the singleton banner is back"


def test_the_run_ids_survive_a_page_reload():
    """The export id used to be a local const. Reload = the run finishes on the
    server and the operator never sees it."""
    src = _index_src()
    assert "activeExports" in src
    assert "localStorage" in src
    assert "resumeActiveExports" in src


def test_the_voice_clone_error_is_per_export_not_global():
    """Two exports finishing together used to overwrite each other's VC error."""
    src = _index_src()
    assert "window._vcErrorMsg" not in src
    assert "window._masterAudioFilename" not in src
