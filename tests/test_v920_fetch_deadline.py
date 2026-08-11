"""v920 — no in-page fetch may hang the worker forever.

page.evaluate has no timeout in Playwright, so a bare `await fetch(...)` inside
it blocks the worker's main thread until the page answers. On 2026-08-11 the
worker printed "Switching from job ... to ..." for node 4817 (a new batch key,
so no prior project -> straight into createProject) and went silent for
minutes, right after a "server slow or network stall" poll timeout. Nothing
logged, nothing recovered.

Every fetch helper now carries an AbortController deadline so the promise
always settles and Python regains control.
"""
import image_worker as iw


ALL_FETCH_JS = {
    "single": iw._FA_FETCH_JS,
    "batch": iw._FA_BATCH_FETCH_JS,
    "trpc": iw._FA_TRPC_FETCH_JS,
}


def test_every_fetch_helper_aborts_on_a_deadline():
    for name, js in ALL_FETCH_JS.items():
        assert "AbortController" in js, f"{name} has no abort controller"
        assert "opts.signal = ctl.signal" in js, f"{name} never wires the signal"
        assert "setTimeout(() => ctl.abort(), timeoutMs)" in js, f"{name} has no deadline"


def test_every_helper_takes_the_deadline_as_an_argument():
    for name, js in ALL_FETCH_JS.items():
        assert "timeoutMs" in js.split("=>")[0], f"{name} does not accept timeoutMs"


def test_timeout_is_reported_distinctly_from_a_transport_failure():
    for name, js in ALL_FETCH_JS.items():
        assert "e.name === 'AbortError'" in js, f"{name} cannot tell abort from error"
        assert "fetch timed out after" in js, f"{name} has no timeout message"


def test_timer_is_always_cleared():
    for name, js in ALL_FETCH_JS.items():
        assert "clearTimeout(timer)" in js, f"{name} leaks its timer"
        assert "} finally {" in js, f"{name} does not clear on the error path"


def test_a_timeout_reads_as_transient_not_as_an_account_block():
    # the worker must retry / fall back, never trigger a golden restore
    reason = "submit_image failed: fetch timed out after 300s"
    assert iw._is_unusual(reason) is False


def test_deadlines_are_generous_on_the_data_plane():
    # abandoning a generate POST Flow actually served costs an orphan render and
    # risks a duplicate submit — the worst observed healthy call was ~127s
    assert iw._FA_FETCH_TIMEOUT_MS >= 180000


def test_deadlines_are_tight_on_the_control_plane():
    # createProject / telemetry are fast or broken; no reason to wait minutes
    assert iw._FA_TRPC_TIMEOUT_MS <= 120000
    assert iw._FA_TRPC_TIMEOUT_MS < iw._FA_FETCH_TIMEOUT_MS


def test_python_wrappers_pass_a_deadline_through():
    import inspect
    src_fetch = inspect.getsource(iw._fa_api_fetch)
    assert "timeout_ms=_FA_FETCH_TIMEOUT_MS" in src_fetch
    assert "int(timeout_ms)" in src_fetch

    src_trpc = inspect.getsource(iw._fa_trpc_fetch)
    assert "timeout_ms=_FA_TRPC_TIMEOUT_MS" in src_trpc
    assert "int(timeout_ms)" in src_trpc

    src_many = inspect.getsource(iw._fa_api_fetch_many)
    assert "int(_FA_FETCH_TIMEOUT_MS)" in src_many


def test_no_fetch_helper_was_left_unbounded():
    """Guards against a future helper reintroducing a bare awaited fetch."""
    for name, js in ALL_FETCH_JS.items():
        assert js.count("await fetch(") == js.count("opts.signal = ctl.signal"), (
            f"{name} has an awaited fetch without a signal")
