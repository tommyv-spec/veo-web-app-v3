"""Every browser probe on the path to the picker must have a deadline.

`page.evaluate()` takes no timeout. Its siblings `wait_for_function()` and
`inner_text()` do. That is not a style difference, it is the bug: two of the
worker's probes run immediately after `page.reload()`, when the JavaScript
execution context is being torn down, and an evaluate issued into a dying
context has nothing to return and no deadline. The call blocks forever, the
heartbeat thread keeps posting "online" from its own thread, and the worker
looks healthy while doing nothing.

Measured on this machine before any of this was written
(`tools/flow_probe_bound_smoke.py`, camoufox 152.0.4-beta.28 / playwright
1.57.0), each across a reload armed to destroy the context mid-call:

    wait_for_function(timeout=3000)  ->  raised at 3.01-3.02s, 5 of 5
    bare page.evaluate, never settles ->  still hanging at 8s, killed

So the conversion works, and the earlier in-tree warning that Camoufox lets
Playwright timeouts overrun does not apply here - that was about
`wait_for_load_state("networkidle")`, which waits on a network condition a live
Flow page never reaches. Different failure.

Scope, stated honestly: these tests cover the probes on the login and redo path
to the picker. The rest of the file's `evaluate` calls are still unbounded and
are covered only by the watchdog. Do not let anyone write "every probe is
bounded" - that overclaim is what let the actual wedge site get deferred.

Full analysis: docs/flow-worker-root-cause-2026-09-07.md
Repair plan:   docs/flow-worker-repair-plan-2026-09-07.md (Steps 3-4, 8)
"""

import ast
from functools import lru_cache
from pathlib import Path

import pytest

from playwright.sync_api import TimeoutError as PWTimeout


ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "static" / "flow_worker.py"
SOURCE = WORKER.read_text(encoding="utf-8")

# The three probes that sit between "the session is signed in" and the picker.
# L11614 ensure_videos_tab_selected is the FIRST unbounded call after the point
# where the 2026-09-07 log goes silent; _redo_tile_info runs after a PAID submit.
PICKER_PATH_PROBES = ("_flow_project_state", "_redo_tile_info",
                      "ensure_videos_tab_selected")


@lru_cache(maxsize=1)
def _tree():
    return ast.parse(SOURCE)


@lru_cache(maxsize=None)
def _node(name):
    for n in _tree().body:
        if isinstance(n, ast.FunctionDef) and n.name == name:
            return n
    return None


@lru_cache(maxsize=None)
def _const(name):
    """A module-level literal constant, e.g. the probe JS."""
    for n in _tree().body:
        if isinstance(n, ast.Assign):
            for t in n.targets:
                if isinstance(t, ast.Name) and t.id == name:
                    try:
                        return ast.literal_eval(n.value)
                    except Exception:
                        return None
    return None


class _Speaker:
    """A print() that remembers, so a swallowed error cannot pass as a pass.

    Loading a function by AST gives it an EMPTY global namespace. Miss one name
    and it raises NameError *inside* the function, where a broad `except` turns
    it into a clean-looking 'no answer'. That happened twice while writing these
    tests, and both times the test still went green on a code path that had not
    run. So: supply the real constants below, AND fail on any probe-error line.
    """

    def __init__(self):
        self.lines = []

    def __call__(self, *a, **k):
        self.lines.append(" ".join(str(x) for x in a))

    def assert_no_probe_error(self):
        bad = [ln for ln in self.lines if "probe error" in ln or "unavailable" in ln]
        assert not bad, f"the probe did not run cleanly: {bad}"


def _module_globals(speaker=None):
    return {
        "_FLOW_PROJECT_STATE_JS": _const("_FLOW_PROJECT_STATE_JS") or "() => null",
        "_REDO_TILE_SCAN_JS": _const("_REDO_TILE_SCAN_JS") or "() => null",
        "human_delay": lambda *a, **k: None,
        "time": __import__("time"),
        "print": speaker or (lambda *a, **k: None),
    }


def _function(name, globals_dict=None):
    node = _node(name)
    if node is None:
        pytest.fail(f"{name}() does not exist at module level in static/flow_worker.py")
    ns = dict(globals_dict or {})
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(WORKER), "exec"), ns)
    return ns[name]


def _source_of(name):
    node = _node(name)
    if node is None:
        pytest.fail(f"{name}() does not exist at module level in static/flow_worker.py")
    return ast.get_source_segment(SOURCE, node) or ""


class _Handle:
    def __init__(self, value):
        self._value = value

    def json_value(self):
        return self._value


class FakePage:
    """Records the timeout it was given; raises or returns as told."""

    def __init__(self, url="https://flow.google.com/project/abc", result=None,
                 raises=None):
        self.url = url
        self._result = result
        self._raises = raises
        self.timeouts = []
        self.clicked = []

    def wait_for_function(self, expression, timeout=None, **kw):
        self.timeouts.append(timeout)
        if self._raises is not None:
            raise self._raises
        return _Handle(self._result)

    # Anything that reaches for evaluate on this path is the bug coming back.
    def evaluate(self, *a, **k):
        raise AssertionError("an unbounded page.evaluate ran on the picker path")

    def locator(self, *a, **k):
        return _FakeLocator(self)

    def wait_for_load_state(self, *a, **k):
        return None


class _FakeLocator:
    def __init__(self, page, visible=True):
        self._page = page
        self._visible = visible
        self.first = self

    def wait_for(self, state=None, timeout=None):
        # Needed, and its absence is instructive: without it the sidebar branch
        # raised AttributeError, the bare `except:` swallowed it, and the
        # function fell through to the old-UI path — so the test went green
        # while the probe under test never ran at all.
        if not self._visible:
            raise PWTimeout("not visible")

    def is_visible(self, timeout=None):
        return self._visible

    def count(self):
        return 1

    def click(self, *a, **k):
        self._page.clicked.append(True)


# ------------------------------------------------------- the project-state probe

def test_project_state_probe_is_bounded():
    say = _Speaker()
    fn = _function("_flow_project_state", _module_globals(say))

    page = FakePage(raises=PWTimeout("timed out"))
    assert fn(page, timeout_s=10.0) == "wait", (
        "a probe that times out means 'not decided yet', not a crash")
    assert page.timeouts == [10000], (
        f"the budget must reach the driver as milliseconds, got {page.timeouts}")

    assert fn(FakePage(result="ok")) == "ok"
    say.assert_no_probe_error()

    gone = FakePage(url="https://flow.google.com/", result="ok")
    assert fn(gone, timeout_s=1.0) == "gone", (
        "leaving /project/ is its own outcome and must not need a DOM read")


def test_project_state_probe_has_no_evaluate_and_no_skip_flag():
    body = _source_of("_flow_project_state")
    assert "wait_for_function(" in body
    assert ".evaluate(" not in body
    # The previous fix answered a hanging probe by adding an env flag to SKIP
    # the probe. That left the unbounded call it was guarding exactly where it
    # was; it only stopped looking at it. Bounded now, so the flag is gone.
    assert "FLOW_SKIP_PROJECT_STATE_PROBE" not in SOURCE


def test_the_probe_still_recognises_the_error_page_and_the_ready_page():
    """The v834 locale fix must survive the conversion."""
    js = _source_of("_flow_project_state") + SOURCE[
        SOURCE.find("_FLOW_PROJECT_STATE_JS"):SOURCE.find("_FLOW_PROJECT_STATE_JS") + 2000]
    for marker in ("something went wrong", "se produjo un error",
                   "back to projects", "volver a los proyectos", "escenas"):
        assert marker in js, f"{marker!r} was dropped - v834 locale coverage lost"


# ------------------------------------------------------------- the fetch helpers

def test_fetch_js_carries_an_abort_timeout():
    for name in ("_FA_API_FETCH_JS", "_FA_TRPC_FETCH_JS"):
        i = SOURCE.find(name + " = ")
        assert i > 0, f"{name} not found"
        block = SOURCE[i:i + 2500]
        assert "AbortSignal.timeout(" in block, (
            f"{name} can hang the browser side forever; its Python side is a "
            f"page.evaluate and cannot be bounded, so the JS must settle itself")


# ---------------------------------------------------------------- the videos tab

def test_videos_tab_check_is_bounded():
    fn = _function("ensure_videos_tab_selected", _module_globals())
    page = FakePage(raises=PWTimeout("timed out"))
    fn(page)  # must not raise: a timeout means "not selected", not a failure
    assert page.timeouts and page.timeouts[0] is not None, (
        "the tab check must pass a timeout to the driver")


def test_videos_tab_source_has_no_unbounded_evaluate():
    body = _source_of("ensure_videos_tab_selected")
    assert "wait_for_function(" in body
    assert ".evaluate(" not in body


# ------------------------------------------------------------- the redo tile scan

def test_redo_tile_scan_is_bounded_and_returns_not_exists_on_timeout():
    say = _Speaker()
    fn = _function("_redo_tile_info", _module_globals(say))

    page = FakePage(raises=PWTimeout("timed out"))
    assert fn(page) == {"exists": False}, (
        "on timeout the scan must say 'nothing there yet' so the existing loop "
        "keeps waiting - raising here would report a slow render as a failure")
    assert page.timeouts == [8000]

    payload = {"exists": True, "videoUrls": ["https://x/v.mp4"], "videoCount": 1}
    assert fn(FakePage(result=payload)) == payload


# ------------------------------------------------------------- the standing guard

def test_no_unbounded_evaluate_on_the_picker_path():
    """If a later change reintroduces one here, this names it."""
    offenders = [name for name in PICKER_PATH_PROBES
                 if ".evaluate(" in _source_of(name)]
    assert not offenders, (
        f"unbounded page.evaluate is back on the picker path: {offenders}. "
        f"These three are the calls between a confirmed login and the Generate "
        f"click; a hang in any of them is invisible until a human notices.")
