"""`trace_page_calls` must terminate on a Locator whose `.first` is a property.

Why this file exists. Playwright's `Locator.first` builds a NEW Locator on every
access. `trace_page_calls` used to wrap `.first` by RECURSING into it and
guarding with a `_traced` attribute set on the child -- which cannot work, because
the next `.first` is a different object and never carries the flag. So the
tracer descended `.first.first.first...` until the interpreter's stack ran out.

Measured 2026-09-12 on a real run: with FLOW_TRACE_PAGE=1 the worker burned 104%
of one core for seven minutes with NO log output and NO traced call, then raised
`RecursionError: maximum recursion depth exceeded`. There was no log line because
the recursion happens inside `page.locator()`, before any ENTER line can print --
so the diagnostic that existed to show where the worker stops became the reason it
stopped, and its silence read as "the worker is stuck somewhere else". Two runs
were spent chasing that.

The bug was latent in the codebase: the tracer was armed only on the redo path,
behind FLOW_TRACE_PAGE=1, so it fired the day it was armed on the first-generation
path as well.

The stub below is the whole point -- `first` and `last` are properties returning
fresh objects, exactly like the real thing.
"""
import importlib.util
import os
import pathlib
import sys

_STATIC = pathlib.Path(__file__).parent / "static"
_SPEC = importlib.util.spec_from_file_location(
    "flow_worker_trace", _STATIC / "flow_worker.py",
)


def _load():
    if str(_STATIC) not in sys.path:
        sys.path.insert(0, str(_STATIC))
    mod = importlib.util.module_from_spec(_SPEC)
    _SPEC.loader.exec_module(mod)
    return mod


class _Locator:
    """A Locator whose `.first` / `.last` hand back a NEW object every time."""

    def __init__(self, selector):
        self.selector = selector
        self.calls = []

    def count(self):
        self.calls.append("count")
        return 1

    def is_visible(self, timeout=None):
        self.calls.append("is_visible")
        return True

    def click(self, timeout=None):
        self.calls.append("click")

    @property
    def first(self):
        return _Locator(self.selector + ".first")

    @property
    def last(self):
        return _Locator(self.selector + ".last")


class _Page:
    def __init__(self):
        self.url = "https://flow.google.com/project/abc"

    def locator(self, selector, **kw):
        return _Locator(selector)

    def goto(self, url, **kw):
        return None

    def evaluate(self, js, *a):
        return None


def _armed_page():
    os.environ["FLOW_TRACE_PAGE"] = "1"
    fw = _load()
    return fw.trace_page_calls(_Page(), label="[test]")


def test_wrapping_a_locator_terminates():
    """The regression: this used to recurse until RecursionError.

    A low recursion limit makes the failure fast and unambiguous instead of
    burning a core for minutes the way the real run did.
    """
    page = _armed_page()
    original_limit = sys.getrecursionlimit()
    sys.setrecursionlimit(200)
    try:
        loc = page.locator("button:has-text('Upload')")
    finally:
        sys.setrecursionlimit(original_limit)
    assert loc is not None


def test_the_locator_still_gets_traced():
    """Terminating is not enough — it has to still do its job."""
    page = _armed_page()
    loc = page.locator("button[aria-label='Add media menu']")
    assert loc.count() == 1
    assert "count" in loc.calls, "the wrapper must still call through"


def test_first_is_traced_one_level_deep():
    """`.first` is the form the worker actually uses, so it must be wrapped."""
    page = _armed_page()
    child = page.locator("div[role='menuitem']").first
    assert child.is_visible() is True
    assert "is_visible" in child.calls


def test_the_tracer_is_off_unless_the_env_var_is_set():
    """It is loud by design and must never arm itself in a production run."""
    os.environ["FLOW_TRACE_PAGE"] = "0"
    fw = _load()
    page = _Page()
    same = fw.trace_page_calls(page, label="[off]")
    assert same is page
    assert page.locator("x").__class__ is _Locator


# --- v977: the bounded evaluate helper ---------------------------------------
#
# The primitive matters. `wait_for_function` is a POLLING predicate, not a
# bounded one-shot evaluate, and using it made every call burn the full timeout
# — measured 10.0s on a scroll that takes 0.1s. So the deadline lives inside the
# JS instead, as a Promise.race against a timer, and these tests hold that shape
# down: fast calls must not pay the budget, falsy values must survive, a plain
# expression must work as well as a function, and a timeout must be loud.


class _EvalPage:
    """Records the expression handed to page.evaluate and replays a result."""

    def __init__(self, result, raise_exc=None):
        self.result = result
        self.raise_exc = raise_exc
        self.seen = []

    def evaluate(self, expression, arg=None):
        self.seen.append((expression, arg))
        if self.raise_exc is not None:
            raise self.raise_exc
        return self.result


def test_it_uses_page_evaluate_not_a_polling_predicate():
    """The regression: wait_for_function polls, so it cost the full budget."""
    fw = _load()
    page = _EvalPage({"v": None})
    fw._v977_eval(page, "() => window.scrollTo(0, 0)")
    assert page.seen, "it must go through page.evaluate"
    expression = page.seen[0][0]
    assert "Promise.race" in expression, expression
    assert "setTimeout" in expression, expression


def test_the_deadline_is_baked_into_the_js():
    fw = _load()
    page = _EvalPage({"v": 1})
    fw._v977_eval(page, "() => 1", timeout_ms=4500)
    assert "4500" in page.seen[0][0]


def test_a_plain_expression_works_not_just_a_function():
    """`page.evaluate("window.blur()")` is legal, so the wrapper must allow it."""
    fw = _load()
    page = _EvalPage({"v": None})
    fw._v977_eval(page, "window.blur()")
    assert "typeof __f === 'function'" in page.seen[0][0]


def test_a_falsy_value_survives():
    """0 / false / "" must not be mistaken for "no answer" — hence the object."""
    fw = _load()
    for falsy in (0, False, ""):
        page = _EvalPage({"v": falsy})
        assert fw._v977_eval(page, "() => x", default="MISSING") == falsy


def test_the_value_comes_back_and_the_arg_is_passed_through():
    fw = _load()
    page = _EvalPage({"v": {"ct": "video/mp4"}})
    got = fw._v977_eval(page, "async (u) => fetch(u)", arg="https://x")
    assert got["ct"] == "video/mp4"
    assert page.seen[0][1] == "https://x"


def test_a_timeout_returns_the_default():
    """The JS timer won the race: return the default rather than a bad answer."""
    fw = _load()
    page = _EvalPage({"__v977_timeout": True})
    assert fw._v977_eval(page, "() => 1", default="unasked") == "unasked"


def test_a_raising_evaluate_returns_the_default_instead_of_propagating():
    """A destroyed context must not take the lane down with it."""
    fw = _load()
    page = _EvalPage(None, raise_exc=RuntimeError("Execution context was destroyed"))
    assert fw._v977_eval(page, "() => 1", default="unasked") == "unasked"
