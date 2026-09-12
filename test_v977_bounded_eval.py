"""Nothing in the worker may wait on the renderer without a deadline.

`page.evaluate` takes no timeout argument, so every call is an unbounded wait.
On 2026-09-12 the park moved between four of them as each was bounded:
`_uuid_video_ready` -> the tile scroll -> the delivery-path reads -> a
`[data-index]` scan on the submit path. Each fix was correct and each was
followed by the same stall somewhere else, because it is a missing default and
not a per-call bug.

The tests here hold down the two things that make the sweep safe, both of which
Codex found in the PLAN before a line shipped:

  * the helper must not sweep ITSELF (a blanket replacement made `_v977_eval`
    call itself -- infinite recursion, and it can never reach Playwright);
  * a TIMEOUT must raise, not return a value, because 60 of the 62 call sites
    have an `except` that handles a failed evaluate and returning a default
    would silently disable all 60.

What this does NOT promise: protection when the renderer never runs JavaScript
at all. The bound is an in-JS Promise.race, so the timer needs the page to
execute. That case belongs to the v978 liveness watchdog.
"""
import ast
import importlib.util
import pathlib
import sys

import pytest

_STATIC = pathlib.Path(__file__).parent / "static"
_SPEC = importlib.util.spec_from_file_location(
    "flow_worker_bounded", _STATIC / "flow_worker.py",
)


def _load():
    if str(_STATIC) not in sys.path:
        sys.path.insert(0, str(_STATIC))
    mod = importlib.util.module_from_spec(_SPEC)
    _SPEC.loader.exec_module(mod)
    return mod


def _src():
    return (_STATIC / "flow_worker.py").read_text(encoding="utf-8", errors="replace")


class _P:
    """A page whose evaluate returns a canned result or raises."""

    def __init__(self, result=None, exc=None):
        self.result, self.exc = result, exc
        self.seen = []

    def evaluate(self, expr, arg=None):
        self.seen.append((expr, arg))
        if self.exc:
            raise self.exc
        return self.result


# ------------------------------------------------- failure vs timeout


def test_a_js_error_still_raises_so_existing_except_branches_run():
    """60 of 62 call sites have a try/except. Swallowing disables them."""
    fw = _load()
    with pytest.raises(RuntimeError):
        fw._v977_eval(_P(exc=RuntimeError("ReferenceError: foo")), "() => foo")


def test_a_destroyed_context_still_raises():
    fw = _load()
    with pytest.raises(RuntimeError):
        fw._v977_eval(_P(exc=RuntimeError("Execution context was destroyed")),
                      "() => 1")


def test_a_timeout_raises_the_dedicated_error_by_default():
    """A timeout IS a failure, so the caller's existing handler runs."""
    fw = _load()
    with pytest.raises(fw._V977EvalTimeout):
        fw._v977_eval(_P(result={"__v977_timeout": True}), "() => 1")


def test_a_timeout_returns_the_default_only_when_one_is_asked_for():
    fw = _load()
    got = fw._v977_eval(_P(result={"__v977_timeout": True}), "() => 1",
                        default="unasked")
    assert got == "unasked"


def test_default_none_is_distinguishable_from_no_default():
    """`default=None` must stay expressible, hence the sentinel."""
    fw = _load()
    assert fw._v977_eval(_P(result={"__v977_timeout": True}), "() => 1",
                         default=None) is None
    with pytest.raises(fw._V977EvalTimeout):
        fw._v977_eval(_P(result={"__v977_timeout": True}), "() => 1")


def test_a_falsy_value_is_not_mistaken_for_a_failure():
    fw = _load()
    for falsy in (0, False, "", None):
        assert fw._v977_eval(_P(result={"v": falsy}), "() => x",
                             default="MISSING") == falsy


# ------------------------------------------------------------- budgets


def test_the_dom_read_budget_is_baked_into_the_js():
    fw = _load()
    page = _P(result={"v": 1})
    fw._v977_eval(page, "() => 1")
    assert str(int(fw._V977_EVAL_TIMEOUT_MS)) in page.seen[0][0]


def test_an_explicit_budget_overrides_it():
    fw = _load()
    page = _P(result={"v": 1})
    fw._v977_eval(page, "() => 1", timeout_ms=4500)
    assert "4500" in page.seen[0][0]


def test_the_agent_off_post_gets_a_network_sized_budget():
    """Its JS is a CONSTANT, so an inline scan of the call site cannot see the
    `await fetch` inside -- which is how it nearly got the DOM-read budget."""
    fw = _load()
    assert fw._V977_NET_EVAL_TIMEOUT_MS > fw._V977_EVAL_TIMEOUT_MS

    tree = ast.parse(_src())
    calls = [n for n in ast.walk(tree)
             if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Name) and n.func.id == "_v977_eval"
             and any(isinstance(a, ast.Name) and a.id == "_V974_AGENT_OFF_JS"
                     for a in n.args)]
    assert len(calls) == 1, "the agent-off POST should go through the helper once"
    budget = next((k.value for k in calls[0].keywords if k.arg == "timeout_ms"), None)
    assert isinstance(budget, ast.Name), "it must pass an explicit budget"
    assert budget.id == "_V977_NET_EVAL_TIMEOUT_MS", budget.id


# ------------------------------------------------ the sweep, by AST


def _page_level_offenders(src):
    tree = ast.parse(src)
    helper = next(n for n in ast.walk(tree)
                  if isinstance(n, ast.FunctionDef) and n.name == "_v977_eval")
    exempt = set(range(helper.lineno, (helper.end_lineno or helper.lineno) + 1))
    out = []
    for n in ast.walk(tree):
        if not isinstance(n, ast.Call):
            continue
        f = n.func
        if not (isinstance(f, ast.Attribute) and f.attr == "evaluate"):
            continue
        if n.lineno in exempt:
            continue
        v = f.value
        name = (v.id if isinstance(v, ast.Name)
                else f"{getattr(v.value, 'id', '?')}.{v.attr}"
                if isinstance(v, ast.Attribute) else "?")
        if name in ("page", "self.page"):
            out.append((n.lineno, name))
    return out


def test_no_unbounded_page_evaluate_survives():
    """A new raw call re-opens the hole.

    By AST, not substring: a text search counted 65 where there are 62 real
    calls, because it also caught the helper's own line and two mentions in
    prose. Both receivers matter -- `self.page.evaluate` exists at five sites
    and a naive replacement turned it into `self._v977_eval(page, ...)`.
    """
    offenders = _page_level_offenders(_src())
    assert offenders == [], f"unbounded evaluate at {offenders}"


def test_the_helper_still_reaches_playwright_and_does_not_recurse():
    """The check a parse and an import both pass happily on.

    A blanket sweep rewrote the helper's own `page.evaluate` into a call to
    itself. That file parsed and imported without complaint, because importing
    a module never calls the function.
    """
    tree = ast.parse(_src())
    helper = next(n for n in ast.walk(tree)
                  if isinstance(n, ast.FunctionDef) and n.name == "_v977_eval")

    self_calls = [n.lineno for n in ast.walk(helper)
                  if isinstance(n, ast.Call)
                  and isinstance(n.func, ast.Name) and n.func.id == "_v977_eval"]
    assert self_calls == [], f"the helper calls itself at {self_calls}"

    reaches = [n.lineno for n in ast.walk(helper)
               if isinstance(n, ast.Call)
               and isinstance(n.func, ast.Attribute) and n.func.attr == "evaluate"]
    assert reaches, "the helper must still call page.evaluate itself"


def test_the_locator_evaluate_calls_were_left_alone():
    """Out of scope by decision, not by accident: Locator.evaluate resolves an
    element first and takes a different signature."""
    tree = ast.parse(_src())
    helper = next(n for n in ast.walk(tree)
                  if isinstance(n, ast.FunctionDef) and n.name == "_v977_eval")
    exempt = set(range(helper.lineno, (helper.end_lineno or helper.lineno) + 1))

    receivers = set()
    for n in ast.walk(tree):
        if not isinstance(n, ast.Call) or n.lineno in exempt:
            continue
        f = n.func
        if isinstance(f, ast.Attribute) and f.attr == "evaluate" and isinstance(f.value, ast.Name):
            receivers.add(f.value.id)
    # `page` survives only inside the helper, which is exempted above.
    assert "page" not in receivers, receivers
    assert receivers == {"container", "_gallery_list", "_dl_tab"}, receivers
