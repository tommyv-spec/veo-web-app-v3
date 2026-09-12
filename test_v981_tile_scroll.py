"""The tile scroll is the call that parked the lane, and it is host-specific.

Traced 2026-09-12 on build `de67596423d4`, with BOTH instruments confirmed on
from the log before any output was read:

    PAGE calls : 119 traced | UNCLOSED -> evaluate(() => window.scrollTo(0, document.…
    RESP reads :   4 traced | UNCLOSED -> none

One unclosed call in the whole run. Its `_v977_eval` bound could not help,
because that bound is a `setTimeout` INSIDE the page and a wedged renderer never
runs the timer — the carve-out v977.2 wrote down, observed for real.

Two tests here exist because Codex found the corresponding mistake in the plan:

  * the removal must be CONDITIONAL. The scroll serves
    `scan_tiles_for_policy_failures`, which reads `[data-index]` /
    `[data-tile-id]`. Those are absent on flow.google.com, which is what makes
    the scroll pointless there — but they exist on the legacy host, where
    position attribution still applies. My first version deleted it everywhere
    and would have silently disabled policy scanning on legacy.
  * the test must check BEHAVIOUR, not a source string. My first version
    asserted one expression was gone, so it would have passed with the upward
    scroll and both sleeps still in place.

The other test guards the instrument itself: the first-generation page tracer
was missing from HEAD, and that absence produced a false negative — a run
reporting "no unclosed calls" while tracing nothing. An instrument that is off
looks exactly like an instrument that found nothing.
"""
import ast
import importlib.util
import pathlib
import sys

_STATIC = pathlib.Path(__file__).parent / "static"
_SPEC = importlib.util.spec_from_file_location(
    "flow_worker_v981", _STATIC / "flow_worker.py",
)


def _load():
    if str(_STATIC) not in sys.path:
        sys.path.insert(0, str(_STATIC))
    mod = importlib.util.module_from_spec(_SPEC)
    _SPEC.loader.exec_module(mod)
    return mod


def _src():
    return (_STATIC / "flow_worker.py").read_text(encoding="utf-8", errors="replace")


# ------------------------------------------------- the instrument (Task 0)


def test_the_firstgen_page_tracer_is_armed():
    """Missing from HEAD once already, which produced a false negative.

    The arming had only ever been applied to the working tree, so every commit
    blob rebuilt from HEAD silently dropped it. A run then reported "no unclosed
    calls" while tracing nothing, and that read as "page methods are innocent"
    for hours.
    """
    tree = ast.parse(_src())
    fn = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
              and n.name == "process_job_submission")
    armed = [n for n in ast.walk(fn) if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Name) and n.func.id == "trace_page_calls"]
    assert armed, "the first-generation path must arm the page tracer"


# ------------------------------------------------------ the scroll (Task 1)


def test_no_scroll_on_the_new_host(monkeypatch):
    fw = _load()
    calls = []
    monkeypatch.setattr(fw, "_v962_on_new_host", lambda p: True)
    monkeypatch.setattr(fw, "_v977_eval", lambda *a, **k: calls.append(a[1]))
    assert fw._v981_nudge_tiles(object()) is False
    assert calls == [], "the scroll parked the lane and the listing does not need it"


def test_the_legacy_host_still_scrolls(monkeypatch):
    """`[data-index]` exists there, so the policy scan still needs rendered
    tiles. Deleting this branch would disable policy scanning on that host."""
    fw = _load()
    calls = []
    monkeypatch.setattr(fw, "_v962_on_new_host", lambda p: False)
    monkeypatch.setattr(fw, "_v977_eval", lambda *a, **k: calls.append(a[1]))
    monkeypatch.setattr(fw.time, "sleep", lambda s: None)
    assert fw._v981_nudge_tiles(object()) is True
    assert len(calls) == 2, calls
    assert "scrollHeight" in calls[0]
    assert "0, 0" in calls[1]


def test_an_unknown_host_behaves_like_legacy(monkeypatch):
    """Fail toward the side that needs the scroll: a pointless scroll costs a
    second, a missed policy failure costs a clip."""
    fw = _load()
    calls = []

    def boom(p):
        raise RuntimeError("cannot tell which host")

    monkeypatch.setattr(fw, "_v962_on_new_host", boom)
    monkeypatch.setattr(fw, "_v977_eval", lambda *a, **k: calls.append(a[1]))
    monkeypatch.setattr(fw.time, "sleep", lambda s: None)
    assert fw._v981_nudge_tiles(object()) is True
    assert len(calls) == 2


def test_the_poll_calls_the_helper_rather_than_scrolling_inline():
    """Behaviour, not a string match on one expression.

    The earlier version of this test asserted `scrollTo(0, document.body.scrollHeight)`
    was absent from the file, which would have passed while the upward scroll and
    both sleeps remained inline, and passed again if the same scroll came back
    written slightly differently.
    """
    src = _src()
    tree = ast.parse(src)
    fn = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
              and n.name == "_v981_nudge_tiles")
    helper_lines = set(range(fn.lineno, (fn.end_lineno or fn.lineno) + 1))

    # `window.scrollTo`, not `scrollTo` — the loose version matches `scrollTop`
    # and flagged an unrelated locator-scoped gallery scroll
    # (`el => el.scrollTop += 200`). Fourth substring mis-match of the day; the
    # precise token is the whole point of the assertion.
    stray = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
            continue
        if "window.scrollTo" in node.value and node.lineno not in helper_lines:
            stray.append(node.lineno)
    assert stray == [], f"window-level scroll JS outside the helper at {stray}"

    # and the poll must actually call it
    assert "_v981_nudge_tiles(page)" in src
