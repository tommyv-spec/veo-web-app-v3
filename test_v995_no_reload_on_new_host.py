"""v995 -- on flow.google.com a page.reload() wedges the tab; every reload routes
through _v995_reload (goto of the same url there), and the four wait-loop
reloads that only forced legacy tiles to render are skipped there.

Measured 2026-09-13 in a clean process on the worker's own profile, twice:
goto -> settings chip in 4 s; reload -> chip never visible, locator.count()
hangs; goto of the SAME url -> chip in 3 s. And in two runs the same day
(batch23, batch25) every clip after the resume path's reload rounds failed
`[v962.3] settings chip not found`, then the post-job loop's own 30 s reload
timed out until it gave the in-flight renders up. Checked by AST, not by text.
"""
import ast
import os
import pathlib

_STATIC = pathlib.Path(__file__).parent / "static"
_PATH = pathlib.Path(os.environ.get("FLOW_WORKER_PATH") or (_STATIC / "flow_worker.py"))


def _tree():
    return ast.parse(_PATH.read_text(encoding="utf-8", errors="replace"))


def _fn(tree, name):
    return next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == name)


def _ifs(tree):
    return [n for n in ast.walk(tree) if isinstance(n, ast.If)]


def _reload_calls(node):
    return [c for c in ast.walk(node) if isinstance(c, ast.Call)
            and isinstance(c.func, ast.Attribute) and c.func.attr == "reload"]


def _has_helper_call(node):
    return any(isinstance(c, ast.Call) and isinstance(c.func, ast.Name) and c.func.id == "_v995_reload"
               for c in ast.walk(node))


def test_every_page_reload_routes_through_the_helper():
    """THE REGRESSION. A bare page.reload() anywhere on the single-account path
    is a wedged tab on flow.google.com."""
    tree = _tree()
    inside = {id(c) for c in _reload_calls(_fn(tree, "_v995_reload"))}
    stray = [ast.unparse(c) for c in _reload_calls(tree)
             if id(c) not in inside and isinstance(c.func.value, ast.Name) and c.func.value.id == "page"]
    assert not stray, stray


def test_the_helper_gotos_the_same_url_on_the_new_host_and_reloads_elsewhere():
    helper = _fn(_tree(), "_v995_reload")
    src = ast.unparse(helper)
    assert "page.url" in src and "page.goto(_url" in src, src
    guards = [n for n in ast.walk(helper) if isinstance(n, ast.If)
              and ast.unparse(n.test) == "not _v962_on_new_host(page)"]
    assert len(guards) == 1, "the legacy host must keep the real reload behind one guard"
    assert any(isinstance(s, ast.Return) and "page.reload(**kwargs)" in ast.unparse(s)
               for s in guards[0].body), ast.unparse(guards[0])


def test_legacy_multi_account_class_reloads_are_untouched():
    """The patch is mechanical on `page.reload(`; `self.page.reload(` in the
    legacy class stays as it was (5 sites on 2026-09-13)."""
    n = sum(1 for c in _reload_calls(_tree())
            if isinstance(c.func.value, ast.Attribute) and ast.unparse(c.func.value) == "self.page")
    assert n == 5, n


def test_resume_scan_stops_before_its_first_reload_on_the_new_host():
    """Rounds 2-3 of the resume scan reload; on flow.google.com the loop leaves first."""
    guards = [n for n in _ifs(_tree())
              if ast.unparse(n.test) == "_scan_round > 0 and _v962_on_new_host(page)"]
    assert len(guards) == 1, "expected exactly one new-host guard on the resume scan rounds"
    assert isinstance(guards[0].body[-1], ast.Break), ast.unparse(guards[0])
    assert not _has_helper_call(guards[0]) and not _reload_calls(guards[0])


def test_resume_reload_rounds_still_exist_for_the_legacy_host():
    legacy = [n for n in _ifs(_tree()) if ast.unparse(n.test) == "_scan_round > 0" and _has_helper_call(n)]
    assert len(legacy) == 1, "the legacy reload round is gone or duplicated"


def test_the_three_wait_loop_reloads_are_guarded_on_the_new_host():
    """post-job 30 s, the 600 s poll's 30 s, continue-mode's 90 s: each test
    carries `not _v962_on_new_host(page)` itself."""
    sites = [n for n in _ifs(_tree())
             if any(k in ast.unparse(n.test) for k in ("_last_reload", "_poll_start) % 30", "elapsed == 90"))
             and _has_helper_call(n)]
    assert len(sites) == 3, [ast.unparse(n.test) for n in sites]
    for n in sites:
        assert "not _v962_on_new_host(page)" in ast.unparse(n.test), ast.unparse(n.test)
