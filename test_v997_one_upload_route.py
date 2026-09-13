"""v997 -- one upload route on flow.google.com; a chooser failure is explained and retried once.

Measured across every run of 2026-09-13: the five synthetic file-drop routes
attached a frame 0 times in 9 runs, and the Add-media chooser worked at most
once per browser session, every later attempt dying on `Locator.click: Timeout
8000ms` of its button with the call log cut at 100 characters. Checked by AST.
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


def _calls(node, attr_or_name):
    out = []
    for c in ast.walk(node):
        if not isinstance(c, ast.Call):
            continue
        f = c.func
        if (isinstance(f, ast.Name) and f.id == attr_or_name) or (isinstance(f, ast.Attribute) and f.attr == attr_or_name):
            out.append(c)
    return out


def test_drop_ladder_is_skipped_on_the_new_host_before_any_drop():
    fn = _fn(_tree(), "_v962_upload_into_picker")
    guards = [n for n in ast.walk(fn) if isinstance(n, ast.If) and ast.unparse(n.test) == "_v962_on_new_host(page)"]
    assert len(guards) == 1, "expected one new-host guard in the drop ladder"
    last = guards[0].body[-1]
    assert isinstance(last, ast.Return) and ast.unparse(last.value) == "False", ast.unparse(last)
    src = ast.unparse(fn)
    assert src.index("_v962_on_new_host(page)") < src.index("targets = ["), "the guard must come before the drop targets"


def test_chooser_gets_two_attempts_with_a_clear_between_and_a_full_error():
    fn = _fn(_tree(), "_v962_pick_asset_in_picker")
    loops = [n for n in ast.walk(fn) if isinstance(n, ast.For) and ast.unparse(n.target) == "_v997_attempt"]
    assert len(loops) == 1, "expected the two-attempt chooser loop"
    loop = loops[0]
    assert ast.unparse(loop.iter) == "(1, 2)", ast.unparse(loop.iter)
    assert _calls(loop, "expect_file_chooser"), "the chooser must still be used inside the loop"
    assert _calls(loop, "_v975_clear_overlays"), "the page must be cleared before the second attempt"
    assert _calls(loop, "press"), "Escape must be pressed before the second attempt"
    src = ast.unparse(fn)
    assert "str(e)[:100]" not in src, "the failure print must not cut the call log to 100 chars any more"
    assert "page state at the failure" in src, "the failure must record the page state"


def test_a_second_failure_still_fails_closed():
    fn = _fn(_tree(), "_v962_pick_asset_in_picker")
    after = [n for n in ast.walk(fn) if isinstance(n, ast.If) and ast.unparse(n.test) == "_v997_err is not None"]
    assert len(after) == 1
    assert isinstance(after[0].body[-1], ast.Return) and ast.unparse(after[0].body[-1].value) == "False"
