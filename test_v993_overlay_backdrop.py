"""v993 -- the overlay clear must wait for the CDK backdrop, not just the pane.

Measured 2026-09-13 on a 22-clip run: after clip 5 every clip failed its first
frame attach and, on the retry pass, the settings chip was also unreachable --
16 clips failed with `could NOT set the duration tab`. Two different page
controls unreachable from the same point in one session is one cause. The
worker's own earlier page dump recorded `cdk-overlay-backdrop (visible)` next to
the pane, and `_v975_clear_overlays` only ever counted the pane.

Also pins v985's wait to 0 by default: it was built on a hypothesis that turned
out wrong and stacked into a 480 s stall on the same run.
"""
import ast
import os
import pathlib

_STATIC = pathlib.Path(__file__).parent / "static"
_PATH = pathlib.Path(os.environ.get("FLOW_WORKER_PATH") or (_STATIC / "flow_worker.py"))


def _fn(name):
    tree = ast.parse(_PATH.read_text(encoding="utf-8", errors="replace"))
    return next(n for n in ast.walk(tree)
                if isinstance(n, ast.FunctionDef) and n.name == name)


def _selectors(fn):
    """Every string literal handed to page.locator(...) inside the function.
    Adjacent literals ("a " "b") are one ast.Constant once parsed."""
    out = []
    for n in ast.walk(fn):
        if (isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                and n.func.attr == "locator" and n.args
                and isinstance(n.args[0], ast.Constant) and isinstance(n.args[0].value, str)):
            out.append(n.args[0].value)
    return out


def test_every_overlay_count_in_the_clear_includes_the_backdrop():
    """THE REGRESSION. A pane count of 0 with a backdrop still up read as
    'picker gone', and every later click in the session timed out."""
    sels = _selectors(_fn("_v975_clear_overlays"))
    assert sels, "the overlay clear no longer counts overlays at all"
    for s in sels:
        assert ".cdk-overlay-pane" in s, s
        assert ".cdk-overlay-backdrop" in s, (
            f"this overlay count ignores the backdrop -- a lingering "
            f".cdk-overlay-backdrop makes every control present-but-unclickable: {s!r}")


def test_the_clear_still_returns_a_boolean_on_both_paths():
    """The call site (v986) branches on the return; it must stay a bool."""
    fn = _fn("_v975_clear_overlays")
    returns = [n for n in ast.walk(fn) if isinstance(n, ast.Return)]
    assert returns
    for r in returns:
        v = r.value
        ok = (isinstance(v, ast.Constant) and isinstance(v.value, bool)) or isinstance(v, ast.Compare)
        assert ok, ast.unparse(r)


def test_the_v985_wait_is_off_by_default():
    """30 s per absent asset with no proven benefit stacked into a 480 s stall.
    The env knob stays; the default does not."""
    src = _PATH.read_text(encoding="utf-8", errors="replace")
    tree = ast.parse(src)
    assigns = [n for n in ast.walk(tree) if isinstance(n, ast.Assign)
               and any(isinstance(t, ast.Name) and t.id == "_V985_PICK_WAIT_S" for t in n.targets)]
    assert len(assigns) == 1, "expected exactly one module-level _V985_PICK_WAIT_S"
    expr = ast.unparse(assigns[0].value)
    assert 'FLOW_PICK_WAIT_S' in expr, "the env override must survive"
    assert expr.rstrip(")").endswith("or 0"), (
        f"the default must be 0 so the loop is skipped unless someone measures a "
        f"need for it; got {expr!r}")
