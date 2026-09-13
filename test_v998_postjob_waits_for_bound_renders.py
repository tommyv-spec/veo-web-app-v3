"""v998 -- the post-job wait gives a bound render the time a render takes.

Measured 2026-09-13: batch25 and batch26c gave six clips up after the post-job
300 s window while their media ids were bound at submit and their renders
simply were not finished; all six finished in Flow and shipped by harvest. The
window is now 900 s while any pending clip is bound and unrefused; ghosts and
refusals keep 300 s and the v992 ladder unchanged. Checked by AST, plus the
window function run against fakes.
"""
import ast
import os
import pathlib

_STATIC = pathlib.Path(__file__).parent / "static"
_PATH = pathlib.Path(os.environ.get("FLOW_WORKER_PATH") or (_STATIC / "flow_worker.py"))


def _tree():
    return ast.parse(_PATH.read_text(encoding="utf-8", errors="replace"))


def _calls(node, name):
    return [c for c in ast.walk(node) if isinstance(c, ast.Call)
            and isinstance(c.func, ast.Name) and c.func.id == name]


def _window_fn(tree):
    fns = [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "_v998_window"]
    assert len(fns) == 1, "expected one _v998_window"
    return fns[0]


def test_the_post_job_loop_is_bounded_by_the_window_not_by_300():
    tree = _tree()
    loops = [n for n in ast.walk(tree) if isinstance(n, ast.While)
             and "_pending_left" in ast.unparse(n.test) and "_poll_start" in ast.unparse(n.test)]
    assert loops, "the post-job poll loop was not found"
    bounded = [n for n in loops if "_v998_window()" in ast.unparse(n.test)]
    assert len(bounded) == 1, [ast.unparse(n.test) for n in loops]
    assert "< 300" not in ast.unparse(bounded[0].test)


def test_the_window_asks_for_bound_uuids_and_refusals_and_returns_the_deadline():
    fn = _window_fn(_tree())
    assert _calls(fn, "bound_media_ids_for_clip")
    assert _calls(fn, "_v992_peek_refusal_for_clip")
    rets = [ast.unparse(r.value) for r in ast.walk(fn) if isinstance(r, ast.Return)]
    assert "_V998_RENDER_DEADLINE_S" in rets and "300" in rets, rets


def test_the_deadline_defaults_to_900_with_an_env_override():
    tree = _tree()
    assigns = [n for n in ast.walk(tree) if isinstance(n, ast.Assign)
               and any(isinstance(t, ast.Name) and t.id == "_V998_RENDER_DEADLINE_S" for t in n.targets)]
    assert len(assigns) == 1
    expr = ast.unparse(assigns[0].value)
    assert "FLOW_POSTJOB_RENDER_DEADLINE_S" in expr and expr.rstrip(")").endswith("or 900"), expr


def test_the_window_function_behaves(monkeypatch):
    """Run _v998_window against fakes: bound+unrefused -> long; refused or unbound -> 300."""
    fn = _window_fn(_tree())
    mod = ast.Module(body=[fn], type_ignores=[])
    ast.fix_missing_locations(mod)
    calls = {"bound": {}, "refused": {}}
    ns = {
        "clips": [{"clip_index": 1}, {"clip_index": 2}],
        "_pending_left": {1, 2},
        "http_enqueued_clips": set(),
        "job_id": "j",
        "_V998_RENDER_DEADLINE_S": 900.0,
        "bound_media_ids_for_clip": lambda j, i: calls["bound"].get(i, []),
        "_v992_peek_refusal_for_clip": lambda j, i: calls["refused"].get(i),
    }
    exec(compile(mod, "<v998>", "exec"), ns)
    w = ns["_v998_window"]
    assert w() == 300                                   # nothing bound
    calls["bound"][1] = ["abcd"]
    assert w() == 900.0                                 # one bound, unrefused
    calls["refused"][1] = "PUBLIC_ERROR_UNSAFE_GENERATION"
    assert w() == 300                                   # bound but refused -> ladder, short window
    calls["bound"][2] = ["efgh"]
    assert w() == 900.0                                 # the other clip is bound and clean
    ns["http_enqueued_clips"].add(2)
    assert w() == 300                                   # already enqueued clips do not hold the window


def test_the_give_up_still_runs_and_says_how_long_a_bound_clip_waited():
    tree = _tree()
    loops = [n for n in ast.walk(tree) if isinstance(n, ast.For)
             and ast.unparse(n.target) == "_ci" and ast.unparse(n.iter) == "_pending_left"
             and _calls(n, "_v992_give_up_on_clip")]
    assert len(loops) == 1
    src = ast.unparse(loops[0])
    assert "still has no video after" in src
    assert src.index("still has no video after") < src.index("_v992_give_up_on_clip(")
