"""v998.1 -- the long post-job wait prints a heartbeat the v978 watchdog can see.

batch27 (2026-09-13 19:48): v998's 900 s window held three bound renders, and
the v978 watchdog -- which measures liveness as main-thread prints and acts at
480 s of silence -- killed the worker at 483 s. The loop now says once a minute
what it waits on. Checked by AST.
"""
import ast
import os
import pathlib

_STATIC = pathlib.Path(__file__).parent / "static"
_PATH = pathlib.Path(os.environ.get("FLOW_WORKER_PATH") or (_STATIC / "flow_worker.py"))


def _tree():
    return ast.parse(_PATH.read_text(encoding="utf-8", errors="replace"))


def _postjob_loop(tree):
    loops = [n for n in ast.walk(tree) if isinstance(n, ast.While) and "_v998_window()" in ast.unparse(n.test)]
    assert len(loops) == 1, "the v998-bounded post-job loop was not found"
    return loops[0]


def test_the_loop_prints_a_heartbeat_from_inside_its_body():
    loop = _postjob_loop(_tree())
    prints = [c for c in ast.walk(loop) if isinstance(c, ast.Call) and isinstance(c.func, ast.Name)
              and c.func.id == "print" and "still cooking" in ast.unparse(c)]
    assert len(prints) == 1, "expected exactly one heartbeat print inside the post-job loop"
    assert any(isinstance(k.value, ast.Constant) and k.value.value is True
               for k in prints[0].keywords if k.arg == "flush"), "the heartbeat must flush"


def test_the_heartbeat_cadence_beats_the_watchdog():
    """60 s between heartbeats; the watchdog acts at _V978_ACT_S (480 by default)."""
    tree = _tree()
    src = ast.unparse(_postjob_loop(tree))
    assert "_v998_next_say = time.time() + 60" in src
    acts = [n for n in ast.walk(tree) if isinstance(n, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == "_V978_ACT_S" for t in n.targets)]
    assert len(acts) == 1
    assert "or 480" in ast.unparse(acts[0].value), ast.unparse(acts[0].value)
    assert 60 < 480
