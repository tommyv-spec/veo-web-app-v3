"""v998.4 -- the download thread waits as long as a render takes on a listed-but-poster url.

v998.3 enqueues a bound clip as soon as the listing holds its mp4 url; if that
url still serves a poster, the poster/partial retry loop IS the render wait,
and 6 x 20 s was far below the 3-16 min renders measured on 2026-09-13.
Checked by AST: the patience covers the v998 render window.
"""
import ast
import os
import pathlib

_STATIC = pathlib.Path(__file__).parent / "static"
_PATH = pathlib.Path(os.environ.get("FLOW_WORKER_PATH") or (_STATIC / "flow_worker.py"))


def _assign(tree, name):
    hits = [n for n in ast.walk(tree) if isinstance(n, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == name for t in n.targets)]
    assert len(hits) == 1, f"{name}: {len(hits)} assignments"
    return hits[0]


def test_poster_patience_covers_the_render_window():
    tree = ast.parse(_PATH.read_text(encoding="utf-8", errors="replace"))
    mx = ast.unparse(_assign(tree, "POSTER_RETRY_MAX").value)
    assert "FLOW_POSTER_RETRY_MAX" in mx and mx.rstrip(")").endswith("or 45"), mx
    delay = ast.literal_eval(ast.unparse(_assign(tree, "POSTER_RETRY_DELAY").value))
    deadline = ast.unparse(_assign(tree, "_V998_RENDER_DEADLINE_S").value)
    assert deadline.rstrip(")").endswith("or 900"), deadline
    assert 45 * delay >= 900, f"45 x {delay}s does not cover the 900 s render window"


def test_the_poster_retry_loop_still_retries_the_same_url():
    src = _PATH.read_text(encoding="utf-8", errors="replace")
    assert "poster/partial → clip still rendering; wait + retry SAME url" in src
    assert "still poster/too-small after" in src
