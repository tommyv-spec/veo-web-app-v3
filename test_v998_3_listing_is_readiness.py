"""v998.3 -- on flow.google.com a bound uuid is ready when the media listing has its mp4.

Measured 2026-09-13 20:38: the ids the worker binds ARE the listing's keys, and
the in-page Range fetch that decided readiness times out on this host for every
listed mp4 -- eight finished renders read as "not ready" across three runs.
Checked by AST, plus the function run against fakes.
"""
import ast
import os
import pathlib
import types

_STATIC = pathlib.Path(__file__).parent / "static"
_PATH = pathlib.Path(os.environ.get("FLOW_WORKER_PATH") or (_STATIC / "flow_worker.py"))


def _tree():
    return ast.parse(_PATH.read_text(encoding="utf-8", errors="replace"))


def _fn(tree, name):
    return next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == name)


def test_the_new_host_branch_comes_before_the_page_fetch():
    fn = _fn(_tree(), "_uuid_video_ready")
    src = ast.unparse(fn)
    assert "_v962_on_new_host(page)" in src and "_V963_MEDIA_URLS" in src
    assert src.index("_v962_on_new_host(page)") < src.index("wait_for_function"), "the listing answer must come before any page fetch"
    guards = [n for n in ast.walk(fn) if isinstance(n, ast.If) and ast.unparse(n.test) == "_v962_on_new_host(page)"]
    assert len(guards) == 1
    assert isinstance(guards[0].body[-1], ast.Return)


def test_behaviour_against_fakes():
    """listing has the mp4 -> ready; listing lacks it -> not ready; legacy host -> the fetch path (fake page raises -> False)."""
    fn = _fn(_tree(), "_uuid_video_ready")
    mod = ast.Module(body=[fn], type_ignores=[])
    ast.fix_missing_locations(mod)

    class _Page:
        def wait_for_function(self, *a, **k):
            raise RuntimeError("fetch path must not run on the new host")

    ns = {
        "_v962_on_new_host": lambda p: True,
        "_V963_MEDIA_URLS": {"abcd1234": "https://flow.google.com/asb/token=mm,22,15"},
        "_construct_media_url": lambda u: "",
        "_V977_MEDIA_PROBE_TIMEOUT_MS": 1,
    }
    exec(compile(mod, "<v998.3>", "exec"), ns)
    ready = ns["_uuid_video_ready"]
    assert ready(_Page(), "ABCD1234") is True          # case-insensitive, listed -> ready
    assert ready(_Page(), "ffff0000") is False         # not listed -> not ready
    ns["_v962_on_new_host"] = lambda p: False           # legacy host: the fetch path runs and, failing, answers False
    assert ready(_Page(), "abcd1234") is False
