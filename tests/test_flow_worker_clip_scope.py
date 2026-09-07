"""A scoped Flow proof run must be unable to touch any unlisted clip."""

import ast
from pathlib import Path
from typing import Optional

import pytest


ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "static" / "flow_worker.py"
MAIN = ROOT / "main.py"


def _function(path: Path, name: str, globals_dict=None):
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
    namespace = dict(globals_dict or {})
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), namespace)
    return namespace[name]


def test_worker_scope_parser_accepts_unique_numeric_ids():
    parse = _function(WORKER, "_parse_flow_only_clip_ids")
    assert parse("14907, 14935,14907") == (14907, 14935)


@pytest.mark.parametrize("raw", ["14907,nope", "0", "-4", ",,,"])
def test_worker_scope_parser_fails_closed(raw):
    parse = _function(WORKER, "_parse_flow_only_clip_ids")
    with pytest.raises(RuntimeError):
        parse(raw)


def test_server_scope_parser_rejects_bad_input():
    class HTTPException(Exception):
        def __init__(self, status_code, detail):
            self.status_code = status_code
            self.detail = detail

    parse = _function(
        MAIN,
        "_parse_worker_clip_ids",
        {"HTTPException": HTTPException, "Optional": Optional},
    )
    assert parse("14935,14907,14935") == [14907, 14935]
    with pytest.raises(HTTPException) as exc:
        parse("14907,bad")
    assert exc.value.status_code == 422


def test_worker_sends_scope_and_disables_normal_job_polling():
    source = WORKER.read_text(encoding="utf-8")
    pending = source.split("def get_pending_job(", 1)[1].split("\ndef refresh_clip_statuses", 1)[0]
    redo = source.split("def get_redo_clips(", 1)[1].split("\ndef clip_done_in_platform", 1)[0]
    assert "if FLOW_ONLY_CLIP_IDS:" in pending
    assert "return None" in pending
    assert 'clip_ids={_url_quote(_flow_only_clip_ids_q())}' in redo
    assert "clip.get(\"id\") in allowed" in redo
    assert "BLOCKED" in redo


def test_worker_disables_kling_drain_during_scoped_flow_run():
    source = WORKER.read_text(encoding="utf-8")
    kling = source.split("def _kling_drain_loop(", 1)[1].split(
        '\nif __name__ == "__main__":', 1
    )[0]
    assert "if FLOW_ONLY_CLIP_IDS:" in kling
    assert kling.index("if FLOW_ONLY_CLIP_IDS:") < kling.index("hf_cli =")
    assert "return" in kling.split("if FLOW_ONLY_CLIP_IDS:", 1)[1].split("if not API_KEY:", 1)[0]


def test_both_redo_endpoints_filter_before_claiming():
    source = MAIN.read_text(encoding="utf-8")
    local = source.split("async def local_worker_get_redo_clips(", 1)[1].split(
        "\nclass LocalWorkerJobUpdate", 1
    )[0]
    user = source.split("async def user_worker_get_redo_clips(", 1)[1].split(
        '\n@app.get("/api/user-worker/clips/kling-pending")', 1
    )[0]
    for endpoint in (local, user):
        assert "allowed_clip_ids = _parse_worker_clip_ids(clip_ids)" in endpoint
        assert "_q = _q.filter(Clip.id.in_(allowed_clip_ids))" in endpoint
        assert endpoint.index("_q = _q.filter(Clip.id.in_(allowed_clip_ids))") < endpoint.index(
            "clip.claimed_by_worker = worker_id"
        )
