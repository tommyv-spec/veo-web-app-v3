"""A scoped Flow proof run must be unable to touch any unlisted clip."""

import ast
import os
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


def test_shared_hold_blocks_general_start_but_allows_exact_scope(tmp_path):
    hold = tmp_path / "flow"
    hold.write_text("paused", encoding="utf-8")
    blocks = _function(
        WORKER,
        "_flow_worker_hold_blocks_start",
        {"os": os, "_flow_worker_hold_path": lambda: str(hold)},
    )
    assert blocks(str(hold), "") is True
    assert blocks(str(hold), "14907,14935") is False
    assert blocks(str(tmp_path / "missing"), "") is False


def test_hold_guard_runs_before_singleton_and_browser_start():
    source = WORKER.read_text(encoding="utf-8")
    early_main = source.split('if __name__ == "__main__":', 1)[1].split(
        "# Pin the console encoding", 1
    )[0]
    assert early_main.index("_flow_worker_hold_blocks_start()") < early_main.index(
        "_acquire_flow_worker_singleton()"
    )


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


def test_scoped_worker_stops_only_after_every_clip_is_terminal():
    states = {
        14907: {"status": "completed", "has_video": True},
        14935: {"status": "failed", "has_video": False},
    }

    def api_request(_method, url):
        clip_id = int(url.split("/clips/", 1)[1].split("/", 1)[0])
        return states.get(clip_id)

    check = _function(
        WORKER,
        "_scoped_flow_work_is_terminal",
        {"os": os, "FLOW_ONLY_CLIP_IDS": (14907, 14935), "api_request": api_request},
    )
    assert check() is True
    states[14935] = {"status": "generating", "has_video": False}
    assert check() is False
    states.pop(14935)
    assert check() is False


def test_scoped_worker_auto_exit_can_be_disabled(monkeypatch):
    monkeypatch.setenv("FLOW_SCOPE_AUTO_EXIT", "0")

    def api_request(_method, _url):
        raise AssertionError("terminal API must not run when auto-exit is disabled")

    check = _function(
        WORKER,
        "_scoped_flow_work_is_terminal",
        {"os": os, "FLOW_ONLY_CLIP_IDS": (14907, 14935), "api_request": api_request},
    )
    assert check() is False


def test_project_replay_does_not_wait_for_impossible_network_idle():
    source = WORKER.read_text(encoding="utf-8")
    replay = source.split("def _fa_init_project_best_effort(", 1)[1].split(
        "\ndef force_agent_off", 1
    )[0]
    assert 'wait_for_load_state("networkidle"' not in replay
    assert "time.sleep(2)" in replay


def test_redo_project_state_probe_is_bounded_not_skippable():
    """v963 — replaces test_redo_project_state_probe_can_be_skipped_for_bounded_proof_run.

    That test was added the same day the probe was seen hanging, and it pinned
    an env flag that SKIPPED the probe so a proof run could get past it. The
    goal was right and the flag did unblock that run, but skipping a check does
    not bound the call underneath it: `page.evaluate` takes no timeout, so the
    hang was still there, just no longer looked at. Both lines fired in the
    2026-09-07 failure and the worker went silent immediately afterwards.

    The probe is now `_flow_project_state()`, one `wait_for_function` with the
    same 10-second budget enforced by the driver. Nothing needs skipping, so
    there is no flag to assert. Announced to the original author in HANDOFF
    rev 811 §4 before this landed.
    """
    source = WORKER.read_text(encoding="utf-8")
    redo = source.split("def process_redo_clip(", 1)[1].split(
        "\ndef process_job_submission_with_failover", 1
    )[0]
    assert "_flow_project_state(page, timeout_s=10.0)" in redo
    assert "FLOW_SKIP_PROJECT_STATE_PROBE" not in source
    assert "_skip_project_state_probe" not in redo


def test_empty_scoped_queue_exits_only_after_terminal_check():
    source = WORKER.read_text(encoding="utf-8")
    redo = source.split("def get_redo_clips(", 1)[1].split(
        "\ndef clip_done_in_platform", 1
    )[0]
    assert "_scoped_flow_work_is_terminal()" in redo
    assert "raise SystemExit(0)" in redo
    assert redo.index("if result and result.get(\"clips\")") < redo.index(
        "_scoped_flow_work_is_terminal()"
    )


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
