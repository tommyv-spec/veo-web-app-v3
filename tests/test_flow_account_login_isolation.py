"""A signed-out optional Flow account must not stop a healthy account."""

import ast
from pathlib import Path
from types import SimpleNamespace


WORKER = Path(__file__).resolve().parents[1] / "static" / "flow_worker.py"
SOURCE = WORKER.read_text(encoding="utf-8")
TREE = ast.parse(SOURCE)


def _node(name):
    return next(
        node for node in TREE.body
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name == name
    )


def _function(name, globals_dict=None):
    node = _node(name)
    namespace = dict(globals_dict or {})
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(WORKER), "exec"), namespace)
    return namespace[name]


def _source(name):
    return ast.get_source_segment(SOURCE, _node(name)) or ""


def test_removing_account2_auth_keeps_account1_ready():
    add = _function("_flow_auth_marker_add")
    remove = _function("_flow_auth_marker_remove")
    row = add(None, 77, "Account1", {
        "authenticated_at": 100.0, "url": "https://flow.google.com/a",
        "build": "x", "proof": "dom",
    })
    row = add(row, 77, "Account2", {
        "authenticated_at": 101.0, "url": "https://flow.google.com/b",
        "build": "x", "proof": "dom",
    })
    row = remove(row, 77, "Account2")
    assert row["label"] == "Account1"
    assert set(row["accounts"]) == {"Account1"}


def test_removing_the_only_ready_account_clears_the_marker():
    add = _function("_flow_auth_marker_add")
    remove = _function("_flow_auth_marker_remove")
    row = add(None, 77, "Account1", {
        "authenticated_at": 100.0, "url": "https://flow.google.com/a",
        "build": "x", "proof": "dom",
    })
    assert remove(row, 77, "Account1") is None


def test_login_required_is_quarantined_without_six_restarts():
    run = next(
        node for node in _node("AccountWorker").body
        if isinstance(node, ast.FunctionDef) and node.name == "run"
    )
    handlers = [
        ast.unparse(node.type) if node.type is not None else ""
        for node in ast.walk(run) if isinstance(node, ast.ExceptHandler)
    ]
    assert "FlowLoginRequired" in handlers
    assert handlers.index("FlowLoginRequired") < handlers.index("Exception")


def test_idle_pool_contains_only_live_workers():
    idle = _function("_idle_live_account_names", {"account_health": SimpleNamespace(
        is_busy=lambda name: name == "Account3",
        needs_proactive_restore=lambda name: False,
        is_hot=lambda name: False,
    )})
    active = [{"name": "Account1"}, {"name": "Account2"}, {"name": "Account3"}]
    workers = [
        SimpleNamespace(name="Account1", is_alive=lambda: True),
        SimpleNamespace(name="Account2", is_alive=lambda: False),
        SimpleNamespace(name="Account3", is_alive=lambda: True),
    ]
    queues = {name: SimpleNamespace(qsize=lambda: 0) for name in (
        "Account1", "Account2", "Account3")}
    assert idle(active, workers, queues) == ["Account1"]
    assert _source("main_multi_account").count("_idle_live_account_names(") >= 3
