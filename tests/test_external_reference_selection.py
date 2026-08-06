import json
from types import SimpleNamespace

import pytest

import send_to_platform as cli


def _write_plan(tmp_path, selected_refs):
    plan_dir = tmp_path / "raw" / "refs" / "demo"
    image_dir = plan_dir / "image_1"
    image_dir.mkdir(parents=True)
    (image_dir / "porch.jpg").write_bytes(b"jpeg")
    plan_path = plan_dir / "refs_plan.json"
    plan_path.write_text(json.dumps({
        "build": "demo",
        "images": [{"image": "image_1", "selected_refs": selected_refs}],
    }), encoding="utf-8")
    return plan_path


def test_selection_requires_explicit_selected_refs(tmp_path):
    plan = _write_plan(tmp_path, [])
    with pytest.raises(cli.PlatformError, match="has no selected_refs"):
        cli.load_external_reference_selection("demo.md", plan)


def test_selection_keeps_open_role_and_instruction(tmp_path):
    plan = _write_plan(tmp_path, [{
        "file": "porch.jpg",
        "role": "background geometry",
        "instruction": "Take only the porch layout. Ignore people and clothing.",
    }])

    actual_plan, selected = cli.load_external_reference_selection("demo.md", plan)

    assert actual_plan == plan.resolve()
    assert selected["image_1"][0]["role"] == "background geometry"
    assert selected["image_1"][0]["instruction"].startswith("Take only the porch")


def test_selection_rejects_path_escape(tmp_path):
    plan = _write_plan(tmp_path, [{
        "file": "../outside.jpg",
        "role": "background",
        "instruction": "Take the layout.",
    }])
    (plan.parent / "outside.jpg").write_bytes(b"jpeg")

    with pytest.raises(cli.PlatformError, match="must stay inside"):
        cli.load_external_reference_selection("demo.md", plan)


def test_upload_builds_per_image_api_binding():
    class FakeClient:
        def __init__(self):
            self.calls = []

        def upload_image(self, path, display_name):
            self.calls.append((path, display_name))
            return {"id": 701 + len(self.calls)}

    client = FakeClient()
    report = {}
    selected = {"image_3": [{
        "path": "C:/refs/pose.jpg",
        "file": "pose.jpg",
        "role": "body pose",
        "instruction": "Take only the arm and torso pose.",
    }]}

    bindings = cli.upload_external_reference_selection(client, selected, report)

    assert bindings == {"image_3": [{
        "parent_node_id": 702,
        "role": "body pose",
        "reference_instruction": "Take only the arm and torso pose.",
        # v912: a fetched candidate is unverified until someone looks at it.
        "origin": "auto",
    }]}
    assert report["external_references"][0]["node_id"] == 702


def _import_args(external_reference_nodes=None):
    return SimpleNamespace(
        subject=10,
        variants=4,
        name=None,
        ingredient=[],
        product_node=None,
        external_reference_nodes=external_reference_nodes,
    )


def test_import_payload_omits_external_refs_by_default():
    class FakeClient:
        def post(self, path, payload):
            assert "external_references" not in payload
            return {"batch_id": "b1", "scene_nodes": {}}

    assert cli.do_import(FakeClient(), "markdown", _import_args(), {}) == "b1"


def test_import_payload_includes_external_refs_only_when_selected():
    refs = {"image_1": [{
        "parent_node_id": 22,
        "role": "lighting",
        "reference_instruction": "Take only the warm window light.",
    }]}

    class FakeClient:
        def post(self, path, payload):
            assert payload["external_references"] == refs
            return {"batch_id": "b2", "scene_nodes": {}}

    assert cli.do_import(FakeClient(), "markdown", _import_args(refs), {}) == "b2"
