from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import image_platform as ip


TEST_ROOT = Path(__file__).resolve().parents[1] / "data" / "dual_backend_gate_tests"


def _root(name):
    path = TEST_ROOT / name
    path.mkdir(parents=True, exist_ok=True)
    return path


def _session():
    engine = create_engine("sqlite:///:memory:")
    ip.ImageNode.__table__.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def _generated(db, root, *, batch_id=None, chatgpt_source="ai", cg_status="ready"):
    node = ip.ImageNode(
        id=1,
        user_id="u1",
        kind="generated",
        name="scene 1",
        prompt="current prompt",
        status="ready",
        cg_status=cg_status,
        batch_id=batch_id,
        scene_index_in_batch=0,
    )
    db.add(node)
    for variant_id, backend, source in (
        (101, "banana", "ai"),
        (102, "chatgpt", chatgpt_source),
    ):
        relative = f"nodes/1/{backend}.png"
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(backend.encode())
        db.add(ip.ImageVariant(
            id=variant_id,
            node_id=1,
            variant_index=variant_id - 100,
            image_path=relative,
            source=source,
            backend=backend,
        ))
    db.commit()
    node.completed_contract_hash = ip._generation_contract_hash(db, node, "banana")
    node.cg_completed_contract_hash = ip._generation_contract_hash(
        db, node, "chatgpt"
    )
    db.commit()
    return node


def test_choose_requires_real_non_manual_output_from_both_backends(monkeypatch):
    db = _session()
    root = _root("manual-does-not-count")
    monkeypatch.setattr(ip, "images_root", lambda: root)
    _generated(db, root, chatgpt_source="manual")

    with pytest.raises(HTTPException) as exc:
        ip.choose_variant(
            1,
            ip.ChooseVariantRequest(variant_id=101),
            db=db,
            current_user=SimpleNamespace(id="u1"),
        )

    assert exc.value.status_code == 409
    assert "chatgpt has no current non-manual variant" in exc.value.detail["problems"]


def test_choose_waits_for_chained_chatgpt_lane_even_when_old_file_exists(
    monkeypatch,
):
    db = _session()
    root = _root("chain-waits")
    monkeypatch.setattr(ip, "images_root", lambda: root)
    _generated(db, root, cg_status="queued")
    db.add_all([
        ip.ImageNode(id=2, user_id="u1", kind="upload", status="ready"),
        ip.ImageEdge(parent_node_id=2, child_node_id=1, role="chain", slot_order=0),
    ])
    db.commit()

    with pytest.raises(HTTPException) as exc:
        ip.choose_variant(
            1,
            ip.ChooseVariantRequest(variant_id=101),
            db=db,
            current_user=SimpleNamespace(id="u1"),
        )

    assert exc.value.status_code == 409
    assert "chatgpt lane is queued" in exc.value.detail["problems"]


def test_choose_accepts_exact_current_files_from_both_backends(monkeypatch):
    db = _session()
    root = _root("both-ready")
    monkeypatch.setattr(ip, "images_root", lambda: root)
    _generated(db, root)

    result = ip.choose_variant(
        1,
        ip.ChooseVariantRequest(variant_id=102),
        db=db,
        current_user=SimpleNamespace(id="u1"),
    )

    assert result["chosen_variant_id"] == 102


def test_choose_refuses_the_requested_row_when_its_exact_file_is_missing(monkeypatch):
    db = _session()
    root = _root("chosen-file-missing")
    monkeypatch.setattr(ip, "images_root", lambda: root)
    _generated(db, root)
    db.add(ip.ImageVariant(
        id=103,
        node_id=1,
        variant_index=3,
        image_path="nodes/1/missing-banana.png",
        source="ai",
        backend="banana",
    ))
    db.commit()

    with pytest.raises(HTTPException) as exc:
        ip.choose_variant(
            1,
            ip.ChooseVariantRequest(variant_id=103),
            db=db,
            current_user=SimpleNamespace(id="u1"),
        )

    assert exc.value.status_code == 409
    assert "current file is missing" in exc.value.detail


def test_promote_rechecks_dual_outputs_instead_of_trusting_an_old_choice(
    monkeypatch,
):
    db = _session()
    root = _root("promote-recheck")
    monkeypatch.setattr(ip, "images_root", lambda: root)
    batch = ip.ImageJobBatch(id="batch-1", user_id="u1", name="batch", total_scenes=1)
    db.add(batch)
    node = _generated(db, root, batch_id="batch-1", cg_status="failed")
    node.chosen_variant_id = 101
    db.commit()

    with pytest.raises(HTTPException) as exc:
        ip.promote_batch_to_video(
            "batch-1", db=db, current_user=SimpleNamespace(id="u1")
        )

    assert exc.value.status_code == 409
    assert exc.value.detail["missing"][0]["problems"] == [
        "chatgpt lane is failed"
    ]


def test_choose_rejects_outputs_from_an_old_parent_choice(monkeypatch):
    db = _session()
    root = _root("stale-parent-choice")
    monkeypatch.setattr(ip, "images_root", lambda: root)
    node = _generated(db, root)
    parent = ip.ImageNode(
        id=2, user_id="u1", kind="upload", status="ready",
        chosen_variant_id=201,
    )
    db.add_all([
        parent,
        ip.ImageVariant(
            id=201, node_id=2, variant_index=1,
            image_path="nodes/2/first.png", source="manual",
        ),
        ip.ImageVariant(
            id=202, node_id=2, variant_index=2,
            image_path="nodes/2/second.png", source="manual",
        ),
        ip.ImageEdge(
            parent_node_id=2, child_node_id=1, role="character", slot_order=0,
        ),
    ])
    db.commit()
    node.completed_contract_hash = ip._generation_contract_hash(db, node, "banana")
    node.cg_completed_contract_hash = ip._generation_contract_hash(
        db, node, "chatgpt"
    )
    db.commit()

    parent.chosen_variant_id = 202
    db.commit()
    with pytest.raises(HTTPException) as exc:
        ip.choose_variant(
            1,
            ip.ChooseVariantRequest(variant_id=101),
            db=db,
            current_user=SimpleNamespace(id="u1"),
        )

    assert exc.value.status_code == 409
    assert any(
        "output contract is stale or unproven" in problem
        for problem in exc.value.detail["problems"]
    )


def test_promote_rejects_outputs_from_old_dimensions(monkeypatch):
    db = _session()
    root = _root("stale-promote-dimensions")
    monkeypatch.setattr(ip, "images_root", lambda: root)
    batch = ip.ImageJobBatch(
        id="batch-stale", user_id="u1", name="batch", total_scenes=1,
    )
    db.add(batch)
    node = _generated(db, root, batch_id="batch-stale")
    node.chosen_variant_id = 101
    db.commit()

    node.resolution = "4K"
    db.commit()
    with pytest.raises(HTTPException) as exc:
        ip.promote_batch_to_video(
            "batch-stale", db=db, current_user=SimpleNamespace(id="u1")
        )

    assert exc.value.status_code == 409
    assert any(
        "output contract is stale or unproven" in problem
        for problem in exc.value.detail["missing"][0]["problems"]
    )


def test_new_parent_choice_invalidates_and_requeues_child_outputs(monkeypatch):
    db = _session()
    root = _root("parent-choice-requeues-child")
    monkeypatch.setattr(ip, "images_root", lambda: root)
    child = _generated(db, root)
    for name in ("first.png", "second.png"):
        path = root / "nodes" / "2" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(name.encode())
    parent = ip.ImageNode(
        id=2, user_id="u1", kind="upload", status="ready",
        chosen_variant_id=201,
    )
    db.add_all([
        parent,
        ip.ImageVariant(
            id=201, node_id=2, variant_index=1,
            image_path="nodes/2/first.png", source="manual",
        ),
        ip.ImageVariant(
            id=202, node_id=2, variant_index=2,
            image_path="nodes/2/second.png", source="manual",
        ),
        ip.ImageEdge(
            parent_node_id=2, child_node_id=1, role="character", slot_order=0,
        ),
    ])
    db.commit()
    child.completed_contract_hash = ip._generation_contract_hash(
        db, child, "banana"
    )
    child.cg_completed_contract_hash = ip._generation_contract_hash(
        db, child, "chatgpt"
    )
    db.commit()
    monkeypatch.setattr(ip, "write_generation_job", lambda *args, **kwargs: None)
    monkeypatch.setattr(ip, "_storage_delete", lambda *args, **kwargs: None)

    ip.choose_variant(
        2,
        ip.ChooseVariantRequest(variant_id=202),
        db=db,
        current_user=SimpleNamespace(id="u1"),
    )

    db.refresh(child)
    assert child.status == "queued"
    assert child.cg_status == "queued"
    assert child.completed_contract_hash is None
    assert child.cg_completed_contract_hash is None
    assert db.query(ip.ImageVariant).filter(
        ip.ImageVariant.node_id == child.id
    ).count() == 0


def _stale_child_with_two_parents(db, root, *, second_parent_chosen):
    child = _generated(db, root)
    first = ip.ImageNode(
        id=2, user_id="u1", kind="upload", status="ready",
        chosen_variant_id=201,
    )
    second = ip.ImageNode(
        id=3, user_id="u1", kind="upload", status="ready",
        chosen_variant_id=301 if second_parent_chosen else None,
    )
    db.add_all([
        first,
        second,
        ip.ImageVariant(
            id=201, node_id=2, variant_index=1,
            image_path="nodes/2/first.png", source="manual",
        ),
        ip.ImageVariant(
            id=202, node_id=2, variant_index=2,
            image_path="nodes/2/second.png", source="manual",
        ),
        ip.ImageVariant(
            id=301, node_id=3, variant_index=1,
            image_path="nodes/3/first.png", source="manual",
        ),
        ip.ImageEdge(
            parent_node_id=2, child_node_id=1, role="character", slot_order=0,
        ),
        ip.ImageEdge(
            parent_node_id=3, child_node_id=1, role="product", slot_order=1,
        ),
    ])
    db.commit()
    child.completed_contract_hash = ip._generation_contract_hash(
        db, child, "banana"
    )
    child.cg_completed_contract_hash = ip._generation_contract_hash(
        db, child, "chatgpt"
    )
    db.commit()
    first.chosen_variant_id = 202
    db.commit()
    return child


def test_stale_child_invalidation_commits_when_second_parent_is_unchosen(
    monkeypatch,
):
    db = _session()
    engine = db.get_bind()
    root = _root("stale-child-second-parent-unchosen")
    monkeypatch.setattr(ip, "images_root", lambda: root)
    _stale_child_with_two_parents(db, root, second_parent_chosen=False)
    cleanup_row_counts = []

    def observe_committed_cleanup(*args, **kwargs):
        check = sessionmaker(bind=engine)()
        cleanup_row_counts.append(check.query(ip.ImageVariant).filter(
            ip.ImageVariant.node_id == 1
        ).count())
        check.close()

    monkeypatch.setattr(ip, "_storage_delete", observe_committed_cleanup)

    ip._promote_ready_children(db, 2)
    db.rollback()
    db.close()

    check = sessionmaker(bind=engine)()
    child = check.query(ip.ImageNode).filter(ip.ImageNode.id == 1).one()
    assert child.status == "draft"
    assert child.cg_status is None
    assert check.query(ip.ImageVariant).filter(
        ip.ImageVariant.node_id == child.id
    ).count() == 0
    assert cleanup_row_counts and set(cleanup_row_counts) == {0}
    assert not (root / "nodes/1/banana.png").exists()
    assert not (root / "nodes/1/chatgpt.png").exists()


def test_stale_child_invalidation_survives_queue_write_failure_and_rollback(
    monkeypatch,
):
    db = _session()
    engine = db.get_bind()
    root = _root("stale-child-queue-write-failure")
    monkeypatch.setattr(ip, "images_root", lambda: root)
    _stale_child_with_two_parents(db, root, second_parent_chosen=True)
    monkeypatch.setattr(ip, "_storage_delete", lambda *args, **kwargs: None)

    def fail_write(*args, **kwargs):
        raise OSError("queue unavailable")

    monkeypatch.setattr(ip, "write_generation_job", fail_write)
    ip._promote_ready_children(db, 2)
    db.rollback()
    db.close()

    check = sessionmaker(bind=engine)()
    child = check.query(ip.ImageNode).filter(ip.ImageNode.id == 1).one()
    assert child.status == "draft"
    assert child.cg_status is None
    assert child.error_message == "Queue failed: queue unavailable"
    assert check.query(ip.ImageVariant).filter(
        ip.ImageVariant.node_id == child.id
    ).count() == 0
    assert not (root / "nodes/1/banana.png").exists()
    assert not (root / "nodes/1/chatgpt.png").exists()


def test_upload_source_nodes_are_outside_the_dual_render_gate():
    db = _session()
    upload = ip.ImageNode(id=9, user_id="u1", kind="upload", status="ready")
    db.add(upload)
    db.commit()
    assert ip._dual_backend_output_problems(db, upload) == []
