"""PostgreSQL-only concurrency coverage for the image QC lock boundary.

SQLite's in-memory database does not enforce ``FOR UPDATE`` row locks, so
this test is deliberately skipped unless the operator supplies a real
PostgreSQL URL in ``IMAGE_QC_TEST_POSTGRES_URL``.
"""

from __future__ import annotations

import os
import threading
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import image_platform as ip


def test_qc_lock_blocks_parent_mutation_until_child_transaction_releases():
    """A QC-auto transaction's parent/child locks serialize parent writes."""
    url = os.environ.get("IMAGE_QC_TEST_POSTGRES_URL")
    if not url:
        pytest.skip(
            "set IMAGE_QC_TEST_POSTGRES_URL to run the real PostgreSQL row-lock test"
        )

    engine = create_engine(url, pool_pre_ping=True)
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    # ImageNode.user_id is VARCHAR(36); keep the run id within that limit.
    owner = f"qclock-{uuid.uuid4().hex[:28]}"
    first = Session()
    parent = child = None
    parent_id = child_id = None
    worker = None
    worker_done = threading.Event()
    worker_started = threading.Event()
    worker_state = {}

    try:
        parent = ip.ImageNode(
            user_id=owner, kind="upload", status="ready", name="lock parent"
        )
        child = ip.ImageNode(
            user_id=owner, kind="generated", status="draft", name="lock child",
            prompt="a test frame",
        )
        first.add_all([parent, child])
        first.flush()
        # Keep scalar ids before the session can expire/detach the ORM rows.
        parent_id, child_id = int(parent.id), int(child.id)
        edge = ip.ImageEdge(
            parent_node_id=parent_id, child_node_id=child_id,
            role="product", kind="product", slot_order=0,
        )
        first.add(edge)
        first.commit()

        # This is the transaction a QC-auto choice would use.  Keep it open
        # after FOR UPDATE so the independent writer cannot commit yet.
        locked_child, locked_rows = ip._lock_node_and_parents(
            first, child_id, owner
        )
        assert locked_child.id == child_id
        assert list(locked_rows) == sorted((parent_id, child_id))

        def conflicting_parent_write():
            second = Session()
            try:
                # Bound the test if the implementation ever stops taking the
                # row lock.  The lock is released well before this timeout in
                # the passing path, so it does not turn a real failure into a
                # hanging test.
                second.execute(text("SET LOCAL lock_timeout = '5s'"))
                parent_copy = second.get(ip.ImageNode, parent_id)
                parent_copy.name = "writer must wait"
                worker_started.set()
                second.flush()  # UPDATE blocks on first's FOR UPDATE lock.
                second.commit()
                worker_state["committed"] = True
            except BaseException as exc:  # surfaced in the main test
                worker_state["error"] = exc
                second.rollback()
            finally:
                second.close()
                worker_done.set()

        worker = threading.Thread(target=conflicting_parent_write, daemon=True)
        worker.start()
        assert worker_started.wait(5), "writer did not reach the conflicting UPDATE"
        # A fast completion here means the helper did not lock the parent row.
        assert not worker_done.wait(0.5), (
            "parent UPDATE committed while the QC child/parent locks were held"
        )

        # Releasing the QC transaction must let the same write proceed.
        first.rollback()
        worker.join(5)
        assert worker_done.is_set(), "writer remained blocked after lock release"
        assert "error" not in worker_state, worker_state.get("error")
        assert worker_state.get("committed") is True
    finally:
        first.rollback()
        first.close()
        if worker is not None:
            worker.join(5)
        # Clean only the rows owned by this test.  This runs after the worker
        # has joined, so it cannot race with the writer's open transaction.
        cleanup = Session()
        try:
            if child_id is not None:
                cleanup.query(ip.ImageEdge).filter(
                    ip.ImageEdge.child_node_id == child_id
                ).delete(synchronize_session=False)
                cleanup.query(ip.ImageNode).filter(
                    ip.ImageNode.id == child_id,
                    ip.ImageNode.user_id == owner,
                ).delete(synchronize_session=False)
            if parent_id is not None:
                cleanup.query(ip.ImageNode).filter(
                    ip.ImageNode.id == parent_id,
                    ip.ImageNode.user_id == owner,
                ).delete(synchronize_session=False)
            cleanup.commit()
        finally:
            cleanup.rollback()
            cleanup.close()
        engine.dispose()
