# tests/test_approval_summary.py
#
# /nodes/approval-summary answers "how many images are waiting to be approved"
# across ALL time. The sidebar's own count is derived from a windowed /nodes
# fetch (since_days, default 3), so it silently omits older unapproved images —
# this endpoint exists so the counter can't undercount.
#
# "Waiting" must mirror the frontend's imgIsAwaitingApproval exactly:
# kind='generated' AND status='ready' AND chosen_variant_id IS NULL.

import json
import types
from datetime import datetime, timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import image_platform as ip


def _session():
    eng = create_engine("sqlite:///:memory:")
    ip.ImageNode.__table__.metadata.create_all(eng)
    return sessionmaker(bind=eng)()


def _call(db, user_id="u1"):
    return ip.approval_summary(db=db, current_user=types.SimpleNamespace(id=user_id))


def _node(db, node_id, status="ready", chosen=None, kind="generated",
          user_id="u1", batch="A", scene=0, created=None):
    db.add(ip.ImageNode(
        id=node_id, user_id=user_id, kind=kind, name=f"node {node_id}",
        status=status, chosen_variant_id=chosen,
        batch_id=batch, scene_index_in_batch=scene,
        created_at=created or datetime.utcnow(),
    ))


def _edge(db, edge_id, parent, child):
    db.add(ip.ImageEdge(id=edge_id, parent_node_id=parent, child_node_id=child))


def test_counts_only_rendered_but_unapproved():
    db = _session()
    _node(db, 1)                                  # waiting
    _node(db, 2, chosen=99)                       # already approved
    _node(db, 3, status="generating")             # not rendered yet
    _node(db, 4, status="draft")                  # blocked, not rendered
    _node(db, 5, status="failed")                 # failed
    _node(db, 6, kind="upload")                   # uploads are never "approved"
    db.commit()
    out = _call(db)
    assert out["awaiting_total"] == 1
    assert [r["id"] for r in out["queue"]] == [1]


def test_old_nodes_are_counted_too():
    # The whole point: a node far outside the sidebar's 3-day window still
    # counts. A windowed count would report 1 here instead of 2.
    db = _session()
    _node(db, 1, created=datetime.utcnow())
    _node(db, 2, created=datetime.utcnow() - timedelta(days=400))
    db.commit()
    assert _call(db)["awaiting_total"] == 2


def test_blocks_counts_draft_children_only():
    db = _session()
    _node(db, 1)                                   # chain head
    _node(db, 2, status="draft")                   # blocked by 1
    _node(db, 3, status="draft")                   # blocked by 1
    _node(db, 4)                                   # terminal, waiting
    _node(db, 5, status="ready", chosen=7)         # rendered child, not blocked
    _edge(db, 1, 1, 2)
    _edge(db, 2, 1, 3)
    _edge(db, 3, 4, 5)
    db.commit()
    out = _call(db)
    by_id = {r["id"]: r["blocks"] for r in out["queue"]}
    assert by_id == {1: 2, 4: 0}
    assert out["chained_total"] == 1


def test_chain_heads_sort_first_then_batch_then_scene():
    db = _session()
    _node(db, 1, batch="B", scene=5)               # blocks nothing
    _node(db, 2, batch="A", scene=9)               # blocks nothing
    _node(db, 3, batch="A", scene=1)               # blocks nothing
    _node(db, 4, batch="Z", scene=0)               # chain head
    _node(db, 5, status="draft")
    _edge(db, 1, 4, 5)
    db.commit()
    # 4 leads on blocks; the rest fall into batch then scene order.
    assert [r["id"] for r in _call(db)["queue"]] == [4, 3, 2, 1]


def test_user_scoped():
    db = _session()
    _node(db, 1, user_id="u2")
    db.commit()
    out = _call(db, user_id="u1")
    assert out["awaiting_total"] == 0
    assert out["queue"] == []


def test_queue_capped_but_total_is_not():
    db = _session()
    for i in range(1, 251):
        _node(db, i, scene=i)
    db.commit()
    out = _call(db)
    assert out["awaiting_total"] == 250      # total is honest
    assert len(out["queue"]) == 200          # payload is bounded
    assert out["queue_truncated"] is True


def test_empty_is_clean():
    db = _session()
    out = _call(db)
    assert out == {"awaiting_total": 0, "chained_total": 0,
                   "queue": [], "queue_truncated": False}
