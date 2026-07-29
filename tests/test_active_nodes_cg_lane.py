# tests/test_active_nodes_cg_lane.py
#
# Regression guard for the cg-lane clause in /nodes/active (added by 7aa5363,
# previously untested): the endpoint must also return nodes kept alive purely
# by an open ChatGPT lane (cg_status queued/generating) even though their
# Banana lane (node.status) is already terminal.
#
# Why it matters: the frontend's 2s diff poll reads ONLY this endpoint. Filter
# on `status` alone and a node at status='ready' + cg_status='queued' vanishes
# from the poll — the UI never sees the ChatGPT variant land, so the GPT
# variant only appears after a full page refresh and the node's pending-GPT
# loader tile spins forever.

import json
import types

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import image_platform as ip


def _session():
    eng = create_engine("sqlite:///:memory:")
    ip.ImageNode.__table__.metadata.create_all(eng)
    return sessionmaker(bind=eng)()


def _request():
    # list_active_nodes only reads the if-none-match header.
    return types.SimpleNamespace(headers={})


def _call(db, user_id="u1"):
    res = ip.list_active_nodes(
        _request(), db=db, current_user=types.SimpleNamespace(id=user_id)
    )
    return {n["id"] for n in json.loads(res.body)["nodes"]}


def _node(db, node_id, status, cg_status, user_id="u1"):
    db.add(ip.ImageNode(
        id=node_id, user_id=user_id, kind="generated",
        name=f"node {node_id}", status=status, cg_status=cg_status,
    ))


def test_cg_queued_node_is_active_even_when_main_lane_ready():
    db = _session()
    _node(db, 1, "ready", "queued")
    db.commit()
    assert _call(db) == {1}


def test_cg_generating_node_is_active_even_when_main_lane_ready():
    db = _session()
    _node(db, 1, "ready", "generating")
    db.commit()
    assert _call(db) == {1}


def test_fully_terminal_node_is_not_active():
    db = _session()
    _node(db, 1, "ready", "ready")
    _node(db, 2, "failed", "failed")
    _node(db, 3, "ready", None)     # no ChatGPT lane at all (chain node)
    db.commit()
    assert _call(db) == set()


def test_main_lane_active_still_returned_with_no_cg_lane():
    db = _session()
    _node(db, 1, "queued", None)
    _node(db, 2, "generating", None)
    _node(db, 3, "draft", None)
    db.commit()
    assert _call(db) == {1, 2, 3}


def test_other_users_cg_pending_node_is_not_leaked():
    db = _session()
    _node(db, 1, "ready", "queued", user_id="u2")
    db.commit()
    assert _call(db, user_id="u1") == set()
