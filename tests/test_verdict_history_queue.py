# tests/test_verdict_history_queue.py
#
# v940.1 — labelling the HISTORY.
#
# v940 put the two verdict buttons on every non-chosen tile, so an image the
# operator reviews from now on gets its left-behind variants labelled while he
# is looking at it. Everything ALREADY picked carries the pick and nothing else
# — the same "not chosen = bad" conflation v940 exists to kill, aimed at the
# past. This is the worklist that walks back through it, newest first.
#
# Two rules the whole thing rests on:
#   * v886.3 — DATA CAPTURE ONLY. Nothing here may select, render, promote or
#     delete anything. The queue endpoint is read-only; the browser mode only
#     navigates, and the labelling is still the v940 buttons on the tiles.
#   * NULL means "the operator never said". Skipping an image must write
#     nothing at all — a skip is not a quiet 'rejected'.

import os
import re
import types
from datetime import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import image_platform as ip

_HERE = os.path.dirname(os.path.abspath(__file__))
_CODE = os.path.dirname(_HERE)
_INDEX = os.path.join(_CODE, "static", "index.html")
_PLATFORM = os.path.join(_CODE, "image_platform.py")


def _index_src():
    return open(_INDEX, encoding="utf-8").read()


def _platform_src():
    return open(_PLATFORM, encoding="utf-8").read()


def _session():
    eng = create_engine("sqlite:///:memory:")
    ip.ImageNode.__table__.metadata.create_all(eng)
    return sessionmaker(bind=eng)()


def _user(user_id="u1"):
    return types.SimpleNamespace(id=user_id)


def _node(db, node_id=1, user_id="u1", n_variants=3, status="ready",
          kind="generated", chosen=True, created_at=None, batch_id="b1",
          verdicts=None):
    """One node with n variants (ids node_id*100 + i).

    chosen=True picks variant 1, so variants 2..n are the siblings this feature
    is about. `verdicts` maps a variant index (1-based) to a verdict string.
    """
    n = ip.ImageNode(
        id=node_id, user_id=user_id, kind=kind,
        name=f"node {node_id}", prompt="a prompt", status=status,
        batch_id=batch_id, scene_index_in_batch=node_id,
        created_at=created_at or datetime(2026, 1, 1),
    )
    db.add(n)
    for i in range(1, n_variants + 1):
        db.add(ip.ImageVariant(
            id=node_id * 100 + i, node_id=node_id, variant_index=i,
            image_path=f"nodes/{node_id}/variant_{i}.png",
            operator_verdict=(verdicts or {}).get(i),
        ))
    db.flush()
    if chosen and n_variants:
        n.chosen_variant_id = node_id * 100 + 1
    db.commit()
    return n


def _queue(db, user_id="u1", limit=200, offset=0):
    return ip.verdict_queue(
        limit=limit, offset=offset, db=db, current_user=_user(user_id),
    )


def _ids(out):
    return [r["id"] for r in out["queue"]]


# --- what belongs in the queue -------------------------------------------

def test_picked_node_with_untagged_siblings_is_queued():
    db = _session()
    _node(db, node_id=1)
    out = _queue(db)
    assert _ids(out) == [1]
    assert out["pending_total"] == 1
    # 3 variants, 1 of them is the pick → 2 siblings, both unanswered.
    assert out["queue"][0]["siblings"] == 2
    assert out["queue"][0]["untagged"] == 2


def test_node_with_no_pick_is_not_in_this_queue():
    # An image still waiting for a pick belongs to the APPROVAL queue. Mixing
    # the two would send the operator to label an image he has not judged yet.
    db = _session()
    _node(db, node_id=1, chosen=False)
    out = _queue(db)
    assert _ids(out) == []
    assert out["pending_total"] == 0
    assert out["done_total"] == 0


def test_fully_tagged_node_is_done_not_pending():
    db = _session()
    _node(db, node_id=1, verdicts={2: "still_good", 3: "rejected"})
    out = _queue(db)
    assert _ids(out) == []
    assert out["pending_total"] == 0
    assert out["done_total"] == 1


def test_partially_tagged_node_is_still_pending():
    db = _session()
    _node(db, node_id=1, n_variants=4, verdicts={2: "rejected"})
    out = _queue(db)
    assert _ids(out) == [1]
    assert out["queue"][0]["siblings"] == 3
    assert out["queue"][0]["untagged"] == 2


def test_the_pick_itself_never_counts_as_unlabelled():
    # The endpoint refuses to label the chosen variant (it is already recorded
    # by chosen_variant_id). If it were counted as "untagged" here, every node
    # would sit in the queue forever and could never be cleared.
    db = _session()
    _node(db, node_id=1, n_variants=2, verdicts={2: "still_good"})
    out = _queue(db)
    assert _ids(out) == []
    assert out["done_total"] == 1


def test_node_whose_only_variant_is_the_pick_is_in_neither_count():
    # Nothing was left behind, so there is no question to answer. Counting it
    # as "done" would inflate the progress number with work never done.
    db = _session()
    _node(db, node_id=1, n_variants=1)
    out = _queue(db)
    assert _ids(out) == []
    assert out["pending_total"] == 0
    assert out["done_total"] == 0


@pytest.mark.parametrize("status", ["draft", "queued", "generating", "failed"])
def test_only_ready_nodes_are_queued(status):
    db = _session()
    _node(db, node_id=1, status=status)
    assert _ids(_queue(db)) == []


def test_uploads_are_not_queued():
    # An upload has no variants the operator chose between.
    db = _session()
    _node(db, node_id=1, kind="upload")
    assert _ids(_queue(db)) == []


# --- ownership ------------------------------------------------------------

def test_another_users_nodes_are_invisible():
    db = _session()
    _node(db, node_id=1, user_id="u1")
    _node(db, node_id=2, user_id="u2")
    assert _ids(_queue(db, user_id="u1")) == [1]
    assert _ids(_queue(db, user_id="u2")) == [2]


def test_another_users_finished_work_is_not_in_my_done_count():
    db = _session()
    _node(db, node_id=1, user_id="u2", verdicts={2: "rejected", 3: "rejected"})
    out = _queue(db, user_id="u1")
    assert out["pending_total"] == 0
    assert out["done_total"] == 0


# --- order + paging -------------------------------------------------------

def test_newest_first():
    # The operator remembers this week's picks and can label them honestly; a
    # year-old render is the wrong place to start.
    db = _session()
    _node(db, node_id=1, created_at=datetime(2026, 1, 1))
    _node(db, node_id=2, created_at=datetime(2026, 6, 1))
    _node(db, node_id=3, created_at=datetime(2026, 3, 1))
    assert _ids(_queue(db)) == [2, 3, 1]


def test_order_is_stable_when_timestamps_tie():
    # The browser holds an index into this list, so two calls must not disagree.
    db = _session()
    same = datetime(2026, 5, 5)
    for nid in (1, 2, 3):
        _node(db, node_id=nid, created_at=same)
    assert _ids(_queue(db)) == _ids(_queue(db)) == [3, 2, 1]


def test_limit_and_offset_page_the_queue():
    db = _session()
    for nid in (1, 2, 3):
        _node(db, node_id=nid, created_at=datetime(2026, 1, nid))
    first = _queue(db, limit=2, offset=0)
    assert _ids(first) == [3, 2]
    assert first["has_more"] is True
    # The totals are the WHOLE set, not the page — the counter says how much
    # work is left, not how much fitted in one response.
    assert first["pending_total"] == 3
    second = _queue(db, limit=2, offset=2)
    assert _ids(second) == [1]
    assert second["has_more"] is False


def test_offset_past_the_end_is_empty_not_an_error():
    db = _session()
    _node(db, node_id=1)
    out = _queue(db, offset=50)
    assert out["queue"] == []
    assert out["pending_total"] == 1
    assert out["has_more"] is False


def test_queue_rows_carry_what_the_client_needs_to_open_the_node():
    # An out-of-window node is not in the browser's node cache, so the client
    # pulls it in by batch_id first. A row without one is unreachable.
    db = _session()
    _node(db, node_id=7, batch_id="batch-xyz", created_at=datetime(2026, 2, 2))
    row = _queue(db)["queue"][0]
    assert row["id"] == 7
    assert row["batch_id"] == "batch-xyz"
    assert row["name"] == "node 7"
    assert row["scene_index_in_batch"] == 7
    assert row["created_at"].startswith("2026-02-02")


# --- v886.3: the mode captures, it never decides --------------------------

def test_reading_the_queue_writes_nothing():
    db = _session()
    node = _node(db, node_id=1, verdicts={2: "still_good"})
    _queue(db)
    _queue(db)
    db.refresh(node)
    assert node.chosen_variant_id == 101
    assert node.status == "ready"
    verdicts = {v.id: v.operator_verdict for v in
                db.query(ip.ImageVariant).order_by(ip.ImageVariant.id).all()}
    assert verdicts == {101: None, 102: "still_good", 103: None}


def test_labelling_every_sibling_clears_the_node_without_touching_the_pick():
    # The end-to-end shape of one walked node: tag the siblings through the
    # existing v940 endpoint, and the node leaves the queue on its own — with
    # the pick exactly where it was.
    db = _session()
    node = _node(db, node_id=1)
    assert _ids(_queue(db)) == [1]
    for vid, verdict in ((102, "still_good"), (103, "rejected")):
        ip.set_variant_verdict(
            1, vid, ip.VariantVerdictRequest(verdict=verdict),
            db=db, current_user=_user(),
        )
    out = _queue(db)
    assert _ids(out) == []
    assert out["done_total"] == 1
    db.refresh(node)
    assert node.chosen_variant_id == 101


# --- source wiring --------------------------------------------------------

def _get_routes():
    """(index, path) for every GET on the images router, in match order."""
    return [(i, r.path) for i, r in enumerate(ip.router.routes)
            if "GET" in (getattr(r, "methods", None) or ())]


def test_the_literal_path_wins_over_the_node_id_route():
    # FastAPI matches routes in declaration order. Declared after
    # /nodes/{node_id}, the literal path would be swallowed as a node id and
    # 422 on every call — and nothing else in the suite would notice. Asserted
    # against the real router rather than the source text, so a reorder is
    # caught however it happens.
    routes = _get_routes()
    want = "/api/images/nodes/verdict-queue"
    matcher = re.compile
    first = next(
        (path for _, path in routes
         if matcher("^" + re.sub(r"\{[^}]+\}", "[^/]+", path) + "$").match(want)),
        None,
    )
    assert first == want, f"/nodes/verdict-queue is shadowed by {first}"


def test_the_endpoint_is_actually_mounted():
    assert any(p == "/api/images/nodes/verdict-queue" for _, p in _get_routes())


def test_queue_endpoint_only_reads():
    # A commit/flush/delete inside the listing would break v886.3 silently.
    src = _platform_src()
    body = src[src.index('@router.get("/nodes/verdict-queue")'):]
    body = body[:body.index('@router.get("/nodes/{node_id}/final-prompt")')]
    for banned in ("db.commit(", "db.add(", "db.delete(", "db.flush("):
        assert banned not in body, f"verdict_queue must not {banned}"


def _history_block():
    """The v940.1 browser-side block, from its banner to the next function."""
    src = _index_src()
    start = src.index("// v940.1 — labelling the HISTORY.")
    end = src.index("function imgRenderJobOverview(groupKey)", start)
    return src[start:end]


def test_history_mode_never_chooses_a_variant():
    # The one thing this mode must never do. Navigating through already-picked
    # images while accidentally re-picking them would rewrite the very history
    # it exists to annotate.
    block = _history_block()
    assert "/choose" not in block
    assert "imgChooseVariant" not in block
    assert not re.search(r"chosen_variant_id\s*=[^=]", block), \
        "the history walk must never assign chosen_variant_id"


def test_skipping_writes_nothing():
    # A skipped image stays NULL. If skip ever POSTed, "the operator never
    # said" would become a forged verdict.
    block = _history_block()
    skip = block[block.index("window.imgVerdictHistorySkip"):]
    skip = skip[:skip.index("window.imgVerdictHistoryResume")]
    assert "fetch(" not in skip


def test_the_walk_cannot_loop_forever_refetching():
    # A queue whose every row is unopenable (batch fetch failing, a row with no
    # batch_id) would otherwise run skip-all → refetch → same queue → skip-all
    # → refetch, forever, hammering the server from a page that looks idle.
    block = _history_block()
    goto = block[block.index("async function imgVerdictHistoryGoTo"):]
    goto = goto[:goto.index("window.imgVerdictHistorySkip")]
    assert "if (!_refetched)" in goto
    assert "imgVerdictHistoryGoTo(0, true)" in goto


def test_the_walk_only_advances_when_every_sibling_is_answered():
    block = _history_block()
    assert "function imgUntaggedSiblings(node)" in block
    after = block[block.index("function imgVerdictHistoryAfterVerdict"):]
    assert "imgUntaggedSiblings(node) > 0" in after


def test_resolved_nodes_still_render_the_v940_buttons():
    # The whole retro path depends on this line: the buttons are gated on the
    # TILE being the pick, not on the NODE being unresolved. Gate it on the
    # node and every old image becomes unlabelable.
    src = _index_src()
    assert "${chosen ? '' : verdictBtnsHtml(v)}" in src
    assert "const chosen = v.id === node.chosen_variant_id;" in src


def test_detail_pane_renders_the_progress_banner():
    src = _index_src()
    assert "${imgVerdictHistoryBannerHtml(node)}" in src
    assert "function imgVerdictHistoryBannerHtml(node)" in src


def test_a_saved_verdict_repaints_the_tile():
    # imgRefreshNodes only repaints on a status/variant-count/cg fingerprint,
    # none of which a verdict changes — so without the explicit repaint the
    # button never lights up and the operator cannot tell saved from lost.
    src = _index_src()
    handler = src[src.index("async function imgSetVariantVerdict"):]
    handler = handler[:handler.index("async function imgGenerate")]
    assert "imgVerdictHistoryAfterVerdict(nodeId)" in handler
    assert "imgRenderNodeDetail(imgState.nodesById[nodeId])" in handler


def test_there_is_a_way_in():
    src = _index_src()
    assert "imgStartVerdictHistory()" in src
    assert "Label history" in src
