# tests/test_variant_operator_verdict.py
#
# v940 — the operator's three-way review split on image variants.
#
# Choosing a variant only ever recorded which one was BEST. It said nothing
# about the ones left behind, so every reader of that history had to treat
# "not chosen" as "bad" — and among four variants some genuinely are bad while
# others are perfectly usable. These tests guard the column, the endpoint and
# the two places a stale verdict could survive.
#
# v886.3's verdict tags remain data capture only. v963 adds a separate,
# explicit qc_auto choice path, guarded again by the server; tagging a verdict
# still may not select, render, or promote anything.

import inspect
import json
import types

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import image_platform as ip
from image_qc_contract import build_input_snapshot, snapshot_digest, RUBRIC_VERSION


def _session():
    eng = create_engine("sqlite:///:memory:")
    ip.ImageNode.__table__.metadata.create_all(eng)
    return sessionmaker(bind=eng)()


def _user(user_id="u1"):
    return types.SimpleNamespace(id=user_id)


def _node(db, node_id=1, user_id="u1", n_variants=2, status="ready"):
    """One ready node with n variants, ids 100+i. Returns the node."""
    n = ip.ImageNode(
        id=node_id, user_id=user_id, kind="generated",
        name=f"node {node_id}", prompt="a prompt", status=status,
    )
    db.add(n)
    for i in range(1, n_variants + 1):
        db.add(ip.ImageVariant(
            id=node_id * 100 + i, node_id=node_id, variant_index=i,
            image_path=f"nodes/{node_id}/variant_{i}.png",
        ))
    db.commit()
    return n


def _set(db, node_id, variant_id, verdict, user_id="u1"):
    return ip.set_variant_verdict(
        node_id, variant_id,
        ip.VariantVerdictRequest(verdict=verdict),
        db=db, current_user=_user(user_id),
    )


def _variant(db, variant_id):
    return db.query(ip.ImageVariant).filter(ip.ImageVariant.id == variant_id).first()


# --- storage -------------------------------------------------------------

def test_variant_has_verdict_columns():
    cols = ip.ImageVariant.__table__.c
    assert "operator_verdict" in cols
    assert "verdict_at" in cols


def test_image_node_exposes_choice_source():
    cols = ip.ImageNode.__table__.c
    assert "choice_source" in cols
    node = ip.ImageNode(kind="generated", status="ready")
    assert node.to_dict()["choice_source"] is None


def test_untagged_variant_is_null_not_empty_string():
    # NULL is a THIRD state ("the operator never said"), distinct from both
    # verdicts. Defaulting it to '' or to 'rejected' would recreate the exact
    # bug this feature exists to kill.
    v = ip.ImageVariant(node_id=1, variant_index=1, image_path="p.png")
    assert v.operator_verdict is None
    assert v.to_dict()["operator_verdict"] is None
    assert v.to_dict()["verdict_at"] is None


def test_to_dict_emits_the_verdict():
    v = ip.ImageVariant(node_id=1, variant_index=1, image_path="p.png",
                        operator_verdict="still_good")
    assert v.to_dict()["operator_verdict"] == "still_good"


def test_only_two_verdicts_exist():
    # The pick is deliberately NOT in here — it is already recorded by
    # ImageNode.chosen_variant_id and two sources could disagree.
    assert ip.VARIANT_VERDICTS == ("still_good", "rejected")


# --- endpoint: the happy paths -------------------------------------------

@pytest.mark.parametrize("verdict", ["still_good", "rejected"])
def test_set_verdict_stores_it(verdict):
    db = _session()
    _node(db)
    out = _set(db, 1, 101, verdict)
    assert out["ok"] is True
    assert out["verdict"] == verdict
    v = _variant(db, 101)
    assert v.operator_verdict == verdict
    assert v.verdict_at is not None


def test_set_verdict_does_not_choose_anything():
    # The whole point of v886.3 — tagging must never move the pick.
    db = _session()
    node = _node(db)
    _set(db, 1, 101, "still_good")
    _set(db, 1, 102, "rejected")
    db.refresh(node)
    assert node.chosen_variant_id is None
    assert node.status == "ready"


def test_verdict_is_per_variant_not_per_node():
    db = _session()
    _node(db)
    _set(db, 1, 101, "still_good")
    _set(db, 1, 102, "rejected")
    assert _variant(db, 101).operator_verdict == "still_good"
    assert _variant(db, 102).operator_verdict == "rejected"


def test_verdict_overwrites_rather_than_stacking():
    db = _session()
    _node(db)
    _set(db, 1, 101, "still_good")
    _set(db, 1, 101, "rejected")
    assert _variant(db, 101).operator_verdict == "rejected"


def test_null_clears_the_verdict():
    db = _session()
    _node(db)
    _set(db, 1, 101, "rejected")
    out = _set(db, 1, 101, None)
    assert out["verdict"] is None
    v = _variant(db, 101)
    assert v.operator_verdict is None
    # The timestamp goes too — a cleared tag must not leave a time behind
    # suggesting someone said something.
    assert v.verdict_at is None


# --- endpoint: the validation ladder -------------------------------------

def test_unknown_node_404():
    db = _session()
    _node(db)
    with pytest.raises(HTTPException) as e:
        _set(db, 999, 101, "rejected")
    assert e.value.status_code == 404


def test_another_users_node_404():
    db = _session()
    _node(db)
    with pytest.raises(HTTPException) as e:
        _set(db, 1, 101, "rejected", user_id="u2")
    assert e.value.status_code == 404


def test_variant_belonging_to_another_node_is_refused():
    # The failure this guards: node 1 tagging node 2's variant would file the
    # verdict under the wrong image entirely.
    db = _session()
    _node(db, node_id=1)
    _node(db, node_id=2)
    with pytest.raises(HTTPException) as e:
        _set(db, 1, 201, "rejected")   # 201 belongs to node 2
    assert e.value.status_code == 404
    assert _variant(db, 201).operator_verdict is None


def test_unknown_variant_404():
    db = _session()
    _node(db)
    with pytest.raises(HTTPException) as e:
        _set(db, 1, 55555, "rejected")
    assert e.value.status_code == 404


# Blank strings are NOT in this list — they normalize to a clear, which is
# covered by test_blank_string_reads_as_a_clear below.
@pytest.mark.parametrize("bad", ["good", "picked", "chosen", "still good", "1"])
def test_bad_verdict_value_422(bad):
    db = _session()
    _node(db)
    with pytest.raises(HTTPException) as e:
        _set(db, 1, 101, bad)
    assert e.value.status_code == 422
    # The message has to name the allowed words — a bare "invalid" leaves the
    # caller guessing.
    assert "still_good" in e.value.detail
    assert _variant(db, 101).operator_verdict is None


@pytest.mark.parametrize("shouty", ["REJECTED", "Still_Good", " rejected "])
def test_verdict_value_is_normalized_not_rejected(shouty):
    # Case and stray spaces are the operator's tooling being sloppy, not a
    # different verdict. Normalize rather than 422.
    db = _session()
    _node(db)
    _set(db, 1, 101, shouty)
    assert _variant(db, 101).operator_verdict == shouty.strip().lower()


@pytest.mark.parametrize("blank", ["", "   "])
def test_blank_string_reads_as_a_clear(blank):
    db = _session()
    _node(db)
    _set(db, 1, 101, "rejected")
    out = _set(db, 1, 101, blank)
    assert out["verdict"] is None
    assert _variant(db, 101).operator_verdict is None


def test_verdict_on_the_chosen_variant_422():
    db = _session()
    node = _node(db)
    node.chosen_variant_id = 101
    db.commit()
    with pytest.raises(HTTPException) as e:
        _set(db, 1, 101, "rejected")
    assert e.value.status_code == 422
    assert "unchoose" in e.value.detail
    assert _variant(db, 101).operator_verdict is None


def test_clearing_on_the_chosen_variant_is_still_allowed():
    # A label you cannot remove is worse than no label: if a verdict ever gets
    # stranded on a chosen row, the operator must still be able to drop it.
    db = _session()
    node = _node(db)
    _variant(db, 101).operator_verdict = "rejected"
    node.chosen_variant_id = 101
    db.commit()
    out = _set(db, 1, 101, None)
    assert out["verdict"] is None
    assert _variant(db, 101).operator_verdict is None


def test_tagging_a_sibling_of_the_chosen_variant_is_fine():
    db = _session()
    node = _node(db)
    node.chosen_variant_id = 101
    db.commit()
    _set(db, 1, 102, "rejected")
    assert _variant(db, 102).operator_verdict == "rejected"


# --- invalidation --------------------------------------------------------

def test_choosing_a_variant_clears_its_verdict():
    # Choosing is the strongest statement the operator can make; a row that is
    # both chosen and 'rejected' is a contradiction the ledger cannot read.
    db = _session()
    _node(db)
    _set(db, 1, 101, "rejected")
    ip.choose_variant(
        1, ip.ChooseVariantRequest(variant_id=101),
        db=db, current_user=_user(),
    )
    v = _variant(db, 101)
    assert v.operator_verdict is None
    assert v.verdict_at is None


def test_operator_choose_defaults_choice_source_to_operator():
    db = _session()
    _node(db)
    ip.choose_variant(
        1, ip.ChooseVariantRequest(variant_id=101),
        db=db, current_user=_user(),
    )
    assert _node_from_id(db, 1).choice_source == "operator"


def _node_from_id(db, node_id):
    return db.query(ip.ImageNode).filter(ip.ImageNode.id == node_id).one()


def test_qc_auto_refuses_a_missing_or_invalid_report():
    db = _session()
    _node(db)
    with pytest.raises(HTTPException) as exc:
        ip.choose_variant(
            1, ip.ChooseVariantRequest(variant_id=101, source="qc_auto"),
            db=db, current_user=_user(),
        )
    assert exc.value.status_code == 422
    assert _node_from_id(db, 1).chosen_variant_id is None


def test_qc_auto_refuses_an_already_chosen_node():
    db = _session()
    node = _node(db)
    node.chosen_variant_id = 101
    db.commit()
    with pytest.raises(HTTPException) as exc:
        ip.choose_variant(
            1, ip.ChooseVariantRequest(variant_id=102, source="qc_auto"),
            db=db, current_user=_user(),
        )
    assert exc.value.status_code == 422


def test_qc_auto_refuses_legacy_report_even_if_legacy_decider_would_choose(monkeypatch):
    import image_qc
    db = _session()
    node = _node(db)
    node.qc_json = '{"version": 1}'
    db.commit()
    monkeypatch.setattr(
        image_qc, "decide_auto_choice",
        lambda current, report: {"decision": "choose", "variant_id": 102},
    )
    with pytest.raises(HTTPException) as exc:
        ip.choose_variant(
            1, ip.ChooseVariantRequest(variant_id=102, source="qc_auto"),
            db=db, current_user=_user(),
        )
    assert exc.value.status_code == 422
    assert _node_from_id(db, 1).chosen_variant_id is None


def test_update_node_noop_keeps_qc_but_prompt_edit_clears_it():
    db = _session()
    node = _node(db)
    node.qc_json = '{"version": 1}'
    db.commit()
    ip.update_node(1, ip.UpdateNodeRequest(prompt="a prompt"),
                   db=db, current_user=_user())
    assert _node_from_id(db, 1).qc_json == '{"version": 1}'
    ip.update_node(1, ip.UpdateNodeRequest(prompt="a changed prompt"),
                   db=db, current_user=_user())
    assert _node_from_id(db, 1).qc_json is None


def test_update_node_parent_edit_clears_qc():
    db = _session()
    node = _node(db)
    parent = ip.ImageNode(id=50, user_id="u1", kind="upload", status="ready")
    db.add(parent)
    node.qc_json = '{"version": 1}'
    db.commit()
    ip.update_node(
        1,
        ip.UpdateNodeRequest(parents=[ip.ParentRef(
            parent_node_id=50, kind="character")]),
        db=db, current_user=_user(),
    )
    assert _node_from_id(db, 1).qc_json is None


def test_choosing_leaves_the_other_variants_verdicts_alone():
    # Only the newly chosen row is contradicted. The siblings' verdicts are
    # exactly the data this feature exists to collect — losing them on every
    # pick would make the whole column empty in practice.
    db = _session()
    _node(db)
    _set(db, 1, 102, "rejected")
    ip.choose_variant(
        1, ip.ChooseVariantRequest(variant_id=101),
        db=db, current_user=_user(),
    )
    assert _variant(db, 102).operator_verdict == "rejected"


def test_clear_verdict_helper_drops_both_columns():
    v = ip.ImageVariant(node_id=1, variant_index=1, image_path="p.png",
                        operator_verdict="rejected")
    v.verdict_at = ip.datetime.utcnow()
    ip._clear_verdict(v)
    assert v.operator_verdict is None
    assert v.verdict_at is None


# --- migrations ----------------------------------------------------------

def _migration_halves():
    """Split run_image_platform_migrations' source at the postgres list, so a
    SQLite-only entry can actually be detected. That is the failure this guards:
    production runs Postgres, so a column added only to the SQLite list never
    exists live and every write to it 500s."""
    src = inspect.getsource(ip.run_image_platform_migrations)
    marker = "postgres_migrations = ["
    assert marker in src
    head, tail = src.split(marker, 1)
    return head, tail


@pytest.mark.parametrize("col", ["operator_verdict", "verdict_at"])
def test_verdict_columns_are_in_the_sqlite_migration_list(col):
    sqlite_half, _ = _migration_halves()
    assert f"ALTER TABLE image_variants ADD COLUMN {col}" in sqlite_half


def test_choice_source_is_in_both_migration_lists():
    sqlite_half, postgres_half = _migration_halves()
    assert "ALTER TABLE image_nodes ADD COLUMN choice_source" in sqlite_half
    assert "ALTER TABLE image_nodes ADD COLUMN IF NOT EXISTS choice_source" in postgres_half


@pytest.mark.parametrize("col", ["operator_verdict", "verdict_at"])
def test_verdict_columns_are_in_the_postgres_migration_list(col):
    _, postgres_half = _migration_halves()
    assert f"ALTER TABLE image_variants ADD COLUMN IF NOT EXISTS {col}" in postgres_half


def test_qc_auto_strict_snapshot_accepts_then_rejects_changed_parent_bytes(
        monkeypatch, tmp_path):
    """The server rechecks the actual product parent before committing."""
    db = _session()
    root = tmp_path / "images"
    (root / "nodes" / "1").mkdir(parents=True)
    (root / "nodes" / "50").mkdir(parents=True)
    candidate_a = root / "nodes" / "1" / "variant_1.png"
    candidate_b = root / "nodes" / "1" / "variant_2.png"
    parent_path = root / "nodes" / "50" / "variant_1.png"
    candidate_a.write_bytes(b"candidate-a")
    candidate_b.write_bytes(b"candidate-b")
    parent_path.write_bytes(b"product-parent-v1")
    monkeypatch.setattr(ip, "images_root", lambda: root)

    parent = ip.ImageNode(id=50, user_id="u1", kind="upload", status="ready",
                          origin="manual", chosen_variant_id=501)
    parent_variant = ip.ImageVariant(id=501, node_id=50, variant_index=1,
                                     image_path="nodes/50/variant_1.png",
                                     source="manual")
    child = ip.ImageNode(id=1, user_id="u1", kind="generated", status="ready",
                         prompt="a product jar on a counter", n_variants=2)
    child_a = ip.ImageVariant(id=101, node_id=1, variant_index=1,
                              image_path="nodes/1/variant_1.png", source="ai")
    child_b = ip.ImageVariant(id=102, node_id=1, variant_index=2,
                              image_path="nodes/1/variant_2.png", source="ai")
    edge = ip.ImageEdge(id=700, parent_node_id=50, child_node_id=1,
                        role="product", kind="product", slot_order=0,
                        origin="manual")
    db.add_all([parent, parent_variant, child, child_a, child_b, edge])
    db.commit()
    db.refresh(child)

    node_payload = child.to_dict()
    edge_payload = node_payload["parents"]
    parent_payload = parent.to_dict(include_variants=False)
    candidates = [child_a.to_dict(), child_b.to_dict()]
    snap = build_input_snapshot(
        node_payload, candidates=candidates, edges=edge_payload,
        parents=[parent_payload],
        candidate_bytes={101: b"candidate-a", 102: b"candidate-b"},
        parent_bytes={501: b"product-parent-v1"},
    )
    digest = snapshot_digest(snap)

    def read(variant_id, status):
        return {
            "variant_id": variant_id, "person_count": "one",
            "prompt_status": status,
            "required_elements_visible": True,
            "element_misses": [], "warnings": [], "hard_fail_codes": [],
            "references": [{
                "edge_id": 700, "parent_variant_id": 501,
                "status": "pass", "observable": True, "fail_codes": [],
                "observed_hero_text": "KORELLA",
            }],
        }

    report = {
        "version": 1, "auto_contract_version": 1,
        "rubric_version": RUBRIC_VERSION, "input_snapshot": snap,
        "snapshot_digest": digest,
        "variants": {"101": {"integrity": {"ok": True}},
                     "102": {"integrity": {"ok": True}}},
        "reads": [{"read_id": 1, "snapshot_digest": digest,
                   "candidates": [read(101, "pass"), read(102, "fail")]},
                  {"read_id": 2, "snapshot_digest": digest,
                   "candidates": [read(101, "pass"), read(102, "fail")]}],
    }
    child.qc_json = json.dumps(report)
    db.commit()

    out = ip.choose_variant(
        1, ip.ChooseVariantRequest(variant_id=101, source="qc_auto"),
        db=db, current_user=_user(),
    )
    assert out["chosen_variant_id"] == 101

    # Re-open the child for a second auto attempt, then alter only the actual
    # parent bytes. The stale report must not be allowed to choose again.
    child = _node_from_id(db, 1)
    child.chosen_variant_id = None
    child.choice_source = None
    db.commit()
    parent_path.write_bytes(b"product-parent-v2")
    with pytest.raises(HTTPException) as exc:
        ip.choose_variant(
            1, ip.ChooseVariantRequest(variant_id=101, source="qc_auto"),
            db=db, current_user=_user(),
        )
    assert exc.value.status_code == 422
    assert _node_from_id(db, 1).chosen_variant_id is None


# --- shared generation/choice locking ------------------------------------

def _add_parent_edges(db, child, parent_ids=(20, 50)):
    """Attach deliberately out-of-order parent ids for lock-order tests."""
    for parent_id in parent_ids:
        parent = ip.ImageNode(
            id=parent_id, user_id=child.user_id, kind="upload", status="ready",
            name=f"parent {parent_id}",
        )
        db.add(parent)
        db.add(ip.ImageEdge(
            parent_node_id=parent_id, child_node_id=child.id,
            role="product", kind="product", slot_order=0,
        ))
    db.commit()
    db.refresh(child)


def test_shared_lock_helper_returns_target_and_parents_in_global_id_order():
    # This suite uses SQLite :memory:, where FOR UPDATE is not enforced.  It
    # proves the exact row set and ordering; Postgres contention must still be
    # covered by the production integration environment when a test database
    # URL is available.
    db = _session()
    child = _node(db, node_id=100, status="draft")
    _add_parent_edges(db, child, parent_ids=(50, 20))

    locked, locked_by_id = ip._lock_node_and_parents(db, 100, "u1")

    assert locked.id == 100
    assert list(locked_by_id) == [20, 50, 100]


@pytest.mark.parametrize("endpoint_name", ["generate_node", "regenerate_node"])
def test_generation_endpoints_lock_before_deleting_or_resetting(
        endpoint_name, monkeypatch):
    """The shared lock must precede every destructive generation step."""
    db = _session()
    node = _node(db, status="ready")
    events = []
    original_lock = ip._lock_node_and_parents
    original_clear = ip._clear_choice

    def lock(*args, **kwargs):
        events.append("lock")
        return original_lock(*args, **kwargs)

    def clear(*args, **kwargs):
        events.append("clear_choice")
        return original_clear(*args, **kwargs)

    monkeypatch.setattr(ip, "_lock_node_and_parents", lock)
    monkeypatch.setattr(ip, "_clear_choice", clear)
    monkeypatch.setattr(ip, "_resolve_parent_image_paths", lambda *a: [])
    monkeypatch.setattr(
        ip, "_delete_variant_files", lambda *a, **k: events.append("delete"))
    monkeypatch.setattr(ip, "_seed_chatgpt_lane", lambda *a: None)
    monkeypatch.setattr(
        ip, "write_generation_job", lambda *a: events.append("queue"))

    getattr(ip, endpoint_name)(1, db=db, current_user=_user())

    assert events.index("lock") < events.index("delete")
    assert events.index("lock") < events.index("clear_choice")
    assert events.index("lock") < events.index("queue")


def test_choose_endpoint_uses_shared_lock_before_choice_mutation(monkeypatch):
    db = _session()
    node = _node(db, status="ready")
    events = []
    original_lock = ip._lock_node_and_parents
    original_commit = db.commit

    def lock(*args, **kwargs):
        events.append("lock")
        return original_lock(*args, **kwargs)

    def commit(*args, **kwargs):
        events.append("commit")
        return original_commit(*args, **kwargs)

    monkeypatch.setattr(ip, "_lock_node_and_parents", lock)
    monkeypatch.setattr(db, "commit", commit)
    ip.choose_variant(
        1, ip.ChooseVariantRequest(variant_id=101),
        db=db, current_user=_user(),
    )
    assert events[0] == "lock"
    assert events.index("lock") < events.index("commit")


# --- canonical QC serialization vs renderer-only chain jobs ---------------

def test_qc_serialization_keeps_generic_chain_role_and_explicit_chain_continuity():
    db = _session()
    generic_parent = ip.ImageNode(
        id=10, user_id="u1", kind="generated", status="ready",
        chosen_variant_id=1001,
    )
    explicit_parent = ip.ImageNode(
        id=11, user_id="u1", kind="generated", status="ready",
        chosen_variant_id=1101,
    )
    child = ip.ImageNode(
        id=1, user_id="u1", kind="generated", status="ready",
        prompt="one adult in a kitchen",
    )
    db.add_all([
        generic_parent, explicit_parent, child,
        ip.ImageEdge(id=1, parent_node_id=10, child_node_id=1,
                     role="reference", slot_order=0),
        ip.ImageEdge(id=2, parent_node_id=11, child_node_id=1,
                     role="chain_from_image_2", slot_order=1),
    ])
    db.commit()
    rows = {row["id"]: row for row in child.to_dict()["parents"]}

    assert rows[1]["reference_intent"] == "role"
    assert rows[2]["reference_intent"] == "continuity"


def test_renderer_chain_mapping_remains_positional_for_generation_only():
    assert ip._reference_intent_for_class("chain", 0) == "continuity"
    assert ip._reference_intent_for_class("chain", 1) == "body"
    assert ip._reference_intent_for_class("chain", 2) == "support"
