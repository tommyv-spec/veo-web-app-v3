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
# The hard rule the whole feature rests on (v886.3): this is DATA CAPTURE
# ONLY. Nothing here may select, render, or promote anything.

import inspect
import types

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import image_platform as ip


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


@pytest.mark.parametrize("col", ["operator_verdict", "verdict_at"])
def test_verdict_columns_are_in_the_postgres_migration_list(col):
    _, postgres_half = _migration_halves()
    assert f"ALTER TABLE image_variants ADD COLUMN IF NOT EXISTS {col}" in postgres_half
