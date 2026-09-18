import image_platform as ip
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def _node(chain=False):
    n = ip.ImageNode(name="x", prompt="p")
    if chain:
        # kind NULL + variant_chain role = a chain (dependent) edge
        e = ip.ImageEdge(kind=None, role="variant_chain:1")
    else:
        # kind set (persona upload) = NOT a chain -> base node
        e = ip.ImageEdge(kind="persona", role="")
    # append via the mapped relationship (SimpleNamespace fails _sa_instance_state)
    n.parent_edges.append(e)
    return n


def test_seed_sets_queued_on_base_node():
    n = _node(chain=False)
    ip._seed_chatgpt_lane(n)
    assert n.cg_status == "queued"


def test_seed_queues_dependent_node_for_chatgpt_too():
    n = _node(chain=True)
    ip._seed_chatgpt_lane(n)
    assert n.cg_status == "queued"


def test_seed_is_idempotent_when_already_ready():
    n = _node(chain=False)
    n.cg_status = "ready"
    ip._seed_chatgpt_lane(n)
    assert n.cg_status == "ready"  # don't clobber a completed lane


def test_parent_choice_promotes_chain_child_to_both_lanes(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    ip.ImageNode.__table__.metadata.create_all(engine)
    db = sessionmaker(bind=engine)()
    parent = ip.ImageNode(
        id=1, user_id="u1", kind="generated", name="parent",
        prompt="p", status="ready",
    )
    child = ip.ImageNode(
        id=2, user_id="u1", kind="generated", name="child",
        prompt="p", status="draft",
    )
    chosen = ip.ImageVariant(
        id=11, node_id=1, variant_index=1,
        image_path="nodes/1/variant_1.png",
    )
    parent.chosen_variant_id = 11
    db.add_all([parent, child, chosen])
    db.add(ip.ImageEdge(
        parent_node_id=1, child_node_id=2,
        role="chain_from_image_1", slot_order=0,
    ))
    db.commit()

    written = []
    monkeypatch.setattr(
        ip, "write_generation_job",
        lambda _db, node: written.append(node.id),
    )

    ip._promote_ready_children(db, 1)

    db.refresh(child)
    assert child.status == "queued"
    assert child.cg_status == "queued"
    assert written == [2]
