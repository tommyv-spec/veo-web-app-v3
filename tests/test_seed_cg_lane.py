import image_platform as ip


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


def test_seed_skips_dependent_node():
    n = _node(chain=True)
    ip._seed_chatgpt_lane(n)
    assert n.cg_status is None


def test_seed_is_idempotent_when_already_ready():
    n = _node(chain=False)
    n.cg_status = "ready"
    ip._seed_chatgpt_lane(n)
    assert n.cg_status == "ready"  # don't clobber a completed lane
