import image_platform as ip

def test_node_has_cg_lane_columns():
    cols = ip.ImageNode.__table__.c
    assert "cg_status" in cols
    assert "cg_claimed_by" in cols
    assert "cg_claimed_at" in cols

def test_node_to_dict_emits_cg_status():
    n = ip.ImageNode(name="x", prompt="p", cg_status="queued")
    assert n.to_dict(include_variants=False)["cg_status"] == "queued"

def test_node_to_dict_cg_status_defaults_none():
    n = ip.ImageNode(name="x", prompt="p")
    assert n.to_dict(include_variants=False)["cg_status"] is None
