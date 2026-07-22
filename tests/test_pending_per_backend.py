import types
import image_platform as ip

class FakeNode:
    def __init__(self, id, status="queued", cg_status=None, chain=False):
        self.id = id; self.status = status; self.cg_status = cg_status
        self.parent_edges = ([types.SimpleNamespace(kind=None, role="variant_chain:1")]
                             if chain else [])

def test_chatgpt_lane_selects_cg_queued_base_node():
    base = FakeNode(1, status="ready", cg_status="queued")
    dep = FakeNode(2, status="queued", cg_status=None, chain=True)
    assert ip._select_for_backend([dep, base], "chatgpt") is base

def test_chatgpt_lane_skips_node_without_cg_queued():
    n = FakeNode(1, status="queued", cg_status=None)
    assert ip._select_for_backend([n], "chatgpt") is None

def test_banana_lane_selects_status_queued():
    n = FakeNode(1, status="queued", cg_status="ready")
    assert ip._select_for_backend([n], "banana") is n

def test_banana_lane_ignores_cg_only_node():
    n = FakeNode(1, status="ready", cg_status="queued")
    assert ip._select_for_backend([n], "banana") is None
