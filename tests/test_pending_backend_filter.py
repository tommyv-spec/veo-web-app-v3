import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import image_platform as ip

class _Edge:
    def __init__(self, kind=None, role=""): self.kind=kind; self.role=role
class _Node:
    def __init__(self, edges): self.parent_edges = edges

def test_chatgpt_backend_wants_only_base():
    base = _Node([_Edge(kind="persona")])
    dep = _Node([_Edge(kind=None, role="variant_chain:image_1")])
    assert ip._node_eligible_for_backend(base, "chatgpt") is True
    assert ip._node_eligible_for_backend(dep, "chatgpt") is False

def test_banana_backend_wants_everything():
    base = _Node([]); dep = _Node([_Edge(kind=None, role="chain_from_image_2")])
    assert ip._node_eligible_for_backend(base, "banana") is True
    assert ip._node_eligible_for_backend(dep, "banana") is True

def test_default_backend_is_banana_everything():
    dep = _Node([_Edge(kind=None, role="variant_chain:image_1")])
    assert ip._node_eligible_for_backend(dep, None) is True
