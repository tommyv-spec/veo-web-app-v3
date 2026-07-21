import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import image_platform as ip

class _Edge:
    def __init__(self, kind=None, role=""):
        self.kind = kind; self.role = role

class _Node:
    def __init__(self, edges):
        self.parent_edges = edges

def test_node_with_no_edges_is_base():
    assert ip._node_has_chain_dependency(_Node([])) is False

def test_node_with_only_upload_edges_is_base():
    n = _Node([_Edge(kind="persona"), _Edge(kind="product")])
    assert ip._node_has_chain_dependency(n) is False

def test_node_with_chain_edge_is_dependent():
    n = _Node([_Edge(kind=None, role="variant_chain:image_3")])
    assert ip._node_has_chain_dependency(n) is True

def test_node_with_chain_from_image_role_is_dependent():
    n = _Node([_Edge(kind=None, role="chain_from_image_2")])
    assert ip._node_has_chain_dependency(n) is True

def test_mixed_upload_plus_chain_is_dependent():
    n = _Node([_Edge(kind="persona"), _Edge(kind=None, role="variant_chain:image_1")])
    assert ip._node_has_chain_dependency(n) is True
