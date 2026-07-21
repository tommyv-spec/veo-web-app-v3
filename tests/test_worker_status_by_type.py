import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import image_platform as ip

def test_worker_kind_from_id():
    assert ip._worker_kind("chatgpt-HOSTX") == "chatgpt"
    assert ip._worker_kind("HOSTX-1234") == "flow"
    assert ip._worker_kind("") == "flow"
    assert ip._worker_kind(None) == "flow"
