import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import image_worker

def test_flow_worker_handles_banana():
    assert image_worker._flow_handles_model({"model": "nano_banana_2"}) is True
    assert image_worker._flow_handles_model({"model": "nano_banana_pro"}) is True
    assert image_worker._flow_handles_model({}) is True  # default = banana

def test_flow_worker_handles_all_models_now():
    for m in ("nano_banana_2", "nano_banana_pro", "flow", "chatgpt", None):
        assert image_worker._flow_handles_model({"model": m}) is True
