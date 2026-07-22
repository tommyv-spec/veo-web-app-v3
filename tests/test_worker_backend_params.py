import importlib
# import from the static/ dir; adjust if the repo runs tests from code/
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "static"))
import chatgpt_http_pull as cp

def test_pull_module_declares_chatgpt_backend():
    # The module must expose the backend it claims/uploads/completes under.
    assert cp.WORKER_BACKEND == "chatgpt"
