import importlib.util, os
spec = importlib.util.spec_from_file_location("flow_worker", os.path.join("static","flow_worker.py"))
fw = importlib.util.module_from_spec(spec); spec.loader.exec_module(fw)

# fail_clip_general_policy hits the platform API; in a bare test run there is no
# server, so no-op it and just record the terminal-fail message. This keeps the
# terminal tests deterministic (we assert the RETURNED action, not the API side).
_FAILED = {}
def _noop_fail(cid, msg):
    _FAILED[cid] = msg
fw.fail_clip_general_policy = _noop_fail

def _reset(cid):
    fw._CLIP_PROMPT_B.pop(cid, None); fw._PROMPT_B_TRIED.pop(cid, None); _FAILED.pop(cid, None)

def test_prominent_with_untried_b_retries_b():
    cid="c1"; _reset(cid); fw._CLIP_PROMPT_B[cid]="reworded B text"
    action,_ = fw.route_generation_policy(cid,"omni",is_prominent=True)
    assert action=="retry_prompt_b"
    assert fw._PROMPT_B_TRIED.get(cid) is True

def test_prominent_after_b_is_terminal():
    cid="c2"; _reset(cid); fw._CLIP_PROMPT_B[cid]="reworded B text"; fw._PROMPT_B_TRIED[cid]=True
    action,_ = fw.route_generation_policy(cid,"omni",is_prominent=True)
    assert action in ("fail_terminal","fail")
    assert cid in _FAILED

def test_prominent_no_b_is_terminal():
    cid="c3"; _reset(cid)
    action,_ = fw.route_generation_policy(cid,"omni",is_prominent=True)
    assert action in ("fail_terminal","fail")
    assert cid in _FAILED
