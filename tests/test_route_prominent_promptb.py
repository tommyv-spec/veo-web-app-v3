import importlib.util, os
spec = importlib.util.spec_from_file_location("flow_worker", os.path.join("static","flow_worker.py"))
fw = importlib.util.module_from_spec(spec); spec.loader.exec_module(fw)

# fail_clip_general_policy AND route_terminal_content_reject both hit the platform
# API; in a bare test run there is no server, so no-op both and record which one
# fired. v821b: "B tried, still trips" -> fail_clip_general_policy (rework-line
# terminal); "no Prompt B" (silent/no line) -> route_terminal_content_reject
# (replace-image card, the old behavior). Assert the RETURNED action + which path.
_FAILED = {}
_REJECTED = {}
def _noop_fail(cid, msg):
    _FAILED[cid] = msg
def _noop_reject(cid, reason, account_name=""):
    _REJECTED[cid] = reason
    return ('fail_terminal', 'replace-image card')
fw.fail_clip_general_policy = _noop_fail
fw.route_terminal_content_reject = _noop_reject

def _reset(cid):
    fw._CLIP_PROMPT_B.pop(cid, None); fw._PROMPT_B_TRIED.pop(cid, None)
    _FAILED.pop(cid, None); _REJECTED.pop(cid, None)

def test_prominent_with_untried_b_retries_b():
    cid="c1"; _reset(cid); fw._CLIP_PROMPT_B[cid]="reworded B text"
    action,_ = fw.route_generation_policy(cid,"omni",is_prominent=True)
    assert action=="retry_prompt_b"
    assert fw._PROMPT_B_TRIED.get(cid) is True

def test_prominent_after_b_is_terminal():
    cid="c2"; _reset(cid); fw._CLIP_PROMPT_B[cid]="reworded B text"; fw._PROMPT_B_TRIED[cid]=True
    action,_ = fw.route_generation_policy(cid,"omni",is_prominent=True)
    assert action == "fail_terminal"
    assert cid in _FAILED

def test_prominent_no_b_falls_back_to_replace_image():
    # v821b — no Prompt B (silent / no line): must NOT get the "rework the line"
    # message; falls back to the replace-image card (route_terminal_content_reject).
    cid="c3"; _reset(cid)
    action,_ = fw.route_generation_policy(cid,"omni",is_prominent=True)
    assert action == "fail_terminal"
    assert cid in _REJECTED      # replace-image card path
    assert cid not in _FAILED    # NOT the rework-line terminal
