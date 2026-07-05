import importlib.util, os

spec = importlib.util.spec_from_file_location("fw", os.path.join("static", "flow_worker.py"))
fw = importlib.util.module_from_spec(spec); spec.loader.exec_module(fw)


def _r(c):
    # _PROMPT_B_TRIED is a dict (clip_id -> True); _CLIP_PROMPT_B is a dict too.
    fw._CLIP_PROMPT_B.pop(c, None)
    fw._PROMPT_B_TRIED.pop(c, None)


def test_untried_b_retry():
    _r("a"); fw._CLIP_PROMPT_B["a"] = "B"
    assert fw.prominent_promptb_decision("a") == "retry_prompt_b"
    # side effect: marks the clip as Prompt-B-tried so the NEXT trip is terminal
    assert "a" in fw._PROMPT_B_TRIED


def test_tried_b_terminal_line():
    _r("b"); fw._CLIP_PROMPT_B["b"] = "B"; fw._PROMPT_B_TRIED["b"] = True
    assert fw.prominent_promptb_decision("b") == "terminal_line"


def test_no_b_terminal_image():
    _r("c")
    assert fw.prominent_promptb_decision("c") == "terminal_image"


def test_none_clip_terminal_image():
    assert fw.prominent_promptb_decision(None) == "terminal_image"


def test_handle_terminal_reject_requeue_write_failure_rolls_back():
    # v821b hardening: if the requeue write (update_clip_status) fails, the clip must
    # NOT be stranded marked-tried-but-not-queued. handle_terminal_reject rolls back
    # _PROMPT_B_TRIED and falls to the terminal card (returns 'terminal').
    _r("d"); fw._CLIP_PROMPT_B["d"] = "B"
    _orig_ucs, _orig_rtcr = fw.update_clip_status, fw.route_terminal_content_reject
    _carded = {}
    fw.update_clip_status = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("db down"))
    fw.route_terminal_content_reject = lambda cid, reason, account_name="": _carded.__setitem__(cid, reason)
    try:
        result = fw.handle_terminal_reject("d", "PROMINENT_PEOPLE", job_id="j", clip_index=0)
    finally:
        fw.update_clip_status, fw.route_terminal_content_reject = _orig_ucs, _orig_rtcr
    assert result == "terminal"          # not stranded — fell to the card
    assert "d" not in fw._PROMPT_B_TRIED  # tried-mark rolled back
    assert "d" in _carded                 # replace-image card shown


if __name__ == "__main__":
    test_untried_b_retry()
    test_tried_b_terminal_line()
    test_no_b_terminal_image()
    test_none_clip_terminal_image()
    test_handle_terminal_reject_requeue_write_failure_rolls_back()
    print("ALL PASS")
