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


if __name__ == "__main__":
    test_untried_b_retry()
    test_tried_b_terminal_line()
    test_no_b_terminal_image()
    test_none_clip_terminal_image()
    print("ALL PASS")
