from verify_video_format import lint_promptb_gate

A = 'IMMEDIATE ACTION: x. He says ... (American accent): "your soldier won\'t wake up."'


def _clip(a_prompt, b_prompt, a_line="your soldier won't wake up.", b_line=None):
    return {"a_prompt": a_prompt, "a_line": a_line, "b_prompt": b_prompt, "b_line": b_line}


def test_missing_b_fails():
    assert any("Prompt B missing" in e for e in lint_promptb_gate([_clip(A, None)]))


def test_identical_line_fails():
    errs = lint_promptb_gate([_clip(A, A, b_line="your soldier won't wake up.")])
    assert any("identical" in e for e in errs)


def test_body_differs_fails():
    b = A.replace("IMMEDIATE ACTION: x", "IMMEDIATE ACTION: DIFFERENT").replace("your soldier won't wake up.", "your drive isn't the same.")
    errs = lint_promptb_gate([_clip(A, b, b_line="your drive isn't the same.")])
    assert any("body must match" in e for e in errs)


def test_valid_passes():
    b = A.replace("your soldier won't wake up.", "your drive isn't the same.")
    errs = lint_promptb_gate([_clip(A, b, b_line="your drive isn't the same.")])
    assert errs == []


def test_silent_clip_skipped():
    assert lint_promptb_gate([{"a_prompt": None, "a_line": None, "b_prompt": None, "b_line": None}]) == []
