import verify_video_format
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


def test_lint_parse_exception_hard_fails(tmp_path, monkeypatch, capsys):
    # A raised parse exception must HARD-FAIL (non-zero exit), never warn+skip.
    md = tmp_path / "build.md"
    md.write_text("## Veo 3.1 Final Prompts\n", encoding="utf-8")

    import veo_prompt_overrides

    def _boom(_text):
        raise ValueError("boom")

    monkeypatch.setattr(veo_prompt_overrides, "parse_veo_prompts_block", _boom)
    rc = verify_video_format.lint(str(md))
    out = capsys.readouterr().out
    assert rc != 0
    assert "could not parse Veo clips" in out


def test_lint_empty_parse_passes_promptb(tmp_path, monkeypatch, capsys):
    # An EMPTY parse (build with no Veo/dialogue clips) must NOT trip v821.
    md = tmp_path / "build.md"
    md.write_text("## Veo 3.1 Final Prompts\n", encoding="utf-8")

    import veo_prompt_overrides

    monkeypatch.setattr(veo_prompt_overrides, "parse_veo_prompts_block", lambda _t: {})
    verify_video_format.lint(str(md))
    out = capsys.readouterr().out
    # No v821 error line at all (other gates may fail; v821 must be clean).
    assert "v821" not in out
