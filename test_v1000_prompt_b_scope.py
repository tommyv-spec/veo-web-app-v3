"""v1000/v1001 -- a clip renders with its Prompt B when its OWN platform record asks for it.

The record is the ladder's durable v849 marker in `error_message`
("retry reworded line (Prompt B)", sent in every pending/claim payload) plus a
`prompt_b`. No env list: a job-specific instruction lives on the clip row, never
in the worker's environment (operator 2026-09-13). v1000.1: applying B marks the
clip tried so a refused B fails once with its reason. Checked by AST, plus the
helper run against fakes.
"""
import ast
import os
import pathlib

_STATIC = pathlib.Path(__file__).parent / "static"
_PATH = pathlib.Path(os.environ.get("FLOW_WORKER_PATH") or (_STATIC / "flow_worker.py"))
MARK = "retry reworded line (Prompt B)"


def _tree():
    return ast.parse(_PATH.read_text(encoding="utf-8", errors="replace"))


def _fn(tree, name):
    return next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == name)


def test_both_prompt_sites_route_through_the_helper():
    """process_job_submission reads the platform prompt in two clip-loop branches;
    both swap through the helper. v1000.1: at the second site the swap sits BELOW
    the v943.4 capture (`_cs_platform_prompt = prompt`) so that capture keeps
    Prompt A; at the first site it follows the read directly."""
    src = _PATH.read_text(encoding="utf-8", errors="replace").replace("\r\n", "\n")
    assert src.count("prompt = _v1000_prompt_for(clip, prompt)") == 2
    assert src.count("prompt = clip.get('prompt')\n        prompt = _v1000_prompt_for(clip, prompt)") == 1, \
        "the first site swaps right after the read"
    i_cap = src.index("_cs_platform_prompt = prompt")
    i_swap2 = src.index("prompt = _v1000_prompt_for(clip, prompt)", i_cap - 400)
    assert i_swap2 > i_cap, "the second site's swap must come after the v943.4 capture"


def test_no_env_decides_prompt_b_any_more():
    src = _PATH.read_text(encoding="utf-8", errors="replace")
    assert "FLOW_PROMPT_B_CLIPS" not in src, "a job-specific instruction must not be read from the environment"
    assert "_V1000_PROMPT_B_CLIPS" not in src
    fn = _fn(_tree(), "_v1000_prompt_for")
    assert "_V1001_PROMPT_B_MARKER" in ast.unparse(fn) and "error_message" in ast.unparse(fn)


def _load():
    fn = _fn(_tree(), "_v1000_prompt_for")
    mod = ast.Module(body=[fn], type_ignores=[])
    ast.fix_missing_locations(mod)
    tried = {}
    ns = {"_V1001_PROMPT_B_MARKER": MARK, "_PROMPT_B_TRIED": tried}
    exec(compile(mod, "<v1001>", "exec"), ns)
    return ns["_v1000_prompt_for"], tried


def test_marked_clip_with_prompt_b_gets_it_and_is_marked_tried(capsys):
    f, tried = _load()
    clip = {"id": 14908, "clip_index": 1, "prompt": "A", "prompt_b": "B reworded",
            "error_message": f"v992 PUBLIC_ERROR_UNSAFE_GENERATION -> {MARK}"}
    assert f(clip, "A") == "B reworded"
    assert tried.get(14908) is True
    assert "[v1000]" in capsys.readouterr().out


def test_marked_clip_without_prompt_b_keeps_a(capsys):
    f, tried = _load()
    assert f({"id": 14908, "clip_index": 1, "prompt": "A", "prompt_b": "", "error_message": MARK}, "A") == "A"
    assert "has none" in capsys.readouterr().out
    assert 14908 not in tried


def test_unmarked_clip_is_untouched_even_with_a_prompt_b():
    f, tried = _load()
    assert f({"id": 14934, "clip_index": 27, "prompt": "A", "prompt_b": "B", "error_message": ""}, "A") == "A"
    assert f({"id": 14934, "clip_index": 27, "prompt": "A", "prompt_b": "B"}, "A") == "A"           # no error_message at all
    assert f({"id": 14934, "prompt_b": "B", "error_message": "some other failure"}, "A") == "A"      # a different reason is not the marker
    assert not tried
