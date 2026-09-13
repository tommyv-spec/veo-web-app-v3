"""v1000 -- a scoped launch renders named clips with their Prompt B.

Two clips of job 0d456c24 were refused on Prompt A and parked for a Prompt-B redo
the age-capped redo lane never serves; a firstgen launch re-sent Prompt A. Now
FLOW_PROMPT_B_CLIPS (set by run_scoped_flow_worker.py --prompt-b) names the
clips whose prompt_b is used. Checked by AST, plus the helper run against fakes.
"""
import ast
import os
import pathlib

_STATIC = pathlib.Path(__file__).parent / "static"
_PATH = pathlib.Path(os.environ.get("FLOW_WORKER_PATH") or (_STATIC / "flow_worker.py"))


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


def test_applying_prompt_b_marks_it_tried():
    """v1000.1 -- the ladder's marker is set when B is handed to the composer, so a
    refused B fails with its reason instead of earning a second B retry."""
    fn = _fn(_tree(), "_v1000_prompt_for")
    mod = ast.Module(body=[fn], type_ignores=[])
    ast.fix_missing_locations(mod)
    tried = {}
    ns = {"_V1000_PROMPT_B_CLIPS": {"14908"}, "_PROMPT_B_TRIED": tried}
    exec(compile(mod, "<v1000.1>", "exec"), ns)
    assert ns["_v1000_prompt_for"]({"id": 14908, "clip_index": 1, "prompt_b": "B"}, "A") == "B"
    assert tried.get(14908) is True
    assert ns["_v1000_prompt_for"]({"id": 14934, "clip_index": 27, "prompt_b": "B"}, "A") == "A"   # unnamed: untouched, unmarked
    assert 14934 not in tried


def _load(env_ids):
    fn = _fn(_tree(), "_v1000_prompt_for")
    mod = ast.Module(body=[fn], type_ignores=[])
    ast.fix_missing_locations(mod)
    ns = {"_V1000_PROMPT_B_CLIPS": set(env_ids)}
    exec(compile(mod, "<v1000>", "exec"), ns)
    return ns["_v1000_prompt_for"]


def test_named_clip_with_prompt_b_gets_it(capsys):
    f = _load({"14908"})
    clip = {"id": 14908, "clip_index": 1, "prompt": "A", "prompt_b": "B reworded"}
    assert f(clip, "A") == "B reworded"
    assert "[v1000]" in capsys.readouterr().out


def test_named_clip_without_prompt_b_keeps_a(capsys):
    f = _load({"14908"})
    assert f({"id": 14908, "clip_index": 1, "prompt": "A", "prompt_b": ""}, "A") == "A"
    assert "has none" in capsys.readouterr().out


def test_unnamed_clip_is_untouched_even_with_a_prompt_b():
    f = _load({"14908"})
    assert f({"id": 14934, "clip_index": 27, "prompt": "A", "prompt_b": "B"}, "A") == "A"
    assert _load(set())({"id": 14908, "prompt_b": "B"}, "A") == "A"    # nothing named -> nothing changes


def test_env_parsing_is_what_the_launcher_writes():
    tree = _tree()
    a = [n for n in ast.walk(tree) if isinstance(n, ast.Assign)
         and any(isinstance(t, ast.Name) and t.id == "_V1000_PROMPT_B_CLIPS" for t in n.targets)]
    assert len(a) == 1 and "FLOW_PROMPT_B_CLIPS" in ast.unparse(a[0].value)
