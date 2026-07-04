"""v821 — active_dialogue_line picks the SPOKEN line (B when B rendered).

Prefers importing the real function from main. In this sandbox `import main`
raises a pre-existing Starlette `on_startup` TypeError (unrelated to our code),
so on that known failure we load the ACTUAL function source out of main.py via
ast and exec it — the exact same code the platform runs, not a copy.
"""
import ast
import os


def _load_active_dialogue_line():
    try:
        from main import active_dialogue_line as fn  # real import path
        return fn
    except TypeError as e:
        # Known sandbox-only failure: Router.__init__() got 'on_startup'.
        if "on_startup" not in str(e):
            raise
    # Fallback: extract the genuine function definition from main.py source.
    main_path = os.path.join(os.path.dirname(__file__), "..", "main.py")
    with open(main_path, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename="main.py")
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "active_dialogue_line":
            mod = ast.Module(body=[node], type_ignores=[])
            ns = {}
            exec(compile(mod, "main.py", "exec"), ns)  # exec the REAL source
            return ns["active_dialogue_line"]
    raise AssertionError("active_dialogue_line not found in main.py")


active_dialogue_line = _load_active_dialogue_line()


def test_variant_a_uses_a_line():
    assert active_dialogue_line({"dialogue_text": "A line", "dialogue_text_b": "B line", "rendered_prompt_variant": "A"}) == "A line"


def test_variant_b_uses_b_line():
    assert active_dialogue_line({"dialogue_text": "A line", "dialogue_text_b": "B line", "rendered_prompt_variant": "B"}) == "B line"


def test_variant_b_missing_bline_falls_back_to_a():
    assert active_dialogue_line({"dialogue_text": "A line", "dialogue_text_b": None, "rendered_prompt_variant": "B"}) == "A line"


def test_missing_variant_defaults_a():
    assert active_dialogue_line({"dialogue_text": "A line"}) == "A line"


def test_variant_b_whitespace_bline_falls_back_to_a():
    assert active_dialogue_line({"dialogue_text": "A line", "dialogue_text_b": "   ", "rendered_prompt_variant": "B"}) == "A line"
