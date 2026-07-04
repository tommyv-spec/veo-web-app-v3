"""v821 — worker reports + platform persists rendered_prompt_variant (A/B).

Two layers:
  1. Unit-test the pure signal helper `_rendered_variant_for` in flow_worker
     (importable standalone; maps _PROMPT_B_TRIED membership -> "A"/"B").
  2. Source-grep-assert the new symbol names are present + spelled identically
     on BOTH ends (this codebase has been bitten by missing-name regressions
     that py_compile does not catch).
"""
import importlib.util
import os

_HERE = os.path.dirname(os.path.abspath(__file__))
_CODE = os.path.dirname(_HERE)
_FW = os.path.join(_CODE, "static", "flow_worker.py")
_MAIN = os.path.join(_CODE, "main.py")


def _load_fw():
    spec = importlib.util.spec_from_file_location("fw_variant_test", _FW)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


# ---- Layer 1: pure helper behaviour --------------------------------------
def test_helper_defaults_to_A_when_not_tried():
    fw = _load_fw()
    fw._PROMPT_B_TRIED.pop("clip-x", None)
    assert fw._rendered_variant_for("clip-x") == "A"


def test_helper_returns_B_when_prompt_b_tried():
    fw = _load_fw()
    fw._PROMPT_B_TRIED["clip-x"] = True
    try:
        assert fw._rendered_variant_for("clip-x") == "B"
    finally:
        fw._PROMPT_B_TRIED.pop("clip-x", None)


def test_helper_guards_missing_clip_id():
    fw = _load_fw()
    assert fw._rendered_variant_for(None) == "A"
    assert fw._rendered_variant_for("") == "A"


def test_completed_status_carries_variant_key():
    """update_clip_status must add the variant to the JSON body ONLY for
    'completed' (not e.g. 'failed'/'generating')."""
    fw = _load_fw()
    src = open(_FW, encoding="utf-8").read()
    assert "if status == 'completed':" in src
    assert 'data["rendered_prompt_variant"] = _rendered_variant_for(clip_id)' in src


# ---- Layer 2: both-ends symbol consistency -------------------------------
def test_worker_defines_and_uses_helper():
    src = open(_FW, encoding="utf-8").read()
    assert "def _rendered_variant_for(clip_id):" in src
    assert "_rendered_variant_for(clip_id)" in src


def test_main_model_accepts_field():
    src = open(_MAIN, encoding="utf-8").read()
    assert "rendered_prompt_variant: Optional[str] = None" in src


def test_main_endpoint_persists_field():
    src = open(_MAIN, encoding="utf-8").read()
    assert "if update.rendered_prompt_variant is not None:" in src
    assert "clip.rendered_prompt_variant = update.rendered_prompt_variant" in src
