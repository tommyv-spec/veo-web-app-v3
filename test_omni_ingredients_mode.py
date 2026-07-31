"""v881 — Omni + start&end frame runs Ingredients; everything else runs Frames."""
import importlib.util
import pathlib

_SPEC = importlib.util.spec_from_file_location(
    "flow_worker_ingredients",
    pathlib.Path(__file__).parent / "static" / "flow_worker.py",
)


def _load():
    mod = importlib.util.module_from_spec(_SPEC)
    _SPEC.loader.exec_module(mod)
    return mod


class _Page:
    """Stand-in for the Playwright page — the switch only reads attributes."""
    pass


def _page(model=None, has_end=None):
    p = _Page()
    if model is not None:
        p._veo_model = model
    if has_end is not None:
        p._clip_has_end_frame = has_end
    return p


def test_omni_with_both_frames_uses_ingredients():
    fw = _load()
    assert fw._omni_ingredients_mode(_page("Omni Flash", True)) is True
    assert fw._omni_ingredients_mode(_page("Omni Flash [Beta]", True)) is True


def test_omni_with_start_frame_only_stays_on_frames():
    fw = _load()
    assert fw._omni_ingredients_mode(_page("Omni Flash", False)) is False


def test_veo_never_uses_ingredients():
    fw = _load()
    assert fw._omni_ingredients_mode(_page("Veo 3.1 - Lite [Lower Priority]", True)) is False
    assert fw._omni_ingredients_mode(_page("Veo 3.1 - Quality", True)) is False


def test_unset_attributes_read_as_frames():
    """Older call paths that never set the flag keep the pre-v881 behavior."""
    fw = _load()
    assert fw._omni_ingredients_mode(_page()) is False
    assert fw._omni_ingredients_mode(_page("Omni Flash")) is False


def test_set_clip_input_mode_records_shape_and_mode():
    fw = _load()
    calls = []
    fw.select_frames_to_video_mode = lambda page, **kw: calls.append(kw) or True

    p = _page("Omni Flash")
    p._input_mode_applied = 'Frames'
    assert fw.set_clip_input_mode(p, "start.png", "end.png") == 'Ingredients'
    assert p._clip_has_end_frame is True
    assert calls and calls[-1].get('input_mode_only') is True

    # Same mode twice = no second dropdown open.
    p._input_mode_applied = 'Ingredients'
    n = len(calls)
    assert fw.set_clip_input_mode(p, "start.png", "end.png") == 'Ingredients'
    assert len(calls) == n

    # Start-only clip on the same job flips back to Frames.
    assert fw.set_clip_input_mode(p, "start.png", None) == 'Frames'
    assert p._clip_has_end_frame is False
    assert len(calls) == n + 1
