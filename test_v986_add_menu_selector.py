"""v986 — the upload path must use the add-menu button the rest of the file uses.

Measured 2026-09-13, on every clip whose start frame was not already in the Flow
project:

    [v974] Add-media upload of image_07.png failed: Locator.click: Timeout 8000ms
           exceeded. Call log: - waiting for locator("button[aria-label='Add me

`waiting for locator` is Playwright saying the element never appeared, so the
button was absent rather than covered. `_V962_ADD_MENU_BTN`
("button[aria-label='Add ingredients to the prompt box']") is the same button and
is what the Ingredients path, image_worker.py and tools/flow_ui_audit.py's
contract test all use. 'Add media menu' appeared nowhere else in the tree except
that one click and a test pinning it.

That cost the clip outright: an image not in the project can only be attached by
uploading, both upload routes were dead, and the clip never submitted at all --
so it burned no render and looked like a flaky "frame attach glitch" for six
batches.
"""
import ast
import pathlib

_SRC = pathlib.Path(__file__).parent / "static" / "flow_worker.py"


def _fn():
    tree = ast.parse(_SRC.read_text(encoding="utf-8", errors="replace"))
    return next(n for n in ast.walk(tree)
                if isinstance(n, ast.FunctionDef)
                and n.name == "_v962_pick_asset_in_picker")


def _strings(node):
    return [n.value for n in ast.walk(node)
            if isinstance(n, ast.Constant) and isinstance(n.value, str)]


def _names(node):
    return {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}


def test_the_known_good_constant_is_used():
    """THE REGRESSION: the upload path hard-coded its own selector."""
    fn = _fn()
    assert "_V962_ADD_MENU_BTN" in _names(fn), (
        "the upload path still does not use _V962_ADD_MENU_BTN, the add-menu "
        "button every other caller in this file opens the menu with")


def test_the_constant_is_tried_before_the_v974_selector():
    """Order matters: the constant is the one the contract test verifies."""
    fn = _fn()
    tup = next((n for n in ast.walk(fn)
                if isinstance(n, ast.Tuple)
                and any(isinstance(e, ast.Name) and e.id == "_V962_ADD_MENU_BTN"
                        for e in n.elts)), None)
    assert tup is not None, "the two selectors should be tried from one tuple"
    first = tup.elts[0]
    assert isinstance(first, ast.Name) and first.id == "_V962_ADD_MENU_BTN", \
        ast.unparse(tup)


def test_the_v974_selector_survives_as_a_fallback():
    """v974 recorded a real measurement for it and this host moves under us."""
    fn = _fn()
    assert any("Add media menu" in s for s in _strings(fn)), (
        "the v974 selector was removed rather than demoted -- if that button "
        "comes back there is nothing left to find it")


def test_a_missing_button_is_reported_rather_than_timed_out():
    """'waiting for locator' for 8s says nothing; naming the absence does."""
    fn = _fn()
    assert any("neither add-menu button" in s for s in _strings(fn))


def test_a_stuck_overlay_is_distinguishable_from_a_missing_button():
    """_v975_clear_overlays returns whether it cleared, and the call site
    discarded it -- so both failures produced the same click timeout."""
    fn = _fn()
    calls = [n for n in ast.walk(fn)
             if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
             and n.func.id == "_v975_clear_overlays"]
    assert calls, "the overlay clear vanished"
    assigns = [n for n in ast.walk(fn)
               if isinstance(n, ast.Assign)
               and isinstance(n.value, ast.Call)
               and isinstance(n.value.func, ast.Name)
               and n.value.func.id == "_v975_clear_overlays"]
    assert assigns, "the return value of _v975_clear_overlays is still discarded"
