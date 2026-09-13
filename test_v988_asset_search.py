"""v988 — filter the picker with Flow's own search box before giving up.

Measured 2026-09-13 with the v987 title dump, on a clip whose start frame was in
the project all along:

    "titles": ["image_32.png","image_31.png","image_24.png","image_04.png",
               "image_17.png","image_12.png","image_11.png","image_10.png"]
    "shapes": {"div.asset-item-container": 15}
    "alts":   ["Preview of image_32.png"]
    Frames: start=image_07.png
    [v985] image_07.png never became clickable in 30s
    [v974] uploaded image_07.png through Flow's Add-media chooser
    ⚠ image_07.png uploaded but no asset option with that name in 60 s

The titles are plain file names, so the match rule was never wrong. 15 rows
rendered carried 8 distinct names and one lazy thumbnail alt: the list is
virtualised, and `_find()` reads only rendered DOM, so an asset outside the
window is invisible however long it waits.

Six hypotheses died before this one, all of them guesses at why the NAME did not
match. It always matched; the row was not on the page. These tests therefore pin
the one thing that matters -- the search runs, with the name, before the upload
branch -- and not a theory.
"""
import ast
import pathlib

_SRC = pathlib.Path(__file__).parent / "static" / "flow_worker.py"


def _fn():
    tree = ast.parse(_SRC.read_text(encoding="utf-8", errors="replace"))
    return next(n for n in ast.walk(tree)
                if isinstance(n, ast.FunctionDef)
                and n.name == "_v962_pick_asset_in_picker")


def _names(node):
    return {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}


def _call(fn, name):
    return [n for n in ast.walk(fn)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
            and n.func.id == name]


def test_the_picker_is_searched_before_the_upload_branch():
    """THE REGRESSION: an asset already in the project was re-uploaded and
    still never found, because it was never rendered."""
    fn = _fn()
    assert "_V988_SEARCH_BOX" in _names(fn), "the search box is never used"
    upload = _call(fn, "_v962_upload_into_picker")
    assert upload, "the upload fallback vanished"
    box = next(n for n in ast.walk(fn)
               if isinstance(n, ast.Name) and n.id == "_V988_SEARCH_BOX")
    assert box.lineno < upload[0].lineno, (
        f"the search runs at line {box.lineno}, after the upload at "
        f"{upload[0].lineno} — an asset already in the project would still be "
        f"uploaded again")


def test_it_searches_for_the_file_name_we_already_have():
    """The titles are plain file names; searching for anything else is a guess."""
    fn = _fn()
    fills = [n for n in ast.walk(fn)
             if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Attribute) and n.func.attr == "fill"]
    assert fills, "nothing is typed into the search box"
    typed = {a.id for n in fills for a in n.args if isinstance(a, ast.Name)}
    assert "stem" in typed or "name" in typed, (
        f"the search types {typed or 'no variable'} — it must use the file name")


def test_the_existing_finder_is_reused():
    """No second matching rule: the row just needs to exist on the page."""
    fn = _fn()
    assert _call(fn, "_find"), "the search must hand off to _find()"


def test_it_can_be_switched_off():
    src = _SRC.read_text(encoding="utf-8", errors="replace")
    assert 'os.environ.get("FLOW_ASSET_SEARCH"' in src
    assert "_V988_ASSET_SEARCH" in _names(_fn())


def test_a_failed_search_cannot_take_the_clip_down():
    """It is an optimisation in front of a fallback, never a new failure mode."""
    fn = _fn()
    box = next(n for n in ast.walk(fn)
               if isinstance(n, ast.Name) and n.id == "_V988_SEARCH_BOX")
    guarded = [t for t in ast.walk(fn)
               if isinstance(t, ast.Try)
               and t.lineno <= box.lineno <= (t.end_lineno or t.lineno)]
    assert guarded, "the search is not wrapped in a try -- a throw would lose the clip"
