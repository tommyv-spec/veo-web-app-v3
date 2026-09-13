"""v985 — the first asset lookup must wait, not give up after one try.

Measured over six failures (batches 11-15, 2026-09-13): the picker pane holds N
asset containers and N-1 clickable buttons, every time, and the one without a
button is the file the worker wants -- the newest, still being processed by Flow.
`_find()` matches `button` and `img[alt]`; an asset mid-processing has neither.

Being a SINGLE SHOT is what turned that into a lost clip. The fallback cannot
recover on flow.google.com: every drop target refuses the file, then the
Add-media click times out under the still-open picker overlay, and the clip never
submits at all.

Three earlier hypotheses died on the evidence, so these tests pin the SHAPE of
the repair rather than the theory: the first lookup polls, it polls the same
`_find()` the post-upload path uses, and it still falls through to upload when
the asset genuinely is not there.
"""
import ast
import pathlib

_SRC = pathlib.Path(__file__).parent / "static" / "flow_worker.py"


def _fn():
    tree = ast.parse(_SRC.read_text(encoding="utf-8", errors="replace"))
    return next(n for n in ast.walk(tree)
                if isinstance(n, ast.FunctionDef)
                and n.name == "_v962_pick_asset_in_picker")


def _find_calls(node):
    return [n for n in ast.walk(node)
            if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Name) and n.func.id == "_find"]


def test_the_first_lookup_is_retried_not_single_shot():
    """THE REGRESSION. One miss used to cost the whole clip.

    Counted BEFORE the upload call on purpose. Two `_find()` polling loops
    already existed after it, so a test that merely counted loops in the
    function passed against the unfixed file -- which is how the first draft of
    this test was wrong.
    """
    fn = _fn()
    upload = next(n for n in ast.walk(fn)
                  if isinstance(n, ast.Call)
                  and isinstance(n.func, ast.Name)
                  and n.func.id == "_v962_upload_into_picker")
    before = [n for n in ast.walk(fn)
              if isinstance(n, ast.While) and _find_calls(n)
              and n.lineno < upload.lineno]
    assert before, (
        "the first asset lookup is still a single shot -- a present-but-not-yet-"
        "clickable asset goes straight to the upload branch, which cannot work "
        "on this host")


def test_the_wait_happens_before_the_upload_branch():
    """Waiting after the upload has already failed would be useless."""
    fn = _fn()
    upload = next(n for n in ast.walk(fn)
                  if isinstance(n, ast.Call)
                  and isinstance(n.func, ast.Name)
                  and n.func.id == "_v962_upload_into_picker")
    loops = sorted(n.lineno for n in ast.walk(fn)
                   if isinstance(n, ast.While) and _find_calls(n))
    assert loops[0] < upload.lineno, (
        f"the first polling loop is at {loops[0]}, after the upload call at "
        f"{upload.lineno} -- it would never save a clip")


def test_it_reuses_find_rather_than_a_new_lookup():
    """No new mechanism: the same matcher, given the patience the other path has."""
    fn = _fn()
    loops = [n for n in ast.walk(fn)
             if isinstance(n, ast.While) and _find_calls(n)]
    first = min(loops, key=lambda n: n.lineno)
    assert _find_calls(first), "the wait must call _find(), not re-implement it"


def test_the_wait_is_bounded_and_switchable():
    """An unbounded wait would hang the lane; 0 must restore the old behaviour."""
    src = _SRC.read_text(encoding="utf-8", errors="replace")
    assert "_V985_PICK_WAIT_S" in src
    assert 'os.environ.get("FLOW_PICK_WAIT_S")' in src
    fn = _fn()
    loops = [n for n in ast.walk(fn)
             if isinstance(n, ast.While) and _find_calls(n)]
    first = min(loops, key=lambda n: n.lineno)
    names = {n.id for n in ast.walk(first.test) if isinstance(n, ast.Name)}
    assert "_V985_PICK_WAIT_S" in names, ast.unparse(first.test)


def test_a_genuinely_missing_asset_still_reaches_the_upload_branch():
    """The wait must not swallow the fallback -- a new image really can be absent."""
    fn = _fn()
    upload = [n for n in ast.walk(fn)
              if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
              and n.func.id == "_v962_upload_into_picker"]
    assert len(upload) == 1, "the upload fallback must survive the change"
