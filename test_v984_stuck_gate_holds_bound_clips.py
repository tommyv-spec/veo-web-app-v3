"""v984 — the stuck gate must not redo a clip that is still rendering.

Measured 2026-09-13. batch6 (job 0d456c24): clip 14912 was submitted, bound to two
media ids and generating. At 229s the v738 gate called it "stuck ... no URL + no fail
signal", set flow_redo_queued and popped it from clip_submit_times. Two minutes later
the post-job harvest delivered every clip it had NOT popped -- three of them, in 32
seconds, straight off the media listing. 14912 was absent from that harvest only
because of the pop. batch7 repeated it exactly, on 14916 and 14917.

The gate's "no URL" reading comes from `[data-index]`, which does not exist on
flow.google.com, so it is zero for a healthy clip and a dead one alike.

Two things these tests exist to stop, both found by Codex in the plan before a line
shipped:

  * a guard that LOGS the hold but still runs the redo -- the first draft's
    assertions passed against exactly that;
  * a hold read too early. Bindings arrive asynchronously (the measured case is a
    LATE-BIND at 57s) and the rescue between the gate head and the redo takes up to
    15s, so a snapshot taken at the head can be stale by the time it is used.

By AST, not substring: a text search over this file has been wrong four times in two
days -- it counted the helper's own line, matched `scrollTop` for `scrollTo`, and
matched a function's own signature as its recursion guard.
"""
import ast
import pathlib

_SRC = pathlib.Path(__file__).parent / "static" / "flow_worker.py"


def _tree():
    return ast.parse(_SRC.read_text(encoding="utf-8", errors="replace"))


def _names(node):
    return {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}


def _dump(stmts):
    return ast.dump(ast.Module(body=list(stmts), type_ignores=[]))


def _hold_if():
    """The `if _v984_hold:` whose ELSE branch performs the redo.

    Located by what it does, not by a comment or a line number: its test names
    `_v984_hold` and its orelse marks flow_redo_queued.
    """
    hits = [n for n in ast.walk(_tree())
            if isinstance(n, ast.If)
            and "_v984_hold" in _names(n.test)
            and n.orelse
            and "flow_redo_queued" in _dump(n.orelse)]
    assert len(hits) == 1, f"expected exactly one v984 hold branch, found {len(hits)}"
    return hits[0]


def _gate():
    """The enclosing `if _age >= STUCK_REDO_THRESHOLD:`."""
    node = _hold_if()
    for cand in ast.walk(_tree()):
        if not isinstance(cand, ast.If):
            continue
        if "STUCK_REDO_THRESHOLD" not in ast.dump(cand.test):
            continue
        if cand.lineno <= node.lineno <= (cand.end_lineno or cand.lineno):
            return cand
    raise AssertionError("the v738 redo gate is gone -- this test needs rewriting")


def _span(stmts):
    lines = set()
    for st in stmts:
        for n in ast.walk(st):
            if hasattr(n, "lineno"):
                lines.add(n.lineno)
    return lines


def test_the_redo_lives_only_in_the_unheld_branch():
    """THE REGRESSION, half one: a held clip must not be downgraded.

    A guard that computes `_v984_hold`, logs it, and still calls
    update_clip_status fails here -- which the first draft of this test did not.
    """
    node = _hold_if()
    unheld = _span(node.orelse)
    redos = [n for n in ast.walk(_gate())
             if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Name) and n.func.id == "update_clip_status"
             and any(isinstance(a, ast.Constant) and a.value == "flow_redo_queued"
                     for a in n.args)]
    assert redos, "the redo call vanished -- the unheld path still needs it"
    for n in redos:
        assert n.lineno in unheld, (
            f"update_clip_status at line {n.lineno} is reachable for a HELD clip")


def test_the_pop_that_hid_the_clip_from_the_harvest_is_guarded():
    """THE REGRESSION, half two, and the one that actually lost the render.

    The post-job harvest takes its pending set from
    `_all_submitted = set(clip_submit_times.keys())` (flow_worker.py:30502), so
    the pop in the redo body is what made 14912 invisible to it.

    The v739 and v739b rescues pop too, and MUST keep doing so -- they have
    already handed the clip to the download worker, so it is finished, not
    hidden. Those are identified by the `_v739_rescued = True` in the same
    branch rather than excused by line number.
    """
    node = _hold_if()
    unheld = _span(node.orelse)

    rescue_lines = set()
    for n in ast.walk(_gate()):
        if not isinstance(n, ast.If):
            continue
        for branch in (n.body, n.orelse):
            if not branch:
                continue
            assigns_rescued = any(
                isinstance(a, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == "_v739_rescued"
                        for t in a.targets)
                and isinstance(a.value, ast.Constant) and a.value.value is True
                for st in branch for a in ast.walk(st))
            if assigns_rescued:
                rescue_lines |= _span(branch)

    pops = [n for n in ast.walk(_gate())
            if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Attribute) and n.func.attr == "pop"
            and isinstance(n.func.value, ast.Name)
            and n.func.value.id == "clip_submit_times"]
    assert pops, "every pop vanished -- the unheld path still needs one"
    assert any(n.lineno in unheld for n in pops), \
        "the redo body no longer pops; an unheld stuck clip would be re-scanned forever"
    for n in pops:
        assert n.lineno in unheld or n.lineno in rescue_lines, (
            f"clip_submit_times.pop at line {n.lineno} is neither guarded by the "
            f"hold nor part of a successful rescue -- a held clip would still be "
            f"hidden from the post-job harvest")


def test_the_hold_is_read_immediately_before_it_is_used():
    """A binding that arrives during the rescue must still save the clip.

    Bindings land asynchronously -- the measured case is a LATE-BIND at 57s --
    and the v739 rescue between the gate head and the redo can take 15s. A hold
    snapshotted at the head would be stale exactly when it matters, so the read
    must sit inside the same `if not _v739_rescued:` that performs the redo, with
    no rescue work between the read and the branch.
    """
    node = _hold_if()
    reads = [n for n in ast.walk(_gate())
             if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
             and n.func.id == "bound_media_ids_for_clip"]
    assert reads, "the hold must come from bound_media_ids_for_clip"
    assert len(reads) == 1, \
        f"the hold is read {len(reads)}x in the gate; one read, one decision"
    gap = node.test.lineno - reads[0].lineno
    assert 0 < gap <= 8, (
        f"{gap} lines between reading the binding and branching on it -- "
        f"anything that can block in between makes the value stale")


def test_the_announcement_sits_with_the_action_it_describes():
    """The redo message used to print 130 lines before the update, so a held
    clip would have announced a downgrade it never performed."""
    node = _hold_if()
    unheld = _span(node.orelse)
    announce = [n.lineno for n in ast.walk(_gate())
                if isinstance(n, ast.Constant) and isinstance(n.value, str)
                and "marking flow_redo_queued" in n.value]
    assert announce, "the redo announcement vanished"
    for line in announce:
        assert line in unheld, (
            f"'marking flow_redo_queued' at line {line} prints for a HELD clip too")
