"""--local-dev / WORKER_LOCAL_DEV skips the flow worker's self-update.

Exists because every worker-side fix used to cost push -> Render deploy ->
self-update -> restart (~10+ min per iteration, 2026-08-27 charswap night).
flow_worker.py is not importable here (browser_driver only exists on
installed workers), so these are source asserts in the house style.
"""
import os

_HERE = os.path.dirname(os.path.abspath(__file__))
_FW = os.path.join(os.path.dirname(_HERE), "static", "flow_worker.py")


def _src():
    return open(_FW, encoding="utf-8").read()



def _assignment_expression(src, assign):
    """The right-hand side of the `_local_dev = ...` statement, and nothing else.

    One line is too little - HEAD spells the read across several lines inside
    parentheses. A fixed window is too much - it reaches the "drop --local-dev /
    WORKER_LOCAL_DEV to resync" banner, and then `_local_dev = False` passes.
    """
    rest = src[src.index("=", assign) + 1:]
    if rest.lstrip().startswith("("):
        depth, start = 0, rest.index("(")
        for i, ch in enumerate(rest[start:], start=start):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    return rest[:i + 1]
    return rest[:rest.index("\n")]


def test_flag_prescanned_before_update():
    """The flag is read BEFORE the self-update gate.

    Deliberately does NOT assert how it is read. The earlier version pinned two exact
    expressions (`"--local-dev" in sys.argv`, `os.environ.get("WORKER_LOCAL_DEV"`) and
    broke the moment the read moved into `_local_dev_requested()`, which is a better
    shape. Asserting the tokens appear anywhere would be worse than useless: the
    "drop --local-dev / WORKER_LOCAL_DEV to resync" banner contains both, so such a
    test passes with the reading logic deleted.

    The BEHAVIOUR - CLI flag, adjacent .env, defaults - is covered executably by
    test_movie_section_worker_arm.py::test_direct_launch_reads_local_dev_from_adjacent_env,
    which runs `_local_dev_requested` with injected argv/environ/env_path. What only a
    source assert can prove is the ORDER, so that is all this one claims.
    """
    src = _src()
    assign = src.index("_local_dev = ")
    gate = src.index("if _local_dev:")
    assert assign < gate, "the flag must be read before the update gate"

    # ...and the assignment must READ something. Order alone is satisfied by
    # `_local_dev = False`, which was true of the first version of this fix.
    rhs = _assignment_expression(src, assign)
    assert any(tok in rhs for tok in ("_local_dev_requested", "argv", "WORKER_LOCAL_DEV")), (
        f"the flag must be read from argv, the env var or the helper, got: {rhs.strip()!r}"
    )


def test_update_call_is_gated_not_removed():
    """check_for_updates() must still run in normal mode — the gate is an
    else-branch, not a deletion."""
    src = _src()
    gate = src.index("if _local_dev:")
    tail = src[gate:gate + 1200]
    assert "else:" in tail
    assert "check_for_updates()  # Auto-update on startup" in tail


def test_mode_announces_itself_loudly():
    """A worker silently running divergent code is the silent-Chrome-fallback
    trap (bde3702) again — the banner is load-bearing."""
    src = _src()
    assert "LOCAL DEV MODE" in src


def test_argparse_accepts_the_flag():
    """parse_args() runs after the pre-scan; without this the flag would crash
    argument parsing further down main."""
    src = _src()
    assert "parser.add_argument('--local-dev'" in src
