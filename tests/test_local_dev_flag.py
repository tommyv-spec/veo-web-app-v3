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


def test_flag_prescanned_before_update():
    src = _src()
    assert '"--local-dev" in sys.argv' in src
    assert 'os.environ.get("WORKER_LOCAL_DEV"' in src


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
