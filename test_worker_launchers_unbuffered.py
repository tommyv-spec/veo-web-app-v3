"""Every launcher that starts flow_worker.py must run it unbuffered.

Why this file exists rather than a comment. `flow_worker.py` prints its poll
line in three places and only ONE of them flushes, so its output only looks
live when stdout is a terminal. Every launcher that redirects or pipes it has
been sitting on roughly 24 minutes of buffered output, which reads exactly
like a hung worker. On 2026-09-04 that symptom cost two sessions most of a day
deciding whether a healthy idle worker had stalled, and
`tools/launch_workers.py` records an afternoon it would have killed a healthy
worker mid-render for the same reason.

The fix was applied SEVEN times, in four waves, because each sweep found
another launcher nobody had looked at: a hand-made .bat, then the launcher
`main.py` generates, then three more from `static/setup_worker.py`, then two
in `setup_vps_worker.sh` (both piping, which buffers harder than a file), then
the installer running the worker itself. Two of those waves came from checking
a claim that there were no more.

So the rule does not live in anybody's memory. This test walks the source and
fails on launcher number eight.
"""
import re
from pathlib import Path

import pytest

_HERE = Path(__file__).resolve().parent

# Files that GENERATE or RUN a worker launcher. A new one belongs here.
LAUNCHER_SOURCES = ("main.py", "static/setup_worker.py", "setup_vps_worker.sh")

# Anything that actually invokes the worker, in any of the four dialects this
# repo emits: bash, cmd, PowerShell and a systemd unit.
_INVOKES = re.compile(
    r"(python3?|\$PY|ExecStart|executable\}\"|&\s*\")\s*.{0,30}flow_worker\.py")

# How far back to look for the export. Generous on purpose: the point is that
# it is set somewhere in the same block, not that it sits on an exact line.
_LOOKBACK = 14


def _invocations():
    for name in LAUNCHER_SOURCES:
        path = _HERE / name
        if not path.exists():                      # pragma: no cover
            pytest.fail(f"{name} is gone — update LAUNCHER_SOURCES deliberately")
        lines = path.read_text(encoding="utf-8").splitlines()
        for i, line in enumerate(lines):
            if _INVOKES.search(line):
                yield name, i + 1, line.strip(), "\n".join(
                    lines[max(0, i - _LOOKBACK):i])


def test_every_launcher_starts_the_worker_unbuffered():
    missing = [f"{name}:{n}  {line[:70]}"
               for name, n, line, before in _invocations()
               if "PYTHONUNBUFFERED" not in before]
    assert not missing, (
        "these start flow_worker.py without turning buffering off, so their "
        "log will trail reality by ~24 minutes and a working worker will read "
        "as hung:\n  " + "\n  ".join(missing))


def test_the_sweep_still_finds_every_known_launcher():
    """Guards the guard. If the regex or the file list silently stops matching,
    the test above passes vacuously and the next launcher ships unprotected —
    which is exactly how this defect survived four sweeps."""
    found = {(name, n) for name, n, _, _ in _invocations()}
    by_file = {}
    for name, _ in found:
        by_file[name] = by_file.get(name, 0) + 1
    assert by_file.get("main.py", 0) >= 2, by_file
    assert by_file.get("static/setup_worker.py", 0) >= 3, by_file
    assert by_file.get("setup_vps_worker.sh", 0) >= 2, by_file
    assert len(found) >= 7, f"expected at least the 7 known launchers, got {found}"


def test_the_export_is_set_after_the_env_load_not_before():
    """A stale .env must not be able to put the buffering back. Every dialect
    loads .env first, so the export has to come after it, and 'somewhere in the
    block' is not enough for this one."""
    for name in LAUNCHER_SOURCES:
        src = (_HERE / name).read_text(encoding="utf-8")
        for m in re.finditer(r"PYTHONUNBUFFERED", src):
            before = src[:m.start()]
            # the nearest preceding env load in any dialect
            env = max(before.rfind("source .env"), before.rfind('in (".env")'),
                      before.rfind("Get-Content .env"),
                      before.rfind("EnvironmentFile="))
            if env == -1:
                continue          # no .env in this block; nothing to order against
            assert env < m.start(), (
                f"{name}: PYTHONUNBUFFERED is set BEFORE the .env load, so a "
                f"stale .env can override it")
