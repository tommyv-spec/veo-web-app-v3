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

# Anything that actually invokes the worker. Deliberately NOT a list of known
# spellings: the first version of this regex enumerated python3?/$PY/ExecStart/
# sys.executable and MISSED `!PY!`, the cmd delayed-expansion form used by the
# WINDOWS installer -- the most-used path of all. That miss shipped, and the
# per-file minimum below still passed because the two unix hits satisfied it.
#
# What separates a COMMAND from a MENTION is what follows the filename, not
# what precedes it. Every real launcher passes the worker a flag; every prose
# hit ("a worker reads the served flow_worker.py ONCE", the download route,
# a print) does not. Keying on the tail rather than the interpreter is what
# makes this independent of how the interpreter happens to be spelled -- which
# is the thing that went wrong.
_MENTIONS = re.compile(
    r"flow_worker\.py\"?,?\s*\"?--"         # any launcher passing a flag,
                                            # bare or as a python arg list
    r"|ExecStart=.*flow_worker\.py")        # a systemd unit
_NOT_AN_INVOCATION = re.compile(
    r"^\s*(#|::|rem\b)"                     # a comment in any dialect here
    r"|py_compile|ast\.parse|read_text|WORKER_SRC|print\(")


def _code_lines(path):
    """Yield (line_number, text) for lines that are not inside a docstring.

    Prose is where most mentions of the worker live, and pattern-guessing at
    prose was producing false hits on every sentence containing the filename.
    Tracking the triple quotes is exact instead of clever: a mention inside a
    docstring is documentation, full stop.
    """
    in_doc = False
    for i, line in enumerate(path.read_text(encoding="utf-8").splitlines()):
        opens = line.count('"""') + line.count("'''")
        was_in = in_doc
        if opens % 2:
            in_doc = not in_doc
        # A line that opens AND closes on itself is code around a docstring;
        # a line while `was_in` is documentation regardless of what it holds.
        if was_in or (opens % 2 and not was_in and line.strip().startswith(('"""', "'''"))):
            continue
        yield i + 1, line

# Every OTHER line that names the worker must fit one of these harmless shapes.
# This is the half that makes the rule above safe to keep narrow: a launcher
# spelled some new way does not match _MENTIONS, so it lands here instead, fits
# nothing, and the third test fails asking a human to classify it. Without it,
# narrowing the invocation pattern would just recreate the blind spot that let
# the Windows installer ship buffered.
BENIGN_MENTION = (
    ("Path(", "a filesystem path, not a command"),
    ('/ "flow_worker.py"', "a path join, not a command"),
    ("download/flow_worker.py", "the download route or a curl of it"),
    ("flow_worker.py.tmp", "the self-update download, not a launch"),
    ("local_flow_worker.py", "the other module's name"),
    ("static/flow_worker.py", "a repo path in prose or a sync"),
    ("HKCU", "the kavenobuilder: URL protocol handler — `cmd /c` gets a real "
             "console, so stdout is a terminal and python line-buffers to it. "
             "Left alone deliberately: it is a registry value with three nested "
             "layers of cmd escaping that cannot be tested from this repo."),
)

# Lines that mention the worker, are not comments, and still do not need the
# flag. Each needs a REASON, and the exemption is keyed to text so that editing
# the line stops the exemption matching and the guard speaks up again.
EXEMPT = {
    # The kavenobuilder: URL protocol handler. `cmd /c` gets a real console, so
    # stdout is a terminal and python line-buffers to it -- the bug cannot bite
    # here, and there is no piping path for a shell-launched protocol handler.
    # Left alone on purpose: it is a registry value with three nested layers of
    # cmd escaping that cannot be tested from this repo, so the risk of a silent
    # break outweighs a fix for a case that does not misbehave.
    "HKCU": "URL protocol handler; console stdout, and untestable reg escaping",
}

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
            if not _MENTIONS.search(line) or _NOT_AN_INVOCATION.search(line):
                continue
            if any(k in line for k in EXEMPT):
                continue
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


def test_every_other_mention_of_the_worker_fits_a_known_harmless_shape():
    """The blind-spot guard, and the reason this file exists in this form.

    The first version of this test enumerated interpreter spellings and missed
    `!PY!`, the cmd delayed-expansion form used by the WINDOWS installer — the
    most-used path of all. It shipped buffered, and the per-file minimums still
    passed because the two unix hits satisfied them. Measuring the DELIVERED
    installer over HTTP is what found it, not the source scan.

    So anything naming the worker that is NOT recognised as a command has to
    fit a shape someone has already looked at. A launcher spelled a new way
    fits none of them and lands here, which is a question rather than silence.
    """
    unclassified = []
    for name in LAUNCHER_SOURCES:
        for n, line in _code_lines(_HERE / name):
            if "flow_worker.py" not in line:
                continue
            if _MENTIONS.search(line) or _NOT_AN_INVOCATION.search(line):
                continue
            if any(token in line for token, _ in BENIGN_MENTION):
                continue
            unclassified.append(f"{name}:{n}  {line.strip()[:90]}")
    assert not unclassified, (
        "these name flow_worker.py and match neither a known command shape nor "
        "a known harmless one. If any is a launcher, it needs PYTHONUNBUFFERED "
        "and _MENTIONS needs to recognise it; if it is harmless, add its shape "
        "to BENIGN_MENTION with a reason:\n  " + "\n  ".join(unclassified))


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
