#!/usr/bin/env python3
"""Syntax-check the inline JavaScript inside an HTML file before it deploys.

`static/index.html` is 25,923 lines and 21,621 of them are inline JavaScript in
two <script> blocks. Nothing checked them: a syntax error there is not caught by
`py_compile`, not caught by an import test, and not caught by the platform
linter, so it ships and breaks the page at runtime.

`node --check` parses a file without running it, which is exactly the test we
want. Blocks are extracted, written to a temp file and parsed one by one; any
error is reported back at its ORIGINAL line number in the HTML, because a line
number relative to an extracted block is useless when you go to fix it.

Usage:
    python code/check_inline_js.py                    # defaults to static/index.html
    python code/check_inline_js.py path/to/page.html [more.html ...]

Exit codes: 0 = every block parses (or node is unavailable), 1 = a block failed.

Deliberately NOT a hard failure when node is missing: this runs in a pre-push
path and must not block a push from a box that has no node installed. It says
so loudly instead.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

DEFAULT_TARGET = Path(__file__).resolve().parent / "static" / "index.html"

SCRIPT_RE = re.compile(r"<script([^>]*)>(.*?)</script\s*>", re.S | re.I)

# Only these type= values are classic JavaScript. A block with any other type
# (text/template, application/json, importmap) is data, not code, and parsing
# it as JS would produce a false failure.
JS_TYPES = {"", "text/javascript", "application/javascript", "module"}


def _stdout_utf8_safe() -> None:
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError, OSError):
            pass


def _attr(attrs: str, name: str) -> str | None:
    m = re.search(name + r"""\s*=\s*["']([^"']*)["']""", attrs, re.I)
    return m.group(1) if m else None


def check_file(path: Path) -> list[str]:
    """Return a list of human-readable problems ([] means clean)."""
    problems: list[str] = []
    text = path.read_text(encoding="utf-8", errors="replace")

    checked = 0
    for m in SCRIPT_RE.finditer(text):
        attrs, body = m.group(1), m.group(2)
        if _attr(attrs, "src") is not None:
            continue  # external file, not inline
        stype = (_attr(attrs, "type") or "").strip().lower()
        if stype not in JS_TYPES:
            continue  # data block, not code
        if not body.strip():
            continue

        # 1-based line where the block's first line sits in the HTML.
        start_line = text[: m.start(2)].count("\n") + 1
        suffix = ".mjs" if stype == "module" else ".js"

        tmp = None
        try:
            with tempfile.NamedTemporaryFile(
                "w", suffix=suffix, delete=False, encoding="utf-8"
            ) as fh:
                fh.write(body)
                tmp = Path(fh.name)
            proc = subprocess.run(
                ["node", "--check", str(tmp)],
                capture_output=True, text=True, errors="replace",
            )
        finally:
            if tmp is not None:
                tmp.unlink(missing_ok=True)

        checked += 1
        if proc.returncode != 0:
            err = (proc.stderr or proc.stdout or "").strip()
            # node reports "<tmpfile>:LINE" — rewrite to the real HTML line.
            def remap(mm: re.Match) -> str:
                return "%s:%d" % (path.name, start_line + int(mm.group(1)) - 1)

            err = re.sub(re.escape(str(tmp)) + r":(\d+)", remap, err)
            err = re.sub(r"^.*[/\\][^/\\]+\.m?js:(\d+)", remap, err, flags=re.M)
            problems.append(
                "%s: inline <script> starting at line %d failed to parse:\n%s"
                % (path, start_line, err)
            )

    if checked == 0:
        problems.append("%s: no inline JavaScript found — is this the right file?" % path)
    return problems


def main(argv: list[str] | None = None) -> int:
    _stdout_utf8_safe()
    args = list(argv if argv is not None else sys.argv[1:])
    targets = [Path(a) for a in args] or [DEFAULT_TARGET]

    if shutil.which("node") is None:
        print("SKIP: node is not installed, so inline JavaScript was NOT checked.")
        print("      Install Node to enable this gate (it only parses, never runs).")
        return 0

    failed = False
    for target in targets:
        if not target.exists():
            print("FAIL: %s does not exist" % target)
            failed = True
            continue
        problems = check_file(target)
        if problems:
            failed = True
            for p in problems:
                print("FAIL: %s" % p)
        else:
            print("PASS: %s — every inline <script> parses" % target)

    print("RESULT: %s" % ("FAIL" if failed else "PASS"))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
