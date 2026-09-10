#!/usr/bin/env python3
"""The `created:` field is the era boundary for every forward-only decode rule.

Run: python code/test_verify_decode_created_date.py

WHY THIS EXISTS. Every era gate in verify_decode_format.py reads
`bool(created) and created >= SOME_DATE`, so an EMPTY `created` means old era and the
forward-only rules soften from FAIL to WARN. The field was parsed with a strict
`YYYY-MM-DD` regex and nothing else, so anything the regex missed — `created: today`,
`created: 2026-8-4`, a quoted value, a trailing comment — left it empty. A brand-new
decode could therefore buy legacy treatment by typing a bad date, and no check would say
so. Found 2026-09-11 while designing an era guard for the two ledger checks; the guard
would have widened the same hole to two hard gates.

The rule now: an ABSENT `created:` is the legitimate legacy path and still only warns.
A field that is PRESENT and wrong is a defect and FAILs.

Measured before landing (all 207 decodes in raw/videos): 6 valid, 201 absent, 0
malformed, 0 impossible — and a full corpus lint introduced 0 new failures.
"""
from __future__ import annotations

import pathlib
import re
import shutil
import subprocess
import sys
import tempfile

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = pathlib.Path(__file__).resolve().parent.parent
LINTER = ROOT / "code" / "verify_decode_format.py"
CREATED_FAIL = ("not a valid YYYY-MM-DD", "is not a real date")

FAILURES: list[str] = []


def _base() -> tuple[pathlib.Path, str]:
    """A real decode that already carries a valid `created:`, so only that line varies."""
    for path in sorted((ROOT / "raw" / "videos").glob("decoded_*.md")):
        text = path.read_text(encoding="utf-8", errors="replace")
        if re.search(r"^created:\s*\d{4}-\d{2}-\d{2}\s*$", text, re.M):
            return path, text
    raise SystemExit("no decode with a valid created: date to build the fixture from")


def _lint(text: str, name: str) -> str:
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="created-gate-"))
    try:
        target = tmp / name
        target.write_text(text, encoding="utf-8")
        proc = subprocess.run(
            [sys.executable, str(LINTER), str(target)],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )
        return proc.stdout + proc.stderr
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def check(label: str, text: str, name: str, expect_fail: bool) -> None:
    out = _lint(text, name)
    got = any(marker in out for marker in CREATED_FAIL)
    if got == expect_fail:
        print(f"  ok   {label}")
        return
    print(f"  FAIL {label} (created-date failure reported={got}, expected={expect_fail})")
    FAILURES.append(label)


def main() -> int:
    path, text = _base()
    name = path.name
    print(f"fixture: {name}\n")

    def swap(value: str | None) -> str:
        """Replace the created: line, or drop it entirely when value is None."""
        return re.sub(r"^created:.*$", "" if value is None else f"created: {value}",
                      text, count=1, flags=re.M)

    current = re.search(r"^created:\s*(\d{4}-\d{2}-\d{2})\s*$", text, re.M).group(1)

    # legitimate: pass
    check(f"a valid date ({current}) is accepted", text, name, expect_fail=False)
    check("an ABSENT created: keeps the legacy path", swap(None), name, expect_fail=False)

    # defects: fail
    check("`today` is rejected", swap("today"), name, expect_fail=True)
    check("`2026-8-4` (unpadded) is rejected", swap("2026-8-4"), name, expect_fail=True)
    check("`2026-13-45` (impossible) is rejected", swap("2026-13-45"), name, expect_fail=True)
    check('`"2026-07-22"` (quoted) is rejected', swap('"2026-07-22"'), name, expect_fail=True)
    check("a trailing comment is rejected", swap("2026-07-22  # guessed"), name, expect_fail=True)

    print()
    if FAILURES:
        print(f"RESULT: FAIL ({len(FAILURES)}): {', '.join(FAILURES)}")
        return 1
    print("RESULT: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
