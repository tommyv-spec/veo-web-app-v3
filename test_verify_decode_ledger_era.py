#!/usr/bin/env python3
"""The two decode ledgers are forward-only, and the era boundary is a date the linter reads.

Run: python code/test_verify_decode_ledger_era.py

WHY. `_lint_intensity_ledger` (v622) and `_lint_shown_beats_ledger` (v790 Half C) were the
only checks in verify_decode_format.py that ran unconditionally. Every other forward-only
rule here (v887a, v887c, v890b, v938) gates on the decode's own `created:` date, and the
file says why in its own words: "a decode written on or after the rule date must comply
(FAIL); anything older keeps its thin blocks (WARN)… the era boundary is a date the linter
reads, not a habit."

The cost of the missing guard was concrete: 141 of 207 decodes were red for not carrying
ledgers that did not exist when they were written, and a source file could not be committed
without faking a ledger or bypassing the hook.

THE RISK THIS FILE GUARDS AGAINST is the opposite one — that the guard quietly turns the
ledgers off. So the tests below check BOTH directions: an old decode warns, and a NEW decode
still hard-fails. If the "new-era still FAILs" cases ever go green while the ledger is
missing, the checks have been disabled rather than gated.

Dates, each established twice (rule file + the commit that introduced the check):
  v622 intensity     rules/v622.md "2026-07-22 intensity-calibration amendment" · 621aca9
  v790 Half C beats  rules/v790.md "Half C … (2026-07-22)"                      · 2fbd93e
They coincide today. The linter keeps them as SEPARATE constants so a later amendment to
one cannot silently move the other — which is also why this file asserts them separately.
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
SRC = LINTER.read_text(encoding="utf-8", errors="replace")

INTENSITY_MARK = "intensity ledger"
BEATS_MARK = "shown beat"

FAILURES: list[str] = []


def check(label: str, cond: bool, detail: str = "") -> None:
    if cond:
        print(f"  ok   {label}")
    else:
        print(f"  FAIL {label} {detail}")
        FAILURES.append(label)


def _fixture() -> tuple[str, str]:
    """A decode that PASSES today AND actually reaches the ledger checks.

    The second condition is not obvious and cost a debugging round. The ledger checks only
    run inside a real `## Adaptation-extraction` section, but the required-section check is
    a SUBSTRING test, so a decode whose heading is `### Adaptation-extraction` satisfies the
    requirement while its ledgers are never linted at all. Three decodes in the corpus are
    in that state. Picking one as the fixture makes every assertion here vacuous - it passes
    with the ledger removed because nothing looked. So: require the real `##` heading and
    require both ledgers to be present before using the file.
    """
    for path in sorted((ROOT / "raw" / "videos").glob("decoded_*.md")):
        text = path.read_text(encoding="utf-8", errors="replace")
        if not re.search(r"^## Adaptation-extraction\s*$", text, re.M):
            continue
        if not re.search(r"^###\s+Hero-symptom intensity ledger", text, re.M):
            continue
        if not re.search(r"^###\s+Shown beats ledger", text, re.M):
            continue
        proc = subprocess.run(
            [sys.executable, str(LINTER), str(path)],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )
        if proc.returncode == 0:
            return path.name, text
    raise SystemExit("no passing decode with a real ## Adaptation-extraction and both ledgers")


def _lint(text: str, name: str) -> tuple[int, str]:
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="ledger-era-"))
    try:
        f = tmp / name
        f.write_text(text, encoding="utf-8")
        p = subprocess.run(
            [sys.executable, str(LINTER), str(f)],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )
        return p.returncode, p.stdout + p.stderr
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def _strip_section(text: str, heading_rx: str) -> str:
    """Remove one `### <heading>` block, so the ledger check has something to complain about."""
    return re.sub(rf"^###\s+{heading_rx}\s*$\n.*?(?=^###\s|^##\s)", "", text,
                  count=1, flags=re.S | re.M | re.I)


def _set_created(text: str, value: str | None) -> str:
    if re.search(r"^created:", text, re.M):
        return re.sub(r"^created:.*$", "" if value is None else f"created: {value}",
                      text, count=1, flags=re.M)
    if value is None:
        return text
    return re.sub(r"^---\s*$", f"---\ncreated: {value}", text, count=1, flags=re.M)


def main() -> int:
    # The constants must exist SEPARATELY - one shared constant would be the conflation
    # the review specifically warned about.
    check("v622 has its own date constant", "V622_LEDGER_DATE" in SRC)
    check("v790 Half C has its own date constant", "V790C_LEDGER_DATE" in SRC)
    dates = dict(re.findall(r'(V622_LEDGER_DATE|V790C_LEDGER_DATE)\s*=\s*"([0-9-]+)"', SRC))
    check("both dates are 2026-07-22 as established",
          dates.get("V622_LEDGER_DATE") == "2026-07-22"
          and dates.get("V790C_LEDGER_DATE") == "2026-07-22", str(dates))
    print("  note  the two cutoffs coincide today, so 'a date BETWEEN them' has no test case;")
    print("        they are separate constants precisely so that can change.")
    print()

    name, base = _fixture()
    print(f"fixture: {name} (passes clean today)\n")

    for label, heading, mark in (
        ("v622 intensity", r"Hero-symptom intensity ledger", INTENSITY_MARK),
        ("v790 shown beats", r"Shown beats ledger", BEATS_MARK),
    ):
        stripped = _strip_section(base, heading)
        if stripped == base:
            check(f"{label}: fixture actually has the ledger to remove", False,
                  "(heading not found - test would be vacuous)")
            continue

        # NEW ERA -> must still hard-fail. This is the anti-disable test.
        for when, day in (("ON the cutoff", "2026-07-22"), ("AFTER the cutoff", "2026-08-30")):
            rc, out = _lint(_set_created(stripped, day), name)
            check(f"{label}: missing ledger, dated {when} -> FAILS",
                  rc != 0 and mark in out.lower(), f"(rc={rc})")

        # OLD ERA -> warns, does not block
        rc, out = _lint(_set_created(stripped, "2026-07-21"), name)
        check(f"{label}: missing ledger, dated the DAY BEFORE -> warns only",
              rc == 0 and "forward-only from" in out, f"(rc={rc})")

        rc, out = _lint(_set_created(stripped, None), name)
        check(f"{label}: missing ledger, NO created: date -> warns only",
              rc == 0 and "forward-only from" in out, f"(rc={rc})")

        # a decode that HAS the ledger is unaffected either way
        rc, _ = _lint(_set_created(base, "2026-08-30"), name)
        check(f"{label}: a new-era decode WITH the ledger still passes", rc == 0, f"(rc={rc})")

    print()
    if FAILURES:
        print(f"RESULT: FAIL ({len(FAILURES)}): {', '.join(FAILURES)}")
        return 1
    print("RESULT: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
