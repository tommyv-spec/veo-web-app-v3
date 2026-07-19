#!/usr/bin/env python3
"""check_rule_index.py — the v-rule index coverage linter.

Answers one question the four scattered ledgers could never answer reliably:
"is EVERY defined v-rule listed in the index?"

It compares:
  - MASTERS  = code/template_reference.md   (the deep-dive — a rule is DEFINED
               when its vNNN shows up in a markdown heading line `^#{1,4} `)
  - INDEX    = wiki/patterns/conventions.md (the one-row-per-rule table —
               a rule is INDEXED when its vNNN is in a table row's first cell)

Reports two gaps:
  1. DEFINED but NOT INDEXED  -> the index is blind on these (the real disease)
  2. INDEXED but NOT DEFINED  -> an index row whose deep-dive has no heading
                                 anchor (dead pointer, or defined only in prose)

Rule numbers are BASE-normalized (v791.2 -> v791, v681e -> v681) so a family's
sub-rules collapse onto the one row the index actually carries.

Exit code 0 = index complete. 1 = gaps found (use as a CI / pre-commit gate).

Usage:
    python code/check_rule_index.py            # human report
    python code/check_rule_index.py --json     # machine-readable
    python code/check_rule_index.py --quiet     # exit code only
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

# repo root = parent of this file's dir (code/ -> repo root)
ROOT = Path(__file__).resolve().parent.parent
MASTERS = ROOT / "code" / "template_reference.md"
INDEX = ROOT / "wiki" / "patterns" / "conventions.md"

HEADING = re.compile(r"^#{1,4}\s+(.*)$")
VTOKEN = re.compile(r"v(\d{3})(?:\.\d+|[a-z]+(?:\.\d+)?)?", re.IGNORECASE)


def base(token_num: str) -> str:
    """'791' -> 'v791' (already base — the regex hands us the 3-digit core)."""
    return "v" + token_num


def defined_rules(text: str) -> dict[str, list[tuple[int, str]]]:
    """base-rule -> list of (line_no, heading_text) where it appears in a heading."""
    out: dict[str, list[tuple[int, str]]] = {}
    for i, line in enumerate(text.splitlines(), 1):
        m = HEADING.match(line)
        if not m:
            continue
        htext = m.group(1).strip()
        for vm in VTOKEN.finditer(htext):
            rule = base(vm.group(1))
            out.setdefault(rule, []).append((i, htext))
    return out


_CROSSREF_PREP = r"beyond|per|behind|supersede[sd]?|mirror of|extends|amends|replaces|alongside|generali[sz]ation of"


def best_location(rule: str, locs: list[tuple[int, str]]) -> tuple[int, str]:
    """Pick the heading where `rule` is the SUBJECT (its defining section),
    not one that only cross-references it.

    A rule can appear in several headings — its own definition PLUS mentions
    like "Generalization beyond v539" or "... (supersedes v552)". The first
    occurrence is often a cross-ref, so score each heading and pick the best:
      + rule in trailing parens  "... (v539)"        -> defining
      + heading starts with the rule  "v710 — ..."   -> defining
      - rule is the object of a cross-ref preposition -> not defining
    Ties break to the lowest line number.
    """
    n = rule[1:]
    suffix = r"(?:\.\d+|[a-z]+)?"

    def score(heading: str) -> int:
        h = heading.strip()
        s = 0
        if re.search(rf"\(v{n}{suffix}\)", h):
            s += 3
        if re.match(rf"(?:\d+\.\s*)?v{n}\b", h):
            s += 3
        if re.search(rf"\b(?:{_CROSSREF_PREP})\s+v{n}\b", h, re.IGNORECASE):
            s -= 4
        return s

    return max(locs, key=lambda t: (score(t[1]), -t[0]))


def indexed_rules(text: str) -> set[str]:
    """base-rules that appear in the FIRST cell of a markdown table row."""
    out: set[str] = set()
    for line in text.splitlines():
        s = line.lstrip()
        if not s.startswith("|"):
            continue
        first_cell = s.split("|")[1] if s.count("|") >= 2 else ""
        for vm in VTOKEN.finditer(first_cell):
            out.add(base(vm.group(1)))
    return out


def main() -> int:
    # Windows consoles / piped stdout default to cp1252 and choke on the
    # report glyphs (checkmarks, em-dashes copied from headings). Force UTF-8
    # with replacement so the linter never crashes inside a git hook / CI pipe.
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    ap = argparse.ArgumentParser(description="v-rule index coverage linter")
    ap.add_argument("--json", action="store_true", help="machine-readable output")
    ap.add_argument("--quiet", action="store_true", help="exit code only, no report")
    ap.add_argument("--toc", action="store_true",
                    help="emit a jump-map (rule -> line + heading) of the rulebook to stdout")
    args = ap.parse_args()

    if not MASTERS.exists() or not INDEX.exists():
        print(f"ERROR: missing file(s): {MASTERS if not MASTERS.exists() else INDEX}",
              file=sys.stderr)
        return 2

    defined = defined_rules(MASTERS.read_text(encoding="utf-8"))
    indexed = indexed_rules(INDEX.read_text(encoding="utf-8"))

    if args.toc:
        # A navigation map INTO template_reference.md: jump straight to any
        # rule's section instead of grepping a 1.4 MB file blind. Non-destructive
        # (the master is untouched); regenerate any time the rulebook changes.
        rows = []
        for rule, locs in defined.items():
            line, heading = best_location(rule, locs)
            rows.append((line, rule, heading))
        rows.sort(key=lambda t: t[0])
        out = [
            "<!-- GENERATED by `python code/check_rule_index.py --toc > code/rules_toc.md`",
            "     Do NOT hand-edit. Canonical rule text lives in code/template_reference.md. -->",
            "",
            "# v-rule jump-map (table of contents for template_reference.md)",
            "",
            f"{len(rows)} rules, in document order. `line` = where the rule's section starts.",
            "",
            "| v-rule | line | heading |",
            "|---|---|---|",
        ]
        for line, rule, heading in rows:
            safe = heading.replace("|", "/").strip()
            out.append(f"| {rule} | {line} | {safe} |")
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
        print("\n".join(out))
        return 0

    def_set = set(defined)
    missing = sorted(def_set - indexed, key=lambda r: int(r[1:]))   # defined, not indexed
    dangling = sorted(indexed - def_set, key=lambda r: int(r[1:]))  # indexed, no heading

    result = {
        "defined_count": len(def_set),
        "indexed_count": len(indexed),
        "missing_from_index": missing,
        "indexed_without_heading": dangling,
        "missing_detail": {
            r: {"line": best_location(r, defined[r])[0],
                "heading": best_location(r, defined[r])[1]} for r in missing
        },
    }

    if args.json:
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 1 if missing else 0

    if not args.quiet:
        print(f"v-rule index coverage  —  masters: {len(def_set)} defined  "
              f"|  index: {len(indexed)} listed\n")
        if missing:
            print(f"✗ {len(missing)} DEFINED but MISSING from the index "
                  f"(the scan-before-inventing gate is blind on these):")
            for r in missing:
                d = defined[r][0]
                print(f"    {r:<7} L{d[0]:<6} {d[1][:78]}")
        else:
            print("✓ every defined rule is in the index")
        if dangling:
            print(f"\n⚠ {len(dangling)} INDEXED but NO heading anchor in masters "
                  f"(dead pointer or prose-only def — verify):")
            print("    " + " ".join(dangling))
        print()
        print("RESULT:", "FAIL — index incomplete" if missing else "PASS")

    return 1 if missing else 0


if __name__ == "__main__":
    raise SystemExit(main())
