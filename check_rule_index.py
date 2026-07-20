#!/usr/bin/env python3
"""check_rule_index.py — the v-rule index coverage linter.

Answers one question the four scattered ledgers could never answer reliably:
"is EVERY defined v-rule listed in the index?"

It compares the DEFINED rules against TWO indexes that both went stale the same
way and now both get guarded:
  - MASTERS  = code/template_reference.md   (the deep-dive — a rule is DEFINED
               when its vNNN shows up in a markdown heading line `^#{1,4} `)
  - INDEX    = wiki/patterns/conventions.md (the one-row-per-rule table —
               INDEXED when its vNNN is in a table row's first cell)
  - BUILD_INDEX = wiki/meta/build-rule-index.md (the /build authoring
               denominator — its §A is a table but §B/§C are prose lists, so a
               rule is CLASSIFIED when its vNNN appears ANYWHERE in the file)

Reports:
  1. DEFINED but NOT INDEXED       -> conventions.md is blind on these
  2. INDEXED but NOT DEFINED       -> an index row with no heading anchor
  3. DEFINED but NOT CLASSIFIED    -> build-rule-index.md is blind on these
Exit is nonzero if EITHER index has a gap.

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
BUILD_INDEX = ROOT / "wiki" / "meta" / "build-rule-index.md"

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


# Rules that are real + classified in build-rule-index but have NO defining
# heading in template_reference.md (they live in the decode pipeline / skeleton,
# e.g. code/CLAUDE.md). Allow-listed so the unknown-ID check does not flag them.
KNOWN_NON_HEADING_RULES = {"v578", "v585"}

_SECTION_RX = {
    "A": re.compile(r"^##\s+§A\b"),
    "B": re.compile(r"^##\s+§B\b"),
    "C": re.compile(r"^##\s+§C\b"),
    "D": re.compile(r"^##\s+§D\b"),
}
_SUPERSEDED_RX = re.compile(r"\*\*Superseded\s*/\s*folded", re.IGNORECASE)


def build_index_buckets(text: str) -> dict[str, set[str]]:
    """Parse build-rule-index.md into {base-rule -> set of buckets it is
    CLASSIFIED in}, using the CLASSIFICATION token only (not cross-references):

      - §A: the FIRST table cell of each row (the rule's own row).
      - §B / §C: every vNNN token BEFORE the first '(' in each '·'-separated
        item (handles multi-rule items like 'v739 / v740 / v747 (UI...)').
      - superseded: the FIRST vNNN token of each '·'-item on the
        '**Superseded / folded ...**' line (the '→ vXXX' targets that follow
        are cross-refs, not classifications).

    Intro prose and §D (non-v-rule) are NOT buckets, so range-text like
    'v176→v861' and descriptive cross-refs never count as a classification.
    """
    lines = text.splitlines()
    pos: dict[str, int] = {}
    sup_i = None
    for i, l in enumerate(lines):
        for name, rx in _SECTION_RX.items():
            if rx.match(l):
                pos[name] = i
        if sup_i is None and _SUPERSEDED_RX.search(l):
            sup_i = i
    end = pos.get("D", len(lines))
    out: dict[str, set[str]] = {}

    def add(rule: str, bucket: str) -> None:
        out.setdefault(rule, set()).add(bucket)

    # §A — table first cells
    a0, b0 = pos.get("A"), pos.get("B")
    if a0 is not None:
        for l in lines[a0: (b0 if b0 is not None else end)]:
            s = l.lstrip()
            if s.startswith("|") and s.count("|") >= 2:
                for vm in VTOKEN.finditer(s.split("|")[1]):
                    add(base(vm.group(1)), "A")

    # §B / §C — leading tokens (before first '(') of each '·' item
    def prose(start: int, stop: int, bucket: str) -> None:
        for item in "\n".join(lines[start:stop]).split("·"):
            head = item.split("(")[0]
            for vm in VTOKEN.finditer(head):
                add(base(vm.group(1)), bucket)

    c0 = pos.get("C")
    if b0 is not None:
        prose(b0, (c0 if c0 is not None else end), "B")
    if c0 is not None:
        c_stop = sup_i if (sup_i is not None and c0 < sup_i < end) else end
        prose(c0, c_stop, "C")

    # superseded line — first vNNN of each '·' item (skip the header paren)
    if sup_i is not None:
        body = lines[sup_i].split(":**", 1)[-1]
        for item in body.split("·"):
            vm = VTOKEN.search(item)
            if vm:
                add(base(vm.group(1)), "superseded")
    return out


def build_index_report(def_set: set[str], text: str) -> dict:
    """Compare build-rule-index classification against the defined rules.
    Returns missing / unknown / conflict / contradiction lists + the map."""
    buckets = build_index_buckets(text)
    classified = set(buckets)
    primary = {"A", "B", "C"}

    missing = sorted(def_set - classified, key=lambda r: int(r[1:]))
    unknown = sorted(
        (r for r in classified - def_set if r not in KNOWN_NON_HEADING_RULES),
        key=lambda r: int(r[1:]),
    )
    # a rule classified into more than one PRIMARY bucket (A/B/C)
    conflict = sorted(
        (r for r, bs in buckets.items() if len(bs & primary) > 1),
        key=lambda r: int(r[1:]),
    )
    # a rule listed as superseded AND also active in a primary bucket
    contradiction = sorted(
        (r for r, bs in buckets.items() if "superseded" in bs and (bs & primary)),
        key=lambda r: int(r[1:]),
    )
    return {
        "buckets": {r: sorted(b) for r, b in buckets.items()},
        "missing": missing,
        "unknown": unknown,
        "conflict": conflict,
        "contradiction": contradiction,
    }


def _mini_index(a=(), b=(), c=(), superseded=()) -> str:
    """Build a synthetic build-rule-index.md for self-tests."""
    L = ["# idx", "", "## §A GENERATE-AUTHORING v-rules",
         "| v-rule | one | scope |", "|---|---|---|"]
    L += [f"| {r} | desc | scope |" for r in a]
    L += ["", "## §B DECODE-AUTHORING v-rules",
          " · ".join(f"{r} (decode)" for r in b) or "(none)"]
    L += ["", "## §C PLATFORM-INTERNAL v-rules", "These ship in code/. Listed: "
          + (" · ".join(f"{r} (platform)" for r in c) or "(none)")]
    if superseded:
        L += ["", "**Superseded / folded (note):** "
              + " · ".join(f"{r} (→ v001 target)" for r in superseded)]
    L += ["", "## §D Non-v-rule authoring rules", "- CLAUDE.md mentions v791 as a cross-ref"]
    return "\n".join(L)


def selftest() -> int:
    """Inline tests for the build-rule-index bucket gate (no external deps)."""
    cases = []

    def check(name, cond):
        cases.append((name, bool(cond)))

    # 1. PASS — each defined rule classified in exactly one bucket
    r = build_index_report({"v100", "v200", "v300"},
                           _mini_index(a=["v100"], b=["v200"], c=["v300"]))
    check("pass:no-missing", r["missing"] == [])
    check("pass:no-unknown", r["unknown"] == [])
    check("pass:no-conflict", r["conflict"] == [])
    check("pass:no-contradiction", r["contradiction"] == [])

    # 2. MISSING — a defined rule absent from every bucket
    r = build_index_report({"v100", "v400"}, _mini_index(a=["v100"]))
    check("missing:detected", r["missing"] == ["v400"])

    # 3. UNKNOWN — classified id that is not defined + not allow-listed
    r = build_index_report({"v100"}, _mini_index(a=["v100", "v999"]))
    check("unknown:detected", r["unknown"] == ["v999"])

    # 4. ALLOWLIST — v578 in §B, not defined, must NOT be unknown
    r = build_index_report({"v100"}, _mini_index(a=["v100"], b=["v578"]))
    check("allowlist:v578-not-unknown", r["unknown"] == [])

    # 5. CONFLICT — same rule in two primary buckets (A and C)
    r = build_index_report({"v100"}, _mini_index(a=["v100"], c=["v100"]))
    check("conflict:detected", r["conflict"] == ["v100"])

    # 6. CONTRADICTION — active in §A AND on the superseded line
    r = build_index_report({"v100"}, _mini_index(a=["v100"], superseded=["v100"]))
    check("contradiction:detected", r["contradiction"] == ["v100"])

    # 7. cross-ref inside a description must NOT classify (no false conflict)
    #    §A row for v100 whose summary mentions v200; v200 only lives in §B.
    txt = _mini_index(a=["v100"], b=["v200"]).replace(
        "| v100 | desc | scope |", "| v100 | see v200 for details | scope |")
    r = build_index_report({"v100", "v200"}, txt)
    check("xref:no-false-conflict", r["conflict"] == [] and r["missing"] == [])

    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ok = all(p for _, p in cases)
    for name, passed in cases:
        print(f"  {'PASS' if passed else 'FAIL'}  {name}")
    print("\nSELFTEST:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


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
    ap.add_argument("--selftest", action="store_true",
                    help="run the build-index gate self-tests (pass/missing/unknown/conflict)")
    args = ap.parse_args()

    if args.selftest:
        return selftest()

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

    # Second target: build-rule-index.md (the /build authoring denominator),
    # which went stale exactly like conventions.md did. Real bucket parsing
    # (not presence-only): missing / unknown-ID / superseded-contradiction FAIL;
    # multi-primary-bucket conflict is a WARN (some rules are legit dual-nature).
    bri_present = BUILD_INDEX.exists()
    bri = {"missing": [], "unknown": [], "conflict": [], "contradiction": []}
    if bri_present:
        bri = build_index_report(def_set, BUILD_INDEX.read_text(encoding="utf-8"))

    fail = (bool(missing) or bool(bri["missing"]) or bool(bri["unknown"])
            or bool(bri["contradiction"]))

    result = {
        "defined_count": len(def_set),
        "indexed_count": len(indexed),
        "missing_from_index": missing,
        "indexed_without_heading": dangling,
        "build_index_present": bri_present,
        "missing_from_build_index": bri["missing"],
        "build_index_unknown_ids": bri["unknown"],
        "build_index_conflict": bri["conflict"],
        "build_index_contradiction": bri["contradiction"],
        "missing_detail": {
            r: {"line": best_location(r, defined[r])[0],
                "heading": best_location(r, defined[r])[1]} for r in missing
        },
    }

    if args.json:
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 1 if fail else 0

    if not args.quiet:
        print(f"v-rule index coverage  —  masters: {len(def_set)} defined  "
              f"|  conventions: {len(indexed)} listed\n")
        # 1. conventions.md (the scan-before-inventing index)
        if missing:
            print(f"✗ {len(missing)} DEFINED but MISSING from conventions.md "
                  f"(the scan-before-inventing gate is blind on these):")
            for r in missing:
                d = defined[r][0]
                print(f"    {r:<7} L{d[0]:<6} {d[1][:78]}")
        else:
            print("✓ conventions.md indexes every defined rule")
        if dangling:
            print(f"\n⚠ {len(dangling)} INDEXED but NO heading anchor in masters "
                  f"(dead pointer or prose-only def — verify):")
            print("    " + " ".join(dangling))
        # 2. build-rule-index.md (the /build authoring denominator)
        if not bri_present:
            print("\n⚠ build-rule-index.md not found — skipped its coverage check")
        else:
            clean = True
            if bri["missing"]:
                clean = False
                print(f"\n✗ {len(bri['missing'])} DEFINED but NOT CLASSIFIED in build-rule-index.md "
                      f"(the /build authoring denominator is blind on these):")
                print("    " + " ".join(bri["missing"]))
            if bri["unknown"]:
                clean = False
                print(f"\n✗ {len(bri['unknown'])} UNKNOWN rule id(s) in build-rule-index.md "
                      f"(classified but not defined + not allow-listed — typo?):")
                print("    " + " ".join(bri["unknown"]))
            if bri["contradiction"]:
                clean = False
                print(f"\n✗ {len(bri['contradiction'])} rule(s) listed as SUPERSEDED yet also "
                      f"active in a primary bucket (contradiction):")
                print("    " + " ".join(bri["contradiction"]))
            if bri["conflict"]:
                print(f"\n⚠ {len(bri['conflict'])} rule(s) classified in MORE THAN ONE of §A/§B/§C "
                      f"(dual-nature or mis-file — review):")
                for r in bri["conflict"]:
                    print(f"    {r:<7} in §" + " §".join(bri["buckets"][r]))
            if clean:
                print("✓ build-rule-index.md classifies every defined rule (no unknown/contradiction)")
        print()
        print("RESULT:", "FAIL — coverage incomplete" if fail else "PASS")

    return 1 if fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
