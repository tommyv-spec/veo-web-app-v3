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
  4. ROW OVER THE LENGTH BUDGET    -> conventions.md stopped being an index
                                      and became a second rulebook
Exit is nonzero if either index has a gap or any row busts its budget.

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


# --------------------------------------------------------------------------
# ROW LENGTH BUDGET  (P4, 2026-08-30)
#
# conventions.md promises "one row per v-rule". Rows kept growing into full
# deep-dives — measured before this check: 218 rows, median 418 chars, p90
# 1680, longest 17916, and 117 rows over 300. That prose is a SECOND copy of
# the canonical section the row already points at, so it goes stale silently
# and it is paid for on every read of the index.
#
# The budget is per ROW, not per file: a rule earns one plain-language line
# plus its source + canonical link. Deep-dive content belongs in the canonical
# home named in the row's last cell.
#
# 300 chars: measured, the non-summary part of a row (rule id + source cell +
# canonical link + pipes) has a median of ~130 chars, which leaves ~170 for an
# honest one-line summary.
ROW_BUDGET = 300

# Rows that legitimately stay over budget: they carry information that is NOT
# in the canonical home they point at, so trimming them would DELETE knowledge.
# Each entry is named + dated + says what the unique content is. These are the
# repair backlog for the separate rule-content track — an entry here is a debt,
# not a pass. Key = the row's first-cell v-token exactly as written.
ROW_BUDGET_EXCEPTIONS: dict[str, str] = {
    # (empty) — the v888 entry was deleted 2026-08-30 when its own repair
    # instruction was carried out: §v888 now exists in template_reference.md
    # (with the stale "UI CONFLICT (unresolved)" clause marked RESOLVED by
    # v890.5 / b0c84cf), so the conventions row was trimmed to the budget and
    # the excuse is no longer needed. Repair matrix: docs/audits/
    # repair-matrix-2026-08-30.md §2, Wave 2.
}


def index_rows(text: str) -> list[tuple[int, str, str]]:
    """(line_no, first-cell v-token, full source line) for every v-rule row.

    Same row definition the coverage check uses (a table row whose FIRST cell
    names a v-rule), so the two checks can never disagree about what a row is.
    """
    out: list[tuple[int, str, str]] = []
    for i, line in enumerate(text.splitlines(), 1):
        s = line.lstrip()
        if not s.startswith("|") or s.count("|") < 2:
            continue
        first_cell = s.split("|")[1]
        vm = VTOKEN.search(first_cell)
        if not vm:
            continue
        out.append((i, vm.group(0).lower(), line))
    return out


def row_budget_report(text: str, budget: int = ROW_BUDGET,
                      exceptions: dict[str, str] | None = None) -> dict:
    """Every row over `budget`, split into real offenders and named exceptions.

    Reports ALL offenders — never stops at the first — because a checker that
    reports one fault per run trains you to fix them one commit at a time.
    """
    exc = ROW_BUDGET_EXCEPTIONS if exceptions is None else exceptions
    rows = index_rows(text)
    over = [(ln, tok, len(line)) for ln, tok, line in rows if len(line) > budget]
    offenders = [r for r in over if r[1] not in exc]
    excepted = [r for r in over if r[1] in exc]
    # an exception key that no longer matches an over-budget row is stale —
    # report it so the list cannot quietly outlive the problem it excused
    over_toks = {t for _, t, _ in over}
    stale = sorted(k for k in exc if k not in over_toks)
    lens = sorted(len(line) for _, _, line in rows)
    return {
        "budget": budget,
        "row_count": len(rows),
        "max_len": lens[-1] if lens else 0,
        "median_len": lens[len(lens) // 2] if lens else 0,
        "offenders": offenders,
        "excepted": excepted,
        "stale_exceptions": stale,
    }


# Rules that are real + classified in build-rule-index but have NO defining
# heading in template_reference.md (they live in the decode pipeline / skeleton,
# e.g. code/CLAUDE.md). Reasoned exception MAP (rule -> why it's legit), so the
# unknown-ID check does not flag them and the reason is auditable.
KNOWN_NON_HEADING_RULES = {
    "v578": "whisper transcription — decode pipeline; defined in code/CLAUDE.md "
            "+ template_new_format, not a template_reference heading",
    "v585": "Farneback motion detection — decode pipeline; same as v578",
    "v890": "decode start-frame-spec family; canonical = stage4d schema "
            "(v589_video_understanding.py) + decode-grammar-checklist + "
            "verify_decode_format linter; code-side v890.x is the unrelated "
            "beat-align UI (v861 note); 2026-08-30",
    "v893": "split-lane decode; canonical = decode-grammar-checklist "
            "§SPLIT-LANE + READING_PROFILES; code-side v893 is the unrelated "
            "Flow error-page detection (v861 note); 2026-08-30",
}

# Rules legitimately classified in MORE THAN ONE primary bucket (§A/§B/§C)
# because their sub-rules split by nature. Reasoned map (rule -> why), so a
# multi-bucket rule not listed here is a real conflict and FAILs.
DUAL_BUCKET_ALLOWLIST = {
    "v621": "v621b caption-ban is §A authoring; v621a decoder-narrative-lens is §B decode",
    "v622": "base v622 preserves decoded symptom scale + intensity/headroom in §B; v622b applies geometric anti-normalization when authoring generated symptom frames in §A",
    "v708": "zero-word-loss contract is §A authoring; its export implementation is §C platform",
    "v712": "Banana-2 attached-ref composition is §A authoring; Stage-4d relational is §B decode",
    "v718": "morph-diagnostics §A authoring; VLM forensic-perception §B decode; plumbing §C platform",
    "v867": "test-axis fields: §0 TEST AXES declaration line is §A authoring; the YAML frontmatter keys on new decodes are §B decode",
    "v934": "the two layers ARE the rule: the model-agnostic performance/action read is §B decode; the Veo prompt dialect that renders it is §A authoring, and swapping the video model changes only the §A half",
}

_SECTION_RX = {
    "A": re.compile(r"^##\s+§A\b"),
    "B": re.compile(r"^##\s+§B\b"),
    "C": re.compile(r"^##\s+§C\b"),
    "D": re.compile(r"^##\s+§D\b"),
}
_SUPERSEDED_RX = re.compile(r"\*\*Superseded\s*/\s*folded", re.IGNORECASE)


def build_index_buckets(text: str) -> tuple[dict[str, dict[str, int]], dict[str, dict[str, int]]]:
    """Parse build-rule-index.md → (base_map, full_map).
      base_map: base-rule -> {bucket -> # of rows/items} (CLASSIFICATION token
                only, not cross-references).
      full_map: bucket -> {LEADING full-token -> count} — the subject token of
                each row/item, for duplicate detection. Using the leading token
                (not every token) means a family spread across rows (v718a-h /
                v718i / v718j) is not a dup, while a genuinely repeated row is.

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
    # base classification: base-rule -> {bucket: # of rows/items}
    out: dict[str, dict[str, int]] = {}
    # full-token counts: bucket -> {full-token: count} (for duplicate detection —
    # a family that spans rows via distinct sub-versions v718a/v718i/v718j is NOT
    # a duplicate; the SAME full token listed twice IS).
    full: dict[str, dict[str, int]] = {}

    def add_cell(cell: str, bucket: str) -> None:
        toks = [vm.group(0).lower() for vm in VTOKEN.finditer(cell)]
        for r in {base(re.match(r"v(\d{3})", t).group(1)) for t in toks}:
            out.setdefault(r, {})[bucket] = out.setdefault(r, {}).get(bucket, 0) + 1
        if toks:                                  # LEADING token = the row/item subject
            fb = full.setdefault(bucket, {})
            fb[toks[0]] = fb.get(toks[0], 0) + 1

    a0, b0 = pos.get("A"), pos.get("B")
    if a0 is not None:                            # §A — table first cell
        for l in lines[a0: (b0 if b0 is not None else end)]:
            s = l.lstrip()
            if s.startswith("|") and s.count("|") >= 2:
                add_cell(s.split("|")[1], "A")

    def prose(start: int, stop: int, bucket: str) -> None:  # §B/§C — before each '·' item's '('
        for item in "\n".join(lines[start:stop]).split("·"):
            add_cell(item.split("(")[0], bucket)

    c0 = pos.get("C")
    if b0 is not None:
        prose(b0, (c0 if c0 is not None else end), "B")
    if c0 is not None:
        c_stop = sup_i if (sup_i is not None and c0 < sup_i < end) else end
        prose(c0, c_stop, "C")

    if sup_i is not None:                         # superseded — first token per '·' item
        body = lines[sup_i].split(":**", 1)[-1]
        for item in body.split("·"):
            vm = VTOKEN.search(item)
            if vm:
                r = base(vm.group(1))
                out.setdefault(r, {})["superseded"] = out.setdefault(r, {}).get("superseded", 0) + 1
                fb = full.setdefault("superseded", {})
                fb[vm.group(0).lower()] = fb.get(vm.group(0).lower(), 0) + 1

    return out, full


def build_index_report(def_set: set[str], text: str) -> dict:
    """Compare build-rule-index classification against the defined rules.
    All of missing / unknown / duplicate / conflict / contradiction are FAILs
    (conflict only when NOT in the reasoned DUAL_BUCKET_ALLOWLIST)."""
    buckets, full = build_index_buckets(text)
    classified = set(buckets)
    primary = {"A", "B", "C"}

    def bset(r: str) -> set[str]:
        return {b for b, c in buckets[r].items() if c > 0}

    missing = sorted(def_set - classified, key=lambda r: int(r[1:]))
    unknown = sorted(
        (r for r in classified - def_set if r not in KNOWN_NON_HEADING_RULES),
        key=lambda r: int(r[1:]),
    )
    # the SAME full token listed 2+ times in one bucket = a real duplicate row
    # (distinct sub-versions of a family across rows are NOT duplicates)
    dup_bases = {base(re.match(r"v(\d{3})", t).group(1))
                 for counter in full.values() for t, c in counter.items() if c > 1}
    duplicate = sorted(dup_bases, key=lambda r: int(r[1:]))
    # a rule in >1 PRIMARY bucket that is NOT a reasoned dual-nature exception
    conflict = sorted(
        (r for r in buckets
         if len(bset(r) & primary) > 1 and r not in DUAL_BUCKET_ALLOWLIST),
        key=lambda r: int(r[1:]),
    )
    # a rule listed as superseded AND also active in a primary bucket
    contradiction = sorted(
        (r for r in buckets if "superseded" in bset(r) and (bset(r) & primary)),
        key=lambda r: int(r[1:]),
    )
    return {
        "buckets": {r: sorted(bset(r)) for r in buckets},
        "missing": missing,
        "unknown": unknown,
        "duplicate": duplicate,
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

    # 8. DUPLICATE — same rule listed twice in the same bucket (§A)
    r = build_index_report({"v100"}, _mini_index(a=["v100", "v100"]))
    check("duplicate:same-bucket-detected", r["duplicate"] == ["v100"])

    # 9. ALLOWLISTED CONFLICT — a reasoned dual-nature rule (v708) in §A+§C is OK
    r = build_index_report({"v708"}, _mini_index(a=["v708"], c=["v708"]))
    check("conflict:allowlisted-ok", r["conflict"] == [] and r["duplicate"] == [])

    # 10. NON-ALLOWLISTED CONFLICT — a plain rule in two primary buckets FAILs
    r = build_index_report({"v300"}, _mini_index(a=["v300"], b=["v300"]))
    check("conflict:non-allowlisted-flagged", r["conflict"] == ["v300"])

    # --- row-length budget (P4) ---
    short = "| v100 | short summary | src | `canonical.md` |"
    longr = "| v200 | " + ("x" * 400) + " | src | `canonical.md` |"
    longr2 = "| v300 | " + ("y" * 500) + " | src | `canonical.md` |"
    tbl = "\n".join(["| v-rule | s | src | canon |", "|---|---|---|---|",
                     short, longr, longr2])

    r = row_budget_report(tbl, budget=300, exceptions={})
    check("budget:counts-all-rows", r["row_count"] == 3)
    # ALL offenders reported, not just the first
    check("budget:reports-every-offender",
          [t for _, t, _ in r["offenders"]] == ["v200", "v300"])
    check("budget:short-row-not-flagged",
          "v100" not in [t for _, t, _ in r["offenders"]])
    check("budget:reports-line-and-length",
          all(isinstance(ln, int) and L > 300 for ln, _, L in r["offenders"]))

    # a named exception moves a row out of offenders, not out of sight
    r = row_budget_report(tbl, budget=300, exceptions={"v200": "reason, 2026-08-30"})
    check("budget:exception-clears-offender",
          [t for _, t, _ in r["offenders"]] == ["v300"])
    check("budget:exception-still-listed",
          [t for _, t, _ in r["excepted"]] == ["v200"])
    check("budget:no-stale-when-used", r["stale_exceptions"] == [])

    # an exception for a row that is now under budget is STALE and reported
    r = row_budget_report(tbl, budget=300, exceptions={"v100": "obsolete, 2026-08-30"})
    check("budget:stale-exception-detected", r["stale_exceptions"] == ["v100"])

    # a row header / separator line must never count as a rule row
    r = row_budget_report("| v-rule | one | scope |\n|---|---|---|\n" + short,
                          budget=300, exceptions={})
    check("budget:ignores-header-and-separator", r["row_count"] == 1)

    # the real index parses and the check runs on it
    if INDEX.exists():
        rr = row_budget_report(INDEX.read_text(encoding="utf-8"))
        check("budget:real-index-parses", rr["row_count"] > 100)

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
    index_text = INDEX.read_text(encoding="utf-8")
    indexed = indexed_rules(index_text)

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
    # (not presence-only): missing / unknown-ID / duplicate / superseded-
    # contradiction all FAIL; a multi-primary-bucket conflict FAILs too unless
    # the rule is in the reasoned DUAL_BUCKET_ALLOWLIST.
    bri_present = BUILD_INDEX.exists()
    bri = {"missing": [], "unknown": [], "duplicate": [], "conflict": [],
           "contradiction": [], "buckets": {}}
    if bri_present:
        bri = build_index_report(def_set, BUILD_INDEX.read_text(encoding="utf-8"))

    # Third target: the ROW LENGTH BUDGET on conventions.md — the index only
    # stays an index if a row stays a row.
    budget = row_budget_report(index_text)

    fail = (bool(missing) or bool(bri["missing"]) or bool(bri["unknown"])
            or bool(bri["duplicate"]) or bool(bri["conflict"])
            or bool(bri["contradiction"])
            or bool(budget["offenders"]) or bool(budget["stale_exceptions"]))

    result = {
        "row_budget": budget["budget"],
        "row_count": budget["row_count"],
        "row_median_len": budget["median_len"],
        "row_max_len": budget["max_len"],
        "rows_over_budget": [{"line": ln, "rule": r, "len": L}
                             for ln, r, L in budget["offenders"]],
        "rows_over_budget_excepted": [{"line": ln, "rule": r, "len": L}
                                      for ln, r, L in budget["excepted"]],
        "stale_row_budget_exceptions": budget["stale_exceptions"],
        "defined_count": len(def_set),
        "indexed_count": len(indexed),
        "missing_from_index": missing,
        "indexed_without_heading": dangling,
        "build_index_present": bri_present,
        "missing_from_build_index": bri["missing"],
        "build_index_unknown_ids": bri["unknown"],
        "build_index_duplicate": bri["duplicate"],
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
            if bri["duplicate"]:
                clean = False
                print(f"\n✗ {len(bri['duplicate'])} rule(s) listed TWICE in the SAME bucket "
                      f"(duplicate row):")
                print("    " + " ".join(bri["duplicate"]))
            if bri["contradiction"]:
                clean = False
                print(f"\n✗ {len(bri['contradiction'])} rule(s) listed as SUPERSEDED yet also "
                      f"active in a primary bucket (contradiction):")
                print("    " + " ".join(bri["contradiction"]))
            if bri["conflict"]:
                clean = False
                print(f"\n✗ {len(bri['conflict'])} rule(s) in MORE THAN ONE of §A/§B/§C and NOT "
                      f"in the reasoned dual-bucket allowlist (real conflict — split the rows "
                      f"or add a reason to DUAL_BUCKET_ALLOWLIST):")
                for r in bri["conflict"]:
                    print(f"    {r:<7} in §" + " §".join(bri["buckets"][r]))
            if clean:
                allow = [r for r in bri["buckets"] if r in DUAL_BUCKET_ALLOWLIST]
                extra = (f" ({len(allow)} reasoned dual-bucket exception(s): "
                         + ", ".join(sorted(allow)) + ")") if allow else ""
                print("✓ build-rule-index.md classifies every defined rule "
                      "(no missing/unknown/duplicate/conflict/contradiction)" + extra)
        # 3. row length budget on conventions.md
        print(f"\nrow budget — {budget['row_count']} rows, "
              f"median {budget['median_len']} chars, longest {budget['max_len']}, "
              f"budget {budget['budget']}")
        if budget["offenders"]:
            print(f"✗ {len(budget['offenders'])} row(s) OVER the {budget['budget']}-char "
                  f"budget (move the deep-dive prose into the canonical home the row "
                  f"points at, or add a dated ROW_BUDGET_EXCEPTIONS entry saying what "
                  f"is unique about it):")
            for ln, tok, L in budget["offenders"]:
                print(f"    {tok:<10} L{ln:<5} {L:>6} chars  (+{L - budget['budget']})")
        else:
            print(f"✓ every row fits the {budget['budget']}-char budget")
        if budget["excepted"]:
            print(f"\n⚠ {len(budget['excepted'])} row(s) over budget by NAMED exception "
                  f"(unique content with no canonical home yet — repair backlog):")
            for ln, tok, L in budget["excepted"]:
                print(f"    {tok:<10} L{ln:<5} {L:>6} chars — "
                      f"{ROW_BUDGET_EXCEPTIONS[tok]}")
        if budget["stale_exceptions"]:
            print(f"\n✗ {len(budget['stale_exceptions'])} STALE ROW_BUDGET_EXCEPTIONS "
                  f"entr(ies) — the row is under budget now, so delete the excuse:")
            print("    " + " ".join(budget["stale_exceptions"]))
        print()
        print("RESULT:", "FAIL — coverage incomplete" if fail else "PASS")

    return 1 if fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
