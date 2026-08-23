#!/usr/bin/env python3
"""Compare the rule/skeleton masters in THIS checkout against origin/main.

WHY THIS EXISTS (2026-07-30, and it cost a bad deploy to learn):

The shared `code/` checkout had been parked on `dual-backend-phase2` for weeks —
41 commits ahead of main, 75 BEHIND it. Nobody had a way to see that twelve rule
deep-dives (v867, v833.1, v868, v869, v870, v872, v872-legacy, v873-v877) had
never reached main, so anyone reading the canon from a main checkout was missing
weeks of rules.

Worse, the first attempt to fix it synced the masters WHOLESALE from the branch
after checking that "every rule heading on main also exists on the branch". That
check passed and was still wrong: three sections had RICHER TEXT on main, and the
takeover silently dropped 21 lines of main-only work (the full v871 deep-dive, the
anchor-format skeleton template, and the "two literal strings the auditor
hard-checks" note).

So this script answers BOTH questions, and the second one is the one that bites:

    A. which rules exist here but not on main?          (drift — main is stale)
    B. which LINES exist on main but not here?          (loss — a sync would clobber)

Exit 1 if B is non-empty, because that is the destructive direction.

USAGE
    python check_masters_vs_main.py            # compare working tree vs origin/main
    python check_masters_vs_main.py --staged   # compare the STAGED content (pre-commit)
    python check_masters_vs_main.py --ref REF  # compare some other ref vs origin/main
    python check_masters_vs_main.py --fetch    # git fetch origin main first

WHICH SIDE TO COMPARE (2026-08-23)

The pre-commit hook now passes --staged, so it judges WHAT IS BEING COMMITTED
rather than everything sitting in the working tree.

The working tree was the wrong side for a hook, for one reason: this checkout is
shared by several sessions at once (root CLAUDE.md §16.5). A master file that
ANOTHER session has open and half-edited would block a commit that does not
touch it — and did: a one-line correction was blocked by 130 uncommitted lines
belonging to someone else's in-progress rule.

The protection is unchanged for the commit itself. If the staged content drops a
line that origin/main has, the index carries that removal and the gate still
fails. What it no longer does is judge lines nobody is committing yet; those get
caught when whoever owns them stages them.

Run it with no flag to audit the whole working tree — still the right call before
a sync or a deploy.

NEVER sync these files with `git checkout <branch> -- <file>` while B is non-empty.
Append the missing sections instead, then re-run this until B is zero.
"""

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys

MASTERS = ("template_reference.md", "template_new_format.md", "template_omni_master.md")
RULE_HEADING = re.compile(r"(?m)^#{2,4} (v[0-9][0-9a-zA-Z._-]*)")

# The masters live next to this script, inside the `code/` submodule. Every git
# command and every file read is anchored HERE, so the script gives the same
# answer whether you run it from `code/` or as `python code/check_masters_vs_main.py`
# from the wiki root. Before 2026-08-10 it used bare relative paths: from the root
# it read the ROOT repo (which has no upstream) and reported "cannot read
# origin/main — is the remote fetched?", which sent readers off fetching a remote
# that was already fetched.
HERE = os.path.dirname(os.path.abspath(__file__))


def git(*args):
    return subprocess.run(["git", *args], capture_output=True, text=True,
                          encoding="utf-8", cwd=HERE)


def show(ref, path):
    r = git("show", "%s:%s" % (ref, path))
    return r.stdout if r.returncode == 0 else None


def read_local(ref, path, staged=False):
    """The content to compare against main.

    `staged=True` reads the INDEX (`git show :path`) — what the commit in
    progress actually contains. That is the right side for a pre-commit hook:
    see --staged in main() for why the working tree is the wrong one there.
    """
    if ref:
        return show(ref, path)
    if staged:
        # `:path` is stage 0 of the index. A file with no index entry (never
        # added, or deleted) returns non-zero -> None -> the caller SKIPs it,
        # which is the same thing it does for a file missing on either side.
        return show("", path)
    full = os.path.join(HERE, path)
    if not os.path.isfile(full):
        return None
    with open(full, encoding="utf-8") as fh:
        return fh.read()


SUPERSEDED_MANIFEST = os.path.join(HERE, "masters_superseded.json")
_SUPERSEDED_CACHE = None


def _line_hash(line):
    return hashlib.sha256(line.strip().encode("utf-8")).hexdigest()[:16]


def _superseded_hashes(path):
    """Hashes of main-only lines a later amendment deliberately replaced.

    Declared, not inferred: the manifest names the amendment and the reason for
    every retired line, so a reviewer can check the claim. A missing or unreadable
    manifest simply means nothing is exempt.
    """
    global _SUPERSEDED_CACHE
    if _SUPERSEDED_CACHE is None:
        try:
            with open(SUPERSEDED_MANIFEST, encoding="utf-8") as fh:
                _SUPERSEDED_CACHE = json.load(fh)
        except (OSError, ValueError):
            _SUPERSEDED_CACHE = {}
    entry = _SUPERSEDED_CACHE.get(path) or {}
    return set(entry.get("retired_line_sha256_16") or [])


def significant(text):
    """Lines that carry meaning — blank lines and pure whitespace do not count."""
    return [ln.strip() for ln in text.split("\n") if ln.strip()]


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--ref", default=None, help="compare this ref instead of the working tree")
    ap.add_argument("--main", default="origin/main", help="reference to compare against")
    ap.add_argument("--fetch", action="store_true", help="git fetch the main ref first")
    ap.add_argument("--staged", action="store_true",
                    help="compare the STAGED content (the index) instead of the working tree — "
                         "what a pre-commit hook should judge")
    args = ap.parse_args(argv[1:])

    if args.fetch:
        remote, _, branch = args.main.partition("/")
        git("fetch", "-q", remote, branch or "main")

    if show(args.main, MASTERS[0]) is None:
        # Separate the two failures instead of blaming the fetch for both.
        if git("rev-parse", "--verify", "--quiet", args.main).returncode != 0:
            sys.stderr.write(
                "cannot resolve ref %s in %s — run with --fetch, or check the remote name.\n"
                % (args.main, HERE))
        else:
            sys.stderr.write(
                "%s resolves, but it has no %s — wrong ref or the masters moved.\n"
                % (args.main, MASTERS[0]))
        return 2

    branch = git("rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
    counts = git("rev-list", "--left-right", "--count", "%s...HEAD" % args.main).stdout.split()
    behind, ahead = (counts + ["?", "?"])[:2]

    print("=" * 78)
    print("MASTERS vs %s   |   branch: %s   |   ahead %s / behind %s" % (args.main, branch, ahead, behind))
    print("=" * 78)

    drift, loss = {}, {}
    for path in MASTERS:
        theirs, ours = show(args.main, path), read_local(args.ref, path, args.staged)
        if ours is None or theirs is None:
            print("[SKIP] %-26s (missing on one side)" % path)
            continue

        here = {n for n in RULE_HEADING.findall(ours)}
        there = {n for n in RULE_HEADING.findall(theirs)}
        only_here = sorted(here - there)
        if only_here:
            drift[path] = only_here

        theirs_lines = significant(theirs)
        ours_set = set(significant(ours))
        missing = [ln for ln in theirs_lines if ln not in ours_set]
        # A main-only line is LOST unless a deliberate amendment replaced it.
        # The gate cannot tell those apart by itself, so the difference is
        # DECLARED in masters_superseded.json: each retired line is recorded by
        # hash together with the amendment that replaced it and why. Anything
        # main-only and undeclared still blocks — that is the protection.
        retired = _superseded_hashes(path)
        if retired:
            kept = [ln for ln in missing if _line_hash(ln) not in retired]
            skipped = len(missing) - len(kept)
            if skipped:
                print("       %d main-only line(s) declared SUPERSEDED in %s"
                      % (skipped, os.path.basename(SUPERSEDED_MANIFEST)))
            missing = kept
        if missing:
            loss[path] = missing

        print("[%s] %-26s rules here-only: %-3d | %s lines on %s missing here: %d"
              % ("WARN" if only_here else "ok  ", path, len(only_here),
                 "MAIN", args.main, len(missing)))

    if drift:
        print("\nA. RULES DEFINED HERE BUT NOT ON %s — main is stale, these never shipped:" % args.main)
        for path, names in drift.items():
            print("   %s: %s" % (path, " ".join(names)))
        print("   Fix: APPEND those sections to main. Do not take the whole file.")

    if loss:
        total = sum(len(v) for v in loss.values())
        print("\nB. LINES ON %s THAT ARE NOT HERE — %d line(s). A wholesale sync WOULD DESTROY THESE:"
              % (args.main, total))
        for path, lines in loss.items():
            print("   %s (%d):" % (path, len(lines)))
            for ln in lines[:6]:
                print("      %s" % ln[:150])
            if len(lines) > 6:
                print("      … and %d more" % (len(lines) - 6))
        print("\nRESULT: FAIL — this checkout is NOT a superset of %s." % args.main)
        print("Sync by APPENDING the section(s) from A onto main's copy, then re-run.")
        return 1

    print("\nRESULT: PASS — nothing on %s is missing here." % args.main)
    if drift:
        print("Main is behind by the rules listed in A; append them to ship.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
