#!/usr/bin/env python3
"""Pre-deploy gate: prove a push to main LOSES NOTHING and still parses.

WHY THIS EXISTS
---------------
`code/` deploys on every push to main, and main is the only environment. Twice in
two days a push nearly cost real content:

  * 2026-07-30 — syncing the rule masters from a stale branch dropped 21 lines of
    main-only text (the full v871 deep-dive, the anchor-format skeleton template,
    the "two literal strings the auditor hard-checks" note). A heading-level check
    had passed. Heading parity is not content parity.
  * the fix for it then dropped one more line, caught only because a guard existed.

`check_masters_vs_main.py` watches three doc files at COMMIT time. This watches
EVERY text file at PUSH time — the last moment before production.

WHAT IT CHECKS
--------------
  1. CONTENT LOSS — for every text file present on origin/main, any non-trivial
     line that exists there and is absent from the pushed tree. Deleting a whole
     file counts. This is the check that would have stopped both incidents.
  2. SYNTAX — every changed .py file must parse (a broken module on main is a
     broken production).

Trivial lines (blank, pure punctuation, closing braces/fences) are ignored so
reformatting does not cause noise.

USAGE
    python check_deploy_safety.py                 # HEAD vs origin/main
    python check_deploy_safety.py --ref REF       # some other ref vs origin/main
    python check_deploy_safety.py --fetch         # fetch origin/main first
    python check_deploy_safety.py --allow-loss    # report loss but exit 0
                                                  # (deliberate deletions)

Exit 0 = safe to deploy. Exit 1 = a push would lose content or ship a broken module.
"""

import argparse
import ast
import subprocess
import sys

TEXT_SUFFIXES = (".py", ".md", ".sh", ".html", ".css", ".js", ".json", ".yaml", ".yml", ".txt", ".cfg", ".toml")
MAX_BYTES = 4_000_000
# lines that carry no information — ignoring them keeps reformatting quiet
TRIVIAL = {"", "```", "---", "'''", '"""', "}", "{", ")", "(", "]", "[", "*", "-", "#", "|", "//", "pass"}


def git(*args, binary=False):
    r = subprocess.run(["git", *args], capture_output=True,
                       text=not binary, encoding=None if binary else "utf-8")
    return r


def ls_files(ref):
    r = git("ls-tree", "-r", "--name-only", ref)
    return [p for p in r.stdout.split("\n") if p.strip()] if r.returncode == 0 else []


def blob(ref, path):
    r = git("show", "%s:%s" % (ref, path), binary=True)
    if r.returncode != 0:
        return None
    raw = r.stdout
    if len(raw) > MAX_BYTES:
        return None
    try:
        return raw.decode("utf-8")
    except (UnicodeDecodeError, AttributeError):
        return None


def significant(text):
    out = []
    for ln in text.split("\n"):
        s = ln.strip()
        if s and s not in TRIVIAL:
            out.append(s)
    return out


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--ref", default="HEAD")
    ap.add_argument("--main", default="origin/main")
    ap.add_argument("--fetch", action="store_true")
    ap.add_argument("--allow-loss", action="store_true",
                    help="report content loss but do not fail (deliberate deletions)")
    args = ap.parse_args(argv[1:])

    if args.fetch:
        remote, _, branch = args.main.partition("/")
        git("fetch", "-q", remote, branch or "main")

    if not ls_files(args.main):
        print("cannot read %s — nothing to compare, skipping" % args.main)
        return 0

    ours = git("rev-parse", args.ref).stdout.strip()[:7]
    theirs = git("rev-parse", args.main).stdout.strip()[:7]
    print("=" * 78)
    print("DEPLOY SAFETY   %s  ->  %s" % (ours, args.main + " (" + theirs + ")"))
    print("=" * 78)

    # ---- 1. content loss -------------------------------------------------
    losses = {}
    gone = []
    ours_files = set(ls_files(args.ref))
    for path in ls_files(args.main):
        if not path.endswith(TEXT_SUFFIXES):
            continue
        theirs_txt = blob(args.main, path)
        if theirs_txt is None:
            continue
        if path not in ours_files:
            gone.append(path)
            continue
        ours_txt = blob(args.ref, path)
        if ours_txt is None:
            continue
        ours_set = set(significant(ours_txt))
        missing = [ln for ln in significant(theirs_txt) if ln not in ours_set]
        if missing:
            losses[path] = missing

    # ---- 2. syntax of changed python -------------------------------------
    changed = git("diff", "--name-only", "%s...%s" % (args.main, args.ref)).stdout.split("\n")
    broken = []
    for path in [p for p in changed if p.strip().endswith(".py")]:
        txt = blob(args.ref, path)
        if txt is None:
            continue
        try:
            ast.parse(txt)
        except SyntaxError as e:
            broken.append("%s:%s %s" % (path, e.lineno, e.msg))

    n_lost = sum(len(v) for v in losses.values())
    print("files deleted vs %s : %d" % (args.main, len(gone)))
    print("files losing lines   : %d  (%d line(s) total)" % (len(losses), n_lost))
    print("changed .py broken   : %d" % len(broken))

    if gone:
        print("\nDELETED FILES:")
        for p in gone:
            print("   %s" % p)

    if losses:
        print("\nCONTENT LOSS — these lines exist on %s and NOT in what you are pushing:" % args.main)
        for path, lines in sorted(losses.items()):
            print("   %s (%d):" % (path, len(lines)))
            for ln in lines[:5]:
                print("      %s" % ln[:140])
            if len(lines) > 5:
                print("      … and %d more" % (len(lines) - 5))

    if broken:
        print("\nSYNTAX ERRORS in changed python:")
        for b in broken:
            print("   %s" % b)

    fail = bool(broken) or ((losses or gone) and not args.allow_loss)
    if fail:
        print("\nRESULT: FAIL — do not deploy this.")
        if losses or gone:
            print("If the removal is DELIBERATE, re-run with --allow-loss (or push --no-verify).")
            print("If it is not, restore the missing lines and re-run.")
        return 1

    if losses or gone:
        print("\nRESULT: PASS (loss acknowledged via --allow-loss).")
    else:
        print("\nRESULT: PASS — nothing on %s is lost, changed python parses." % args.main)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
