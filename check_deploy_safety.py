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

Exit 0 = safe to deploy. Exit 1 = content or syntax failure. Exit 2 = the
comparison itself could not be proved; deploy remains blocked.
"""

import argparse
import ast
from collections import Counter
import subprocess
import sys

TEXT_SUFFIXES = (
    ".bat", ".cfg", ".css", ".flow", ".gitignore", ".html", ".ini",
    ".js", ".json", ".jsx", ".md", ".ps1", ".py", ".sh", ".sql",
    ".toml", ".ts", ".tsx", ".txt", ".webmanifest", ".xml", ".yaml", ".yml",
)
TEXT_BASENAMES = {"Dockerfile", "Makefile", "Procfile"}
INDENT_SENSITIVE_SUFFIXES = (".py", ".yaml", ".yml")
MAX_BYTES = 32_000_000
# lines that carry no information — ignoring them keeps reformatting quiet
TRIVIAL = {"", "```", "---", "'''", '"""', "}", "{", ")", "(", "]", "[", "*", "-", "#", "|", "//", "pass"}


def git(*args, binary=False):
    r = subprocess.run(["git", *args], capture_output=True,
                       text=not binary, encoding=None if binary else "utf-8")
    return r


def ls_files(ref):
    r = git("ls-tree", "-r", "--name-only", ref)
    if r.returncode != 0:
        raise RuntimeError("cannot list %s: %s" % (ref, r.stderr.strip()[:200]))
    return [p for p in r.stdout.split("\n") if p.strip()]


def blob(ref, path):
    r = git("show", "%s:%s" % (ref, path), binary=True)
    if r.returncode != 0:
        return None, "cannot read blob"
    raw = r.stdout
    if len(raw) > MAX_BYTES:
        return None, "larger than %d bytes" % MAX_BYTES
    try:
        return raw.decode("utf-8"), None
    except (UnicodeDecodeError, AttributeError):
        return None, "not UTF-8 text"


def is_text_path(path):
    name = path.rsplit("/", 1)[-1]
    return (
        path.lower().endswith(TEXT_SUFFIXES)
        or name in TEXT_BASENAMES
        or path.startswith("git-hooks/")
    )


def significant(text, path):
    out = []
    preserve_indent = path.lower().endswith(INDENT_SENSITIVE_SUFFIXES)
    for ln in text.split("\n"):
        s = ln.strip()
        if s and s not in TRIVIAL:
            out.append(ln.rstrip() if preserve_indent else s)
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
        fetched = git("fetch", "-q", remote, branch or "main")
        if fetched.returncode != 0:
            print("cannot refresh %s: %s" % (args.main, fetched.stderr.strip()[:200]))
            return 2

    for label, ref in (("candidate", args.ref), ("protected main", args.main)):
        exists = git("rev-parse", "--verify", "%s^{commit}" % ref)
        if exists.returncode != 0:
            print("cannot read %s ref %s — deploy blocked" % (label, ref))
            return 2

    try:
        main_files = ls_files(args.main)
        ours_files = set(ls_files(args.ref))
    except RuntimeError as exc:
        print("%s — deploy blocked" % exc)
        return 2

    if not main_files:
        print("protected main %s has no files — deploy blocked" % args.main)
        return 2

    ours = git("rev-parse", args.ref).stdout.strip()[:7]
    theirs = git("rev-parse", args.main).stdout.strip()[:7]
    print("=" * 78)
    print("DEPLOY SAFETY   %s  ->  %s" % (ours, args.main + " (" + theirs + ")"))
    print("=" * 78)

    # ---- 1. content loss -------------------------------------------------
    losses = {}
    gone = []
    unverified = []
    for path in main_files:
        if not is_text_path(path):
            continue
        theirs_txt, reason = blob(args.main, path)
        if reason:
            unverified.append("%s on protected main: %s" % (path, reason))
            continue
        if path not in ours_files:
            gone.append(path)
            continue
        ours_txt, reason = blob(args.ref, path)
        if reason:
            unverified.append("%s in candidate: %s" % (path, reason))
            continue
        theirs_counts = Counter(significant(theirs_txt, path))
        ours_counts = Counter(significant(ours_txt, path))
        missing = []
        for line, count in theirs_counts.items():
            missing.extend([line] * max(0, count - ours_counts[line]))
        if missing:
            losses[path] = missing

    # ---- 2. syntax of changed python -------------------------------------
    diff = git("diff", "--name-only", args.main, args.ref, "--")
    if diff.returncode != 0:
        unverified.append("cannot diff protected main against candidate: %s" % diff.stderr.strip()[:200])
        changed = []
    else:
        changed = diff.stdout.split("\n")
    broken = []
    for path in [p for p in changed if p.strip().endswith(".py")]:
        if path not in ours_files:
            continue
        txt, reason = blob(args.ref, path)
        if reason:
            unverified.append("%s syntax: %s" % (path, reason))
            continue
        try:
            ast.parse(txt)
        except SyntaxError as e:
            broken.append("%s:%s %s" % (path, e.lineno, e.msg))

    n_lost = sum(len(v) for v in losses.values())
    print("files deleted vs %s : %d" % (args.main, len(gone)))
    print("files losing lines   : %d  (%d line(s) total)" % (len(losses), n_lost))
    print("changed .py broken   : %d" % len(broken))
    print("files/checks unread  : %d" % len(unverified))

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

    if unverified:
        print("\nUNVERIFIED — safety check could not prove these inputs:")
        for item in unverified:
            print("   %s" % item)

    fail = bool(broken or unverified) or ((losses or gone) and not args.allow_loss)
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
