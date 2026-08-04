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
    python check_deploy_safety.py --ack           # acknowledge THIS exact loss

ACKNOWLEDGING A DELIBERATE REFACTOR (--ack, added 2026-08-03)
-------------------------------------------------------------
Any refactor that moves or rewords a line reads as "loss" here — that is the
tripwire working as designed. The problem was the escape hatch: --allow-loss is
a blanket flag that neither the pre-push hook nor deploy.ps1 passes through, so
a deliberate refactor forced either `git push --no-verify` (which skips EVERY
check, including syntax and file-deletion, and is blocked in auto mode) or
contorting the code to fake zero loss. Both happened in practice.

`--ack` replaces that with a SCOPED acknowledgment. It reviews the current
loss, then writes `.deploy_ack.json` (gitignored) recording the candidate's
exact TREE hash plus a fingerprint of the exact lost lines. On any later run —
including the one inside the pre-push hook and the one inside deploy.ps1 —
this checker honors the file only when BOTH still match:

  * a new commit changes the tree hash        -> ack is stale, gate FAILS again
  * the loss set changes in any way           -> ack is stale, gate FAILS again
  * syntax errors / unverified inputs         -> never acknowledgeable, FAIL

So the acknowledgment can never outlive the exact push it approved, a blanket
flag can never be left switched on, and the hooked push + deploy.ps1 keep
running every other check. No --no-verify, no code contortion.

Exit 0 = safe to deploy. Exit 1 = content or syntax failure. Exit 2 = the
comparison itself could not be proved; deploy remains blocked.
"""

import argparse
import ast
from collections import Counter
import hashlib
import json
import os
import subprocess
import sys

ACK_FILENAME = ".deploy_ack.json"

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


def ack_path():
    """The ack file lives at the repo toplevel (gitignored), so the manual run,
    the pre-push hook (cwd = repo root, script piped via stdin) and deploy.ps1
    (Push-Location repo root) all resolve the same file."""
    r = git("rev-parse", "--show-toplevel")
    top = r.stdout.strip() if r.returncode == 0 and r.stdout.strip() else "."
    return os.path.join(top, ACK_FILENAME)


def loss_fingerprint(ref, losses, gone):
    """(tree_sha, digest) for the candidate tree + the EXACT loss set.

    Bound to the tree hash, not the commit hash, so an amend that produces
    byte-identical content stays acknowledged while any real change stales it.
    Duplicate lost lines are counted, not deduped — losing a second copy of a
    repeated line is a different loss set than losing one."""
    tree = git("rev-parse", "%s^{tree}" % ref).stdout.strip()
    h = hashlib.sha256()
    h.update(tree.encode("utf-8"))
    for path in sorted(gone):
        h.update(("\x00GONE\x00%s" % path).encode("utf-8"))
    for path in sorted(losses):
        for line in sorted(losses[path]):
            h.update(("\x00LOST\x00%s\x00%s" % (path, line)).encode("utf-8"))
    return tree, h.hexdigest()


def read_ack():
    try:
        with open(ack_path(), "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except (OSError, ValueError):
        return None


def write_ack(tree, digest, n_lost, n_gone):
    payload = {
        "tree": tree,
        "fingerprint": digest,
        "lost_lines": n_lost,
        "deleted_files": n_gone,
        "note": "scoped deploy acknowledgment — valid ONLY for this exact tree "
                "and this exact loss set; stales itself on any new commit",
    }
    with open(ack_path(), "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
        f.write("\n")


def _safe(s):
    """Console-safe text. Never let a non-ASCII source line kill the gate."""
    try:
        enc = (getattr(sys.stdout, "encoding", None) or "utf-8")
        return s.encode(enc, "replace").decode(enc, "replace")
    except Exception:
        return s.encode("ascii", "replace").decode("ascii")


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--ref", default="HEAD")
    ap.add_argument("--main", default="origin/main")
    ap.add_argument("--fetch", action="store_true")
    ap.add_argument("--allow-loss", action="store_true",
                    help="report content loss but do not fail (deliberate deletions)")
    ap.add_argument("--ack", action="store_true",
                    help="acknowledge the CURRENT loss set: write %s scoped to "
                         "this exact tree + these exact lines; the hooked push "
                         "and deploy.ps1 then pass without --no-verify" % ACK_FILENAME)
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
                # The gate must never die on its own output. A lost line can
                # contain anything the source does — an emoji in a UI label was
                # enough to raise UnicodeEncodeError on a cp1252 console and
                # block every push touching that line (2026-08-05, the 🥁 in the
                # export modal). Degrade the character, never the check.
                print("      %s" % _safe(ln[:140]))
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

    # ---- scoped acknowledgment (--ack / .deploy_ack.json) -----------------
    # Only a pure loss can be acknowledged. Broken python or unverified inputs
    # stay hard failures no matter what — an ack must never widen past loss.
    acknowledged = False
    ack_stale = None
    if (losses or gone) and not (broken or unverified):
        tree, digest = loss_fingerprint(args.ref, losses, gone)
        if args.ack:
            write_ack(tree, digest, n_lost, len(gone))
            acknowledged = True
            print("\nACK WRITTEN: %s" % ack_path())
            print("Scoped to tree %s + the %d line(s) / %d file(s) above."
                  % (tree[:7], n_lost, len(gone)))
            print("Any new commit or any change to the loss set stales it.")
        else:
            data = read_ack()
            if data is not None:
                if data.get("tree") == tree and data.get("fingerprint") == digest:
                    acknowledged = True
                else:
                    ack_stale = ("%s exists but does not match this push "
                                 "(tree or loss set changed since --ack)." % ACK_FILENAME)
    elif args.ack:
        if broken or unverified:
            print("\n--ack refused: syntax errors / unverified inputs are never acknowledgeable.")
        else:
            print("\n--ack: nothing to acknowledge — no content loss in this push.")

    fail = bool(broken or unverified) or ((losses or gone) and not args.allow_loss)
    if fail and acknowledged:
        # A matching ack answers the LOSS only; syntax / unverified failures
        # (already excluded above before an ack can exist) would still fail.
        fail = False
    if fail:
        print("\nRESULT: FAIL — do not deploy this.")
        if losses or gone:
            if ack_stale:
                print("STALE ACK: %s" % ack_stale)
            print("If the removal is DELIBERATE, re-run with --allow-loss (or push --no-verify).")
            print("PREFERRED: python check_deploy_safety.py --ack  — a scoped acknowledgment")
            print("for exactly this tree + this loss set; the hooked push and deploy.ps1")
            print("then pass with every other check still enforced (no --no-verify).")
            print("If it is not, restore the missing lines and re-run.")
        return 1

    if (losses or gone) and acknowledged and not args.allow_loss:
        print("\nRESULT: PASS (loss acknowledged via %s — scoped to this exact tree)."
              % (ACK_FILENAME if not args.ack else "--ack"))
        return 0
    if losses or gone:
        print("\nRESULT: PASS (loss acknowledged via --allow-loss).")
    else:
        print("\nRESULT: PASS — nothing on %s is lost, changed python parses." % args.main)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
