#!/usr/bin/env python3
"""Pre-deploy gate: prove a push to main REWINDS NOTHING and still parses.

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

THE v898 SPLIT (2026-08-03, operator-approved)
----------------------------------------------
The original line-loss check did TWO jobs with one blunt test, and every FAIL
after install was the wrong job firing: deliberate replacements (reviewed,
tested, authored on top of main) were blocked exactly like stale-tree clobbers,
forcing --ack ceremony or "line-additive" code contortions. The jobs are now
split:

  * STALE-TREE REWIND (the incident class) is caught MECHANICALLY: the
    candidate must be a descendant of protected main
    (`git merge-base --is-ancestor`). A push from a tree whose history never
    contained main's newest lines fails HARD and is NOT acknowledgeable —
    rebase it, don't ack it.
  * DELIBERATE EDITS no longer fail. When ancestry holds, every change vs main
    was authored in a commit being pushed, so line differences are reported as
    a REPLACEMENT ACCOUNTING (each lost line printed next to the added line
    that most resembles it, or VANISHED when nothing does) for the deploy log.
    Whoever deploys reviews that accounting — that is the review step, moved
    from a blocking gate into the deploy transcript.

WHAT STILL FAILS
----------------
  1. ANCESTRY — candidate does not descend from protected main (rewind risk).
     Never acknowledgeable.
  2. DELETED FILES — a whole text file present on main and absent from the
     push. Rare + highest blast radius, so it keeps the --ack ceremony.
  3. SYNTAX — every changed .py file must parse. Never acknowledgeable.

Trivial lines (blank, pure punctuation, closing braces/fences) are ignored so
reformatting does not cause noise.

USAGE
    python check_deploy_safety.py                 # HEAD vs origin/main
    python check_deploy_safety.py --ref REF       # some other ref vs origin/main
    python check_deploy_safety.py --fetch         # fetch origin/main first
    python check_deploy_safety.py --allow-loss    # legacy: also exit 0 on deletions
    python check_deploy_safety.py --ack           # acknowledge THIS exact deletion set

ACKNOWLEDGING A DELIBERATE DELETION (--ack, added 2026-08-03; scope narrowed v898)
----------------------------------------------------------------------------------
`--ack` writes `.deploy_ack.json` (gitignored) recording the candidate's exact
TREE hash plus a fingerprint of the exact loss set. On any later run — including
the one inside the pre-push hook and the one inside deploy.ps1 — this checker
honors the file only when BOTH still match:

  * a new commit changes the tree hash        -> ack is stale, gate FAILS again
  * the loss set changes in any way           -> ack is stale, gate FAILS again
  * ancestry / syntax / unverified inputs     -> never acknowledgeable, FAIL

Since v898 an ack is only ever NEEDED for file deletions; it still fingerprints
line losses too so an ack written by this version satisfies the pre-v898 copy of
the checker that the pre-push hook runs until this version lands on main.

Exit 0 = safe to deploy. Exit 1 = ancestry / deletion / syntax failure. Exit 2 =
the comparison itself could not be proved; deploy remains blocked.
"""

import argparse
import ast
from collections import Counter
import difflib
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile

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

    # ---- 0. ancestry — the stale-tree tripwire (v898) --------------------
    # `--is-ancestor` exits 0 (yes), 1 (no), >1 (couldn't tell). A candidate
    # that does not CONTAIN protected main would rewind whatever main gained
    # since the histories diverged — the exact 2026-07-30 incident class. Not
    # acknowledgeable: rebase onto main instead.
    anc = git("merge-base", "--is-ancestor", args.main, args.ref)
    if anc.returncode == 0:
        descendant = True
    elif anc.returncode == 1:
        descendant = False
    else:
        print("cannot establish ancestry between %s and %s: %s — deploy blocked"
              % (args.main, args.ref, anc.stderr.strip()[:200]))
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

    # ---- 1. content loss + replacement accounting ------------------------
    losses = {}
    added = {}      # path -> lines new in the candidate (the replacement pool)
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
            added[path] = [ln for ln, c in ours_counts.items()
                           for _ in range(max(0, c - theirs_counts[ln]))]

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

    # Inline JavaScript in changed .html files (2026-08-24).
    # static/index.html is ~26k lines and ~83% of it is inline JS in two
    # <script> blocks. ast.parse above never saw it, so a syntax error there
    # shipped and broke the page at runtime. `node --check` parses without
    # running. Written inline on purpose: this file is piped in over stdin by
    # the pre-push hook (`git show main:check_deploy_safety.py | python -`),
    # so it has no sibling modules to import.
    html_changed = [p for p in changed
                    if p.strip().lower().endswith((".html", ".htm")) and p in ours_files]
    if html_changed:
        if not shutil.which("node"):
            unverified.append(
                "inline JS in %d changed .html file(s): node is not installed, "
                "so it could not be parsed" % len(html_changed))
        else:
            for path in html_changed:
                txt, reason = blob(args.ref, path)
                if reason:
                    unverified.append("%s inline JS: %s" % (path, reason))
                    continue
                for m in re.finditer(r"<script([^>]*)>(.*?)</script\s*>", txt, re.S | re.I):
                    attrs, body = m.group(1), m.group(2)
                    if re.search(r"""\bsrc\s*=\s*["']""", attrs, re.I) or not body.strip():
                        continue
                    tm = re.search(r"""\btype\s*=\s*["']([^"']*)["']""", attrs, re.I)
                    stype = (tm.group(1) if tm else "").strip().lower()
                    # anything else (text/template, application/json, importmap)
                    # is data, not code — parsing it as JS is a false failure
                    if stype not in ("", "text/javascript", "application/javascript", "module"):
                        continue
                    start_line = txt[: m.start(2)].count("\n") + 1
                    tmp = None
                    try:
                        with tempfile.NamedTemporaryFile(
                            "w", suffix=".mjs" if stype == "module" else ".js",
                            delete=False, encoding="utf-8",
                        ) as fh:
                            fh.write(body)
                            tmp = fh.name
                        proc = subprocess.run(["node", "--check", tmp],
                                              capture_output=True, text=True, errors="replace")
                    except OSError as e:
                        unverified.append("%s inline JS could not be parsed: %s" % (path, e))
                        continue
                    finally:
                        if tmp:
                            try:
                                os.unlink(tmp)
                            except OSError:
                                pass
                    if proc.returncode != 0:
                        err = (proc.stderr or proc.stdout or "").strip().split("\n")
                        detail = next((ln for ln in err if "Error" in ln), err[0] if err else "?")
                        broken.append("%s: inline <script> at line %d — %s"
                                      % (path, start_line, detail.strip()))

    n_lost = sum(len(v) for v in losses.values())
    print("candidate descends from %s : %s" % (args.main, "YES" if descendant else "NO"))
    print("files deleted vs %s : %d" % (args.main, len(gone)))
    print("files losing lines   : %d  (%d line(s) total)" % (len(losses), n_lost))
    print("changed .py/.html broken : %d  (python AST + inline JS via node --check)" % len(broken))
    print("files/checks unread  : %d" % len(unverified))

    if not descendant:
        print("\nSTALE TREE — the candidate's history does not contain %s." % args.main)
        print("Pushing it would REWIND main to before the histories diverged (the")
        print("2026-07-30 incident class). Rebase onto %s; this is never" % args.main)
        print("acknowledgeable.")

    if gone:
        print("\nDELETED FILES:")
        for p in gone:
            print("   %s" % p)

    if losses:
        # v898 — when ancestry holds these are DELIBERATE edits (each removal
        # was authored in a commit being pushed), so they are reported for the
        # deploy log instead of blocking. Each lost line is shown next to the
        # added line that most resembles it; VANISHED = nothing similar was
        # added, the shape a stale copy-paste over a newer file would leave —
        # read those before deploying.
        vanished_n = 0
        header = ("REPLACEMENT ACCOUNTING — lines on %s changed by this push:"
                  if descendant else
                  "CONTENT AT RISK — these lines exist on %s and NOT in what you are pushing:")
        print("\n" + header % args.main)
        for path, lines in sorted(losses.items()):
            print("   %s (%d):" % (path, len(lines)))
            # Each added line may vouch for at most ONE lost line — without
            # consuming the pool, a single new line "replaced" every lost line
            # it loosely resembled and real vanishings went unflagged.
            pool = list(added.get(path, []))
            for ln in lines:
                # The gate must never die on its own output. A lost line can
                # contain anything the source does — an emoji in a UI label was
                # enough to raise UnicodeEncodeError on a cp1252 console and
                # block every push touching that line (2026-08-05, the 🥁 in the
                # export modal). Degrade the character, never the check.
                near = difflib.get_close_matches(ln, pool, n=1, cutoff=0.5)
                if near:
                    pool.remove(near[0])
                    print("      - %s" % _safe(ln.strip()[:110]))
                    print("        -> %s" % _safe(near[0].strip()[:110]))
                else:
                    vanished_n += 1
                    print("      - %s   [VANISHED — no similar line added]"
                          % _safe(ln.strip()[:110]))
        if descendant and vanished_n:
            print("   %d line(s) VANISHED with no replacement — confirm each was"
                  " meant to go before deploying." % vanished_n)

    if broken:
        print("\nSYNTAX ERRORS in changed python / inline JavaScript:")
        for b in broken:
            print("   %s" % b)

    if unverified:
        print("\nUNVERIFIED — safety check could not prove these inputs:")
        for item in unverified:
            print("   %s" % item)

    # ---- scoped acknowledgment (--ack / .deploy_ack.json) -----------------
    # v898: an ack is only ever NEEDED for file deletions. It still fingerprints
    # line losses too, so an ack written by this version also satisfies the
    # pre-v898 checker copy that the pre-push hook runs until this version is
    # itself on main. Ancestry, broken python and unverified inputs stay hard
    # failures no matter what — an ack must never widen past deletions.
    acknowledged = False
    ack_stale = None
    ackable = (losses or gone) and not (broken or unverified) and descendant
    if ackable:
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
        if not descendant:
            print("\n--ack refused: a stale tree is never acknowledgeable — rebase onto %s."
                  % args.main)
        elif broken or unverified:
            print("\n--ack refused: syntax errors / unverified inputs are never acknowledgeable.")
        else:
            print("\n--ack: nothing to acknowledge — no deletions or line changes in this push.")

    # v898 verdict: line losses no longer fail on a descendant tree — they are
    # the accounting above. Deletions keep the ack ceremony. A stale tree fails
    # regardless of everything else.
    fail = (bool(broken or unverified)
            or not descendant
            or (bool(gone) and not args.allow_loss and not acknowledged))
    if fail:
        print("\nRESULT: FAIL — do not deploy this.")
        if not descendant:
            print("Rebase the work onto %s (git rebase %s), then re-run."
                  % (args.main, args.main))
        elif gone and not acknowledged:
            if ack_stale:
                print("STALE ACK: %s" % ack_stale)
            print("If the deletion is DELIBERATE:")
            print("PREFERRED: python check_deploy_safety.py --ack  — a scoped acknowledgment")
            print("for exactly this tree + this deletion set; the hooked push and deploy.ps1")
            print("then pass with every other check still enforced (no --no-verify).")
            print("If it is not, restore the deleted file(s) and re-run.")
        return 1

    if gone and acknowledged:
        print("\nRESULT: PASS (deletion acknowledged via %s — scoped to this exact tree)."
              % ("--ack" if args.ack else ACK_FILENAME))
    elif gone:
        print("\nRESULT: PASS (deletion allowed via --allow-loss).")
    elif losses:
        print("\nRESULT: PASS — %d changed line(s) accounted above; review the"
              " accounting in this deploy log." % n_lost)
    else:
        print("\nRESULT: PASS — nothing on %s is lost, changed python parses." % args.main)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
