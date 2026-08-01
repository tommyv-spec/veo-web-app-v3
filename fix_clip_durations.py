"""v884 — rewrite every `- **clip_duration_s:**` in a build to the right bucket.

The v861 word table and the v884 char table both live in clip_duration.py; this
script only edits markdown. It never touches anything but the duration bullets.

    python code/fix_clip_durations.py videos/<build>.md          # show the diff
    python code/fix_clip_durations.py videos/<build>.md --write  # apply it

A missing bullet is inserted directly under its line. Forward-only by hand: run
it on a build you are ALREADY working on, never in a sweep over shipped builds
(`feedback_rule-changes-forward-only`).
"""
import re
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from clip_duration import (  # noqa: E402
    count_line_chars,
    count_line_words,
    pick_clip_duration_for_line,
)

LINE_RE = re.compile(r"^(\s*[-*]\s*\*\*line\s*:\*\*)\s*(.+?)\s*$", re.I)
DUR_RE = re.compile(r"^(\s*[-*]\s*\*\*clip_duration_s\s*:\*\*)\s*(.*)$", re.I)
HEADER_RE = re.compile(r"^###\s+(Scene|Image|Clip)\s")


def fix(text):
    """Return (new_text, changes). changes = [(lineno, old, new, line_text)]."""
    out, changes = [], []
    lines = text.split("\n")
    pending = None          # (want, line_text) for the line we just passed
    for i, t in enumerate(lines, 1):
        m = LINE_RE.match(t)
        if m:
            # the previous line never got a duration bullet — insert one
            if pending:
                out.append(_insert(pending, changes, i))
            spoken = m.group(2).strip()
            pending = (pick_clip_duration_for_line(spoken), spoken, i) if spoken else None
            out.append(t)
            continue

        d = DUR_RE.match(t)
        if d and pending:
            want, spoken, _ln = pending
            got = d.group(2).strip()
            if got != str(want):
                changes.append((i, got or "(none)", want, spoken))
            out.append("%s %d" % (d.group(1), want))
            pending = None
            continue

        if HEADER_RE.match(t) and pending:
            out.append(_insert(pending, changes, i))
            pending = None

        out.append(t)

    if pending:
        out.append(_insert(pending, changes, len(lines)))
    return "\n".join(out), changes


def _insert(pending, changes, at):
    want, spoken, _ln = pending
    changes.append((at, "(missing)", want, spoken))
    return "- **clip_duration_s:** %d" % want


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    write = "--write" in sys.argv
    if not args:
        print(__doc__)
        return 2
    path = args[0]
    text = open(path, encoding="utf-8").read()
    new, changes = fix(text)
    if not changes:
        print("no change — every clip_duration_s already matches (v861 + v884)")
        return 0
    for ln, old, want, spoken in changes:
        print("L%-6d %-9s -> %-3d  %2dw/%3dc  %s" % (
            ln, old, want, count_line_words(spoken), count_line_chars(spoken),
            spoken[:58]))
    delta = sum(w for _l, _o, w, _s in changes) - sum(
        int(o) for _l, o, _w, _s in changes if o.isdigit())
    print("\n%d line(s), %+ds total render time%s" % (
        len(changes), delta, "" if write else " -- re-run with --write to apply"))
    if write:
        open(path, "w", encoding="utf-8", newline="").write(new)
        print("written: %s" % path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
