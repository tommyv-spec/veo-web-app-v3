#!/usr/bin/env python3
"""Confirm a deploy actually went live — do not guess, poll.

Root `CLAUDE.md` §2: never claim "should work after deploy" without evidence.
`code/` deploys on every push to main and Render takes 2-3 minutes, so the only
proof is `render_commit` on the live health endpoint matching what you pushed.
This makes that mechanical instead of a habit someone remembers.

USAGE
    python verify_deploy.py                 # confirm HEAD is live
    python verify_deploy.py <sha>           # confirm a specific commit is live
    python verify_deploy.py --timeout 600   # wait longer (default 480s)
    python verify_deploy.py --url https://…/api/health

Exit 0 = that commit is serving. Exit 1 = it is not (yet). Exit 2 = bad usage.

NOTE the host is NOT the render.yaml service name (`veo-studio`) — the live host
is veo-web-app-v3.onrender.com. That cost a round of guessing on 2026-07-30.
"""

import argparse
import json
import subprocess
import sys
import time
import urllib.request

DEFAULT_URL = "https://veo-web-app-v3.onrender.com/api/health"


def head_sha():
    r = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, encoding="utf-8")
    return r.stdout.strip() if r.returncode == 0 else ""


def probe(url, timeout=20):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as fh:
            return json.loads(fh.read().decode("utf-8"))
    except Exception as exc:  # network flakiness is expected while a deploy restarts
        return {"_error": str(exc)[:90]}


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("sha", nargs="?", default=None, help="commit that should be live (default: HEAD)")
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--timeout", type=int, default=480, help="seconds to wait (default 480)")
    ap.add_argument("--interval", type=int, default=20)
    args = ap.parse_args(argv[1:])

    want = (args.sha or head_sha()).strip()
    if len(want) < 7 or len(want) > 40 or any(ch not in "0123456789abcdefABCDEF" for ch in want):
        sys.stderr.write("expected a 7-40 character commit SHA\n")
        return 2
    short = want[:7]

    print("waiting for %s to serve %s (timeout %ds)" % (args.url, short, args.timeout))
    deadline = time.time() + args.timeout
    seen = None
    while time.time() < deadline:
        data = probe(args.url)
        if "_error" in data:
            print("  … %s" % data["_error"])
        else:
            live = (data.get("render_commit") or "")
            status = data.get("status", "?")
            if live[:7] != seen:
                seen = live[:7]
                print("  live=%s status=%s" % (seen or "?", status))
            commit_matches = live.startswith(want) or (len(live) >= 7 and want.startswith(live))
            if commit_matches and live[:7] == short:
                if str(status).lower() == "healthy":
                    print("\nDEPLOY CONFIRMED: %s is live and healthy" % short)
                    return 0
                print("  commit matches, but service is not healthy yet (status=%s)" % status)
        time.sleep(args.interval)

    print("\nNOT CONFIRMED: %s never appeared as render_commit (last seen %s)." % (short, seen or "none"))
    print("Do not claim the deploy landed. Check the Render dashboard for a failed build.")
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
