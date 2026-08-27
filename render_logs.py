#!/usr/bin/env python3
"""render_logs.py — read the production Render logs from here.

For a week the answer to "did that diagnostic fire?" was unreachable: the
Render log was only visible in the dashboard, so every `[TEMP]` line this
project ships had no reader. Operator: "i don't know where to watch, you can
do it yourself."

The key lives in `~/veo-worker/.env` as RENDER_API_KEY (outside the repo, never
committed) and as a persistent user environment variable, so any shell and any
session can use it. This is the tool that uses it.

Usage:
    python code/render_logs.py                      # last 40 lines
    python code/render_logs.py --text v892          # only lines containing v892
    python code/render_logs.py --text v892 -n 100
    python code/render_logs.py --service veo-web-app-V2-2 --text error
    python code/render_logs.py --services           # list services and exit

Notes:
  * `--text` is Render's own server-side filter, so it searches the whole
    retained window rather than just the tail.
  * Exit 2 = no key configured, with instructions. Exit 1 = API error.
"""
import argparse
import io
import json
import os
import sys
import urllib.parse
import urllib.request

# §9.1.1 — pin the encoding INSIDE the program, never via PYTHONIOENCODING
# (that env var is inherited by child processes and blinds the auditor).
# Log lines carry arrows/emoji; this box's console is cp1252.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

API = "https://api.render.com/v1"
DEFAULT_SERVICE = "veo-web-app-v3"


def load_key():
    key = os.environ.get("RENDER_API_KEY")
    if key:
        return key.strip()
    env = os.path.expanduser("~/veo-worker/.env")
    try:
        for line in io.open(env, encoding="utf-8", errors="ignore"):
            if line.startswith("RENDER_API_KEY="):
                return line.split("=", 1)[1].strip()
    except OSError:
        pass
    return None


def api(key, path, **params):
    if params:
        path += "?" + urllib.parse.urlencode(params, doseq=True)
    req = urllib.request.Request(API + path,
                                 headers={"Authorization": "Bearer " + key,
                                          "Accept": "application/json"})
    return json.load(urllib.request.urlopen(req, timeout=60))


def main():
    p = argparse.ArgumentParser(description="read production Render logs")
    p.add_argument("--text", action="append", default=[],
                   help="server-side substring filter (repeatable)")
    p.add_argument("-n", "--limit", type=int, default=40)
    p.add_argument("--service", default=DEFAULT_SERVICE,
                   help="service name or srv-id (default: %s)" % DEFAULT_SERVICE)
    p.add_argument("--services", action="store_true", help="list services and exit")
    args = p.parse_args()

    key = load_key()
    if not key:
        print("No RENDER_API_KEY configured.")
        print("  Put it in ~/veo-worker/.env as  RENDER_API_KEY=rnd_...")
        print("  (that file is outside the repo and is never committed)")
        return 2

    try:
        services = api(key, "/services", limit=50)
    except Exception as exc:
        print("Render API error: %s" % exc)
        return 1

    resolved = {}
    for s in services:
        svc = s.get("service", s)
        resolved[svc.get("name")] = svc.get("id")
        resolved[svc.get("id")] = svc.get("id")

    if args.services:
        for s in services:
            svc = s.get("service", s)
            print("  %-26s %s  (%s)" % (svc.get("name"), svc.get("id"), svc.get("type")))
        return 0

    srv = resolved.get(args.service)
    if not srv:
        print("Unknown service %r. Known: %s"
              % (args.service, ", ".join(sorted(n for n in resolved if not n.startswith("srv-")))))
        return 1

    owners = api(key, "/owners", limit=1)
    owner_id = (owners[0].get("owner") or owners[0]).get("id")

    params = {"ownerId": owner_id, "resource": srv, "limit": args.limit}
    if args.text:
        params["text"] = args.text
    try:
        res = api(key, "/logs", **params)
    except Exception as exc:
        print("Render API error: %s" % exc)
        return 1

    rows = res.get("logs", res) if isinstance(res, dict) else res
    if not rows:
        print("no matching log lines")
        return 0
    for r in rows:
        ts = (r.get("timestamp") or "")[:19].replace("T", " ")
        msg = r.get("message") or ""      # message can be null; do not len() it blindly
        print("%s  %s" % (ts, msg.rstrip()))
    print("\n%d line(s)%s" % (len(rows), (" matching %s" % args.text) if args.text else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
