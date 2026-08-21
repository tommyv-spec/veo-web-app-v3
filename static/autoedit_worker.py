#!/usr/bin/env python3
"""Auto-edit worker — runs on the operator PC, NEVER on Render.

Claims queued AutoEditRuns from the platform, runs the local pipeline
(OpenCV + ffmpeg + whisper + a headless browser for captions), uploads the
finished mp4 back. The server only QUEUES the work — nothing renders while
no worker is polling.

  python code/static/autoedit_worker.py            # claim one run, then exit
  python code/static/autoedit_worker.py --once     # same, said out loud
  python code/static/autoedit_worker.py --watch    # poll forever (default 15s)

Runbook: code/static/AUTOEDIT_WORKER.md
"""
import argparse
import json
import sys
import threading
import time
import traceback
from pathlib import Path

import requests

CODE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(CODE))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
from send_to_platform import resolve_token          # noqa: E402
from autoedit_pipeline import run_autoedit          # noqa: E402

BASE = "https://kavenobuilder.com"
WORK_ROOT = Path.home() / ".kaveno" / "autoedit"

# Short calls are claim / progress / fail — a hung server must not freeze the
# worker. The upload is the only call that legitimately takes minutes.
SHORT_TIMEOUT = 60
UPLOAD_TIMEOUT = 900

# The server treats a run with no heartbeat for 5 minutes as abandoned and lets
# another worker take it. `captions` can run far longer than that with no
# progress() call, so we ping on our own while a run is being handled.
HEARTBEAT_SECS = 60


def api(method, path, token, timeout=SHORT_TIMEOUT, **kw):
    r = requests.request(method, f"{BASE}{path}",
                         headers={"Authorization": f"Bearer {token}"},
                         timeout=timeout, **kw)
    r.raise_for_status()
    return r.json()


def handle(run, token):
    """Run one job to the end. Never raises — a failure is reported, not fatal."""
    rid, job_id = run["autoedit_id"], run["job_id"]
    work = WORK_ROOT / job_id
    out = work / f"result_{rid[:6]}.mp4"

    state = {"stage": "download"}
    stop = threading.Event()

    def ping(stage):
        api("POST", f"/api/autoedit/{rid}/progress", token, json={"stage": stage})

    def progress(stage):
        state["stage"] = stage
        print(f"[worker] {rid[:6]} {stage}")
        ping(stage)

    def beat():
        # A dropped ping is not fatal — the next one lands 60s later, well
        # inside the 5 minute window.
        while not stop.wait(HEARTBEAT_SECS):
            try:
                ping(state["stage"])
            except Exception as e:
                print(f"[worker] heartbeat failed ({e}) — retrying")

    hb = threading.Thread(target=beat, daemon=True)
    hb.start()
    try:
        try:
            run_autoedit(job_id, work, out, template=run["template"],
                         placement=run["placement"], offset=run["offset"],
                         progress=progress, repairs=run.get("repairs"))
            report_path = work / "qc_report.json"
            qc_report = (report_path.read_text(encoding="utf-8")
                         if report_path.exists() else json.dumps({
                             "schema_version": 1,
                             "verdict": "NEEDS_MANUAL_EDIT",
                             "reasons": ["The worker did not create a quality report"],
                             "checks": [],
                         }))
            # Stop the heartbeat BEFORE any terminal call: /progress sets the
            # run back to "running", so a late ping would undo /complete.
            stop.set()
            with open(out, "rb") as f:
                api("POST", f"/api/autoedit/{rid}/complete", token,
                    timeout=UPLOAD_TIMEOUT,
                    files={"video": (out.name, f, "video/mp4")},
                    data={"qc_report": qc_report})
            verdict = json.loads(qc_report).get("verdict", "NEEDS_MANUAL_EDIT")
            print(f"[worker] DONE {rid} verdict={verdict} -> {out}")
        except Exception:
            # AutoEditError derives from RuntimeError, so it lands here. The
            # pipeline must never raise SystemExit — that would bypass this
            # handler and kill the worker with the run stuck until the stale
            # sweep.
            stop.set()
            err = traceback.format_exc()
            print(f"[worker] FAIL {rid}\n{err}")
            try:
                api("POST", f"/api/autoedit/{rid}/fail", token,
                    json={"error": err[-2000:]})
            except Exception as e:
                # Reporting the failure must not become a second failure that
                # takes the worker down. The server reclaims the run after 5
                # minutes of silence anyway.
                print(f"[worker] could not report the failure to the server: {e}")
    finally:
        stop.set()
        hb.join(timeout=5)


def main():
    ap = argparse.ArgumentParser(
        description="Auto-edit worker. Runs on the operator PC only, never on Render.")
    ap.add_argument("--watch", action="store_true",
                    help="keep polling forever; handle every run that appears")
    ap.add_argument("--once", action="store_true",
                    help="claim at most one run, handle it, exit (this is the default)")
    ap.add_argument("--interval", type=int, default=15,
                    help="seconds between polls in --watch mode (default 15)")
    args = ap.parse_args()

    token, source = resolve_token(None)
    if not token:
        print("no platform token found — run: "
              "python code/send_to_platform.py set-token <token>", file=sys.stderr)
        return 2
    print(f"[worker] token: {source}")

    if not args.watch:
        # Default and --once are the same thing: one claim, then out.
        run = api("POST", "/api/autoedit/claim", token)
        if run.get("autoedit_id"):
            handle(run, token)
        else:
            print("[worker] queue empty")
        return 0

    print(f"[worker] watching {BASE} every {args.interval}s "
          f"— nothing renders while this is not running")
    while True:
        try:
            run = api("POST", "/api/autoedit/claim", token)
        except Exception as e:
            # Server restart or network drop must not end the loop.
            print(f"[worker] claim failed ({e}) — retrying in {args.interval}s")
            time.sleep(args.interval)
            continue
        if run.get("autoedit_id"):
            handle(run, token)
            continue          # take the next one right away
        time.sleep(args.interval)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n[worker] stopped")
        sys.exit(0)
