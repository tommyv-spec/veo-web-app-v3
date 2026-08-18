#!/usr/bin/env python3
"""verify_v892_live.py — is the v892 composite plate actually working in production?

v892.1 and v892.2 shipped with a `[TEMP]` diagnostic each, on the assumption that
someone would read them in the Render log. Nobody can: the Render log is not
reachable from the working environment, and the operator should not have to go
hunting for two lines in it.

They do not need to. The proof is DATABASE state, and the platform API serves it:
a v892 scene produces a `composite_key` clip, and Phase 3a/3b must give it a
`composite_plate` sibling that has BOTH a prompt and a start frame. If the plate
row is missing, Phase 3a never got the node id (the v892.2 bug). If it exists but
has no prompt or no start frame, Phase 3b did not run (the v892.1 bug). If it has
both, the whole chain worked.

Usage:
    python code/verify_v892_live.py            # scan recent jobs
    python code/verify_v892_live.py <job_id>   # check one job

Exit 0 = a composite job was found and its plate is complete.
Exit 2 = no composite job exists yet (nothing to judge — not a failure).
Exit 1 = a composite job exists and its plate is broken.
"""
import json
import os
import sys
import urllib.request

BASE = os.environ.get("VEO_APP_URL", "https://kavenobuilder.com").rstrip("/")


def _token():
    for p in (os.path.expanduser("~/veo-worker/.env"), ".env"):
        try:
            with open(p, encoding="utf-8") as fh:
                for line in fh:
                    if "TOKEN" in line.split("=")[0].upper() and "=" in line:
                        return line.split("=", 1)[1].strip().strip("\"'")
        except OSError:
            continue
    return os.environ.get("USER_WORKER_TOKEN", "")


def get(path, tok):
    req = urllib.request.Request(BASE + path, headers={"Authorization": "Bearer " + tok})
    return json.load(urllib.request.urlopen(req, timeout=90))


def judge(job_id, clips):
    """Return (verdict, lines) for one job's clips."""
    keys = [c for c in clips if (c.get("clip_role") or "") == "composite_key"]
    plates = [c for c in clips if (c.get("clip_role") or "") == "composite_plate"]
    if not keys:
        return None, []

    lines = ["job %s — %d composite_key clip(s), %d plate(s)"
             % (job_id[:8], len(keys), len(plates))]
    ok = True
    for k in keys:
        mine = [p for p in plates if p.get("paired_clip_id") == k.get("id")]
        if not mine:
            ok = False
            lines.append("   scene %s: NO PLATE CLIP — Phase 3a never got the plate "
                         "node id (the v892.2 bug)" % k.get("scene_index"))
            continue
        p = mine[0]
        has_prompt = bool((p.get("prompt_text") or "").strip())
        has_frame = bool((p.get("start_frame") or "").strip())
        if has_prompt and has_frame:
            lines.append("   scene %s: plate OK — prompt %d chars, frame %s, status %s"
                         % (k.get("scene_index"), len(p.get("prompt_text") or ""),
                            (p.get("start_frame") or "").split("/")[-1], p.get("status")))
        else:
            ok = False
            lines.append("   scene %s: plate EXISTS but prompt=%s frame=%s — Phase 3b "
                         "did not complete (the v892.1 bug)"
                         % (k.get("scene_index"), has_prompt, has_frame))
    return ok, lines


def main():
    tok = _token()
    if not tok:
        print("no worker token found — set USER_WORKER_TOKEN or ~/veo-worker/.env")
        return 2

    targets = sys.argv[1:] or None
    if targets:
        jobs = [{"id": t} for t in targets if not t.startswith("-")]
    else:
        jobs = get("/api/jobs", tok)
        jobs = sorted(jobs, key=lambda j: j.get("created_at") or "", reverse=True)[:40]

    found = False
    all_ok = True
    for j in jobs:
        try:
            clips = get("/api/jobs/%s/clips" % j["id"], tok)
        except Exception as exc:
            print("  (skipped %s: %s)" % (j["id"][:8], exc))
            continue
        verdict, lines = judge(j["id"], clips)
        if verdict is None:
            continue
        found = True
        all_ok = all_ok and verdict
        for ln in lines:
            print(ln)

    print()
    if not found:
        print("NO COMPOSITE JOB YET — no job carries a composite_key clip, so there is")
        print("nothing to judge. This is not a pass and not a failure. Promote a build")
        print("with a `- **composite_plate_image:**` scene and run this again.")
        return 2
    if all_ok:
        print("PASS — every composite scene has a plate clip with a prompt AND a start")
        print("frame. v892.1 + v892.2 are confirmed working in production; the two")
        print("[TEMP] diagnostics can be stripped.")
        return 0
    print("FAIL — see above. The plate did not come through.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
