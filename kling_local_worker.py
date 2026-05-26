#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Local Kling (Higgsfield) variant worker.

Runs on the OPERATOR'S machine (residential IP, authenticated `higgsfield` CLI)
alongside the Flow worker. Drains the platform's Kling-variant queue:

  1. GET  /api/user-worker/clips/kling-pending   → clips needing a Kling variant
  2. download each clip's start frame
  3. `higgsfield generate create kling3_0 --start-image <frame> --sound on --wait --json`
     → Kling 3.0 video WITH audio (works here; the headless server can't reach it)
  4. download the result mp4
  5. POST /api/user-worker/jobs/{job_id}/upload-video/{clip_index}  → appears as a clip variant

Why local: Kling 3.0 lives only on Higgsfield's private web API (Clerk-auth +
Cloudflare). It works from your machine via the CLI, but a datacenter (Render)
IP gets Cloudflare-blocked. So we generate where it works and push the result in.

Setup (once):
  pip install requests
  npm install -g @higgsfield/cli   &&   higgsfield auth login
  set USER_WORKER_TOKEN  (same token your Flow worker uses)
  set HF_PLATFORM_URL    (e.g. https://veo-web-app-v3.onrender.com)

Run:
  python kling_local_worker.py
"""
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import time

import requests

PLATFORM_URL = os.environ.get("HF_PLATFORM_URL", "https://veo-web-app-v3.onrender.com").rstrip("/")
TOKEN = os.environ.get("USER_WORKER_TOKEN", "").strip()
HF_CLI = os.environ.get("HF_CLI", "higgsfield")
MODEL = os.environ.get("HF_KLING_MODEL", "kling3_0")
SOUND = os.environ.get("HF_KLING_SOUND", "on")          # on | off
MODE = os.environ.get("HF_KLING_MODE", "")               # "", std, pro, 4k
WORKER_ID = os.environ.get("WORKER_ID", f"kling-{socket.gethostname()}-{os.getpid()}")
POLL_INTERVAL = int(os.environ.get("KLING_POLL_INTERVAL", "15"))
GEN_TIMEOUT = int(os.environ.get("KLING_GEN_TIMEOUT", "900"))

HEADERS = {"Authorization": f"Bearer {TOKEN}"}
_URL_RE = re.compile(r"https?://[^\s\"']+\.mp4", re.IGNORECASE)


def log(msg):
    print(f"[kling-local] {msg}", flush=True)


def _find_mp4_url(obj):
    """Recursively find the first .mp4 URL in arbitrary JSON."""
    if isinstance(obj, str):
        m = _URL_RE.search(obj)
        return m.group(0) if m else None
    if isinstance(obj, dict):
        # prefer obvious keys first
        for k in ("result_url", "url", "video_url", "output_url"):
            v = obj.get(k)
            found = _find_mp4_url(v) if v is not None else None
            if found:
                return found
        for v in obj.values():
            found = _find_mp4_url(v)
            if found:
                return found
    if isinstance(obj, (list, tuple)):
        for v in obj:
            found = _find_mp4_url(v)
            if found:
                return found
    return None


def generate_kling(prompt, frame_path, duration):
    """Run the higgsfield CLI for one clip. Returns the result mp4 URL or raises."""
    cmd = [HF_CLI, "generate", "create", MODEL,
           "--prompt", prompt,
           "--start-image", frame_path,
           "--duration", str(duration),
           "--sound", SOUND,
           "--wait", "--wait-timeout", f"{GEN_TIMEOUT}s",
           "--json"]
    if MODE:
        cmd += ["--mode", MODE]
    log(f"CLI: {' '.join(cmd[:6])} … (dur={duration} sound={SOUND})")
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=GEN_TIMEOUT + 60)
    out = (proc.stdout or "") + "\n" + (proc.stderr or "")
    if proc.returncode != 0:
        raise RuntimeError(f"CLI exit {proc.returncode}: {out.strip()[:400]}")
    # Parse JSON if possible, else regex the raw output for an mp4 URL.
    url = None
    try:
        url = _find_mp4_url(json.loads(proc.stdout.strip()))
    except Exception:
        pass
    if not url:
        m = _URL_RE.search(out)
        url = m.group(0) if m else None
    if not url:
        raise RuntimeError(f"no result URL in CLI output: {out.strip()[:400]}")
    return url


def process_clip(clip):
    cid = clip["clip_id"]
    job_id = clip["job_id"]
    clip_index = clip["clip_index"]
    log(f"clip {clip_index} (id={cid}) — downloading start frame")

    # 1. download start frame
    fr = requests.get(clip["start_frame_url"], headers=HEADERS, timeout=60)
    fr.raise_for_status()
    suffix = ".png" if "png" in (clip["start_frame_url"].lower()) else ".jpg"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tf:
        tf.write(fr.content)
        frame_path = tf.name

    try:
        # 2. generate via local CLI
        url = generate_kling(clip.get("prompt") or "", frame_path, int(clip.get("duration") or 5))
        log(f"clip {clip_index} — Kling done, downloading {url[:60]}…")

        # 3. download the result mp4
        mp4 = requests.get(url, timeout=180)
        mp4.raise_for_status()

        # 4. upload as a clip variant (attempt 9 = Kling marker, distinct from Flow's 1.x)
        fname = f"clip_{clip_index}_9.1.mp4"
        up = requests.post(
            f"{PLATFORM_URL}/api/user-worker/jobs/{job_id}/upload-video/{clip_index}",
            headers=HEADERS,
            files={"file": (fname, mp4.content, "video/mp4")},
            timeout=180,
        )
        up.raise_for_status()
        log(f"clip {clip_index} — uploaded Kling variant ✓")
    finally:
        try:
            os.remove(frame_path)
        except Exception:
            pass


def main():
    if not TOKEN:
        log("ERROR: set USER_WORKER_TOKEN (same token as your Flow worker).")
        sys.exit(1)
    # sanity: CLI present?
    try:
        subprocess.run([HF_CLI, "version"], capture_output=True, text=True, timeout=30)
    except Exception as e:
        log(f"ERROR: `{HF_CLI}` CLI not found/working ({e}). Run: npm i -g @higgsfield/cli && higgsfield auth login")
        sys.exit(1)
    log(f"started — platform={PLATFORM_URL} model={MODEL} worker={WORKER_ID}")
    while True:
        try:
            r = requests.get(
                f"{PLATFORM_URL}/api/user-worker/clips/kling-pending",
                headers=HEADERS, params={"worker_id": WORKER_ID}, timeout=30,
            )
            if r.status_code == 401:
                log("ERROR: 401 — bad/expired USER_WORKER_TOKEN.")
                time.sleep(POLL_INTERVAL)
                continue
            r.raise_for_status()
            clips = r.json().get("clips", [])
            if not clips:
                time.sleep(POLL_INTERVAL)
                continue
            log(f"{len(clips)} clip(s) to generate")
            for clip in clips:
                try:
                    process_clip(clip)
                except Exception as e:
                    log(f"clip {clip.get('clip_index')} FAILED: {e}")
        except Exception as e:
            log(f"poll error: {e}")
            time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
