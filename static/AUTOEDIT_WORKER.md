# Auto-edit worker — operator runbook

## What it is

The local half of "auto-edit". Auto-edit turns a finished video job into a
posted-ready video: it downloads the job's export plus its 16:9 b-roll support
track, keys the green-screen hook, drops a rounded PIP of the b-roll, cleans
the voice, and burns word-by-word karaoke captions that dodge faces, the PIP
and the busiest part of the frame.

**The server never renders any of that.** Render has 1 CPU / 2 GB and no
OpenCV, no whisper, no browser. When you press auto-edit in the UI the server
only writes a row in the queue. This worker, running on the operator's PC,
claims the row, does the whole pass, and uploads the finished mp4 back.

So: **nothing renders while no worker is polling.** A job that looks stuck at
"queued" usually means this script is not running.

Files:
- `static/autoedit_worker.py` — the worker (this runbook's subject).
- `autoedit_pipeline.py` — the pipeline it calls. Also imported by the server,
  so all its heavy imports live inside functions. Do not move them out.
- `autoedit_queue.py` — the pure queue decisions (claimable, retry, stale).

## LOCAL-ONLY

**This never runs on Render.** It needs ffmpeg, OpenCV, a whisper model and a
headless browser, and it writes multi-GB work directories. Same posture as
`chatgpt_image_worker.py` and `flow_worker.py`: operator machine only.

## One-time setup

1. `ffmpeg` and `ffprobe` on PATH (`ffmpeg -version` must print something).
2. Python packages:
   ```bash
   pip install opencv-python faster-whisper requests
   pip install git+https://github.com/francozanardi/pycaps
   python -m playwright install chromium
   ```
   pycaps renders the captions in a headless Chromium — that is what the
   playwright step is for.
3. Nothing else. **The platform token is found automatically** (see below).

## The token

Never paste a token into this script and never put one on the command line.
The worker calls `resolve_token()` from `send_to_platform.py`, which looks in
order at `KAVENO_API_TOKEN`, `VEO_TOKEN`, the Flow worker's `~/veo-worker/.env`,
then `~/.kaveno/token`. On start it prints which one it used:

```
[worker] token: ~/veo-worker/.env (flow worker token)
```

If it prints `no platform token found`, save one once:

```bash
python code/send_to_platform.py set-token <token>
```

The pipeline resolves the same token again on its own to download the job
files, so one saved token covers everything.

## How to run

Poll forever — this is the normal mode. Leave it in a terminal:

```bash
python code/static/autoedit_worker.py --watch
```

Do one run and stop (handy for testing, or to clear a single job):

```bash
python code/static/autoedit_worker.py --once
python code/static/autoedit_worker.py            # same thing — --once is the default
```

`--interval N` changes the poll gap in `--watch` (default 15 seconds). After
it finishes a run it claims the next one immediately, without waiting.

Ctrl-C stops it. A run that was mid-render is picked up again by the next
worker after 5 minutes of silence.

## What you see

```
[worker] token: ~/veo-worker/.env (flow worker token)
[worker] watching https://kavenobuilder.com every 15s — nothing renders while this is not running
[worker] a1b2c3 download
[worker] a1b2c3 scan
[worker] a1b2c3 layout
[worker] a1b2c3 audio
[worker] a1b2c3 compose
[worker] a1b2c3 captions
[worker] DONE ... -> C:\Users\you\.kaveno\autoedit\<job-id>\result_a1b2c3.mp4
```

Those six stage names are also what the UI shows, because each one is posted
to the server as it starts.

## Work directories

Everything lands in `~/.kaveno/autoedit/<job-id>/` — the downloaded tracks, the
green-screen scan, the layout numbers, the enhanced audio, the composed video,
and the final `result_<id>.mp4`.

**The directory is a cache, on purpose.** A second run of the same job reuses
the download, the scan (`scan: cached`), the layout and the face-occupancy map,
so a re-render takes a fraction of the time. **To force a clean redo, delete
the job's folder** and queue it again. They are large — clear out old ones now
and then.

## Heartbeat, and why it matters

The server marks a run abandoned after **5 minutes** with no word from the
worker, and lets another worker take it. The captions stage alone can run
longer than that in silence, so while a job is being handled the worker pings
the server with the current stage every 60 seconds in a background thread.

The ping stops before the upload, deliberately: a progress ping puts the run
back into `running`, so a late one would undo the `done` the upload just set.

## Failures

A failed run is reported to the server, not swallowed and not fatal:

- The pipeline raises `AutoEditError`, the worker catches it, prints the full
  traceback, and posts it to `/fail`.
- The server requeues the run. After **3 attempts** it is marked `failed` and
  the error text shows in the UI.
- If reporting the failure itself fails (network blip), the worker says so and
  keeps going. The run goes stale after 5 minutes and is queued again.
- In `--watch`, a server restart or a dropped connection during the claim poll
  prints `claim failed (...) — retrying` and the loop continues. The worker
  does not die on a network problem.

The upload is capped at **512 MB** server-side. A bigger result is rejected
with a 413 and reported like any other failure.

Common real causes: `export not done for this job` (export it in the platform
first), `pycaps not found` (setup step 2 not done), or `ffmpeg` / `ffprobe` not
on PATH. A job with no support track is **not** an error — the PIP stage is
skipped and the rest still runs.

## Safety

- **LOCAL-ONLY.** Never add this to the Render start command or a Procfile.
- No secrets in the file. The token is resolved at runtime and only ever
  printed as its source name, never its value.
