"""autoedit_pipeline.py — the CapCut auto-edit pass, as an importable module.

Turns a completed platform job into a posted-ready video: downloads the job's
final export, its 16:9 b-roll "support track" and its full-frame `final_broll`,
keys the green-screen hook, overlays a rounded-corner PIP of the b-roll,
enhances the voice, and burns word-by-word karaoke captions (via pycaps) whose
placement dynamically avoids covering any face, the PIP, or the highest-motion
zone.

Hook layout is selectable (v938.15). By default the keyed speaker is placed at
full size over a blurred backdrop. With `hook_corner` set, the full-frame b-roll
plays SHARP and the speaker shrinks into the bottom-left corner — the layout
measured in 16 decoded videos and in the operator's own CapCut projects. See
docs/experiments/autoedit-hook-composite-placement-2026-08-22.md.

MUST stay importable on the Render server, where cv2 / faster_whisper / pycaps
are NOT installed. Every one of those imports lives inside a function body —
keep it that way. Only stdlib + argparse/json/os/shutil/subprocess/sys/pathlib
may be imported at module level.

The local CLI wrapper is tools/capcut_autoedit.py (wiki repo) — it just calls
run_autoedit() below. This module is the moved-verbatim pipeline body.
"""
import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path


class AutoEditError(RuntimeError):
    """Pipeline failure — catchable by callers (worker, server, CLI).

    Library code in this module must always raise this (never the
    process-exit exception argparse-style CLIs use): that one derives
    from BaseException, not Exception, so a caller's normal
    `except Exception` (a background worker reporting failures to the
    server, for instance) would not catch it and the process would die
    silently instead of reporting the failure.
    """


# The platform's DeepFilter helper prints emoji; on cp1252 consoles that print
# CRASHES the call and masquerades as a Modal failure. Force utf-8 stdout.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

BASE_URL = os.environ.get("KAVENO_BASE_URL", "https://kavenobuilder.com")
CODE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = CODE_DIR / "caption_templates"
PIP_W, PIP_H, PIP_X, PIP_Y = 800, 450, 140, 1050  # clears the Reels bottom-420px UI zone
# Face sensor for caption placement (2026-08-25). YuNet (a small ONNX face
# detector bundled with OpenCV's zoo) replaces the Haar cascade as the default:
# measured on a real job (fecd12), Haar never saw a passenger's three-quarter
# face, saw a burger t-shirt graphic as a face for one second (which pushed the
# hook captions to the top of the frame), and saw a shirt as a face once more
# mid-video (5s caption excursion). YuNet found every real face in those same
# frames and none of the phantoms. Haar stays as the fallback when the model
# file is missing so the pipeline still runs.
YUNET_MODEL = CODE_DIR / "models" / "face_detection_yunet_2023mar.onnx"


def face_detector_tag():
    """Which face sensor is active — baked into cache names (v938.1: a cache is
    named after everything baked into it; detector output IS baked into the
    occupancy map and the layout numbers)."""
    return "yunet" if YUNET_MODEL.exists() else "haar"


def make_face_detector(width, height):
    """One callable `frame -> [[x0,y0,x1,y1], ...]` (fractions of the frame),
    shared by detect_layout and build_occupancy so both see the same faces."""
    import cv2
    if YUNET_MODEL.exists():
        det = cv2.FaceDetectorYN.create(str(YUNET_MODEL), "", (width, height),
                                        score_threshold=0.6)
        def detect(frame):
            h, w = frame.shape[:2]
            if (w, h) != (width, height):
                det.setInputSize((w, h))
            _, faces = det.detect(frame)
            out = []
            if faces is not None:
                for f in faces:
                    x, y, fw, fh = (float(v) for v in f[:4])
                    out.append([max(0.0, x / w), max(0.0, y / h),
                                min(1.0, (x + fw) / w), min(1.0, (y + fh) / h)])
            return out
        return detect
    if not hasattr(cv2, "CascadeClassifier"):
        # OpenCV 5 dropped this from the default build; requirements pin <5.
        raise AutoEditError(
            f"This OpenCV build ({getattr(cv2, '__version__', '?')}) has no CascadeClassifier "
            f"and the YuNet model file is missing ({YUNET_MODEL}), so faces cannot be "
            "detected and captions could cover one.")
    cascade = cv2.CascadeClassifier(cv2.data.haarcascades + "haarcascade_frontalface_default.xml")
    def detect(frame):
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        h, w = gray.shape
        # Haar's sliding window cannot leave the frame, but clamp anyway so a
        # future detector swap inherits the same [0,1] contract as YuNet above.
        return [[max(0.0, x / w), max(0.0, y / h),
                 min(1.0, (x + fw) / w), min(1.0, (y + fh) / h)]
                for x, y, fw, fh in cascade.detectMultiScale(gray, 1.1, 5, minSize=(60, 60))]
    return detect
BUILTIN_TEMPLATES = ["classic", "default", "explosive", "fast", "hype", "line-focus",
                     "minimalist", "model", "neo-minimal", "retro-gaming", "vibrant", "word-focus"]
# Watermark font: this pipeline only runs on this Windows PC today, so the
# Windows path stays first (byte-identical behavior there). The other two
# are a minimal fallback so a non-Windows machine fails with a clear error
# instead of a cryptic ffmpeg one, if this ever runs elsewhere.
WATERMARK_FONT_CANDIDATES = [
    "C:/Windows/Fonts/arial.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/System/Library/Fonts/Supplemental/Arial.ttf",
]


def run(cmd, **kw):
    r = subprocess.run(cmd, capture_output=True, text=True,
                       encoding="utf-8", errors="replace", **kw)
    if r.returncode != 0:
        print(r.stderr[-2000:] if r.stderr else r.stdout[-2000:])
        raise AutoEditError(f"command failed: {cmd[0]} ...")
    return r


def file_fingerprint(path, n=8):
    """Short hash of a file's CONTENT, for use in a cache name.

    Content, not mtime: an identical re-render should still hit the cache.
    Used so a cached artifact that BAKES IN another file (compose() muxes the
    enhanced audio into its mp4) is invalidated when that file changes.
    """
    h = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()[:n]


# ---------------------------------------------------------------------------
# CACHE NAMES — one pure function per cached artifact.
#
# THE RULE: a cached artifact is named after EVERYTHING baked into it. Not the
# settings of the stage that wrote it — every input whose change would change
# the bytes. Five stages in this file broke that rule, and each one silently
# served stale output while the log said the new code had run:
#
#   audio_pol.wav      keyed on nothing        -> a rebuilt voice chain never ran
#   nocap_wm_*.mp4     keyed on picture+music  -> muxed the NEW audio, served the OLD video
#   cap_pass_*.mp4     keyed on offset+template-> burned captions over yesterday's composite
#   occupancy_*.json   keyed on trim values    -> reused a scan of the wrong video
#   (plus a caption helper that guessed a filename that had stopped existing)
#
# They live here, as pure functions, so the rule is READABLE and TESTABLE
# (code/tests/test_autoedit_cache_keys.py asserts each name moves when any of
# its inputs moves). Adding a cache? Add its builder here and a test with it.
# ---------------------------------------------------------------------------

def audio_chain_key():
    """Fingerprint of the whole voice-processing definition.

    Both chains AND the constants behind the measured low shelf: the shelf gain
    is derived from the audio, so the same source always yields the same gain,
    but changing the target or the clamps must invalidate every cached file.
    """
    return hashlib.md5(
        (_VOICE_CHAIN + "|" + _VOICE_CHAIN_RAW +
         f"|{_LOW_TARGET_DB}|{_LOW_GAIN_MIN}|{_LOW_GAIN_MAX}").encode()).hexdigest()[:8]


def audio_cache_name(chain_key, denoised):
    """`denoised` is in the name because the two paths need OPPOSITE low-end
    corrections, and because a fallback result must never be mistaken for a
    denoised one on a later run."""
    return f"audio_pol_{chain_key}_{'df' if denoised else 'raw'}.wav"


def compose_cache_key(pip_y, pip_enabled, chroma_similarity, chroma_blend,
                      music, music_db, audio_fingerprint, hook_corner):
    """Every visual setting AND the audio this mp4 muxes in.

    The audio is the half that was missing. It is not a "setting", which is
    exactly why it was overlooked — the old comment said every repair setting
    was in the name, and it was true, and the file still shipped the wrong voice.
    """
    music_key = music.stem[:24] if music else "none"
    corner_key = "off" if not hook_corner else f"{hook_corner:.3f}"
    return (f"y{pip_y}_p{int(pip_enabled)}_k{chroma_similarity:.3f}_"
            f"b{chroma_blend:.3f}_m{music_key}_{music_db:.1f}_"
            f"a{audio_fingerprint}_hc{corner_key}")


def cap_pass_name(offset, template, source_fingerprint):
    """A captioned pass is named after the VIDEO it was burned over."""
    tag = str(offset).replace('-', 'm').replace('.', '_')
    return f"cap_pass_{tag}_{template}_{source_fingerprint}.mp4"


def occupancy_name(trim_start_s, trim_end_s, source_fingerprint):
    """The face/motion map is named after the video that was SCANNED — which
    is not always the base export (see the hook_corner path in run_autoedit) —
    AND after the face detector that scanned it: a Haar-era map must not be
    served to a YuNet-era planner (v938.1)."""
    return (f"occupancy_{face_detector_tag()}_s{trim_start_s:.2f}_e{trim_end_s:.2f}_"
            f"{source_fingerprint}.json")


def pycaps_exe():
    p = shutil.which("pycaps")
    if p:
        return p
    guess = Path(os.environ.get("APPDATA", "")) / "Python" / "Python313" / "Scripts" / "pycaps.exe"
    if guess.exists():
        return str(guess)
    raise AutoEditError("pycaps not found: pip install git+https://github.com/francozanardi/pycaps")


def local_styles():
    return sorted(d.name for d in TEMPLATES_DIR.iterdir() if (d / "pycaps.template.json").exists()) \
        if TEMPLATES_DIR.exists() else []


def api_get(path, token, stream=False):
    import requests
    r = requests.get(f"{BASE_URL}{path}", headers={"Authorization": f"Bearer {token}"},
                     timeout=600, stream=stream)
    if r.status_code != 200:
        raise AutoEditError(f"GET {path} -> {r.status_code}: {r.text[:200]}")
    return r


def local_outputs_dir():
    """Where this job's files already sit on THIS machine, if they do.

    When the render runs on the server, the source export is already on the
    server's own disk. Set AUTOEDIT_LOCAL_OUTPUTS to that directory and we copy
    it instead of pulling ~150MB back through our own public URL — which on a
    1-CPU box costs real time and bandwidth for nothing.
    """
    d = os.environ.get("AUTOEDIT_LOCAL_OUTPUTS")
    return Path(d) if d else None


def download(path, token, dest: Path):
    if dest.exists() and dest.stat().st_size > 0:
        print(f"  cached: {dest.name}")
        return
    src_dir = local_outputs_dir()
    if src_dir:
        local = src_dir / dest.name
        if local.exists() and local.stat().st_size > 0:
            shutil.copyfile(local, dest)
            print(f"  local copy: {dest.name} ({dest.stat().st_size / 1e6:.1f} MB, no download)")
            return
    r = api_get(path, token, stream=True)
    with open(dest, "wb") as f:
        for chunk in r.iter_content(1 << 20):
            f.write(chunk)
    print(f"  downloaded: {dest.name} ({dest.stat().st_size / 1e6:.1f} MB)")


def fetch_job_files(job_id, work: Path, music_filename=None, hook_bg_filename=None):
    from send_to_platform import resolve_token
    token, source = resolve_token(None)
    print(f"token: {source}")
    st = api_get(f"/api/jobs/{job_id}/export-status", token).json()
    if st.get("state") != "done":
        raise AutoEditError(f"export not done for this job (state={st.get('state')}) — export it in the platform first")
    base_fn = st["result"]["filename"]

    # v958 — THE STAGE CACHE IS KEYED ON THE INPUT EXPORT, NOT JUST THE JOB.
    #
    # The work dir deliberately caches every stage (v938.8: the downloaded
    # source, scan.json, the layout, the cleaned audio, the composed video)
    # so a redeploy resumes instead of redoing 20 minutes. But the dir is
    # per JOB, and on 2026-09-01 (job 248198f6) that shipped stale content
    # THREE times: the operator switched the clip to variant 2, both fresh
    # exports provably carried the new take (frame-probed yellow dress),
    # and every re-run still delivered the variant-1 cut in ~40s — the
    # cached stages built from the FIRST export were reused wholesale.
    # §v938.1's own words: name a cache after everything baked into it.
    #
    # The input's identity is the export basename (exports mint unique
    # names per run, v856). A marker records which export this work dir's
    # stages were built from; a different one wipes the dir before any
    # download. Same export → marker matches → deploy-resume still works.
    work.mkdir(parents=True, exist_ok=True)
    _marker = work / ".input_export"
    _prev = _marker.read_text(encoding="utf-8").strip() if _marker.exists() else None
    if _prev is not None and _prev != base_fn:
        print(f"  input export changed ({_prev} -> {base_fn}) — "
              f"wiping the cached stages (v958)")
        for _child in work.iterdir():
            try:
                if _child.is_dir():
                    shutil.rmtree(_child, ignore_errors=True)
                else:
                    _child.unlink()
            except OSError as _we:
                print(f"  (could not remove {_child.name}: {_we})")
    _marker.write_text(base_fn, encoding="utf-8")
    outs = api_get(f"/api/jobs/{job_id}/list-outputs", token).json()["files"]
    sup_fn = next((f for f in outs if f.startswith("support_track_") and f.endswith(".mp4")), None)
    # v938.15 — jobs also export `final_broll_<job>_<stamp>.mp4`: the b-roll as a
    # FULL-FRAME 1080x1920 sharp video, not the 16:9 band. That is the file the
    # operator composites behind the corner speaker in CapCut, and until now
    # nothing here ever looked for it — which is why the hook had no real
    # background to use and fell back to blurring a scrap of the 16:9 track.
    broll_fn = next((f for f in outs if f.startswith("final_broll_") and f.endswith(".mp4")), None)
    # v938.16 — an explicit hook background wins over the auto-picked one, and
    # may be a still image (see autoedit_qc.HOOK_BG_EXTENSIONS).
    if hook_bg_filename:
        if hook_bg_filename not in outs:
            raise AutoEditError(
                f"hook background is not in this job's outputs: {hook_bg_filename}")
        broll_fn = hook_bg_filename
    music_fn = music_filename if music_filename in outs else None
    if music_filename and not music_fn:
        raise AutoEditError(f"music file is not in this job's outputs: {music_filename}")
    base, sup = work / base_fn, (work / sup_fn if sup_fn else None)
    broll = work / broll_fn if broll_fn else None
    music = work / music_fn if music_fn else None
    download(f"/api/jobs/{job_id}/outputs/{base_fn}", token, base)
    if sup_fn:
        download(f"/api/jobs/{job_id}/outputs/{sup_fn}", token, sup)
    else:
        print("  no support track found — PIP stage will be skipped")
    if broll_fn:
        download(f"/api/jobs/{job_id}/outputs/{broll_fn}", token, broll)
    else:
        print("  no full-frame b-roll (final_broll_*) — hook-corner layout will blur a fallback")
    if music_fn:
        download(f"/api/jobs/{job_id}/outputs/{music_fn}", token, music)
    return base, sup, music, broll


def probe_duration(path):
    r = run(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", str(path)])
    return float(r.stdout.strip())


def scan_tracks(base, sup, dur):
    """0.1s scan: green-screen window in base, non-black windows in support."""
    import numpy as np

    def frames(path, w, h):
        raw = subprocess.run(["ffmpeg", "-v", "error", "-i", str(path), "-vf", f"fps=10,scale={w}:{h}",
                              "-f", "rawvideo", "-pix_fmt", "rgb24", "-"], capture_output=True).stdout
        n = len(raw) // (w * h * 3)
        return np.frombuffer(raw[: n * w * h * 3], dtype=np.uint8).reshape(n, h, w, 3).astype(np.int16)

    fb = frames(base, 64, 114)
    gmask = (fb[:, :, :, 1] - fb[:, :, :, 0] > 40) & (fb[:, :, :, 1] - fb[:, :, :, 2] > 40) & (fb[:, :, :, 1] > 90)
    gfrac = gmask.mean(axis=(1, 2))
    gidx = np.where(gfrac > 0.10)[0]
    # only a LEADING green block counts as the hook
    hook_end, key_hex = 0.0, None
    if len(gidx) and gidx[0] <= 5:
        stop = gidx[0]
        for i in gidx:
            if i - stop > 5:
                break
            stop = i
        hook_end = (stop + 1) / 10.0
        gpx = fb[: stop + 1][gmask[: stop + 1]]
        key_hex = "0x{:02x}{:02x}{:02x}".format(*(int(x) for x in np.median(gpx, axis=0)))
    segs = []
    if sup is not None:
        fs = frames(sup, 114, 64)
        flags = (fs.max(axis=3) > 24).mean(axis=(1, 2)) > 0.05
        start = None
        for i, f in enumerate(flags):
            if f and start is None:
                start = i
            if not f and start is not None:
                segs.append((start / 10.0, i / 10.0)); start = None
        if start is not None:
            segs.append((start / 10.0, len(flags) / 10.0))
        segs = [(round(a + 0.2, 2), round(b - 0.2, 2)) for a, b in segs if b - a > 1.0]
    print(f"scan: hook 0-{hook_end}s key={key_hex} | {len(segs)} b-roll windows: {segs}")
    return hook_end, key_hex, segs


def detect_layout(base, dur, segs):
    """Face-aware constant placement (the corpus + best-practice method):
    find the median chin line, give captions the band right below it, put the
    PIP below the captions, clamp everything to platform safe zones.
    Returns (caption_offset_for_pycaps, pip_y_px)."""
    import cv2
    cap = cv2.VideoCapture(str(base))
    fps = cap.get(cv2.CAP_PROP_FPS) or 24
    bottoms, detect = [], None
    for t in range(1, int(dur), 2):
        cap.set(cv2.CAP_PROP_POS_FRAMES, int(t * fps))
        ok, frame = cap.read()
        if not ok:
            continue
        h, w = frame.shape[:2]
        small = cv2.resize(frame, (360, int(h * 360 / w)))
        if detect is None:
            detect = make_face_detector(small.shape[1], small.shape[0])
        faces = detect(small)
        if faces:
            x0, y0, x1, y1 = max(faces, key=lambda f: (f[2] - f[0]) * (f[3] - f[1]))
            bottoms.append(y1)
    cap.release()
    if bottoms:
        import statistics
        chin = min(statistics.median(bottoms) + 0.02, 0.60)
    else:
        chin = 0.40  # no face found: assume close-up framing
    bottom_safe = 1500 / 1920  # Reels reserves the bottom ~420px
    if segs:
        pip_y = int(min((chin + 0.15) * 1920, 1500 - PIP_H))
        band_bottom = pip_y / 1920
    else:
        pip_y = PIP_Y
        band_bottom = min(chin + 0.16, bottom_safe)
    center = (chin + band_bottom) / 2
    offset = round(max(-0.15, min(center - 0.5, 0.28)), 3)
    print(f"layout: chin={chin:.2f} pip_y={pip_y} caption-center={center:.2f} (offset {offset:+.3f}) "
          f"from {len(bottoms)} face samples")
    return offset, pip_y, chin


def build_occupancy(base, dur):
    """Per-second map of what the captions must NEVER cover: every detected
    face box + a coarse motion grid (where the action is). Fractions of frame.
    Detection runs at 360px width (not the old 180): YuNet needs the pixels,
    and Haar at 180 was the sensor that hallucinated shirt-graphic faces."""
    import cv2
    import numpy as np
    cap = cv2.VideoCapture(str(base))
    fps = cap.get(cv2.CAP_PROP_FPS) or 24
    buckets, prev, detect = [], None, None
    for t in [x + 0.5 for x in range(int(dur))]:
        cap.set(cv2.CAP_PROP_POS_FRAMES, int(t * fps))
        ok, frame = cap.read()
        if not ok:
            buckets.append({"t": t, "faces": [], "motion": [0.0] * 10})
            continue
        h, w = frame.shape[:2]
        small = cv2.resize(frame, (360, int(h * 360 / w)))
        gray = cv2.cvtColor(small, cv2.COLOR_BGR2GRAY)
        if detect is None:
            detect = make_face_detector(small.shape[1], small.shape[0])
        faces = detect(small)
        if prev is not None and prev.shape == gray.shape:
            diff = cv2.absdiff(gray, prev).astype("float32") / 255.0
            rows = np.array_split(diff, 10, axis=0)
            motion = [float(r.mean()) for r in rows]  # motion per 10% height band
        else:
            motion = [0.0] * 10
        prev = gray
        buckets.append({"t": t, "faces": faces, "motion": motion})
    cap.release()
    return buckets


def clean_buckets(buckets):
    """Sensor cleanup between detection and planning (pure function).

    Measured failure modes this removes (job fecd12, 2026-08-25):
    - a PHANTOM face that exists for one second (Haar saw a burger t-shirt
      graphic as a face at t=1.5 — that single box made the hook captions
      jump to the top of the frame);
    - single-second BOX JITTER on a real face (a chin 'dipping' 0.03 for one
      bucket grazed the caption band edge and caused two mid-video jumps).

    For every second, boxes from the surrounding ±2 buckets are clustered by
    overlap; a cluster must appear in ≥2 distinct buckets to count as a face
    (kills one-second phantoms), and the emitted box is the per-coordinate
    MEDIAN of the cluster (kills one-second jitter). A real face that the
    detector drops every other second still survives: it appears in ≥2 of the
    5 buckets around any given second."""
    import statistics

    def overlaps(a, b):
        ix = min(a[2], b[2]) - max(a[0], b[0])
        iy = min(a[3], b[3]) - max(a[1], b[1])
        if ix <= 0 or iy <= 0:
            return False
        inter = ix * iy
        area_a = (a[2] - a[0]) * (a[3] - a[1])
        area_b = (b[2] - b[0]) * (b[3] - b[1])
        return inter / max(min(area_a, area_b), 1e-9) >= 0.3

    cleaned = []
    for i, b in enumerate(buckets):
        clusters = []  # each: list of (bucket_index, box)
        # A full 5-bucket window even at the edges (slid, not truncated): a
        # truncated 3-bucket window at the last second demanded 2-of-3 from a
        # face the detector only sees every other second, and dropped it.
        lo = max(0, min(i - 2, len(buckets) - 5))
        for j in range(lo, min(len(buckets), lo + 5)):
            for box in buckets[j]["faces"]:
                home = next((cl for cl in clusters if overlaps(cl[-1][1], box)), None)
                if home is None:
                    clusters.append([(j, box)])
                else:
                    home.append((j, box))
        faces = []
        for cl in clusters:
            if len({j for j, _ in cl}) < 2:
                continue  # one-second phantom
            faces.append([statistics.median(box[k] for _, box in cl) for k in range(4)])
        cleaned.append({**b, "faces": faces})
    return cleaned


def plan_caption_windows(buckets, chin, segs, pip_y, dur):
    """Dynamic placement: per second pick a caption band that covers NO face,
    NO PIP window, least action — decided globally (shortest path with a cost
    per move) so captions hold ONE home position and move only when the
    picture forces it. Returns [(start, end, offset)] merged windows."""
    half = 0.085                      # caption band half-height (2-line card + highlight padding)
    cands = [min(chin + 0.095, 0.60), 0.70, 0.15]  # below-chin | lower-third | top-last-resort
    pip_band = (pip_y / 1920, (pip_y + PIP_H) / 1920)

    def pip_active(t):
        return any(a - 0.3 <= t <= b + 0.3 for a, b in segs)

    def valid(c, b):
        y0, y1 = c - half, c + half
        if y0 < 0.06 or y1 > 0.79:    # platform UI safe zones
            return False
        for fx0, fy0, fx1, fy1 in b["faces"]:
            if fy1 > y0 - 0.015 and fy0 < y1 + 0.015 and fx1 > 0.12 and fx0 < 0.88:
                return False          # never cover ANY face
        if pip_active(b["t"]) and pip_band[1] > y0 - 0.01 and pip_band[0] < y1 + 0.01:
            return False              # never cover the insert
        return True

    def _merge_y_intervals(faces):
        """Union of face y-ranges (x-overlapping the caption's central zone only).
        Near-duplicate face boxes from consecutive seconds must not be summed twice —
        union, not sum, is what makes the pixel-overlap numbers below match measured
        reality instead of double-counting the same face."""
        ivals = sorted((fy0, fy1) for fx0, fy0, fx1, fy1 in faces if fx1 > 0.12 and fx0 < 0.88)
        merged = []
        for s, e in ivals:
            if merged and s <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(merged[-1][1], e))
            else:
                merged.append((s, e))
        return merged

    def face_overlap_px(c, b):
        """Raw (unpadded) vertical overlap in pixels, on a 1920-tall frame, between the
        candidate's band and every face in this bucket. Used only to RANK candidates
        when nothing is fully legal -- valid()'s 0.015 safety margin stays the gate for
        deciding whether a second is squeezed at all."""
        y0, y1 = c - half, c + half
        total = 0.0
        for s, e in _merge_y_intervals(b["faces"]):
            ov = min(y1, e) - max(y0, s)
            if ov > 0:
                total += ov
        return total * 1920

    def pip_overlap_px(c, b):
        """Raw vertical overlap in pixels between the candidate's band and the PIP,
        when the PIP is on screen; 0 otherwise. Same ranking role as face_overlap_px."""
        if not pip_active(b["t"]):
            return 0.0
        y0, y1 = c - half, c + half
        ov = min(y1, pip_band[1]) - max(y0, pip_band[0])
        return max(0.0, ov) * 1920

    def action_score(c, b):
        y0, y1 = c - half, c + half
        rows = [m for i, m in enumerate(b["motion"]) if y1 > i / 10 and y0 < (i + 1) / 10]
        return sum(rows) / max(len(rows), 1)

    # Sensor cleanup first: one-second phantom faces and one-second box jitter
    # were the trigger for EVERY unwanted caption move on the job that led to
    # this design (see clean_buckets).
    buckets = clean_buckets(buckets)

    # Decision layer: shortest path over (second, candidate) with a price on
    # every move — replaces the old greedy hold/lookahead heuristic, which
    # moved whenever a locally-better candidate looked stable for 3-4s and
    # produced 8 moves in 88s on a real job. Here a move must EARN its switch
    # cost over the rest of the video, which is the actual requirement:
    # captions live in one home position and move only when the picture forces
    # it. Hard rules unchanged: a face or the PIP is never covered while ANY
    # candidate clears both (valid()); when nothing is legal the squeeze
    # ladder still applies (face-clear first, then least face overlap).
    SWITCH = 25.0                     # one move costs this much accumulated preference
    # Lower-third is the preferred home, below-chin second: verified on real
    # frames (job fecd12) that the below-chin band in a selling video sits on
    # the DEMO ZONE — the chest-height area where this persona presents the
    # product and the palm-with-pill shot — while the lower third lands on the
    # counter. "Never cover the main action" outranks the below-chin habit;
    # below-chin remains for when the lower third is face- or PIP-blocked.
    PRIOR = {0: 0.5, 1: 0.0, 2: 8.0}  # below-chin | lower-third | top only when pushed
    HOOK_TOP_PRIOR = 30.0             # top captions over the hook read broken — never
    HOOK_S = 6.0                      # unless every other band would cover a face

    squeezed, heavy_squeezed = 0, 0
    allowed_per_s = []                # per second: {candidate: cost}
    for b in buckets:
        options = [c for c in cands if valid(c, b)]
        if options:
            costs = {}
            for c in options:
                prior = PRIOR[cands.index(c)]
                if cands.index(c) == 2 and b["t"] < HOOK_S:
                    prior = HOOK_TOP_PRIOR
                costs[c] = 10.0 * action_score(c, b) + prior
        else:
            # Squeeze: no candidate clears both the face(s) and the PIP + safe
            # zones. Operator rule, verbatim: "never cover the main action or
            # any face -- never". Faces are the hard, unconditional priority;
            # the PIP is our own inserted overlay, not a person, so it is the
            # one allowed to give way. Ladder:
            #   2. face-clear (zero measured face overlap) -- insert overlap
            #      accepted, priced so the least-covering position wins.
            #   3. when even that is impossible, the candidate with the
            #      SMALLEST total face overlap (pixels, 1920-tall frame) is
            #      forced, tie-broken by smaller insert overlap, then order.
            squeezed += 1
            face_clear = [c for c in cands if face_overlap_px(c, b) == 0]
            if face_clear:
                costs = {c: 0.05 * pip_overlap_px(c, b) + PRIOR[cands.index(c)]
                         for c in face_clear}
            else:
                heavy_squeezed += 1
                forced = min(cands, key=lambda c: (round(face_overlap_px(c, b), 1),
                                                   round(pip_overlap_px(c, b), 1),
                                                   cands.index(c)))
                costs = {forced: 0.0}
        allowed_per_s.append(costs)

    if not allowed_per_s:
        return []
    INF = float("inf")
    dp = [{c: allowed_per_s[0].get(c, INF) for c in cands}]
    back = []
    for i in range(1, len(allowed_per_s)):
        row, brow = {}, {}
        for c in cands:
            own = allowed_per_s[i].get(c, INF)
            if own == INF:
                row[c], brow[c] = INF, None
                continue
            prev_c = min(cands, key=lambda p: dp[-1][p] + (0.0 if p == c else SWITCH))
            row[c] = own + dp[-1][prev_c] + (0.0 if prev_c == c else SWITCH)
            brow[c] = prev_c
        dp.append(row)
        back.append(brow)
    plan = [min(cands, key=lambda c: dp[-1][c])]
    for brow in reversed(back):
        plan.append(brow[plan[-1]])
    plan.reverse()
    windows, start = [], 0.0
    for i in range(1, len(plan) + 1):
        if i == len(plan) or plan[i] != plan[i - 1]:
            end = dur if i == len(plan) else float(i)
            windows.append((start, end, round(plan[i - 1] - 0.5, 3)))
            start = float(i)
    windows = enforce_min_dwell(windows, buckets)
    switches = len(windows) - 1
    print(f"placement plan: {len(windows)} windows, {switches} moves -> "
          + " | ".join(f"{a:.0f}-{b:.0f}s@{0.5 + o:.2f}" for a, b, o in windows))
    if squeezed:
        print(f"placement: WARNING — {squeezed}s had no band clear of both face and insert "
              f"({heavy_squeezed}s of those could not clear a face either; "
              f"used the least-covering position)")
    return windows


def enforce_min_dwell(windows, buckets, min_dwell=2.0):
    """Post-pass, pure function: no returned window may stand alone for less than
    min_dwell seconds (a caption that hops for one second and hops right back off
    again reads as broken -- the same flapping problem hysteresis exists to prevent,
    just at the merged-window level instead of the per-second level).

    A too-short window gets absorbed into whichever neighbour leaves the LOWER total
    face overlap (pixels, 1920-tall frame) over that window's own seconds; a tie keeps
    the earlier/left neighbour, so the caption simply stays where it already was.
    Repeats until every window is >= min_dwell or only one window remains (both
    conditions guarantee termination: each merge strictly shrinks the window count)."""
    half = 0.085
    by_t = {b["t"]: b for b in buckets}

    def merged_y_intervals(faces):
        ivals = sorted((fy0, fy1) for fx0, fy0, fx1, fy1 in faces if fx1 > 0.12 and fx0 < 0.88)
        merged = []
        for s, e in ivals:
            if merged and s <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(merged[-1][1], e))
            else:
                merged.append((s, e))
        return merged

    def face_px(offset, t):
        b = by_t.get(t)
        if b is None:
            return 0.0
        c = offset + 0.5
        y0, y1 = c - half, c + half
        total = 0.0
        for s, e in merged_y_intervals(b["faces"]):
            ov = min(y1, e) - max(y0, s)
            if ov > 0:
                total += ov
        return total * 1920

    windows = list(windows)
    changed = True
    while changed and len(windows) > 1:
        changed = False
        for i, (start, end, off) in enumerate(windows):
            if end - start >= min_dwell:
                continue
            ts = [b["t"] for b in buckets if start <= b["t"] < end]
            left = windows[i - 1] if i > 0 else None
            right = windows[i + 1] if i < len(windows) - 1 else None
            if left is None:
                target_off = right[2]
            elif right is None:
                target_off = left[2]
            else:
                left_cost = sum(face_px(left[2], t) for t in ts)
                right_cost = sum(face_px(right[2], t) for t in ts)
                target_off = left[2] if left_cost <= right_cost else right[2]
            windows[i] = (start, end, target_off)
            coalesced = [windows[0]]
            for w in windows[1:]:
                ps, pe, po = coalesced[-1]
                s, e, o = w
                if o == po and abs(s - pe) < 1e-9:
                    coalesced[-1] = (ps, e, po)
                else:
                    coalesced.append(w)
            windows = coalesced
            changed = True
            break
    return windows


# v938.12 — the voice chain lives here as a constant so the audio cache can be
# keyed on it: change the chain, and every cached file is invalidated by its
# own name. See the comment in enhance_audio.
#
# v938.13 — the chain is now MEASURED against CapCut, not guessed.
#
# The operator said our audio did not sound as good as CapCut's. Rather than
# keep guessing at a "broadcast voice" chain, we measured it: five files exist
# on this machine in both states — the platform export the operator dropped
# into CapCut, and the file CapCut wrote back out. Same speech, one editing
# pass in between. Averaging the long-term spectrum of both sides gives what
# CapCut actually did, in dB per band. Reproduce it with
# `python code/measure_capcut_match.py --derive <before> <after> ...`:
#
#     60-120 Hz  -13.8      2-3 kHz   +1.1
#    120-250 Hz   -0.3      3-5 kHz   +1.1
#    250-500 Hz   +0.3      5-8 kHz   -1.0
#    500-1k Hz    +0.0      8-12 kHz  -1.9
#      1-2 kHz    -0.0     12-16 kHz  -0.5
#
# CapCut does ONE substantial thing: it loses the sub-120 Hz rumble. Every
# other band moves less than 2 dB. There is no neural resynthesis to chase.
#
# The old chain above did the opposite of what that curve wants. Against the
# untouched source it cut 300 Hz (the voice's body) and lifted 3.2 kHz, which
# left our output 3.3 dB THINNER and 4.5 dB BRIGHTER than the source — and so
# thinner and brighter than CapCut, which leaves both alone. Thin plus bright
# is exactly the harsh, brittle character the operator was hearing.
#
# It happened because the metric was wrong. "presence-to-mud" (2-5 kHz over
# 200-500 Hz) rewards removing body and adding treble, so optimising it drove
# the sound away from the target while the number went up. The target is now
# CapCut's own curve, and the score is the weighted deviation from it:
# 3.47 for the old chain, 1.23 for tone alone, 0.73 once the low shelf below
# puts back what DeepFilter over-removed. `code/measure_capcut_match.py` scores
# any file, so every number here is checkable rather than asserted.
#
# No fixed highpass on the denoised path: DeepFilterNet already removes the
# rumble, and adding 80 Hz on top overshot to -22 dB where CapCut sits at -14.
_TONE = ("equalizer=f=190:t=q:w=1.0:g=4,"       # restore the voice body
         "equalizer=f=9000:t=q:w=1.6:g=-4,")    # take off the brittle top
_LEVEL = ("acompressor=threshold=-20dB:ratio=2.5:attack=10:release=180:makeup=1.5,"
          "alimiter=limit=0.95")

# v938.14 — the low end, which is where the last of the gap was.
#
# DeepFilterNet over-removes rumble: measured on a real job, 60-120 Hz sits at
# +1.0 dB before it and -21.6 dB after, while CapCut leaves that band at
# -13.9 dB. So after denoising we are ~5-8 dB thinner than the file the
# operator says sounds better, in the chest register. A low shelf puts back
# what was over-removed. Swept on real audio: g=5 scores 0.74, g=7 scores 0.73,
# g=9 scores 0.85 — so g=7, and the curve is flat enough that the exact value
# is not delicate.
_VOICE_CHAIN = "bass=g=7:f=110:width_type=q:w=0.7," + _TONE + _LEVEL

# v938.17 — the low-end correction is MEASURED per job, not fixed.
#
# A fixed +7dB shelf assumes the source is the one it was tuned on. It is not:
# on a deep male voice whose fundamental sits at 100-120Hz, the same shelf
# boosts the fundamental instead of replacing stripped rumble, and the mix
# lands at +2.8dB where CapCut sits at -13.9 (measured: the 1f35eac2 render
# scored 4.27 against CapCut's curve with the fixed shelf, worse than the
# 3.47 the whole exercise started from). One number cannot serve both voices.
#
# So: measure this job's own 60-120Hz level against its own 500-2000Hz body,
# and apply whatever gain lands it on CapCut's ratio. Clamped, because a huge
# correction means the estimate is wrong, not that the fix should be huge.
_LOW_TARGET_DB = -13.9      # CapCut's 60-120Hz level, relative to the speech body
_LOW_GAIN_MIN, _LOW_GAIN_MAX = -12.0, 12.0


def low_shelf_gain(src):
    """How much 60-120Hz this file needs to sit where CapCut sits.

    The measurement MUST match code/measure_capcut_match.py exactly — same FFT
    size, same speech-active gate, same body normalisation — because the target
    (-13.9 dB) comes from that scorer. Measuring the same physical thing a
    different way does not give a comparable number: an ffmpeg
    bandpass+volumedetect version of this read the same audio as -12.9 where the
    FFT reads -21.6, because volumedetect averages the whole file including
    silence and the filter skirts leak. Mixing the two scales produced a
    confident correction with the wrong sign.

    Returns a dB gain for the 110Hz shelf, or None if it could not measure
    (caller falls back to the fixed shelf rather than skipping the fix).
    """
    import numpy as np

    raw = subprocess.run(["ffmpeg", "-v", "error", "-i", str(src), "-ac", "1",
                          "-ar", "48000", "-f", "f32le", "-"], capture_output=True).stdout
    x = np.frombuffer(raw, dtype=np.float32)
    nfft, hop = 8192, 4096
    if len(x) < nfft * 2:
        return None
    win = np.hanning(nfft)
    frames = [np.abs(np.fft.rfft(x[s:s + nfft] * win))
              for s in range(0, len(x) - nfft, hop)
              if float(np.sqrt((x[s:s + nfft] ** 2).mean())) >= 0.01]
    if not frames:
        return None
    spec = np.mean(frames, axis=0)
    freq = np.fft.rfftfreq(nfft, 1 / 48000)

    def band(lo, hi):
        return 20 * np.log10(max(spec[(freq >= lo) & (freq < hi)].mean(), 1e-12))

    body = (band(500, 1000) + band(1000, 2000)) / 2
    return float(band(60, 120) - body)


def _low_shelf(gain):
    return f"bass=g={gain:.1f}:f=110:width_type=q:w=0.7"


def fit_low_shelf(src, work: Path):
    """Pick the 110Hz shelf gain that lands this file on CapCut's low end.

    Closed loop, because a shelf is not a band control: it spills into
    120-250Hz and the achieved change never equals the requested one. Measured
    open-loop on a deep male voice, asking for -8.2 dB moved 60-120Hz only to
    -6.6 against a -13.9 target. So: measure, apply to a short probe, measure
    again, and correct by whatever is still missing.

    Returns the gain in dB, or None if it could not measure.
    """
    have = low_shelf_gain(src)
    if have is None:
        return None
    gain = max(_LOW_GAIN_MIN, min(_LOW_GAIN_MAX, _LOW_TARGET_DB - have))

    probe = work / "audio_probe.wav"
    try:
        run(["ffmpeg", "-v", "error", "-t", "40", "-i", str(src),
             "-af", _low_shelf(gain), "-ar", "48000", "-ac", "1", "-y", str(probe)])
        got = low_shelf_gain(probe)
        if got is not None:
            gain = max(_LOW_GAIN_MIN, min(_LOW_GAIN_MAX, gain + (_LOW_TARGET_DB - got)))
    except AutoEditError:
        pass          # keep the open-loop estimate rather than failing the render
    finally:
        probe.unlink(missing_ok=True)
    return gain

# The DeepFilter-SKIPPED path needs the OPPOSITE correction. If denoising did
# not run, the rumble is still there (+1.0 dB), so lifting it makes the voice
# boomy — measured 5.50 with the shelf against 4.07 with a cut. Same tone and
# level either way; only the low-end correction flips.
_VOICE_CHAIN_RAW = "highpass=f=90:p=2," + _TONE + _LEVEL


def enhance_audio(base, work: Path, denoise: bool = True):
    # v938.12 — the cache is keyed on the CHAIN, not just the filename.
    #
    # It used to be a bare `audio_pol.wav` + `if pol.exists(): return pol`.
    # So when the voice chain was rebuilt, every job with a cached file kept
    # serving the OLD audio — byte-identical, proven by md5 — and the new
    # chain silently never ran. The operator would have heard no change at
    # all and reasonably concluded the fix did nothing. compose() already
    # keys its own cache this way; this did not, and that asymmetry is what
    # hid the bug.
    #
    # v938.14 — the cached name also records WHETHER DEEPFILTER RAN, because
    # the two paths need opposite low-end corrections. Only the denoised file
    # is reused; if a previous run fell back (Modal down, network blip) that
    # result is deliberately NOT reused, so a transient outage cannot leave a
    # job permanently serving degraded audio.
    # The measured shelf is derived from the audio itself, so the same source
    # always yields the same gain and the cache stays sound — but the CONSTANTS
    # behind that measurement must invalidate it when they change.
    # v947.4 — the SOURCE is baked into the output, so it belongs in the key
    # (v938.1). Chain-only keying meant a NEW export landing in the same work
    # dir served the OLD export's enhanced voice, and compose (correctly keyed
    # on that audio's fingerprint) then rebuilt the old composite wholesale —
    # measured 2026-08-27 on job 63097756: v3's autoedit shipped v2's video to
    # the millisecond (46.875s), and only QC's duration check caught it.
    # v948.2 — `denoise=False` is the DELIBERATE no-denoiser path (audio_enhance
    # "level"). It reuses the chain the Modal-unavailable fallback already runs
    # — 90Hz cut instead of the +7dB shelf, then the same tone, compressor,
    # limiter and two-pass loudness — because that chain is already measured
    # and shipped. The difference is only in the CACHE: a fallback result is
    # never reused (a transient outage must not leave a job permanently serving
    # degraded audio), but a deliberate choice is a correct result and IS.
    chain_key = f"{audio_chain_key()}_s{file_fingerprint(base)}"
    raw_wav, enh = work / "audio_raw.wav", work / "audio_enh.wav"
    pol = work / audio_cache_name(chain_key, denoised=True)
    pol_raw = work / audio_cache_name(chain_key, denoised=False)
    if denoise and pol.exists():
        print(f"audio: cached (chain {chain_key})")
        return pol
    if not denoise and pol_raw.exists():
        print(f"audio: cached, denoiser deliberately off (chain {chain_key})")
        return pol_raw
    for stale in work.glob("audio_pol*.wav"):
        stale.unlink(missing_ok=True)   # do not hoard one file per chain edit
    run(["ffmpeg", "-v", "error", "-i", str(base), "-vn", "-ac", "1", "-ar", "48000", "-y", str(raw_wav)])
    ok = False
    if denoise:
        try:
            from audio_processor import try_deepfilter_modal
            ok = try_deepfilter_modal(raw_wav, enh)
        except Exception as e:
            print(f"deepfilter modal unavailable: {e}")
    if not ok:
        shutil.copy(raw_wav, enh)
        pol = pol_raw

    # The chains are defined and justified above, where they are measured
    # against CapCut's own output. In short:
    #
    #   +7dB shelf @110Hz   put back the low end DeepFilter over-removed
    #                       (or a 90Hz CUT instead, when DeepFilter did not run)
    #   +4dB @ 190Hz        restore the voice body the old chain cut away
    #   -4dB @ 9kHz         take off the brittle top the old chain added
    #   acompressor         steadier level, gentle ratio so it does not pump
    #   alimiter            catch peaks without clipping
    #
    # Scored as deviation from CapCut's measured tonal curve, on real job audio:
    # 3.47 before v938.13, 1.23 with tone only, 0.73 with the shelf. Check any
    # file with `python code/measure_capcut_match.py <file>`.
    if ok:
        # v938.17 — measure this job's own low end instead of assuming it.
        g = fit_low_shelf(enh, work)
        if g is None:
            chain = _VOICE_CHAIN
            print("audio: low-end measurement failed, using the fixed shelf")
        else:
            chain = _low_shelf(g) + "," + _TONE + _LEVEL
            print(f"audio: low shelf {g:+.1f} dB (fitted to CapCut's -13.9)")
    else:
        chain = _VOICE_CHAIN_RAW

    # Two-pass loudness. One pass guesses and undershoots — the old chain
    # aimed at -15 LUFS and landed at -17.1. Measuring first hits the target.
    ln = "loudnorm=I=-15:TP=-1.2:LRA=9"
    try:
        m = subprocess.run(
            ["ffmpeg", "-v", "info", "-i", str(enh),
             "-af", "loudnorm=I=-15:TP=-1.2:LRA=9:print_format=json", "-f", "null", "-"],
            capture_output=True, text=True, encoding="utf-8", errors="replace").stderr
        blob = json.loads(m[m.rfind("{"): m.rfind("}") + 1])
        ln = (f"loudnorm=I=-15:TP=-1.2:LRA=9:measured_I={blob['input_i']}:"
              f"measured_TP={blob['input_tp']}:measured_LRA={blob['input_lra']}:"
              f"measured_thresh={blob['input_thresh']}:"
              f"offset={blob['target_offset']}:linear=true")
        measured = blob["input_i"]
    except Exception as e:
        measured = f"measure failed ({e}); single pass"

    run(["ffmpeg", "-v", "error", "-i", str(enh), "-af", f"{chain},{ln}",
         "-ar", "48000", "-y", str(pol)])
    _df = "modal" if ok else ("OFF by request" if not denoise else "SKIPPED (raw)")
    print(f"audio: deepfilter={_df} + capcut-matched EQ "
          f"+ two-pass loudness (input {measured} LUFS, chain {chain_key})")
    return pol


def watermark_font():
    """First existing font from WATERMARK_FONT_CANDIDATES, escaped for
    ffmpeg's drawtext filter. A drive-letter colon (C:) is also the
    filter-option separator, so it must be escaped as C\\: or ffmpeg's
    parser misreads the path as a key:value pair."""
    for path in WATERMARK_FONT_CANDIDATES:
        if Path(path).exists():
            if len(path) > 1 and path[1] == ":":
                return path[0] + "\\:" + path[2:]
            return path
    raise AutoEditError(
        "no watermark font found on this machine — tried: " + ", ".join(WATERMARK_FONT_CANDIDATES))


def compose(base, sup, work: Path, dur, hook_end, key_hex, segs, audio,
            pip_y=PIP_Y, pip_enabled=True, chroma_similarity=0.10,
            chroma_blend=0.02, music=None, music_db=-20.0,
            hook_corner=None, broll=None):
    # Every visual/audio repair setting is in the cache name. A re-run with a
    # stronger key or different music must never silently reuse the old video.
    #
    # v938.14 — the AUDIO ITSELF is in the key too, and it was the missing half.
    # This function muxes `audio` into the cached mp4, but the key only ever
    # described the picture and the music. So a rebuilt voice chain produced a
    # new audio file, and compose handed back the old video with the OLD voice
    # still baked in — the fix would ship, the render would succeed, and nothing
    # would sound different. That is the same defect just fixed one stage
    # earlier in enhance_audio; the two caches have to agree or neither works.
    # Fingerprinting the bytes (not the mtime) means an identical re-render
    # still hits the cache.
    cache_key = compose_cache_key(pip_y, pip_enabled, chroma_similarity, chroma_blend,
                                  music, music_db, file_fingerprint(audio), hook_corner)
    nocap = work / f"nocap_wm_{cache_key}.mp4"
    if nocap.exists():
        print("compose: cached")
        return nocap
    inputs = ["-i", str(base)]
    fc_parts, vin, idx = [], "[0:v]", 1
    corner_used = False
    if hook_end > 0 and key_hex:
        bg = work / "hookbg.mp4"
        # v938.15 — the hook background.
        #
        # With hook_corner set we want what the corpus and the operator's own
        # CapCut edits do: the b-roll fills the frame SHARP and carries the
        # meaning, while the keyed speaker shrinks into a corner. `final_broll`
        # is already full-frame 1080x1920, so it is used as-is from its own
        # start. Without it (older jobs export only the 16:9 support track)
        # there is no full-frame b-roll to show, so we keep the original
        # behaviour: a blurred, cropped grab of a later insert as a backdrop.
        if hook_corner and broll is not None:
            # The background may be a VIDEO (final_broll) or a STILL IMAGE — the
            # operator's own 1f35eac2 edit used a still (the black-and-white
            # interview frame) behind the corner speaker, and it reads the same.
            # A still needs -loop 1 or ffmpeg emits a single frame and the hook
            # freezes to one image for a fraction of a second, then goes black.
            is_image = broll.suffix.lower() in (".png", ".jpg", ".jpeg", ".webp", ".bmp")
            cmd = ["ffmpeg", "-v", "error"]
            if is_image:
                cmd += ["-loop", "1"]
            cmd += ["-t", str(hook_end + 0.2), "-i", str(broll),
                    "-vf", "scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920,"
                           "tpad=stop_mode=clone:stop_duration=2,setsar=1",
                    "-an", "-r", "24", "-y", str(bg)]
            run(cmd)
            print(f"hook: full-frame {'still' if is_image else 'b-roll'} "
                  f"+ speaker at {hook_corner:.3f} flush bottom-left")
        else:
            if hook_corner:
                print("hook: no final_broll for this job — corner speaker over the blurred fallback")
            src = segs[3][0] if len(segs) >= 4 else (segs[-1][0] if segs else 0)
            run(["ffmpeg", "-v", "error", "-ss", str(src), "-t", str(hook_end + 0.2), "-i", str(sup or base),
                 "-vf", "crop=608:1080,scale=1080:1920,gblur=sigma=18,eq=brightness=0.02:saturation=1.05,"
                        "tpad=stop_mode=clone:stop_duration=2,setsar=1", "-an", "-r", "24", "-y", str(bg)])
        inputs += ["-i", str(bg)]
        if hook_corner:
            # Flush to the bottom-left corner, which is how every measured
            # source sits — bottom at exactly the frame edge, left at exactly 0,
            # never floated with a margin. Measured reference: the decoded
            # green-screen doctor reads 37.5%W x 29%H and the granny PiP
            # 39%W x 33%H; a 0.43 clip scale lands the cut-out person right
            # there, and 0.429 is what the operator's own CapCut edit used.
            fg_chain = (f"chromakey={key_hex}:{chroma_similarity}:{chroma_blend},despill=type=green,"
                        f"scale=iw*{hook_corner}:ih*{hook_corner}")
            overlay = "overlay=x=0:y=H-h:shortest=1"
            corner_used = True
        else:
            fg_chain = f"chromakey={key_hex}:{chroma_similarity}:{chroma_blend},despill=type=green"
            overlay = "overlay=x=0:y=0:shortest=1"
        fc_parts.append(
            f"{vin}split[b0][b1];"
            f"[b0]trim=0:{hook_end},setpts=PTS-STARTPTS,{fg_chain}[fg];"
            f"[{idx}:v]trim=0:{hook_end},setpts=PTS-STARTPTS[bgt];"
            f"[bgt][fg]{overlay}[hook];"
            f"[b1]trim={hook_end},setpts=PTS-STARTPTS[rest];"
            f"[hook][rest]concat=n=2:v=1:a=0[v0]")
        vin, idx = "[v0]", idx + 1
    if pip_enabled and segs and sup is not None:
        mask = work / "pipmask.png"
        if not mask.exists():
            run(["ffmpeg", "-v", "error", "-f", "lavfi", "-i", f"color=c=white:s={PIP_W}x{PIP_H}", "-vf",
                 f"format=gray,geq=lum='255*lte(hypot(max(0,max(40-X,X-{PIP_W-41})),max(0,max(40-Y,Y-{PIP_H-41}))),40)'",
                 "-frames:v", "1", "-y", str(mask)])
        inputs += ["-i", str(sup), "-loop", "1", "-i", str(mask)]
        enable = "+".join(f"between(t,{a},{b})" for a, b in segs)
        fc_parts.append(
            f"[{idx}:v]scale={PIP_W}:{PIP_H},setsar=1[supS];"
            f"[{idx + 1}:v]format=gray[am];"
            f"[supS][am]alphamerge[supA];"
            f"{vin}[supA]overlay=x={PIP_X}:y={pip_y}:enable='{enable}'[v1]")
        vin, idx = "[v1]", idx + 2
    inputs += ["-i", str(audio)]
    aidx = idx
    audio_map = f"{aidx}:a"
    if music is not None:
        inputs += ["-stream_loop", "-1", "-i", str(music)]
        midx = aidx + 1
        delay_ms = max(0, int(round(hook_end * 1000)))
        body_dur = max(0.1, dur - hook_end)
        fc_parts.append(
            f"[{midx}:a]atrim=0:{body_dur},asetpts=PTS-STARTPTS,volume={music_db}dB,"
            f"adelay={delay_ms}:all=1[music];"
            f"[{aidx}:a][music]amix=inputs=2:duration=first:normalize=0[aout]")
        audio_map = "[aout]"
    fontfile = watermark_font()
    # v938.15 — the disclosure watermark normally sits bottom-left, which is
    # exactly where the corner speaker goes. §14.1 requires it to stay legible
    # on every frame, so when the corner is in use it moves to the bottom-RIGHT
    # for the whole video (one position all the way through — a watermark that
    # jumps mid-video reads as a glitch). Several source decodes note the same
    # collision and solve it the same way: move one of the two.
    wm_x = "w-tw-44" if corner_used else "44"
    fc_parts.append(f"{vin}drawtext=text='syntheticperformer':fontfile='{fontfile}'"
                    f":fontcolor=white@0.5:fontsize=34:x={wm_x}:y=h-78[vout]")
    print("compose: rendering base (no captions) ...")
    run(["ffmpeg", "-v", "error", *inputs, "-filter_complex", ";".join(fc_parts),
         "-map", "[vout]", "-map", audio_map, "-c:v", "libx264", "-crf", "19", "-preset", "medium",
         "-r", "24", "-c:a", "aac", "-b:a", "192k", "-t", str(dur), "-movflags", "+faststart", "-y", str(nocap)])
    return nocap


def render_captions(nocap: Path, out: Path, template: str, offset=-0.05, subtitle_data=None):
    """Burn captions once, at one vertical offset.

    `subtitle_data` replays the transcription+tagging pycaps already did for
    this exact video, which is ~45% of the work and identical across offsets.
    See render_captions_dynamic for why that matters and how it was measured.
    """
    cwd = str(TEMPLATES_DIR) if template in local_styles() else None
    out.unlink(missing_ok=True)  # pycaps refuses to overwrite
    cmd = [pycaps_exe(), "render", "--input", str(nocap), "--output", str(out),
           "--template", template, "--layout-align", "center",
           "--layout-align-offset", str(offset)]
    if subtitle_data and Path(subtitle_data).exists():
        cmd += ["--subtitle-data", str(subtitle_data)]
        print(f"captions: pycaps template={template} (reusing transcript)")
    else:
        print(f"captions: pycaps template={template}")
    r = subprocess.run(cmd,
                       capture_output=True, text=True, encoding="utf-8", errors="replace",
                       cwd=cwd, env={**os.environ, "PYTHONIOENCODING": "utf-8"})
    if r.returncode != 0 or not out.exists():
        detail = ((r.stderr or "") + "\n" + (r.stdout or "")).strip()
        print(detail[-2000:], flush=True)
        # Carry the real reason into the error itself. "pycaps render failed"
        # alone is what the operator saw in the UI while the actual cause
        # (a missing graphics library) sat only in the server log.
        tail = " | ".join(ln.strip() for ln in detail.splitlines()
                          if ln.strip() and ("Error" in ln or "error" in ln))[-400:]
        raise AutoEditError(f"pycaps render failed: {tail or detail[-200:] or 'no output'}")


def render_captions_dynamic(nocap: Path, out: Path, template: str, windows, work: Path):
    """Render pycaps once per distinct height, then splice the passes by time
    window (captions land identically because the input/transcript is identical)."""
    offsets = sorted({o for _, _, o in windows})
    passes = {}
    # v938.16 — the pass name carries a fingerprint of the VIDEO it was burned
    # over. It used to be offset+template only, so a rerun after ANY change to
    # the composite (new hook layout, new audio, new keying) silently reused
    # yesterday's captioned passes and threw the fresh composite away. Measured:
    # a corner-layout render produced a correct nocap and then shipped the old
    # full-size hook, because cap_pass_0_2_korella.mp4 already existed.
    # Fourth instance of the same defect in this file — see enhance_audio and
    # compose. Anything cached here must be named after everything baked into it.
    src_key = file_fingerprint(nocap)
    first_data = None          # v938.23 — the transcript every later pass replays
    for o in offsets:
        tag = str(o).replace('-', 'm').replace('.', '_')
        p = work / cap_pass_name(o, template, src_key)
        for stale in work.glob(f"cap_pass_{tag}_{template}_*.mp4"):
            if stale != p:
                stale.unlink(missing_ok=True)
        legacy = work / f"cap_pass_{tag}_{template}.mp4"   # pre-v938.16 name
        legacy.unlink(missing_ok=True)
        if not p.exists():
            # v938.23 — every pass after the first replays the FIRST pass's
            # transcription instead of redoing it.
            #
            # This stage is the slow one, and it costs one full pycaps pass per
            # distinct caption height: a plan with three heights ran 32 minutes
            # on the server (job 732b7f8f, 22:43 -> 22:54 -> 23:04 in the render
            # log), while a two-height plan took four. Timed on a 30s clip, a
            # pass is ~14s transcribing and ~13s drawing frames — and the
            # transcript is IDENTICAL across offsets, because only the vertical
            # position changes.
            #
            # pycaps writes its subtitle data beside the output, and
            # --subtitle-data replays it, skipping transcription and tagging.
            # Measured: 31s -> 17s, a 45% cut per extra pass. Verified it still
            # honours a DIFFERENT offset rather than baking in the first one —
            # caption centre 0.59 at +0.10 vs 0.29 at -0.25, same text, same
            # timing. That check is the whole reason this is safe.
            render_captions(nocap, p, template, o, subtitle_data=first_data)
        passes[o] = p
        # pycaps names it <output-stem>.json; the first pass to land owns it.
        if first_data is None:
            cand = p.with_suffix(".json")
            if cand.exists():
                first_data = cand
    if len(offsets) == 1:
        shutil.copy(passes[offsets[0]], out)
        return
    # base pass = the offset covering the most time; overlay the rest per window
    time_per = {o: sum(b - a for a, b, oo in windows if oo == o) for o in offsets}
    base_o = max(time_per, key=time_per.get)
    inputs, fc, vin = ["-i", str(passes[base_o])], [], "[0:v]"
    idx = 1
    for o in offsets:
        if o == base_o:
            continue
        enable = "+".join(f"between(t,{a},{b})" for a, b, oo in windows if oo == o)
        inputs += ["-i", str(passes[o])]
        fc.append(f"{vin}[{idx}:v]overlay=enable='{enable}'[v{idx}]")
        vin = f"[v{idx}]"
        idx += 1
    out.unlink(missing_ok=True)
    run(["ffmpeg", "-v", "error", *inputs, "-filter_complex", ";".join(fc),
         "-map", vin.strip("[]").join(["[", "]"]), "-map", "0:a",
         "-c:v", "libx264", "-crf", "19", "-preset", "medium",
         "-c:a", "copy", "-movflags", "+faststart", "-y", str(out)])


def trim_media(source: Path, dest: Path, start_s: float, end_s: float, dur: float):
    """Trim a media file and return (path, new_duration)."""
    if start_s <= 0 and end_s <= 0:
        return source, dur
    new_dur = dur - start_s - end_s
    if new_dur < 1.0:
        raise AutoEditError(
            f"trim removes the whole video ({dur:.2f}s source, {start_s:.2f}s + {end_s:.2f}s trim)")
    if not dest.exists():
        run(["ffmpeg", "-v", "error", "-ss", str(start_s), "-i", str(source),
             "-t", str(new_dur), "-map", "0:v:0", "-map", "0:a?",
             "-c:v", "libx264", "-crf", "19", "-preset", "medium",
             "-c:a", "aac", "-b:a", "192k", "-movflags", "+faststart", "-y", str(dest)])
    return dest, new_dur


def shifted_segments(segs, start_s, new_dur):
    shifted = []
    for a, b in segs:
        a2, b2 = max(0.0, a - start_s), min(new_dur, b - start_s)
        if b2 - a2 > 0.25:
            shifted.append((round(a2, 2), round(b2, 2)))
    return shifted


def probe_media(path: Path):
    r = run(["ffprobe", "-v", "error", "-show_streams", "-show_format",
             "-of", "json", str(path)])
    return json.loads(r.stdout)


def audio_levels(path: Path):
    """Return ffmpeg volumedetect levels, or None when they cannot be parsed."""
    import re
    r = subprocess.run(
        ["ffmpeg", "-v", "info", "-i", str(path), "-vn", "-af", "volumedetect", "-f", "null", "-"],
        capture_output=True, text=True, encoding="utf-8", errors="replace")
    text = (r.stderr or "") + (r.stdout or "")
    mean = re.search(r"mean_volume:\s*(-?[\d.]+) dB", text)
    peak = re.search(r"max_volume:\s*(-?[\d.]+) dB", text)
    if not mean or not peak:
        return None
    return {"mean_db": float(mean.group(1)), "peak_db": float(peak.group(1))}


def green_spill_ratio(path: Path, hook_end: float):
    """Sample the keyed hook and measure strong-green pixels."""
    if hook_end <= 0:
        return None
    import cv2
    import numpy as np
    cap = cv2.VideoCapture(str(path))
    fps = cap.get(cv2.CAP_PROP_FPS) or 24
    ratios = []
    for t in np.linspace(0.2, max(0.2, hook_end - 0.1), num=min(8, max(2, int(hook_end * 2)))):
        cap.set(cv2.CAP_PROP_POS_FRAMES, int(t * fps))
        ok, frame = cap.read()
        if not ok:
            continue
        b, g, r = cv2.split(frame.astype(np.float32))
        ratios.append(float(((g > 70) & (g > r * 1.25) & (g > b * 1.15)).mean()))
    cap.release()
    return max(ratios) if ratios else None


def pip_difference_ratio(output: Path, base: Path, segs, pip_y: int):
    """Check that a requested PIP made a visible change in its target region."""
    if not segs:
        return None
    import cv2
    import numpy as np
    t = (segs[0][0] + segs[0][1]) / 2

    def frame_at(path):
        cap = cv2.VideoCapture(str(path))
        cap.set(cv2.CAP_PROP_POS_MSEC, t * 1000)
        ok, frame = cap.read()
        cap.release()
        return frame if ok else None

    a, b = frame_at(output), frame_at(base)
    if a is None or b is None or a.shape != b.shape:
        return None
    h, w = a.shape[:2]
    x1, x2 = int(PIP_X / 1080 * w), int((PIP_X + PIP_W) / 1080 * w)
    y1, y2 = int(pip_y / 1920 * h), int((pip_y + PIP_H) / 1920 * h)
    return float(np.abs(a[y1:y2, x1:x2].astype(np.float32)
                        - b[y1:y2, x1:x2].astype(np.float32)).mean())


def run_quality_checks(output: Path, base: Path, expected_dur: float, buckets,
                       windows, segs, pip_y: int, hook_end: float, repairs):
    from autoedit_qc import build_qc_report, caption_face_overlap_metrics

    checks = []
    try:
        media = probe_media(output)
        streams = media.get("streams", [])
        video = next((s for s in streams if s.get("codec_type") == "video"), None)
        audio = next((s for s in streams if s.get("codec_type") == "audio"), None)
        portrait = bool(video and int(video.get("width", 0)) == 1080 and int(video.get("height", 0)) == 1920)
        checks.append({"id": "portrait_video", "status": "pass" if portrait else "fail",
                       "message": "Video is 1080×1920" if portrait else "Video is not 1080×1920"})
        checks.append({"id": "audio_stream", "status": "pass" if audio else "fail",
                       "message": "Audio stream is present" if audio else "The finished video has no audio"})
        actual_dur = float(media.get("format", {}).get("duration", 0.0))
        dur_ok = abs(actual_dur - expected_dur) <= max(0.6, expected_dur * 0.01)
        checks.append({"id": "duration", "status": "pass" if dur_ok else "fail",
                       "message": (f"Duration matches ({actual_dur:.2f}s)" if dur_ok else
                                   f"Duration mismatch: expected {expected_dur:.2f}s, got {actual_dur:.2f}s"),
                       "value": actual_dur})
    except Exception as exc:
        checks.append({"id": "media_probe", "status": "fail",
                       "message": f"Could not inspect the finished file: {exc}"})

    # v938.20 — report how close the finished audio sits to CapCut's measured
    # tonal curve. The level check above answers "is it loud enough"; this
    # answers "does it sound like the reference", which is the thing the
    # operator actually judged and which nothing was watching.
    #
    # NEVER a fail. A `fail` flips the verdict to NEEDS_MANUAL_EDIT, and the
    # target is an average of five exports that are mostly ONE speaker — a
    # deep male voice legitimately scores worse without anything being wrong
    # (§v938.2). Blocking a delivery on that would be the v936.2 mistake:
    # a soft signal given a hard gate. So it is recorded, with the scale
    # attached, and the human decides.
    try:
        from measure_capcut_match import band_curve, score as capcut_score
        tone = round(float(capcut_score(band_curve(output))), 2)
    except Exception:
        tone = None
    if tone is not None:
        close = tone <= 2.5
        checks.append({
            "id": "audio_tone_match",
            "status": "pass" if close else "info",
            "message": (f"Voice tone matches CapCut ({tone} dB deviation; a real CapCut "
                        f"export scores 1.78 against its own siblings)" if close else
                        f"Voice tone is {tone} dB off CapCut's curve — worth a listen. "
                        f"Expected under ~2.5; a very deep or unusual voice can score "
                        f"higher without anything being wrong"),
            "value": tone})

    try:
        levels = audio_levels(output)
    except Exception:
        levels = None
    level_ok = bool(levels and -23.0 <= levels["mean_db"] <= -10.0
                    and -8.0 <= levels["peak_db"] <= -0.1)
    checks.append({"id": "audio_levels", "status": "pass" if level_ok else "fail",
                   "message": (f"Voice level is usable ({levels['mean_db']:.1f} dB mean, "
                               f"{levels['peak_db']:.1f} dB peak)" if levels else
                               "Could not measure the finished audio level") if level_ok else
                              (f"Audio needs review ({levels['mean_db']:.1f} dB mean, "
                               f"{levels['peak_db']:.1f} dB peak)" if levels else
                               "Could not measure the finished audio level"),
                   "value": levels})

    if repairs["captions_enabled"]:
        overlap = caption_face_overlap_metrics(buckets, windows)
        overlap_ok = overlap["worst_vertical_px"] <= 40.0
        checks.append({"id": "caption_face_clearance", "status": "pass" if overlap_ok else "fail",
                       "message": ("Captions stay clear of detected faces" if overlap_ok else
                                   f"Captions overlap a detected face by up to "
                                   f"{overlap['worst_vertical_px']:.0f}px"),
                       "value": overlap})
    else:
        checks.append({"id": "captions", "status": "pass",
                       "message": "Captions were intentionally disabled"})

    try:
        spill = green_spill_ratio(output, hook_end)
        spill_ok = spill is None or spill <= 0.02
        spill_message = ("Green-screen hook is clean" if spill_ok else
                         f"Green spill remains in about {spill * 100:.1f}% of the keyed hook")
    except Exception as exc:
        spill, spill_ok = None, False
        spill_message = f"Could not verify the green-screen hook: {exc}"
    checks.append({"id": "green_key", "status": "pass" if spill_ok else "fail",
                   "message": spill_message, "value": spill})

    if repairs["pip_enabled"] and segs:
        try:
            diff = pip_difference_ratio(output, base, segs, pip_y)
            pip_ok = diff is not None and diff >= 5.0
            pip_message = ("Support footage is visible" if pip_ok else
                           "Support footage was requested but was not visibly detected")
        except Exception as exc:
            diff, pip_ok = None, False
            pip_message = f"Could not verify the support footage: {exc}"
        checks.append({"id": "support_footage", "status": "pass" if pip_ok else "fail",
                       "message": pip_message,
                       "value": diff})
    else:
        checks.append({"id": "support_footage", "status": "pass",
                       "message": "Support footage was unavailable or intentionally disabled"})
    return build_qc_report(checks)


def prepare_composition(job_id: str, work: Path, progress=lambda stage: None, repairs=None):
    """Everything up to (and including) the uncaptioned composed video.
    Returns (nocap_path, dur, segs, auto_offset, pip_y, chin, base_path, audio_path).
    `base_path` is returned because the caption-placement stage's occupancy scan
    needs the raw downloaded video, not the composed one — dropping it would
    change what build_occupancy() scans on an uncached run.
    `audio_path` is returned (v938.14) because the libass caption fallback needs
    the ENHANCED audio and used to guess its filename; see _render_caption_pass."""
    from autoedit_qc import normalize_repairs
    repairs = normalize_repairs(repairs)
    work.mkdir(parents=True, exist_ok=True)
    progress("download")
    base, sup, music, broll = fetch_job_files(
        job_id, work, repairs["music_filename"], repairs.get("hook_bg"))
    dur = probe_duration(base)
    scan_file = work / "scan.json"
    s = json.loads(scan_file.read_text()) if scan_file.exists() else {}
    if "hook_end" not in s:
        progress("scan")
        hook_end, key_hex, segs = scan_tracks(base, sup, dur)
        s.update({"hook_end": hook_end, "key_hex": key_hex, "segs": segs})
    else:
        hook_end, key_hex, segs = s["hook_end"], s["key_hex"], [tuple(x) for x in s["segs"]]
        print("scan: cached")
    # The 4th element records WHICH face detector produced these numbers — a
    # Haar-era chin must be re-measured when YuNet is active (v938.1).
    if len(s.get("layout", [])) != 4 or s["layout"][3] != face_detector_tag():
        progress("layout")
        auto_offset, pip_y, chin = detect_layout(base, dur, segs)
        s["layout"] = [auto_offset, pip_y, chin, face_detector_tag()]
    else:
        auto_offset, pip_y, chin = s["layout"][:3]
        print(f"layout: cached (offset {auto_offset:+.3f}, pip_y {pip_y}, chin {chin:.2f})")
    scan_file.write_text(json.dumps(s))
    progress("audio")
    _audio_mode = repairs.get("audio_enhance", "voice")
    if _audio_mode == "off":
        # v947.2 — source-original / music-bed videos: the export's audio IS the
        # final audio. Extract it untouched; compose muxes and fingerprints it
        # exactly like an enhanced wav, so the cache contract is unchanged.
        # Source fingerprint in the name (v947.4 / v938.1): a new export must
        # never reuse the previous export's extracted audio.
        audio = work / f"audio_source_{file_fingerprint(base)}.wav"
        if not audio.exists():
            run(["ffmpeg", "-y", "-i", str(base), "-vn", "-acodec", "pcm_s16le",
                 "-ar", "48000", "-ac", "2", str(audio)])
        print("audio: enhance OFF — export audio passed through untouched")
    elif _audio_mode == "level":
        # v948.2 — everything except the denoiser. For a v948-swept export:
        # keeps the loudness pass (without it a swept final measured -25.1 LUFS
        # against a -14.3 published reference) while leaving the quiet room tone
        # the denoiser would crush back into silence.
        audio = enhance_audio(base, work, denoise=False)
    else:
        audio = enhance_audio(base, work)
    hook_corner = resolve_hook_corner(
        repairs.get("hook_corner"), hook_end, key_hex, broll is not None)
    if hook_corner is not None and repairs.get("hook_corner") is None:
        print(f"[compose] hook layout AUTO: keyed hook ({hook_end:.1f}s) + "
              f"full-frame background present → corner speaker at "
              f"{hook_corner:.2f} (the documented corpus rule; pass "
              f"hook_corner=0 to disable)", flush=True)
    progress("compose")
    nocap = compose(
        base, sup, work, dur, hook_end, key_hex, segs, audio, pip_y,
        pip_enabled=repairs["pip_enabled"],
        chroma_similarity=repairs["chroma_similarity"],
        chroma_blend=repairs["chroma_blend"],
        music=music, music_db=repairs["music_db"],
        hook_corner=hook_corner, broll=broll,
    )
    start_s, end_s = repairs["trim_start_s"], repairs["trim_end_s"]
    trim_key = f"s{start_s:.2f}_e{end_s:.2f}"
    nocap, trimmed_dur = trim_media(nocap, work / f"nocap_trim_{trim_key}.mp4",
                                    start_s, end_s, dur)
    base, _ = trim_media(base, work / f"base_trim_{trim_key}.mp4",
                         start_s, end_s, dur)
    segs = shifted_segments(segs, start_s, trimmed_dur)
    return nocap, trimmed_dur, segs, auto_offset, pip_y, chin, base, audio


def resolve_hook_corner(hook_corner, hook_end, key_hex, has_fullframe_bg):
    """Which hook layout this render uses (pure function).

    The corner rule is REUSABLE, not a per-run setting to remember (operator
    2026-08-25: "how can we apply this to all the jobs that need it").
    docs/experiments/autoedit-hook-composite-placement-2026-08-22.md §4 states
    the rule conditionally — green-keyed hook + a sharp full-frame background
    → speaker at 0.43 flush bottom-left for the hook only — so when the
    ingredients are present the layout applies BY ITSELF:

    - an explicit hook_corner always wins (0 or negative = explicitly OFF);
    - None = AUTO: 0.43 when the job HAS a keyed hook AND a full-frame
      background (final_broll_* or an explicit/inherited hook_bg);
    - otherwise the legacy full-size-over-blurred layout.
    """
    if hook_corner is not None:
        return hook_corner if hook_corner > 0 else None
    if hook_end and hook_end > 0 and key_hex and has_fullframe_bg:
        return 0.43
    return None


def caption_engine() -> str:
    """Which renderer draws the captions.

    **pycaps is the quality path and the default.** It is a purpose-built
    caption tool: CSS templates rendered through a real browser engine, with
    proper typography, rounded highlights and animation, plus a dozen ready
    presets. It is what makes these look like TikTok/CapCut captions rather
    than burned-in subtitles.

    `libass` is a FALLBACK, not an equal. It draws with ffmpeg, so it needs no
    browser and runs anywhere, but it cannot do rounded corners, pop-in, or the
    non-korella presets — it looks visibly more homemade. It exists so a
    machine without pycaps still produces a video instead of failing.

    Order: an explicit AUTOEDIT_CAPTION_ENGINE wins; otherwise pycaps when it
    is actually installed; otherwise libass.
    """
    forced = (os.environ.get("AUTOEDIT_CAPTION_ENGINE") or "").strip().lower()
    if forced in ("pycaps", "libass"):
        return forced
    try:
        pycaps_exe()
        return "pycaps"
    except AutoEditError:
        return "libass"


def _render_caption_pass(nocap: Path, out: Path, template: str, windows, work: Path,
                         dur: float, audio: Path):
    """One entry point for both caption renderers, so run_autoedit does not care."""
    engine = caption_engine()
    if engine == "pycaps":
        if len({o for _, _, o in windows}) > 1:
            render_captions_dynamic(nocap, out, template, windows, work)
        else:
            render_captions(nocap, out, template, windows[0][2] if windows else -0.05)
        return
    import autoedit_captions as _ac
    print("captions: pycaps unavailable — falling back to the plainer libass "
          "renderer (no rounded highlight, no pop-in)", flush=True)
    if not _ac.supports(template):
        # A pycaps-only style cannot be drawn by libass; say so rather than
        # silently shipping a different look than the operator picked.
        raise AutoEditError(
            f"Caption style '{template}' needs the browser renderer (pycaps), "
            f"which is not installed here. The fallback renderer only has: "
            f"{', '.join(sorted(_ac.STYLES))}.")
    # v938.14 — the caller hands us the enhanced audio. This used to be
    # `work / "audio_pol.wav"` with a fallback to `audio_enh.wav`, and once the
    # audio cache started keying its filename on the voice chain, that hardcoded
    # name stopped existing — so this path silently fell through to audio_enh,
    # which is the DENOISED-BUT-UN-EQ'd file. The libass fallback would have
    # shipped video with none of the CapCut matching applied, and nothing would
    # have said so. Guessing a filename is what made that possible.
    _ac.render(nocap, out, template, windows, audio, work)


# ===========================================================================
# v944 — THE READ-CAPTION TEXT OVERLAY.
#
# Ported from tools/readcaption_overlay.py, which was the proven local tool and
# is now a thin wrapper around this. ONE implementation, the flow_charswap
# precedent: the tool and the platform must never be able to draw a different
# overlay from the same declaration.
#
# The tool's own docstring is the contract, and it is repeated here because it
# is the reason every number below is what it is:
#
#   Type is not guessed. It was matched against @agelessjudy frames on three
#   terms at once -- band height, x-width and ink density -- and re-calibrated
#   against @noemi_healthy_men's own live posts. Century Gothic Bold won on all
#   three; the letterforms agree (single-story 'a', straight-tailed 'y',
#   tailless 't').
#
#   Placement is per clip and must hold for the WHOLE video, because the
#   overlay is burned for its whole duration. Two hard constraints:
#     * Reels safe zone y 6%..79% -- the bottom ~420px of 1920 is platform UI.
#       The same numbers plan_caption_windows above already enforces.
#     * Never cross the FACE at any moment. Crossing legs, torso, a treadmill
#       or a chalkboard is fine -- both reference accounts do it constantly.
#
#   The age line and the body block are SEPARATE elements, not one stack: on
#   @agelessjudy the body block disappears at t=10.2s while the age line and
#   (READ CAPTION) stay to the end, and on @noemi the body block holds a fixed
#   position while only the age line moves to dodge the runner.
#
# THREE DELIBERATE DIFFERENCES FROM THE TOOL, and no others:
#   1. Names carry an `rc_` / `RC_` prefix. This module is 1600 lines with its
#      own vocabulary; a bare `layout` or `build` here would read as the
#      pipeline's own and would collide with the next one added.
#   2. `SystemExit` becomes `AutoEditError`. This module's docstring forbids
#      SystemExit in library code for a measured reason: it derives from
#      BaseException, so a worker's `except Exception` misses it and the
#      process dies silently instead of reporting the failure.
#   3. The occupancy engine is called DIRECTLY. The tool importlib-loaded this
#      file to reach build_occupancy; inside it, that is just a function call.
#
# Every heavy import (PIL, ultralytics, cv2, numpy) stays inside a function
# body, so this module still imports on Render where none of them exist.
# ===========================================================================

RC_FONT = "C:/Windows/Fonts/GOTHICB.TTF"          # Century Gothic Bold
RC_SAFE_TOP, RC_SAFE_BOTTOM = 0.06, 0.79          # Reels UI, ORGANIC

# THESE ARE THE ORGANIC NUMBERS, AND THEY ARE NOT THE AD NUMBERS (2026-08-24).
# Meta's Ads Guide for Reels asks for 0.14 / 0.65 / 0.06 free of text; YouTube
# publishes 0.15 / 0.65 for Shorts ads. Ours is WIDER than either, and it is
# kept because it is measured off organic posts that are working:
# @noemi_healthy_men runs ink down to 0.79 and @niastrong7 to 0.82, and both
# start above 0.07. Organic Reels has no CTA button, which is what the ad
# bottom margin reserves. So: correct for organic, and every build in this lane
# would FAIL an ad review on the bottom margin. Promote one as an ad and the
# block has to move up to 0.65 with shorter copy.
#
# THE SIDE MARGINS ARE THE REAL GAP. Neither number constrains x at all, and
# the block runs to 0.95. TikTok's own safe-zone template narrows the usable
# width to x 0.11-0.72 below y=0.44, because the action rail lives there. §13
# says every winner gets distributed to TikTok, so a block that reads fine on
# Reels has its right end under the like/comment/share buttons on TikTok.
RC_TIKTOK_RAIL_X = 0.72       # usable right edge below RC_TIKTOK_RAIL_Y
RC_TIKTOK_RAIL_Y = 0.44

# RE-CALIBRATED AGAINST @noemi_healthy_men'S OWN LIVE POSTS (2026-08-24).
# Operator: "i don't like the style and font / and the spacing of the text
# itself" -- and the rule behind it: "the style should match the vibe of each
# account and video." Measured against the account we actually post to, three
# numbers were wrong and they are the three he named: the outline ring was far
# too thin (0.5-0.7 against his 2.1-2.6), the body line pitch was cramped (49px
# against his 73-92), and the tag was rendering as wide as a headline.
RC_OUTLINE = 10                                   # px at 720 wide; was 5

# Sizes re-tuned once the renderer moved to libass: an ASS Fontsize is an em
# box, a PIL size is not, so the PIL-era numbers came out 15-25% off. These are
# measured back against the SAME strings in his own post `DcO9hDTonCa`.
RC_SPEC = {                                       # size, tracking, at 720x1280
    "age":   (94, 0),                             # tracking was 4; his tag has none
    "body":  (47, 0),
    "route": (52, 0),                             # his route is SMALLER than his body
}
# Leading, re-measured on his own posts. `DcO9hDTonCa` sets its three lines at
# y 0.632 / 0.704 / 0.761 of frame height -- pitches of 92px and 73px at 1280.
# The old 49px came from the judy match and is the "spacing of the text itself"
# the operator rejected: it packs three lines into the height his account gives
# two. The route line is NOT a separately-spaced element on his account, it is
# just the next line, so its gap is the same order as the body pitch.
RC_BODY_PITCH = 49        # MEASURED from the account's own posted winner
# (Dbn4yKwxCrl seg1, 1080x1920): block line centers 1236/1309/1380/1449 ->
# pitch 71px at 1080 = 47.3 in spec units; the original 49 renders 73.5px and
# matches. The 2026-08-24 "84" was a mismeasurement (rendered ~125px, nearly
# double the reference) and the operator flagged the spacing on two finals
# before this was traced (2026-08-26).
RC_GAP_AGE_BODY = 67      # age and block are placed independently anyway
RC_GAP_BODY_ROUTE = 84    # was 85 measured as a DOUBLE gap; his is one more line

RC_MAX_TEXT_W = 0.90      # reference longest line measures 93% of frame width

# The age tag is a TAG, not a headline. Measured over 77 samples of
# @ginadrewalowski plus the two live @noemi posts, the top element spans
# 0.24-0.48 of frame width. Ours was rendering at 0.90 -- roughly double --
# which was the loudest single difference between our frames and theirs.
RC_AGE_MAX_W = 0.35       # his own tag measures 0.31 of frame width
# ...but only when the string is actually a TAG. Every reference top element is
# short -- `60 years`, `I'M 74`. Forcing a whole hook line into a tag's width
# just shrinks it to 58% and reads as a mistake, so a longer top element is
# treated as a LINE and keeps the block's width budget.
RC_AGE_TAG_CHARS = 10

# PIL PREDICTS a line's width; libass DRAWS it, and the two disagree. Measured
# on the same string and font at three sizes: libass comes out at 0.81, 0.82
# and 0.78 of PIL's number. Left uncorrected, rc_fit_scale shrank the body block
# by 33% to fit a width it was never going to reach.
RC_ASS_PIL_WIDTH_RATIO = 0.81

# WHERE THE OVERLAY SITS RELATIVE TO THE SUBJECT (2026-08-24).
# Operator: "it's not the absolute position of the overlays (still outside the
# danger zone), but where they sit compared to the subject and action."
#
# NOT motion. Across 172 text rows in 10 reference reels the median sits at the
# 38th percentile of the subject's own motion profile -- a weak lean toward
# quiet, nowhere near strong enough to be the rule.
#
# It is the SUBJECT'S BODY. Expressing every text band as a depth -- 0.0 at the
# crown, 1.0 at the feet, in units of the subject's own height -- the reference
# lands in two tight clusters and never between them. Their absolute frame
# numbers are also tight, but that is a CONSEQUENCE: she is framed the same way
# every time. Our renders inherit whatever framing the source had, so copying
# her absolute numbers reproduces her look only by accident. Copy the relation.
RC_AGE_DEPTH = (-0.32, 0.12)      # crown line
RC_BLOCK_DEPTH = (0.28, 0.82)     # hip line down to the feet

RC_FONT_CANDIDATES = [
    RC_FONT,
    "C:/Windows/Fonts/gothicb.ttf",
]


def _rc_font():
    """The typeface file, resolved LATE and with a real message.

    Deliberately not resolved at import: this module must stay importable on
    Render, which has no Windows fonts and never runs this stage. A missing
    font has to be a clear failure of THIS stage, not of the whole module.
    """
    for cand in RC_FONT_CANDIDATES:
        if os.path.exists(cand):
            return cand
    raise AutoEditError(
        f"The read-caption overlay needs Century Gothic Bold and none of "
        f"{RC_FONT_CANDIDATES} exists on this machine. The overlay is matched to "
        f"the reference accounts on this exact face — substituting another one "
        f"changes the look, so this fails instead of guessing.")


def rc_age_width_limit(text):
    return RC_AGE_MAX_W if len(text or "") <= RC_AGE_TAG_CHARS else RC_MAX_TEXT_W


def rc_line_width(d, text, size, tracking, scale):
    from PIL import ImageFont
    f = ImageFont.truetype(_rc_font(), int(round(size * scale)))
    ws = [d.textlength(c, font=f) for c in text]
    return sum(ws) + tracking * scale * (len(text) - 1)


def rc_fit_scale(d, lines, size, tracking, W, scale, limit_frac=None, ass=False):
    """Uniform shrink so the WIDEST line fits. Scaling the block as a whole keeps
    the type consistent; shrinking one line alone reads as a mistake.

    `ass=True` when the caller renders through libass rather than PIL.
    """
    widest = max((rc_line_width(d, t, size, tracking, scale) for t in lines), default=0)
    if ass:
        widest *= RC_ASS_PIL_WIDTH_RATIO
    limit = W * (RC_MAX_TEXT_W if limit_frac is None else limit_frac)
    if widest <= limit or widest == 0:
        return 1.0
    return limit / widest


def _rc_warn_side_margins(events, W, H, scale, scales):
    """Flag text that would sit under TikTok's action rail on a cross-post."""
    from PIL import Image as _I, ImageDraw as _D
    d = _D.Draw(_I.new("RGBA", (8, 8)))
    worst = 0.0
    for t0, t1, y, text, sty in events:
        if y / H < RC_TIKTOK_RAIL_Y:
            continue
        size, tracking = RC_SPEC[sty]
        k = scales.get(sty, 1.0)
        w = rc_line_width(d, text, size, tracking, scale * k) * RC_ASS_PIL_WIDTH_RATIO
        worst = max(worst, 0.5 + (w / W) / 2)
    if worst > RC_TIKTOK_RAIL_X:
        print(f"  NOTE: text reaches x={worst:.2f} below y={RC_TIKTOK_RAIL_Y:.2f}; "
              f"TikTok's action rail starts at x={RC_TIKTOK_RAIL_X:.2f}. Fine on "
              f"Reels (the reference accounts do the same), but the right end "
              f"sits under the buttons on a TikTok cross-post.", flush=True)
    return worst


def rc_head_band(src, samples=12, head_frac=0.20):
    """Where the FACE lives over the whole clip, on ANY video, with no manual input.

    Person detection, not face detection. build_occupancy above uses a face
    cascade, which is the right idea and the wrong detector for this material:
    on these clips it found 1 face in 10 samples (sunglasses, motion, profile
    turns) and a false positive in the pavement. Background subtraction is worse
    on GENERATED footage, where foliage, shimmer and camera drift all move.

    YOLO person boxes land 10/10 on the same clips. The head is the top fifth of
    a standing person box; union that across time and you have the band the
    overlay must never enter. Returns (top, bottom) as fractions, or None.
    """
    try:
        from ultralytics import YOLO
    except Exception:
        return None
    import tempfile as _tf
    dur = float(subprocess.run(["ffprobe", "-v", "error", "-show_entries",
                                "format=duration", "-of", "csv=p=0", str(src)],
                               capture_output=True, text=True).stdout)
    model = YOLO("yolov8n.pt")
    tops, bots = [], []
    tmp = os.path.join(_tf.gettempdir(), "rc_probe.png")
    for i in range(samples):
        t = dur * (i + 0.5) / samples
        subprocess.run(["ffmpeg", "-v", "error", "-y", "-ss", f"{t:.3f}", "-i", str(src),
                        "-frames:v", "1", tmp], check=True)
        r = model.predict(tmp, classes=[0], verbose=False, conf=0.35)[0]
        if not len(r.boxes):
            continue
        x0, y0, x1, y1 = max(r.boxes.xyxy.tolist(), key=lambda b: (b[2]-b[0])*(b[3]-b[1]))
        H = r.orig_shape[0]
        tops.append(y0 / H)
        bots.append(y0 / H + head_frac * (y1 - y0) / H)
    if not tops:
        return None
    return min(tops), max(bots)


def rc_draw_line(d, text, y, size, tracking, W, scale, align="centre", margin=0.10):
    from PIL import ImageFont
    f = ImageFont.truetype(_rc_font(), int(round(size * scale)))
    tr = tracking * scale
    ws = [d.textlength(c, font=f) for c in text]
    total = sum(ws) + tr * (len(text) - 1)
    x = (W * margin) if align == "left" else (W - total) / 2
    bw = max(1, int(round(RC_OUTLINE * scale)))
    # PIL's own stroke, not a hand-rolled offset stamp. The old loop drew the
    # glyph once per pixel in a disc of radius bw -- fine at bw=7, but the
    # re-calibrated ring is bw=15 at 1080 wide, ~700 draws per character.
    # TWO PASSES, and it has to be two. Drawing each character complete (stroke
    # then fill) in one loop was fine at OUTLINE=5 and shreds the line at
    # OUTLINE=10: every glyph's black halo is painted ON TOP of the white of the
    # glyph to its left, so counters fill in and the line came out as
    # "1he Gcal Was Never To Lcok 30". Lay the whole black slab down first, then
    # put every white letter on top of it.
    cx = x
    for i, c in enumerate(text):
        d.text((cx, y), c, font=f, fill=(0, 0, 0, 235),
               stroke_width=bw, stroke_fill=(0, 0, 0, 235))
        cx += ws[i] + tr
    cx = x
    for i, c in enumerate(text):
        d.text((cx, y), c, font=f, fill=(255, 255, 255, 255))
        cx += ws[i] + tr
    return int(round(size * scale))


def rc_coverage_profile(src, samples=10):
    """Per-row share of the frame width occupied by the SUBJECT, unioned over time.

    A bounding box is the wrong shape for this question. A standing man's box
    covers the empty floor beside him, so box-overlap says text crosses the
    subject when it passes harmlessly either side of his legs. Measured on the
    reference accounts, box overlap read a median 4% while the true silhouette
    overlap was 0-1%.

    Returns a float array of length H, or None.
    """
    try:
        from ultralytics import YOLO
    except Exception:
        return None
    import tempfile as _tf
    import numpy as _np
    try:
        import cv2 as _cv
    except Exception:
        return None
    dur = float(subprocess.run(["ffprobe", "-v", "error", "-show_entries",
                                "format=duration", "-of", "csv=p=0", str(src)],
                               capture_output=True, text=True).stdout)
    model = YOLO("yolov8n-seg.pt")
    tmp = os.path.join(_tf.gettempdir(), "rc_cov.png")
    # AVERAGE the per-frame row coverage, do not union the masks. A union answers
    # "did the subject ever touch this row", which for a running man is every row
    # at full width, and the profile goes flat and useless. The average answers
    # "how much of this row is subject, typically", which is what the overlay
    # actually competes with.
    profs, H = [], None
    for i in range(samples):
        subprocess.run(["ffmpeg", "-v", "error", "-y", "-ss",
                        f"{dur * (i + 0.5) / samples:.3f}", "-i", str(src),
                        "-frames:v", "1", tmp], check=True)
        im = _cv.imread(tmp)
        if im is None:
            continue
        H, W = im.shape[:2]
        m = _np.zeros((H, W), bool)
        r = model.predict(tmp, classes=[0], verbose=False, conf=0.35)[0]
        if r.masks is not None:
            for mm in r.masks.data.cpu().numpy():
                m |= _cv.resize(mm, (W, H)) > 0.5
        profs.append(m.mean(axis=1))
    if not profs:
        return None
    return _np.mean(_np.stack(profs), axis=0)


def rc_place_min_coverage(prof, H, need, lo_frac, hi_frac):
    """Slide a window of `need` px through [lo,hi] and return the y that covers
    the least subject. Returns (y, mean_coverage) or None if the range is too small."""
    lo, hi = int(lo_frac * H), int(hi_frac * H)
    need = int(round(need))
    if hi - lo < need:
        return None
    import numpy as _np
    c = _np.cumsum(_np.concatenate([[0.0], prof]))
    best = None
    for y in range(lo, hi - need + 1):
        cov = (c[y + need] - c[y]) / need
        if best is None or cov < best[1]:
            best = (y, float(cov))
    return best


def rc_smart_layout(prof, H, need_age, need_block, gap):
    """Place both elements where the SUBJECT IS NARROWEST, not merely where the
    face is not.

    This is the rule the reference accounts actually follow. Their low text sits
    at ankle height, where a standing person is two thin legs and centred type
    slips past on both sides — which is how they reach 0-1% overlap while our
    block, parked just under the chin at his widest point, reached 13%.
    """
    import numpy as _np
    age = rc_place_min_coverage(prof, H, need_age, RC_SAFE_TOP, RC_SAFE_BOTTOM - 0.30)
    if age is None:
        return None

    # The block searches only BELOW the subject's centre of mass. Minimum coverage
    # on its own is not enough: a head is narrow, so the gap just under the chin
    # scores well and the block lands across his face region — technically a small
    # overlap, visually wrong.
    rows = _np.arange(H)
    total = prof.sum()
    centre = float((rows * prof).sum() / total) / H if total > 0 else 0.5
    lo = max(age[0] / H + (need_age + gap) / H, centre)
    blk = rc_place_min_coverage(prof, H, need_block, lo, RC_SAFE_BOTTOM)
    if blk is None:                      # nothing fits below centre — best anywhere
        blk = rc_place_min_coverage(prof, H, need_block,
                                    age[0] / H + (need_age + gap) / H, RC_SAFE_BOTTOM)
    if blk is None:
        return None
    return {"age": float(age[0]), "block": float(blk[0]), "mode": "smart",
            "age_cov": age[1], "block_cov": blk[1]}


RC_SPLIT_MIN_GAIN = 0.08   # absolute body-coverage win required before splitting


def rc_band_coverage(prof, H, top_px, height_px):
    """Mean share of frame width the SUBJECT occupies behind a text band."""
    import numpy as _np
    a = max(0, int(round(top_px)))
    b = min(len(prof), int(round(top_px + height_px)))
    if b <= a:
        return 1.0
    return float(_np.mean(prof[a:b]))


def rc_best_top(prof, H, height_px, lo_frac, hi_frac, step=0.005):
    """Lowest-coverage top edge for a band of `height_px` inside [lo,hi].

    Returns (top_frac, coverage) or None when the band does not fit.
    """
    best = None
    t = float(lo_frac)
    while t + height_px / H <= hi_frac + 1e-9:
        c = rc_band_coverage(prof, H, t * H, height_px)
        if best is None or c < best[1]:
            best = (t, c)
        t += step
    return best


def rc_split_layout(prof, H, scale, n_lines, has_route, face_lo, face_hi,
                    age_top_frac, age_h):
    """v952 — SPLIT the body block: some lines above the face, the rest low.

    Why this exists, measured on job bb159509 (a standing man on a cable
    machine). The body block plus its route line is 495px on a 1920 frame —
    25.8% of the picture — and it must clear the face, which sits 27.1%..41.9%.
    That leaves exactly one window, 43.5%..53.0%, and every position in it lands
    across his chest: best achievable coverage 0.411. No amount of re-ranking a
    SINGLE block can fix that, because the slab is simply taller than any clean
    gap. The operator said it plainly: "if the big block of text was splitted and
    moved up and down it would have been better."

    He is right, and the frame says why: above his head, 0%..22.7%, is COMPLETELY
    empty and the old layout never used it. Measured on that clip:

        one slab, below the face            0.411
        2 lines up top + 2 lines & route low 0.000 and 0.381  -> 0.190 average

    Returns {"split_at", "block_top", "block", "top_cov", "low_cov", "cov"} or
    None when a split is impossible (too few lines, no room above the face, no
    room below it) or when `prof` is missing. The CALLER decides whether the win
    is big enough to take — see RC_SPLIT_MIN_GAIN.
    """
    if prof is None or n_lines < 2:
        return None
    line_h = RC_SPEC["body"][0] * scale
    pitch = RC_BODY_PITCH * scale
    route_h = RC_SPEC["route"][0] * scale
    gap_br = RC_GAP_BODY_ROUTE * scale

    def group_h(k, with_route):
        h = (k - 1) * pitch + line_h
        return h + gap_br + route_h if with_route else h

    # The top group lives between the age line and the face. Never above the
    # age line: the age tag is the account's fixed masthead.
    top_lo = age_top_frac + age_h / H + 0.02
    top_hi = face_lo - 0.015
    low_lo = face_hi + 0.015

    best = None
    # Prefer putting MORE lines up top (the empty zone is free real estate), but
    # always leave at least one line for the low group so the split is real.
    for k in range(n_lines - 1, 0, -1):
        top = rc_best_top(prof, H, group_h(k, False), top_lo, top_hi)
        if top is None:
            continue                      # k lines do not fit above the face
        low = rc_best_top(prof, H, group_h(n_lines - k, has_route),
                          low_lo, RC_SAFE_BOTTOM)
        if low is None:
            continue
        cov = (top[1] + low[1]) / 2.0
        if best is None or cov < best["cov"]:
            best = {"split_at": k, "block_top": top[0] * H, "block": low[0] * H,
                    "top_cov": top[1], "low_cov": low[1], "cov": cov}
    return best


def rc_layout(head_top, head_bottom, H, need_age, need_block):
    """Pick y for the age line and for the body+route block.

    head_top/head_bottom are fractions — the union of the face's vertical range
    over the WHOLE clip.
    """
    top_clear = (RC_SAFE_TOP, max(RC_SAFE_TOP, head_top - 0.02))
    low_clear = (min(RC_SAFE_BOTTOM, head_bottom + 0.03), RC_SAFE_BOTTOM)
    top_px = (top_clear[1] - top_clear[0]) * H
    low_px = (low_clear[1] - low_clear[0]) * H

    plan = {}
    if top_px >= need_age + need_block + RC_GAP_AGE_BODY:
        # judy layout: the whole thing sits above the subject
        y = top_clear[0] * H + (top_px - (need_age + RC_GAP_AGE_BODY + need_block)) / 2
        plan["age"] = y
        plan["block"] = y + need_age + RC_GAP_AGE_BODY
        plan["mode"] = "all-top"
    elif top_px >= need_age and low_px >= need_block:
        # noemi layout: age alone above, block below the chin — SEPARATE elements
        plan["age"] = top_clear[0] * H + (top_px - need_age) / 2
        plan["block"] = low_clear[0] * H + (low_px - need_block) * 0.35
        plan["mode"] = "split"
    elif low_px >= need_age + RC_GAP_AGE_BODY + need_block:
        # no usable band above the head at all: everything goes low
        y = low_clear[0] * H + (low_px - (need_age + RC_GAP_AGE_BODY + need_block)) / 2
        plan["age"] = y
        plan["block"] = y + need_age + RC_GAP_AGE_BODY
        plan["mode"] = "all-low"
    else:
        raise AutoEditError(
            f"No legal placement: top band {top_px:.0f}px, low band {low_px:.0f}px, "
            f"need {need_age:.0f}px + {need_block:.0f}px. Shorten the copy or reframe.")
    return plan, top_clear, low_clear


def _rc_frange(lo, hi, step):
    out, x = [], lo
    while x <= hi:
        out.append(round(x, 4))
        x += step
    return out


def rc_occupancy_layout(src, H, need_age, need_block, gap, block_until=None):
    """Place both elements with the SAME engine the auto-edit lane uses.

    Operator, 2026-08-21 and again 2026-08-24: captions must never cover the main
    ACTION or any face, and must respect the platform safe zones. build_occupancy
    above walks the clip one second at a time and returns, for each second, every
    detected face box plus a motion figure for each 10% band of frame height.
    Motion IS the action.

      * a face is vetoed if it appears in ANY second, not on average — our
        overlay persists, so it must be legal for the whole time it is on screen;
      * candidates are ranked by SUMMED motion, so the block lands in the calmest
        part of the frame rather than merely the emptiest.

    Returns the same plan dict the other two layouts return, or None.
    """
    dur = float(subprocess.run(["ffprobe", "-v", "error", "-show_entries",
                                "format=duration", "-of", "csv=p=0", str(src)],
                               capture_output=True, text=True).stdout or 0)
    if dur <= 0:
        return None
    buckets = build_occupancy(str(src), dur)
    if not buckets:
        return None

    # An element is only ever illegal against the seconds it is ON SCREEN FOR.
    # The block drops at block_until, so judging it against the closing close-ups
    # (where a face fills half the frame) rejects every position it could legally
    # have held.
    b_buckets = buckets if block_until is None else [
        b for b in buckets if b["t"] <= block_until] or buckets

    def face_free(c, hh, bks=None):
        """Legal at EVERY second, with the same 0.015 margin the auto-edit lane
        uses. Only faces overlapping the central 12-88% of width count — centred
        type slips past a face at the edge of frame, which is what both reference
        accounts actually do."""
        y0, y1 = c - hh, c + hh
        if y0 < RC_SAFE_TOP or y1 > RC_SAFE_BOTTOM:
            return False
        for b in (buckets if bks is None else bks):
            for fx0, fy0, fx1, fy1 in b["faces"]:
                if fy1 > y0 - 0.015 and fy0 < y1 + 0.015 and fx1 > 0.12 and fx0 < 0.88:
                    return False
        return True

    def action(c, hh, bks=None):
        y0, y1 = c - hh, c + hh
        tot = 0.0
        for b in (buckets if bks is None else bks):
            rows = [m for i, m in enumerate(b["motion"]) if y1 > i / 10 and y0 < (i + 1) / 10]
            tot += sum(rows) / max(len(rows), 1)
        return tot

    ha, hb = (need_age / H) / 2, (need_block / H) / 2
    step = 0.005

    def overlap_px(c, hh, bks):
        """Vertical pixels of face the band would cover, on a 1920 frame, unioned
        so two boxes of the same face in consecutive seconds are not counted twice."""
        y0, y1 = c - hh, c + hh
        worst = 0.0
        for b in bks:
            iv = sorted((f[1], f[3]) for f in b["faces"] if f[2] > 0.12 and f[0] < 0.88)
            merged = []
            for s, e in iv:
                if merged and s <= merged[-1][1]:
                    merged[-1] = (merged[-1][0], max(merged[-1][1], e))
                else:
                    merged.append((s, e))
            tot = sum(max(0.0, min(y1, e) - max(y0, s)) for s, e in merged)
            worst = max(worst, tot * 1920)
        return worst

    # GRACEFUL DEGRADATION. A tall block on a clip with two mid-frame faces can
    # have NO fully legal position — measured on the wood build, where the
    # face-free gaps are 0.215 and 0.175 of frame height and the block is 0.234.
    # Returning None there threw the placement back to the averaged-silhouette
    # layout, which is the thing being replaced. Instead: prefer legal positions,
    # and when none exist rank by how few pixels of FACE the band crosses.
    all_a = _rc_frange(RC_SAFE_TOP + ha, RC_SAFE_BOTTOM - hb - gap / H - hb, step)
    all_b = _rc_frange(RC_SAFE_TOP + hb, RC_SAFE_BOTTOM - hb, step)
    ages = [c for c in all_a if face_free(c, ha)] or all_a
    blocks = [c for c in all_b if face_free(c, hb, b_buckets)] or all_b
    squeezed = not [c for c in all_b if face_free(c, hb, b_buckets)]
    if not ages or not blocks:
        return None

    best = None
    for a_c in ages:
        for b_c in blocks:
            if b_c - hb < a_c + ha + gap / H:
                continue                      # block must sit below the age line
            s = action(a_c, ha) + action(b_c, hb, b_buckets)
            # Face pixels dominate the score, so a legal-ish position always beats
            # a calm one that sits on his face.
            s += (overlap_px(a_c, ha, buckets) + overlap_px(b_c, hb, b_buckets)) / 50.0
            # Tie-break toward the shape both reference banks use on a full-height
            # subject: age high, block low. Tiny weight.
            s += 0.02 * (a_c + (1.0 - b_c))
            if best is None or s < best[0]:
                best = (s, a_c, b_c)
    if best is None:
        return None
    _, a_c, b_c = best
    # v952 — the face union this engine actually vetoed against, so the split
    # layout downstream can reuse it instead of measuring the face twice. Only
    # the boxes that count: the central 12-88% of width, same as face_free().
    _f_lo, _f_hi = 1.0, 0.0
    for b in buckets:
        for fx0, fy0, fx1, fy1 in b["faces"]:
            if fx1 > 0.12 and fx0 < 0.88:
                _f_lo, _f_hi = min(_f_lo, fy0), max(_f_hi, fy1)
    return {"age": (a_c - ha) * H, "block": (b_c - hb) * H, "mode": "occupancy",
            # v952 — these were hardcoded 0.0, which is why the engine reported
            # "covers 0% of subject" for a block sitting across a man's chest:
            # it vetoes FACES and ranks MOTION, and never knew a body was there.
            # The caller fills them in from the silhouette when it has one.
            "age_cov": None, "block_cov": None,
            "face_lo": (_f_lo if _f_hi > _f_lo else None),
            "face_hi": (_f_hi if _f_hi > _f_lo else None),
            "age_action": action(a_c, ha),
            "block_action": action(b_c, hb, b_buckets),
            "squeezed": squeezed,
            "face_px": overlap_px(a_c, ha, buckets) + overlap_px(b_c, hb, b_buckets)}


def _rc_ass_time(t):
    t = max(0.0, float(t))
    h = int(t // 3600); m = int((t % 3600) // 60); sec = t % 60
    return f"{h}:{m:02d}:{sec:05.2f}"


def _rc_ass_escape(text):
    # ASS treats a brace as the start of an override block and a backslash as an
    # escape, so both have to be neutralised before the line reaches libass.
    return (text.replace("\\", r"\\").replace("{", r"\{").replace("}", r"\}")
                .replace("\n", r"\N"))


def rc_write_ass(path, W, H, scale, events, scales, watermark=None, align="centre"):
    """One ASS file for the whole overlay: styles, positions and timings.

    PlayResX/PlayResY are set to the real frame size, so every number in here is
    a PIXEL and the numbers measured off the reference transfer directly. Colours
    are ASS's &HAABBGGRR (alpha first, and 00 means opaque).

    Alignment 5 is middle-centre, which makes a pos(x, y) override place the
    CENTRE of the line -- the same anchor the placement engine reasons about.
    `align="left"` switches to 4 (middle-LEFT) and anchors x at the margin.
    """
    an = 4 if align == "left" else 5
    x = W * 0.06 if align == "left" else W / 2

    def style(name, key, extra_scale=1.0):
        size, tracking = RC_SPEC[key]
        k = scales.get(key, 1.0) * extra_scale
        return (f"Style: {name},Century Gothic,{size * scale * k:.0f},"
                f"&H00FFFFFF,&H00FFFFFF,&H00000000,&H00000000,"
                f"-1,0,0,0,100,100,{tracking * scale * k:.1f},0,"
                f"1,{RC_OUTLINE * scale * k:.1f},0,{an},0,0,0,1")

    head = [
        "[Script Info]", "ScriptType: v4.00+", "WrapStyle: 2",
        "ScaledBorderAndShadow: yes", f"PlayResX: {W}", f"PlayResY: {H}", "",
        "[V4+ Styles]",
        "Format: Name,Fontname,Fontsize,PrimaryColour,SecondaryColour,OutlineColour,"
        "BackColour,Bold,Italic,Underline,StrikeOut,ScaleX,ScaleY,Spacing,Angle,"
        "BorderStyle,Outline,Shadow,Alignment,MarginL,MarginR,MarginV,Encoding",
        style("age", "age"), style("body", "body"), style("route", "route"),
        # the watermark is the one element that never moves and never resizes
        (f"Style: wm,Century Gothic,{22 * scale:.0f},&H30FFFFFF,&H30FFFFFF,"
         f"&H60000000,&H60000000,-1,0,0,0,100,100,0,0,1,{2 * scale:.1f},0,1,"
         f"{24 * scale:.0f},0,{30 * scale:.0f},1"),
        "", "[Events]",
        "Format: Layer,Start,End,Style,Name,MarginL,MarginR,MarginV,Effect,Text",
    ]
    body = []
    for t0, t1, y, text, sty in events:
        body.append(f"Dialogue: 0,{_rc_ass_time(t0)},{_rc_ass_time(t1)},{sty},,0,0,0,,"
                    rf"{{\pos({x:.0f},{y:.0f})}}{_rc_ass_escape(text)}")
    if watermark:
        body.append(f"Dialogue: 0,{_rc_ass_time(0)},{_rc_ass_time(9999)},wm,,0,0,0,,"
                    f"{_rc_ass_escape(watermark)}")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(head + body) + "\n")
    return path


def rc_burn_ass(src, ass_path, out):
    """Burn the ASS track in one ffmpeg pass.

    The `subtitles` filter takes a filtergraph ARGUMENT, so a Windows path has to
    survive two levels of parsing: the backslashes and the drive colon both need
    escaping. Running from the file's own directory with a bare filename sidesteps
    the whole problem, which is why this chdirs instead of building an escape.
    """
    d, name = os.path.split(os.path.abspath(ass_path))
    subprocess.run(["ffmpeg", "-v", "error", "-y", "-i", os.path.abspath(str(src)),
                    "-vf", f"subtitles={name}", "-c:a", "copy",
                    os.path.abspath(str(out))],
                   cwd=d, check=True)
    print(f"  wrote {out}", flush=True)


def rc_build_ass(args):
    """The static read-caption block: ONE placement, held for the whole video.

    Ported verbatim from the tool's `build()`. `args` is any object carrying the
    same attributes the CLI produced (src, out, width, height, age, body, route,
    watermark, occupancy, no_smart, head_top, head_bottom, body_until,
    route_with_body) — a SimpleNamespace here, an argparse Namespace there.
    """
    import tempfile
    from PIL import Image, ImageDraw
    W, H = args.width, args.height
    scale = W / 720.0
    age_h = RC_SPEC["age"][0] * scale
    body_lines = [l for l in args.body if l.strip()]
    block_h = (len(body_lines) - 1) * RC_BODY_PITCH * scale + RC_SPEC["body"][0] * scale \
        + RC_GAP_BODY_ROUTE * scale + RC_SPEC["route"][0] * scale
    # v952 — the silhouette is measured ONCE, up front, and used for three
    # things: the split decision below, honest coverage reporting, and the smart
    # fallback. It used to be computed only on the smart path, which is exactly
    # why the occupancy path could report 0% coverage over a man's chest.
    prof = None
    if not args.no_smart:
        prof = rc_coverage_profile(args.src)
        if prof is None:
            print("  no silhouette available — body coverage is unknown this run",
                  flush=True)

    plan = None
    if args.occupancy:
        plan = rc_occupancy_layout(args.src, H, age_h, block_h, RC_GAP_AGE_BODY,
                                   args.body_until)
        if plan is not None:
            if prof is not None:
                plan["age_cov"] = rc_band_coverage(prof, H, plan["age"], age_h)
                plan["block_cov"] = rc_band_coverage(prof, H, plan["block"], block_h)
            print(f"  occupancy placement: age action {plan['age_action']:.3f}, "
                  f"block action {plan['block_action']:.3f} (lower = calmer)", flush=True)
            if plan.get("squeezed"):
                print(f"  !! no fully face-free band for the block — best available "
                      f"crosses {plan['face_px']:.0f}px of face. Shorten the copy.",
                      flush=True)
        else:
            print("  occupancy placement found no window — falling back", flush=True)
    if plan is None and not args.no_smart:
        if prof is not None:                       # v952 — measured once, above
            plan = rc_smart_layout(prof, H, age_h, block_h, RC_GAP_AGE_BODY)
            if plan is not None:
                print(f"  smart placement: age covers {100*plan['age_cov']:.0f}% of "
                      f"subject, block {100*plan['block_cov']:.0f}%", flush=True)
            else:
                print("  smart placement found no window — falling back to bands",
                      flush=True)
        else:
            print("  no segmentation available — falling back to bands", flush=True)
    if plan is None:
        if args.head_top is None or args.head_bottom is None:
            raise AutoEditError(
                "The read-caption overlay could not place itself: neither the "
                "occupancy engine nor the silhouette profile produced a window, "
                "and no head band was measured to fall back on. The overlay must "
                "never cross the face, so this stops rather than guessing.")
        plan, top_clear, low_clear = rc_layout(args.head_top, args.head_bottom, H,
                                               age_h, block_h)
        print(f"  clear top  {100*top_clear[0]:.0f}%..{100*top_clear[1]:.0f}%"
              f"   clear low {100*low_clear[0]:.0f}%..{100*low_clear[1]:.0f}%", flush=True)

    # v952 — SPLIT the block when one slab cannot clear the body. A tall block
    # that must also clear the face has, on a standing subject, exactly one
    # window and it is his chest. Splitting uses the empty frame above his head,
    # which no single-slab layout can reach. Taken only on a real, measured win
    # (RC_SPLIT_MIN_GAIN), so a clip the current layout already handles keeps the
    # account's proven one-block look.
    face_lo = plan.get("face_lo")
    face_hi = plan.get("face_hi")
    if args.head_top is not None:
        face_lo = args.head_top if face_lo is None else min(face_lo, args.head_top)
    if args.head_bottom is not None:
        face_hi = args.head_bottom if face_hi is None else max(face_hi, args.head_bottom)
    if prof is not None and face_lo is not None and face_hi is not None:
        single_cov = plan.get("block_cov")
        if single_cov is None:
            single_cov = rc_band_coverage(prof, H, plan["block"], block_h)
        sp = rc_split_layout(prof, H, scale, len(body_lines), bool(args.route),
                             face_lo, face_hi, plan["age"] / H, age_h)
        if sp is not None and single_cov - sp["cov"] >= RC_SPLIT_MIN_GAIN:
            print(f"  [v952] SPLIT: one slab would cover {100*single_cov:.0f}% of the "
                  f"subject; {sp['split_at']} line(s) above the face cover "
                  f"{100*sp['top_cov']:.0f}% and the rest {100*sp['low_cov']:.0f}%",
                  flush=True)
            plan.update({"block_top": sp["block_top"], "block": sp["block"],
                         "split_at": sp["split_at"], "mode": plan["mode"] + "+split",
                         "block_cov": sp["low_cov"], "top_cov": sp["top_cov"]})
        elif sp is not None:
            print(f"  [v952] split considered ({100*sp['cov']:.0f}% vs "
                  f"{100*single_cov:.0f}%) — not worth it, keeping one block",
                  flush=True)

    print(f"  frame {W}x{H}", flush=True)
    print(f"  layout: {plan['mode']}   age y={plan['age']:.0f} ({100*plan['age']/H:.1f}%)"
          f"   block y={plan['block']:.0f} ({100*plan['block']/H:.1f}%)"
          + (f"   top-group y={plan['block_top']:.0f} "
             f"({100*plan['block_top']/H:.1f}%)" if plan.get("split_at") else ""),
          flush=True)

    # Two INDEPENDENT TIMINGS, not two bitmap layers. The age line and the body
    # block expire separately on the reference (judy's body lines vanish at
    # t=10.2s of 13.1s while the age line and the route stay to the end). An ASS
    # event carries its own start and end, so the timing is a field, not a layer.
    d = ImageDraw.Draw(Image.new("RGBA", (W, H)))          # measuring only

    age_k = rc_fit_scale(d, [args.age], *RC_SPEC["age"], W, scale,
                         limit_frac=rc_age_width_limit(args.age), ass=True)
    body_k = rc_fit_scale(d, body_lines, *RC_SPEC["body"], W, scale, ass=True) \
        if body_lines else 1.0
    route_k = rc_fit_scale(d, [args.route], *RC_SPEC["route"], W, scale, ass=True) \
        if args.route else 1.0
    for nm, k in (("age", age_k), ("body", body_k), ("route", route_k)):
        if k < 0.999:
            print(f"  {nm}: shrunk to {100*k:.0f}% so the widest line fits "
                  f"{100*RC_MAX_TEXT_W:.0f}% of frame width", flush=True)

    END = 9999.0
    blk_end = args.body_until if args.body_until else END
    events = [(0.0, END, plan["age"] + RC_SPEC["age"][0] * scale * age_k / 2,
               args.age, "age")]
    # v952 — with a split, the first `split_at` lines sit in their own group in
    # the empty band above the face; the rest (and the route) stay low. Without
    # one this is the original single run, line for line.
    split_at = int(plan.get("split_at") or 0)
    if split_at:
        y_top = plan["block_top"] + RC_SPEC["body"][0] * scale * body_k / 2
        for line in body_lines[:split_at]:
            events.append((0.0, blk_end, y_top, line, "body"))
            y_top += RC_BODY_PITCH * scale
    low_lines = body_lines[split_at:]

    y = plan["block"] + RC_SPEC["body"][0] * scale * body_k / 2
    for line in low_lines:
        events.append((0.0, blk_end, y, line, "body"))
        y += RC_BODY_PITCH * scale
    y += RC_GAP_BODY_ROUTE * scale - RC_BODY_PITCH * scale
    # The route line normally persists to the end, as the reference does. But a
    # clip whose framing CHANGES — a swap render inherits every cut in its source
    # — can start wide and end on a close-up, and a line that was sitting on the
    # sky at t=1 is sitting on the subject's face at t=8. `route_with_body` drops
    # the route on the same timer as the block. It is a framing question, not a
    # preference.
    if args.route:
        events.append((0.0, blk_end if args.route_with_body else END,
                       y + RC_SPEC["route"][0] * scale * route_k / 2, args.route, "route"))
        bottom = y + RC_SPEC["route"][0] * scale
        print(f"  lowest ink {100*bottom/H:.1f}%  (safe limit {100*RC_SAFE_BOTTOM:.0f}%)"
              + ("  OK" if bottom / H <= RC_SAFE_BOTTOM else "  !! CROSSES THE REELS UI"),
              flush=True)

    if args.body_until:
        print(f"  body block drops at t={args.body_until}s; age persists"
              + ("" if args.route_with_body else ", route persists"), flush=True)
    ks = {"age": age_k, "body": body_k, "route": route_k}
    _rc_warn_side_margins(events, W, H, scale, ks)
    ass = os.path.join(tempfile.gettempdir(), "rc_overlay.ass")
    return rc_write_ass(ass, W, H, scale, events, scales=ks, watermark=args.watermark)


def overlay_stage_plan(spec):
    """v944 — decide whether the overlay stage runs, and with what.

    Pure: no files, no ffmpeg. `None` in, `None` out; `overlay: none` likewise —
    the run is then byte-identical to a pre-v944 run, which is the whole
    regression contract. Anything else is validated hard HERE rather than
    halfway through a render.
    """
    if not spec:
        return None
    engine = str(spec.get("overlay") or "none").strip().lower()
    if engine == "none":
        return None
    if engine != "readcaption":
        raise AutoEditError(
            f"Unknown overlay engine {engine!r} — 'readcaption' is the only one "
            f"there is (v944)")
    age = str(spec.get("overlay_age") or "").strip()
    if not age:
        raise AutoEditError(
            "A readcaption overlay needs an age line (overlay_age) — it is the "
            "element the mentor calls non-negotiable (v944)")
    body = spec.get("overlay_block") or []
    if isinstance(body, str):
        body = [p.strip() for p in body.split(" / ") if p.strip()]
    return {
        "engine": "readcaption",
        "age": age,
        "body": [str(b).strip() for b in body if str(b).strip()],
        # The tool's own default, and the reason the format has this name: a
        # read-caption post without the route line is not asking anyone to read
        # the caption.
        "route": str(spec.get("overlay_footer") or "(READ CAPTION)").strip(),
        # v952 — DEFAULT NONE. compose() already burns `syntheticperformer`
        # bottom-left on every frame (v938.15), so stamping it again here put TWO
        # overlapping copies in the corner of every readcaption post — one grey
        # at 50% from compose, one solid white from this ASS. autoedit_captions
        # has always documented the right rule ("No watermark here on purpose:
        # the composed video already carries it"); this stage just never followed
        # it. Still declarable for a path where compose did not run.
        "watermark": spec.get("overlay_watermark") or None,
        "body_until": spec.get("overlay_body_until"),
        "route_with_body": bool(spec.get("overlay_route_with_body")),
        # Line pitch in spec units. Default = the account constant, MEASURED
        # from its own posted winner (49 -> ~73px at 1080); a declared value is
        # a deliberate spacing test, already range-checked upstream.
        "pitch": spec.get("overlay_pitch"),
    }


def render_readcaption_overlay(video_in, video_out, plan):
    """Burn the read-caption overlay onto a finished cut.

    ONE placement for the whole video, because the overlay is burned for its
    whole duration and the reference posts hold it pixel-identical frame to
    frame. The engine picks the pixels: occupancy first (per-second face vetoes
    plus per-band motion — the auto-edit lane's own map), the silhouette profile
    second, the measured head band last.
    """
    import types
    probe = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries",
         "stream=width,height", "-of", "csv=p=0", str(video_in)],
        capture_output=True, text=True).stdout.strip().split(",")
    try:
        width, height = int(probe[0]), int(probe[1])
    except (IndexError, ValueError):
        raise AutoEditError(
            f"Could not read the frame size of {video_in} — the overlay is placed "
            f"in pixels, so it cannot run without one")

    band = rc_head_band(str(video_in))
    if band is None:
        print("  head band not measured (no person detector here) — occupancy and "
              "silhouette placement carry it", flush=True)

    # A declared overlay_pitch overrides the account constant FOR THIS RENDER
    # only. The rc_ helpers read the module constant (they are the tool's
    # functions, moved verbatim), so the override is scoped set-and-restore —
    # safe because one worker renders one run at a time.
    global RC_BODY_PITCH
    _saved_pitch = RC_BODY_PITCH
    if plan.get("pitch"):
        RC_BODY_PITCH = int(plan["pitch"])
        print(f"  [v944] overlay pitch override: {RC_BODY_PITCH} spec units", flush=True)

    args = types.SimpleNamespace(
        src=str(video_in), out=str(video_out), width=width, height=height,
        age=plan["age"], body=list(plan.get("body") or []),
        route=plan.get("route") or "",
        watermark=plan.get("watermark"),
        body_until=plan.get("body_until"),
        route_with_body=bool(plan.get("route_with_body")),
        # The occupancy engine IS this module's own, so it is always available
        # and is the right default for any clip where the subject moves.
        occupancy=True, no_smart=False,
        head_top=band[0] if band else None,
        head_bottom=band[1] if band else None,
    )
    print(f"overlay: readcaption — age {args.age!r}, {len(args.body)} body line(s)",
          flush=True)
    try:
        ass = rc_build_ass(args)
        rc_burn_ass(args.src, ass, args.out)
    finally:
        RC_BODY_PITCH = _saved_pitch
    return Path(video_out)


def run_autoedit(job_id: str, work: Path, out: Path, template: str = "korella",
                 placement: str = "dynamic", offset: float | None = None,
                 progress=lambda stage: None, repairs=None) -> Path:
    """The whole pass. `progress` gets called with a stage-name string."""
    from autoedit_qc import normalize_repairs
    repairs = normalize_repairs(repairs)
    nocap, dur, segs, auto_offset, pip_y, chin, base, audio = prepare_composition(
        job_id, work, progress, repairs=repairs)
    buckets = []
    windows = []
    if repairs["captions_enabled"]:
        progress("captions")
    # v938.17 — WHICH video the occupancy scan reads.
    #
    # Normally it reads the raw downloaded export: the composite only adds the
    # PIP, whose box the planner is told about separately, so base and composite
    # agree about where the faces are.
    #
    # With hook_corner they do NOT agree. In the base export the hook speaker is
    # full-frame on green; in the composite he is a small figure in the bottom-
    # left and a different person fills the frame behind him. Planning against
    # the base put captions straight across his face — exactly the thing the
    # operator said must never happen. So when the hook is recomposited, scan
    # what the viewer will actually see.
    occ_src = nocap if repairs.get("hook_corner") else base
    if repairs["captions_enabled"] and placement == "dynamic" and offset is None:
        # The cache name records WHICH video was scanned. It used to key on the
        # trim values alone, so switching sources would have silently reused the
        # other one's map — the same stale-cache shape as the four before it.
        occ_file = work / occupancy_name(repairs["trim_start_s"], repairs["trim_end_s"],
                                         file_fingerprint(occ_src))
        for stale in work.glob("occupancy_*.json"):
            if stale != occ_file:
                stale.unlink(missing_ok=True)
        if occ_file.exists():
            buckets = json.loads(occ_file.read_text())
            print("occupancy: cached")
        else:
            buckets = build_occupancy(occ_src, dur)
        occ_file.write_text(json.dumps(buckets))
        windows = plan_caption_windows(buckets, chin, segs, pip_y, dur)
        _render_caption_pass(nocap, out, template, windows, work, dur, audio)
    elif repairs["captions_enabled"]:
        buckets = build_occupancy(occ_src, dur)
        chosen_offset = offset if offset is not None else auto_offset
        windows = [(0.0, dur, chosen_offset)]
        _render_caption_pass(nocap, out, template, windows, work, dur, audio)
    else:
        out.unlink(missing_ok=True)
        shutil.copy2(nocap, out)

    # v944 — the declared TEXT OVERLAY, the last video stage of the run.
    #
    # Guarded on the spec being there at all: no spec, no stage, and the run is
    # what it was before this feature existed. It burns AFTER the caption pass
    # (or after the plain copy when captions are off) so the quality check below
    # measures the DELIVERED artifact and not a pre-overlay version of it —
    # §v938.1, a stage log proves a stage RAN, never that its output shipped.
    _rc_plan = overlay_stage_plan((repairs or {}).get("overlay_spec"))
    if _rc_plan is not None:
        progress("overlay")
        pre = work / "overlay_input.mp4"
        pre.unlink(missing_ok=True)
        shutil.copy2(out, pre)
        try:
            render_readcaption_overlay(pre, out, _rc_plan)
        finally:
            pre.unlink(missing_ok=True)      # disk-bounded: one temp, deleted

    if repairs["captions_enabled"] and not buckets:
        buckets = build_occupancy(occ_src, dur)
    if not windows:
        windows = [(0.0, dur, 0.0)]
    scan = json.loads((work / "scan.json").read_text()) if (work / "scan.json").exists() else {}
    hook_end = max(0.0, float(scan.get("hook_end", 0.0)) - repairs["trim_start_s"])
    progress("quality-check")
    report = run_quality_checks(out, base, dur, buckets, windows, segs, pip_y,
                                hook_end, repairs)
    (work / "qc_report.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"quality: {report['verdict']}"
          + (f" — {'; '.join(report['reasons'])}" if report["reasons"] else ""))
    return out
