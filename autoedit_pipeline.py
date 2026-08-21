"""autoedit_pipeline.py — the CapCut auto-edit pass, as an importable module.

Turns a completed platform job into a posted-ready video: downloads the job's
final export + 16:9 b-roll "support track", keys the green-screen hook,
overlays a rounded-corner PIP of the b-roll, enhances the voice, and burns
word-by-word karaoke captions (via pycaps) whose placement dynamically avoids
covering any face, the PIP, or the highest-motion zone.

MUST stay importable on the Render server, where cv2 / faster_whisper / pycaps
are NOT installed. Every one of those imports lives inside a function body —
keep it that way. Only stdlib + argparse/json/os/shutil/subprocess/sys/pathlib
may be imported at module level.

The local CLI wrapper is tools/capcut_autoedit.py (wiki repo) — it just calls
run_autoedit() below. This module is the moved-verbatim pipeline body.
"""
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


def fetch_job_files(job_id, work: Path, music_filename=None):
    from send_to_platform import resolve_token
    token, source = resolve_token(None)
    print(f"token: {source}")
    st = api_get(f"/api/jobs/{job_id}/export-status", token).json()
    if st.get("state") != "done":
        raise AutoEditError(f"export not done for this job (state={st.get('state')}) — export it in the platform first")
    base_fn = st["result"]["filename"]
    outs = api_get(f"/api/jobs/{job_id}/list-outputs", token).json()["files"]
    sup_fn = next((f for f in outs if f.startswith("support_track_") and f.endswith(".mp4")), None)
    music_fn = music_filename if music_filename in outs else None
    if music_filename and not music_fn:
        raise AutoEditError(f"music file is not in this job's outputs: {music_filename}")
    base, sup = work / base_fn, (work / sup_fn if sup_fn else None)
    music = work / music_fn if music_fn else None
    download(f"/api/jobs/{job_id}/outputs/{base_fn}", token, base)
    if sup_fn:
        download(f"/api/jobs/{job_id}/outputs/{sup_fn}", token, sup)
    else:
        print("  no support track found — PIP stage will be skipped")
    if music_fn:
        download(f"/api/jobs/{job_id}/outputs/{music_fn}", token, music)
    return base, sup, music


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
    if not hasattr(cv2, "CascadeClassifier"):
        # OpenCV 5 dropped this from the default build; requirements pin <5.
        # Say so plainly — the raw AttributeError reads like a code bug.
        raise AutoEditError(
            f"This OpenCV build ({getattr(cv2, '__version__', '?')}) has no CascadeClassifier, "
            "so faces cannot be detected and captions could cover one. "
            "Install opencv-python-headless<5.")
    cascade = cv2.CascadeClassifier(cv2.data.haarcascades + "haarcascade_frontalface_default.xml")
    cap = cv2.VideoCapture(str(base))
    fps = cap.get(cv2.CAP_PROP_FPS) or 24
    bottoms = []
    for t in range(1, int(dur), 2):
        cap.set(cv2.CAP_PROP_POS_FRAMES, int(t * fps))
        ok, frame = cap.read()
        if not ok:
            continue
        h, w = frame.shape[:2]
        scale = 360 / w
        small = cv2.resize(frame, (360, int(h * scale)))
        gray = cv2.cvtColor(small, cv2.COLOR_BGR2GRAY)
        faces = cascade.detectMultiScale(gray, 1.1, 5, minSize=(60, 60))
        if len(faces):
            x, y, fw, fh = max(faces, key=lambda f: f[2] * f[3])  # largest face
            bottoms.append((y + fh) / small.shape[0])
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
    face box + a coarse motion grid (where the action is). Fractions of frame."""
    import cv2
    import numpy as np
    cascade = cv2.CascadeClassifier(cv2.data.haarcascades + "haarcascade_frontalface_default.xml")
    cap = cv2.VideoCapture(str(base))
    fps = cap.get(cv2.CAP_PROP_FPS) or 24
    buckets, prev = [], None
    for t in [x + 0.5 for x in range(int(dur))]:
        cap.set(cv2.CAP_PROP_POS_FRAMES, int(t * fps))
        ok, frame = cap.read()
        if not ok:
            buckets.append({"t": t, "faces": [], "motion": [0.0] * 10})
            continue
        h, w = frame.shape[:2]
        small = cv2.resize(frame, (180, int(h * 180 / w)))
        gray = cv2.cvtColor(small, cv2.COLOR_BGR2GRAY)
        sh, sw = gray.shape
        faces = [[x / sw, y / sh, (x + fw) / sw, (y + fh) / sh]
                 for x, y, fw, fh in cascade.detectMultiScale(gray, 1.1, 5, minSize=(30, 30))]
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


def plan_caption_windows(buckets, chin, segs, pip_y, dur):
    """Dynamic placement: per second pick a caption band that covers NO face,
    NO PIP window, least action — with hysteresis so it moves only when it must.
    Returns [(start, end, offset)] merged windows."""
    half = 0.075                      # caption band half-height (2-line card)
    cands = [min(chin + 0.095, 0.60), 0.70, 0.14]  # below-chin | lower-third | top
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

    # smooth face flicker: a face counts if seen in this bucket OR a neighbour
    smoothed = []
    for i, b in enumerate(buckets):
        faces = []
        for j in (i - 1, i, i + 1):
            if 0 <= j < len(buckets):
                faces.extend(buckets[j]["faces"])
        smoothed.append({**b, "faces": faces})
    buckets = smoothed

    def stable(c, i, n=3):
        """candidate stays valid for the next n buckets"""
        return all(valid(c, buckets[j]) for j in range(i, min(i + n, len(buckets))))

    cur, plan, squeezed, heavy_squeezed = None, [], 0, 0
    for i, b in enumerate(buckets):
        options = [c for c in cands if valid(c, b)]
        best = next((c for c in cands if c in options and stable(c, i)), None)
        if cur is not None and cur in options and (best is None or best == cur or not stable(best, i, 4)):
            choice = cur              # hold position; move back up-priority only when it will last
        elif best is not None:
            choice = best
        elif options:
            choice = min(options, key=lambda c: (round(action_score(c, b), 3), cands.index(c)))
        else:
            # Squeeze: no candidate clears both the face(s) and the PIP + safe zones.
            # Operator rule, verbatim: "never cover the main action or any face --
            # never". Faces are the hard, unconditional priority; the PIP is our own
            # inserted overlay, not a person, so it is the one allowed to give way.
            # Graceful degradation ladder:
            #   2. face-clear (zero measured face overlap) -- insert overlap accepted.
            #      Prefer candidates in their normal priority order.
            #   3. when even that is impossible, the candidate with the SMALLEST total
            #      face overlap wins (pixels on a 1920-tall frame), tie-broken by
            #      smaller insert overlap, then candidate order. This is what stops the
            #      old blind "always cands[0]" pick from choosing the WORST-covering
            #      option purely because it was first in the list.
            face_clear = [c for c in cands if face_overlap_px(c, b) == 0]
            if face_clear:
                choice = face_clear[0]
            else:
                choice = min(cands, key=lambda c: (round(face_overlap_px(c, b), 1),
                                                     round(pip_overlap_px(c, b), 1),
                                                     cands.index(c)))
                heavy_squeezed += 1
            squeezed += 1
        plan.append(choice)
        cur = choice
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
    half = 0.075
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


def enhance_audio(base, work: Path):
    raw_wav, enh, pol = work / "audio_raw.wav", work / "audio_enh.wav", work / "audio_pol.wav"
    if pol.exists():
        print("audio: cached")
        return pol
    run(["ffmpeg", "-v", "error", "-i", str(base), "-vn", "-ac", "1", "-ar", "48000", "-y", str(raw_wav)])
    ok = False
    try:
        from audio_processor import try_deepfilter_modal
        ok = try_deepfilter_modal(raw_wav, enh)
    except Exception as e:
        print(f"deepfilter modal unavailable: {e}")
    if not ok:
        shutil.copy(raw_wav, enh)
    run(["ffmpeg", "-v", "error", "-i", str(enh),
         "-af", "highpass=f=70,acompressor=threshold=-18dB:ratio=3:attack=8:release=120,loudnorm=I=-15:TP=-1.2:LRA=9",
         "-ar", "48000", "-y", str(pol)])
    print(f"audio: deepfilter={'modal' if ok else 'SKIPPED (raw)'} + compressor + loudnorm")
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
            chroma_blend=0.02, music=None, music_db=-20.0):
    # Every visual/audio repair setting is in the cache name. A re-run with a
    # stronger key or different music must never silently reuse the old video.
    music_key = music.stem[:24] if music else "none"
    cache_key = (f"y{pip_y}_p{int(pip_enabled)}_k{chroma_similarity:.3f}_"
                 f"b{chroma_blend:.3f}_m{music_key}_{music_db:.1f}")
    nocap = work / f"nocap_wm_{cache_key}.mp4"
    if nocap.exists():
        print("compose: cached")
        return nocap
    inputs = ["-i", str(base)]
    fc_parts, vin, idx = [], "[0:v]", 1
    if hook_end > 0 and key_hex:
        bg = work / "hookbg.mp4"
        src = segs[3][0] if len(segs) >= 4 else (segs[-1][0] if segs else 0)
        run(["ffmpeg", "-v", "error", "-ss", str(src), "-t", str(hook_end + 0.2), "-i", str(sup or base),
             "-vf", "crop=608:1080,scale=1080:1920,gblur=sigma=18,eq=brightness=0.02:saturation=1.05,"
                    "tpad=stop_mode=clone:stop_duration=2,setsar=1", "-an", "-r", "24", "-y", str(bg)])
        inputs += ["-i", str(bg)]
        fc_parts.append(
            f"{vin}split[b0][b1];"
            f"[b0]trim=0:{hook_end},setpts=PTS-STARTPTS,"
            f"chromakey={key_hex}:{chroma_similarity}:{chroma_blend},despill=type=green[fg];"
            f"[{idx}:v]trim=0:{hook_end},setpts=PTS-STARTPTS[bgt];"
            f"[bgt][fg]overlay=x=0:y=0:shortest=1[hook];"
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
    fc_parts.append(f"{vin}drawtext=text='syntheticperformer':fontfile='{fontfile}'"
                    f":fontcolor=white@0.5:fontsize=34:x=44:y=h-78[vout]")
    print("compose: rendering base (no captions) ...")
    run(["ffmpeg", "-v", "error", *inputs, "-filter_complex", ";".join(fc_parts),
         "-map", "[vout]", "-map", audio_map, "-c:v", "libx264", "-crf", "19", "-preset", "medium",
         "-r", "24", "-c:a", "aac", "-b:a", "192k", "-t", str(dur), "-movflags", "+faststart", "-y", str(nocap)])
    return nocap


def render_captions(nocap: Path, out: Path, template: str, offset=-0.05):
    cwd = str(TEMPLATES_DIR) if template in local_styles() else None
    out.unlink(missing_ok=True)  # pycaps refuses to overwrite
    print(f"captions: pycaps template={template}")
    r = subprocess.run([pycaps_exe(), "render", "--input", str(nocap), "--output", str(out),
                        "--template", template, "--layout-align", "center",
                        "--layout-align-offset", str(offset)],
                       capture_output=True, text=True, encoding="utf-8", errors="replace",
                       cwd=cwd, env={**os.environ, "PYTHONIOENCODING": "utf-8"})
    if r.returncode != 0 or not out.exists():
        print((r.stderr or r.stdout)[-2000:])
        raise AutoEditError("pycaps render failed")


def render_captions_dynamic(nocap: Path, out: Path, template: str, windows, work: Path):
    """Render pycaps once per distinct height, then splice the passes by time
    window (captions land identically because the input/transcript is identical)."""
    offsets = sorted({o for _, _, o in windows})
    passes = {}
    for o in offsets:
        p = work / f"cap_pass_{str(o).replace('-', 'm').replace('.', '_')}_{template}.mp4"
        if not p.exists():
            render_captions(nocap, p, template, o)
        passes[o] = p
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
    Returns (nocap_path, dur, segs, auto_offset, pip_y, chin, base_path).
    `base_path` is returned too (in addition to the documented 6 fields)
    because the caption-placement stage's occupancy scan needs the raw
    downloaded video, not the composed one — dropping it would change
    what build_occupancy() scans on an uncached run."""
    from autoedit_qc import normalize_repairs
    repairs = normalize_repairs(repairs)
    work.mkdir(parents=True, exist_ok=True)
    progress("download")
    base, sup, music = fetch_job_files(job_id, work, repairs["music_filename"])
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
    if len(s.get("layout", [])) != 3:
        progress("layout")
        auto_offset, pip_y, chin = detect_layout(base, dur, segs)
        s["layout"] = [auto_offset, pip_y, chin]
    else:
        auto_offset, pip_y, chin = s["layout"]
        print(f"layout: cached (offset {auto_offset:+.3f}, pip_y {pip_y}, chin {chin:.2f})")
    scan_file.write_text(json.dumps(s))
    progress("audio")
    audio = enhance_audio(base, work)
    progress("compose")
    nocap = compose(
        base, sup, work, dur, hook_end, key_hex, segs, audio, pip_y,
        pip_enabled=repairs["pip_enabled"],
        chroma_similarity=repairs["chroma_similarity"],
        chroma_blend=repairs["chroma_blend"],
        music=music, music_db=repairs["music_db"],
    )
    start_s, end_s = repairs["trim_start_s"], repairs["trim_end_s"]
    trim_key = f"s{start_s:.2f}_e{end_s:.2f}"
    nocap, trimmed_dur = trim_media(nocap, work / f"nocap_trim_{trim_key}.mp4",
                                    start_s, end_s, dur)
    base, _ = trim_media(base, work / f"base_trim_{trim_key}.mp4",
                         start_s, end_s, dur)
    segs = shifted_segments(segs, start_s, trimmed_dur)
    return nocap, trimmed_dur, segs, auto_offset, pip_y, chin, base


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


def _render_caption_pass(nocap: Path, out: Path, template: str, windows, work: Path, dur: float):
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
    audio = work / "audio_pol.wav"
    if not audio.exists():
        audio = work / "audio_enh.wav"
    _ac.render(nocap, out, template, windows, audio, work)


def run_autoedit(job_id: str, work: Path, out: Path, template: str = "korella",
                 placement: str = "dynamic", offset: float | None = None,
                 progress=lambda stage: None, repairs=None) -> Path:
    """The whole pass. `progress` gets called with a stage-name string."""
    from autoedit_qc import normalize_repairs
    repairs = normalize_repairs(repairs)
    nocap, dur, segs, auto_offset, pip_y, chin, base = prepare_composition(
        job_id, work, progress, repairs=repairs)
    buckets = []
    windows = []
    if repairs["captions_enabled"]:
        progress("captions")
    if repairs["captions_enabled"] and placement == "dynamic" and offset is None:
        occ_file = work / (f"occupancy_s{repairs['trim_start_s']:.2f}_"
                           f"e{repairs['trim_end_s']:.2f}.json")
        if occ_file.exists():
            buckets = json.loads(occ_file.read_text())
            print("occupancy: cached")
        else:
            buckets = build_occupancy(base, dur)
        occ_file.write_text(json.dumps(buckets))
        windows = plan_caption_windows(buckets, chin, segs, pip_y, dur)
        _render_caption_pass(nocap, out, template, windows, work, dur)
    elif repairs["captions_enabled"]:
        buckets = build_occupancy(base, dur)
        chosen_offset = offset if offset is not None else auto_offset
        windows = [(0.0, dur, chosen_offset)]
        _render_caption_pass(nocap, out, template, windows, work, dur)
    else:
        out.unlink(missing_ok=True)
        shutil.copy2(nocap, out)

    if repairs["captions_enabled"] and not buckets:
        buckets = build_occupancy(base, dur)
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
