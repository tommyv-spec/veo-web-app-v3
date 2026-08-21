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


def run(cmd, **kw):
    r = subprocess.run(cmd, capture_output=True, text=True,
                       encoding="utf-8", errors="replace", **kw)
    if r.returncode != 0:
        print(r.stderr[-2000:] if r.stderr else r.stdout[-2000:])
        raise SystemExit(f"command failed: {cmd[0]} ...")
    return r


def pycaps_exe():
    p = shutil.which("pycaps")
    if p:
        return p
    guess = Path(os.environ.get("APPDATA", "")) / "Python" / "Python313" / "Scripts" / "pycaps.exe"
    if guess.exists():
        return str(guess)
    raise SystemExit("pycaps not found: pip install git+https://github.com/francozanardi/pycaps")


def local_styles():
    return sorted(d.name for d in TEMPLATES_DIR.iterdir() if (d / "pycaps.template.json").exists()) \
        if TEMPLATES_DIR.exists() else []


def api_get(path, token, stream=False):
    import requests
    r = requests.get(f"{BASE_URL}{path}", headers={"Authorization": f"Bearer {token}"},
                     timeout=600, stream=stream)
    if r.status_code != 200:
        raise SystemExit(f"GET {path} -> {r.status_code}: {r.text[:200]}")
    return r


def download(path, token, dest: Path):
    if dest.exists() and dest.stat().st_size > 0:
        print(f"  cached: {dest.name}")
        return
    r = api_get(path, token, stream=True)
    with open(dest, "wb") as f:
        for chunk in r.iter_content(1 << 20):
            f.write(chunk)
    print(f"  downloaded: {dest.name} ({dest.stat().st_size / 1e6:.1f} MB)")


def fetch_job_files(job_id, work: Path):
    from send_to_platform import resolve_token
    token, source = resolve_token(None)
    print(f"token: {source}")
    st = api_get(f"/api/jobs/{job_id}/export-status", token).json()
    if st.get("state") != "done":
        raise SystemExit(f"export not done for this job (state={st.get('state')}) — export it in the platform first")
    base_fn = st["result"]["filename"]
    outs = api_get(f"/api/jobs/{job_id}/list-outputs", token).json()["files"]
    sup_fn = next((f for f in outs if f.startswith("support_track_") and f.endswith(".mp4")), None)
    base, sup = work / base_fn, (work / sup_fn if sup_fn else None)
    download(f"/api/jobs/{job_id}/outputs/{base_fn}", token, base)
    if sup_fn:
        download(f"/api/jobs/{job_id}/outputs/{sup_fn}", token, sup)
    else:
        print("  no support track found — PIP stage will be skipped")
    return base, sup


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

    cur, plan = None, []
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
            choice = cands[0]         # nothing fully legal: least-bad = below chin
        plan.append(choice)
        cur = choice
    windows, start = [], 0.0
    for i in range(1, len(plan) + 1):
        if i == len(plan) or plan[i] != plan[i - 1]:
            end = dur if i == len(plan) else float(i)
            windows.append((start, end, round(plan[i - 1] - 0.5, 3)))
            start = float(i)
    switches = len(windows) - 1
    print(f"placement plan: {len(windows)} windows, {switches} moves -> "
          + " | ".join(f"{a:.0f}-{b:.0f}s@{0.5 + o:.2f}" for a, b, o in windows))
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


def compose(base, sup, work: Path, dur, hook_end, key_hex, segs, audio, pip_y=PIP_Y):
    nocap = work / f"nocap_wm_y{pip_y}.mp4"  # pip_y in the name invalidates the cache on layout change
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
            f"[b0]trim=0:{hook_end},setpts=PTS-STARTPTS,chromakey={key_hex}:0.10:0.02,despill=type=green[fg];"
            f"[{idx}:v]trim=0:{hook_end},setpts=PTS-STARTPTS[bgt];"
            f"[bgt][fg]overlay=x=0:y=0:shortest=1[hook];"
            f"[b1]trim={hook_end},setpts=PTS-STARTPTS[rest];"
            f"[hook][rest]concat=n=2:v=1:a=0[v0]")
        vin, idx = "[v0]", idx + 1
    if segs and sup is not None:
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
    fontfile = "C\\:/Windows/Fonts/arial.ttf"
    fc_parts.append(f"{vin}drawtext=text='syntheticperformer':fontfile='{fontfile}'"
                    f":fontcolor=white@0.5:fontsize=34:x=44:y=h-78[vout]")
    print("compose: rendering base (no captions) ...")
    run(["ffmpeg", "-v", "error", *inputs, "-filter_complex", ";".join(fc_parts),
         "-map", "[vout]", "-map", f"{aidx}:a", "-c:v", "libx264", "-crf", "19", "-preset", "medium",
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
        raise SystemExit("pycaps render failed")


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


def run_autoedit(job_id: str, work: Path, out: Path, template: str = "korella",
                 placement: str = "dynamic", offset: float | None = None,
                 progress=lambda stage: None) -> Path:
    """The whole pass. `progress` gets called with a stage-name string."""
    work.mkdir(parents=True, exist_ok=True)
    progress("download")
    base, sup = fetch_job_files(job_id, work)
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
    nocap = compose(base, sup, work, dur, hook_end, key_hex, segs, audio, pip_y)
    progress("captions")
    if placement == "dynamic" and offset is None:
        occ_file = work / "occupancy.json"
        if occ_file.exists():
            buckets = json.loads(occ_file.read_text())
            print("occupancy: cached")
        else:
            buckets = build_occupancy(base, dur)
        occ_file.write_text(json.dumps(buckets))
        windows = plan_caption_windows(buckets, chin, segs, pip_y, dur)
        render_captions_dynamic(nocap, out, template, windows, work)
    else:
        render_captions(nocap, out, template, offset if offset is not None else auto_offset)
    return out
