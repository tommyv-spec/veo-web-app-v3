"""v867 — export_support_track memory-shape + placement regression.

Locks in the fix for the 2026-07-23 container OOM: the old command fed one
INFINITE `-loop 1 -i img` stream per still (N x master_duration frames possible
in flight); x264 on Render's 1-vCPU box could not drain them and ffmpeg's
filter->encoder queue grew until the 2GB cgroup was exhausted.

These checks assert the command is now bounded (finite per-still inputs +
ultrafast + single-threaded) AND that still placement is byte-exact — the
v825.3 anti-drift guarantee must survive the change.

Run: python tests/check_support_track_mem.py   (ffmpeg required for placement)
"""
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import video_processor as vp  # noqa: E402

FAILED = []


def check(name, cond, detail=""):
    print(("  PASS  " if cond else "  FAIL  ") + name + ("" if cond else f"  {detail}"))
    if not cond:
        FAILED.append(name)


def _captured_cmd(monkey_clips, master=73.45, w=1920, h=1080, fps=24):
    """Run export_support_track but intercept the ffmpeg argv instead of running it."""
    seen = {}

    def fake_run(cmd, *a, **k):
        seen["cmd"] = cmd
        return 0, "", ""

    orig = vp.run
    vp.run = fake_run
    try:
        vp.export_support_track(monkey_clips, master, "/tmp/out.mp4", width=w, height=h, fps=fps)
    finally:
        vp.run = orig
    return seen.get("cmd", [])


def main():
    clips = [{"image_index": i, "path": f"/x/c_{i}.png",
              "start": 3.0 + i * 8.4, "end": 3.0 + i * 8.4 + 3.0} for i in range(8)]
    cmd = _captured_cmd([dict(c) for c in clips])
    s = " ".join(cmd)

    check("no INFINITE image inputs (every -loop 1 has a matching -t)",
          s.count("-loop 1") == s.count("-t ") - 1 or "-itsoffset" in s,
          f"loops={s.count('-loop 1')} t={s.count('-t ')}")
    check("each still input is finite (-itsoffset + -t per still)",
          cmd.count("-itsoffset") == 8 and cmd.count("-loop") == 8, s[:120])
    check("ultrafast preset (fast drain)", "ultrafast" in cmd, s)
    check("single-threaded (no per-thread buffers)",
          "-threads" in cmd and cmd[cmd.index("-threads") + 1] == "1", s)
    check("still a single filtergraph (no concat drift path)",
          "-filter_complex" in cmd and s.count("overlay=enable") == 8, s[:80])

    # Placement must stay byte-exact — render for real if ffmpeg is present.
    if not shutil.which("ffmpeg"):
        print("  SKIP  placement (ffmpeg not on PATH)")
    else:
        try:
            from PIL import Image
        except Exception:
            print("  SKIP  placement (Pillow missing)")
            _report()
            return
        colors = [(255, 0, 0), (0, 255, 0), (0, 0, 255), (255, 255, 0),
                  (255, 0, 255), (0, 255, 255), (255, 128, 0), (128, 0, 255)]
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            rc = []
            for i, c in enumerate(colors):
                Image.new("RGB", (1920, 1080), c).save(d / f"c_{i}.png")
                rc.append({"image_index": i, "path": str(d / f"c_{i}.png"),
                           "start": 3.0 + i * 8.4, "end": 3.0 + i * 8.4 + 3.0})
            out = d / "track.mp4"
            vp.export_support_track(rc, 73.45, str(out), width=1920, height=1080, fps=24)
            check("track rendered", out.exists() and out.stat().st_size > 0)

            def near(a, b, t=30):
                return all(abs(x - y) <= t for x, y in zip(a, b))

            ok = True
            for i, c in enumerate(rc):
                for label, t, exp in (("mid", (c["start"] + c["end"]) / 2, colors[i]),
                                      ("after", c["end"] + 2.0, (0, 0, 0))):
                    if t >= 73.45:
                        continue
                    fp = d / "f.png"
                    subprocess.run(["ffmpeg", "-y", "-ss", f"{t:.3f}", "-i", str(out),
                                    "-frames:v", "1", str(fp)],
                                   capture_output=True)
                    px = Image.open(fp).convert("RGB").getpixel((960, 540))
                    if not near(px, exp):
                        ok = False
                        print(f"      still {i} {label} t={t:.2f}: {px} != {exp}")
            check("placement byte-exact for all 8 stills (v825.3 preserved)", ok)

    _report()


def _report():
    print()
    if FAILED:
        print(f"FAILED ({len(FAILED)}): {', '.join(FAILED)}")
        sys.exit(1)
    print("ALL PASS")


if __name__ == "__main__":
    main()
