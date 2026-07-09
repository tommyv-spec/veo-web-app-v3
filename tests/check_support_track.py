import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import subprocess, tempfile
from pathlib import Path
import video_processor as vp

tmp = Path(tempfile.mkdtemp())
def mk_png(name, color):
    p = tmp / name
    subprocess.run([vp.FFMPEG_BIN, "-y", "-f", "lavfi", "-i",
                    f"color=c={color}:s=400x400:d=1", "-frames:v", "1", str(p)],
                   capture_output=True)
    return p

support_clips = [
    {"image_index": 7, "path": str(mk_png("a.png", "red")),  "start": 3.0,  "end": 5.0},
    {"image_index": 8, "path": str(mk_png("b.png", "blue")), "start": 10.0, "end": 12.0},
]
out = tmp / "support_track.mp4"
stats = vp.export_support_track(support_clips, master_duration=15.0,
                                output_path=out, width=720, height=1280, fps=24)
assert out.exists()
dur = vp.get_duration(vp.ffprobe_json(out))
assert 14.7 <= dur <= 15.3, f"track duration {dur} != master 15.0"
info = vp.ffprobe_json(out)
assert not any(s["codec_type"] == "audio" for s in info["streams"]), "track must be silent"
print("OK support track", stats)
