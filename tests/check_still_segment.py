import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import subprocess, tempfile
from pathlib import Path
import video_processor as vp

tmp = Path(tempfile.mkdtemp())
png = tmp / "s.png"
subprocess.run([vp.FFMPEG_BIN, "-y", "-f", "lavfi", "-i", "color=c=red:s=400x400:d=1",
                "-frames:v", "1", str(png)], capture_output=True)
out = tmp / "seg.mp4"
vp.make_still_segment(png, 3.0, out, 720, 1280, fps=24)
assert out.exists(), "segment not created"
info = vp.ffprobe_json(out)
dur = vp.get_duration(info)
assert 2.8 <= dur <= 3.2, f"duration {dur} != ~3.0"
vstream = next(s for s in info["streams"] if s["codec_type"] == "video")
assert int(vstream["width"]) == 720 and int(vstream["height"]) == 1280, vstream
assert not any(s["codec_type"] == "audio" for s in info["streams"]), "must be silent"
print("OK still segment")
