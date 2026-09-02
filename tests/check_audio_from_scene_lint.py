import pathlib
import subprocess
import sys
import tempfile

LINTER = pathlib.Path(__file__).resolve().parent.parent / "verify_video_format.py"
HEAD = "## Images\n\n### Image 1\n- **Image prompt:**\n```\nx\n```\n\n## Storyboard\n\n"


def run(body: str) -> str:
    d = tempfile.mkdtemp()
    f = pathlib.Path(d) / "b.md"
    f.write_text(HEAD + body, encoding="utf-8")
    r = subprocess.run([sys.executable, str(LINTER), str(f)], capture_output=True,
                       text=True, encoding="utf-8", errors="replace")
    return r.stdout + r.stderr


SILENT_SOURCE = """\
### Scene 1
- **image:** image_1
- **speaker:** silent
- **clip_duration_s:** 4

### Scene 2
- **image:** image_1
- **speaker:** voiceover
- **audio_from_scene:** 1
- **line:** a line
- **clip_duration_s:** 4
"""

SPOKEN_SOURCE = """\
### Scene 1
- **image:** image_1
- **speaker:** on-camera
- **line:** a spoken line
- **clip_duration_s:** 4

### Scene 2
- **image:** image_1
- **speaker:** voiceover
- **audio_from_scene:** 1
- **line:** a spoken
- **clip_duration_s:** 4
"""

MISSING_SOURCE = """\
### Scene 1
- **image:** image_1
- **speaker:** on-camera
- **line:** a spoken line
- **clip_duration_s:** 4

### Scene 2
- **image:** image_1
- **speaker:** voiceover
- **audio_from_scene:** 9
- **line:** a spoken
- **clip_duration_s:** 4
"""

BOTH_DECLARED = """\
### Scene 1
- **image:** image_1
- **speaker:** on-camera
- **line:** a spoken line
- **clip_duration_s:** 4

### Scene 2
- **image:** image_1
- **speaker:** voiceover
- **audio_from_scene:** 1
- **voiceover_anchor_image:** image_1
- **line:** a spoken
- **clip_duration_s:** 4
"""

NEITHER = """\
### Scene 1
- **image:** image_1
- **speaker:** voiceover
- **line:** a line
- **clip_duration_s:** 4
"""

out = run(SILENT_SOURCE)
assert "does not speak" in out, out

out = run(SPOKEN_SOURCE)
assert "does not speak" not in out, out
# Gate 9 must NOT fire: audio_from_scene is the other legal way to get audio
assert "Gate 9" not in out, out

out = run(MISSING_SOURCE)
assert "no scene 9" in out, out

out = run(BOTH_DECLARED)
assert "both" in out, out

out = run(NEITHER)
assert "Gate 9" in out, out

print("check_audio_from_scene_lint: OK")
