"""Small real-ffmpeg smoke test for the repair filter graph."""
import pathlib
import importlib.util
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from autoedit_pipeline import (  # noqa: E402
    audio_levels,
    compose,
    probe_media,
    run_quality_checks,
    trim_media,
)
from autoedit_qc import normalize_repairs  # noqa: E402


@unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "ffmpeg is required")
class AutoEditMediaSmokeTests(unittest.TestCase):
    def test_key_pip_music_and_trim_filter_graph(self):
        with tempfile.TemporaryDirectory(prefix="autoedit-smoke-") as td:
            work = pathlib.Path(td)
            base = work / "base.mp4"
            support = work / "support.mp4"
            voice = work / "voice.wav"
            music = work / "music.wav"

            subprocess.run([
                "ffmpeg", "-v", "error",
                "-f", "lavfi", "-i", "color=c=0x2d974c:s=1080x1920:d=0.8:r=12",
                "-f", "lavfi", "-i", "color=c=blue:s=1080x1920:d=1.2:r=12",
                "-f", "lavfi", "-i", "sine=frequency=440:duration=2",
                "-filter_complex", "[0:v][1:v]concat=n=2:v=1:a=0[v]",
                "-map", "[v]", "-map", "2:a", "-c:v", "libx264", "-preset", "ultrafast",
                "-pix_fmt", "yuv420p", "-c:a", "aac", "-y", str(base),
            ], check=True)
            subprocess.run([
                "ffmpeg", "-v", "error", "-f", "lavfi", "-i",
                "color=c=red:s=1920x1080:d=2:r=12", "-c:v", "libx264", "-preset", "ultrafast",
                "-pix_fmt", "yuv420p", "-y", str(support),
            ], check=True)
            subprocess.run([
                "ffmpeg", "-v", "error", "-i", str(base), "-vn", "-ac", "1", "-ar", "48000",
                "-y", str(voice),
            ], check=True)
            subprocess.run([
                "ffmpeg", "-v", "error", "-f", "lavfi", "-i", "sine=frequency=220:duration=2",
                "-ar", "48000", "-y", str(music),
            ], check=True)

            composed = compose(
                base, support, work, 2.0, 0.8, "0x2d974c", [(1.0, 1.8)], voice,
                pip_y=1050, pip_enabled=True, chroma_similarity=0.10,
                chroma_blend=0.02, music=music, music_db=-20.0,
            )
            trimmed, new_dur = trim_media(composed, work / "trimmed.mp4", 0.1, 0.2, 2.0)
            media = probe_media(trimmed)
            streams = media["streams"]

            self.assertTrue(any(s.get("codec_type") == "video" for s in streams))
            self.assertTrue(any(s.get("codec_type") == "audio" for s in streams))
            self.assertAlmostEqual(new_dur, 1.7)
            self.assertLess(abs(float(media["format"]["duration"]) - 1.7), 0.2)
            self.assertIsNotNone(audio_levels(trimmed))

            if importlib.util.find_spec("cv2"):
                report = run_quality_checks(
                    trimmed, base, new_dur,
                    buckets=[{"t": 0.5, "faces": []}],
                    windows=[(0.0, new_dur, 0.0)],
                    segs=[(0.9, 1.6)], pip_y=1050, hook_end=0.7,
                    repairs=normalize_repairs({"captions_enabled": False}),
                )
                self.assertIn(report["verdict"], ("READY", "NEEDS_MANUAL_EDIT"))
                self.assertTrue(report["checks"])


if __name__ == "__main__":
    unittest.main()
