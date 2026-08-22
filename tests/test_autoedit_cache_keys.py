"""A cached artifact must be named after EVERYTHING baked into it.

This file exists because that rule was broken FIVE times in autoedit_pipeline.py
in a single day, and every breach failed the same way: the stage ran, the log
said so truthfully, and the delivered file was still the old one.

  audio_pol.wav     keyed on nothing         -> a rebuilt voice chain never ran
                                                (proved bit-identical by md5,
                                                 mtime 10h older than the fix)
  nocap_wm_*.mp4    keyed on picture + music -> muxed the NEW audio, then handed
                                                back the OLD cached video
  cap_pass_*.mp4    keyed on offset+template -> burned captions over yesterday's
                                                composite and discarded the fresh one
  occupancy_*.json  keyed on trim values     -> would reuse a scan of the wrong video

The tests below are deliberately dumb: for every input of every cache name,
change ONLY that input and assert the name moves. A cache builder that ignores
one of its inputs cannot pass. When you add a cache, add its builder to the
CACHE NAMES block in autoedit_pipeline.py and a case here.
"""
import hashlib
import importlib.util
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "autoedit_pipeline.py"
SPEC = importlib.util.spec_from_file_location("autoedit_pipeline", MODULE_PATH)
ap = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ap)


BASE_COMPOSE = dict(
    pip_y=1050, pip_enabled=True, chroma_similarity=0.10, chroma_blend=0.02,
    music=None, music_db=-20.0, audio_fingerprint="aaaaaaaa", hook_corner=None,
)


class FileFingerprintTests(unittest.TestCase):
    def test_same_bytes_same_key(self):
        with tempfile.TemporaryDirectory() as td:
            a, b = Path(td) / "a.bin", Path(td) / "b.bin"
            a.write_bytes(b"identical payload")
            b.write_bytes(b"identical payload")
            self.assertEqual(ap.file_fingerprint(a), ap.file_fingerprint(b))

    def test_different_bytes_different_key(self):
        with tempfile.TemporaryDirectory() as td:
            a, b = Path(td) / "a.bin", Path(td) / "b.bin"
            a.write_bytes(b"payload one")
            b.write_bytes(b"payload two")
            self.assertNotEqual(ap.file_fingerprint(a), ap.file_fingerprint(b))

    def test_mtime_does_not_change_the_key(self):
        """Content, not mtime — an identical re-render must still hit the cache."""
        import os
        import time
        with tempfile.TemporaryDirectory() as td:
            a = Path(td) / "a.bin"
            a.write_bytes(b"payload")
            first = ap.file_fingerprint(a)
            os.utime(a, (time.time() + 5000, time.time() + 5000))
            self.assertEqual(first, ap.file_fingerprint(a))


class AudioCacheNameTests(unittest.TestCase):
    def test_chain_key_tracks_the_voice_chain(self):
        before = ap.audio_chain_key()
        original = ap._VOICE_CHAIN
        try:
            ap._VOICE_CHAIN = original + ",highpass=f=200"
            self.assertNotEqual(before, ap.audio_chain_key(),
                                "editing the voice chain must invalidate cached audio")
        finally:
            ap._VOICE_CHAIN = original
        self.assertEqual(before, ap.audio_chain_key())

    def test_chain_key_tracks_the_fallback_chain(self):
        before = ap.audio_chain_key()
        original = ap._VOICE_CHAIN_RAW
        try:
            ap._VOICE_CHAIN_RAW = original + ",volume=2"
            self.assertNotEqual(before, ap.audio_chain_key())
        finally:
            ap._VOICE_CHAIN_RAW = original

    def test_chain_key_tracks_the_low_shelf_constants(self):
        """The shelf gain is MEASURED per job, so it is not in the name — but the
        target and clamps that produce it are, or a retuned target is invisible."""
        for attr, bump in (("_LOW_TARGET_DB", 1.0),
                           ("_LOW_GAIN_MIN", 1.0),
                           ("_LOW_GAIN_MAX", 1.0)):
            before = ap.audio_chain_key()
            original = getattr(ap, attr)
            try:
                setattr(ap, attr, original + bump)
                self.assertNotEqual(before, ap.audio_chain_key(),
                                    f"{attr} must invalidate cached audio")
            finally:
                setattr(ap, attr, original)

    def test_denoised_and_fallback_are_different_files(self):
        key = ap.audio_chain_key()
        self.assertNotEqual(ap.audio_cache_name(key, denoised=True),
                            ap.audio_cache_name(key, denoised=False))


class ComposeCacheKeyTests(unittest.TestCase):
    def key(self, **over):
        return ap.compose_cache_key(**{**BASE_COMPOSE, **over})

    def test_audio_is_in_the_key(self):
        """The half that was missing: compose() MUXES the audio into this mp4."""
        self.assertNotEqual(self.key(), self.key(audio_fingerprint="bbbbbbbb"))

    def test_every_visual_input_is_in_the_key(self):
        for field, other in (("pip_y", 900),
                             ("pip_enabled", False),
                             ("chroma_similarity", 0.17),
                             ("chroma_blend", 0.05),
                             ("music_db", -14.0),
                             ("hook_corner", 0.43)):
            with self.subTest(field=field):
                self.assertNotEqual(self.key(), self.key(**{field: other}),
                                    f"{field} must change the composed-video cache name")

    def test_music_track_is_in_the_key(self):
        self.assertNotEqual(self.key(), self.key(music=Path("bed_a.mp3")))
        self.assertNotEqual(self.key(music=Path("bed_a.mp3")),
                            self.key(music=Path("bed_b.mp3")))

    def test_identical_inputs_are_stable(self):
        self.assertEqual(self.key(), self.key())


class CaptionPassNameTests(unittest.TestCase):
    def test_source_video_is_in_the_name(self):
        """The breach that shipped: a correct new composite was built, then
        thrown away for a captioned pass from the previous day."""
        self.assertNotEqual(ap.cap_pass_name(-0.05, "korella", "aaaaaaaa"),
                            ap.cap_pass_name(-0.05, "korella", "bbbbbbbb"))

    def test_offset_and_template_are_in_the_name(self):
        self.assertNotEqual(ap.cap_pass_name(-0.05, "korella", "aaaaaaaa"),
                            ap.cap_pass_name(0.20, "korella", "aaaaaaaa"))
        self.assertNotEqual(ap.cap_pass_name(-0.05, "korella", "aaaaaaaa"),
                            ap.cap_pass_name(-0.05, "hype", "aaaaaaaa"))

    def test_name_is_filesystem_safe(self):
        """A negative offset must not put '-' or '.' into the filename."""
        name = ap.cap_pass_name(-0.05, "korella", "aaaaaaaa")
        self.assertNotIn("-0.05", name)
        self.assertTrue(name.endswith(".mp4"))


class OccupancyNameTests(unittest.TestCase):
    def test_scanned_video_is_in_the_name(self):
        """With hook_corner the scan reads the COMPOSITE, not the base export —
        so the two maps must not share a filename."""
        self.assertNotEqual(ap.occupancy_name(0.0, 0.0, "aaaaaaaa"),
                            ap.occupancy_name(0.0, 0.0, "bbbbbbbb"))

    def test_trim_is_in_the_name(self):
        self.assertNotEqual(ap.occupancy_name(0.0, 0.0, "aaaaaaaa"),
                            ap.occupancy_name(0.3, 0.0, "aaaaaaaa"))
        self.assertNotEqual(ap.occupancy_name(0.0, 0.0, "aaaaaaaa"),
                            ap.occupancy_name(0.0, 0.5, "aaaaaaaa"))


class NoUnkeyedCacheNamesTests(unittest.TestCase):
    """A canary, not a proof.

    Every cached artifact in the pipeline should be built by one of the pure
    functions above. If a raw `work / f"..."` cache name appears again, that is
    how all five breaches started — a name assembled inline, next to the stage
    that wrote it, thinking only about that stage's own settings.
    """

    ALLOWED_INLINE = {
        "audio_raw.wav", "audio_enh.wav", "audio_probe.wav",  # not caches: fixed intermediates
        "hookbg.mp4", "pipmask.png", "scan.json", "qc_report.json",
        "preview12.mp4", "style_previews",
    }

    def test_builders_are_used_for_the_four_known_caches(self):
        src = MODULE_PATH.read_text(encoding="utf-8")
        for builder in ("audio_cache_name(", "compose_cache_key(",
                        "cap_pass_name(", "occupancy_name("):
            with self.subTest(builder=builder):
                # defined once, called at least once
                self.assertGreaterEqual(
                    src.count(builder), 2,
                    f"{builder} should be defined AND called; if a call site went "
                    f"back to building its name inline, that is the bug this file guards")


if __name__ == "__main__":
    unittest.main()
