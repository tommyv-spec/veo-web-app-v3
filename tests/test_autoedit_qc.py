import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from autoedit_qc import (  # noqa: E402
    build_qc_report,
    caption_face_overlap_metrics,
    normalize_repairs,
)


class AutoEditQcTests(unittest.TestCase):
    def test_default_repairs_are_safe_and_json_ready(self):
        r = normalize_repairs()
        self.assertIs(r["pip_enabled"], True)
        self.assertIs(r["captions_enabled"], True)
        self.assertIsNone(r["music_filename"])
        self.assertEqual(r["music_db"], -20.0)

    def test_bad_trim_is_rejected(self):
        for value in (-1, 601):
            with self.assertRaises(ValueError):
                normalize_repairs({"trim_start_s": value})

    def test_music_must_be_a_plain_media_filename(self):
        with self.assertRaises(ValueError):
            normalize_repairs({"music_filename": "../secret.mp3"})
        with self.assertRaises(ValueError):
            normalize_repairs({"music_filename": "notes.txt"})
        self.assertEqual(
            normalize_repairs({"music_filename": "Last Hope Reverb.mp3"})["music_filename"],
            "Last Hope Reverb.mp3",
        )

    def test_caption_overlap_reports_vertical_pixels(self):
        buckets = [{"t": 0.5, "faces": [[0.2, 0.4, 0.8, 0.6]]}]
        windows = [(0.0, 1.0, 0.0)]  # caption band 0.415-0.585 (0.17 = the planner's reserve)
        result = caption_face_overlap_metrics(buckets, windows)
        # overlap 0.415-0.585 vs face 0.40-0.60 -> 0.17 of a 1920-tall frame
        self.assertAlmostEqual(result["worst_vertical_px"], 326.4)
        self.assertEqual(result["seconds_checked"], 1)

    def test_qc_report_routes_failures_to_manual_edit(self):
        report = build_qc_report([
            {"id": "video", "status": "pass", "message": "Video is valid"},
            {"id": "captions", "status": "fail", "message": "Captions cover a face"},
        ])
        self.assertEqual(report["verdict"], "NEEDS_MANUAL_EDIT")
        self.assertEqual(report["reasons"], ["Captions cover a face"])

    def test_qc_report_marks_all_passes_ready(self):
        report = build_qc_report([
            {"id": "video", "status": "pass", "message": "Video is valid"},
        ])
        self.assertEqual(report["verdict"], "READY")
        self.assertEqual(report["reasons"], [])

    def test_info_status_reports_without_blocking(self):
        """v938.20 — the CapCut tone score is recorded on every render and must
        never flip the verdict.

        The target curve is an average of exports that are mostly ONE speaker,
        so a deep or unusual voice scores worse with nothing actually wrong
        (measured: 1.08 and 1.89 on two deliveries, 2.91 on a deep male voice).
        Hard-gating that would repeat the v936.2 mistake of giving a soft
        signal a hard gate.
        """
        report = build_qc_report([
            {"id": "video", "status": "pass", "message": "Video is valid"},
            {"id": "audio_tone_match", "status": "info",
             "message": "Voice tone is 2.91 dB off CapCut's curve", "value": 2.91},
        ])
        self.assertEqual(report["verdict"], "READY")
        self.assertEqual(report["reasons"], [])
        row = next(c for c in report["checks"] if c["id"] == "audio_tone_match")
        self.assertEqual(row["value"], 2.91)      # recorded, so it can be reviewed

    def test_a_real_fail_still_blocks(self):
        report = build_qc_report([
            {"id": "audio_tone_match", "status": "info", "message": "off", "value": 3.0},
            {"id": "captions", "status": "fail", "message": "Captions cover a face"},
        ])
        self.assertEqual(report["verdict"], "NEEDS_MANUAL_EDIT")
        self.assertEqual(report["reasons"], ["Captions cover a face"])


if __name__ == "__main__":
    unittest.main()
