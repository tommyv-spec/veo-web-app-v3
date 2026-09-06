"""Safety contract for the persistent Gemini emergency video worker."""
from __future__ import annotations

import ast
from pathlib import Path
import unittest


SOURCE_PATH = Path(__file__).resolve().parents[1] / "static" / "gemini_video_worker.py"
SOURCE = SOURCE_PATH.read_text(encoding="utf-8")


class GeminiEmergencyLifecycle(unittest.TestCase):
    def test_persistent_serve_requires_the_shared_lifecycle(self):
        self.assertIn('args.serve and os.environ.get("KAVENO_LIFECYCLE_LAUNCH") != "1"',
                      SOURCE)
        self.assertIn("worker_lifecycle.py ensure emergency", SOURCE)

    def test_persistent_serve_is_headless(self):
        self.assertIn("launch(p, headless=args.serve)", SOURCE)

    def test_lifecycle_supplies_email_without_putting_it_on_the_command_line(self):
        self.assertIn('default=os.environ.get("GEMINI_WORKER_EMAIL")', SOURCE)

    def test_missing_or_signed_out_private_state_holds_before_serving(self):
        tree = ast.parse(SOURCE)
        main = next(node for node in tree.body
                    if isinstance(node, ast.FunctionDef) and node.name == "main")
        body = ast.get_source_segment(SOURCE, main)
        self.assertIn("_private_profile_seeded(PROFILE_DIR)", body)
        self.assertIn("_hold_emergency", body)
        self.assertIn("timeout_s=(60 if args.serve else 600)", body)


if __name__ == "__main__":
    unittest.main()
