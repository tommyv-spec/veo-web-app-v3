# v908 — stop submitting once the account is blocked.
#
# Operator screenshot 2026-08-06 (Flow project 95cef954): FOUR tiles in ONE
# project, every one "Failed - We noticed some unusual activity."
#
# Flow answers a blocked generate with 403 PUBLIC_ERROR_UNUSUAL_ACTIVITY roughly
# 40s after the Generate click, but the submit loop fires a clip every few
# seconds. So by the time the first block is observable, the rest of the job has
# already been pushed into the same burnt session. Those submits cannot succeed
# and they deepen the block.
#
# The guard peeks at the existing generate-403 marker before each subsequent
# clip and stops the loop, requeuing the unsent clips and raising the same
# golden-restore signal the rest of the worker uses.

import os
import unittest

WORKER = os.path.join(os.path.dirname(__file__), "..", "static", "flow_worker.py")


def _source():
    with open(WORKER, encoding="utf-8") as f:
        return f.read()


def _guard(src):
    i = src.index("[v908] account block detected")
    return src[i - 1200: i + 1600]


class TestV908StopSubmittingWhenBlocked(unittest.TestCase):
    def test_peek_does_not_consume_the_marker(self):
        """FailCheck and the delayed-failure classifier still need the marker to
        route the golden restore - the guard must not eat it."""
        self.assertIn("consume=False", _guard(_source()),
                      "the block peek must not consume the 403 marker")

    def test_guard_skips_the_first_clip(self):
        """Clip 0 has nothing before it; a stale marker must not block a job
        before it has even submitted once."""
        self.assertIn("if i > 0:", _guard(_source()))

    def test_unsent_clips_are_requeued(self):
        src = _source()
        self.assertIn("update_clip_status(_rc['id'], 'pending'", _guard(src),
                      "clips never submitted must go back to pending, not be lost")

    def test_raises_the_golden_restore_signal(self):
        self.assertIn("trigger golden restore (v758.24)", _guard(_source()),
                      "the account thread keys off this string to restore")

    def test_peek_failure_cannot_break_the_submit_loop(self):
        """A DOM/page error during the peek must never abort a healthy job."""
        g = _guard(_source())
        self.assertIn("_v908_blocked = \"\"  # a peek must never break the submit loop", g)


if __name__ == "__main__":
    unittest.main()
