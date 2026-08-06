# v911 — a delayed hard failure caused by the account block must actually
# restore the golden folder.
#
# Measured 2026-08-07, job 4f8687cf. Submit/403/restore ordering pulled from the
# live log:
#
#   RESTORE Account1 / Account2
#   SUBMIT clip 20 -> 403 -> UPLOAD        <- the one clip that landed
#   SUBMIT clip 1  -> 403,403 -> HARDFAIL  <- no restore
#   SUBMIT clip 2  -> 403,403 -> HARDFAIL  <- no restore
#   SUBMIT clip 1  -> 403,403 -> HARDFAIL  <- no restore
#   ... 7 hard failures, ONE restore in the whole stretch
#
# The branch prints "aborting job for golden restore" but raised
#   Exception("Flow delayed failure - clip(s) [N] failed after generating")
# while the account thread restores ONLY when str(e) contains
# "stopping job to trigger golden restore". The restore therefore never ran and
# the account self-resumed into the same blocked session, where every resubmit
# 403'd again. The only clip that succeeded was submitted right after a real
# restore - which is the whole point: restore, then it works.

import os
import re
import unittest

WORKER = os.path.join(os.path.dirname(__file__), "..", "static", "flow_worker.py")


def _source():
    with open(WORKER, encoding="utf-8") as f:
        return f.read()


def _branch(src):
    i = src.index("[v911] delayed failure on clip(s)")
    return src[i - 1500: i + 900]


class TestV911BlockHardFailRestores(unittest.TestCase):
    def test_block_hardfail_raises_the_restoring_signal(self):
        b = _branch(_source())
        self.assertIn("raise FlowAccountBlocked(job_id)", b,
                      "a block-caused hard failure must raise the typed restore signal")

    def test_detection_is_a_non_consuming_peek(self):
        """FailCheck and the other classifiers still need the marker."""
        b = _branch(_source())
        self.assertIn("consume=False", b)

    def test_non_block_hard_failure_keeps_generic_behaviour(self):
        """A genuine render failure (no 403 marker) must NOT force a restore -
        that would turn every content failure into a profile rebuild."""
        src = _source()
        b = _branch(src)
        self.assertIn('raise Exception(f"Flow delayed failure', b,
                      "the generic path must still exist for non-block failures")
        # the generic raise must come AFTER the guarded block raise
        self.assertLess(b.index("raise FlowAccountBlocked(job_id)"),
                        b.index('raise Exception(f"Flow delayed failure'))

    def test_peek_failure_cannot_break_the_job(self):
        b = _branch(_source())
        self.assertIn("_v911_blocked = False", b,
                      "a failed peek must fall back to the generic path, not raise")

    def test_signal_message_still_matches_the_restore_trigger(self):
        """The whole fix depends on this string reaching the account thread."""
        src = _source()
        m = re.search(r"class FlowAccountBlocked\(Exception\):.*?self\.job_id = job_id", src, re.S)
        ns = {}
        exec(m.group(0), ns)
        self.assertIn("stopping job to trigger golden restore", str(ns["FlowAccountBlocked"]("j")))
        self.assertIn('"stopping job to trigger golden restore" in str(e)', src)


if __name__ == "__main__":
    unittest.main()
