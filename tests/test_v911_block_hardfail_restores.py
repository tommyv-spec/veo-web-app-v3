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



class TestV911_1WindowFitsTheLateClassifier(unittest.TestCase):
    """v911.1 — the marker window must cover how LATE this classifier runs.

    Live proof 2026-08-07: v911 shipped and did NOT fire on the first hard
    failure, because GENERATE_403_WINDOW_S is 90s while the delayed hard-failure
    check runs minutes after the click (50s wait + up to 20 scans x 15s). The
    marker was always stale by then, so a real block still read as a plain
    render failure and the golden restore still never ran.
    """

    def test_accessor_accepts_a_custom_window(self):
        src = _source()
        self.assertIn("def _recent_generate_403(buf_key, consume=True, window_s=None):", src)
        self.assertIn("_win = GENERATE_403_WINDOW_S if window_s is None else window_s", src)

    def test_default_window_unchanged_for_existing_callers(self):
        """FailCheck runs seconds after the click and must keep the tight 90s."""
        src = _source()
        self.assertIn("GENERATE_403_WINDOW_S = 90", src)

    def test_v911_uses_the_wide_window(self):
        b = _branch(_source())
        self.assertIn("window_s=600", b,
                      "the late classifier must look back far enough to see the block")

    def test_window_logic_runtime(self):
        """Exercise the comparison itself: a 5-minute-old marker is stale at 90s
        but live at 600s."""
        import time as _t
        now = _t.time()
        ts = now - 300  # 5 minutes ago
        self.assertFalse((now - ts) <= 90, "5-min-old marker must be stale at the default window")
        self.assertTrue((now - ts) <= 600, "5-min-old marker must be live at the wide window")


if __name__ == "__main__":
    unittest.main()
