# v922 — a queued redo must not be starved by the post-restore self-resume.
#
# Measured 2026-08-07, job 09083c15:
#   [12:59:58] Found 1 NEW clip(s) needing redo (1 idle accounts)
#     -> Clip 2 (attempt 1) assigned to Account2
#   [Account2] Self-resuming job 09083c15... after golden restore
#   [Account2] PARALLEL SECONDARY: Processing job 09083c15...
# and no "Processing redo" line ever followed. The assigned redo sat in
# self.job_queue unread.
#
# The account loop carried the comment "Check for redo clips first" while doing
# the opposite: the self-resume branch ran first and the queue was read ONLY in
# its else. Because a self-resume re-enters the same job that just 403'd ->
# hard failure -> golden restore -> self-resume again, the queue could go unread
# indefinitely. An earlier run made 11 main-path submits and 0 redo submits with
# redos queued the whole time; the only 2 clips that landed all night came from
# redos left over from a previous session.
#
# The first cut of this fix had a control-flow bug worth locking out: it left
# the original `else: job = self.job_queue.get(timeout=1)` reachable after a
# redo had already been taken, which would have blocked for a second and then
# overwritten the redo with whatever came next. Hence the sentinel.

import os
import re
import unittest

WORKER = os.path.join(os.path.dirname(__file__), "..", "static", "flow_worker.py")


def _source():
    with open(WORKER, encoding="utf-8") as f:
        return f.read()


def _block(src):
    """The dispatch block inside the account run-loop."""
    i = src.index("_retry_job = self._pending_resume_job")
    j = src.index("if job.get('type') == 'redo':", i)
    return src[i:j]


class TestV922RedoNotStarvedBySelfResume(unittest.TestCase):
    def setUp(self):
        self.b = _block(_source())

    def test_queue_is_checked_before_the_self_resume_is_consumed(self):
        i_peek = self.b.index("self.job_queue.get_nowait()")
        i_take = self.b.index("job = _retry_job")
        self.assertLess(i_peek, i_take,
                        "the queued redo must be looked at before the self-resume is taken")

    def test_only_a_redo_preempts_the_resume(self):
        """A normal job must NOT jump ahead of a self-resume — that would
        re-split parallel assignments (the v174 behaviour this protects)."""
        self.assertIn("_peeked.get('type') == 'redo'", self.b)
        self.assertIn("self.job_queue.put(_peeked)", self.b,
                      "a non-redo peeked off the queue must be put back")

    def test_resume_stays_pending_when_a_redo_wins(self):
        """_retry_job must not be cleared on the redo path, or the job it was
        going to resume is silently dropped."""
        i_peek = self.b.index("self.job_queue.get_nowait()")
        i_take = self.b.index("job = _retry_job")
        between = self.b[i_peek:i_take]
        self.assertNotIn("_retry_job = None", between,
                         "taking a redo must leave the self-resume pending")
        self.assertNotIn("self._pending_resume_job = None", between)

    def test_sentinel_prevents_the_blocking_get_from_overwriting_a_redo(self):
        """Regression guard for the first cut of v922: the fallback
        job_queue.get(timeout=1) must be unreachable once a redo was taken."""
        self.assertIn("job = None", self.b)
        i_guard = self.b.index("if job is None:")
        i_block_get = self.b.index("self.job_queue.get(timeout=1)")
        self.assertLess(i_guard, i_block_get,
                        "the blocking get must sit inside the `job is None` guard")

    def test_from_queue_is_set_for_the_redo_path(self):
        """task_done bookkeeping: a redo really did come off the queue."""
        i_peek = self.b.index("job = _peeked")
        tail = self.b[i_peek:i_peek + 200]
        self.assertIn("_from_queue = True", tail)

    def test_peek_failure_is_not_fatal(self):
        """An empty queue raises — that is the normal case, not an error."""
        self.assertIn("except Exception:", self.b)
        self.assertIn("_peeked = None", self.b)


if __name__ == "__main__":
    unittest.main()
