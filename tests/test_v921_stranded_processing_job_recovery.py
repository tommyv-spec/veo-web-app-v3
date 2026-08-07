# v921 — a worker that dies mid-job must not strand its job forever.
#
# Measured 2026-08-07, job 09083c15. The worker was killed mid-job (to install a
# new build). The job stayed at status 'processing' with claimed_by_worker still
# set, and the live worker then polled "No pending jobs or redos" indefinitely.
# It had to be reset to 'pending' by hand.
#
# Cause: the only claim-release sweep in /jobs/pending filtered
#     Job.status.in_(['pending', 'queued_for_flow'])
# so 'processing' — the state a job is actually IN when its worker dies — was
# never covered. Any crash, OOM, Render deploy or kill produced the same
# permanent orphan.
#
# The dangerous naive fix is to release 'processing' on claimed_at age. A real
# 15-clip job runs far longer than the 10-minute claim window, so that would
# hand a live worker's job to a second worker and double-submit its clips
# (burning Flow credits). v921 therefore keys on updated_at — "has any clip
# reported recently" — and only after a deliberately generous 30 minutes.
#
# That required a second change: Clip has NO updated_at column (only Job does,
# models.py:167) and the clip-status endpoints wrote only clip fields on the
# common path, so Job.updated_at sat frozen at the moment the job went
# 'processing'. Both endpoints now touch it on every clip report, which is what
# makes it a real liveness signal.

import os
import re
import unittest

MAIN = os.path.join(os.path.dirname(__file__), "..", "main.py")


def _source():
    with open(MAIN, encoding="utf-8") as f:
        return f.read()


class TestV921StrandedProcessingJobRecovery(unittest.TestCase):
    def setUp(self):
        self.src = _source()

    def test_both_pending_endpoints_sweep_processing(self):
        """user-worker AND local-worker /jobs/pending must both recover."""
        blocks = re.findall(r"Job\.status == 'processing',\s*\n\s*Job\.updated_at < stranded_cutoff",
                            self.src)
        self.assertGreaterEqual(len(blocks), 2,
                                "both /jobs/pending endpoints need the stranded sweep")

    def test_sweep_keys_on_updated_at_not_claimed_at(self):
        """Keying on claimed_at would steal jobs from live workers."""
        for m in re.finditer(r"stranded_jobs = db\.query\(Job\)\.filter\((.*?)\)\.all\(\)",
                             self.src, re.S):
            body = m.group(1)
            self.assertIn("Job.updated_at < stranded_cutoff", body)
            self.assertNotIn("claimed_at", body,
                             "must not release a live multi-clip job on claim age")

    def test_cutoff_is_generous(self):
        """A long legitimate job must never be swept. 30min >> any gap between
        clip reports (CLIP_READY_WAIT is 50s)."""
        cuts = re.findall(r"stranded_cutoff = datetime\.utcnow\(\) - timedelta\(minutes=(\d+)\)",
                          self.src)
        self.assertTrue(cuts, "stranded_cutoff not found")
        for c in cuts:
            self.assertGreaterEqual(int(c), 30)

    def test_release_clears_claim_and_requeues(self):
        """Clearing the claim alone is not enough: the pickup query filters on
        status in ('pending','queued_for_flow'), so status must go back too."""
        for m in re.finditer(r"for sj in stranded_jobs:(.*?)if stranded_jobs:", self.src, re.S):
            body = m.group(1)
            self.assertIn("sj.claimed_by_worker = None", body)
            self.assertIn("sj.claimed_at = None", body)
            self.assertIn("sj.status = 'pending'", body)

    def test_clip_reports_touch_the_job_heartbeat(self):
        """Without this, Job.updated_at stays frozen at job start and the sweep
        would eventually eat a healthy long-running job."""
        self.assertGreaterEqual(
            self.src.count("job.updated_at = datetime.utcnow()"), 2,
            "both clip-status endpoints must bump the job heartbeat")

    def test_original_claim_sweep_is_unchanged(self):
        """The 10-minute pending/queued_for_flow release must still work."""
        self.assertIn("Job.status.in_(['pending', 'queued_for_flow'])", self.src)
        self.assertIn("Job.claimed_at < claim_timeout", self.src)

    def test_sweep_is_scoped_to_flow_backend(self):
        for m in re.finditer(r"stranded_jobs = db\.query\(Job\)\.filter\((.*?)\)\.all\(\)",
                             self.src, re.S):
            self.assertIn("Job.backend == 'flow'", m.group(1))

    def test_user_scoped_sweep_stays_user_scoped(self):
        """The user-worker endpoint must not touch other users' jobs."""
        m = re.search(r"Job\.user_id == user_id,\s*\n\s*Job\.backend == 'flow',\s*\n\s*"
                      r"Job\.status == 'processing'", self.src)
        self.assertIsNotNone(m, "user-worker stranded sweep must filter on user_id")


if __name__ == "__main__":
    unittest.main()
