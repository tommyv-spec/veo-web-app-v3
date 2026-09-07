"""FLOW_ONLY_JOB_IDS — a scoped PRODUCTION worker claims only its named jobs.

Why this is separate from FLOW_ONLY_CLIP_IDS: that one is a proof-run guard whose
only input is the redo endpoint, so it can never render a clip that has never
generated — the server answers "Clip is pending initial generation". That makes it
unusable for delivering a new video. This scope is the production case: a second
worker takes NAMED jobs, first generation included, and nothing else, so an
80-clip job cannot park the day's two short ones.
"""

import os
import types
import unittest
from pathlib import Path

WORKER = Path(__file__).resolve().parents[1] / "static" / "flow_worker.py"
SOURCE = WORKER.read_text(encoding="utf-8")


def _load(name, start_marker, end_marker):
    mod = types.ModuleType("_probe")
    mod.__dict__["os"] = os
    src = SOURCE[SOURCE.index(start_marker):SOURCE.index(end_marker)]
    exec(compile(src, "<probe>", "exec"), mod.__dict__)
    return mod.__dict__[name]


parse = _load("_parse_flow_only_job_ids",
              "def _parse_flow_only_job_ids", "FLOW_ONLY_JOB_IDS = ")

JOB_A = "94ebffdd-cc42-40c3-b13e-a0c381fe2b77"
JOB_B = "8b800f8b-0ed6-4f06-96f1-40bbc0ce2360"


class JobScopeParser(unittest.TestCase):
    def test_none_means_no_scope(self):
        self.assertEqual(frozenset(), parse(None))

    def test_a_single_job(self):
        self.assertEqual({JOB_A}, set(parse(JOB_A)))

    def test_several_jobs_any_separator_or_spacing(self):
        for raw in (f"{JOB_A},{JOB_B}", f" {JOB_B} , {JOB_A} ", f"{JOB_A};{JOB_B}"):
            self.assertEqual({JOB_A, JOB_B}, set(parse(raw)), raw)

    def test_blank_is_no_scope_not_an_error(self):
        """An unset variable is the normal case for every existing worker."""
        for raw in ("", "   "):
            self.assertEqual(frozenset(), parse(raw))

    def test_junk_fails_CLOSED(self):
        """A typo must never silently become 'claim anything'."""
        for raw in (",", ",,,", "; ,"):
            with self.assertRaises(RuntimeError, msg=raw):
                parse(raw)


class JobScopeWiring(unittest.TestCase):
    """The scope is only real if it reaches BOTH claim paths."""

    def test_the_pending_claim_walks_read_only_before_claiming(self):
        """Anchored inside get_pending_job, not at the startup banner — the first
        `if FLOW_ONLY_JOB_IDS:` in the file is the announcement, not the claim."""
        block = SOURCE[SOURCE.index("def get_pending_job("):]
        block = block[:block.index("def ", 10)]
        self.assertIn("no worker_id -> claims nothing", block)
        self.assertIn("peek_url", block)
        # The read-only walk must come BEFORE the claiming request, or the worker
        # claims someone else's job first and releases it after the fact.
        self.assertLess(block.index("peek_url"),
                        block.index('url = f"/jobs/pending?worker_id='))

    def test_a_wrongly_claimed_job_is_released_untouched(self):
        self.assertIn("[Scope] BLOCKED job", SOURCE)
        self.assertIn('update_job_status(got, "pending")', SOURCE)

    def test_the_redo_poll_is_filtered_by_job_not_emptied(self):
        """Returning [] would strand the worker beside its OWN retry clip."""
        self.assertIn(
            'clips = [c for c in clips if str(c.get("job_id") or "") in FLOW_ONLY_JOB_IDS]',
            SOURCE)

    def test_the_clip_proof_run_guard_is_untouched(self):
        """FLOW_ONLY_CLIP_IDS keeps its redo-only semantics; this is additive."""
        self.assertIn("A clip-scoped proof run must never claim a regular job.", SOURCE)
        idx = SOURCE.index("A clip-scoped proof run must never claim a regular job.")
        self.assertIn("if FLOW_ONLY_CLIP_IDS:\n        return None", SOURCE[idx:idx + 400])

    def test_the_scope_announces_itself_at_startup(self):
        """A silent scope is unverifiable in a log after the fact."""
        self.assertIn("[Scope] Flow JOB allowlist active:", SOURCE)


if __name__ == "__main__":
    unittest.main()
