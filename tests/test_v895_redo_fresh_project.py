# v895 — a redo clip whose job has NO project URL must fall through to the
# fresh-project branch, NOT park itself back to flow_redo_queued. The park
# livelocked: the redo-pending poll filters on flow_redo_queued, so the same
# clip was re-claimed, re-marked generating, and re-parked forever
# (job 46dc610a, 2026-08-05, clips 12-15 cycling on both accounts).
#
# The decision lives inline in _process_redo_clip_impl (needs a live page to
# exercise), so these are source-contract tests: they pin the exact lines whose
# regression would reintroduce the livelock.

import os
import re
import unittest

WORKER = os.path.join(os.path.dirname(__file__), "..", "static", "flow_worker.py")


def _source():
    with open(WORKER, encoding="utf-8") as f:
        return f.read()


class TestV895RedoFreshProject(unittest.TestCase):
    def test_no_url_branch_does_not_park_to_flow_redo_queued(self):
        # The old livelock line: park + return inside the no-project-url branch.
        src = _source()
        self.assertNotIn(
            'error_message="No project URL available',
            src,
            "no-URL branch parks the clip back to flow_redo_queued again — "
            "that re-queues into the same redo-pending poll forever (livelock)",
        )

    def test_missing_url_forces_fresh_project(self):
        # _need_new_project must include the missing-URL condition, or the
        # fall-through would goto(None) with no declared intent.
        src = _source()
        self.assertRegex(
            src,
            re.compile(
                r"_need_new_project\s*=\s*bool\(_policy_swap_model\)\s*or\s*not\s+project_url"
            ),
            "_need_new_project no longer accounts for a missing project URL",
        )

    def test_fresh_project_url_cached_for_sibling_clips(self):
        # The created project must be written back to the job cache (per
        # account) so the job's other queued redo clips reuse it instead of
        # each creating another project — a new-project burst per clip is the
        # automated signal that trips the unusual-activity block.
        src = _source()
        self.assertIn(
            "_j.setdefault('account_projects', {})[account_name] = project_url",
            src,
            "fresh redo project is no longer cached for the job's other clips",
        )
        # Policy-swap projects are model-locked and must NOT be cached.
        self.assertRegex(
            src,
            re.compile(r"if\s+cache\s+is\s+not\s+None\s+and\s+not\s+_policy_swap_model"),
            "cache write-back lost the policy-swap guard",
        )


if __name__ == "__main__":
    unittest.main()
