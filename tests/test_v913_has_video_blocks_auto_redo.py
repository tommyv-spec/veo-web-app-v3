# v913 — never auto-redo a clip the platform already has a video for.
#
# Operator screenshot 2026-08-07: clips #2 and #3 showed a PLAYABLE VIDEO and a
# "REDO QUEUED" badge at the same time, while #4/#5 sat "Waiting..." behind
# them - finished work being regenerated while fresh clips starved.
#
# clip_done_in_platform() is the guard that is supposed to prevent exactly this.
# It only accepted clip.status in ('completed', 'approved'). But an uploaded
# render sits at approval_status 'pending_review' (UI: "PENDING REVIEW"), and
# clip.status can be anything - including 'flow_redo_queued', which the worker
# itself sets on a false failure. So for a clip that HAD a video the guard
# answered "not done" and the auto-redo went ahead.
#
# The endpoint already publishes the authoritative signal:
#     "has_video": clip.status == "completed" or bool(versions)

import os
import re
import unittest

WORKER = os.path.join(os.path.dirname(__file__), "..", "static", "flow_worker.py")


def _source():
    with open(WORKER, encoding="utf-8") as f:
        return f.read()


def _load_guard():
    """Exec just clip_done_in_platform with a stubbed api_request."""
    src = _source()
    m = re.search(r"def clip_done_in_platform\(.*?\n(?=\n\ndef )", src, re.S)
    assert m, "clip_done_in_platform not found"
    ns = {}
    def _make(resp):
        ns.clear()
        ns["api_request"] = lambda *a, **k: resp
        exec(m.group(0), ns)
        return ns["clip_done_in_platform"]
    return _make


class TestV913HasVideoBlocksAutoRedo(unittest.TestCase):
    def setUp(self):
        self.make = _load_guard()

    def test_pending_review_render_is_treated_as_done(self):
        """The exact case in the screenshot: a video exists, approval is still
        pending, and clip.status is NOT completed."""
        guard = self.make({"has_video": True,
                           "approval_status": "pending_review",
                           "status": "flow_redo_queued"})
        self.assertTrue(guard(123),
                        "a clip with an uploaded render must never be auto-redone")

    def test_completed_status_still_counts(self):
        guard = self.make({"has_video": False, "status": "completed"})
        self.assertTrue(guard(123))

    def test_genuinely_missing_render_still_redoes(self):
        """No video anywhere -> the auto-redo must still fire, or real failures
        would never recover."""
        guard = self.make({"has_video": False,
                           "approval_status": "pending_review",
                           "status": "flow_redo_queued"})
        self.assertFalse(guard(123))

    def test_require_approved_is_unchanged(self):
        """The v848 submit guard must stay strict: only APPROVED counts, so an
        uploaded-but-unapproved render does not stop the normal worker."""
        guard = self.make({"has_video": True, "approval_status": "pending_review",
                           "status": "completed"})
        self.assertFalse(guard(123, require_approved=True))
        guard2 = self.make({"has_video": False, "approval_status": "approved"})
        self.assertTrue(guard2(123, require_approved=True))

    def test_api_failure_still_fails_open(self):
        """On an API error the guard must return False so a genuine failure can
        still redo (fail-open, as the original docstring promises)."""
        guard = self.make(None)
        self.assertFalse(guard(123))


if __name__ == "__main__":
    unittest.main()
