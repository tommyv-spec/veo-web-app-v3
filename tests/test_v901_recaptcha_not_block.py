# v901 — a generate 403 caused by the PAGE's reCAPTCHA must not be treated as
# an account block.
#
# Captured live 2026-08-06 (job 2f72065d) once v900 made the response visible:
#
#   HTTP 403 {"error":{"code":403,
#     "message":"reCAPTCHA evaluation failed",
#     "status":"PERMISSION_DENIED",
#     "details":[{"reason":"PUBLIC_ERROR_UNUSUAL_ACTIVITY"}]}}
#
# Byte-identical in shape to a genuinely flagged account, so v829 classified it
# as a block and went golden restore -> relaunch -> resubmit -> 403, forever,
# never generating a clip. The image worker's v894 proved the cause is
# PAGE-level (Flow's reCAPTCHA script missing/stale) and that ONLY a page reload
# reloads that script - a browser relaunch cannot.

import os
import unittest

WORKER = os.path.join(os.path.dirname(__file__), "..", "static", "flow_worker.py")


def _source():
    with open(WORKER, encoding="utf-8") as f:
        return f.read()


class TestV901RecaptchaNotBlock(unittest.TestCase):
    def test_recaptcha_marker_recorded_from_403_body(self):
        src = _source()
        self.assertIn("_record_generate_recaptcha_fail(buf_key)", src,
                      "the 403 body scanner no longer stamps the reCAPTCHA cause")
        self.assertIn("if 'recaptcha' in _body.lower():", src,
                      "reCAPTCHA detection no longer reads the response body")

    def test_recaptcha_branch_runs_before_the_account_block_branch(self):
        """Ordering is the whole fix: the generic 403 branch would otherwise
        consume the marker and golden-restore."""
        src = _source()
        i_rc = src.index("_recent_generate_recaptcha_fail(_bk)")
        i_403 = src.index("if _recent_generate_403(_bk):")
        self.assertLess(i_rc, i_403,
                        "the reCAPTCHA branch must be checked before the account-block branch")

    def test_recaptcha_branch_reloads_and_does_not_restore(self):
        src = _source()
        i_rc = src.index("_recent_generate_recaptcha_fail(_bk)")
        i_403 = src.index("if _recent_generate_403(_bk):")
        branch = src[i_rc:i_403]
        self.assertIn("page.reload(", branch,
                      "only a page reload re-fetches Flow's reCAPTCHA script (v894)")
        self.assertNotIn("abort_unusual_activity", branch,
                         "a page-level reCAPTCHA failure must NOT trigger a golden restore")

    def test_marker_window_is_consumed(self):
        """Consume-on-read so a stale marker cannot re-trigger on a later clip."""
        src = _source()
        i = src.index("def _recent_generate_recaptcha_fail(")
        body = src[i:i + 600]
        self.assertIn("hit and consume", body)
        self.assertIn("_GENERATE_RECAPTCHA_TS.pop", body)


if __name__ == "__main__":
    unittest.main()
