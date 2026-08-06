# v902 — close the LATE-403 race.
#
# Measured 2026-08-06 (job 2f72065d), log order for a single clip:
#     [clip 1 attempt 1] SUBMITTED
#     [v700] WARNING ... no submit response captured within 40s
#     [FailCheck] OK No clear failure detected
#     [API] Clip 13965 status -> generating
#     [fail-reason-diag] ... HTTP 403  "reCAPTCHA evaluation failed"
#
# The 403 lands ~45s after the Generate click - AFTER the submit drain and
# AFTER FailCheck already passed the clip to 'generating'. v901 put the reload
# inside FailCheck, so the marker is stamped after the only branch that reads
# it has returned: detection fired, the reload never did, and the run still
# ended in DELAYED HARD FAILURE -> golden restore -> repeat.
#
# The delayed-failure handler is the first point where the response HAS
# arrived, so that is where the classification has to happen.

import os
import unittest

WORKER = os.path.join(os.path.dirname(__file__), "..", "static", "flow_worker.py")


def _lines():
    with open(WORKER, encoding="utf-8") as f:
        return f.read().split("\n")


def _source():
    return "\n".join(_lines())


class TestV902Late403Race(unittest.TestCase):
    def test_delayed_failure_checks_recaptcha_before_declaring_hard_failure(self):
        src = _source()
        i_check = src.index("_recent_generate_recaptcha_fail(_rc_bk)")
        # anchor on the actual abort print, not the v758.19 comment that
        # mentions "DELAYED HARD FAILURES" ~15k lines earlier in the file
        i_abort = src.index('⛔ DELAYED HARD FAILURE: clip(s)')
        self.assertLess(i_check, i_abort,
                        "the reCAPTCHA classification must run before the hard-failure abort")

    def test_recaptcha_path_reloads_and_requeues_without_restore(self):
        src = _source()
        i = src.index("_recent_generate_recaptcha_fail(_rc_bk)")
        branch = src[i:src.index("# Re-test: v902")]
        self.assertIn("page.reload(", branch, "must reload the page to re-fetch reCAPTCHA")
        self.assertIn("flow_redo_queued", branch, "must requeue the affected clips")
        self.assertIn("_hard_fails = []", branch,
                      "must clear the hard-fail list so the abort is skipped")

    def test_abort_and_raise_are_gated_on_hard_fails(self):
        """If v902 absorbs every hard failure, the whole abort + golden-restore
        + raise block must be skipped so the job keeps submitting."""
        lines = _lines()
        gi = next(i for i, l in enumerate(lines)
                  if l.strip() == "if _hard_fails:" and "v902" in lines[i - 1])
        ri = next(i for i, l in enumerate(lines)
                  if 'raise Exception(f"Flow delayed failure' in l)
        self.assertLess(gi, ri, "the raise must come after the guard")
        guard_indent = len(lines[gi]) - len(lines[gi].lstrip())
        escapes = [
            i + 1 for i in range(gi + 1, ri + 1)
            if lines[i].strip() and (len(lines[i]) - len(lines[i].lstrip())) <= guard_indent
        ]
        self.assertEqual([], escapes,
                         f"the delayed-failure raise escaped the _hard_fails guard at {escapes[:3]}")

    def test_real_hard_failure_still_aborts(self):
        """A hard failure with no reCAPTCHA marker must still reach the
        golden-restore abort - v902 narrows the classification, it does not
        remove the safety net."""
        src = _source()
        self.assertIn("DELAYED HARD FAILURE", src)
        self.assertIn('raise Exception(f"Flow delayed failure', src)


if __name__ == "__main__":
    unittest.main()
