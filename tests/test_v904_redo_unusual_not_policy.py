# v904 — an account block in the REDO path must reach the golden restore, not
# the content-policy lane.
#
# Measured 2026-08-06, clip 13967: every generate answered
#   403 {"message":"reCAPTCHA evaluation failed","reason":"PUBLIC_ERROR_UNUSUAL_ACTIVITY"}
# and the operator's Flow UI showed tiles reading "We noticed some unusual
# activity". The worker logged instead:
#   [promptB] clip 13967: policy block — retrying SAME model with Prompt B
#   [REDO] policy-blocked — retry_prompt_b + requeuing redo
#
# Cause: the redo "persistent failure after retry" branch (v758.16) assumed any
# such failure is a prompt/policy block. It checks terminal-reason text, then
# prominent-people text, then DEFAULTS to route_generation_policy. An account
# block matches neither, so it fell into the policy lane and looped
# Prompt B -> requeue -> block, never reaching the golden restore that clears
# it. The MAIN path has this check (v829/v831); the REDO path never got it.

import os
import unittest

WORKER = os.path.join(os.path.dirname(__file__), "..", "static", "flow_worker.py")


def _source():
    with open(WORKER, encoding="utf-8") as f:
        return f.read()


class TestV904RedoUnusualNotPolicy(unittest.TestCase):
    def test_unusual_activity_checked_before_the_policy_default(self):
        """Ordering is the fix: the policy default must not run first."""
        src = _source()
        i_ua = src.index("[v904] Clip")
        i_policy = src.index("policy-blocked — {_pa} + requeuing redo", i_ua - 6000)
        self.assertLess(i_ua, i_policy,
                        "the unusual-activity check must precede the content-policy default")

    def test_it_raises_the_golden_restore_signal(self):
        """The account thread keys off this exact string to golden-restore."""
        src = _source()
        i = src.index("[v904] Clip")
        branch = src[i: src.index("_term_reason = tile_text_terminal_reason", i)]
        # v904.1 — the signal is now a typed FlowAccountBlocked whose message
        # carries the same text; a plain Exception here got swallowed by the
        # scan loop's broad handler (11x on job d8051bf6).
        self.assertIn("raise FlowAccountBlocked(job_id)", branch)
        self.assertIn("flow_redo_queued", branch,
                      "the clip must be requeued so the restore can retry it")

    def test_403_marker_checked_before_dom_text(self):
        """The card renders in the account locale (es-419 seen in prod), so the
        locale-independent HTTP 403 marker has to be the primary signal."""
        src = _source()
        self.assertLess(src.index("_ua_403 = _recent_generate_403"),
                        src.index("_ua_dom = bool(page.evaluate"))

    def test_dom_check_is_locale_robust(self):
        src = _source()
        i = src.index("_ua_dom = bool(page.evaluate")
        branch = src[i: src.index("_term_reason = tile_text_terminal_reason", i)]
        for phrase in ("unusual activity", "actividad inusual", "help center", "centro de ayuda"):
            self.assertIn(phrase, branch, f"locale variant {phrase!r} not matched")

    def test_content_policy_lane_still_exists(self):
        """v904 narrows the classification; it must not remove the real
        content-policy handling."""
        src = _source()
        self.assertIn("route_generation_policy(clip_id", src)
        self.assertIn("retrying SAME model with Prompt B", src)


if __name__ == "__main__":
    unittest.main()
