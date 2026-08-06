# v900 — the Flow submit-response listener must be installed in EVERY mode.
#
# Measured 2026-08-06 (job 2f72065d, start_worker.bat --single): a whole
# single-account run produced ZERO [flow-api-capture] and ZERO
# [fail-reason-diag] lines, versus dozens in a multi-account run. Cause: all
# seven _stash_profile_on_page call sites lived inside the AccountWorker class,
# so single-account main() never installed the listener. Consequences:
#   * v700 mediaId binding always failed ("no submit response captured in 40s")
#   * _scan_failure_reason never ran -> no 403 / RAI / policy reason EVER seen
#   * so the v829/v831 account-block and terminal-content routes could not fire
#   * every failure degraded to a DOM-only "refresh button + no video" HARD
#     FAILURE -> abort job -> golden restore -> reset clips -> repeat.

import os
import re
import unittest

WORKER = os.path.join(os.path.dirname(__file__), "..", "static", "flow_worker.py")


def _source():
    with open(WORKER, encoding="utf-8") as f:
        return f.read()


def _body_of(src, sig, span=2500):
    i = src.index(sig)
    return src[i:i + span]


class TestV900ListenerInstalled(unittest.TestCase):
    def test_both_submit_paths_install_on_their_own_page(self):
        """The authoritative fix: every submit path installs on the page it
        submits with, so a mid-loop golden restore that rebuilds the page
        cannot silently go blind again."""
        src = _source()
        for sig in ("def process_job_submission(", "def _process_redo_clip_impl("):
            self.assertIn(
                "_install_submit_response_listener(page",
                _body_of(src, sig),
                f"{sig} no longer installs the submit-response listener",
            )

    def test_submit_path_does_not_rewrite_user_data_dir(self):
        """Must NOT use _stash_profile_on_page here: it also rewrites
        page._user_data_dir, which in multi-account mode would stamp the wrong
        profile onto the page and break the HWND lookup."""
        src = _source()
        for sig in ("def process_job_submission(", "def _process_redo_clip_impl("):
            self.assertNotIn(
                "_stash_profile_on_page(page",
                _body_of(src, sig),
                f"{sig} must not rewrite _user_data_dir on the submit page",
            )

    def test_single_mode_main_installs_at_page_creation(self):
        """main() (start_worker.bat --single) installs at its page-creation
        sites too, so the listener is live before the first job."""
        src = _source()
        i = src.index("def main(account_session=None")
        main_body = src[i:i + 22000]
        self.assertIn("_stash_profile_on_page(page, SESSION_FOLDER", main_body,
                      "single-mode main() no longer installs the listener at page creation")

    def test_scan_failure_reason_still_wired_into_listener(self):
        """The reason scanner is what makes 403 / RAI / policy visible; if it
        ever leaves the listener, the worker is blind again even when the
        listener is installed."""
        src = _source()
        listener = _body_of(src, "def _install_submit_response_listener(", 4000)
        self.assertIn("_scan_failure_reason(resp, url, buf_key)", listener)


if __name__ == "__main__":
    unittest.main()
