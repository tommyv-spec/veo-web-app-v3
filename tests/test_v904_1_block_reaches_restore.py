# v904.1 — the account-block signal must REACH the account thread.
#
# Measured 2026-08-06, job d8051bf6 clip 20: v904 detected the block correctly
# but raised a plain Exception from inside the redo scan loop, whose broad
# `except Exception` printed "[REDO] Scan error: ..." and kept scanning. The
# block was detected and swallowed ELEVEN times in a row, the clip was requeued
# each time, and the golden restore never ran - the loop simply fell through to
# "HTTP scan failed - falling back to download tab".
#
# The fix is a dedicated exception type that every broad handler on the path
# re-raises, carrying the exact message the account thread matches on.

import os
import re
import unittest

WORKER = os.path.join(os.path.dirname(__file__), "..", "static", "flow_worker.py")


def _source():
    with open(WORKER, encoding="utf-8") as f:
        return f.read()


class TestV904_1BlockReachesRestore(unittest.TestCase):
    def test_exception_type_exists(self):
        self.assertIn("class FlowAccountBlocked(Exception):", _source())

    def test_message_matches_what_the_account_thread_looks_for(self):
        """The account thread decides to golden-restore with
        `"stopping job to trigger golden restore" in str(e)` - so the typed
        exception's message MUST contain that substring, or the restore is
        silently skipped."""
        src = _source()
        i = src.index("class FlowAccountBlocked(Exception):")
        body = src[i:i + 1600]
        self.assertIn("stopping job to trigger golden restore", body,
                      "message no longer matches the account thread's trigger")
        # and the thread really does match on that string
        self.assertIn('"stopping job to trigger golden restore" in str(e)', src)

    def test_message_is_built_correctly_at_runtime(self):
        """Construct the real class in isolation and check the message."""
        src = _source()
        m = re.search(r"class FlowAccountBlocked\(Exception\):.*?self\.job_id = job_id",
                      src, re.S)
        self.assertIsNotNone(m, "FlowAccountBlocked body not found")
        ns = {}
        exec(m.group(0), ns)
        err = ns["FlowAccountBlocked"]("abc123")
        self.assertIn("stopping job to trigger golden restore", str(err))
        self.assertIn("unusual activity", str(err))
        self.assertEqual("abc123", err.job_id)

    def test_redo_scan_loop_reraises_instead_of_swallowing(self):
        """The exact handler that ate it 11 times must now re-raise, and the
        re-raise must come BEFORE the broad handler."""
        src = _source()
        i_typed = src.index("except FlowAccountBlocked:")
        i_broad = src.index('print(f"[REDO] Scan error: {e}"')
        self.assertLess(i_typed, i_broad,
                        "the typed re-raise must precede the broad 'Scan error' handler")

    def test_all_block_raises_are_typed(self):
        """No detection site may raise a plain Exception for this signal - a
        plain one gets swallowed by the next broad handler."""
        src = _source()
        stray = re.findall(
            r'raise Exception\(f?"Job \{job_id\} unusual activity', src)
        self.assertEqual([], stray,
                         "an account-block site still raises an untyped Exception")
        self.assertGreaterEqual(src.count("raise FlowAccountBlocked(job_id)"), 2)


if __name__ == "__main__":
    unittest.main()
