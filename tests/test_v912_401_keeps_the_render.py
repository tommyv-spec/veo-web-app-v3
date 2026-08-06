# v912 — a stale-session 401 must not destroy a finished render.
#
# Measured 2026-08-06, clip 20 (job d8051bf6). The clip RENDERED successfully:
#
#   [REDO] partial-ok (MAIN parity v870) — taking 1 good video(s) at scan 1
#   [Account1-HTTP-DL] Clip 20 variant 1.1: .../media.getMediaUrlRedirect?name=d4e7bb5a...
#   [Account1-HTTP-DL] 401 on clip 20 — waiting for session refresh (try 1/6)
#   ... 2/6 ... 6/6
#   [Account1-HTTP-DL] ✗ Clip 20 variant 1.1 failed: HTTP 401
#   [Account1-HTTP-DL] ✗ Clip 20 all variants failed — queuing for redo
#
# A finished video was thrown away and re-generated because our cookie snapshot
# went stale. The v843 retry only waits for the SUBMIT thread to publish a new
# session (session_ref[0] is not sess); when that thread is busy or mid-restore
# the session never changes and all six waits are wasted.
#
# The download worker cannot mint its own session: it is explicitly "zero
# browser interaction" so it stays immune to greenlet thread affinity, and
# calling browser.cookies() from it would break that. So instead of
# regenerating, it now re-queues the DOWNLOAD - the mediaId URL is stable and
# the render still exists on Flow's side.

import os
import unittest

WORKER = os.path.join(os.path.dirname(__file__), "..", "static", "flow_worker.py")


def _source():
    with open(WORKER, encoding="utf-8") as f:
        return f.read()


def _branch(src):
    i = src.index("[v912] Clip")
    return src[i - 2200: i + 1600]


class TestV912_401KeepsTheRender(unittest.TestCase):
    def test_401_requeues_the_download_not_a_regeneration(self):
        b = _branch(_source())
        self.assertIn("_dl_401_retry", b, "the download must be re-queued with a retry counter")
        self.assertLess(b.index("_dl_401_retry"),
                        b.index("update_clip_status(clip_id, 'flow_redo_queued')"),
                        "the download re-queue must be tried BEFORE falling back to a redo")

    def test_only_a_401_takes_this_path(self):
        """A genuine render failure must still redo - otherwise a broken clip
        would loop on downloads forever."""
        b = _branch(_source())
        self.assertIn("_last_err_401 and _dl_retry < HTTP_DL_401_REQUEUE_MAX", b)
        self.assertIn("update_clip_status(clip_id, 'flow_redo_queued')", b,
                      "the real-failure redo path must still exist")

    def test_401_is_detected_from_the_variant_error(self):
        src = _source()
        self.assertIn('if "401" in str(ve):', src)
        self.assertIn("_last_err_401 = True", src)

    def test_retries_are_bounded_with_a_growing_delay(self):
        """Unbounded re-queues would spin forever on a genuinely dead session."""
        src = _source()
        self.assertIn("HTTP_DL_401_REQUEUE_MAX = 3", src)
        self.assertIn("HTTP_DL_401_REQUEUE_DELAY", src)
        self.assertIn("HTTP_DL_401_REQUEUE_DELAY * (_dl_retry + 1)", src,
                      "delay should grow with the attempt number")

    def test_already_completed_clip_is_still_short_circuited_first(self):
        b = _branch(_source())
        self.assertLess(b.index("clip_done_in_platform"), b.index("_last_err_401 and"),
                        "a clip already completed in the platform must never be re-queued")

    def test_requeue_block_itself_touches_no_browser(self):
        """The whole reason this fix re-queues instead of refreshing: the
        download worker must stay free of browser interaction (greenlet
        affinity). Scope the check to the inserted branch only."""
        src = _source()
        i = src.index("elif _last_err_401 and _dl_retry < HTTP_DL_401_REQUEUE_MAX:")
        j = src.index("update_clip_status(clip_id, 'flow_redo_queued')", i)
        # strip comment lines - the block DESCRIBES browser.cookies() in prose
        # explaining why it must not call it; only executable code matters here
        block = "\n".join(
            l for l in src[i:j].splitlines() if not l.lstrip().startswith("#")
        )
        for forbidden in ("browser.cookies()", "_snapshot_cookies()", "page.goto", "page.evaluate"):
            self.assertNotIn(forbidden, block,
                             f"download worker must not touch the browser ({forbidden})")


if __name__ == "__main__":
    unittest.main()
