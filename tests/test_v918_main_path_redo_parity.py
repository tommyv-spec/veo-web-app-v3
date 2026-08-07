# v918 — the main submit path must run the redo path's pre-Generate sequence.
#
# Measured 2026-08-07, job 4f8687cf, on ONE golden built the operator's way
# (verified on disk: labs.google cookies = 0, Local Storage / Session Storage /
# Service Worker / IndexedDB all purged, both goldens sharing one Google
# account and one auth fingerprint):
#
#   Account1, REDO path, abra_i2v_8s  -> SCHEDULED -> ACTIVE -> SUCCESSFUL
#                                        2 variants downloaded and uploaded
#   Account2, MAIN path, abra_i2v_4s  -> HTTP 403 on BOTH variants
#                                        "reCAPTCHA evaluation failed"
#                                        PUBLIC_ERROR_UNUSUAL_ACTIVITY
#
# Same golden, same account, same minute. So the golden was not the variable.
# The duration bucket was not either: the historical log has abra_i2v_6s 403ing
# on Account2 and abra_i2v_4s 403ing on Account1.
#
# What actually differed was the last few seconds before the click. The redo
# path (flow_worker.py ~16452-16476) does:
#
#     fill_prompt_textarea(...)
#     human_pre_generate_wait(page)      <- main path never called this
#     human_mouse_move / human_delay / scroll_randomly / human_delay
#     <wait for enabled>
#     human_delay(0.5, 1.5)              <- main path had no settle
#     human_click_element(...)           <- nothing injected in between
#
# while the main path fired page.evaluate (the v700h tile snapshot) as the very
# last thing to touch the page and then clicked immediately. The reCAPTCHA
# token is minted by the page at click time, so the last thing to touch the
# page should be a human action, not injected script.
#
# Note the redo path's own comment claims it "match[es] main flow exactly"
# (line ~16456). It never did — and it is the path that lands.
#
# This test locks the ordering. It does NOT claim the 403 is fixed; that needs
# production evidence.

import os
import re
import unittest

WORKER = os.path.join(os.path.dirname(__file__), "..", "static", "flow_worker.py")


def _source():
    with open(WORKER, encoding="utf-8") as f:
        return f.read()


def _click_generate_button(src):
    """Just the body of click_generate_button, so neighbouring functions
    (notably the redo path and the rebuild-retry fallback, which have always
    called human_pre_generate_wait) cannot satisfy these assertions."""
    m = re.search(r"\ndef click_generate_button\(.*?(?=\ndef click_generate_with_crash_handler\()",
                  src, re.S)
    assert m, "click_generate_button not found"
    return m.group(0)


class TestV918MainPathRedoParity(unittest.TestCase):
    def setUp(self):
        self.fn = _click_generate_button(_source())

    def test_main_path_calls_human_pre_generate_wait(self):
        """The beat the main path was missing entirely."""
        self.assertIn("human_pre_generate_wait(page)", self.fn,
                      "the normal submit must run the same pre-generate wait as the redo path")

    def test_tile_snapshot_runs_before_the_human_beats(self):
        """page.evaluate must not be the last thing to touch the page."""
        i_eval = self.fn.index("_v700h_pre_ids = page.evaluate(")
        i_human = self.fn.index("human_pre_generate_wait(page)")
        i_move = self.fn.index("human_mouse_move(page)")
        self.assertLess(i_eval, i_human,
                        "the v700h snapshot must run BEFORE the human beats")
        self.assertLess(i_eval, i_move,
                        "the v700h snapshot must run BEFORE the look-around")

    def test_settle_delay_immediately_precedes_the_click(self):
        i_delay = self.fn.index("human_delay(0.5, 1.5)")
        i_click = self.fn.index("human_click_element(page, arrow_btn")
        self.assertLess(i_delay, i_click)
        between = self.fn[i_delay:i_click]
        self.assertNotIn("page.evaluate", between,
                         "nothing may touch the page between the settle and the click")

    def test_v700j_stamp_stays_immediately_before_the_click(self):
        """v918.1 regression guard.

        The first cut of v918 put the settle delay BELOW the v700j block, so
        the click-time stamp fired 0.5-1.5s ahead of the real click. v700j
        exists to reject submit-responses captured BEFORE the click
        (min_captured_at, ~line 1960); widening that gap lets a response
        captured inside it pass the filter and bind to the wrong clip.

        The stamp must therefore be the LAST thing before the click, and the
        settle must sit above it — which is safe because the v700j block
        touches no page."""
        i_delay = self.fn.index("human_delay(0.5, 1.5)")
        i_stamp = self.fn.index("page._v700j_last_click_at = time.time()")
        i_click = self.fn.index("human_click_element(page, arrow_btn")
        self.assertLess(i_delay, i_stamp,
                        "the settle must run BEFORE the click-time stamp")
        self.assertLess(i_stamp, i_click)
        tail = self.fn[i_stamp:i_click]
        for blocking in ("human_delay(", "human_pre_generate_wait(",
                         "human_mouse_move(", "scroll_randomly(", "time.sleep("):
            self.assertNotIn(blocking, tail,
                             f"{blocking} must not sit between the stamp and the click")

    def test_nothing_touches_the_page_after_the_last_human_beat(self):
        """The v700j stamp + drain may stay before the click because they are
        pure Python (time.time() and an in-process buffer), but no page call
        may sneak back in."""
        i_move = self.fn.index("human_mouse_move(page)")
        i_click = self.fn.index("human_click_element(page, arrow_btn")
        tail = self.fn[i_move:i_click]
        self.assertNotIn("page.evaluate", tail,
                         "no script injection between the human beats and the click")
        # the v700j bookkeeping must survive the reorder
        self.assertIn("_v700j_last_click_at", tail)
        self.assertIn("_drain_submit_responses", tail)

    def test_v700h_snapshot_still_exists_and_still_binds(self):
        """The reorder must not drop ghost detection."""
        self.assertIn("page._v700h_pre_tile_ids = set(_v700h_pre_ids)", self.fn)
        self.assertIn("page._v700h_snapshot_at", self.fn)

    def test_redo_path_sequence_is_untouched(self):
        """The path that works must not be 'harmonised' in the other
        direction — it is the reference, not the thing to change."""
        src = _source()
        i = src.index("human_click_element(page, page.locator(\"button:has(i:text('arrow_forward'))\").first, \"Generate button\"")
        before = src[i - 900:i]
        self.assertIn("human_pre_generate_wait(page)", before)
        self.assertIn("human_delay(0.5, 1.5)", before)


if __name__ == "__main__":
    unittest.main()
