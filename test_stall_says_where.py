"""v1007 — a stall must name itself in the log, with no browser watching.

Operator, 2026-09-16: *"improve the logging if needed, so you don't need to
monitor the worker acting on the browser, but just the logs."*

The v978 watchdog already prints the main thread's stack. It is useless here and
always has been: the worker's own frames run inside a Playwright greenlet, so
`sys._current_frames()[main]` shows the asyncio dispatcher every single time --

    File "playwright/sync_api/_context_manager.py", line 56, in greenlet
      self._loop.run_until_complete(self._connection.run_as_sync())
    File "asyncio/base_events.py", line 712, in run_until_complete
    ...

identical for all 17 stalls on 2026-09-16, and naming no worker function at all.
Finding that the hang was `_refresh_and_verify` took an hour of reading 14,000
log lines backwards past the watchdog's own output.

So the worker leaves a breadcrumb -- what it is doing, since when -- and the
stall lines quote it. The point is that the log alone answers "where is it
stuck", the first time, without attaching to a browser.
"""
import importlib.util
import os
import pathlib
import sys
import time
import unittest

_STATIC = pathlib.Path(__file__).parent / "static"
_PATH = pathlib.Path(os.environ.get("FLOW_WORKER_PATH") or (_STATIC / "flow_worker.py"))


def _load():
    if str(_STATIC) not in sys.path:
        sys.path.insert(0, str(_STATIC))
    spec = importlib.util.spec_from_file_location("flow_worker_v1007", _PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TheBreadcrumb(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fw = _load()

    def test_where_names_the_last_activity(self):
        self.fw.activity("deep-scan refresh (popup) clip 10")
        self.assertIn("deep-scan refresh (popup) clip 10", self.fw._v978_where())

    def test_where_says_how_long_it_has_been_doing_it(self):
        self.fw.activity("submitting clip 4")
        self.fw._ACTIVITY["at"] = time.time() - 300
        where = self.fw._v978_where()
        self.assertIn("300s", where, f"expected an age in seconds, got {where!r}")

    def test_it_never_raises_even_with_nothing_recorded(self):
        """The stall path must not be the thing that crashes the worker."""
        self.fw._ACTIVITY.clear()
        self.assertIsInstance(self.fw._v978_where(), str)

    def test_a_later_activity_replaces_the_earlier_one(self):
        self.fw.activity("first")
        self.fw.activity("second")
        self.assertIn("second", self.fw._v978_where())
        self.assertNotIn("first", self.fw._v978_where())


class TheStallLinesQuoteIt(unittest.TestCase):
    """A breadcrumb nothing prints is a breadcrumb nobody reads."""

    @classmethod
    def setUpClass(cls):
        cls.src = _PATH.read_text(encoding="utf-8", errors="replace")

    def _tick(self):
        at = self.src.index("def _v978_tick")
        return self.src[at:self.src.index("def _v978_main_stack")]

    def test_the_warn_line_says_where(self):
        body = self._tick()
        warn = body[body.index("NO MAIN-THREAD PROGRESS"):]
        self.assertIn("_v978_where", warn[:400],
                      "the warn line must name the activity, not just a stack")

    def test_the_act_line_says_where(self):
        body = self._tick()
        act = body[body.index("STALLED — no main-thread progress"):]
        self.assertIn("_v978_where", act[:400],
                      "the kill line must name the activity that caused it")


class TheHangingPathLeavesCrumbs(unittest.TestCase):
    """Cover the exact path that produced 15 of the 17 deaths."""

    @classmethod
    def setUpClass(cls):
        cls.src = _PATH.read_text(encoding="utf-8", errors="replace")

    def test_each_budgeted_step_records_itself(self):
        """_run_within_budget wraps the three refresh helpers, so one call here
        gives per-helper granularity exactly where the pile-up happened."""
        at = self.src.index("def _run_within_budget")
        body = self.src[at:at + 1600]
        self.assertIn("activity(", body,
                      "each budgeted step must record what it is before running it")

    def test_the_deep_scan_records_itself(self):
        at = self.src.index("Deep scan attempt")
        window = self.src[max(0, at - 400):at + 400]
        self.assertIn("activity(", window,
                      "the deep scan is where 10 of 17 deaths happened; it must "
                      "say so in the log")


if __name__ == "__main__":
    unittest.main(verbosity=2)
