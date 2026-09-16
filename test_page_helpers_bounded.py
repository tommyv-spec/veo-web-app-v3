"""v1008 — bound the page helpers THEMSELVES, not one of their 114 callers.

2026-09-16, second pass. v1006 put a budget around the three helpers inside
`_refresh_and_verify`, because 15 of 17 worker deaths pointed there. The worker
restarted on that fix and hung for 251s anyway, in the download loop:

    [DOWNLOAD] Clips [9] reached 50s — refreshing page...
    [STALL] main thread unchanged for 120s
    [v978] NO MAIN-THREAD PROGRESS for 251s

`_refresh_and_verify` returned fine. The very next statement in the download
loop is a bare `check_and_dismiss_popup(self.page)` — a DIFFERENT caller of the
same helper, with no budget at all. Counting them:

    check_and_dismiss_popup      76 call sites
    ensure_videos_tab_selected   28 call sites
    ensure_batch_view_mode       10 call sites

Guarding 114 callers is not a fix, it is a chore with a deadline. The guard goes
in the shared function, once. That is also the ponytail rule I broke: "grep every
caller of the function you're about to touch — one guard in the shared function
is a smaller diff than a guard in every caller."

WHY a per-call cap is the right ceiling and not a threshold tweak: nothing in the
worker ever calls `set_default_timeout`, so every locator operation inherits
Playwright's built-in **30s**. These three functions are PROBES — "is a cookie
banner showing", "is the Videos tab selected". Thirty seconds to answer "is this
element visible" is not a budget anyone chose; it is the default nobody set. A
helper making eight such probes can burn four minutes, and the v978 watchdog
kills the worker at 480s.
"""
import importlib.util
import os
import pathlib
import sys
import unittest

_STATIC = pathlib.Path(__file__).parent / "static"
_PATH = pathlib.Path(os.environ.get("FLOW_WORKER_PATH") or (_STATIC / "flow_worker.py"))


def _load():
    if str(_STATIC) not in sys.path:
        sys.path.insert(0, str(_STATIC))
    spec = importlib.util.spec_from_file_location("flow_worker_v1008", _PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakePage:
    """Records every default-timeout change, so the test can prove the window."""

    def __init__(self):
        self.timeouts = []

    def set_default_timeout(self, ms):
        self.timeouts.append(ms)


class TheDecoratorCapsAndRestores(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fw = _load()

    def test_it_lowers_the_per_call_default_while_the_helper_runs(self):
        page = FakePage()
        seen = {}

        @self.fw._bounded_page_helper(8)
        def probe(p):
            seen["during"] = list(p.timeouts)
            return "done"

        self.assertEqual("done", probe(page))
        self.assertTrue(seen["during"], "the cap must be applied BEFORE the body runs")
        self.assertLess(seen["during"][0], 30000, "the cap must be below the 30s default")

    def test_it_restores_the_default_afterwards(self):
        """Leaving a low default on the page would break real waits elsewhere."""
        page = FakePage()

        @self.fw._bounded_page_helper(8)
        def probe(p):
            return None

        probe(page)
        self.assertEqual(30000, page.timeouts[-1],
                         "the 30s default must be restored when the helper returns")

    def test_it_restores_the_default_even_when_the_helper_raises(self):
        page = FakePage()

        @self.fw._bounded_page_helper(8)
        def boom(p):
            raise RuntimeError("wedged")

        boom(page)
        self.assertEqual(30000, page.timeouts[-1])

    def test_a_raising_helper_does_not_take_the_worker_down(self):
        page = FakePage()

        @self.fw._bounded_page_helper(8)
        def boom(p):
            raise RuntimeError("wedged")

        self.assertIsNone(boom(page), "a probe that cannot answer returns None")

    def test_a_page_that_rejects_set_default_timeout_is_survivable(self):
        """Some call sites pass a Locator or a closed page. Never crash there."""
        class Awkward:
            def set_default_timeout(self, ms):
                raise RuntimeError("no such thing here")

        @self.fw._bounded_page_helper(8)
        def probe(p):
            return "ok"

        self.assertEqual("ok", probe(Awkward()))

    def test_it_records_the_activity_so_a_stall_names_the_helper(self):
        page = FakePage()

        @self.fw._bounded_page_helper(8)
        def my_probe(p):
            return None

        my_probe(page)
        self.assertIn("my_probe", self.fw._v978_where())


class TheThreeHelpersAreActuallyDecorated(unittest.TestCase):
    """A guard the hanging functions do not carry cannot save them."""

    @classmethod
    def setUpClass(cls):
        cls.src = _PATH.read_text(encoding="utf-8", errors="replace")

    def test_each_helper_carries_the_bound(self):
        for fn in ("check_and_dismiss_popup", "ensure_videos_tab_selected",
                   "ensure_batch_view_mode"):
            at = self.src.index(f"def {fn}(")
            above = self.src[max(0, at - 220):at]
            self.assertIn("_bounded_page_helper", above,
                          f"{fn} has no bound, and it has dozens of callers")


if __name__ == "__main__":
    unittest.main(verbosity=2)
