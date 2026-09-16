"""v1009 — the popup sweep must give up, because it is what kills the lane.

2026-09-16, death #20, and the first one whose cause the log states outright
(v1007's breadcrumb doing its job):

    [v978] STALLED — no main-thread progress for 480s. Releasing the job and exiting.
    [v978] STUCK IN: check_and_dismiss_popup (started 476s ago)

`check_and_dismiss_popup` is 155 lines that probe for about a dozen dialogs that
are almost never present — 40 Playwright calls: 16 `.count()`, 13 `.is_visible()`,
11 `.click()`, across 10 bare `try/except: pass` blocks.

v1008 put a 5s per-call cap on it (`_bounded_page_helper`). That was not enough and
could not be: **`locator.count()` does not auto-wait and takes no timeout**, so
`set_default_timeout` does not govern it at all. Sixteen calls that the cap cannot
reach, on a slow page, is minutes — and the decorator's `finally` never runs,
which is why `[bounded]` printed nothing while the function sat there for 476s.

So the ceiling has to be INSIDE the sweep: a deadline it checks between blocks and
then simply stops. That is safe precisely because the function is best-effort — it
already swallows every error it meets and returns False when it finds nothing. A
popup left undismissed costs one retry; a sweep that never returns costs the job,
the lane, and eight minutes.
"""
import importlib.util
import os
import pathlib
import re
import sys
import time
import unittest

_STATIC = pathlib.Path(__file__).parent / "static"
_PATH = pathlib.Path(os.environ.get("FLOW_WORKER_PATH") or (_STATIC / "flow_worker.py"))


def _load():
    if str(_STATIC) not in sys.path:
        sys.path.insert(0, str(_STATIC))
    spec = importlib.util.spec_from_file_location("flow_worker_v1009", _PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _popup_body(src: str) -> str:
    start = src.index("def check_and_dismiss_popup(page):")
    nxt = re.search(r"\n(?:@\w|def )", src[start + 10:])
    return src[start:start + 10 + nxt.start()] if nxt else src[start:start + 12000]


class TheDeadlinePredicate(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fw = _load()

    def test_time_up_is_false_before_the_deadline(self):
        self.assertFalse(self.fw._popup_time_up(time.time() + 30))

    def test_time_up_is_true_after_the_deadline(self):
        self.assertTrue(self.fw._popup_time_up(time.time() - 1))

    def test_it_never_raises_on_nonsense(self):
        """This runs on the path that is already failing; it may not add a fault."""
        for bad in (None, "", "later", object()):
            self.assertIsInstance(self.fw._popup_time_up(bad), bool)

    def test_the_budget_is_well_under_the_watchdog_kill(self):
        """A budget at or above 480s cannot save anything — that IS the kill."""
        self.assertLess(self.fw.POPUP_SWEEP_BUDGET_S, 120)
        self.assertGreater(self.fw.POPUP_SWEEP_BUDGET_S, 2)


class TheSweepActuallyChecksIt(unittest.TestCase):
    """A deadline the loop never reads is a constant, not a ceiling."""

    @classmethod
    def setUpClass(cls):
        cls.body = _popup_body(_PATH.read_text(encoding="utf-8", errors="replace"))

    def test_the_sweep_arms_a_deadline(self):
        self.assertIn("POPUP_SWEEP_BUDGET_S", self.body,
                      "the sweep must arm its own deadline")

    def test_it_checks_the_deadline_between_its_blocks(self):
        """One check at the top would only ever catch an already-late caller."""
        hits = len(re.findall(r"_popup_time_up\(", self.body))
        blocks = len(re.findall(r"\n        try:", self.body))
        self.assertGreaterEqual(
            hits, max(4, blocks - 2),
            f"only {hits} deadline check(s) for {blocks} probe blocks — the sweep "
            f"can still run away between them")

    def test_giving_up_returns_rather_than_raising(self):
        """Its callers treat it as best-effort; ten bare `except: pass` blocks
        inside would swallow a raise and carry on probing anyway."""
        for m in re.finditer(r"_popup_time_up\([^)]*\):\s*\n(\s*)(\S+)", self.body):
            self.assertTrue(m.group(2).startswith("return"),
                            f"deadline check does not return, it does: {m.group(2)!r}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
