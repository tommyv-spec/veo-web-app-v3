"""v1006 — the refresh path must return, so the watchdog stops shooting the lane.

2026-09-16, measured from `~/.kaveno/flow_worker.log`: the flow worker died on its
own v978 watchdog **17 times** and the sweep restarted it **69 times**, while
`clips_owed` sat at 5 for ~4.5 hours. Net delivery was about zero clips an hour
and nothing published all day against a 9-video target.

15 of those 17 deaths have the same last main-thread line. Watched live:

    [DOWNLOAD] ═══ DEEP SCAN: 1 clip(s) missing: [10] ═══
    [DOWNLOAD] Deep scan attempt 1/3...
    [v978] NO MAIN-THREAD PROGRESS for 240s → 300s → 360s → 420s   (kill at 480s)

The very next statement after that print is `self._refresh_and_verify(project_url)`.

It is NOT one call hanging for ever. `page.reload(timeout=30000)` is bounded and
prints `Refresh error:` when it expires — no such line appears. Nothing calls
`set_default_timeout`, so Playwright's built-in **30s** applies to every locator
operation, and the three helpers the refresh runs afterwards
(`check_and_dismiss_popup`, `ensure_videos_tab_selected`, `ensure_batch_view_mode`)
make roughly nineteen of them between them. On a wedged page those **pile up**:
19 × 30s is far past the 480s kill. Each individual timeout is doing its job; the
aggregate has no ceiling at all.

So the fix is a ceiling, in two layers — a cheaper per-call default while the
refresh runs, and one wall-clock budget across the whole sequence. Neither is a
"raise the threshold" patch: the point is that a refresh which cannot finish must
give up and let the caller carry on, never hold the lane until it is shot.
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
    spec = importlib.util.spec_from_file_location("flow_worker_v1006", _PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TheSequenceHasACeiling(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fw = _load()

    def test_steps_after_the_budget_is_spent_are_skipped(self):
        ran = []
        steps = [
            ("slow", lambda: (time.sleep(0.5), ran.append("slow"))),
            ("next", lambda: ran.append("next")),
            ("last", lambda: ran.append("last")),
        ]
        skipped = self.fw._run_within_budget(steps, budget_s=0.2)
        self.assertEqual(["slow"], ran, "only the step that started should run")
        self.assertEqual(["next", "last"], skipped)

    def test_a_fast_sequence_runs_every_step(self):
        ran = []
        steps = [(n, (lambda n=n: ran.append(n))) for n in ("a", "b", "c")]
        skipped = self.fw._run_within_budget(steps, budget_s=5)
        self.assertEqual(["a", "b", "c"], ran)
        self.assertEqual([], skipped)

    def test_one_step_raising_does_not_stop_the_rest(self):
        """A popup helper that throws must not cost us the tab selection."""
        ran = []
        def boom():
            raise RuntimeError("wedged")
        steps = [("boom", boom), ("after", lambda: ran.append("after"))]
        skipped = self.fw._run_within_budget(steps, budget_s=5)
        self.assertEqual(["after"], ran)
        self.assertEqual([], skipped)

    def test_the_whole_sequence_returns_near_its_budget_not_far_past_it(self):
        """The property the watchdog cares about: bounded wall-clock."""
        steps = [(str(i), lambda: time.sleep(0.3)) for i in range(20)]
        t0 = time.time()
        self.fw._run_within_budget(steps, budget_s=1)
        self.assertLess(time.time() - t0, 2.0)


class TheRefreshPathUsesIt(unittest.TestCase):
    """A ceiling that the hanging function does not call cannot save it."""

    def test_refresh_and_verify_runs_its_helpers_under_the_budget(self):
        src = _PATH.read_text(encoding="utf-8", errors="replace")
        at = src.index("def _refresh_and_verify")
        body = src[at:at + 3000]
        self.assertIn("_run_within_budget", body,
                      "_refresh_and_verify must run its helpers under a budget")
        for helper in ("check_and_dismiss_popup", "ensure_videos_tab_selected",
                       "ensure_batch_view_mode"):
            self.assertIn(helper, body, f"{helper} vanished; re-point this test")

    def test_refresh_lowers_the_per_call_default_while_it_probes(self):
        """19 locator calls at Playwright's 30s default is the pile-up itself."""
        src = _PATH.read_text(encoding="utf-8", errors="replace")
        at = src.index("def _refresh_and_verify")
        body = src[at:at + 3000]
        self.assertIn("set_default_timeout", body,
                      "the refresh must cap each locator call, not just the sequence")

    def test_the_default_timeout_is_restored_afterwards(self):
        """Leaving a 5s default on the page would break real waits elsewhere."""
        src = _PATH.read_text(encoding="utf-8", errors="replace")
        at = src.index("def _refresh_and_verify")
        body = src[at:at + 3000]
        self.assertIn("finally:", body,
                      "restore the default timeout in a finally, or one wedged "
                      "refresh silently re-times every later call on this page")


if __name__ == "__main__":
    unittest.main(verbosity=2)
