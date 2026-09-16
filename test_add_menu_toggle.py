"""v1013 — do not re-click a menu that is already open; the click closes it.

This is the frame-attach failure that actually stops renders. Measured
2026-09-16 across the whole day's log:

    start frame attached OK .... 52
    frame attach glitched ...... 15      (78% success)
    Add-media upload failed .... 26

A clip whose start frame will not attach is queued for redo with the SAME
image, so it fails the same way for ever — three of noemi's five clips and
three of nuri's four remaining were sitting in exactly that loop.

WHAT THE LOG SHOWS, in order:

    Clicked: Clip 4 start frame slot
    [v988] searched the picker for 'image_03' - still not listed
    [v962.8-diag] picker pane contents: {"titles": [], "alts": [], "total": 44,
        "shapes": {"flow-add-menu-popover-content": 1,
                   "div.add-menu-popover-container.flow-menu-panel": 1, ...}}
    [v997] file-drop routes skipped on flow.google.com (0 attaches in 9 runs)
    [v986] opening the add menu via button[aria-label='Add media menu']
    [v974] Add-media upload of image_03.png failed (attempt 1):
           Locator.click: Timeout 8000ms exceeded
    [v997] page state at the failure: {"pane":0,"popover":0,"dialog":0,...}

The "picker pane" it searched is the **Add-media popover**, already open: the
shapes are `flow-add-menu-popover-content` and `add-menu-popover-container`,
44 elements with no image tiles in them. Then v986 clicks the trigger to open a
menu that is already open, which TOGGLES IT SHUT, and the `upload` menu item
inside the same `expect_file_chooser` block then times out at exactly the 8000ms
it is given. The state captured after the failure agrees: `popover: 0`.

So the fix is not a longer timeout or another retry - both would re-close the
menu just as reliably. It is to look before clicking.
"""
import importlib.util
import os
import pathlib
import re
import sys
import unittest

_STATIC = pathlib.Path(__file__).parent / "static"
_PATH = pathlib.Path(os.environ.get("FLOW_WORKER_PATH") or (_STATIC / "flow_worker.py"))


def _load():
    if str(_STATIC) not in sys.path:
        sys.path.insert(0, str(_STATIC))
    spec = importlib.util.spec_from_file_location("flow_worker_v1013", _PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeLocator:
    def __init__(self, n, raises=False):
        self._n, self._raises = n, raises

    def count(self):
        if self._raises:
            raise RuntimeError("context destroyed")
        return self._n


class FakePage:
    """Records what was asked for, so the test can prove the selector is real."""

    def __init__(self, n=0, raises=False):
        self.n, self.raises, self.asked = n, raises, []

    def locator(self, sel):
        self.asked.append(sel)
        return FakeLocator(self.n, self.raises)


class SeeingAnOpenMenu(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fw = _load()

    def test_an_open_popover_is_detected(self):
        self.assertTrue(self.fw._v1013_menu_already_open(FakePage(n=1)))

    def test_no_popover_means_the_trigger_must_be_clicked(self):
        self.assertFalse(self.fw._v1013_menu_already_open(FakePage(n=0)))

    def test_it_looks_for_flows_actual_popover(self):
        """The selector must name what the diagnostic saw, or it never matches."""
        page = FakePage(n=1)
        self.fw._v1013_menu_already_open(page)
        asked = " ".join(page.asked)
        self.assertIn("add-menu-popover", asked)

    def test_a_dead_page_is_treated_as_closed(self):
        """Fail toward the OLD behaviour: if we cannot look, click as before.
        Guessing "already open" on a dead page would skip the only click that
        can open it and guarantee the upload fails."""
        self.assertFalse(self.fw._v1013_menu_already_open(FakePage(raises=True)))
        self.assertFalse(self.fw._v1013_menu_already_open(None))


class TheUploadPathChecksBeforeClicking(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.src = _PATH.read_text(encoding="utf-8", errors="replace")

    def _v986_block(self):
        at = self.src.index("[v986] opening the add menu via")
        return self.src[at:at + 1400]

    def test_the_trigger_click_is_guarded(self):
        self.assertIn("_v1013_menu_already_open", self._v986_block(),
                      "the trigger click must be skipped when the menu is open")

    def test_the_upload_item_is_still_clicked_either_way(self):
        """Skipping the trigger must not skip the actual upload."""
        block = self._v986_block()
        self.assertIn('has_text="upload"', block)

    def test_no_timeout_was_simply_raised_instead(self):
        """A longer timeout re-closes the menu just as reliably; the bug is the
        click, not the budget."""
        block = self._v986_block()
        for bigger in ("timeout=15000", "timeout=20000)", "timeout=30000"):
            self.assertNotIn(f".click({bigger}", block)


if __name__ == "__main__":
    unittest.main(verbosity=2)
