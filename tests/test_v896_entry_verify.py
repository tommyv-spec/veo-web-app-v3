# v896 — the Flow marketing-page entry click must be VERIFIED (labs
# session-token minted) instead of trusted. Background: the 2026-08 marketing
# redesign let the blind coordinate click land on the '#models' nav anchor
# while the log said clicked, so Account2 looped logged-out (2026-08-05).
# A live golden-2 probe proved the mechanism: goldens carry NO labs session
# cookie; one correct hero click makes Google SSO silently re-mint
# __Secure-next-auth.session-token and the app loads.
#
# The helper's control flow is testable with a fake page (no browser).

import os
import re
import sys
import types
import unittest

WORKER = os.path.join(os.path.dirname(__file__), "..", "static", "flow_worker.py")


def _source():
    with open(WORKER, encoding="utf-8") as f:
        return f.read()


def _load_helper():
    """Extract and exec just the _flow_entry_login_click function so the test
    avoids importing the whole worker (module import has runtime side effects).
    human_click_element/time are injected fakes."""
    src = _source()
    m = re.search(
        r"^def _flow_entry_login_click\(.*?(?=^def )", src, re.S | re.M
    )
    assert m, "_flow_entry_login_click not found in flow_worker.py"
    ns = {
        "time": types.SimpleNamespace(sleep=lambda *_: None),
        "human_click_element": lambda *a, **k: True,
        "print": lambda *a, **k: None,
    }
    exec(m.group(0), ns)
    return ns["_flow_entry_login_click"]


class _FakeLocator:
    def __init__(self, visible=False):
        self._visible = visible
        self.first = self

    def is_visible(self, timeout=None):
        return self._visible

    def click(self, timeout=None):
        pass


class _FakePage:
    """Page whose context mints the session token after N cookie polls."""

    def __init__(self, mint_after_polls):
        self._polls = 0
        self._mint_after = mint_after_polls
        self.url = "https://labs.google/fx/tools/flow#models"
        self.context = self

    def cookies(self, _url):
        self._polls += 1
        if self._mint_after is not None and self._polls > self._mint_after:
            return [{"name": "__Secure-next-auth.session-token"}]
        return []

    def locator(self, _sel):
        return _FakeLocator(visible=True)

    def get_by_role(self, _role, name=None):
        return _FakeLocator(visible=True)


class TestV896EntryVerify(unittest.TestCase):
    def test_already_minted_returns_true_without_clicking(self):
        fn = _load_helper()
        self.assertTrue(fn(_FakePage(mint_after_polls=0)))

    def test_mint_after_click_returns_true(self):
        fn = _load_helper()
        # token appears on a later poll (post-OAuth) -> verified success
        self.assertTrue(fn(_FakePage(mint_after_polls=3)))

    def test_never_minted_returns_false(self):
        fn = _load_helper()
        self.assertFalse(fn(_FakePage(mint_after_polls=None)))

    def test_wired_into_login_loop_and_v836_recovery(self):
        src = _source()
        self.assertIn("clicked = _flow_entry_login_click(page, label)", src)
        self.assertIn('_flow_entry_login_click(page, label="v836-recovery")', src)
        # fallback selector list must be skipped after a verified entry
        self.assertIn("for sel in ([] if clicked else entry_selectors):", src)


if __name__ == "__main__":
    unittest.main()
