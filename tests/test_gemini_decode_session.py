from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "static" / "gemini_decode_worker.py"


def _load_worker():
    # Session detection is pure. Stub the browser worker dependency so this test
    # does not import Playwright/Camoufox or touch a real profile.
    previous = sys.modules.get("gemini_video_worker")
    sys.modules["gemini_video_worker"] = types.ModuleType("gemini_video_worker")
    try:
        spec = importlib.util.spec_from_file_location("gemini_decode_worker_session_test", MODULE_PATH)
        module = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(module)
        return module
    finally:
        if previous is None:
            sys.modules.pop("gemini_video_worker", None)
        else:
            sys.modules["gemini_video_worker"] = previous


worker = _load_worker()


class _Locator:
    def __init__(self, *, count=0, visible=None, label="", count_error=False):
        self._count = count
        self._visible = list(visible or [])
        self._label = label
        self._count_error = count_error

    @property
    def first(self):
        return self

    def count(self):
        if self._count_error:
            raise RuntimeError("DOM changed during negative scan")
        return self._count

    def nth(self, index):
        return _Locator(count=1, visible=[self._visible[index]])

    def is_visible(self):
        return bool(self._visible[0]) if self._visible else False

    def get_attribute(self, _name):
        return self._label

    def inner_text(self):
        return ""


class _Page:
    def __init__(self, negative, model):
        self.negative = negative
        self.model = model

    def locator(self, selector):
        if "Sign in" in selector:
            return self.negative
        return self.model


class GeminiSessionTests(unittest.TestCase):
    def test_positive_model_pill_and_confirmed_no_signin_is_live(self):
        page = _Page(_Locator(count=0), _Locator(count=1, label="3.1 Pro"))
        self.assertTrue(worker.signed_in(page))

    def test_visible_signin_always_wins(self):
        page = _Page(
            _Locator(count=2, visible=[False, True]),
            _Locator(count=1, label="3.1 Pro"),
        )
        self.assertFalse(worker.signed_in(page))

    def test_failed_negative_scan_fails_closed(self):
        page = _Page(
            _Locator(count_error=True),
            _Locator(count=1, label="3.1 Pro"),
        )
        self.assertFalse(worker.signed_in(page))

    def test_flash_lite_is_not_a_live_account_session(self):
        page = _Page(_Locator(count=0), _Locator(count=1, label="Flash-Lite"))
        self.assertFalse(worker.signed_in(page))


if __name__ == "__main__":
    unittest.main()
