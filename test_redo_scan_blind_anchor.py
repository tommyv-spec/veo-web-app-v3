"""v1005 — a blind tile scan must not spend the download path's budget.

`[data-index]` does not exist on flow.google.com. Every
`document.querySelector("div[data-index='0']")` returns null there — on a
healthy render and a dead one alike (`document.querySelectorAll('[data-index]')
.length === 0`, v963.19). v981, v983 and v984 were that one fact producing three
separate destructive bugs; this is the fourth place it reaches.

The REDO scan read that null as "not ready yet" and slept 15s, twenty times.
That is 300s — exactly the cap the download tab gets — so the path that ACTUALLY
delivers started already over budget. Job 1984be6c, 2026-09-16:

    [REDO] Scan 20/20: data-index=0 not found, waiting 15s...
    [REDO] ⚠️ HTTP scan failed — falling back to download tab
    [DOWNLOAD] ⚠️ Max poll time exceeded (417s since last submit, cap=300s)

Lowering `_max_scan_attempts` is NOT the fix and the memory says why: "raising
the threshold never helps — the reading is zero at every threshold." The reading
has to be told apart from a slow one instead.

A missing ANCHOR ATTRIBUTE means the instrument is blind: it reads the same at
attempt 1 and attempt 20, so the scan must stop and let the working path run. A
missing INDEX while the attribute is present is a slow render, and a scan that
timed out is transient — both of those keep waiting, unchanged.
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
    spec = importlib.util.spec_from_file_location("flow_worker_v1005", _PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class BlindAnchorPredicate(unittest.TestCase):
    """Blind vs merely-slow, which the old `{"exists": False}` could not express."""

    @classmethod
    def setUpClass(cls):
        cls.fw = _load()

    def test_no_data_index_anywhere_is_blind(self):
        """The real flow.google.com reading. Retrying it cannot change it."""
        self.assertTrue(self.fw._redo_scan_blind(
            {"exists": False, "anchorAttrPresent": False}))

    def test_attribute_present_but_index_missing_is_a_slow_render(self):
        """A host that DOES emit [data-index]: tile 0 may just not be drawn yet."""
        self.assertFalse(self.fw._redo_scan_blind(
            {"exists": False, "anchorAttrPresent": True}))

    def test_timeout_shape_keeps_waiting(self):
        """_redo_tile_info's except path returns bare {"exists": False}. A scan
        that never ran says nothing about the page, so it must NOT read as blind."""
        self.assertFalse(self.fw._redo_scan_blind({"exists": False}))
        self.assertFalse(self.fw._redo_scan_blind(None))

    def test_a_tile_that_exists_is_never_blind(self):
        self.assertFalse(self.fw._redo_scan_blind(
            {"exists": True, "anchorAttrPresent": True}))


class PredicateIsWiredToRealData(unittest.TestCase):
    """A predicate reading a field nothing emits is a warning outside its own
    path: always False, never fires, and the 300s burn continues silently."""

    @classmethod
    def setUpClass(cls):
        cls.fw = _load()

    def test_scan_js_emits_the_field_the_predicate_reads(self):
        js = self.fw._REDO_TILE_SCAN_JS
        self.assertIn("anchorAttrPresent", js,
                      "the browser-side scan must report whether [data-index] "
                      "exists at all, or _redo_scan_blind can never be True")

    def test_scan_js_reports_it_on_the_not_found_return(self):
        """The `if (!c) return ...` object is the ONLY return the predicate sees.

        Checks the whole returned object, not one line — the return may wrap.
        """
        js = self.fw._REDO_TILE_SCAN_JS
        at = js.find("exists: false")
        self.assertNotEqual(at, -1, "expected a not-found return in the scan JS")
        returned = js[at:js.find("}", at)]
        self.assertIn("anchorAttrPresent", returned,
                      f"not-found return carries no anchor reading: {returned.strip()!r}")

    def test_blind_scan_breaks_the_loop_instead_of_sleeping(self):
        """The whole point: on blind, stop — do not fall through to time.sleep(15)."""
        src = _PATH.read_text(encoding="utf-8", errors="replace")
        marker = "data-index=0 not found, waiting 15s"
        at = src.find(marker)
        self.assertNotEqual(at, -1, "the scan-retry print vanished; re-point this test")
        window = src[max(0, at - 700):at]
        self.assertIn("_redo_scan_blind", window,
                      "the retry branch must consult _redo_scan_blind BEFORE sleeping")


if __name__ == "__main__":
    unittest.main(verbosity=2)
