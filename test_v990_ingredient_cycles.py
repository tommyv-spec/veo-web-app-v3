"""v990 — the ingredient attach must commit by OUTCOME, not by a flag the page never sets.

Why these exist. On 2026-09-13 every movie-section clip of job 34824da1 died at
the chip attach: the scene image went on, the first face image did not, and the
run refused to submit. Both run logs show the same shape —

    [v959 clip 2]-face1 ⚠ [v963.11] ms_face_1_0.png: no chip
                          (chips 2, in-box 2, commit clicked=True)

v963.14 was supposed to prevent exactly that. It skips the item click when
`_v962_asset_selected(item)` is True, because a freshly uploaded asset arrives
already selected and clicking it turns it back OFF. But on flow.google.com the
add-menu item carries neither `aria-selected` nor `aria-checked`, so the helper
returns None, None is not True, and every upload was clicked off anyway. The
proof is an absence: the skip line "already selected after upload — not clicking
it off" has ZERO hits in both logs.

So v990 stops asking and measures: for a FRESH upload, cycle A commits without
clicking; if no chip appears, cycle B clicks once and commits again. An EXISTING
project asset keeps the order that already works today (click, commit) and gets
one retry cycle. After any chip-count increase the newest chip's label is read
when the page exposes a real file name, so a wrong asset is caught rather than
rendered.

NOTE ON THE MODULE PATH. This loads `static/flow_worker.py` by default, which is
correct in any clean checkout. On the shared dev box that file also carries
another session's uncommitted rewrite of the same function (tagged v963.33), so
point `FLOW_WORKER_PATH` at the constructed blob to test what was actually
committed.
"""
import importlib.util
import os
import pathlib
import sys

import pytest

_STATIC = pathlib.Path(__file__).parent / "static"
_TARGET = pathlib.Path(os.environ.get("FLOW_WORKER_PATH")
                       or (_STATIC / "flow_worker.py"))


def _load():
    # flow_worker imports its siblings (browser_driver) by bare name, so the
    # folder holding it has to be importable.
    if str(_STATIC) not in sys.path:
        sys.path.insert(0, str(_STATIC))
    spec = importlib.util.spec_from_file_location("flow_worker_v990", _TARGET)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _Chip:
    """One ingredient chip. Its only identity is the label the page chooses."""

    def __init__(self, page, label):
        self.page = page
        self.label = label

    def get_attribute(self, attr):
        if attr == "aria-label":
            return self.label
        return None

    def locator(self, sel, **_kw):
        if "cancel" in sel:
            return _Btn(self.page, 1, on_click=self.page._remove_chip)
        return _Btn(self.page, 0)


class _Btn:
    """A generic control. `n` is how many the page has of it."""

    def __init__(self, page, n, on_click=None, attrs=None):
        self.page = page
        self._n = n
        self._on_click = on_click
        self._attrs = attrs or {}

    def count(self):
        return self._n

    @property
    def first(self):
        return self

    def nth(self, _i):
        return self

    def is_visible(self):
        return self._n > 0

    def is_enabled(self):
        return self._n > 0

    def wait_for(self, **_kw):
        if self._n == 0:
            raise RuntimeError("never became visible")

    def click(self, **_kw):
        if self._on_click:
            self._on_click()

    def get_attribute(self, attr):
        return self._attrs.get(attr)

    def locator(self, sel, **_kw):
        return _Btn(self.page, 0)


class _Chips:
    """The chip collection. Counting is what the real code trusts."""

    def __init__(self, page):
        self.page = page

    def count(self):
        return self.page.chips

    @property
    def first(self):
        return self.nth(0)

    def nth(self, _i):
        return _Chip(self.page, self.page.chip_label)


class _FileChooser:
    def __init__(self, page):
        self.page = page

    def __enter__(self):
        return self

    def __exit__(self, *_a):
        return False

    @property
    def value(self):
        return self

    def set_files(self, path):
        self.page.uploaded = path
        self.page.asset_present = True
        # MEASURED (v963.14's own comment): an upload lands SELECTED.
        self.page.selected = True


class _Page:
    """flow.google.com's Ingredients composer, as measured on 2026-09-13.

    The asset item exposes NO aria-selected / aria-checked — that absence is
    the whole bug. 'Add to prompt' only commits something when the item is
    selected; clicking the item toggles it.
    """

    def __init__(self, *, existing=False, aria=None, chip_label=None,
                 commit_works=True):
        self.chips = 0
        self.asset_present = existing
        self.existing = existing
        self.aria = aria or {}
        self.selected = False
        self.chip_label = chip_label
        self.commit_works = commit_works
        self.item_clicks = 0
        self.commit_clicks = 0
        self.removed = 0
        self.uploaded = None

    # -- behaviour ---------------------------------------------------------
    def _click_item(self):
        self.item_clicks += 1
        self.selected = not self.selected

    def _commit(self):
        self.commit_clicks += 1
        if self.commit_works and self.selected:
            # The real counter moves by two: _V962_ANY_CHIP matches two
            # elements per chip (rev 894).
            self.chips += 2

    def _remove_chip(self):
        self.removed += 1
        self.chips = max(0, self.chips - 2)

    # -- playwright surface ------------------------------------------------
    def locator(self, sel, **_kw):
        if "flow-image-ingredient-chip" in sel or "flow-ingredient-chip" in sel:
            return _Chips(self)
        if "Add ingredients to the prompt box" in sel:
            return _Btn(self, 1)
        if "flow-add-menu-asset-item" in sel:
            n = 1 if self.asset_present else 0
            return _Btn(self, n, on_click=self._click_item, attrs=self.aria)
        if "Upload media" in sel:
            return _Btn(self, 1)
        if "Add to prompt" in sel:
            return _Btn(self, 1, on_click=self._commit)
        if "cancel" in sel:
            # Page-level clear_existing sweep: nothing left over.
            return _Btn(self, 0)
        if "role='listbox'" in sel or "role='option'" in sel:
            return _Btn(self, 1)
        return _Btn(self, 0)

    def expect_file_chooser(self, **_kw):
        return _FileChooser(self)


class _Monitor:
    def __init__(self, _page):
        pass

    def start(self):
        pass

    def stop(self):
        pass

    def is_rejected(self):
        return False


@pytest.fixture
def fw(monkeypatch):
    mod = _load()
    monkeypatch.setattr(mod.time, "sleep", lambda *_a, **_k: None)
    monkeypatch.setattr(mod, "_v962_ensure_ingredients_mode",
                        lambda *_a, **_k: True)
    monkeypatch.setattr(mod, "FramePolicyMonitor", _Monitor)

    def _click(_page, loc, _label=None):
        loc.click()
        return True

    monkeypatch.setattr(mod, "human_click_locator", _click)
    return mod


def _png(tmp_path, name="ms_face_1_0.png"):
    p = tmp_path / name
    p.write_bytes(b"\x89PNG\r\n\x1a\n")
    return str(p)


def test_fresh_upload_attaches_on_cycle_a_without_clicking(fw, tmp_path, capsys):
    """(a) The case that broke: an upload, no aria flag, must NOT be clicked."""
    page = _Page(existing=False, aria={})
    ok, reason = fw._v962_attach_ingredient(page, _png(tmp_path), prefix="")

    assert (ok, reason) == (True, None)
    assert page.item_clicks == 0, "cycle A must not touch the item"
    assert page.uploaded is not None
    assert page.chips == 2
    out = capsys.readouterr().out
    assert "attached on cycle A" in out
    assert "selected_flag=None" in out, "the raw reading must be logged"


def test_existing_unselected_asset_attaches_after_one_click(fw, tmp_path):
    """(b) A project asset is genuinely unselected — today's order still wins."""
    page = _Page(existing=True, aria={})
    ok, reason = fw._v962_attach_ingredient(page, _png(tmp_path), prefix="")

    assert (ok, reason) == (True, None)
    assert page.item_clicks == 1, "existing assets keep the proven click-first order"
    assert page.uploaded is None, "it was already there — do not re-upload"
    assert page.chips == 2


def test_wrong_label_removes_the_chip_and_fails(fw, tmp_path, capsys):
    """(c) A chip naming a different file is taken back off, not rendered."""
    page = _Page(existing=False, aria={}, chip_label="ms_face_9_9.png")
    ok, reason = fw._v962_attach_ingredient(page, _png(tmp_path), prefix="")

    assert (ok, reason) == (False, 'wrong_asset')
    assert page.removed == 1, "the wrong chip must be removed"
    assert "wrong asset attached" in capsys.readouterr().out


def test_nothing_attaches_reports_no_buttons(fw, tmp_path, capsys):
    """(d) Both cycles tried, still no chip — say so, never claim success."""
    page = _Page(existing=False, aria={}, commit_works=False)
    ok, reason = fw._v962_attach_ingredient(page, _png(tmp_path), prefix="")

    assert (ok, reason) == (False, 'no_buttons')
    assert page.item_clicks == 1, "cycle B must have tried the click"
    assert "no chip after both cycles" in capsys.readouterr().out


def test_generic_chip_label_is_unverified_not_wrong(fw, tmp_path, capsys):
    """A label with no file name must never cost us a good chip."""
    page = _Page(existing=False, aria={}, chip_label="Remove ingredient")
    ok, reason = fw._v962_attach_ingredient(page, _png(tmp_path), prefix="")

    assert (ok, reason) == (True, None)
    assert page.removed == 0
    assert "identity unverified" in capsys.readouterr().out


def test_matching_chip_label_passes(fw, tmp_path):
    """The happy identity case: the label names our file."""
    page = _Page(existing=False, aria={}, chip_label="ms_face_1_0.png")
    ok, reason = fw._v962_attach_ingredient(page, _png(tmp_path), prefix="")

    assert (ok, reason) == (True, None)
    assert page.removed == 0
