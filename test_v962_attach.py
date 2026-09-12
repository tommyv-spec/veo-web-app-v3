"""The Frames attach path on flow.google.com must not report success it did not have.

Why these exist. On 2026-09-12 a scoped run attached the start frame on 1 of 4
clips. The other three logged `picked, but no chip appeared in the bar`, which the
caller classifies as a transient `start_glitch` and retries with the SAME image.
The retry could never work: the page has no `input[type=file]` anywhere
(measured document-wide), and the one drop target the code believes in,
`flow-composer`, matches nothing -- it appears exactly once in the whole file and
was never validated. `_v962_upload_into_picker` nevertheless returned True after
every route had failed, so a total failure travelled upward as a delivered file.

Full evidence: docs/flow-frame-attach-root-cause-2026-09-12.md
"""
import importlib.util
import pathlib
import sys

_STATIC = pathlib.Path(__file__).parent / "static"

_SPEC = importlib.util.spec_from_file_location(
    "flow_worker_attach", _STATIC / "flow_worker.py",
)


def _load():
    # flow_worker imports its siblings (browser_driver) by bare name, so the
    # folder holding it has to be importable. Without this the module dies at
    # `static/flow_worker.py:626` with ModuleNotFoundError and every test in the
    # file fails for a reason that has nothing to do with what it asserts --
    # which is the state `test_is_omni.py` is in today (verified 2026-09-12,
    # both of its tests fail this way before any change of mine).
    if str(_STATIC) not in sys.path:
        sys.path.insert(0, str(_STATIC))
    mod = importlib.util.module_from_spec(_SPEC)
    _SPEC.loader.exec_module(mod)
    return mod


class _Loc:
    """Empty by default: no file input, and no chip in the frames bar."""

    def __init__(self, n=0):
        self._n = n

    def count(self):
        return self._n

    @property
    def first(self):
        return self

    def set_input_files(self, *_a, **_k):
        raise AssertionError("route 1 must not run when there is no file input")


class _Page:
    """flow.google.com as MEASURED on 2026-09-12: no input[type=file] anywhere,
    and a drop that lands changes nothing.

    `present` is the set of selectors that actually exist on the page; anything
    else makes document.querySelector return null, which the real code reads as
    the string 'missing'.
    """

    def __init__(self, present=()):
        self.present = set(present)
        self.dropped = []

    def locator(self, _sel):
        return _Loc(0)

    def evaluate(self, _js, args=None):
        _blob, _name, sel = args
        if sel not in self.present:
            return "missing"
        self.dropped.append(sel)
        return "ok"


def _png(tmp_path):
    img = tmp_path / "image_01.png"
    img.write_bytes(b"\x89PNG\r\n\x1a\n")
    return str(img)


def test_reports_failure_when_no_target_exists(tmp_path, monkeypatch):
    """Nothing on the page at all -- there was no route, so say so."""
    fw = _load()
    monkeypatch.setattr(fw.time, "sleep", lambda *_a, **_k: None)
    page = _Page(present=())
    assert fw._v962_upload_into_picker(page, _png(tmp_path), "") is False


def test_reports_failure_when_drops_land_but_nothing_attaches(tmp_path, monkeypatch):
    """The targets exist and the drops are dispatched, but no chip ever appears.

    This is the exact shape of the 2026-09-12 run: four targets reached, zero
    effect. Dispatching an event is not delivering a file.
    """
    fw = _load()
    monkeypatch.setattr(fw.time, "sleep", lambda *_a, **_k: None)
    page = _Page(present=("flow-ingredient-bar", "main", "body"))
    assert fw._v962_upload_into_picker(page, _png(tmp_path), "") is False
    assert page.dropped, "the drops must really have been attempted"


def test_still_reports_success_when_a_drop_attaches_a_chip(tmp_path, monkeypatch):
    """The success path must not regress: a chip appearing is still True."""
    fw = _load()
    monkeypatch.setattr(fw.time, "sleep", lambda *_a, **_k: None)

    class _ChipPage(_Page):
        def locator(self, sel):
            return _Loc(1) if sel == fw._V962_FRAME_CHIP else _Loc(0)

    page = _ChipPage(present=("flow-ingredient-bar",))
    assert fw._v962_upload_into_picker(page, _png(tmp_path), "") is True


def test_missing_file_still_reports_failure(tmp_path, monkeypatch):
    """An absent image was already a False and must stay one."""
    fw = _load()
    monkeypatch.setattr(fw.time, "sleep", lambda *_a, **_k: None)
    page = _Page(present=("flow-ingredient-bar",))
    assert fw._v962_upload_into_picker(page, str(tmp_path / "nope.png"), "") is False


# --- v974: the Add-media-menu route, the only one that works on this host ---

class _Chooser:
    def __init__(self):
        self.files = None

    def set_files(self, path):
        self.files = path


class _ChooserCtx:
    """Stands in for page.expect_file_chooser()."""

    def __init__(self, chooser, raise_on_enter=False):
        self._chooser = chooser
        self._raise = raise_on_enter

    def __enter__(self):
        if self._raise:
            raise TimeoutError("no file chooser appeared")
        return self

    def __exit__(self, *a):
        return False

    @property
    def value(self):
        return self._chooser


class _Keyboard:
    def press(self, _key):
        return None


class _MenuLoc:
    def __init__(self, page, fail_click=False):
        self._page = page
        self._fail = fail_click

    @property
    def first(self):
        return self

    def filter(self, **_k):
        return self

    def click(self, **_k):
        if self._fail:
            raise RuntimeError("control not present")
        self._page.clicks += 1


class _MenuPage:
    """A page where Add media menu -> Upload raises a native file chooser."""

    def __init__(self, chooser_works=True, slot_works=True):
        self.keyboard = _Keyboard()
        self.chooser = _Chooser()
        self.clicks = 0
        self._chooser_works = chooser_works
        self._slot_works = slot_works

    def expect_file_chooser(self, **_k):
        return _ChooserCtx(self.chooser, raise_on_enter=not self._chooser_works)

    def locator(self, sel):
        # the frame slot is the last thing clicked, to re-open the picker
        if "ingredient-bar" in sel or "Start" in sel or "End" in sel:
            return _MenuLoc(self, fail_click=not self._slot_works)
        return _MenuLoc(self)


def test_add_media_menu_route_hands_the_file_over(tmp_path, monkeypatch):
    fw = _load()
    monkeypatch.setattr(fw.time, "sleep", lambda *_a, **_k: None)
    img = _png(tmp_path)
    page = _MenuPage()
    assert fw._v962_upload_via_add_media_menu(page, img, "start", "") is True
    assert page.chooser.files == img, "the chooser must actually receive the file"


def test_add_media_menu_route_fails_when_no_chooser_appears(tmp_path, monkeypatch):
    """No chooser means no upload — it must not report success."""
    fw = _load()
    monkeypatch.setattr(fw.time, "sleep", lambda *_a, **_k: None)
    page = _MenuPage(chooser_works=False)
    assert fw._v962_upload_via_add_media_menu(page, _png(tmp_path), "start", "") is False


def test_add_media_menu_route_fails_if_the_picker_cannot_reopen(tmp_path, monkeypatch):
    """The caller searches the picker next, so a picker that never re-opens is a
    failure even though the file was handed over."""
    fw = _load()
    monkeypatch.setattr(fw.time, "sleep", lambda *_a, **_k: None)
    page = _MenuPage(slot_works=False)
    assert fw._v962_upload_via_add_media_menu(page, _png(tmp_path), "start", "") is False


def test_upload_into_picker_tries_the_measured_route_first(tmp_path, monkeypatch):
    """The dead routes must not run when the live one succeeds."""
    fw = _load()
    monkeypatch.setattr(fw.time, "sleep", lambda *_a, **_k: None)
    called = {"n": 0}

    def _fake(page, path, which="start", prefix=""):
        called["n"] += 1
        return True

    monkeypatch.setattr(fw, "_v962_upload_via_add_media_menu", _fake)
    page = _Page(present=("flow-ingredient-bar",))
    assert fw._v962_upload_into_picker(page, _png(tmp_path), "") is True
    assert called["n"] == 1
    assert not page.dropped, "no drop should be attempted once the real route worked"
