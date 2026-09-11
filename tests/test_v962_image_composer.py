"""v962.7 — the image worker must drive the NEW composer on flow.google.com.

Why a test. The v962 port into image_worker.py stopped halfway: host detection,
the settings overlay, the radios and the model picker were ported; the composer
after them was not. So on flow.google.com the worker set Image/aspect/variants/
model correctly and then drove the RETIRED DOM for everything else.
fill_prompt_textarea looked for div[role="textbox"], which the Angular Material
app does not render, printed "Prompt textbox not found on page", returned False,
and the caller reported:

    IMAGE_GEN_FAIL: node 5395: Submit failed: Failed to fill prompt

These tests pin the branch itself — on the new host the legacy selectors must
never be touched, and on the legacy host the new ones must never be touched.
A test that only checked "it returns True" would pass against either DOM.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import image_worker


class FakeLocator:
    def __init__(self, page, selector):
        self.page = page
        self.selector = selector

    @property
    def first(self):
        return self

    # -- the calls _v962_type_prompt makes --
    def wait_for(self, **kw):
        return None

    def click(self, **kw):
        return None

    def inner_text(self, **kw):
        return self.page.editor_text

    # -- the legacy Slate path walks further than the new-host path --
    def scroll_into_view_if_needed(self, **kw):
        return None

    def focus(self, **kw):
        return None

    def bounding_box(self):
        return None  # skips the humanised mouse move

    # -- the calls _v962_generate_enabled makes --
    def count(self):
        return 1

    def is_visible(self, **kw):
        return True

    def is_disabled(self):
        return self.page.generate_disabled

    def get_attribute(self, name, **kw):
        if name == "aria-disabled":
            return "true" if self.page.generate_disabled else "false"
        return None

    def is_enabled(self):
        return not self.page.generate_disabled


class FakeKeyboard:
    def __init__(self, page):
        self.page = page

    def press(self, _keys):
        return None

    def insert_text(self, text):
        self.page.editor_text = text


class FakePage:
    """Records every selector the code under test asks for."""

    def __init__(self, url, generate_disabled=False):
        self.url = url
        self.selectors = []
        self.editor_text = ""
        self.generate_disabled = generate_disabled
        self.keyboard = FakeKeyboard(self)

    def locator(self, selector, **kw):
        self.selectors.append(selector)
        return FakeLocator(self, selector)

    def evaluate(self, *a, **kw):
        return True


NEW_HOST = "https://flow.google.com/project/b226b464"
OLD_HOST = "https://labs.google/fx/tools/flow/project/b226b464"

LEGACY_PROMPT_SELECTOR = 'div[role="textbox"]'
LEGACY_GENERATE_FRAGMENT = "arrow_forward"


# --------------------------------------------------------------------------
# the prompt editor — this is the node 5395 failure
# --------------------------------------------------------------------------

def test_prompt_uses_the_material_editor_on_the_new_host():
    page = FakePage(NEW_HOST)
    assert image_worker.fill_prompt_textarea(page, "a test prompt that is long enough") is True
    assert image_worker._V962_PROMPT_EDITOR in page.selectors
    # the whole bug: the legacy selector must not be consulted at all
    assert LEGACY_PROMPT_SELECTOR not in page.selectors


def test_prompt_still_uses_the_slate_textbox_on_the_legacy_host():
    page = FakePage(OLD_HOST)
    image_worker.fill_prompt_textarea(page, "a test prompt")
    assert LEGACY_PROMPT_SELECTOR in page.selectors
    assert image_worker._V962_PROMPT_EDITOR not in page.selectors


def test_prompt_reports_failure_when_the_text_does_not_land():
    """A short read-back must fail, not be rounded up to success."""
    page = FakePage(NEW_HOST)

    class Silent(FakeLocator):
        def inner_text(self, **kw):
            return ""

    page.locator = lambda sel, **kw: (page.selectors.append(sel), Silent(page, sel))[1]
    assert image_worker.fill_prompt_textarea(page, "x" * 200) is False


# --------------------------------------------------------------------------
# the Generate button
# --------------------------------------------------------------------------

def test_generate_enabled_reads_the_aria_label_button_on_the_new_host():
    page = FakePage(NEW_HOST, generate_disabled=False)
    assert image_worker._is_generate_enabled(page) is True
    assert image_worker._V962_GENERATE_BTN in page.selectors
    assert not any(LEGACY_GENERATE_FRAGMENT in s for s in page.selectors)


def test_generate_disabled_is_reported_as_disabled_on_the_new_host():
    page = FakePage(NEW_HOST, generate_disabled=True)
    assert image_worker._is_generate_enabled(page) is False


def test_generate_enabled_uses_the_icon_button_on_the_legacy_host():
    page = FakePage(OLD_HOST, generate_disabled=False)
    image_worker._is_generate_enabled(page)
    assert any(LEGACY_GENERATE_FRAGMENT in s for s in page.selectors)
    assert image_worker._V962_GENERATE_BTN not in page.selectors


def test_click_generate_keeps_its_retry_structure_on_the_new_host():
    """The new host changes the LOCATOR only.

    Codex, reviewing the plan: an early-return branch here would discard the
    3 retries, the popup dismissal, the 60s readiness wait and the final
    raise. Pin that a disabled button still raises rather than returning.
    """
    page = FakePage(NEW_HOST, generate_disabled=True)
    calls = {"popup": 0}
    orig_popup = image_worker.check_and_dismiss_popup
    orig_sleep = image_worker.time.sleep

    def fake_popup(_page):
        calls["popup"] += 1
        return False

    image_worker.check_and_dismiss_popup = fake_popup
    image_worker.time.sleep = lambda *_a, **_k: None
    try:
        raised = False
        try:
            image_worker.click_generate_image(page, context="t", max_retries=2)
        except Exception:
            raised = True
        assert raised, "a permanently disabled Generate button must raise, not return"
        assert image_worker._V962_GENERATE_BTN in page.selectors
        assert calls["popup"] > 0, "the popup dismissal must survive the new-host branch"
    finally:
        image_worker.check_and_dismiss_popup = orig_popup
        image_worker.time.sleep = orig_sleep


# --------------------------------------------------------------------------
# the reference attach — the two defects Codex caught in the plan
# --------------------------------------------------------------------------

def _attach_page(tmp_files):
    page = FakePage(NEW_HOST)
    page.attach_calls = []
    return page


def test_first_reference_clears_and_later_ones_do_not(tmp_path, monkeypatch):
    """clear_existing=True on every call deletes each reference as it attaches
    the next, leaving only the last. The Flow worker's own caller documents
    this (flow_worker.py ~L25029: a True there 'would delete the scene chip')."""
    paths = []
    for n in ("a.png", "b.png", "c.png"):
        p = tmp_path / n
        p.write_bytes(b"x")
        paths.append(str(p))
    page = FakePage(NEW_HOST)
    seen = []

    def fake_attach(_page, image_path, prefix="", clear_existing=True):
        seen.append((os.path.basename(image_path), clear_existing))
        return (True, None)

    monkeypatch.setattr(image_worker, "_v962_attach_reference", fake_attach)
    assert image_worker.upload_reference_images(page, paths, context="t") is True
    assert seen == [("a.png", True), ("b.png", False), ("c.png", False)]


def test_a_failed_attach_returns_literal_false_not_a_truthy_tuple(tmp_path, monkeypatch):
    """`(False, 'reason')` is a non-empty tuple and therefore TRUTHY.

    Both callers use `if not upload_reference_images(...)`, so returning the
    tuple through would make a failed attach read as success and generate an
    image with no references. This is the exact trap Codex caught in the plan.
    """
    paths = []
    for n in ("a.png", "b.png"):
        p = tmp_path / n
        p.write_bytes(b"x")
        paths.append(str(p))
    page = FakePage(NEW_HOST)
    attempted = []

    def fake_attach(_page, image_path, prefix="", clear_existing=True):
        attempted.append(os.path.basename(image_path))
        return (False, "attachment_unverified")

    monkeypatch.setattr(image_worker, "_v962_attach_reference", fake_attach)
    result = image_worker.upload_reference_images(page, paths, context="t")
    assert result is False, "a failed attach must return literal False"
    assert not result, "and must be falsy for `if not ...` callers"
    assert attempted == ["a.png"], "a failed attach must stop the remaining inputs"


def test_attach_refuses_when_the_composer_count_is_unreadable(tmp_path):
    """None from the chip count means 'cannot tell', which must never be read
    as zero — an unreadable count cannot prove an attach landed."""
    p = tmp_path / "a.png"
    p.write_bytes(b"x")
    page = FakePage(NEW_HOST)
    # FakePage.locator returns a generic locator whose count() is 1, so the
    # active-prompt-box check sees 1 box and 1 bar; force the unreadable case.
    import types
    orig = image_worker._v962_composer_chips
    image_worker._v962_composer_chips = lambda _p: None
    try:
        ok, reason = image_worker._v962_attach_reference(page, str(p), prefix="t ")
        assert ok is False
        assert reason == "attachment_unverified"
    finally:
        image_worker._v962_composer_chips = orig


def test_legacy_host_still_uses_the_dialog_path(tmp_path, monkeypatch):
    """Forward-only: the legacy host must not reach the v962.9 branch."""
    p = tmp_path / "a.png"
    p.write_bytes(b"x")
    page = FakePage(OLD_HOST)
    called = []
    monkeypatch.setattr(image_worker, "_v962_attach_reference",
                        lambda *a, **k: called.append(1) or (True, None))
    try:
        image_worker.upload_reference_images(page, [str(p)], context="t")
    except Exception:
        pass  # the legacy path needs a far richer fake; reaching it is the point
    assert called == [], "legacy host must not use the new-host attach"
