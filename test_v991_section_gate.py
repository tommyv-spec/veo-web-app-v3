"""v991 — the movie-section gate must count chips and identify them separately.

Why these exist. On 2026-09-13 (run 3, job 34824da1) v990 attached all twelve
ingredients — three on every one of four clips, `6 in the box` each time — and
three of the four clips still refused to submit:

    clip 2   not ready to generate: composer holds 1 chip(s), needs 2
                                    (avatar + source video)
    clip 3   not submitting: composer holds 4 chip(s), needs 3 (1 scene + 2 face)
    clip 4   not submitting: composer holds 5 chip(s), needs 3

One instrument produced all three wrong numbers: `charswap_composer_chip_media_ids`,
a page-wide scrape of mediaId uuids out of `<img src>`. It is blind to a freshly
uploaded chip (whose thumbnail is an /asb/ token with no uuid in the URL) and it
is fooled by stale uuids left on the page by earlier clips. Clip 1 read 3 by luck
— its project was resumed from cache, so all three assets already existed and
carried uuids, and clip 1 is the only one that rendered.

Counting chips and identifying them are different jobs, and one number cannot do
both. v991 splits them:

  * `_v962_section_chip_count` counts the composer's image-chip ELEMENTS, one per
    chip, cross-checked against the 'cancel' button count and failing closed
    (smaller wins) when the two disagree;
  * every attach returns the media id it committed, and `expected_ids` is what
    the verdict and the submit probe judge.

NOTE ON THE MODULE PATH. Same as test_v990: `static/flow_worker.py` is correct in
a clean checkout, but on the shared dev box that file carries another session's
uncommitted work, so point `FLOW_WORKER_PATH` at the constructed blob (or at
`~/veo-worker/flow_worker.py`) to test what is actually being shipped.
"""
import importlib.util
import os
import pathlib
import sys

import pytest

_STATIC = pathlib.Path(__file__).parent / "static"
_TARGET = pathlib.Path(os.environ.get("FLOW_WORKER_PATH")
                       or (_STATIC / "flow_worker.py"))

ID_SCENE = "11111111-2222-3333-4444-555555555555"
ID_FACE1 = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
ID_FACE2 = "99999999-8888-7777-6666-555555555555"


def _load():
    if str(_STATIC) not in sys.path:
        sys.path.insert(0, str(_STATIC))
    spec = importlib.util.spec_from_file_location("flow_worker_v991", _TARGET)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def fw():
    return _load()


# ── a fake page whose only job is to answer .locator(sel).count() ────────────

class _Loc:
    def __init__(self, n):
        self._n = n

    def count(self):
        return self._n


class _CountPage:
    """Answers the two chip selectors with the numbers it was given.

    `chips` is how many image-chip elements the composer holds; `cancels` how
    many remove buttons. Anything else counts 0, so a selector typo shows up as
    a zero rather than as a silent pass.
    """

    def __init__(self, chips, cancels, clip_index=0):
        self.chips = chips
        self.cancels = cancels
        self._movie_section_clip_index = clip_index
        self.asked = []

    def locator(self, sel, **_kw):
        self.asked.append(sel)
        if "flow-image-ingredient-chip" in sel:
            return _Loc(self.chips)
        if "cancel" in sel:
            return _Loc(self.cancels)
        return _Loc(0)


class _ProbePage:
    """Enough page for charswap_install_submit_probe to install on."""

    def __init__(self):
        self.handlers = []

    def on(self, event, cb):
        self.handlers.append((event, cb))

    def remove_listener(self, event, cb):
        self.handlers = [h for h in self.handlers if h != (event, cb)]


# ── A1: the chip counter ─────────────────────────────────────────────────────

def test_a1_counts_one_element_per_chip(fw, capsys):
    page = _CountPage(chips=3, cancels=3)
    assert fw._v962_section_chip_count(page) == 3
    out = capsys.readouterr().out
    # The first call per clip MEASURES both readings instead of assuming them.
    assert "3 image-chip element(s), 3 cancel button(s)" in out


def test_a1_selector_is_the_image_chip_only(fw):
    """The doubled selector is what made every attach read '0 → 2' for one image."""
    assert "flow-ingredient-chip" not in fw._V962_SECTION_CHIP
    assert fw._V962_SECTION_CHIP.count("flow-image-ingredient-chip") == 2


def test_a1_disagreement_fails_closed(fw, capsys):
    page = _CountPage(chips=4, cancels=3)
    assert fw._v962_section_chip_count(page) == 3
    assert "the two chip readings disagree" in capsys.readouterr().out


def test_a1_unreadable_page_is_minus_one(fw):
    class _Dead:
        def locator(self, *_a, **_k):
            raise RuntimeError("page is gone")
    assert fw._v962_section_chip_count(_Dead()) == -1


# ── A2: the verdict ──────────────────────────────────────────────────────────

def test_a2_three_ids_and_three_chips_pass(fw):
    ok, why = fw.movie_section_chip_verdict(
        [ID_SCENE, ID_FACE1, ID_FACE2], 2, 3)
    assert ok, why
    assert "3 identified" in why


def test_a2_short_chip_count_refuses_and_names_the_count(fw):
    """Clip 2's shape: identity fine, the composer is short."""
    ok, why = fw.movie_section_chip_verdict(
        [ID_SCENE, ID_FACE1, ID_FACE2], 2, 2)
    assert not ok
    assert "composer holds 2 chip(s), needs 3" in why


def test_a2_too_many_chips_refuses(fw):
    """Clips 3 and 4's shape, once the count is read by the right instrument."""
    ok, why = fw.movie_section_chip_verdict(
        [ID_SCENE, ID_FACE1, ID_FACE2], 2, 5)
    assert not ok
    assert "composer holds 5 chip(s), needs 3" in why


def test_a2_missing_identity_refuses_and_says_so(fw):
    """Two ids for three ingredients: never submit on a count."""
    ok, why = fw.movie_section_chip_verdict([ID_SCENE, ID_FACE1], 2, 3)
    assert not ok
    assert "2 of 3 ingredient(s) identified" in why
    assert "refusing to submit on a count" in why


def test_a2_repeated_identity_refuses(fw):
    ok, why = fw.movie_section_chip_verdict(
        [ID_SCENE, ID_FACE1, ID_FACE1], 2, 3)
    assert not ok
    assert "not distinct" in why


def test_a2_blank_ids_do_not_count_as_identified(fw):
    ok, why = fw.movie_section_chip_verdict([ID_SCENE, "", None], 2, 3)
    assert not ok
    assert "1 of 3 ingredient(s) identified" in why


# ── A3: readiness ────────────────────────────────────────────────────────────

def test_a3_section_wants_three_and_says_three(fw):
    ready, why = fw.charswap_generate_readiness(True, 1711, 1, want=3)
    assert not ready
    assert "needs 3 (3 ingredients)" in why
    # The old sentence belonged to a different render method entirely.
    assert "avatar + source video" not in why


def test_a3_charswap_default_is_unchanged(fw):
    ready, why = fw.charswap_generate_readiness(True, 900, 1)
    assert not ready
    assert "needs 2 (avatar + source video)" in why


def test_a3_three_chips_three_wanted_is_ready(fw):
    ready, why = fw.charswap_generate_readiness(True, 1731, 3, want=3)
    assert ready
    assert "3 chips" in why


# ── A4: identity, and the probe that has to find it in the body ──────────────

def test_a4_probe_wants_exactly_the_expected_ids(fw):
    page = _ProbePage()
    state = fw.charswap_install_submit_probe(page, [ID_SCENE, ID_FACE1, ID_FACE2])
    assert state["want"] == 3
    assert state["hits"] == 0
    assert page.handlers, "the probe installed no request listener"


def test_a4_submit_verdict_needs_every_expected_id_in_the_body(fw):
    ok, why = fw.movie_section_submit_verdict(seen=True, hits=2, want=3,
                                              api_last=None)
    assert not ok
    assert "2/3" in why


def test_a4_media_uuids_reads_ids_out_of_any_body(fw):
    body = '{"media":{"name":"%s"},"other":"%s"}' % (ID_SCENE, ID_FACE1)
    assert fw._v991_media_uuids(body) == [ID_SCENE, ID_FACE1]
    assert fw._v991_media_uuids("no ids here") == []
    assert fw._v991_media_uuids(None) == []


def test_a4_item_media_id_reads_the_thumbnail_uuid(fw):
    class _Img:
        def __init__(self, src):
            self.src = src

        def get_attribute(self, attr):
            return self.src if attr == "src" else None

    class _Imgs:
        def __init__(self, srcs):
            self.srcs = srcs

        def count(self):
            return len(self.srcs)

        def nth(self, i):
            return _Img(self.srcs[i])

    class _Item:
        def __init__(self, srcs):
            self.srcs = srcs

        def locator(self, sel, **_kw):
            return _Imgs(self.srcs)

    existing = _Item([f"https://x/media/{ID_FACE2}=w200"])
    assert fw._v991_item_media_id(existing) == ID_FACE2
    # A fresh upload's thumbnail is an /asb/ token and names nothing — the
    # measured 2026-09-13 shape. It must answer None, not a wrong id.
    fresh = _Item(["https://flow.google.com/asb/AB-nOUZpV2h7omh4BNK57c30"])
    assert fw._v991_item_media_id(fresh) is None
    assert fw._v991_item_media_id(None) is None


def test_a4_upload_monitor_picks_the_id_the_page_has_not_shown_before(fw):
    m = fw.FramePolicyMonitor.__new__(fw.FramePolicyMonitor)
    m.media_ids = [ID_SCENE, ID_FACE1]
    assert m.new_media_id(exclude={ID_SCENE}) == ID_FACE1
    assert m.new_media_id() == ID_SCENE
    m.media_ids = []
    assert m.new_media_id() is None


# ── the run-3 log lines, as fixtures ─────────────────────────────────────────

def test_run3_clip2_refusal_is_gone_under_the_new_instruments(fw):
    """Clip 2 attached 3 and was refused on a read of 1. With the DOM count and
    three captured ids it now passes both gates."""
    ok, why = fw.movie_section_chip_verdict(
        [ID_SCENE, ID_FACE1, ID_FACE2], 2, 3)
    assert ok, why
    ready, ready_why = fw.charswap_generate_readiness(True, 1711, 3, want=3)
    assert ready, ready_why


def test_run3_clip4_phantom_chips_still_refuse(fw):
    """Five real chip ELEMENTS is a genuinely dirty composer and must still
    refuse — the fix is a better instrument, not a looser gate."""
    page = _CountPage(chips=5, cancels=5, clip_index=3)
    ok, why = fw.movie_section_chip_verdict(
        [ID_SCENE, ID_FACE1, ID_FACE2], 2,
        fw._v962_section_chip_count(page))
    assert not ok
    assert "needs 3" in why
