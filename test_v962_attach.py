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


# --- v974: the uploader's CONTROL changed, not the mechanism ------------------
#
# The file-chooser upload already lived in _v962_pick_asset_in_picker. The only
# thing that broke was the caption it clicked: flow.google.com has no button
# saying "Upload media" any more, the uploader is `Add media menu` -> `Upload`.
# One check, on the thing that would silently rot: the control it drives.


def test_the_picker_upload_drives_the_add_media_menu():
    """A regression here is invisible at runtime -- the old caption simply never
    matches and the attach fails with "no chip", which reads as a glitch."""
    src = (_STATIC / "flow_worker.py").read_text(encoding="utf-8")
    start = src.index("def _v962_pick_asset_in_picker(")
    body = src[start:src.index("\ndef ", start + 10)]
    assert "button[aria-label='Add media menu']" in body, \
        "the picker upload must open Flow's Add-media menu"
    assert "expect_file_chooser" in body, \
        "it is a NATIVE chooser -- there is no input[type=file] to drive"
    assert "button:has-text('Upload media')" not in body, \
        "the dead caption must not come back"


# --- v974: agent-off on the new host -----------------------------------------
#
# Copied from the operator's HAR (flow.google.com, 2026-09-08). The payload shape
# is the whole trick, so guard it: a wrong index or a wrong enum silently leaves
# Agent ON, which hides the Settings chip and fails every clip with
# "Settings button not found" -- a symptom nobody traces back to here.


def test_agent_off_payloads_match_the_har():
    fw = _load()
    calls = []

    class _P:
        url = "https://flow.google.com/project/abc"

        def evaluate(self, _js, args=None):
            calls.append(args)
            return {"status": 200}

    assert fw._v974_agent_off_batchexecute(_P(), "abc", "") is True
    shapes = [(rpcid, inner) for rpcid, inner, _src in calls]
    assert [s[0] for s in shapes] == ["DA4VGb", "DA4VGb", "Kcr7Ub", "Kcr7Ub"]
    # user-level: the flag sits at index 11 / 12 and must be 0 (off)
    assert shapes[0][1] == [[None] * 11 + [0], [["is_agent_mode_toggled"]]]
    assert shapes[1][1] == [[None] * 12 + [0], [["is_chat_panel_open"]]]
    # per-project: agent_toggle_state 2 == DISABLED (1 == ENABLED in the HAR)
    assert shapes[3][1] == ["projects/abc", [None] * 4 + [2],
                            [["agent_toggle_state"]]]


def test_project_init_asserts_agent_off_not_just_force_agent_off():
    """Operator, 2026-09-12: "we always need to make sure it's off."

    The account flag persists, so a project usually comes up clean -- but
    persistence is not a guarantee. A human, a parallel session or a Google
    default flip can turn Agent back on, and the reactive path
    (force_agent_off, called when the gear is already missing) only notices
    after a clip has failed with "Settings button not found". So init must
    ASSERT it. This guards against that call being dropped as redundant.
    """
    src = (_STATIC / "flow_worker.py").read_text(encoding="utf-8")
    start = src.index("def _fa_init_project_best_effort(")
    body = src[start:src.index("\ndef ", start + 10)]
    assert "_v974_agent_off_batchexecute(page, project_id" in body, \
        "project init must assert Agent OFF, not rely on the remembered state"


def test_agent_off_reads_back_instead_of_trusting_the_writes():
    """The agent-off payloads are POSITIONAL protobuf, and a positional index is
    the one hardcode here that cannot be removed -- the wire format IS positions.
    If Google inserts a field they shift and we write the WRONG setting: agent
    stays on, the chip stays hidden, every clip then fails with "Settings button
    not found", and nobody traces that back to a payload index.

    4/4 accepted means Google took the writes, not that they meant what we
    intended. So force_agent_off must return what the DOM says, not what the
    HTTP status said.
    """
    src = (_STATIC / "flow_worker.py").read_text(encoding="utf-8")
    start = src.index("def force_agent_off(")
    body = src[start:src.index("\ndef ", start + 10)]
    assert "_V962_SETTINGS_CHIP" in body, (
        "agent-off must read the chip back, not trust the write")
    assert "return _chip_back" in body, (
        "the return value must be the DOM's answer, not the request's")
