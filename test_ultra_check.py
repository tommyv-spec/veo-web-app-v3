import importlib.util
import pathlib

_SPEC = importlib.util.spec_from_file_location(
    "fw_ultra",
    pathlib.Path(__file__).parent / "static" / "flow_worker.py",
)


def _load():
    m = importlib.util.module_from_spec(_SPEC)
    _SPEC.loader.exec_module(m)
    # Speed up + isolate from network.
    m.time.sleep = lambda *a, **k: None
    m.api_request = lambda *a, **k: None
    return m


class FakePage:
    """Minimal Playwright-page stand-in. `badge_seq` is the sequence of bools
    page.evaluate(_ULTRA_BADGE_JS) returns on successive calls (False after
    exhausted)."""

    def __init__(self, badge_seq, url="https://labs.google/fx/tools/flow"):
        self._seq = list(badge_seq)
        self.url = url
        self.reloads = 0

    def evaluate(self, js):
        return self._seq.pop(0) if self._seq else False

    def reload(self, **kw):
        self.reloads += 1


def test_ultra_present_marks_session_verified():
    fw = _load()
    p = FakePage([True])
    assert fw.check_ultra_account(p, "Acc1", timeout=3) is True
    assert "Acc1" in fw._ULTRA_VERIFIED


def test_never_verified_absent_raises_and_does_not_reload():
    fw = _load()
    p = FakePage([False, False, False])  # badge never appears
    raised = False
    try:
        fw.check_ultra_account(p, "AccX", timeout=3)
    except fw.NotUltraError:
        raised = True
    assert raised is True
    assert p.reloads == 0  # no previously-verified bypass for a fresh account


def test_previously_verified_absent_reloads_and_continues_without_killing():
    fw = _load()
    fw._ULTRA_VERIFIED.add("Acc1")  # simulate earlier successful verification
    p = FakePage([False, False, False])  # badge never reappears, even post-reload
    # Must NOT raise — reload once, re-poll, then continue True.
    assert fw.check_ultra_account(p, "Acc1", timeout=3) is True
    assert p.reloads == 1


def test_previously_verified_reconfirms_after_reload():
    fw = _load()
    fw._ULTRA_VERIFIED.add("Acc1")
    # 3 misses in first loop, then badge appears on first post-reload poll.
    p = FakePage([False, False, False, True])
    assert fw.check_ultra_account(p, "Acc1", timeout=3) is True
    assert p.reloads == 1


def test_blank_label_does_not_bypass_kill():
    fw = _load()
    fw._ULTRA_VERIFIED.add("")  # a blank-label "verification" must not shield others
    p = FakePage([False, False, False])
    raised = False
    try:
        fw.check_ultra_account(p, "", timeout=3)
    except fw.NotUltraError:
        raised = True
    assert raised is True
    assert p.reloads == 0  # blank label never takes the bypass
