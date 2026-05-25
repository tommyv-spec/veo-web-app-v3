import importlib.util
import pathlib

_SPEC = importlib.util.spec_from_file_location(
    "flow_worker_autoredo",
    pathlib.Path(__file__).parent / "static" / "flow_worker.py",
)


def _load():
    mod = importlib.util.module_from_spec(_SPEC)
    _SPEC.loader.exec_module(mod)
    return mod


def test_auto_redo_cap_fails_after_max_cycles():
    fw = _load()
    cid = 999001
    c1 = fw.register_auto_redo_cycle(cid)
    assert c1 == 1
    assert fw.auto_redo_exhausted(c1) is False  # first redo still allowed
    c2 = fw.register_auto_redo_cycle(cid)
    assert c2 == 2
    assert fw.auto_redo_exhausted(c2) is True    # second hard-fail → give up


def test_auto_redo_cap_independent_per_clip():
    fw = _load()
    a = fw.register_auto_redo_cycle(111)
    b = fw.register_auto_redo_cycle(222)
    assert a == 1 and b == 1
    assert fw.auto_redo_exhausted(a) is False


def test_auto_redo_cap_ignores_empty_clip_id():
    fw = _load()
    assert fw.register_auto_redo_cycle(None) == 0
    assert fw.register_auto_redo_cycle(0) == 0


def test_clear_resets_counter_for_fresh_budget():
    fw = _load()
    cid = 999002
    assert fw.register_auto_redo_cycle(cid) == 1
    assert fw.register_auto_redo_cycle(cid) == 2  # exhausted / gave up
    fw.clear_auto_redo_cycle(cid)                 # success or give-up reset
    # A later attempt (e.g. user Retry) starts from a fresh budget.
    assert fw.register_auto_redo_cycle(cid) == 1
    assert fw.auto_redo_exhausted(1) is False


def test_clear_ignores_empty_clip_id():
    fw = _load()
    fw.clear_auto_redo_cycle(None)  # must not raise
    fw.clear_auto_redo_cycle(0)
