"""v862 — tests for the redo in-flight dedup guard.
Run: python -m pytest code/test_redo_inflight.py -v"""
import importlib.util
import pathlib

_SPEC = importlib.util.spec_from_file_location(
    "flow_worker_redoinflight",
    pathlib.Path(__file__).parent / "static" / "flow_worker.py",
)


def _load():
    mod = importlib.util.module_from_spec(_SPEC)
    _SPEC.loader.exec_module(mod)
    return mod


def test_registry_second_concurrent_claim_is_rejected():
    fw = _load()
    reg = fw._RedoInFlightRegistry()
    assert reg.try_claim(555) is True     # first claim granted
    assert reg.try_claim(555) is False    # still held → rejected
    reg.release(555)
    assert reg.try_claim(555) is True      # released → claimable again


def test_registry_independent_per_clip():
    fw = _load()
    reg = fw._RedoInFlightRegistry()
    assert reg.try_claim(1) is True
    assert reg.try_claim(2) is True        # different clip → not blocked


def test_registry_none_clip_id_fails_open():
    fw = _load()
    reg = fw._RedoInFlightRegistry()
    assert reg.try_claim(None) is True
    assert reg.try_claim(None) is True      # None never dedups
    reg.release(None)                        # no crash


def test_registry_stale_claim_is_taken_over():
    fw = _load()
    reg = fw._RedoInFlightRegistry()
    reg.try_claim(777)
    reg._inflight[777] = reg._inflight[777] - reg.TTL_S - 1  # force stale
    assert reg.try_claim(777) is True        # stale → taken over


def test_wrapper_skips_impl_when_clip_in_flight(monkeypatch):
    fw = _load()
    calls = []

    def _fake_impl(page, clip, download_queue, cache, http_dl_queue=None, http_session=None):
        calls.append(clip['id'])
        return "ran"

    monkeypatch.setattr(fw, "_process_redo_clip_impl", _fake_impl)
    clip = {'id': 42, 'clip_index': 0}
    assert fw._redo_in_flight.try_claim(42) is True   # another account holds it
    try:
        result = fw.process_redo_clip(None, clip, None, None)
        assert result is True        # skip sentinel
        assert calls == []           # impl NOT invoked — duplicate suppressed
    finally:
        fw._redo_in_flight.release(42)


def test_wrapper_runs_impl_and_releases_when_free(monkeypatch):
    fw = _load()
    calls = []

    def _fake_impl(page, clip, download_queue, cache, http_dl_queue=None, http_session=None):
        calls.append(clip['id'])
        return "ran"

    monkeypatch.setattr(fw, "_process_redo_clip_impl", _fake_impl)
    clip = {'id': 43, 'clip_index': 1}
    result = fw.process_redo_clip(None, clip, None, None)
    assert result == "ran"           # impl ran, return propagated
    assert calls == [43]
    assert fw._redo_in_flight.try_claim(43) is True   # released in finally
    fw._redo_in_flight.release(43)
