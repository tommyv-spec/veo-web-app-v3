"""v863 — tests for the redo status pre-check retry + fail-closed behavior.

Regression guard: a single API timeout used to return None WITHOUT raising, the
pre-check treated that as "nothing to skip on", and the worker regenerated a clip
that was already terminally content-rejected AND already replaced by the operator.

Run: python -m pytest code/test_redo_status_precheck.py -v"""
import importlib.util
import pathlib

_SPEC = importlib.util.spec_from_file_location(
    "flow_worker_precheck",
    pathlib.Path(__file__).parent / "static" / "flow_worker.py",
)


def _load():
    mod = importlib.util.module_from_spec(_SPEC)
    _SPEC.loader.exec_module(mod)
    return mod


def test_returns_status_on_first_success(monkeypatch):
    fw = _load()
    calls = []

    def _fake_api(method, endpoint, data=None):
        calls.append(endpoint)
        return {"status": "completed"}

    monkeypatch.setattr(fw, "api_request", _fake_api)
    monkeypatch.setattr(fw.time, "sleep", lambda s: None)
    assert fw._fetch_clip_status_with_retry(123) == {"status": "completed"}
    assert len(calls) == 1          # no wasted retries on success


def test_retries_then_succeeds(monkeypatch):
    fw = _load()
    calls = []

    def _fake_api(method, endpoint, data=None):
        calls.append(endpoint)
        if len(calls) < 3:
            return None              # transient timeout (api_request returns None)
        return {"status": "approved"}

    monkeypatch.setattr(fw, "api_request", _fake_api)
    monkeypatch.setattr(fw.time, "sleep", lambda s: None)
    assert fw._fetch_clip_status_with_retry(123) == {"status": "approved"}
    assert len(calls) == 3           # retried through the transient failures


def test_returns_none_when_all_attempts_fail(monkeypatch):
    fw = _load()
    calls = []

    def _fake_api(method, endpoint, data=None):
        calls.append(endpoint)
        return None                  # sustained outage

    monkeypatch.setattr(fw, "api_request", _fake_api)
    monkeypatch.setattr(fw.time, "sleep", lambda s: None)
    assert fw._fetch_clip_status_with_retry(123) is None
    assert len(calls) == 3           # exactly `attempts` tries, then give up


def test_attempts_is_configurable(monkeypatch):
    fw = _load()
    calls = []

    monkeypatch.setattr(fw, "api_request",
                        lambda m, e, data=None: calls.append(e))   # always None
    monkeypatch.setattr(fw.time, "sleep", lambda s: None)
    assert fw._fetch_clip_status_with_retry(123, attempts=5) is None
    assert len(calls) == 5


def test_queries_the_approval_status_endpoint_for_the_clip(monkeypatch):
    fw = _load()
    seen = {}

    def _fake_api(method, endpoint, data=None):
        seen['method'] = method
        seen['endpoint'] = endpoint
        return {"status": "failed", "error_code": "CONTENT_POLICY_VIOLATION"}

    monkeypatch.setattr(fw, "api_request", _fake_api)
    monkeypatch.setattr(fw.time, "sleep", lambda s: None)
    out = fw._fetch_clip_status_with_retry(12896)
    assert seen['method'] == "GET"
    assert seen['endpoint'] == "/clips/12896/approval-status"
    # the terminal marker must survive so the caller's skip-check can see it
    assert out['error_code'] == "CONTENT_POLICY_VIOLATION"
