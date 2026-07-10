# tests/test_image_worker_health_retry.py
#
# v845: the startup connectivity check used to be a SINGLE 10s request. One
# ReadTimeout while the platform (Render) redeployed killed the whole worker:
#   [API] ❌ Can't reach http://kavenobuilder.com: ... Read timed out
#   [IMAGE] Closing browser... Done.
# _api_wait_for_health now retries with backoff so the worker rides out a deploy
# window (~2-3 min), and only gives up after WORKER_HEALTH_MAX_WAIT_S.
#
# Run from code/:  PYTHONUTF8=1 python -m pytest tests/test_image_worker_health_retry.py -v

import image_worker


def test_returns_true_immediately_when_platform_healthy(monkeypatch):
    calls = {"n": 0}

    def fake_request(api_url, api_key, method, endpoint, **kw):
        calls["n"] += 1
        return {"status": "ok"}

    monkeypatch.setattr(image_worker, "_api_request", fake_request)
    monkeypatch.setattr(image_worker.time, "sleep", lambda *a, **k: None)

    assert image_worker._api_wait_for_health("http://x", "k") is True
    assert calls["n"] == 1  # no retries needed


def test_retries_through_a_deploy_then_succeeds(monkeypatch):
    """Platform is down for the first 2 checks (deploy window), then answers."""
    calls = {"n": 0}

    def flaky_request(api_url, api_key, method, endpoint, **kw):
        calls["n"] += 1
        if calls["n"] < 3:
            raise OSError("Read timed out. (read timeout=10)")
        return {"status": "ok"}

    monkeypatch.setattr(image_worker, "_api_request", flaky_request)
    monkeypatch.setattr(image_worker.time, "sleep", lambda *a, **k: None)
    monkeypatch.setenv("WORKER_HEALTH_MAX_WAIT_S", "900")

    assert image_worker._api_wait_for_health("http://x", "k") is True
    assert calls["n"] == 3  # failed twice, recovered on the third — worker survives


def test_gives_up_after_budget_exhausted(monkeypatch):
    """A genuinely-down platform still exits (returns False), not spin forever."""
    calls = {"n": 0}

    def always_fail(api_url, api_key, method, endpoint, **kw):
        calls["n"] += 1
        raise OSError("connection refused")

    monkeypatch.setattr(image_worker, "_api_request", always_fail)
    monkeypatch.setattr(image_worker.time, "sleep", lambda *a, **k: None)
    # zero budget → the first failure exhausts it immediately
    monkeypatch.setenv("WORKER_HEALTH_MAX_WAIT_S", "0")

    assert image_worker._api_wait_for_health("http://x", "k") is False
    assert calls["n"] == 1
