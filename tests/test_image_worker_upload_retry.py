# tests/test_image_worker_upload_retry.py
#
# v851: a Render deploy mid-upload used to throw away 4 already-rendered Flow
# variants. requests raised ChunkedEncodingError ("Response ended
# prematurely") — which is NOT a ConnectionError — so the retry catch missed
# it, the bare `except Exception` re-raised on attempt 1/4, and the node was
# posted as failed.
#
# Run from code/:  PYTHONUTF8=1 python -m pytest tests/test_image_worker_upload_retry.py -v

import pytest
import requests

import image_worker


def test_chunked_encoding_error_is_retryable():
    # THE 2026-07-12 BUG. This is the whole point of v851.
    assert image_worker.is_retryable_api_error(
        requests.exceptions.ChunkedEncodingError("Response ended prematurely")
    ) is True


def test_connection_error_and_timeout_stay_retryable():
    assert image_worker.is_retryable_api_error(requests.exceptions.ConnectionError()) is True
    assert image_worker.is_retryable_api_error(requests.exceptions.Timeout()) is True


def test_5xx_is_retryable():
    resp = requests.Response()
    resp.status_code = 502
    err = requests.exceptions.HTTPError(response=resp)
    assert image_worker.is_retryable_api_error(err) is True


def test_4xx_is_not_retryable():
    # A client error will fail identically forever — don't burn 6 attempts.
    resp = requests.Response()
    resp.status_code = 422
    err = requests.exceptions.HTTPError(response=resp)
    assert image_worker.is_retryable_api_error(err) is False


def test_non_requests_error_is_not_retryable():
    assert image_worker.is_retryable_api_error(ValueError("bug in our code")) is False


@pytest.mark.parametrize("exc", [
    requests.exceptions.MissingSchema("api_url has no http://"),
    requests.exceptions.InvalidSchema("ftp:// is not a thing here"),
    requests.exceptions.InvalidURL("not a url"),
    requests.exceptions.URLRequired("no url at all"),
])
def test_malformed_url_errors_are_not_retryable(exc):
    # These are RequestException subclasses, but they come from OUR OWN broken
    # config — a typo'd api_url fails identically on every attempt. Retrying
    # would burn 6 attempts + ~200s of backoff per node before admitting it.
    assert image_worker.is_retryable_api_error(exc) is False


def test_transport_errors_are_still_retryable_after_the_carve_out():
    # Guard: subtracting the permanent set must not cost us the real ones.
    assert image_worker.is_retryable_api_error(
        requests.exceptions.ChunkedEncodingError("Response ended prematurely")) is True
    assert image_worker.is_retryable_api_error(requests.exceptions.ConnectionError()) is True
    assert image_worker.is_retryable_api_error(requests.exceptions.Timeout()) is True


def test_backoff_outlasts_a_render_deploy():
    # A redeploy leaves the platform unreachable for 60-180s. The old
    # [2, 5, 15] schedule gave up after ~22s.
    assert sum(image_worker.API_RETRY_BACKOFF) >= 180


def test_upload_retries_a_chunked_drop_then_succeeds(monkeypatch, tmp_path):
    png = tmp_path / "variant_1.png"
    png.write_bytes(b"\x89PNG fake")
    calls = {"n": 0}

    class _OK:
        status_code = 200
        def raise_for_status(self): pass
        def json(self): return {"ok": True}

    def flaky_post(url, **kw):
        calls["n"] += 1
        if calls["n"] == 1:
            raise requests.exceptions.ChunkedEncodingError("Response ended prematurely")
        return _OK()

    monkeypatch.setattr(image_worker.time, "sleep", lambda *a, **k: None)
    monkeypatch.setattr(requests, "post", flaky_post)

    out = image_worker._upload_variants_to_api("http://x", "k", 2791, [str(png)])
    assert out == {"ok": True}
    assert calls["n"] == 2  # retried the chunked drop instead of giving up


# ---------------------------------------------------------------------------
# v851 health gate — the last line of defence for renders already on disk.
# Both worker paths use it: the [API] main-thread loop and the [API:http] one.
# ---------------------------------------------------------------------------

def test_health_gate_reuploads_after_platform_comes_back(monkeypatch, capsys):
    calls = {"n": 0}
    health = {"n": 0}

    def flaky_upload(api_url, api_key, node_id, paths):
        calls["n"] += 1
        if calls["n"] == 1:
            raise requests.exceptions.ChunkedEncodingError("Response ended prematurely")
        return {"ok": True}

    def fake_health(api_url, api_key, **kw):
        health["n"] += 1
        return True

    monkeypatch.setattr(image_worker, "_upload_variants_to_api", flaky_upload)
    monkeypatch.setattr(image_worker, "_api_wait_for_health", fake_health)

    out = image_worker._upload_variants_with_health_gate(
        "http://x", "k", 2791, ["/tmp/variant_1.png"]
    )
    assert out == {"ok": True}
    assert calls["n"] == 2   # waited for the platform, then re-uploaded
    assert health["n"] == 1  # ...and it actually waited

    # The gate is shared by BOTH callers now, so its log must not claim to be
    # the http path when the main-thread path is the one calling it.
    logged = capsys.readouterr().out
    assert "[API:http]" not in logged
    assert "[API/v851]" in logged


def test_health_gate_does_not_wait_on_a_permanent_error(monkeypatch):
    # A 4xx will fail identically forever. Don't sit in a health wait for it.
    resp = requests.Response()
    resp.status_code = 422
    calls = {"n": 0}

    def bad_upload(api_url, api_key, node_id, paths):
        calls["n"] += 1
        raise requests.exceptions.HTTPError(response=resp)

    def boom_health(*a, **kw):
        raise AssertionError("must not wait for health on a permanent 4xx")

    monkeypatch.setattr(image_worker, "_upload_variants_to_api", bad_upload)
    monkeypatch.setattr(image_worker, "_api_wait_for_health", boom_health)

    with pytest.raises(requests.exceptions.HTTPError):
        image_worker._upload_variants_with_health_gate(
            "http://x", "k", 2791, ["/tmp/variant_1.png"]
        )
    assert calls["n"] == 1  # re-raised immediately, no second attempt


def test_health_gate_surfaces_the_upload_error_when_the_health_check_itself_raises(
    monkeypatch, capsys
):
    # _api_wait_for_health can RAISE instead of returning False. If that escapes,
    # the operator sees a health-check traceback and the "binning N finished
    # variants" line never prints — the real story (an upload that gave up) is
    # lost. Treat a raising health check as "platform did not come back".
    def dead_upload(api_url, api_key, node_id, paths):
        raise requests.exceptions.ChunkedEncodingError("Response ended prematurely")

    def exploding_health(*a, **kw):
        raise RuntimeError("health check blew up")

    monkeypatch.setattr(image_worker, "_upload_variants_to_api", dead_upload)
    monkeypatch.setattr(image_worker, "_api_wait_for_health", exploding_health)

    with pytest.raises(requests.exceptions.ChunkedEncodingError):
        image_worker._upload_variants_with_health_gate(
            "http://x", "k", 2791, ["/tmp/variant_1.png"]
        )

    logged = capsys.readouterr().out
    assert "giving up" in logged           # the give-up line still printed
    assert "1 finished variant(s)" in logged
