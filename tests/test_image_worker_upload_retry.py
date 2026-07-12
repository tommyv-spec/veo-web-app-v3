# tests/test_image_worker_upload_retry.py
#
# v851: a Render deploy mid-upload used to throw away 4 already-rendered Flow
# variants. requests raised ChunkedEncodingError ("Response ended
# prematurely") — which is NOT a ConnectionError — so the retry catch missed
# it, the bare `except Exception` re-raised on attempt 1/4, and the node was
# posted as failed.
#
# Run from code/:  PYTHONUTF8=1 python -m pytest tests/test_image_worker_upload_retry.py -v

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
