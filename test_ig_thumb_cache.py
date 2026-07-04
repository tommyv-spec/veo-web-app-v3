"""Tests for instagram_client thumb-caching helpers."""
import importlib.util
import pathlib


def _load():
    spec = importlib.util.spec_from_file_location(
        "instagram_client", pathlib.Path(__file__).parent / "instagram_client.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _FakeStorage:
    def __init__(self):
        self.uploaded = []

    def upload_bytes(self, data, remote_key, content_type="application/octet-stream", metadata=None):
        self.uploaded.append((remote_key, data, content_type))
        return remote_key


def test_ig_thumb_key_shape():
    m = _load()
    assert m.ig_thumb_key(7, "AbCdEf") == "instagram/thumbs/7/AbCdEf.jpg"


def test_cache_thumb_bytes_uploads_and_returns_key():
    m = _load()
    storage = _FakeStorage()
    key = m.cache_thumb_bytes(
        "https://scontent.example/x.jpg",
        "instagram/thumbs/7/AbCdEf.jpg",
        storage,
        fetch=lambda url, timeout: (b"\xff\xd8\xff\x00jpegbytes", "image/jpeg"),
    )
    assert key == "instagram/thumbs/7/AbCdEf.jpg"
    assert len(storage.uploaded) == 1
    assert storage.uploaded[0][0] == "instagram/thumbs/7/AbCdEf.jpg"
    assert storage.uploaded[0][2] == "image/jpeg"


def test_cache_thumb_bytes_none_when_no_url():
    m = _load()
    storage = _FakeStorage()
    assert m.cache_thumb_bytes(None, "k", storage, fetch=lambda u, t: (b"x", "image/jpeg")) is None
    assert storage.uploaded == []


def test_cache_thumb_bytes_none_when_storage_none():
    m = _load()
    assert m.cache_thumb_bytes("https://x/y.jpg", "k", None, fetch=lambda u, t: (b"x", "image/jpeg")) is None


def test_cache_thumb_bytes_swallows_fetch_error():
    m = _load()
    storage = _FakeStorage()

    def _boom(url, timeout):
        raise RuntimeError("network down")

    assert m.cache_thumb_bytes("https://x/y.jpg", "k", storage, fetch=_boom) is None
    assert storage.uploaded == []


def test_cache_thumb_bytes_none_when_empty_bytes():
    m = _load()
    storage = _FakeStorage()
    assert m.cache_thumb_bytes("https://x/y.jpg", "k", storage, fetch=lambda u, t: (b"", "image/jpeg")) is None
    assert storage.uploaded == []


def test_thumb_public_url_prefers_cached_route():
    m = _load()
    assert m.thumb_public_url(42, "instagram/thumbs/7/x.jpg", "https://scontent/dead.jpg") == "/api/instagram/videos/42/thumb"


def test_thumb_public_url_falls_back_to_raw():
    m = _load()
    assert m.thumb_public_url(42, None, "https://scontent/live.jpg") == "https://scontent/live.jpg"


def test_thumb_public_url_none_when_nothing():
    m = _load()
    assert m.thumb_public_url(42, None, None) is None
