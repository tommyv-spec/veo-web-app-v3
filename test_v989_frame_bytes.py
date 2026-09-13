"""v989 — a frame that failed to download must never become a file we trust.

Read off disk 2026-09-13, after clip 14939 failed its frame attach five times:

    frames/image_07.png   190 bytes
    b'upstream connect error or disconnect/reset before headers. retried and the
      latest reset reason: remote connection failure ... Connection refused'

Its siblings are 500-700 KB. The proxy answered HTTP 200 with an error body, so
`raise_for_status()` passed and the text was written as the image. Flow silently
refuses it on upload, so the asset never enters the project, the picker never
lists it, and the clip dies as "frame attach glitched".

The second half is what made it permanent: the fast path at the top of
`download_frame` returns any non-empty file as already downloaded, so the poison
was served back for the rest of the job and every retry reused it.

WEBP matters here. Flow serves these frames as WEBP under a .png name -- the four
good files in that directory are all `RIFF....WEBP` -- so a PNG-only check would
reject every valid frame. That is the one way this fix could quietly destroy the
lane, so it is tested first.
"""
import importlib.util
import pathlib
import sys

_STATIC = pathlib.Path(__file__).parent / "static"
_SPEC = importlib.util.spec_from_file_location("fw_v989", _STATIC / "flow_worker.py")

PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 16
JPEG = b"\xff\xd8\xff\xe0" + b"\x00" * 16
WEBP = b"RIFF\x2c\x9f\x08\x00WEBPVP8 " + b"\x00" * 8
GIF = b"GIF89a" + b"\x00" * 16
PROXY_ERROR = (b"upstream connect error or disconnect/reset before headers. "
               b"retried and the latest reset reason: remote connection failure")


def _load():
    if str(_STATIC) not in sys.path:
        sys.path.insert(0, str(_STATIC))
    mod = importlib.util.module_from_spec(_SPEC)
    _SPEC.loader.exec_module(mod)
    return mod


def test_webp_counts_as_an_image():
    """THE ONE THAT MATTERS MOST. Flow serves these frames as WEBP named .png;
    a PNG-only check would reject every good frame and stop the lane dead."""
    fw = _load()
    assert fw._v989_looks_like_an_image(WEBP) is True


def test_the_real_formats_pass():
    fw = _load()
    for head in (PNG, JPEG, GIF):
        assert fw._v989_looks_like_an_image(head) is True, head[:8]


def test_the_proxy_error_that_caused_this_is_rejected():
    fw = _load()
    assert fw._v989_looks_like_an_image(PROXY_ERROR) is False


def test_empty_and_short_bodies_are_rejected():
    fw = _load()
    for head in (b"", b"\x89PNG", None):
        assert fw._v989_looks_like_an_image(head) is False


def test_a_poisoned_file_on_disk_is_not_trusted(tmp_path):
    """The cached-failure half: 190 bytes of error text is 'non-empty', and that
    was enough to be served back as a frame for five consecutive runs."""
    fw = _load()
    bad = tmp_path / "image_07.png"
    bad.write_bytes(PROXY_ERROR)
    assert fw._v989_file_is_an_image(str(bad)) is False

    good = tmp_path / "image_08.png"
    good.write_bytes(WEBP)
    assert fw._v989_file_is_an_image(str(good)) is True


def test_a_missing_file_is_not_an_image(tmp_path):
    fw = _load()
    assert fw._v989_file_is_an_image(str(tmp_path / "nope.png")) is False
