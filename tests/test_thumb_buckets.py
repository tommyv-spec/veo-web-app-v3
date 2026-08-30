"""The thumbnail bucket list.

A phone screen is ~1170 device pixels wide, and the biggest bucket was 512 --
so a full-screen review was either soft or a multi-megabyte original. Adding
1024 must not move any existing caller to a different bucket, because the
picker is nearest-match and every current call site asks for 128 or 256.
"""
from image_platform import _THUMB_WIDTHS


def _bucket(w):
    return min(_THUMB_WIDTHS, key=lambda a: abs(a - w))


def test_a_phone_sized_bucket_exists():
    assert 1024 in _THUMB_WIDTHS


def test_existing_callers_keep_their_bucket():
    assert _bucket(128) == 128
    assert _bucket(256) == 256
    assert _bucket(512) == 512


def test_the_viewer_gets_the_big_one():
    assert _bucket(1024) == 1024


def test_buckets_are_sorted_ascending():
    # The nearest-match picker does not need it, but a tie resolves to the
    # first entry, so the order is load-bearing on any future value.
    assert list(_THUMB_WIDTHS) == sorted(_THUMB_WIDTHS)
