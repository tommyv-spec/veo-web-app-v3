"""Slug-shortener: Python mirror of the JS helper in static/index.html.

We test the regex transforms against the actual operator's slug patterns so
that any future tweak to the JS keeps the same shape.
"""
import re


def shorten_slug(slug):
    if not slug:
        return "(untitled)"
    s = slug
    s = re.sub(r"^nuri-korella-", "", s)
    s = re.sub(r"^(ed|fiber|mens|male-vitality|puffy-face|testosterone)-", "", s)
    s = re.sub(r"-v(\d+)$", r" · v\1", s)
    s = s.replace("-", " · ")
    return s


def test_shorten_strips_avatar_prefix():
    assert shorten_slug("nuri-korella-ed-bedroom-morph-v1").startswith("bedroom")


def test_shorten_strips_niche_prefix():
    # "ed" niche prefix stripped — result starts with "bedroom", not "ed"
    out = shorten_slug("nuri-korella-ed-bedroom-morph-v1")
    assert out.startswith("bedroom"), f"expected 'bedroom…', got '{out}'"


def test_shorten_version_suffix():
    out = shorten_slug("nuri-korella-ed-bedroom-morph-v1")
    assert out.endswith("· v1")


def test_shorten_empty():
    assert shorten_slug("") == "(untitled)"
    assert shorten_slug(None) == "(untitled)"


def test_shorten_unknown_avatar():
    out = shorten_slug("custom-build-v3")
    assert out == "custom · build · v3"
