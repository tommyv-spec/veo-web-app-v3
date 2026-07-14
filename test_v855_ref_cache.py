"""v855 — uploaded-reference media_id cache, keyed by (project_id, content hash).

The persona ref is the same 6-7 MB PNG on every node of a batch. It was being
base64'd and re-POSTed each time: operator log node 2928 (3 refs) showed a
98.4s submit_wall with only 46.3s accounted for by cooldown+mint+fetch. The
missing ~52s was re-uploading bytes Flow already had.

Run: python -m pytest test_v855_ref_cache.py -q
"""
import collections

import image_worker as iw


class FakePage:
    """Accepts attributes (the cache lives on the page) and counts uploads."""

    url = "https://labs.google/fx/tools/flow/project/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def _cache(page):
    return iw._fa_ref_cache(page)


def test_cache_starts_empty_and_persists_on_the_page():
    page = FakePage()
    c1 = _cache(page)
    c1[("p1", "hash1")] = "media-1"
    c2 = _cache(page)
    assert c2 is c1, "cache must persist across calls on the same page"
    assert c2[("p1", "hash1")] == "media-1"


def test_cache_is_scoped_per_project():
    # Flow scopes uploaded media to a project (uploadImage carries projectId).
    # The SAME bytes in a DIFFERENT project must re-upload, not reuse an id the
    # other project minted.
    page = FakePage()
    c = _cache(page)
    iw._fa_ref_cache_put(c, ("proj-A", "samehash"), "media-A")

    assert c.get(("proj-A", "samehash")) == "media-A"
    assert c.get(("proj-B", "samehash")) is None, "media_id leaked across projects"


def test_cache_evicts_oldest_past_the_cap():
    page = FakePage()
    c = _cache(page)
    for i in range(iw._FA_REF_CACHE_CAP + 10):
        iw._fa_ref_cache_put(c, ("p", f"hash{i}"), f"media-{i}")

    assert len(c) == iw._FA_REF_CACHE_CAP
    assert ("p", "hash0") not in c, "oldest entry should have been evicted"
    assert ("p", f"hash{iw._FA_REF_CACHE_CAP + 9}") in c, "newest entry must survive"


def test_reuse_moves_an_entry_to_the_end_so_it_is_not_evicted():
    page = FakePage()
    c = _cache(page)
    iw._fa_ref_cache_put(c, ("p", "persona"), "media-persona")
    for i in range(iw._FA_REF_CACHE_CAP - 1):
        iw._fa_ref_cache_put(c, ("p", f"filler{i}"), f"m{i}")

    # The persona is the oldest — but it's the one reused on every node, so
    # touching it must keep it alive.
    iw._fa_ref_cache_put(c, ("p", "persona"), "media-persona")
    for i in range(20):
        iw._fa_ref_cache_put(c, ("p", f"more{i}"), f"mm{i}")

    assert c.get(("p", "persona")) == "media-persona", "hot persona ref got evicted"


def test_drop_project_clears_only_that_project():
    page = FakePage()
    c = _cache(page)
    iw._fa_ref_cache_put(c, ("proj-A", "h1"), "a1")
    iw._fa_ref_cache_put(c, ("proj-A", "h2"), "a2")
    iw._fa_ref_cache_put(c, ("proj-B", "h1"), "b1")

    iw._fa_ref_cache_drop_project(page, "proj-A")

    assert ("proj-A", "h1") not in c
    assert ("proj-A", "h2") not in c
    assert c.get(("proj-B", "h1")) == "b1", "dropping one project nuked another"


def test_cache_disabled_gracefully_when_page_rejects_attributes():
    class NoAttrPage:
        __slots__ = ()   # cannot take _fa_ref_media_cache

    c = iw._fa_ref_cache(NoAttrPage())
    assert isinstance(c, collections.OrderedDict)
    assert len(c) == 0
    # A fresh dict every call = every ref re-uploads. Slow, but correct — the
    # worker must not crash just because the cache can't attach.
    c2 = iw._fa_ref_cache(NoAttrPage())
    assert c2 is not c


def test_drop_project_on_a_page_with_no_cache_is_a_noop():
    iw._fa_ref_cache_drop_project(FakePage(), "proj-A")   # must not raise


def test_timing_line_exposes_uploads():
    # The whole reason ~52s was invisible: uploads never hit a timing bucket.
    class _Cli(iw._FaClient):
        def __init__(self):
            self._t = self._zero_timings()

    cli = _Cli()
    cli._t["upload"] = 51.7
    cli._t["upload_n"] = 3
    cli._t["upload_mb"] = 8.1
    cli.note_cached_upload()
    summary = cli.timings_summary()

    assert "upload=51.7s(3x, 8.1MB, cached=1)" in summary, summary
    assert "fetch=" in summary and "mint=" in summary
