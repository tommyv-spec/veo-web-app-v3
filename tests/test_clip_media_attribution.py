"""v794 stuck-clip fix — proves captured_urls_for_clip() attributes each clip
ONLY its own bound mediaId (the per-clip isolation the old dialogue-text match
violated, which let clip 3 download a sibling's video / poison the dedup set).

Run: python tests/test_clip_media_attribution.py
"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static"))

import flow_worker as fw


def _seed():
    fw._PRIMARY_MEDIA_BINDINGS.clear()
    # clip 0 owns A1/A2 ; clip 2 owns B1/B2 (the real clip-3 stuck scenario)
    fw._PRIMARY_MEDIA_BINDINGS['39cc4e86'] = {'job_id': 'J', 'clip_index': 0}
    fw._PRIMARY_MEDIA_BINDINGS['ede9adf7'] = {'job_id': 'J', 'clip_index': 0}
    fw._PRIMARY_MEDIA_BINDINGS['62b86f5d'] = {'job_id': 'J', 'clip_index': 2}
    fw._PRIMARY_MEDIA_BINDINGS['4df8a77f'] = {'job_id': 'J', 'clip_index': 2}


CAP = {'39cc4e86': 'urlA1', 'ede9adf7': 'urlA2',
       '62b86f5d': 'urlB1', '4df8a77f': 'urlB2'}


def test_clip_gets_only_its_own_media():
    _seed()
    assert sorted(fw.captured_urls_for_clip('J', 2, CAP)) == ['urlB1', 'urlB2'], \
        "clip 2 must resolve to its OWN media, never a sibling's"
    assert sorted(fw.captured_urls_for_clip('J', 0, CAP)) == ['urlA1', 'urlA2']


def test_unbound_clip_returns_empty():
    _seed()
    # clip 5 has no binding → [] → fix does NOT queue → does NOT poison dedup set
    assert fw.captured_urls_for_clip('J', 5, CAP) == []


def test_no_captured_urls_returns_empty():
    _seed()
    assert fw.captured_urls_for_clip('J', 2, {}) == []
    assert fw.captured_urls_for_clip('J', 2, None) == []


def test_wrong_job_isolated():
    _seed()
    assert fw.captured_urls_for_clip('OTHER_JOB', 2, CAP) == []


if __name__ == "__main__":
    test_clip_gets_only_its_own_media()
    test_unbound_clip_returns_empty()
    test_no_captured_urls_returns_empty()
    test_wrong_job_isolated()
    print("PASS: per-clip mediaId isolation holds — clip resolves to own media, "
          "unbound/wrong-job → [] (no dedup poisoning)")
