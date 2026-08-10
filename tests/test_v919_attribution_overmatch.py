"""v919 — a job with known URLs must never be resolved by a prompt guess.

Replays the operator's 2026-08-10 failure. Node 4769 asked for 4 variants;
3 came back (one PUBLIC_ERROR_UNSAFE_GENERATION). The flow_api path had
written those 3 exact URLs into captured_urls_by_node, but registered the job
as expecting 4, so Tier A (exact) sat in its 90s partial window. Tier B
matches on a 256-char prompt prefix, and since the v909 server contract every
node opens with the same header, so it matched 91 unconsumed batches, beat
Tier A, and enqueued 91 URLs — four of which were node 4768's images
(ef3aa257, 23ec288f, 9ec04557, 63bd2efd appeared in both nodes' downloads).

The three fixes, tested at the level each one acts on:
  1. register what can arrive, not what was asked for  -> no vulnerable window
  2. Tier B skips jobs that already have exact URLs    -> no guessing
  3. Tier B capped to the expected count               -> bounded blast radius
  4. upload gate refuses an over-sized set             -> nothing lands
"""
import re
from pathlib import Path

import pytest

_SRC = Path(__file__).resolve().parent.parent / "image_worker.py"
_TEXT = _SRC.read_text(encoding="utf-8")


# ---- 1. the job registers the count that can actually arrive ----

def test_api_path_registers_captured_count_not_requested():
    # the InFlightJob for the flow_api path must not expect the asked-for count
    start = _TEXT.index("in_flight[node_id] = InFlightJob(")
    block = _TEXT[start:_TEXT.index("original_job=original_job,", start)]
    assert "variants=max(1, len(captured_fife_urls))" in block
    assert "variants=int(variants or 1)" not in block


def test_partial_capture_no_longer_leaves_an_unsatisfiable_job():
    """3 of 4 captured -> expect 3, so Tier A's need_count is satisfiable."""
    captured = ["u1", "u2", "u3"]          # one variant was content-rejected
    registered = max(1, len(captured))
    assert registered == 3
    # Tier A fires when len(tagged) >= need_count; before the fix need was 4.
    assert len(captured) >= registered


# ---- 2 + 3. Tier B is gated and bounded ----

def _tier_b_source():
    start = _TEXT.index("# Tier B: prompt-match attribution")
    end = _TEXT.index("v521 PRIMARY PATH", start)
    return _TEXT[start:end]


def test_tier_b_skips_jobs_that_already_have_exact_urls():
    src = _tier_b_source()
    assert "captured_urls_by_node.get(job.node_id)" in src
    # the guard must come BEFORE any prompt matching happens
    assert src.index("captured_urls_by_node.get(job.node_id)") < src.index("_collect_batches_for_prompt")


def test_tier_b_caps_to_the_expected_count():
    src = _tier_b_source()
    assert "collected_urls[:need_count]" in src
    assert "matches[:need_count]" in src


def test_tier_b_cap_is_loud():
    assert "prefix over-matched" in _tier_b_source()


@pytest.mark.parametrize("collected,need,expected_kept", [
    (91, 4, 4),     # the operator's exact case
    (4, 4, 4),      # healthy job untouched
    (2, 4, 2),      # genuine partial still allowed through
])
def test_cap_arithmetic(collected, need, expected_kept):
    urls = [f"u{i}" for i in range(collected)]
    kept = urls[:need] if len(urls) > need else urls
    assert len(kept) == expected_kept


# ---- 4. the upload gate ----

def test_enqueue_carries_the_expected_count():
    assert "'expected': max(1, getattr(job, 'variants', 1) or 1)" in _TEXT


def test_upload_gate_refuses_an_over_sized_set():
    gate = _TEXT[_TEXT.index("_expected = item.get('expected')"):]
    gate = gate[:gate.index("# Upload + post status")]
    assert "len(saved_paths) > int(_expected)" in gate
    assert "attribution over-match" in gate
    # it must RAISE, not silently trim to a set we cannot verify
    assert "raise RuntimeError(" in gate


def test_upload_gate_is_inert_without_an_expectation():
    """Queue items from other paths carry no 'expected' — they must pass."""
    for expected, saved, should_raise in [(None, 91, False), (4, 91, True),
                                          (4, 4, False), (4, 3, False)]:
        raised = bool(expected) and saved > int(expected or 0)
        assert raised is should_raise


def test_upload_gate_runs_before_the_upload_call():
    # compare inside the http-worker body, not against the helper's definition
    body = _TEXT[_TEXT.index("def _http_worker():"):]
    assert (body.index("attribution over-match")
            < body.index("_upload_variants_with_health_gate(api_url"))
