"""v939.8 — the v667 anchor may LENGTHEN a clip, never shorten it below what
the spoken line needs.

Measured 2026-08-23: all 9 under-bound clips in production took the anchor
path. `target_duration_s` was set and equal to the stored duration in every
case, and every one of them was then cut short at render — including two
(14302, 14303) that turned up in the cut-clip investigation.

The anchor is derived from frame anchors: it describes VISUAL timing and
knows nothing about how long the words take to say. Letting it outrank the
line's own requirement breaks v708's zero-word-loss contract by construction.

An EXPLICIT `- **clip_duration_s:**` bullet still wins outright — that is a
deliberate author choice and v861 says it outranks everything.

Run: python -m pytest code/tests/test_clip_duration_anchor_floor.py -q
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from clip_duration import resolve_clip_duration_s, pick_clip_duration_for_line

# 17 words / 76 chars -> the table asks for 8s.
LONG_LINE = "this batch sells out fast, so follow me first or it will not let me send it."
# 10 words / 59 chars -> 4s.
SHORT_LINE = "cold opens them. that is blood moving, three seconds later."


def test_the_line_needs_what_we_think_it_needs():
    assert pick_clip_duration_for_line(LONG_LINE) == 8
    assert pick_clip_duration_for_line(SHORT_LINE) == 4


def test_an_anchor_shorter_than_the_speech_is_raised_to_fit():
    # THE BUG: production clip 14303 stored 4s for this exact line and was cut.
    assert resolve_clip_duration_s(
        explicit=None, anchor_bucket=4, line_text=LONG_LINE) == 8


def test_an_anchor_longer_than_the_speech_still_wins():
    # A visual beat may legitimately want a longer clip than the words need.
    assert resolve_clip_duration_s(
        explicit=None, anchor_bucket=8, line_text=SHORT_LINE) == 8


def test_an_anchor_equal_to_the_speech_is_unchanged():
    assert resolve_clip_duration_s(
        explicit=None, anchor_bucket=8, line_text=LONG_LINE) == 8


def test_an_explicit_bullet_still_outranks_the_anchor_and_the_line():
    # v861: a declared duration is a deliberate author choice. The auditor is
    # where a bad declaration gets caught, not here.
    assert resolve_clip_duration_s(
        explicit=6, anchor_bucket=8, line_text=LONG_LINE) == 6


def test_a_silent_scene_still_takes_the_anchor():
    # No speech to fit, so the anchor is the only signal and must be honoured.
    assert resolve_clip_duration_s(
        explicit=None, anchor_bucket=4, line_text=None) == 4
    assert resolve_clip_duration_s(
        explicit=None, anchor_bucket=4, line_text="") == 4


def test_no_anchor_and_no_explicit_still_sizes_from_the_line():
    assert resolve_clip_duration_s(
        explicit=None, anchor_bucket=None, line_text=LONG_LINE) == 8


def test_nothing_at_all_is_still_none():
    assert resolve_clip_duration_s(
        explicit=None, anchor_bucket=None, line_text=None) is None
