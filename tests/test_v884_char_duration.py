"""v884 — the char table and the hyphen-split word count.

The rule the operator hit (2026-08-01): two 10-word lines both landed in the
4s bucket and only one of them fits.
"""
import pytest

from clip_duration import (
    CLIP_CHAR_BUCKETS,
    CLIP_DURATION_BUCKETS,
    count_line_chars,
    count_line_words,
    pick_clip_duration_for_line,
    pick_clip_duration_s,
    resolve_clip_duration_s,
)

S5 = "neither. i stopped wasting money on quick fixes years ago."
S6 = "then explain my forty-five-year-old husband. his soldier already gave up."


def test_the_two_lines_that_started_this():
    """Same word count under v861, different clip under v884."""
    assert count_line_chars(S5) == 58
    assert count_line_chars(S6) == 73
    assert pick_clip_duration_for_line(S5) == 4
    assert pick_clip_duration_for_line(S6) == 6


def test_hyphen_split_counts_spoken_words():
    assert count_line_words("forty-five-year-old husband") == 5
    assert count_line_words("two words") == 2
    assert count_line_words("") == 0
    assert count_line_words(None) == 0
    assert count_line_words("   ") == 0
    # a trailing / leading hyphen must not produce an empty word
    assert count_line_words("well- yes") == 2


@pytest.mark.parametrize("chars,expected", [
    (0, 4), (1, 4), (59, 4),
    (60, 6), (86, 6),
    (87, 8), (129, 8),
    (130, 10), (151, 10),
    (152, 10), (400, 10),      # over cap -> biggest bucket; auditor FAILs it
])
def test_char_buckets(chars, expected):
    assert pick_clip_duration_s(0, chars) == expected


def test_char_count_is_none_keeps_pure_v861():
    """A caller holding only a count cannot be given a char answer."""
    for words, expected in ((11, 4), (16, 6), (24, 8), (28, 10)):
        assert pick_clip_duration_s(words) == expected


def test_takes_the_longer_of_the_two():
    # many SHORT words: few chars, but still slow to say -> the WORD table wins
    many_short = " ".join(["is"] * 20)          # 20w, 59c
    assert count_line_chars(many_short) == 59
    assert pick_clip_duration_for_line(many_short) == 8   # not the char table's 4

    # few LONG words: the CHAR table wins
    few_long = "extraordinarily complicated pharmaceutical advertisements everywhere nationwide"
    assert count_line_words(few_long) == 6
    assert pick_clip_duration_for_line(few_long) >= 6


def test_v884_never_shortens_a_v861_pick():
    """max() is the whole safety argument: shortening cuts speech off."""
    for n in range(0, 40):
        line = " ".join(["word"] * n)
        assert pick_clip_duration_for_line(line) >= pick_clip_duration_s(n)


def test_char_table_matches_the_word_table_shape():
    """Same durations, same order — the frontend zips them the same way."""
    assert [d for _, d in CLIP_CHAR_BUCKETS] == [d for _, d in CLIP_DURATION_BUCKETS]


def test_resolve_uses_the_char_table():
    assert resolve_clip_duration_s(
        explicit=None, anchor_bucket=None, line_text=S6) == 6
    assert resolve_clip_duration_s(
        explicit=None, anchor_bucket=None, line_text=S5) == 4


def test_resolve_still_returns_none_for_silent():
    """A hyphen-only bullet is not a spoken line."""
    assert resolve_clip_duration_s(
        explicit=None, anchor_bucket=None, line_text="-") is None


def test_explicit_and_anchor_still_win_over_the_char_table():
    assert resolve_clip_duration_s(explicit=4, anchor_bucket=None, line_text=S6) == 4
    assert resolve_clip_duration_s(explicit=None, anchor_bucket=8, line_text=S6) == 8
