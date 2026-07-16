"""v857 — per-clip duration bucket math."""
import pytest

from clip_duration import (
    ALLOWED_CLIP_DURATIONS_S,
    VEO_API_DURATIONS_S,
    clamp_for_veo_api,
    pick_clip_duration_s,
    resolve_clip_duration_s,
)


@pytest.mark.parametrize("words,expected", [
    (0, 4), (1, 4), (11, 4),          # <=11 -> 4s
    (12, 6), (16, 6),                  # 12-16 -> 6s
    (17, 8), (24, 8),                  # 17-24 -> 8s
    (25, 10), (28, 10),                # 25-28 -> 10s
    (29, 10), (60, 10),                # >28 -> 10s (auditor FAILs the build)
])
def test_pick_clip_duration_s_buckets(words, expected):
    assert pick_clip_duration_s(words) == expected


def test_operator_anchor_points():
    """The 4 points the operator specified on 2026-07-16."""
    assert pick_clip_duration_s(11) == 4    # "less than 12 words is 4 seconds"
    assert pick_clip_duration_s(16) == 6    # "16 is 6 seconds"
    assert pick_clip_duration_s(24) == 8    # "24 is 8 seconds"
    assert pick_clip_duration_s(28) == 10   # "around 28 words we 10 seconds"


def test_allowed_values():
    assert ALLOWED_CLIP_DURATIONS_S == (4, 6, 8, 10)
    assert VEO_API_DURATIONS_S == (4, 6, 8)


def test_clamp_for_veo_api():
    assert clamp_for_veo_api(4) == 4
    assert clamp_for_veo_api(6) == 6
    assert clamp_for_veo_api(8) == 8
    assert clamp_for_veo_api(10) == 8   # Veo API has no 10s bucket
    assert clamp_for_veo_api(None) is None


def test_resolve_precedence_explicit_wins():
    assert resolve_clip_duration_s(explicit=6, anchor_bucket=8, line_text="a b c" * 20) == 6


def test_resolve_precedence_anchor_beats_wordcount():
    assert resolve_clip_duration_s(explicit=None, anchor_bucket=8, line_text="two words") == 8


def test_resolve_precedence_wordcount_when_no_anchor():
    # 18 words -> 8s bucket
    assert resolve_clip_duration_s(
        explicit=None, anchor_bucket=None,
        line_text=" ".join(["w"] * 18)) == 8


def test_resolve_returns_none_for_silent_scene():
    """No explicit, no anchor, no words -> NULL -> job default duration applies."""
    assert resolve_clip_duration_s(explicit=None, anchor_bucket=None, line_text="") is None
    assert resolve_clip_duration_s(explicit=None, anchor_bucket=None, line_text=None) is None


def test_resolve_rejects_bad_explicit():
    with pytest.raises(ValueError, match="not in"):
        resolve_clip_duration_s(explicit=7, anchor_bucket=None, line_text="hello")
