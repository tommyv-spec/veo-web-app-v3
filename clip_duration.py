"""v857 — per-clip render duration picked from the spoken line's word count.

SINGLE SOURCE OF TRUTH for the bucket math. Imported by image_platform.py
(resolve at import time) and main.py (validate the PATCH). worker.py and
static/flow_worker.py do NOT import this — they read the resolved integer off
the Clip row / API payload, so the table lives in exactly one place.

Operator table (2026-07-16), literal upper bounds:

    words <= 11   -> 4s
    12..16        -> 6s
    17..24        -> 8s
    25..28        -> 10s
    > 28          -> 10s + caller logs a warning (v831 caps lines at 28 words;
                     the /build auditor FAILs before a build gets this far)

Implied speech rate 2.67-3.0 words/sec (least-squares fit of the operator's
four points = 2.8 w/s). Full deep-dive: template_reference.md §v857.
"""
from typing import Optional

# Everything the platform can ask for. Flow's 2026-07 composer has a
# 4s/6s/8s/10s tablist (static/flow_worker.py select_frames_to_video_mode).
ALLOWED_CLIP_DURATIONS_S = (4, 6, 8, 10)

# What the Veo API itself accepts on durationSeconds — 10s does NOT exist here.
# https://ai.google.dev/gemini-api/docs/veo
VEO_API_DURATIONS_S = (4, 6, 8)

# (max_words, duration_s) — first row whose max_words the count fits under wins.
_BUCKETS = ((11, 4), (16, 6), (24, 8), (28, 10))

# v831 (amended 2026-07-16) — a spoken line over this many words must be split
# into two clips. Was 25; raised to 28 so the 10s bucket is reachable.
LINE_WORD_CAP = 28


def pick_clip_duration_s(word_count: int) -> int:
    """Map a word count to its v857 duration bucket. Never returns None."""
    for max_words, duration in _BUCKETS:
        if word_count <= max_words:
            return duration
    return 10  # over the cap — biggest bucket; caller should warn


def clamp_for_veo_api(duration_s: Optional[int]) -> Optional[int]:
    """Veo API has no 10s bucket — fold 10 down to 8. None passes through."""
    if duration_s is None:
        return None
    if duration_s in VEO_API_DURATIONS_S:
        return duration_s
    return 8


def resolve_clip_duration_s(
    explicit: Optional[int],
    anchor_bucket: Optional[int],
    line_text: Optional[str],
) -> Optional[int]:
    """Final per-clip duration. Precedence, highest first:

    1. ``explicit``      — the scene's `- **clip_duration_s:**` bullet (v857)
    2. ``anchor_bucket`` — the v667 frame-anchor-derived bucket (transformation
                           montages; already ceil'd to [4,6,8] by the caller)
    3. word count of ``line_text`` — the v857 table
    4. None              — no line, no anchor: the job-level duration applies

    Raises ValueError on an explicit value outside ALLOWED_CLIP_DURATIONS_S.
    """
    if explicit is not None:
        if int(explicit) not in ALLOWED_CLIP_DURATIONS_S:
            raise ValueError(
                "clip_duration_s %r not in %r (v857)"
                % (explicit, list(ALLOWED_CLIP_DURATIONS_S))
            )
        return int(explicit)
    if anchor_bucket is not None:
        return int(anchor_bucket)
    words = len((line_text or "").split())
    if words == 0:
        return None
    return pick_clip_duration_s(words)
