"""v861 — per-clip render duration picked from the spoken line's word count.

SINGLE SOURCE OF TRUTH for the bucket math. Imported by image_platform.py
(resolve at import time) and main.py (validate the PATCH). worker.py and
static/flow_worker.py do NOT import this — they read the resolved integer off
the Clip row / API payload, so the table lives in exactly one place.

Operator table (2026-07-16), literal upper bounds:

    words <= 11   -> 4s
    12..16        -> 6s
    17..24        -> 8s
    25..28        -> 10s
    > 28          -> 10s + caller logs a warning

The table tops out at 28 because a spoken line may not exceed 28 words (v831,
amended 2026-07-16 from 25 so the 10s bucket is reachable). That cap is
enforced by the /build auditor, which lives outside this repo and cannot
import this module — so the number is deliberately NOT re-declared here as a
constant free to drift out of step with the auditor's copy.

Implied speech rate 2.67-3.0 words/sec (least-squares fit of the operator's
four points = 2.8 w/s). Full deep-dive: template_reference.md §v861.
"""
from typing import Optional

# Everything the platform can ask for. Flow's 2026-07 composer has a
# 4s/6s/8s/10s tablist (static/flow_worker.py select_frames_to_video_mode).
ALLOWED_CLIP_DURATIONS_S = (4, 6, 8, 10)

# What the Veo API itself accepts on durationSeconds — 10s does NOT exist here.
# https://ai.google.dev/gemini-api/docs/veo
VEO_API_DURATIONS_S = (4, 6, 8)

# (max_words, duration_s) — first row whose max_words the count fits under wins.
CLIP_DURATION_BUCKETS = ((11, 4), (16, 6), (24, 8), (28, 10))


def _validated_duration_s(value, field_name: str) -> int:
    """Gate every caller-supplied duration entering this module.

    Accepts an int, or a value exactly equal to one of ALLOWED_CLIP_DURATIONS_S
    (so ``4.0`` passes, ``4.5`` does not). Rejects bools — ``isinstance(True,
    int)`` is True in Python, but a bool is not a duration. Returns a real int,
    so a float can never reach the Veo durationSeconds payload.

    Validation runs on the RAW value, BEFORE any int() coercion — otherwise
    ``6.7`` would silently truncate into a valid-looking 6.
    """
    if isinstance(value, bool) or value not in ALLOWED_CLIP_DURATIONS_S:
        raise ValueError(
            "%s %r not in %r (v861)"
            % (field_name, value, list(ALLOWED_CLIP_DURATIONS_S))
        )
    return int(value)


def pick_clip_duration_s(word_count: int) -> int:
    """Map a word count to its v861 duration bucket. Never returns None."""
    for max_words, duration in CLIP_DURATION_BUCKETS:
        if word_count <= max_words:
            return duration
    return 10  # over the cap — biggest bucket; caller should warn


def veo_api_duration_s(
    duration_s: Optional[int],
    field_name: str = "clip_duration_s",
) -> Optional[int]:
    """The Veo API's durationSeconds for an already-picked clip duration.

    The Veo API has no 10s bucket, Flow's composer does — so 10 folds down to
    8. 4/6/8 pass through unchanged; None passes through. Anything outside
    ALLOWED_CLIP_DURATIONS_S raises rather than folding: a below-range value is
    an upstream bug, and quietly promoting it to the longest, most expensive
    bucket would hide that bug behind a bigger render bill.

    ``field_name`` names the field the value was read from, so the raised
    message points at something the operator can actually find. Callers reading
    a DB column pass its name (worker.py reads clips.veo_render_duration_s);
    the default suits a caller holding the markdown bullet's value.
    """
    if duration_s is None:
        return None
    picked = _validated_duration_s(duration_s, field_name)
    if picked in VEO_API_DURATIONS_S:
        return picked
    return 8  # 10 is the only value left — the Veo-vs-Flow dialect fold


def resolve_clip_duration_s(
    explicit: Optional[int],
    anchor_bucket: Optional[int],
    line_text: Optional[str],
) -> Optional[int]:
    """Final per-clip duration. Precedence, highest first:

    1. ``explicit``      — the scene's `- **clip_duration_s:**` bullet (v861)
    2. ``anchor_bucket`` — the v667 frame-anchor-derived bucket (transformation
                           montages; already ceil'd to [4,6,8] by the caller)
    3. word count of ``line_text`` — the v861 table
    4. None              — no line, no anchor: the job-level duration applies

    Both caller-supplied durations go through the same validation gate. Neither
    is trusted: ``explicit`` arrives from a markdown parser, and at the
    ``anchor_bucket`` call-site a float target_duration_s sits one line away
    from the valid int, so a wiring slip must raise here, not reach a render.

    Raises ValueError on a value outside ALLOWED_CLIP_DURATIONS_S.
    """
    if explicit is not None:
        return _validated_duration_s(explicit, "clip_duration_s")
    if anchor_bucket is not None:
        return _validated_duration_s(anchor_bucket, "anchor_bucket")
    words = len((line_text or "").split())
    if words == 0:
        return None
    return pick_clip_duration_s(words)
