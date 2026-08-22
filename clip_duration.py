"""v861 + v884 — per-clip render duration picked from the spoken line.

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
four points = 2.8 w/s).

v884 (operator 2026-08-01) — WORD COUNT ALONE IS NOT A DURATION. Two 10-word
lines can take very different times to say:

    "neither. i stopped wasting money on quick fixes years ago."   10w  58c
    "then explain my forty-five-year-old husband. his soldier      10w  73c
     already gave up."

Both landed in the 4s bucket; the second does not fit. Measured over the 2708
spoken lines in videos/*.md, ONE word bucket spans a huge char range — the 4s
bucket holds lines from 11 to 78 chars, and the 8s bucket starts at 71. The
buckets overlap, so a word count cannot separate them.

v884 keeps the word table and ADDS a character table, then takes the LONGER of
the two. Two parts:

1. char buckets, calibrated on the same corpus (median 5.38 chars per word
   including its space), so the v861 word boundaries 11/16/24/28 map to
   59/86/129/151 chars — a consistent ~15 chars/sec at every boundary.
2. the word count now splits on hyphens too, so `forty-five-year-old` counts
   as the four words it is spoken as, not one.

max() and not the char table alone: a line of many SHORT words has few chars
but still takes time to say (pure chars would have SHORTENED 146 corpus lines
→ speech cut off mid-sentence, the bad failure). max() only ever lengthens
(337 lines, all by exactly one bucket bar one). Extra dead air is harmless —
v810 already wants post-speech silence.

Full deep-dive: template_reference.md §v861 + §v884.
"""
import re
from typing import Optional

# Everything the platform can ask for. Flow's 2026-07 composer has a
# 4s/6s/8s/10s tablist (static/flow_worker.py select_frames_to_video_mode).
ALLOWED_CLIP_DURATIONS_S = (4, 6, 8, 10)

# What the Veo API itself accepts on durationSeconds — 10s does NOT exist here.
# https://ai.google.dev/gemini-api/docs/veo
VEO_API_DURATIONS_S = (4, 6, 8)

# (max_words, duration_s) — first row whose max_words the count fits under wins.
CLIP_DURATION_BUCKETS = ((11, 4), (16, 6), (24, 8), (28, 10))

# v884 — (max_chars, duration_s), same shape. Calibrated on the 2708 spoken
# lines in videos/*.md: median 5.38 chars per word INCLUDING its space, so the
# word boundaries above map to 59/86/129/151 chars (~15 chars/sec throughout).
# Chars are counted raw: spaces and punctuation included, because a comma or a
# full stop is a real pause the renderer has to fit.
CLIP_CHAR_BUCKETS = ((59, 4), (86, 6), (129, 8), (151, 10))

# v884 — a word ends at whitespace OR a hyphen. `forty-five-year-old` is four
# spoken words; a plain .split() called it one and under-sized the clip.
# Covers the unicode dash block too, though v615 bans em-dashes in a line.
_WORD_SPLIT = re.compile(r"[\s\-‐-―]+")


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


def count_line_words(line_text: Optional[str]) -> int:
    """v884 word count: split on whitespace AND hyphens.

    `forty-five-year-old husband` is five spoken words, not two. Kept as a
    named function so the auditor, the parser hint and the frontend all count
    the same way.
    """
    return len([w for w in _WORD_SPLIT.split(line_text or "") if w])


def count_line_chars(line_text: Optional[str]) -> int:
    """v884 char count: the raw length, spaces and punctuation included."""
    return len((line_text or "").strip())


def _bucket(count: int, table) -> int:
    for max_count, duration in table:
        if count <= max_count:
            return duration
    return 10  # over the cap — biggest bucket; caller should warn


def pick_clip_duration_s(word_count: int, char_count: Optional[int] = None) -> int:
    """Map a line's size to its duration bucket. Never returns None.

    v861 gave the word table. v884 adds the char table and takes the LONGER of
    the two, because a word count alone cannot tell a fast 10-word line from a
    slow one. ``char_count=None`` keeps the pure-v861 behaviour, which is what
    a caller holding only a count (not the text) can honestly ask for.
    """
    picked = _bucket(word_count, CLIP_DURATION_BUCKETS)
    if char_count is None:
        return picked
    return max(picked, _bucket(char_count, CLIP_CHAR_BUCKETS))


def pick_clip_duration_for_line(line_text: Optional[str]) -> int:
    """v884 — the whole rule for a line of text, counting done here.

    The one call site every caller should prefer: it cannot be fed a word
    count that was split the pre-v884 way.
    """
    return pick_clip_duration_s(
        count_line_words(line_text), count_line_chars(line_text)
    )


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

    1. ``explicit``      — the scene's `- **clip_duration_s:**` bullet (v861).
                           Outranks everything: a declared duration is a
                           deliberate author choice, and the auditor is where a
                           bad one gets caught, not here.
    2. SPOKEN line — ``max(anchor_bucket, what the line needs)`` (v939.8). The
       v667 anchor may LENGTHEN a clip but never shorten it below the speech:
       the anchor describes visual timing and knows nothing about how long the
       words take. The line is sized by the v861 word table and the v884 char
       table, whichever asks for the longer clip.
    3. SILENT scene — ``anchor_bucket`` alone; there is no speech to fit.
    4. None              — no line, no anchor: the job-level duration applies

    Both caller-supplied durations go through the same validation gate. Neither
    is trusted: ``explicit`` arrives from a markdown parser, and at the
    ``anchor_bucket`` call-site a float target_duration_s sits one line away
    from the valid int, so a wiring slip must raise here, not reach a render.

    Raises ValueError on a value outside ALLOWED_CLIP_DURATIONS_S.
    """
    if explicit is not None:
        return _validated_duration_s(explicit, "clip_duration_s")

    # v939.8 — THE ANCHOR MAY LENGTHEN A CLIP, NEVER SHORTEN IT BELOW THE
    # SPEECH. The anchor bucket is derived from frame anchors: it describes
    # VISUAL timing and knows nothing about how long the words take to say.
    # Letting it outrank the line's own requirement breaks v708's zero
    # word-loss contract by construction.
    #
    # Measured 2026-08-23: all NINE under-bound clips in production took this
    # path — `target_duration_s` set and equal to the stored duration in every
    # case — and every one was cut short at render. Two of them (14302, 14303)
    # are clips that turned up in the cut-clip investigation. One stored 4s for
    # a line the table sizes at 8s.
    #
    # A longer anchor still wins: a visual beat may legitimately want more room
    # than the words need. Only the shortening direction is a defect.
    if count_line_words(line_text) == 0:
        # Silent scene: no speech to fit, so the anchor is the only signal.
        if anchor_bucket is not None:
            return _validated_duration_s(anchor_bucket, "anchor_bucket")
        return None

    line_needs = pick_clip_duration_for_line(line_text)
    if anchor_bucket is None:
        return line_needs
    return max(_validated_duration_s(anchor_bucket, "anchor_bucket"), line_needs)
