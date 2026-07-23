#!/usr/bin/env python3
"""
tone_correct.py — yellow/warm-tint correction for GPT-4o images (ChatGPT worker).

Reverse-engineered from gpt-tone.com (see tools/gpt-tone/CODEX-HANDOFF.md). Their
correction is a per-channel linear levels stretch (out = a*in + b, R^2 > 0.99, no
cross-channel mixing). We reproduce it generally as per-channel auto-levels:
  - white point -> ~99.9th percentile per channel (kills the warm cast; blue's
    highlight sits lowest under a yellow cast, so blue gets the biggest gain).
  - the RED channel gets a harder black clip (~7th percentile) than green/blue
    (~0.8th) — that shadow crush is the site's contrast "punch".

Called by chatgpt_image_backend.generate() on the freshly downloaded image, in
place, before it is uploaded to the platform.

DEPENDENCIES: Pillow only. Deliberately NO numpy — the worker is a light
distributed bundle, and a 256-bin histogram + a per-channel LUT does the same
job (and is faster on large images than percentile over a full pixel array).

Fail-safe by contract: correct_bytes NEVER raises. On any error (bad bytes,
missing Pillow, tiny image) it returns the ORIGINAL bytes unchanged, so a
correction problem can never break a generation or an upload.
"""
from __future__ import annotations

import io
import logging

log = logging.getLogger("tone_correct")

# Reverse-engineered defaults (see handoff doc).
PLO = 0.8      # green/blue black-point percentile
PHI = 99.9     # white-point percentile (all channels)
RED_LO = 7.0   # red black-point percentile = the contrast punch


def _percentile_from_hist(hist, pct):
    """Value (0..255) at `pct` percentile of a 256-bin channel histogram."""
    total = sum(hist)
    if total <= 0:
        return 0
    target = total * (pct / 100.0)
    cum = 0
    for v, c in enumerate(hist):
        cum += c
        if cum >= target:
            return v
    return 255


def _levels_lut(lo, hi):
    """256-entry LUT mapping [lo, hi] -> [0, 255], clipped."""
    if hi <= lo:
        hi = lo + 1
    scale = 255.0 / (hi - lo)
    lut = []
    for v in range(256):
        x = int(round((v - lo) * scale))
        lut.append(0 if x < 0 else (255 if x > 255 else x))
    return lut


def correct_bytes(data: bytes, fmt: str | None = None) -> bytes:
    """Return de-yellowed image bytes. On ANY failure, returns `data` unchanged.

    fmt: optional output format hint ("PNG"/"JPEG"/"WEBP"); defaults to the
    source image's own format, falling back to PNG.
    """
    if not data:
        return data
    try:
        from PIL import Image

        im = Image.open(io.BytesIO(data))
        src_fmt = (fmt or im.format or "PNG").upper()
        if src_fmt == "JPG":
            src_fmt = "JPEG"

        # Preserve an alpha channel if present (correct only RGB, re-attach A).
        has_alpha = im.mode in ("RGBA", "LA") or (im.mode == "P" and "transparency" in im.info)
        alpha = im.convert("RGBA").getchannel("A") if has_alpha else None

        rgb = im.convert("RGB")
        if rgb.width < 4 or rgb.height < 4:
            return data  # too small to level meaningfully

        los = (RED_LO, PLO, PLO)          # red crushed harder than green/blue
        out_channels = []
        for ch, plo in zip(rgb.split(), los):
            hist = ch.histogram()
            lo = _percentile_from_hist(hist, plo)
            hi = _percentile_from_hist(hist, PHI)
            out_channels.append(ch.point(_levels_lut(lo, hi)))
        out_im = Image.merge("RGB", out_channels)
        if alpha is not None:
            out_im.putalpha(alpha)

        buf = io.BytesIO()
        save_kwargs = {}
        if src_fmt in ("JPEG", "WEBP"):
            save_kwargs["quality"] = 95
        if src_fmt == "JPEG" and out_im.mode == "RGBA":
            out_im = out_im.convert("RGB")   # JPEG has no alpha
        try:
            out_im.save(buf, format=src_fmt, **save_kwargs)
        except Exception:
            buf = io.BytesIO()
            out_im.save(buf, format="PNG")
        return buf.getvalue()
    except Exception as e:  # never break the caller
        log.warning(f"[tone_correct] skipped (returning original): {e}")
        return data


def is_enabled() -> bool:
    """Toggle. ON by default; set TONE_CORRECT_CHATGPT=0 to disable."""
    import os
    return os.environ.get("TONE_CORRECT_CHATGPT", "1").strip().lower() not in ("0", "false", "no", "off")
