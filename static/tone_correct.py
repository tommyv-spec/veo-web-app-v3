#!/usr/bin/env python3
"""
tone_correct.py — server-side yellow/warm-tint correction for GPT-4o images.

Reverse-engineered from gpt-tone.com (see tools/gpt-tone/CODEX-HANDOFF.md). Their
correction is a per-channel linear levels stretch (out = a*in + b, R^2 > 0.99, no
cross-channel mixing). We reproduce it generally as per-channel auto-levels:
  - white point -> ~99.9th percentile per channel (kills the warm cast; blue's
    highlight sits lowest under a yellow cast, so blue gets the biggest gain).
  - the RED channel gets a harder black clip (~7th percentile) than green/blue
    (~0.8th) — that shadow crush is the site's contrast "punch".

Used by image_platform.worker_upload_variants to correct ChatGPT-backend variants
on the server (Render) right before they are stored — so every user's ChatGPT
images are de-yellowed without any local worker changes.

Fail-safe by contract: correct_bytes NEVER raises. On any error (bad bytes,
missing numpy, tiny image) it returns the ORIGINAL bytes unchanged, so a
correction problem can never break an upload.
"""
from __future__ import annotations

import io
import logging

log = logging.getLogger("tone_correct")

# Reverse-engineered defaults (robust general levels; see handoff doc).
PLO = 0.8      # green/blue black-point percentile
PHI = 99.9     # white-point percentile (all channels)
RED_LO = 7.0   # red black-point percentile = the contrast punch


def _correct_array(img, plo=PLO, phi=PHI, red_lo=RED_LO):
    import numpy as np
    los = [red_lo, plo, plo]
    out = np.empty_like(img)
    for c in range(3):
        ch = img[..., c]
        a = np.percentile(ch, los[c])
        b = np.percentile(ch, phi)
        if b <= a:
            b = a + 1
        out[..., c] = np.clip((ch - a) * 255.0 / (b - a), 0, 255)
    return out


def correct_bytes(data: bytes, fmt: str | None = None) -> bytes:
    """Return de-yellowed image bytes. On ANY failure, returns `data` unchanged.

    fmt: optional output format hint ("PNG"/"JPEG"/"WEBP"); defaults to the
    source image's own format, falling back to PNG.
    """
    if not data:
        return data
    try:
        import numpy as np
        from PIL import Image

        im = Image.open(io.BytesIO(data))
        src_fmt = (fmt or im.format or "PNG").upper()
        if src_fmt == "JPG":
            src_fmt = "JPEG"
        rgb = im.convert("RGB")
        arr = np.asarray(rgb).astype(np.float64)
        if arr.ndim != 3 or arr.shape[2] != 3 or arr.shape[0] < 4 or arr.shape[1] < 4:
            return data  # not a normal color raster — leave it alone

        corrected = _correct_array(arr).astype("uint8")
        out_im = Image.fromarray(corrected)

        buf = io.BytesIO()
        save_kwargs = {}
        if src_fmt in ("JPEG", "WEBP"):
            save_kwargs["quality"] = 95
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
    """Server toggle. ON by default; set TONE_CORRECT_CHATGPT=0 to disable."""
    import os
    return os.environ.get("TONE_CORRECT_CHATGPT", "1").strip().lower() not in ("0", "false", "no", "off")
