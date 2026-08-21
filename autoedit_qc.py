"""Pure contracts for auto-edit repair settings and quality verdicts.

Keep this module free of OpenCV, ffmpeg, SQLAlchemy and web-framework imports.
The server, local worker and tests all use the same normalization rules.
"""
from __future__ import annotations

from pathlib import Path


DEFAULT_REPAIRS = {
    "trim_start_s": 0.0,
    "trim_end_s": 0.0,
    "pip_enabled": True,
    "captions_enabled": True,
    "chroma_similarity": 0.10,
    "chroma_blend": 0.02,
    "music_filename": None,
    "music_db": -20.0,
}

MUSIC_EXTENSIONS = {".aac", ".m4a", ".mp3", ".mp4", ".wav"}


def normalize_repairs(value=None):
    """Return validated, JSON-safe repair settings.

    Raises ValueError with a user-facing message. The API returns that message
    before a run is queued; the worker calls this again so older/bad rows fail
    loudly instead of producing a surprising edit.
    """
    raw = dict(value or {})
    unknown = sorted(set(raw) - set(DEFAULT_REPAIRS))
    if unknown:
        raise ValueError("Unknown auto-edit repair setting(s): " + ", ".join(unknown))

    out = dict(DEFAULT_REPAIRS)
    out.update(raw)
    try:
        out["trim_start_s"] = float(out["trim_start_s"])
        out["trim_end_s"] = float(out["trim_end_s"])
        out["chroma_similarity"] = float(out["chroma_similarity"])
        out["chroma_blend"] = float(out["chroma_blend"])
        out["music_db"] = float(out["music_db"])
    except (TypeError, ValueError) as exc:
        raise ValueError("Auto-edit timing, keying and music values must be numbers") from exc

    if out["trim_start_s"] < 0 or out["trim_end_s"] < 0:
        raise ValueError("Trim values cannot be negative")
    if out["trim_start_s"] + out["trim_end_s"] > 600:
        raise ValueError("Combined trim cannot exceed 10 minutes")
    if not 0.02 <= out["chroma_similarity"] <= 0.25:
        raise ValueError("Green-key strength must be between 0.02 and 0.25")
    if not 0.0 <= out["chroma_blend"] <= 0.10:
        raise ValueError("Green-key softness must be between 0.00 and 0.10")
    if not -40.0 <= out["music_db"] <= -8.0:
        raise ValueError("Music volume must be between -40 dB and -8 dB")

    out["pip_enabled"] = bool(out["pip_enabled"])
    out["captions_enabled"] = bool(out["captions_enabled"])
    music = out.get("music_filename")
    if music in (None, ""):
        out["music_filename"] = None
    else:
        music = str(music).strip()
        if Path(music).name != music or Path(music).suffix.lower() not in MUSIC_EXTENSIONS:
            raise ValueError("Music must be a plain output filename ending in mp3, wav, m4a, aac or mp4")
        out["music_filename"] = music
    return out


def caption_face_overlap_metrics(buckets, windows, frame_height=1920, band_height=0.15):
    """Measure caption-band overlap with every detected face.

    Returns total and worst overlap in vertical pixels. This deliberately uses
    the same one-second occupancy buckets as the placement planner; it is a
    conservative screen-space check, not a claim about subtitle glyph pixels.
    """
    total = 0.0
    worst = 0.0
    seconds = 0
    half = band_height / 2
    for bucket in buckets or []:
        t = float(bucket.get("t", 0.0))
        active = next((w for w in windows if w[0] <= t < w[1]), None)
        if not active:
            continue
        center = 0.5 + float(active[2])
        top, bottom = center - half, center + half
        for face in bucket.get("faces", []):
            if len(face) < 4:
                continue
            overlap = max(0.0, min(bottom, float(face[3])) - max(top, float(face[1])))
            pixels = overlap * frame_height
            total += pixels
            worst = max(worst, pixels)
        seconds += 1
    return {
        "total_vertical_px": round(total, 1),
        "worst_vertical_px": round(worst, 1),
        "seconds_checked": seconds,
    }


def build_qc_report(checks):
    """Build the stored READY / NEEDS_MANUAL_EDIT report."""
    normalized = []
    reasons = []
    for check in checks:
        row = {
            "id": str(check["id"]),
            "status": str(check.get("status", "fail")).lower(),
            "message": str(check.get("message", "")),
        }
        if "value" in check:
            row["value"] = check["value"]
        normalized.append(row)
        if row["status"] == "fail":
            reasons.append(row["message"] or row["id"])
    return {
        "schema_version": 1,
        "verdict": "READY" if not reasons else "NEEDS_MANUAL_EDIT",
        "reasons": reasons,
        "checks": normalized,
    }
