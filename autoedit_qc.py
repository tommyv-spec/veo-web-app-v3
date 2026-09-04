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
    # v947.2 — what the audio stage does. "voice" = the full talking-head chain
    # (DeepFilter denoise + voice EQ + loudnorm), the right thing for every
    # spoken video. "off" = pass the export's audio through UNTOUCHED — for
    # source-original / music-bed videos, where the voice chain measurably guts
    # the track (DeepFilter treats music as noise; Venice job 1e574970,
    # 2026-08-27: spectrogram lost the whole mid/high band).
    # v948.2 — "level" is the middle setting: the whole voice chain EXCEPT the
    # denoiser, so a v948-swept export keeps its loudness pass without the
    # denoiser re-creating the silence holes the sweep removed.
    "audio_enhance": "voice",
    # v938.15 — hook composite. None = today's layout (keyed speaker at full
    # size over a blurred backdrop). A float = the corpus/CapCut layout: the
    # b-roll fills the frame SHARP and the speaker is scaled to this fraction
    # and anchored flush to the bottom-left corner, for the hook only.
    # Measured reference: the operator's own CapCut project used 0.429, giving
    # x[0..463] y[1097..1920] on 1080x1920. See
    # docs/experiments/autoedit-hook-composite-placement-2026-08-22.md
    "hook_corner": None,
    # v938.16 — what plays full-frame behind the corner speaker. A filename from
    # this job's own outputs. Omitted, the pipeline auto-picks final_broll_*.
    # It may be a STILL IMAGE: the operator's own 1f35eac2 edit put a black-and-
    # white interview frame behind the corner speaker, and it reads the same as
    # a moving b-roll.
    "hook_bg": None,
    # v944 — the read-caption text overlay, as the build declared it:
    #   {"overlay": "readcaption", "overlay_age": "I'M 74",
    #    "overlay_block": [...], "overlay_footer": "(READ CAPTION)"}
    # None = no overlay, which is every run that existed before this key.
    #
    # It rides HERE, inside repair_json, rather than on a new AutoEditRun
    # column: repair_json already round-trips to both workers (the server claim
    # and the local one each hand it straight back to run_autoedit), so there
    # is nothing to migrate and nothing new to keep in sync.
    "overlay_spec": None,
    # v960 — the source look, declared by the build.
    #   caption_case  : "lower" applies the source ad's casing rule to every
    #                   transcribed word before the captions are drawn.
    #   caption_words : what pycaps WRITES -> what it should SAY, so a misheard
    #                   brand ("garnices") and a proper noun's capital are one
    #                   map instead of a hand edit after the render.
    #   text_overlays : burned banner/CTA text with fractional positions and a
    #                   start time. A second overlay engine beside v944's
    #                   read-caption one, running after it.
    # All three None = every run that existed before this key, unchanged.
    "caption_case": None,
    "caption_words": None,
    "text_overlays": None,
}

HOOK_BG_EXTENSIONS = {".mp4", ".mov", ".png", ".jpg", ".jpeg", ".webp"}

MUSIC_EXTENSIONS = {".aac", ".m4a", ".mp3", ".mp4", ".wav"}

# v960 — the only keys a text-overlay item may carry. Deliberately closed:
# colour, font and shadow are the constants measured off the source frames and
# pinned by test, the same discipline v944.1 applies to the read-caption ones.
TEXT_OVERLAY_KEYS = ("text", "y", "size", "from", "until")


def validate_caption_case(value):
    """v960 — the casing rule for the burned captions. None = leave it alone."""
    if value in (None, ""):
        return None
    v = str(value).strip().lower()
    if v != "lower":
        raise ValueError(
            "caption_case must be 'lower' (the source ad's rule: all-lowercase "
            "except the I-forms and the words caption_words names), or omitted")
    return v


def validate_caption_words(value):
    """v960 — the word map. Keys are lowercased: the match is case-insensitive
    on the word core, so a build declares the fix once and it lands wherever
    pycaps happened to capitalise it."""
    if value in (None, "", {}):
        return None
    if not isinstance(value, dict):
        raise ValueError(
            "caption_words must be an object mapping what the transcript says "
            "to what it should say, e.g. {\"garnices\": \"Garnissa's\"}")
    out = {}
    for key, replacement in value.items():
        k = str(key).strip().lower()
        if not k:
            raise ValueError("caption_words has an empty key")
        v = str(replacement).strip()
        if not v:
            raise ValueError(f"caption_words[{key!r}] has no replacement text")
        out[k] = v
    return out


def validate_text_overlays(value):
    """v960 — the burned text overlays, validated HERE so a bad item dies at
    queue time rather than halfway through an ffmpeg pass.

    `y` is a FRACTION of frame height, never a raw ffmpeg expression: the
    renderer emits `y=h*<fraction>`, which removes a filter-injection surface
    and is easier to author than `h*0.175`. `size` is whole pixels at the
    OUTPUT height — the reference numbers are for 1080x1920 and do not scale.
    """
    if value in (None, "", [], ()):
        return None
    if not isinstance(value, (list, tuple)):
        raise ValueError(
            "text_overlays must be a list of overlay items, or omitted for none")
    out = []
    for i, item in enumerate(value):
        where = f"text_overlays[{i}]"
        if not isinstance(item, dict):
            raise ValueError(f"{where} must be an object with "
                             f"{', '.join(TEXT_OVERLAY_KEYS)}")
        unknown = sorted(set(item) - set(TEXT_OVERLAY_KEYS))
        if unknown:
            raise ValueError(
                f"{where} has unknown key(s): {', '.join(unknown)}. "
                f"Known: {', '.join(TEXT_OVERLAY_KEYS)}")
        text = item.get("text")
        if not isinstance(text, str) or not text.strip():
            raise ValueError(f"{where}.text must be a non-empty string")
        try:
            y = float(item["y"])
        except (KeyError, TypeError, ValueError):
            raise ValueError(
                f"{where}.y must be a number: the fraction of frame height the "
                f"text sits at (0.0 top, 1.0 bottom)")
        if not 0.0 <= y <= 1.0:
            raise ValueError(f"{where}.y must be between 0.0 and 1.0 (got {y})")
        try:
            size = int(item["size"])
        except (KeyError, TypeError, ValueError):
            raise ValueError(f"{where}.size must be a whole number of pixels")
        if not 8 <= size <= 300:
            raise ValueError(
                f"{where}.size must be between 8 and 300 pixels (got {size})")
        clean = {"text": text, "y": y, "size": size, "from": 0.0, "until": None}
        for key in ("from", "until"):
            if item.get(key) in (None, ""):
                continue
            try:
                t = float(item[key])
            except (TypeError, ValueError):
                raise ValueError(f"{where}.{key} must be a number of seconds")
            if t < 0:
                raise ValueError(f"{where}.{key} cannot be negative (got {t})")
            clean[key] = t
        if clean["until"] is not None and clean["until"] <= clean["from"]:
            raise ValueError(
                f"{where}.until ({clean['until']}) must be later than "
                f"from ({clean['from']})")
        out.append(clean)
    return out or None


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

    if out.get("audio_enhance") not in ("voice", "off", "level"):
        raise ValueError("audio_enhance must be 'voice', 'off' or 'level'")

    # v938.15 — hook corner scale. None keeps today's layout; a float switches
    # to the measured corpus layout. Bounds are deliberately wide (the operator
    # has shipped 0.429 and 0.895) but reject nonsense.
    hc = out.get("hook_corner")
    if hc in (None, ""):
        out["hook_corner"] = None
    else:
        try:
            hc = float(hc)
        except (TypeError, ValueError):
            raise ValueError("Hook corner size must be a number")
        if hc == 0:
            # Explicit OFF: distinct from None (= AUTO — the corner rule
            # applies by itself when the job has a keyed hook + full-frame
            # background; see resolve_hook_corner). 0 blocks both the auto
            # rule and settings inheritance at queue time.
            out["hook_corner"] = 0.0
        elif not 0.20 <= hc <= 0.95:
            raise ValueError("Hook corner size must be between 0.20 and 0.95 "
                             "(0.43 matches the decoded corpus and the operator's own edit), "
                             "or exactly 0 to disable the corner layout")
        else:
            out["hook_corner"] = hc

    bg = out.get("hook_bg")
    if bg in (None, ""):
        out["hook_bg"] = None
    else:
        bg = str(bg).strip()
        if Path(bg).name != bg or Path(bg).suffix.lower() not in HOOK_BG_EXTENSIONS:
            raise ValueError("Hook background must be a plain output filename ending in "
                             "mp4, mov, png, jpg, jpeg or webp")
        out["hook_bg"] = bg

    # v944 — the text overlay. Validated HERE and not at render time: the
    # worker is the one place where a bad value costs a full download and
    # minutes of rendering before it says anything.
    ov = out.get("overlay_spec")
    if ov in (None, "", {}):
        out["overlay_spec"] = None
    elif not isinstance(ov, dict):
        raise ValueError("Overlay spec must be an object, or omitted for no overlay")
    else:
        engine = str(ov.get("overlay") or "").strip().lower()
        if engine != "readcaption":
            raise ValueError(
                "Overlay engine must be 'readcaption' (the only engine there is)")
        if not str(ov.get("overlay_age") or "").strip():
            raise ValueError("A readcaption overlay needs an age line (overlay_age)")
        block = ov.get("overlay_block") or []
        if isinstance(block, str):
            block = [p.strip() for p in block.split(" / ") if p.strip()]
        if not isinstance(block, list) or not all(isinstance(b, str) for b in block):
            raise ValueError("Overlay block must be a list of text lines")
        clean = {
            "overlay": "readcaption",
            "overlay_age": str(ov["overlay_age"]).strip(),
        }
        if block:
            clean["overlay_block"] = [b.strip() for b in block if b.strip()]
        footer = str(ov.get("overlay_footer") or "").strip()
        if footer:
            clean["overlay_footer"] = footer
        pitch = ov.get("overlay_pitch")
        if pitch not in (None, ""):
            try:
                pitch_i = int(pitch)
            except (TypeError, ValueError):
                raise ValueError("Overlay pitch must be a whole number of spec units")
            if not 30 <= pitch_i <= 120:
                raise ValueError("Overlay pitch must sit between 30 and 120 spec units")
            clean["overlay_pitch"] = pitch_i
        out["overlay_spec"] = clean

    # v960 — the three source-look keys, through the SAME validators the
    # request model uses, so the declaration, the endpoint and the worker can
    # never disagree about what is legal.
    out["caption_case"] = validate_caption_case(out.get("caption_case"))
    out["caption_words"] = validate_caption_words(out.get("caption_words"))
    out["text_overlays"] = validate_text_overlays(out.get("text_overlays"))

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


def caption_face_overlap_metrics(buckets, windows, frame_height=1920, band_height=0.17):
    """Measure caption-band overlap with every detected face.

    Returns total and worst overlap in vertical pixels. This deliberately uses
    the same one-second occupancy buckets as the placement planner, and the
    same band height the planner reserves (2 * its half of 0.085); it is a
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


def build_qc_report(checks, picture_source="export"):
    """Build the stored READY / NEEDS_MANUAL_EDIT report.

    v698A.2.5 — `picture_source` records WHICH file the captions were burned
    over: "final_broll" (the job's cutaway edit) or "export" (the speaker
    file). Top-level, next to the verdict, so the record can answer "did this
    auto-edit carry the cutaways?" without opening the video.
    """
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
        "picture_source": picture_source if picture_source in ("final_broll", "export") else "export",
    }
