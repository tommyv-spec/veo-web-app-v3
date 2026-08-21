"""Server-side caption renderer — the same korella look without a browser.

Why this exists: the pycaps renderer draws captions through a headless
Chromium. That is ~300MB of RAM and 150MB of install, which does not fit on
the 2GB Render box beside the web server and the export pipeline. libass is
already inside ffmpeg, which the server already runs.

Two side benefits over the pycaps path:
  * ONE ffmpeg pass instead of three renders spliced together — every caption
    line carries its own MarginV, so moving the captions costs nothing.
  * The font travels with the repo and is passed via `fontsdir`, so nothing
    has to be installed on the machine doing the render.

Keep this module free of cv2 / pycaps. faster-whisper is imported inside the
function that needs it (the server has it; a bare import must still work).
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

CODE_DIR = Path(__file__).resolve().parent
FONTS_DIR = CODE_DIR / "caption_templates" / "korella" / "resources"

FRAME_W, FRAME_H = 1080, 1920
BAND_HALF = 0.075          # a caption band is ~15% of frame height
SIDE_MARGIN = 90           # matches the safe-zone research (keep text off the edges)

# ASS colours are &HBBGGRR& — BLUE-GREEN-RED, the reverse of a web hex colour.
# The corpus orange #F57C0A therefore becomes 0A7CF5.
STYLES = {
    "korella":    dict(size=112, upper=True, box="&H000A7CF5&", text="&H00FFFFFF&"),
    "yellowtext": dict(size=112, upper=True, box=None, hi_text="&H0000E8FF&", text="&H00FFFFFF&"),
    "tealbox":    dict(size=112, upper=True, box="&H00D8D840&", text="&H00FFFFFF&",
                       hi_text="&H00000000&"),
}
DEFAULT_STYLE = "korella"
# The font's own subfamily is "Regular" — its weight lives in the FAMILY name.
# Asking ASS for bold (-1) makes libass look for a bold face it cannot find and
# silently fall back to Arial, which is how this shipped looking wrong once.
FONT_NAME = "Montserrat ExtraBold"
FONT_BOLD = 0


def supports(style: str) -> bool:
    return style in STYLES


def _ts(t: float) -> str:
    t = max(float(t), 0.0)
    return f"{int(t // 3600)}:{int(t % 3600 // 60):02d}:{t % 60:05.2f}"


def transcribe_words(audio_path: Path, cache: Path):
    """Word-level timings, cached so re-rendering never re-transcribes."""
    if cache.exists():
        try:
            return [tuple(w) for w in json.loads(cache.read_text())]
        except Exception:
            pass  # a corrupt cache should cost a re-run, not a failure
    from faster_whisper import WhisperModel
    model = WhisperModel("small.en", device="cpu", compute_type="int8")
    segments, _ = model.transcribe(str(audio_path), word_timestamps=True, vad_filter=False)
    words = [(w.start, w.end, w.word.strip()) for s in segments for w in (s.words or [])]
    cache.write_text(json.dumps(words))
    return words


def build_cards(words, max_words=3, gap=0.55):
    """Group words into cards; break on a long pause or sentence punctuation."""
    cards, cur = [], []
    for w in words:
        if cur and (w[0] - cur[-1][1] > gap or len(cur) >= max_words
                    or cur[-1][2].rstrip()[-1:] in ".?!,"):
            cards.append(cur)
            cur = []
        cur.append(w)
    if cur:
        cards.append(cur)
    return cards


def margin_for(t: float, windows, fallback_centre=0.59) -> int:
    """Placement windows give a band CENTRE as a fraction of height; ASS measures
    MarginV up from the BOTTOM, so convert."""
    centre = fallback_centre
    for a, b, off in windows or ():
        if a <= t < b:
            centre = 0.5 + off
            break
    return max(60, int((1.0 - centre - BAND_HALF) * FRAME_H))


def build_ass(words, windows, style_key: str, out_path: Path) -> int:
    """Write the subtitle file. One event per spoken word = true karaoke.

    No watermark here on purpose: the composed video already carries it, and
    adding a second one printed it twice over itself.
    """
    st = STYLES.get(style_key) or STYLES[DEFAULT_STYLE]
    hi_text = st.get("hi_text", st["text"])
    box = st.get("box")
    common = f"{FONT_BOLD},0,0,0,100,100,0,0"
    # BorderStyle 3 paints an opaque box behind the glyphs (the highlight);
    # BorderStyle 1 is a plain outline.
    if box:
        hi_style = (f"Style: Hi,{FONT_NAME},{st['size']},{hi_text},{hi_text},{box},{box},"
                    f"{common},3,14,0,2,{SIDE_MARGIN},{SIDE_MARGIN},430,1")
    else:
        hi_style = (f"Style: Hi,{FONT_NAME},{st['size']},{hi_text},{hi_text},"
                    f"&H00000000&,&H00000000&,{common},1,8,0,2,"
                    f"{SIDE_MARGIN},{SIDE_MARGIN},430,1")

    header = (
        "[Script Info]\n"
        "ScriptType: v4.00+\n"
        f"PlayResX: {FRAME_W}\nPlayResY: {FRAME_H}\n"
        "WrapStyle: 0\nScaledBorderAndShadow: yes\n\n"
        "[V4+ Styles]\n"
        "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, "
        "BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, "
        "BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding\n"
        f"Style: Cap,{FONT_NAME},{st['size']},{st['text']},{st['text']},&H00000000&,"
        f"&H00000000&,{common},1,8,0,2,{SIDE_MARGIN},{SIDE_MARGIN},430,1\n"
        f"{hi_style}\n\n"
        "[Events]\n"
        "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n"
    )

    events = []
    cards = build_cards(words)
    for ci, card in enumerate(cards):
        end = max(card[-1][1] + 0.12, card[0][0] + 0.4)
        if ci + 1 < len(cards):
            end = min(end, cards[ci + 1][0][0] - 0.01)   # never stack two cards
        mv = margin_for((card[0][0] + end) / 2, windows)
        for i in range(len(card)):
            a = card[i][0]
            b = card[i + 1][0] if i + 1 < len(card) else end
            if b - a < 0.02:
                continue
            parts = []
            for j, ww in enumerate(card):
                txt = ww[2].upper() if st["upper"] else ww[2]
                parts.append(f"{{\\rHi}}{txt}{{\\rCap}}" if j == i else txt)
            events.append(f"Dialogue: 0,{_ts(a)},{_ts(b)},Cap,,0,0,{mv},,{' '.join(parts)}")

    out_path.write_text(header + "\n".join(events), encoding="utf-8")
    return len(cards)


def _escape(p: Path) -> str:
    """ffmpeg filter arguments need the Windows drive colon escaped."""
    return str(p).replace("\\", "/").replace(":", "\\:")


def burn(nocap: Path, ass_path: Path, out: Path):
    out.unlink(missing_ok=True)
    r = subprocess.run(
        ["ffmpeg", "-v", "error", "-i", str(nocap),
         "-vf", f"subtitles='{_escape(ass_path)}':fontsdir='{_escape(FONTS_DIR)}'",
         "-c:v", "libx264", "-crf", "19", "-preset", "medium",
         "-c:a", "copy", "-movflags", "+faststart", "-y", str(out)],
        capture_output=True, text=True, encoding="utf-8", errors="replace")
    if r.returncode != 0 or not out.exists():
        raise RuntimeError((r.stderr or r.stdout or "")[-2000:])


def render(nocap: Path, out: Path, style: str, windows, audio_path: Path, work: Path) -> Path:
    """Full server-side caption pass: transcribe -> subtitle file -> burn."""
    words = transcribe_words(audio_path, work / "words.json")
    ass_path = work / f"captions_{style}.ass"
    cards = build_ass(words, windows, style, ass_path)
    print(f"captions: libass style={style} ({len(words)} words, {cards} cards, 1 pass)",
          flush=True)
    burn(nocap, ass_path, out)
    return out
