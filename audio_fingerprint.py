"""Audio fingerprint — is this posted reel the SAME RENDER as our export?

Text cannot decide: near-duplicate builds share the script bank verbatim.
Duration cannot decide either: twins with the same clip structure export to the
same length to the last bit (measured: two reels both 46.02000045776367s).

But they are different PERFORMANCES. The posted reel is a re-encode of the
exported mp4 — same speech, same timing. The LOUDNESS ENVELOPE (RMS per ~25ms
frame) survives re-encoding, resampling and loudness normalisation, yet is a
signature of that specific take.

Dependency-free on purpose: ffmpeg (already on PATH, see video_processor.py) +
stdlib. No chromaprint, no numpy.
"""
import array
import base64
import math
import os
import struct
import subprocess

FFMPEG_BIN = os.environ.get("FFMPEG_BIN", "ffmpeg")
SAMPLE_RATE = 8000        # speech survives 8k and keeps the vector small
FRAME_SAMPLES = 200       # 25 ms @ 8 kHz
MAX_LAG_FRAMES = 40       # +/- 1.0s alignment slack (platform head/tail trims)


def envelope_from_pcm(pcm_bytes, frame_samples=FRAME_SAMPLES):
    """s16le mono PCM -> unit-length RMS energy envelope."""
    if not pcm_bytes:
        return []
    samples = array.array("h")
    usable = len(pcm_bytes) - (len(pcm_bytes) % 2)
    samples.frombytes(pcm_bytes[:usable])
    env = []
    for start in range(0, len(samples) - frame_samples + 1, frame_samples):
        acc = 0
        for i in range(start, start + frame_samples):
            s = samples[i]
            acc += s * s
        env.append(math.sqrt(acc / frame_samples))
    norm = math.sqrt(sum(x * x for x in env))
    if norm <= 0:
        return [0.0] * len(env)
    return [x / norm for x in env]


def envelope_similarity(a, b, max_lag=MAX_LAG_FRAMES):
    """Best normalized cross-correlation in [0,1] over a small lag window.

    The lag search absorbs the head/tail trim a re-encode applies; without it a
    100ms shift would tank an otherwise perfect match.

    Each lag's correlation is WEIGHTED BY COVERAGE (how much of the shorter
    envelope the overlap explains). Without that weight the lag search is a
    footgun: a plain per-overlap NCC is scale-free, so two OPPOSITE performances
    (loud-then-quiet vs quiet-then-loud) slide into a big lag where only their
    two loud halves overlap and score a perfect 1.0 on that half. Coverage kills
    it — a match that explains half the audio is worth half a match — while a
    real re-encode (trimmed by a few frames) keeps coverage ~1.0 and is untouched.
    """
    if not a or not b:
        return 0.0
    n, m = len(a), len(b)
    shortest = min(n, m)
    best = 0.0
    for lag in range(-max_lag, max_lag + 1):
        lo = max(0, -lag)            # first i with 0 <= i+lag
        hi = min(n, m - lag)         # last i (exclusive) with i+lag < m
        if hi <= lo:
            continue
        num = ea = eb = 0.0
        for i in range(lo, hi):
            x = a[i]
            y = b[i + lag]
            num += x * y
            ea += x * x
            eb += y * y
        denom = math.sqrt(ea * eb)
        if denom <= 0:
            continue
        coverage = (hi - lo) / float(shortest)
        best = max(best, (num / denom) * coverage)
    return max(0.0, min(1.0, best))


def encode_fingerprint(env):
    """Envelope -> compact base64 (float32) for a TEXT column."""
    if not env:
        return ""
    return base64.b64encode(struct.pack(f"<{len(env)}f", *env)).decode("ascii")


def decode_fingerprint(blob):
    if not blob:
        return []
    raw = base64.b64decode(blob)
    n = len(raw) // 4
    return list(struct.unpack(f"<{n}f", raw[:n * 4]))


def fingerprint_media(path_or_url):
    """Media file OR URL -> fingerprint blob. '' on any failure, never raises.

    ffmpeg reads http(s) directly, so an Instagram CDN url needs no pre-download.
    Decodes to 8kHz mono s16le on stdout — no temp file.
    """
    try:
        proc = subprocess.run(
            [FFMPEG_BIN, "-v", "error", "-i", str(path_or_url),
             "-vn", "-ac", "1", "-ar", str(SAMPLE_RATE), "-f", "s16le", "-"],
            capture_output=True, timeout=300,
        )
        if proc.returncode != 0:
            print(f"[audio-fp] ffmpeg failed: {(proc.stderr or b'')[:180]!r}", flush=True)
            return ""
        return encode_fingerprint(envelope_from_pcm(proc.stdout))
    except Exception as e:
        print(f"[audio-fp] fingerprint failed: {e}", flush=True)
        return ""
