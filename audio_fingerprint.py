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
MAX_LAG_FRAMES = 40       # legacy fixed window (kept for callers that pass it)
LAG_SLACK_FRAMES = 240    # 6.0s of slack ON TOP of the length difference


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


def auto_max_lag(n, m, slack_frames=LAG_SLACK_FRAMES):
    """How far we must be willing to slide one envelope against the other.

    THE bug this exists to prevent: the operator TRIMS the export before posting.
    A cut of N seconds shifts the audio by N seconds, so the true alignment sits
    OUTSIDE a narrow window and the correct match scores at the NOISE FLOOR.
    Measured on a real file: the same video against its own 5s-trimmed copy
    scored 0.582 at +/-1s (indistinguishable from an unrelated video) and 1.000
    at +/-5s. A fixed 1s window silently failed on every trimmed video.

    So the window is derived, not guessed: it must at least cover the LENGTH
    DIFFERENCE (which is exactly how much was cut), plus slack for where the cut
    was made.
    """
    return abs(n - m) + max(0, int(slack_frames))


def envelope_similarity(a, b, max_lag=None):
    """Best coverage-weighted normalized cross-correlation in [0, 1].

    `max_lag=None` derives the window from the length difference (see
    auto_max_lag) — the right default, because the amount trimmed IS the shift.

    Each lag's correlation is WEIGHTED BY COVERAGE (how much of the shorter
    envelope the overlap explains). Without that weight the lag search is a
    footgun: a plain per-overlap NCC is scale-free, so two OPPOSITE performances
    (loud-then-quiet vs quiet-then-loud) slide into a big lag where only their
    two loud halves overlap and score a perfect 1.0 on that half. Coverage kills
    it — a match that explains half the audio is worth half a match — while a
    genuinely trimmed copy keeps coverage ~1.0 and is untouched. Coverage is what
    makes a WIDE window safe.

    Uses numpy (already a dependency) for an O(n log n) FFT correlation, since a
    wide window makes the naive O(lag x frames) loop far too slow for the request
    path. Falls back to the exact same maths in pure Python if numpy is absent.
    """
    if not a or not b:
        return 0.0
    n, m = len(a), len(b)
    if max_lag is None:
        max_lag = auto_max_lag(n, m)
    shortest = min(n, m)

    try:
        import numpy as np
    except ImportError:
        return _envelope_similarity_py(a, b, max_lag, shortest)

    A = np.asarray(a, dtype=np.float64)
    B = np.asarray(b, dtype=np.float64)
    # We want num(lag) = sum_i A[i] * B[i + lag].
    #   irfft(rfft(B) * conj(rfft(A)))[k] == sum_i B[i + k] * A[i]  == num(k)
    # The operand order matters: the other way round yields num(-k), which mirrors
    # every lag and quietly reports the wrong alignment.
    size = 1 << int(np.ceil(np.log2(n + m)))
    corr = np.fft.irfft(np.fft.rfft(B, size) * np.conj(np.fft.rfft(A, size)), size)
    # Overlap energy per lag, from prefix sums (so each lag is normalized over
    # exactly the frames it actually overlaps — not the whole vector).
    ca = np.concatenate(([0.0], np.cumsum(A * A)))
    cb = np.concatenate(([0.0], np.cumsum(B * B)))

    # Lags with a non-empty overlap: lo = max(0, -lag) < hi = min(n, m - lag).
    #   lag > 0 needs lag < m  -> lag <= m - 1
    #   lag < 0 needs -lag < n -> lag >= -(n - 1)
    # so the range is [-(n-1), m-1]. Getting these the wrong way round searches
    # the wrong side of the alignment and silently misses the true match.
    best = 0.0
    lo_lag, hi_lag = -min(max_lag, n - 1), min(max_lag, m - 1)
    for lag in range(lo_lag, hi_lag + 1):
        lo = max(0, -lag)
        hi = min(n, m - lag)
        if hi <= lo:
            continue
        num = corr[lag % size]          # sum_i A[i] * B[i + lag]
        ea = ca[hi] - ca[lo]
        eb = cb[hi + lag] - cb[lo + lag]
        denom = math.sqrt(ea * eb)
        if denom <= 0:
            continue
        coverage = (hi - lo) / float(shortest)
        best = max(best, (num / denom) * coverage)
    return max(0.0, min(1.0, float(best)))


def _envelope_similarity_py(a, b, max_lag, shortest):
    """Pure-Python fallback — identical maths, no numpy."""
    n, m = len(a), len(b)
    best = 0.0
    for lag in range(-max_lag, max_lag + 1):
        lo = max(0, -lag)
        hi = min(n, m - lag)
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
