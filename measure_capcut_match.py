"""Score an audio or video file against CapCut's measured tonal curve.

Why this exists
---------------
The auto-edit pipeline's voice chain used to be tuned by ear and defended with
a made-up metric ("presence-to-mud"). That metric rewarded removing the voice's
body and adding treble, so optimising it drove the sound AWAY from CapCut while
the number went up. The operator heard the result as thin and brittle.

The fix was to stop guessing and measure the target. Five videos existed on the
operator's machine in both states: the platform export they dropped into CapCut,
and the file CapCut wrote back out. Same speech, one editing pass in between.
Averaging the long-term speech spectrum of both sides gives what CapCut actually
does, per band, in dB:

     60-120 Hz  -13.8      2-3 kHz   +1.1
    120-250 Hz   -0.3      3-5 kHz   +1.1
    250-500 Hz   +0.3      5-8 kHz   -1.0
    500-1k Hz    +0.0      8-12 kHz  -1.9
      1-2 kHz    -0.0     12-16 kHz  -0.5

One substantial move: lose the sub-120 Hz rumble. Everything else is under 2 dB.
There is no neural resynthesis to chase and no tool to buy.

TARGET below is the resulting shape of CapCut's output, expressed relative to
the 500-2000 Hz speech body so that pure level changes do not affect it.

Usage
-----
    python code/measure_capcut_match.py <file.wav|file.mp4> [more files...]

    # re-derive TARGET from your own before/after pairs:
    python code/measure_capcut_match.py --derive before1.mp4 after1.mp4 [b2 a2 ...]
"""
import subprocess
import sys

import numpy as np

SR = 48000
BANDS = [(60, 120), (120, 250), (250, 500), (500, 1000), (1000, 2000),
         (2000, 3000), (3000, 5000), (5000, 8000), (8000, 12000), (12000, 16000)]

# CapCut's output shape, relative to the 500-2000 Hz speech body.
TARGET = np.array([-13.9, +9.1, +6.3, +2.7, -2.7, -8.3, -15.9, -20.7, -22.8, -32.7])

# Bands a listener actually judges a voice on. Sub-120 Hz is rumble that gets
# lost on a phone speaker either way, so it carries little weight; the body and
# presence bands decide whether it sounds like CapCut or not.
WEIGHTS = np.array([0.4, 1.5, 1.0, 1.0, 1.0, 1.0, 1.0, 1.2, 1.2, 0.5])


def _pcm(path):
    r = subprocess.run(["ffmpeg", "-v", "error", "-i", str(path), "-ac", "1",
                        "-ar", str(SR), "-f", "f32le", "-"], capture_output=True)
    if not r.stdout:
        raise SystemExit(f"no audio decoded from {path}")
    return np.frombuffer(r.stdout, dtype=np.float32)


def band_curve(path, nfft=8192):
    """Long-term average spectrum over speech-active frames, in dB per band,
    normalised to the 500-2000 Hz body so level changes drop out."""
    x = _pcm(path)
    hop = nfft // 2
    win = np.hanning(nfft)
    frames = [np.abs(np.fft.rfft(x[s:s + nfft] * win))
              for s in range(0, len(x) - nfft, hop)
              if float(np.sqrt((x[s:s + nfft] ** 2).mean())) >= 0.01]
    if not frames:
        raise SystemExit(f"no speech-level audio found in {path}")
    spec = np.mean(frames, axis=0)
    freq = np.fft.rfftfreq(nfft, 1 / SR)
    db = np.array([20 * np.log10(max(spec[(freq >= lo) & (freq < hi)].mean(), 1e-12))
                   for lo, hi in BANDS])
    return db - np.mean([db[3], db[4]])


def score(curve):
    """Weighted deviation from CapCut, in dB. Lower is closer."""
    return float(np.sqrt((WEIGHTS * (curve - TARGET) ** 2).sum() / WEIGHTS.sum()))


def _header():
    return "".join(f"{lo}-{hi}".rjust(8) for lo, hi in BANDS)


def _row(label, values, extra=""):
    return f"{label:26s}" + "".join(f"{v:+8.1f}" for v in values) + extra


def derive(pairs):
    """Re-derive CapCut's curve from your own before/after files."""
    afters, deltas = [], []
    for before, after in pairs:
        cb, ca = band_curve(before), band_curve(after)
        afters.append(ca)
        deltas.append(ca - cb)
        print(_row(f"  delta {after.split('/')[-1][:18]}", ca - cb))
    print()
    print(_row("CapCut mean delta", np.mean(deltas, axis=0)))
    print(_row("CapCut mean shape", np.mean(afters, axis=0), "   <- TARGET"))


def main(argv):
    if len(argv) > 1 and argv[1] == "--derive":
        rest = argv[2:]
        if len(rest) < 2 or len(rest) % 2:
            raise SystemExit("--derive needs before/after pairs")
        print(f"{'':26s}{_header()}")
        derive(list(zip(rest[::2], rest[1::2])))
        return 0

    if len(argv) < 2:
        raise SystemExit(__doc__)

    print(f"{'':26s}{_header()}{'score':>8s}")
    print(_row("TARGET (CapCut)", TARGET))
    print("-" * (26 + 8 * len(BANDS) + 8))
    worst = 0.0
    for path in argv[1:]:
        curve = band_curve(path)
        value = score(curve)
        worst = max(worst, value)
        name = path.replace("\\", "/").split("/")[-1][:26]
        print(_row(name, curve, f"{value:8.2f}"))
    print("\nscore = weighted dB deviation from CapCut. For reference: the chain "
          "shipped before 2026-08-22 scored 3.47; the current one scores ~1.4.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
