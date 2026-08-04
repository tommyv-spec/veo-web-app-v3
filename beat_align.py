#!/usr/bin/env python3
"""beat_align.py — author-time beat alignment for a videos/*.md build.

WHY THIS IS NOT A RUNTIME MODULE
--------------------------------
This is the bridge between a build md and the AutoEditing beat planner
(`C:/Users/tomma/Documents/AutoEditing/beat_drop_aligner_v5.py`). It NEVER runs
on Render and `librosa` is deliberately NOT in requirements.txt.

Reason: v5's analysis computes an HPSS split plus three independent full-song
STFTs (drop detection, beat salience, chroma). Measured peak RSS for a 3-minute
track is ~0.5-1 GB. The render box is 2 GB with a documented OOM history, and
`main.py` must stay importable without librosa — so the librosa import lives
INSIDE `analyze_song()`, not at module scope. Importing this file on the server
is harmless; calling `analyze_song` there is not.

THE SPLIT
---------
    author machine                       Render
    --------------                       ------
    analyse song (librosa)   ──plan──>   ffmpeg trim to target_duration_s
    solve cut boundaries                 (the existing v668 `_trim_one` path,
    write target_duration_s               unchanged — no new dependency)

Everything expensive happens here; the platform only reads seconds.

TWO MODES
---------
`snap`  (default, safe for narrative builds) — keep the authored durations and
        move each CUT BOUNDARY to the nearest strong beat. Durations shift by
        a few hundred ms at most, so captions and shown beats stay where the
        author put them. This is the mode for a build like elder76 where each
        clip's length was chosen to match a decoded source.

`solve` (music-led) — the full v5 dynamic-programming solve: the music chooses
        every duration inside [min, max], clips accelerate into the drop, and a
        nominated clip lands ON the drop. Authored durations are DISCARDED.
        This is the mode for a montage, and it will desynchronise anything
        timed to the original cut lengths.

Pure logic lifted from beat_drop_aligner_v5.py (Tommaso's project): robust_z,
estimate_downbeats, compute_beat_salience, detect_drop_candidates,
build_pacing_targets, segment_choice_score, select_boundaries_before/after_drop.
Credited inline at each function.

USAGE
    python code/beat_align.py videos/<build>.md --song path/to/song.mp3
    python code/beat_align.py videos/<build>.md --song s.mp3 --mode solve --drop-clip 5
    python code/beat_align.py videos/<build>.md --song s.mp3 --write     # patch the md
"""
from __future__ import annotations

import argparse
import json
import math
import re
import sys
from functools import lru_cache
from pathlib import Path

import numpy as np

HOP = 512


# ----------------------------------------------------------------- md parsing

SCENE_RE = re.compile(r"^###\s+Scene\s+(\d+)\s*$", re.M)
DUR_RE = re.compile(r"^-\s+\*\*target_duration_s:\*\*\s*([\d.]+)", re.M)
RENDER_RE = re.compile(r"^-\s+\*\*clip_duration_s:\*\*\s*(\d+)", re.M)

# v861 render buckets. A clip is RENDERED at one of these and then trimmed DOWN
# to target_duration_s. There is no trim-up: see check_headroom.
ALLOWED_RENDER_S = (4, 6, 8, 10)


def read_build(md_path: Path):
    """Ordered scenes. Returns (text, [(scene_no, target_dur, render_bucket|None), ...])."""
    text = md_path.read_text(encoding="utf-8")
    # Split on scene headers so each scene's own bullets stay with it.
    parts = SCENE_RE.split(text)
    if len(parts) < 3:
        raise SystemExit("no `### Scene N` headers found in %s" % md_path)
    scenes = []
    for i in range(1, len(parts), 2):
        num = int(parts[i])
        body = parts[i + 1]
        m = DUR_RE.search(body)
        if not m:
            raise SystemExit("Scene %d has no `- **target_duration_s:**` bullet" % num)
        r = RENDER_RE.search(body)
        scenes.append((num, float(m.group(1)), int(r.group(1)) if r else None))
    return text, scenes


def required_bucket(target_s: float):
    """Smallest v861 bucket that can hold `target_s`, or None if none can."""
    for b in ALLOWED_RENDER_S:
        if b >= target_s - 1e-6:
            return b
    return None


def check_headroom(scenes, targets, job_default=None):
    """THE guard. A clip is rendered at a fixed bucket and trimmed DOWN only.

    ffmpeg `-t 5.9` against a 4.0s file returns 4.0s with no error and no
    warning (verified empirically 2026-08-04). The concat then drifts and every
    later cut is off the beat, silently. So any aligned target longer than the
    clip's render bucket is a HARD failure here, at plan time, where it is still
    cheap to fix.

    Returns (problems, needed) — `needed` maps scene -> the bucket it must be
    rendered at for the plan to survive.
    """
    problems, needed = [], {}
    for (num, _authored, bucket), tgt in zip(scenes, targets):
        have = bucket if bucket is not None else job_default
        want = required_bucket(tgt)
        if want is None:
            problems.append("Scene %d needs %.2fs but the longest render bucket is %ds"
                            % (num, tgt, ALLOWED_RENDER_S[-1]))
            continue
        if have is None:
            needed[num] = want
            problems.append("Scene %d declares no `clip_duration_s` and no --job-duration "
                            "was given; it needs >= %ds to hold %.2fs" % (num, want, tgt))
        elif tgt > have + 1e-6:
            needed[num] = want
            problems.append("Scene %d renders at %ds but the plan wants %.2fs — ffmpeg "
                            "would silently return %ds and drift the whole reel; set "
                            "`clip_duration_s: %d`" % (num, have, tgt, have, want))
    return problems, needed


def write_durations(text: str, new_durs: list[float]) -> str:
    """Rewrite each scene's target_duration_s in order. Count must match."""
    parts = SCENE_RE.split(text)
    out = [parts[0]]
    k = 0
    for i in range(1, len(parts), 2):
        num, body = parts[i], parts[i + 1]
        body = DUR_RE.sub(
            lambda m, d=new_durs[k]: "- **target_duration_s:** %.3f" % d, body, count=1)
        out.append("### Scene %s\n" % num)
        out.append(body)
        k += 1
    return "".join(out)


# ------------------------------------------------------- analysis (librosa)

def analyze_song(song: Path, beats_per_bar: int = 4, sr_target: int = 22050) -> dict:
    """Beat grid + per-beat salience + ranked drops.

    librosa is imported HERE, not at module scope, so this file stays importable
    on a box that does not have it. `sr_target` downsamples on load (v5 uses
    sr=None); at 22.05 kHz the STFTs are half the size and beat/drop detection
    is unaffected, which is the cheapest of the memory mitigations.
    """
    try:
        import librosa
        from scipy.ndimage import gaussian_filter1d
        from scipy.signal import find_peaks
    except ImportError as exc:
        raise SystemExit(
            "beat_align needs librosa + scipy on the AUTHOR machine "
            "(never on Render): pip install librosa scipy\n  %s" % exc)

    y, sr = librosa.load(str(song), sr=sr_target, mono=True)
    y_h, y_p = librosa.effects.hpss(y)
    onset_env = librosa.onset.onset_strength(y=y_p, sr=sr, hop_length=HOP,
                                             aggregate=np.median)
    tempo, beat_frames = librosa.beat.beat_track(
        onset_envelope=onset_env, sr=sr, hop_length=HOP, units="frames", trim=False)
    tempo = float(np.atleast_1d(tempo)[0])
    beat_times = librosa.frames_to_time(beat_frames, sr=sr, hop_length=HOP)

    # ONE spectrogram, reused by salience and drop detection. v5 computes three
    # independent full-song STFTs (v5.py:377, :535, :540); that is the single
    # biggest memory cost and it is pure waste.
    S = np.abs(librosa.stft(y, n_fft=2048, hop_length=HOP))
    freqs = librosa.fft_frequencies(sr=sr, n_fft=2048)
    rms = librosa.feature.rms(S=S, frame_length=2048, hop_length=HOP)[0]
    bass = S[freqs <= 180.0].sum(axis=0)

    def rz(x):  # robust z, v5.py:328
        x = np.asarray(x, dtype=float)
        med = np.median(x)
        mad = np.median(np.abs(x - med))
        return (x - med) / (1.4826 * mad + 1e-9)

    # --- downbeat PHASE, v5.py:485. Test every phase, keep the most energetic.
    best_phase, best_energy = 0, -np.inf
    for phase in range(beats_per_bar):
        idx = beat_frames[phase::beats_per_bar]
        if len(idx) == 0:
            continue
        e = float(onset_env[np.clip(idx, 0, len(onset_env) - 1)].sum())
        if e > best_energy:
            best_phase, best_energy = phase, e
    is_downbeat = np.zeros(len(beat_times), dtype=bool)
    is_downbeat[best_phase::beats_per_bar] = True

    # --- per-beat salience, v5.py:509 weights
    bi = np.clip(beat_frames, 0, S.shape[1] - 1)
    onset_at = rz(onset_env[np.clip(beat_frames, 0, len(onset_env) - 1)])
    rms_rise = rz(np.diff(np.concatenate([[rms[0]], rms]))[bi])
    bass_rise = rz(np.diff(np.concatenate([[bass[0]], bass]))[bi])
    chroma = librosa.feature.chroma_stft(S=S, sr=sr)
    nov = np.concatenate([[0.0], np.linalg.norm(np.diff(chroma, axis=1), axis=0)])
    chroma_nov = rz(nov[bi])
    raw = (0.38 * onset_at + 0.22 * rms_rise + 0.18 * bass_rise
           + 0.12 * chroma_nov + 0.85 * is_downbeat.astype(float))
    lo, hi = np.percentile(raw, 5), np.percentile(raw, 95)
    salience = np.clip((raw - lo) / (hi - lo + 1e-9), 0.0, 1.0)

    # --- drops, v5.py:358 (pre/post contrast on smoothed envelopes)
    def jump(sig):
        s = gaussian_filter1d(np.log1p(np.maximum(sig, 0)), sigma=3)
        fps_ = sr / HOP
        pre_a, pre_b, post = int(1.8 * fps_), int(0.35 * fps_), int(0.75 * fps_)
        out = np.zeros_like(s)
        for i in range(len(s)):
            a0, a1 = max(0, i - pre_a), max(0, i - pre_b)
            b0, b1 = i, min(len(s), i + post)
            if a1 > a0 and b1 > b0:
                out[i] = s[b0:b1].mean() - s[a0:a1].mean()
        return out

    combined = (0.32 * rz(jump(rms)) + 0.28 * rz(jump(bass))
                + 0.25 * rz(jump(onset_env[:len(rms)])) + 0.15 * rz(np.log1p(rms)))
    combined = gaussian_filter1d(combined, sigma=2)
    peaks, props = find_peaks(combined, distance=int(3.0 * sr / HOP), prominence=0.5)
    # Drop the first ~1.5s: a track that opens loud produces a spurious rank-1
    # "drop" at t=0 from the silence-to-signal edge, which would anchor a solve
    # on the very first beat.
    if len(peaks):
        peaks = peaks[librosa.frames_to_time(peaks, sr=sr, hop_length=HOP) > 1.5]
    order = np.argsort(-combined[peaks])[:8] if len(peaks) else []
    drops = [{"rank": r + 1,
              "time_seconds": float(librosa.frames_to_time(peaks[i], sr=sr, hop_length=HOP)),
              "score": float(combined[peaks[i]])}
             for r, i in enumerate(order)]

    return {"bpm": tempo, "beats_per_bar": beats_per_bar,
            "beat_times": beat_times.tolist(), "beat_salience": salience.tolist(),
            "is_downbeat": is_downbeat.tolist(), "drops": drops,
            "duration_s": float(len(y) / sr), "sample_rate": int(sr)}


# ------------------------------------------------------------- mode: snap

def snap_boundaries(scenes, beat_times, salience, start_time=0.0, tol_beats=0.6):
    """Keep authored durations; move each CUT to a nearby beat, preferring a strong one.

    Each boundary snaps to its OWN ideal absolute time (running authored total
    from `start_time`), so snapping error never compounds down the reel.

    Within +/-`tol_beats` of that ideal we take the MOST SALIENT beat; only when
    the window is empty do we fall back to the nearest beat of any strength. The
    window is what makes this a snap: no cut can move more than ~0.6 of a beat,
    so a 2.0s clip stays about 2.0s and the author's shot lengths survive.

    An earlier version filtered to a global strong-beat set and then took the
    nearest — with a sparse strong set that reached 1.6s away and turned a
    4.17s clip into 2.02s. Bound the move, then optimise inside the bound.
    """
    beat_times = np.asarray(beat_times, dtype=float)
    salience = np.asarray(salience, dtype=float)
    if len(beat_times) == 0:
        raise SystemExit("no beats detected")
    period = float(np.median(np.diff(beat_times))) if len(beat_times) > 1 else 0.5
    tol = tol_beats * period

    edges = [start_time]
    ideal = start_time
    # Index rather than unpack: callers pass (scene, dur) or (scene, dur, bucket)
    # and this function only ever needs the duration.
    for s in scenes:
        ideal += s[1]
        floor_ = edges[-1] + 0.20  # never produce a zero/negative-length clip
        window = np.where((np.abs(beat_times - ideal) <= tol) & (beat_times > floor_))[0]
        if len(window):
            pick = int(window[np.argmax(salience[window])])
        else:
            pool = np.where(beat_times > floor_)[0]
            if len(pool) == 0:
                edges.append(ideal)
                continue
            pick = int(pool[np.abs(beat_times[pool] - ideal).argmin()])
        edges.append(float(beat_times[pick]))
    return edges


# ------------------------------------------------------------ mode: solve

def _pacing_targets(count, lo, hi, before):
    """v5.py:589 — energy curve + zig-zag so consecutive clips differ in length."""
    if count <= 0:
        return []
    curve = (np.linspace(0.90, 0.12, count) if before else np.linspace(0.20, 0.58, count))
    patt = np.array([0.15, 0.55, 0.25, 0.75, 0.35, 0.95, 0.45, 0.65, 0.05])
    pattern = np.array([patt[i % len(patt)] for i in range(count)])
    mix = 0.55 * curve + 0.45 * pattern
    return (lo + mix * (hi - lo)).tolist()


def _seg_score(sal, dur, target, lo, hi):
    """v5.py:614 — salience dominates duration fit 2.8 vs 1.6, deliberately."""
    rng = max(hi - lo, 1e-6)
    return 2.8 * sal + 1.6 * (-abs(dur - target) / rng)


def solve_boundaries(beat_times, salience, anchor_idx, count, lo, hi, before):
    """v5.py:630 / :739 — memoized DP over (position, beat index), globally optimal.

    Returns beat INDICES, ordered. `before=True` walks right-to-left ending at
    the anchor so the nominated clip lands exactly on the drop; `before=False`
    walks forward from it.
    """
    bt = np.asarray(beat_times, dtype=float)
    sal = np.asarray(salience, dtype=float)
    targets = _pacing_targets(count, lo, hi, before)
    if count == 0:
        return []

    @lru_cache(maxsize=None)
    def solve(pos, idx):
        if pos == count:
            return (0.0, ())
        best = None
        if before:
            rng = range(idx - 1, -1, -1)
        else:
            rng = range(idx + 1, len(bt))
        for j in rng:
            dur = abs(bt[idx] - bt[j])
            if dur < lo:
                continue
            if dur > hi:
                break
            tgt = targets[count - 1 - pos] if before else targets[pos]
            sc = _seg_score(float(sal[j]), dur, tgt, lo, hi)
            sub = solve(pos + 1, j)
            if sub is None:
                continue
            tot = sc + sub[0]
            if best is None or tot > best[0]:
                best = (tot, (j,) + sub[1])
        return best if best is not None else (float("-inf"), ())

    score, idxs = solve(0, anchor_idx)
    if not math.isfinite(score):
        raise SystemExit(
            "could not place %d clips between %.2fs and %.2fs per clip — "
            "widen --min/--max" % (count, lo, hi))
    seq = list(idxs)
    return sorted(seq + [anchor_idx]) if before else [anchor_idx] + seq


# ------------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("build_md", type=Path)
    ap.add_argument("--song", type=Path, required=True)
    ap.add_argument("--mode", choices=["snap", "solve"], default="snap")
    ap.add_argument("--start-at", type=float, default=None,
                    help="song time of the first cut (default: first downbeat)")
    ap.add_argument("--drop-clip", type=int, default=None,
                    help="solve mode: 1-based scene that lands ON the drop")
    ap.add_argument("--drop-rank", type=int, default=1)
    ap.add_argument("--drop-time", type=float, default=None)
    ap.add_argument("--min", dest="lo", type=float, default=0.5)
    ap.add_argument("--max", dest="hi", type=float, default=2.0)
    ap.add_argument("--beats-per-bar", type=int, default=4)
    ap.add_argument("--job-duration", type=int, default=None, choices=ALLOWED_RENDER_S,
                    help="render bucket for scenes with no `clip_duration_s` bullet")
    ap.add_argument("--write", action="store_true",
                    help="patch target_duration_s in the md (default: report only)")
    ap.add_argument("--allow-drift", action="store_true",
                    help="DANGEROUS: write a plan whose targets exceed the render "
                         "buckets. ffmpeg cannot trim UP, so the reel will drift.")
    ap.add_argument("--plan-out", type=Path, default=None)
    a = ap.parse_args()

    text, scenes = read_build(a.build_md)
    n = len(scenes)
    print("build   : %s" % a.build_md.name)
    print("scenes  : %d   authored total %.2fs" % (n, sum(d for _, d, _ in scenes)))

    an = analyze_song(a.song, beats_per_bar=a.beats_per_bar)
    bt, sal = np.array(an["beat_times"]), np.array(an["beat_salience"])
    bar = 4 * 60.0 / an["bpm"]
    print("song    : %s" % a.song.name)
    print("tempo   : %.1f BPM   beat %.3fs   bar %.3fs   %d beats"
          % (an["bpm"], 60.0 / an["bpm"], bar, len(bt)))
    if an["drops"]:
        print("drops   : " + ", ".join("#%d @ %.2fs" % (d["rank"], d["time_seconds"])
                                       for d in an["drops"][:4]))

    if a.start_at is not None:
        start = a.start_at
    else:
        db = bt[np.array(an["is_downbeat"])]
        start = float(db[0]) if len(db) else float(bt[0])

    if a.mode == "snap":
        edges = snap_boundaries(scenes, bt, sal, start_time=start)
    else:
        if not a.drop_clip:
            raise SystemExit("--mode solve requires --drop-clip N (which scene hits the drop)")
        if a.drop_time is not None:
            dt = a.drop_time
        else:
            if not an["drops"]:
                raise SystemExit("no drop detected; pass --drop-time")
            dt = an["drops"][min(a.drop_rank, len(an["drops"])) - 1]["time_seconds"]
        anchor = int(np.abs(bt - dt).argmin())
        print("drop    : %.2fs -> beat %d @ %.2fs" % (dt, anchor, bt[anchor]))
        # drop_clip STARTS on the drop, so it belongs to the after-block:
        # before = clips 1..drop_clip-1, after = clips drop_clip..n.
        before_count = a.drop_clip - 1
        after_count = n - a.drop_clip + 1
        pre = solve_boundaries(bt, sal, anchor, before_count, a.lo, a.hi, before=True)
        post = solve_boundaries(bt, sal, anchor, after_count, a.lo, a.hi, before=False)
        # pre ends at the anchor and post starts at it — drop the shared copy.
        edges = [float(bt[i]) for i in pre[:-1]] + [float(bt[i]) for i in post]
        if len(edges) != n + 1:
            raise SystemExit("internal: built %d edges for %d clips" % (len(edges), n))
        start = edges[0]

    durs = [round(edges[i + 1] - edges[i], 3) for i in range(n)]

    period = 60.0 / an["bpm"]
    # A single CUT moves at most tol; a DURATION sits between two cuts that can
    # move opposite ways, so its bound is 2*tol. Warning on tol alone would fire
    # on every legitimate snap.
    dur_bound = 2 * 0.6 * period
    loose = []
    print("\n%-6s %9s %9s %9s   %6s  %s" % ("scene", "authored", "aligned", "delta",
                                            "bars", "note"))
    for i, ((num, old, _bk), new) in enumerate(zip(scenes, durs), 1):
        note = ""
        if a.mode == "snap" and abs(new - old) > dur_bound + 1e-6:
            note = "OUT OF SNAP WINDOW (no beat in reach)"
            loose.append(num)
        print("%-6d %9.2f %9.2f %+9.2f   %6.2f  %s" % (num, old, new, new - old,
                                                       new / bar, note))
    print("%-6s %9.2f %9.2f %+9.2f" % ("TOTAL", sum(d for _, d, _ in scenes),
                                       sum(durs), sum(durs) - sum(d for _, d, _ in scenes)))
    if loose:
        print("\nWARNING scene(s) %s could not snap inside +/-%.2fs — usually the track "
              "ends before the reel does (song %.1fs, last beat %.1fs). Use a longer "
              "track or --start-at earlier."
              % (", ".join(str(s) for s in loose), dur_bound,
                 an["duration_s"], bt[-1]))
    print("\nmusic starts at %.3fs — lay the track with that offset so cut 1 is on the grid."
          % start)

    plan = {"build": str(a.build_md), "song": str(a.song), "mode": a.mode,
            "bpm": an["bpm"], "bar_seconds": bar, "music_source_start": start,
            "music_source_end": edges[-1], "drops": an["drops"][:4],
            "clips": [{"scene": num, "authored_duration": old, "target_duration": new,
                       "timeline_start": round(edges[i], 3),
                       "timeline_end": round(edges[i + 1], 3),
                       "output_start": round(edges[i] - start, 3)}
                      for i, ((num, old, _bk), new) in enumerate(zip(scenes, durs))]}
    # --- HEADROOM GUARD. Must run before anything is written.
    problems, needed = check_headroom(scenes, durs, job_default=a.job_duration)
    if problems:
        print("\n" + "=" * 70)
        print("HEADROOM FAIL — %d scene(s) want more than they render" % len(problems))
        print("=" * 70)
        for p in problems:
            print("  " + p)
        if needed:
            print("\nSet these before rendering, then re-run:")
            for scene, bucket in sorted(needed.items()):
                print("  Scene %-3d  - **clip_duration_s:** %d" % (scene, bucket))
        print("\nWhy this is fatal: ffmpeg trims DOWN only. `-t 5.9` on a 4.0s clip\n"
              "returns 4.0s with no error, the concat drifts, and every later cut\n"
              "falls off the beat with nothing in the logs.")
        if not a.allow_drift:
            raise SystemExit(1)
        print("\n--allow-drift set — writing anyway. The reel WILL drift.")
    else:
        print("\nheadroom : OK — every target fits inside its render bucket")

    out = a.plan_out or a.build_md.with_suffix(".beatplan.json")
    plan["headroom_ok"] = not problems
    plan["required_clip_duration_s"] = {str(k): v for k, v in needed.items()}
    out.write_text(json.dumps(plan, indent=2), encoding="utf-8")
    print("plan    : %s" % out)

    if a.write:
        a.build_md.write_text(write_durations(text, durs), encoding="utf-8")
        print("WROTE   : target_duration_s patched in %s" % a.build_md.name)
    else:
        print("(dry run — pass --write to patch the md)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
