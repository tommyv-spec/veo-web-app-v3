"""v925 — b-roll must come out the same length as the delivered speaker file.

The bug this locks down (found 2026-08-08 from the operator's own downloads):

    final_broll_d8051bf6_20260808_112658  88.511s
    final_export_d8051bf6_20260808_112658 97.291s   ratio 1.0992

The b-roll pipeline used to run BEFORE the speaker's speed pass, so it had to
PREDICT the speed by re-deriving the speed pass's condition. It re-derived only
part of it: the speaker's gate also carries a max-duration OOM guard (and can
fail non-fatally). When that guard fired, the speaker stayed 1.0x while the
b-roll had already been rendered against a 1.1x master → every b-roll frame
lands ~10% early, growing to seconds by the end.

v925 removes the prediction: the master audio is extracted from the FINISHED
speaker file, and the per-clip targets are scaled by a MEASURED ratio
(final speaker duration / sum of the per-clip durations the targets came from).

This test simulates the guard firing (speaker NOT sped) and asserts:
  - the OLD predicted scaling (1/playback_speed) produces the reported failure
  - the NEW measured scaling produces a b-roll that matches the speaker

Needs ffmpeg. Skips without it.
"""

import subprocess
import json
import shutil
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    shutil.which("ffmpeg") is None or shutil.which("ffprobe") is None,
    reason="ffmpeg/ffprobe not installed",
)

# Per-clip post-VAD durations, deliberately not frame-aligned — real Whisper-VAD
# cuts land wherever the speech ends, never on a 24fps grid.
CLIP_DURATIONS = [2.417, 1.583, 2.251, 1.877, 2.334, 1.019]
BROLL_SLOTS = [1, 3, 5]          # which speaker slots have a visual_pair partner
PLAYBACK_SPEED = 1.1
LENGTH_TOLERANCE_S = 0.15        # same tolerance the export logs FAIL on


def _probe_duration(path: Path) -> float:
    out = subprocess.run(
        ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", str(path)],
        capture_output=True, text=True,
    ).stdout
    return float(json.loads(out)["format"]["duration"])


def _make_clip(path: Path, duration: float, freq: int) -> None:
    subprocess.run(
        ["ffmpeg", "-y", "-loglevel", "error",
         "-f", "lavfi", "-i", f"testsrc=size=270x480:rate=24:d={duration + 1:.3f}",
         "-f", "lavfi", "-i", f"sine=frequency={freq}:duration={duration + 1:.3f}",
         "-t", f"{duration:.6f}",
         "-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
         "-c:a", "aac", "-b:a", "96k", "-ar", "48000", "-ac", "2", str(path)],
        capture_output=True,
    )


def _targets_from(durations, slots, scale):
    """Cumulative per-clip windows for the given slots, scaled by `scale`."""
    starts, cursor = [], 0.0
    for d in durations:
        starts.append((cursor, cursor + d))
        cursor += d
    return [
        {"start": s * scale, "end": e * scale,
         "target_duration": (e - s) * scale, "confidence": 1.0}
        for s, e in (starts[i] for i in slots)
    ]


@pytest.fixture(scope="module")
def rendered(tmp_path_factory):
    """Build a speaker file the way the export does, WITHOUT the speed pass.

    That is the production case that broke: playback_speed was 1.1 but the
    speaker's own gate declined to apply it.
    """
    from video_processor import concat_videos

    work = tmp_path_factory.mktemp("v925")
    clips = []
    for i, d in enumerate(CLIP_DURATIONS):
        p = work / f"clip_{i}.mp4"
        _make_clip(p, d, 220 + i * 40)
        clips.append(p)

    speaker = work / "speaker.mp4"
    concat_videos(clips, speaker)

    # v925 master: extracted from the finished speaker, untouched.
    master = work / "master.mp3"
    subprocess.run(
        ["ffmpeg", "-y", "-loglevel", "error", "-i", str(speaker),
         "-vn", "-acodec", "libmp3lame", "-q:a", "2", str(master)],
        capture_output=True,
    )
    # pre-v925 master: atempo pre-applied because the code ASSUMED the speaker
    # would be sped later. Here it never is — that is the whole bug.
    master_predicted = work / "master_predicted.mp3"
    subprocess.run(
        ["ffmpeg", "-y", "-loglevel", "error", "-i", str(speaker),
         "-vn", "-filter:a", f"atempo={PLAYBACK_SPEED:.6f}",
         "-acodec", "libmp3lame", "-q:a", "2", str(master_predicted)],
        capture_output=True,
    )
    return {
        "work": work,
        "clips": clips,
        "speaker": speaker,
        "speaker_duration": _probe_duration(speaker),
        "master": master,
        "master_predicted": master_predicted,
    }


def _render_broll(rendered, scale, name, master_key="master"):
    from video_processor import export_with_master_audio

    out = rendered["work"] / name
    export_with_master_audio(
        clip_info=[{"path": rendered["clips"][i]} for i in BROLL_SLOTS],
        dialogue_lines=[f"line {i}" for i in BROLL_SLOTS],
        master_audio_path=rendered[master_key],
        output_path=out,
        max_clip_speed=2.0,
        min_gap_for_black=1.0,
        pre_computed_targets=_targets_from(CLIP_DURATIONS, BROLL_SLOTS, scale),
    )
    return _probe_duration(out)


def test_old_predicted_scaling_reproduces_the_reported_desync(rendered):
    """Pre-v925: master audio atempo'd and targets scaled by 1/playback_speed,
    even though the speaker never got sped. Reproduces the ~1.1x ratio measured
    on the shipped files."""
    broll = _render_broll(
        rendered, 1.0 / PLAYBACK_SPEED, "broll_old.mp4",
        master_key="master_predicted",
    )
    speaker = rendered["speaker_duration"]
    ratio = speaker / broll
    assert ratio == pytest.approx(PLAYBACK_SPEED, abs=0.02), (
        f"expected the old code to be ~{PLAYBACK_SPEED}x short; "
        f"got speaker={speaker:.3f}s broll={broll:.3f}s ratio={ratio:.4f}"
    )


def test_measured_scaling_keeps_broll_and_speaker_the_same_length(rendered):
    """v925: scale by the MEASURED ratio instead. No condition is re-derived,
    so the speaker declining its speed pass cannot desync the pair."""
    speaker = rendered["speaker_duration"]
    measured = speaker / sum(CLIP_DURATIONS)
    broll = _render_broll(rendered, measured, "broll_new.mp4")
    delta = abs(broll - speaker)
    assert delta <= LENGTH_TOLERANCE_S, (
        f"broll {broll:.3f}s vs speaker {speaker:.3f}s "
        f"(delta {delta:.3f}s > {LENGTH_TOLERANCE_S}s, measured k={measured:.4f})"
    )


def test_measured_ratio_also_absorbs_concat_normalize_drift(rendered):
    """The per-clip durations are probed BEFORE concat_videos re-encodes each
    clip to fps=24 + 48k AAC, so their sum is not the real timeline. The
    measured ratio corrects that too — it is never exactly 1.0."""
    drift = rendered["speaker_duration"] - sum(CLIP_DURATIONS)
    assert drift > 0.0, (
        "expected the normalized concat to run longer than the sum of the "
        f"probed clip durations; got {drift:+.3f}s"
    )
