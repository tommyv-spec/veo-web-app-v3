"""Lock-in tests for the action-aware silence remover (silence_mode='action').

Operator rule (2026-08-25): keep every spoken script word AND every action
beat (blender sound, throw) even when nobody speaks; cut only stretches that
are silent AND visually static. Pure-function tests, no media needed.
"""
import sys
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from video_processor import merge_action_keep_spans, assemble_action_evidence  # noqa: E402


def test_bridges_short_gaps_between_speech():
    # Two lines 0.4s apart with trigger 0.5 -> one block, no cut.
    spans = merge_action_keep_spans(
        speech=[(0.0, 2.0), (2.4, 4.0)], events=[], motion=[],
        total_duration=8.0, min_cut_gap=0.5, breathing_gap=0.3)
    assert len(spans) == 1, spans
    assert spans[0][0] == 0.0 and abs(spans[0][1] - 4.15) < 0.01, spans


def test_keeps_silent_action_island():
    # A throw at 5-6s, no speech there: must survive (with breathing pads).
    spans = merge_action_keep_spans(
        speech=[(0.0, 2.0)], events=[], motion=[(5.0, 6.0)],
        total_duration=10.0, min_cut_gap=0.5, breathing_gap=0.3)
    assert any(s <= 5.0 and e >= 6.0 for s, e in spans), spans


def test_drops_orphan_noise_blip():
    # 0.2s event blip 3s away from anything: stutter island, dropped.
    spans = merge_action_keep_spans(
        speech=[(0.0, 2.0)], events=[(5.0, 5.2)], motion=[],
        total_duration=10.0, min_cut_gap=0.5, breathing_gap=0.3)
    assert not any(s >= 4.0 and e <= 6.0 for s, e in spans), spans


def test_breathing_gap_at_cut():
    # Speech ends 2.0, action at 6.0-7.0. The cut between them must leave
    # breathing_gap total, split half/half: prev end 2.15, next start 5.85.
    spans = merge_action_keep_spans(
        speech=[(0.0, 2.0)], events=[], motion=[(6.0, 7.0)],
        total_duration=10.0, min_cut_gap=0.5, breathing_gap=0.3)
    assert len(spans) == 2, spans
    assert abs(spans[0][1] - 2.15) < 0.01, spans
    assert abs(spans[1][0] - 5.85) < 0.01, spans


def test_pad_speech_outside_window_is_cut():
    # Silero heard speech 0-3s (script) and 5-7s (Veo pad trailer). The
    # Whisper anchor window ends at 3.2 -> pad speech is excluded, and the
    # event detector must not resurrect it as a "sound event" either.
    speech, events, motion = assemble_action_evidence(
        silero_spans=[(0.0, 3.0), (5.0, 7.0)], anchor_window=(0.0, 3.2),
        event_spans=[(5.0, 7.0)], motion_spans=[])
    assert speech == [(0.0, 3.0)], speech
    assert events == [], events


def test_event_partially_overlapping_pad_speech_keeps_the_clean_part():
    # A blender (4-8s) overlaps a pad-speech span (5-6s): the spoken part is
    # trimmed out of the event, the clean sound before and after survives.
    speech, events, motion = assemble_action_evidence(
        silero_spans=[(0.0, 3.0), (5.0, 6.0)], anchor_window=(0.0, 3.2),
        event_spans=[(4.0, 8.0)], motion_spans=[])
    assert events == [(4.0, 5.0), (6.0, 8.0)], events


def test_no_anchor_window_keeps_all_silero_speech():
    # Whisper found no anchors (window=None): silero speech must be kept
    # as-is rather than discarded.
    speech, events, motion = assemble_action_evidence(
        silero_spans=[(1.0, 2.0)], anchor_window=None,
        event_spans=[], motion_spans=[])
    assert speech == [(1.0, 2.0)], speech


def test_full_clip_when_kept_below_script_floor():
    # 20 script words need >=3.6s (0.18s/word); evidence collapsed to ~1s
    # -> distrust the sensors, keep the full clip (v706 spirit).
    spans = merge_action_keep_spans(
        speech=[(0.0, 1.0)], events=[], motion=[],
        total_duration=8.0, min_cut_gap=0.5, breathing_gap=0.3,
        script_word_count=20)
    assert spans == [(0.0, 8.0)], spans


def test_no_evidence_keeps_full_clip():
    spans = merge_action_keep_spans(
        speech=[], events=[], motion=[],
        total_duration=6.0, min_cut_gap=0.5, breathing_gap=0.3)
    assert spans == [(0.0, 6.0)], spans


def test_event_threshold_is_relative_to_the_voice():
    # Synthetic dB envelope at 50ms hop over 10s: floor -60, speech -20 at
    # 0-2s, blender -25 at 5-7s. The threshold derives from the SPEECH level
    # (-20 - 18 = -38), so the -25 blender is an event and the -60 floor is
    # not. A fixed -29dB floor would also pass here; the relative test is
    # test_quiet_voice below.
    from video_processor import _event_spans_from_envelope
    hop = 0.05
    n = int(10.0 / hop)
    env = [-60.0] * n
    for i in range(0, int(2.0 / hop)):
        env[i] = -20.0
    for i in range(int(5.0 / hop), int(7.0 / hop)):
        env[i] = -25.0
    spans = _event_spans_from_envelope(env, hop, speech_spans=[(0.0, 2.0)])
    assert any(abs(s - 5.0) < 0.11 and abs(e - 7.0) < 0.11 for s, e in spans), spans
    assert not any(s >= 7.5 for s, e in spans), spans


def test_sustained_background_motion_is_not_an_action():
    # Operator 2026-08-25: "the husband smiling is not important and is dead
    # space" — the car's scrolling scenery holds a HIGH but CONSTANT motion
    # share. Constant = baseline = never an event.
    from video_processor import _onset_spans
    values = [0.25] * 60  # 10s of steady 25% share at 6fps
    spans = _onset_spans(values, 1 / 6.0, abs_floor=0.03,
                         min_delta=0.05, rel_delta_factor=0.8)
    assert spans == [], spans


def test_motion_spike_over_moving_background_is_an_action():
    # Even over a moving background (baseline 0.25), a real action spikes
    # above it and must trigger; and over a quiet kitchen (baseline 0.06)
    # a palm-raise at 0.30 must trigger.
    from video_processor import _onset_spans
    car = [0.25] * 30 + [0.55] * 6 + [0.25] * 24
    spans = _onset_spans(car, 1 / 6.0, abs_floor=0.03,
                         min_delta=0.05, rel_delta_factor=0.8)
    assert any(4.7 <= s <= 5.3 for s, e in spans), spans
    kitchen = [0.06] * 30 + [0.30] * 6 + [0.06] * 24
    spans = _onset_spans(kitchen, 1 / 6.0, abs_floor=0.03,
                         min_delta=0.05, rel_delta_factor=0.8)
    assert any(4.7 <= s <= 5.3 for s, e in spans), spans


def test_long_action_survives_past_its_onset():
    # A 4s pour: spikes at onset, then holds. The span must extend across
    # the hold, not just cover the first spike.
    from video_processor import _onset_spans
    values = [0.05] * 30 + [0.35] * 24 + [0.05] * 12
    spans = _onset_spans(values, 1 / 6.0, abs_floor=0.03,
                         min_delta=0.05, rel_delta_factor=0.8)
    assert any(s <= 5.2 and e >= 8.5 for s, e in spans), spans


def test_clip_opening_action_still_counts():
    # Veo clips open mid-action (v752 catalyst rule): a signal already
    # elevated at t=0 has no forward onset, but it ENDS — the reversed pass
    # must catch it. Pure ambience (constant to both edges) stays invisible.
    from video_processor import _bidirectional_onset_spans
    opening = [0.35] * 12 + [0.05] * 48  # action for the first 2s, then quiet
    spans = _bidirectional_onset_spans(opening, 1 / 6.0, abs_floor=0.03,
                                       min_delta=0.05, rel_delta_factor=0.8)
    assert any(s <= 0.2 and e >= 1.6 for s, e in spans), spans
    ambience = [0.25] * 60
    assert _bidirectional_onset_spans(ambience, 1 / 6.0, abs_floor=0.03,
                                      min_delta=0.05, rel_delta_factor=0.8) == []


def test_constant_road_noise_is_not_a_sound_event():
    # Road noise at -30dB throughout, voice at -20 (0-2s): the noise is loud
    # enough to pass the voice-relative floor (-38) but it is SUSTAINED —
    # ambience, not an event. Including right after the spoken line ends.
    from video_processor import _event_spans_from_envelope
    hop = 0.05
    n = int(10.0 / hop)
    env = [-30.0] * n
    for i in range(0, int(2.0 / hop)):
        env[i] = -20.0
    spans = _event_spans_from_envelope(env, hop, speech_spans=[(0.0, 2.0)])
    assert spans == [], spans


def test_blender_onset_over_road_noise_is_an_event():
    # Same road-noise clip, but a blender kicks in at 5s (-18dB): a clear
    # onset over the -30 ambience baseline -> event.
    from video_processor import _event_spans_from_envelope
    hop = 0.05
    n = int(10.0 / hop)
    env = [-30.0] * n
    for i in range(0, int(2.0 / hop)):
        env[i] = -20.0
    for i in range(int(5.0 / hop), int(7.0 / hop)):
        env[i] = -18.0
    spans = _event_spans_from_envelope(env, hop, speech_spans=[(0.0, 2.0)])
    assert any(abs(s - 5.0) < 0.15 and e >= 6.8 for s, e in spans), spans


def test_quiet_voice_lowers_the_event_threshold():
    # A quiet TTS voice at -35dB: threshold becomes max(-45, -35-18) = -45,
    # so a -42dB pour still counts as an event. The old energy mode's fixed
    # -29dB floor would have cut it as silence.
    from video_processor import _event_spans_from_envelope
    hop = 0.05
    n = int(10.0 / hop)
    env = [-70.0] * n
    for i in range(0, int(2.0 / hop)):
        env[i] = -35.0
    for i in range(int(5.0 / hop), int(6.0 / hop)):
        env[i] = -42.0
    spans = _event_spans_from_envelope(env, hop, speech_spans=[(0.0, 2.0)])
    assert any(s < 5.2 and e > 5.8 for s, e in spans), spans
