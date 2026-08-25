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
