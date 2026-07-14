"""Tests for audio_fingerprint — pure signal work on synthetic PCM."""
import array
import importlib.util
import math
import pathlib


def _load():
    spec = importlib.util.spec_from_file_location(
        "audio_fingerprint", pathlib.Path(__file__).parent / "audio_fingerprint.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _pcm(samples):
    """int list -> s16le bytes, as ffmpeg emits."""
    return array.array("h", samples).tobytes()


def _tone(n, amp):
    return [int(amp * math.sin(i / 8.0)) for i in range(n)]


def test_envelope_is_normalized_to_unit_length():
    m = _load()
    env = m.envelope_from_pcm(_pcm(_tone(8000, 10000)), frame_samples=200)
    assert abs(math.sqrt(sum(x * x for x in env)) - 1.0) < 1e-6


def test_envelope_tracks_loudness_over_time():
    m = _load()
    env = m.envelope_from_pcm(_pcm(_tone(4000, 500) + _tone(4000, 20000)), frame_samples=200)
    half = len(env) // 2
    assert sum(env[half:]) > 3 * sum(env[:half])


def test_identical_audio_scores_one():
    m = _load()
    env = m.envelope_from_pcm(_pcm(_tone(8000, 9000)), frame_samples=200)
    assert m.envelope_similarity(env, env) > 0.999


def test_re_encoded_audio_still_scores_high():
    """A re-encode perturbs samples but not the loudness shape."""
    m = _load()
    clean = _tone(8000, 9000)
    noisy = [min(32767, max(-32768, s + (37 if i % 3 else -29))) for i, s in enumerate(clean)]
    a = m.envelope_from_pcm(_pcm(clean), frame_samples=200)
    b = m.envelope_from_pcm(_pcm(noisy), frame_samples=200)
    assert m.envelope_similarity(a, b) > 0.95


def test_different_performances_score_low():
    m = _load()
    a = m.envelope_from_pcm(_pcm(_tone(4000, 20000) + _tone(4000, 500)), frame_samples=200)
    b = m.envelope_from_pcm(_pcm(_tone(4000, 500) + _tone(4000, 20000)), frame_samples=200)
    assert m.envelope_similarity(a, b) < 0.6


def test_similarity_tolerates_a_small_offset():
    """A platform re-encode can trim a few frames off the head."""
    m = _load()
    base = _tone(2000, 1000) + _tone(4000, 18000) + _tone(2000, 1000)
    a = m.envelope_from_pcm(_pcm(base), frame_samples=200)
    b = m.envelope_from_pcm(_pcm(base[800:]), frame_samples=200)
    assert m.envelope_similarity(a, b) > 0.85


def test_serialization_roundtrips():
    m = _load()
    env = m.envelope_from_pcm(_pcm(_tone(4000, 7000)), frame_samples=200)
    restored = m.decode_fingerprint(m.encode_fingerprint(env))
    assert len(restored) == len(env)
    assert m.envelope_similarity(env, restored) > 0.999


def test_empty_pcm_yields_empty_envelope():
    m = _load()
    assert m.envelope_from_pcm(b"", frame_samples=200) == []
    assert m.envelope_similarity([], [1.0]) == 0.0
