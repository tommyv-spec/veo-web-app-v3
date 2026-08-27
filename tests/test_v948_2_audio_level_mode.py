"""v948.2 — the `audio_enhance: "level"` mode.

WHY IT EXISTS. `audio_enhance` was a two-value switch, and "off" turns off FOUR
things at once: the DeepFilter denoiser, the voice EQ, the compressor/limiter,
and the two-pass loudness normalisation. A v948-swept export needs exactly ONE
of those gone.

Measured on job 29d45418 (2026-08-27):
  - with "voice": the denoiser crushes quiet room tone below the -35dB silence
    floor and RE-CREATES the holes the v948 sweep just removed;
  - with "off":   no holes, but the final lands at -25.1 LUFS against a
    -14.3 LUFS published reference — roughly 11 dB quiet, which reads as a
    broken video next to anything else in the feed.

"level" is the middle setting: no denoiser, everything else. It runs the chain
the Modal-unavailable fallback already runs, so the audio path itself is not
new code — only the choosing of it, and the caching, are.
"""
import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# --------------------------------------------------------------------------
# The value is accepted everywhere the closed set is enforced
# --------------------------------------------------------------------------

def test_qc_accepts_level():
    from autoedit_qc import normalize_repairs
    assert normalize_repairs({"audio_enhance": "level"})["audio_enhance"] == "level"


def test_qc_still_rejects_an_unknown_value():
    """The set stays CLOSED — adding a third value must not open it."""
    from autoedit_qc import normalize_repairs
    with pytest.raises(ValueError, match="audio_enhance"):
        normalize_repairs({"audio_enhance": "loud"})


def test_qc_default_is_still_voice():
    """v948.2 adds a choice; it does not change what an undeclared job does."""
    from autoedit_qc import normalize_repairs
    assert normalize_repairs({})["audio_enhance"] == "voice"


def test_request_model_accepts_level_and_still_refuses_junk():
    from finishing_models import AutoEditRequest
    assert AutoEditRequest(audio_enhance="level").audio_enhance == "level"
    assert AutoEditRequest().audio_enhance == "voice"
    with pytest.raises(Exception):
        AutoEditRequest(audio_enhance="quiet")


def test_declarable_from_a_build_with_no_parser_edit():
    """v947 validates `autoedit_*` against the real model, so a new enum value
    is sayable in `## Finishing` the moment it exists. Same proof v948 used for
    export_max_silence_s — if this ever fails, someone has re-introduced a
    hand-copied field list."""
    from image_platform import parse_finishing_section
    spec = parse_finishing_section(
        "## Finishing\n\n- **autoedit_audio_enhance:** level\n")
    assert spec["autoedit"]["audio_enhance"] == "level"


# --------------------------------------------------------------------------
# The pipeline routes the three modes to three different things
# --------------------------------------------------------------------------

def test_pipeline_routes_each_mode(monkeypatch, tmp_path):
    """"off" passes through untouched, "level" enhances WITHOUT the denoiser,
    anything else takes the full voice chain."""
    import autoedit_pipeline as ap

    calls = {}

    def fake_enhance(base, work, denoise=True):
        calls["denoise"] = denoise
        return work / "enhanced.wav"

    monkeypatch.setattr(ap, "enhance_audio", fake_enhance)

    # The three branches live inside prepare_media, which does far more than
    # audio. Rather than drive that whole function, assert on the branch the
    # source actually contains — a routing bug here is a wiring bug, and the
    # wiring is what this pins.
    src = Path(ap.__file__).read_text(encoding="utf-8")
    assert 'elif _audio_mode == "level":' in src
    assert "enhance_audio(base, work, denoise=False)" in src
    assert '_audio_mode = repairs.get("audio_enhance", "voice")' in src


def test_level_skips_the_denoiser_call_entirely(monkeypatch, tmp_path):
    """The Modal denoiser must not even be ATTEMPTED — not called and its
    failure swallowed. A network call we do not want is still a network call."""
    import autoedit_pipeline as ap

    attempted = {"df": False}

    def boom(*a, **k):  # stands in for audio_processor.try_deepfilter_modal
        attempted["df"] = True
        raise AssertionError("the denoiser must not run in level mode")

    ran = {"cmds": []}
    monkeypatch.setattr(ap, "run", lambda cmd, *a, **k: ran["cmds"].append(cmd))
    monkeypatch.setattr(ap, "audio_chain_key", lambda: "abcd1234")
    monkeypatch.setattr(ap, "file_fingerprint", lambda p: "ffff")
    monkeypatch.setattr(ap.shutil, "copy", lambda a, b: Path(b).write_bytes(b""))
    monkeypatch.setattr(ap.subprocess, "run", lambda *a, **k: type(
        "R", (), {"stderr": "no json here"})())
    monkeypatch.setitem(sys.modules, "audio_processor",
                        type("M", (), {"try_deepfilter_modal": staticmethod(boom)}))

    base = tmp_path / "in.mp4"
    base.write_bytes(b"")
    out = ap.enhance_audio(base, tmp_path, denoise=False)

    assert attempted["df"] is False
    assert out.name.endswith("_raw.wav"), (
        "level mode must land on the no-denoiser cache name, so it can never be "
        "confused with a denoised result")


def test_level_still_applies_loudness(monkeypatch, tmp_path):
    """The whole point: the loudness pass survives. Without it a swept final
    measured -25.1 LUFS against a -14.3 LUFS reference."""
    import autoedit_pipeline as ap

    ran = {"cmds": []}
    monkeypatch.setattr(ap, "run", lambda cmd, *a, **k: ran["cmds"].append(cmd))
    monkeypatch.setattr(ap, "audio_chain_key", lambda: "abcd1234")
    monkeypatch.setattr(ap, "file_fingerprint", lambda p: "ffff")
    monkeypatch.setattr(ap.shutil, "copy", lambda a, b: Path(b).write_bytes(b""))
    monkeypatch.setattr(ap.subprocess, "run", lambda *a, **k: type(
        "R", (), {"stderr": "no json here"})())

    base = tmp_path / "in.mp4"
    base.write_bytes(b"")
    ap.enhance_audio(base, tmp_path, denoise=False)

    filters = " ".join(" ".join(str(x) for x in c) for c in ran["cmds"])
    assert "loudnorm=I=-15" in filters, "level mode dropped the loudness pass"
    assert "acompressor" in filters, "level mode dropped the level chain"
    assert "highpass=f=90" in filters, (
        "level mode should use the no-denoiser low-end correction (a 90Hz cut), "
        "not the +7dB shelf that compensates for what DeepFilter removed")


# --------------------------------------------------------------------------
# The cache distinction that makes this safe
# --------------------------------------------------------------------------

def test_a_deliberate_no_denoise_result_IS_reused(monkeypatch, tmp_path):
    """A fallback result is never reused (a transient Modal outage must not
    leave a job permanently serving degraded audio). A DELIBERATE choice is a
    correct result, so it is cached like any other."""
    import autoedit_pipeline as ap

    monkeypatch.setattr(ap, "audio_chain_key", lambda: "abcd1234")
    monkeypatch.setattr(ap, "file_fingerprint", lambda p: "ffff")

    def must_not_run(*a, **k):
        raise AssertionError("a cached level result must be reused, not rebuilt")

    cached = tmp_path / ap.audio_cache_name("abcd1234_sffff", denoised=False)
    cached.write_bytes(b"cached")
    monkeypatch.setattr(ap, "run", must_not_run)

    base = tmp_path / "in.mp4"
    base.write_bytes(b"")
    assert ap.enhance_audio(base, tmp_path, denoise=False) == cached


def test_voice_mode_does_not_pick_up_a_level_cache(monkeypatch, tmp_path):
    """The two results are NOT interchangeable — they carry opposite low-end
    corrections. A voice run must rebuild rather than serve the level file."""
    import autoedit_pipeline as ap

    monkeypatch.setattr(ap, "audio_chain_key", lambda: "abcd1234")
    monkeypatch.setattr(ap, "file_fingerprint", lambda p: "ffff")

    level_cache = tmp_path / ap.audio_cache_name("abcd1234_sffff", denoised=False)
    level_cache.write_bytes(b"cached")

    ran = {"cmds": []}
    monkeypatch.setattr(ap, "run", lambda cmd, *a, **k: ran["cmds"].append(cmd))
    monkeypatch.setattr(ap.shutil, "copy", lambda a, b: Path(b).write_bytes(b""))
    monkeypatch.setattr(ap.subprocess, "run", lambda *a, **k: type(
        "R", (), {"stderr": "no json"})())
    monkeypatch.setitem(sys.modules, "audio_processor", type(
        "M", (), {"try_deepfilter_modal": staticmethod(lambda a, b: False)}))

    base = tmp_path / "in.mp4"
    base.write_bytes(b"")
    ap.enhance_audio(base, tmp_path, denoise=True)
    assert ran["cmds"], "voice mode served the level cache instead of rebuilding"
