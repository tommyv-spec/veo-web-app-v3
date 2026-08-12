"""Profile mechanism tests for v589 Stage 4d (fbads-video vs default ugc-reel).

One decode engine reads two formats. The load-bearing invariant is that the
default "ugc-reel" lane stays byte-identical to the pre-profile behavior — the
Korella production lane runs on it — while "fbads-video" adds a required
top-level ad_read block plus its own context/task text.

Run: python -m pytest test_v589_profiles.py -q
"""
import importlib.util
import inspect
import json
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "v589", Path(__file__).parent / "v589_video_understanding.py")
v589 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(v589)


def test_default_profile_is_the_declared_default():
    default = inspect.signature(v589.build_user_prompt).parameters["profile"].default
    assert default == "ugc-reel"
    assert v589.READING_PROFILES[default]["ad_read"] is False
    assert v589.READING_PROFILES["fbads-video"]["ad_read"] is True


def test_schema_default_profile_is_unchanged():
    assert v589.schema_for_profile("ugc-reel") == v589.STAGE4D_JSON_SCHEMA
    # and it is a copy, not the global itself
    assert v589.schema_for_profile("ugc-reel") is not v589.STAGE4D_JSON_SCHEMA


def test_schema_fbads_adds_required_ad_read():
    schema = v589.schema_for_profile("fbads-video")
    assert "ad_read" in schema["properties"]
    assert "ad_read" in schema["required"]
    # base global must NOT have been mutated
    assert "ad_read" not in v589.STAGE4D_JSON_SCHEMA["properties"]
    # the attached block is a copy too — SDKs mutate response schemas in place
    assert schema["properties"]["ad_read"] is not v589.AD_READ_SCHEMA
    ad = schema["properties"]["ad_read"]
    for field in ("offer", "cta", "overlay_text_timeline", "captions",
                  "sound_off_comprehension", "aspect_ratio", "safe_zones",
                  "end_card", "brand_assets"):
        assert field in ad["properties"], field
        assert field in ad["required"], field


def test_prompt_carries_fbads_context_only_for_fbads():
    shots = [{"shot": 1, "start": 0.0, "end": 3.0}]
    default_prompt = v589.build_user_prompt(shots, "t", [], profile="ugc-reel")
    fb_prompt = v589.build_user_prompt(shots, "t", [], profile="fbads-video")
    assert "PAID" in fb_prompt and "ad_read" in fb_prompt
    assert "PAID" not in default_prompt
    # the format declaration is DATA (<context>); the read job is an INSTRUCTION
    assert fb_prompt.index("PAID") < fb_prompt.index("<task>")
    assert "fill the top-level ad_read object" in fb_prompt.split("<task>")[1]
    # the caller's own extra_context still survives the profile prefix
    assert "TIMELINE-SENTINEL" in v589.build_user_prompt(
        shots, "t", [], extra_context="TIMELINE-SENTINEL", profile="fbads-video")
    # existing behavior preserved: no-profile call == ugc-reel call
    assert v589.build_user_prompt(shots, "t", []) == default_prompt


def test_fbads_instruction_survives_schema_suppression():
    """The gemini lane suppresses the schema block; the profile text must still land."""
    shots = [{"shot": 1, "start": 0.0, "end": 3.0}]
    p = v589.build_user_prompt(shots, "t", [], include_schema=False,
                               profile="fbads-video")
    assert "PAID" in p
    assert "fill the top-level ad_read object" in p
    assert "Stage 4d JSON schema:" not in p


def test_unknown_profile_names_the_valid_choices():
    with pytest.raises(ValueError, match="unknown reading profile"):
        v589.schema_for_profile("fbads")


def _minimal_valid_stage4d():
    """Load a real passing artifact as the base fixture."""
    p = Path(__file__).parent.parent / "raw/decode_work/DbjOnA3B8o7/stage4d_vlm.json"
    return json.loads(p.read_text(encoding="utf-8"))


def _expected_shots_for(data):
    return [{"shot": s["shot_index"], "start": s["start"], "end": s["end"]}
            for s in data["shots"]]


def test_validator_default_profile_ignores_ad_read():
    data = _minimal_valid_stage4d()
    shots = _expected_shots_for(data)
    v589.validate_stage4d_output(data, shots)  # no profile arg — must not raise
    v589.validate_stage4d_output(data, shots, profile="ugc-reel")


def test_validator_fbads_requires_ad_read():
    data = _minimal_valid_stage4d()
    shots = _expected_shots_for(data)
    try:
        v589.validate_stage4d_output(data, shots, profile="fbads-video")
        raise AssertionError("expected Stage4dValidationError")
    except v589.Stage4dValidationError as exc:
        assert "ad_read" in str(exc)


def test_validator_fbads_passes_with_ad_read():
    data = _minimal_valid_stage4d()
    shots = _expected_shots_for(data)
    data["ad_read"] = {
        "offer": "20% off first order", "cta": "Shop Now",
        "overlay_text_timeline": [{"time": "00:01", "end": "00:04",
                                   "text": "20% OFF",
                                   "style": "bold white center"}],
        "captions": "burned, bottom-center, word-accurate",
        "sound_off_comprehension": "muted viewer sees product + 20% OFF overlay",
        "aspect_ratio": "9:16",
        "safe_zones": "CTA and face inside center-safe area",
        "end_card": "logo + Shop Now button",
        "brand_assets": "logo at 00:00 and 00:14",
    }
    v589.validate_stage4d_output(data, shots, profile="fbads-video")


def test_validator_fbads_names_every_missing_ad_read_field():
    """A half-filled ad_read must report all its gaps, not just the first."""
    data = _minimal_valid_stage4d()
    shots = _expected_shots_for(data)
    data["ad_read"] = {"offer": "20% off", "overlay_text_timeline": "not an array"}
    try:
        v589.validate_stage4d_output(data, shots, profile="fbads-video")
        raise AssertionError("expected Stage4dValidationError")
    except v589.Stage4dValidationError as exc:
        msg = str(exc)
        assert "ad_read.overlay_text_timeline must be an array" in msg
        for field in ("cta", "captions", "sound_off_comprehension", "aspect_ratio",
                      "safe_zones", "end_card", "brand_assets"):
            assert f"ad_read.{field}" in msg, field
        assert "ad_read.offer" not in msg


def test_parse_and_validate_threads_the_profile():
    data = _minimal_valid_stage4d()
    shots = _expected_shots_for(data)
    raw = json.dumps(data)
    v589.parse_and_validate_stage4d(raw, shots)  # default lane unchanged
    try:
        v589.parse_and_validate_stage4d(raw, shots, profile="fbads-video")
        raise AssertionError("expected Stage4dValidationError")
    except v589.Stage4dValidationError as exc:
        assert "ad_read" in str(exc)


def test_gemini_config_uses_profile_schema():
    """The gemini lane passes the schema out-of-band; it must be the PROFILE's schema."""
    src = inspect.getsource(v589.call_gemini)
    assert "schema_for_profile(profile)" in src
    assert "response_json_schema=STAGE4D_JSON_SCHEMA" not in src


def test_providers_and_cli_thread_the_profile():
    for fn in (v589.call_gemini, v589.call_lmstudio,
               v589.validate_stage4d_output, v589.parse_and_validate_stage4d):
        assert inspect.signature(fn).parameters["profile"].default == "ugc-reel", fn.__name__
    # each provider hands the profile to the prompt builder
    for fn in (v589.call_gemini, v589.call_lmstudio):
        assert "profile=profile" in inspect.getsource(fn), fn.__name__
    main_src = inspect.getsource(v589.main)
    assert '"--profile"' in main_src
    assert "profile=args.profile" in main_src
    # the saved artifact self-identifies which profile produced it
    assert 'parsed["profile"] = args.profile' in main_src


@pytest.mark.xfail(reason="KNOWN, UNFIXED: validate_stage4d_output's hand-typed "
                          "required_top omits frame_inventory + start_frame_spec. "
                          "Deriving it from PER_SHOT_SCHEMA['required'] fixes it but "
                          "newly rejects 3 of the 6 stage4d.v2 artifacts already saved "
                          "under raw/decode_work, which pass today. Operator decision — "
                          "flip this test to a plain pass when the call is made.",
                   strict=False)
def test_validator_checks_every_schema_required_shot_field():
    """Guard against the hand-typed/schema drift that let frame_inventory go unchecked."""
    base = _minimal_valid_stage4d()
    # expectations come from the PRISTINE fixture — deriving them from the
    # mutated copy would KeyError on the shot_index/start/end rounds.
    shots = _expected_shots_for(base)
    for field in v589.PER_SHOT_SCHEMA["required"]:
        data = json.loads(json.dumps(base))
        data["shots"][0].pop(field, None)
        try:
            v589.validate_stage4d_output(data, shots)
        except v589.Stage4dValidationError:
            continue
        raise AssertionError(f"validator accepted a shot missing required field {field!r}")
