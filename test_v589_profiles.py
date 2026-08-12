"""Profile mechanism tests for v589 Stage 4d (fbads-video vs default ugc-reel).

One decode engine reads two formats. The load-bearing invariant is that the
default "ugc-reel" lane stays byte-identical to the pre-profile behavior — the
Korella production lane runs on it — while "fbads-video" adds a required
top-level ad_read block plus its own context/task text.

Run: python -m pytest test_v589_profiles.py -q
"""
import importlib.util
import inspect
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
