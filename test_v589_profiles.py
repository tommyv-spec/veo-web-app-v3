"""Profile mechanism tests for v589 Stage 4d (fbads-video vs default ugc-reel)."""
import importlib.util
import json
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "v589", Path(__file__).parent / "v589_video_understanding.py")
v589 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(v589)


def test_profiles_registry():
    assert set(v589.PROFILES) == {"ugc-reel", "fbads-video"}
    assert v589.PROFILES["ugc-reel"]["ad_read"] is False
    assert v589.PROFILES["fbads-video"]["ad_read"] is True


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
    ad = schema["properties"]["ad_read"]
    for field in ("offer", "cta", "overlay_text_timeline", "captions",
                  "sound_off_comprehension", "aspect_ratio", "end_card",
                  "brand_assets"):
        assert field in ad["properties"], field
        assert field in ad["required"], field


def test_prompt_carries_fbads_context_only_for_fbads():
    shots = [{"shot": 1, "start": 0.0, "end": 3.0}]
    default_prompt = v589.build_user_prompt(shots, "t", [], profile="ugc-reel")
    fb_prompt = v589.build_user_prompt(shots, "t", [], profile="fbads-video")
    assert "PAID" in fb_prompt and "ad_read" in fb_prompt
    assert "PAID" not in default_prompt
    # existing behavior preserved: no-profile call == ugc-reel call
    assert v589.build_user_prompt(shots, "t", []) == default_prompt
