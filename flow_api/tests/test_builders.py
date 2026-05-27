"""Offline unit tests for flow_api.builders + config (no browser, no network)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # code/

from flow_api import builders, config


def test_build_url_appends_key():
    url = builders.build_url("generate_video")
    assert url.startswith("https://aisandbox-pa.googleapis.com/v1/video:batchAsyncGenerateVideoStartImage")
    assert "key=" in url


def test_build_url_get_media_formats_id():
    url = builders.build_url("get_media", media_id="abc-123")
    assert "/v1/media/abc-123?" in url
    assert "key=" in url


def test_client_context_has_empty_recaptcha_placeholder():
    cc = builders.client_context("proj1")
    assert cc["projectId"] == "proj1"
    assert cc["recaptchaContext"]["token"] == ""
    assert cc["tool"] == "PINHOLE"


def test_build_generate_video_start_only():
    body = builders.build_generate_video(
        "hello", "media-start", "proj", "scene1", "veo_3_1_i2v_lite_low_priority")
    req = body["requests"][0]
    assert req["startImage"]["mediaId"] == "media-start"
    assert "endImage" not in req
    assert req["videoModelKey"] == "veo_3_1_i2v_lite_low_priority"
    assert req["textInput"]["structuredPrompt"]["parts"][0]["text"] == "hello"
    assert req["metadata"]["sceneId"] == "scene1"
    assert body["useV2ModelConfig"] is True


def test_build_generate_video_start_end():
    body = builders.build_generate_video(
        "hi", "s", "proj", "sc", "k", end_media_id="media-end")
    req = body["requests"][0]
    assert req["endImage"]["mediaId"] == "media-end"


def test_inject_captcha_token_fills_placeholder():
    body = builders.build_generate_video("p", "s", "proj", "sc", "k")
    builders.inject_captcha_token(body, "TOKEN123")
    assert body["clientContext"]["recaptchaContext"]["token"] == "TOKEN123"


def test_build_upload_image_no_captcha_fields():
    body = builders.build_upload_image("BASE64", project_id="proj", file_name="x.jpg")
    assert body["imageBytes"] == "BASE64"
    assert body["isUserUploaded"] is True
    assert body["clientContext"]["tool"] == "PINHOLE"


def test_gen_type_for():
    assert builders.gen_type_for("") == "frame_2_video"
    assert builders.gen_type_for("end") == "start_end_frame_2_video"


def test_resolve_model_key_confirmed_lite_low_priority():
    key = config.resolve_model_key(
        "Veo 3.1 - Lite [Lower Priority]", "frame_2_video", "VIDEO_ASPECT_RATIO_PORTRAIT")
    assert key == "veo_3_1_i2v_lite_low_priority"


def test_resolve_model_key_needs_capture_returns_empty():
    key = config.resolve_model_key(
        "Veo 3.1 - Quality", "frame_2_video", "VIDEO_ASPECT_RATIO_PORTRAIT")
    assert key == ""


def test_resolve_model_key_unknown_model_returns_empty():
    assert config.resolve_model_key("No Such Model", "frame_2_video", "VIDEO_ASPECT_RATIO_PORTRAIT") == ""
