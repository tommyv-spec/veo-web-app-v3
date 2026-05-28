"""Offline unit tests for flow_api image generation (HAR-confirmed 2026-05-28)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # code/

from flow_api import builders, config, parsing


_UUID = "12345678-1234-1234-1234-123456789abc"
_REF_UUID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def test_resolve_image_model_name_nano_banana_2():
    assert config.resolve_image_model_name("Nano Banana 2") == "NARWHAL"


def test_resolve_image_model_name_pro():
    assert config.resolve_image_model_name("Nano Banana Pro") == "GEM_PIX_2"


def test_resolve_image_model_name_unknown():
    assert config.resolve_image_model_name("Nope") == ""


def test_build_generate_image_plain_text_to_image():
    body = builders.build_generate_image("hello", "proj-1", "NARWHAL")
    assert body["clientContext"]["projectId"] == "proj-1"
    assert body["clientContext"]["recaptchaContext"]["token"] == ""
    req = body["requests"][0]
    # HAR-confirmed top-level request keys for plain t2i (no inputs)
    assert set(req.keys()) == {"imageAspectRatio", "imageModelName", "seed", "structuredPrompt"}
    assert req["imageModelName"] == "NARWHAL"
    assert req["imageAspectRatio"] == "IMAGE_ASPECT_RATIO_PORTRAIT"
    assert req["structuredPrompt"]["parts"][0]["text"] == "hello"
    # No mediaGenerationContext / useNewMedia on the plain shape
    assert "mediaGenerationContext" not in body
    assert "useNewMedia" not in body


def test_build_generate_image_with_reference():
    body = builders.build_generate_image(
        "make it pop", "proj-1", "NARWHAL",
        reference_media_ids=[_REF_UUID],
    )
    req = body["requests"][0]
    assert req["imageInputs"] == [
        {"name": _REF_UUID, "imageInputType": "IMAGE_INPUT_TYPE_REFERENCE"}
    ]
    # With references, batchId + useNewMedia get added
    assert "batchId" in body["mediaGenerationContext"]
    assert body["useNewMedia"] is True


def test_build_generate_image_with_base_and_reference_order():
    body = builders.build_generate_image(
        "edit", "proj-1", "NARWHAL",
        base_image_media_id=_UUID,
        reference_media_ids=[_REF_UUID],
    )
    inputs = body["requests"][0]["imageInputs"]
    assert inputs[0] == {"name": _UUID, "imageInputType": "IMAGE_INPUT_TYPE_BASE_IMAGE"}
    assert inputs[1] == {"name": _REF_UUID, "imageInputType": "IMAGE_INPUT_TYPE_REFERENCE"}


def test_extract_image_media_id_from_media_name():
    res = {"data": {"media": [{"name": _UUID}]}}
    assert parsing.extract_image_media_id(res) == _UUID


def test_extract_image_media_id_falls_to_fife_url():
    res = {"data": {"media": [
        {"name": "CAMSnotauuid",
         "image": {"generatedImage": {"fifeUrl": f"https://x/image/{_UUID}?s=1"}}}
    ]}}
    assert parsing.extract_image_media_id(res) == _UUID


def test_extract_image_url():
    res = {"data": {"media": [
        {"image": {"generatedImage": {"fifeUrl": "https://x/img.png"}}}
    ]}}
    assert parsing.extract_image_url(res) == "https://x/img.png"


def test_inject_captcha_token_on_image_body():
    body = builders.build_generate_image("p", "proj", "NARWHAL")
    builders.inject_captcha_token(body, "TOK")
    assert body["clientContext"]["recaptchaContext"]["token"] == "TOK"
