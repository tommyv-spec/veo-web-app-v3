"""Offline unit tests for flow_api.parsing (attribution by media_id UUID)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # code/

from flow_api import parsing

_UUID = "12345678-1234-1234-1234-123456789abc"
_CAMS = "CAMSxyzNOTAUUIDbase64protobuf"


def test_is_uuid():
    assert parsing.is_uuid(_UUID)
    assert not parsing.is_uuid(_CAMS)
    assert not parsing.is_uuid("")


def test_uuid_from_url():
    url = f"https://storage.googleapis.com/ai-sandbox-videofx/video/{_UUID}?sig=x"
    assert parsing.uuid_from_url(url) == _UUID
    assert parsing.uuid_from_url("https://x/none") == ""


def test_extract_video_media_id_from_operations():
    res = {"data": {"operations": [
        {"operation": {"metadata": {"video": {"mediaId": _UUID}}}}
    ]}}
    assert parsing.extract_video_media_id(res) == _UUID


def test_extract_video_media_id_rejects_cams_falls_to_url():
    res = {"data": {"operations": [
        {"operation": {"metadata": {"video": {
            "mediaId": _CAMS,
            "fifeUrl": f"https://storage.googleapis.com/x/video/{_UUID}?s=1",
        }}}}
    ]}}
    assert parsing.extract_video_media_id(res) == _UUID


def test_extract_video_media_id_empty_when_none():
    assert parsing.extract_video_media_id({"data": {"operations": []}}) == ""
    assert parsing.extract_video_media_id({}) == ""


def test_is_error_detects_http_and_error_field():
    assert parsing.is_error({"status": 403, "data": None})
    assert parsing.is_error({"error": "boom"})
    assert parsing.is_error({"data": {"error": {"message": "x"}}})
    assert not parsing.is_error({"status": 200, "data": {"operations": []}})


def test_error_reason_extracts_message():
    assert "boom" in parsing.error_reason({"error": "boom"})
    assert "unsafe" in parsing.error_reason(
        {"data": {"error": {"message": "PUBLIC_ERROR_unsafe"}}}).lower()


def test_poll_status_enum():
    assert parsing.poll_status({"status": parsing.STATUS_SUCCESS}) == parsing.STATUS_SUCCESS
    assert parsing.poll_status({}) == parsing.STATUS_PENDING


def test_extract_output_url_from_operations():
    res = {"data": {"operations": [
        {"operation": {"metadata": {"video": {"fifeUrl": "https://x/v.mp4"}}}}
    ]}}
    assert parsing.extract_output_url(res) == "https://x/v.mp4"
