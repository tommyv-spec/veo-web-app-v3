# -*- coding: utf-8 -*-
"""Unit tests for the Higgsfield Kling backend. HTTP is mocked; no real API calls."""
from pathlib import Path
from unittest.mock import patch, MagicMock

import higgsfield_generator as hg
from config import get_higgsfield_credentials_from_env, KLING_I2V_SLUG


def test_credentials_single_hf_key(monkeypatch):
    monkeypatch.setenv("HF_KEY", "kid:secret")
    monkeypatch.delenv("HF_API_KEY", raising=False)
    monkeypatch.delenv("HF_API_SECRET", raising=False)
    assert get_higgsfield_credentials_from_env() == "kid:secret"


def test_credentials_split_pair(monkeypatch):
    monkeypatch.delenv("HF_KEY", raising=False)
    monkeypatch.setenv("HF_API_KEY", "kid")
    monkeypatch.setenv("HF_API_SECRET", "secret")
    assert get_higgsfield_credentials_from_env() == "kid:secret"


def test_credentials_absent_returns_none(monkeypatch):
    for v in ("HF_KEY", "HF_API_KEY", "HF_API_SECRET"):
        monkeypatch.delenv(v, raising=False)
    assert get_higgsfield_credentials_from_env() is None


def test_kling_slug_constant():
    assert KLING_I2V_SLUG == "kling-video/v2.1/pro/image-to-video"


def test_animate_image_polls_then_downloads(tmp_path):
    submit = MagicMock(status_code=200)
    submit.raise_for_status = lambda: None
    submit.json.return_value = {
        "request_id": "r1",
        "status_url": "https://platform.higgsfield.ai/requests/r1/status",
    }
    poll = MagicMock(status_code=200)
    poll.raise_for_status = lambda: None
    poll.json.return_value = {"status": "completed", "video": {"url": "https://cdn/h.mp4"}}
    mp4 = MagicMock(status_code=200)
    mp4.raise_for_status = lambda: None
    mp4.content = b"MP4BYTES"

    with patch.object(hg.requests, "post", return_value=submit) as p, \
         patch.object(hg.requests, "get", side_effect=[poll, mp4]):
        out = hg.animate_image(
            image_url="https://r2/frame.png",
            prompt="slow zoom in",
            credentials="kid:secret",
            out_path=tmp_path / "clip.mp4",
            slug="kling-video/v2.1/pro/image-to-video",
            duration=5,
            poll_interval=0,
        )

    assert Path(out).read_bytes() == b"MP4BYTES"
    sent = p.call_args.kwargs["json"]
    assert sent == {"image_url": "https://r2/frame.png", "prompt": "slow zoom in", "duration": 5}
    assert p.call_args.kwargs["headers"]["Authorization"] == "Key kid:secret"


def test_animate_image_raises_on_failed_status(tmp_path):
    submit = MagicMock(status_code=200)
    submit.raise_for_status = lambda: None
    submit.json.return_value = {"request_id": "r1", "status_url": "https://x/status"}
    poll = MagicMock(status_code=200)
    poll.raise_for_status = lambda: None
    poll.json.return_value = {"status": "failed"}

    with patch.object(hg.requests, "post", return_value=submit), \
         patch.object(hg.requests, "get", return_value=poll):
        try:
            hg.animate_image(
                image_url="u", prompt="p", credentials="k:s",
                out_path=tmp_path / "c.mp4", poll_interval=0, max_polls=1,
            )
            assert False, "expected RuntimeError"
        except RuntimeError as e:
            assert "failed" in str(e).lower()
