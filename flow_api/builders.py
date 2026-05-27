"""Pure functions that build Flow API request bodies + URLs. No I/O — unit-testable.

Bodies ported from FlowKit agent/services/flow_client.py. The reCAPTCHA token is left
empty here; page_ops injects the real token (minted in-page) right before the fetch.
"""
import time
import uuid

from . import config


def build_url(endpoint_key: str, **fmt) -> str:
    path = config.ENDPOINTS[endpoint_key].format(**fmt)
    sep = "&" if "?" in path else "?"
    return f"{config.GOOGLE_FLOW_API}{path}{sep}key={config.GOOGLE_API_KEY}"


def client_context(project_id: str, tier: str = "PAYGATE_TIER_TWO") -> dict:
    """clientContext with an empty recaptcha placeholder (filled in-page)."""
    return {
        "projectId": str(project_id or ""),
        "recaptchaContext": {
            "applicationType": "RECAPTCHA_APPLICATION_TYPE_WEB",
            "token": "",
        },
        "sessionId": f";{int(time.time() * 1000)}",
        "tool": "PINHOLE",
        "userPaygateTier": tier,
    }


def build_upload_image(image_base64: str, project_id: str = "",
                       file_name: str = "image.jpg",
                       mime_type: str = "image/jpeg") -> dict:
    """No captcha. Response: data.media.name = media_id (UUID)."""
    return {
        "clientContext": {"projectId": str(project_id or ""), "tool": "PINHOLE"},
        "fileName": file_name,
        "imageBytes": image_base64,
        "isHidden": False,
        "isUserUploaded": True,
        "mimeType": mime_type,
    }


def build_generate_video(prompt: str, start_media_id: str, project_id: str,
                         scene_id: str, model_key: str,
                         end_media_id: str = "",
                         aspect: str = None,
                         tier: str = "PAYGATE_TIER_TWO") -> dict:
    """Start-image i2v, or start+end if end_media_id given. Captcha required."""
    aspect = aspect or config.DEFAULT_ASPECT_RATIO
    request = {
        "aspectRatio": aspect,
        "seed": int(time.time()) % 10000,
        "textInput": {"structuredPrompt": {"parts": [{"text": prompt}]}},
        "videoModelKey": model_key,
        "startImage": {"mediaId": start_media_id},
        "metadata": {"sceneId": str(scene_id or "")},
    }
    if end_media_id:
        request["endImage"] = {"mediaId": end_media_id}
    return {
        "mediaGenerationContext": {"batchId": f"{uuid.uuid4()}"},
        "clientContext": client_context(project_id, tier),
        "requests": [request],
        "useV2ModelConfig": True,
    }


def build_generate_video_references(prompt: str, reference_media_ids: list,
                                    project_id: str, scene_id: str, model_key: str,
                                    aspect: str = None,
                                    tier: str = "PAYGATE_TIER_TWO") -> dict:
    """Reference-images r2v (Omni Ingredients candidate). Captcha required.

    NOTE: the exact body for Omni Ingredients is unconfirmed for this account — confirm
    via capture_helper before relying on it. This is the FlowKit r2v shape.
    """
    aspect = aspect or config.DEFAULT_ASPECT_RATIO
    request = {
        "aspectRatio": aspect,
        "seed": int(time.time()) % 10000,
        "textInput": {"structuredPrompt": {"parts": [{"text": prompt}]}},
        "videoModelKey": model_key,
        "referenceImages": [
            {"mediaId": mid, "imageUsageType": "IMAGE_USAGE_TYPE_ASSET"}
            for mid in reference_media_ids
        ],
        "metadata": {"sceneId": str(scene_id or "")},
    }
    return {
        "mediaGenerationContext": {"batchId": f"{uuid.uuid4()}"},
        "clientContext": client_context(project_id, tier),
        "requests": [request],
        "useV2ModelConfig": True,
    }


def build_check_status(operations: list) -> dict:
    """No captcha."""
    return {"operations": operations}


def gen_type_for(end_media_id: str = "") -> str:
    return "start_end_frame_2_video" if end_media_id else "frame_2_video"


def inject_captcha_token(body: dict, token: str) -> dict:
    """Fill the empty recaptcha placeholder(s) with the minted token (mutates + returns)."""
    cc = body.get("clientContext")
    if isinstance(cc, dict) and isinstance(cc.get("recaptchaContext"), dict):
        cc["recaptchaContext"]["token"] = token
    for req in body.get("requests", []) or []:
        rcc = req.get("clientContext") if isinstance(req, dict) else None
        if isinstance(rcc, dict) and isinstance(rcc.get("recaptchaContext"), dict):
            rcc["recaptchaContext"]["token"] = token
    return body
