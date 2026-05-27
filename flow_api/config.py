"""Flow private-API constants. All env-overridable.

Sources: FlowKit (crisng95/flowkit) agent/config.py + agent/models.json, cross-checked
vs useapi.net Google Flow API v1 docs. See module docstring for the NEEDS_CAPTURE note.
"""
import json
import os
from pathlib import Path

# ─── API base + keys ─────────────────────────────────────────
GOOGLE_FLOW_API = os.environ.get("FLOW_API_BASE", "https://aisandbox-pa.googleapis.com")
GOOGLE_API_KEY = os.environ.get("FLOW_API_KEY", "AIzaSyBtrm0o5ab1c-Ec8ZuLcGt3oJAA5VWt3pY")
RECAPTCHA_SITE_KEY = os.environ.get("FLOW_RECAPTCHA_SITE_KEY", "6LdsFiUsAAAAAIjVDZcuLhaHiDn5nnHVXVRQGeMV")

# ─── Endpoints ───────────────────────────────────────────────
ENDPOINTS = {
    "generate_video": "/v1/video:batchAsyncGenerateVideoStartImage",
    "generate_video_start_end": "/v1/video:batchAsyncGenerateVideoStartAndEndImage",
    "generate_video_references": "/v1/video:batchAsyncGenerateVideoReferenceImages",
    "upload_image": "/v1/flow/uploadImage",
    "check_video_status": "/v1/video:batchCheckAsyncVideoGenerationStatus",
    "get_credits": "/v1/credits",
    "get_media": "/v1/media/{media_id}",
}

# ─── reCAPTCHA actions ───────────────────────────────────────
CAPTCHA_VIDEO = "VIDEO_GENERATION"
CAPTCHA_IMAGE = "IMAGE_GENERATION"

# ─── Worker tuning (mirrors FlowKit anti-throttle defaults) ──
VIDEO_POLL_INTERVAL = int(os.environ.get("FLOW_API_POLL_INTERVAL", "10"))
VIDEO_POLL_TIMEOUT = int(os.environ.get("FLOW_API_POLL_TIMEOUT", "420"))
API_COOLDOWN = int(os.environ.get("FLOW_API_COOLDOWN", "10"))
MAX_CONCURRENT_REQUESTS = int(os.environ.get("FLOW_API_MAX_CONCURRENT", "5"))
CAPTCHA_MAX_RETRIES = int(os.environ.get("FLOW_API_CAPTCHA_RETRIES", "10"))
SUBMIT_MAX_RETRIES = int(os.environ.get("FLOW_API_SUBMIT_RETRIES", "5"))

# ─── Default aspect ratio (platform ships 9:16 vertical) ─────
DEFAULT_ASPECT_RATIO = os.environ.get("FLOW_API_ASPECT", "VIDEO_ASPECT_RATIO_PORTRAIT")

# ─── Model-key map: worker model NAME -> Flow videoModelKey ──
# Confirmed: "Veo 3.1 - Lite [Lower Priority]" == veo_3_1_i2v_lite_low_priority (FlowKit
# TIER_TWO frame_2_video key, matches the worker's default model name exactly).
# Others are FlowKit-derived best guesses and MUST be confirmed via capture_helper.py
# before enabling FLOW_API_MODE=on. NEEDS_CAPTURE marks unconfirmed values.
NEEDS_CAPTURE = "__NEEDS_CAPTURE__"

_DEFAULT_MODEL_MAP = {
    # model_name: { gen_type: { aspect: videoModelKey } }
    "Veo 3.1 - Lite [Lower Priority]": {
        "frame_2_video": {
            "VIDEO_ASPECT_RATIO_PORTRAIT": "veo_3_1_i2v_lite_low_priority",
            "VIDEO_ASPECT_RATIO_LANDSCAPE": "veo_3_1_i2v_lite_low_priority",
        },
        "start_end_frame_2_video": {
            "VIDEO_ASPECT_RATIO_PORTRAIT": "veo_3_1_i2v_lite_low_priority",
            "VIDEO_ASPECT_RATIO_LANDSCAPE": "veo_3_1_i2v_lite_low_priority",
        },
    },
    "Veo 3.1 - Lite": {
        "frame_2_video": {"VIDEO_ASPECT_RATIO_PORTRAIT": NEEDS_CAPTURE},
        "start_end_frame_2_video": {"VIDEO_ASPECT_RATIO_PORTRAIT": NEEDS_CAPTURE},
    },
    "Veo 3.1 - Fast": {
        "frame_2_video": {"VIDEO_ASPECT_RATIO_PORTRAIT": "veo_3_1_i2v_s_fast_portrait"},
        "start_end_frame_2_video": {"VIDEO_ASPECT_RATIO_PORTRAIT": "veo_3_1_i2v_s_fast_portrait_fl"},
    },
    "Veo 3.1 - Quality": {
        "frame_2_video": {"VIDEO_ASPECT_RATIO_PORTRAIT": NEEDS_CAPTURE},
        "start_end_frame_2_video": {"VIDEO_ASPECT_RATIO_PORTRAIT": NEEDS_CAPTURE},
    },
    # Omni Flash runs Ingredients-mode in the UI (single image as ingredient, NOT a
    # start frame). Its submit endpoint + body shape are unconfirmed — capture before use.
    "Omni Flash": {
        "reference_frame_2_video": {"VIDEO_ASPECT_RATIO_PORTRAIT": NEEDS_CAPTURE},
    },
}

# Allow a JSON override file to replace the map once real keys are captured.
_MODEL_MAP_FILE = Path(__file__).parent / "model_map.json"


def load_model_map() -> dict:
    if _MODEL_MAP_FILE.exists():
        try:
            return json.loads(_MODEL_MAP_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    return _DEFAULT_MODEL_MAP


def resolve_model_key(model_name: str, gen_type: str, aspect: str) -> str:
    """Return the videoModelKey for a worker model name, or '' if unknown/uncaptured."""
    mp = load_model_map()
    key = mp.get(model_name, {}).get(gen_type, {}).get(aspect, "")
    if not key or key == NEEDS_CAPTURE:
        return ""
    return key
