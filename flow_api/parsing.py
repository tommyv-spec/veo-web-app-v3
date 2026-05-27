"""Parse Flow API responses. Attribution is by media_id UUID — never by tile index.

Ported from FlowKit agent/worker/_parsing.py. Rule #1: mediaId is a UUID;
mediaGenerationId is a base64 protobuf (CAMS...) — do NOT use it.
"""
import re

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I
)
_UUID_IN_URL_RE = re.compile(
    r"/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})", re.I
)


def is_uuid(value: str) -> bool:
    return bool(value) and bool(_UUID_RE.match(value))


def uuid_from_url(url: str) -> str:
    m = _UUID_IN_URL_RE.search(url or "")
    return m.group(1) if m else ""


def is_error(result: dict) -> bool:
    """True if an api_fetch result represents a failure."""
    if not isinstance(result, dict):
        return True
    if result.get("error"):
        return True
    status = result.get("status")
    if isinstance(status, int) and status >= 400:
        return True
    data = result.get("data")
    if isinstance(data, dict) and data.get("error"):
        return True
    return False


def error_reason(result: dict) -> str:
    """Human/string reason for a failed result (for retry classification)."""
    if not isinstance(result, dict):
        return "non-dict result"
    if result.get("error"):
        return str(result["error"])
    data = result.get("data")
    if isinstance(data, dict) and data.get("error"):
        err = data["error"]
        if isinstance(err, dict):
            return str(err.get("message") or err.get("status") or err)
        return str(err)
    status = result.get("status")
    if isinstance(status, int) and status >= 400:
        return f"HTTP {status}: {str(result.get('text') or '')[:300]}"
    return ""


def extract_video_media_id(result: dict) -> str:
    """media_id UUID of a submitted video clip, from the submit response.

    Shape: data.operations[0].operation.metadata.video.mediaId (must be UUID).
    Falls back to UUID parsed out of fifeUrl. Returns '' if none found.
    """
    data = result.get("data", result) if isinstance(result, dict) else {}
    if not isinstance(data, dict):
        return ""
    ops = data.get("operations", [])
    if not ops:
        return ""
    video_meta = (
        ops[0].get("operation", {}).get("metadata", {}).get("video", {})
    )
    val = video_meta.get("mediaId", "")
    if is_uuid(val):
        return val
    fife = video_meta.get("fifeUrl", "")
    got = uuid_from_url(fife)
    if got:
        return got
    return ""


def extract_operation(result: dict) -> dict:
    """The operation object to feed back into batchCheckAsyncVideoGenerationStatus."""
    data = result.get("data", result) if isinstance(result, dict) else {}
    if not isinstance(data, dict):
        return {}
    ops = data.get("operations", [])
    return ops[0] if ops else {}


# Status-poll enums
STATUS_SUCCESS = "MEDIA_GENERATION_STATUS_SUCCESSFUL"
STATUS_FAILED = "MEDIA_GENERATION_STATUS_FAILED"
STATUS_PENDING = "MEDIA_GENERATION_STATUS_PENDING"


def poll_status(operation: dict) -> str:
    """Read the status enum off one operation entry from a status-poll response."""
    if not isinstance(operation, dict):
        return STATUS_PENDING
    return operation.get("status", STATUS_PENDING)


def extract_output_url(result_or_op: dict) -> str:
    """Best-effort finished-video URL from a submit/poll/get_media response."""
    data = result_or_op.get("data", result_or_op) if isinstance(result_or_op, dict) else {}
    if not isinstance(data, dict):
        return ""
    ops = data.get("operations", [])
    if ops:
        video_meta = ops[0].get("operation", {}).get("metadata", {}).get("video", {})
        url = video_meta.get("fifeUrl", "")
        if url:
            return url
    # get_media shape
    return data.get("fifeUrl", data.get("servingUri", data.get("videoUri", "")))
