"""v807 — pure, dependency-free helpers for prominent-people image
auto-retry. Kept out of main.py so the logic is unit-testable locally
(import main fails in dev on a Starlette version mismatch)."""
import json as _json

VALID_RETRY_MODES = {"off", "next", "prev", "batch"}


def parse_auto_image_retry_mode(settings_json):
    """Extract the account's auto-image-retry mode from a User.settings_json
    string. Defaults to 'batch' (mode C) when missing/invalid. Never raises."""
    if not settings_json:
        return "batch"
    try:
        data = _json.loads(settings_json)
        mode = (data.get("auto_image_retry") or {}).get("mode")
    except Exception:
        return "batch"
    return mode if mode in VALID_RETRY_MODES else "batch"
