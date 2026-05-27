"""Capture REAL Flow submit traffic to confirm the unknowns before enabling the API path.

Unknowns to fill (see config.NEEDS_CAPTURE):
  - exact videoModelKey strings for Veo 3.1 Lite / Quality (and confirm Fast)
  - Omni Flash "Ingredients" submit: endpoint + body shape

Usage (operator, with the worker's logged-in Patchright page):
    from flow_api.capture_helper import attach_capture
    cap = attach_capture(page, out_path="flow_api_capture.jsonl")
    # ... now do ONE manual generate per model/mode in the Flow tab ...
    # Each matching POST is appended to flow_api_capture.jsonl with url + body.

Then read flow_api_capture.jsonl: the videoModelKey + endpoint per model name go into
flow_api/model_map.json (overrides config defaults).
"""
import json
import logging
import time

logger = logging.getLogger(__name__)

_WATCH = (
    "batchAsyncGenerateVideoStartImage",
    "batchAsyncGenerateVideoStartAndEndImage",
    "batchAsyncGenerateVideoReferenceImages",
    "batchAsyncGenerateVideo",  # catch any other Omni/variant submit
    "uploadImage",
    "batchCheckAsyncVideoGenerationStatus",
)


def attach_capture(page, out_path: str = "flow_api_capture.jsonl"):
    """Append every matching Flow API POST (url + parsed body) to out_path as JSONL."""

    def _on_request(req):
        try:
            url = req.url or ""
            if "aisandbox-pa.googleapis.com" not in url:
                return
            if not any(w in url for w in _WATCH):
                return
            body = None
            try:
                body = req.post_data  # str or None
            except Exception:
                body = None
            parsed = None
            if body:
                try:
                    parsed = json.loads(body)
                except json.JSONDecodeError:
                    parsed = {"_raw": body[:4000]}
            entry = {
                "ts": time.time(),
                "method": req.method,
                "url": url,
                "videoModelKey": _dig_model_key(parsed),
                "body": parsed,
            }
            with open(out_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
            logger.info("flow_api capture: %s modelKey=%s", url.split("?")[0], entry["videoModelKey"])
        except Exception as e:
            logger.warning("flow_api capture error: %s", e)

    page.on("request", _on_request)
    logger.info("flow_api capture attached -> %s", out_path)
    return _on_request


def _dig_model_key(parsed) -> str:
    if not isinstance(parsed, dict):
        return ""
    for req in parsed.get("requests", []) or []:
        if isinstance(req, dict) and req.get("videoModelKey"):
            return req["videoModelKey"]
    return ""
