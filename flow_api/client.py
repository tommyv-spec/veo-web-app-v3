"""FlowApiClient (sync) — orchestrates upload → submit → poll → resolve, all in-page.

Sync flavor. One client per logged-in Flow page. Attribution by media_id UUID
throughout, so a finished clip is always the one we submitted (no DOM tile guessing).
"""
import base64
import logging
import threading
import time

from . import builders, config, page_ops, parsing

logger = logging.getLogger(__name__)


class FlowApiError(Exception):
    """Raised on an unrecoverable API step so the caller can fall back to the DOM path."""


class FlowApiClient:
    def __init__(self, page, project_id: str = "", tier: str = "PAYGATE_TIER_TWO"):
        self.page = page
        self.project_id = project_id
        self.tier = tier
        self._token_store = page_ops.install_token_capture(page)
        self._last_call = 0.0
        self._gate = threading.Lock()

    # ─── rate limit (anti-throttle, mirrors FlowKit) ─────────
    def _cooldown(self):
        with self._gate:
            elapsed = time.monotonic() - self._last_call
            if elapsed < config.API_COOLDOWN:
                time.sleep(config.API_COOLDOWN - elapsed)
            self._last_call = time.monotonic()

    def _token(self) -> str:
        tok = page_ops.wait_for_token(self._token_store, self.page, timeout=30)
        if not tok:
            raise FlowApiError("no bearer token captured (open/refresh a logged-in Flow tab)")
        return tok

    # ─── upload a frame -> media_id ──────────────────────────
    def upload_image(self, image_bytes: bytes, file_name: str = "frame.jpg",
                     mime_type: str = "image/jpeg") -> str:
        self._cooldown()
        b64 = base64.b64encode(image_bytes).decode("ascii")
        body = builders.build_upload_image(b64, self.project_id, file_name, mime_type)
        url = builders.build_url("upload_image")
        res = page_ops.api_fetch(self.page, url, "POST", self._token(), body)
        if parsing.is_error(res):
            raise FlowApiError(f"uploadImage failed: {parsing.error_reason(res)}")
        data = res.get("data") or {}
        media_id = (data.get("media") or {}).get("name", "")
        if not parsing.is_uuid(media_id):
            raise FlowApiError(f"uploadImage returned non-UUID media id: {media_id[:40]}")
        return media_id

    # ─── submit a video clip -> (media_id, operation) ────────
    def submit_video(self, prompt: str, start_media_id: str, model_key: str,
                     scene_id: str, end_media_id: str = "",
                     aspect: str = None):
        if not model_key:
            raise FlowApiError("no videoModelKey resolved (run capture_helper to fill model_map)")
        body = builders.build_generate_video(
            prompt, start_media_id, self.project_id, scene_id, model_key,
            end_media_id=end_media_id, aspect=aspect, tier=self.tier,
        )
        endpoint = "generate_video_start_end" if end_media_id else "generate_video"
        url = builders.build_url(endpoint)
        res = self._submit_with_captcha(url, body)
        media_id = parsing.extract_video_media_id(res)
        if not media_id:
            raise FlowApiError(f"submit returned no media_id: {parsing.error_reason(res) or res.get('text','')[:200]}")
        return media_id, parsing.extract_operation(res)

    # ─── image generation (Nano Banana) ──────────────────────
    def submit_image(self, prompt: str, image_model_name: str,
                     reference_media_ids: list = None,
                     base_image_media_id: str = "",
                     aspect: str = None,
                     seed: int = None,
                     cooldown: bool = True):
        """Synchronous image gen (Nano Banana). Returns (media_id UUID, fife/imageUri).

        `cooldown=False` skips the inter-call cooldown — use for variants of the
        same batch (mimics the UI's parallel-fire behavior; cooldown is for cross-job
        anti-spam, not intra-batch variants)."""
        if not image_model_name:
            raise FlowApiError("no imageModelName resolved (check IMAGE_MODELS)")
        body = builders.build_generate_image(
            prompt=prompt, project_id=self.project_id,
            image_model_name=image_model_name,
            aspect=aspect, seed=seed,
            reference_media_ids=reference_media_ids,
            base_image_media_id=base_image_media_id,
            tier=self.tier,
        )
        url = builders.build_url("generate_images", project_id=self.project_id)
        res = self._submit_with_captcha(url, body, action=config.CAPTCHA_IMAGE, cooldown=cooldown)
        media_id = parsing.extract_image_media_id(res)
        if not media_id:
            raise FlowApiError(
                f"image submit returned no media_id: "
                f"{parsing.error_reason(res) or (res.get('text','') or '')[:200]}"
            )
        return media_id, parsing.extract_image_url(res)

    def _submit_with_captcha(self, url: str, body: dict, action: str = None,
                              cooldown: bool = True) -> dict:
        """Mint captcha in-page, inject, POST. Retries captcha-only failures.

        cooldown=False skips the inter-call gate for intra-batch variants."""
        action = action or config.CAPTCHA_VIDEO
        last = {}
        for attempt in range(config.CAPTCHA_MAX_RETRIES):
            if cooldown:
                self._cooldown()
            token = mint_or_empty(self.page, action=action)
            if not token:
                last = {"error": "captcha mint failed"}
                continue
            builders.inject_captcha_token(body, token)
            res = page_ops.api_fetch(self.page, url, "POST", self._token(), body)
            if not parsing.is_error(res):
                return res
            reason = parsing.error_reason(res).lower()
            last = res
            if "captcha" in reason or "recaptcha" in reason:
                logger.warning("flow_api: captcha retry %d/%d", attempt + 1, config.CAPTCHA_MAX_RETRIES)
                continue
            break
        return last

    # ─── poll an operation until done ────────────────────────
    def poll_until_done(self, operation: dict, timeout: int = None) -> dict:
        timeout = timeout or config.VIDEO_POLL_TIMEOUT
        deadline = time.time() + timeout
        ops = [operation]
        while time.time() < deadline:
            time.sleep(config.VIDEO_POLL_INTERVAL)
            self._cooldown()
            url = builders.build_url("check_video_status")
            body = builders.build_check_status(ops)
            res = page_ops.api_fetch(self.page, url, "POST", self._token(), body)
            if parsing.is_error(res):
                continue
            data = res.get("data") or {}
            ops = data.get("operations", ops)
            if not ops:
                continue
            status = parsing.poll_status(ops[0])
            if status == parsing.STATUS_SUCCESS:
                return {"status": "done", "operation": ops[0], "url": parsing.extract_output_url(res)}
            if status == parsing.STATUS_FAILED:
                return {"status": "failed", "operation": ops[0], "reason": parsing.error_reason(res)}
        return {"status": "timeout"}

    def get_media_url(self, media_id: str) -> str:
        self._cooldown()
        url = builders.build_url("get_media", media_id=media_id)
        res = page_ops.api_fetch(self.page, url, "GET", self._token())
        if parsing.is_error(res):
            return ""
        return parsing.extract_output_url(res)


def mint_or_empty(page, action: str = None) -> str:
    action = action or config.CAPTCHA_VIDEO
    try:
        return page_ops.mint_captcha(page, action)
    except Exception as e:
        logger.warning("flow_api: captcha mint error: %s", e)
        return ""
