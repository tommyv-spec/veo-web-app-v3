"""FlowApiClient — orchestrates upload → submit → poll → resolve, all in-page.

One client per logged-in Flow page. Attribution by media_id UUID throughout, so a
finished clip is always the one we submitted (no DOM tile guessing).
"""
import asyncio
import base64
import logging
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
        self._gate = asyncio.Lock()

    # ─── rate limit (anti-throttle, mirrors FlowKit) ─────────
    async def _cooldown(self):
        async with self._gate:
            elapsed = time.monotonic() - self._last_call
            if elapsed < config.API_COOLDOWN:
                await asyncio.sleep(config.API_COOLDOWN - elapsed)
            self._last_call = time.monotonic()

    async def _token(self) -> str:
        tok = await page_ops.wait_for_token(self._token_store, self.page, timeout=30)
        if not tok:
            raise FlowApiError("no bearer token captured (open/refresh a logged-in Flow tab)")
        return tok

    # ─── upload a frame -> media_id ──────────────────────────
    async def upload_image(self, image_bytes: bytes, file_name: str = "frame.jpg",
                           mime_type: str = "image/jpeg") -> str:
        await self._cooldown()
        b64 = base64.b64encode(image_bytes).decode("ascii")
        body = builders.build_upload_image(b64, self.project_id, file_name, mime_type)
        url = builders.build_url("upload_image")
        res = await page_ops.api_fetch(self.page, url, "POST", await self._token(), body)
        if parsing.is_error(res):
            raise FlowApiError(f"uploadImage failed: {parsing.error_reason(res)}")
        data = res.get("data") or {}
        media_id = (data.get("media") or {}).get("name", "")
        if not parsing.is_uuid(media_id):
            raise FlowApiError(f"uploadImage returned non-UUID media id: {media_id[:40]}")
        return media_id

    # ─── submit a video clip -> (media_id, operation) ────────
    async def submit_video(self, prompt: str, start_media_id: str, model_key: str,
                           scene_id: str, end_media_id: str = "",
                           aspect: str = None) -> tuple[str, dict]:
        if not model_key:
            raise FlowApiError("no videoModelKey resolved (run capture_helper to fill model_map)")
        body = builders.build_generate_video(
            prompt, start_media_id, self.project_id, scene_id, model_key,
            end_media_id=end_media_id, aspect=aspect, tier=self.tier,
        )
        endpoint = "generate_video_start_end" if end_media_id else "generate_video"
        url = builders.build_url(endpoint)
        res = await self._submit_with_captcha(url, body)
        media_id = parsing.extract_video_media_id(res)
        if not media_id:
            raise FlowApiError(f"submit returned no media_id: {parsing.error_reason(res) or res.get('text','')[:200]}")
        return media_id, parsing.extract_operation(res)

    async def _submit_with_captcha(self, url: str, body: dict) -> dict:
        """Mint captcha in-page, inject, POST. Retries captcha-only failures."""
        last = {}
        for attempt in range(config.CAPTCHA_MAX_RETRIES):
            await self._cooldown()
            token = await mint_or_empty(self.page)
            if not token:
                last = {"error": "captcha mint failed"}
                continue
            builders.inject_captcha_token(body, token)
            res = await page_ops.api_fetch(self.page, url, "POST", await self._token(), body)
            if not parsing.is_error(res):
                return res
            reason = parsing.error_reason(res).lower()
            last = res
            if "captcha" in reason or "recaptcha" in reason:
                logger.warning("flow_api: captcha retry %d/%d", attempt + 1, config.CAPTCHA_MAX_RETRIES)
                continue
            # non-captcha error: stop, let caller fall back
            break
        return last

    # ─── poll an operation until done ────────────────────────
    async def poll_until_done(self, operation: dict, timeout: int = None) -> dict:
        timeout = timeout or config.VIDEO_POLL_TIMEOUT
        deadline = time.time() + timeout
        ops = [operation]
        while time.time() < deadline:
            await asyncio.sleep(config.VIDEO_POLL_INTERVAL)
            await self._cooldown()
            url = builders.build_url("check_video_status")
            body = builders.build_check_status(ops)
            res = await page_ops.api_fetch(self.page, url, "POST", await self._token(), body)
            if parsing.is_error(res):
                continue  # transient; keep polling until timeout
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

    # ─── resolve a fresh media URL ───────────────────────────
    async def get_media_url(self, media_id: str) -> str:
        await self._cooldown()
        url = builders.build_url("get_media", media_id=media_id)
        res = await page_ops.api_fetch(self.page, url, "GET", await self._token())
        if parsing.is_error(res):
            return ""
        return parsing.extract_output_url(res)


async def mint_or_empty(page) -> str:
    try:
        return await page_ops.mint_captcha(page, config.CAPTCHA_VIDEO)
    except Exception as e:
        logger.warning("flow_api: captcha mint error: %s", e)
        return ""
