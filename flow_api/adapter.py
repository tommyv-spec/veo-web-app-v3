"""Worker integration seam. ONE function the Flow worker calls per clip when
FLOW_API_MODE == 'on'. Raises FlowApiError on any failure so the caller can fall
back to the existing DOM-click path for that clip.

Wiring (see README.md): in the per-clip generate step, instead of clicking the
arrow_forward Generate button, call generate_clip_via_api(...). On FlowApiError,
run the existing DOM path. Log which path ran (path=api|dom) + the media_id.
"""
import logging

from . import config
from .client import FlowApiClient, FlowApiError

logger = logging.getLogger(__name__)


async def generate_clip_via_api(
    page,
    *,
    prompt: str,
    start_image_bytes: bytes,
    model_name: str,
    scene_id: str = "",
    project_id: str = "",
    end_image_bytes: bytes = None,
    aspect: str = None,
    tier: str = "PAYGATE_TIER_TWO",
    client: FlowApiClient = None,
) -> dict:
    """Generate one clip through the Flow private API, fully in-page.

    Returns {"media_id": uuid, "url": str, "path": "api", "operation": dict}.
    Raises FlowApiError on any unrecoverable step (caller falls back to DOM).
    """
    aspect = aspect or config.DEFAULT_ASPECT_RATIO
    gen_type = "start_end_frame_2_video" if end_image_bytes else "frame_2_video"
    model_key = config.resolve_model_key(model_name, gen_type, aspect)
    if not model_key:
        raise FlowApiError(
            f"no videoModelKey for model='{model_name}' type={gen_type} aspect={aspect} "
            f"(fill flow_api/model_map.json via capture_helper.py)"
        )

    cli = client or FlowApiClient(page, project_id=project_id, tier=tier)

    start_id = await cli.upload_image(start_image_bytes, file_name="start.jpg")
    end_id = ""
    if end_image_bytes:
        end_id = await cli.upload_image(end_image_bytes, file_name="end.jpg")

    media_id, operation = await cli.submit_video(
        prompt=prompt, start_media_id=start_id, model_key=model_key,
        scene_id=scene_id, end_media_id=end_id, aspect=aspect,
    )
    logger.info("flow_api: submitted clip scene=%s media_id=%s model=%s", scene_id, media_id, model_key)

    poll = await cli.poll_until_done(operation)
    if poll.get("status") != "done":
        raise FlowApiError(f"poll ended status={poll.get('status')} reason={poll.get('reason','')}")

    url = poll.get("url") or await cli.get_media_url(media_id)
    if not url:
        raise FlowApiError(f"clip done but no media URL resolved for {media_id}")

    return {"media_id": media_id, "url": url, "path": "api", "operation": poll.get("operation", {})}
