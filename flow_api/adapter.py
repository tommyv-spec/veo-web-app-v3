"""Worker integration seam (sync). ONE function the workers call per generation
when FLOW_API_MODE == 'on'. Raises FlowApiError on any failure so the caller can
fall back to the existing DOM-click path.
"""
import logging

from . import config
from .client import FlowApiClient, FlowApiError

logger = logging.getLogger(__name__)


def generate_clip_via_api(
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
    """Generate one video clip via the Flow private API, fully in-page.

    Returns {"media_id": uuid, "url": str, "path": "api", "operation": dict}.
    Raises FlowApiError on any unrecoverable step (caller falls back to DOM).
    """
    aspect = aspect or config.DEFAULT_ASPECT_RATIO
    gen_type = "start_end_frame_2_video" if end_image_bytes else "frame_2_video"
    model_key = config.resolve_model_key(model_name, gen_type, aspect)
    if not model_key:
        raise FlowApiError(
            f"no videoModelKey for model='{model_name}' type={gen_type} aspect={aspect} "
            f"(fill flow_api/model_map.json via capture)"
        )

    cli = client or FlowApiClient(page, project_id=project_id, tier=tier)

    start_id = cli.upload_image(start_image_bytes, file_name="start.jpg")
    end_id = ""
    if end_image_bytes:
        end_id = cli.upload_image(end_image_bytes, file_name="end.jpg")

    media_id, operation = cli.submit_video(
        prompt=prompt, start_media_id=start_id, model_key=model_key,
        scene_id=scene_id, end_media_id=end_id, aspect=aspect,
    )
    logger.info("flow_api: submitted clip scene=%s media_id=%s model=%s", scene_id, media_id, model_key)

    poll = cli.poll_until_done(operation)
    if poll.get("status") != "done":
        raise FlowApiError(f"poll ended status={poll.get('status')} reason={poll.get('reason','')}")

    url = poll.get("url") or cli.get_media_url(media_id)
    if not url:
        raise FlowApiError(f"clip done but no media URL resolved for {media_id}")

    return {"media_id": media_id, "url": url, "path": "api", "operation": poll.get("operation", {})}


def generate_image_variants_via_api(
    page,
    *,
    prompt: str,
    count: int = 1,
    model_name: str = "Nano Banana 2",
    project_id: str = "",
    reference_image_bytes_list: list = None,
    base_image_bytes: bytes = None,
    aspect: str = None,
    tier: str = "PAYGATE_TIER_TWO",
    client: FlowApiClient = None,
) -> list:
    """Generate N image variants via the private API. Uploads each reference image
    ONCE (shared across variants) then fires N independent submits — cooldown gate
    bypassed for variants 2..N (mimics the UI's parallel fire; cooldown is for
    cross-job anti-spam, not intra-batch).

    Returns a list of {"media_id": uuid, "url": str, "path": "api"} dicts (one per
    successful variant). Raises FlowApiError on the FIRST submit failure — caller
    should fall back to DOM and discard any partial results. If at least one variant
    succeeds the partial list is returned with no exception (caller decides).
    """
    count = max(1, int(count or 1))
    image_model = config.resolve_image_model_name(model_name)
    if not image_model:
        raise FlowApiError(
            f"no imageModelName for model='{model_name}' (see flow_api/config.py IMAGE_MODELS)"
        )

    cli = client or FlowApiClient(page, project_id=project_id, tier=tier)

    # Upload references ONCE; reused across variants.
    base_media_id = ""
    ref_ids = []
    if base_image_bytes:
        base_media_id = cli.upload_image(base_image_bytes, file_name="base.jpg")
    for i, b in enumerate(reference_image_bytes_list or []):
        ref_ids.append(cli.upload_image(b, file_name=f"ref_{i}.jpg"))

    out = []
    for v in range(count):
        try:
            media_id, url = cli.submit_image(
                prompt=prompt,
                image_model_name=image_model,
                reference_media_ids=ref_ids or None,
                base_image_media_id=base_media_id,
                aspect=aspect,
                cooldown=(v == 0),  # only the first call pays cooldown
            )
        except FlowApiError as e:
            if v == 0:
                raise  # nothing worked, signal full fall-back
            logger.warning("flow_api: variant %d/%d failed (%s); returning %d partial", v + 1, count, e, len(out))
            break
        out.append({"media_id": media_id, "url": url, "path": "api"})
        logger.info("flow_api: variant %d/%d generated media_id=%s", v + 1, count, media_id)
    return out


def generate_image_via_api(
    page,
    *,
    prompt: str,
    model_name: str = "Nano Banana 2",
    project_id: str = "",
    reference_image_bytes_list: list = None,
    base_image_bytes: bytes = None,
    aspect: str = None,
    seed: int = None,
    tier: str = "PAYGATE_TIER_TWO",
    client: FlowApiClient = None,
) -> dict:
    """Generate one image through the Flow private API, fully in-page.

    Returns {"media_id": uuid, "url": str, "path": "api"}.
    Raises FlowApiError on any failure (caller falls back to DOM path).
    """
    image_model = config.resolve_image_model_name(model_name)
    if not image_model:
        raise FlowApiError(
            f"no imageModelName for model='{model_name}' "
            f"(see flow_api/config.py IMAGE_MODELS)"
        )

    cli = client or FlowApiClient(page, project_id=project_id, tier=tier)

    base_media_id = ""
    ref_ids = []
    if base_image_bytes:
        base_media_id = cli.upload_image(base_image_bytes, file_name="base.jpg")
    for i, b in enumerate(reference_image_bytes_list or []):
        ref_ids.append(cli.upload_image(b, file_name=f"ref_{i}.jpg"))

    media_id, url = cli.submit_image(
        prompt=prompt,
        image_model_name=image_model,
        reference_media_ids=ref_ids or None,
        base_image_media_id=base_media_id,
        aspect=aspect,
        seed=seed,
    )
    logger.info("flow_api: image generated model=%s media_id=%s", image_model, media_id)
    return {"media_id": media_id, "url": url, "path": "api"}
