"""Pure job -> ChatGPT mapping. No browser, no filesystem — unit-testable.

Maps a platform image job (DATA_DIR/_image_jobs/node_<id>.json) into the prompt
+ reference list the ChatGPT drive core consumes, and builds the .done.json
payload the platform polls for.
"""

_ASPECT = {"9:16": "vertical 9:16", "16:9": "horizontal 16:9"}
_PROMPT_CONTRACT_MARKERS = (
    "IMAGE REFERENCE CONTRACT v2",
    "IMAGE REFERENCE CONTRACT v1",
)


def aspect_phrase(aspect_ratio):
    """ChatGPT has no size param -> encode aspect in prompt text. '' if unknown."""
    return _ASPECT.get((aspect_ratio or "").strip(), "")


def ref_paths(job):
    """Reference image paths ordered by slot_order."""
    imgs = job.get("input_images") or []
    return [i["path"] for i in sorted(imgs, key=lambda i: i.get("slot_order", 0))]


def build_prompt(job):
    """Compose the ChatGPT prompt: image trigger + body + per-ref role lines + aspect."""
    body = (job.get("render_prompt") or job.get("prompt") or "").strip()
    # v909: the server already produced a numbered, backend-specific prompt.
    # Returning it byte-for-byte avoids a second trigger, role map, or aspect.
    # Unmarked queued/legacy jobs keep the fallback below.
    if any(marker in body for marker in _PROMPT_CONTRACT_MARKERS):
        return body
    lines = [f"Crea immagine: {body}"]
    for img in sorted(job.get("input_images") or [], key=lambda i: i.get("slot_order", 0)):
        role = (img.get("role") or "").strip()
        if role:
            lines.append(f"use the uploaded reference for {role}.")
    ap = aspect_phrase(job.get("aspect_ratio"))
    if ap:
        lines.append(f"The image is {ap}.")
    return "\n".join(lines)


def done_payload(job_id, status, output_paths, error):
    """The node_<id>.done.json body the platform polls for."""
    return {"id": job_id, "status": status,
            "output_paths": list(output_paths), "error": error}
