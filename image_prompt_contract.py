"""Model-neutral prompt contract for image jobs with reference inputs.

The platform sends the same scene brief to more than one image backend.  This
module adds only the small, explicit wrapper both backends need: which attached
image is which, what that image controls, and the requested output shape.

The scene brief remains the source of truth.  Keeping this module pure makes
the contract easy to test without a database, browser, or worker process.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List


PROMPT_CONTRACT_MARKER = "IMAGE REFERENCE CONTRACT v2"
PROMPT_CONTRACT_MARKERS = (
    PROMPT_CONTRACT_MARKER,
    "IMAGE REFERENCE CONTRACT v1",
)


def has_prompt_contract(prompt: str) -> bool:
    """Return True for current and still-supported queued contracts."""
    body = prompt or ""
    return any(marker in body for marker in PROMPT_CONTRACT_MARKERS)


def _ordered_inputs(input_images: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        list(input_images or []),
        key=lambda item: int(item.get("slot_order", 0) or 0),
    )


def _clean_role(value: Any, index: int) -> str:
    role = " ".join(str(value or "").split()).strip(" .")
    if role.startswith("variant_chain:"):
        role = role.split(":", 1)[1].strip()
    elif role.startswith("external:"):
        role = role.split(":", 1)[1].strip()
    match = re.fullmatch(r"chain_from_image_(\d+)", role, flags=re.IGNORECASE)
    if match:
        role = f"the prior scene from image {match.group(1)}"
    elif role.lower() == "reference":
        role = "the prior scene"
    elif role.lower() == "subject":
        role = "the main character"
    if " " not in role and "_" in role:
        role = role.replace("_", " ")
    return role or f"reference {index}"


def _clean_instruction(value: Any) -> str:
    """Keep an operator instruction open-ended but safe for one-line output."""
    return " ".join(str(value or "").split()).strip()


def _reference_use(reference_class: str, role: str, reference_intent: str = "") -> str:
    cls = (reference_class or "other").strip().lower()
    intent = (reference_intent or "").strip().lower()
    if cls == "persona":
        return (
            f"Identity reference for {role}. Keep the same face, defining facial "
            "features, apparent age, skin tone, hair, body shape, and proportions "
            "exactly as this reference shows them — rendered as they are, with "
            "natural skin texture rather than smoothing or restyling. Follow "
            "the scene brief for pose, expression, wardrobe, action, framing, and setting."
        )
    if cls == "product":
        return (
            f"Exact appearance reference for {role}. Keep its shape, packaging, label, "
            "colors, logo, materials, and proportions unless the "
            "scene brief explicitly requests a visible change."
        )
    if cls == "chain":
        if intent == "body":
            return (
                f"Body or pose reference for {role}. Keep or transfer only the body "
                "shape, proportions, pose, limb position, or clothing fit that the "
                "scene brief names. Do not replace the main identity, face, product, "
                "setting, camera, background, or unrelated objects with details from this image."
            )
        if intent not in ("", "continuity"):
            return (
                f"Reference for the stated {intent} job of {role}. Transfer or keep only "
                "the details the scene brief assigns to this image. Do not copy unrelated details."
            )
        return (
            f"Continuity or base-scene reference for {role}. Keep all "
            "people, setting geometry, camera relationship, object placement, and "
            "other unchanged details; apply only the changes requested by the scene brief."
        )
    return (
        f"Reference only for the stated role of {role} and for any transfer or relationship "
        "the scene brief explicitly assigns to it. Do not borrow unrelated traits."
    )


_LEGACY_MANIFEST_LINE = re.compile(
    r"^\s*Use Image \d+ for [^.\n]+\.\s*$",
    flags=re.IGNORECASE | re.MULTILINE,
)


def strip_legacy_manifest_lines(prompt: str) -> str:
    """Remove the old generated manifest lines before adding the current contract.

    This matches the same narrow line shape the Banana worker already removes.
    Other body references to ``Image N`` remain untouched.
    """
    cleaned = _LEGACY_MANIFEST_LINE.sub("", prompt or "")
    return re.sub(r"\n{3,}", "\n\n", cleaned).strip()


def build_image_prompt_contract(
    body: str,
    input_images: Iterable[Dict[str, Any]],
    aspect_ratio: str = "",
    backend: str = "banana",
) -> str:
    """Wrap a scene brief in the versioned, numbered reference contract.

    ``input_images`` must describe the files in the same slot order used by the
    worker. Each item may include ``role``, ``reference_class``, and an optional
    open ``reference_instruction``. The explicit instruction is authoritative;
    class-based behavior is only a fallback.
    """
    refs = _ordered_inputs(input_images)
    scene = strip_legacy_manifest_lines(body)

    sections = [PROMPT_CONTRACT_MARKER]
    # v912.3 — both vendors' official guides ask for the INTENT up front (Google:
    # "provide context and intent"; OpenAI: "include the intended use to set the
    # mode"), and for the word "photorealistic" stated plainly to engage the
    # photorealistic mode. One line, before anything else.
    sections.append(
        "Goal: one photorealistic, unposed phone snapshot for a social video. "
        "Combine the numbered reference images and the scene brief into a single "
        "coherent photograph captured in the moment."
    )
    if refs:
        lines = [
            "REFERENCE IMAGES",
            "Image numbering below matches the attachment order.",
        ]
        for index, item in enumerate(refs, start=1):
            role = _clean_role(item.get("role"), index)
            ref_class = str(item.get("reference_class") or "other").strip().lower()
            instruction = _clean_instruction(item.get("reference_instruction"))
            lines.append(f"Image {index} - Role: {role}.")
            if instruction:
                lines.append(f"Use (authoritative): {instruction}")
            else:
                lines.append(
                    f"Use (fallback): {_reference_use(ref_class, role, item.get('reference_intent') or '')}"
                )
        lines.append(
            "Read each Use line as four parts: what to take from that image, where "
            "to apply it, what to preserve untouched, and what to ignore. Follow "
            "each image's Use line. An authoritative Use line wins over any "
            "conflicting scene instruction. A fallback Use line defers to the scene "
            "brief only where that fallback says it does. Keep every identity, "
            "product, style, pose, and background bound to its own reference; "
            "combine them only as these rules direct."
        )
        sections.append("\n".join(lines))

    sections.append("SCENE TO CREATE\n" + scene)

    output_lines = ["OUTPUT"]
    aspect = " ".join(str(aspect_ratio or "").split())
    if aspect:
        orientation = {
            "9:16": "vertical",
            "16:9": "horizontal",
            "1:1": "square",
        }.get(aspect)
        label = f"{orientation} {aspect}" if orientation else aspect
        output_lines.append(f"Aspect ratio: {label}.")
    output_lines.append(
        "Render as a real photograph captured in the moment: natural skin texture "
        "with visible pores and fine lines where faces appear, real fabric weave "
        "and worn surfaces, everyday imperfection, honest unstaged framing."
    )
    output_lines.append(
        "The scene brief is the source of truth for the final scene except that it "
        "cannot override an authoritative Use line. Preserve every reference detail "
        "that the permitted changes do not need to alter, and keep those preserved "
        "details identical in the output."
    )
    sections.append("\n".join(output_lines))

    prompt = "\n\n".join(sections).strip()
    if (backend or "banana").strip().lower() == "chatgpt":
        return "Crea immagine:\n" + prompt
    return prompt
