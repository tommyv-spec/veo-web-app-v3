"""
v589 Stage 4d — VLM-grounded video understanding (provider-agnostic).

Adds a structured video-understanding pass to the v579 decode pipeline. Produces
a per-shot action-arc JSON (stage4d_vlm.json) that becomes the AUTHORITATIVE
source for visual action arcs (parallel to whisper.cpp being authoritative for
dialogue).

The schema captures: static_composition + action_arc (start_state / mid_state /
end_state / magnitude COMPLETE/PARTIAL/MINIMAL / verbs_observed) + audio +
veo_reproduction_hints.

Provider cascade (first available wins):
  1. LM Studio  — local OpenAI-compatible server at http://localhost:1234.
                  Detected by GET /v1/models. Recommended free path:
                  install LM Studio, load a vision-capable model
                  (e.g. gemma-4-E2B-it-GGUF with mmproj), open the app,
                  enable the local server. Uploads dense frames + transcript;
                  no per-call cost.
  2. Gemini API — when GEMINI_API_KEY env var is set. Native MP4 upload
                  (1fps + audio + per-second timestamps). Default model
                  gemini-3.6-flash (~$0.05-0.10 per 45s decode with
                  thinking; gemini-2.5-flash rejects NEW keys since 2026).
                  Cheap alt: --model gemini-3-flash-preview (~$0.02).
                  Free tier covers many decodes/day.
                  Gemini-only knobs: --thinking (default high), --fps
                  (raise to 2-5 for fast motion), --media-resolution low
                  (cheap mode). Every call's token usage + USD cost is
                  appended to output/gemini_costs.jsonl (override with
                  GEMINI_COST_LEDGER env var); print the ledger with
                  `python v589_video_understanding.py --costs`.
  3. Human-walk template (always available) — when no automated provider is
                  configured, this script writes a stage4d_vlm.json TEMPLATE
                  skeleton with empty fields per shot. The human-walking
                  decoder LLM session (Claude in the chat) walks the dense
                  frames produced by v588 and fills in the JSON manually.
                  The v589 STRUCTURAL RULE still holds — the schema is
                  produced, just by a human walker instead of an API.

Usage:
    python v589_video_understanding.py <source.mp4>
    # auto-detects which provider to use; pass --provider to force one

Output: stage4d_vlm.json next to the input video (or per --out).
"""
from __future__ import annotations
import argparse
import copy
import json
import os
import sys
import time
from pathlib import Path


# ──────────────────────────────────────────────────────────────────────
# Schema + prompts (shared across providers)
# ──────────────────────────────────────────────────────────────────────

STAGE4D_SCHEMA_VERSION = "stage4d.v2"
PRIMARY_CHANGE_AXES = [
    "Surface/Texture",
    "Structural Integrity",
    "Volume/Shape",
    "Color/Illumination",
    "NONE",
]
MORPHOLOGY_MAGNITUDES = ["COMPLETE", "PARTIAL", "MINIMAL", "NONE"]

# ── Reading profiles (2026-08-12) ─────────────────────────────────────
# One decode engine, per-format reading profiles. "ugc-reel" is the default
# and is byte-identical to the pre-profile behavior. "fbads-video" reads
# paid Facebook-ad creatives: same per-shot protocol PLUS a top-level
# ad_read block (offer / CTA / overlays / captions / sound-off / end card).
# The profile's "context" text declares the format and rides in the <context>
# data block; its "task" text is an instruction and rides in <task>.
# The ad_read flag attaches AD_READ_SCHEMA; see schema_for_profile() below.

FBADS_FORMAT_DECLARATION = (
    "This video is a PAID Facebook/Instagram AD CREATIVE (direct response), "
    "not an organic reel.\n\n"
)

FBADS_READ_INSTRUCTIONS = (
    "On top of the per-shot protocol, fill the top-level ad_read object: "
    "capture every offer element (price, discount, bundle, guarantee, "
    "urgency/scarcity), every CTA quoted verbatim (spoken + overlay + end "
    "card), the full overlay-text timeline with each overlay's on-screen "
    "span, burned-caption style, what a MUTED viewer understands (most ad "
    "views are sound-off), the aspect ratio, whether key content sits in "
    "center-safe zones, the end card, and every logo/product appearance "
    "with MM:SS timing.\n"
)

READING_PROFILES = {
    "ugc-reel": {"context": "", "task": "", "ad_read": False},
    "fbads-video": {
        "context": FBADS_FORMAT_DECLARATION,
        "task": FBADS_READ_INSTRUCTIONS,
        "ad_read": True,
    },
}


def _string_schema(description: str) -> dict:
    return {"type": "string", "description": description}


PER_SHOT_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "shot_index", "start", "end", "summary", "forensic_perception",
        "static_composition", "frame_inventory", "start_frame_spec", "action_arc",
        "audio", "motion_cross_check", "veo_reproduction_hints", "human_walk_corrections",
    ],
    "properties": {
        "shot_index": {"type": "integer", "minimum": 1},
        "start": {"type": "number"},
        "end": {"type": "number"},
        "summary": _string_schema("One sentence: source-derived job plus visible action."),
        "forensic_perception": {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "kinematic_traces", "z_depth_layers", "literal_vfx_observations",
                "intrinsic_state_isolation",
            ],
            "properties": {
                "kinematic_traces": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["body_part_or_object", "pixel_path", "origin_or_owner", "evidence"],
                        "properties": {
                            "body_part_or_object": _string_schema("The limb, body part, or moving object traced."),
                            "pixel_path": _string_schema("Visible path from extremity/object back to its origin."),
                            "origin_or_owner": _string_schema("Observed owner or source of the movement."),
                            "evidence": _string_schema("Clothing, contact, or pixel-continuity evidence."),
                        },
                    },
                },
                "z_depth_layers": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["foreground", "midground", "background"],
                    "properties": {
                        "foreground": _string_schema("Closest visible layer, including occlusion."),
                        "midground": _string_schema("Middle visible layer."),
                        "background": _string_schema("Furthest visible layer."),
                    },
                },
                "literal_vfx_observations": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Literal impossible VFX or an empty list when none are visible.",
                },
                "intrinsic_state_isolation": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "focus_object", "axis_1_surface_texture", "axis_2_structural_integrity",
                        "axis_3_volume_shape", "axis_4_color_illumination",
                        "primary_change_axis", "intrinsic_state_start", "intrinsic_state_end",
                    ],
                    "properties": {
                        "focus_object": _string_schema("The object whose intrinsic state is checked."),
                        "axis_1_surface_texture": _string_schema("Observed surface/texture state and change."),
                        "axis_2_structural_integrity": _string_schema("Observed structural state and change."),
                        "axis_3_volume_shape": _string_schema("Observed volume/shape state and change."),
                        "axis_4_color_illumination": _string_schema("Observed color/light state and change."),
                        "primary_change_axis": {"type": "string", "enum": PRIMARY_CHANGE_AXES},
                        "intrinsic_state_start": _string_schema("Literal state at the first observed frame."),
                        "intrinsic_state_end": _string_schema("Literal state at the last observed frame."),
                    },
                },
            },
        },
        "static_composition": {
            "type": "object",
            "additionalProperties": False,
            "required": ["subject", "framing", "anchor_props_with_positions", "lighting_and_palette"],
            "properties": {
                "subject": _string_schema(
                    "Observed person labels, pose, eye direction, mouth state, and expression. "
                    "Wardrobe and identity markers belong in top-level observed_people."
                ),
                "framing": _string_schema(
                    "Subject-to-subject geometry, detail-density distance, depth, crop, and background. "
                    "Do not use frame-grid positions; viewer-left/right is allowed only for a literal lateral limb vector."
                ),
                "anchor_props_with_positions": _string_schema(
                    "Every visible prop positioned relative to people or other objects, not frame quadrants."
                ),
                "lighting_and_palette": _string_schema("Observed lighting direction, palette, and source mood."),
            },
        },
        # v890b — FRAME INVENTORY. The CONTENT half of a rebuildable frame:
        # start_frame_spec fixes the camera and the light, this fixes what is
        # actually in the shot. Together they let a decode's `### Image N` block
        # carry a full image prompt at the same level of detail as a build's, and
        # they make backgrounds / palettes / props / wardrobe comparable field by
        # field across the siblings of a decode family.
        "frame_inventory": {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "set_and_background", "wardrobe_and_grooming", "props_and_objects",
                "on_screen_text_verbatim", "color_palette", "production_method",
            ],
            "properties": {
                "set_and_background": _string_schema(
                    "What the space actually is, its surfaces and architecture, and everything visible at depth "
                    "with its colour and material — even when defocused. 'Blurred background' is not an answer; "
                    "say what is blurred."
                ),
                "wardrobe_and_grooming": _string_schema(
                    "Per person in this shot: every garment with its exact colour, cut and fabric, plus "
                    "accessories, jewellery, badges, hair state and facial hair."
                ),
                "props_and_objects": _string_schema(
                    "Every object in frame with position relative to a person or another object, colour, "
                    "material and current state. Include tools, containers, food, packaging and set dressing. "
                    "For EACH object also say WHOSE it is and WHICH LAYER it sits on — an object near a person "
                    "is not automatically theirs. In an assembled frame the tells are decisive and visible: an "
                    "object rendered in the background plate's colour grade (monochrome while the subject is in "
                    "colour), sitting behind the subject's matte edge, or holding still while the subject moves, "
                    "belongs to the PLATE and to whoever is in it — not to the person keyed in front of it. "
                    "Write it as 'object — layer — owner — the tell', e.g. 'Shure microphone — background plate "
                    "— the seated man's, in his studio — monochrome and occluded by her cut-out edge'."
                ),
                "on_screen_text_verbatim": _string_schema(
                    "Every legible word on screen, quoted exactly, with where it sits and how it is styled "
                    "(case, weight, colour, and whether it sits on a solid plate or is outlined text over the "
                    "image — these are different treatments and often BOTH appear in one frame). Also record "
                    "NON-TEXT drawn marks here: hand-drawn scribbles, circles, arrows and doodles added on top "
                    "of the image, saying what they cover and their style. 'none' when the frame carries "
                    "neither text nor drawn marks."
                ),
                "color_palette": _string_schema(
                    "The three to six dominant colours as plain names, each tied to what carries it "
                    "(e.g. 'maroon — shirt; teal — back wall; warm amber — key on skin')."
                ),
                "production_method": _string_schema(
                    "HOW THIS FRAME WAS MADE, not what is in it. Was everything captured together in "
                    "one camera take, or assembled? Name the assembly when you see it: a green-screen "
                    "or cut-out subject keyed over a separate background, a still image with a moving "
                    "element added on top, a screen-recording layer, a picture-in-picture inset, a "
                    "split screen, or a graphics-only card. Cite the tell — a hard matte edge around "
                    "the subject, lighting or grain that does not match between layers, a background "
                    "that never moves while the subject does, a shadow that is missing or painted on, "
                    "or perspective that does not agree. This decides how the frame is REBUILT: a "
                    "composite has to be rebuilt as a composite, and describing it as a single photo "
                    "produces something that cannot be made the same way."
                ),
            },
        },
        # v890 — START-FRAME RECREATION SPEC. static_composition DESCRIBES a shot;
        # this REBUILDS it. Everything here is a camera/light/grade decision an
        # image prompt has to state explicitly, because a generator cannot infer
        # "medium close-up" into the right lens, height, crop and defocus. The
        # point is a port that copies the shot exactly and swaps only the slots
        # named in recreation_variable_slots.
        "start_frame_spec": {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "camera_height_and_angle", "camera_distance_and_lens", "subject_placement_and_crop",
                "background_depth_and_defocus", "lighting_setup", "color_temp_and_grade",
                "texture_and_artifacts", "graphics_geometry", "posture_and_gaze",
                "recreation_variable_slots",
            ],
            "properties": {
                "camera_height_and_angle": _string_schema(
                    "Lens height relative to the subject's eyes and any tilt: level with the eyes, "
                    "slightly above/below by roughly how much, straight-on or off-axis by roughly how many degrees."
                ),
                "camera_distance_and_lens": _string_schema(
                    "Approximate subject distance and the apparent focal length / compression: wide-angle facial "
                    "distortion, neutral ~35-50mm, or compressed ~85mm+. Say which cue shows it (ear-to-nose "
                    "relationship, background size relative to the subject)."
                ),
                "subject_placement_and_crop": _string_schema(
                    "Where the head sits horizontally, how much headroom above the hair, and the exact crop line "
                    "on the body (collarbone / mid-chest / waist), named against a garment or body landmark."
                ),
                "background_depth_and_defocus": _string_schema(
                    "How far the background sits behind the subject and how defocused it is (readable, softly "
                    "blurred, fully abstract), plus what is actually back there."
                ),
                "lighting_setup": _string_schema(
                    "Key direction and height, source size / shadow-edge hardness, fill level relative to key, "
                    "and any rim, practical or background light with its position."
                ),
                "color_temp_and_grade": _string_schema(
                    "Warm/cool split between subject and background, overall contrast, black level, saturation."
                ),
                "texture_and_artifacts": _string_schema(
                    "Sharpness, grain or noise, compression, vignette, halation, and the capture look "
                    "(broadcast-clean, phone-camera, cinema)."
                ),
                "graphics_geometry": _string_schema(
                    "Every burned-in overlay with its position, height as a share of the frame, typeface weight, "
                    "case, color and outline. 'none' when the frame is clean."
                ),
                "posture_and_gaze": _string_schema(
                    "Seated or standing, torso lean, shoulder line, hand rest position, and where the eyes point "
                    "relative to the lens."
                ),
                "recreation_variable_slots": _string_schema(
                    "What a port MUST swap versus what it copies: name the identity, setting, product and "
                    "language slots, and state explicitly that camera, lighting, crop and grade are copied."
                ),
            },
        },
        "action_arc": {
            "type": "object",
            "additionalProperties": False,
            "required": ["has_state_evolution", "kinematics", "morphology", "verbs_observed"],
            "properties": {
                "has_state_evolution": {"type": "boolean"},
                "kinematics": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["start_state", "mid_state", "end_state", "movement_path", "camera_shift"],
                    "properties": {
                        "start_state": _string_schema("Observable movement/pose state at shot start."),
                        "mid_state": _string_schema("Observable movement/pose state at midpoint."),
                        "end_state": _string_schema("Observable movement/pose state at shot end."),
                        "movement_path": _string_schema("Ordered subject/object movement path."),
                        "camera_shift": _string_schema("Observed camera movement or static."),
                    },
                },
                "morphology": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "focus_object", "intrinsic_state_start", "intrinsic_state_end",
                        "primary_change_axis", "magnitude",
                    ],
                    "properties": {
                        "focus_object": _string_schema("Object whose intrinsic state changes, or n/a."),
                        "intrinsic_state_start": _string_schema("Literal intrinsic state at shot start."),
                        "intrinsic_state_end": _string_schema("Literal intrinsic state at shot end."),
                        "primary_change_axis": {"type": "string", "enum": PRIMARY_CHANGE_AXES},
                        "magnitude": {"type": "string", "enum": MORPHOLOGY_MAGNITUDES},
                    },
                },
                "verbs_observed": {"type": "array", "items": {"type": "string"}},
            },
        },
        "audio": {
            "type": "object",
            "additionalProperties": False,
            "required": ["dialogue_verbatim", "ambient", "music", "voice_register"],
            "properties": {
                "dialogue_verbatim": _string_schema("Verbatim supplied transcript overlapping this shot, or none."),
                "ambient": _string_schema("Observed ambient or diegetic sound cues, or none."),
                "music": _string_schema("Observed music description, or none/unknown."),
                "voice_register": _string_schema("Observed speaker delivery register, or none."),
            },
        },
        "motion_cross_check": {
            "type": "object",
            "additionalProperties": False,
            "required": ["input_classification", "vlm_observation", "agrees", "discrepancy"],
            "properties": {
                "input_classification": _string_schema("The supplied motion.json classification for this shot."),
                "vlm_observation": _string_schema("The VLM's observed movement level."),
                "agrees": {"type": "boolean"},
                "discrepancy": _string_schema("Difference to reconcile, or none."),
            },
        },
        "veo_reproduction_hints": {
            "type": "object",
            "additionalProperties": False,
            "required": ["use_blend_to_next_scene", "needs_platform_future_image_end", "transition_prompt"],
            "properties": {
                "use_blend_to_next_scene": {"type": "boolean"},
                "needs_platform_future_image_end": {"type": "boolean"},
                "transition_prompt": _string_schema("Source-faithful motion cue with absolute magnitude when complete."),
            },
        },
        "human_walk_corrections": _string_schema(
            "Any point needing dense-frame confirmation, or none when the evidence is clear."
        ),
    },
}


STAGE4D_JSON_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["schema_version", "observed_people", "shots"],
    "properties": {
        "schema_version": {"type": "string", "enum": [STAGE4D_SCHEMA_VERSION]},
        "observed_people": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["label", "identity_markers", "wardrobe", "shots_present"],
                "properties": {
                    "label": _string_schema("Neutral source label such as person_1 or host."),
                    "identity_markers": _string_schema("Observed age range, hair, build, and stable identity cues."),
                    "wardrobe": _string_schema("Observed clothing and accessories, recorded once here."),
                    "shots_present": {"type": "array", "items": {"type": "integer"}},
                },
            },
        },
        "shots": {"type": "array", "minItems": 1, "items": PER_SHOT_SCHEMA},
    },
}

AD_READ_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "offer", "cta", "overlay_text_timeline", "captions",
        "sound_off_comprehension", "aspect_ratio", "safe_zones", "end_card",
        "brand_assets",
    ],
    "properties": {
        "offer": _string_schema(
            "Every offer element shown or spoken: price, discount, bundle, "
            "guarantee, urgency/scarcity. 'none' if absent."),
        "cta": _string_schema(
            "Every call-to-action: spoken line(s), overlay text, end-card "
            "wording. Quote exactly. This is the union of ALL CTA wording "
            "anywhere in the ad. 'none' if absent."),
        "overlay_text_timeline": {
            "type": "array",
            "description": (
                "The whole-ad ordered view of the per-shot graphics_geometry "
                "observations, in chronological order."),
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["time", "end", "text", "style"],
                "properties": {
                    "time": _string_schema("MM:SS the overlay appears."),
                    "end": _string_schema("MM:SS the overlay disappears."),
                    "text": _string_schema("Exact overlay text, verbatim."),
                    "style": _string_schema(
                        "Weight/color/position/animation in plain words."),
                },
            },
        },
        "captions": _string_schema(
            "Burned-in captions: present or absent, style, position, and how "
            "closely they track the spoken words."),
        "sound_off_comprehension": _string_schema(
            "What a MUTED viewer understands from 0-3s and from the whole ad. "
            "Name the visual elements doing that work."),
        "aspect_ratio": _string_schema(
            "e.g. 9:16, 4:5, 1:1."),
        "safe_zones": _string_schema(
            "Whether key content (faces, text, CTA) sits inside center-safe "
            "zones, and what gets clipped if cropped to another placement."),
        "end_card": _string_schema(
            "Final-frame card: product, logo, offer text, CTA button mock. "
            "Describe the final frame's LAYOUT only — its CTA wording also "
            "belongs in cta. 'none' if the ad just ends."),
        "brand_assets": _string_schema(
            "Every logo / product-pack appearance with MM:SS timing."),
    },
}


def _reading_profile(name: str) -> dict:
    try:
        return READING_PROFILES[name]
    except KeyError:
        raise ValueError(
            f"unknown reading profile {name!r}; choose one of "
            f"{', '.join(sorted(READING_PROFILES))}") from None


def schema_for_profile(profile: str) -> dict:
    """Stage 4d response schema for a reading profile. Default = unchanged."""
    schema = copy.deepcopy(STAGE4D_JSON_SCHEMA)
    if _reading_profile(profile)["ad_read"]:
        schema["properties"]["ad_read"] = copy.deepcopy(AD_READ_SCHEMA)
        schema["required"] = list(schema.get("required", [])) + ["ad_read"]
    return schema


SYSTEM_INSTRUCTION = """\
You are a video-understanding assistant for a video reverse-engineering pipeline.

Your job: describe each shot of a viral short-form video with the precision a
generative-video model (Veo 3.1) needs to RE-RENDER it faithfully. Capture
VISIBLE STATE-EVOLUTION ARCS within shots — not just the static composition.

For each shot, output a JSON object matching the schema in your prompt.

Hard rules:
- Capture dialogue verbatim (use the supplied whisper transcript).
- Identify verbs of state change (pour, squeeze, add, stir, mix, melt, dissolve).
- If a shot is a static talking-head with no state evolution, set
  has_state_evolution=false.
- Use ABSOLUTE magnitude language for state changes ('completely melts away',
  'fully revealed', 'entirely dissolves'). 'Dramatically reduced' is forbidden
  when the source shows complete melt — reserved for genuinely partial states.

FORENSIC-PERCEPTION PROTOCOL (v718, MANDATORY pre-grammar):

Before writing ANY composition prose, complete three perceptual steps. Skipping
them produces three observed failure classes — misattribution / blocking
blindness / anatomical normalization — that no amount of downstream grammar
discipline can recover from.

v718a — KINEMATIC TRACING (limb attribution):

VLMs suffer from proximity bias — if a face is near a hand, the VLM assumes
it's their hand. Before attributing any body part, symptom, or held prop to
a character, VISUALLY TRACE THE LIMB back to its origin:

  1. Look at the limb.
  2. Trace the pixels from fingertip / extremity back to the shoulder
     or torso of origin.
  3. Note the CLOTHING COLOR at the shoulder where the limb originates.
  4. Assign the limb ONLY to the character wearing that clothing color.
  5. DO NOT assign ownership based on which face is closest to the limb
     in the 2D frame.

If two characters are close together and a hand reaches across the frame,
the hand belongs to whichever torso the wrist+forearm trace back to — not
the face it appears near.

v890b — FRAME INVENTORY (fill `frame_inventory` on EVERY shot):

The decode's image blocks must be detailed enough to REGENERATE the frame and
to compare backgrounds, palettes, props and wardrobe across sibling videos. So
inventory what is actually there, not an impression of it. "Blurred blue
background" fails both jobs; "acoustic foam panels in teal-blue, lit from
behind, roughly five feet back, defocused to soft vertical bands" serves both.

Quote on-screen text EXACTLY — it is often the strongest single element in the
frame and it is what a port rewrites first. Name colours as plain words tied to
the object carrying them. When something is defocused past recognition, say
what it most likely is AND that it is unreadable, rather than omitting it.

v890 — START-FRAME RECREATION SPEC (fill `start_frame_spec` on EVERY shot):

static_composition DESCRIBES the shot. start_frame_spec REBUILDS it. Write it
for someone who must generate this exact frame from text alone, with a
different person, a different room and a different product in it, and have it
still read as the same shot.

That means naming the decisions a describer skips. "Medium close-up, blurred
background" is not a spec: it does not say whether the lens sits at the
subject's eye height or above it, whether the face shows wide-angle stretch or
telephoto flattening, where the body is cropped, how much air is above the
head, how far back the wall is, which side the key comes from, how hard its
shadow edge is, or how warm the skin is against the background. Every one of
those changes the frame, and every one has to be stated.

Rules for the field:
  * Anchor measurements to something visible — a garment seam, the ear-to-nose
    relationship, the lanyard clip — not to frame percentages you cannot see.
  * Say what the CUE was when you infer a camera decision (background size
    relative to the subject implies compression; visible ear behind the cheek
    implies a longer lens).
  * `recreation_variable_slots` splits the frame into what a port MUST swap
    (identity, setting, product, spoken language) and what it COPIES exactly
    (camera, lighting, crop, grade, graphics geometry). Be explicit about both
    halves — the copy half is the part that makes a port look professional.
  * When a value is genuinely not readable, write "not readable" rather than a
    confident guess. A blank slot is recoverable; a wrong one ships.
  * A REPEATED composition repeats the spec IN FULL. Do not abbreviate later
    shots to "same as before", "setup identical" or a two-word value because
    the setup has not changed — each shot's spec is read on its own by whoever
    rebuilds that frame, and a shortened one silently loses the lens, the crop
    line and the light direction. Ten shots of one composition carry ten
    complete, identical specs.

v718b — Z-DEPTH ISOLATION (blocking detection):

VLMs process frames as flat 2D posters and miss occlusion / depth layering.
Before writing the static_composition, EXPLICITLY MAP THE Z-AXIS:

  1. Identify what is in the ABSOLUTE FOREGROUND (closest to camera, in
     focus, blocking pixels behind it).
  2. Identify what is in the MIDGROUND (one layer behind foreground).
  3. Identify what is in the BACKGROUND (furthest from camera, often
     blurred / out of focus).
  4. Check for OVERLAPPING PIXELS: if Object A covers Object B's pixels,
     Object A is in front of B.
  5. Explicitly note when a character's body part crosses the frame
     horizontally and BLOCKS another character standing behind it.

A patient's arm extended toward camera that crosses in front of a
practitioner's torso = arm is foreground, practitioner is midground. The
practitioner's body is partially OCCLUDED by the arm. Decode this
explicitly; do not treat the two as side-by-side flat-2D companions.

v718c — LITERAL PIXEL VFX RECOGNITION (anti-anatomical-normalization):

Source videos frequently use extreme VFX that violate real-world physics
(flesh loops, floating objects, impossible stretching, detached body
parts, multiplied features). VLMs default to mapping impossible shapes
back to closest normal anatomical concepts because normal anatomy is
familiar prior. THIS IS A HALLUCINATION — the VLM is overriding what
its eyes see with what its training data expects.

If you see shapes that defy normal anatomy:

  - Flesh connecting back to itself to form a closed loop -> describe
    the LOOP, the HOLE in the flesh, the CIRCULAR CONNECTION. Do not
    snap to "deep U-shape sagging" because U-shape is the closest
    normal-anatomy concept; U-shape is open, a LOOP is closed.
  - Objects floating with no visible support -> describe the FLOAT
    explicitly; do not invent invisible attachment.
  - Impossible stretching (skin stretched 12 inches) -> describe the
    LITERAL DISTANCE; do not normalize to "a few inches".
  - Detached body parts -> describe the DETACHMENT; do not reattach in
    prose because reattachment is the normal-anatomy default.
  - Multiplied features (three eyes, two mouths) -> describe the
    LITERAL COUNT; do not collapse to one because one is the
    normal-anatomy default.

The rule: DESCRIBE LITERAL SHAPES AND CONNECTIONS YOU SEE IN THE PIXELS.
Do NOT map impossible VFX back to "normal" anatomical descriptors just
because normal makes more logical sense. If there is a hole in the
flesh, say "a hole in the flesh." If skin forms a closed loop, say "a
closed loop of skin." Banana 2 + Veo can RENDER impossible shapes but
only if the prompt names them literally.

v718c COROLLARY (v719c — bidirectional VFX recognition, MANDATORY):

The literal-pixel rule above (v718c) cures hallucinations in BOTH
directions, not just one. The original v718c failure was: VLM
normalizes IMPOSSIBLE VFX back to NORMAL anatomy (closed flesh-loop
described as "U-shape sagging"). The MIRROR failure observed
afterward: VLM hallucinates IMPOSSIBLE VFX where source has SOLID,
UNBROKEN anatomy (Banana 2 reads "deep U-shape" vocabulary in the
prompt and renders a literal U-shape HOLE / LOOP that doesn't exist
in the source).

Bidirectional discipline:

  If source has IMPOSSIBLE VFX (closed loops, holes, detached parts,
  multiplied features) -> describe LITERALLY (closed loop with hole;
  3-inch detachment gap; three eyes). DO NOT normalize to closest
  anatomical concept.

  If source has SOLID, UNBROKEN anatomy (continuous sagging flesh,
  draped skin sheet, hanging curtain of tissue) -> describe AS SOLID
  AND UNBROKEN explicitly. DO NOT use vocabulary that implies
  topology (U-shape, V-shape, loop, hole, opening, ring, gap) when
  the source is solid.

Vocabulary that implies topology / negative space (avoid when source
is solid):
  "U-shape", "V-shape", "loop", "ring", "hole", "opening", "gap",
  "split", "fork", "Y-shape", "C-shape", "doughnut shape"

Vocabulary that names solid volume (use when source is solid):
  "continuous sheet of draped flesh", "dense unbroken curtain of
  loose skin", "solid flap hanging straight down", "thick mass of
  pendulous flesh", "uninterrupted drape of skin", "single
  continuous fold"

Rule: if the source shows the flesh as ONE continuous solid mass
with no holes / no negative space / no loops, name it as such
explicitly. Adding "U-shape" to a solid mass creates a hole in
Banana 2's render that wasn't in the source.

v712 LATERAL-VECTOR REQUIREMENT (v720b, MANDATORY):

For ANY extended limb, you MUST declare its LATERAL VECTOR relative
to the viewer (not just that it is "extended"). The VLM's default
"extended arm" interpretation gets rendered by Banana 2 as either
forward-toward-camera (most common) or crossing-the-chest (second
most common) — neither matching the source if the source has the
arm extended TO THE SIDE.

Required: every extended limb gets a directional clause:

  "extended straight outward to the viewer-left"
  "extended straight outward to the viewer-right"
  "extended straight forward toward the camera"
  "extended straight upward overhead"
  "extended straight downward toward the floor"
  "extended at a 45-degree angle upward to the viewer-right"

Banned (loses lateral vector):

  "extended arm" (no direction)
  "outstretched arm" (no direction)
  "arm reaching out" (no direction)
  "arm in the foreground" (vector ambiguous)

When two characters stand side-by-side and the patient's arm extends
LATERALLY (to the side, not toward the camera), the arm and torso
share the SAME midground depth plane — there is NO Z-axis layering
to describe (v713f does NOT apply in this case). Use X-axis
relational grammar instead.

Apply v718a -> v718b -> v718c (with v719c bidirectional corollary)
-> v720b lateral-vector check, in order, BEFORE writing
static_composition. The four steps are pre-grammar perceptual
checks; they constrain what the v712 / v713 / v715 / v716 / v717
grammar rules describe.

SOURCE WARDROBE OBSERVATION (v722 handoff rule, MANDATORY):

Stage 4d is an observation handoff, not a build prompt. Observe every source
person's stable identity markers and wardrobe once in top-level
observed_people. Per-shot composition then uses the same neutral label plus
pose, expression, eye direction, mouth state, and interaction. Do not repeat
wardrobe inside each shot.

Record what is literally visible, including attire that a later compliant
build may need to swap. Do not invent an Ingredients table: Stage 4d has no
such field. Generation-side persona and wardrobe rules apply only after this
source observation is saved.

COMPOSITION GRAMMAR (v712, decode-side):

Describe composition through SUBJECT-TO-SUBJECT geometry, not through frame
quadrants. The VLM cannot reliably grid-anchor a source frame, so coordinate
grammar produces precise-but-wrong descriptions. Relational grammar anchors
to subjects (which the VLM CAN identify) and encodes geometry through verb +
preposition chains.

Allowed prepositions for spatial geometry:
  above / below / behind / in front of / over the shoulder of / beside /
  between / under / next to / from above / from below.

Allowed verbs encoding pose + action:
  pointing / leaning / standing / sitting / holding / lifting / reaching /
  gesturing / smiling / wincing / closing eyes / looking forward / looking
  down / looking at / facing the camera / turning toward.

Shot size: encode through DETAIL-DENSITY anchor, not jargon. Name the
micro-features that are visible-and-sharp at the actual framing:
- "forehead wrinkles clearly visible, dark eye circles clearly visible" → close-macro
- "full lab coat visible, stethoscope visible, ID badge visible" → medium-wide
- "full body visible from head to feet" → wide
Banana 2 infers framing from the named visible-and-sharp detail.

Crop: signal cropped-out content through OMISSION, not negation. Do NOT write
"NO floor visible" / "NO feet visible" / "NO background props" — the negation
tokens occasionally invoke rendering of the negated item. Just don't mention
the cropped content. What's unnamed at tight framing = not rendered.

Subject orientation: explicit per subject ("faces the camera / looks forward /
looks down / turns toward him / closes her eyes").

Background: single blur statement at the end ("background: slightly out-of-focus
clinic interior" / "background: blurred kitchen counter and pendant lights").

BANNED on decode side (these tokens push the VLM into wrong-values coordinate
grammar). Viewer-left/right remains allowed only for the literal lateral limb
vector required above; never use it as a frame-grid subject slot:
  upper-third line / lower-third line / left half /
  right half / chest-up two-shot / cropped at mid-chest / NO floor visible /
  NO feet visible / NO background visible / heads near the upper-third /
  rule of thirds.

Worked example (Dr. Kim Image 1 frame — extreme face-macro, doctor face partial
top-right, patient face dominating lower-left + center):

  WRONG (coordinate, pre-v712):
    "Tight chest-up two-shot. Patient on viewer-left filling left half. Main
    character stands close beside her on viewer-right. Heads near upper-third
    line. Cropped at mid-chest, NO floor visible, NO feet visible."

  RIGHT (relational, v712):
    "The main character with tan-framed glasses and dark hair leans forward
    over the right shoulder of a white woman in her 60s with a short blonde
    bob. He points a purple-gloved index finger DOWN at her forehead from
    above, the fingertip near her right temple. She faces the camera and
    looks forward, deep horizontal forehead wrinkles and dark circles under
    her eyes clearly visible. His face is close to her head, faces nearly
    touching. The camera focuses sharply on both their expressions.
    Background: slightly out-of-focus clinic interior."

Five geometric constraints encoded in the RIGHT version:
  1. Doctor above woman (via "over the right shoulder of" + "from above")
  2. Woman lower-frame (via "leans forward over")
  3. Hand crossing down (via "points DOWN at her forehead from above")
  4. Hand near her right temple (via "fingertip near her right temple")
  5. Both faces visible + sharp (via "camera focuses sharply on both")

Shot size: encoded by detail-listing (forehead wrinkles + dark eye circles
visible) = close-macro inferred.

Crop: encoded by omission — no clothing below chest, no feet, no floor
mentioned at all.

Anchor props in field `anchor_props_with_positions`: position relative to
SUBJECTS, not frame quadrants. "Purple-gloved finger near her right temple"
NOT "purple-gloved hand mid-right of frame". "Saffron bottle in his left
hand, held up to camera" NOT "saffron bottle viewer-right at chest height".

Carve-out — when relational is genuinely ambiguous (two subjects at the same
vertical level with no clear above/below relationship, both at the same depth,
with no third anchor to disambiguate), lateral relational prepositions allowed
(beside / next to / between / on either side of). Coordinate fallback only as
LAST resort, never on the primary subject geometry sentence.
"""


def build_user_prompt(shots: list, transcript_summary: str, motion: object,
                      include_schema: bool = True, extra_context: str = "",
                      profile: str = "ugc-reel") -> str:
    # <context>/<task>/<final_instruction> structure per the official Gemini 3
    # prompt-design template (ai.google.dev/gemini-api/docs/prompting-strategies,
    # read 2026-08-11). Data first, then the task, then the closing reminder.
    profile_spec = _reading_profile(profile)
    schema_block = (
        f"Stage 4d JSON schema:\n{json.dumps(schema_for_profile(profile), indent=2)}\n\n"
        if include_schema else ""
    )
    return (
        "<context>\n"
        f"{profile_spec['context']}{extra_context}"
        f"Shots:\n{json.dumps(shots, indent=2)}\n\n"
        f"Whisper transcript (authoritative for dialogue):\n{transcript_summary}\n\n"
        f"Motion classifications (cross-check; visual evidence wins):\n{json.dumps(motion, indent=2)}\n\n"
        f"{schema_block}"
        "</context>\n\n"
        "<task>\n"
        "Produce one JSON OBJECT matching the Stage 4d schema. "
        "Cover every shot in order. Use the EXACT shot start/end timestamps "
        "provided. Pay extra attention to action arcs in shots whose dialogue "
        "contains a verb-of-state-change (pour, squeeze, add, stir, mix, melt).\n"
        f"{profile_spec['task']}"
        "</task>\n\n"
        "<final_instruction>Run the forensic-perception protocol on each shot "
        "BEFORE writing its composition fields. Output: ONLY the JSON object. "
        "No prose preamble, no code fences.</final_instruction>"
    )


class Stage4dValidationError(ValueError):
    """Raised when Stage 4d output cannot safely enter the decode handoff."""


def _nonempty(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def validate_stage4d_output(data: object, expected_shots: list) -> dict:
    """Validate the handoff contract without requiring a third-party package."""
    errors: list[str] = []
    if not isinstance(data, dict):
        raise Stage4dValidationError("top level must be an object, not an array")
    if "<REQUIRED" in json.dumps(data, ensure_ascii=False) or "<COPY FROM" in json.dumps(data, ensure_ascii=False):
        errors.append("template placeholders remain; complete the human walk before handoff")
    if data.get("schema_version") != STAGE4D_SCHEMA_VERSION:
        errors.append(f"schema_version must be {STAGE4D_SCHEMA_VERSION!r}")
    if not isinstance(data.get("observed_people"), list):
        errors.append("observed_people must be an array")
    shots = data.get("shots")
    if not isinstance(shots, list):
        errors.append("shots must be an array")
        shots = []
    if len(shots) != len(expected_shots):
        errors.append(f"shots count {len(shots)} does not match expected {len(expected_shots)}")

    required_top = [
        "summary", "forensic_perception", "static_composition", "action_arc",
        "audio", "motion_cross_check", "veo_reproduction_hints", "human_walk_corrections",
    ]
    for pos, expected in enumerate(expected_shots, start=1):
        if pos > len(shots) or not isinstance(shots[pos - 1], dict):
            errors.append(f"shot {pos}: missing object")
            continue
        shot = shots[pos - 1]
        source_index = expected.get("shot", expected.get("shot_index", pos))
        if shot.get("shot_index") != source_index:
            errors.append(f"shot {pos}: shot_index must be {source_index}")
        for key in ("start", "end"):
            try:
                if abs(float(shot.get(key)) - float(expected[key])) > 0.02:
                    errors.append(f"shot {pos}: {key} must match source {expected[key]}")
            except (TypeError, ValueError, KeyError):
                errors.append(f"shot {pos}: {key} must be a source timestamp")
        for key in required_top:
            if key not in shot:
                errors.append(f"shot {pos}: missing {key}")

        forensic = shot.get("forensic_perception")
        if not isinstance(forensic, dict):
            errors.append(f"shot {pos}: forensic_perception must be an object")
        else:
            for key in ("kinematic_traces", "z_depth_layers", "literal_vfx_observations", "intrinsic_state_isolation"):
                if key not in forensic:
                    errors.append(f"shot {pos}: forensic_perception missing {key}")
            isolation = forensic.get("intrinsic_state_isolation", {})
            if isolation.get("primary_change_axis") not in PRIMARY_CHANGE_AXES:
                errors.append(f"shot {pos}: invalid forensic primary_change_axis")

        arc = shot.get("action_arc")
        if not isinstance(arc, dict):
            errors.append(f"shot {pos}: action_arc must be an object")
        else:
            if not isinstance(arc.get("kinematics"), dict):
                errors.append(f"shot {pos}: action_arc.kinematics must be an object")
            morphology = arc.get("morphology")
            if not isinstance(morphology, dict):
                errors.append(f"shot {pos}: action_arc.morphology must be an object")
            else:
                axis = morphology.get("primary_change_axis")
                magnitude = morphology.get("magnitude")
                if axis not in PRIMARY_CHANGE_AXES:
                    errors.append(f"shot {pos}: invalid morphology primary_change_axis")
                if magnitude not in MORPHOLOGY_MAGNITUDES:
                    errors.append(f"shot {pos}: invalid morphology magnitude")
                if axis != "NONE" and morphology.get("intrinsic_state_start") == morphology.get("intrinsic_state_end"):
                    errors.append(f"shot {pos}: changed morphology needs distinct start/end states")
        for key in ("summary", "human_walk_corrections"):
            if key in shot and not _nonempty(shot[key]):
                errors.append(f"shot {pos}: {key} must be a non-empty observation (use 'none' when clear)")

    if errors:
        raise Stage4dValidationError("Stage 4d validation failed:\n- " + "\n- ".join(errors))
    return data


def parse_and_validate_stage4d(raw_output: str, expected_shots: list) -> dict:
    try:
        data = json.loads(raw_output)
    except json.JSONDecodeError as exc:
        raise Stage4dValidationError(f"provider returned invalid JSON: {exc}") from exc
    return validate_stage4d_output(data, expected_shots)


# ──────────────────────────────────────────────────────────────────────
# Pipeline I/O
# ──────────────────────────────────────────────────────────────────────

def _shots_from_clips_tsv(tsv_path: Path) -> list:
    """decode-reel's hardcut_frames.py writes clips.tsv (clip / start_sec /
    start_frame / end_sec / dur_s / image / same_as). Convert to the shots
    list this module expects, so the skill's output plugs in directly."""
    import csv
    shots = []
    with tsv_path.open(newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            shots.append({
                "shot": int(row["clip"]),
                "start": float(row["start_sec"]),
                "end": float(row["end_sec"]),
                "image": row.get("image"),
                "same_as": row.get("same_as") or None,
            })
    return shots


def _farneback_motion(video_path: Path, shots: list) -> list:
    """v585-style per-shot motion classification, computed on the fly when no
    motion.json exists (the decode-reel skill has no separate Stage 4c step).
    Downscaled Farneback mean-flow, ~4 frame pairs per shot."""
    import cv2  # PySceneDetect dependency, present wherever hardcut ran
    import numpy as np
    cap = cv2.VideoCapture(str(video_path))

    def grab(t):
        cap.set(cv2.CAP_PROP_POS_MSEC, t * 1000)
        ok, fr = cap.read()
        if not ok:
            return None
        return cv2.cvtColor(cv2.resize(fr, (270, 480)), cv2.COLOR_BGR2GRAY)

    out = []
    for s in shots:
        start, end = float(s["start"]), float(s["end"])
        dur = max(end - start, 0.05)
        mags = []
        for i in range(4):
            t0 = start + dur * i / 4
            a, b = grab(t0), grab(min(t0 + 0.12, end - 0.01))
            if a is None or b is None:
                continue
            flow = cv2.calcOpticalFlowFarneback(a, b, None, 0.5, 3, 15, 3, 5, 1.2, 0)
            mags.append(float(np.mean(np.linalg.norm(flow, axis=2))))
        mean_mag = float(np.mean(mags)) if mags else 0.0
        cls = "high" if mean_mag > 0.7 else ("medium" if mean_mag > 0.25 else "low")
        out.append({"shot": s["shot"], "start": round(start, 3), "end": round(end, 3),
                    "motion": cls, "mean_flow_mag": round(mean_mag, 4)})
    cap.release()
    return out


def load_pipeline_inputs(video_path: Path, shots_path: Path | None,
                         transcript_path: Path | None, motion_path: Path | None):
    workdir = video_path.parent
    if shots_path is None:
        shots_path = workdir / "shots.json"
        if not shots_path.exists():
            # decode-reel skill layout: hardcut*/clips.tsv next to the mp4
            for cand in sorted(workdir.glob("hardcut*/clips.tsv"), reverse=True):
                shots_path = cand
                break
    if transcript_path is None:
        transcript_path = workdir / "transcript.json"
        if not transcript_path.exists():
            # openai-whisper CLI writes <stem>.json next to the mp4
            cand = workdir / f"{video_path.stem}.json"
            if cand.exists():
                transcript_path = cand
    if motion_path is None:
        motion_path = workdir / "motion.json"

    if not shots_path.exists():
        raise FileNotFoundError(
            f"shots not found at {shots_path}. Run v579 Stage 3 or the decode-reel "
            f"hardcut step (clips.tsv is accepted directly) first.")
    if not transcript_path.exists():
        raise FileNotFoundError(
            f"transcript not found at {transcript_path}. Run v579 Stage 2 or "
            f"`python -m whisper <mp4> --output_format json` first.")

    if shots_path.suffix.lower() == ".tsv":
        shots = _shots_from_clips_tsv(shots_path)
    else:
        shots = json.loads(shots_path.read_text(encoding="utf-8-sig"))
        for s in shots:  # tolerate shot_index from older converters
            if "shot" not in s and "shot_index" in s:
                s["shot"] = s["shot_index"]

    transcript = json.loads(transcript_path.read_text(encoding="utf-8-sig"))

    if motion_path.exists():
        motion = json.loads(motion_path.read_text(encoding="utf-8-sig"))
    else:
        print(f"[v589] motion.json missing — computing Farneback per-shot motion inline")
        motion = _farneback_motion(video_path, shots)
        motion_path.write_text(json.dumps(motion, indent=1), encoding="utf-8")
        print(f"[v589] wrote {motion_path} ({len(motion)} shots)")

    transcript_summary = "\n".join(
        f"  [{seg['start']:6.2f}-{seg['end']:6.2f}] {seg['text']}"
        for seg in transcript["segments"]
    )
    return shots, transcript, motion, transcript_summary


def list_dense_frames(video_path: Path) -> list[Path]:
    frames_dir = video_path.parent / "frames"
    if not frames_dir.exists():
        return []
    return sorted(frames_dir.glob("*.png"))


# ──────────────────────────────────────────────────────────────────────
# Provider 1: LM Studio (local, free, OpenAI-compatible)
# ──────────────────────────────────────────────────────────────────────

def lmstudio_available(base_url: str = "http://localhost:1234") -> tuple[bool, str | None]:
    try:
        import urllib.request, urllib.error
        with urllib.request.urlopen(f"{base_url}/v1/models", timeout=2) as r:
            data = json.loads(r.read())
            models = [m["id"] for m in data.get("data", [])]
            if not models:
                return False, None
            # Prefer vision-capable model name patterns
            for m in models:
                if any(k in m.lower() for k in ["vl", "vision", "gemma-4", "gemma-3", "llava", "qwen2.5-vl"]):
                    return True, m
            return True, models[0]  # fallback to first available
    except Exception:
        return False, None


def call_lmstudio(video_path: Path, frames: list[Path], shots: list, transcript_summary: str,
                  motion: object,
                  base_url: str = "http://localhost:1234", model: str | None = None) -> str:
    import base64, urllib.request, urllib.error

    available, default_model = lmstudio_available(base_url)
    if not available:
        raise RuntimeError(f"LM Studio not reachable at {base_url}/v1/models — open the app and enable the local server")
    model = model or default_model
    print(f"[v589] LM Studio: model={model}, sending {len(frames)} dense frames + transcript")

    content = [
        {"type": "text", "text": SYSTEM_INSTRUCTION},
        {"type": "text", "text": build_user_prompt(shots, transcript_summary, motion, include_schema=True)},
    ]
    for f in frames:
        with open(f, "rb") as fh:
            b64 = base64.b64encode(fh.read()).decode()
        content.append({"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64}"}})
        content.append({"type": "text", "text": f"^^^ frame: {f.name}"})

    req = urllib.request.Request(
        f"{base_url}/v1/chat/completions",
        method="POST",
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "model": model,
            "messages": [{"role": "user", "content": content}],
            "temperature": 0.0,
            "max_tokens": 8000,
        }).encode(),
    )
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=600) as r:
        resp = json.loads(r.read())
    print(f"[v589] LM Studio responded in {time.time() - t0:.1f}s")
    return resp["choices"][0]["message"]["content"]


# ──────────────────────────────────────────────────────────────────────
# Provider 2: Gemini API (paid, native MP4)
# ──────────────────────────────────────────────────────────────────────

def gemini_api_key() -> str | None:
    """Process env first; on Windows fall back to the USER environment
    (HKCU\\Environment) — shells opened before the key was set, and Git Bash
    sessions generally, do not inherit per-user variables."""
    key = os.environ.get("GEMINI_API_KEY")
    if key:
        return key
    if sys.platform == "win32":
        try:
            import winreg
            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment") as h:
                key, _ = winreg.QueryValueEx(h, "GEMINI_API_KEY")
                return key or None
        except OSError:
            return None
    return None


def gemini_available() -> bool:
    return bool(gemini_api_key())


# Standard-tier prices per 1M tokens in USD, verified against
# https://ai.google.dev/gemini-api/docs/pricing on 2026-08-11.
# "in" covers text/image/video input; audio input is priced separately.
# Output prices include thinking tokens. Unknown models still get their
# token counts logged — only the USD column stays empty.
GEMINI_PRICING = {
    "gemini-2.5-flash":       {"in": 0.30, "in_audio": 1.00, "out": 2.50, "in_cached": 0.03},
    "gemini-2.5-flash-lite":  {"in": 0.10, "in_audio": 0.30, "out": 0.40, "in_cached": 0.01},
    "gemini-2.5-pro":         {"in": 1.25, "in_audio": 1.25, "out": 10.00, "in_cached": 0.125,
                               "in_big": 2.50, "out_big": 15.00, "tier_threshold": 200_000},
    "gemini-3-flash-preview": {"in": 0.50, "in_audio": 1.00, "out": 3.00, "in_cached": 0.05},
    "gemini-3.5-flash":       {"in": 1.50, "in_audio": 1.50, "out": 9.00, "in_cached": 0.15},
    "gemini-3.5-flash-lite":  {"in": 0.30, "in_audio": 0.30, "out": 2.50, "in_cached": 0.03},
    "gemini-3.6-flash":       {"in": 1.50, "in_audio": 1.50, "out": 7.50, "in_cached": 0.15},
    "gemini-3.1-pro-preview": {"in": 2.00, "in_audio": 2.00, "out": 12.00, "in_cached": 0.20,
                               "in_big": 4.00, "out_big": 18.00, "tier_threshold": 200_000},
}

# 2026-08-11: gemini-2.5-flash rejects NEW API keys with 404 "no longer
# available to new users" — default moved to the newest flash generation.
GEMINI_DEFAULT_MODEL = "gemini-3.6-flash"

# Inline-data requests cap at 20MB total; leave headroom for the prompt.
INLINE_VIDEO_MAX_BYTES = 19 * 1024 * 1024


def _gemini_price_for(model: str) -> dict | None:
    if model in GEMINI_PRICING:
        return GEMINI_PRICING[model]
    for k in sorted(GEMINI_PRICING, key=len, reverse=True):
        if model.startswith(k):
            return GEMINI_PRICING[k]
    return None


def compute_gemini_cost(model: str, prompt_tokens: int, audio_tokens: int,
                        output_tokens: int, cached_tokens: int = 0) -> float | None:
    """Standard-tier USD estimate. Output includes candidates + thoughts.

    cached_tokens = implicit-cache hits (enabled by default on 2.5+ models),
    billed at the context-caching rate instead of the full input rate.
    """
    p = _gemini_price_for(model)
    if p is None:
        return None
    big = p.get("tier_threshold") and prompt_tokens > p["tier_threshold"]
    in_rate = p["in_big"] if big and "in_big" in p else p["in"]
    out_rate = p["out_big"] if big and "out_big" in p else p["out"]
    cached = min(max(cached_tokens, 0), prompt_tokens)
    non_audio_in = max(prompt_tokens - audio_tokens - cached, 0)
    return (non_audio_in * in_rate
            + cached * p.get("in_cached", in_rate * 0.1)
            + audio_tokens * p.get("in_audio", in_rate)
            + output_tokens * out_rate) / 1e6


def gemini_ledger_path() -> Path:
    env = os.environ.get("GEMINI_COST_LEDGER")
    if env:
        return Path(env)
    return Path(__file__).resolve().parent.parent / "output" / "gemini_costs.jsonl"


def log_gemini_usage(video_path: Path, model: str, resp, elapsed_s: float,
                     request_config: dict | None = None,
                     schema_status: str = "not_checked") -> dict:
    """Log token usage plus an explicitly labelled standard-tier estimate."""
    from datetime import datetime, timezone

    u = getattr(resp, "usage_metadata", None)
    modality: dict[str, int] = {}
    for d in (getattr(u, "prompt_tokens_details", None) or []):
        name = getattr(getattr(d, "modality", None), "name", "unknown") or "unknown"
        modality[name.lower()] = modality.get(name.lower(), 0) + (getattr(d, "token_count", 0) or 0)
    prompt_tokens = (getattr(u, "prompt_token_count", 0) or 0)
    thoughts_tokens = (getattr(u, "thoughts_token_count", 0) or 0)
    candidate_tokens = (getattr(u, "candidates_token_count", 0) or 0)
    cached_tokens = (getattr(u, "cached_content_token_count", 0) or 0)
    output_tokens = thoughts_tokens + candidate_tokens
    audio_tokens = modality.get("audio", 0)

    cost = compute_gemini_cost(model, prompt_tokens, audio_tokens, output_tokens, cached_tokens)
    record = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "video": str(video_path),
        "model": model,
        "prompt_tokens": prompt_tokens,
        "prompt_by_modality": modality,
        "cached_tokens": cached_tokens,
        "thoughts_tokens": thoughts_tokens,
        "candidate_tokens": candidate_tokens,
        "total_tokens": (getattr(u, "total_token_count", 0) or 0),
        "estimated_standard_cost_usd": round(cost, 6) if cost is not None else None,
        "pricing_basis": "Gemini Developer API standard paid tier; excludes free, batch, flex, and negotiated pricing",
        "request_config": request_config or {},
        "schema_status": schema_status,
        "elapsed_s": round(elapsed_s, 1),
    }
    ledger = gemini_ledger_path()
    ledger.parent.mkdir(parents=True, exist_ok=True)
    with open(ledger, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(record) + "\n")

    cost_str = f"${cost:.4f}" if cost is not None else f"UNKNOWN PRICE for {model} (tokens logged)"
    cached_str = f", cached={cached_tokens}" if cached_tokens else ""
    print(f"[v589] Gemini standard-tier estimate: {cost_str}  "
          f"(in={prompt_tokens} [{', '.join(f'{k}={v}' for k, v in modality.items()) or 'no modality detail'}]{cached_str}, "
          f"think={thoughts_tokens}, out={candidate_tokens})  ledger={ledger}")
    return record


def print_gemini_costs():
    ledger = gemini_ledger_path()
    if not ledger.exists():
        print(f"no ledger at {ledger}; no Gemini decodes logged yet")
        return
    rows = []
    bad_rows = 0
    for line in ledger.read_text(encoding="utf-8-sig").splitlines():
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            bad_rows += 1
    if not rows:
        print(f"ledger at {ledger} is empty")
        return
    print(f"Gemini standard-tier cost estimates: {len(rows)} calls ({ledger})\n")
    for r in rows:
        estimate = r.get("estimated_standard_cost_usd", r.get("cost_usd"))
        cost = f"${estimate:.4f}" if estimate is not None else "?"
        print(f"  {r['ts']}  {cost:>9}  {r['model']:<24} "
              f"in={r['prompt_tokens']:>7} think={r.get('thoughts_tokens', 0):>6} "
              f"out={r.get('candidate_tokens', 0):>6}  {Path(r['video']).name}")
    print()
    by_model: dict[str, list] = {}
    for r in rows:
        by_model.setdefault(r["model"], []).append(r)
    unknown = 0
    total = 0.0
    for model, rs in sorted(by_model.items()):
        estimates = [r.get("estimated_standard_cost_usd", r.get("cost_usd")) for r in rs]
        known = [cost for cost in estimates if cost is not None]
        unknown += len(rs) - len(known)
        subtotal = sum(known)
        total += subtotal
        print(f"  {model:<24} {len(rs):>3} calls  ${subtotal:.4f}")
    print(f"  {'TOTAL':<24} {len(rows):>3} calls  ${total:.4f}"
          + (f"  (+{unknown} calls with unknown pricing)" if unknown else ""))
    print("  Basis: standard paid tier estimate; actual billing can differ by traffic tier.")
    if bad_rows:
        print(f"  WARN: skipped {bad_rows} malformed ledger row(s).")


def _mmss(seconds: float) -> str:
    s = int(seconds)
    return f"{s // 60:02d}:{s % 60:02d}"


def _delete_gemini_upload(client, uploaded) -> None:
    try:
        client.files.delete(name=uploaded.name)
        print(f"[v589] Gemini: deleted uploaded file {uploaded.name}")
    except Exception as exc:
        print(f"[v589] WARN: could not delete uploaded file {uploaded.name} ({exc})")


def call_gemini(video_path: Path, shots: list, transcript_summary: str, motion: object,
                model: str | None = None, thinking: str | None = "high",
                media_resolution: str | None = None, fps: float | None = None,
                upload_timeout_s: float = 300) -> str:
    from google import genai
    from google.genai import types

    model = model or GEMINI_DEFAULT_MODEL
    api_key = gemini_api_key()
    client = genai.Client(api_key=api_key)

    size_bytes = video_path.stat().st_size
    uploaded = None  # set only when the File API path is used

    def _cleanup():
        if uploaded is not None:
            _delete_gemini_upload(client, uploaded)

    if fps is not None and not 0 < fps <= 24:
        raise ValueError("--fps must be greater than 0 and no more than 24")

    # File passing (per ai.google.dev/gemini-api/docs/file-input-methods, read
    # 2026-08-11): inline data is the recommended path for small one-off files —
    # it skips the File API upload+processing round trip (measured 7-26s per
    # decode). File API for files too big to inline (20MB request cap, minus
    # prompt headroom) — AND as the automatic fallback when the inline
    # generate_content POST dies at the transport layer: some VPN/middlebox
    # setups kill the multi-MB inline request body while the resumable
    # /upload/ endpoint passes clean (observed 2026-08-12, Surfshark).
    def _upload_video_part():
        nonlocal uploaded
        print(f"[v589] Gemini: uploading {video_path.name} ({size_bytes / 1e6:.1f} MB via File API)")
        t0 = time.time()
        uploaded = client.files.upload(file=str(video_path))
        deadline = time.monotonic() + upload_timeout_s
        while uploaded.state.name == "PROCESSING":
            if time.monotonic() >= deadline:
                try:
                    client.files.delete(name=uploaded.name)
                finally:
                    raise TimeoutError(f"Gemini upload processing exceeded {upload_timeout_s:.0f}s")
            time.sleep(2)
            uploaded = client.files.get(name=uploaded.name)
        if uploaded.state.name != "ACTIVE":
            state = uploaded.state.name
            _cleanup()
            raise RuntimeError(f"upload failed: {state}")
        print(f"[v589] Gemini: uploaded in {time.time() - t0:.1f}s, state ACTIVE")
        if fps is not None:
            try:
                part = types.Part(
                    file_data=types.FileData(file_uri=uploaded.uri, mime_type=uploaded.mime_type),
                    video_metadata=types.VideoMetadata(fps=fps),
                )
            except Exception:
                _cleanup()
                raise
            print(f"[v589] Gemini: sampling video at {fps} fps")
            return part
        return uploaded

    if size_bytes <= INLINE_VIDEO_MAX_BYTES:
        part_kwargs: dict = {"inline_data": types.Blob(data=video_path.read_bytes(),
                                                       mime_type="video/mp4")}
        if fps is not None:
            part_kwargs["video_metadata"] = types.VideoMetadata(fps=fps)
            print(f"[v589] Gemini: sampling video at {fps} fps")
        video_part = types.Part(**part_kwargs)
        print(f"[v589] Gemini: sending {video_path.name} INLINE ({size_bytes / 1e6:.1f} MB — no upload round trip)")
    else:
        video_part = _upload_video_part()

    # Gemini's video tokens carry per-second timestamps; the docs' native
    # reference format is MM:SS. Give the model an explicit shot -> MM:SS map
    # so shot boundaries land on its internal timeline.
    timestamp_map = "\n".join(
        f"  shot {s['shot']}: {_mmss(s['start'])}-{_mmss(s['end'])} "
        f"({s['start']:.2f}s-{s['end']:.2f}s)"
        for s in shots
    )
    user_prompt = build_user_prompt(
        shots, transcript_summary, motion, include_schema=False,
        extra_context=f"Video timeline map (MM:SS matches the video's timestamps):\n{timestamp_map}\n\n",
    )

    config_kwargs: dict = dict(
        system_instruction=SYSTEM_INSTRUCTION,
        response_mime_type="application/json",
        response_json_schema=STAGE4D_JSON_SCHEMA,
    )
    if media_resolution == "low":
        try:
            config_kwargs["media_resolution"] = types.MediaResolution.MEDIA_RESOLUTION_LOW
            print("[v589] Gemini: media_resolution=LOW (cheap mode, less fine detail)")
        except AttributeError as exc:
            _cleanup()
            raise RuntimeError("installed google-genai is too old; install code/requirements.txt") from exc
    if thinking and thinking != "default":
        try:
            config_kwargs["thinking_config"] = types.ThinkingConfig(thinking_level=thinking)
            print(f"[v589] Gemini: thinking_level={thinking}")
        except Exception as exc:
            _cleanup()
            raise RuntimeError("installed google-genai is too old for Gemini thinking levels; install code/requirements.txt") from exc

    def _generate_streamed(contents):
        # STREAMING is load-bearing, not an optimization: a non-streaming
        # generate_content holds the connection silent until the FULL response
        # is ready, and this network (Surfshark VPN, observed 2026-08-12) kills
        # any connection with no response bytes after exactly ~60s. A long
        # structured decode (66 shots) always exceeds that; streaming starts
        # emitting bytes early and survives (measured: non-stream FAIL at
        # 60.4s, stream OK at 89.5s on the same task).
        chunks = client.models.generate_content_stream(
            model=model,
            contents=contents,
            config=types.GenerateContentConfig(**config_kwargs),
        )
        text_parts = []
        usage = None
        for chunk in chunks:
            if chunk.text:
                text_parts.append(chunk.text)
            if getattr(chunk, "usage_metadata", None) is not None:
                usage = chunk.usage_metadata
        from types import SimpleNamespace
        return SimpleNamespace(text="".join(text_parts), usage_metadata=usage)

    print(f"[v589] Gemini: calling {model} (streamed)")
    t1 = time.time()
    try:
        resp = _generate_streamed([video_part, user_prompt])  # video FIRST (doc-recommended order)
    except Exception as first_exc:
        transport_error = type(first_exc).__name__ in (
            "RemoteProtocolError", "ConnectError", "ReadError", "WriteError",
            "ReadTimeout", "WriteTimeout", "ConnectTimeout",
        )
        if uploaded is None and transport_error:
            # Inline POST killed at the transport layer — retry once through
            # the File API path, whose endpoint survives the same networks.
            print(f"[v589] Gemini: inline request died ({type(first_exc).__name__}) — "
                  f"retrying via File API upload")
            try:
                video_part = _upload_video_part()
                resp = _generate_streamed([video_part, user_prompt])
            except Exception:
                _cleanup()
                raise
        else:
            _cleanup()
            raise
    elapsed = time.time() - t1
    print(f"[v589] Gemini: generated in {elapsed:.1f}s")
    schema_status = "pass"
    try:
        parse_and_validate_stage4d(resp.text, shots)
    except Stage4dValidationError:
        schema_status = "fail"
        raise
    finally:
        try:
            log_gemini_usage(
                video_path, model, resp, elapsed,
                request_config={
                    "thinking": thinking, "fps": fps,
                    "media_resolution": media_resolution or "default",
                    "schema_version": STAGE4D_SCHEMA_VERSION,
                },
                schema_status=schema_status,
            )
        except Exception as exc:
            print(f"[v589] WARN: cost logging failed ({exc}); decode output unaffected")
        _cleanup()
    return resp.text


# ──────────────────────────────────────────────────────────────────────
# Provider 3: Human-walk template (always available)
# ──────────────────────────────────────────────────────────────────────

def write_human_walk_template(shots: list, transcript: dict, frames_dir: Path | None) -> str:
    """Emit a stage4d_vlm.json TEMPLATE — same schema, empty fields per shot.

    The human-walking decoder (Claude in the chat) fills in the fields by
    walking the v588 dense-frame inspection plus the whisper transcript. The
    v589 STRUCTURAL rule still holds — the schema is produced, just by a
    human walker instead of an API.
    """
    out = {"schema_version": STAGE4D_SCHEMA_VERSION, "observed_people": [], "shots": []}
    for s in shots:
        # Find dense frames overlapping this shot
        shot_frames = []
        if frames_dir and frames_dir.exists():
            shot_frames = sorted(frames_dir.glob(f"shot{s['shot']:02d}_*.png"))

        # Find dialogue overlapping this shot
        dialogue = [
            seg for seg in transcript["segments"]
            if seg["end"] > s["start"] and seg["start"] < s["end"]
        ]

        dialogue_text = " ".join(seg.get("text", "").strip() for seg in dialogue).strip() or "none"
        out["shots"].append({
            "shot_index": s["shot"],
            "start": s["start"],
            "end": s["end"],
            "summary": "<REQUIRED: source-derived job plus visible action>",
            "forensic_perception": {
                "kinematic_traces": [],
                "z_depth_layers": {"foreground": "<REQUIRED>", "midground": "<REQUIRED>", "background": "<REQUIRED>"},
                "literal_vfx_observations": [],
                "intrinsic_state_isolation": {
                    "focus_object": "<REQUIRED>",
                    "axis_1_surface_texture": "<REQUIRED>",
                    "axis_2_structural_integrity": "<REQUIRED>",
                    "axis_3_volume_shape": "<REQUIRED>",
                    "axis_4_color_illumination": "<REQUIRED>",
                    "primary_change_axis": "NONE",
                    "intrinsic_state_start": "<REQUIRED>",
                    "intrinsic_state_end": "<REQUIRED>",
                },
            },
            "static_composition": {
                "subject": "<REQUIRED>", "framing": "<REQUIRED>",
                "anchor_props_with_positions": "<REQUIRED>", "lighting_and_palette": "<REQUIRED>",
            },
            "action_arc": {
                "has_state_evolution": False,
                "kinematics": {
                    "start_state": "<REQUIRED>", "mid_state": "<REQUIRED>", "end_state": "<REQUIRED>",
                    "movement_path": "<REQUIRED>", "camera_shift": "<REQUIRED>",
                },
                "morphology": {
                    "focus_object": "n/a", "intrinsic_state_start": "none",
                    "intrinsic_state_end": "none", "primary_change_axis": "NONE", "magnitude": "NONE",
                },
                "verbs_observed": [],
            },
            "audio": {"dialogue_verbatim": dialogue_text, "ambient": "<REQUIRED>", "music": "<REQUIRED>", "voice_register": "<REQUIRED>"},
            "motion_cross_check": {"input_classification": "<COPY FROM motion.json>", "vlm_observation": "<REQUIRED>", "agrees": False, "discrepancy": "<REQUIRED>"},
            "veo_reproduction_hints": {
                "use_blend_to_next_scene": False,
                "needs_platform_future_image_end": False,
                "transition_prompt": "<REQUIRED>",
            },
            "human_walk_corrections": "<REQUIRED: dense frames " + ", ".join(f.name for f in shot_frames) + ">",
        })
    return json.dumps(out, indent=2, ensure_ascii=False)


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="v589 Stage 4d - VLM-grounded video understanding")
    p.add_argument("video", type=Path, nargs="?", help="path to source MP4")
    p.add_argument("--shots", type=Path, default=None)
    p.add_argument("--transcript", type=Path, default=None)
    p.add_argument("--motion", type=Path, default=None)
    p.add_argument("--out", type=Path, default=None)
    p.add_argument("--provider", choices=["lmstudio", "gemini", "template", "auto"], default="auto",
                   help="force a provider; default 'auto' cascades lmstudio -> gemini -> template")
    p.add_argument("--model", default=None, help="override default model id for lmstudio/gemini")
    p.add_argument("--lmstudio-url", default="http://localhost:1234")
    p.add_argument("--thinking", choices=["minimal", "low", "medium", "high", "default"], default="high",
                   help="Gemini thinking level (default 'high'; forensic perception is a hard task; "
                        "'default' = let the model decide)")
    p.add_argument("--media-resolution", choices=["default", "low"], default="default",
                   help="Gemini video frame detail: default=258 tok/frame, low=66 tok/frame (cheaper, "
                        "less fine detail; NOT recommended for decode)")
    p.add_argument("--fps", type=float, default=None,
                   help="Gemini video sampling fps (default 1.0; raise to 2-5 for fast-motion videos)")
    p.add_argument("--upload-timeout", type=float, default=300,
                   help="seconds to wait for Gemini upload processing (default 300)")
    p.add_argument("--validate-stage4d", type=Path, default=None,
                   help="validate a saved Stage 4d JSON against --shots and exit")
    p.add_argument("--costs", action="store_true",
                   help="print the Gemini decode cost ledger and exit (no video needed)")
    args = p.parse_args()

    if args.costs:
        print_gemini_costs()
        return

    if args.validate_stage4d:
        shots_path = args.shots or args.validate_stage4d.parent / "shots.json"
        if not shots_path.exists():
            p.error(f"shots file not found: {shots_path}")
        expected = json.loads(shots_path.read_text(encoding="utf-8-sig"))
        candidate = json.loads(args.validate_stage4d.read_text(encoding="utf-8-sig"))
        try:
            validate_stage4d_output(candidate, expected)
        except Stage4dValidationError as exc:
            print(f"[v589] FAIL: {exc}", file=sys.stderr)
            sys.exit(2)
        print(f"[v589] PASS: {args.validate_stage4d} matches {STAGE4D_SCHEMA_VERSION} and {len(expected)} source shots")
        return

    if args.video is None:
        p.error("video is required (or pass --costs/--validate-stage4d)")

    if not args.video.exists():
        print(f"error: {args.video} not found", file=sys.stderr)
        sys.exit(1)

    try:
        shots, transcript, motion, transcript_summary = load_pipeline_inputs(
            args.video, args.shots, args.transcript, args.motion,
        )
    except (FileNotFoundError, KeyError, json.JSONDecodeError) as exc:
        p.error(str(exc))
    print(f"[v589] loaded {len(shots)} shots + {len(transcript['segments'])} dialogue segments + motion.json")

    out = args.out or args.video.parent / "stage4d_vlm.json"
    raw_output = None
    provider_used = None

    def try_lmstudio():
        nonlocal raw_output, provider_used
        ok, _ = lmstudio_available(args.lmstudio_url)
        if not ok:
            print(f"[v589] LM Studio not reachable at {args.lmstudio_url} — skipping")
            return False
        frames = list_dense_frames(args.video)
        if not frames:
            print(f"[v589] no dense frames found at {args.video.parent / 'frames'} — run v588 dense extraction first")
            return False
        raw_output = call_lmstudio(args.video, frames, shots, transcript_summary, motion, args.lmstudio_url, args.model)
        provider_used = "lmstudio"
        return True

    def try_gemini():
        nonlocal raw_output, provider_used
        if not gemini_available():
            print(f"[v589] GEMINI_API_KEY not set — skipping Gemini")
            return False
        raw_output = call_gemini(
            args.video, shots, transcript_summary, motion,
            model=args.model,
            thinking=args.thinking,
            media_resolution=(args.media_resolution if args.media_resolution != "default" else None),
            fps=args.fps,
            upload_timeout_s=args.upload_timeout,
        )
        provider_used = "gemini"
        return True

    def write_template():
        nonlocal raw_output, provider_used
        frames_dir = args.video.parent / "frames"
        raw_output = write_human_walk_template(shots, transcript, frames_dir)
        provider_used = "template"

    if args.provider == "lmstudio":
        if not try_lmstudio():
            sys.exit(2)
    elif args.provider == "gemini":
        if not try_gemini():
            sys.exit(2)
    elif args.provider == "template":
        write_template()
    else:  # auto
        automated = False
        try:
            automated = try_lmstudio()
        except Exception as exc:
            print(f"[v589] LM Studio failed ({exc}); trying next provider")
        if not automated:
            try:
                automated = try_gemini()
            except Exception as exc:
                print(f"[v589] Gemini failed ({exc}); writing review template")
        if not automated:
            print(f"[v589] no automated VLM provider available — writing human-walk template")
            print(f"       (recommended free path: install LM Studio + Gemma 4 E2B vision model;")
            print(f"        then re-run this script with the LM Studio app open)")
            write_template()

    if provider_used == "template":
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(raw_output + "\n", encoding="utf-8")
        print(f"[v589] wrote DRAFT template to {out}; fill placeholders, then run --validate-stage4d before handoff")
        return

    try:
        parsed = parse_and_validate_stage4d(raw_output, shots)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(parsed, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"[v589] wrote {out} ({len(parsed['shots'])} shots) via provider={provider_used}; schema PASS")
    except Stage4dValidationError as exc:
        out_raw = out.with_suffix(".raw.txt")
        out_raw.parent.mkdir(parents=True, exist_ok=True)
        out_raw.write_text(raw_output, encoding="utf-8")
        print(f"[v589] FAIL: {exc}; raw saved to {out_raw}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
