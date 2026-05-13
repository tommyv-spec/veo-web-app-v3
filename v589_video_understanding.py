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
                  (1fps + audio + per-second timestamps). Cheapest paid path:
                  ~$0.01 per 45s decode on gemini-2.5-flash. Free tier covers
                  many decodes/day.
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
import json
import os
import sys
import time
from pathlib import Path


# ──────────────────────────────────────────────────────────────────────
# Schema + prompts (shared across providers)
# ──────────────────────────────────────────────────────────────────────

PER_SHOT_SCHEMA = {
    "shot": "<int>",
    "start": "<float seconds>",
    "end": "<float seconds>",
    "summary": "<one sentence: rhetorical function + visible action>",
    "static_composition": {
        "subject": "<persona pose + eye direction + mouth state + expression>",
        "framing": "<v712 relational: subject-to-subject geometry chain (above / below / behind / over the shoulder of / pointing DOWN at) + shot-size DETAIL-DENSITY anchor (which micro-features are visible-and-sharp) + background blur statement. NO coordinate tokens (viewer-left / viewer-right / upper-third / chest-up / NO floor visible) on decode side.>",
        "anchor_props_with_positions": "<every visible prop and its position relative to SUBJECTS (in the patient's lap / behind the doctor's right shoulder / on the counter beside her / in his left hand). NOT relative to frame quadrants.>",
        "lighting_and_palette": "<lighting direction + color palette + mood>",
    },
    "action_arc": {
        "has_state_evolution": "<bool>",
        "start_state": "<foreground prop / subject look at shot start>",
        "mid_state": "<what's happening at midpoint>",
        "end_state": "<foreground prop / subject look at shot end>",
        "magnitude": "<COMPLETE | PARTIAL | MINIMAL>",
        "verbs_observed": ["<verb1>", "<verb2>"],
    },
    "audio": "<dialogue + ambient sound cues + register>",
    "veo_reproduction_hints": {
        "use_blend_to_next_scene": "<bool — true when the action arc continues into the next shot's start state>",
        "needs_platform_future_image_end": "<bool — true when the action arc is contained within ONE shot AND the platform's existing blend-to-next-scene mechanism is insufficient (PLATFORM-FUTURE candidate)>",
        "transition_prompt": "<Veo 3.1 transition narration with ABSOLUTE-magnitude language ('completely melts away', 'fully revealed')>",
    },
    "human_walk_corrections": "<flags any aspect where a midpoint-only walk would under-describe the arc>",
}

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

PERSONA WARDROBE BAN (v722, MANDATORY):

The persona's identity — INCLUDING clothing, wardrobe, accessories,
medical attire, scrubs, coats, ties, stethoscope, badge, glasses,
hair, race, age, build — is carried by the UPLOADED CHARACTER
REFERENCE IMAGE, not by prose. Per v553.1 / v609 / v610, persona
descriptions are minimal: refer to the persona only as "the main
character" (or canonical handle from cast: list). NEVER describe
persona wardrobe in prose.

This applies to decode AND generate AND innovate. The Stage 4d VLM
must observe the source video's persona wardrobe AS METADATA in the
Ingredients table (in the Description column), NEVER in any Image
prompt body / static_composition.subject / action arc / scene line.

Banned phrasings when referring to the PERSONA (the main character):

  "wears her [clothing item]"
  "wearing [clothing item]"
  "[clothing item] on the main character"
  "her crisp white doctor's coat"
  "her white lab coat"
  "her scrub top"
  "her blue scrubs"
  "her V-neck scrub"
  "her uniform"
  "stethoscope around her neck"
  "wears a stethoscope"
  "her medical badge"
  "her clinical attire"
  "wears [color] [garment]"

Required when persona action involves clothing or visible attire:

  "the main character [does action]" — no wardrobe mention
  Identity is in the upload; prose stays minimal.

ASYMMETRY (do NOT confuse with non-persona):

  PERSONA wardrobe -> v722 BANNED (upload carries it)
  NON-PERSONA wardrobe -> v610 / v622 / v669 REQUIRED (prose is the
                          only anchor; without it Banana 2
                          hallucinates the non-persona's clothing)

The Stage 4d VLM:
  - Captures the persona's visible wardrobe ONCE in the Ingredients
    table Description column ("white doctor's coat, professional
    attire, stethoscope") as identity-metadata for the upload bind.
  - Does NOT repeat that wardrobe in any per-image static_composition
    or action_arc field.
  - Captures NON-persona character wardrobe (patient / customer /
    bystander) IN the Image prompt body's [Subject — Host] block
    on first appearance, per v610 / v622 / v669.

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
grammar):
  viewer-left / viewer-right / upper-third line / lower-third line / left half /
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


def build_user_prompt(shots: list, transcript_summary: str) -> str:
    return (
        f"Shots:\n{json.dumps(shots, indent=2)}\n\n"
        f"Whisper transcript (authoritative for dialogue):\n{transcript_summary}\n\n"
        f"Per-shot schema:\n{json.dumps(PER_SHOT_SCHEMA, indent=2)}\n\n"
        "Task: produce a JSON ARRAY where each element follows the schema. "
        "Cover every shot in order. Use the EXACT shot start/end timestamps "
        "provided. Pay extra attention to action arcs in shots whose dialogue "
        "contains a verb-of-state-change (pour, squeeze, add, stir, mix, melt). "
        "Output: ONLY the JSON array. No prose preamble, no code fences."
    )


# ──────────────────────────────────────────────────────────────────────
# Pipeline I/O
# ──────────────────────────────────────────────────────────────────────

def load_pipeline_inputs(video_path: Path, shots_path: Path | None, transcript_path: Path | None):
    workdir = video_path.parent
    if shots_path is None:
        shots_path = workdir / "shots.json"
    if transcript_path is None:
        transcript_path = workdir / "transcript.json"

    if not shots_path.exists():
        raise FileNotFoundError(f"shots.json not found at {shots_path}. Run v579 Stage 3 first.")
    if not transcript_path.exists():
        raise FileNotFoundError(f"transcript.json not found at {transcript_path}. Run v579 Stage 2 first.")

    shots = json.loads(shots_path.read_text())
    transcript = json.loads(transcript_path.read_text())
    transcript_summary = "\n".join(
        f"  [{seg['start']:6.2f}-{seg['end']:6.2f}] {seg['text']}"
        for seg in transcript["segments"]
    )
    return shots, transcript, transcript_summary


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
                  base_url: str = "http://localhost:1234", model: str | None = None) -> str:
    import base64, urllib.request, urllib.error

    available, default_model = lmstudio_available(base_url)
    if not available:
        raise RuntimeError(f"LM Studio not reachable at {base_url}/v1/models — open the app and enable the local server")
    model = model or default_model
    print(f"[v589] LM Studio: model={model}, sending {len(frames)} dense frames + transcript")

    content = [{"type": "text", "text": SYSTEM_INSTRUCTION}, {"type": "text", "text": build_user_prompt(shots, transcript_summary)}]
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

def gemini_available() -> bool:
    return bool(os.environ.get("GEMINI_API_KEY"))


def call_gemini(video_path: Path, shots: list, transcript_summary: str,
                model: str = "gemini-2.5-flash") -> str:
    from google import genai
    from google.genai import types

    api_key = os.environ.get("GEMINI_API_KEY")
    client = genai.Client(api_key=api_key)

    print(f"[v589] Gemini: uploading {video_path.name} ({video_path.stat().st_size / 1e6:.1f} MB)")
    t0 = time.time()
    uploaded = client.files.upload(file=str(video_path))
    while uploaded.state.name == "PROCESSING":
        time.sleep(2)
        uploaded = client.files.get(name=uploaded.name)
    if uploaded.state.name != "ACTIVE":
        raise RuntimeError(f"upload failed: {uploaded.state.name}")
    print(f"[v589] Gemini: uploaded in {time.time() - t0:.1f}s, state ACTIVE")

    user_prompt = build_user_prompt(shots, transcript_summary)
    print(f"[v589] Gemini: calling {model}")
    t1 = time.time()
    resp = client.models.generate_content(
        model=model,
        contents=[uploaded, user_prompt],
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_INSTRUCTION,
            response_mime_type="application/json",
            temperature=0.0,
        ),
    )
    print(f"[v589] Gemini: generated in {time.time() - t1:.1f}s")
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
    out = []
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

        out.append({
            "shot": s["shot"],
            "start": s["start"],
            "end": s["end"],
            "_meta": {
                "dense_frames_to_walk": [f.name for f in shot_frames],
                "overlapping_dialogue": dialogue,
                "instruction": "Fill in fields below by walking the dense frames + dialogue. Use ABSOLUTE-magnitude language when state change is COMPLETE.",
            },
            "summary": "",
            "static_composition": {
                "subject": "", "framing": "", "anchor_props_with_positions": "", "lighting_and_palette": "",
            },
            "action_arc": {
                "has_state_evolution": None, "start_state": "", "mid_state": "", "end_state": "",
                "magnitude": "<COMPLETE | PARTIAL | MINIMAL>", "verbs_observed": [],
            },
            "audio": "",
            "veo_reproduction_hints": {
                "use_blend_to_next_scene": None,
                "needs_platform_future_image_end": None,
                "transition_prompt": "",
            },
            "human_walk_corrections": "",
        })
    return json.dumps(out, indent=2, ensure_ascii=False)


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="v589 Stage 4d — VLM-grounded video understanding")
    p.add_argument("video", type=Path, help="path to source MP4")
    p.add_argument("--shots", type=Path, default=None)
    p.add_argument("--transcript", type=Path, default=None)
    p.add_argument("--out", type=Path, default=None)
    p.add_argument("--provider", choices=["lmstudio", "gemini", "template", "auto"], default="auto",
                   help="force a specific provider; default 'auto' cascades lmstudio → gemini → template")
    p.add_argument("--model", default=None, help="override default model id for lmstudio/gemini")
    p.add_argument("--lmstudio-url", default="http://localhost:1234")
    args = p.parse_args()

    if not args.video.exists():
        print(f"error: {args.video} not found", file=sys.stderr)
        sys.exit(1)

    shots, transcript, transcript_summary = load_pipeline_inputs(args.video, args.shots, args.transcript)
    print(f"[v589] loaded {len(shots)} shots + {len(transcript['segments'])} dialogue segments")

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
        raw_output = call_lmstudio(args.video, frames, shots, transcript_summary, args.lmstudio_url, args.model)
        provider_used = "lmstudio"
        return True

    def try_gemini():
        nonlocal raw_output, provider_used
        if not gemini_available():
            print(f"[v589] GEMINI_API_KEY not set — skipping Gemini")
            return False
        raw_output = call_gemini(args.video, shots, transcript_summary, args.model or "gemini-2.5-flash")
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
        if not try_lmstudio() and not try_gemini():
            print(f"[v589] no automated VLM provider available — writing human-walk template")
            print(f"       (recommended free path: install LM Studio + Gemma 4 E2B vision model;")
            print(f"        then re-run this script with the LM Studio app open)")
            write_template()

    try:
        parsed = json.loads(raw_output)
        out.write_text(json.dumps(parsed, indent=2, ensure_ascii=False))
        print(f"[v589] wrote {out} ({len(parsed)} shots) via provider={provider_used}")
    except json.JSONDecodeError:
        out_raw = out.with_suffix(".raw.txt")
        out_raw.write_text(raw_output)
        print(f"[v589] WARN: provider returned non-JSON; raw saved to {out_raw}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
