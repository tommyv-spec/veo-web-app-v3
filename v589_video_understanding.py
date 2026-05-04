"""
v589 Stage 4d — VLM-grounded video understanding for action-arc capture.

Adds a structured video-understanding pass to the v579 decode pipeline using
the Gemini API. Acts as the AUTHORITATIVE source for visual action arcs
(parallel to whisper.cpp being the authoritative source for dialogue).

Cross-validates the human-walk findings (v588 dense-frame inspection):
when Gemini reports "the fat completely melts away and the abdominal organs
are fully revealed" but the human-walk wrote "dramatically reduced", the
VLM correction wins (or at minimum the discrepancy is flagged).

Why this stage exists: the @icelandicwisdom HOOK fat-melt was first
mis-decoded as "points at anatomy model" because the midpoint frame caught
only mid-pour. v588 fixed the dense-walk (start/mid/end frames). v589
adds a structural backstop — Gemini sees the entire 6-second clip with
audio + temporal context and produces a timestamped action-arc description
that can't miss the visible state evolution.

Usage:
    export GEMINI_API_KEY="..."           # https://ai.google.dev/
    python v589_video_understanding.py <source.mp4> [--shots shots.json]

Output: stage4d_vlm.json — structured per-shot action-arc descriptions
with timestamps, written next to the input video.

Cost (rough): ~300 tokens/sec at default media resolution. A 45s video
≈ 13.5K input tokens + ~1-2K output tokens. On gemini-2.5-flash that's
well under $0.01 per decode. Free tier covers many decodes per day.

Migration path:
    1. Decoder runs v579 stages 1-4 (audio, whisper, scenes, frames + flow)
       and v588 dense-frame inspection by hand.
    2. Decoder runs THIS script as v589 Stage 4d.
    3. Decoder reconciles human-walk findings with the VLM output before
       authoring the markdown. The VLM JSON is written to
       raw/decode_artifacts/<source-id>/stage4d_vlm.json for audit.
    4. v586 grammar parity + v587 reproduction-ready artifact requirements
       remain — Gemini's output FEEDS the markdown, doesn't replace it.
"""
from __future__ import annotations
import argparse
import json
import os
import sys
import time
from pathlib import Path


# ──────────────────────────────────────────────────────────────────────
# Prompts
# ──────────────────────────────────────────────────────────────────────

SYSTEM_INSTRUCTION = """\
You are a video-understanding assistant for a video reverse-engineering pipeline.

Your job: describe each shot of a viral short-form video with the precision a
generative-video model (Veo 3.1) needs to RE-RENDER it faithfully. The
description must capture VISIBLE STATE-EVOLUTION ARCS within shots — not
just the static composition.

For each shot you receive, output a JSON object with these fields:

{
  "shot": <int>,
  "start": <float seconds>,
  "end": <float seconds>,
  "summary": "<one sentence: the shot's rhetorical function + the visible action>",
  "static_composition": {
    "subject": "<persona pose, eye direction, mouth state, expression>",
    "framing": "<camera distance, frame partition, depth layers, crop>",
    "anchor_props_with_positions": "<every visible prop and its EXACT position in frame>",
    "lighting_and_palette": "<lighting direction, color palette, mood>"
  },
  "action_arc": {
    "has_state_evolution": <bool>,
    "start_state": "<what the foreground prop / subject looks like at shot start>",
    "mid_state": "<what's happening at the mid-point — the pour, squeeze, add, melt>",
    "end_state": "<what the foreground prop / subject looks like at shot end>",
    "magnitude": "<COMPLETE / PARTIAL / MINIMAL — quantify the state change. Use 'COMPLETE' when the prop reaches an end-state visibly distinct from the start-state (fat fully melted off, glass fully drained, lemon fully squeezed). Use 'PARTIAL' only when the source genuinely shows an unfinished state.>",
    "verbs_observed": ["<verb1>", "<verb2>", "..."]
  },
  "audio": "<dialogue + ambient sound cues + register>",
  "veo_reproduction_hints": {
    "use_first_last_frame_workflow": <bool — true when has_state_evolution=true>,
    "start_image_caption": "<one-paragraph Nano Banana 2 description of the optimal START frame>",
    "end_image_caption": "<one-paragraph Nano Banana 2 description of the optimal END frame>",
    "transition_prompt": "<one-paragraph Veo 3.1 transition prompt using ABSOLUTE magnitude language ('completely melts away', 'fully revealed', 'entirely dissolves') — NEVER hedging language ('dramatically', 'mostly', 'almost')>"
  },
  "human_walk_corrections": "<flag any aspect where a frame-by-frame human walk would likely under-describe the action arc — e.g. 'midpoint-only inspection misses the fat-melt payoff in last 2 seconds'>"
}

Hard rules:
- Be precise about object POSITIONS in frame (lower-left, immediate foreground, behind subject at jaw height, etc.).
- Use ABSOLUTE magnitude language for state changes. 'Dramatically reduced' is forbidden when the source shows complete melt. Reserved for genuinely partial states.
- Capture the dialogue verbatim (timestamps from audio track).
- Identify verbs of state change (pour, squeeze, add, stir, mix, melt, dissolve, spread, press, pull, crack).
- If a shot is a static talking-head with no state evolution, set has_state_evolution=false and use_first_last_frame_workflow=false.
"""

USER_PROMPT_TEMPLATE = """\
This source video is a viral short-form ad. The detected shot boundaries are below (from PySceneDetect). Use these to anchor your per-shot analysis.

Shots:
{shots_json}

Whisper transcript (authoritative for dialogue):
{transcript_summary}

Task: produce a JSON ARRAY where each element follows the shot-analysis schema in your system instruction. Cover every shot in order. Use the EXACT shot start/end timestamps provided. Pay extra attention to action arcs in shots whose dialogue contains a verb-of-state-change (pour, squeeze, add, stir, mix, melt, etc.) — these almost always have visible state-evolution that must be captured with absolute magnitude language.

Output: ONLY the JSON array. No prose preamble, no code fences.
"""


# ──────────────────────────────────────────────────────────────────────
# Pipeline
# ──────────────────────────────────────────────────────────────────────

def load_pipeline_inputs(video_path: Path, shots_path: Path | None, transcript_path: Path | None):
    """Load shots.json + transcript.json from the v579 pipeline output dir."""
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


def call_gemini(video_path: Path, shots: list, transcript_summary: str, model: str = "gemini-2.5-flash") -> str:
    """Upload video + structured prompt → Gemini → JSON action-arc description."""
    from google import genai
    from google.genai import types

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "GEMINI_API_KEY env var is not set. Get a key at https://ai.google.dev/ and:\n"
            "  export GEMINI_API_KEY=...\n"
            "Free tier covers ~1500 requests/day on gemini-2.5-flash."
        )

    client = genai.Client(api_key=api_key)

    print(f"[v589] uploading {video_path.name} ({video_path.stat().st_size / 1e6:.1f} MB)...")
    t0 = time.time()
    uploaded = client.files.upload(file=str(video_path))

    while uploaded.state.name == "PROCESSING":
        time.sleep(2)
        uploaded = client.files.get(name=uploaded.name)

    if uploaded.state.name != "ACTIVE":
        raise RuntimeError(f"upload failed: {uploaded.state.name}")
    print(f"[v589] uploaded in {time.time() - t0:.1f}s, state ACTIVE")

    user_prompt = USER_PROMPT_TEMPLATE.format(
        shots_json=json.dumps(shots, indent=2),
        transcript_summary=transcript_summary,
    )

    print(f"[v589] calling {model} with system instruction + video + per-shot prompt...")
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
    print(f"[v589] generated in {time.time() - t1:.1f}s")

    return resp.text


def main():
    p = argparse.ArgumentParser(description="v589 Stage 4d — Gemini VLM video understanding")
    p.add_argument("video", type=Path, help="path to source MP4")
    p.add_argument("--shots", type=Path, default=None, help="path to shots.json (default: alongside video)")
    p.add_argument("--transcript", type=Path, default=None, help="path to transcript.json (default: alongside video)")
    p.add_argument("--model", default="gemini-2.5-flash", help="Gemini model id")
    p.add_argument("--out", type=Path, default=None, help="output stage4d_vlm.json (default: alongside video)")
    args = p.parse_args()

    if not args.video.exists():
        print(f"error: {args.video} not found", file=sys.stderr)
        sys.exit(1)

    shots, transcript, transcript_summary = load_pipeline_inputs(args.video, args.shots, args.transcript)
    print(f"[v589] loaded {len(shots)} shots + {len(transcript['segments'])} transcript segments")

    raw_output = call_gemini(args.video, shots, transcript_summary, args.model)

    out = args.out or args.video.parent / "stage4d_vlm.json"

    try:
        parsed = json.loads(raw_output)
        out.write_text(json.dumps(parsed, indent=2, ensure_ascii=False))
        print(f"[v589] wrote {out} with {len(parsed)} shot analyses")
    except json.JSONDecodeError as e:
        out_raw = out.with_suffix(".raw.txt")
        out_raw.write_text(raw_output)
        print(f"[v589] WARN: could not parse JSON ({e}); raw output saved to {out_raw}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
