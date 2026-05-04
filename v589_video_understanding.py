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
        "framing": "<camera distance + frame partition + depth layers + crop>",
        "anchor_props_with_positions": "<every visible prop and its EXACT position>",
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
- Be precise about object POSITIONS in frame (lower-left, immediate foreground,
  behind subject at jaw height, etc.).
- Use ABSOLUTE magnitude language for state changes ('completely melts away',
  'fully revealed', 'entirely dissolves'). 'Dramatically reduced' is forbidden
  when the source shows complete melt — reserved for genuinely partial states.
- Capture dialogue verbatim (use the supplied whisper transcript).
- Identify verbs of state change (pour, squeeze, add, stir, mix, melt, dissolve).
- If a shot is a static talking-head with no state evolution, set
  has_state_evolution=false.
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
