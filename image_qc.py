"""v936 — image variant QC, shadow mode.

Runs LOCALLY (operator box), never on Render. Scores every AI variant of the
nodes in a batch and POSTs a per-node report to /api/images/nodes/{id}/qc.
NEVER chooses a variant (v886.3): the operator keeps the pick; this only
records what the machine would have picked so agreement can be measured.

Funnel per node (built across Tasks 3-7):
  1. integrity gates  (cv2, free)                      <- THIS TASK
  2. face gate        (InsightFace, optional)
  3. Gemini judge     (checklist, prompt-as-rubric) on survivors
  4. pairwise top-2   (both orders; inconsistent = keep checklist order)

Server contract (Task 2): reports carry version: 1, recommended_variant_id
must be a plain int, reports stay under 64,000 bytes, POST returns 409 while
the node is still generating (retry after the render lands).
"""
from __future__ import annotations

import json
import os
import sys
import time
from typing import Any, Dict, List, Optional

import numpy as np
import cv2

INTEGRITY_MIN_SHORT_SIDE = 256        # below this the render is junk / a thumbnail
INTEGRITY_BLANK_STD = 8.0             # grayscale std-dev floor: near-uniform frame
INTEGRITY_BLUR_LAPLACIAN_VAR = 40.0   # Laplacian variance floor: catastrophic blur only


def analyze_integrity(img_bytes: bytes) -> Dict[str, Any]:
    """Cheap deterministic gates. ok=False means the variant is broken —
    it is excluded from judging and ranked last.

    Always reports the measured numbers alongside the verdict so thresholds
    can be recalibrated from accumulated shadow reports (Task 10) instead of
    guessed. Metrics are measured unconditionally; the elif only decides
    which reason strings get appended.
    """
    reasons: List[str] = []
    try:
        arr = cv2.imdecode(np.frombuffer(img_bytes, np.uint8), cv2.IMREAD_COLOR)
    except cv2.error:
        # OpenCV asserts on a zero-length buffer rather than returning None.
        # A failed download must not abort the whole batch run.
        arr = None
    if arr is None:
        return {"ok": False, "reasons": ["undecodable"], "metrics": None}

    h, w = arr.shape[:2]
    gray = cv2.cvtColor(arr, cv2.COLOR_BGR2GRAY)
    gray_std = float(gray.std())
    lap_var = float(cv2.Laplacian(gray, cv2.CV_64F).var())

    if min(h, w) < INTEGRITY_MIN_SHORT_SIDE:
        reasons.append("low_resolution")
    if gray_std < INTEGRITY_BLANK_STD:
        reasons.append("blank_frame")
    elif lap_var < INTEGRITY_BLUR_LAPLACIAN_VAR:
        reasons.append("extreme_blur")

    return {"ok": not reasons, "reasons": reasons,
            "metrics": {"short_side": int(min(h, w)),
                        "gray_std": round(gray_std, 2),
                        "lap_var": round(lap_var, 1)}}


# ──────────────────────────────────────────────────────────────────────
# Gemini checklist judge — the build's own image prompt IS the rubric
# ──────────────────────────────────────────────────────────────────────

# §8 / v808 compliance rows are ALWAYS in the rubric regardless of the prompt.
COMPLIANCE_BANS = (
    "doctor/physician/nurse persona, white or lab coat, scrubs, stethoscope, "
    "medical badge, clinical/exam-room setting, IV, vitals monitor, "
    "certificates or diplomas, framed anatomy poster"
)
MINOR_BAN = "any child, teen, baby, or minor anywhere in frame (v808)"

JUDGE_SCHEMA_HINT = (
    'Reply ONLY with JSON: {"overall": 0-10, "verdict": "pass"|"fail", '
    '"element_misses": [strings], "artifacts": [strings], '
    '"compliance": [strings], "reasons": [strings]}'
)

# The list-valued fields, normalised to lists of strings on every reply so
# callers never have to type-check what the model returned.
_JUDGE_LIST_FIELDS = ("element_misses", "artifacts", "compliance", "reasons")


def build_judge_prompt(image_prompt: str) -> str:
    """The build's own image prompt IS the rubric — every named element
    (subject, prop, pose, wardrobe, setting, text) is checkable."""
    return (
        "You are a strict production QC judge for an AI-generated ad image.\n"
        "SPEC (the exact prompt this image was generated from):\n"
        "---\n" + image_prompt.strip() + "\n---\n"
        "Check, in order:\n"
        "1. element_misses: every element the SPEC names that is missing, "
        "wrong, or replaced (prop, pose, wardrobe, setting, on-image text).\n"
        "2. artifacts: malformed hands or fingers, warped limbs or faces, "
        "garbled or misspelled rendered text, impossible object geometry.\n"
        "3. compliance: report if the image shows " + COMPLIANCE_BANS + ", or "
        + MINOR_BAN + ".\n"
        "4. overall: 0-10 for how well the image fulfils the SPEC (10 = every "
        "element present, clean, well-composed).\n"
        "verdict is 'fail' if there is ANY compliance hit, ANY artifact that "
        "a viewer would notice at feed speed, or a missing element that "
        "changes the shot's meaning. Otherwise 'pass'.\n"
        + JUDGE_SCHEMA_HINT
    )


def parse_judge_reply(raw: Any) -> Optional[Dict[str, Any]]:
    """Tolerant JSON extraction + hard rules the model may not override.

    Two answers only — a normalised dict or None. It never raises: every
    downstream decision reads this dict, and one malformed reply must not
    abort a whole batch run.

    Normalisation:
      * a code fence around the JSON is stripped, and prose either side of
        the outermost {...} is ignored;
      * the four list fields always come back as lists of strings (a bare
        scalar is wrapped, not iterated character by character);
      * `overall` is coerced then CLAMPED to 0-10 — the model is not trusted
        to respect its own scale;
      * `verdict` is RECOMPUTED, never trusted: any compliance hit is 'fail',
        whatever the model said (§8 / v808 can never be talked into a pass).
    """
    if not raw or not isinstance(raw, str):
        return None
    text = raw.strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.lower().startswith("json"):
            text = text[4:]
    start, end = text.find("{"), text.rfind("}")
    if start == -1 or end <= start:
        return None
    try:
        obj = json.loads(text[start:end + 1])
    except Exception:
        return None
    if not isinstance(obj, dict) or "overall" not in obj:
        return None

    for key in _JUDGE_LIST_FIELDS:
        val = obj.get(key)
        if not val:
            obj[key] = []
        elif isinstance(val, (list, tuple)):
            obj[key] = [str(x) for x in val]
        else:
            # A model that answers "compliance": "stethoscope" means ONE hit,
            # not one hit per character.
            obj[key] = [str(val)]

    overall = obj["overall"]
    # bool is an int subclass (image_platform.py:3610 makes the same call for
    # recommended_variant_id) — JSON `true` must not read as a score of 1.
    # Unlike that identity field a float score is meaningful, so 7.9 truncates
    # to 7 rather than being rejected; "high" and NaN/Infinity fall out here.
    if isinstance(overall, bool):
        return None
    try:
        overall = int(float(overall))
    except (TypeError, ValueError, OverflowError):
        return None
    obj["overall"] = max(0, min(10, overall))

    obj["verdict"] = "fail" if (obj["compliance"] or
                                obj.get("verdict") == "fail") else "pass"
    return obj


# ──────────────────────────────────────────────────────────────────────
# Thin API layer — no business logic lives here. Every verdict, clamp and
# override is decided by the pure functions above so it stays unit-testable
# without a network call.
# ──────────────────────────────────────────────────────────────────────

GEMINI_MODEL = os.environ.get("QC_GEMINI_MODEL", "gemini-3.6-flash")


def _gemini_api_key() -> Optional[str]:
    """Process env first; on Windows fall back to the USER environment
    (HKCU\\Environment) — shells opened before the key was set, and Git Bash
    sessions generally, do not inherit per-user variables. Mirrors
    v589_video_understanding.py:1751."""
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


def _gemini_client() -> Any:
    """One client per batch run, reused across every variant judged."""
    key = _gemini_api_key()
    if not key:
        raise RuntimeError(
            "GEMINI_API_KEY not set (process env, nor the Windows USER environment)")
    from google import genai
    return genai.Client(api_key=key)


def judge_variant(client: Any, image_bytes: bytes, image_prompt: str,
                  retries: int = 2) -> Optional[Dict[str, Any]]:
    """Judge one variant against its own image prompt. Returns the parsed
    dict, or None when every attempt failed or came back unparseable — a
    dead judge must degrade the funnel, not abort the batch."""
    from google.genai import types

    prompt = build_judge_prompt(image_prompt)
    attempts = retries + 1
    for attempt in range(1, attempts + 1):
        try:
            resp = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=[types.Part.from_bytes(data=image_bytes,
                                                mime_type="image/png"),
                          types.Part.from_text(text=prompt)],
            )
            parsed = parse_judge_reply(getattr(resp, "text", None))
            if parsed is not None:
                return parsed
            print(f"[qc] judge reply unparseable (attempt {attempt}/{attempts})")
        except Exception as exc:
            print(f"[qc] judge call failed (attempt {attempt}/{attempts}): {exc}")
        if attempt < attempts:
            time.sleep(2 * attempt)
    return None
