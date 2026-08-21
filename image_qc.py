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

from typing import Any, Dict, List

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
