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
must be a plain int, reports stay under 64KB, POST returns 409 while the
node is still generating (retry after the render lands).
"""
from __future__ import annotations

# json/os/sys/time/argparse/Optional: used by Tasks 4-7 (judge, CLI)
import json
import os
import sys
import time
import argparse
from typing import Any, Dict, List, Optional

import numpy as np
import cv2

MIN_SHORT_SIDE = 256        # below this the render is junk / a thumbnail
BLANK_STD = 8.0             # grayscale std-dev floor: near-uniform frame
BLUR_LAPLACIAN_VAR = 40.0   # Laplacian variance floor: catastrophic blur only


def analyze_integrity(img_bytes: bytes) -> Dict[str, Any]:
    """Cheap deterministic gates. ok=False means the variant is broken —
    it is excluded from judging and ranked last."""
    reasons: List[str] = []
    arr = cv2.imdecode(np.frombuffer(img_bytes, np.uint8), cv2.IMREAD_COLOR)
    if arr is None:
        return {"ok": False, "reasons": ["undecodable"]}
    h, w = arr.shape[:2]
    if min(h, w) < MIN_SHORT_SIDE:
        reasons.append("low_resolution")
    gray = cv2.cvtColor(arr, cv2.COLOR_BGR2GRAY)
    if float(gray.std()) < BLANK_STD:
        reasons.append("blank_frame")
    elif float(cv2.Laplacian(gray, cv2.CV_64F).var()) < BLUR_LAPLACIAN_VAR:
        reasons.append("extreme_blur")
    return {"ok": not reasons, "reasons": reasons}
