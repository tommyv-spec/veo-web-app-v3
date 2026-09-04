"""v961 — the render models Flow offers, in ONE dependency-neutral place.

SINGLE SOURCE OF TRUTH for which model strings are legal. Imported by
image_platform.py (validate the markdown bullet at import time), main.py
(validate the API payload), static/flow_worker.py (drive the dropdown) and the
linters.

WHY THIS IS ITS OWN MODULE AND NOT A CONSTANT IN main.py
--------------------------------------------------------
main.py imports image_platform (main.py:166). If image_platform imported the
allowlist back out of main, that is a cycle; and importing main from the worker
or a linter would load the whole application entry point to read a list.

This mirrors ALLOWED_CLIP_DURATIONS_S, the analogous per-clip override's
allowlist, which lives in the dependency-neutral clip_duration.py and is
imported from there by image_platform.py:49-55. The v961 plan's first draft put
this constant in main.py; the Codex review caught the cycle before it was
written.

Keep this module free of project imports. It exists so five callers can agree.

THE STRINGS ARE THE FLOW UI'S OWN LABELS
----------------------------------------
They are not identifiers we chose — they are matched against the model
dropdown's menu item text by static/flow_worker.py (ensure_lower_priority_model,
MODEL_SELECTORS). Change one here and the dropdown pick stops finding its
option. `Veo 3.1 - Lite [Lower Priority]` and `Veo 3.1 - Lite` are DIFFERENT
options and the bracket variant is absent on some account tiers (v781), which is
why both are legal.
"""
from __future__ import annotations

# The job-level default, unchanged since before v961. Every read site in
# main.py and static/flow_worker.py falls back to this exact string.
DEFAULT_VEO_MODEL = "Veo 3.1 - Lite [Lower Priority]"

# Every model the Flow dropdown offers, as the dropdown spells it.
ALLOWED_VEO_MODELS = (
    "Omni Flash",
    "Veo 3.1 - Quality",
    "Veo 3.1 - Fast",
    "Veo 3.1 - Lite",
    "Veo 3.1 - Lite [Lower Priority]",
)

# Models that expose the Ingredients tab. Only Omni does, and two arms fail
# closed without it: v943 charswap (static/flow_worker.py:22709-22729) and v959
# movie sections (:23137). Used by the v961 import-time conflict check so a
# build is refused at AUTHORING time rather than burning a render slot.
INGREDIENTS_CAPABLE_MODELS = ("Omni Flash",)


def is_legal_veo_model(value) -> bool:
    """Exact, case-sensitive membership. Deliberately strict: a model string we
    do not recognise cannot be driven in the dropdown, and guessing at it is how
    a whole job renders on the wrong model without anything failing."""
    return isinstance(value, str) and value in ALLOWED_VEO_MODELS


def offers_ingredients_tab(value) -> bool:
    """True when this model exposes the Ingredients tab (Omni only)."""
    return isinstance(value, str) and value in INGREDIENTS_CAPABLE_MODELS


def normalize_veo_model(value):
    """Trim surrounding whitespace and return the value, or None when it is
    empty/None. Does NOT coerce case or repair near-misses — see
    is_legal_veo_model on why guessing is unsafe."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def describe_allowed() -> str:
    """One-line list for an error message, so every caller words it the same."""
    return " | ".join(ALLOWED_VEO_MODELS)
