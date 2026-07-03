"""v807 — pure, dependency-free helpers for prominent-people image
auto-retry. Kept out of main.py so the logic is unit-testable locally
(import main fails in dev on a Starlette version mismatch)."""
import json as _json

VALID_RETRY_MODES = {"off", "next", "prev", "batch"}


def parse_auto_image_retry_mode(settings_json):
    """Extract the account's auto-image-retry mode from a User.settings_json
    string. Defaults to 'batch' (mode C) when missing/invalid. Never raises."""
    if not settings_json:
        return "batch"
    try:
        data = _json.loads(settings_json)
        mode = (data.get("auto_image_retry") or {}).get("mode")
    except Exception:
        return "batch"
    return mode if mode in VALID_RETRY_MODES else "batch"


def order_distinct_frames(clips):
    """v807 — distinct start_frame keys in clip_index order. Accepts dicts
    or Clip objects (duck-typed on .get / attribute)."""
    def _idx(c): return c["clip_index"] if isinstance(c, dict) else c.clip_index
    def _sf(c):  return c["start_frame"] if isinstance(c, dict) else c.start_frame
    seen, out = set(), []
    for c in sorted(clips, key=_idx):
        sf = _sf(c)
        if sf and sf not in seen:
            seen.add(sf); out.append(sf)
    return out


def pick_substitute(mode, frames, original, tried):
    """v807 — choose the next substitute frame. A/B are single-step
    neighbors; batch (C) returns the first untried OTHER frame (caller loops
    it as a bounded sweep). Returns None when no candidate is available."""
    if mode == "off" or not frames or original not in frames:
        return None
    i = frames.index(original)
    if mode == "next":
        cand = frames[i + 1] if i + 1 < len(frames) else None
        return cand if cand and cand not in tried else None
    if mode == "prev":
        cand = frames[i - 1] if i - 1 >= 0 else None
        return cand if cand and cand not in tried else None
    if mode == "batch":
        for cand in frames:
            if cand != original and cand not in tried:
                return cand
        return None
    return None
