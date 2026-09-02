"""v698A many-to-one pairing: who supplies a silent visual's audio, and how a
shared span divides between several visuals.

Pure and dependency-free on purpose. The 1:1 rule it replaces lived inside a DB
loop (main.py:3482) and was therefore never unit-tested, which is why the
constraint stayed invisible until a build needed a sentence-cut read under
picture-cut visuals.

WHY split_span EXISTS AND WHY IT IS NOT TEXT MATCHING. export_with_master_audio
can match clips to a master by transcribing it, but the v698A b-roll path
deliberately does NOT use that: it supplies pre_computed_targets and v701zd then
skips transcription entirely, because the Whisper-master path was measured
under-transcribing a 55s concat (61 words for a 127-word script) and bricking
alignment. Targets are looked up by clip id (main.py:13502-13508), so several
visuals sharing one paired_clip_id would all land on the SAME full-sentence
window and stack. split_span is what gives each sharer its own slice, without a
second transcription.

Weighting is by character length of each sharer's line fragment, in scene order.
It is an approximation of where a fragment falls inside the sentence. Real word
timings would be better and are deliberately future work.
"""
from __future__ import annotations

# Normalised forms (hyphens and underscores stripped) of the speaker modes whose
# clip carries real audio of its own. `voiceover` is NOT one of them: a
# voiceover scene's audio comes from somewhere else by definition, so allowing
# it as a source would let pairings chain, and a chain has no single span to
# place a visual inside.
SPEAKING_MODES = {"oncamera", "dialogue", "speaks", "spoken",
                  "lipsync", "character", "characterspeaks"}


class PairingError(ValueError):
    """A pairing the export could not resolve. Raised at lint and at job setup."""


def _mode(scene: dict) -> str:
    return (scene.get("speaker_mode") or "").strip().lower().replace("-", "").replace("_", "")


def _speaks(scene: dict) -> bool:
    return _mode(scene) in SPEAKING_MODES


def resolve_audio_sources(scenes: list[dict]) -> dict:
    """scene_index -> {audio_source_scene, mint_twin}.

    `audio_source_scene` is the scene whose clip supplies this visual's audio,
    or None when the scene mints its own twin (the pre-existing 1:1 path) or
    needs no audio at all.
    """
    by_index = {s["scene_index"]: s for s in scenes}
    out: dict = {}
    for s in scenes:
        idx = s["scene_index"]
        audio_from = s.get("audio_from_scene")
        anchor = s.get("anchor_node_id")
        if audio_from is None:
            out[idx] = {"audio_source_scene": None, "mint_twin": bool(anchor)}
            continue
        if anchor:
            raise PairingError(
                f"scene {idx} declares both `audio_from_scene: {audio_from}` and a "
                f"voiceover anchor. An anchor means 'mint me an audio twin' and "
                f"`audio_from_scene` means 'do not' - declare exactly one."
            )
        src = by_index.get(audio_from)
        if src is None:
            raise PairingError(
                f"scene {idx} declares `audio_from_scene: {audio_from}` but there is "
                f"no scene {audio_from} in this build."
            )
        if not _speaks(src):
            raise PairingError(
                f"scene {idx} declares `audio_from_scene: {audio_from}`, but scene "
                f"{audio_from} does not speak (speaker: {src.get('speaker_mode')!r}). "
                f"An audio source must be an on-camera speaking scene; pairings cannot "
                f"chain through another voiceover scene."
            )
        out[idx] = {"audio_source_scene": audio_from, "mint_twin": False}
    return out


def split_span(start: float, end: float, fragments: list) -> list:
    """Divide one spoken clip's window between the visuals riding under it.

    Contiguous, ordered, non-overlapping, and the last window ends exactly on
    `end` so no gap opens at the seam. A single fragment therefore receives the
    whole span, which is why a one-sharer group needs no special case.
    """
    if not fragments:
        raise PairingError("split_span called with no fragments")
    total = float(end) - float(start)
    if total <= 0:
        raise PairingError(f"span is not positive: start={start} end={end}")
    n = len(fragments)
    # Weight by fragment length, with a floor of 1 so an EMPTY fragment still
    # gets a real window instead of a zero-length one. That floor is why no
    # separate minimum-window constant is needed: every weight is >= 1, so
    # every window is > 0.
    weights = [max(len((f or "").strip()), 1) for f in fragments]
    wsum = float(sum(weights))
    spans = []
    cursor = float(start)
    for i, w in enumerate(weights):
        if i == n - 1:
            spans.append((cursor, float(end)))   # land exactly on the parent end
            break
        width = total * (w / wsum)
        spans.append((cursor, cursor + width))
        cursor += width
    return spans
