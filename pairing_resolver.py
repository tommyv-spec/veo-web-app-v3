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

# A `speaker:` cell is often a PHRASE, not a bare token — builds write
# "the main character on-camera", and the platform's own normaliser
# (image_platform._normalize_speaker_mode, mirrored in
# verify_video_format._speaker_mode) handles that by looking at the whole
# string, then the last token, then every token with the priority
# voiceover > on-camera > silent > auto. This module mirrors that priority,
# because a resolver that only understood bare tokens read
# "the main character on-camera" as NOT speaking and rejected a perfectly
# good build (caught on the first real build to use audio_from_scene).
SPEAKING_TOKENS = {"oncamera", "dialogue", "speaks", "spoken",
                   "lipsync", "character", "characterspeaks"}
# `voiceover` is not a speaking source: its audio comes from somewhere else by
# definition, so allowing it would let pairings chain, and a chain has no
# single span to place a visual inside.
VOICEOVER_TOKENS = {"voiceover", "vo", "narration", "offscreen",
                    "narrator", "narrated"}
SILENT_TOKENS = {"silent", "mute", "nodialogue", "nospeech", "music",
                 "musiconly", "sfx", "sfxonly", "broll", "brolloverlay"}


class PairingError(ValueError):
    """A pairing the export could not resolve. Raised at lint and at job setup."""


# THE ONE PLACE THE TWO NUMBERINGS MEET.
# Clip.scene_index is 0-based because the browser assigns it BY POSITION: a
# line's scene_index is the position of its scene in the ordered scene list
# (static/index.html:10795-10802 `sceneIdx = s`, over sceneBreaks built from
# scene_assignments in order). Everything the AUTHOR writes — `### Scene N`
# and the `audio_from_scene: N` that points at it — is 1-based, and the
# resolver works in the author's numbering because that is what the linter
# feeds it.
#
# These live HERE, not inside a caller, for the same reason the pairing rules
# do: three places now need the conversion (Phase 3a in main.py, the
# from-batch payload builder, and the verifier that checks the written rows),
# and three private copies of `- 1` is exactly how an off-by-one goes
# unnoticed — it produces a job that renders the WRONG video instead of an
# error.
def scene_no_to_db_index(scene_no: int) -> int:
    """The author's 1-based `### Scene N` -> the 0-based Clip.scene_index."""
    return scene_no - 1


def db_index_to_scene_no(db_index: int) -> int:
    """The 0-based Clip.scene_index -> the author's 1-based `### Scene N`."""
    return db_index + 1


def _tokens(scene: dict) -> list:
    raw = (scene.get("speaker_mode") or "").strip().lower()
    return [t.strip(",.;:()").replace("-", "").replace("_", "")
            for t in raw.split()]


def _speaks(scene: dict) -> bool:
    """True when this scene's own clip carries real speech.

    Same priority the platform uses: voiceover beats on-camera beats silent.
    A phrase like "the main character on-camera" resolves through its tokens.
    """
    toks = _tokens(scene)
    if not toks:
        return False
    if any(t in VOICEOVER_TOKENS for t in toks):
        return False
    if any(t in SILENT_TOKENS for t in toks):
        return False
    return any(t in SPEAKING_TOKENS for t in toks)


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


# ---------------------------------------------------------------------------
# v698A.2 — place each cutaway on its spoken words (2026-09-04).
#
# The export already listens to the finished audio word by word for the stills
# lane (v825: video_processor.transcribe_master_audio + resolve_support_spans).
# These helpers turn a shared sentence's fragments into that resolver's inputs
# and turn its answers into cutaway windows. Pure and dependency-free like the
# rest of this module: the text normaliser is injected, so this file never
# imports video_processor (whisper, ffmpeg).
#
# One rule at two levels: a cutaway begins when its FIRST word is heard, and a
# sentence begins when ITS first word is heard. split_span (letters) stays as
# the fallback the export keeps whenever the words cannot be trusted.
# ---------------------------------------------------------------------------

def build_alignment_inputs(groups, slot_texts, normalize):
    """Build `scene_lines` + `support_inserts` for resolve_support_spans.

    groups:     ordered [(slot_index, [fragment text, ...]), ...] - one entry
                per shared sentence; slot_index = its position in the speaker
                concat.
    slot_texts: ordered [(authored line, [candidate lines]), ...] - one entry
                per speaker concat slot (every spoken clip, shared or not), so
                the resolver walks the sentences in order.
    normalize:  the SAME normaliser the resolver applies to the heard words
                (video_processor._normalize), so start_word / end_word can never
                disagree with the master on case or punctuation.

    Returns (scene_lines, support_inserts, index, skipped):
      index   {support_index: (group_no, k)}   k is 1-based inside the group
      skipped {group_no: reason}               a group with an empty fragment -
                                               the export keeps its letter
                                               windows and logs the reason.
    """
    scene_lines = []
    for authored, cands in slot_texts:
        a = (authored or "").strip()
        cs = [c.strip() for c in (cands or []) if c and c.strip()]
        if a and a not in cs:
            cs.insert(0, a)
        scene_lines.append({"authored": a, "candidates": cs})
    inserts, index, skipped = [], {}, {}
    n = 0
    for group_no, (slot_index, fragments) in enumerate(groups):
        toks_per = [normalize(f or "").split() for f in fragments]
        bad = [k + 1 for k, t in enumerate(toks_per) if not t]
        if not fragments or bad:
            skipped[group_no] = ("no fragments" if not fragments
                                 else f"empty fragment {bad[0]}")
            continue
        for k, (frag, toks) in enumerate(zip(fragments, toks_per), 1):
            n += 1
            inserts.append({
                "phrase": (frag or "").strip(),
                "start_word": toks[0],
                "end_word": toks[-1],
                "image_index": slot_index,
                "support_index": n,
            })
            index[n] = (group_no, k)
    return scene_lines, inserts, index, skipped


def sentence_container(env, first_word_start, next_line_start, last_word_end,
                       prev_lo, this_lo, envelope_tol=1.0, min_len=0.3):
    """The shared sentence's window on the shipped audio.

    env:              (start, end) - the sentence's MAPPED window (letters ->
                      VAD / speed / sweep mapping). Exact up to concat drift, so
                      it is the sanity envelope, never the answer.
    first_word_start: master time of fragment 1's first word (None = unresolved)
    next_line_start:  master time of the NEXT speaker slot's first word (None
                      when that slot did not align, or there is none)
    last_word_end:    master time of the last fragment's last word (None = unresolved)
    prev_lo / this_lo: word index where the previous / this slot's line aligned
                      (this_lo None = this line did not align). The resolver's
                      cursor advances to a matched line's START, so a repeated
                      line can re-match an earlier occurrence; "did not advance"
                      and the envelope check are what catch that.

    Returns ((start, end), None) or (None, reason).
    """
    if this_lo is None:
        return None, "sentence line did not align"
    if prev_lo is not None and this_lo <= prev_lo:
        return None, "scene alignment did not advance"
    if first_word_start is None:
        return None, "first fragment unresolved"
    start = float(first_word_start)
    if next_line_start is not None:
        end, end_src = float(next_line_start), "next line"
    elif last_word_end is not None:
        end, end_src = float(last_word_end), "last word"
    else:
        end, end_src = float(env[1]), "envelope"
    d0 = abs(start - float(env[0]))
    if d0 > envelope_tol:
        return None, f"aligned sentence drifts from mapped window by {d0:.2f}s (start)"
    d1 = abs(end - float(env[1]))
    if d1 > envelope_tol:
        return None, f"aligned sentence drifts from mapped window by {d1:.2f}s (end, {end_src})"
    if end - start < min_len:
        return None, f"sentence container too short ({end - start:.2f}s)"
    return (start, end), None


def tile_fragment_windows(container, resolved_starts, min_step=0.15):
    """Cut a sentence's container into one window per fragment.

    resolved_starts: [master time of fragment k's first word | None, ...] in
    fragment order; entry 0 is ignored (fragment 1 starts at the container).
    Boundary k = fragment k's first word. Every fragment ends where the next
    begins; the last ends at the container end - no hole, so the exporter never
    has to fill one. The container's edges are themselves word-measured, so a
    boundary outside them is a contradiction, not something to clamp: it means
    the alignment is not trusted for this sentence.

    Returns (windows, None) or (None, reason).
    """
    if not resolved_starts:
        return None, "no fragments"
    cs, ce = float(container[0]), float(container[1])
    if ce - cs <= 0:
        return None, "empty container"
    bounds = [cs]
    for k in range(1, len(resolved_starts)):
        s = resolved_starts[k]
        if s is None:
            return None, f"unresolved fragment {k + 1}"
        s = float(s)
        if s < cs or s > ce:
            off = (cs - s) if s < cs else (s - ce)
            return None, f"boundary {k + 1} outside sentence window by {off:.2f}s"
        bounds.append(s)
    for k in range(1, len(bounds)):
        if bounds[k] - bounds[k - 1] < min_step:
            return None, "non-monotonic"
    if len(bounds) > 1 and ce - bounds[-1] < min_step:
        return None, "non-monotonic"
    windows = [(bounds[k], bounds[k + 1]) for k in range(len(bounds) - 1)]
    windows.append((bounds[-1], ce))
    return windows, None
