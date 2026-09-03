"""v892.12 — turn a `prepare-for-video` response into a job-creation request,
and prove afterwards that the rows the database got are the rows prepare sent.

WHY THIS IS A SEPARATE, PURE MODULE. The same reasoning that produced
pairing_resolver.py: the rule it replaces "lived inside a DB loop and was
therefore never unit-tested, which is why the constraint stayed invisible".
Everything here is plain dicts — no FastAPI, no SQLAlchemy, no database — so
the 2026-09-03 failure shape can be reproduced in a test and watched to fail.

WHAT THE 2026-09-03 FAILURE WAS. A many-to-one build has spoken scenes (a
person reading a sentence on camera) and cutaway scenes (a silent shot that
rides under one of those sentences, declared `audio_from_scene: N`). Three
facts have to reach the Clip row for a cutaway to work: `clip_role =
'visual_pair'`, `audio_from_scene = N`, and a 0-based `scene_index`. The CLI
promote path wrote none of the first two. The cutaways kept their line fragment
in `dialogue_text`, so the render treated them as spoken and asked Veo to
lip-sync a fragment onto a shot with no face. Nothing errored. The build passed
every gate. The video was wrong.

So the question this module is judged by is not "does it work" but: what can it
do that produces a WRONG VIDEO instead of an error, and what stops that.

  * A hand-enumerated field list drifts (v892.2, v892.5, v892.8 all came from
    one). So build_create_job_request enumerates NOTHING: the caller passes in
    `set(DialogueLineInput.model_fields)` and each prepared row is filtered
    through it by name. Two keys need a real rename and both are named, with
    the reason, in RENAMED_LINE_FIELDS.
  * An off-by-one on `scene_index` makes every cutaway ride under the NEXT
    sentence — silently. The 1-based/0-based conversion is imported from
    pairing_resolver, the one place it is defined.
  * A guessed job config changes cost and quality with nobody choosing. This
    module holds no config key names at all: missing_config_keys takes the
    required set IN, and the server computes it from the pydantic model.
"""
from __future__ import annotations

from collections import Counter

from pairing_resolver import scene_no_to_db_index

# The only two keys whose name differs between a prepared row and the line
# model. Every other field crosses by name, which is the point — a list of
# renames is a list someone must defend, so it is kept at two entries.
#
#   transition -> scene_transition : the prepared row calls it what the SCENE
#       calls it; the line model calls it what the LINE calls it. The browser
#       does the same rename (index.html builds sceneTransition, sends
#       scene_transition).
#   image_local_index -> start_image_idx : not a rename but a LOOKUP. The row
#       carries a position in prepare's `uploaded[]` list; the line model wants
#       the server-side file index, which is `uploaded[local]["index"]`. The
#       browser does exactly this (`uploadedFilesData[localIdx]?.index`).
RENAMED_LINE_FIELDS = {"transition": "scene_transition"}

# Clips at or above this index are audio twins, minted by Phase 3a AFTER job
# setup starts (main.py's +100000 convention). The verifier runs before setup
# exists, so none are present — the filter is there so the function stays
# correct if it is ever reused later.
AUDIO_TWIN_INDEX_OFFSET = 100000


def missing_config_keys(config: dict, required) -> list:
    """Names in `required` that `config` does not carry, sorted.

    The required set is an ARGUMENT on purpose. VideoConfigInput defaults every
    single field, so a half-written config silently becomes a full config —
    a different Veo model, a different variant count, a different backend, all
    unchosen. The first draft of this rule was a hand-picked tuple of ten keys
    and it had already forgotten video_backend, kling_variant,
    short_dialogue_mode, max_retries_per_clip and use_interpolation. So the
    server passes `set(VideoConfigInput.model_fields)` and a field added to the
    model is required here the moment it exists.
    """
    return sorted(k for k in required if k not in (config or {}))


def check_prepare_transport(assignment_rows: list, scenes_metadata: list) -> list:
    """Did the authored duration survive the trip from the row to the payload?

    Runs BEFORE any job exists, so a problem here costs a 400 and nothing has
    to be torn down. It answers the one question the post-create verifier
    cannot: the verifier only checks rows that DECLARE a duration, so a
    prepared row that lost the key makes that check skip itself silently. This
    function sees both sides and notices the loss (v889).
    """
    problems = []
    by_scene = {}
    for row in scenes_metadata or []:
        by_scene.setdefault(row.get("scene_index"), []).append(row)
    for a in assignment_rows or []:
        declared = a.get("explicit_target_s")
        if declared is None:
            continue
        scene_no = a.get("scene_index")
        rows = by_scene.get(scene_no) or []
        if not rows:
            problems.append(
                f"scene {scene_no} declares explicit_target_s={declared} but "
                f"prepare produced no row for that scene at all")
            continue
        for row in rows:
            if "explicit_target_s" not in row:
                problems.append(
                    f"scene {scene_no} declares explicit_target_s={declared} "
                    f"but the prepared row carries no explicit_target_s key — "
                    f"the authored duration is dropped in transport (v889)")
            elif row.get("explicit_target_s") != declared:
                problems.append(
                    f"scene {scene_no} declares explicit_target_s={declared} "
                    f"but the prepared row carries "
                    f"{row.get('explicit_target_s')!r}")
    return problems


def _scene_position_map(prepared: dict) -> dict:
    """author's scene number -> its 0-based POSITION in the storyboard.

    This is the browser's rule and the reason Clip.scene_index is 0-based at
    all: `sceneIdx = s`, the scene's position in the ordered list built from
    scene_assignments (index.html:10795-10802, 24545-24548). It is NOT
    `scene_index - 1`; a build whose scene numbers are not contiguous still
    maps by position, and the two only look the same on a tidy build.
    """
    return {sa.get("scene_index"): pos
            for pos, sa in enumerate(prepared.get("scene_assignments") or [])}


def _image_index(prepared: dict, local_idx):
    """prepare's local upload position -> the server-side file index."""
    if local_idx is None:
        return None                      # text_card: no image, by design
    uploaded = prepared.get("uploaded") or []
    if not isinstance(local_idx, int) or local_idx < 0 or local_idx >= len(uploaded):
        raise ValueError(
            f"image_local_index {local_idx!r} is not a position in prepare's "
            f"uploaded list of {len(uploaded)}")
    entry = uploaded[local_idx]
    return entry.get("index", local_idx)


def build_create_job_request(prepared: dict, config: dict, batch_id: str,
                             allowed) -> dict:
    """A plain dict shaped like CreateJobRequest. main.py validates it.

    Returned as a dict, not a pydantic object, so this module needs no FastAPI
    import and can be tested without one. `allowed` is
    `set(DialogueLineInput.model_fields)`, passed in for the same reason the
    config keys are: a field list written here would drift from the model.
    """
    lines_text = prepared.get("dialogue_lines")
    rows = prepared.get("scenes_metadata")
    if lines_text is None or rows is None:
        raise ValueError("prepare response is missing dialogue_lines or "
                         "scenes_metadata")
    if len(lines_text) != len(rows):
        # The pre-v682f bug in one assertion: only on-camera lines became
        # clips, so a silent scene vanished between the two lists.
        raise ValueError(
            f"prepare returned {len(lines_text)} dialogue_lines but "
            f"{len(rows)} scenes_metadata rows — they are parallel by "
            f"contract, so one silent or text_card scene has been dropped")

    scene_position = _scene_position_map(prepared)

    dialogue_lines = []
    for i, row in enumerate(rows):
        scene_no = row.get("scene_index")
        if scene_no not in scene_position:
            raise ValueError(
                f"row {i} belongs to scene {scene_no!r}, which is not one of "
                f"the {len(scene_position)} scenes prepare returned")
        pos = scene_position[scene_no]
        line = {k: v for k, v in row.items() if k in allowed}
        for src, dst in RENAMED_LINE_FIELDS.items():
            if src in row and dst in allowed:
                line[dst] = row.get(src)
        line["id"] = i + 1
        line["text"] = lines_text[i] or ""
        line["start_image_idx"] = _image_index(prepared, row.get("image_local_index"))
        # 0-BASED, BY POSITION. Never the row's own scene number — that is the
        # author's 1-based `### Scene N`, and shipping it here is the off-by-one
        # that makes every cutaway ride under the next sentence.
        line["scene_index"] = pos
        # The browser sends no transition on the first scene; match it exactly
        # rather than leaving a value the browser would never have sent.
        if pos == 0 and "scene_transition" in allowed:
            line["scene_transition"] = None
        dialogue_lines.append(line)

    scenes = []
    for pos, sa in enumerate(prepared.get("scene_assignments") or []):
        scenes.append({
            "sceneIndex": pos,
            "imageIndex": _image_index(prepared, sa.get("image_local_index")),
            "clipMode": (sa.get("clip_mode") or "fresh"),
            "transition": (sa.get("transition") if pos > 0 else None),
            "clips": [i for i, ln in enumerate(dialogue_lines)
                      if ln["scene_index"] == pos],
            "scene_type": sa.get("scene_type"),
        })

    return {
        "config": config,
        "dialogue_lines": dialogue_lines,
        "api_keys": {"gemini_keys": [], "openai_key": None},
        "job_id": prepared.get("upload_job_id"),
        "scenes": scenes,
        # v827 — the from-batch path never designates a trailing end frame.
        "last_frame_index": None,
        "image_batch_id": batch_id,
    }


def expected_rows_from_prepare(prepared: dict) -> list:
    """The rows the verifier compares against: prepare's own metadata rows,
    each carrying the dialogue text that belongs to it.

    Deliberately derived from what prepare RETURNED rather than re-derived from
    the assignments. The question being asked is "did what prepare said reach
    the database unchanged", and re-deriving would answer a different one.
    """
    rows = prepared.get("scenes_metadata") or []
    texts = prepared.get("dialogue_lines") or []
    out = []
    for i, row in enumerate(rows):
        r = dict(row)
        r["dialogue_text"] = texts[i] if i < len(texts) else None
        out.append(r)
    return out


def _role(value):
    """The DB stores NULL where a payload may carry ''. Same thing."""
    return None if value in (None, "") else value


def _speaks(clip: dict) -> bool:
    return (_role(clip.get("clip_role")) in (None, "single")
            and bool((clip.get("dialogue_text") or "").strip()))


def verify_promoted_clips(expected_rows: list, clip_rows: list,
                          scene_position: dict) -> list:
    """Compare the written Clip rows with what prepare said. [] = pass.

    Called with the background setup task NOT YET SPAWNED, so nothing can race
    this and a failure can delete the job before anything renders.

    It deliberately does NOT re-decide the pairing — whether scene N really
    SPEAKS by the resolver's rules is Phase 3a's job, and it still runs
    afterwards and still refuses setup on an unresolvable pairing. This
    function answers two narrower questions: was the TRANSPORT faithful, and is
    the NUMBERING BASE right.
    """
    problems = []
    visual = [c for c in clip_rows
              if (c.get("clip_index") or 0) < AUDIO_TWIN_INDEX_OFFSET]
    by_index = {c.get("clip_index"): c for c in visual}

    # 1. count
    if len(visual) != len(expected_rows):
        problems.append(
            f"count: prepare returned {len(expected_rows)} rows but the job "
            f"has {len(visual)} clips")

    # per-row comparison
    for i, row in enumerate(expected_rows):
        clip = by_index.get(i)
        if clip is None:
            problems.append(f"clip_index {i}: no clip was written for it")
            continue
        want_scene = scene_position.get(row.get("scene_index"))
        if clip.get("scene_index") != want_scene:
            problems.append(
                f"clip_index {i} (build scene {row.get('scene_index')}): "
                f"scene_index is {clip.get('scene_index')!r}, expected "
                f"{want_scene!r} (the scene's 0-based POSITION)")
        if _role(clip.get("clip_role")) != _role(row.get("clip_role")):
            problems.append(
                f"clip_index {i} (build scene {row.get('scene_index')}): "
                f"clip_role is {clip.get('clip_role')!r}, prepare said "
                f"{row.get('clip_role')!r}")
        if clip.get("audio_from_scene") != row.get("audio_from_scene"):
            problems.append(
                f"clip_index {i} (build scene {row.get('scene_index')}): "
                f"audio_from_scene is {clip.get('audio_from_scene')!r}, "
                f"prepare said {row.get('audio_from_scene')!r} — this value is "
                f"passed through unconverted, so any difference is a bug")
        if (clip.get("dialogue_text") or "") != (row.get("dialogue_text") or ""):
            problems.append(
                f"clip_index {i} (build scene {row.get('scene_index')}): "
                f"dialogue_text does not match the prepared line")
        # v889 (§4 item 6) — the authored duration, checked at the ROW, not at
        # the log line. On the 2026-09-03 job every spoken clip stored 1.1s
        # while the build declared 8/4/6/8/8/4/8/4/8; this assertion fails that
        # shape. Rows that declare nothing are not checked — which is exactly
        # why check_prepare_transport runs first and refuses a dropped key.
        declared = row.get("explicit_target_s")
        if declared is not None:
            got = clip.get("target_duration_s")
            if got is None or abs(float(got) - float(declared)) > 0.01:
                problems.append(
                    f"clip_index {i} (build scene {row.get('scene_index')}): "
                    f"target_duration_s is {got!r}, but the build declares "
                    f"{declared} (v889 — the authored bullet is authoritative)")

    # 2. role histogram — on the 2026-09-03 failure this line alone would have
    #    said `expected {None: 9, 'visual_pair': 15}, got {None: 24}`.
    want_roles = Counter(_role(r.get("clip_role")) for r in expected_rows)
    got_roles = Counter(_role(c.get("clip_role")) for c in visual)
    if want_roles != got_roles:
        problems.append(
            f"clip_role histogram: expected {dict(want_roles)}, got "
            f"{dict(got_roles)}")

    # 3. the audio_from_scene values, in clip order
    want_afs = [(i, r.get("audio_from_scene")) for i, r in enumerate(expected_rows)
                if r.get("audio_from_scene") is not None]
    got_afs = sorted((c.get("clip_index"), c.get("audio_from_scene"))
                     for c in visual if c.get("audio_from_scene") is not None)
    if want_afs != got_afs:
        problems.append(
            f"audio_from_scene by clip: expected {want_afs}, got {got_afs}")

    # 4. the base check — the silent one. A cutaway must point at a clip that
    #    SPEAKS, and never at itself.
    for c in visual:
        afs = c.get("audio_from_scene")
        if afs is None:
            continue
        target = scene_no_to_db_index(afs)
        if target == c.get("scene_index"):
            problems.append(
                f"clip_index {c.get('clip_index')}: audio_from_scene={afs} "
                f"resolves to scene_index {target}, which is its OWN scene — a "
                f"scene cannot ride under itself, so the 1-based/0-based base "
                f"is wrong")
            continue
        if not any(_speaks(o) and o.get("scene_index") == target for o in visual):
            problems.append(
                f"clip_index {c.get('clip_index')}: audio_from_scene={afs} "
                f"resolves to scene_index {target}, where no clip speaks — "
                f"either the base is wrong or the source scene is silent")

    # 5. no spoken clip carries audio_from_scene
    for c in visual:
        if c.get("audio_from_scene") is not None and \
                _role(c.get("clip_role")) != "visual_pair":
            problems.append(
                f"clip_index {c.get('clip_index')}: carries "
                f"audio_from_scene={c.get('audio_from_scene')} but its "
                f"clip_role is {c.get('clip_role')!r} — only a visual_pair "
                f"rides under another clip's audio")

    return problems
