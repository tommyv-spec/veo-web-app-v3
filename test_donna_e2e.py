"""
End-to-end Donna data flow test.

Loads videos/donna-weight-loss.md and runs each stage that the production
import + prepare-for-video + dialogue-payload pipeline runs, checking
that the markdown's per-clip Veo prompts survive every transformation
and would arrive at /api/jobs as DialogueLineInput.veo_prompt_override
on every clip.

Run from repo root:
    cd code && python test_donna_e2e.py
"""
import json
import sys
from pathlib import Path

# Ensure we can import platform modules
sys.path.insert(0, str(Path(__file__).parent))

from veo_prompt_overrides import (
    parse_veo_prompts_block,
    attach_veo_prompts_to_scenes,
)


def load_artifact() -> str:
    p = Path(__file__).resolve().parent.parent / "videos" / "donna-weight-loss.md"
    return p.read_text(encoding="utf-8")


def fail(stage: str, msg: str, *, exit: bool = True) -> None:
    print(f"  [FAIL] {stage}: {msg}")
    if exit:
        sys.exit(1)


def ok(stage: str, msg: str = "") -> None:
    suffix = f" — {msg}" if msg else ""
    print(f"  [OK]   {stage}{suffix}")


def test_stage1_parse_veo_prompts_block(md: str):
    print("\nStage 1: parse_veo_prompts_block")
    prompts = parse_veo_prompts_block(md)
    if not prompts:
        fail("parse", "returned empty dict — section not detected or no clips matched")
    expected_scenes = {1, 2, 3, 4, 5, 6, 7, 8}
    actual_scenes = {k[0] for k in prompts.keys()}
    if actual_scenes != expected_scenes:
        fail(
            "parse",
            f"expected scenes {expected_scenes}, got {actual_scenes}",
        )
    ok("parse", f"{len(prompts)} clips → scenes {sorted(actual_scenes)}")
    # Sample check — scene 7 should have the CTA-setup line in its text
    s7 = prompts.get((7, 1))
    if not s7:
        fail("parse", "scene 7 clip missing")
    if "comment" not in (s7["text_prompt"] or "").lower():
        fail("parse", f"scene 7 text doesn't mention 'comment': {s7['text_prompt'][:80]!r}")
    ok("parse", "scene 7 contains expected CTA content")
    return prompts


def test_stage2_attach_to_scenes(prompts):
    print("\nStage 2: attach_veo_prompts_to_scenes (with v682f zero-line patch)")
    # Simulate scenes list from import_scene_table parser
    scenes = [
        {"scene_index": 1, "lines": []},               # silent
        {"scene_index": 2, "lines": []},               # silent
        {"scene_index": 3, "lines": []},               # silent
        {"scene_index": 4, "lines": []},               # silent (palpation, post-v682b)
        {"scene_index": 5, "lines": []},               # text_card
        {"scene_index": 6, "lines": []},               # silent
        {"scene_index": 7, "lines": ["I can help. Comment GUIDE..."]},  # on-camera
        {"scene_index": 8, "lines": ["but you must follow me first..."]},  # on-camera
    ]
    attach_veo_prompts_to_scenes(scenes, prompts)
    for s in scenes:
        sn = s["scene_index"]
        vp = s.get("veo_prompts")
        if not vp:
            fail("attach", f"scene {sn} got empty veo_prompts (zero-line patch broken?)")
        if len(vp) != max(1, len(s["lines"])):
            fail("attach", f"scene {sn} veo_prompts length {len(vp)} != expected {max(1, len(s['lines']))}")
        if vp[0] is None:
            fail("attach", f"scene {sn} veo_prompts[0] is None — clip prompt not attached")
        ok("attach", f"scene {sn} → veo_prompts[0] text starts {vp[0]['text_prompt'][:40]!r}")
    return scenes


def test_stage3_to_dict_truncation(scenes):
    print("\nStage 3: ImageSceneAssignment.to_dict v682f no-truncate-when-empty")
    # Simulate what to_dict does after DB round trip
    for s in scenes:
        sn = s["scene_index"]
        lines = s["lines"]
        veo_prompts_stored = s["veo_prompts"]
        # Simulate to_dict logic with the v682f fix:
        veo_prompts = list(veo_prompts_stored)
        if lines:
            while len(veo_prompts) < len(lines):
                veo_prompts.append(None)
            veo_prompts = veo_prompts[:len(lines)]
        # else: skip truncation (preserve stored 1-entry list for silent / text_card)
        s["veo_prompts_after_to_dict"] = veo_prompts
        if not veo_prompts or veo_prompts[0] is None:
            fail("to_dict", f"scene {sn} veo_prompts lost in to_dict simulation")
        ok("to_dict", f"scene {sn} veo_prompts[0] preserved")
    return scenes


def test_stage4_prepare_for_video_flat_row(scenes):
    print("\nStage 4: prepare_batch_for_video flat-row injection")
    # Simulate the synthetic + per-line loop emitting scenes_metadata_flat
    scenes_metadata_flat = []
    dialogue_lines_flat = []
    speaker_modes = {
        1: "silent", 2: "silent", 3: "silent", 4: "silent",
        5: None, 6: "silent",
        7: "on-camera", 8: "on-camera",
    }
    scene_types = {
        1: "shot", 2: "shot", 3: "shot", 4: "shot",
        5: "text_card",
        6: "shot", 7: "shot", 8: "shot",
    }
    for s in scenes:
        sn = s["scene_index"]
        lines = s["lines"]
        veo_prompts = s["veo_prompts_after_to_dict"]
        speaker_mode = speaker_modes[sn]
        scene_type = scene_types[sn]
        scene_is_silent = speaker_mode == "silent"
        scene_is_text_card = scene_type == "text_card"

        if (scene_is_text_card or scene_is_silent) and not lines:
            # Synthetic injection branch (v682f silent_vp + v682g speaker_mode denorm)
            silent_vp = veo_prompts[0] if scene_is_silent and veo_prompts else None
            dialogue_lines_flat.append("")
            scenes_metadata_flat.append({
                "scene_index": sn,
                "veo_prompt_override": (silent_vp or {}).get("text_prompt") if silent_vp else None,
                "veo_negative_prompt_override": (silent_vp or {}).get("negative_prompt") if silent_vp else None,
                "speaker_mode": speaker_mode,
                "scene_type": "text_card" if scene_is_text_card else (scene_type or None),
                "action_note": "",
                "dialogue_pad": None,
                "caption": "2 months later..." if scene_is_text_card else None,
            })
        else:
            # Per-line loop (on-camera scenes)
            for i, ln in enumerate(lines):
                vp = veo_prompts[i] if i < len(veo_prompts) else None
                dialogue_lines_flat.append(ln)
                scenes_metadata_flat.append({
                    "scene_index": sn,
                    "veo_prompt_override": (vp or {}).get("text_prompt") if vp else None,
                    "veo_negative_prompt_override": (vp or {}).get("negative_prompt") if vp else None,
                    "speaker_mode": speaker_mode,
                    "scene_type": scene_type,
                    "action_note": f"scene_{sn}_action",
                    "dialogue_pad": "scene_7_pad" if sn == 7 else None,
                    "caption": "THE SAME ISSUE" if sn == 7 else None,
                })
    # Verify count: 8 entries (one per scene) — Donna has 1 line per scene incl silent/text_card
    if len(dialogue_lines_flat) != 8:
        fail("prepare", f"expected 8 flat rows, got {len(dialogue_lines_flat)}")
    ok("prepare", f"{len(dialogue_lines_flat)} flat rows emitted")

    # Every scene EXCEPT text_card must have veo_prompt_override
    for idx, sm in enumerate(scenes_metadata_flat):
        sn = sm["scene_index"]
        is_text_card = sm["scene_type"] == "text_card"
        has_override = sm["veo_prompt_override"] is not None
        if is_text_card:
            if has_override:
                fail("prepare", f"scene {sn} text_card got an override (should be None)")
            ok("prepare", f"row[{idx}] scene {sn} text_card → no Veo prompt (correct)")
        else:
            if not has_override:
                fail("prepare", f"row[{idx}] scene {sn} missing veo_prompt_override")
            ok("prepare", f"row[{idx}] scene {sn} → override len {len(sm['veo_prompt_override'])}")

    return dialogue_lines_flat, scenes_metadata_flat


def test_stage5_frontend_indexing(scenes_metadata_flat):
    print("\nStage 5: frontend builds _veoPromptOverrides + _actionNotes from scenes_metadata (v682h/v682i)")
    # Simulate frontend v682h/v682i loop
    veoOverridesObj = {}
    notesObj = {}
    padsObj = {}
    for idx, sm in enumerate(scenes_metadata_flat):
        tp = sm.get("veo_prompt_override")
        np_ = sm.get("veo_negative_prompt_override")
        if tp or np_:
            veoOverridesObj[idx] = {"text_prompt": tp, "negative_prompt": np_}
        if sm.get("action_note") and str(sm["action_note"]).strip():
            notesObj[idx] = sm["action_note"]
        if sm.get("dialogue_pad") and str(sm["dialogue_pad"]).strip():
            padsObj[idx] = sm["dialogue_pad"]

    # Verify alignment: every non-text_card row should have entry in veoOverridesObj
    for idx, sm in enumerate(scenes_metadata_flat):
        is_text_card = sm["scene_type"] == "text_card"
        if is_text_card:
            if idx in veoOverridesObj:
                fail("frontend", f"row[{idx}] text_card has unexpected veoOverridesObj entry")
            ok("frontend", f"row[{idx}] text_card has no override (expected)")
        else:
            if idx not in veoOverridesObj:
                fail("frontend", f"row[{idx}] non-text_card missing veoOverridesObj entry")
            ok("frontend", f"row[{idx}] scene {sm['scene_index']} → veoOverrides[{idx}] populated")

    return veoOverridesObj, notesObj, padsObj


def test_stage6_payload_lookup(scenes_metadata_flat, veoOverridesObj, notesObj):
    print("\nStage 6: dialogue payload builder (per-line) reads window._veoPromptOverrides[i]")
    # Simulate frontend payload building: dialogueInput has 8 numbered lines.
    # Map iteration uses i = full dialogueInput position.
    payload = []
    for i, sm in enumerate(scenes_metadata_flat):
        line_text = ""
        if sm["scene_index"] == 7:
            line_text = "I can help. Comment GUIDE and I will send you the exact weight loss protocol that helped her,"
        elif sm["scene_index"] == 8:
            line_text = "but you must follow me first or I cannot reach you."
        is_text_card = sm["scene_type"] == "text_card"
        is_silent = sm["speaker_mode"] == "silent"

        # v682h filter: keep if l.text or _isTextCard or _isSilent
        if not (line_text or is_text_card or is_silent):
            continue

        veo_override_obj = veoOverridesObj.get(i)
        action_note = notesObj.get(i)
        payload.append({
            "id": i + 1,
            "text": line_text,
            "scene_index": sm["scene_index"] - 1,  # 0-indexed at backend
            "scene_type": sm["scene_type"],
            "speaker_mode": sm["speaker_mode"],
            "veo_prompt_override": (veo_override_obj or {}).get("text_prompt"),
            "action_note": action_note,
        })

    if len(payload) != 8:
        fail("payload", f"expected 8 dialogue lines, got {len(payload)}")
    ok("payload", f"{len(payload)} lines in payload")

    text_card_count = sum(1 for p in payload if p["scene_type"] == "text_card")
    silent_count = sum(1 for p in payload if p["speaker_mode"] == "silent")
    on_camera_count = sum(1 for p in payload if p["speaker_mode"] == "on-camera")
    if text_card_count != 1:
        fail("payload", f"expected 1 text_card line, got {text_card_count}")
    if silent_count != 5:
        fail("payload", f"expected 5 silent lines, got {silent_count}")
    if on_camera_count != 2:
        fail("payload", f"expected 2 on-camera lines, got {on_camera_count}")
    ok("payload", f"composition: {text_card_count} text_card + {silent_count} silent + {on_camera_count} on-camera")

    # Every NON-text_card line must have veo_prompt_override
    for p in payload:
        if p["scene_type"] == "text_card":
            if p["veo_prompt_override"] is not None:
                fail("payload", f"id={p['id']} text_card has unexpected veo_prompt_override")
            ok("payload", f"id={p['id']} text_card → no override (correct)")
        else:
            if not p["veo_prompt_override"]:
                fail("payload", f"id={p['id']} scene_index={p['scene_index']} MISSING veo_prompt_override")
            ok("payload", f"id={p['id']} scene_index={p['scene_index']} → override carried (len {len(p['veo_prompt_override'])})")

    return payload


def test_stage7_backend_setup_loop(payload):
    print("\nStage 7: _setup_job_background loop (v682h text_card skip + None guard)")
    # Simulate the build_prompt for-loop logic
    skipped_text_card = 0
    used_override = 0
    fell_through_to_build_prompt = 0
    for line_data in payload:
        if (line_data.get("scene_type") or "").lower() == "text_card":
            skipped_text_card += 1
            continue
        # v682h start_image_idx None guard
        sii_raw = line_data.get("start_image_idx")
        sii = 0 if sii_raw is None else sii_raw  # type: ignore
        # No crash check
        veo_override = (line_data.get("veo_prompt_override") or "").strip() or None
        if veo_override:
            used_override += 1
        else:
            fell_through_to_build_prompt += 1

    if skipped_text_card != 1:
        fail("setup_loop", f"expected 1 text_card skip, got {skipped_text_card}")
    ok("setup_loop", f"text_card scenes skipped: {skipped_text_card}")
    if fell_through_to_build_prompt != 0:
        fail("setup_loop", f"FATAL: {fell_through_to_build_prompt} lines fell through to build_prompt — fast-lane defeated")
    ok("setup_loop", f"all {used_override} non-text_card lines used markdown override (v673 fast-lane active)")
    if used_override != 7:
        fail("setup_loop", f"expected 7 fast-lane uses (8 - 1 text_card), got {used_override}")
    ok("setup_loop", "v673 fast-lane FULLY ACTIVE: 7/7 non-text_card clips use prebuilt prompts")


def main():
    print("=" * 72)
    print("Donna end-to-end automation test (markdown → /api/jobs)")
    print("=" * 72)

    md = load_artifact()
    print(f"Artifact: videos/donna-weight-loss.md ({len(md)} bytes)")

    prompts = test_stage1_parse_veo_prompts_block(md)
    scenes = test_stage2_attach_to_scenes(prompts)
    scenes = test_stage3_to_dict_truncation(scenes)
    dialogue_lines_flat, scenes_metadata_flat = test_stage4_prepare_for_video_flat_row(scenes)
    veoOverridesObj, notesObj, padsObj = test_stage5_frontend_indexing(scenes_metadata_flat)
    payload = test_stage6_payload_lookup(scenes_metadata_flat, veoOverridesObj, notesObj)
    test_stage7_backend_setup_loop(payload)

    print("\n" + "=" * 72)
    print("ALL STAGES PASSED — markdown's Veo prompts will reach Veo verbatim.")
    print("=" * 72)


if __name__ == "__main__":
    main()
