"""v892.12 — the from-batch payload builder, on the shape that broke.

No database, no network. It builds a CreateJobRequest body out of the reference
`prepare-for-video` response (fixtures/prepare_response_garnissa_v4.json: 9
spoken scenes, 15 silent cutaways) and asserts the three facts the 2026-09-03
job got wrong, plus the two ways the builder itself could go wrong:

  * clip_role reaches every cutaway (the job wrote NULL on all 24)
  * audio_from_scene reaches them unconverted (a "helpful" -1 is a wrong video)
  * scene_index is the scene's POSITION, not the author's 1-based number
  * a silent row is never dropped (the pre-v682f bug: only spoken lines became
    clips), and a length mismatch is refused instead of guessed at
  * the module enumerates NO field names of its own — not config keys, not line
    fields. Every list it uses is passed in from the pydantic models, because a
    hand-written list is what produced v892.2, v892.5 and v892.8.
"""
import json
import os
import re
import sys

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from promote_from_prepare import (  # noqa: E402
    build_create_job_request,
    missing_config_keys,
)

import main  # noqa: E402  (for the two model field sets — no server is started)

ALLOWED = set(main.DialogueLineInput.model_fields)
REQUIRED_CONFIG = set(main.VideoConfigInput.model_fields)

failures = []


def check(ok, why):
    if not ok:
        failures.append(why)


with open(os.path.join(HERE, "fixtures", "prepare_response_garnissa_v4.json"),
          encoding="utf-8") as fh:
    PREPARED = json.load(fh)

# The complete config, GENERATED from the model rather than typed out, so a
# new VideoConfigInput field cannot silently pass this test.
CONFIG = {k: f.default for k, f in main.VideoConfigInput.model_fields.items()}
CONFIG["storyboard_mode"] = True

body = build_create_job_request(PREPARED, CONFIG, "batch-ref", allowed=ALLOWED)
lines = body["dialogue_lines"]

# --- 1. the shape ----------------------------------------------------------
check(len(lines) == 24, f"expected 24 dialogue lines, got {len(lines)}")
check([ln["scene_index"] for ln in lines] == list(range(24)),
      "scene_index must be 0..23 — the POSITION of each scene, not the "
      "author's 1-based number")
check([ln["id"] for ln in lines] == list(range(1, 25)),
      "line ids must be 1..24")
check([ln["text"] for ln in lines] == PREPARED["dialogue_lines"],
      "the line text must be prepare's dialogue_lines, in order")

# --- 2. the three facts the failing job dropped ----------------------------
roles = [ln.get("clip_role") for ln in lines]
check(roles == [None] * 9 + ["visual_pair"] * 15,
      f"clip_role must be None x9 then visual_pair x15, got {roles}")

afs = [ln.get("audio_from_scene") for ln in lines if ln.get("audio_from_scene")
       is not None]
check(afs == [8, 2, 3, 3, 3, 4, 4, 4, 5, 5, 7, 7, 7, 9, 9],
      f"audio_from_scene must pass through unconverted, got {afs}")
check(all(ln.get("audio_from_scene") is None for ln in lines[:9]),
      "a spoken line must not carry audio_from_scene")

# --- 3. images and scenes --------------------------------------------------
check([ln["start_image_idx"] for ln in lines[:9]] == [15] * 9,
      "the nine spoken lines all reuse image 15 in the reference build")
check([ln["start_image_idx"] for ln in lines[9:]] == list(range(15)),
      "each cutaway takes its own image, by uploaded[local]['index']")

scenes = body["scenes"]
check(len(scenes) == 24, f"expected 24 scenes, got {len(scenes)}")
check([s["sceneIndex"] for s in scenes] == list(range(24)),
      "sceneIndex must be 0..23")
check(all(s["clips"] == [s["sceneIndex"]] for s in scenes),
      "each reference scene owns exactly its own single clip")
check(scenes[0]["transition"] is None,
      "the first scene must carry no transition (the browser sends none)")
check(lines[0].get("scene_transition") is None,
      "the first line must carry no scene_transition")
check(lines[1].get("scene_transition") == "cut",
      "a later scene's first line carries the scene's transition, renamed from "
      "the prepared row's `transition`")

# --- 4. the rest of the CreateJobRequest envelope --------------------------
check(body["job_id"] == PREPARED["upload_job_id"],
      "job_id must be prepare's upload_job_id")
check(body["image_batch_id"] == "batch-ref", "image_batch_id must be the batch")
check(body["last_frame_index"] is None, "last_frame_index must be None (v827)")
check(body["api_keys"] == {"gemini_keys": [], "openai_key": None},
      "api_keys must be the empty server-side shape")
check(body["config"] is CONFIG, "the config must be forwarded untouched")

# --- 5. it really validates as a CreateJobRequest --------------------------
parsed = main.CreateJobRequest(**body)
check(len(parsed.dialogue_lines) == 24, "pydantic dropped lines")
check(parsed.dialogue_lines[9].clip_role == "visual_pair",
      "pydantic dropped clip_role — the v892.2 failure mode")
check(parsed.dialogue_lines[9].audio_from_scene == 8,
      "pydantic dropped audio_from_scene")
check(parsed.dialogue_lines[0].target_duration_s == 8.0,
      "pydantic dropped the authored duration")

# --- 6. text_card: a row with no image ------------------------------------
tc = {
    "upload_job_id": "u-tc",
    "uploaded": [{"filename": "a.png", "index": 0}],
    "scene_assignments": [
        {"scene_index": 1, "image_local_index": 0, "clip_mode": "fresh",
         "transition": None, "scene_type": None},
        {"scene_index": 2, "image_local_index": None, "clip_mode": "fresh",
         "transition": "cut", "scene_type": "text_card"},
    ],
    "dialogue_lines": ["spoken", ""],
    "scenes_metadata": [
        {"scene_index": 1, "image_local_index": 0, "clip_mode": "fresh",
         "transition": None, "clip_role": None, "audio_from_scene": None},
        {"scene_index": 2, "image_local_index": None, "clip_mode": "fresh",
         "transition": "cut", "scene_type": "text_card", "caption": "hello",
         "bg_color": "#000", "duration_s": 1.5},
    ],
}
tc_body = build_create_job_request(tc, CONFIG, "b-tc", allowed=ALLOWED)
check(tc_body["dialogue_lines"][1]["start_image_idx"] is None,
      "a text_card line must carry start_image_idx=None, not a guessed index")
check(tc_body["scenes"][1]["imageIndex"] is None,
      "a text_card scene must carry imageIndex=None")
check(tc_body["scenes"][1]["scene_type"] == "text_card",
      "scene_type must survive onto the scene entry")
check(tc_body["dialogue_lines"][1]["text"] == "",
      "an empty line is KEPT — dropping it is the pre-v682f bug")

# --- 7. non-contiguous scene numbers still map by POSITION ----------------
gap = {
    "upload_job_id": "u-gap",
    "uploaded": [{"filename": "a.png", "index": 0}],
    "scene_assignments": [
        {"scene_index": 1, "image_local_index": 0, "clip_mode": "fresh",
         "transition": None},
        {"scene_index": 2, "image_local_index": 0, "clip_mode": "fresh",
         "transition": "cut"},
        {"scene_index": 4, "image_local_index": 0, "clip_mode": "fresh",
         "transition": "cut"},
    ],
    "dialogue_lines": ["a", "b", "c"],
    "scenes_metadata": [
        {"scene_index": 1, "image_local_index": 0},
        {"scene_index": 2, "image_local_index": 0},
        {"scene_index": 4, "image_local_index": 0},
    ],
}
gap_body = build_create_job_request(gap, CONFIG, "b-gap", allowed=ALLOWED)
check([ln["scene_index"] for ln in gap_body["dialogue_lines"]] == [0, 1, 2],
      "scene 4 sits at POSITION 2 — `scene_index - 1` would say 3 and every "
      "cutaway pointing at it would land on the wrong clip")


def expect_value_error(fn, needle):
    try:
        fn()
    except ValueError as exc:
        if needle not in str(exc):
            failures.append(f"wrong ValueError: {exc}")
        return
    failures.append(f"expected a ValueError containing {needle!r}")


# --- 8. refusals -----------------------------------------------------------
short = dict(PREPARED)
short["dialogue_lines"] = PREPARED["dialogue_lines"][:23]
expect_value_error(
    lambda: build_create_job_request(short, CONFIG, "b", allowed=ALLOWED),
    "parallel by contract")

stray = json.loads(json.dumps(PREPARED))
stray["scenes_metadata"][0]["scene_index"] = 99
expect_value_error(
    lambda: build_create_job_request(stray, CONFIG, "b", allowed=ALLOWED),
    "which is not one of")

# --- 9. config completeness is computed, never enumerated -----------------
check(sorted(missing_config_keys({}, REQUIRED_CONFIG)) == sorted(REQUIRED_CONFIG),
      "an empty config must be reported as missing EVERY key — every one of "
      "them has a default, so a half config silently becomes a full one")
check(missing_config_keys(CONFIG, REQUIRED_CONFIG) == [],
      "a config built from VideoConfigInput.model_fields must be complete")
partial = {k: "x" for k in list(REQUIRED_CONFIG)[:3]}
check(len(missing_config_keys(partial, REQUIRED_CONFIG)) == len(REQUIRED_CONFIG) - 3,
      "a partial config must name exactly the keys it lacks")

# --- 10. source guards -----------------------------------------------------
src = open(os.path.join(ROOT, "promote_from_prepare.py"), encoding="utf-8").read()
check("dialogue_list.append" not in src,
      "promote_from_prepare.py contains the literal `dialogue_list.append` — "
      "check_field_plumbing CHECK 2 brace-walks the FIRST match of that string "
      "in image_platform.py, and a second one anywhere is a trap waiting for "
      "whoever moves this code")
for key in sorted(REQUIRED_CONFIG):
    if re.search(r'["\']' + re.escape(key) + r'["\']', src):
        failures.append(
            f"promote_from_prepare.py names the config key {key!r} — this "
            f"module must hold NO config key list; the server computes the "
            f"required set from VideoConfigInput.model_fields")

if failures:
    print("FAIL")
    for f in failures:
        print("  -", f)
    sys.exit(1)
print("PASS — 24-line reference payload: positions 0..23, roles None x9 + "
      "visual_pair x15, audio_from_scene [8,2,3,3,3,4,4,4,5,5,7,7,7,9,9] "
      "unconverted, 24 scenes, text_card kept with no image, scene gaps map by "
      "position, length mismatch refused, config completeness computed from "
      "the model, no enumerated key list in the module.")
