"""Profile mechanism tests for v589 Stage 4d (fbads-video vs default ugc-reel).

One decode engine reads two formats. The load-bearing invariant is that the
default "ugc-reel" lane stays byte-identical to the pre-profile behavior — the
Korella production lane runs on it — while "fbads-video" adds a required
top-level ad_read block plus its own context/task text.

Run: python -m pytest test_v589_profiles.py -q
"""
import importlib.util
import inspect
import json
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "v589", Path(__file__).parent / "v589_video_understanding.py")
v589 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(v589)


def test_default_profile_is_the_declared_default():
    default = inspect.signature(v589.build_user_prompt).parameters["profile"].default
    assert default == "ugc-reel"
    assert v589.READING_PROFILES[default]["ad_read"] is False
    assert v589.READING_PROFILES["fbads-video"]["ad_read"] is True


def test_schema_default_profile_is_unchanged():
    assert v589.schema_for_profile("ugc-reel") == v589.STAGE4D_JSON_SCHEMA
    # and it is a copy, not the global itself
    assert v589.schema_for_profile("ugc-reel") is not v589.STAGE4D_JSON_SCHEMA
    # including its shot contract and version, which now come from the profile
    assert (v589.schema_for_profile("ugc-reel")["properties"]["shots"]["items"]
            == v589.PER_SHOT_SCHEMA)
    assert (v589.schema_for_profile("ugc-reel")["properties"]["schema_version"]["enum"]
            == [v589.STAGE4D_SCHEMA_VERSION] == ["stage4d.v2"])


def test_profiles_declare_their_own_shot_schema_and_version():
    """The profile owns the contract; nothing downstream hardcodes stage4d.v2."""
    ugc = v589.READING_PROFILES["ugc-reel"]
    fb = v589.READING_PROFILES["fbads-video"]
    assert ugc["shot_schema"] is v589.PER_SHOT_SCHEMA
    assert ugc["schema_version"] == v589.STAGE4D_SCHEMA_VERSION == "stage4d.v2"
    assert fb["shot_schema"] is v589.LEAN_AD_SHOT_SCHEMA
    assert fb["schema_version"] == "adread.v1"
    # a lean artifact must never be mistakable for a full stage4d.v2 one
    assert fb["schema_version"] != ugc["schema_version"]


LEAN_FIELDS = [
    "shot_index", "start", "end", "shot_type", "action", "camera_move",
    "end_state", "on_screen_text", "sell_function",
]


def test_lean_shot_schema_is_flat_and_open_ended():
    lean = v589.LEAN_AD_SHOT_SCHEMA
    assert lean["required"] == LEAN_FIELDS
    # ORDER is part of the contract: the model emits fields in schema order and
    # we read the artifact as a table, so timestamps -> what kind -> what
    # happens -> how the camera moves -> where it lands -> text -> sell job.
    assert list(lean["properties"]) == LEAN_FIELDS
    assert set(lean["properties"]) == set(lean["required"])
    assert lean["additionalProperties"] is False
    assert lean["type"] == "object"
    assert lean["properties"]["shot_index"] == {"type": "integer", "minimum": 1}
    assert lean["properties"]["start"]["type"] == "number"
    assert lean["properties"]["end"]["type"] == "number"
    # DELIBERATE DESIGN RULE (root CLAUDE.md 3.6): creative lists are open
    # examples, never closed menus. sell_function is a plain string so a beat
    # that does something unlisted can be NAMED, not forced into a bucket.
    sell = lean["properties"]["sell_function"]
    assert sell["type"] == "string"
    assert "enum" not in sell
    assert "const" not in sell
    # every lean field is a flat scalar: no nesting, that is the whole point
    for name, prop in lean["properties"].items():
        assert prop["type"] in ("string", "number", "integer"), name
    # and the description says the list is open
    assert "not a closed list" in sell["description"].lower()


def test_lean_schema_no_longer_describes_appearance():
    """A separate per-frame pass reads the start frame at native resolution and
    owns appearance — measurably better at it (it caught a peacock-feather
    print and 'wire-rimmed glasses, blue jeans, beige apron' this pass missed).
    Paying the video pass to describe the same thing is the waste being cut.

    Assert ABSENCE, not just the new shape: a stale field left in the schema
    still ships to the model and still bills output tokens for every shot.
    """
    lean = v589.LEAN_AD_SHOT_SCHEMA
    assert len(lean["required"]) == 9
    for gone in ("visual", "who_on_camera", "production_method"):
        assert gone not in lean["properties"], gone
        assert gone not in lean["required"], gone
    # scoped to the PER-SHOT items on purpose: production_method survives at
    # AD level (one line for the whole creative) and that is the point — what
    # must never come back is the per-shot copy that cost 742 tokens to repeat
    # one boilerplate sentence eighty times.
    items = json.dumps(v589.schema_for_profile("fbads-video")
                       ["properties"]["shots"]["items"])
    for gone in ("who_on_camera", "production_method", '"visual"'):
        assert gone not in items, gone


def test_lean_shot_schema_records_the_action_in_order():
    """Operator: "the actions taking place in each clip are very important".
    A clip holds several beats; a one-verb summary is the named failure mode.
    """
    lean = v589.LEAN_AD_SHOT_SCHEMA
    assert "action" in lean["required"]
    act = lean["properties"]["action"]
    assert act["type"] == "string"
    assert "enum" not in act and "const" not in act
    desc = act["description"].lower()
    assert "in order" in desc
    assert "then" in desc            # the connector that forces the sequence
    assert "one-verb" in desc        # the failure mode is named, not implied
    assert "failure" in desc
    assert "held still" in desc      # the declared answer when nothing moves


def test_lean_shot_schema_records_the_camera_move():
    lean = v589.LEAN_AD_SHOT_SCHEMA
    assert "camera_move" in lean["required"]
    cam = lean["properties"]["camera_move"]
    assert cam["type"] == "string"
    assert "enum" not in cam and "const" not in cam
    desc = cam["description"].lower()
    for example in ("static", "handheld", "pan", "push in", "pull out",
                    "whip", "tilt"):
        assert example in desc, example
    assert "fast" in desc            # speed is part of the ask
    assert "not a closed list" in desc


def test_lean_shot_schema_records_the_end_state():
    """The end frame an animator interpolates TOWARD. A STATE, not a replay."""
    lean = v589.LEAN_AD_SHOT_SCHEMA
    assert "end_state" in lean["required"]
    end = lean["properties"]["end_state"]
    assert end["type"] == "string"
    assert "enum" not in end and "const" not in end
    desc = end["description"].lower()
    assert "state" in desc
    assert "not a repeat of the action" in desc
    for token in ("lands", "start"):
        assert token in desc, token


def test_on_screen_text_states_why_it_is_not_the_frame_pass_job():
    """It survives the cut on a SCOPE argument, not a habit one: burned-in
    captions change DURING a clip, so a still start-frame read cannot see them.
    """
    desc = (v589.LEAN_AD_SHOT_SCHEMA["properties"]["on_screen_text"]
            ["description"].lower())
    assert "verbatim" in desc
    assert "'none'" in desc
    assert "change" in desc          # the scope argument is written down
    assert "start frame" in desc or "start-frame" in desc


def test_lean_shot_schema_records_shot_type():
    """Operator's ask, in their words: "what's on screen (avatar talking,
    brolls, etc.)". Spread through prose, counting an ad's shot mix meant
    regexing it back out. A dedicated label field answers it at a glance.
    """
    lean = v589.LEAN_AD_SHOT_SCHEMA
    assert len(lean["required"]) == 9
    assert "shot_type" in lean["required"]
    assert lean["additionalProperties"] is False
    st = lean["properties"]["shot_type"]
    # 3.6: open string. A competitor ships shot kinds no enum anticipated.
    assert st["type"] == "string"
    assert "enum" not in st
    assert "const" not in st
    desc = st["description"].lower()
    for example in ("talking-head", "b-roll", "demo", "product shot",
                    "screen recording", "text card", "social proof"):
        assert example in desc, example
    assert "not a closed list" in desc
    # it is the CATEGORY, a label — not a second description
    assert "category" in desc
    assert "label, not a sentence" in desc
    # and it must stay terse, because its value is being countable at a glance
    assert "count" in desc


def test_schema_fbads_uses_the_lean_shot_schema():
    schema = v589.schema_for_profile("fbads-video")
    assert schema["properties"]["shots"]["items"] == v589.LEAN_AD_SHOT_SCHEMA
    assert schema["properties"]["shots"]["items"] is not v589.LEAN_AD_SHOT_SCHEMA
    assert schema["properties"]["schema_version"]["enum"] == ["adread.v1"]
    # the heavy per-shot contract must be nowhere in it
    blob = json.dumps(schema)
    for heavy in ("forensic_perception", "start_frame_spec", "frame_inventory",
                  "veo_reproduction_hints", "motion_cross_check", "action_arc"):
        assert heavy not in blob, heavy
    # and the base globals stay untouched
    assert v589.STAGE4D_JSON_SCHEMA["properties"]["shots"]["items"] is v589.PER_SHOT_SCHEMA


def test_ad_read_owns_production_method_at_ad_level():
    """The one thing the per-frame pass structurally cannot recover.

    Dropping per-shot production_method was right — 742 tokens to repeat one
    boilerplate sentence eighty times. But the frame pass reads a STILL, and
    the strongest AI tells are TEMPORAL: detail that drifts between frames,
    flicker, morphing background geometry, hands or faces that change across a
    cut. None of those exist in one frame. Only the video pass can see them,
    and "is this competitor shooting real UGC or generating it?" decides
    whether we can copy a creative at all. So it lives once, for the whole ad.
    """
    ad = v589.AD_READ_SCHEMA
    assert len(ad["required"]) == 10
    assert "production_method" in ad["required"]
    assert "production_method" in ad["properties"]
    assert ad["additionalProperties"] is False
    pm = ad["properties"]["production_method"]
    # 3.6: open string, never an enum of blessed labels
    assert pm["type"] == "string"
    assert "enum" not in pm and "const" not in pm
    desc = " ".join(pm["description"].lower().split())
    # the TELL is the ask, as everywhere else in this profile
    assert "tell" in desc
    # the TEMPORAL tells specifically — the ones a still cannot show
    for temporal in ("between frames", "flicker", "morph", "across"):
        assert temporal in desc, temporal
    assert "hands" in desc and "faces" in desc
    assert "still" in desc          # says why these are the video pass's job
    # a MIXED ad must not be forced into one label
    assert "mixed" in desc
    assert "which shots" in desc
    # and the common kinds are named as examples, not a closed set
    for example in ("ugc", "ai-generated", "stock", "screen recording"):
        assert example in desc, example
    assert "not a closed list" in desc


def test_ad_read_missing_production_method_is_rejected():
    data = _minimal_valid_lean()
    del data["ad_read"]["production_method"]
    with pytest.raises(v589.Stage4dValidationError,
                       match="ad_read.production_method"):
        v589.validate_stage4d_output(data, _lean_expected_shots(),
                                     profile="fbads-video")
    # blank is not an observation either
    data = _minimal_valid_lean()
    data["ad_read"]["production_method"] = "   "
    with pytest.raises(v589.Stage4dValidationError,
                       match="ad_read.production_method"):
        v589.validate_stage4d_output(data, _lean_expected_shots(),
                                     profile="fbads-video")


def test_fbads_task_asks_for_the_ad_level_production_method():
    """The schema description alone is not the brief; the task text must ask."""
    shots = [{"shot": 1, "start": 0.0, "end": 3.0}]
    task = v589.build_user_prompt(shots, "t", [], profile="fbads-video").split("<task>")[1]
    low = " ".join(task.lower().split())
    assert "how the ad as a whole was made" in low
    assert "between frames" in low
    assert "mixed" in low


def test_schema_fbads_adds_required_ad_read():
    schema = v589.schema_for_profile("fbads-video")
    assert "ad_read" in schema["properties"]
    assert "ad_read" in schema["required"]
    # base global must NOT have been mutated
    assert "ad_read" not in v589.STAGE4D_JSON_SCHEMA["properties"]
    # the attached block is a copy too — SDKs mutate response schemas in place
    assert schema["properties"]["ad_read"] is not v589.AD_READ_SCHEMA
    ad = schema["properties"]["ad_read"]
    for field in ("offer", "cta", "overlay_text_timeline", "captions",
                  "sound_off_comprehension", "aspect_ratio", "safe_zones",
                  "end_card", "brand_assets", "production_method"):
        assert field in ad["properties"], field
        assert field in ad["required"], field


def test_prompt_carries_fbads_context_only_for_fbads():
    shots = [{"shot": 1, "start": 0.0, "end": 3.0}]
    default_prompt = v589.build_user_prompt(shots, "t", [], profile="ugc-reel")
    fb_prompt = v589.build_user_prompt(shots, "t", [], profile="fbads-video")
    assert "PAID" in fb_prompt and "ad_read" in fb_prompt
    assert "PAID" not in default_prompt
    # the format declaration is DATA (<context>); the read job is an INSTRUCTION
    assert fb_prompt.index("PAID") < fb_prompt.index("<task>")
    assert "fill the top-level ad_read object" in fb_prompt.split("<task>")[1]
    # the caller's own extra_context still survives the profile prefix
    assert "TIMELINE-SENTINEL" in v589.build_user_prompt(
        shots, "t", [], extra_context="TIMELINE-SENTINEL", profile="fbads-video")
    # existing behavior preserved: no-profile call == ugc-reel call
    assert v589.build_user_prompt(shots, "t", []) == default_prompt


def test_fbads_task_declares_the_structural_per_shot_job():
    """The lean read is what makes the ad affordable; the prompt must say so.

    Output tokens are 81% of a real 140s ad decode. Two levers live here: the
    per-shot job is STRUCTURAL (no frame-recreation detail), and whisper
    already transcribed the dialogue locally and for free, so the model must
    not spend output tokens re-typing speech into the shots.
    """
    shots = [{"shot": 1, "start": 0.0, "end": 3.0}]
    task = v589.build_user_prompt(shots, "t", [], profile="fbads-video").split("<task>")[1]
    low = task.lower()
    assert "sell" in low
    assert "verbatim" in low                     # on-screen text is quoted exactly
    assert "not a closed list" in low            # 3.6: open example set
    # whisper is authoritative; do NOT re-transcribe speech into the shots
    assert "transcript" in low
    assert "re-transcribe" in low
    # the ad layer survives
    assert "fill the top-level ad_read object" in task


def test_fbads_instruction_survives_schema_suppression():
    """The gemini lane suppresses the schema block; the profile text must still land."""
    shots = [{"shot": 1, "start": 0.0, "end": 3.0}]
    p = v589.build_user_prompt(shots, "t", [], include_schema=False,
                               profile="fbads-video")
    assert "PAID" in p
    assert "fill the top-level ad_read object" in p
    assert "Stage 4d JSON schema:" not in p


# ── FIX 3: the profile owns its system instruction ────────────────────
# The shared SYSTEM_INSTRUCTION is written for the heavy lane (RE-RENDER
# precision + the forensic-perception protocol). The fbads task text used to
# countermand it inline, which is a prompt arguing with itself: verbosity
# creeps back into `visual` at premium output rates and the model is pulled
# toward a job the lean schema does not have.


def test_each_profile_owns_a_system_instruction():
    ugc = v589.READING_PROFILES["ugc-reel"]["system_instruction"]
    fb = v589.READING_PROFILES["fbads-video"]["system_instruction"]
    # the default lane keeps the exact module-level object, not a copy of it
    assert ugc is v589.SYSTEM_INSTRUCTION
    assert fb is v589.FBADS_SYSTEM_INSTRUCTION
    assert fb != ugc


def test_fbads_system_instruction_is_written_for_the_lean_job():
    fb = v589.READING_PROFILES["fbads-video"]["system_instruction"]
    low = fb.lower()
    # the actual job
    assert "ad" in low and "sell" in low
    # whisper is supplied and authoritative; speech is NOT re-typed into shots
    assert "transcript" in low
    assert "authoritative" in low
    assert "re-transcribe" in low
    # observation, with the tell named, never inference
    assert "visible" in low
    assert "tell" in low
    # verbatim on-screen text is the mining payload
    assert "exactly" in low
    # one sentence per field, no padding
    assert "one clear sentence per field" in low
    assert "padding" in low
    # and NONE of the heavy lane's re-render / forensic language survives:
    # nothing to countermand means nothing to argue with.
    for heavy in ("forensic", "re-render", "veo", "state-evolution"):
        assert heavy not in low, heavy


def test_fbads_brief_hands_the_still_frame_to_the_other_pass():
    """The division of labour, stated in the prompt.

    A separate per-frame pass reads each clip's start frame at native
    resolution for $0.006/frame and owns composition and appearance. So the
    VIDEO pass must stop describing appearance: it pays premium output rate
    for a worse answer. Its job is what MOVES, in order, so an animator can
    drive a generated start frame through the same motion.
    """
    fb = v589.READING_PROFILES["fbads-video"]["system_instruction"]
    # collapse whitespace: these are prose assertions and must not be hostage
    # to where a line happens to wrap in the source literal.
    low = " ".join((fb + v589.FBADS_READ_INSTRUCTIONS).lower().split())
    # the handoff is stated outright, not left to be inferred
    assert "separate pass" in low
    assert "still" in low
    assert "if a still photograph could show it" in low
    # the positive job is motion
    for token in ("what moves", "in order", "camera", "animator"):
        assert token in low, token
    # the one-verb failure mode is named where the model will read it
    assert "one-verb" in low
    # the OLD positive orders to describe appearance are gone from both texts
    for gone in ("one sentence of what is on screen", "who is visible",
                 "how the shot was made", "shot scale", "close-up / medium"):
        assert gone not in low, gone
    # and the discipline that must SURVIVE the rewrite
    assert "transcript" in low and "authoritative" in low
    assert "re-transcribe" in low
    assert "tell" in low              # name the tell, never infer
    assert "unreadable" in low or "unclear" in low


def test_fbads_prompt_no_longer_countermands_the_forensic_pass():
    """The countermand moved into the fbads system instruction as a positive
    statement of the job, so the task text must not carry it any more."""
    task = v589.FBADS_READ_INSTRUCTIONS
    assert "skip the forensic-perception pass" not in task
    assert "forensic" not in task.lower()
    # what is still load-bearing there must NOT have gone with it
    assert "SELL STRUCTURE" in task
    assert "VERBATIM" in task
    assert "re-transcribe" in task
    assert "fill the top-level ad_read object" in task


def test_task_preamble_is_profile_owned():
    """Same defect class as the final_instruction: the shared <task> preamble
    ordered extra attention to ACTION ARCS and verbs-of-state-change, and the
    lean schema has no action_arc and no state-evolution field at all."""
    ugc = v589.READING_PROFILES["ugc-reel"]["task_preamble"]
    fb = v589.READING_PROFILES["fbads-video"]["task_preamble"]
    # the default lane's preamble is byte-for-byte what it has always been
    assert ugc == v589.DEFAULT_TASK_PREAMBLE
    assert ugc == (
        "Produce one JSON OBJECT matching the Stage 4d schema. "
        "Cover every shot in order. Use the EXACT shot start/end timestamps "
        "provided. Pay extra attention to action arcs in shots whose dialogue "
        "contains a verb-of-state-change (pour, squeeze, add, stir, mix, melt).\n"
    )
    assert fb is v589.FBADS_TASK_PREAMBLE
    assert fb != ugc
    # the heavy-lane orders are gone from the lean one
    low = fb.lower()
    assert "action arc" not in low
    assert "verb-of-state-change" not in low
    # what is genuinely shared applies to BOTH and must survive
    for shared in ("Cover every shot in order",
                   "Use the EXACT shot start/end timestamps"):
        assert shared in ugc, shared
        assert shared in fb, shared
    assert "JSON OBJECT" in fb


def test_no_heavy_lane_order_reaches_the_fbads_request_at_all():
    """The whole fbads request — system instruction, context, task preamble,
    task and the closing final_instruction — must never order work against
    fields this profile does not have. The default lane keeps every order."""
    shots = [{"shot": 1, "start": 0.0, "end": 3.0}]
    fb = (v589.READING_PROFILES["fbads-video"]["system_instruction"]
          + v589.build_user_prompt(shots, "t", [], profile="fbads-video"))
    low = fb.lower()
    for absent in ("forensic", "action arc", "verb-of-state-change"):
        assert absent not in low, absent
    default = v589.build_user_prompt(shots, "t", [], profile="ugc-reel")
    assert "Run the forensic-perception protocol on each shot" in default
    assert "Pay extra attention to action arcs" in default


def test_lmstudio_sends_the_profile_system_instruction(monkeypatch, tmp_path):
    """Behavioral pin, no network: the request body must carry the PROFILE's
    system instruction, not the module-level heavy-lane constant."""
    import urllib.request

    sent: list[dict] = []

    class _Resp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return json.dumps(
                {"choices": [{"message": {"content": "{}"}}]}).encode()

    def _fake_urlopen(req, timeout=None):
        sent.append(json.loads(req.data.decode()))
        return _Resp()

    monkeypatch.setattr(v589, "lmstudio_available", lambda base_url: (True, "m"))
    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen)

    def _first_text(body):
        return body["messages"][0]["content"][0]["text"]

    v589.call_lmstudio(tmp_path / "v.mp4", [], [], "t", [])
    assert _first_text(sent[-1]) is not None
    assert _first_text(sent[-1]) == v589.SYSTEM_INSTRUCTION

    v589.call_lmstudio(tmp_path / "v.mp4", [], [], "t", [], profile="fbads-video")
    assert _first_text(sent[-1]) == v589.FBADS_SYSTEM_INSTRUCTION
    assert "forensic" not in _first_text(sent[-1]).lower()


def test_unknown_profile_names_the_valid_choices():
    with pytest.raises(ValueError, match="unknown reading profile"):
        v589.schema_for_profile("fbads")


def _minimal_valid_stage4d():
    """Load a real passing artifact as the base fixture."""
    p = Path(__file__).parent.parent / "raw/decode_work/DbjOnA3B8o7/stage4d_vlm.json"
    return json.loads(p.read_text(encoding="utf-8"))


def _expected_shots_for(data):
    return [{"shot": s["shot_index"], "start": s["start"], "end": s["end"]}
            for s in data["shots"]]


def _ad_read_block():
    return {
        "offer": "20% off first order", "cta": "Shop Now",
        "overlay_text_timeline": [{"time": "00:01", "end": "00:04",
                                   "text": "20% OFF",
                                   "style": "bold white center"}],
        "captions": "burned, bottom-center, word-accurate",
        "sound_off_comprehension": "muted viewer sees product + 20% OFF overlay",
        "aspect_ratio": "9:16",
        "safe_zones": "CTA and face inside center-safe area",
        "end_card": "logo + Shop Now button",
        "brand_assets": "logo at 00:00 and 00:14",
        "production_method": "mixed - the talking-head shots are real UGC on a "
                             "phone, but shots 6-9 are AI-generated: the "
                             "counter tiles drift between frames and her left "
                             "hand gains a finger across the cut at 00:07",
    }


def _minimal_valid_lean():
    """A complete adread.v1 artifact, built by hand — this is the whole point.

    Two shots so the per-shot loop actually iterates. Nothing nested: if this
    fixture ever needs a sub-block, the lean schema stopped being lean.
    """
    return {
        "schema_version": "adread.v1",
        "observed_people": [
            {"label": "person_1", "identity_markers": "woman, 60s, grey bob",
             "wardrobe": "cream cardigan", "shots_present": [1, 2]},
        ],
        "ad_read": _ad_read_block(),
        "shots": [
            {"shot_index": 1, "start": 0.0, "end": 2.5,
             "shot_type": "talking-head",
             "action": "she rubs her right wrist with her left thumb, then "
                       "winces and looks down at it, then lifts her eyes back "
                       "to the lens and shakes her head",
             "camera_move": "static, locked off",
             "end_state": "her hands are back in her lap and she is looking "
                          "straight at the lens, jaw set",
             "on_screen_text": "MY HANDS HURT EVERY MORNING",
             "sell_function": "hook — names the pain in the first frame"},
            {"shot_index": 2, "start": 2.5, "end": 6.0,
             "shot_type": "b-roll demo",
             "action": "her thumb twists the lid, then the lid lifts clear, "
                       "then she tips the open jar toward the lens",
             "camera_move": "slow push in, steady",
             "end_state": "the open jar is tilted toward the lens with the lid "
                          "resting on the counter beside it",
             "on_screen_text": "none",
             "sell_function": "proof/demo — shows the product in use"},
        ],
    }


def _old_shape_lean_artifact():
    """The pre-rework lean read: visual / who_on_camera / production_method.

    Kept as a fixture on purpose — the point of the rework is that this shape
    no longer validates, and an artifact is the only honest way to prove it.
    """
    return {
        "schema_version": "adread.v1",
        "observed_people": [
            {"label": "person_1", "identity_markers": "woman, 60s, grey bob",
             "wardrobe": "cream cardigan", "shots_present": [1, 2]},
        ],
        "ad_read": _ad_read_block(),
        "shots": [
            {"shot_index": 1, "start": 0.0, "end": 2.5,
             "shot_type": "talking-head",
             "visual": "Medium, static: older woman sits at a kitchen table "
                       "holding her wrist.",
             "on_screen_text": "MY HANDS HURT EVERY MORNING",
             "who_on_camera": "the older woman, alone",
             "sell_function": "hook — names the pain in the first frame",
             "production_method": "real UGC handheld, phone camera"},
            {"shot_index": 2, "start": 2.5, "end": 6.0,
             "shot_type": "b-roll demo",
             "visual": "Close-up, slow push in on the jar being opened.",
             "on_screen_text": "none",
             "who_on_camera": "hands only, no face",
             "sell_function": "proof/demo — shows the product in use",
             "production_method": "licensed stock"},
        ],
    }


def test_old_lean_shape_no_longer_validates():
    """An artifact carrying the retired shape must FAIL, naming the new fields."""
    with pytest.raises(v589.Stage4dValidationError) as exc:
        v589.validate_stage4d_output(_old_shape_lean_artifact(),
                                     _lean_expected_shots(),
                                     profile="fbads-video")
    msg = str(exc.value)
    for field in ("action", "camera_move", "end_state"):
        assert f"missing {field}" in msg, field


def _lean_expected_shots():
    return [{"shot": 1, "start": 0.0, "end": 2.5},
            {"shot": 2, "start": 2.5, "end": 6.0}]


def test_lean_artifact_validates_under_fbads():
    v589.validate_stage4d_output(_minimal_valid_lean(), _lean_expected_shots(),
                                 profile="fbads-video")


def test_lean_artifact_missing_shot_type_is_rejected():
    """A required field is only real if the validator rejects its absence."""
    data = _minimal_valid_lean()
    del data["shots"][0]["shot_type"]
    with pytest.raises(v589.Stage4dValidationError, match="missing shot_type"):
        v589.validate_stage4d_output(data, _lean_expected_shots(),
                                     profile="fbads-video")
    data = _minimal_valid_lean()
    data["shots"][1]["shot_type"] = ""
    with pytest.raises(v589.Stage4dValidationError, match="shot_type"):
        v589.validate_stage4d_output(data, _lean_expected_shots(),
                                     profile="fbads-video")


def test_lean_artifact_missing_action_is_rejected():
    """A required field is only real if the validator rejects its absence."""
    data = _minimal_valid_lean()
    del data["shots"][0]["action"]
    with pytest.raises(v589.Stage4dValidationError, match="missing action"):
        v589.validate_stage4d_output(data, _lean_expected_shots(),
                                     profile="fbads-video")
    # and an empty one is not an observation either ('held still' is the answer)
    data = _minimal_valid_lean()
    data["shots"][1]["action"] = "  "
    with pytest.raises(v589.Stage4dValidationError, match="action"):
        v589.validate_stage4d_output(data, _lean_expected_shots(),
                                     profile="fbads-video")


def test_the_two_contracts_do_not_cross_validate():
    """A lean read must never pass as a stage4d.v2 one, or the reverse."""
    lean = _minimal_valid_lean()
    with pytest.raises(v589.Stage4dValidationError) as exc:
        v589.validate_stage4d_output(lean, _lean_expected_shots())  # default lane
    msg = str(exc.value)
    assert "schema_version must be 'stage4d.v2'" in msg
    assert "missing forensic_perception" in msg          # heavy fields demanded

    heavy = _minimal_valid_stage4d()
    heavy["ad_read"] = _ad_read_block()
    with pytest.raises(v589.Stage4dValidationError) as exc2:
        v589.validate_stage4d_output(heavy, _expected_shots_for(heavy),
                                     profile="fbads-video")
    msg2 = str(exc2.value)
    assert "schema_version must be 'adread.v1'" in msg2
    assert "missing sell_function" in msg2


def test_deep_subblock_checks_do_not_fire_on_lean_shots():
    """The heavy sub-block checks are gated on the PROFILE's shot schema.

    A lean shot has no forensic_perception / action_arc / morphology at all;
    running those checks would make every lean artifact unvalidatable.
    """
    lean = _minimal_valid_lean()
    lean["shots"][0]["action"] = ""     # force a failure so we can read the list
    with pytest.raises(v589.Stage4dValidationError) as exc:
        v589.validate_stage4d_output(lean, _lean_expected_shots(),
                                     profile="fbads-video")
    msg = str(exc.value)
    assert "action must be a non-empty observation" in msg
    for absent in ("forensic_perception", "action_arc", "morphology",
                   "primary_change_axis", "magnitude"):
        assert absent not in msg, absent


def test_lean_footer_note_is_a_stage4d_v2_concern_only():
    """The 2026-08-12 forward-only footer must not fire on a lean artifact."""
    lean = _minimal_valid_lean()
    lean["shots"][0].pop("on_screen_text")
    with pytest.raises(v589.Stage4dValidationError) as exc:
        v589.validate_stage4d_output(lean, _lean_expected_shots(),
                                     profile="fbads-video")
    assert "forward-only tightening" not in str(exc.value)


def test_validator_checks_every_lean_required_shot_field():
    """Lean twin of the ugc-reel drift guard: the checked list must stay
    derived from LEAN_AD_SHOT_SCHEMA, never hand-typed."""
    base = _minimal_valid_lean()
    shots = _lean_expected_shots()
    v589.validate_stage4d_output(base, shots, profile="fbads-video")  # non-vacuous
    for field in v589.LEAN_AD_SHOT_SCHEMA["required"]:
        data = json.loads(json.dumps(base))
        data["shots"][0].pop(field, None)
        try:
            v589.validate_stage4d_output(data, shots, profile="fbads-video")
        except v589.Stage4dValidationError as exc:
            assert field in str(exc), f"{field!r} dropped but not named in: {exc}"
            continue
        raise AssertionError(f"validator accepted a lean shot missing {field!r}")


def test_lean_string_fields_must_carry_an_observation():
    """'none' is the declared empty answer; an empty string is not."""
    data = _minimal_valid_lean()
    data["shots"][1]["on_screen_text"] = "   "
    with pytest.raises(v589.Stage4dValidationError, match="on_screen_text"):
        v589.validate_stage4d_output(data, _lean_expected_shots(),
                                     profile="fbads-video")


def test_lean_shot_timestamps_are_still_checked_against_the_source():
    data = _minimal_valid_lean()
    data["shots"][1]["end"] = 9.9
    with pytest.raises(v589.Stage4dValidationError, match="end must match source"):
        v589.validate_stage4d_output(data, _lean_expected_shots(),
                                     profile="fbads-video")


def test_lean_lane_still_requires_ad_read():
    data = _minimal_valid_lean()
    del data["ad_read"]
    with pytest.raises(v589.Stage4dValidationError, match="ad_read"):
        v589.validate_stage4d_output(data, _lean_expected_shots(),
                                     profile="fbads-video")


def test_validator_default_profile_ignores_ad_read():
    data = _minimal_valid_stage4d()
    shots = _expected_shots_for(data)
    v589.validate_stage4d_output(data, shots)  # no profile arg — must not raise
    v589.validate_stage4d_output(data, shots, profile="ugc-reel")


def test_validator_fbads_requires_ad_read():
    data = _minimal_valid_stage4d()
    shots = _expected_shots_for(data)
    try:
        v589.validate_stage4d_output(data, shots, profile="fbads-video")
        raise AssertionError("expected Stage4dValidationError")
    except v589.Stage4dValidationError as exc:
        assert "ad_read" in str(exc)


def test_validator_fbads_passes_with_ad_read():
    """A COMPLETE fbads artifact is a lean read plus the ad_read block."""
    v589.validate_stage4d_output(_minimal_valid_lean(), _lean_expected_shots(),
                                 profile="fbads-video")


def test_validator_fbads_names_every_missing_ad_read_field():
    """A half-filled ad_read must report all its gaps, not just the first."""
    data = _minimal_valid_lean()
    shots = _lean_expected_shots()
    data["ad_read"] = {"offer": "20% off", "overlay_text_timeline": "not an array"}
    try:
        v589.validate_stage4d_output(data, shots, profile="fbads-video")
        raise AssertionError("expected Stage4dValidationError")
    except v589.Stage4dValidationError as exc:
        msg = str(exc)
        assert "ad_read.overlay_text_timeline must be an array" in msg
        for field in ("cta", "captions", "sound_off_comprehension", "aspect_ratio",
                      "safe_zones", "end_card", "brand_assets",
                      "production_method"):
            assert f"ad_read.{field}" in msg, field
        assert "ad_read.offer" not in msg


def test_parse_and_validate_threads_the_profile():
    data = _minimal_valid_stage4d()
    shots = _expected_shots_for(data)
    raw = json.dumps(data)
    v589.parse_and_validate_stage4d(raw, shots)  # default lane unchanged
    try:
        v589.parse_and_validate_stage4d(raw, shots, profile="fbads-video")
        raise AssertionError("expected Stage4dValidationError")
    except v589.Stage4dValidationError as exc:
        assert "ad_read" in str(exc)


def test_gemini_config_uses_profile_schema():
    """The gemini lane passes the schema out-of-band; it must be the PROFILE's schema."""
    src = inspect.getsource(v589.call_gemini)
    assert "schema_for_profile(profile)" in src
    assert "response_json_schema=STAGE4D_JSON_SCHEMA" not in src


def test_providers_and_cli_thread_the_profile():
    for fn in (v589.call_gemini, v589.call_lmstudio,
               v589.validate_stage4d_output, v589.parse_and_validate_stage4d):
        assert inspect.signature(fn).parameters["profile"].default == "ugc-reel", fn.__name__
    # each provider hands the profile to the prompt builder
    for fn in (v589.call_gemini, v589.call_lmstudio):
        assert "profile=profile" in inspect.getsource(fn), fn.__name__
    main_src = inspect.getsource(v589.main)
    assert '"--profile"' in main_src
    assert "profile=args.profile" in main_src
    # the saved artifact self-identifies which profile produced it
    assert 'parsed["profile"] = args.profile' in main_src


def _install_fake_genai(monkeypatch, response_text, captured=None):
    """Minimal google.genai stand-in so the gemini lane runs offline.

    Fakes ONLY the network surface (client, streamed response, the types the
    inline path constructs). The part under test stays real: call_gemini's own
    validation gate, the config it builds, and the schema_status it hands to
    the cost ledger. Pass `captured` (a list) to record each request's config.
    """
    import sys
    import types as stdlib_types

    class _Chunk:
        def __init__(self, text):
            self.text = text
            self.usage_metadata = None

    class _Models:
        def generate_content_stream(self, model, contents, config):
            if captured is not None:
                captured.append(config)
            return [_Chunk(response_text)]

    class _Client:
        def __init__(self, api_key=None):
            self.models = _Models()

    class _Any:
        def __init__(self, **kw):
            self.__dict__.update(kw)

    fake_types = stdlib_types.SimpleNamespace(
        Blob=_Any, Part=_Any, VideoMetadata=_Any, GenerateContentConfig=_Any)
    genai_mod = stdlib_types.ModuleType("google.genai")
    genai_mod.types = fake_types
    genai_mod.Client = _Client
    google_pkg = stdlib_types.ModuleType("google")
    google_pkg.genai = genai_mod
    monkeypatch.setitem(sys.modules, "google", google_pkg)
    monkeypatch.setitem(sys.modules, "google.genai", genai_mod)


def test_call_gemini_gate_uses_the_call_profile(monkeypatch, tmp_path):
    """call_gemini validated its OWN response under the default profile.

    The bug: `parse_and_validate_stage4d(resp.text, shots)` with no profile.
    A fbads-video run therefore graded a response with no ad_read at all as
    schema_status="pass" in output/gemini_costs.jsonl — the ledger we read to
    judge whether a decode was good. No network: the client is faked.
    """
    data = _minimal_valid_stage4d()          # a valid ugc-reel read; NO ad_read
    shots = _expected_shots_for(data)
    video = tmp_path / "clip.mp4"
    video.write_bytes(b"\x00" * 32)          # under the inline cap -> no upload
    logged: dict = {}
    monkeypatch.setattr(v589, "log_gemini_usage",
                        lambda *a, **kw: logged.update(kw) or {})
    monkeypatch.setenv("GEMINI_API_KEY", "fake-key-never-sent")
    _install_fake_genai(monkeypatch, json.dumps(data))

    # default lane: unchanged — this response IS a valid ugc-reel read
    out = v589.call_gemini(video, shots, "t", [], thinking="default")
    assert json.loads(out) == data
    assert logged["schema_status"] == "pass"

    # fbads lane: same response, still no ad_read -> the in-call gate must fail
    logged.clear()
    with pytest.raises(v589.Stage4dValidationError, match="ad_read"):
        v589.call_gemini(video, shots, "t", [], thinking="default",
                         profile="fbads-video")
    assert logged["schema_status"] == "fail"


def test_call_gemini_sends_the_profile_system_instruction(monkeypatch, tmp_path):
    """Behavioral pin on the risky path — FIX 3 edits the request builder.

    system_instruction reaches Gemini out-of-band, exactly like the schema, so
    a hardcoded module constant would silently give a fbads run the heavy
    lane's RE-RENDER + forensic-protocol brief. No network: client is faked.
    """
    video = tmp_path / "clip.mp4"
    video.write_bytes(b"\x00" * 32)
    monkeypatch.setattr(v589, "log_gemini_usage", lambda *a, **kw: {})
    monkeypatch.setenv("GEMINI_API_KEY", "fake-key-never-sent")

    heavy = _minimal_valid_stage4d()
    configs: list = []
    _install_fake_genai(monkeypatch, json.dumps(heavy), captured=configs)
    v589.call_gemini(video, _expected_shots_for(heavy), "t", [], thinking="default")
    assert configs[-1].system_instruction is v589.SYSTEM_INSTRUCTION

    lean = _minimal_valid_lean()
    configs.clear()
    _install_fake_genai(monkeypatch, json.dumps(lean), captured=configs)
    v589.call_gemini(video, _lean_expected_shots(), "t", [], thinking="default",
                     profile="fbads-video")
    assert configs[-1].system_instruction is v589.FBADS_SYSTEM_INSTRUCTION
    assert "forensic" not in configs[-1].system_instruction.lower()


def test_call_gemini_gate_names_the_profile_in_source():
    """Belt to the behavioral brace: pin the exact regression shape."""
    src = inspect.getsource(v589.call_gemini)
    assert "parse_and_validate_stage4d(resp.text, shots)" not in src
    assert "parse_and_validate_stage4d(resp.text, shots, profile=profile)" in src


def test_validator_checks_every_schema_required_shot_field():
    """Drift guard. required_top was hand-typed and had silently dropped
    frame_inventory + start_frame_spec; it now derives from the schema. Every
    field PER_SHOT_SCHEMA demands must be rejected when absent, so the two can
    never diverge again."""
    base = _minimal_valid_stage4d()
    # expectations come from the PRISTINE fixture — deriving them from the
    # mutated copy would KeyError on the shot_index/start/end rounds.
    shots = _expected_shots_for(base)
    # Non-vacuous by construction: the unmutated fixture must PASS, otherwise
    # every round below would "detect" a failure that was already there.
    v589.validate_stage4d_output(base, shots)
    for field in v589.PER_SHOT_SCHEMA["required"]:
        data = json.loads(json.dumps(base))
        data["shots"][0].pop(field, None)
        try:
            v589.validate_stage4d_output(data, shots)
        except v589.Stage4dValidationError as exc:
            # and it must be THIS field that is named, not some other complaint
            assert field in str(exc), f"{field!r} dropped but not named in: {exc}"
            continue
        raise AssertionError(f"validator accepted a shot missing required field {field!r}")


def test_nonempty_string_rule_did_not_widen_the_default_lane():
    """The 'required string field must be non-empty' check now derives from the
    profile's shot schema instead of the hand-typed ("summary",
    "human_walk_corrections") pair. For ugc-reel the derived set must be
    EXACTLY that pair, or the default lane's validation behavior changed."""
    derived = {k for k in v589.PER_SHOT_SCHEMA["required"]
               if v589.PER_SHOT_SCHEMA["properties"][k].get("type") == "string"}
    assert derived == {"summary", "human_walk_corrections"}
