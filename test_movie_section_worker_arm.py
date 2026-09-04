"""v959 Task 5 — the worker's movie-section arm, and the proof it changed nothing else.

Two halves:

1. The arm's own pure functions (selection, chip verdict, submit verdict) and
   the policy ladder a section clip walks: Prompt B once in the SAME mode, then
   fail. Never the Omni->Veo model swap, because Veo has no Ingredients tab.

2. The inertness pins. Every clip that is not movie-section must take exactly
   the code path it took before: `_omni_ingredients_mode` reads the new force
   flag only when something set it, `set_clip_input_mode` resets that flag on
   every single call, and the policy functions behave as before when
   `render_method` is None.

The worker cannot be imported here — flow_worker.py boots a browser driver —
so these read the shipped source and execute just the function under test.
The charswap suite hits the same wall and solves it the same way.
"""
import pathlib
import re
import sys

import pytest

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

_HERE = pathlib.Path(__file__).parent
WORKER_SRC = _HERE / "static" / "flow_worker.py"


def _worker_function(name, extra_ns=None):
    """Pull ONE top-level function out of the worker and make it callable.

    Importing flow_worker.py boots a browser driver, which is not available in
    a test run — the existing worker tests hit the same wall. Reading the real
    source and executing just the function under test keeps the check honest:
    it runs the shipped code, not a paraphrase of it.

    The slice runs to the NEXT `def`, so it can also pick up a module-level
    constant parked in the gap below the function. The four stdlib modules are
    pre-seeded for that reason — the sibling test file does the same.
    extra_ns adds whatever else the function reads out of module scope (the
    policy functions here read five globals).
    """
    src = WORKER_SRC.read_text(encoding="utf-8")
    start = src.index(f"\ndef {name}(")
    rest = src[start + 1:]
    end = rest.index("\ndef ", 1)
    ns = {"os": __import__("os"), "re": re, "json": __import__("json"),
          "time": __import__("time"), "__file__": str(WORKER_SRC)}
    ns.update(extra_ns or {})
    exec(rest[:end], ns)  # noqa: S102 — running our own file, on purpose
    return ns[name]


def _worker_src():
    return WORKER_SRC.read_text(encoding="utf-8")


def _body(src, name):
    """The source text of one top-level worker function."""
    b = src[src.index(f"\ndef {name}("):]
    return b[:b.index("\ndef ", 1)]


def _arm_block(src):
    """The movie-section arm: from its `elif` to the next sibling `elif`."""
    start = src.index("elif movie_section_selected(clip):")
    return src[start:src.index("elif first_submission_in_project:", start)]


# =============================================================================
# 1. the arm's pure functions
# =============================================================================

def test_movie_section_selected_only_on_exact_value():
    sel = _worker_function("movie_section_selected")
    assert sel({"render_method": "movie-section"}) is True
    assert sel({"render_method": "charswap"}) is False
    assert sel({}) is False and sel(None) is False


def test_chip_verdict_requires_scene_plus_every_face_distinct():
    verdict = _worker_function("movie_section_chip_verdict")
    ok, why = verdict(["a", "b", "c"], faces_wanted=2)
    assert ok
    ok, why = verdict(["a", "b"], faces_wanted=2)
    assert not ok and "3" in why
    ok, why = verdict(["a", "a", "b"], faces_wanted=2)
    assert not ok and "distinct" in why


def test_chip_verdict_holds_for_a_single_face_too():
    """D3 allows 1 or 2 faces, so the wanted count is derived, never hard-coded."""
    verdict = _worker_function("movie_section_chip_verdict")
    assert verdict(["scene", "face"], faces_wanted=1)[0] is True
    assert verdict(["scene", "face", "extra"], faces_wanted=1)[0] is False


def test_submit_verdict_needs_every_media_and_reference_shape():
    verdict = _worker_function("movie_section_submit_verdict")
    ok, why = verdict(seen=True, hits=3, want=3,
                      api_last={"shape": "referenceImages", "videoModelKey": "abra_r2v_8s"})
    assert ok
    ok, why = verdict(seen=True, hits=2, want=3,
                      api_last={"shape": "referenceImages", "videoModelKey": "abra_r2v_8s"})
    assert not ok and "2/3" in why
    ok, why = verdict(seen=True, hits=3, want=3,
                      api_last={"shape": "startImage", "videoModelKey": "abra_i2v_8s"})
    assert not ok and "startImage" in why
    ok, why = verdict(seen=False, hits=0, want=3, api_last=None)
    assert not ok
    # capture switched off: the probe still proves the chips; the shape is unverified, not wrong
    ok, why = verdict(seen=True, hits=3, want=3, api_last=None)
    assert ok and "unverified" in why


def test_submit_verdict_rejects_a_non_reference_model_key():
    """referenceImages with an i2v key would mean the composer sent the chips
    down a path that is not reference-to-video."""
    verdict = _worker_function("movie_section_submit_verdict")
    ok, why = verdict(seen=True, hits=3, want=3,
                      api_last={"shape": "referenceImages", "videoModelKey": "abra_i2v_8s"})
    assert not ok and "abra_i2v_8s" in why


# =============================================================================
# 2. the policy ladder (D7)
# =============================================================================

def _policy_fn():
    """policy_gen_next_action with its module state injected (it reads five globals)."""
    import threading
    src = _worker_src()
    start = src.index("\ndef policy_gen_next_action(")
    rest = src[start + 1:]
    end = rest.index("\ndef ", 1)
    ns = {"is_omni": lambda m: "omni" in str(m).lower(),
          "_swap_model_for_policy": lambda m: "Veo 3.1 - Fast" if "omni" in str(m).lower() else "Omni Flash",
          "_CLIP_PROMPT_B": {}, "_PROMPT_B_TRIED": {}, "_POLICY_GEN_ATTEMPTS": {},
          "_POLICY_SWAP_DONE": {}, "_POLICY_GEN_LOCK": threading.Lock(), "POLICY_FAIL_ATTEMPT": 3,
          "print": lambda *a, **k: None}
    exec(rest[:end], ns)  # noqa: S102
    return ns["policy_gen_next_action"], ns


def test_policy_ladder_for_a_section_is_prompt_b_then_fail_never_swap():
    fn, ns = _policy_fn()
    ns["_CLIP_PROMPT_B"]["c1"] = "prompt b text"
    a1, _ = fn("c1", "Omni Flash", 1, render_method="movie-section")
    a2, _ = fn("c1", "Omni Flash", 2, render_method="movie-section")
    assert (a1, a2) == ("retry_prompt_b", "fail")
    assert ns["_POLICY_SWAP_DONE"] == {}


def test_policy_ladder_for_a_section_without_prompt_b_fails_at_once():
    fn, ns = _policy_fn()
    assert fn("c2", "Omni Flash", 1, render_method="movie-section")[0] == "fail"
    assert ns["_POLICY_SWAP_DONE"] == {}


def test_policy_ladder_unchanged_for_normal_clips():
    fn, ns = _policy_fn()
    assert fn("c3", "Omni Flash", 1)[0] == "retry_swap"          # pre-v959 ladder, no Prompt B
    assert ns["_POLICY_SWAP_DONE"]["c3"] == "Veo 3.1 - Fast"


def test_policy_ladder_unchanged_for_a_charswap_clip():
    """The two arms share the branch point; only 'movie-section' takes the new rung."""
    fn, ns = _policy_fn()
    assert fn("c3b", "Omni Flash", 1, render_method="charswap")[0] == "retry_swap"
    assert ns["_POLICY_SWAP_DONE"]["c3b"] == "Veo 3.1 - Fast"


def _route_fn():
    """route_generation_policy driving the REAL policy_gen_next_action (shared state)."""
    inner, ns = _policy_fn()
    src = _worker_src()
    start = src.index("\ndef route_generation_policy(")
    rest = src[start + 1:]
    end = rest.index("\ndef ", 1)
    ns.update({"policy_gen_next_action": inner,
               "prominent_promptb_decision": lambda cid: "terminal_image",
               "fail_clip_general_policy": lambda cid, msg: None})
    exec(rest[:end], ns)  # noqa: S102
    return ns["route_generation_policy"], ns


def test_route_level_section_ladder_is_prompt_b_then_fail_never_swap():
    route, ns = _route_fn()
    ns["_CLIP_PROMPT_B"]["c4"] = "prompt b text"
    a1, _ = route("c4", "Omni Flash", False, generation_attempt=1, render_method="movie-section")
    a2, _ = route("c4", "Omni Flash", False, generation_attempt=2, render_method="movie-section")
    assert (a1, a2) == ("retry_prompt_b", "fail")
    assert ns["_POLICY_SWAP_DONE"] == {}


def test_route_level_ladder_unchanged_when_no_method_is_passed():
    route, ns = _route_fn()
    assert route("c5", "Omni Flash", False, generation_attempt=1)[0] == "retry_swap"
    assert ns["_POLICY_SWAP_DONE"]["c5"] == "Veo 3.1 - Fast"


def test_every_policy_route_passes_render_method():
    """Codex pass 2 (HIGH): a call site left on the old form forwards None and
    silently keeps the model-swap ladder for a section clip. Zero such sites."""
    src = _worker_src()
    calls = [ln for ln in src.splitlines()
             if re.search(r"\b(route_generation_policy|policy_gen_next_action)\(", ln)
             and not ln.lstrip().startswith("def ")
             and "render_method=" not in ln
             and not ln.lstrip().startswith("#")]
    assert calls == [], calls


# =============================================================================
# 3. inertness — a clip that is not movie-section keeps its old path
# =============================================================================

class _Page:
    """Just an attribute bag; the worker reads the page with getattr defaults."""


def test_omni_ingredients_mode_unchanged_when_nothing_forces_it():
    """v881's rule, untouched: Omni + start&end -> Ingredients, everything else
    -> Frames. No clip in flight today sets _force_ingredients, so every one of
    them reads exactly as it did before v959."""
    fn = _worker_function("_omni_ingredients_mode",
                          {"is_omni": lambda m: bool(m) and "omni" in str(m).lower()})
    p = _Page()
    p._veo_model = "Veo 3.1 - Omni Flash"
    assert fn(p) is False                     # start only -> Frames
    p._clip_has_end_frame = True
    assert fn(p) is True                      # start+end -> Ingredients
    for other in ("Veo 3.1 - Quality", "Veo 3.1 - Fast", "Veo 2", "", None):
        q = _Page()
        q._veo_model = other
        q._clip_has_end_frame = True
        assert fn(q) is False, other


def test_omni_ingredients_mode_is_true_when_a_section_forces_it():
    """v959: one scene chip and no end frame still has to land on Ingredients."""
    fn = _worker_function("_omni_ingredients_mode",
                          {"is_omni": lambda m: bool(m) and "omni" in str(m).lower()})
    p = _Page()
    p._veo_model = "Omni Flash"
    p._force_ingredients = True
    assert fn(p) is True


def test_forcing_ingredients_cannot_rescue_a_non_omni_model():
    """No model but Omni offers the tab at all, so the force flag is not a
    licence to attach blind — the arm's own gate still refuses."""
    fn = _worker_function("_omni_ingredients_mode",
                          {"is_omni": lambda m: bool(m) and "omni" in str(m).lower()})
    p = _Page()
    p._veo_model = "Veo 3.1 - Fast"
    p._force_ingredients = True
    assert fn(p) is False


def test_set_clip_input_mode_resets_the_force_flag_on_every_call():
    """The flag is per clip. A movie-section clip that left it set would push
    the NEXT ordinary clip onto the Ingredients tab, where its frame upload has
    no slot. Resetting on entry — not on exit — means no error path can skip it.
    """
    body = _body(_worker_src(), "set_clip_input_mode")
    signature = body[:body.index('"""')]
    assert "force_ingredients=False" in signature, "the kwarg must default to False"
    assert "page._force_ingredients = bool(force_ingredients)" in body
    # it runs before anything can return early
    assert (body.index("page._force_ingredients = bool(force_ingredients)")
            < body.index("if applied == mode:"))


# =============================================================================
# 4. the arm's shape — where it sits and how it refuses
# =============================================================================

def test_the_arm_is_a_sibling_elif_between_charswap_and_the_normal_path():
    """A separate `if` would run the normal upload path for a section clip too."""
    src = _worker_src()
    charswap = src.index("if charswap_selected(clip):")
    section = src.index("elif movie_section_selected(clip):")
    normal = src.index("elif first_submission_in_project:")
    assert charswap < section < normal


def test_the_arm_proves_the_tab_before_it_downloads_anything():
    """A refusal must cost nothing: no frame download, no upload dialog."""
    arm = _arm_block(_worker_src())
    mode = arm.index("set_clip_input_mode(")
    guard = arm.index("if _ms_mode != 'Ingredients':")
    fetch = arm.index("movie_section_fetch_inputs(")
    assert mode < guard < fetch


def test_every_refusal_in_the_arm_fails_the_clip_closed():
    """Same discipline as the charswap arm: failed + permanently_failed + continue.
    Never flow_redo_queued — the redo lane has no arm for this method."""
    arm = _arm_block(_worker_src())
    assert "flow_redo_queued" not in arm
    assert arm.count("update_clip_status(clip['id'], 'failed'") >= 6
    lines = [ln.strip() for ln in arm.split("\n")]
    marks = [i for i, ln in enumerate(lines)
             if ln == "permanently_failed_clips.add(clip_index)"]
    assert len(marks) >= 6, marks
    for i in marks:
        following = [ln for ln in lines[i + 1:] if ln and not ln.startswith("#")]
        assert following[0] == "continue", lines[i:i + 4]


def test_the_arm_refuses_a_section_with_no_authored_prompt():
    """D11: the Text prompt ships verbatim, so an empty one is not renderable."""
    arm = _arm_block(_worker_src())
    assert "_ms_prompt = _cs_platform_prompt" in arm
    assert "if not (_ms_prompt or \"\").strip():" in arm
    assert arm.index("_ms_prompt = _cs_platform_prompt") < arm.index(
        "movie_section_attach_and_prompt(")


def test_the_submit_verdict_gates_the_success_tail():
    """The tile bookkeeping and the confirmed-submit stamp sit AFTER the gate."""
    arm = _arm_block(_worker_src())
    verdict = arm.index("movie_section_submit_verdict(")
    refuse = arm.index("movie-section submit not proven")
    tail = arm.index("human_delay(1, 2)")
    assert verdict < refuse < tail
    assert arm.index("_MOVIE_SECTION_SUBMIT_CONFIRMED_IDS.add(") > tail


def test_the_captured_shape_is_cleared_before_the_click():
    """page._flow_api_last holds the LAST submit the page saw — on clip 2 that
    is clip 1's. Clearing it first means the shape the verdict reads can only
    have come from this clip's own request."""
    arm = _arm_block(_worker_src())
    clear = arm.index("page._flow_api_last = None")
    click = arm.index("click_generate_button(page,")
    read = arm.index('getattr(page, "_flow_api_last", None)')
    assert clear < click < read


def test_the_ghost_check_exempts_a_confirmed_section_submit():
    """v945.15.2, one arm along: the downstream ghost check runs on a dead
    selector and would park a proven submit for a redo lane that refuses it."""
    flat = " ".join(_worker_src().split())
    assert ("_cs_ghost_exempt = bool((charswap_selected(clip) "
            "or movie_section_selected(clip)) and (") in flat


def test_the_redo_parking_door_knows_about_sections_too():
    """update_clip_status is the ONE door every parking path passes (v945.15.3).
    A section clip must be refused there exactly like a swap clip."""
    body = _body(_worker_src(), "update_clip_status")
    assert "_MOVIE_SECTION_CLIP_IDS" in body
    assert "_MOVIE_SECTION_SUBMIT_CONFIRMED_IDS" in body


def test_the_diag_file_records_every_stage_a_failure_can_stop_at():
    """A lost console window must not eat the evidence — same reason the
    charswap arm writes its own jsonl beside the worker."""
    src = _worker_src()
    written = _arm_block(src) + _body(src, "movie_section_attach_and_prompt")
    for stage in ("settings_failed", "tab_not_ingredients", "inputs_unavailable",
                  "no_prompt", "scene_attach_failed", "face_attach_failed",
                  "chips_read", "submit_verdict"):
        assert f'stage="{stage}"' in written, stage


# =============================================================================
# 5. the payload contract — a misspelled key is None, not an error
# =============================================================================

class _Clip:
    def __init__(self, **kw):
        self.render_method = None
        self.face_ref_frames_json = None
        self.veo_render_duration_s = None
        self.job_id = "job1"
        self.__dict__.update(kw)


def test_payload_keys_match_worker_reads():
    """v945.8 lesson: a misspelled key is None, not an error. Every key the
    worker reads off a movie-section clip must be one the server emits.

    face_ref_locals / start_frame_local are the exception by design: the
    off-platform E2E driver hands the arm local files instead of URLs, and no
    server payload ever carries them.
    """
    src = _worker_src()
    scanned = (_body(src, "movie_section_fetch_inputs")
               + _body(src, "movie_section_selected")
               + _arm_block(src))
    reads = set(re.findall(
        r"clip\.get\(['\"](face_ref_[a-z_]+|input_mode|section_window_s|render_method)['\"]",
        scanned))
    assert "face_ref_urls" in reads and "section_window_s" in reads
    from main import _v959_movie_section_payload
    emitted = set(_v959_movie_section_payload(
        _Clip(render_method="movie-section", face_ref_frames_json="[]"),
        "https://x", "user-worker"))
    assert reads - emitted - {"face_ref_locals"} == set(), reads - emitted
