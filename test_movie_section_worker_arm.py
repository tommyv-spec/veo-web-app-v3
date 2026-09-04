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

These read the shipped source and execute just the function under test rather
than importing flow_worker. The module imports fine in principle; the reason
not to is that `_bootstrap_browser_driver()` runs at MODULE SCOPE (:263) and,
when `browser_driver.py` is missing or stale, fetches and rewrites it from
`WEB_APP_URL` as a side effect of the import. A test run must not reach out to
production or rewrite a worker file. The charswap suite solves it the same way.
"""
import json
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


_GEN_OK = {"endpoint": "batchAsyncGenerateVideoReferenceImages",
           "shape": "referenceImages", "videoModelKey": "abra_r2v_8s"}


def test_submit_verdict_needs_every_media_and_reference_shape():
    verdict = _worker_function("movie_section_submit_verdict")
    ok, why = verdict(seen=True, hits=3, want=3, api_last=dict(_GEN_OK))
    assert ok
    ok, why = verdict(seen=True, hits=2, want=3, api_last=dict(_GEN_OK))
    assert not ok and "2/3" in why
    ok, why = verdict(seen=True, hits=3, want=3,
                      api_last={"endpoint": "batchAsyncGenerateVideoStartImage",
                                "shape": "startImage", "videoModelKey": "abra_i2v_8s"})
    assert not ok and "startImage" in why
    ok, why = verdict(seen=False, hits=0, want=3, api_last=None)
    assert not ok
    # nothing captured at all: the probe still proves the chips; unverified, not wrong
    ok, why = verdict(seen=True, hits=3, want=3, api_last=None)
    assert ok and "unverified" in why


def test_submit_verdict_rejects_a_non_reference_model_key():
    """referenceImages with an i2v key would mean the composer sent the chips
    down a path that is not reference-to-video."""
    verdict = _worker_function("movie_section_submit_verdict")
    ok, why = verdict(seen=True, hits=3, want=3,
                      api_last={"endpoint": "batchAsyncGenerateVideoReferenceImages",
                                "shape": "referenceImages", "videoModelKey": "abra_i2v_8s"})
    assert not ok and "abra_i2v_8s" in why


def test_a_status_poll_overwriting_the_capture_is_unverified_not_a_refusal():
    """THE REGRESSION THIS EXISTS FOR. The shared listener stashes EVERY watched
    request, and a submit is immediately followed by status polls and log posts
    that carry no shape at all. Judging that stash would refuse a clip that is
    already rendering. Only a generate capture is judged."""
    verdict = _worker_function("movie_section_submit_verdict")
    for poll in ({"endpoint": "batchCheckAsyncVideoGenerationStatus", "shape": "",
                  "videoModelKey": ""},
                 {"endpoint": "batchLogFrontendEvents", "shape": "", "videoModelKey": ""},
                 {"endpoint": "submitBatchLog", "shape": "", "videoModelKey": ""},
                 {"endpoint": "agentInfo", "shape": "", "videoModelKey": ""}):
        ok, why = verdict(seen=True, hits=3, want=3, api_last=poll)
        assert ok, (poll, why)
        assert "unverified" in why and poll["endpoint"] in why


def test_a_reference_images_generate_with_no_model_key_is_still_accepted():
    """The SHAPE is the proof that the composer was on Ingredients. An unread
    videoModelKey (the capture parses the body best-effort) says nothing
    against it, so refusing on it would fail a proven section."""
    verdict = _worker_function("movie_section_submit_verdict")
    for key in ("", None):
        ok, why = verdict(seen=True, hits=3, want=3,
                          api_last={"endpoint": "batchAsyncGenerateVideoReferenceImages",
                                    "shape": "referenceImages", "videoModelKey": key})
        assert ok, key
        assert "unread" in why and "referenceImages" in why


def test_a_generate_with_an_empty_shape_is_unverified_too():
    """A generate whose body could not be parsed says nothing about the shape."""
    verdict = _worker_function("movie_section_submit_verdict")
    ok, why = verdict(seen=True, hits=3, want=3,
                      api_last={"endpoint": "batchAsyncGenerateVideo", "shape": "",
                                "videoModelKey": ""})
    assert ok and "unverified" in why


def test_a_real_start_image_generate_is_still_refused():
    """The half this must never lose: a generate that really went out as an
    image-to-video animation of the scene frame is not a section."""
    verdict = _worker_function("movie_section_submit_verdict")
    for bad in ({"endpoint": "batchAsyncGenerateVideoStartImage", "shape": "startImage",
                 "videoModelKey": "abra_i2v_8s"},
                {"endpoint": "batchAsyncGenerateVideoStartAndEndImage",
                 "shape": "startImage+endImage", "videoModelKey": "abra_i2v_8s"}):
        ok, why = verdict(seen=True, hits=3, want=3, api_last=bad)
        assert not ok, bad


# =============================================================================
# 1b. fetching the inputs — all of them, or none
# =============================================================================

def _fetch_inputs(downloads_ok=True):
    """movie_section_fetch_inputs with download_frame stubbed. `downloads_ok`
    False makes the SECOND face fail, which is the partial case that matters."""
    calls = []

    def _dl(url, path):
        calls.append((url, path))
        if not downloads_ok and len(calls) >= 3:
            return None
        return path

    fn = _worker_function("movie_section_fetch_inputs",
                          {"download_frame": _dl, "print": lambda *a, **k: None})
    return fn, calls


def test_fetch_inputs_downloads_the_scene_and_every_face():
    fn, calls = _fetch_inputs()
    scene, faces = fn({"clip_index": 2, "start_frame_url": "https://x/s.png",
                       "face_ref_urls": ["https://x/f1.png", "https://x/f2.png"]}, "/tmp")
    assert scene and len(faces) == 2
    assert len(calls) == 3


def test_one_face_that_will_not_download_takes_the_whole_clip_down():
    """A section rendered with the scene chip and one of its two faces is not a
    cheaper version of the clip, it is a different clip."""
    fn, _ = _fetch_inputs(downloads_ok=False)
    assert fn({"clip_index": 0, "start_frame_url": "https://x/s.png",
               "face_ref_urls": ["https://x/f1.png", "https://x/f2.png"]}, "/tmp") == (None, [])


def test_no_scene_frame_at_all_is_refused():
    fn, _ = _fetch_inputs()
    assert fn({"clip_index": 0, "face_ref_urls": ["https://x/f1.png"]}, "/tmp") == (None, [])


def test_locals_win_over_urls_and_download_nothing():
    """The off-platform E2E driver hands the arm files instead of URLs."""
    fn, calls = _fetch_inputs()
    scene, faces = fn({"clip_index": 0, "start_frame_local": "/l/s.png",
                       "face_ref_locals": ["/l/f1.png", "/l/f2.png"],
                       "start_frame_url": "https://x/s.png",
                       "face_ref_urls": ["https://x/f1.png", "https://x/f2.png"]}, "/tmp")
    assert scene == "/l/s.png" and faces == ["/l/f1.png", "/l/f2.png"]
    assert calls == []


def test_a_partial_set_of_locals_is_refused_not_silently_completed():
    """One local beside two declared URLs is a half-built hand-in. Accepting it
    would render a two-face section with one face and call it a success."""
    fn, _ = _fetch_inputs()
    assert fn({"clip_index": 0, "start_frame_local": "/l/s.png",
               "face_ref_locals": ["/l/f1.png"],
               "face_ref_urls": ["https://x/f1.png", "https://x/f2.png"]}, "/tmp") == (None, [])


def test_an_empty_local_path_is_refused_at_the_fetch_not_inside_the_attach():
    """`["", "/f2.png"]` is one usable file, not two. Counting the blank would
    pass the all-or-nothing guard here and hand the attach an empty path, so
    the failure would surface as a chip that would not attach — a much longer
    way round to the same refusal, with a browser click spent on it."""
    fn, calls = _fetch_inputs()
    assert fn({"clip_index": 0, "start_frame_local": "/l/s.png",
               "face_ref_locals": ["", "/l/f2.png"],
               "face_ref_urls": ["https://x/f1.png", "https://x/f2.png"]}, "/tmp") == (None, [])
    assert calls == []


# =============================================================================
# 1c. attaching — the scene clears, every face must not
# =============================================================================

def _attach_fn(attach_results=None, chips=("s", "f1", "f2"), armed=(True, "ok")):
    """movie_section_attach_and_prompt with every browser call stubbed."""
    log = []

    def _attach(page, path, context="", clear_existing=True, **kw):
        log.append(("attach", path, clear_existing))
        return (attach_results or {}).get(path, (True, None))

    diag = []
    fn = _worker_function("movie_section_attach_and_prompt", {
        "attach_ingredient_image_with_check": _attach,
        "movie_section_write_diag": lambda **f: diag.append(f),
        "movie_section_chip_verdict": _worker_function("movie_section_chip_verdict"),
        "charswap_composer_chip_media_ids": lambda page: list(chips),
        "charswap_install_submit_probe": lambda page, ids: log.append(("probe", ids)),
        "fill_prompt_textarea": lambda page, p: log.append(("prompt", p)),
        "charswap_arm_generate": lambda page, p, ids, context="": armed,
        "time": type("T", (), {"sleep": staticmethod(lambda s: None)}),
    })
    return fn, log, diag


def test_the_scene_chip_clears_and_every_face_chip_does_not():
    """clear_existing=True on a face would delete the scene chip it must sit
    beside — the v881 pair discipline, one chip further along."""
    fn, log, _ = _attach_fn()
    ok, chips = fn(_Page(), "/s.png", ["/f1.png", "/f2.png"], "the section prompt")
    assert ok and chips == ["s", "f1", "f2"]
    assert [(e[1], e[2]) for e in log if e[0] == "attach"] == [
        ("/s.png", True), ("/f1.png", False), ("/f2.png", False)]


def test_a_face_that_will_not_attach_stops_before_the_probe():
    fn, log, diag = _attach_fn(attach_results={"/f1.png": (False, 'no_buttons')})
    page = _Page()
    ok, chips = fn(page, "/s.png", ["/f1.png"], "prompt")
    assert not ok and chips == []
    assert "face chip 1" in page._movie_section_block_reason
    assert [d["stage"] for d in diag] == ["face_attach_failed"]
    assert not [k for k, *_ in log if k == "probe"]


def test_the_chips_read_diag_records_what_was_on_the_composer_before():
    """Three chips can mean 'scene + two faces' or 'one bled in from the last
    clip and a face never attached'. The before-count is what tells them apart."""
    fn, _, diag = _attach_fn()
    fn(_Page(), "/s.png", ["/f1.png", "/f2.png"], "prompt")
    read = [d for d in diag if d["stage"] == "chips_read"]
    assert read and "chips_before" in read[0]


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


def test_the_probe_verdict_can_be_asked_for_every_chip_not_just_two():
    """A section holds three chips. `hits >= 2` would call a submit that dropped
    a face 'both media present' and stop the wait early on a half-attached body.
    The charswap callers keep the old meaning through the default."""
    fn = _worker_function("charswap_submit_body_verdict")

    class _P:
        _charswap_submit_probe = {"seen": True, "hits": 2}

    assert fn(_P()) == (True, True)              # charswap, unchanged
    assert fn(_P(), want=3) == (True, False)     # a section is one chip short
    _P._charswap_submit_probe = {"seen": True, "hits": 3}
    assert fn(_P(), want=3) == (True, True)


def test_the_arm_waits_for_every_chip_it_attached():
    arm = _arm_block(_worker_src())
    assert "charswap_await_submit_verdict(page, want=len(_ms_chips))" in arm


def test_the_await_signature_keeps_the_charswap_default():
    signature = _body(_worker_src(), "charswap_await_submit_verdict")
    signature = signature[:signature.index('"""')]
    assert "want=2" in signature


# --- 4.1 one project per section clip (v945.13, ported) ---------------------

def test_the_arm_rotates_to_a_fresh_project_for_a_second_section():
    """Three chips into a composer another clip already used is a bigger hazard
    than the two the swap arm rotates for; v945.11 measured a chip surviving the
    generic clear. Rotation is pre-click, so a refusal costs no render."""
    arm = _arm_block(_worker_src())
    rot = arm.index("_fa_try_create_new_project_api(")
    settings = arm.index("select_frames_to_video_mode(")
    attach = arm.index("movie_section_attach_and_prompt(")
    assert arm.index("if not first_submission_in_project:") < rot < settings < attach
    assert 'stage="project_rotation_failed"' in arm
    assert 'stage="project_rotated"' in arm
    # the rotation refusal is fail-closed like every other one
    refusal = arm[rot:arm.index("first_submission_in_project = True", rot)]
    assert "update_clip_status(clip['id'], 'failed'" in refusal
    assert "permanently_failed_clips.add(clip_index)" in refusal


def test_the_rotated_project_url_is_written_back_to_the_cache():
    """Without this the job's cached project_url points at the old project and a
    restart re-opens the composer this clip deliberately left."""
    arm = _arm_block(_worker_src())
    assert "cache['jobs'][job_id]['project_url'] = project_url" in arm
    assert "save_cache(cache)" in arm


# --- 4.2 the diag says which job and clip it is talking about ---------------

def test_the_arm_stamps_its_identity_for_the_attach_stage_diag():
    """The attach helper takes no job_id argument (several call paths share the
    signature), so the identity rides on the page — the charswap pair's trick."""
    arm = _arm_block(_worker_src())
    assert "page._movie_section_job_id = clip.get('job_id') or job_id" in arm
    assert "page._movie_section_clip_index = clip_index" in arm
    assert arm.index("page._movie_section_job_id") < arm.index("movie_section_attach_and_prompt(")
    helper = _body(_worker_src(), "movie_section_attach_and_prompt")
    assert 'getattr(page, "_movie_section_job_id", None)' in helper
    assert 'getattr(page, "_movie_section_clip_index", None)' in helper
    # every diag line the helper writes carries them
    assert helper.count("**_who") == 3


def test_a_failed_diag_write_says_so_instead_of_vanishing():
    """A silent diagnostic failure is how you end up with no evidence and no
    idea there was none."""
    assert "[v959] diag write failed" in _body(_worker_src(), "movie_section_write_diag")


def test_the_diag_file_is_ignored_like_its_charswap_twin():
    """Runtime observation from whichever machine ran the worker, on a SHARED
    working tree where a stray `git add -A` sweeps whatever is untracked."""
    ignored = (_HERE / ".gitignore").read_text(encoding="utf-8")
    assert "static/movie_section_diag.jsonl" in ignored


def test_the_force_flag_is_reset_in_the_clip_loop_preamble_too():
    """set_clip_input_mode resets it, but readers that run BEFORE it — the full
    settings pass, rebuild_clip — would otherwise see the previous clip's flag."""
    src = _worker_src()
    preamble = src.index("_cs_platform_prompt = prompt")
    arm = src.index("elif movie_section_selected(clip):")
    reset = src.index("page._force_ingredients = False", preamble)
    assert reset < arm


def test_the_captured_shape_is_cleared_before_the_click():
    """page._flow_api_last holds the LAST submit the page saw — on clip 2 that
    is clip 1's. Clearing it first means the shape the verdict reads can only
    have come from this clip's own request."""
    arm = _arm_block(_worker_src())
    clear = arm.index("page._flow_api_last = None")
    click = arm.index("click_generate_button(page,")
    read = arm.index('getattr(page, "_flow_api_last", None)')
    assert clear < click < read


# =============================================================================
# 4.3 the dedicated generate slot — what makes the shape check real
# =============================================================================
#
# The capture listener stashes EVERY watched request. A submit is followed
# within milliseconds by status polls, frontend log posts and credit reads, and
# the verdict waits up to 20s for the probe — so the shared slot almost always
# held a poll by the time it was read, and every real submit was accepted
# through the "shape unverified" door. The shape was never actually judged in
# production. A second slot, written only by a submit, fixes that.

_GEN_URL = ("https://aisandbox-pa.googleapis.com/v1/flow:"
            "batchAsyncGenerateVideoReferenceImages")
_START_URL = ("https://aisandbox-pa.googleapis.com/v1/flow:"
              "batchAsyncGenerateVideoStartImage")
_POLL_URL = ("https://aisandbox-pa.googleapis.com/v1/flow:"
             "batchCheckAsyncVideoGenerationStatus")
_GEN_BODY = json.dumps({"requests": [{"videoModelKey": "abra_r2v_8s",
                                      "referenceImages": [{"mediaId": "m1"}]}]})
_START_BODY = json.dumps({"requests": [{"videoModelKey": "veo_3_1_i2v",
                                        "startImage": {"mediaId": "m1"}}]})


class _Req:
    """The three attributes the listener reads off a Playwright request."""

    def __init__(self, url, body=None):
        self.url = url
        self.method = "POST"
        self.post_data = body


def _capture_listener(tmp_path):
    """Install the REAL capture listener on a fake page; hand back both.

    Feeding fake requests through the shipped listener is the only honest way
    to test which slot a request lands in — the alternative is re-typing the
    condition in the test and pinning the copy.
    """
    installed = []

    class _Page:
        def on(self, event, fn):
            installed.append((event, fn))

    install = _worker_function("_install_flow_api_capture", extra_ns={
        "_flow_api_capture_enabled": lambda: True,
        "_flow_api_capture_path": lambda: str(tmp_path / "capture.jsonl"),
        "SESSION_FOLDER": str(tmp_path),
    })
    page = _Page()
    install(page)
    assert installed and installed[0][0] == "request"
    return page, installed[0][1]


def test_a_status_poll_lands_in_the_shared_slot_and_not_the_generate_one(tmp_path):
    page, listener = _capture_listener(tmp_path)
    listener(_Req(_GEN_URL, _GEN_BODY))
    captured = dict(page._flow_api_last_generate)
    assert captured["shape"] == "referenceImages"
    assert captured["videoModelKey"] == "abra_r2v_8s"
    listener(_Req(_POLL_URL, "{}"))
    assert page._flow_api_last["shape"] == ""          # the shared slot moved on
    assert page._flow_api_last_generate == captured    # this one did not


def test_the_slot_the_arm_reads_now_carries_a_shape_where_it_read_a_poll(tmp_path):
    """The whole point: same two requests, two different verdicts."""
    verdict = _worker_function("movie_section_submit_verdict")
    page, listener = _capture_listener(tmp_path)
    listener(_Req(_GEN_URL, _GEN_BODY))
    listener(_Req(_POLL_URL, "{}"))
    ok, why = verdict(True, 3, 3, getattr(page, "_flow_api_last", None))
    assert ok and "unverified" in why                  # what it used to read
    ok, why = verdict(True, 3, 3, getattr(page, "_flow_api_last_generate", None))
    assert ok and "shape=referenceImages" in why       # what it reads now


def test_a_start_image_submit_followed_by_a_poll_is_now_actually_refused(tmp_path):
    """The wrong render this check exists to catch. Before the dedicated slot
    the poll hid it and the clip was accepted."""
    verdict = _worker_function("movie_section_submit_verdict")
    page, listener = _capture_listener(tmp_path)
    listener(_Req(_START_URL, _START_BODY))
    listener(_Req(_POLL_URL, "{}"))
    assert verdict(True, 3, 3, getattr(page, "_flow_api_last", None))[0] is True
    ok, why = verdict(True, 3, 3, getattr(page, "_flow_api_last_generate", None))
    assert not ok and "startImage" in why


def test_nothing_captured_at_all_is_still_unverified_not_a_refusal(tmp_path):
    """With the capture off, or before any request, the slot is empty. That must
    stay an accept-on-the-probe — refusing would kill a clip already rendering."""
    verdict = _worker_function("movie_section_submit_verdict")
    ok, why = verdict(True, 3, 3, None)
    assert ok and "unverified" in why


def test_only_a_submit_endpoint_writes_the_generate_slot(tmp_path):
    """Guarded by the v770 substring, which that rule already proved matches
    every submit endpoint and no status poll."""
    body = _body(_worker_src(), "_install_flow_api_capture")
    assert "page._flow_api_last = _cap" in body
    guard = body.index("if _SUBMIT_BIND_URL_SUBSTR in endpoint:")
    assert guard < body.index("page._flow_api_last_generate = _cap")
    assert body.count("_flow_api_last_generate") == 1


def test_the_arm_clears_and_reads_the_generate_slot(tmp_path):
    arm = _arm_block(_worker_src())
    clear = arm.index("page._flow_api_last_generate = None")
    click = arm.index("click_generate_button(page,")
    read = arm.index('getattr(page, "_flow_api_last_generate", None)')
    assert clear < click < read
    # the verdict is handed the generate slot, not the shared one
    call = arm[arm.index("movie_section_submit_verdict("):]
    call = call[:call.index(")\n")]
    assert '_flow_api_last_generate' in call and '"_flow_api_last", None' not in call


def test_the_verdict_says_why_an_unread_model_key_is_allowed_to_pass(tmp_path):
    """An unread videoModelKey stays a pass. That is only safe because the shape
    above it is now really judged, and the docstring has to say so — otherwise
    the next person to move this back to the shared slot reopens a hole."""
    doc = _body(_worker_src(), "movie_section_submit_verdict")
    doc = doc[doc.index('"""'):doc.index('"""', doc.index('"""') + 3)]
    assert "_flow_api_last_generate" in doc
    assert "only safe BECAUSE the shape above was really judged" in doc


# =============================================================================
# 4.4 the claim-time arm — a worker says what it can render
# =============================================================================

def test_both_polls_advertise_the_arms_this_build_carries():
    """A worker pulls flow_worker.py once, at startup. A process older than the
    arm's deploy does not have it, and would render a section clip as an
    ordinary animation of the wide frame. Saying so at claim time is the only
    place the server can tell."""
    src = _worker_src()
    assert 'WORKER_ARMS = ("movie-section",)' in src
    assert 'f"/jobs/pending?worker_id={WORKER_ID}&arms={\',\'.join(WORKER_ARMS)}"' in src
    assert ('f"/clips/redo-pending?worker_id={WORKER_ID}'
            '&arms={\',\'.join(WORKER_ARMS)}"') in src


def test_the_charswap_arm_is_deliberately_not_advertised():
    """Every worker in the field already has it; gating on it would strand live
    jobs behind a param no running worker sends."""
    src = _worker_src()
    block = src[src.index("WORKER_ARMS = ("):]
    assert "charswap" not in block[:block.index("\n")]
    assert "charswap arm predates this" in src


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
