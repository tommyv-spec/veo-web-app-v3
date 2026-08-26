"""v943 — the charswap render method, and the proof it changed nothing else.

Four things are checked here, and the fourth is the point of the other three:
a build that says nothing about charswap must parse, promote and reach the
worker exactly as it did before this feature existed.

A Veo render is stochastic, so "byte-identical output" is not a claim anyone
can make. What CAN be compared is everything up to the render: the parsed
scene, the JSON the worker is handed, and which branch the worker picks.

Sections 5-8 were added after the rev-482 diff review, and each one pins a
blocker that review found: owner scoping on both asset reads, the temp file a
served download used to leave behind, the unbounded upload route, and the
image-led mode that was a label rather than a technique.
"""
import asyncio
import io
import os
import pathlib
import sys

import pytest

# The console on the build box is cp1252 and several of these strings are not.
# Pinned here rather than in the environment: PYTHONIOENCODING is inherited by
# every child process and has already blinded a different checker.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

_HERE = pathlib.Path(__file__).parent


WORKER_SRC = _HERE / "static" / "flow_worker.py"


def _worker_function(name):
    """Pull ONE top-level function out of the worker and make it callable.

    Importing flow_worker.py boots a browser driver, which is not available in
    a test run — the existing worker tests hit the same wall. Reading the real
    source and executing just the function under test keeps the check honest:
    it runs the shipped code, not a paraphrase of it.
    """
    src = WORKER_SRC.read_text(encoding="utf-8")
    start = src.index(f"\ndef {name}(")
    rest = src[start + 1:]
    end = rest.index("\ndef ", 1)
    ns = {}
    exec(rest[:end], ns)  # noqa: S102 — running our own file, on purpose
    return ns[name]


LEGACY_SCENE = """### Scene 1

- **image:** image_1
- **speaker:** on-camera
- **line:** american men over sixty are doing this every morning
- **action_note:** she lifts the jar [Start beat]
"""

CHARSWAP_SCENE = """### Scene 1

- **image:** image_1
- **speaker:** silent
- **render_method:** charswap
- **swap_source_video:** raw/refs/curls.mp4
- **swap_mode:** video-led
- **action_note:** he finishes the curl [Start beat]
"""


def _parse(md):
    from image_platform import _parse_scene_blocks_new
    return _parse_scene_blocks_new(md, {1})


# --- 1. a build with no new bullets parses to all-None new fields ------------

def test_legacy_scene_has_all_charswap_fields_none():
    scene = _parse(LEGACY_SCENE)[0]
    assert scene["render_method"] is None
    assert scene["swap_source_video"] is None
    assert scene["swap_mode"] is None


def test_legacy_scene_keeps_every_other_field():
    scene = _parse(LEGACY_SCENE)[0]
    assert scene["scene_index"] == 1
    assert scene["image_index"] == 1
    assert scene["lines"] == [
        "american men over sixty are doing this every morning"]
    assert scene["speaker_mode"] == "on-camera"


def test_charswap_scene_parses_all_three():
    scene = _parse(CHARSWAP_SCENE)[0]
    assert scene["render_method"] == "charswap"
    assert scene["swap_source_video"] == "raw/refs/curls.mp4"
    assert scene["swap_mode"] == "video-led"


# --- 2. the parser fails CLOSED ---------------------------------------------

def test_unknown_render_method_hard_fails():
    md = CHARSWAP_SCENE.replace("charswap", "faceswap", 1)
    with pytest.raises(ValueError, match="render_method"):
        _parse(md)


def test_partial_charswap_set_hard_fails():
    md = "\n".join(
        l for l in CHARSWAP_SCENE.splitlines()
        if "swap_mode" not in l)
    with pytest.raises(ValueError, match="swap_mode"):
        _parse(md)


def test_swap_source_alone_hard_fails():
    md = LEGACY_SCENE + "- **swap_source_video:** raw/refs/curls.mp4\n"
    with pytest.raises(ValueError, match="render_method"):
        _parse(md)


def test_bad_swap_mode_hard_fails():
    md = CHARSWAP_SCENE.replace("video-led", "sideways", 1)
    with pytest.raises(ValueError, match="swap_mode"):
        _parse(md)


def test_charswap_scene_with_two_lines_hard_fails():
    md = CHARSWAP_SCENE.replace(
        "- **speaker:** silent", "- **speaker:** on-camera") + (
        "- **line:** first line here\n- **line:** second line here\n")
    with pytest.raises(ValueError, match="one clip"):
        _parse(md)


def test_charswap_scene_with_one_line_is_fine():
    md = CHARSWAP_SCENE.replace(
        "- **speaker:** silent", "- **speaker:** on-camera") + (
        "- **line:** only one line here\n")
    scene = _parse(md)[0]
    assert scene["render_method"] == "charswap"
    assert len(scene["lines"]) == 1


# --- 3. the pending-job payload is unchanged for a legacy clip ---------------

class _Clip:
    """Just the attributes the payload builder reads."""

    def __init__(self, **kw):
        self.render_method = None
        self.swap_source_r2_key = None
        self.swap_mode = None
        self.swap_avatar_upload_id = None
        for k, v in kw.items():
            setattr(self, k, v)


LEGACY_PAYLOAD = {
    "id": 7,
    "clip_index": 0,
    "dialogue_text": "a line",
    "prompt": "a prompt",
    "start_frame_key": "jobs/x/frames/image_01.png",
    "status": "pending",
    "clip_mode": "fresh",
    "scene_index": 0,
    "veo_render_duration_s": 8,
}


def test_legacy_clip_payload_is_untouched():
    from main import _v943_maybe_charswap
    before = dict(LEGACY_PAYLOAD)
    after = _v943_maybe_charswap(dict(LEGACY_PAYLOAD), _Clip(),
                                 "https://x.test", "local-worker")
    assert after == before


def test_clip_with_empty_render_method_is_also_untouched():
    from main import _v943_maybe_charswap
    before = dict(LEGACY_PAYLOAD)
    after = _v943_maybe_charswap(dict(LEGACY_PAYLOAD), _Clip(render_method=""),
                                 "https://x.test", "local-worker")
    assert after == before


def test_charswap_clip_payload_carries_both_inputs():
    from main import _v943_maybe_charswap
    clip = _Clip(render_method="charswap", swap_mode="image-led",
                 swap_source_r2_key="swap-sources/u1/abc_curls.mp4",
                 swap_avatar_upload_id=4616)
    out = _v943_maybe_charswap(dict(LEGACY_PAYLOAD), clip,
                               "https://x.test", "local-worker")
    assert out["render_method"] == "charswap"
    assert out["swap_mode"] == "image-led"
    assert out["swap_avatar_url"] == "https://x.test/api/local-worker/swap-avatar/4616"
    assert out["swap_source_url"] == (
        "https://x.test/api/local-worker/swap-source"
        "?key=swap-sources%2Fu1%2Fabc_curls.mp4")
    assert out["swap_max_source_s"] == 10
    # everything the legacy payload said is still said
    for k, v in LEGACY_PAYLOAD.items():
        assert out[k] == v


def test_user_worker_lane_gets_its_own_urls():
    from main import _v943_maybe_charswap
    clip = _Clip(render_method="charswap", swap_mode="video-led",
                 swap_source_r2_key="swap-sources/u1/a.mp4",
                 swap_avatar_upload_id=1)
    out = _v943_maybe_charswap({}, clip, "https://x.test", "user-worker")
    assert "/api/user-worker/swap-avatar/1" in out["swap_avatar_url"]
    assert "/api/user-worker/swap-source" in out["swap_source_url"]


# --- 4. the worker picks the old handler when render_method is NULL ----------

def test_worker_branch_not_selected_without_render_method():
    selected = _worker_function("charswap_selected")
    assert selected({}) is False
    assert selected({"render_method": None}) is False
    assert selected({"render_method": ""}) is False
    assert selected({"render_method": "CHARSWAP"}) is False
    assert selected({"render_method": "veo"}) is False


def test_worker_branch_selected_only_for_charswap():
    selected = _worker_function("charswap_selected")
    assert selected({"render_method": "charswap"}) is True


def test_worker_arm_is_gated_by_that_one_function():
    """The arm must ask charswap_selected, not re-derive the condition.

    A second copy of the test is how a branch quietly widens later.
    """
    src = WORKER_SRC.read_text(encoding="utf-8")
    assert "if charswap_selected(clip):" in src
    assert "elif first_submission_in_project:" in src


# --- the CLI side: a build with no charswap uploads nothing ------------------

def test_no_swap_sources_declared_means_no_upload():
    from send_to_platform import declared_swap_sources
    assert declared_swap_sources(LEGACY_SCENE) == []


def test_swap_source_names_are_collected_once():
    from send_to_platform import declared_swap_sources
    md = CHARSWAP_SCENE + CHARSWAP_SCENE
    assert declared_swap_sources(md) == ["raw/refs/curls.mp4"]


# =============================================================================
# 5. OWNER SCOPING — a worker token reads its own user's assets, and no others
# =============================================================================
# Both swap assets used to be addressed by a caller-supplied string: an R2 key
# under a global prefix, and a small integer node id. Either one, plus any
# valid worker token, read any account's file.

class _FakeStorage:
    """Stands in for R2. Records what it was asked to do."""

    def __init__(self, payload=b"mp4-bytes", fail=False):
        self.payload = payload
        self.fail = fail
        self.downloaded_to = []
        self.uploaded = []

    def download_file(self, key, dst_path):
        self.downloaded_to.append(dst_path)
        if self.fail:
            raise RuntimeError("no such object")
        with open(dst_path, "wb") as fh:
            fh.write(self.payload)

    def upload_file(self, src_path, key, content_type=None):
        self.uploaded.append((src_path, key, content_type))


@pytest.fixture
def storage(monkeypatch):
    """Configured storage that never touches the network."""
    import backends.storage as bs
    fake = _FakeStorage()
    monkeypatch.setattr(bs, "is_storage_configured", lambda: True)
    monkeypatch.setattr(bs, "get_storage", lambda: fake)
    return fake


def test_key_owner_is_the_segment_after_the_prefix():
    from main import _v943_swap_source_key_owner as owner
    assert owner("swap-sources/u1/abc_curls.mp4") == "u1"
    assert owner("swap-sources/u1/nested/abc.mp4") == "u1"
    assert owner("jobs/u1/frames/x.png") is None
    assert owner("swap-sources/u1") is None          # no object, just a folder
    assert owner("swap-sources//abc.mp4") is None    # empty owner
    assert owner("swap-sources/u1/../u2/a.mp4") is None
    assert owner("") is None


def test_source_of_another_user_is_not_served(storage):
    """The whole point: u2's token must not read u1's source clip."""
    from fastapi import HTTPException
    from main import _v943_swap_source_response
    with pytest.raises(HTTPException) as e:
        asyncio.run(_v943_swap_source_response(
            "swap-sources/u1/abc_curls.mp4", user_id="u2"))
    assert e.value.status_code == 404
    assert storage.downloaded_to == []  # refused before any read


def test_source_of_the_token_user_is_served(storage):
    from main import _v943_swap_source_response
    resp = asyncio.run(_v943_swap_source_response(
        "swap-sources/u1/abc_curls.mp4", user_id="u1"))
    try:
        assert os.path.exists(resp.path)
        assert len(storage.downloaded_to) == 1
    finally:
        asyncio.run(resp.background())


def test_non_swap_prefix_key_is_refused_even_unscoped(storage):
    from fastapi import HTTPException
    from main import _v943_swap_source_response
    for bad in ("jobs/j1/frames/a.png", "swap-sources/../secrets/a.mp4"):
        with pytest.raises(HTTPException) as e:
            asyncio.run(_v943_swap_source_response(bad))
        assert e.value.status_code == 400
    assert storage.downloaded_to == []


class _FakeQuery:
    def __init__(self, criteria, result):
        self.criteria = criteria
        self.result = result

    def filter(self, *crit):
        self.criteria.extend(str(c) for c in crit)
        return self

    def first(self):
        return self.result


class _FakeDB:
    def __init__(self, criteria):
        self.criteria = criteria

    def query(self, *_models):
        return _FakeQuery(self.criteria, None)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_avatar_lookup_is_scoped_by_user_and_upload_kind(monkeypatch):
    """The query itself has to carry the scope — not a check after the fact."""
    import models
    from fastapi import HTTPException
    from main import _v943_swap_avatar_response
    criteria = []
    monkeypatch.setattr(models, "get_db", lambda: _FakeDB(criteria))
    with pytest.raises(HTTPException) as e:
        asyncio.run(_v943_swap_avatar_response(4616, user_id="u1"))
    assert e.value.status_code == 404
    joined = " ".join(criteria)
    assert "image_nodes.id" in joined
    assert "image_nodes.kind" in joined
    assert "image_nodes.user_id" in joined


def test_avatar_lookup_still_requires_upload_kind_without_a_user(monkeypatch):
    import models
    from fastapi import HTTPException
    from main import _v943_swap_avatar_response
    criteria = []
    monkeypatch.setattr(models, "get_db", lambda: _FakeDB(criteria))
    with pytest.raises(HTTPException):
        asyncio.run(_v943_swap_avatar_response(4616))
    joined = " ".join(criteria)
    assert "image_nodes.kind" in joined
    assert "image_nodes.user_id" not in joined


def test_user_worker_routes_pass_their_token_user_through():
    """A route that authenticates a user and drops it is the original bug."""
    import inspect
    import main
    for fn in (main.user_worker_download_swap_source,
               main.user_worker_download_swap_avatar):
        src = inspect.getsource(fn)
        assert "user_id=user_id" in src, fn.__name__


def test_import_refuses_a_source_key_from_another_user():
    """The import path takes the R2 key from the request body, so it is input."""
    import inspect
    import image_platform
    src = inspect.getsource(image_platform._import_scene_table_impl)
    assert 'f"swap-sources/{current_user.id}/"' in src
    assert "_v943_own_prefix" in src


# =============================================================================
# 6. TEMP FILE — a served download must not leave its copy on the disk
# =============================================================================

def test_served_source_temp_is_deleted_when_the_response_finishes(storage):
    from main import _v943_swap_source_response
    resp = asyncio.run(_v943_swap_source_response("swap-sources/u1/a.mp4"))
    path = resp.path
    assert os.path.exists(path)
    assert resp.background is not None, "no cleanup attached to the response"
    asyncio.run(resp.background())          # what Starlette runs after sending
    assert not os.path.exists(path)


def test_served_source_temp_is_deleted_when_the_download_fails(monkeypatch):
    import backends.storage as bs
    from fastapi import HTTPException
    from main import _v943_swap_source_response
    fake = _FakeStorage(fail=True)
    monkeypatch.setattr(bs, "is_storage_configured", lambda: True)
    monkeypatch.setattr(bs, "get_storage", lambda: fake)
    with pytest.raises(HTTPException):
        asyncio.run(_v943_swap_source_response("swap-sources/u1/a.mp4"))
    assert fake.downloaded_to, "storage was never asked"
    assert not os.path.exists(fake.downloaded_to[0])


def test_cleanup_is_quiet_about_a_file_that_is_already_gone():
    from main import _v943_unlink
    _v943_unlink(os.path.join(str(_HERE), "no-such-v943-temp.mp4"))  # no raise


# =============================================================================
# 7. BOUNDED UPLOAD — cap the bytes, then prove it is a short mp4
# =============================================================================

class _FakeUpload:
    """Only what _spool_upload_to_path touches."""

    def __init__(self, data):
        self.file = io.BytesIO(data)


class _FakeUser:
    id = "u1"


def _mp4_probe(duration=9.8, fmt="mov,mp4,m4a,3gp,3g2,mj2", video=True):
    streams = [{"codec_type": "video"}] if video else [{"codec_type": "audio"}]
    return {"format": {"format_name": fmt, "duration": str(duration)},
            "streams": streams}


def _upload(monkeypatch, data=b"x" * 1000, probe=None, name="curls.mp4"):
    import main
    monkeypatch.setattr(main, "_v943_probe_source",
                        lambda p: (probe if probe is not None else _mp4_probe()))
    return asyncio.run(main.upload_swap_source_video(
        file=_FakeUpload(data), name=name, current_user=_FakeUser()))


def test_the_caps_are_conservative_and_named():
    import main
    assert main.SWAP_SOURCE_MAX_BYTES == 80 * 1024 * 1024
    assert main.SWAP_SOURCE_RENDER_CAP_S == 10
    assert main.SWAP_SOURCE_MAX_DURATION_S == 12.0


def test_spool_stops_at_the_cap_instead_of_copying_to_eof(tmp_path):
    from main import UploadTooLarge, _spool_upload_to_path
    dst = str(tmp_path / "capped.mp4")
    with pytest.raises(UploadTooLarge):
        _spool_upload_to_path(_FakeUpload(b"x" * 5000), dst,
                              chunk_bytes=1024, max_bytes=1024)
    # the cap fires DURING the copy, so the whole upload never lands
    assert os.path.getsize(dst) <= 1024


def test_spool_without_a_cap_is_the_old_unbounded_copy(tmp_path):
    from main import _spool_upload_to_path
    dst = str(tmp_path / "plain.mp4")
    n = _spool_upload_to_path(_FakeUpload(b"x" * 5000), dst)
    assert n == 5000


def test_a_short_mp4_is_accepted_and_stored(monkeypatch, storage):
    out = _upload(monkeypatch)
    assert out["success"] is True
    assert out["r2_key"].startswith("swap-sources/u1/")
    assert out["bytes"] == 1000
    assert out["duration_s"] == 9.8
    assert len(storage.uploaded) == 1


def test_oversize_upload_is_refused_and_never_stored(monkeypatch, storage):
    import main
    from fastapi import HTTPException
    monkeypatch.setattr(main, "SWAP_SOURCE_MAX_BYTES", 1024)
    with pytest.raises(HTTPException) as e:
        _upload(monkeypatch, data=b"x" * 5000)
    assert e.value.status_code == 413
    assert storage.uploaded == []


def test_non_mp4_upload_is_refused_and_never_stored(monkeypatch, storage):
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as e:
        _upload(monkeypatch, probe=_mp4_probe(fmt="matroska,webm"))
    assert e.value.status_code == 415
    assert storage.uploaded == []


def test_a_file_with_no_video_stream_is_refused(monkeypatch, storage):
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as e:
        _upload(monkeypatch, probe=_mp4_probe(video=False))
    assert e.value.status_code == 415
    assert storage.uploaded == []


def test_a_file_ffprobe_cannot_read_is_refused(monkeypatch, storage):
    import main
    from fastapi import HTTPException

    def _boom(_p):
        raise RuntimeError("ffprobe failed")

    monkeypatch.setattr(main, "_v943_probe_source", _boom)
    with pytest.raises(HTTPException) as e:
        asyncio.run(main.upload_swap_source_video(
            file=_FakeUpload(b"not a video"), name="x.mp4",
            current_user=_FakeUser()))
    assert e.value.status_code == 415
    assert storage.uploaded == []


def test_over_duration_upload_is_refused_and_never_stored(monkeypatch, storage):
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as e:
        _upload(monkeypatch, probe=_mp4_probe(duration=31.0))
    assert e.value.status_code == 422
    assert "31.0s" in e.value.detail
    assert storage.uploaded == []


def test_a_source_slightly_over_the_render_cap_is_still_accepted(
        monkeypatch, storage):
    """10.2s is a correct cut the worker trims — not a wrong file."""
    out = _upload(monkeypatch, probe=_mp4_probe(duration=10.2))
    assert out["success"] is True
    assert len(storage.uploaded) == 1


def test_a_rejected_upload_leaves_no_temp_file_behind(monkeypatch, storage):
    import main
    import tempfile
    from fastapi import HTTPException
    made = []
    real = tempfile.NamedTemporaryFile

    def _spy(*a, **kw):
        f = real(*a, **kw)
        made.append(f.name)
        return f

    monkeypatch.setattr(tempfile, "NamedTemporaryFile", _spy)
    with pytest.raises(HTTPException):
        _upload(monkeypatch, probe=_mp4_probe(duration=31.0))
    assert made, "the route never spooled anything"
    assert not any(os.path.exists(p) for p in made)


# =============================================================================
# 8. IMAGE-LED — accepted by the grammar, refused by the worker, on purpose
# =============================================================================
# The worker attaches the same avatar image and the same source video in both
# modes, and the thing that would make image-led different (the avatar
# composited into the source's pose-matched FIRST FRAME) is an image step this
# pipeline does not have. So the mode fails closed rather than rendering
# video-led under a second name and calling the pair a comparison.

def test_video_led_is_the_mode_that_runs():
    refusal = _worker_function("charswap_mode_refusal")
    assert refusal({"swap_mode": "video-led"}) is None
    assert refusal({"swap_mode": None}) is None      # default
    assert refusal({}) is None
    assert refusal({"swap_mode": " Video-Led "}) is None


def test_image_led_is_refused_with_a_clear_reason():
    refusal = _worker_function("charswap_mode_refusal")
    msg = refusal({"swap_mode": "image-led"})
    assert msg is not None
    assert "image-led not implemented in worker" in msg
    assert "render video-led or wait" in msg


def test_an_unknown_mode_is_refused_too():
    refusal = _worker_function("charswap_mode_refusal")
    msg = refusal({"swap_mode": "sideways"})
    assert msg is not None
    assert "sideways" in msg


def test_the_arm_refuses_before_it_fetches_anything():
    """A refusal must cost nothing — no download, no browser work."""
    src = WORKER_SRC.read_text(encoding="utf-8")
    arm = src.index("if charswap_selected(clip):")
    refuse = src.index("charswap_mode_refusal(clip)", arm)
    fetch = src.index("charswap_fetch_inputs(clip", arm)
    assert refuse < fetch
    assert "update_clip_status(clip['id'], 'failed', error_message=_cs_refusal)" in src


def test_the_parser_still_accepts_image_led():
    """Fail-closed lives in the WORKER; the build grammar is unchanged."""
    md = CHARSWAP_SCENE.replace("video-led", "image-led", 1)
    assert _parse(md)[0]["swap_mode"] == "image-led"


# --- 9. the submit-body proof must fail CLOSED (rev 487) --------------------
# The probe proves both media ids (avatar + video) reached Flow's generate
# body. Printing the verdict and carrying on would let an avatar-only or
# source-only submit be accepted as a charswap render — the gate must stop it.

def test_a_proven_submit_is_accepted_and_counted():
    gate = _worker_function("charswap_submit_gate")
    assert gate(True, True) == (True, True, None)


def test_a_submit_missing_a_media_id_is_rejected_but_its_tile_counted():
    """seen=True means the request went out — Flow likely made a tile.
    Count it so tile indexing stays aligned; never accept the clip."""
    gate = _worker_function("charswap_submit_gate")
    accept, count_tile, why = gate(True, False)
    assert accept is False
    assert count_tile is True
    assert "missing both media ids" in why


def test_an_unseen_submit_is_rejected_and_not_counted():
    gate = _worker_function("charswap_submit_gate")
    accept, count_tile, why = gate(False, False)
    assert accept is False
    assert count_tile is False
    assert "no generate request observed" in why
    # both=True with seen=False is a probe inconsistency; still fail closed
    assert gate(False, True)[0] is False


def test_the_arm_acts_on_the_gate_before_the_shared_bookkeeping():
    """The gate's rejection must fail the clip and skip the normal
    submitted/generating path (the rev-487 blocker was print-and-continue)."""
    src = WORKER_SRC.read_text(encoding="utf-8")
    arm = src.index("if charswap_selected(clip):")
    verdict = src.index("charswap_submit_body_verdict(page)", arm)
    gate = src.index("charswap_submit_gate(", verdict)
    fail = src.index("charswap submit not proven", gate)
    tail = src.index("human_delay(1, 2)", gate)
    assert verdict < gate < fail < tail
    # the rejection path both fails the clip and marks it permanently failed
    rejection = src[gate:tail]
    assert "update_clip_status(clip['id'], 'failed'" in rejection
    assert "permanently_failed_clips.add(clip_index)" in rejection
    assert "continue" in rejection
