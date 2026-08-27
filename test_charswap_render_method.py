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

def test_video_led_is_the_default_mode_that_runs():
    refusal = _worker_function("charswap_mode_refusal")
    assert refusal({"swap_mode": "video-led"}) is None
    assert refusal({"swap_mode": None}) is None      # default
    assert refusal({}) is None
    assert refusal({"swap_mode": " Video-Led "}) is None


def test_image_led_runs_when_the_clip_has_a_start_frame():
    """Operator 2026-08-26: the technique is proven (A-E split test + the
    mountain final); the composite = the clip's chosen start frame."""
    refusal = _worker_function("charswap_mode_refusal")
    assert refusal({"swap_mode": "image-led",
                    "start_frame_url": "https://x/frame.png"}) is None
    assert refusal({"swap_mode": "image-led",
                    "swap_start_frame_url": "https://x/frame.png"}) is None


def test_the_refusal_accepts_only_what_the_fetcher_consumes():
    """rev 503 contract mismatch: the refusal used to pass a clip carrying a
    local `start_frame` path, but charswap_fetch_inputs reads only
    swap_start_frame_url / start_frame_url — so that clip cleared the gate and
    then died at the download. The two must agree on the same fields."""
    refusal = _worker_function("charswap_mode_refusal")
    msg = refusal({"swap_mode": "image-led", "start_frame": "jobs/x.png"})
    assert msg is not None, "a local start_frame path is not something the fetcher can use"
    assert "start frame" in msg

    src = WORKER_SRC.read_text(encoding="utf-8")
    fetch = src[src.index("\ndef charswap_fetch_inputs("):]
    fetch = fetch[:fetch.index("\ndef ", 1)]
    consumed = [f for f in ("swap_start_frame_url", "start_frame_url")
                if f in fetch]
    assert consumed == ["swap_start_frame_url", "start_frame_url"]
    gate = src[src.index("\ndef charswap_mode_refusal("):]
    gate = gate[:gate.index("\ndef ", 1)]
    accepted = gate[gate.index("if mode == 'image-led':"):]
    for field in consumed:
        assert f"clip.get('{field}')" in accepted
    assert "clip.get('start_frame')" not in accepted


def test_image_led_without_a_start_frame_is_refused():
    refusal = _worker_function("charswap_mode_refusal")
    msg = refusal({"swap_mode": "image-led"})
    assert msg is not None
    assert "start frame" in msg


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


# =============================================================================
# 10. v943.1 — SOURCE-ORIGINAL AUDIO
# =============================================================================
# The render stays silent (Flow needs a muted upload). This is an EXPORT-time
# feature: the stored source's own audio is laid back over the swap clip's
# segment. So the checks are: does the bullet parse and fail closed, does the
# column reach every surface a Clip row is built at, and does the one decision
# that drives the mux answer correctly on its own.

CHARSWAP_AUDIO_SCENE = CHARSWAP_SCENE + "- **audio:** source-original\n"


def test_legacy_scene_has_no_swap_audio():
    assert _parse(LEGACY_SCENE)[0]["swap_audio"] is None


def test_charswap_scene_without_the_bullet_has_no_swap_audio():
    assert _parse(CHARSWAP_SCENE)[0]["swap_audio"] is None


def test_charswap_scene_takes_source_original():
    assert _parse(CHARSWAP_AUDIO_SCENE)[0]["swap_audio"] == "source-original"


def test_charswap_scene_takes_none_explicitly():
    md = CHARSWAP_SCENE + "- **audio:** none\n"
    assert _parse(md)[0]["swap_audio"] == "none"


def test_the_value_is_case_and_space_insensitive():
    md = CHARSWAP_SCENE + "- **audio:**   Source-Original   \n"
    assert _parse(md)[0]["swap_audio"] == "source-original"


def test_an_unknown_audio_value_hard_fails():
    md = CHARSWAP_SCENE + "- **audio:** original-mix\n"
    with pytest.raises(ValueError, match="audio"):
        _parse(md)


def test_audio_on_a_non_charswap_scene_hard_fails():
    """A normal Veo clip has its own audio and no stored source to take a
    track from, so the bullet there is a mistake, not a no-op."""
    md = LEGACY_SCENE + "- **audio:** source-original\n"
    with pytest.raises(ValueError, match="charswap"):
        _parse(md)


def test_even_audio_none_on_a_non_charswap_scene_hard_fails():
    md = LEGACY_SCENE + "- **audio:** none\n"
    with pytest.raises(ValueError, match="charswap"):
        _parse(md)


def _function_source(path, name):
    """Source text of ONE top-level function, so a match cannot leak in."""
    import ast
    src = path.read_text(encoding="utf-8")
    tree = ast.parse(src)
    for n in ast.walk(tree):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name:
            return ast.get_source_segment(src, n) or ""
    raise AssertionError(f"{name}() not found in {path.name}")


# Every surface that builds a per-clip dict or a Clip row already names
# swap_avatar_upload_id. Deriving the expectation from that, instead of listing
# the sites here, means a new build path starts being checked the day it lands.
@pytest.mark.parametrize("filename,func", [
    ("image_platform.py", "_import_scene_table_impl"),
    ("image_platform.py", "prepare_batch_for_video"),
    ("image_platform.py", "promote_batch_to_video"),
    ("main.py", "_create_job_impl"),
])
def test_swap_audio_reaches_every_clip_building_surface(filename, func):
    src = _function_source(_HERE / filename, func)
    assert "swap_avatar_upload_id" in src, "fixture is out of date"
    assert src.count("swap_audio") >= 1, (
        f"{func}() carries the other charswap fields but drops swap_audio — "
        f"the column would reach the Clip row as NULL")


def test_promote_carries_swap_audio_on_both_dicts_and_the_clip_row():
    """promote_batch_to_video builds three things per clip: the stored
    dialogue entry, the clip spec, and the Clip row itself."""
    src = _function_source(_HERE / "image_platform.py", "promote_batch_to_video")
    assert src.count('"swap_audio": (') == 2          # dialogue_list + clip_specs
    assert 'swap_audio=spec.get("swap_audio")' in src  # Clip(...)


def test_the_browser_promote_payload_sends_swap_audio_too():
    """The field-plumbing checker is satisfied by EITHER promote path, so a
    miss in index.html passes the checker and still arrives NULL on every job
    promoted from the UI — the v892.2 failure exactly."""
    src = (_HERE / "static" / "index.html").read_text(encoding="utf-8")
    assert "swap_avatar_upload_id:" in src, "fixture is out of date"
    assert "swap_audio: promoteMeta.swap_audio" in src


def test_the_column_is_in_the_readback_contract():
    from image_platform import CHARSWAP_COLUMNS
    assert "swap_audio" in CHARSWAP_COLUMNS["clips"]
    assert "swap_audio" in CHARSWAP_COLUMNS["image_scene_assignments"]


def test_both_migration_dialects_register_the_column():
    src = (_HERE / "image_platform.py").read_text(encoding="utf-8")
    for table in ("image_scene_assignments", "clips"):
        assert f"ALTER TABLE {table} ADD COLUMN swap_audio VARCHAR(20)" in src
        assert (f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "
                f"swap_audio VARCHAR(20)") in src


# --- the mux decision, on its own, with no ffmpeg and no R2 ------------------

def _clip(**kw):
    base = {"render_method": None, "swap_audio": None, "swap_source_r2_key": None}
    base.update(kw)
    return base


def test_a_legacy_clip_never_gets_source_audio():
    from main import charswap_export_audio_key
    assert charswap_export_audio_key(_clip(), "7") is None


def test_a_swap_clip_that_did_not_ask_stays_silent():
    from main import charswap_export_audio_key
    assert charswap_export_audio_key(_clip(
        render_method="charswap", swap_audio="none",
        swap_source_r2_key="swap-sources/7/abc.mp4"), "7") is None
    assert charswap_export_audio_key(_clip(
        render_method="charswap",
        swap_source_r2_key="swap-sources/7/abc.mp4"), "7") is None


def test_a_swap_clip_that_asked_returns_its_source_key():
    from main import charswap_export_audio_key
    assert charswap_export_audio_key(_clip(
        render_method="charswap", swap_audio="source-original",
        swap_source_r2_key="swap-sources/7/abc.mp4"), "7"
    ) == "swap-sources/7/abc.mp4"


def test_asking_without_a_stored_source_is_silent_not_an_error():
    from main import charswap_export_audio_key
    assert charswap_export_audio_key(_clip(
        render_method="charswap", swap_audio="source-original"), "7") is None
    assert charswap_export_audio_key(_clip(
        render_method="charswap", swap_audio="source-original",
        swap_source_r2_key="   "), "7") is None


def test_a_normal_clip_asking_for_source_audio_is_still_silent():
    """Belt and braces on the parser's refusal: even if a row somehow carries
    the flag without render_method, the export must not go looking for a key."""
    from main import charswap_export_audio_key
    assert charswap_export_audio_key(_clip(
        swap_audio="source-original",
        swap_source_r2_key="swap-sources/7/abc.mp4"), "7") is None


def test_the_decision_reads_a_clip_object_too():
    from main import charswap_export_audio_key

    class _Row:
        render_method = "charswap"
        swap_audio = "Source-Original"
        swap_source_r2_key = "swap-sources/7/abc.mp4"

    assert charswap_export_audio_key(_Row(), "7") == "swap-sources/7/abc.mp4"
    assert charswap_export_audio_key(_Row(), 7) == "swap-sources/7/abc.mp4"


def test_the_mux_copies_the_video_and_re_encodes_only_the_audio():
    from main import _v943_1_mux_argv
    argv = _v943_1_mux_argv("render.mp4", "source.mp4", "out.mp4")
    assert argv[-1] == "out.mp4"
    assert argv[argv.index("-i") + 1] == "render.mp4"
    assert argv[argv.index("-i", argv.index("-i") + 1) + 1] == "source.mp4"
    # video from the render, audio from the source
    assert "0:v:0" in argv and "1:a:0" in argv
    # the render is NOT encoded a second time
    assert argv[argv.index("-c:v") + 1] == "copy"
    assert argv[argv.index("-c:a") + 1] == "aac"
    assert "-shortest" in argv


def test_the_export_runs_the_pass_before_the_concat_pipeline():
    """The mux has to happen while each clip is still its own file. After the
    concat, VAD and the speed pass have moved every segment boundary."""
    src = _function_source(_HERE / "main.py", "_do_export_final_impl")
    download = src.index("pool.map(_download_clip")
    mux = src.index("_v943_1_apply_source_audio", download)
    concat = src.index("process_export,", mux)
    assert download < mux < concat


def test_a_source_with_no_audio_never_fails_the_export():
    """The whole pass is wrapped so one bad clip cannot take the video down."""
    src = _function_source(_HERE / "main.py", "_v943_1_apply_source_audio")
    assert "NO audio stream" in src
    assert "export continues" in src
    # the temp source and the temp output are both cleaned in `finally`
    assert "finally:" in src


# =============================================================================
# 11. rev 503 — the five blockers Codex found in the live swap/audio code
# =============================================================================
# Each block below pins ONE of them. They are grouped by blocker, not by file,
# because that is how the review reads: "this cannot happen again", not "this
# line says the right thing".


# --- 11.1 a charswap on a non-Omni model must stop BEFORE the attach --------
# _omni_ingredients_mode returns False for every model that is not Omni, so
# set_clip_input_mode lands on Frames — a tab with no add_2 Create button at
# all. The arm used to print a warning and walk into that known failure,
# burning a render slot on a certainty.

def test_a_composer_left_on_frames_never_reaches_the_attach():
    src = WORKER_SRC.read_text(encoding="utf-8")
    arm = src.index("if charswap_selected(clip):")
    mode = src.index("_cs_mode = set_clip_input_mode(", arm)
    guard = src.index("if _cs_mode != 'Ingredients':", mode)
    attach = src.index("charswap_attach_and_prompt(", guard)
    assert mode < guard < attach

    # everything between the guard and the attach is the refusal, and it has
    # to end the iteration rather than fall through
    refusal = src[guard:attach]
    assert "FAILED CLOSED" in refusal
    assert "update_clip_status(clip['id'], 'failed'" in refusal
    assert "permanently_failed_clips.add(clip_index)" in refusal
    assert "continue" in refusal
    # and it has to say WHY, naming the model restriction
    assert "Omni" in refusal
    assert "Ingredients" in refusal


def test_the_non_omni_models_are_the_ones_that_land_on_frames():
    """The guard is only worth anything because this is still true: no model
    other than Omni gets the Ingredients tab, whatever the clip asks for."""
    src = WORKER_SRC.read_text(encoding="utf-8")
    ns = {}
    for name in ("is_omni", "_omni_ingredients_mode"):
        start = src.index(f"\ndef {name}(")
        rest = src[start + 1:]
        exec(rest[:rest.index("\ndef ", 1)], ns)  # noqa: S102 — our own file
    omni = ns["_omni_ingredients_mode"]

    class _Page:
        def __init__(self, model):
            self._veo_model = model
            # what the charswap arm's set_clip_input_mode(page, True, True) sets
            self._clip_has_end_frame = True

    assert omni(_Page("Veo 3.1 - Omni Flash")) is True
    for other in ("Veo 3.1 - Quality", "Veo 3.1 - Fast", "Veo 2", "", None):
        assert omni(_Page(other)) is False, other


# --- 11.2a a foreign swap source is refused at JOB CREATION -----------------

def test_job_creation_refuses_a_swap_source_key_from_another_user():
    """POST /api/jobs takes swap_source_r2_key from the request body, so it is
    caller input. The image import already guards it; this route did not."""
    src = _function_source(_HERE / "main.py", "_create_job_impl")
    guard = src.index('_v943_own_prefix = f"swap-sources/{current_user.id}/"')
    creation = src.index("job = Job(")
    assert guard < creation, "the guard has to run before anything persists"
    block = src[guard:creation]
    assert "swap_source_r2_key" in block
    assert "status_code=400" in block
    assert "v943 owner scoping" in block
    assert '".." in _key' in block


def test_the_two_owner_guards_use_the_same_prefix_wording():
    """Same rule in both places, so one cannot drift into a different shape."""
    import inspect

    import image_platform
    ip = inspect.getsource(image_platform._import_scene_table_impl)
    mn = _function_source(_HERE / "main.py", "_create_job_impl")
    needle = 'f"swap-sources/{current_user.id}/"'
    assert needle in ip and needle in mn


# --- 11.2b and independently at the EXPORT read ----------------------------
# The worker download route is owner-scoped; this export read was not. The
# check lives inside the pure decision function on purpose: it lands before
# storage.download_file is ever called.

def test_the_export_refuses_a_source_stored_under_another_user():
    from main import charswap_export_audio_key
    row = _clip(render_method="charswap", swap_audio="source-original",
                swap_source_r2_key="swap-sources/u1/abc.mp4")
    assert charswap_export_audio_key(row, "u1") == "swap-sources/u1/abc.mp4"
    assert charswap_export_audio_key(row, "u2") is None
    assert charswap_export_audio_key(row, None) is None
    # a key outside the swap-sources prefix is not ownable at all
    assert charswap_export_audio_key(
        _clip(render_method="charswap", swap_audio="source-original",
              swap_source_r2_key="jobs/u1/outputs/a.mp4"), "u1") is None
    assert charswap_export_audio_key(
        _clip(render_method="charswap", swap_audio="source-original",
              swap_source_r2_key="swap-sources/u1/../u2/a.mp4"), "u1") is None


def test_a_foreign_key_is_refused_before_any_storage_read(tmp_path):
    """The end-to-end shape of the same thing: run the real pass with a real
    fake storage and prove it was never asked for the file."""
    import backends.storage as bs
    import main as _main

    fake = _FakeStorage()
    original = (bs.is_storage_configured, bs.get_storage)
    bs.is_storage_configured = lambda: True
    bs.get_storage = lambda: fake
    try:
        class _Row:
            id = 1
            render_method = "charswap"
            swap_audio = "source-original"
            swap_source_r2_key = "swap-sources/u1/abc.mp4"

        clip_file = tmp_path / "clip.mp4"
        clip_file.write_bytes(b"not-really-a-render")
        rows = [{"_clip_db_id": 1, "path": clip_file}]
        done = _main._v943_1_apply_source_audio(
            [_Row()], rows, "u2", str(tmp_path))
        assert done == 0
        assert fake.downloaded_to == [], "storage was read for a foreign key"
        assert rows[0]["path"] == clip_file      # lineup untouched
    finally:
        bs.is_storage_configured, bs.get_storage = original


def test_the_export_passes_the_jobs_user_into_the_audio_pass():
    src = _function_source(_HERE / "main.py", "_do_export_final_impl")
    call = src.index("_v943_1_apply_source_audio,")
    tail = src[call:call + 300]
    assert "job.user_id" in tail


# --- 11.3 the export must not rewrite the canonical clip file ---------------
# _download_clip writes output_dir/clip.output_filename and re-downloads only
# when it is missing, and the per-clip output endpoint serves that same file
# under an immutable URL. Muxing over it changed what a clip URL returned and
# poisoned every later export on the instance.

def _make_silent_video(path, seconds, size="128x128"):
    import subprocess
    from video_processor import FFMPEG_BIN
    subprocess.run([
        FFMPEG_BIN, "-y", "-loglevel", "error",
        "-f", "lavfi", "-i", f"color=c=blue:s={size}:d={seconds}:r=24",
        "-c:v", "libx264", "-pix_fmt", "yuv420p", "-t", f"{seconds}",
        str(path)], check=True)
    return path


def _make_sounded_video(path, seconds, size="128x128"):
    import subprocess
    from video_processor import FFMPEG_BIN
    subprocess.run([
        FFMPEG_BIN, "-y", "-loglevel", "error",
        "-f", "lavfi", "-i", f"color=c=green:s={size}:d={seconds}:r=24",
        "-f", "lavfi", "-i", f"sine=frequency=440:duration={seconds}",
        "-c:v", "libx264", "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-shortest", "-t", f"{seconds}",
        str(path)], check=True)
    return path


def _probe(path):
    from video_processor import ffprobe_json
    return ffprobe_json(pathlib.Path(str(path)))


def _duration(path):
    from video_processor import get_duration
    return float(get_duration(_probe(path)))


def _has_audio(path):
    return any((s.get("codec_type") or "") == "audio"
               for s in (_probe(path).get("streams") or []))


def _mean_volume_db(path):
    """Mean volume of the file's audio, or None when it has no audio at all.

    Stream presence alone cannot answer "did the track survive": concat_videos
    gives every output an audio stream, so a dropped track comes back as a
    SILENT one, not a missing one. A 440Hz sine reads around -3dB; digital
    silence reads -91dB.
    """
    import re
    import subprocess
    from video_processor import FFMPEG_BIN
    if not _has_audio(path):
        return None
    proc = subprocess.run(
        [FFMPEG_BIN, "-hide_banner", "-i", str(path),
         "-af", "volumedetect", "-f", "null", "-"],
        capture_output=True, text=True)
    m = re.search(r"mean_volume:\s*(-?[\d.]+) dB", (proc.stderr or ""))
    assert m, f"volumedetect said nothing: {(proc.stderr or '')[-400:]}"
    return float(m.group(1))


def test_the_mux_leaves_the_canonical_clip_byte_identical(tmp_path):
    import hashlib

    import backends.storage as bs
    import main as _main

    canonical_dir = tmp_path / "outputs"
    canonical_dir.mkdir()
    canonical = _make_silent_video(canonical_dir / "clip_0.mp4", 2)
    before = hashlib.sha256(canonical.read_bytes()).hexdigest()

    source = _make_sounded_video(tmp_path / "source.mp4", 2)
    payload = source.read_bytes()

    export_dir = tmp_path / "export_scoped"
    export_dir.mkdir()

    fake = _FakeStorage(payload=payload)
    original = (bs.is_storage_configured, bs.get_storage)
    bs.is_storage_configured = lambda: True
    bs.get_storage = lambda: fake
    try:
        class _Row:
            id = 42
            render_method = "charswap"
            swap_audio = "source-original"
            swap_source_r2_key = "swap-sources/u1/abc.mp4"

        rows = [{"_clip_db_id": 42, "path": canonical}]
        done = _main._v943_1_apply_source_audio(
            [_Row()], rows, "u1", str(export_dir))
    finally:
        bs.is_storage_configured, bs.get_storage = original

    assert done == 1
    # the canonical file is untouched, byte for byte
    assert hashlib.sha256(canonical.read_bytes()).hexdigest() == before
    assert not _has_audio(canonical)
    # and only THIS export run's lineup follows the muxed copy
    muxed = pathlib.Path(rows[0]["path"])
    assert muxed != canonical
    assert muxed.parent == export_dir
    assert _has_audio(muxed)
    assert rows[0]["swap_audio_restored"] is True


def test_the_export_copies_are_cleaned_up_after_the_run():
    """v945.3 — the directory is now created and removed by the wrapper, so
    the cleanup covers the whole call instead of only the part below the old
    creation point."""
    src = _function_source(_HERE / "main.py", "_do_export_final")
    made = src.index("mkdtemp(")
    call = src.index("_do_export_final_impl(", made)
    fin = src.index("finally:", call)
    cleanup = src.index("rmtree(export_tmp_dir", fin)
    assert made < call < fin < cleanup
    # nothing may run between the create and the try that owns it
    assert "try:" in src[made:call]


# --- 11.4 short source audio must not truncate the picture -----------------

def test_the_mux_pads_the_audio_and_cuts_at_the_render_length():
    from main import _v943_1_mux_argv
    argv = _v943_1_mux_argv("render.mp4", "source.mp4", "out.mp4",
                            render_duration=4.0)
    assert argv[argv.index("-af") + 1] == "apad"
    assert argv[argv.index("-t") + 1] == "4.000000"
    # with an explicit length, -shortest would defeat the padding
    assert "-shortest" not in argv
    # the video is still copied, never re-encoded
    assert argv[argv.index("-c:v") + 1] == "copy"


def test_an_unprobeable_render_still_stops_the_pad_somewhere():
    """apad without a length runs forever. When the duration could not be
    read, -shortest is the only stop left — worse output, never a hang."""
    from main import _v943_1_mux_argv
    for bad in (None, 0, 0.0, "", "nonsense"):
        argv = _v943_1_mux_argv("r.mp4", "s.mp4", "o.mp4", render_duration=bad)
        assert "-shortest" in argv, bad
        assert "-t" not in argv, bad


def test_two_seconds_of_audio_over_a_four_second_render_keeps_four_seconds(tmp_path):
    """The real thing, through ffmpeg. `-shortest` alone cut this to 2s."""
    import subprocess

    render = _make_silent_video(tmp_path / "render.mp4", 4)
    source = _make_sounded_video(tmp_path / "source.mp4", 2)
    out = tmp_path / "out.mp4"

    from main import _v943_1_mux_argv, _v943_1_render_duration
    dur = _v943_1_render_duration(render)
    assert 3.8 < dur < 4.2, dur
    proc = subprocess.run(
        _v943_1_mux_argv(render, source, out, render_duration=dur),
        capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr[-500:]
    assert 3.8 < _duration(out) < 4.2, "the picture was truncated to the audio"
    assert _has_audio(out)


def test_longer_audio_than_the_render_is_trimmed_not_stretched(tmp_path):
    import subprocess

    render = _make_silent_video(tmp_path / "render.mp4", 2)
    source = _make_sounded_video(tmp_path / "source.mp4", 5)
    out = tmp_path / "out.mp4"

    from main import _v943_1_mux_argv, _v943_1_render_duration
    proc = subprocess.run(
        _v943_1_mux_argv(render, source, out,
                         render_duration=_v943_1_render_duration(render)),
        capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr[-500:]
    assert 1.8 < _duration(out) < 2.3


# --- 11.5 timeline retiming must not throw the restored track away ----------

def test_the_atempo_chain_covers_speeds_ffmpeg_will_not_take_in_one_step():
    from video_processor import _v943_1_atempo_chain
    assert _v943_1_atempo_chain(1.0) == "atempo=1.000000"
    assert _v943_1_atempo_chain(2.0) == "atempo=2.000000"
    # below 0.5 ffmpeg refuses a single atempo, so it has to chain
    assert _v943_1_atempo_chain(0.25) == "atempo=0.5,atempo=0.500000"
    assert _v943_1_atempo_chain(0.1).count("atempo=") >= 2
    # nonsense in, a harmless pass-through out
    for bad in (0, -1, None, "x"):
        assert _v943_1_atempo_chain(bad) == "atempo=1.000000"


def test_a_clip_without_restored_audio_keeps_the_old_video_only_retime():
    """Nothing changes for the clips this feature does not touch."""
    src = _function_source(_HERE / "video_processor.py", "export_final_video")
    branch = src.index("[VideoProcessor/v888] clip ")
    block = src[branch:branch + 2500]
    assert 'info.get("swap_audio_restored")' in block
    assert '"-an"' in block, "the silent path must still pass -an"
    assert '"-map", "[a]"' in block


def test_a_retimed_clip_with_restored_audio_keeps_its_track(tmp_path):
    """Integration: run the real export trim pass over a sounded clip that has
    to be stretched into a longer timeline slot, and probe the result."""
    import video_processor as vp

    sounded = _make_sounded_video(tmp_path / "clip.mp4", 2)
    out = tmp_path / "final.mp4"

    info = {
        "path": sounded,
        "clip_index": 0,
        "cut_mode": "timeline",
        "target_duration_s": 3.0,     # longer than the source -> v888 retime
        "swap_audio_restored": True,
        "dialogue_text": "",
        "skip_start_trim": True,
    }
    vp.export_final_video(
        clip_info=[info], output_path=out,
        frames_to_cut_start=0, frames_to_cut_end=0,
        remove_silence=False, dialogue_texts=[""])

    assert out.exists()
    assert 2.7 < _duration(out) < 3.3, _duration(out)
    assert _has_audio(out), "the retime branch dropped the restored track"
    vol = _mean_volume_db(out)
    assert vol is not None and vol > -50, f"the track is there but silent ({vol} dB)"


def test_the_same_retime_without_the_flag_is_still_silent(tmp_path):
    """Proves the previous test measured the flag, not ffmpeg being generous.

    The final file always HAS an audio stream — concat_videos gives every
    output one. Without the flag that stream is digital silence."""
    import video_processor as vp

    sounded = _make_sounded_video(tmp_path / "clip.mp4", 2)
    out = tmp_path / "final.mp4"
    vp.export_final_video(
        clip_info=[{
            "path": sounded, "clip_index": 0, "cut_mode": "timeline",
            "target_duration_s": 3.0, "dialogue_text": "",
            "skip_start_trim": True,
        }],
        output_path=out, frames_to_cut_start=0, frames_to_cut_end=0,
        remove_silence=False, dialogue_texts=[""])
    assert out.exists()
    vol = _mean_volume_db(out)
    assert vol is None or vol < -60, f"expected silence, measured {vol} dB"


# =============================================================================
# 12. v945.3 — the live failure: a Generate click that submitted nothing
# =============================================================================
# Job 5f1eef9d attached both chips, logged "✓ Prompt pasted via clipboard",
# logged "✓ Clicked Generate button", and produced ZERO network requests. A
# click on a DISABLED button resolves happily, so the log said success for a
# no-op. The arm now reads the prompt box back and checks the button's real
# state before clicking; the two decisions are pure functions so they can be
# checked here, and the DOM parts stay thin enough to read.


def test_an_unreadable_prompt_box_is_not_treated_as_a_good_one():
    """None means "could not read", which is the whole point of the check —
    the old code inferred success from the paster's own log line."""
    act = _worker_function("charswap_prompt_readback_action")
    action, why = act(None, "a real prompt")
    assert action == "retype"
    assert "could not be read" in why


def test_an_empty_prompt_box_asks_for_a_retype():
    act = _worker_function("charswap_prompt_readback_action")
    action, why = act("", "a real prompt")
    assert action == "retype"
    assert "empty" in why


def test_a_partially_pasted_prompt_asks_for_a_retype():
    act = _worker_function("charswap_prompt_readback_action")
    action, why = act("a real", "a real prompt that is much longer than this")
    assert action == "retype"
    assert "/" in why  # names got/wanted so the log is diagnosable


def test_a_prompt_that_read_back_whole_is_accepted():
    act = _worker_function("charswap_prompt_readback_action")
    prompt = "swap the man in the video for the man in the photo"
    action, why = act(prompt, prompt)
    assert action == "ok"
    assert str(len(prompt)) in why


def test_no_prompt_at_all_is_not_a_retype_loop():
    act = _worker_function("charswap_prompt_readback_action")
    assert act(None, "")[0] == "ok"


def test_a_disabled_generate_button_is_refused_and_says_the_visible_state():
    """The failure this whole item exists for: everything attached, prompt
    full, button dead. The reason has to carry the numbers or the next log is
    as unreadable as tonight's."""
    ready_fn = _worker_function("charswap_generate_readiness")
    ready, why = ready_fn(False, 120, 2)
    assert ready is False
    assert "disabled" in why
    assert "120" in why and "2" in why


def test_an_empty_prompt_is_refused_before_the_button_is_even_blamed():
    ready_fn = _worker_function("charswap_generate_readiness")
    ready, why = ready_fn(True, 0, 2)
    assert ready is False
    assert "empty" in why


def test_one_chip_is_refused_even_with_a_full_prompt_and_a_live_button():
    ready_fn = _worker_function("charswap_generate_readiness")
    ready, why = ready_fn(True, 120, 1)
    assert ready is False
    assert "1 chip" in why


def test_both_chips_a_full_prompt_and_a_live_button_is_the_only_ready_state():
    ready_fn = _worker_function("charswap_generate_readiness")
    ready, why = ready_fn(True, 120, 2)
    assert ready is True
    assert "enabled" in why


def test_the_readiness_check_survives_junk_counts():
    """chip ids come from page.evaluate and the prompt length from inner_text;
    neither is worth trusting to be an int."""
    ready_fn = _worker_function("charswap_generate_readiness")
    assert ready_fn(True, None, None)[0] is False
    assert ready_fn(True, "120", "2")[0] is True


def test_the_prompt_box_reader_returns_none_when_the_dom_read_throws():
    read = _worker_function("charswap_prompt_box_text")

    class _Boom:
        def locator(self, _sel):
            raise RuntimeError("page closed")

    assert read(_Boom()) is None


def test_the_prompt_box_reader_returns_none_when_there_is_no_box():
    read = _worker_function("charswap_prompt_box_text")

    class _Box:
        def count(self):
            return 0

    class _Page:
        def locator(self, _sel):
            return type("L", (), {"first": _Box()})()

    assert read(_Page()) is None


def test_the_prompt_box_reader_strips_what_the_dom_shows():
    read = _worker_function("charswap_prompt_box_text")

    class _Box:
        def count(self):
            return 1

        def inner_text(self):
            return "  swap the man  \n"

    class _Page:
        def locator(self, _sel):
            return type("L", (), {"first": _Box()})()

    assert read(_Page()) == "swap the man"


def test_the_arm_runs_before_the_click_and_can_stop_it():
    """Source-level, because the ordering is the fix: read back and verify
    BEFORE click_generate_button, and refuse rather than click."""
    src = WORKER_SRC.read_text(encoding="utf-8")
    fn = src.index("\ndef charswap_attach_and_prompt(")
    end = src.index("\ndef ", fn + 1)
    body = src[fn:end]
    paste = body.index("fill_prompt_textarea(")
    arm = body.index("charswap_arm_generate(", paste)
    assert paste < arm
    assert "return False, chip_ids" in body[arm:]

    arm_site = src.index("_cs_ok, _cs_chips = charswap_attach_and_prompt(")
    click = src.index("click_generate_button(page,", arm_site)
    refusal = src[arm_site:click]
    assert "if not _cs_ok:" in refusal
    assert "continue" in refusal


def test_the_submit_body_gate_is_still_the_last_line():
    """The new check stops one failure mode; it does not replace the proof
    that both media ids reached the generate body."""
    src = WORKER_SRC.read_text(encoding="utf-8")
    arm = src.index("_cs_ok, _cs_chips = charswap_attach_and_prompt(")
    click = src.index("click_generate_button(page,", arm)
    gate = src.index("charswap_submit_gate(", click)
    assert click < gate
    # v945.4 widened this window: post-click forensics (state dump, toast
    # scrape, button census, screenshot) now sit between the gate and the
    # FAILED CLOSED line. The ordering claim is unchanged.
    assert "FAILED CLOSED" in src[gate:gate + 3500]


def test_the_refusal_reason_reaches_the_clip_status():
    """"charswap ingredients did not attach" was printed for a composer whose
    chips were both there. The status now carries the real reason."""
    src = WORKER_SRC.read_text(encoding="utf-8")
    arm = src.index("_cs_ok, _cs_chips = charswap_attach_and_prompt(")
    click = src.index("click_generate_button(page,", arm)
    refusal = src[arm:click]
    assert "_charswap_block_reason" in refusal
    assert "flow_redo_queued" in refusal


# =============================================================================
# 13. v945.3 — the Ingredients guard has to OBSERVE the tab, not compute it
# =============================================================================
# set_clip_input_mode returned the mode it derived from the model name. When
# the switch threw it logged the failure and returned 'Ingredients' anyway, so
# the charswap arm's fail-closed `!= 'Ingredients'` gate passed on a composer
# nobody had looked at.


def _set_clip_input_mode_ns():
    """set_clip_input_mode plus everything it calls, executed from the real
    source. The tab-switch call is replaced by the test so the DOM stays out
    of it — what is under test is which string comes back."""
    src = WORKER_SRC.read_text(encoding="utf-8")
    ns = {}
    for name in ("is_omni", "_omni_ingredients_mode",
                 "resolve_observed_input_mode", "input_mode_from_tab_states",
                 "set_clip_input_mode"):
        start = src.index(f"\ndef {name}(")
        rest = src[start + 1:]
        exec(rest[:rest.index("\ndef ", 1)], ns)  # noqa: S102 — our own file
    return ns


class _ModePage:
    def __init__(self, model="Omni Flash"):
        self._veo_model = model


def test_a_switch_that_throws_no_longer_reports_ingredients():
    ns = _set_clip_input_mode_ns()

    def _boom(page, **kwargs):
        raise RuntimeError("dropdown never opened")

    ns["select_frames_to_video_mode"] = _boom
    page = _ModePage()
    assert ns["set_clip_input_mode"](page, True, True) == "Unverified"


def test_a_switch_that_cannot_be_observed_reports_unverified():
    """The tab click "worked" but the DOM would not say which tab is on."""
    ns = _set_clip_input_mode_ns()

    def _switch(page, **kwargs):
        page._input_mode_applied = "Ingredients"
        page._input_mode_observed = None  # observe_input_mode_tab read nothing

    ns["select_frames_to_video_mode"] = _switch
    assert ns["set_clip_input_mode"](_ModePage(), True, True) == "Unverified"


def test_an_observed_ingredients_tab_is_the_only_pass():
    ns = _set_clip_input_mode_ns()

    def _switch(page, **kwargs):
        page._input_mode_applied = "Ingredients"
        page._input_mode_observed = "Ingredients"

    ns["select_frames_to_video_mode"] = _switch
    assert ns["set_clip_input_mode"](_ModePage(), True, True) == "Ingredients"


def test_an_observation_that_disagrees_with_the_computed_mode_wins():
    """The click landed on Frames. The old code would still have said
    'Ingredients' because that is what the model name implied."""
    ns = _set_clip_input_mode_ns()

    def _switch(page, **kwargs):
        page._input_mode_applied = "Ingredients"
        page._input_mode_observed = "Frames"

    ns["select_frames_to_video_mode"] = _switch
    assert ns["set_clip_input_mode"](_ModePage(), True, True) == "Frames"


def test_a_no_op_switch_reports_the_last_real_observation():
    """Second charswap clip in a row: the dropdown is not re-opened, so the
    answer is the observation from when the tab WAS clicked."""
    ns = _set_clip_input_mode_ns()
    ns["select_frames_to_video_mode"] = lambda page, **kw: pytest.fail(
        "a no-op switch must not re-open the dropdown")
    page = _ModePage()
    page._input_mode_applied = "Ingredients"
    page._input_mode_observed = "Frames"
    assert ns["set_clip_input_mode"](page, True, True) == "Frames"


def test_a_no_op_switch_with_no_observation_at_all_is_unverified():
    """An older call path stamped _input_mode_applied without ever looking."""
    ns = _set_clip_input_mode_ns()
    ns["select_frames_to_video_mode"] = lambda page, **kw: None
    page = _ModePage()
    page._input_mode_applied = "Ingredients"
    assert ns["set_clip_input_mode"](page, True, True) == "Unverified"


def test_a_stale_observation_cannot_survive_a_real_switch():
    """Clip N observed Ingredients; clip N+1's switch fails to observe. The
    old value must not be read as clip N+1's proof."""
    ns = _set_clip_input_mode_ns()

    def _switch(page, **kwargs):
        pass  # observes nothing

    ns["select_frames_to_video_mode"] = _switch
    page = _ModePage()
    page._input_mode_applied = "Frames"        # forces a real switch
    page._input_mode_observed = "Ingredients"  # stale, from the last clip
    assert ns["set_clip_input_mode"](page, True, True) == "Unverified"


@pytest.mark.parametrize("ing,frames,expect", [
    ("true", "false", "Ingredients"),
    (True, False, "Ingredients"),
    ("false", "true", "Frames"),
    ("false", "false", None),   # neither claims to be selected
    ("true", "true", None),     # both do — the DOM is not saying
    (None, None, None),
])
def test_the_tab_state_reader_only_answers_when_the_dom_is_clear(ing, frames, expect):
    fn = _worker_function("input_mode_from_tab_states")
    assert fn(ing, frames) == expect


def test_anything_that_is_not_a_real_tab_name_becomes_unverified():
    fn = _worker_function("resolve_observed_input_mode")
    assert fn("Ingredients") == "Ingredients"
    assert fn("Frames") == "Frames"
    for junk in (None, "", "ingredients", "Unverified", 0, []):
        assert fn(junk) == "Unverified", junk


def test_the_observation_is_taken_while_the_dropdown_is_still_open():
    """observe_input_mode_tab reads the settings-dropdown tab elements, which
    leave the DOM on close — so the call has to sit next to the click."""
    src = WORKER_SRC.read_text(encoding="utf-8")
    start = src.index("\ndef _click_input_mode_tab(")
    body = src[start:src.index("\ndef ", start + 1)]
    assert body.count("observe_input_mode_tab(page)") == 2  # both branches
    assert body.index("_input_mode_applied = 'Ingredients'") < body.index(
        "observe_input_mode_tab(page)")


def test_the_arm_still_fails_closed_on_anything_but_ingredients():
    """The gate is unchanged equality — which is why 'Unverified' works."""
    src = WORKER_SRC.read_text(encoding="utf-8")
    arm = src.index("if charswap_selected(clip):")
    guard = src.index("if _cs_mode != 'Ingredients':", arm)
    attach = src.index("charswap_attach_and_prompt(", guard)
    refusal = src[guard:attach]
    assert "Unverified" in refusal  # named as its own case, not a model fault
    assert "permanently_failed_clips.add(clip_index)" in refusal
    assert "continue" in refusal


# =============================================================================
# 14. v945.3 — the direct batch promote dropped veo_model
# =============================================================================
# promote_batch_to_video built config_json with no veo_model, so the worker
# fell back to its own default (Veo 3.1 Lite) and the v945.2 Ingredients gate
# then failed every charswap clip closed BY CONSTRUCTION. The browser promote
# goes through main.py and was never affected.


def test_a_promote_with_no_charswap_scene_stamps_nothing():
    from image_platform import _v943_charswap_veo_model as pick
    assert pick(None, False) == (None, None)
    assert pick("Veo 3.1 - Lite [Lower Priority]", False) == (None, None)


def test_a_charswap_scene_stamps_the_omni_model():
    from image_platform import V943_CHARSWAP_VEO_MODEL, _v943_charswap_veo_model
    model, conflict = _v943_charswap_veo_model(None, True)
    assert model == V943_CHARSWAP_VEO_MODEL
    assert conflict is None
    assert "omni" in model.lower()


def test_a_config_already_on_omni_is_left_exactly_as_it_is():
    """Suffixed names exist ("Omni Flash [Beta]") and flow_worker.is_omni
    matches loosely — rewriting the string would be a change nobody asked
    for."""
    from image_platform import _v943_charswap_veo_model as pick
    assert pick("Omni Flash [Beta]", True) == (None, None)


def test_a_charswap_scene_with_a_different_explicit_model_is_a_conflict():
    from image_platform import _v943_charswap_veo_model as pick
    model, conflict = pick("Veo 3.1 - Lite [Lower Priority]", True)
    assert model is None
    assert conflict and "Veo 3.1 - Lite [Lower Priority]" in conflict
    assert "charswap" in conflict


def test_the_stamp_lands_on_the_config_the_job_row_actually_stores():
    """Source-level: the decision has to run on config_dict BEFORE the Job is
    built, or the stamp never reaches config_json."""
    src = _function_source(_HERE / "image_platform.py", "promote_batch_to_video")
    built = src.index("config_dict = {")
    decide = src.index("_v943_charswap_veo_model(", built)
    job = src.index("job = Job(", decide)
    assert built < decide < job
    block = src[decide:job]
    assert 'config_dict["veo_model"] = _v943_model' in block
    assert "HTTPException(400" in block


def test_the_charswap_test_reads_the_same_field_the_worker_branches_on():
    """render_method on the clip spec — the same key charswap_selected asks
    for. A different spelling here would silently never match."""
    src = _function_source(_HERE / "image_platform.py", "promote_batch_to_video")
    decide = src.index("_v943_has_charswap = any(")
    block = src[decide:src.index("_v943_charswap_veo_model(", decide)]
    assert 'spec.get("render_method")' in block
    assert '"charswap"' in block

    worker = WORKER_SRC.read_text(encoding="utf-8")
    sel = worker.index("\ndef charswap_selected(")
    assert "render_method" in worker[sel:worker.index("\ndef ", sel + 1)]


# =============================================================================
# 15. v945.3 — the export-scoped temp directory must not leak
# =============================================================================
# It was created part-way down the export body and cleaned in a `finally`
# several hundred lines below. Every raise in between — the VAD 400, the
# no-valid-clip-files 400, any unexpected error — walked past the cleanup, and
# the container keeps running after a failed export.


def _run_export_wrapper(monkeypatch, impl):
    """Call _do_export_final with its body replaced, and return the temp
    directory the real wrapper handed that body."""
    import main

    seen = {}

    async def _fake(job_id, settings, db, current_user, export_tmp_dir):
        seen["dir"] = export_tmp_dir
        return await impl(export_tmp_dir)

    monkeypatch.setattr(main, "_do_export_final_impl", _fake)
    result = {}
    try:
        result["value"] = asyncio.run(
            main._do_export_final("job-1234-5678", None, None, None))
    except BaseException as e:  # noqa: BLE001 — the raise IS the case under test
        result["error"] = e
    return seen.get("dir"), result


def test_a_preflight_failure_leaves_no_orphan_directory(monkeypatch):
    """The exact shape of the bug: an HTTPException raised before the old
    cleanup point."""
    from fastapi import HTTPException

    async def _preflight_400(export_tmp_dir):
        assert os.path.isdir(export_tmp_dir), "the body never got a directory"
        raise HTTPException(status_code=400, detail="No valid clip files found")

    tmpdir, result = _run_export_wrapper(monkeypatch, _preflight_400)
    assert tmpdir and isinstance(result.get("error"), HTTPException)
    assert result["error"].status_code == 400
    assert not os.path.exists(tmpdir), f"orphan left behind: {tmpdir}"


def test_an_unexpected_error_leaves_no_orphan_directory(monkeypatch):
    async def _boom(export_tmp_dir):
        raise RuntimeError("ffmpeg died")

    tmpdir, result = _run_export_wrapper(monkeypatch, _boom)
    assert tmpdir and isinstance(result.get("error"), RuntimeError)
    assert not os.path.exists(tmpdir)


def test_a_directory_with_files_in_it_is_still_removed(monkeypatch):
    """The real body writes muxed clip copies in there before it fails."""
    async def _write_then_fail(export_tmp_dir):
        with open(os.path.join(export_tmp_dir, "clip0.mp4"), "wb") as fh:
            fh.write(b"not really an mp4")
        raise RuntimeError("failed after writing a copy")

    tmpdir, _ = _run_export_wrapper(monkeypatch, _write_then_fail)
    assert not os.path.exists(tmpdir)


def test_a_successful_export_also_cleans_up_and_returns_its_result(monkeypatch):
    async def _ok(export_tmp_dir):
        return {"success": True, "filename": "final.mp4"}

    tmpdir, result = _run_export_wrapper(monkeypatch, _ok)
    assert result["value"] == {"success": True, "filename": "final.mp4"}
    assert not os.path.exists(tmpdir)


def test_the_runner_still_calls_the_wrapper_and_not_the_body():
    """The wrapper is the only thing that owns the directory, so nothing may
    reach past it into the implementation."""
    src = (_HERE / "main.py").read_text(encoding="utf-8")
    assert src.count("await _do_export_final(") == 1
    assert src.count("await _do_export_final_impl(") == 1
