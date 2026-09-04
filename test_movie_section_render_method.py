"""v959 — the movie-section render method, and the proof it changed nothing else.

Same contract as code/test_charswap_render_method.py: a build that says nothing
about movie-section must parse exactly as it did before.

What is covered TODAY is the parser plus the import latch. The promote and
worker sections arrive with Tasks 4-5, which append to this file — that is why
`_worker_function` and the json / pathlib imports already sit here.
"""
import json
import pathlib
import sys

import pytest

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

_HERE = pathlib.Path(__file__).parent
WORKER_SRC = _HERE / "static" / "flow_worker.py"


def _worker_function(name):
    """Execute ONE top-level worker function from the shipped source (importing
    flow_worker.py boots a browser driver). Same helper as the charswap tests."""
    src = WORKER_SRC.read_text(encoding="utf-8")
    start = src.index(f"\ndef {name}(")
    rest = src[start + 1:]
    end = rest.index("\ndef ", 1)
    ns = {"re": __import__("re"), "json": json, "time": __import__("time"),
          "os": __import__("os")}
    exec(rest[:end], ns)  # noqa: S102 — our own file, on purpose
    return ns[name]


LEGACY_SCENE = """### Scene 1

- **image:** image_1
- **speaker:** on-camera
- **line:** american men over sixty are doing this every morning
- **clip_duration_s:** 6
- **action_note:** she lifts the jar [Start beat]
"""

SECTION_SCENE = """### Scene 1

- **image:** image_1
- **render_method:** movie-section
- **face_refs:** image_2, image_3
- **speaker:** on-camera
- **line:** wow if my husband looked like you i would never leave the house then he should do what i do
- **clip_duration_s:** 10
- **action_note:** she watches him lift the sack one-handed [Start beat]
"""

SECOND_SECTION = SECTION_SCENE.replace("### Scene 1", "### Scene 2").replace(
    "- **clip_duration_s:** 10", "- **clip_duration_s:** 8")

CHARSWAP_SCENE = """### Scene 1

- **image:** image_1
- **render_method:** charswap
- **swap_source_video:** raw/refs/curls.mp4
- **swap_mode:** image-led
- **speaker:** on-camera
- **line:** american men over sixty are doing this every morning
- **clip_duration_s:** 6
"""

SILENT_SCENE = """### Scene 2

- **image:** image_1
- **speaker:** silent
- **action_note:** the jar sits on the counter [Mid-clip beat]
- **clip_duration_s:** 6
"""

TEXT_CARD_WITH_METHOD = """### Scene 2

- **scene_type:** text_card
- **render_method:** movie-section
- **caption:** later
- **bg_color:** black
"""


def _parse(md, known=(1, 2, 3)):
    from image_platform import _parse_scene_blocks_new
    return _parse_scene_blocks_new(md, set(known))


# --- 1. a build with no new bullets parses to all-None new fields ------------

def test_legacy_scene_has_movie_section_fields_none():
    scene = _parse(LEGACY_SCENE)[0]
    assert scene["render_method"] is None
    assert scene["face_refs"] == []


def test_legacy_scene_keeps_every_other_field():
    scene = _parse(LEGACY_SCENE)[0]
    assert scene["scene_index"] == 1
    assert scene["image_index"] == 1
    assert scene["lines"] == ["american men over sixty are doing this every morning"]
    assert scene["clip_durations"] == [6]


def test_section_scene_parses_method_refs_and_window():
    scene = _parse(SECTION_SCENE)[0]
    assert scene["render_method"] == "movie-section"
    assert scene["face_refs"] == [2, 3]
    assert scene["clip_durations"] == [10]


# --- 2. the parser fails CLOSED ---------------------------------------------

def test_unknown_render_method_still_hard_fails():
    with pytest.raises(ValueError, match="not a known method"):
        _parse(SECTION_SCENE.replace("movie-section", "movie_section", 1))


def test_face_refs_without_method_hard_fails():
    md = LEGACY_SCENE + "- **face_refs:** image_2\n"
    with pytest.raises(ValueError, match="only means something"):
        _parse(md)


def test_face_refs_on_a_charswap_scene_hard_fails():
    """A swap clip renders from a source video, so it has nowhere to put a
    face chip. The bullet there is a mistake, not a no-op."""
    md = CHARSWAP_SCENE + "- **face_refs:** image_2\n"
    with pytest.raises(ValueError, match="only means something"):
        _parse(md)


def test_section_without_face_refs_hard_fails():
    md = "\n".join(l for l in SECTION_SCENE.splitlines() if "face_refs" not in l)
    with pytest.raises(ValueError, match="hold identity"):
        _parse(md)


def test_three_face_refs_hard_fails():
    with pytest.raises(ValueError, match="takes 1-2"):
        _parse(SECTION_SCENE.replace("image_2, image_3", "image_2, image_3, image_1"))


def test_face_ref_equal_to_scene_image_hard_fails():
    with pytest.raises(ValueError, match="is the scene's own"):
        _parse(SECTION_SCENE.replace("image_2, image_3", "image_1, image_3"))


def test_unknown_face_ref_hard_fails():
    with pytest.raises(ValueError, match="not a defined"):
        _parse(SECTION_SCENE.replace("image_2, image_3", "image_2, image_9"))


def test_repeated_face_ref_hard_fails():
    with pytest.raises(ValueError, match="repeats"):
        _parse(SECTION_SCENE.replace("image_2, image_3", "image_2, image_2"))


def test_face_ref_token_must_be_image_n():
    with pytest.raises(ValueError, match=r"is not .image_N"):
        _parse(SECTION_SCENE.replace("image_2, image_3", "img_2, image_3"))


def test_section_scene_refuses_swap_bullets():
    with pytest.raises(ValueError, match="swap"):
        _parse(SECTION_SCENE + "- **swap_mode:** video-led\n")


def test_section_scene_refuses_a_swap_source_on_its_own():
    with pytest.raises(ValueError, match="does not take"):
        _parse(SECTION_SCENE + "- **swap_source_video:** raw/refs/curls.mp4\n")


def test_audio_bullet_on_a_section_scene_says_to_drop_it():
    """The v943.1 message tells the author to declare the swap trio instead.
    A movie-section scene can never grow that trio, so it needs its own fix."""
    with pytest.raises(ValueError, match="drop the bullet"):
        _parse(SECTION_SCENE + "- **audio:** source-original\n")


def test_text_card_declaring_the_method_hard_fails():
    with pytest.raises(ValueError, match="take no render_method"):
        _parse(TEXT_CARD_WITH_METHOD)


def test_silent_scene_in_a_section_build_names_the_dead_end():
    """'Declare it on every shot scene too' is advice a silent scene cannot
    take — it has no words to carry. The message has to say that."""
    with pytest.raises(ValueError, match="no silent scenes"):
        _parse(SECTION_SCENE + "\n" + SILENT_SCENE)


def test_section_with_two_lines_hard_fails():
    md = SECTION_SCENE + "- **line:** a second line here\n- **clip_duration_s:** 10\n"
    with pytest.raises(ValueError, match="one clip"):
        _parse(md)


def test_section_without_window_hard_fails():
    md = "\n".join(l for l in SECTION_SCENE.splitlines() if "clip_duration_s" not in l)
    with pytest.raises(ValueError, match="clip_duration_s"):
        _parse(md)


def test_section_window_must_be_8_or_10():
    with pytest.raises(ValueError, match="clip_duration_s"):
        _parse(SECTION_SCENE.replace("- **clip_duration_s:** 10", "- **clip_duration_s:** 6"))


def test_mixed_build_hard_fails():
    md = SECTION_SCENE + "\n" + LEGACY_SCENE.replace("### Scene 1", "### Scene 2")
    with pytest.raises(ValueError, match="all shot scenes"):
        _parse(md)


def test_text_card_is_exempt_from_all_or_none():
    card = ("### Scene 2\n- **scene_type:** text_card\n- **caption:** later\n"
            "- **bg_color:** black\n")
    scenes = _parse(SECTION_SCENE + "\n" + card)
    assert [s["render_method"] for s in scenes] == ["movie-section", None]


def test_two_sections_parse():
    scenes = _parse(SECTION_SCENE + "\n" + SECOND_SECTION)
    assert [s["clip_durations"] for s in scenes] == [[10], [8]]


# --- the import latch: declared, but the render arm is not shipped ----------

def test_the_arm_is_not_shipped_yet():
    """The parser understands the method before anything can render it. While
    this is False the import route refuses a build that declares it."""
    import image_platform
    assert image_platform.MOVIE_SECTION_ARM_SHIPPED is False


def test_the_import_route_carries_the_latch():
    """Pin what the latch DOES, not how it is worded.

    The latch sits in the batch-import route, which needs a database session and
    a request body, so no unit test can call it. So this reads the route's own
    source and checks the two things that make it a latch: the condition asks
    about THIS render method, and the body under it refuses with a 400. The
    prose of the 400 is free to be reworded."""
    import inspect

    import image_platform
    src = inspect.getsource(image_platform._import_scene_table_impl)
    assert "if not MOVIE_SECTION_ARM_SHIPPED and any(" in src
    # The constant has to be inside that same condition — a latch that reads
    # some OTHER method's name is not this latch. `):` closes the `any(...)`.
    latch = src[src.index("if not MOVIE_SECTION_ARM_SHIPPED and any("):]
    condition = latch[:latch.index("):") + 2]
    assert "MOVIE_SECTION_RENDER_METHOD" in condition
    # And the body under that condition raises, rather than logging and
    # carrying on — "logging and continuing IS failing open".
    body = latch[latch.index("):") + 2:]
    assert body.lstrip().startswith("raise HTTPException(")
    assert "400" in body[:200]


# --- 3. the columns exist on both rows and serialise ------------------------

def test_clip_row_has_face_ref_frames_column():
    from sqlalchemy import Text

    from models import Clip
    assert "face_ref_frames_json" in Clip.__table__.columns
    col = Clip.__table__.columns["face_ref_frames_json"]
    # Nullable because every legacy row has to keep working, and Text because
    # the value is a JSON list of frame keys, not one key.
    assert col.nullable and isinstance(col.type, Text)


def test_assignment_row_has_face_ref_node_ids_column():
    from sqlalchemy import Text

    from image_platform import ImageSceneAssignment
    assert "face_ref_node_ids_json" in ImageSceneAssignment.__table__.columns
    col = ImageSceneAssignment.__table__.columns["face_ref_node_ids_json"]
    assert col.nullable and isinstance(col.type, Text)


def test_both_migration_dialects_register_the_columns():
    """A column added to the model and not to BOTH migration lists exists on a
    fresh database and is missing on every deployed one."""
    src = (_HERE / "image_platform.py").read_text(encoding="utf-8")
    for table, col in (("image_scene_assignments", "face_ref_node_ids_json"),
                       ("clips", "face_ref_frames_json")):
        assert f"ALTER TABLE {table} ADD COLUMN {col} TEXT" in src
        assert (f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "
                f"{col} TEXT") in src


def test_the_columns_are_in_the_readback_contract():
    """Startup swallows a failed migration and keeps serving, so the readback
    proof is the only thing that says the columns really landed."""
    from image_platform import CHARSWAP_COLUMNS
    assert "face_ref_node_ids_json" in CHARSWAP_COLUMNS["image_scene_assignments"]
    assert "face_ref_frames_json" in CHARSWAP_COLUMNS["clips"]


# --- 4. binding helpers are pure and fail closed ----------------------------

def test_face_ref_node_ids_resolve_in_declared_order():
    from image_platform import _v959_face_ref_node_ids
    idx_to_node = {1: 101, 2: 202, 3: 303}
    assert _v959_face_ref_node_ids({"scene_index": 1, "face_refs": [3, 2]}, idx_to_node) == [303, 202]


def test_face_ref_node_ids_refuse_unrendered_image():
    from image_platform import _v959_face_ref_node_ids
    with pytest.raises(ValueError, match="image_9"):
        _v959_face_ref_node_ids({"scene_index": 1, "face_refs": [9]}, {1: 101})


def test_movie_section_veo_model_forces_omni_when_unset():
    from image_platform import _v959_movie_section_veo_model, V943_CHARSWAP_VEO_MODEL
    assert _v959_movie_section_veo_model(None, True) == (V943_CHARSWAP_VEO_MODEL, None)
    assert _v959_movie_section_veo_model("Omni Flash", True) == (None, None)
    assert _v959_movie_section_veo_model(None, False) == (None, None)


def test_movie_section_veo_model_refuses_explicit_veo():
    from image_platform import _v959_movie_section_veo_model
    model, conflict = _v959_movie_section_veo_model("Veo 3.1 - Fast", True)
    assert model is None and "movie-section" in conflict


# --- 5. the binding reaches every boundary the end frame crosses ------------
#
# These read the route source, the same way the charswap suite pins its own
# route wiring: the functions need a database session and a request body, so a
# unit test cannot call them. A face ref that is bound on three of four
# surfaces is exactly the silent drop check_field_plumbing.py exists for.

def _function_source(name, filename="image_platform.py"):
    """Source text of ONE top-level function, so a match cannot leak in."""
    import ast
    src = (_HERE / filename).read_text(encoding="utf-8")
    tree = ast.parse(src)
    for n in ast.walk(tree):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name:
            return ast.get_source_segment(src, n) or ""
    raise AssertionError(f"{name}() not found in {filename}")


def test_the_import_writes_face_ref_node_ids_onto_the_assignment():
    src = _function_source("_import_scene_table_impl")
    assert "end_frame_image_node_id=end_frame_node_id_resolved" in src, "fixture is out of date"
    assert "_v959_face_ref_node_ids(" in src
    assert "face_ref_node_ids_json=" in src


def test_prepare_uploads_the_face_frames_and_puts_them_on_the_payload():
    """A face ref that never enters the upload set has no file on disk, and the
    worker discovers that hours later as a 404 on download_frame."""
    src = _function_source("prepare_batch_for_video")
    assert "end_frame_image_local_index" in src, "fixture is out of date"
    assert "_v959_stored_face_ref_node_ids(" in src   # the upload set + payload
    # The per-scene payload and BOTH flat-row branches (silent scene, spoken
    # line) — the frontend reads the flat rows, not the per-scene payload.
    assert src.count('"face_ref_local_indexes"') == 3


def test_promote_carries_the_face_refs_from_the_assignment_to_the_clip():
    src = _function_source("promote_batch_to_video")
    # dialogue_list + clip_specs — a binding on one of the two is the v943.6
    # shape of failure: the row says one thing and the stored dialogue another.
    assert src.count('"face_ref_local_indexes":') == 2
    assert src.count('"face_ref_node_ids":') == 2
    assert "face_ref_frames_json=" in src                  # Clip(...)


def test_promote_forces_omni_and_refuses_a_section_with_no_prompt():
    src = _function_source("promote_batch_to_video")
    assert "_v959_movie_section_veo_model(" in src
    assert "raise HTTPException(400, _v959_conflict)" in src
    # D11 — build_prompt must never author a section, so a section clip with an
    # empty Text prompt is refused rather than filled in. The guard and the
    # prompt_text fill share ONE lookup: a guard that reads a different field
    # from the one the clip is built from is not a guard.
    assert src.count("_prompt_override_for(spec)") == 3  # def + guard + fill
    assert "build_prompt must never author a section" in src
    # A section with no face refs is the same class of broken: the parser makes
    # them mandatory, so an empty list means the row lost them on the way here.
    assert 'not spec.get("face_ref_node_ids")' in src


def test_promote_asks_one_question_about_what_a_section_is():
    """Four spellings of the same test is how one of them comes to disagree."""
    src = _function_source("promote_batch_to_video")
    assert "def _v959_is_section(spec)" in src
    assert src.count("_v959_is_section(") == 5   # def + the four readers
    # and nobody rolls their own: the normalised comparison is written ONCE,
    # inside that def.
    assert src.count('render_method") or "").strip().lower() == MOVIE_SECTION') == 1


def test_promote_calls_the_materialise_helper_and_refuses_broken_json():
    src = _function_source("promote_batch_to_video")
    assert "_v959_materialise_face_frames(" in src
    assert "_v959_stored_face_ref_node_ids(" in src
    # A broken column is a broken ROW, so it leaves as a 500 rather than as an
    # empty face-ref list nobody notices.
    assert "except ValueError as _v959_je:" in src
    assert "raise HTTPException(500, str(_v959_je))" in src


def test_no_face_ref_reader_swallows_a_broken_column():
    """Logging and continuing IS failing open. The column is machine-written,
    so unreadable JSON is a broken row — and a section that renders with its
    face chips dropped renders a stranger."""
    src = (_HERE / "image_platform.py").read_text(encoding="utf-8")
    assert src.count("_v959_stored_face_ref_node_ids(") == 4  # def + 3 readers
    assert "_v959_face_nids = []" not in src
    assert "_v959_nids = []" not in src


# --- 6. reading the stored column ------------------------------------------

def test_stored_face_refs_read_back_in_order():
    from image_platform import _v959_stored_face_ref_node_ids
    scene = {"scene_index": 4, "face_ref_node_ids_json": "[303, 202]"}
    assert _v959_stored_face_ref_node_ids(scene) == [303, 202]


def test_stored_face_refs_are_empty_when_the_column_is_null():
    from image_platform import _v959_stored_face_ref_node_ids
    assert _v959_stored_face_ref_node_ids({"scene_index": 1}) == []
    assert _v959_stored_face_ref_node_ids(
        {"scene_index": 1, "face_ref_node_ids_json": ""}) == []


def test_stored_face_refs_refuse_a_broken_column():
    from image_platform import _v959_stored_face_ref_node_ids
    with pytest.raises(ValueError) as e:
        _v959_stored_face_ref_node_ids(
            {"scene_index": 7, "face_ref_node_ids_json": "[303,"})
    # the scene AND the value that broke it — a reader hours downstream cannot
    # work out which row was bad from "invalid JSON".
    assert "Scene 7" in str(e.value) and "[303," in str(e.value)


# --- 7. materialising the face frames --------------------------------------
#
# The scene loop copies one frame per scene's OWN image. A face ref is never a
# scene's own image, so nothing copies it, and its key cannot be spelled out
# from the local index alone: it carries THAT node's file extension. Fakes
# stand in for the node rows and the storage so the rule itself is testable.

class _FakeNode:
    def __init__(self, node_id, variant_id):
        self.id = node_id
        self.chosen_variant_id = variant_id


class _FakeVariant:
    def __init__(self, image_path):
        self.image_path = image_path


class _FakeDb:
    """Answers each ImageVariant lookup from a queue, in call order."""

    def __init__(self, results):
        self._results = list(results)

    def query(self, *_a, **_k):
        return self

    def filter(self, *_a, **_k):
        return self

    def first(self):
        return self._results.pop(0) if self._results else None


class _FakeR2:
    def __init__(self, fail=False):
        self.fail = fail
        self.uploaded = []

    def upload_job_frame(self, job_id, filename, path):
        if self.fail:
            raise RuntimeError("R2 is down")
        self.uploaded.append(filename)


def _patch_storage(monkeypatch, root):
    """Point images_root() at a temp tree; record every restore attempt."""
    import image_platform
    attempts = []
    monkeypatch.setattr(image_platform, "images_root", lambda: root)
    monkeypatch.setattr(image_platform, "_storage_download_to_local",
                        lambda p: attempts.append(p) or False)
    return attempts


def test_face_frames_take_the_local_index_and_the_nodes_own_extension(tmp_path, monkeypatch):
    from image_platform import _v959_materialise_face_frames
    root = tmp_path / "images"
    root.mkdir()
    (root / "a.png").write_bytes(b"png-bytes")
    (root / "b.jpg").write_bytes(b"jpg-bytes")
    _patch_storage(monkeypatch, root)
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    nodes_by_id = {7: (3, _FakeNode(7, 71)), 9: (12, _FakeNode(9, 91))}
    db = _FakeDb([_FakeVariant("a.png"), _FakeVariant("b.jpg")])
    keys = {}

    out = _v959_materialise_face_frames(
        db, [9, 7], nodes_by_id, "JOB1", job_dir, keys, None)

    assert out == {7: "jobs/JOB1/frames/image_03.png",
                   9: "jobs/JOB1/frames/image_12.jpg"}
    assert (job_dir / "image_03.png").read_bytes() == b"png-bytes"
    assert (job_dir / "image_12.jpg").read_bytes() == b"jpg-bytes"


def test_face_frames_refuse_a_node_that_is_not_in_the_batch(tmp_path, monkeypatch):
    from fastapi import HTTPException

    from image_platform import _v959_materialise_face_frames
    _patch_storage(monkeypatch, tmp_path)
    with pytest.raises(HTTPException) as e:
        _v959_materialise_face_frames(
            _FakeDb([]), [7], {}, "JOB1", tmp_path, {}, None)
    assert e.value.status_code == 500 and "not in this batch" in str(e.value.detail)


def test_face_frames_refuse_a_node_with_no_chosen_variant(tmp_path, monkeypatch):
    from fastapi import HTTPException

    from image_platform import _v959_materialise_face_frames
    _patch_storage(monkeypatch, tmp_path)
    with pytest.raises(HTTPException) as e:
        _v959_materialise_face_frames(
            _FakeDb([None]), [7], {7: (3, _FakeNode(7, 71))},
            "JOB1", tmp_path, {}, None)
    assert "no chosen variant" in str(e.value.detail)


def test_face_frames_refuse_a_file_that_r2_cannot_restore_either(tmp_path, monkeypatch):
    from fastapi import HTTPException

    from image_platform import _v959_materialise_face_frames
    root = tmp_path / "images"
    root.mkdir()
    attempts = _patch_storage(monkeypatch, root)
    with pytest.raises(HTTPException) as e:
        _v959_materialise_face_frames(
            _FakeDb([_FakeVariant("gone.png")]), [7], {7: (3, _FakeNode(7, 71))},
            "JOB1", tmp_path, {}, None)
    assert "unavailable" in str(e.value.detail)
    # the restore was actually tried before giving up
    assert attempts == ["gone.png"]


def test_face_frames_refuse_when_the_copy_fails(tmp_path, monkeypatch):
    from fastapi import HTTPException

    from image_platform import _v959_materialise_face_frames
    root = tmp_path / "images"
    root.mkdir()
    (root / "a.png").write_bytes(b"png")
    _patch_storage(monkeypatch, root)
    not_a_dir = tmp_path / "not-a-dir"
    not_a_dir.write_text("this is a file")
    with pytest.raises(HTTPException) as e:
        _v959_materialise_face_frames(
            _FakeDb([_FakeVariant("a.png")]), [7], {7: (3, _FakeNode(7, 71))},
            "JOB1", not_a_dir, {}, None)
    assert "copy failed" in str(e.value.detail)


def test_a_failed_r2_upload_still_gives_the_clip_its_key(tmp_path, monkeypatch):
    """Same contract as start_frame_key: the key is the canonical location, and
    a failed mirror is a warning, not a lost clip."""
    from image_platform import _v959_materialise_face_frames
    root = tmp_path / "images"
    root.mkdir()
    (root / "a.png").write_bytes(b"png")
    _patch_storage(monkeypatch, root)
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    keys = {}

    out = _v959_materialise_face_frames(
        _FakeDb([_FakeVariant("a.png")]), [7], {7: (3, _FakeNode(7, 71))},
        "JOB1", job_dir, keys, _FakeR2(fail=True))

    assert out == {7: "jobs/JOB1/frames/image_03.png"}
    assert keys == {}   # nothing claimed to be mirrored


def test_a_good_r2_upload_is_recorded(tmp_path, monkeypatch):
    from image_platform import _v959_materialise_face_frames
    root = tmp_path / "images"
    root.mkdir()
    (root / "a.png").write_bytes(b"png")
    _patch_storage(monkeypatch, root)
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    keys = {}
    r2 = _FakeR2()

    _v959_materialise_face_frames(
        _FakeDb([_FakeVariant("a.png")]), [7], {7: (3, _FakeNode(7, 71))},
        "JOB1", job_dir, keys, r2)

    assert r2.uploaded == ["image_03.png"]
    assert keys == {"image_03.png": "jobs/JOB1/frames/image_03.png"}


def test_a_face_ref_the_scene_loop_already_copied_is_not_copied_again(tmp_path, monkeypatch):
    """A face ref can also be another scene's own image. Re-copying it is
    wasted work and a second R2 round trip for a file already there."""
    from image_platform import _v959_materialise_face_frames
    root = tmp_path / "images"
    root.mkdir()
    (root / "a.png").write_bytes(b"fresh source")
    _patch_storage(monkeypatch, root)
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    (job_dir / "image_03.png").write_bytes(b"already there")
    r2 = _FakeR2()

    out = _v959_materialise_face_frames(
        _FakeDb([_FakeVariant("a.png")]), [7], {7: (3, _FakeNode(7, 71))},
        "JOB1", job_dir, {}, r2, already_copied={"image_03.png"})

    assert out == {7: "jobs/JOB1/frames/image_03.png"}
    assert (job_dir / "image_03.png").read_bytes() == b"already there"
    assert r2.uploaded == []


# --- 8. the payload helper adds keys ONLY on a movie-section clip -----------
#
# Same regression contract as the charswap helper: a Veo render is stochastic
# and can never be byte-compared, but the JSON the worker is handed can. So the
# proof that this arm changed nothing for everybody else is that a legacy clip
# comes back with the dict it went in with.

class _Clip:
    def __init__(self, **kw):
        self.render_method = None
        self.face_ref_frames_json = None
        self.veo_render_duration_s = None
        self.job_id = "job1"
        self.__dict__.update(kw)


def test_legacy_clip_payload_untouched_by_movie_section_helper():
    from main import _v959_maybe_movie_section
    before = {"id": 1, "prompt": "p", "start_frame_url": "u"}
    after = _v959_maybe_movie_section(dict(before), _Clip(), "https://x", "user-worker")
    assert after == before


def test_a_charswap_clip_is_not_touched_by_the_movie_section_helper():
    """The two arms sit on the same branch point, so each helper has to ignore
    the other's clips or a swap clip would leave with an Ingredients mode."""
    from main import _v959_maybe_movie_section
    before = {"id": 2, "prompt": "p"}
    after = _v959_maybe_movie_section(
        dict(before), _Clip(render_method="charswap"), "https://x", "local-worker")
    assert after == before


def test_movie_section_payload_carries_refs_mode_and_window():
    from main import _v959_maybe_movie_section
    clip = _Clip(render_method="movie-section", veo_render_duration_s=10,
                 face_ref_frames_json=json.dumps(["jobs/job1/frames/f2.png",
                                                  "jobs/job1/frames/f3.png"]))
    out = _v959_maybe_movie_section({"id": 1}, clip, "https://x", "user-worker")
    assert out["render_method"] == "movie-section"
    assert out["input_mode"] == "Ingredients"
    assert out["section_window_s"] == 10
    assert out["face_ref_urls"] == ["https://x/api/user-worker/frames/job1/f2.png",
                                    "https://x/api/user-worker/frames/job1/f3.png"]


def test_the_face_ref_urls_follow_the_lane_they_were_asked_for():
    """The two worker lanes carry different credentials, so a face ref handed to
    the local worker on the user-worker path is a 401 hours later."""
    from main import _v959_movie_section_payload
    clip = _Clip(render_method="movie-section", veo_render_duration_s=8,
                 face_ref_frames_json=json.dumps(["jobs/job1/frames/f2.png"]))
    out = _v959_movie_section_payload(clip, "https://x", "local-worker")
    assert out["face_ref_urls"] == ["https://x/api/local-worker/frames/job1/f2.png"]


def test_a_null_face_ref_column_gives_an_empty_list_not_a_crash():
    """Import refuses a section with no face refs on both promote lanes, so a
    NULL column here means the row is already wrong. The payload still has to be
    buildable — the worker's own readiness check is what stops the render."""
    from main import _v959_movie_section_payload
    out = _v959_movie_section_payload(
        _Clip(render_method="movie-section"), "https://x", "user-worker")
    assert out["face_ref_urls"] == []
    assert out["section_window_s"] is None


def test_both_claim_lanes_call_the_movie_section_helper():
    """v945.8's rule, one arm along: a guard on one of two doors is not a guard.
    The helper is called next to its charswap twin at BOTH claim sites."""
    src = (_HERE / "main.py").read_text(encoding="utf-8")
    assert src.count("_v959_maybe_movie_section(") == 3   # def + both lanes
    for lane in ("local-worker", "user-worker"):
        assert f'_v959_maybe_movie_section(clip_data, clip, base_url, "{lane}")' in src \
            or f'_v959_maybe_movie_section(_clip_data, clip, base_url, "{lane}")' in src, lane


@pytest.mark.skip(reason="the worker arm lands in Task 5; unskip it there")
def test_every_payload_key_the_helper_emits_is_read_by_the_worker():
    """The contract between the two halves of this feature. The helper's keys
    are read off the helper itself, so a renamed key cannot pass by being
    renamed in the test too."""
    from main import _v959_movie_section_payload
    clip = _Clip(render_method="movie-section", veo_render_duration_s=10,
                 face_ref_frames_json=json.dumps(["jobs/job1/frames/f2.png"]))
    keys = list(_v959_movie_section_payload(clip, "https://x", "user-worker"))
    src = WORKER_SRC.read_text(encoding="utf-8")
    missing = [k for k in keys
               if f'"{k}"' not in src and f"'{k}'" not in src]
    assert not missing, f"the worker never reads: {missing}"


# --- 9. the API refuses a render method it does not have an arm for ---------

def test_render_method_accepts_the_two_known_values():
    from main import DialogueLineInput
    for m in ("charswap", "movie-section"):
        assert DialogueLineInput(id=1, text="x", render_method=m).render_method == m


def test_render_method_none_and_empty_mean_the_ordinary_renderer():
    """Every downstream reader spells this `(... or "").strip().lower()`, so an
    empty string already means "no method". Normalising it here once keeps the
    stored row equal to what those three comparisons expect."""
    from main import DialogueLineInput
    assert DialogueLineInput(id=1, text="x").render_method is None
    assert DialogueLineInput(id=1, text="x", render_method="").render_method is None
    assert DialogueLineInput(
        id=1, text="x", render_method="  Movie-Section ").render_method == "movie-section"


def test_render_method_rejects_a_third_value():
    """The field is copied straight onto the Clip row, so without this a
    hand-crafted POST /api/jobs could stamp any string past the import latch."""
    from pydantic import ValidationError

    from main import DialogueLineInput
    with pytest.raises(ValidationError) as e:
        DialogueLineInput(id=1, text="x", render_method="deepfake")
    assert "render_method" in str(e.value)


def test_the_api_render_method_list_matches_the_parser_constant():
    """Two lists of the same two strings is how one of them comes to disagree."""
    import main
    from image_platform import MOVIE_SECTION_RENDER_METHOD
    assert MOVIE_SECTION_RENDER_METHOD in main._RENDER_METHODS
    assert "charswap" in main._RENDER_METHODS
    assert len(main._RENDER_METHODS) == 2


# --- 10. the THIRD door: the browser's own promote payload ------------------

def test_the_browser_promote_payload_sends_the_face_refs():
    """check_field_plumbing.py is satisfied by EITHER promote path, so a miss
    here passes the checker and still arrives NULL on every job promoted from
    the UI — the v892.2 failure exactly."""
    src = (_HERE / "static" / "index.html").read_text(encoding="utf-8")
    assert "end_frame_image_local_index:" in src, "fixture is out of date"
    assert "face_ref_node_ids: promoteMeta.face_ref_node_ids" in src
    assert "face_ref_local_indexes: promoteMeta.face_ref_local_indexes" in src


# --- 11. the job creators on the POST /api/jobs lane ------------------------

def test_the_face_ref_indexes_cross_into_the_background_task_by_model_dump():
    """There is no per-field hop in the Clip writer. The whole line model is
    dumped into Job.dialogue_json and the background task reads it back, which
    is exactly how end_frame_image_local_index travels — so declaring the field
    on DialogueLineInput IS the promote-path change."""
    src = _function_source("_create_job_impl", "main.py")
    assert "dialogue_list = [d.model_dump() for d in request.dialogue_lines]" in src
    assert '"lines": dialogue_list' in src


def test_the_background_task_resolves_face_frames_from_the_upload_list():
    src = _function_source("_setup_job_background", "main.py")
    assert 'line_data.get("end_frame_image_local_index")' in src, "fixture is out of date"
    assert 'line_data.get("face_ref_local_indexes")' in src
    assert "clip.face_ref_frames_json" in src


def test_the_background_task_refuses_a_face_ref_index_off_the_end():
    """Bounds-checked the same way the explicit end frame is: an out-of-range
    index is a refused clip, not a silent drop that 404s at download time."""
    src = _function_source("_setup_job_background", "main.py")
    guard = src[src.index("_face_keys_v959"):]
    assert "raise ValueError(" in guard[:1200]
    assert "do not all resolve to uploaded frames" in guard[:1200]


def test_post_jobs_refuses_a_section_with_no_authored_prompt():
    """D11 + v945.8: a section is never built, only written. build_prompt would
    author a dialogue prompt for it, and the worker then trusts a non-empty
    prompt_text — which is the talking-head render this whole chain exists to
    stop, reachable through POST /api/jobs instead of promote."""
    src = _function_source("_setup_job_background", "main.py")
    assert "CHARSWAP_DEFAULT_PROMPT as _cs_default" in src, "fixture is out of date"
    # The two empty-override guards sit together; the section one is the second.
    guards = src.split("if not _veo_prompt_override and (")
    assert len(guards) == 3, "expected exactly two empty-override guards"
    section_guard = guards[2][:900]
    assert '== "movie-section"' in section_guard
    # It REFUSES rather than stamping a default: a swap has a sensible default
    # prompt, a section has none — the section prompt is the operator's text.
    assert "raise ValueError(" in section_guard
    assert "(v959)" in section_guard
    assert "_cs_default" not in section_guard


# --- 12. the redo lane refuses BOTH methods --------------------------------
#
# rules/v945.md:207-230 records what happens when it does not: three failed
# charswap clips were requeued ~30 min later and the redo lane, whose payload
# carries no arm keys, delivered plain renders of the start frame OVER the
# honest failures.

class _RedoClip:
    def __init__(self, render_method=None, clip_index=0):
        self.render_method = render_method
        self.clip_index = clip_index
        self.job_id = "job1"
        self.status = "flow_redo_queued"
        self.claimed_by_worker = "w1"
        self.claimed_at = "now"
        self.approval_status = None
        self.error_code = None
        self.error_message = None
        self.redo_reason = None


class _RedoDb:
    def __init__(self):
        self.commits = 0

    def commit(self):
        self.commits += 1


def test_a_movie_section_clip_is_refused_by_the_redo_lane():
    from main import _v945_14_reject_charswap_redos
    clip = _RedoClip(render_method="movie-section")
    kept = _v945_14_reject_charswap_redos(_RedoDb(), [clip], "user-worker")
    assert kept == []
    assert clip.status == "failed"
    assert "movie-section" in (clip.error_message or "")
    assert clip.claimed_by_worker is None


def test_a_charswap_clip_still_behaves_exactly_as_before():
    from main import _v945_14_reject_charswap_redos
    clip = _RedoClip(render_method="charswap")
    clip.error_message = "generate request missing both media ids"
    kept = _v945_14_reject_charswap_redos(_RedoDb(), [clip], "local-worker")
    assert kept == []
    assert clip.status == "failed"
    assert clip.error_code == "CHARSWAP_NO_REDO"
    assert "charswap" in (clip.error_message or "")
    # v945.15.1 — the parking reason survives the flip.
    assert "generate request missing both media ids" in (clip.redo_reason or "")


def test_an_ordinary_clip_passes_through_the_redo_door_untouched():
    from main import _v945_14_reject_charswap_redos
    clip = _RedoClip(render_method=None)
    db = _RedoDb()
    kept = _v945_14_reject_charswap_redos(db, [clip], "user-worker")
    assert kept == [clip]
    assert clip.status == "flow_redo_queued"
    assert db.commits == 0


def test_the_redo_door_reads_one_list_of_the_methods_that_have_an_arm():
    import main
    assert main._RENDER_METHODS == ("charswap", "movie-section")
    src = _function_source("_v945_14_reject_charswap_redos", "main.py")
    assert "_RENDER_METHODS" in src


# --- 13. the live column readback names both methods ------------------------

def test_the_column_readback_route_names_both_methods():
    """Startup catches a failed migration and keeps serving, so a healthy deploy
    is not evidence the columns landed — this route is. Its wording is what an
    operator reads when asking whether v959 is really on the database."""
    src = (_HERE / "main.py").read_text(encoding="utf-8")
    assert "the render-method columns (v943 charswap + v959 movie-section)" in src
    assert "[v943/v959] column readback" in src

