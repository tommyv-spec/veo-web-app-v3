# tests/test_update_clip_contract.py
#
# v965 — `POST /api/jobs/{id}/clip-contracts`, the contract half of v947's
# update-finishing. A clip row gets its contract at exactly one moment (the row
# is written from the dialogue payload at job creation); a redo does not
# rebuild it and the from-batch promote path never wrote one at all. So a job
# promoted before its build declared `CLIP CONTRACT: v1` has NULL on every
# clip, and short of a re-import that discards the approved clips there was no
# way to change that. This route backfills the rows in place.
#
# What has to hold, one test each below:
#   1. A good build lands the RIGHT scene's declaration on the RIGHT clip row,
#      across a multi-scene build with a multi-line scene and a silent scene.
#   2. The mapping holds under BOTH numbering schemes that write
#      Clip.scene_index — the markdown's `### Scene N` (import path) and the
#      0-based enumerate position (promote_batch_to_video). This is the whole
#      reason the stamp is positional and not keyed on scene_index.
#   3. A mapping that cannot be PROVED refuses the whole job and writes
#      nothing: wrong clip count, scene boundaries in the wrong places,
#      scene_index out of order or NULL, markdown scene numbers not ascending.
#   4. A bad declaration 400s with the `Parse error:` prefix and leaves every
#      stored value alone.
#   5. An ABSENT opt-in is REFUSED, not applied — the deliberate divergence
#      from update-finishing, because un-stamping is silent and the likeliest
#      cause of a missing declaration is the wrong file.
#   6. Running it twice on an already-stamped job changes nothing.
# Plus ownership, since this endpoint writes.

import asyncio
import json
import pathlib
import sys
import types

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import image_platform  # noqa: F401 — registers image_nodes for the FKs
from models import Job, Clip, ClipStatus
import main


# --------------------------------------------------------------------------
# builds
# --------------------------------------------------------------------------

IMAGES = """## Images

### Image 1

- **Image prompt:**

```
a jar on a counter
```

### Image 2

- **Image prompt:**

```
a second jar
```

## Storyboard
"""


def _build(opt_in=True, scene2="silent", scene_numbers=(1, 2)):
    """A two-scene build. Scene 1 has TWO lines, scene 2 has none.

    Three clip rows total, and the two scenes declare DIFFERENT contracts, so
    a stamp that slid by one is visible in the stored values rather than only
    in a count.
    """
    head = "CLIP CONTRACT: v1\n\n" if opt_in else ""
    s2_body = {
        "silent": (
            "- **scene_type:** shot\n"
            "- **speaker:** silent\n"
            "- **input_mode:** ingredients\n"
            "- **isolate_project:** true\n"
            "- **policy_fallback:** fail\n"
            "- **clip_duration_s:** 8\n"
            "- **action_note:** she lifts the jar\n"
        ),
        "text_card": (
            "- **scene_type:** text_card\n"
            "- **caption:** THE END\n"
            "- **bg_color:** #000000\n"
            "- **duration_s:** 1.5\n"
        ),
        "one_line": (
            "- **scene_type:** shot\n"
            "- **line:** third line here\n"
            "- **input_mode:** ingredients\n"
            "- **isolate_project:** true\n"
            "- **policy_fallback:** fail\n"
            "- **clip_duration_s:** 8\n"
        ),
    }[scene2]
    # a text_card scene carries no `image:` bullet and no attach line
    s2_head = (f"### Scene {scene_numbers[1]}\n\n"
               if scene2 == "text_card" else
               f"### Scene {scene_numbers[1]}\n\n"
               "- **image:** image_2\n"
               "- **attach:** image_2:start_frame\n")
    return f"""{head}{IMAGES}

### Scene {scene_numbers[0]}

- **image:** image_1
- **attach:** image_1:start_frame
- **scene_type:** shot
- **line:** first line here
- **input_mode:** frames
- **isolate_project:** false
- **policy_fallback:** fail
- **clip_duration_s:** 4
- **line:** second line here
- **input_mode:** frames
- **isolate_project:** false
- **policy_fallback:** prompt_b, fail
- **clip_duration_s:** 6

{s2_head}{s2_body}"""


GOOD_MD = _build()
TEXT_CARD_MD = _build(scene2="text_card")

# Bullets present, opt-in missing. The PARSER refuses this one outright — a
# declaration the author wrote must never be silently dropped — so it never
# reaches the route's own opt-in check.
BULLETS_WITHOUT_OPT_IN_MD = _build(opt_in=False)

# The genuinely out-of-scope build: no opt-in and no bullets. 345 of the
# builds on disk look like this, and this is what the route's opt-in check is
# actually there to answer.
NO_OPT_IN_MD = BULLETS_WITHOUT_OPT_IN_MD
for _b in ("- **input_mode:** frames\n", "- **input_mode:** ingredients\n",
           "- **isolate_project:** false\n", "- **isolate_project:** true\n",
           "- **policy_fallback:** fail\n",
           "- **policy_fallback:** prompt_b, fail\n",
           "- **attach:** image_1:start_frame\n",
           "- **attach:** image_2:start_frame\n"):
    NO_OPT_IN_MD = NO_OPT_IN_MD.replace(_b, "")

# A declaration the model refuses. `input_mode` is a closed set, so this dies
# inside the parser exactly as it would at import.
BAD_MD = GOOD_MD.replace("- **input_mode:** frames",
                         "- **input_mode:** telepathy", 1)


# --------------------------------------------------------------------------
# fixtures
# --------------------------------------------------------------------------

def _session():
    eng = create_engine("sqlite:///:memory:")
    Job.__table__.metadata.create_all(eng)
    return sessionmaker(bind=eng)()


def _job(db, job_id="job-cc-1", user_id="u1"):
    db.add(Job(
        id=job_id, user_id=user_id,
        config_json="{}", dialogue_json="[]", images_dir="", output_dir="",
    ))
    db.commit()
    return job_id


def _clips(db, job_id, scene_indexes, contract=None, version=None,
           roles=None):
    """One clip row per entry of `scene_indexes`, in clip_index order."""
    made = []
    for i, si in enumerate(scene_indexes):
        role = roles[i] if roles else None
        c = Clip(
            job_id=job_id, clip_index=i, dialogue_id=i + 1,
            dialogue_text=f"line {i}",
            status=ClipStatus.COMPLETED.value, approval_status="approved",
            scene_index=si, clip_role=role,
            clip_contract_json=contract, clip_contract_version=version,
        )
        db.add(c)
        made.append(c)
    db.commit()
    return made


def _update(db, job_id, markdown, user_id="u1"):
    return asyncio.run(main.update_job_clip_contracts(
        job_id=job_id,
        req=main.ClipContractUpdate(markdown=markdown),
        db=db,
        current_user=types.SimpleNamespace(id=user_id),
    ))


def _rows(db, job_id):
    return db.query(Clip).filter(
        Clip.job_id == job_id).order_by(Clip.clip_index).all()


def _stored(db, job_id):
    return [(r.clip_contract_version, r.clip_contract_json)
            for r in _rows(db, job_id)]


def _scene_decls(markdown):
    """What the parser says each scene declares — the expected values."""
    scenes = image_platform.parse_scene_table(markdown)["scenes"]
    return [(s["clip_contract_version"], s["clip_contract_json"])
            for s in scenes]


# --------------------------------------------------------------------------
# 1. the happy path, and the mapping it proves
# --------------------------------------------------------------------------

def test_a_multi_scene_build_stamps_each_clip_with_its_own_scene():
    db = _session()
    job_id = _job(db)
    # scene 1 -> clips 0 and 1, scene 2 (silent, no lines) -> clip 2
    _clips(db, job_id, [1, 1, 2])

    resp = _update(db, job_id, GOOD_MD)

    assert resp["scenes"] == 2
    assert resp["clips_matched"] == 3
    assert resp["stamped"] == 3
    assert resp["cleared"] == 0

    s1, s2 = _scene_decls(GOOD_MD)
    assert _stored(db, job_id) == [s1, s1, s2]
    # and the two scenes really do declare different things, so the assertion
    # above could have caught a stamp that slid by one
    assert s1[1] != s2[1]
    assert json.loads(s1[1])["input_mode"] == "frames"
    assert json.loads(s2[1])["input_mode"] == "ingredients"


def test_the_mapping_holds_under_both_scene_index_schemes():
    """`Clip.scene_index` means two different things depending on which path
    wrote the row: the markdown's `### Scene N` on the import path
    (`main.create_job`), and the 0-based enumerate position on
    `image_platform.promote_batch_to_video`. A stamp keyed on the number would
    be off by one on half the jobs in the database; a positional stamp is
    right on both, and this is the test that says so.
    """
    expected = _scene_decls(GOOD_MD)
    for scheme in ([1, 1, 2], [0, 0, 1]):
        db = _session()
        job_id = _job(db)
        _clips(db, job_id, scheme)

        _update(db, job_id, GOOD_MD)

        assert _stored(db, job_id) == [expected[0], expected[0], expected[1]], \
            f"scene_index scheme {scheme} mapped wrong"


def test_a_text_card_scene_clears_its_clip_and_says_so():
    """A text_card is drawn by ffmpeg and never reaches Flow, so the parser
    gives it a NULL declaration and import writes NULL. The backfill writes the
    same value — the ONE clearing this route does, and it is counted apart."""
    db = _session()
    job_id = _job(db)
    _clips(db, job_id, [1, 1, 2], contract='{"stale":true}', version=1)

    resp = _update(db, job_id, TEXT_CARD_MD)

    assert resp["stamped"] == 2
    assert resp["cleared"] == 1
    rows = _rows(db, job_id)
    assert rows[2].clip_contract_version is None
    assert rows[2].clip_contract_json is None


def test_spawned_pair_and_plate_rows_are_skipped():
    """Phase 3a spawns audio_pair / composite_plate rows at clip_index
    100000+/200000+ from their partner, and the import path gives them no
    contract either. Stamping them would invent a state import cannot make."""
    db = _session()
    job_id = _job(db)
    _clips(db, job_id, [1, 1, 2])
    db.add(Clip(job_id=job_id, clip_index=100000, dialogue_id=1,
                dialogue_text="vo", status=ClipStatus.COMPLETED.value,
                scene_index=1, clip_role="audio_pair"))
    db.add(Clip(job_id=job_id, clip_index=200000, dialogue_id=1,
                dialogue_text="", status=ClipStatus.COMPLETED.value,
                scene_index=1, clip_role="composite_plate"))
    db.commit()

    resp = _update(db, job_id, GOOD_MD)

    assert resp["clips_matched"] == 3
    assert resp["skipped_spawned"] == 2
    spawned = [r for r in _rows(db, job_id) if r.clip_index >= 100000]
    assert [r.clip_contract_version for r in spawned] == [None, None]


# --------------------------------------------------------------------------
# 2. idempotence
# --------------------------------------------------------------------------

def test_running_it_twice_changes_nothing():
    db = _session()
    job_id = _job(db)
    _clips(db, job_id, [1, 1, 2])

    first = _update(db, job_id, GOOD_MD)
    after_first = _stored(db, job_id)
    second = _update(db, job_id, GOOD_MD)

    assert first["unchanged"] == 0        # they were all NULL to begin with
    assert second["unchanged"] == 3       # ...and identical the second time
    assert second["stamped"] == 3
    assert _stored(db, job_id) == after_first


# --------------------------------------------------------------------------
# 3. a mapping that cannot be proved refuses the WHOLE job
# --------------------------------------------------------------------------

def test_a_clip_count_that_does_not_match_refuses_everything():
    db = _session()
    job_id = _job(db)
    _clips(db, job_id, [1, 1])            # build predicts 3, job has 2

    with pytest.raises(HTTPException) as exc:
        _update(db, job_id, GOOD_MD)

    assert exc.value.status_code == 409
    assert "predicts 3" in str(exc.value.detail)
    assert _stored(db, job_id) == [(None, None), (None, None)]


def test_scene_boundaries_in_the_wrong_place_refuse_everything():
    """Right count, wrong shape: the build holds 2 clips then 1, these rows
    hold 1 then 2. A positional stamp would give clip 1 scene 2's contract."""
    db = _session()
    job_id = _job(db)
    _clips(db, job_id, [1, 2, 2])

    with pytest.raises(HTTPException) as exc:
        _update(db, job_id, GOOD_MD)

    assert exc.value.status_code == 409
    assert "scene boundaries" in str(exc.value.detail).lower()
    assert _stored(db, job_id) == [(None, None)] * 3


def test_scene_index_out_of_order_refuses_everything():
    db = _session()
    job_id = _job(db)
    _clips(db, job_id, [2, 2, 1])         # right shape, descending

    with pytest.raises(HTTPException) as exc:
        _update(db, job_id, GOOD_MD)

    assert exc.value.status_code == 409
    assert _stored(db, job_id) == [(None, None)] * 3


def test_a_null_scene_index_refuses_everything():
    """`Clip.scene_index` is `Column(Integer, default=0)`, so a fresh insert
    can never leave it NULL — but the column is nullable and the migration that
    added it (`models.py:1372`) only backfilled a DEFAULT, so an old row can
    still hold one. A NULL makes the run grouping meaningless, so it refuses."""
    db = _session()
    job_id = _job(db)
    rows = _clips(db, job_id, [1, 1, 2])
    rows[0].scene_index = None            # UPDATE does not re-apply the default
    rows[1].scene_index = None
    db.commit()

    with pytest.raises(HTTPException) as exc:
        _update(db, job_id, GOOD_MD)

    assert exc.value.status_code == 409
    assert _stored(db, job_id) == [(None, None)] * 3


def test_duplicate_markdown_scene_numbers_refuse_everything():
    """The parser SORTS its scenes by `### Scene N` (`image_platform.py:7327`),
    which is what makes document order and the promote path's
    `ORDER BY scene_index` agree. Two scenes carrying the same number break
    that: the sort picks between them arbitrarily and nothing downstream can
    say which clip belongs to which."""
    db = _session()
    job_id = _job(db)
    _clips(db, job_id, [1, 1, 2])

    with pytest.raises(HTTPException) as exc:
        _update(db, job_id, _build(scene_numbers=(1, 1)))

    assert exc.value.status_code == 409
    assert "unique and ascending" in str(exc.value.detail)
    assert _stored(db, job_id) == [(None, None)] * 3


# --------------------------------------------------------------------------
# 4. a bad declaration dies here exactly as it would at import
# --------------------------------------------------------------------------

def test_a_bad_declaration_400s_and_leaves_the_stored_values_alone():
    db = _session()
    job_id = _job(db)
    good = _scene_decls(GOOD_MD)[0][1]
    _clips(db, job_id, [1, 1, 2], contract=good, version=1)

    with pytest.raises(HTTPException) as exc:
        _update(db, job_id, BAD_MD)

    assert exc.value.status_code == 400
    # the prefix send_to_platform classifies as EXIT_PARSE
    assert str(exc.value.detail).startswith("Parse error:")
    # fail-closed: a typo must never quietly wipe a working declaration
    assert _stored(db, job_id) == [(1, good)] * 3


def test_markdown_that_is_not_a_build_at_all_400s():
    db = _session()
    job_id = _job(db)
    _clips(db, job_id, [1, 1, 2])

    with pytest.raises(HTTPException) as exc:
        _update(db, job_id, "# not a build\n\njust prose\n")

    assert exc.value.status_code == 400
    assert str(exc.value.detail).startswith("Parse error:")


# --------------------------------------------------------------------------
# 5. the absent opt-in — REFUSED, not applied
# --------------------------------------------------------------------------

def test_a_build_with_no_opt_in_is_refused_and_nothing_is_un_stamped():
    """The deliberate divergence from update-finishing, which CLEARS on an
    absent section. Two reasons, both in the endpoint docstring: the parser
    already defines a missing opt-in as OUT OF SCOPE rather than "clear", and
    un-stamping is silent — the job drops back to legacy inference with nothing
    on screen to say so, and the likeliest cause is the wrong build file."""
    db = _session()
    job_id = _job(db)
    good = _scene_decls(GOOD_MD)[0][1]
    _clips(db, job_id, [1, 1, 2], contract=good, version=1)

    with pytest.raises(HTTPException) as exc:
        _update(db, job_id, NO_OPT_IN_MD)

    assert exc.value.status_code == 400
    assert "CLIP CONTRACT: v1" in str(exc.value.detail)
    assert "never clears" in str(exc.value.detail)
    assert _stored(db, job_id) == [(1, good)] * 3


def test_contract_bullets_without_the_opt_in_die_in_the_parser():
    """The other half of the absent-opt-in case, and it never reaches the
    route's own check: a build carrying the bullets with no `CLIP CONTRACT: v1`
    line is refused by the parser, because silently dropping a declaration the
    author wrote is the exact failure the rule exists to remove."""
    db = _session()
    job_id = _job(db)
    _clips(db, job_id, [1, 1, 2])

    with pytest.raises(HTTPException) as exc:
        _update(db, job_id, BULLETS_WITHOUT_OPT_IN_MD)

    assert exc.value.status_code == 400
    assert str(exc.value.detail).startswith("Parse error:")
    assert _stored(db, job_id) == [(None, None)] * 3


def test_an_indented_opt_in_does_not_count_as_one():
    """Column 0, same as the parser. A build that quoted the line inside a
    comment must not opt itself in — and with the bullets present the parser
    refuses it outright, which is the stronger answer."""
    db = _session()
    job_id = _job(db)
    _clips(db, job_id, [1, 1, 2])

    indented = GOOD_MD.replace("CLIP CONTRACT: v1",
                               "<!-- docs say:\n    CLIP CONTRACT: v1\n -->", 1)
    with pytest.raises(HTTPException) as exc:
        _update(db, job_id, indented)

    assert exc.value.status_code == 400
    assert _stored(db, job_id) == [(None, None)] * 3


# --------------------------------------------------------------------------
# 6. ownership
# --------------------------------------------------------------------------

def test_another_users_job_is_refused():
    db = _session()
    job_id = _job(db, user_id="someone-else")
    _clips(db, job_id, [1, 1, 2])

    with pytest.raises(HTTPException) as exc:
        _update(db, job_id, GOOD_MD, user_id="u1")

    # get_user_job: 404 when the job does not exist, 403 when it is not yours.
    assert exc.value.status_code == 403
    assert _stored(db, job_id) == [(None, None)] * 3


# --------------------------------------------------------------------------
# 7. the expansion helper, on its own
# --------------------------------------------------------------------------

def test_the_expansion_copies_the_import_paths_row_count():
    scenes = image_platform.parse_scene_table(GOOD_MD)["scenes"]
    got = main._v965_scene_per_clip(scenes)
    assert [pos for pos, _s in got] == [0, 0, 1]

    # a lineless scene that is NEITHER text_card NOR silent contributes
    # nothing, exactly as prepare_batch_for_video's per-line loop does — and
    # the count check then refuses the job rather than guessing.
    lineless = [{"lines": [], "scene_type": "shot", "speaker_mode": None}]
    assert main._v965_scene_per_clip(lineless) == []


_HERE = pathlib.Path(__file__).resolve().parent.parent
_MAIN_SRC = (_HERE / "main.py").read_text(encoding="utf-8")
_IP_SRC = (_HERE / "image_platform.py").read_text(encoding="utf-8")


def test_the_two_writers_of_clip_scene_index_still_disagree():
    """THE EVIDENCE FOR THE POSITIONAL MAPPING, pinned so it cannot rot.

    `Clip.scene_index` is written by two paths and they do not mean the same
    thing by it, which is why the stamp is positional:

      * `main.create_job` copies it off the dialogue line, and the line carries
        the markdown's `### Scene N` integer — `prepare_batch_for_video` joins
        the re-parse to the assignment rows with `reparsed_scenes.get(
        a.scene_index)`, which is only correct because they are the same
        number;
      * `image_platform.promote_batch_to_video` writes the 0-based ENUMERATE
        POSITION over its scene plan instead.

    If either of those ever changes, this test fails and the endpoint's
    docstring has to be re-derived rather than trusted.
    """
    assert "scene_idx = line.get('scene_index', 0)" in _MAIN_SRC
    assert "reparsed_scenes.get(a.scene_index)" in _IP_SRC
    assert "for scene_pos, (idx, n, _assignment) in enumerate(_scene_plan):" in _IP_SRC
    assert _IP_SRC.count('"scene_index": scene_pos,') >= 1


def test_the_expansion_still_mirrors_the_import_paths_own_branch():
    """`_v965_scene_per_clip` is a hand copy of the branch in
    `prepare_batch_for_video` that decides how many flat rows a scene produces.
    A copy that drifts is a wrong mapping, so both halves are pinned: the
    lineless branch condition, and the truncation that fixes the per-line count
    at exactly `len(lines)`."""
    assert "if (scene_is_text_card or scene_is_silent) and not lines:" in _IP_SRC
    # the four parallel arrays are padded then cut to len(lines) right before
    # the zip, so the zip cannot shorten the scene's clip count
    assert "veo_prompts = veo_prompts[:len(lines)]" in _IP_SRC
    assert "pads = pads[:len(lines)]" in _IP_SRC
    assert "notes = notes[:len(lines)]" in _IP_SRC


def test_spawned_rows_still_carry_the_roles_this_route_skips():
    """The skip list is only safe while Phase 3a keeps marking what it spawns.
    Both creators also offset the clip_index (100000 / 200000), so a role that
    stopped being set would still sort last — but it would be stamped, and the
    import path stamps neither."""
    assert "clip_role='audio_pair'," in _MAIN_SRC
    assert "clip_role='composite_plate'," in _MAIN_SRC
    assert "audio_pair_offset = 100000" in _MAIN_SRC
    assert "composite_plate_offset = 200000" in _MAIN_SRC


def test_runs_groups_consecutive_values():
    assert main._v965_runs([1, 1, 2, 2, 2]) == [(1, 2), (2, 3)]
    assert main._v965_runs([]) == []
    # a value that comes back later is its OWN run, which is what makes an
    # out-of-order storyboard visible instead of silently merged
    assert main._v965_runs([1, 2, 1]) == [(1, 1), (2, 1), (1, 1)]
