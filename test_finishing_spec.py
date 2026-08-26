"""v944 — the Finishing section: overlays + captions declared in the build.

The contract mirrors v943: absent section = NULL spec = byte-identical legacy
behavior; a present section is validated hard at import, never guessed at
render time."""
import sys
import pytest

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

FINISHING_OK = """## Finishing

- **captions:** none
- **overlay:** readcaption
- **overlay_age:** I'M 74
- **overlay_block:** No supplements / No gym / 5 boring things / to feel 40
- **overlay_footer:** (READ CAPTION)
"""


def _parse(md):
    from image_platform import parse_finishing_section
    return parse_finishing_section(md)


# ---------------------------------------------------------------------------
# Task 1 — the parser
# ---------------------------------------------------------------------------

def test_absent_section_is_none():
    assert _parse("# a build with no finishing\n\n### Scene 1\n") is None


def test_full_section_parses():
    spec = _parse(FINISHING_OK)
    assert spec == {
        "captions": "none",
        "overlay": "readcaption",
        "overlay_age": "I'M 74",
        "overlay_block": ["No supplements", "No gym", "5 boring things", "to feel 40"],
        "overlay_footer": "(READ CAPTION)",
    }


def test_captions_none_alone_is_valid():
    spec = _parse("## Finishing\n\n- **captions:** none\n")
    assert spec == {"captions": "none", "overlay": "none"}


def test_unknown_captions_value_hard_fails():
    with pytest.raises(ValueError, match="captions"):
        _parse("## Finishing\n\n- **captions:** rainbow-sparkle\n")


def test_unknown_overlay_engine_hard_fails():
    with pytest.raises(ValueError, match="overlay"):
        _parse("## Finishing\n\n- **overlay:** stickers\n")


def test_readcaption_without_age_hard_fails():
    with pytest.raises(ValueError, match="overlay_age"):
        _parse("## Finishing\n\n- **overlay:** readcaption\n")


def test_overlay_fields_without_engine_hard_fail():
    with pytest.raises(ValueError, match="overlay"):
        _parse("## Finishing\n\n- **overlay_age:** I'M 74\n")


def test_section_stops_at_the_next_header():
    """A bullet under the NEXT section is not a finishing field. Without the
    boundary the parser would read the whole rest of the build."""
    spec = _parse("## Finishing\n\n- **captions:** none\n\n## Images\n\n"
                  "- **overlay:** readcaption\n")
    assert spec == {"captions": "none", "overlay": "none"}


def test_known_caption_template_is_accepted():
    """The allowed set is the pipeline's own template list, not a copy of it.
    'korella' is a local style; 'word-focus' is a builtin."""
    assert _parse("## Finishing\n\n- **captions:** korella\n")["captions"] == "korella"
    assert _parse("## Finishing\n\n- **captions:** word-focus\n")["captions"] == "word-focus"


# ---------------------------------------------------------------------------
# Task 2 — the columns and their migrations
# ---------------------------------------------------------------------------

def _src(name):
    import pathlib
    return (pathlib.Path(__file__).parent / name).read_text(encoding="utf-8")


def test_both_migration_dialects_register_finishing_spec():
    src = _src("image_platform.py")
    assert src.count("finishing_spec") >= 4  # model, to_dict, sqlite mig, pg mig
    models = _src("models.py")
    assert "finishing_spec" in models


def test_batch_column_and_migrations_exist():
    """The batch is ImageJobBatch, and production is Postgres — a SQLite-only
    migration entry means the column never exists live and every write 500s."""
    from image_platform import ImageJobBatch
    assert "finishing_spec" in ImageJobBatch.__table__.columns
    assert ImageJobBatch.__table__.columns["finishing_spec"].nullable
    src = _src("image_platform.py")
    assert "ALTER TABLE image_job_batches ADD COLUMN finishing_spec TEXT" in src
    assert ("ALTER TABLE image_job_batches ADD COLUMN IF NOT EXISTS "
            "finishing_spec TEXT") in src


def test_job_column_and_migrations_exist():
    from models import Job
    assert "finishing_spec" in Job.__table__.columns
    assert Job.__table__.columns["finishing_spec"].nullable
    src = _src("models.py")
    assert "ALTER TABLE jobs ADD COLUMN finishing_spec TEXT" in src
    assert "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS finishing_spec TEXT" in src


# ---------------------------------------------------------------------------
# Task 3 — the spec travels import -> batch -> BOTH promote paths -> Job
# ---------------------------------------------------------------------------

def test_finishing_spec_travels_import_to_job():
    src = _src("image_platform.py")
    imp = src.index("def _import_scene_table_impl")
    assert "parse_finishing_section" in src[imp:imp + 20000]
    prom = src.index("def promote_batch_to_video")
    assert "finishing_spec" in src[prom:prom + 30000]
    main = _src("main.py")
    cj = main.index("def _create_job_impl")
    assert "finishing_spec" in main[cj:cj + 30000]


def test_browser_promote_reads_the_BATCH_not_the_payload():
    """The browser promote does NOT need a new field in static/index.html: the
    request already carries image_batch_id and the server already loads that
    batch row to stamp promoted_video_job_id. Copy the spec there — a payload
    field would be one more hand-maintained enumeration to drift (v892.2)."""
    main = _src("main.py")
    cj = main.index("def _create_job_impl")
    body = main[cj:cj + 30000]
    stamp = body.index("batch.promoted_video_job_id = job_id")
    # the copy sits in the same block that already has the owned batch row
    assert "finishing_spec" in body[stamp - 2000:stamp + 2000]
    index_html = _src("static/index.html")
    assert "finishing_spec" not in index_html


def test_resync_import_refreshes_the_spec_too():
    """v891 re-import updates an existing batch row in place. A corrected build
    that ADDS or REMOVES its ## Finishing section has to move the stored value,
    or the batch keeps the old finish forever."""
    src = _src("image_platform.py")
    resync = src.index("if resync_batch is not None:")
    assert "finishing_spec" in src[resync:resync + 2000]


# ---------------------------------------------------------------------------
# Task 4 — queue_autoedit derives its defaults from the job's spec
# ---------------------------------------------------------------------------

def test_derive_autoedit_defaults_from_spec():
    from main import derive_autoedit_defaults
    spec = {"captions": "none", "overlay": "readcaption", "overlay_age": "I'M 74"}
    req = {"template": "korella", "captions_enabled": True}   # user sent defaults
    out = derive_autoedit_defaults(req, spec, request_was_explicit=set())
    assert out["captions_enabled"] is False
    assert out["overlay_spec"] == spec


def test_explicit_request_beats_the_spec():
    from main import derive_autoedit_defaults
    spec = {"captions": "none", "overlay": "none"}
    out = derive_autoedit_defaults({"captions_enabled": True}, spec,
                                   request_was_explicit={"captions_enabled"})
    assert out["captions_enabled"] is True


def test_no_spec_changes_nothing():
    from main import derive_autoedit_defaults
    req = {"template": "korella", "captions_enabled": True}
    assert derive_autoedit_defaults(dict(req), None, set()) == {**req, "overlay_spec": None}


def test_derive_does_not_mutate_the_request_it_was_given():
    from main import derive_autoedit_defaults
    req = {"template": "korella", "captions_enabled": True}
    derive_autoedit_defaults(req, {"captions": "none", "overlay": "none"}, set())
    assert req == {"template": "korella", "captions_enabled": True}


def test_named_caption_template_turns_captions_on_and_picks_the_style():
    from main import derive_autoedit_defaults
    out = derive_autoedit_defaults({"template": "korella", "captions_enabled": False},
                                   {"captions": "word-focus", "overlay": "none"}, set())
    assert out["template"] == "word-focus"
    assert out["captions_enabled"] is True
    assert out["overlay_spec"] is None


def test_explicit_template_beats_the_spec_template():
    from main import derive_autoedit_defaults
    out = derive_autoedit_defaults({"template": "korella", "captions_enabled": True},
                                   {"captions": "word-focus", "overlay": "none"},
                                   request_was_explicit={"template"})
    assert out["template"] == "korella"


def test_overlay_none_carries_no_overlay_spec():
    from main import derive_autoedit_defaults
    out = derive_autoedit_defaults({}, {"captions": "korella", "overlay": "none"}, set())
    assert out["overlay_spec"] is None


def test_queue_autoedit_calls_the_derive_and_rides_repair_json():
    """The spec travels to the worker INSIDE repair_json — the field that
    already round-trips (server claim + local worker both hand it back to
    run_autoedit). No new AutoEditRun column to migrate."""
    main = _src("main.py")
    q = main.index("async def queue_autoedit")
    body = main[q:q + 8000]
    assert "derive_autoedit_defaults" in body
    assert "finishing_spec" in body
    assert "overlay_spec" in body
    assert "model_fields_set" in body


def test_overlay_spec_is_a_known_repair_setting():
    """normalize_repairs rejects unknown keys outright, so a repair that is not
    declared in DEFAULT_REPAIRS is a 400 at queue time, not a feature."""
    from autoedit_qc import DEFAULT_REPAIRS, normalize_repairs
    assert "overlay_spec" in DEFAULT_REPAIRS
    assert DEFAULT_REPAIRS["overlay_spec"] is None
    assert normalize_repairs({})["overlay_spec"] is None
    spec = {"overlay": "readcaption", "overlay_age": "I'M 74"}
    assert normalize_repairs({"overlay_spec": spec})["overlay_spec"] == spec


def test_bad_overlay_spec_is_rejected_at_queue_time():
    from autoedit_qc import normalize_repairs
    with pytest.raises(ValueError):
        normalize_repairs({"overlay_spec": "readcaption"})       # not a dict
    with pytest.raises(ValueError):
        normalize_repairs({"overlay_spec": {"overlay": "stickers"}})
    with pytest.raises(ValueError):
        normalize_repairs({"overlay_spec": {"overlay": "readcaption"}})   # no age


def test_autoedit_request_accepts_an_overlay_spec():
    from main import AutoEditRequest
    assert "overlay_spec" in AutoEditRequest.model_fields
    assert AutoEditRequest().overlay_spec is None


# ---------------------------------------------------------------------------
# Task 5 — the overlay stage in the pipeline
# ---------------------------------------------------------------------------

def test_overlay_stage_skipped_without_spec():
    from autoedit_pipeline import overlay_stage_plan
    assert overlay_stage_plan(None) is None
    assert overlay_stage_plan({"overlay": "none"}) is None


def test_overlay_stage_runs_for_readcaption():
    from autoedit_pipeline import overlay_stage_plan
    plan = overlay_stage_plan({"overlay": "readcaption", "overlay_age": "I'M 74"})
    assert plan["engine"] == "readcaption"
    assert plan["age"] == "I'M 74"


def test_overlay_plan_carries_block_and_footer():
    from autoedit_pipeline import overlay_stage_plan
    plan = overlay_stage_plan({
        "overlay": "readcaption", "overlay_age": "I'M 74",
        "overlay_block": ["No supplements", "No gym"],
        "overlay_footer": "(READ CAPTION)",
    })
    assert plan["body"] == ["No supplements", "No gym"]
    assert plan["route"] == "(READ CAPTION)"


def test_overlay_plan_defaults_the_route_line():
    """The tool's own default. A read-caption overlay with no footer still says
    (READ CAPTION) — that IS the call to action the format is named after."""
    from autoedit_pipeline import overlay_stage_plan
    plan = overlay_stage_plan({"overlay": "readcaption", "overlay_age": "I'M 74"})
    assert plan["route"] == "(READ CAPTION)"
    assert plan["body"] == []


def test_overlay_plan_rejects_readcaption_without_age():
    from autoedit_pipeline import overlay_stage_plan, AutoEditError
    with pytest.raises(AutoEditError):
        overlay_stage_plan({"overlay": "readcaption"})


def test_overlay_plan_rejects_an_unknown_engine():
    from autoedit_pipeline import overlay_stage_plan, AutoEditError
    with pytest.raises(AutoEditError):
        overlay_stage_plan({"overlay": "stickers"})


def test_the_doctrine_constants_survived_the_port():
    """The numbers are measured, not chosen — they carry the whole reason the
    overlay looks like the reference accounts. A port that quietly rounds them
    is a port that ships a different overlay."""
    import autoedit_pipeline as p
    assert p.RC_FONT.lower().endswith("gothicb.ttf")          # Century Gothic Bold
    assert (p.RC_SAFE_TOP, p.RC_SAFE_BOTTOM) == (0.06, 0.79)  # organic Reels zone
    assert p.RC_SPEC == {"age": (94, 0), "body": (47, 0), "route": (52, 0)}
    assert p.RC_OUTLINE == 10
    # BODY_PITCH corrected 84 -> 49 on 2026-08-26: measured from the account's
    # own posted winner (block pitch ~71px at 1080 = 47.3 spec units; 49
    # renders 73.5px and matches). 84 rendered ~125px — the doubled spacing
    # the operator flagged on two finals before it was traced.
    assert (p.RC_BODY_PITCH, p.RC_GAP_AGE_BODY, p.RC_GAP_BODY_ROUTE) == (49, 67, 84)
    assert p.RC_AGE_MAX_W == 0.35 and p.RC_MAX_TEXT_W == 0.90
    assert p.RC_ASS_PIL_WIDTH_RATIO == 0.81


def test_the_placement_engine_came_across_whole():
    """Never-cross-face + the subject-relative windows + the separate age/block
    elements are the doctrine. Assert the functions exist, not their pixels."""
    import autoedit_pipeline as p
    for fn in ("rc_head_band", "rc_coverage_profile", "rc_place_min_coverage",
               "rc_smart_layout", "rc_layout", "rc_occupancy_layout",
               "rc_fit_scale", "rc_draw_line", "rc_write_ass", "rc_burn_ass",
               "rc_build_ass", "render_readcaption_overlay"):
        assert callable(getattr(p, fn)), fn


def test_the_age_line_and_the_block_are_separate_elements():
    """@agelessjudy drops the body block at t=10.2s while the age line stays;
    @noemi moves the age line while the block holds. One stacked element cannot
    do either, so the split has to survive the port."""
    import inspect
    import autoedit_pipeline as p
    src = inspect.getsource(p.rc_layout)
    assert '"split"' in src              # age alone above, block below the chin
    assert '"all-top"' in src and '"all-low"' in src


def test_ass_escaping_survived_the_port():
    import autoedit_pipeline as p
    assert p._rc_ass_escape("a{b}c\\d\ne") == r"a\{b\}c\\d\Ne"
    assert p._rc_ass_time(3725.5) == "1:02:05.50"


def test_pil_is_lazy_imported_not_at_module_level():
    """The module MUST stay importable on Render, where PIL, cv2 and ultralytics
    are not installed. The port is the easiest way to break that."""
    src = _src("autoedit_pipeline.py")
    head = src[:src.index("class AutoEditError")]
    assert "PIL" not in head and "ultralytics" not in head
    import autoedit_pipeline  # noqa: F401  — proves it imports here too


# ---------------------------------------------------------------------------
# Task 6 — THE LEGACY PATH. A build that declares nothing must behave exactly
# as it did before v944 existed. This is the same regression contract v943
# carried, and it is the section that matters most: the feature is optional,
# so every job that already exists is relying on it.
# ---------------------------------------------------------------------------

LEGACY_BUILD = """# nuri-korella-something-v1

## §0 Citations Check

- METHOD: FULL-BUILD

## Images

### Image 1

- **Image prompt:**

```
a woman in a kitchen
```

## Storyboard

### Scene 1

- **image:** image_1
- **line:** american men over sixty are getting this wrong
- **action_note:** she lifts the jar [Start beat]
"""


def test_a_build_with_no_finishing_section_parses_to_none():
    assert _parse(LEGACY_BUILD) is None


def test_a_none_spec_is_stored_as_NULL_not_as_the_string_null():
    """The importer's own expression. `json.dumps(None)` is the string "null",
    which is TRUTHY and would read back as a declared spec forever."""
    import json
    spec = _parse(LEGACY_BUILD)
    stored = json.dumps(spec, sort_keys=True) if spec else None
    assert stored is None


def test_derive_with_no_spec_returns_the_request_unchanged():
    from main import derive_autoedit_defaults
    req = {"template": "korella", "captions_enabled": True, "trim_start_s": 1.5}
    out = derive_autoedit_defaults(dict(req), None, set())
    assert out == {**req, "overlay_spec": None}


def test_explicit_overlay_spec_survives_a_specless_job():
    """The live bug (run ef707c39): a pre-v944 job has no finishing_spec, the
    caller sends the overlay explicitly, and the no-spec branch ate it. An
    explicit request field must ALWAYS win — that is the whole rev-459 rule."""
    from main import derive_autoedit_defaults
    overlay = {"overlay": "readcaption", "overlay_age": "I'M 74"}
    out = derive_autoedit_defaults(
        {"template": "korella", "captions_enabled": False, "overlay_spec": overlay},
        None,
        request_was_explicit={"captions_enabled", "overlay_spec"},
    )
    assert out["overlay_spec"] == overlay
    assert out["captions_enabled"] is False


def test_normalize_repairs_defaults_are_otherwise_untouched():
    """The only new key is overlay_spec, and it defaults to None. Any other
    default moving would change every existing job's edit."""
    from autoedit_qc import normalize_repairs
    out = normalize_repairs({})
    assert out["overlay_spec"] is None
    assert out["captions_enabled"] is True
    assert out["pip_enabled"] is True
    assert out["hook_corner"] is None
    assert out["trim_start_s"] == 0.0 and out["trim_end_s"] == 0.0
    assert out["chroma_similarity"] == 0.10 and out["chroma_blend"] == 0.02
    assert out["music_filename"] is None and out["music_db"] == -20.0
    assert out["hook_bg"] is None


def test_the_pipeline_overlay_call_site_is_guarded():
    """The v943 branch-spy pattern: read the source and prove the new stage
    cannot run on a run that carries no spec."""
    src = _src("autoedit_pipeline.py")
    body = src[src.index("def run_autoedit"):]
    call = body.index("render_readcaption_overlay(")
    guard = body[:call]
    assert 'overlay_stage_plan((repairs or {}).get("overlay_spec"))' in guard
    assert "if _rc_plan is not None:" in guard


def test_overlay_stage_plan_is_none_for_every_legacy_shape():
    from autoedit_pipeline import overlay_stage_plan
    for legacy in (None, {}, {"overlay": "none"}, {"captions": "korella", "overlay": "none"}):
        assert overlay_stage_plan(legacy) is None


# ---------------------------------------------------------------------------
# v944.1 — the line pitch: measured from the posted winner, declarable per build
# ---------------------------------------------------------------------------

def test_overlay_pitch_parses_and_rides_the_spec():
    spec = _parse("## Finishing\n\n- **overlay:** readcaption\n"
                  "- **overlay_age:** I'M 74\n- **overlay_pitch:** 49\n")
    assert spec["overlay_pitch"] == 49


def test_overlay_pitch_out_of_range_hard_fails():
    with pytest.raises(ValueError, match="overlay_pitch"):
        _parse("## Finishing\n\n- **overlay:** readcaption\n"
               "- **overlay_age:** I'M 74\n- **overlay_pitch:** 200\n")
    with pytest.raises(ValueError, match="overlay_pitch"):
        _parse("## Finishing\n\n- **overlay:** readcaption\n"
               "- **overlay_age:** I'M 74\n- **overlay_pitch:** wide\n")


def test_overlay_plan_carries_the_pitch():
    from autoedit_pipeline import overlay_stage_plan
    plan = overlay_stage_plan({"overlay": "readcaption", "overlay_age": "I'M 74",
                               "overlay_pitch": 49})
    assert plan["pitch"] == 49
    assert overlay_stage_plan({"overlay": "readcaption",
                               "overlay_age": "I'M 74"})["pitch"] is None


def test_qc_normalizes_and_bounds_the_pitch():
    from autoedit_qc import normalize_repairs
    out = normalize_repairs({"overlay_spec": {"overlay": "readcaption",
                                              "overlay_age": "I'M 74",
                                              "overlay_pitch": "49"}})
    assert out["overlay_spec"]["overlay_pitch"] == 49
    with pytest.raises(ValueError, match="pitch"):
        normalize_repairs({"overlay_spec": {"overlay": "readcaption",
                                            "overlay_age": "I'M 74",
                                            "overlay_pitch": 500}})


def test_the_default_pitch_is_the_measured_account_constant():
    """The reference (posted winner Dbn4yKwxCrl): block pitch ~71px at 1080 =
    47.3 spec units; 49 renders 73.5px and matches. 84 rendered ~125px — the
    doubled spacing the operator flagged on two finals."""
    from autoedit_pipeline import RC_BODY_PITCH
    assert RC_BODY_PITCH == 49


# ---- v947: auto_finish + export_* + autoedit_* + unknown-key fail-closed ----
from image_platform import parse_finishing_section

BASE = "## Finishing\n\n- **captions:** none\n"


def _fin(extra: str):
    return parse_finishing_section(BASE + extra + "\n## Next Section\n")


def test_auto_finish_parses_on_off_and_defaults_off():
    assert _fin("- **auto_finish:** on\n")["auto_finish"] == "on"
    assert "auto_finish" not in _fin("- **auto_finish:** off\n")
    assert "auto_finish" not in _fin("")


def test_auto_finish_bad_value_fails_closed():
    with pytest.raises(ValueError):
        _fin("- **auto_finish:** yes\n")


def test_export_fields_validate_through_the_real_model():
    spec = _fin("- **export_remove_silence:** true\n- **export_music_gain_db:** -22\n")
    assert spec["export"] == {"remove_silence": True, "music_gain_db": -22.0}


def test_export_only_declared_fields_are_stored():
    spec = _fin("- **export_smart_trim:** false\n")
    assert set(spec["export"]) == {"smart_trim"}   # sparse — no frozen defaults


def test_export_unknown_field_fails_closed():
    with pytest.raises(ValueError):
        _fin("- **export_does_not_exist:** 1\n")


def test_export_bad_value_fails_closed():
    with pytest.raises(ValueError):
        _fin("- **export_music_gain_db:** loud\n")     # not a number
    with pytest.raises(ValueError):
        _fin("- **export_frames_to_cut_start:** 99\n")  # outside le=30 bound


def test_export_json_value_beat_pins():
    spec = _fin('- **export_beat_pins:** {"3": 2.47}\n')
    assert spec["export"]["beat_pins"] == {"3": 2.47}


def test_autoedit_fields_validate_and_reserved_names_rejected():
    spec = _fin("- **autoedit_pip_enabled:** false\n- **autoedit_music_db:** -18\n")
    assert spec["autoedit"] == {"pip_enabled": False, "music_db": -18.0}
    for reserved in ("template", "captions_enabled", "overlay_spec"):
        with pytest.raises(ValueError):
            _fin(f"- **autoedit_{reserved}:** x\n")


def test_unknown_bullet_fails_closed():
    with pytest.raises(ValueError):
        _fin("- **exprot_music_gain_db:** -22\n")   # the typo class this exists for


def test_v944_only_section_is_unchanged():
    spec = parse_finishing_section(
        "## Finishing\n\n- **captions:** none\n- **overlay:** readcaption\n"
        "- **overlay_age:** I'M 74\n")
    assert spec["captions"] == "none" and spec["overlay"] == "readcaption"
    assert "export" not in spec and "autoedit" not in spec and "auto_finish" not in spec


def test_absent_section_still_none():
    assert parse_finishing_section("# build\n\n## Storyboard\n") is None


def test_malformed_bullet_key_fails_closed():
    """The leftover check can only see keys the bullet regex could READ.
    `(\\w+)` cannot read a hyphenated key, so this line never reaches `fields`
    and would be dropped in silence — the same wrong-render-hours-later class
    the unknown-key check exists to kill, one regex miss upstream of it."""
    with pytest.raises(ValueError, match="malformed"):
        _fin("- **export-music-gain:** -22\n")
    with pytest.raises(ValueError, match="malformed"):
        _fin("- just some words, no key at all\n")


def test_prose_and_html_comments_are_not_bullets():
    """Only bullet-SHAPED lines are candidates. Prose, blank lines and the
    skeleton's trailing HTML comment (which wraps onto a dashed line) must
    still parse — see code/template_new_format.md:999-1005."""
    spec = parse_finishing_section(
        "## Finishing\n\n"
        "Job-level, ONE section per build.\n\n"
        "- **captions:** none\n"
        "<!-- v944 note about placement,\n"
        "     - and a dashed line inside the comment -->\n"
        "\n"
        "## Next Section\n")
    assert spec == {"captions": "none", "overlay": "none"}
