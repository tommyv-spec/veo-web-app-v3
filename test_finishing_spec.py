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
    assert (p.RC_BODY_PITCH, p.RC_GAP_AGE_BODY, p.RC_GAP_BODY_ROUTE) == (84, 67, 84)
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
