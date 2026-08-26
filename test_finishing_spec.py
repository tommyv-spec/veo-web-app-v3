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
