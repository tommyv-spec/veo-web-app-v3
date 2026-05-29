"""Unit tests for the post-render lifecycle tracker.

Pattern: direct import via importlib, plain pytest assertions, no fixtures.
Matches code/test_auto_redo_cap.py style.
"""
import importlib.util
import pathlib

_SPEC = importlib.util.spec_from_file_location(
    "config",
    pathlib.Path(__file__).parent / "config.py",
)
_MOD = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MOD)

LifecycleStage = _MOD.LifecycleStage


def test_lifecycle_stage_values():
    assert LifecycleStage.AWAITING_APPROVAL.value == "awaiting_approval"
    assert LifecycleStage.AWAITING_EXPORT.value == "awaiting_export"
    assert LifecycleStage.AWAITING_FINISHING.value == "awaiting_finishing"
    assert LifecycleStage.PUBLISHED.value == "published"


def test_lifecycle_stage_order():
    """The natural iteration order defines the forward [→ next] path."""
    expected = [
        "awaiting_approval",
        "awaiting_export",
        "awaiting_finishing",
        "published",
    ]
    assert [s.value for s in LifecycleStage] == expected


def test_job_has_lifecycle_columns():
    """The 7 new columns must be present on the Job SQLAlchemy model."""
    _job_spec = importlib.util.spec_from_file_location(
        "models",
        pathlib.Path(__file__).parent / "models.py",
    )
    _job_mod = importlib.util.module_from_spec(_job_spec)
    import sys, os
    os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
    _job_spec.loader.exec_module(_job_mod)
    Job = _job_mod.Job
    cols = {c.name for c in Job.__table__.columns}
    expected = {
        "lifecycle_stage",
        "approval_at",
        "export_at",
        "finishing_at",
        "published_at",
        "notes",
        "archived",
    }
    missing = expected - cols
    assert not missing, f"Missing columns on Job: {missing}"


# ---------------------------------------------------------------------------
# Task 3: apply_lifecycle_change pure-helper tests
# Option B: load code/lifecycle.py via importlib (avoids FastAPI side effects).
# ---------------------------------------------------------------------------
_LIFECYCLE_SPEC = importlib.util.spec_from_file_location(
    "lifecycle",
    pathlib.Path(__file__).parent / "lifecycle.py",
)
_LIFECYCLE_MOD = importlib.util.module_from_spec(_LIFECYCLE_SPEC)
_LIFECYCLE_SPEC.loader.exec_module(_LIFECYCLE_MOD)

apply_lifecycle_change = _LIFECYCLE_MOD.apply_lifecycle_change


from datetime import datetime
from types import SimpleNamespace


def _stub_job(**overrides):
    """Minimal duck-typed Job stand-in (no DB) for pure-helper tests."""
    base = dict(
        lifecycle_stage=None,
        approval_at=None,
        export_at=None,
        finishing_at=None,
        published_at=None,
        notes=None,
        archived=False,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def test_apply_lifecycle_change_sets_first_entry_timestamp():
    job = _stub_job()
    now = datetime(2026, 5, 29, 10, 0, 0)
    apply_lifecycle_change(job, stage="awaiting_export", notes=None, now=now)

    assert job.lifecycle_stage == "awaiting_export"
    assert job.export_at == now
    assert job.approval_at is None  # only the target stage's timestamp is set


def test_apply_lifecycle_change_preserves_existing_timestamp_on_reentry():
    first = datetime(2026, 5, 29, 10, 0, 0)
    later = datetime(2026, 5, 30, 12, 0, 0)

    job = _stub_job(lifecycle_stage="awaiting_export", export_at=first)
    apply_lifecycle_change(job, stage="awaiting_approval", notes=None, now=later)
    apply_lifecycle_change(job, stage="awaiting_export", notes=None, now=later)

    # COALESCE behavior — re-entering a stage does NOT overwrite the original.
    assert job.export_at == first
    assert job.approval_at == later  # was None, now set


def test_apply_lifecycle_change_merges_notes():
    now = datetime(2026, 5, 29, 10, 0, 0)
    job = _stub_job(lifecycle_stage="awaiting_approval", notes="initial")
    apply_lifecycle_change(job, stage=None, notes="updated", now=now)

    assert job.notes == "updated"
    assert job.lifecycle_stage == "awaiting_approval"  # stage unchanged


def test_apply_lifecycle_change_clears_notes_explicitly():
    now = datetime(2026, 5, 29, 10, 0, 0)
    job = _stub_job(notes="prior note")
    apply_lifecycle_change(job, stage=None, notes="", now=now)

    assert job.notes == ""  # empty string is an intentional clear


def test_apply_lifecycle_change_rejects_invalid_stage():
    import pytest

    job = _stub_job()
    with pytest.raises(ValueError, match="invalid lifecycle stage"):
        apply_lifecycle_change(job, stage="not_a_stage", notes=None, now=datetime.utcnow())


def test_apply_lifecycle_change_clears_lifecycle_via_explicit_none():
    """Passing stage=None with the sentinel clear=True empties lifecycle."""
    job = _stub_job(lifecycle_stage="awaiting_export", export_at=datetime(2026, 5, 29))
    apply_lifecycle_change(job, stage=None, notes=None, now=datetime.utcnow(), clear=True)

    assert job.lifecycle_stage is None
    # Timestamps are intentionally preserved for audit purposes.
    assert job.export_at == datetime(2026, 5, 29)


# ---------------------------------------------------------------------------
# Task 4: compute_stuck_days pure-helper tests
# Same importlib pattern as Task 3 — load from code/lifecycle.py directly.
# ---------------------------------------------------------------------------
compute_stuck_days = _LIFECYCLE_MOD.compute_stuck_days


def test_compute_stuck_days_none_when_no_lifecycle():
    job = _stub_job(lifecycle_stage=None)
    assert compute_stuck_days(job, now=datetime(2026, 5, 29)) is None


def test_compute_stuck_days_zero_on_same_day():
    now = datetime(2026, 5, 29, 14, 0, 0)
    job = _stub_job(
        lifecycle_stage="awaiting_export",
        export_at=datetime(2026, 5, 29, 10, 0, 0),
    )
    assert compute_stuck_days(job, now=now) == 0


def test_compute_stuck_days_counts_full_days():
    job = _stub_job(
        lifecycle_stage="awaiting_finishing",
        finishing_at=datetime(2026, 5, 25, 10, 0, 0),
    )
    now = datetime(2026, 5, 29, 11, 0, 0)
    assert compute_stuck_days(job, now=now) == 4


def test_compute_stuck_days_none_when_stage_timestamp_missing():
    """Defensive: corrupt state where stage is set but timestamp is null."""
    job = _stub_job(lifecycle_stage="awaiting_export", export_at=None)
    assert compute_stuck_days(job, now=datetime(2026, 5, 29)) is None


# ---------------------------------------------------------------------------
# Task 5: JobResponse pydantic model includes all 8 lifecycle fields
# ---------------------------------------------------------------------------
def test_job_response_includes_lifecycle_fields():
    """The JobResponse pydantic model must expose all 8 lifecycle fields."""
    import importlib.util, pathlib, sys, os

    os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
    os.environ.setdefault("SECRET_KEY", "test-secret")
    os.environ.setdefault("R2_BUCKET_NAME", "test-bucket")
    os.environ.setdefault("R2_ACCOUNT_ID", "test-account")
    os.environ.setdefault("R2_ACCESS_KEY_ID", "test-key")
    os.environ.setdefault("R2_SECRET_ACCESS_KEY", "test-secret-key")
    os.environ.setdefault("R2_PUBLIC_URL", "https://example.com")

    code_dir = str(pathlib.Path(__file__).parent)
    inserted = code_dir not in sys.path
    if inserted:
        sys.path.insert(0, code_dir)
    try:
        spec = importlib.util.spec_from_file_location(
            "main_for_jobresponse",
            pathlib.Path(__file__).parent / "main.py",
        )
        main = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(main)
        fields = set(main.JobResponse.model_fields.keys())
    except Exception as exc:
        # If main.py is too heavy to load (FastAPI side effects, missing env),
        # fall back to importing JobResponse from job_response.py.
        import importlib as _il
        _jr_spec = _il.util.spec_from_file_location(
            "job_response",
            pathlib.Path(__file__).parent / "job_response.py",
        )
        _jr_mod = _il.util.module_from_spec(_jr_spec)
        _jr_spec.loader.exec_module(_jr_mod)
        fields = set(_jr_mod.JobResponse.model_fields.keys())
    finally:
        if inserted and code_dir in sys.path:
            sys.path.remove(code_dir)

    expected_new = {
        "lifecycle_stage",
        "approval_at",
        "export_at",
        "finishing_at",
        "published_at",
        "notes",
        "archived",
        "stuck_days",
    }
    missing = expected_new - fields
    assert not missing, f"JobResponse missing fields: {missing}"
