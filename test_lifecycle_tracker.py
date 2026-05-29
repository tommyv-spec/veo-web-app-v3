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
