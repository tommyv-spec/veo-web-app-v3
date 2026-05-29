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
