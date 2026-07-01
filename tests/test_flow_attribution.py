# code/tests/test_flow_attribution.py
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "static"))
from flow_attribution import RenderAttributor


def test_stamp_click_records_ordered_entries_per_account():
    a = RenderAttributor()
    a.stamp_click("Account1", job_id="J", clip_index=0, clip_id="c0", now=100.0)
    a.stamp_click("Account1", job_id="J", clip_index=1, clip_id="c1", now=160.0)
    a.stamp_click("Account2", job_id="J", clip_index=0, clip_id="d0", now=110.0)
    log1 = a.click_log_for("Account1")
    assert [e["clip_index"] for e in log1] == [0, 1]
    assert [e["click_at"] for e in log1] == [100.0, 160.0]
    assert a.click_log_for("Account2")[0]["clip_id"] == "d0"
    assert a.click_log_for("Account1") is not a._click_log["Account1"]  # returns a copy


def test_bracket_for_returns_owning_click_entry():
    a = RenderAttributor()
    a.stamp_click("A", "J", 0, "c0", now=100.0)
    a.stamp_click("A", "J", 1, "c1", now=160.0)
    a.stamp_click("A", "J", 2, "c2", now=220.0)
    # inside clip 0's bracket [100,160)
    assert a.bracket_for("A", 130.0)["clip_index"] == 0
    # exactly on a boundary belongs to the later bracket (>= start)
    assert a.bracket_for("A", 160.0)["clip_index"] == 1
    # after the last click -> open-ended last bracket
    assert a.bracket_for("A", 999.0)["clip_index"] == 2
    # before the first click -> None (no owner)
    assert a.bracket_for("A", 50.0) is None
    # unknown account -> None
    assert a.bracket_for("ZZ", 130.0) is None


def test_observe_render_attributes_by_captured_at_bracket():
    a = RenderAttributor()
    a.stamp_click("A", "J", 0, "c0", now=100.0)
    a.stamp_click("A", "J", 1, "c1", now=160.0)
    # render captured at 135 -> clip 0's bracket
    b = a.observe_render("RID1", account="A", captured_at=135.0)
    assert b == {"job_id": "J", "clip_index": 0, "clip_id": "c0"}
    # a second variant of the same clip, captured later but still < next click
    b2 = a.observe_render("RID2", account="A", captured_at=155.0)
    assert b2["clip_index"] == 0
    # render for clip 1
    b3 = a.observe_render("RID3", account="A", captured_at=170.0)
    assert b3["clip_index"] == 1
    # ledger recorded all three with account + status default
    assert set(a.renders_for_clip("J", 0)) == {"rid1", "rid2"}
    assert a.renders_for_clip("J", 1) == ["rid3"]


def test_observe_render_uses_create_time_when_no_captured_at():
    a = RenderAttributor()
    a.stamp_click("A", "J", 0, "c0", now=100.0)
    a.stamp_click("A", "J", 1, "c1", now=160.0)
    # status-poll-only render: no captured_at, fall back to create_time epoch
    b = a.observe_render("RID9", account="A", create_time=150.0)
    assert b["clip_index"] == 0


def test_observe_render_returns_none_when_disabled_or_unbracketed():
    a = RenderAttributor(enabled=False)
    a.stamp_click("A", "J", 0, "c0", now=100.0)
    assert a.observe_render("RID", account="A", captured_at=130.0) is None  # disabled
    a2 = RenderAttributor()
    a2.stamp_click("A", "J", 0, "c0", now=100.0)
    assert a2.observe_render("RID", account="A", captured_at=50.0) is None  # pre-first-click
    # but the ledger still recorded it (for reconcile/backstop), even if unbound
    assert "rid" in a2._ledger


def test_reconcile_reports_per_clip_status():
    a = RenderAttributor()
    a.stamp_click("A", "J", 0, "c0", now=100.0)
    a.stamp_click("A", "J", 1, "c1", now=160.0)
    a.observe_render("RID0", account="A", captured_at=130.0, status="MEDIA_GENERATION_STATUS_SCHEDULED")
    a.observe_render("RID1", account="A", captured_at=170.0, status="MEDIA_GENERATION_STATUS_SCHEDULED")
    # later status poll flips clip 0 -> SUCCESSFUL, clip 1 -> FAILED
    a.observe_render("RID0", account="A", status="MEDIA_GENERATION_STATUS_SUCCESSFUL")
    a.observe_render("RID1", account="A", status="MEDIA_GENERATION_STATUS_FAILED")
    rec = a.reconcile("J", [0, 1])
    assert rec[0]["state"] == "successful" and rec[0]["render_ids"] == ["rid0"]
    assert rec[1]["state"] == "failed"


def test_reconcile_flags_clip_with_no_render():
    a = RenderAttributor()
    a.stamp_click("A", "J", 0, "c0", now=100.0)
    rec = a.reconcile("J", [0, 1])
    assert rec[1]["state"] == "missing" and rec[1]["render_ids"] == []


def test_redo_click_opens_new_bracket_and_old_binding_is_dropped():
    a = RenderAttributor()
    a.stamp_click("A", "J", 0, "c0", now=100.0)
    a.stamp_click("A", "J", 1, "c1", now=160.0)
    a.observe_render("OLD", account="A", captured_at=130.0)      # clip 0, first attempt
    assert a.renders_for_clip("J", 0) == ["old"]
    # redo of clip 0 much later: purge its prior bindings, stamp a new click
    a.purge_clip("J", 0)
    a.stamp_click("A", "J", 0, "c0", now=300.0)
    assert a.renders_for_clip("J", 0) == []                       # old dropped
    a.observe_render("NEW", account="A", captured_at=305.0)       # redo render
    assert a.renders_for_clip("J", 0) == ["new"]
