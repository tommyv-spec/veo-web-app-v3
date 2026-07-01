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
