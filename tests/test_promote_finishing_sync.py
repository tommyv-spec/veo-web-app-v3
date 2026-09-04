"""v951.1 — after promote, the new job's finishing is re-synced from the
markdown the send just read, not left on the batch's import-time snapshot.

Why: promote copies batch.finishing_spec (v944). A --resume-batch send reuses
a batch imported days earlier, so md edits made after import (the export_*
lines, an overlay change) never reached new jobs — job 8eb6b63e (2026-08-30)
was born without the export_smart_trim/export_frames_to_cut_start its md had
carried for hours, and the Export dialog fell back to another video's
localStorage settings.

These tests cover the pure sync function against a fake client — no network.
"""
import importlib.util
import os

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_STP = os.path.join(os.path.dirname(_HERE), "send_to_platform.py")

spec = importlib.util.spec_from_file_location("stp_promote_finishing_test", _STP)
stp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stp)


class FakeClient:
    def __init__(self, resp=None, err=None):
        self.posts = []
        self._resp = resp if resp is not None else {}
        self._err = err

    def post(self, path, payload=None, **kw):
        self.posts.append((path, payload))
        if self._err:
            raise self._err
        return self._resp


def test_posts_the_markdown_to_the_finishing_endpoint():
    client = FakeClient(resp={"finishing_spec": {"auto_finish": "on"}})
    report = {"stages": []}
    spec_out = stp.sync_finishing_after_promote(
        client, "job-1", "# build\n## Finishing\n- **auto_finish:** on\n",
        "videos/x.md", report)
    assert client.posts[0][0] == "/api/jobs/job-1/finishing"
    assert "## Finishing" in client.posts[0][1]["markdown"]
    assert spec_out == {"auto_finish": "on"}
    assert "finishing:synced" in report["stages"]


def test_absent_section_still_syncs_and_reports_cleared():
    """An md with no ## Finishing clears the stored spec — that is the
    update-finishing semantics and it is correct here too: the file is the
    truth, and the file declares nothing."""
    client = FakeClient(resp={"finishing_spec": None})
    report = {"stages": []}
    out = stp.sync_finishing_after_promote(
        client, "job-2", "# build with no finishing\n", "videos/y.md", report)
    assert out is None
    assert "finishing:synced" in report["stages"]


def test_endpoint_failure_fails_the_send_closed_with_recovery_command():
    """A bad ## Finishing must not leave the job silently on the stale batch
    snapshot — the send fails and the message names the exact recovery run."""
    err = stp.PlatformError(stp.EXIT_PARSE, "bad section")
    client = FakeClient(err=err)
    with pytest.raises(stp.PlatformError) as exc_info:
        stp.sync_finishing_after_promote(
            client, "job-3", "md", "videos/z.md", {"stages": []})
    msg = exc_info.value.message
    assert "update-finishing job-3 videos/z.md" in msg
    assert exc_info.value.exit_code == stp.EXIT_PARSE


def test_run_path_calls_the_sync_after_promote():
    """The main send path must actually invoke the sync between promote and
    render polling — the function existing is not the fix (v949 lesson:
    checkers that verify declarations, not decisions)."""
    src = open(_STP, encoding="utf-8").read()
    # Prefix match on purpose: v892.12 Task 5 added `job_config, job_config_source`
    # to the promote call and the exact-string form went stale while the sync
    # it guards was still in place. The call's identity is `job_id = promote(`.
    promote_at = src.index("job_id = promote(client, batch_id, report")
    render_at = src.index("rc = poll_render(client, job_id, args, report)")
    assert "sync_finishing_after_promote(client, job_id, md_text" in src[promote_at:render_at]
