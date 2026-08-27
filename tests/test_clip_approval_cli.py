"""v949 — approve-clip / reject-clip subcommands of send_to_platform.py.

The CLI calls the same /api/clips/{id}/approve|reject endpoints the review UI
clicks, under the operator's bearer token. These tests cover the pure id-list
parser and the command function against a fake client — no network.
"""
import importlib.util
import os

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_STP = os.path.join(os.path.dirname(_HERE), "send_to_platform.py")

spec = importlib.util.spec_from_file_location("stp_clip_approval_test", _STP)
stp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stp)


class FakeClient:
    def __init__(self):
        self.posts = []

    def post(self, path, payload=None, **kw):
        self.posts.append(path)
        return {"ok": True}


class Args:
    token_value = None
    as_json = False


# ---- parse_clip_id_list ---------------------------------------------------
def test_single_id():
    assert stp.parse_clip_id_list("14661") == [14661]


def test_comma_list_with_spaces():
    assert stp.parse_clip_id_list("14661, 14662,14663") == [14661, 14662, 14663]


def test_job_uuid_is_rejected_with_a_pointer():
    """A job id here is the "d" DataError family (4616d16) — must fail client-side."""
    with pytest.raises(stp.PlatformError):
        stp.parse_clip_id_list("63097756-05d7-40d1-bd11-88f085e49bdb")


def test_empty_is_rejected():
    with pytest.raises(stp.PlatformError):
        stp.parse_clip_id_list(",")


# ---- cmd_clip_approval ----------------------------------------------------
def test_approve_posts_to_approve_endpoint():
    client, args, report = FakeClient(), Args(), {"stages": []}
    args.token_value = "14661"
    rc = stp.cmd_clip_approval(client, args, report, "approve")
    assert rc == stp.EXIT_OK
    assert client.posts == ["/api/clips/14661/approve"]
    assert report["stages"] == ["clips:approved:1"]


def test_reject_posts_each_id():
    client, args, report = FakeClient(), Args(), {"stages": []}
    args.token_value = "1,2"
    rc = stp.cmd_clip_approval(client, args, report, "reject")
    assert rc == stp.EXIT_OK
    assert client.posts == ["/api/clips/1/reject", "/api/clips/2/reject"]
    assert report["clips_rejected"][0]["clip_id"] == 1


def test_missing_ids_is_an_error():
    client, args, report = FakeClient(), Args(), {"stages": []}
    with pytest.raises(stp.PlatformError):
        stp.cmd_clip_approval(client, args, report, "approve")
    assert client.posts == []


def test_dispatch_wired_in_main():
    src = open(_STP, encoding="utf-8").read()
    assert 'args.md_file in ("approve-clip", "reject-clip")' in src
    assert "OPERATOR-GATED" in src
