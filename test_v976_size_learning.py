"""Byte sizes must be learned from a HEALTHY response, not only a failing one.

v963.35 made shared-project delivery work by matching downloaded files to a clip
by BYTE SIZE, so `media_sizes_for_clip()` is what lets the guard
`if not _want and not _isolated: continue` pass. If no size is ever learned that
guard skips every clip in a shared project: renders finish and nothing is
delivered, and it reads as "still generating" because the platform status is the
worker's opinion (v974.5).

Measured 2026-09-12: `[v963.35] learned ...` appeared ZERO times across four real
runs, because the learner sat below `data = resp.json()` (batchexecute is not
json) and below `if not _FAIL_MARKER_RE.search(blob): return` (a healthy response
returns there). Both had to be passed to reach it.
"""
import importlib.util
import json
import pathlib
import sys

_STATIC = pathlib.Path(__file__).parent / "static"
_SPEC = importlib.util.spec_from_file_location(
    "flow_worker_sizes", _STATIC / "flow_worker.py",
)

MEDIA = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
PROJECT = "11111111-2222-3333-4444-555555555555"
OP = "99999999-8888-7777-6666-555555555555"
SIZE = 3_491_899


def _load():
    if str(_STATIC) not in sys.path:
        sys.path.insert(0, str(_STATIC))
    mod = importlib.util.module_from_spec(_SPEC)
    _SPEC.loader.exec_module(mod)
    return mod


def _healthy_status_body():
    """A SUCCEEDED status poll: [op, project, media, ..., [..., size]].

    No failure marker anywhere -- that is the point.
    """
    record = [OP, PROJECT, MEDIA, None, None, [None, None, SIZE]]
    payload = [None, None, [record]]
    frame = json.dumps([["wrb.fr", "UpteDb", json.dumps(payload),
                         None, None, None, "generic"]])
    return ")]}'\n\n" + str(len(frame) + 1) + "\n" + frame + "\n"


class _Resp:
    def __init__(self, body):
        self.status = 200
        self._body = body

    def text(self):
        return self._body

    def json(self):
        raise ValueError("batchexecute is not json")


def test_a_healthy_status_poll_teaches_the_size():
    fw = _load()
    fw._V963_MEDIA_SIZES.clear()
    url = ("https://flow.google.com/_/AiSandboxAngularFrontend/data/batchexecute"
           "?rpcids=UpteDb")
    fw._scan_failure_reason(_Resp(_healthy_status_body()), url, "acct:TEST")
    assert fw._V963_MEDIA_SIZES.get(MEDIA) == SIZE, (
        "a SUCCEEDED poll must teach uuid -> size; without it v963.35's "
        "shared-project delivery has nothing to match and skips the clip")


def test_the_scan_survives_a_body_that_is_not_json():
    """resp.json() raising must not stop the size learning."""
    fw = _load()
    fw._V963_MEDIA_SIZES.clear()
    fw._scan_failure_reason(_Resp(_healthy_status_body()), "x/batchexecute", "")
    assert fw._V963_MEDIA_SIZES, "the learner must not sit behind resp.json()"
