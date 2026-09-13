"""v992 — the worker must be able to READ Flow's verdict on a dropped clip.

Clip 14934 (job 0d456c24, clip_index 27) failed identically three times:
submitted, media ids bound, status 'generating', render never in the listing,
then 'Post-job: clip 28 not found after 300s' and an identical resubmit.
The worker could not tell a refusal from 'still rendering' because:
  * HEAD calls _v963_batchexecute_frames without defining it (NameError, swallowed)
  * the listener gate never matched a flow.google.com URL, so the scan never ran
  * the scan then did resp.json() on a body that is never JSON
  * nothing ever wrote _VIDEO_POLICY_TERMINAL on this host
Each section below holds one of those down.
"""
import importlib.util
import json
import os
import pathlib
import sys

_STATIC = pathlib.Path(__file__).parent / "static"
_PATH = pathlib.Path(os.environ.get("FLOW_WORKER_PATH") or (_STATIC / "flow_worker.py"))

OP = "99999999-8888-7777-6666-555555555555"
PROJECT = "11111111-2222-3333-4444-555555555555"
MEDIA = "289a2412-bd34-4ed1-bff1-7ab4bc40767b"      # clip 14934's real bound media id
MEDIA2 = "4d3e6dbd-b307-44e1-8649-928d25b65b8a"
INGREDIENT = "cce4ff1c-a0c0-481b-8221-dae118a788be"  # an attached face image, NOT the render


def _load():
    if str(_STATIC) not in sys.path:
        sys.path.insert(0, str(_STATIC))
    spec = importlib.util.spec_from_file_location("flow_worker_v992", _PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _wire(frames):
    """Google's batchexecute wire format: `)]}'` then length-prefixed chunks,
    each a JSON array of ["wrb.fr", <rpcid>, "<payload as a JSON STRING>", ...].
    `frames` is a list of (rpcid, payload)."""
    out = ")]}'\n\n"
    for rpcid, payload in frames:
        frame = json.dumps([["wrb.fr", rpcid, json.dumps(payload), None, None, None, "generic"]])
        out += str(len(frame) + 1) + "\n" + frame + "\n"
    return out


# ---------------------------------------------------------------- Task 1
def test_the_shared_decoder_returns_every_frame_with_its_rpcid():
    fw = _load()
    body = _wire([("UpteDb", [1, 2]), ("jwpduf", {"a": 1})])
    assert fw._v963_batchexecute_frames(body) == [("UpteDb", [1, 2]), ("jwpduf", {"a": 1})]
    assert fw._v963_batchexecute_payloads(body, "jwpduf") == [{"a": 1}]
    assert fw._v963_batchexecute_frames("not batchexecute at all") == []


# ---------------------------------------------------------------- Task 2
class _Req:
    def __init__(self, post_data=""):
        self.post_data = post_data


class _Frame:
    page = None


class _Resp:
    """A Playwright Response stand-in: status, text(), json() (never JSON on
    this host), request.post_data, url. frame.page is None like a test stub."""
    def __init__(self, url, body, status=200, req_body=""):
        self.url = url
        self.status = status
        self._body = body
        self.request = _Req(req_body)
        self.frame = _Frame()

    def text(self):
        return self._body

    def json(self):
        raise ValueError("batchexecute is not json")


class _Page:
    """Collects handlers passed to page.on(event, fn)."""
    def __init__(self):
        self.handlers = {}

    def on(self, event, fn):
        self.handlers.setdefault(event, []).append(fn)


BX_URL = ("https://flow.google.com/_/AiSandboxAngularFrontend/data/batchexecute"
          "?rpcids=jwpduf&source-path=%2Fproject%2F" + PROJECT + "&rt=c")


def test_the_response_listener_reaches_the_scan_for_a_batchexecute_url():
    fw = _load()
    seen = []
    fw._scan_failure_reason = lambda resp, url, buf_key='': seen.append((url, buf_key))
    page = _Page()
    fw._install_submit_response_listener(page, "TESTACCT")
    handler = page.handlers["response"][0]
    handler(_Resp(BX_URL, _wire([("jwpduf", [None, []])])))
    assert seen == [(BX_URL, "acct:TESTACCT")], (
        "a flow.google.com status poll never reached the scan: the gate only "
        "matched the old host's endpoint names")
    seen.clear()
    handler(_Resp("https://flow.google.com/asb/AB-nOUxyz=mm,22,15", "video bytes"))
    assert seen == [], "a media fetch is not a batchexecute response; the scan must not read it"


# ---------------------------------------------------------------- Task 3
def _dump_env(monkeypatch, tmp_path, on=True):
    path = tmp_path / "flow_bx_dump.jsonl"
    if on:
        monkeypatch.setenv("FLOW_BX_DUMP", "1")
    else:
        monkeypatch.delenv("FLOW_BX_DUMP", raising=False)
    monkeypatch.setenv("FLOW_BX_DUMP_PATH", str(path))
    return path


def test_dump_off_by_default_writes_nothing(monkeypatch, tmp_path):
    fw = _load()
    path = _dump_env(monkeypatch, tmp_path, on=False)
    fw._scan_failure_reason(_Resp(BX_URL, _wire([("jwpduf", [None, []])])), BX_URL, "acct:T")
    assert not path.exists(), "FLOW_BX_DUMP unset must leave no file behind"


def test_dump_writes_one_raw_record_per_batchexecute_response(monkeypatch, tmp_path):
    fw = _load()
    path = _dump_env(monkeypatch, tmp_path)
    body = _wire([("jwpduf", [None, [[OP, PROJECT, MEDIA, "CAE"]]])])
    fw._scan_failure_reason(_Resp(BX_URL, body, req_body="f.req=%5B%5D"), BX_URL, "acct:T")
    fw._scan_failure_reason(_Resp("https://flow.google.com/asb/x", "video"), "https://flow.google.com/asb/x", "acct:T")
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1, "only batchexecute responses are dumped"
    rec = json.loads(lines[0])
    assert rec["rpcids"] == "jwpduf"
    assert rec["status"] == 200
    assert rec["buf_key"] == "acct:T"
    assert rec["url"].endswith("/data/batchexecute")
    assert rec["body"] == body and rec["req_body"] == "f.req=%5B%5D"
    assert rec["truncated"] is False
    assert MEDIA in rec["body"], "a human must be able to grep the dump by the clip's bound uuid"


def test_dump_caps_the_body_and_stops_at_the_file_cap(monkeypatch, tmp_path):
    fw = _load()
    path = _dump_env(monkeypatch, tmp_path)
    monkeypatch.setattr(fw, "_V992_DUMP_BODY_CAP", 20)
    fw._V992_DUMP_STATE.update(off=False, announced=False)
    long_body = _wire([("jwpduf", ["x" * 100])])
    fw._scan_failure_reason(_Resp(BX_URL, long_body), BX_URL, "acct:T")
    rec = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
    assert len(rec["body"]) == 20 and rec["truncated"] is True
    monkeypatch.setattr(fw, "_V992_DUMP_FILE_CAP", 1)      # the file is already past 1 byte
    fw._scan_failure_reason(_Resp(BX_URL, long_body), BX_URL, "acct:T")
    assert len(path.read_text(encoding="utf-8").splitlines()) == 1, "past the file cap nothing more is written"
    assert fw._V992_DUMP_STATE["off"] is True
