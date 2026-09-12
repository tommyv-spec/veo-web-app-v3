"""The submit binder must survive Google renaming the submit rpcid.

Why this file exists. Attribution binds the media uuids out of the submit
RESPONSE to (job, clip), so a download is matched by uuid and tile position
never matters. That layer is fine. The one thing that broke it on 2026-09-12 was
the NAME it gates on: the submit rpcid moved from `MZZa6b` to `eb1hJf`, the gate
stopped matching, `_v963_batchexecute_submit_data` returned None, and the log
said "no submit response captured within 40s" on submits that had worked
perfectly. With no uuid bound there is nothing to attribute a download to, so
every clip rendered and NOT ONE was delivered -- 0/80 on a job whose renders
were all sitting finished in Flow.

That is a silent, total failure of the delivery leg caused by an eight-character
string, which is exactly the kind of thing a test should hold down.
"""
import importlib.util
import json
import pathlib
import sys

_STATIC = pathlib.Path(__file__).parent / "static"
_SPEC = importlib.util.spec_from_file_location(
    "flow_worker_rpcid", _STATIC / "flow_worker.py",
)

OUT = "11111111-2222-3333-4444-555555555555"
IN_ = "99999999-8888-7777-6666-555555555555"


def _load():
    if str(_STATIC) not in sys.path:
        sys.path.insert(0, str(_STATIC))
    mod = importlib.util.module_from_spec(_SPEC)
    _SPEC.loader.exec_module(mod)
    return mod


def _wire(rpcid, out_uuid):
    """A batchexecute response in Google's own format: `)]}'` then
    length-prefixed chunks, each a JSON array of
    ["wrb.fr", <rpcid>, "<payload as a JSON STRING>", ...]."""
    payload = [None, None, [[out_uuid, "a", "b", "c"]]]
    frame = json.dumps([["wrb.fr", rpcid, json.dumps(payload),
                         None, None, None, "generic"]])
    return ")]}'\n\n" + str(len(frame) + 1) + "\n" + frame + "\n"


class _Req:
    def __init__(self, body):
        self.post_data = body


class _Resp:
    def __init__(self, url, body, req_body="", status=200):
        self.url = url
        self.status = status
        self._body = body
        self.request = _Req(req_body)

    def text(self):
        return self._body


def _url(rpcid):
    return ("https://flow.google.com/_/AiSandboxAngularFrontend/data/batchexecute"
            f"?rpcids={rpcid}&source-path=%2Fproject%2Fabc&hl=en-US")


def test_the_current_submit_rpcid_binds():
    """eb1hJf — measured 2026-09-12 by finding the POST carrying the clip prompt."""
    fw = _load()
    assert "eb1hJf" in fw._V963_SUBMIT_RPCIDS
    resp = _Resp(_url("eb1hJf"), _wire("eb1hJf", OUT), req_body=f'["{IN_}"]')
    data = fw._v963_batchexecute_submit_data(resp, resp.url)
    assert data is not None, "the current submit rpcid must bind"
    assert OUT in json.dumps(data).lower(), (
        "the OUTPUT uuid must come back so a download can be attributed")


# NOT tested: that a PREVIOUS rpcid still binds.
#
# It does on the committed parser, which subtracts uuids by regex over the
# whole body and never looks a rpcid up. It does NOT on the refined parser
# another session currently has uncommitted here, which reads the decoded item
# boundary under ONE rpcid name. Asserting it would be a claim about which of
# the two is in the file, which is not a property of this fix. The list keeps
# old names anyway -- they cost nothing and spare a rollback -- but the
# guarantee this file makes is narrower and true: the CURRENT name binds, and
# an unknown one fails closed.


def test_an_input_uuid_is_never_bound_as_an_output():
    """The discriminator is subtraction: a uuid the REQUEST already carried is
    an input we attached, not something the submit created. Binding one would
    attribute a download to the wrong media."""
    fw = _load()
    resp = _Resp(_url("eb1hJf"), _wire("eb1hJf", IN_), req_body=f'["{IN_}"]')
    data = fw._v963_batchexecute_submit_data(resp, resp.url)
    assert IN_ not in json.dumps(data or {}).lower()


def test_an_unknown_rpcid_fails_closed():
    """A rename must produce "nothing bound", never a wrong binding -- that is
    what makes adding a new name to the list safe."""
    fw = _load()
    resp = _Resp(_url("zzZZzz"), _wire("zzZZzz", OUT), req_body=f'["{IN_}"]')
    assert fw._v963_batchexecute_submit_data(resp, resp.url) is None


def test_a_non_batchexecute_url_is_ignored():
    fw = _load()
    resp = _Resp("https://flow.google.com/project/abc", _wire("eb1hJf", OUT))
    assert fw._v963_batchexecute_submit_data(resp, resp.url) is None
