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
