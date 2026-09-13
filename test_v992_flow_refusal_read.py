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


# ---------------------------------------------------------------- Task 4
REFUSED = [4, [3, "PUBLIC_ERROR_UNSAFE_GENERATION"], ["IDENTIFIABLE_PERSON_SAFETY"]]


def _record(media, status, op=OP):
    """A flow.google.com generation record in the MEASURED jwpduf shape:
    [operation, project, MEDIA, "CAE", null, [[ts], "<prompt>"], <ingredient uuid>, <status>].
    The ingredient uuid sits nearer the error than the media uuid on purpose."""
    prompt = "Setting: a sunlit kitchen. Healer: click the link below and get the whole set free."
    return [op, PROJECT, media, "CAE", None, [[1789048452, 429215000], prompt], INGREDIENT, status]


def test_positional_reader_takes_the_record_media_not_the_nearest_uuid():
    fw = _load()
    payload = [None, [_record(MEDIA, REFUSED), _record(MEDIA2, 1, op="88888888-7777-6666-5555-444444444444")]]
    assert fw._v992_failed_media_ids_positional(payload, "PUBLIC_ERROR_UNSAFE_GENERATION") == {MEDIA}
    assert fw._v992_failed_media_ids_positional(payload, "PUBLIC_ERROR_NOPE") == set()


def test_smallest_subtree_fallback_for_an_unseen_shape():
    fw = _load()
    a, b = "aaaaaaaa-1111-2222-3333-444444444444", "bbbbbbbb-1111-2222-3333-444444444444"
    payload = [["x", ["PUBLIC_ERROR_FOO", a]], ["y", b], ["PUBLIC_ERROR_FOO with " + a + " inline"]]
    assert fw._v992_failed_media_ids_smallest(payload, "PUBLIC_ERROR_FOO") == {a}


def _bind(fw):
    fw._VIDEO_POLICY_TERMINAL.clear(); fw._V992_REFUSALS.clear(); fw._POLICY_SOFT_SEEN.clear()
    fw._PRIMARY_MEDIA_BINDINGS.clear()
    fw._PRIMARY_MEDIA_BINDINGS[MEDIA] = {'job_id': 'J', 'clip_index': 27, 'clip_id': 14934}
    fw._PRIMARY_MEDIA_BINDINGS[MEDIA2] = {'job_id': 'J', 'clip_index': 26, 'clip_id': 14933}


def test_a_text_refusal_lands_in_the_prompt_ledger_for_the_clip_whose_media_was_refused():
    fw = _load()
    _bind(fw)
    body = _wire([("jwpduf", [None, [_record(MEDIA, REFUSED), _record(MEDIA2, 1)]])])
    fw._scan_failure_reason(_Resp(BX_URL, body), BX_URL, "acct:T")
    assert fw._v992_peek_refusal_for_clip('J', 27) == "UNSAFE_GENERATION", (
        "the refusal must be readable for the clip whose media was refused")
    assert fw._v992_peek_refusal_for_clip('J', 26) is None, (
        "the healthy sibling in the same poll must not be flagged")
    assert INGREDIENT not in fw._V992_REFUSALS, "an attached image is never the refused media"
    assert fw._v992_peek_refusal_for_clip('J', 27) == "UNSAFE_GENERATION", "peek is read-only"
    assert fw._v992_consume_refusal_for_clip('J', 27) == "UNSAFE_GENERATION"
    assert fw._v992_peek_refusal_for_clip('J', 27) is None, "consume clears it"


def test_an_unknown_public_error_never_acquires_frame_swap_semantics():
    """Codex HIGH 1. Recording a text refusal must not touch the FRAME ledger,
    the frame-swap budget, or policy_reason_is_terminal. The last assert
    documents WHY the ledgers are split: that function's docstring says unknown
    reasons are soft; its code says they are terminal."""
    fw = _load()
    _bind(fw)
    body = _wire([("jwpduf", [None, [_record(MEDIA, REFUSED)]])])
    fw._scan_failure_reason(_Resp(BX_URL, body), BX_URL, "acct:T")
    assert fw._VIDEO_POLICY_TERMINAL == {}, "UNSAFE_GENERATION is a text refusal; the frame ledger stays empty"
    assert fw._peek_video_policy_terminal_for_clip('J', 27) is None
    assert fw._POLICY_SOFT_SEEN == {}, "recording must not spend the frame-swap soft budget"
    assert fw.policy_reason_is_terminal('UNSAFE_GENERATION') is True, (
        "the CODE makes unknown reasons terminal (its docstring says soft) -- "
        "which is exactly why the prompt axis must not route through it")


def test_a_known_frame_reason_still_lands_in_the_frame_ledger():
    fw = _load()
    _bind(fw)
    prominent = [4, [3, "PUBLIC_ERROR_PROMINENT_PEOPLE_FILTER_FAILED"], ["IDENTIFIABLE_PERSON_SAFETY"]]
    body = _wire([("jwpduf", [None, [_record(MEDIA, prominent)]])])
    fw._scan_failure_reason(_Resp(BX_URL, body), BX_URL, "acct:T")
    assert fw._peek_video_policy_terminal_for_clip('J', 27) == "PROMINENT_PEOPLE_FILTER_FAILED", (
        "a face/identity reason keeps the existing frame-axis contract")
    assert fw._V992_REFUSALS == {}, "a frame reason is not also a prompt refusal"


def test_a_healthy_poll_records_nothing():
    fw = _load()
    _bind(fw)
    body = _wire([("jwpduf", [None, [_record(MEDIA, 1)]])])
    fw._scan_failure_reason(_Resp(BX_URL, body), BX_URL, "acct:T")
    assert fw._VIDEO_POLICY_TERMINAL == {} and fw._V992_REFUSALS == {}


# ---------------------------------------------------------------- Task 5
import ast


def _fresh_ladder(fw):
    fw._V992_NOT_FOUND.clear(); fw._CLIP_PROMPT_B.clear(); fw._PROMPT_B_TRIED.clear()
    fw._V992_REFUSALS.clear(); fw._VIDEO_POLICY_TERMINAL.clear(); fw._PRIMARY_MEDIA_BINDINGS.clear()
    fw._PRIMARY_MEDIA_BINDINGS[MEDIA] = {'job_id': 'J', 'clip_index': 27, 'clip_id': 14934}


def test_fail_clip_general_policy_returns_a_verified_boolean(monkeypatch):
    fw = _load()
    monkeypatch.setattr(fw, "api_request_ex", lambda m, e, d=None: ({"ok": True}, 200))
    assert fw.fail_clip_general_policy(14934, "m") is True
    monkeypatch.setattr(fw, "api_request_ex", lambda m, e, d=None: (None, 500))   # HTTP error, no exception
    monkeypatch.setattr(fw, "update_clip_status", lambda *a, **k: {"status": "failed"})
    assert fw.fail_clip_general_policy(14934, "m") is True, "fallback write succeeded"
    monkeypatch.setattr(fw, "update_clip_status", lambda *a, **k: None)
    assert fw.fail_clip_general_policy(14934, "m") is False, "HTTP 500 and a failed fallback = NOT failed"
    def _boom(*a, **k):
        raise RuntimeError("network")
    monkeypatch.setattr(fw, "api_request_ex", _boom)
    assert fw.fail_clip_general_policy(14934, "m") is False


def test_fail_clip_general_policy_keeps_prompt_b_tried_when_both_writes_fail(monkeypatch):
    """Codex HIGH 4. The cleanup pops (attempt count, swap mark, Prompt-B tried)
    used to run whether or not a write landed. When the ladder fails a clip
    BECAUSE Prompt B was already tried, a failed write must not make B look
    untried again, or the next pass queues it a second time."""
    fw = _load()
    fw._PROMPT_B_TRIED.clear(); fw._POLICY_SWAP_DONE.clear()
    with fw._POLICY_GEN_LOCK:
        fw._POLICY_GEN_ATTEMPTS.clear()
    fw._PROMPT_B_TRIED[14934] = True
    fw._POLICY_SWAP_DONE[14934] = "veo-3-fast"
    with fw._POLICY_GEN_LOCK:
        fw._POLICY_GEN_ATTEMPTS[14934] = 2
    monkeypatch.setattr(fw, "api_request_ex", lambda m, e, d=None: (None, 503))
    monkeypatch.setattr(fw, "update_clip_status", lambda *a, **k: None)
    assert fw.fail_clip_general_policy(14934, "m") is False
    assert fw._PROMPT_B_TRIED.get(14934) is True, "a failed terminal write must leave Prompt B marked tried"
    assert fw._POLICY_SWAP_DONE.get(14934) == "veo-3-fast" and fw._POLICY_GEN_ATTEMPTS.get(14934) == 2
    monkeypatch.setattr(fw, "api_request_ex", lambda m, e, d=None: ({"ok": True}, 200))
    assert fw.fail_clip_general_policy(14934, "m") is True
    assert 14934 not in fw._PROMPT_B_TRIED and 14934 not in fw._POLICY_SWAP_DONE and 14934 not in fw._POLICY_GEN_ATTEMPTS, (
        "on a CONFIRMED failure the cleanup runs as before (fresh budget for a later user Retry)")


def test_not_found_route_is_a_pure_decision_over_the_miss_number():
    fw = _load()
    _fresh_ladder(fw)
    fw.register_clip_prompt_b({'id': 14934, 'prompt_b': 'tap the link below and get the whole set free.'})
    assert fw._v992_not_found_route(14934, 1)[0] == 'redo' and 14934 not in fw._PROMPT_B_TRIED
    route, msg = fw._v992_not_found_route(14934, 2)
    assert route == 'prompt_b' and 'retry reworded line (prompt b)' in msg.lower(), (
        "second miss with the same text must switch to Prompt B, carrying the v849 marker")
    assert fw._PROMPT_B_TRIED.get(14934) is True
    assert fw._v992_not_found_route(14934, 3)[0] == 'fail'
    assert fw._V992_NOT_FOUND == {}, "the route never counts; the caller does, after a confirmed write"
    fw._PROMPT_B_TRIED.clear()
    route, msg = fw._v992_not_found_route(14934, 1, reason='UNSAFE_GENERATION')
    assert route == 'prompt_b' and 'UNSAFE_GENERATION' in msg, "a refusal we READ goes straight to Prompt B"
    _fresh_ladder(fw)
    assert fw._v992_not_found_route(777, 1)[0] == 'redo'
    route, msg = fw._v992_not_found_route(777, 2)
    assert route == 'fail' and 'no reworded line' in msg.lower()


def test_give_up_counts_and_consumes_only_after_a_confirmed_write(monkeypatch):
    fw = _load()
    _fresh_ladder(fw)
    fw.register_clip_prompt_b({'id': 14934, 'prompt_b': 'tap the link below.'})
    fw._v992_note_refusal(MEDIA, 'UNSAFE_GENERATION')
    writes = []
    monkeypatch.setattr(fw, "update_clip_status", lambda cid, st, **k: writes.append((cid, st, k.get('error_message'))) or {"ok": 1})
    assert fw._v992_give_up_on_clip('J', 27, 14934) is True
    assert writes == [(14934, 'flow_redo_queued', writes[0][2])] and 'retry reworded line (prompt b)' in writes[0][2].lower()
    assert fw._V992_NOT_FOUND[14934] == 1, "counted once, after the write"
    assert fw._v992_peek_refusal_for_clip('J', 27) is None, "consumed after the write"
    assert fw._PROMPT_B_TRIED.get(14934) is True


def test_give_up_keeps_everything_when_the_redo_write_fails(monkeypatch):
    """Codex HIGH 2: a failed status write must not consume the refusal,
    advance the counter, or leave Prompt B marked tried."""
    fw = _load()
    _fresh_ladder(fw)
    fw.register_clip_prompt_b({'id': 14934, 'prompt_b': 'tap the link below.'})
    fw._v992_note_refusal(MEDIA, 'UNSAFE_GENERATION')
    monkeypatch.setattr(fw, "update_clip_status", lambda *a, **k: None)      # retries exhausted
    assert fw._v992_give_up_on_clip('J', 27, 14934) is False
    assert fw._v992_peek_refusal_for_clip('J', 27) == 'UNSAFE_GENERATION', "refusal still there"
    assert 14934 not in fw._V992_NOT_FOUND, "miss not counted"
    assert 14934 not in fw._PROMPT_B_TRIED, "Prompt B still untried"


def test_give_up_keeps_everything_when_the_failure_write_fails(monkeypatch):
    """Codex HIGH 3: FAILED is only printed and counted when the clip was really marked failed."""
    fw = _load()
    _fresh_ladder(fw)
    fw._V992_NOT_FOUND[777] = 2                       # third miss, no Prompt B -> 'fail'
    fw._PRIMARY_MEDIA_BINDINGS[MEDIA2] = {'job_id': 'J', 'clip_index': 5, 'clip_id': 777}
    fw._v992_note_refusal(MEDIA2, 'UNSAFE_GENERATION')
    calls = []
    monkeypatch.setattr(fw, "fail_clip_general_policy", lambda cid, msg: calls.append(cid) or False)
    assert fw._v992_give_up_on_clip('J', 5, 777) is False
    assert calls == [777]
    assert fw._V992_NOT_FOUND[777] == 2, "not advanced"
    assert fw._v992_peek_refusal_for_clip('J', 5) == 'UNSAFE_GENERATION', "not consumed"
    monkeypatch.setattr(fw, "fail_clip_general_policy", lambda cid, msg: True)
    assert fw._v992_give_up_on_clip('J', 5, 777) is True
    assert fw._V992_NOT_FOUND[777] == 3 and fw._v992_peek_refusal_for_clip('J', 5) is None


def test_give_up_hands_a_frame_reason_to_the_existing_handler_and_does_not_consume(monkeypatch):
    """Decision (a) for Codex HIGH 5: the handler's write is unverified, so the
    wrapper returns False, leaves the frame ledger alone, and never counts."""
    fw = _load()
    _fresh_ladder(fw)
    fw._record_video_policy_terminal(MEDIA, 'PROMINENT_PEOPLE')
    seen = []
    monkeypatch.setattr(fw, "handle_terminal_reject", lambda cid, reason, **k: seen.append((cid, reason)) or 'requeued')
    assert fw._v992_give_up_on_clip('J', 27, 14934) is False, "an unverified write is not a state change"
    assert seen == [(14934, 'PROMINENT_PEOPLE')]
    assert fw._peek_video_policy_terminal_for_clip('J', 27) == 'PROMINENT_PEOPLE', "deliberately NOT consumed"
    assert fw._V992_NOT_FOUND == {}, "a frame reason never walks the text ladder"


def test_give_up_keeps_a_frame_refusal_when_the_handlers_requeue_write_returns_none(monkeypatch):
    """Codex HIGH 5, the real handler: PROMINENT with an untried Prompt B makes
    handle_terminal_reject call update_clip_status and return 'requeued' without
    reading the result. With that write answering None the evidence must survive."""
    fw = _load()
    _fresh_ladder(fw)
    fw.register_clip_prompt_b({'id': 14934, 'prompt_b': 'tap the link below.'})
    fw._record_video_policy_terminal(MEDIA, 'PROMINENT_PEOPLE')
    monkeypatch.setattr(fw, "update_clip_status", lambda *a, **k: None)
    assert fw._v992_give_up_on_clip('J', 27, 14934) is False
    assert fw._peek_video_policy_terminal_for_clip('J', 27) == 'PROMINENT_PEOPLE', "phantom requeue: refusal kept"
    assert fw._V992_NOT_FOUND == {}
    # Pre-existing and documented as residual risk (§Task 5): the handler marks
    # Prompt B tried before it knows whether the requeue landed.
    assert fw._PROMPT_B_TRIED.get(14934) is True


def test_give_up_keeps_a_frame_refusal_when_the_handlers_terminal_write_fails(monkeypatch):
    """Codex HIGH 5, terminal branch: CSAM -> route_terminal_content_reject ->
    report_policy_violation raises -> fail_clip_general_policy answers False ->
    the handler still returns 'terminal'. The wrapper must not consume."""
    fw = _load()
    _fresh_ladder(fw)
    fw._record_video_policy_terminal(MEDIA, 'CSAM')
    def _down(*a, **k):
        raise RuntimeError("api down")
    monkeypatch.setattr(fw, "report_policy_violation", _down)
    monkeypatch.setattr(fw, "fail_clip_general_policy", lambda cid, msg: False)
    assert fw._v992_give_up_on_clip('J', 27, 14934) is False
    assert fw._peek_video_policy_terminal_for_clip('J', 27) == 'CSAM', "phantom terminal: refusal kept"


def test_the_give_up_branch_routes_through_the_ladder():
    """AST, not substring: the post-job give-up branch must call the give-up
    function and must no longer write flow_redo_queued with the old fixed message."""
    tree = ast.parse(_PATH.read_text(encoding="utf-8"))
    fn = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "process_job_submission")
    calls = [n.func.id for n in ast.walk(fn) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)]
    assert "_v992_give_up_on_clip" in calls, "the give-up branch does not consult the ladder"
    old = [n for n in ast.walk(fn) if isinstance(n, ast.Constant)
           and n.value == "Clip not found in project after generation — resubmitting via redo"]
    assert not old, "the fixed give-up message still exists: the branch still resubmits identical text blindly"
