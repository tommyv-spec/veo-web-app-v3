"""Only an authenticated API reply may say "this session is logged in".

The worker declared itself logged in from page text, then made twenty API calls
that all came back "missing required authentication credential", logged each one as
non-blocking, and walked on into the picker (run log
~/veo-worker-b/mixed_model_pair_retry2.log, 2026-09-07). Thirteen of those replies
carried Google's explicit denial text.

The defect is not that the DOM check existed. It is that proof only ever moved in
the positive direction: the DOM could switch "logged in" ON, and no number of
denials could switch it OFF. These tests pin the rule that replaces it:

    A credits 200 is the only thing that may confirm.
    A denial is the only thing that may refuse.
    Transport noise (status 0) may do neither.
    The verdict is decided ONCE, after the replay - never per call.

That last line is load-bearing. `credits` is call 5 of 19; fourteen calls follow it.
A per-call implementation lets a later sibling denial erase a valid confirmation and
declare a healthy session dead - which is the same class of bug, pointing the other
way. `test_credits_200_survives_denials_that_come_AFTER_it` is the one that catches it.

Full analysis: docs/flow-worker-root-cause-2026-09-07.md
Repair plan:   docs/flow-worker-repair-plan-2026-09-07.md (Steps 1-2)
"""

import ast
from functools import lru_cache
import time as _real_time
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "static" / "flow_worker.py"
SOURCE = WORKER.read_text(encoding="utf-8")

CREDITS_MARKER = "/v1/credits"
GOOGLE_DENIAL = ("Request is missing required authentication credential. Expected "
                 "OAuth 2 access token, login cookie or other valid authentication "
                 "credential.")
GOOGLE_APIKEY_DENIAL = ("API keys are not supported by this API. Expected OAuth2 "
                        "access token or other authentication credentials.")


@lru_cache(maxsize=1)
def _tree():
    """Parse the 1.6 MB worker ONCE per session.

    Parsing per call cost 146 s across these tests alone - more than twice the
    whole suite's budget. The file is read once at import and never changes during
    a run, so the tree is cached.
    """
    return ast.parse(SOURCE)


@lru_cache(maxsize=None)
def _node(name):
    for n in _tree().body:
        if isinstance(n, ast.FunctionDef) and n.name == name:
            return n
    return None


@lru_cache(maxsize=None)
def _code(name):
    node = _node(name)
    if node is None:
        return None
    return compile(ast.Module(body=[node], type_ignores=[]), str(WORKER), "exec")


def _function(name, globals_dict=None):
    code = _code(name)
    if code is None:
        pytest.fail(f"{name}() does not exist at module level in static/flow_worker.py")
    namespace = dict(globals_dict or {})
    exec(code, namespace)
    return namespace[name]


def _source_of(name):
    node = _node(name)
    if node is None:
        pytest.fail(f"{name}() does not exist at module level in static/flow_worker.py")
    return ast.get_source_segment(SOURCE, node) or ""


@lru_cache(maxsize=None)
def _const(name):
    """Value of a module-level literal constant (e.g. _FLOW_CREDITS_URL)."""
    for n in _tree().body:
        if isinstance(n, ast.Assign):
            for t in n.targets:
                if isinstance(t, ast.Name) and t.id == name:
                    try:
                        return ast.literal_eval(n.value)
                    except Exception:
                        return None
    return None


def _module_globals():
    """The module-level names the auth functions actually close over.

    Loading a function by AST gives it an EMPTY global namespace, so anything it
    references has to be supplied. Getting this wrong is not harmless: a missing
    name raises NameError inside the function, and if the product code catches
    broadly it will look like a clean 'no answer' instead of a broken test rig.
    That is precisely how the credits probe hid a NameError during development.
    """
    reason = _function("_fa_error_reason")
    base = {
        "_fa_error_reason": reason,
        "_fa_is_error": _function("_fa_is_error"),
        "_FA_AUTH_DENIAL_PHRASES": _const("_FA_AUTH_DENIAL_PHRASES") or (),
        "_FA_AUTH_METHOD_ERROR_PHRASES": _const("_FA_AUTH_METHOD_ERROR_PHRASES") or (),
        "_FLOW_CREDITS_URL": _const("_FLOW_CREDITS_URL")
        or "https://aisandbox-pa.googleapis.com/v1/credits",
        # v963.2 — the verdict now waits for the app to mint a bearer.
        "_FLOW_AUTH_BEARER_WAIT_S": _const("_FLOW_AUTH_BEARER_WAIT_S") or 20,
        "_fa_attach_token_listener": lambda *a, **k: None,
    }
    # _fa_is_auth_denial calls _fa_is_auth_method_error, so it needs it in scope.
    base["_fa_is_auth_method_error"] = _function("_fa_is_auth_method_error", dict(base))
    return base


def _auth_denial_fn():
    """_fa_is_auth_denial wired to the globals it has in the real module."""
    return _function("_fa_is_auth_denial", _module_globals())


def _denial(text=GOOGLE_DENIAL, status=401):
    return {"status": status, "data": {"error": {"message": text}}}


def _ok(payload=None):
    return {"status": 200, "data": payload or {"credits": 100}}


def _transport():
    """status 0 - the in-page fetch never got an HTTP answer at all."""
    return {"status": 0, "text": "fetch failed: NetworkError"}


class RecordingPage:
    """A page that records every auth-state write and when it happened."""

    def __init__(self, **initial):
        object.__setattr__(self, "_writes", [])
        object.__setattr__(self, "_fetches", [0])
        for k, v in initial.items():
            object.__setattr__(self, k, v)

    WATCHED = ("_flow_authenticated_api_proof", "_flow_auth_denied",
               "_flow_auth_confirmed_at")

    def __setattr__(self, name, value):
        if name in self.WATCHED:
            self._writes.append((name, value, self._fetches[0]))
        object.__setattr__(self, name, value)

    def reload(self, *a, **k):
        return None


def _replay(api_reply, trpc_reply=None, page=None, cleared=None):
    """Load _fa_init_project_best_effort with fakes and run one replay.

    api_reply/trpc_reply are called with the request url and return a fake result.
    """
    page = page if page is not None else RecordingPage()
    cleared = cleared if cleared is not None else []

    def fake_api(pg, url, method="GET", bearer="", body=None, *a, **k):
        page._fetches[0] += 1
        return api_reply(url)

    def fake_trpc(pg, url, *a, **k):
        page._fetches[0] += 1
        return (trpc_reply or (lambda _u: _transport()))(url)

    ns = {
        "FLOW_ORIGIN": "https://flow.google.com",
        "_FA_EXPERIMENT_IDS": "1,2,3",
        "_FA_GOOGLE_API_KEY": "APIKEY",
        "_FA_TOKEN_STORE": SimpleNamespace(token="bearer-token"),
        "_FA_USER_AGENT": "UA",
        "_fa_api_fetch": fake_api,
        "_fa_trpc_fetch": fake_trpc,
        "_fa_is_error": _function("_fa_is_error"),
        "_fa_error_reason": _function("_fa_error_reason"),
        "_fa_now_iso": lambda: "2026-09-07T00:00:00.000Z",
        "_fa_session_id": lambda: "session-id",
        "_fa_url_encode": lambda s: str(s),
        "flow_model_event": lambda *a, **k: None,
        "json": __import__("json"),
        "time": SimpleNamespace(time=_real_time.time, sleep=lambda *_: None),
        # Added by the repair (Step 2a); absent today, so the loader supplies them.
        "_clear_flow_auth_ready": lambda *a, **k: cleared.append(True),
    }
    ns.update(_module_globals())
    ns["_fa_is_auth_denial"] = _auth_denial_fn()

    fn = _function("_fa_init_project_best_effort", ns)
    result = fn(page, "b226b464-486b-4cef-aa7f-6c17b4e33332", context="TEST")
    return result, page, cleared


# ---------------------------------------------------------------- 1-2: primitives

def test_is_auth_denial_matches_google_texts_and_401_403():
    is_denial = _auth_denial_fn()
    assert is_denial({"status": 401}) is True
    assert is_denial({"status": 403}) is True
    for text in (GOOGLE_DENIAL,
                 "Expected OAuth2 access token or other authentication credentials"):
        assert is_denial({"status": 401, "data": {"error": {"message": text}}}) is True

    # CORRECTED v963.1. GOOGLE_APIKEY_DENIAL used to be in the list above, and
    # putting it there was wrong: "API keys are not supported by this API" is
    # Google refusing the auth METHOD (api key, no bearer), not rejecting the
    # session. It ran 16 times against a freshly-rebuilt, plausibly-live profile
    # before anyone noticed. Note it arrives WITH a 401/403 and still must not
    # count - which is why _fa_is_auth_denial checks the method error first.
    assert is_denial({"status": 401,
                      "data": {"error": {"message": GOOGLE_APIKEY_DENIAL}}}) is False

    # Not denials: transport, server error, success.
    assert is_denial(_transport()) is False
    assert is_denial({"status": 500, "text": "boom"}) is False
    assert is_denial(_ok()) is False
    assert is_denial({"text": "fetch failed: NetworkError"}) is False


def test_error_reason_shows_status_zero_text():
    reason = _function("_fa_error_reason")
    assert "fetch failed" in reason(_transport()), (
        "status 0 printed a blank reason in the failing run, which made six "
        "transport failures look like silent denials")


# ------------------------------------------------- 3-5b: the verdict is atomic

def test_replay_denial_clears_api_proof_and_reports_dead():
    page = RecordingPage(_flow_authenticated_api_proof="credits")
    result, page, cleared = _replay(lambda url: _denial(), page=page)
    assert result["confirmed"] is False
    assert result["denied"] > 0
    assert page._flow_authenticated_api_proof == ""
    assert page._flow_auth_denied is not None
    assert len(cleared) == 1, "a dead session must clear the ready flag exactly once"


def test_credits_200_confirms_even_with_a_denied_sibling():
    def api(url):
        return _ok() if CREDITS_MARKER in url else _denial()
    result, page, cleared = _replay(api)
    assert result["confirmed"] is True
    assert page._flow_authenticated_api_proof == "credits"
    assert page._flow_auth_denied is None
    assert page._flow_auth_confirmed_at > 0
    assert cleared == []


def test_credits_200_survives_denials_that_come_AFTER_it():
    """The regression test for Codex finding 1.

    credits is call 5 of 19 (flow_worker.py:1374); fourteen calls follow it. An
    implementation that clears the proof on each denial passes the test above and
    fails this one - which is exactly the bug that would declare a healthy session
    dead.
    """
    def api(url):
        return _ok() if CREDITS_MARKER in url else _denial()
    result, page, cleared = _replay(api)

    assert result["confirmed"] is True
    assert page._flow_authenticated_api_proof == "credits", (
        "a denial AFTER the credits 200 erased the confirmation - the verdict is "
        "still being decided per call instead of once at the end")
    assert page._flow_auth_denied is None
    assert cleared == [], "a confirmed session must never be marked dead"


def test_first_denial_is_the_one_recorded():
    seen = []

    def api(url):
        seen.append(url)
        return _denial(text=f"{GOOGLE_DENIAL} [{len(seen)}]")

    _result, page, _cleared = _replay(api)
    assert page._flow_auth_denied is not None
    assert "[1]" in page._flow_auth_denied.get("reason", ""), (
        "the FIRST denial should be recorded, not the last")


def test_transport_failures_are_not_denials():
    page = RecordingPage(_flow_authenticated_api_proof="credits",
                         _flow_auth_denied=None)
    result, page, cleared = _replay(lambda url: _transport(),
                                    trpc_reply=lambda url: _transport(), page=page)
    assert result["denied"] == 0
    assert result["confirmed"] is False
    assert page._flow_authenticated_api_proof == "credits", (
        "status 0 carries no auth information and must not move the verdict")
    assert page._flow_auth_denied is None
    assert cleared == []


def test_no_page_state_is_mutated_before_the_replay_ends():
    """Structural guard: the verdict is applied once, after the last call."""
    def api(url):
        return _ok() if CREDITS_MARKER in url else _denial()

    _result, page, _cleared = _replay(api)
    total = page._fetches[0]
    assert total > 1, "the replay should have made many calls"
    early = [(n, at) for (n, _v, at) in page._writes if at < total]
    assert not early, (
        f"auth state was written mid-replay at call(s) {early} of {total}; the "
        f"verdict must be computed after every call has answered")


# ------------------------------------------------ 6-8: the page-state verdict

def _page_state(api_reply, url="https://flow.google.com/project/abc", **page_attrs):
    calls = []

    def fake_api(pg, u, *a, **k):
        calls.append(u)
        return api_reply(u)

    class FakeLocator:
        def __init__(self, visible=False):
            self._visible = visible
            self.first = self

        def is_visible(self, timeout=None):
            return self._visible

    class FakePage:
        def __init__(self):
            self.url = url
            for k, v in page_attrs.items():
                setattr(self, k, v)

        def evaluate(self, *a, **k):
            raise AssertionError("DOM text must not decide authentication")

        def wait_for_load_state(self, *a, **k):
            return None

        def wait_for_function(self, *a, **k):
            raise AssertionError("DOM text must not decide authentication")

        def locator(self, sel):
            return FakeLocator(False)

    ns = {
        "_fa_api_fetch": fake_api,
        "_fa_is_error": _function("_fa_is_error"),
        "_fa_error_reason": _function("_fa_error_reason"),
        "_FA_TOKEN_STORE": SimpleNamespace(token="bearer-token"),
        "_FA_GOOGLE_API_KEY": "APIKEY",
        "flow_model_event": lambda *a, **k: None,
        "flow_ui_probe": lambda *a, **k: None,
        "_clear_flow_auth_ready": lambda *a, **k: None,
        "is_flow_url": lambda u: "flow.google.com" in u,
        "time": SimpleNamespace(time=_real_time.time, sleep=lambda *_: None),
    }
    ns.update(_module_globals())
    ns["_fa_is_auth_denial"] = _auth_denial_fn()
    ns["_fa_api_fetch"] = fake_api   # _module_globals must not shadow the fake
    fn = _function("_flow_page_state", ns)
    page = FakePage()
    return fn(page), page, calls


def test_page_state_never_confirms_from_dom():
    state, page, _calls = _page_state(lambda u: _denial())
    assert state == "flow_not_logged_in"
    assert getattr(page, "_flow_auth_denied", None) is not None

    state, page, _calls = _page_state(lambda u: _ok())
    assert state == "flow_logged_in"
    assert page._flow_authenticated_api_proof == "credits"


def test_page_state_remembers_a_denial_newer_than_the_confirm():
    def api(u):
        raise AssertionError("a remembered denial must short-circuit the probe")

    state, _page, calls = _page_state(
        api,
        _flow_authenticated_api_proof="credits",
        _flow_auth_confirmed_at=100.0,
        _flow_auth_denied={"label": "agentInfo", "reason": GOOGLE_DENIAL, "at": 200.0},
    )
    assert state == "flow_not_logged_in"
    assert calls == []


# ------------------------------------------------------- 9-10: source contracts

def test_dom_confirmation_strings_are_gone():
    for dead in ("visible signed-in Flow project editor DOM",
                 "visible signed-in Flow DOM"):
        assert dead not in SOURCE, (
            f"{dead!r} still sets an auth verdict from page text")


def test_headless_login_wait_is_zero():
    src = _source_of("ensure_logged_into_flow")
    head = src.split("\n    def ", 1)[0]
    assert "FIREFOX_HEADLESS" in head and "timeout_minutes = 0" in head, (
        "nobody can complete a Google sign-in on a headless worker, so waiting "
        "ten minutes for one just hides the wall")


# ---------------------------------------------------------------------------
# v963.1 — "API keys are not supported" is NOT a denial.
#
# Measured 2026-09-07 on the primary worker, whose golden had JUST been rebuilt
# from the operator's signed-in Firefox ("ff-pull: v914 stripped 6 labs.google
# cookie(s)", "golden built from tocgh2wh.default-release"):
#
#   [STARTUP] FLOW_AUTH_DENIED by 'credits': API keys are not supported by this
#   API. Expected OAuth2 access token or other authentication credentials...
#
# sixteen times in a row. Google was refusing the auth METHOD - /v1/credits was
# called as `?key=<api key>` with no bearer, because a cookie-only Firefox
# session exposes none until the app loads and the listener captures one. The
# reply says nothing about whether anyone is signed in.
#
# Counting it as a denial made every bearer-less session read as DEAD. That is
# the same "healthy session read as dead" failure the review already caught
# once, arriving through a different door: there, sibling denials erased a real
# confirmation; here, an unanswerable question was scored as a refusal.
#
# The rule that survives both: A DENIAL MEANS A CREDENTIAL WAS PRESENTED AND
# REJECTED.
# ---------------------------------------------------------------------------

METHOD_ERR = ("API keys are not supported by this API. Expected OAuth2 access "
              "token or other authentication credentials that assert a principal.")


def test_api_keys_not_supported_is_not_a_denial():
    is_denial = _auth_denial_fn()
    for status in (400, 401, 403):
        res = {"status": status, "data": {"error": {"message": METHOD_ERR}}}
        assert is_denial(res) is False, (
            f"HTTP {status} carrying the method error was scored as a denial; a "
            f"bearer-less session would be declared dead")


def test_api_keys_not_supported_is_recognised_as_a_method_error():
    fn = _function("_fa_is_auth_method_error", _module_globals())
    assert fn({"status": 403, "data": {"error": {"message": METHOD_ERR}}}) is True
    assert fn(_denial()) is False
    assert fn(_ok()) is False
    assert fn(_transport()) is False


def test_a_real_denial_still_denies_after_the_carve_out():
    """The carve-out must not blunt the thing it sits next to."""
    is_denial = _auth_denial_fn()
    assert is_denial(_denial()) is True
    assert is_denial({"status": 401}) is True
    assert is_denial({"status": 403}) is True


def test_the_verdict_waits_for_the_app_to_mint_a_bearer():
    """v963.2 — the app authenticating IS the check.

    `_fa_attach_token_listener` sniffs `Bearer ya29.*` off the page's own
    requests. A signed-in Flow app makes authenticated calls and mints one; a
    signed-out one never does, and a cached shell has nothing to authenticate
    with. So the worker watches for that instead of asking a question of its own
    invention - which is how it came to score "API keys are not supported"
    (an api key sent with no bearer) as a refusal, sixteen times, against a
    freshly-rebuilt profile.
    """
    state = _source_of("_flow_page_state")
    assert "_fa_attach_token_listener(p)" in state
    assert "_FLOW_AUTH_BEARER_WAIT_S" in state
    # credits is still called, but only WITH the bearer - never as the opener
    assert "_fa_api_fetch(" in state
    body = state.split("_FLOW_AUTH_BEARER_WAIT_S", 1)[1]
    assert body.index("_bearer") < body.index("_fa_api_fetch("), (
        "credits must be asked only after a bearer exists to ask it with")


def test_no_bearer_is_a_refusal_stated_by_the_app_itself():
    state = _source_of("_flow_page_state")
    seg = state.split("if not _bearer:", 1)[1].split("return", 1)[0]
    assert "_flow_auth_denied" in seg, (
        "an app that never authenticated in the whole window IS signed out, and "
        "that verdict must be recorded so later checks short-circuit")
    assert "never minted a bearer" in seg
