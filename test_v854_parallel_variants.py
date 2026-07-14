"""v854 — parallel variant submit.

The N batchGenerateImages POSTs used to be fired one at a time, and each POST
blocks until Flow has rendered its image (~18s), so a x4 node cost ~72s of
serial wall time. These tests pin the new behaviour: one concurrent round, N
distinct seeds, and the v818.4 / v843 / v844 failure semantics preserved.

Run: python -m pytest test_v854_parallel_variants.py -q
"""
import json
import time

import image_worker as iw


class FakePage:
    """Stands in for a Playwright page. Records every evaluate() call so a test
    can assert HOW MANY round-trips happened, not just what came back."""

    url = "https://labs.google/fx/tools/flow/project/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

    def __init__(self, responses):
        # responses: list of per-body result dicts, consumed one batch at a time
        self._responses = list(responses)
        self.fetch_calls = []      # one entry per _FA_BATCH_FETCH_JS evaluate
        self.mint_calls = []       # one entry per _FA_CAPTCHA_BATCH_JS evaluate

    def evaluate(self, script, arg=None):
        if script is iw._FA_CAPTCHA_BATCH_JS:
            _site, _action, n = arg
            self.mint_calls.append(n)
            return [f"tok{i}" for i in range(n)]
        if script is iw._FA_BATCH_FETCH_JS:
            _url, _method, _headers, body_strs = arg
            self.fetch_calls.append([json.loads(b) for b in body_strs])
            out = []
            for _ in body_strs:
                out.append(self._responses.pop(0) if self._responses else _ok())
            return out
        raise AssertionError("unexpected evaluate")


def _ok(media_id="11111111-2222-3333-4444-555555555555", fife="https://flow-content.google/image/x"):
    return {"status": 200, "ok": True, "text": "", "data": {
        "media": [{"name": media_id, "image": {"generatedImage": {"fifeUrl": fife}}}]
    }}


def _err(reason, code=400):
    return {"status": code, "ok": False, "text": "", "data": {
        "error": {"code": code, "message": reason, "status": "INVALID_ARGUMENT"}
    }}


def _client(page):
    cli = _FaClientNoToken(page, project_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    cli.reset_timings()
    return cli


class _FaClientNoToken(iw._FaClient):
    """Real client, minus the bearer-token wait (no live Flow tab in a test)."""

    def __init__(self, page, **kw):
        self.page = page
        self.project_id = kw.get("project_id", "")
        self.tier = None
        self._last_call = 0.0
        self._t = self._zero_timings()

    def _token(self):
        return "fake-bearer"


def test_all_four_variants_fire_in_ONE_round_trip():
    page = FakePage([_ok(fife=f"https://flow-content.google/image/{i}") for i in range(4)])
    res = _client(page).submit_image_batch("a prompt", "NARWHAL", 4, cooldown=False)

    # The whole point of v854: one mint call, one fetch call — not four of each.
    assert page.mint_calls == [4]
    assert len(page.fetch_calls) == 1
    assert len(page.fetch_calls[0]) == 4
    assert [r["ok"] for r in res] == [True] * 4
    assert len({r["fife_url"] for r in res}) == 4


def test_each_variant_gets_a_DISTINCT_seed():
    # The serial path got distinct seeds for free (each call re-read the clock).
    # A parallel batch builds all N bodies in the same millisecond, so if the
    # seed isn't set per-slot, Flow returns N identical images.
    page = FakePage([_ok() for _ in range(4)])
    _client(page).submit_image_batch("a prompt", "NARWHAL", 4, cooldown=False)

    seeds = [b["requests"][0]["seed"] for b in page.fetch_calls[0]]
    assert len(set(seeds)) == 4, f"variants shared a seed: {seeds}"


def test_cooldown_is_paid_once_for_the_whole_batch():
    # Not 4 × 10s. The batch is one submit burst.
    page = FakePage([_ok() for _ in range(4)])
    cli = _client(page)
    cli._last_call = time.monotonic()   # pretend a call just happened
    iw._FA_API_COOLDOWN = 2             # keep the test fast
    t0 = time.monotonic()
    cli.submit_image_batch("p", "NARWHAL", 4, cooldown=True)
    waited = time.monotonic() - t0
    assert 1.5 <= waited < 4.0, f"expected ONE ~2s cooldown, waited {waited:.1f}s"


def test_mint_failure_fails_only_its_own_variant():
    class NoMintPage(FakePage):
        def evaluate(self, script, arg=None):
            if script is iw._FA_CAPTCHA_BATCH_JS:
                self.mint_calls.append(arg[2])
                return ["tok0", "", "tok2", ""]     # slots 1 and 3 fail to mint
            return super().evaluate(script, arg)

    page = NoMintPage([_ok(), _ok()])
    res = _client(page).submit_image_batch("p", "NARWHAL", 4, cooldown=False)

    assert [r["ok"] for r in res] == [True, False, True, False]
    assert len(page.fetch_calls[0]) == 2            # only the minted slots were fired
    assert "captcha mint failed" in res[1]["reason"]


def test_partial_success_is_reported_per_slot():
    page = FakePage([_ok(), _err("Internal error", 500), _ok(), _err("PUBLIC_ERROR_UNSAFE_GENERATION")])
    res = _client(page).submit_image_batch("p", "NARWHAL", 4, cooldown=False)

    assert [r["ok"] for r in res] == [True, False, True, False]
    assert "500" in res[1]["reason"] or "Internal" in res[1]["reason"]
    assert "UNSAFE_GENERATION" in res[3]["reason"]


def test_outcome_buckets_still_feed_the_timing_line():
    page = FakePage([_ok(), _err("Internal error", 500), _err("PUBLIC_ERROR_UNSAFE_GENERATION"), _ok()])
    cli = _client(page)
    cli.submit_image_batch("p", "NARWHAL", 4, cooldown=False)
    summary = cli.timings_summary()

    assert "ok=2" in summary
    assert "5xx=1" in summary
    assert "other=1" in summary
    assert "fetch=" in summary and "mint=" in summary


def test_classify_never_calls_an_unusual_block_a_captcha_hiccup():
    # v843: an 'unusual activity' 403 must not be retried. It mentions captcha
    # internals, so the classifier has to keep the two apart.
    assert iw._FaClient._classify("PUBLIC_ERROR_UNUSUAL_ACTIVITY code=403") == "unusual"
    assert iw._FaClient._classify("captcha mint failed") == "recaptcha"
    assert iw._FaClient._classify("INTERNAL 500") == "5xx"
    assert iw._FaClient._classify("PUBLIC_ERROR_UNSAFE_GENERATION") == "other"


def test_batch_fetch_bad_shape_degrades_to_per_body_errors():
    class BadShapePage(FakePage):
        def evaluate(self, script, arg=None):
            if script is iw._FA_BATCH_FETCH_JS:
                return {"not": "a list"}
            return super().evaluate(script, arg)

    page = BadShapePage([])
    res = _client(page).submit_image_batch("p", "NARWHAL", 3, cooldown=False)
    assert [r["ok"] for r in res] == [False, False, False]
    assert all("bad shape" in r["reason"] for r in res)
