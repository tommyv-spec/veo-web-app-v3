"""v894 — mint failure cause + page-level recovery.

Node 4577 (2026-08-05) logged mint=10.0s(4x) — exactly the grecaptcha wait
timeout — meaning the reCAPTCHA script was never in the page. The worker
reported that as an account block and looped on golden restore + relaunch.
Now: the cause is reported, the page is reloaded once, and only a real
execute() rejection counts as a block.
"""
import image_worker as iw


class FakePage:
    """Returns queued mint results; records reloads."""

    def __init__(self, results):
        self._results = list(results)
        self.reloads = 0
        self.reload_fails = False

    def evaluate(self, _js, _args):
        if not self._results:
            raise AssertionError("mint called more times than the test queued")
        r = self._results.pop(0)
        if isinstance(r, Exception):
            raise r
        return r

    def reload(self, **_kw):
        self.reloads += 1
        if self.reload_fails:
            raise RuntimeError("navigation failed")


def test_absent_grecaptcha_reloads_then_recovers():
    page = FakePage([
        {"tokens": [], "err": "grecaptcha not available"},
        {"tokens": ["t1", "t2"], "err": ""},
    ])
    toks = iw._fa_mint_captcha_batch(page, "IMAGE_GENERATION", 2)
    assert toks == ["t1", "t2"]
    assert page.reloads == 1
    assert iw._FA_MINT_LAST_ERR["reason"] == ""


def test_absent_grecaptcha_after_reload_keeps_the_page_cause():
    page = FakePage([
        {"tokens": [], "err": "grecaptcha not available"},
        {"tokens": [], "err": "grecaptcha not available"},
    ])
    toks = iw._fa_mint_captcha_batch(page, "IMAGE_GENERATION", 2)
    assert toks == ["", ""]
    assert page.reloads == 1
    # the cause must survive so _is_unusual keeps this OUT of the restore path
    assert iw._is_unusual(f"captcha mint failed ({iw._FA_MINT_LAST_ERR['reason']})") is False


def test_execute_rejected_does_not_reload_and_stays_a_block():
    page = FakePage([{"tokens": ["", ""], "err": "grecaptcha execute rejected: blocked"}])
    toks = iw._fa_mint_captcha_batch(page, "IMAGE_GENERATION", 2)
    assert toks == ["", ""]
    assert page.reloads == 0  # reloading cannot fix a flagged account
    assert iw._is_unusual(f"captcha mint failed ({iw._FA_MINT_LAST_ERR['reason']})") is True


def test_reload_failure_is_survivable():
    page = FakePage([{"tokens": [], "err": "grecaptcha not available"}])
    page.reload_fails = True
    assert iw._fa_mint_captcha_batch(page, "IMAGE_GENERATION", 2) == ["", ""]


def test_pre_v894_list_shape_still_works():
    page = FakePage([["a", "b"]])
    assert iw._fa_mint_captcha_batch(page, "IMAGE_GENERATION", 2) == ["a", "b"]
    assert page.reloads == 0


def test_evaluate_exception_is_a_page_cause_not_a_block():
    page = FakePage([RuntimeError("Execution context was destroyed"),
                     {"tokens": [], "err": "grecaptcha not available"}])
    assert iw._fa_mint_captcha_batch(page, "IMAGE_GENERATION", 1) == [""]
    assert page.reloads == 1
    assert iw._is_unusual("captcha mint failed (mint evaluate failed: boom)") is False


def test_short_token_list_is_padded():
    page = FakePage([{"tokens": ["only-one"], "err": ""}])
    assert iw._fa_mint_captcha_batch(page, "IMAGE_GENERATION", 3) == ["only-one", "", ""]
