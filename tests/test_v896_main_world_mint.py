"""v896 — mint reCAPTCHA in the page's MAIN world.

Patchright runs page.evaluate in an ISOLATED execution context, and
window.grecaptcha lives in the MAIN world. Probed on the operator's live
session 2026-08-05:

    eval_typeof_grecaptcha = "undefined"   <- what page.evaluate sees
    main_world_typeof      = "object"      <- what the page actually has
    recaptcha_script_tags  = 2             <- Flow had already loaded it

So no script was missing and no account was blocked; the worker was simply
looking in the wrong world. Loading enterprise.js again (v895) changed
nothing. A <script> tag runs in the main world and the DOM is shared, so the
mint runs there and returns its tokens through a DOM attribute.

Live verification of this exact constant on the Flow project page:
4/4 tokens minted in 0.9s, second call 2/2, zero leftover holders in the DOM.
"""
import image_worker as iw

_JS = iw._FA_CAPTCHA_BATCH_JS


def test_mint_runs_inside_an_injected_script_tag():
    # a <script> tag is the main world; page.evaluate alone is not
    assert "createElement('script')" in _JS
    assert "s.textContent = mainWorld" in _JS


def test_tokens_come_back_through_the_dom():
    assert "setAttribute('data-tokens'" in _JS
    assert "getAttribute('data-tokens')" in _JS
    assert "setAttribute('data-done'" in _JS


def test_holder_is_removed_on_every_path():
    # left-behind holders would collide across mints on a long-lived page
    assert _JS.count("holder.remove()") >= 2


def test_holder_id_is_unique_per_call():
    assert "Math.random()" in _JS


def test_enterprise_js_load_stays_as_a_fallback_in_the_main_world():
    load_at = _JS.index("recaptcha/enterprise.js?render=")
    main_at = _JS.index("const mainWorld")
    end_at = _JS.index("s.textContent = mainWorld")
    assert main_at < load_at < end_at   # the loader is inside the main-world code


def test_ready_is_awaited_before_execute():
    assert _JS.index("enterprise.ready") < _JS.index("enterprise.execute(KEY")


def test_timeout_reports_the_csp_suspicion_with_script_tag_count():
    assert "main-world mint timed out" in _JS
    assert "blocked by CSP" in _JS
    assert 'script[src*="recaptcha"]' in _JS


# --- classifier: every main-world failure is a PAGE cause, never a block ---

def test_main_world_unavailable_is_not_an_account_block():
    assert iw._is_unusual(
        "captcha mint failed (grecaptcha not available in main world)") is False


def test_main_world_timeout_is_not_an_account_block():
    assert iw._is_unusual(
        "captcha mint failed (main-world mint timed out (inline script may be "
        "blocked by CSP; recaptcha script tags=2))") is False


def test_injection_failure_is_not_an_account_block():
    assert iw._is_unusual(
        "captcha mint failed (main-world script injection failed: denied)") is False


def test_execute_rejection_is_still_an_account_block():
    assert iw._is_unusual(
        "captcha mint failed (grecaptcha execute rejected: blocked)") is True


def test_single_token_path_uses_the_main_world_mint(monkeypatch):
    # the old single-token JS ran in the isolated world and could only fail
    calls = {}

    def fake_raw(page, action, n):
        calls["n"] = n
        return ["tok"], ""

    monkeypatch.setattr(iw, "_fa_mint_batch_raw", fake_raw)
    assert iw._fa_mint_or_empty(object(), "IMAGE_GENERATION") == "tok"
    assert calls["n"] == 1
    assert iw._FA_MINT_LAST_ERR["reason"] == ""


def test_single_token_path_keeps_the_failure_cause(monkeypatch):
    monkeypatch.setattr(iw, "_fa_mint_batch_raw",
                        lambda p, a, n: ([""], "grecaptcha not available in main world"))
    assert iw._fa_mint_or_empty(object(), "IMAGE_GENERATION") == ""
    assert iw._is_unusual(f"captcha mint failed ({iw._FA_MINT_LAST_ERR['reason']})") is False
