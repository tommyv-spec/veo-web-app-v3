"""v895 — when Flow never loads reCAPTCHA, load it ourselves.

Live evidence 2026-08-05 (node 4577, img-v583): 16 mint attempts, every one
'grecaptcha not available', page reloads changed nothing — the Flow app only
pulls enterprise.js when its own UI is about to generate, and a worker driving
the JSON API never triggers that. The mint JS now injects the script itself.
"""
import re
from pathlib import Path

import image_worker as iw

_JS = iw._FA_CAPTCHA_BATCH_JS


def test_js_injects_the_enterprise_script_with_the_site_key():
    assert "recaptcha/enterprise.js?render=" in _JS
    assert "createElement('script')" in _JS
    assert "encodeURIComponent(siteKey)" in _JS


def test_js_waits_for_grecaptcha_ready_before_execute():
    ready_at = _JS.index("enterprise.ready")
    exec_at = _JS.index("enterprise.execute(siteKey")
    assert ready_at < exec_at


def test_js_reports_diagnostics_with_the_failure():
    # script-tag count + typeof, so an unloaded script is distinguishable from
    # one that loaded but stayed invisible to this execution context.
    assert 'script[src*="recaptcha"]' in _JS
    assert "typeof window.grecaptcha" in _JS
    assert "execution context may be isolated" in _JS


def test_js_does_not_double_inject():
    assert "data-kaveno-recaptcha" in _JS


def test_invisible_after_load_is_not_an_account_block():
    reason = ("captcha mint failed (grecaptcha not visible after loading enterprise.js "
              "(execution context may be isolated) [recaptcha script tags=1, "
              "typeof grecaptcha=undefined])")
    assert iw._is_unusual(reason) is False


def test_invisible_after_load_does_not_trigger_another_reload():
    # the script DID load — reloading the page cannot change the outcome
    assert iw._fa_mint_page_not_ready(
        "grecaptcha not visible after loading enterprise.js") is False
    assert iw._fa_mint_page_not_ready("grecaptcha not available") is True


def test_blocked_script_still_counts_as_a_page_cause():
    reason = "captcha mint failed (grecaptcha not available: enterprise.js failed to load (blocked?))"
    assert iw._is_unusual(reason) is False


def test_execute_rejection_survives_the_rewrite():
    assert iw._is_unusual("captcha mint failed (grecaptcha execute rejected: blocked)") is True
