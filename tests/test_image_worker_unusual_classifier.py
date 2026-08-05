# tests/test_image_worker_unusual_classifier.py
#
# v828: the flow_api account-block classifier `_is_unusual` is the ONLY gate the
# golden-restore trigger depends on (submit_image raises -> _is_unusual(reason)?
# -> yes -> _signal_unusual -> page._flow_api_unusual_reason -> _restore_signal
# -> RELAUNCH_GOLDEN). Two real "unusual activity" manifestations were misrouted
# before v828, so the restore never fired:
#   (1) a flagged account cannot mint a reCAPTCHA token -> submit_image raises
#       "captcha mint failed" (no UNUSUAL_ACTIVITY / RECAPTCHA token in the
#       string) -> classified transient -> DOM cookie-clear (which does NOT clear
#       Flow's block) -> golden restore NEVER triggered.
#   (2) a bare 403 / PERMISSION_DENIED block WITHOUT the literal word "RECAPTCHA"
#       failed the old AND-ed condition (RECAPTCHA and 403).
#
# Strategy (mirrors test_image_worker_scoping.py): extract the `_is_unusual`
# source via AST and exec it in an isolated namespace, so the test does NOT need
# the full image_worker module (playwright / console-encoding) to import.

import ast
from pathlib import Path

_SRC = Path(__file__).resolve().parent.parent / "image_worker.py"


def _load_is_unusual():
    tree = ast.parse(_SRC.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "_is_unusual":
            mod = ast.Module(body=[node], type_ignores=[])
            ast.fix_missing_locations(mod)
            ns = {}
            exec(compile(mod, str(_SRC), "exec"), ns)
            return ns["_is_unusual"]
    raise AssertionError("_is_unusual not found in image_worker.py")


_is_unusual = _load_is_unusual()


def _reason(e):
    # mirror how the submit loop builds the reason string (image_worker.py:4619)
    return f"submit_image raised: {e}"


# --- POSITIVE: real account-block manifestations must classify as unusual ---

def test_captcha_mint_failure_is_unusual():
    # Vector 1 — flagged account can't mint reCAPTCHA
    assert _is_unusual(_reason("submit_image failed: captcha mint failed")) is True


def test_grecaptcha_execute_rejected_is_unusual():
    # v894 — the script LOADED and execute() was refused: still an account block.
    assert _is_unusual(_reason(
        "submit_image failed: captcha mint failed (grecaptcha execute rejected: "
        "Invalid site key)")) is True


def test_bare_permission_denied_403_is_unusual():
    # Vector 2 — 403 block without the literal RECAPTCHA word
    assert _is_unusual(_reason(
        "submit_image failed: The caller does not have permission "
        "| status=PERMISSION_DENIED | code=403")) is True


def test_plain_http_403_is_unusual():
    assert _is_unusual(_reason("submit_image failed: HTTP 403: blocked")) is True


def test_unusual_activity_token_is_unusual():
    assert _is_unusual(_reason("submit_image failed: PUBLIC_ERROR_UNUSUAL_ACTIVITY")) is True


def test_recaptcha_with_403_is_unusual():
    assert _is_unusual(_reason("submit_image failed: reCAPTCHA rejected | code=403")) is True


# --- NEGATIVE: per-prompt / transient errors must NOT trigger a golden restore ---

def test_unsafe_generation_is_not_unusual():
    assert _is_unusual(_reason("submit_image failed: UNSAFE_GENERATION")) is False


def test_user_quota_is_not_unusual():
    assert _is_unusual(_reason("submit_image failed: USER_QUOTA_REACHED")) is False


def test_server_500_is_not_unusual():
    assert _is_unusual(_reason("submit_image failed: INTERNAL | status=INTERNAL | code=500")) is False


def test_plain_timeout_is_not_unusual():
    assert _is_unusual(_reason("TimeoutError: fetch timed out")) is False


def test_empty_reason_is_not_unusual():
    assert _is_unusual("") is False
    assert _is_unusual(None) is False


# --- v894: a mint that failed because the PAGE never loaded reCAPTCHA is a page
# problem. Calling it an account block drove an endless golden-restore/relaunch
# loop (operator 2026-08-05, node 4577: mint=10.0s(4x) = the wait timeout). ---

def test_grecaptcha_absent_is_not_unusual():
    assert _is_unusual(_reason(
        "submit_image failed: captcha mint failed (grecaptcha not available)")) is False


def test_mint_evaluate_failure_is_not_unusual():
    assert _is_unusual(_reason(
        "submit_image failed: captcha mint failed (mint evaluate failed: "
        "Execution context was destroyed)")) is False
