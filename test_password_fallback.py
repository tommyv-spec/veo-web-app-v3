"""v1012 — clear a PASSWORD wall unattended, and only a password wall.

Operator 2026-09-16, asked to run the lane with no supervision: *"if i give you
the google password can you store it safely and use it?"*

Measured in `~/.kaveno/flow_worker.log` before building anything:

    signin/challenge/pwd ............ 7     <- a PASSWORD challenge
    2-Step / device-verify / TOTP .... 0
    "Already logged in on Flow" ..... 84

So the wall is a password prompt with nothing behind it, and the worker does not
even try: v963 collapses the wait to zero when headless ("a headless worker
cannot be signed in by a human") and it exits with "Login timeout after 0
minutes!". Seven renders' worth of lane time, recoverable by typing one string.

THE TWO THINGS THIS MUST NOT DO, both of which are worse than the outage:

1. **Type the password into anything that is not a password box.** A 2-Step
   code page, a device-verification page and a reCAPTCHA all live under
   `accounts.google.com/v3/signin/challenge/...`. Typing a password into a 2SV
   field sends it somewhere it does not belong and burns an attempt. The URL
   must say `pwd` specifically — never "a challenge appeared".

2. **Retry.** This project already measured that rapid automated sign-ins are
   what CREATE these challenges (8 restarts in 4h produced the wall it was then
   blamed on). A password that retries turns one wall into a lockout of the
   account the whole business runs on. Two attempts, then the lane holds and
   waits for a human — deliberately fewer than feels useful.

And the secret itself never reaches a log line, a command line or this repo: it
is read in-process from Windows Credential Manager by `kaveno_secret`.
"""
import importlib.util
import os
import pathlib
import sys
import unittest

_STATIC = pathlib.Path(__file__).parent / "static"
_PATH = pathlib.Path(os.environ.get("FLOW_WORKER_PATH") or (_STATIC / "flow_worker.py"))


def _load():
    if str(_STATIC) not in sys.path:
        sys.path.insert(0, str(_STATIC))
    spec = importlib.util.spec_from_file_location("flow_worker_v1012", _PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


PWD_URL = "https://accounts.google.com/v3/signin/challenge/pwd?tl=acv9tzg4du"


class OnlyOnARealPasswordWall(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fw = _load()

    def test_a_password_challenge_qualifies(self):
        self.assertTrue(self.fw.password_login_allowed(PWD_URL, attempts=0))

    def test_a_second_attempt_is_allowed(self):
        self.assertTrue(self.fw.password_login_allowed(PWD_URL, attempts=1))

    def test_a_third_attempt_is_not(self):
        """Retrying is what turns a wall into a lockout."""
        self.assertFalse(self.fw.password_login_allowed(PWD_URL, attempts=2))
        self.assertFalse(self.fw.password_login_allowed(PWD_URL, attempts=9))

    def test_a_two_step_challenge_is_refused(self):
        """A 2SV code box is not a password box, and a password typed into one
        is sent somewhere it does not belong."""
        for url in (
            "https://accounts.google.com/v3/signin/challenge/totp?tl=x",
            "https://accounts.google.com/v3/signin/challenge/dp?tl=x",
            "https://accounts.google.com/v3/signin/challenge/az?tl=x",
            "https://accounts.google.com/v3/signin/challenge/ipp?tl=x",
        ):
            self.assertFalse(self.fw.password_login_allowed(url, attempts=0), url)

    def test_a_recaptcha_challenge_is_refused(self):
        url = "https://accounts.google.com/v3/signin/challenge/recaptcha?tl=x"
        self.assertFalse(self.fw.password_login_allowed(url, attempts=0))

    def test_an_ordinary_flow_page_is_refused(self):
        self.assertFalse(self.fw.password_login_allowed(
            "https://flow.google.com/project/abc", attempts=0))

    def test_nonsense_never_raises(self):
        for url in (None, "", 12345):
            self.assertFalse(self.fw.password_login_allowed(url, attempts=0))


class TheSecretStaysOutOfTheLog(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.src = _PATH.read_text(encoding="utf-8", errors="replace")

    def test_the_password_is_never_formatted_into_a_message(self):
        """One f-string is all it takes to put it in a log file for ever."""
        at = self.src.index("def password_login_allowed")
        body = self.src[at:at + 4000]
        for bad in ("{secret}", "{pw}", "{password}", "+ secret", "+ pw"):
            self.assertNotIn(bad, body, f"the secret must never be printed: {bad}")

    def test_it_reads_from_the_credential_store_not_an_env_var(self):
        """argv and the environment are both readable by every process on the
        box; the DPAPI store is scoped to this Windows user. The worker runs
        standalone from ~/veo-worker, so it reads the store directly rather than
        importing tools/kaveno_secret — same target name, one owner of the value."""
        at = self.src.index("def stored_google_password")
        body = self.src[at:at + 1600]
        self.assertIn("CredRead", body)
        self.assertNotIn("os.environ", body)
        # The shared target name lives in a module constant, which is the point:
        # one owner of the string, so the worker and kaveno_secret cannot drift
        # onto two different credential entries.
        self.assertIn('GOOGLE_SECRET_TARGET = "kaveno:google:', self.src)
        self.assertIn("GOOGLE_SECRET_TARGET", body)

    def test_it_decodes_the_blob_as_utf_16(self):
        """CredWrite stores UTF-16-LE. A UTF-8 decode yields a string of the
        right length made of the wrong characters — a login that fails while
        looking exactly like a wrong password."""
        at = self.src.index("def stored_google_password")
        self.assertIn("utf-16-le", self.src[at:at + 1600])

    def test_no_password_means_no_attempt_rather_than_a_crash(self):
        fw = _load()
        self.assertEqual("", fw.stored_google_password(_read=lambda t: ""))


if __name__ == "__main__":
    unittest.main(verbosity=2)
