# v914 — the golden must ship Google SSO but NO labs.google session.
#
# Operator's working recipe (2026-08-07), which they had been doing by hand:
#   1. chrome://settings -> labs.google -> Delete data
#   2. do NOT open Flow again
#   3. build the golden / start the worker
#   -> generations work
#
# Why it works: the golden then holds Google SSO but no Flow session, so the
# first entry into Flow makes SSO mint a FRESH labs session, which passes
# reCAPTCHA. Probe evidence 2026-08-06: goldens that WORKED carried zero labs
# session cookies, and a single click on "Create with Google Flow" minted
# __Secure-next-auth.session-token on the spot.
#
# When the profile still carries a labs session (Flow was opened before the
# pull), the golden inherits an already-flagged session and EVERY generate
# returns 403 'reCAPTCHA evaluation failed' / PUBLIC_ERROR_UNUSUAL_ACTIVITY.
# Golden restores cannot fix that - each one faithfully restores the flagged
# session, which is exactly what was observed all night.
#
# _prune_handshake_cookies deliberately KEPT __Secure-next-auth.session-token -
# the one cookie that must go.

import os
import sqlite3
import tempfile
import unittest

PULL = os.path.join(os.path.dirname(__file__), "..", "static", "worker_profile_pull.py")


def _source():
    with open(PULL, encoding="utf-8") as f:
        return f.read()


def _load_pruner():
    """Exec just _prune_labs_session_cookies in isolation."""
    src = _source()
    i = src.index("def _prune_labs_session_cookies(")
    j = src.index("def _prune_handshake_cookies(")
    ns = {"os": os}
    exec(src[i:j], ns)
    return ns["_prune_labs_session_cookies"]


def _make_cookie_db(rows):
    path = os.path.join(tempfile.mkdtemp(), "Cookies")
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE cookies (host_key TEXT, name TEXT)")
    con.executemany("INSERT INTO cookies (host_key, name) VALUES (?, ?)", rows)
    con.commit()
    con.close()
    return path


class TestV914GoldenShipsSSOOnly(unittest.TestCase):
    def test_labs_session_cookies_are_removed(self):
        """The exact 6 cookies the operator deletes by hand."""
        db = _make_cookie_db([
            (".labs.google", "__Secure-next-auth.session-token"),
            (".labs.google", "__Host-next-auth.csrf-token"),
            (".labs.google", "__Secure-next-auth.callback-url"),
            (".labs.google", "EMAIL"),
            (".labs.google", "email"),
            (".labs.google", "_ga"),
        ])
        n = _load_pruner()(db)
        self.assertEqual(6, n)
        con = sqlite3.connect(db)
        self.assertEqual(0, con.execute("SELECT COUNT(*) FROM cookies").fetchone()[0])
        con.close()

    def test_google_sso_is_preserved(self):
        """SSO is what mints the fresh session - deleting it would log the
        worker out entirely."""
        db = _make_cookie_db([
            (".labs.google", "__Secure-next-auth.session-token"),
            (".google.com", "SID"),
            (".google.com", "__Secure-1PSID"),
            (".google.com", "SAPISID"),
            ("accounts.google.com", "LSID"),
        ])
        n = _load_pruner()(db)
        self.assertEqual(1, n, "only the labs cookie should go")
        con = sqlite3.connect(db)
        names = {r[0] for r in con.execute("SELECT name FROM cookies").fetchall()}
        con.close()
        self.assertEqual({"SID", "__Secure-1PSID", "SAPISID", "LSID"}, names)

    def test_session_token_specifically_dies(self):
        """The old handshake prune kept this one; it is the whole problem."""
        db = _make_cookie_db([(".labs.google", "__Secure-next-auth.session-token"),
                              (".google.com", "SID")])
        _load_pruner()(db)
        con = sqlite3.connect(db)
        left = [r[0] for r in con.execute("SELECT name FROM cookies").fetchall()]
        con.close()
        self.assertNotIn("__Secure-next-auth.session-token", left)

    def test_missing_db_is_not_fatal(self):
        self.assertEqual(-1, _load_pruner()(os.path.join(tempfile.mkdtemp(), "nope")))

    def test_builder_calls_the_pruner(self):
        src = _source()
        self.assertIn("_prune_labs_session_cookies(", src)
        i = src.index("_labs_pruned = _prune_labs_session_cookies(")
        j = src.index("_pruned = _prune_handshake_cookies(")
        self.assertLess(i, j, "labs strip should run with the other golden cookie surgery")


if __name__ == "__main__":
    unittest.main()
