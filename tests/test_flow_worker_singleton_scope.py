"""Every Flow task shares one singleton because every worker shares one profile."""

import hashlib
import importlib.util
import os
import re
import sys
import types
import unittest
from pathlib import Path
from unittest import mock

WORKER = Path(__file__).resolve().parents[1] / "static" / "flow_worker.py"
SOURCE = WORKER.read_text(encoding="utf-8")


def _load_scope_fn():
    """Load ONLY the path helper, without importing the 30k-line worker."""
    start = SOURCE.index("def _flow_worker_singleton_path")
    end = SOURCE.index("def _flow_worker_hold_path")
    mod = types.ModuleType("_scope_probe")
    mod.__dict__["os"] = os
    # v963.4 — the helper keys the lock on the profile path, so it needs the
    # same globals the real module gives it. Without these the existing tests
    # still pass (they return early before touching them), which would have
    # hidden a NameError on the branch that actually runs in production.
    mod.__dict__["re"] = re
    mod.__dict__["_hashlib"] = hashlib
    exec(compile(SOURCE[start:end], "<scope>", "exec"), mod.__dict__)
    return mod.__dict__["_flow_worker_singleton_path"]


path_for = _load_scope_fn()


class SingletonScopeTests(unittest.TestCase):
    def test_general_worker_keeps_the_original_lock_name(self):
        """The one-per-machine guarantee for UNSCOPED workers is unchanged."""
        self.assertEqual("flow_worker.singleton.lock",
                         os.path.basename(path_for(scope_raw="")))
        self.assertEqual("flow_worker.singleton.lock",
                         os.path.basename(path_for(scope_raw=None if False else "")))

    def test_a_scoped_worker_collides_with_the_general_one(self):
        self.assertEqual(path_for(scope_raw=""), path_for(scope_raw="14905,14994"))

    def test_the_same_scope_still_collides(self):
        """Order and spacing must not create a second lock for one clip set."""
        self.assertEqual(path_for(scope_raw="14905,14994"),
                         path_for(scope_raw=" 14994 , 14905 "))
        self.assertEqual(path_for(scope_raw="14905,14994"),
                         path_for(scope_raw="14994;14905"))

    def test_different_scopes_still_share_one_profile_lock(self):
        self.assertEqual(path_for(scope_raw="14994"), path_for(scope_raw="14905"))
        self.assertEqual(path_for(scope_raw="14994"),
                         path_for(scope_raw="14905,14994"))

    def test_an_empty_or_junk_scope_is_treated_as_general(self):
        """Fail CLOSED: anything that is not a real scope takes the general lock,
        so a typo can never quietly buy a second unscoped worker."""
        for junk in ("", "   ", ",", ",,,", "; ,"):
            self.assertEqual("flow_worker.singleton.lock",
                             os.path.basename(path_for(scope_raw=junk)),
                             f"scope {junk!r} must fall back to the general lock")

    def test_the_lock_name_is_stable(self):
        name = os.path.basename(path_for(scope_raw="14905,14994"))
        self.assertEqual("flow_worker.singleton.lock", name)


class SingletonScopeWiringTests(unittest.TestCase):
    def test_the_refusal_message_literal_is_still_contiguous(self):
        """test_flow_worker_singleton.py asserts this exact substring against the
        SOURCE. Splitting it across concatenation would break that test silently."""
        self.assertIn("Flow worker singleton is already owned; exiting", SOURCE)

    def test_the_hold_may_allow_scope_but_singleton_stays_global(self):
        """A scoped exception to the hold never creates a second profile owner."""
        start = SOURCE.index("def _flow_worker_singleton_path")
        end = SOURCE.index("def _acquire_flow_worker_singleton")
        self.assertNotIn("FLOW_ONLY_CLIP_IDS", SOURCE[start:SOURCE.index("def _flow_worker_hold_path")])
        hold = SOURCE.index("def _flow_worker_hold_blocks_start")
        self.assertIn("FLOW_ONLY_CLIP_IDS", SOURCE[hold:hold + 500])


if __name__ == "__main__":
    unittest.main()


# ---------------------------------------------------------------------------
# v963.4 — the lock is per PROFILE, not per machine.
#
# The flat machine-wide lock is right whenever there is one Firefox profile and
# wrong the moment there are two. ~/veo-worker-b exists to be a second worker
# with its OWN session folder, golden and cache; it shares no file with the
# primary. Under the flat lock the primary died instantly and silently every
# time worker-b was up:
#
#   [Init] Flow worker singleton is already owned; exiting cleanly.
#          (general worker) lock=flow_worker.singleton.lock
#
# An 80-clip job sat untouched for an hour behind that, while the log the
# operator was shown said two workers were running.
#
# What must NOT change: two workers on the SAME profile still collide, whatever
# their scope. That is the hazard the flat lock was written for and every test
# above still proves it.
# ---------------------------------------------------------------------------

class SingletonIsPerProfile(unittest.TestCase):
    def _path(self, session_folder):
        env = dict(os.environ)
        if session_folder is None:
            env.pop("SESSION_FOLDER", None)
        else:
            env["SESSION_FOLDER"] = session_folder
        with mock.patch.dict(os.environ, env, clear=True):
            return path_for()

    def test_two_different_profiles_get_two_different_locks(self):
        a = self._path(r"C:\Users\tomma\veo-worker\firefox-session")
        b = self._path(r"C:\Users\tomma\veo-worker-b\firefox-session-2")
        self.assertNotEqual(a, b)

    def test_the_same_profile_still_gives_one_lock(self):
        a = self._path(r"C:\Users\tomma\veo-worker-b\firefox-session-2")
        b = self._path(r"c:\users\tomma\veo-worker-b\firefox-session-2\\")
        self.assertEqual(a, b, "case and a trailing slash are the same profile")

    def test_the_default_profile_keeps_the_original_lock_name(self):
        for default in (None, r"C:\Users\tomma\veo-worker\chrome-session"):
            self.assertTrue(
                self._path(default).endswith("flow_worker.singleton.lock"),
                f"{default!r} must keep the historic name")

    def test_same_named_folders_under_different_parents_do_not_collide(self):
        a = self._path(r"C:\a\firefox-session-2")
        b = self._path(r"C:\b\firefox-session-2")
        self.assertNotEqual(a, b)
