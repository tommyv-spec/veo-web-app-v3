"""Every Flow task shares one singleton because every worker shares one profile."""

import importlib.util
import os
import sys
import types
import unittest
from pathlib import Path

WORKER = Path(__file__).resolve().parents[1] / "static" / "flow_worker.py"
SOURCE = WORKER.read_text(encoding="utf-8")


def _load_scope_fn():
    """Load ONLY the path helper, without importing the 30k-line worker."""
    start = SOURCE.index("def _flow_worker_singleton_path")
    end = SOURCE.index("def _flow_worker_hold_path")
    mod = types.ModuleType("_scope_probe")
    mod.__dict__["os"] = os
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
